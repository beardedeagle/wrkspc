#![allow(dead_code)]
//! Shared test doubles for the dispatcher integration tests: a fake
//! session driver (immediate respond or block-until-cancel), a static
//! provider, and the spawn harness over an in-memory store.

use agent_client_protocol::v1::schema::{
    ContentBlock, ContentChunk, MessageId, SessionUpdate, StopReason, TextContent,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use wrkspc_acp::SessionEvent;
use wrkspc_core::{RecipientRequest, Store};
use wrkspc_dispatcher::{
    Dispatcher, DispatcherConfig, DispatcherHandle, SessionBinding, SessionDriver, SessionProvider,
};

/// A fake session driver. Modes:
/// - `Respond(StopReason)`: prompts resolve immediately.
/// - `BlockUntilCancel`: prompts hang until `cancel()` is called, then
///   resolve `Cancelled` (mirrors the ACP cancellation carrier).
#[derive(Clone)]
pub struct FakeDriver {
    inner: Arc<FakeInner>,
}

struct FakeInner {
    mode: Mutex<Mode>,
    prompt_log: Mutex<Vec<String>>,
    /// One release slot per blocked prompt: a second `BlockUntilCancel`
    /// prompt must queue instead of overwriting the first waiter.
    release: Mutex<Vec<oneshot::Sender<()>>>,
    release_waiters: tokio::sync::Notify,
    events_tx: tokio::sync::broadcast::Sender<SessionEvent>,
    /// Concurrent prompts on this driver: the serialization proof
    /// asserts the observed maximum, not wall-clock timestamps.
    in_flight: std::sync::atomic::AtomicUsize,
    max_in_flight: std::sync::atomic::AtomicUsize,
}

#[derive(Clone)]
pub enum Mode {
    Respond(StopReason),
    BlockUntilCancel,
    /// Blocks until `FakeDriver::release_all()` — lets a test hold two
    /// turns in flight at once and prove CONCURRENT dispatch (not
    /// merely distinct session ids).
    BlockUntilReleased,
    /// Returns the stop reason first and emits the turn's chunk AFTER
    /// a short delay — the real-stdio ordering (response before the
    /// trailing update), which the dispatcher must not silently drop.
    RespondThenChunk(StopReason),
}

impl FakeDriver {
    pub fn new(mode: Mode) -> Self {
        let (events_tx, _) = tokio::sync::broadcast::channel::<SessionEvent>(64);
        Self {
            inner: Arc::new(FakeInner {
                mode: Mutex::new(mode),
                prompt_log: Mutex::new(Vec::new()),
                release: Mutex::new(Vec::new()),
                release_waiters: tokio::sync::Notify::new(),
                events_tx,
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                max_in_flight: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    pub fn prompt_texts(&self) -> Vec<String> {
        self.inner.prompt_log.lock().unwrap().clone()
    }

    /// The largest number of prompts this driver had outstanding at
    /// once — the per-session serialization proof.
    pub fn max_overlap(&self) -> usize {
        self.inner
            .max_in_flight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Releases every `BlockUntilReleased` turn.
    pub fn release_all(&self) {
        self.inner.release_waiters.notify_waiters();
    }

    pub async fn wait_for_prompt(&self) {
        wait_until("a prompt", Duration::from_secs(10), || {
            Box::pin(async { !self.inner.prompt_log.lock().unwrap().is_empty() })
        })
        .await;
    }
}

impl SessionDriver for FakeDriver {
    fn prompt(
        &self,
        session_id: &str,
        blocks: Vec<ContentBlock>,
    ) -> Pin<Box<dyn Future<Output = Result<StopReason, String>> + Send + '_>> {
        let inner = Arc::clone(&self.inner);
        let session_id = session_id.to_string();
        Box::pin(async move {
            let text: Vec<String> = blocks
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text(t) => Some(t.text.clone()),
                    _ => None,
                })
                .collect();
            let joined = text.join(" | ");
            inner.prompt_log.lock().unwrap().push(joined.clone());
            // One agent chunk per turn, content-derived so attribution is
            // observable per conversation.
            let _ = inner.events_tx.send(SessionEvent {
                session_id: session_id.clone(),
                update: SessionUpdate::AgentMessageChunk(
                    ContentChunk::new(ContentBlock::Text(TextContent::new(format!(
                        "reply: {joined}"
                    ))))
                    .message_id(MessageId::new("reply-message")),
                ),
            });
            let now = inner
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            inner
                .max_in_flight
                .fetch_max(now, std::sync::atomic::Ordering::SeqCst);
            let result = {
                let mode = inner.mode.lock().unwrap().clone();
                match mode {
                    Mode::Respond(reason) => Ok(reason),
                    Mode::BlockUntilCancel => {
                        let (tx, rx) = oneshot::channel();
                        inner.release.lock().unwrap().push(tx);
                        let _ = rx.await;
                        Ok(StopReason::Cancelled)
                    }
                    Mode::BlockUntilReleased => {
                        inner.release_waiters.notified().await;
                        Ok(StopReason::EndTurn)
                    }
                    Mode::RespondThenChunk(reason) => {
                        // The terminal response goes first; the trailing
                        // chunk follows on its own task — the real-stdio
                        // ordering.
                        let events_tx = inner.events_tx.clone();
                        let sid = session_id.clone();
                        let trailing = joined.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            let _ = events_tx.send(SessionEvent {
                                session_id: sid,
                                update: SessionUpdate::AgentMessageChunk(
                                    ContentChunk::new(ContentBlock::Text(TextContent::new(
                                        format!("trailing: {trailing}"),
                                    )))
                                    .message_id(MessageId::new("trailing-message")),
                                ),
                            });
                        });
                        Ok(reason)
                    }
                }
            };
            inner
                .in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            result
        })
    }

    fn cancel(
        &self,
        _session_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let senders: Vec<_> = std::mem::take(&mut *inner.release.lock().unwrap());
            for tx in senders {
                let _ = tx.send(());
            }
            Ok(())
        })
    }

    fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
        self.inner.events_tx.subscribe()
    }
}

/// Static provider: peer id → one fake binding, with a lookup counter
/// so tests can observe that a sweep actually ran (the positive signal
/// a fixed sleep cannot give).
pub struct FakeProvider {
    pub bindings: HashMap<String, SessionBinding>,
    lookups: std::sync::atomic::AtomicUsize,
}

impl FakeProvider {
    pub fn new(bindings: HashMap<String, SessionBinding>) -> Self {
        Self {
            bindings,
            lookups: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many lookups this provider has served (any peer).
    pub fn lookup_count(&self) -> usize {
        self.lookups.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl SessionProvider for FakeProvider {
    fn lookup(
        &self,
        peer_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<SessionBinding>> + Send + '_>>
    {
        self.lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let bindings = self.bindings.clone();
        let peer_id = peer_id.to_string();
        Box::pin(async move { bindings.get(&peer_id).cloned() })
    }
}

pub struct Harness {
    pub store: Store,
    pub conversation_id: String,
    pub handle: DispatcherHandle,
    pub shutdown: mpsc::Sender<()>,
    pub driver: FakeDriver,
    /// The concrete provider the dispatcher runs on (kept so tests can
    /// read its lookup counter).
    pub provider: Arc<FakeProvider>,
}

/// Seeds the standard fixture into `store`: the operator and agent
/// peers, one conversation, and one executable message for the agent.
/// Returns the conversation id and the accepted message id.
pub async fn seed_fixture(store: &Store) -> (String, String) {
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    let conversation_id = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    let wrkspc_core::AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation_id,
            "operator",
            "k1",
            "do the thing",
            false,
            &[RecipientRequest {
                peer_id: "agent".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .expect("accept")
    else {
        panic!("fresh accept");
    };
    (conversation_id, message_id)
}

/// Builds the fake driver/provider and spawns the dispatcher over an
/// already-seeded store (the restart drill seeds and claims BEFORE the
/// dispatcher exists, so seeding and spawning must be separable).
pub fn spawn_dispatcher_over(
    store: Store,
    conversation_id: String,
    mode: Mode,
    with_binding: bool,
) -> Harness {
    let driver = FakeDriver::new(mode);
    let mut bindings = HashMap::new();
    if with_binding {
        bindings.insert(
            "agent".to_string(),
            SessionBinding {
                peer_id: "agent".to_string(),
                session_id: "sess-agent".to_string(),
                driver: Arc::new(driver.clone()),
            },
        );
    }
    let provider = Arc::new(FakeProvider::new(bindings));
    let provider_dyn: Arc<dyn SessionProvider> = provider.clone();
    let config = DispatcherConfig {
        tick: Duration::from_millis(10),
        lookup_timeout: std::time::Duration::from_secs(10),
        ..Default::default()
    };
    let (dispatcher, handle) = Dispatcher::new(store.clone(), provider_dyn, config);
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        let mut shutdown_rx = shutdown_rx;
        dispatcher
            .run(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("dispatcher run");
    });
    Harness {
        store,
        conversation_id,
        handle,
        shutdown: shutdown_tx,
        driver,
        provider,
    }
}

pub async fn spawn_harness(mode: Mode, with_binding: bool) -> Harness {
    let store = Store::open_in_memory("dispatcher-test").await.unwrap();
    let (conversation_id, _message_id) = seed_fixture(&store).await;
    spawn_dispatcher_over(store, conversation_id, mode, with_binding)
}

impl Harness {
    /// Stops the dispatcher and returns the conversation's event feed.
    pub async fn stop_and_events(&self) -> Vec<(String, String)> {
        let _ = self.shutdown.clone().send(()).await;
        self.store
            .events_since(&self.conversation_id, 0, 100)
            .await
            .unwrap()
            .into_iter()
            .map(|e| (e.kind, e.payload.to_string()))
            .collect()
    }

    pub async fn wait_for_kind(&self, kind: &str) {
        wait_until(&format!("event {kind}"), Duration::from_secs(10), || {
            let store = self.store.clone();
            let conversation_id = self.conversation_id.clone();
            let kind = kind.to_string();
            Box::pin(async move {
                store
                    .events_since(&conversation_id, 0, 100)
                    .await
                    .unwrap()
                    .iter()
                    .any(|e| e.kind == kind)
            })
        })
        .await;
    }
}

/// Polls `check` every 10 ms until it returns true, panicking with a
/// deadline message on timeout. One budget and one panic message for
/// every test sweep-wait instead of per-test magic-number loops.
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !check().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn payload_has(events: &[(String, String)], kind: &str, needle: &str) -> bool {
    events
        .iter()
        .any(|(k, payload)| k == kind && payload.contains(needle))
}
