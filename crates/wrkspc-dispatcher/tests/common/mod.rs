#![allow(dead_code)]
//! Shared test doubles for the dispatcher integration tests: a fake
//! session driver (immediate respond or block-until-cancel), a static
//! provider, and the spawn harness over an in-memory store.

use agent_client_protocol::schema::v1::{
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
    release: Mutex<Option<oneshot::Sender<()>>>,
    release_waiters: tokio::sync::Notify,
    events_tx: tokio::sync::broadcast::Sender<SessionEvent>,
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
                release: Mutex::new(None),
                release_waiters: tokio::sync::Notify::new(),
                events_tx,
            }),
        }
    }

    pub fn prompt_texts(&self) -> Vec<String> {
        self.inner.prompt_log.lock().unwrap().clone()
    }

    /// Releases every `BlockUntilReleased` turn.
    pub fn release_all(&self) {
        self.inner.release_waiters.notify_waiters();
    }

    pub async fn wait_for_prompt(&self) {
        for _ in 0..100 {
            if !self.inner.prompt_log.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("prompt never arrived");
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
            let mode = inner.mode.lock().unwrap().clone();
            match mode {
                Mode::Respond(reason) => Ok(reason),
                Mode::BlockUntilCancel => {
                    let (tx, rx) = oneshot::channel();
                    *inner.release.lock().unwrap() = Some(tx);
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
                                ContentChunk::new(ContentBlock::Text(TextContent::new(format!(
                                    "trailing: {trailing}"
                                ))))
                                .message_id(MessageId::new("trailing-message")),
                            ),
                        });
                    });
                    Ok(reason)
                }
            }
        })
    }

    fn cancel(
        &self,
        _session_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            if let Some(tx) = inner.release.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Ok(())
        })
    }

    fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
        self.inner.events_tx.subscribe()
    }
}

/// Static provider: peer id → one fake binding.
pub struct FakeProvider {
    pub bindings: HashMap<String, SessionBinding>,
}

impl SessionProvider for FakeProvider {
    fn lookup(
        &self,
        peer_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<SessionBinding>> + Send + '_>>
    {
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
}

pub async fn spawn_harness(mode: Mode, with_binding: bool) -> Harness {
    let store = Store::open_in_memory("dispatcher-test").await.unwrap();
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
    let _ = store
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
        .expect("accept");

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
    let provider = Arc::new(FakeProvider { bindings });
    let config = DispatcherConfig {
        tick: Duration::from_millis(10),
        lookup_timeout: std::time::Duration::from_secs(10),
    };
    let (dispatcher, handle) = Dispatcher::new(store.clone(), provider, config);
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
    }
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
        for _ in 0..200 {
            let events = self
                .store
                .events_since(&self.conversation_id, 0, 100)
                .await
                .unwrap();
            if events.iter().any(|e| e.kind == kind) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("event {kind} never appeared");
    }
}

pub fn payload_has(events: &[(String, String)], kind: &str, needle: &str) -> bool {
    events
        .iter()
        .any(|(k, payload)| k == kind && payload.contains(needle))
}
