//! Wave-3 supervision drills: session-id collision (E08), nonresponsive
//! adapter (E03/H-14), panicked drive reconciliation (E07), and
//! broadcast-lag capture gaps (E06/H-16).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use agent_client_protocol::v1::schema::{
    ContentBlock, ContentChunk, MessageId, SessionUpdate, StopReason, TextContent,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use wrkspc_acp::SessionEvent;
use wrkspc_core::{RecipientRequest, Store};
use wrkspc_dispatcher::{
    Dispatcher, DispatcherConfig, SessionBinding, SessionDriver, SessionProvider,
};

fn config() -> DispatcherConfig {
    DispatcherConfig {
        tick: Duration::from_millis(10),
        lookup_timeout: Duration::from_millis(300),
    }
}

/// Seeds a store with the operator, two harness peers, and one
/// executable message per peer. Returns the store + conversation.
async fn seeded() -> (Store, String) {
    let store = Store::open_in_memory("supervision-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    for peer in ["codex", "agy"] {
        store.ensure_peer(peer, peer, "harness").await.unwrap();
    }
    let conversation = store
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();
    for (key, peer) in [("k-codex", "codex"), ("k-agy", "agy")] {
        store
            .accept_message(
                &conversation,
                "operator",
                key,
                &format!("work for {peer}"),
                false,
                &[RecipientRequest {
                    peer_id: peer.to_string(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap();
    }
    (store, conversation)
}

/// Spawns the dispatcher over `provider`; returns the shutdown sender
/// and the handle (kept alive — dropping the handle ends the run loop,
/// since it holds the only command-channel sender).
fn spawn_dispatcher(
    store: Store,
    provider: Arc<dyn SessionProvider>,
) -> (oneshot::Sender<()>, wrkspc_dispatcher::DispatcherHandle) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (dispatcher, handle) = Dispatcher::new(store, provider, config());
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("dispatcher run");
    });
    (shutdown_tx, handle)
}

/// E08: two independent adapters may return the same session id. The
/// busy gate is keyed by PEER id, so a held turn for peer A must not
/// block peer B's dispatch — the collision must not conflate sessions.
#[tokio::test]
async fn shared_session_ids_do_not_serialize_distinct_peers() {
    let (store, _conversation) = seeded().await;

    // Two distinct drivers advertising the SAME adapter-chosen session
    // id (independent adapters have no shared namespace — the
    // repository fake returns `fake-N` per process).
    let held = common::FakeDriver::new(common::Mode::BlockUntilCancel);
    let free = common::FakeDriver::new(common::Mode::Respond(StopReason::EndTurn));
    let provider = Arc::new(common::FakeProvider {
        bindings: HashMap::from([
            (
                "codex".to_string(),
                SessionBinding {
                    peer_id: "codex".to_string(),
                    session_id: "shared-session-1".to_string(),
                    driver: Arc::new(held.clone()),
                },
            ),
            (
                "agy".to_string(),
                SessionBinding {
                    peer_id: "agy".to_string(),
                    session_id: "shared-session-1".to_string(),
                    driver: Arc::new(free.clone()),
                },
            ),
        ]),
    });
    let (shutdown, _handle) = spawn_dispatcher(store, provider);

    // Peer A's turn parks in its driver; peer B must still dispatch.
    held.wait_for_prompt().await;
    free.wait_for_prompt().await;
    let _ = shutdown.send(());
}

/// E03/H-14: a provider whose lookup never returns must not block the
/// dispatcher's command loop. Cancel keeps being serviced, shutdown
/// completes, and the claim task is abandoned at the deadline.
#[tokio::test]
async fn nonresponsive_lookup_does_not_block_cancel_or_shutdown() {
    let (store, conversation) = seeded().await;

    struct HungProvider;
    impl SessionProvider for HungProvider {
        fn lookup(
            &self,
            _peer_id: &str,
        ) -> Pin<Box<dyn Future<Output = Option<SessionBinding>> + Send + '_>> {
            Box::pin(std::future::pending())
        }
    }

    let (dispatcher, handle) = Dispatcher::new(store.clone(), Arc::new(HungProvider), config());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("dispatcher run");
    });

    // While the lookup hangs, the command loop still services a cancel
    // (the intent is still queued — no attempt exists yet).
    let messages = store.list_messages(&conversation).await.unwrap();
    let message_id = messages[0].id.clone();
    let outcome = handle
        .request_cancel(&message_id, "codex", "operator")
        .await
        .expect("cancel must be serviced while lookup hangs");
    assert!(
        matches!(outcome, wrkspc_core::CancelOutcome::IntentCanceled),
        "a queued intent cancels directly: {outcome:?}"
    );

    // Shutdown also completes: the hung lookup rides an aborted task,
    // never the run loop.
    let _ = shutdown_tx.send(());
}

/// E07: a drive task that panics reconciles its attempt as unknown —
/// the session is freed by the guard, but the attempt is blocked until
/// the operator decides, and the failure is visible in the feed.
#[tokio::test]
async fn panicked_drive_reconciles_the_attempt_unknown() {
    let (store, conversation) = seeded().await;

    struct PanicDriver;
    impl SessionDriver for PanicDriver {
        fn prompt(
            &self,
            _session_id: &str,
            _blocks: Vec<ContentBlock>,
        ) -> Pin<Box<dyn Future<Output = Result<StopReason, String>> + Send + '_>> {
            Box::pin(async { panic!("injected drive panic") })
        }
        fn cancel(
            &self,
            _session_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            drop(tx);
            rx
        }
    }
    let provider = Arc::new(common::FakeProvider {
        bindings: HashMap::from([(
            "codex".to_string(),
            SessionBinding {
                peer_id: "codex".to_string(),
                session_id: "sess-codex".to_string(),
                driver: Arc::new(PanicDriver),
            },
        )]),
    });
    let (shutdown, _handle) = spawn_dispatcher(store.clone(), provider);

    for _ in 0..300 {
        if store
            .events_since(&conversation, 0, 100)
            .await
            .unwrap()
            .iter()
            .any(|e| {
                e.kind == "dispatch.unknown"
                    && e.payload["reason"]
                        .as_str()
                        .is_some_and(|r| r.contains("panicked"))
            })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    assert!(
        events.iter().any(|e| e.kind == "dispatch.unknown"
            && e.payload["reason"]
                .as_str()
                .is_some_and(|r| r.contains("panicked"))),
        "the panicked drive must reconcile unknown with a visible event: {:?}",
        events
            .iter()
            .map(|e| (&e.kind, &e.payload))
            .collect::<Vec<_>>(),
    );
    // The session slot was freed by the guard; the OTHER peer's
    // no-binding path also reconciles unknown — both are visible, and
    // the panicked attempt is among them.
    let unknown = store.unknown_attempts().await.unwrap();
    assert_eq!(
        unknown.len(),
        2,
        "the panicked attempt and the no-binding attempt are both unknown: {unknown:#?}"
    );
    let _ = shutdown.send(());
}

/// E06/H-16: an adapter emitting faster than the receiver drains must
/// leave a visible capture gap — never a silent truncation.
#[tokio::test]
async fn broadcast_lag_commits_a_capture_gap_and_keeps_recording() {
    let (store, conversation) = seeded().await;

    let (events_tx, _) = tokio::sync::broadcast::channel::<SessionEvent>(4);
    let driver = Arc::new(SharedBurstDriver { events_tx });
    let provider = Arc::new(common::FakeProvider {
        bindings: HashMap::from([(
            "codex".to_string(),
            SessionBinding {
                peer_id: "codex".to_string(),
                session_id: "sess-codex".to_string(),
                driver: driver.clone(),
            },
        )]),
    });
    let (shutdown, _handle) = spawn_dispatcher(store.clone(), provider);

    for _ in 0..300 {
        if store
            .events_since(&conversation, 0, 200)
            .await
            .unwrap()
            .iter()
            .any(|e| e.kind == "capture.gap")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let events = store.events_since(&conversation, 0, 500).await.unwrap();
    let gaps: Vec<u64> = events
        .iter()
        .filter(|e| e.kind == "capture.gap")
        .map(|e| e.payload["count"].as_u64().unwrap_or(0))
        .collect();
    assert!(
        !gaps.is_empty() && gaps.iter().sum::<u64>() > 0,
        "the burst must leave a committed capture gap: {:?}",
        events.iter().map(|e| &e.kind).collect::<Vec<_>>(),
    );
    let chunks = events.iter().filter(|e| e.kind == "turn.chunk").count();
    assert!(
        chunks > 0,
        "surviving chunks must still be recorded after the gap"
    );
    let _ = shutdown.send(());
}

/// A burst driver over a shared broadcast channel: 300 chunks emitted
/// without yielding, far past the 4-slot receiver capacity.
struct SharedBurstDriver {
    events_tx: tokio::sync::broadcast::Sender<SessionEvent>,
}

impl SessionDriver for SharedBurstDriver {
    fn prompt(
        &self,
        session_id: &str,
        blocks: Vec<ContentBlock>,
    ) -> Pin<Box<dyn Future<Output = Result<StopReason, String>> + Send + '_>> {
        let tx = self.events_tx.clone();
        let session_id = session_id.to_string();
        Box::pin(async move {
            for i in 0..300u32 {
                let _ = tx.send(SessionEvent {
                    session_id: session_id.clone(),
                    update: SessionUpdate::AgentMessageChunk(
                        ContentChunk::new(ContentBlock::Text(TextContent::new(format!(
                            "burst {i}"
                        ))))
                        .message_id(MessageId::new(format!("m{i}"))),
                    ),
                });
            }
            let _ = blocks;
            Ok(StopReason::EndTurn)
        })
    }
    fn cancel(
        &self,
        _session_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
    fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
        self.events_tx.subscribe()
    }
}

/// E09: operator guidance is marked delivered only after a prompt
/// resolves with a terminal stop reason. A turn that fails before the
/// send leaves the guidance undelivered so the next usable turn
/// re-rides it; a successful turn consumes it.
#[tokio::test]
async fn guidance_is_delivered_only_after_a_successful_turn() {
    let (store, conversation) = seeded().await;

    struct FailingDriver;
    impl SessionDriver for FailingDriver {
        fn prompt(
            &self,
            _session_id: &str,
            _blocks: Vec<ContentBlock>,
        ) -> Pin<Box<dyn Future<Output = Result<StopReason, String>> + Send + '_>> {
            Box::pin(async { Err("adapter refused the prompt".to_string()) })
        }
        fn cancel(
            &self,
            _session_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            drop(tx);
            rx
        }
    }

    // One guidance note for the room.
    let _guidance = store
        .add_guidance(
            &conversation,
            "operator",
            "hold the line until further notice",
        )
        .await
        .unwrap();

    // The failing driver: the prompt never succeeds, so the guidance
    // must stay undelivered.
    let provider = Arc::new(common::FakeProvider {
        bindings: HashMap::from([(
            "codex".to_string(),
            SessionBinding {
                peer_id: "codex".to_string(),
                session_id: "sess-codex".to_string(),
                driver: Arc::new(FailingDriver),
            },
        )]),
    });
    let (stop_tx, _handle) = spawn_dispatcher(store.clone(), provider);
    for _ in 0..300 {
        if store
            .events_since(&conversation, 0, 100)
            .await
            .unwrap()
            .iter()
            .any(|e| e.kind == "dispatch.unknown")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        store
            .undelivered_guidance(&conversation)
            .await
            .unwrap()
            .len(),
        1,
        "guidance must survive a failed turn (it never rode a prompt)"
    );
    let _ = stop_tx.send(());

    // A fresh store with a healthy driver consumes the guidance.
    let (store2, conversation2) = seeded().await;
    let _guidance2 = store2
        .add_guidance(
            &conversation2,
            "operator",
            "hold the line until further notice",
        )
        .await
        .unwrap();
    let healthy = common::FakeDriver::new(common::Mode::Respond(StopReason::EndTurn));
    let provider2 = Arc::new(common::FakeProvider {
        bindings: HashMap::from([(
            "codex".to_string(),
            SessionBinding {
                peer_id: "codex".to_string(),
                session_id: "sess-codex".to_string(),
                driver: Arc::new(healthy.clone()),
            },
        )]),
    });
    let (stop_tx2, _handle2) = spawn_dispatcher(store2.clone(), provider2);
    healthy.wait_for_prompt().await;
    for _ in 0..300 {
        if store2
            .undelivered_guidance(&conversation2)
            .await
            .unwrap()
            .is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        store2
            .undelivered_guidance(&conversation2)
            .await
            .unwrap()
            .is_empty(),
        "a successful turn marks the guidance delivered"
    );
    let _ = stop_tx2.send(());
}
