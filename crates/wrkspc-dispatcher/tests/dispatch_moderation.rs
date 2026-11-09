//! Dispatch lifecycle and moderation: happy path, refusal, cancels
//! (mid-turn and queued), pause fences, and held deliveries.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use agent_client_protocol::schema::v1::StopReason;
use common::{FakeDriver, Mode, payload_has, spawn_harness};
use std::sync::Arc;
use std::time::Duration;
use wrkspc_core::{CancelOutcome, FenceKind};
use wrkspc_dispatcher::{Dispatcher, DispatcherConfig, SessionBinding, SessionProvider};

/// The happy path dispatches to completion: started and stopped(completed) events with the end_turn reason.
#[tokio::test]
async fn happy_path_dispatch_to_completion() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), true).await;
    harness.wait_for_kind("dispatch.stopped").await;
    let events = harness.stop_and_events().await;

    assert!(payload_has(&events, "dispatch.started", "\"attempt_id\""),);
    assert!(payload_has(
        &events,
        "dispatch.stopped",
        "\"state\":\"completed\""
    ),);
    assert!(payload_has(
        &events,
        "dispatch.stopped",
        "\"stop_reason\":\"end_turn\""
    ),);
    assert!(
        harness.store.due_intents().await.unwrap().is_empty(),
        "delivered intent leaves the due set"
    );
}

/// A refusal stop reason maps to a failed attempt with the refusal provenance.
#[tokio::test]
async fn refusal_maps_to_failed_with_provenance() {
    let harness = spawn_harness(Mode::Respond(StopReason::Refusal), true).await;
    harness.wait_for_kind("dispatch.stopped").await;
    let events = harness.stop_and_events().await;

    assert!(payload_has(
        &events,
        "dispatch.stopped",
        "\"state\":\"failed\""
    ),);
    assert!(payload_has(
        &events,
        "dispatch.stopped",
        "\"stop_reason\":\"refusal\""
    ),);
}

/// Cancel mid-turn interrupts the session and lands dispatch.stopped(canceled) with the cancel-request event.
#[tokio::test]
async fn cancel_mid_turn_interrupts_the_session() {
    let harness = spawn_harness(Mode::BlockUntilCancel, true).await;
    harness.driver.wait_for_prompt().await;

    let outcome = harness
        .handle
        .request_cancel("k1-does-not-exist", "agent", "operator")
        .await;
    // The above uses a bogus message id: proves the NOT_FOUND path first.
    assert!(outcome.is_err(), "unknown intent cancel fails");

    // Find the real message id from the store (only one message exists).
    let message_id = {
        let events = harness
            .store
            .events_since(&harness.conversation_id, 0, 100)
            .await
            .unwrap();
        events
            .iter()
            .find_map(|e| {
                if e.kind == "message.accepted" {
                    e.payload["message_id"].as_str().map(String::from)
                } else {
                    None
                }
            })
            .expect("accepted message")
    };
    let outcome = harness
        .handle
        .request_cancel(&message_id, "agent", "operator")
        .await
        .expect("cancel request");
    assert!(matches!(outcome, CancelOutcome::Attempt { .. }));

    harness.wait_for_kind("dispatch.stopped").await;
    let events = harness.stop_and_events().await;
    assert!(payload_has(
        &events,
        "dispatch.stopped",
        "\"state\":\"canceled\""
    ),);
    assert!(payload_has(
        &events,
        "dispatch.stopped",
        "\"stop_reason\":\"cancelled\""
    ),);
    assert!(payload_has(&events, "cancel.requested", "\"attempt_id\""),);
}

/// A queued intent cancels directly (IntentCanceled) and never reaches the session.
#[tokio::test]
async fn cancel_queued_intent_before_dispatch() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), true).await;
    // Pause the conversation so the intent stays queued.
    let _fence = harness
        .store
        .set_fence(
            &harness.conversation_id,
            "agent",
            FenceKind::Pause,
            "operator",
        )
        .await
        .unwrap();

    let message_id = {
        let events = harness
            .store
            .events_since(&harness.conversation_id, 0, 100)
            .await
            .unwrap();
        events
            .iter()
            .find_map(|e| {
                if e.kind == "message.accepted" {
                    e.payload["message_id"].as_str().map(String::from)
                } else {
                    None
                }
            })
            .expect("accepted message")
    };
    let outcome = harness
        .handle
        .request_cancel(&message_id, "agent", "operator")
        .await
        .expect("cancel");
    assert_eq!(outcome, CancelOutcome::IntentCanceled);

    harness.wait_for_kind("intent.canceled").await;
    let events = harness.stop_and_events().await;
    assert!(payload_has(&events, "intent.canceled", "\"intent_id\""),);
    assert!(
        !payload_has(&events, "dispatch.started", "attempt_id"),
        "a queued cancel never dispatches"
    );
    assert!(
        harness.driver.prompt_texts().is_empty(),
        "no prompt reached the session"
    );
}

/// A pause fence holds dispatch; clearing it dispatches exactly one prompt.
#[tokio::test]
async fn pause_fence_blocks_dispatch_until_cleared() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), true).await;
    let fence_id = harness
        .store
        .set_fence(
            &harness.conversation_id,
            "agent",
            FenceKind::Pause,
            "operator",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        harness.driver.prompt_texts().is_empty(),
        "paused conversation never dispatches"
    );

    harness
        .store
        .clear_fence(&fence_id, "operator")
        .await
        .unwrap();
    harness.wait_for_kind("dispatch.stopped").await;
    assert_eq!(harness.driver.prompt_texts().len(), 1);
}

/// A held delivery waits for the operator's release before dispatching.
#[tokio::test]
async fn held_delivery_waits_for_release() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), true).await;
    let message_id = {
        let events = harness
            .store
            .events_since(&harness.conversation_id, 0, 100)
            .await
            .unwrap();
        events
            .iter()
            .find_map(|e| {
                if e.kind == "message.accepted" {
                    e.payload["message_id"].as_str().map(String::from)
                } else {
                    None
                }
            })
            .expect("accepted message")
    };
    harness
        .store
        .hold_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        harness.driver.prompt_texts().is_empty(),
        "held delivery never dispatches"
    );

    harness
        .store
        .release_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();
    harness.wait_for_kind("dispatch.stopped").await;
    assert_eq!(harness.driver.prompt_texts().len(), 1);
}

/// A provider whose SECOND lookup parks and signals, then waits for
/// the test's release: the first lookup serves the busy gate (so the
/// intent is claimed), the second — inside the drive task, before the
/// prompt — is the deterministic window in which post-claim moderation
/// lands.
struct GateableProvider {
    binding: SessionBinding,
    lookups: std::sync::atomic::AtomicUsize,
    release_rx: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    parked_tx: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl GateableProvider {
    fn new(
        binding: SessionBinding,
    ) -> (
        Arc<Self>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                binding,
                lookups: std::sync::atomic::AtomicUsize::new(0),
                release_rx: tokio::sync::Mutex::new(Some(release_rx)),
                parked_tx: tokio::sync::Mutex::new(Some(parked_tx)),
            }),
            release_tx,
            parked_rx,
        )
    }
}

impl SessionProvider for GateableProvider {
    fn lookup(
        &self,
        peer_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<SessionBinding>> + Send + '_>>
    {
        let binding = self.binding.clone();
        let release_rx = &self.release_rx;
        let parked_tx = &self.parked_tx;
        let peer_id = peer_id.to_string();
        let n = self
            .lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        Box::pin(async move {
            if binding.peer_id != peer_id {
                return None;
            }
            if n >= 2 {
                if let Some(tx) = parked_tx.lock().await.take() {
                    let _ = tx.send(());
                }
                if let Some(rx) = release_rx.lock().await.take() {
                    let _ = rx.await;
                }
            }
            Some(binding)
        })
    }
}

/// C-4 (moderation half): a pause fence that lands AFTER the claim,
/// while the drive task prepares, must stop the prompt at the dispatch
/// boundary — the attempt is closed as gated (not canceled) and the
/// intent returns to the queue; once the fence clears, the delivery
/// dispatches normally. Nothing was lost.
#[tokio::test]
async fn pause_landing_after_claim_but_before_the_prompt_gates_the_turn_and_requeues() {
    let store = wrkspc_core::Store::open_in_memory("c4-pause")
        .await
        .unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    store
        .accept_message(
            &conversation,
            "operator",
            "k-1",
            "do the thing",
            false,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agent".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();

    let driver = FakeDriver::new(Mode::Respond(StopReason::EndTurn));
    let binding = SessionBinding {
        peer_id: "agent".to_string(),
        session_id: "sess-agent".to_string(),
        driver: Arc::new(driver.clone()),
    };
    let (provider, release_lookup, parked) = GateableProvider::new(binding);
    let (dispatcher, _handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
            lookup_timeout: std::time::Duration::from_secs(10),
        },
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let mut shutdown_rx = shutdown_rx;
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("dispatcher run");
    });

    // The drive task parks in its second lookup AFTER the claim. Land
    // the pause fence while it is parked, then let it reach the
    // dispatch boundary gate.
    parked.await.unwrap();
    store
        .set_fence(
            &conversation,
            "agent",
            wrkspc_core::FenceKind::Pause,
            "operator",
        )
        .await
        .unwrap();
    let _ = release_lookup.send(());

    // The turn must NOT have run, and the stop is recorded as gated —
    // not canceled.
    for _ in 0..200 {
        if store
            .events_since(&conversation, 0, 100)
            .await
            .unwrap()
            .iter()
            .any(|e| e.kind == "attempt.gated")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        driver.prompt_texts().is_empty(),
        "no prompt may be sent against an active fence"
    );
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    assert!(
        events.iter().any(|e| e.kind == "attempt.gated"),
        "the gate is recorded: {:?}",
        events.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );
    assert!(
        !events.iter().any(|e| e.kind == "dispatch.canceled"),
        "a gate is not an operator cancellation"
    );

    // The intent was requeued: clearing the fence dispatches the SAME
    // delivery to completion — the gated turn was not lost.
    let fences = store.active_fences(&conversation, "agent").await.unwrap();
    for fence in fences {
        store.clear_fence(&fence.id, "operator").await.unwrap();
    }
    for _ in 0..200 {
        if store
            .events_since(&conversation, 0, 100)
            .await
            .unwrap()
            .iter()
            .any(|e| e.kind == "dispatch.stopped")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        driver.prompt_texts().len(),
        1,
        "the requeued delivery dispatches once the gate clears"
    );

    let _ = shutdown_tx.send(()).await;
}

/// C-4 (cancel half): a cancel that lands after the claim, while the
/// drive task prepares, stops the turn at the dispatch boundary — the
/// attempt ends canceled and the prompt is never sent.
#[tokio::test]
async fn cancel_landing_after_claim_but_before_the_prompt_stops_the_turn() {
    let store = wrkspc_core::Store::open_in_memory("c4-cancel")
        .await
        .unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    let message_id = match store
        .accept_message(
            &conversation,
            "operator",
            "k-1",
            "do the thing",
            false,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agent".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected acceptance, got {other:?}"),
    };

    let driver = FakeDriver::new(Mode::Respond(StopReason::EndTurn));
    let binding = SessionBinding {
        peer_id: "agent".to_string(),
        session_id: "sess-agent".to_string(),
        driver: Arc::new(driver.clone()),
    };
    let (provider, release_lookup, parked) = GateableProvider::new(binding);
    let (dispatcher, handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
            lookup_timeout: std::time::Duration::from_secs(10),
        },
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let mut shutdown_rx = shutdown_rx;
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("dispatcher run");
    });

    // Parked after the claim: request the cancel, then release the
    // second lookup so the drive task reaches the boundary gate.
    parked.await.unwrap();
    let outcome = handle
        .request_cancel(&message_id, "agent", "operator")
        .await
        .expect("cancel request");
    assert!(
        matches!(outcome, CancelOutcome::Attempt { .. }),
        "an open attempt records the cancel on the attempt: {outcome:?}"
    );
    let _ = release_lookup.send(());

    for _ in 0..200 {
        if store
            .events_since(&conversation, 0, 100)
            .await
            .unwrap()
            .iter()
            .any(|e| e.kind == "dispatch.canceled")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        driver.prompt_texts().is_empty(),
        "no prompt may be sent after a recorded cancel"
    );
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    assert!(
        events.iter().any(|e| e.kind == "dispatch.canceled"),
        "the cancel is enforced at the boundary: {:?}",
        events.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );

    let _ = shutdown_tx.send(()).await;
}
