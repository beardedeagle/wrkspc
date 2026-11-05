//! Dispatch lifecycle and moderation: happy path, refusal, cancels
//! (mid-turn and queued), pause fences, and held deliveries.

mod common;

use agent_client_protocol::schema::v1::StopReason;
use common::{Mode, payload_has, spawn_harness};
use std::time::Duration;
use wrkspc_core::{CancelOutcome, FenceKind};

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
