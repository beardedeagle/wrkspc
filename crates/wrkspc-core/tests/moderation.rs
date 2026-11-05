//! Integration tests for the moderation-engine store operations (unit 5):
//! dispatch eligibility, fence gating, the held queue, cancel requests,
//! guidance, delivery accounting, and restart reconciliation.

use wrkspc_core::{AcceptOutcome, CancelOutcome, FenceKind, RecipientRequest, Store, StoreError};

async fn setup() -> (Store, String) {
    let store = Store::open_in_memory("moderation-test").await.unwrap();
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
    (store, conversation_id)
}

async fn post(store: &Store, conversation_id: &str, key: &str, content: &str) -> String {
    // Returns the message id.
    let outcome = store
        .accept_message(
            conversation_id,
            "operator",
            key,
            content,
            false,
            &[RecipientRequest {
                peer_id: "agent".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .expect("accept");
    let AcceptOutcome::Accepted { message_id } = outcome else {
        panic!("expected fresh accept");
    };
    message_id
}

/// A queued intent with a stored delivery and no fence is due.
#[tokio::test]
async fn due_intents_enumerates_claimable_work() {
    let (store, conversation_id) = setup().await;
    post(&store, &conversation_id, "k1", "do the thing").await;

    let due = store.due_intents().await.unwrap();
    assert_eq!(due.len(), 1, "one queued intent is due");
    assert_eq!(due[0].conversation_id, conversation_id);
    assert_eq!(due[0].recipient_id, "agent");
    assert_eq!(due[0].content, "do the thing");
}

/// A pause fence blocks the claim; clearing it lets the intent dispatch.
#[tokio::test]
async fn claim_is_gated_by_fences_and_recovers_on_clear() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    let fence_id = store
        .set_fence(&conversation_id, "agent", FenceKind::Pause, "operator")
        .await
        .unwrap();
    assert!(
        store.due_intents().await.unwrap().is_empty(),
        "paused conversation has no due intents"
    );
    let err = store
        .claim_intent(&message_id, "agent")
        .await
        .expect_err("claim must be fence-gated");
    assert!(
        matches!(err, StoreError::BlockedByFence { .. }),
        "got {err:?}"
    );

    store.clear_fence(&fence_id, "operator").await.unwrap();
    assert_eq!(store.due_intents().await.unwrap().len(), 1);
    let _attempt = store.claim_intent(&message_id, "agent").await.unwrap();
}

/// Holding a delivery parks it in the held queue (not due) until released.
#[tokio::test]
async fn held_deliveries_form_the_held_queue() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    store
        .hold_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();
    assert!(
        store.due_intents().await.unwrap().is_empty(),
        "held delivery is not due"
    );
    // Holding twice is idempotent.
    store
        .hold_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();

    store
        .release_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();
    assert_eq!(store.due_intents().await.unwrap().len(), 1);
}

/// mark_dispatched consumes the delivery (delivered) and the intent leaves the due set.
#[tokio::test]
async fn dispatch_marks_delivery_delivered() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    store.mark_dispatched(&attempt_id).await.unwrap();

    assert!(
        store.due_intents().await.unwrap().is_empty(),
        "dispatched intent leaves the due set (delivery delivered)"
    );
}

/// Canceling a queued intent cancels it directly with an IntentCanceled outcome.
#[tokio::test]
async fn cancel_queued_intent_directly() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    let outcome = store
        .request_cancel(&message_id, "agent", "operator")
        .await
        .unwrap();
    assert_eq!(outcome, CancelOutcome::IntentCanceled);
    assert!(store.due_intents().await.unwrap().is_empty());

    // Terminal intents refuse further cancel requests.
    let err = store
        .request_cancel(&message_id, "agent", "operator")
        .await
        .expect_err("terminal intent refuses cancel");
    assert!(
        matches!(err, StoreError::IntentTerminal { .. }),
        "got {err:?}"
    );
}

/// Canceling with an open attempt records the cancel request on the attempt for the dispatcher to interrupt.
#[tokio::test]
async fn cancel_open_attempt_records_request() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    let outcome = store
        .request_cancel(&message_id, "agent", "operator")
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CancelOutcome::Attempt {
            attempt_id: attempt_id.clone()
        }
    );
    assert!(
        store.attempt_cancel_requested(&attempt_id).await.unwrap(),
        "durable cancel flag set"
    );
    // The open attempt blocks new work and stays out of the due set.
    assert!(store.due_intents().await.unwrap().is_empty());
}

/// Guidance is undelivered until it rides a prompt, then marked delivered exactly once.
#[tokio::test]
async fn guidance_flows_undelivered_then_delivered() {
    let (store, conversation_id) = setup().await;
    post(&store, &conversation_id, "k1", "do the thing").await;

    let g1 = store
        .add_guidance(&conversation_id, "operator", "prefer short answers")
        .await
        .unwrap();
    let g2 = store
        .add_guidance(&conversation_id, "operator", "no file edits yet")
        .await
        .unwrap();

    let undelivered = store.undelivered_guidance(&conversation_id).await.unwrap();
    assert_eq!(undelivered.len(), 2);
    assert_eq!(undelivered[0].id, g1);
    assert_eq!(undelivered[1].id, g2);

    store.mark_guidance_delivered(&[&g1]).await.unwrap();
    let remaining = store.undelivered_guidance(&conversation_id).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, g2);
}

/// Open attempts are listed for restart reconciliation (claimed/dispatched only).
#[tokio::test]
async fn open_attempts_list_reconciles_restarts() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    let open = store.open_attempts().await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].attempt_id, attempt_id);
    assert_eq!(open[0].state, "claimed");

    store.mark_unknown(&attempt_id, "crash").await.unwrap();
    assert!(
        store.open_attempts().await.unwrap().is_empty(),
        "unknown attempts are not 'open' for re-dispatch"
    );
    assert!(
        store.due_intents().await.unwrap().is_empty(),
        "unknown blocks the intent"
    );
}

/// A retried intent whose recipient delivery is already delivered is due again (regression: unit-9 retry wedge).
#[tokio::test]
async fn retried_intent_after_dispatched_attempt_is_due_again() {
    // The operator-Retry path after a dispatched-but-lost attempt: the
    // recipient is already `delivered` (the mailbox got the message),
    // but the execution intent is `queued` again and must be due.
    // Regression: the live unit-9 run wedged a retried intent because
    // the due filter excluded `delivered` recipients.
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    store.mark_dispatched(&attempt_id).await.unwrap();
    store
        .mark_unknown(&attempt_id, "dispatcher restart during attempt")
        .await
        .unwrap();
    assert!(
        store.due_intents().await.unwrap().is_empty(),
        "unknown attempt blocks"
    );

    store
        .reconcile_unknown(&attempt_id, wrkspc_core::Reconciliation::Retry)
        .await
        .unwrap();

    let due = store.due_intents().await.unwrap();
    assert_eq!(due.len(), 1, "the retried intent must be due: {due:?}");
    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    store.mark_dispatched(&attempt_id).await.unwrap();
}
