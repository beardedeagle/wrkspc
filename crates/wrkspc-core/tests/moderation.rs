//! Integration tests for the moderation-engine store operations (unit 5):
//! dispatch eligibility, fence gating, the held queue, cancel requests,
//! guidance, delivery accounting, and restart reconciliation.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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

/// Guidance claims are atomic: two CONCURRENT claims for one
/// conversation partition the pending rows with no overlap, and
/// releasing a claim returns the rows to the pending queue (C3).
#[tokio::test]
async fn guidance_claims_are_atomic_and_releasable() {
    // A file store with a multi-connection pool: the two claims run on
    // separate connections and the write lock is the only arbiter.
    let dir = std::env::temp_dir().join(format!(
        "wrkspc-guidance-claim-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("claim.db");
    let store = wrkspc_core::Store::open(&db, "claim-test").await.unwrap();
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
    let g1 = store
        .add_guidance(&conversation_id, "operator", "first")
        .await
        .unwrap();
    let g2 = store
        .add_guidance(&conversation_id, "operator", "second")
        .await
        .unwrap();

    // Shared openers take no OS lock (WAL lets a second reader in
    // while the store holds the dispatcher lock) — two real pools, two
    // real connections, one database.
    let s1 = wrkspc_core::Store::open_shared(&db, "claim-a")
        .await
        .unwrap();
    let s2 = wrkspc_core::Store::open_shared(&db, "claim-b")
        .await
        .unwrap();
    let c1 = conversation_id.clone();
    let c2 = conversation_id.clone();
    let (a, b) = tokio::join!(
        tokio::spawn(async move { s1.claim_guidance(&c1, "attempt-a").await.unwrap() }),
        tokio::spawn(async move { s2.claim_guidance(&c2, "attempt-b").await.unwrap() }),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    // Exactly the pending set, partitioned without overlap: the write
    // lock made each claim see only its own rows.
    let mut claimed: Vec<String> = a.iter().chain(b.iter()).map(|g| g.id.clone()).collect();
    claimed.sort();
    let mut expect = vec![g1.clone(), g2.clone()];
    expect.sort();
    assert_eq!(
        claimed, expect,
        "the union of concurrent claims is the pending set"
    );
    assert!(
        a.is_empty() || b.is_empty() || a.iter().all(|g| !b.iter().any(|h| h.id == g.id)),
        "no row may be claimed twice"
    );

    // Releasing a claim returns the rows; a fresh claim sees them again.
    store.release_guidance_claim("attempt-a").await.unwrap();
    let reread = store.undelivered_guidance(&conversation_id).await.unwrap();
    assert_eq!(reread.len(), 2, "released rows return to pending");
    let _ = (g1, g2);
    store.release_lock().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Hub-originated guidance ingests idempotently (envelope re-delivery
/// duplicates nothing) and the delivered-ids query drives the
/// consumption ack (H-guidance propagation).
#[tokio::test]
async fn guidance_ingest_is_idempotent_and_ackable() {
    let (store, conversation_id) = setup().await;
    let entries: Vec<(String, String, String)> = vec![
        (
            "hub-g-1".to_string(),
            "operator".to_string(),
            "prefer short answers".to_string(),
        ),
        (
            "hub-g-2".to_string(),
            "operator".to_string(),
            "no file edits yet".to_string(),
        ),
    ];
    store
        .ingest_guidance(&conversation_id, &entries)
        .await
        .unwrap();
    // Re-delivery: the same envelope again duplicates nothing.
    store
        .ingest_guidance(&conversation_id, &entries)
        .await
        .unwrap();
    let pending = store.undelivered_guidance(&conversation_id).await.unwrap();
    assert_eq!(pending.len(), 2, "ingest is idempotent: {pending:?}");

    store.mark_guidance_delivered(&["hub-g-1"]).await.unwrap();
    let ack = store
        .delivered_guidance_ids(&conversation_id)
        .await
        .unwrap();
    assert_eq!(ack, vec!["hub-g-1".to_string()]);
}

/// The delivery envelope carries the conversation's pending guidance
/// (H-guidance propagation): a hub-UI guidance instruction is part of
/// the payload the node ingests before dispatching.
#[tokio::test]
async fn delivery_envelope_carries_pending_guidance() {
    let store = Store::open_in_memory("envelope-guidance").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let conversation_id = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation_id,
            "operator",
            "k1",
            "do the thing",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    else {
        panic!("fresh accept");
    };
    store
        .add_guidance(&conversation_id, "operator", "only once")
        .await
        .unwrap();
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-work-mac",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &message_id,
            origin_message_id: None,
            conversation_id: &conversation_id,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: "hub",
            content: "do the thing",
            execution_requested: true,
        })
        .await
        .unwrap();
    let row = store.task_get(&task_id).await.unwrap().unwrap();
    let payload: serde_json::Value = serde_json::from_str(&row.task_json).unwrap();
    let guidance = payload["metadata"]["delivery"]["guidance"]
        .as_array()
        .unwrap();
    assert_eq!(
        guidance.len(),
        1,
        "the envelope carries the pending guidance"
    );
    assert_eq!(guidance[0]["text"], "only once");
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

/// A moderation gate landing after the claim closes the attempt as
/// gated and returns the intent to the queue: the delivery stays
/// deliverable (the gate is not a cancellation), the event feed
/// records the gate, and a second claim succeeds once the gate clears.
#[tokio::test]
async fn gated_attempt_returns_to_the_queue_and_is_reclaimable() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;
    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    // The dispatch write may already have run when the gate lands.
    store.mark_dispatched(&attempt_id).await.unwrap();

    store
        .gate_attempt(&attempt_id, "moderation gate after claim: fence:pause")
        .await
        .unwrap();

    // The attempt is closed (not open-unknown) and the intent is due
    // again: the delivery was requeued, not canceled.
    assert!(
        store.open_attempts().await.unwrap().is_empty(),
        "a gated attempt is terminal, not open"
    );
    let due = store.due_intents().await.unwrap();
    assert_eq!(due.len(), 1, "the gated intent must be due again: {due:?}");

    // The gate is recorded in the feed.
    let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
    assert!(
        events.iter().any(|event| event.kind == "attempt.gated"),
        "attempt.gated must be committed: {:?}",
        events.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );

    // The delivery is genuinely reclaimable — a new attempt forms.
    let second_attempt = store.claim_intent(&message_id, "agent").await.unwrap();
    assert_ne!(
        second_attempt, attempt_id,
        "a fresh attempt, not the gated one"
    );
    store
        .mark_terminal(
            &second_attempt,
            wrkspc_core::AttemptState::Completed,
            Some("end_turn"),
            None,
        )
        .await
        .unwrap();
}

/// A gated attempt is not an operator cancellation: once dispatched
/// past the boundary, `gate_attempt` refuses to rewind it.
#[tokio::test]
async fn gate_attempt_refuses_an_attempt_that_left_the_claim_window() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;
    let attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();
    store
        .mark_terminal(
            &attempt_id,
            wrkspc_core::AttemptState::Completed,
            Some("end_turn"),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.gate_attempt(&attempt_id, "late gate").await,
        Err(StoreError::TerminalState { .. })
    ));
}

/// H-17: a hold committed AFTER enumeration still stops the claim —
/// the claim is the moderation linearization point, not the
/// enumeration.
#[tokio::test]
async fn held_delivery_blocks_the_claim_after_enumeration() {
    let (store, conversation_id) = setup().await;
    let message_id = post(&store, &conversation_id, "k1", "do the thing").await;

    // Enumeration ran (the intent is due), THEN the operator holds the
    // delivery. The claim must refuse.
    assert_eq!(store.due_intents().await.unwrap().len(), 1);
    store
        .hold_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();
    match store.claim_intent(&message_id, "agent").await {
        Err(StoreError::BlockedByFence { kinds, .. }) => {
            assert!(kinds.contains("held"), "the gate names the hold: {kinds}");
        }
        other => panic!("a held delivery must not be claimable: {other:?}"),
    }
    assert!(
        store.open_attempts().await.unwrap().is_empty(),
        "no attempt may form while held"
    );

    // The release admits the same delivery.
    store
        .release_delivery(&message_id, "agent", "operator")
        .await
        .unwrap();
    assert!(
        !store
            .claim_intent(&message_id, "agent")
            .await
            .unwrap()
            .is_empty()
    );
}

/// UI-04: an exact peer id wins over a colliding alias — once a client
/// resolves a reference to a canonical id, re-resolving must never
/// route it to a DIFFERENT peer that carries that string as its alias.
#[tokio::test]
async fn resolve_peer_ref_prefers_the_canonical_id_over_a_colliding_alias() {
    let store = Store::open_in_memory("resolve-canonical").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    store
        .ensure_peer("claude", "claude", "harness")
        .await
        .unwrap();
    // The collision: claude's alias is the string "codex".
    store
        .set_peer_alias("claude", Some("codex"), "operator")
        .await
        .unwrap();

    // The reference "codex" is the peer codex — the displayed target
    // and the committed recipient stay identical (UI-04).
    assert_eq!(
        store.resolve_peer_ref("codex").await.unwrap().as_deref(),
        Some("codex")
    );
    // A non-id alias still resolves to the alias owner.
    assert_eq!(
        store.resolve_peer_ref("reviewer").await.unwrap(),
        None,
        "no such alias exists"
    );
    store
        .set_peer_alias("claude", Some("reviewer"), "operator")
        .await
        .unwrap();
    assert_eq!(
        store.resolve_peer_ref("reviewer").await.unwrap().as_deref(),
        Some("claude")
    );
}

///  (store side): the delivery-report guidance ack is scoped
/// to the validated conversation — a client-supplied guidance id from
/// another conversation is ignored without being modified, and the
/// conversation's own rows clear their claimed attempt.
#[tokio::test]
async fn guidance_ack_is_scoped_to_the_validated_conversation() {
    let (store, conversation_id) = setup().await;
    let other = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    let g1 = store
        .add_guidance(&conversation_id, "operator", "only for this room")
        .await
        .unwrap();
    let g_other = store
        .add_guidance(&other, "operator", "for the other room")
        .await
        .unwrap();

    // The dispatcher claimed the conversation's guidance for an attempt.
    let claimed = store
        .claim_guidance(&conversation_id, "attempt-1")
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);

    // A report for THIS conversation that also names a foreign id:
    // only the scoped row is marked; the foreign row survives pending.
    let marked = store
        .mark_guidance_delivered_scoped(&conversation_id, &[&g1, &g_other])
        .await
        .unwrap();
    assert_eq!(marked, 1, "only the conversation's own row is acknowledged");
    assert!(
        store
            .undelivered_guidance(&conversation_id)
            .await
            .unwrap()
            .is_empty(),
        "the scoped row is delivered"
    );
    let other_pending = store.undelivered_guidance(&other).await.unwrap();
    assert_eq!(other_pending.len(), 1, "the foreign row is untouched");
    assert_eq!(other_pending[0].id, g_other);

    // The delivered row's claim was cleared: a fresh claim sees nothing.
    assert!(
        store
            .claim_guidance(&conversation_id, "attempt-2")
            .await
            .unwrap()
            .is_empty(),
        "a delivered row is no longer claimable"
    );
}

/// Guidance added AFTER a delivery task was queued still
/// rides the envelope — the queue read (the node's poll) refreshes the
/// guidance snapshot of unclaimed, non-terminal deliveries, and the
/// claim freezes it.
#[tokio::test]
async fn queued_deliveries_refresh_pending_guidance_at_the_poll_boundary() {
    let store = Store::open_in_memory("queued-guidance-refresh")
        .await
        .unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-work-mac")
        .await
        .unwrap();
    let conversation_id = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation_id,
            "operator",
            "queue-guidance-1",
            "do the thing",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    else {
        panic!("fresh accept");
    };

    // The task is queued BEFORE any guidance exists: the snapshot is
    // empty.
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-work-mac",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &message_id,
            origin_message_id: None,
            conversation_id: &conversation_id,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: "hub",
            content: "do the thing",
            execution_requested: true,
        })
        .await
        .unwrap();
    let envelope_guidance = |payload: &serde_json::Value| -> Vec<String> {
        payload["metadata"]["delivery"]["guidance"]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|entry| {
                        entry
                            .get("text")
                            .and_then(|text| text.as_str())
                            .map(str::to_string)
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let row = store.task_get(&task_id).await.unwrap().unwrap();
    let payload: serde_json::Value = serde_json::from_str(&row.task_json).unwrap();
    assert!(
        envelope_guidance(&payload).is_empty(),
        "the envelope snapshots empty guidance at creation"
    );

    // Guidance lands while the delivery waits unclaimed (node offline /
    // fence active).
    store
        .add_guidance(&conversation_id, "operator", "only once")
        .await
        .unwrap();

    // The node's poll read refreshes the envelope — the guidance rides.
    let (rows, _) = store
        .task_list(Some("node-work-mac"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(&rows[0].task_json).unwrap();
    assert_eq!(
        envelope_guidance(&payload),
        vec!["only once".to_string()],
        "the poll boundary attaches guidance added after queueing"
    );
    // The refresh is a metadata update, not a state transition.
    assert_eq!(rows[0].version, 1, "the version is untouched");

    // The claim freezes the envelope: guidance added afterwards does
    // NOT mutate the handed-off blob.
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::Claimed
    );
    store
        .add_guidance(&conversation_id, "operator", "too late")
        .await
        .unwrap();
    let (rows, _) = store
        .task_list(Some("node-work-mac"), None, None, None, 10)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&rows[0].task_json).unwrap();
    assert_eq!(
        envelope_guidance(&payload),
        vec!["only once".to_string()],
        "a claimed envelope is frozen at the handoff"
    );
}
