//! The D-6 delivery contract's store half: pollable delivery tasks,
//! append-only host assignments, the pilot cutover, per-instance
//! claims, remote fanout, and the hub outbound ledger.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use wrkspc_core::Store;

/// Delivery tasks are pollable by host context with a canonical A2A task blob.
#[tokio::test]
async fn delivery_tasks_are_pollable_by_host_context() {
    let store = Store::open_in_memory("delivery-task-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();

    // A hub-side delivery for a remote-host peer creates a task row in
    // that host's queue context.
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-work-mac",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: "msg-1",
            origin_message_id: None,
            conversation_id: &conversation,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: "local",
            content: "hello from the hub",
            execution_requested: true,
        })
        .await
        .unwrap();

    // The node polls its host context and sees the delivery, not the
    // hub's other rows.
    let (rows, total) = store
        .task_list(Some("node-work-mac"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].task_id, task_id);
    assert_eq!(rows[0].state, "submitted");
    let payload: serde_json::Value = serde_json::from_str(&rows[0].task_json).unwrap();
    let delivery = &payload["metadata"]["delivery"];
    assert_eq!(delivery["kind"], "delivery");
    assert_eq!(delivery["recipient_peer"], "codex");
    assert_eq!(delivery["execution_requested"], true);
    // The stored blob is a canonical A2A Task: id + camelCase context
    // + a submitted status, so the surface can serve it as the wire
    // Task.
    assert_eq!(payload["id"], task_id);
    assert_eq!(payload["contextId"], "node-work-mac");
    assert_eq!(payload["status"]["state"], "TASK_STATE_SUBMITTED");

    // Other hosts see nothing.
    let (_, total) = store
        .task_list(Some("node-other"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 0);
}
/// Host assignment is append-only; the projection updates in one transaction and re-assignment is idempotent.
#[tokio::test]
async fn host_assignments_are_append_only_with_transactional_projection() {
    let store = Store::open_in_memory("assignment-test").await.unwrap();

    // First enrolment records the first assignment.
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-a")
        .await
        .unwrap();
    assert_eq!(
        store.peer_host("codex").await.unwrap().as_deref(),
        Some("node-a")
    );

    // ensure_peer_on_host is immutable for existing peers — a silent
    // re-enrolment never rewrites the host.
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-b")
        .await
        .unwrap();
    assert_eq!(
        store.peer_host("codex").await.unwrap().as_deref(),
        Some("node-a")
    );

    // The explicit re-assignment appends history and updates the
    // projection in one transaction.
    assert!(store.assign_peer_host("codex", "node-b").await.unwrap());
    assert_eq!(
        store.peer_host("codex").await.unwrap().as_deref(),
        Some("node-b")
    );

    // Idempotent no-op when the projection already matches.
    assert!(!store.assign_peer_host("codex", "node-b").await.unwrap());

    // History holds both assignments in order.
    let assignments = store.peer_assignments("codex").await.unwrap();
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].0, "node-a");
    assert_eq!(assignments[1].0, "node-b");

    // Unknown peers cannot be assigned.
    assert!(matches!(
        store.assign_peer_host("ghost", "node-b").await,
        Err(wrkspc_core::StoreError::NotFound { .. })
    ));
}
/// The pilot cutover is an append-only reassignment event and a second run is a no-op.
#[tokio::test]
async fn pilot_cutover_is_a_reassignment_event_and_idempotent() {
    let store = Store::open_in_memory("cutover-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();

    // Team-mode start: cut over pilot-marker peers to the real host id
    // (the run_node loop, inlined).
    for peer in ["operator", "codex"] {
        if store.peer_host(peer).await.unwrap().as_deref() == Some(wrkspc_core::PILOT_HOST_ID) {
            assert!(store.assign_peer_host(peer, "node-test-1").await.unwrap());
        }
    }
    assert_eq!(
        store.peer_host("operator").await.unwrap().as_deref(),
        Some("node-test-1")
    );
    assert_eq!(
        store.peer_host("codex").await.unwrap().as_deref(),
        Some("node-test-1")
    );
    // The pilot assignment stays as history — the peer WAS on the pilot
    // host.
    let assignments = store.peer_assignments("codex").await.unwrap();
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].0, wrkspc_core::PILOT_HOST_ID);
    assert_eq!(assignments[1].0, "node-test-1");

    // A second start is a no-op: no new assignments.
    for peer in ["operator", "codex"] {
        if store.peer_host(peer).await.unwrap().as_deref() == Some(wrkspc_core::PILOT_HOST_ID) {
            store.assign_peer_host(peer, "node-test-1").await.unwrap();
        }
    }
    assert_eq!(store.peer_assignments("codex").await.unwrap().len(), 2);
}
/// Delivery claims are first-wins per instance, operator-releasable, and refused for terminal/unknown tasks.
#[tokio::test]
async fn delivery_claims_are_first_wins_per_instance_and_releasable() {
    let store = Store::open_in_memory("claims-test").await.unwrap();
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-test-1",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: "hub-msg-1",
            origin_message_id: None,
            conversation_id: "hub-conv-1",
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "hello",
            execution_requested: false,
        })
        .await
        .unwrap();

    // First claim wins.
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-test-1", "inst-a")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::Claimed
    );
    // Same instance re-claims idempotently (restart mid-delivery).
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-test-1", "inst-a")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::AlreadyClaimed
    );
    // A second instance with the same cert is foreign (double-attach).
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-test-1", "inst-b")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::ForeignClaimed {
            instance_id: "inst-a".into(),
            claimed_at: store.delivery_claim(&task_id).await.unwrap().unwrap().2,
        }
    );
    // The claim is readable with host + instance.
    let (host, instance, _) = store.delivery_claim(&task_id).await.unwrap().unwrap();
    assert_eq!(host, "node-test-1");
    assert_eq!(instance, "inst-a");

    // Operator release clears it and the delivery re-enters the pool.
    assert!(store.release_delivery_claim(&task_id).await.unwrap());
    assert!(store.delivery_claim(&task_id).await.unwrap().is_none());
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-test-1", "inst-b")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::Claimed
    );

    // Terminal tasks cannot be claimed.
    store
        .task_update(&task_id, "completed", true, "{}")
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_delivery(&task_id, "node-test-1", "inst-c")
            .await,
        Err(wrkspc_core::StoreError::TaskTerminal { .. })
    ));
    // Unknown tasks are NotFound.
    assert!(matches!(
        store.claim_delivery("ghost", "node-test-1", "inst-c").await,
        Err(wrkspc_core::StoreError::NotFound { .. })
    ));
}
/// Remote fanout queues exactly one task per remote-host recipient; local recipients produce none.
#[tokio::test]
async fn queue_remote_deliveries_targets_only_remote_hosts() {
    let store = Store::open_in_memory("queue-remote").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("agy", "agy", "harness", "node-other")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();

    // Both harness peers live on remote hosts: one task each, in their
    // own host queue.
    let queued = store
        .queue_remote_deliveries(wrkspc_core::RemoteFanoutRequest {
            local_host: wrkspc_core::PILOT_HOST_ID,
            conversation_id: &conversation,
            message_id: "m-1",
            origin_message_id: None,
            sender_peer: "operator",
            content: "hello team",
            execution_requested: true,
            recipients: &["codex".to_string(), "agy".to_string()],
        })
        .await
        .unwrap();
    assert_eq!(queued, 2);
    let (_, codex_total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    let (_, agy_total) = store
        .task_list(Some("node-other"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!((codex_total, agy_total), (1, 1));

    // A recipient on the hub's own host produces no task (the local
    // dispatcher path owns it).
    let queued = store
        .queue_remote_deliveries(wrkspc_core::RemoteFanoutRequest {
            local_host: wrkspc_core::PILOT_HOST_ID,
            conversation_id: &conversation,
            message_id: "m-2",
            origin_message_id: None,
            sender_peer: "operator",
            content: "hello team",
            execution_requested: true,
            recipients: &["operator".to_string()],
        })
        .await
        .unwrap();
    assert_eq!(queued, 0);
}
/// The hub outbound ledger tracks only harness-originated messages and clears idempotently.
#[tokio::test]
async fn hub_outbound_tracks_only_local_harness_messages() {
    let store = Store::open_in_memory("hub-outbound").await.unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("operator", "operator", "human", "hub-real-host")
        .await
        .unwrap();
    // A second harness peer gives the reply a directed, non-human
    // recipient to carry routing for.
    store
        .ensure_peer_on_host("agy", "agy", "harness", "node-other")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["codex", "operator", "agy"])
        .await
        .unwrap();

    // A harness-originated message: pending for the hub.
    store
        .accept_message(
            &conversation,
            "codex",
            "k-1",
            "harness reply",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();
    // A mirrored message (remote sender host): never re-sent.
    store
        .accept_message(
            &conversation,
            "operator",
            "k-2",
            "operator says",
            false,
            &[],
        )
        .await
        .unwrap();

    let pending = store.hub_pending_outbound("node-test-1").await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sender_peer, "codex");
    assert_eq!(pending[0].content, "harness reply");
    // Routing and execution intent survive to the hub leg: a directed
    // execution request must never degrade into an unaddressed,
    // non-executing broadcast.
    assert!(
        pending[0].execution_requested,
        "execution request must be carried"
    );
    assert_eq!(pending[0].recipients, vec!["agy".to_string()]);

    // The ledger is idempotent and clears the pending set.
    store.mark_hub_sent(&pending[0].message_id).await.unwrap();
    store.mark_hub_sent(&pending[0].message_id).await.unwrap();
    assert!(
        store
            .hub_pending_outbound("node-test-1")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Seeds a hub store: an operator, a remote-host harness recipient,
/// one committed conversation, and one accepted+queued delivery task.
/// Returns `(store, conversation_id, message_id, task_id)`.
async fn seeded_hub_delivery(label: &str) -> (Store, String, String, String) {
    let store = Store::open_in_memory(label).await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-work-mac")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let message_id = match store
        .accept_message(
            &conversation,
            "operator",
            "k-1",
            "hello from the hub",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected acceptance, got {other:?}"),
    };
    let queued = store
        .queue_remote_deliveries(wrkspc_core::RemoteFanoutRequest {
            local_host: wrkspc_core::PILOT_HOST_ID,
            conversation_id: &conversation,
            message_id: &message_id,
            origin_message_id: None,
            sender_peer: "operator",
            content: "hello from the hub",
            execution_requested: true,
            recipients: &["codex".to_string()],
        })
        .await
        .unwrap();
    assert_eq!(queued, 1);
    let (tasks, _) = store
        .task_list(Some("node-work-mac"), None, None, None, 10)
        .await
        .unwrap();
    let task_id = tasks[0].task_id.clone();
    (store, conversation, message_id, task_id)
}

/// The claim is the hub's moderation linearization point: a pause
/// fence, a held delivery, or an ejected recipient each block the
/// delivery claim in its own transaction, and the claim succeeds once
/// the gate clears — nothing can mirror before it.
#[tokio::test]
async fn fence_hold_and_ejection_block_the_delivery_claim() {
    // A pause fence set before the claim blocks it; clearing the fence
    // admits the claim.
    let (store, conversation, _, task_id) = seeded_hub_delivery("claim-gate-fence").await;
    let fence = store
        .set_fence(
            &conversation,
            "codex",
            wrkspc_core::FenceKind::Pause,
            "operator",
        )
        .await
        .unwrap();
    match store
        .claim_delivery(&task_id, "node-work-mac", "inst-1")
        .await
    {
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => {
            assert!(kinds.contains("pause"), "gate names the fence: {kinds}");
        }
        other => panic!("fenced claim must be blocked, got {other:?}"),
    }
    // No claim row was written while gated.
    assert!(store.delivery_claim(&task_id).await.unwrap().is_none());
    store.clear_fence(&fence, "operator").await.unwrap();
    assert!(matches!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await,
        Ok(wrkspc_core::DeliveryClaimOutcome::Claimed)
    ));

    // A held delivery blocks the claim; the release admits it.
    let (store, _, message_id, task_id) = seeded_hub_delivery("claim-gate-hold").await;
    store
        .hold_delivery(&message_id, "codex", "operator")
        .await
        .unwrap();
    match store
        .claim_delivery(&task_id, "node-work-mac", "inst-1")
        .await
    {
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => {
            assert!(kinds.contains("held"), "gate names the hold: {kinds}");
        }
        other => panic!("held claim must be blocked, got {other:?}"),
    }
    store
        .release_delivery(&message_id, "codex", "operator")
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await,
        Ok(wrkspc_core::DeliveryClaimOutcome::Claimed)
    ));

    // An ejected recipient blocks the claim; the restore admits it.
    let (store, _, _, task_id) = seeded_hub_delivery("claim-gate-boot").await;
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Booted, "operator")
        .await
        .unwrap();
    match store
        .claim_delivery(&task_id, "node-work-mac", "inst-1")
        .await
    {
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => {
            assert!(kinds.contains("booted"), "gate names the status: {kinds}");
        }
        other => panic!("ejected recipient claim must be blocked, got {other:?}"),
    }
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Enrolled, "operator")
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await,
        Ok(wrkspc_core::DeliveryClaimOutcome::Claimed)
    ));
}

/// A duplicate task id surfaces as AlreadyExists (the documented
/// contract), not as a generic database error (P2: SQLite raises a
/// constraint violation, which rows_affected() never sees).
#[tokio::test]
async fn duplicate_task_create_is_already_exists() {
    let store = Store::open_in_memory("task-create-duplicate")
        .await
        .unwrap();
    store
        .task_create("task-dup", "ctx", "submitted", false, "{}")
        .await
        .unwrap();
    assert!(matches!(
        store
            .task_create("task-dup", "ctx", "submitted", false, "{}")
            .await,
        Err(wrkspc_core::StoreError::AlreadyExists {
            entity: "a2a task",
            ..
        })
    ));
}

/// A task whose payload is not a well-formed delivery never claims: a
/// malformed envelope cannot describe executable work.
#[tokio::test]
async fn malformed_delivery_payload_is_not_claimable() {
    let store = Store::open_in_memory("claim-gate-malformed").await.unwrap();
    store
        .task_create(
            "task-bad",
            "node-work-mac",
            "submitted",
            false,
            r#"{"id":"task-bad","contextId":"node-work-mac","status":{"state":"TASK_STATE_SUBMITTED"}}"#,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_delivery("task-bad", "node-work-mac", "inst-1")
            .await,
        Err(wrkspc_core::StoreError::BlockedByFence { .. })
    ));
}

/// An operator cancel marks the delivery task terminal-canceled with
/// the envelope preserved (H-10 hub-cancel half): an unclaimed
/// delivery can then never be claimed, so the node never mirrors it.
#[tokio::test]
async fn canceled_delivery_task_is_terminal_and_not_claimable() {
    let store = Store::open_in_memory("hub-cancel-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-work-mac")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let message_id = store
        .accept_message(
            &conversation,
            "operator",
            "key-cancel-1",
            "work to cancel",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id }
            | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
        })
        .unwrap();
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-work-mac",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &message_id,
            origin_message_id: None,
            conversation_id: &conversation,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "work to cancel",
            execution_requested: true,
        })
        .await
        .unwrap();

    // Cancel before any claim: terminal-canceled with the record
    // intact.
    let canceled = store
        .task_mark_canceled(&task_id)
        .await
        .unwrap()
        .expect("a non-terminal task is canceled now");
    assert!(canceled.terminal);
    assert_eq!(canceled.state, "canceled");
    let payload: serde_json::Value = serde_json::from_str(&canceled.task_json).unwrap();
    assert_eq!(payload["status"]["state"], "TASK_STATE_CANCELED");
    assert_eq!(
        payload["metadata"]["delivery"]["message_id"],
        serde_json::Value::String(message_id.clone()),
        "the delivery envelope survives the cancel"
    );

    // The canceled task can never be claimed: the node never mirrors
    // it.
    assert!(matches!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await,
        Err(wrkspc_core::StoreError::TaskTerminal { .. })
    ));

    // Idempotent: a second cancel (and a cancel of an unknown task)
    // is a satisfied no-op.
    assert!(store.task_mark_canceled(&task_id).await.unwrap().is_none());
    assert!(
        store
            .task_mark_canceled("no-such-task")
            .await
            .unwrap()
            .is_none()
    );
}

/// The fan-out and its repair skip operator-canceled intents (H-10):
/// a canceled turn must never create the delivery task the node polls.
#[tokio::test]
async fn fanout_and_repair_skip_canceled_intents() {
    let store = Store::open_in_memory("fanout-cancel-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-work-mac")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let message_id = store
        .accept_message(
            &conversation,
            "operator",
            "key-fanout-1",
            "cancel me",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id }
            | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
        })
        .unwrap();

    // Operator cancels the queued turn BEFORE the fan-out runs.
    assert!(matches!(
        store.request_cancel(&message_id, "codex", "operator").await,
        Ok(wrkspc_core::CancelOutcome::IntentCanceled)
    ));

    // The fan-out must skip the canceled recipient.
    let queued = store
        .queue_remote_deliveries(wrkspc_core::RemoteFanoutRequest {
            local_host: wrkspc_core::PILOT_HOST_ID,
            conversation_id: &conversation,
            message_id: &message_id,
            origin_message_id: None,
            sender_peer: "operator",
            content: "cancel me",
            execution_requested: true,
            recipients: &["codex".to_string()],
        })
        .await
        .unwrap();
    assert_eq!(queued, 0, "a canceled intent must not fan out");

    // The startup repair must not resurrect the canceled delivery
    // either.
    assert_eq!(
        store
            .repair_remote_fanout(wrkspc_core::PILOT_HOST_ID)
            .await
            .unwrap(),
        0,
        "the repair pass must not re-create a canceled delivery"
    );
    let task_id = wrkspc_core::tasks::delivery_task_id(&message_id, "codex");
    assert!(
        store.task_get(&task_id).await.unwrap().is_none(),
        "no delivery task exists for the canceled turn"
    );
}

/// The H-10 outcome outbox: only terminal attempts whose (message,
/// recipient) pair has a mirrored delivery-task row are reported to
/// the hub; node-local messages are never reported.
#[tokio::test]
async fn hub_pending_outcomes_lists_only_mirrored_terminal_attempts() {
    let store = Store::open_in_memory("outcome-outbox-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-work-mac")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("agy", "agy", "harness", "node-work-mac")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();

    // A hub-originated delivery: the node recorded the mirrored task
    // marker (its local a2a_tasks row under the deterministic id).
    let mirrored_id = store
        .accept_message(
            &conversation,
            "operator",
            "key-mirror-1",
            "from the hub",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id }
            | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
        })
        .unwrap();
    let task_id = wrkspc_core::tasks::delivery_task_id(&mirrored_id, "codex");
    store
        .task_create(&task_id, "node-work-mac", "completed", true, "{}")
        .await
        .unwrap();
    let attempt_id = store.claim_intent(&mirrored_id, "codex").await.unwrap();
    store
        .mark_terminal(
            &attempt_id,
            wrkspc_core::AttemptState::Completed,
            None,
            None,
        )
        .await
        .unwrap();

    // A node-local message: same store, no mirrored task row. It
    // targets a harness peer (human peers never take a delivery), so
    // its attempt exists — but with no mirrored task it must never be
    // reported.
    let local_id = store
        .accept_message(
            &conversation,
            "codex",
            "key-local-1",
            "local post",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id }
            | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
        })
        .unwrap();
    let local_attempt = store.claim_intent(&local_id, "agy").await.unwrap();
    store
        .mark_terminal(
            &local_attempt,
            wrkspc_core::AttemptState::Completed,
            None,
            None,
        )
        .await
        .unwrap();

    // Only the hub-originated delivery is reported.
    let pending = store.hub_pending_outcomes().await.unwrap();
    assert_eq!(pending.len(), 1, "only the mirrored delivery reports");
    assert_eq!(pending[0].task_id, task_id);
    assert_eq!(pending[0].attempt_id, attempt_id);
    assert_eq!(pending[0].state, "completed");
    assert_eq!(pending[0].recipient_peer, "codex");

    // Ledgering removes it; re-running lists nothing.
    store
        .mark_outcome_reported(&attempt_id, &task_id)
        .await
        .unwrap();
    assert!(store.hub_pending_outcomes().await.unwrap().is_empty());

    // Non-terminal attempts are never reported.
    let second = store
        .accept_message(
            &conversation,
            "operator",
            "key-mirror-2",
            "still running",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id }
            | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
        })
        .unwrap();
    let second_task = wrkspc_core::tasks::delivery_task_id(&second, "codex");
    store
        .task_create(&second_task, "node-work-mac", "completed", true, "{}")
        .await
        .unwrap();
    store.claim_intent(&second, "codex").await.unwrap();
    assert!(
        store.hub_pending_outcomes().await.unwrap().is_empty(),
        "a claimed, non-terminal attempt is not reported"
    );
}
