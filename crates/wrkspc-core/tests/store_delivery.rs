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
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
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
            "claims-key-1",
            "hello",
            false,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: false,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected acceptance, got {other:?}"),
    };
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-test-1",
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

/// The H-10 outcome outbox joins the DURABLE
/// local-execution-to-hub-task mapping the mirror wrote (the processed
/// marker row, keyed by the hub task id with the hub envelope in its
/// payload). The hub task id is never recomputed from the local
/// message id — the mirror allocates a fresh local `messages.id`, so a
/// recomputed id differs from the hub's and every outcome would be
/// silently dropped.
#[tokio::test]
async fn hub_pending_outcomes_joins_the_mirrored_hub_task_mapping() {
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

    // A hub-originated delivery mirrored on the node: the mirror
    // accepts under the HUB message id (no origin), so the local row's
    // idempotency key is the hub id and its id is a fresh UUID.
    let mirrored_id = store
        .accept_message(
            &conversation,
            "operator",
            "hub-msg-1",
            "from the hub",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
            other => panic!("expected fresh acceptance, got {other:?}"),
        })
        .unwrap();
    // The hub's delivery-task id derives from the HUB message id — it
    // must differ from anything recomputed from the local row.
    let hub_task_id = wrkspc_core::tasks::delivery_task_id("hub-msg-1", "codex");
    assert_ne!(
        hub_task_id,
        wrkspc_core::tasks::delivery_task_id(&mirrored_id, "codex"),
        "the hub and local identities must be deliberately distinct"
    );
    // The processed marker: the node's local a2a_tasks row under the
    // hub task id, still carrying the hub delivery envelope.
    store
        .task_create(
            &hub_task_id,
            "node-work-mac",
            "completed",
            true,
            &serde_json::json!({
                "id": hub_task_id,
                "contextId": "node-work-mac",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "metadata": {"delivery": {
                    "kind": "delivery",
                    "message_id": "hub-msg-1",
                    "origin_message_id": null,
                    "conversation_id": conversation,
                    "recipient_peer": "codex",
                }},
            })
            .to_string(),
        )
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

    // The co-hosted echo: a delivery of a message THIS node propagated
    // upward carries the original local message id as its origin. The
    // hub task id still derives from the HUB message id.
    let echoed_id = store
        .accept_message(
            &conversation,
            "codex",
            "key-echo-1",
            "echoed post",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
            other => panic!("expected fresh acceptance, got {other:?}"),
        })
        .unwrap();
    let echo_hub_task = wrkspc_core::tasks::delivery_task_id("hub-msg-2", "agy");
    store
        .task_create(
            &echo_hub_task,
            "node-work-mac",
            "completed",
            true,
            &serde_json::json!({
                "id": echo_hub_task,
                "contextId": "node-work-mac",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "metadata": {"delivery": {
                    "kind": "delivery",
                    "message_id": "hub-msg-2",
                    "origin_message_id": echoed_id,
                    "conversation_id": conversation,
                    "recipient_peer": "agy",
                }},
            })
            .to_string(),
        )
        .await
        .unwrap();

    // A node-local message: same store, no mirrored task row. It
    // targets a harness peer, so its attempt exists — but with no
    // mirrored task it must never be reported. It claims BEFORE the
    // echo goes unknown: an unknown attempt quarantines its recipient.
    let local_id = store
        .accept_message(
            &conversation,
            "operator",
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
            wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
            other => panic!("expected fresh acceptance, got {other:?}"),
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

    // The echo mirror replays the ORIGINAL local row (accept key = the
    // origin = the row's own id), never minting a second row.
    let replayed = store
        .accept_message(
            &conversation,
            "codex",
            &echoed_id,
            "echoed post",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
            other => panic!("expected replay, got {other:?}"),
        })
        .unwrap();
    assert_eq!(replayed, echoed_id, "the echo replays the original row");
    let echo_attempt = store.claim_intent(&echoed_id, "agy").await.unwrap();
    store.mark_unknown(&echo_attempt, "crash").await.unwrap();

    // Exactly the two mirrored deliveries report — with the ORIGINAL
    // hub task ids — and the unknown outcome rides the outbox too.
    let pending = store.hub_pending_outcomes().await.unwrap();
    assert_eq!(
        pending.len(),
        2,
        "only mirrored deliveries report: {pending:?}"
    );
    let codex_report = pending
        .iter()
        .find(|report| report.recipient_peer == "codex")
        .expect("the hub-originated delivery reports");
    assert_eq!(
        codex_report.task_id, hub_task_id,
        "the hub task id is reported, never a recomputed local one"
    );
    assert_eq!(codex_report.attempt_id, attempt_id);
    assert_eq!(codex_report.state, "completed");
    let agy_report = pending
        .iter()
        .find(|report| report.recipient_peer == "agy")
        .expect("the echoed delivery reports");
    assert_eq!(agy_report.task_id, echo_hub_task);
    assert_eq!(agy_report.state, "unknown", "unknown outcomes report too");

    // Ledgering removes them; re-running lists nothing.
    store
        .mark_outcome_reported(&attempt_id, &hub_task_id)
        .await
        .unwrap();
    store
        .mark_outcome_reported(&echo_attempt, &echo_hub_task)
        .await
        .unwrap();
    assert!(store.hub_pending_outcomes().await.unwrap().is_empty());

    // Non-terminal attempts are never reported.
    let second = store
        .accept_message(
            &conversation,
            "operator",
            "hub-msg-3",
            "still running",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .map(|outcome| match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
            other => panic!("expected fresh acceptance, got {other:?}"),
        })
        .unwrap();
    let second_task = wrkspc_core::tasks::delivery_task_id("hub-msg-3", "codex");
    store
        .task_create(
            &second_task,
            "node-work-mac",
            "completed",
            true,
            &serde_json::json!({
                "id": second_task,
                "contextId": "node-work-mac",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "metadata": {"delivery": {
                    "kind": "delivery",
                    "message_id": "hub-msg-3",
                    "origin_message_id": null,
                    "conversation_id": conversation,
                    "recipient_peer": "codex",
                }},
            })
            .to_string(),
        )
        .await
        .unwrap();
    store.claim_intent(&second, "codex").await.unwrap();
    assert!(
        store.hub_pending_outcomes().await.unwrap().is_empty(),
        "a claimed, non-terminal attempt is not reported"
    );
}

/// A same-instance re-claim (restart mid-delivery) must
/// re-evaluate the CURRENT moderation gate — ownership idempotency
/// must never admit a delivery the operator held or fenced after the
/// first claim.
#[tokio::test]
async fn same_instance_reclaim_re_evaluates_the_current_gate() {
    let (store, _, message_id, task_id) = seeded_hub_delivery("reclaim-gate").await;
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::Claimed
    );

    // A hold lands AFTER the claim; the same instance retries.
    store
        .hold_delivery(&message_id, "codex", "operator")
        .await
        .unwrap();
    match store
        .claim_delivery(&task_id, "node-work-mac", "inst-1")
        .await
    {
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => {
            assert!(kinds.contains("held"), "the gate names the hold: {kinds}");
        }
        other => panic!("a held same-instance re-claim must be gated, got {other:?}"),
    }
    // Ownership idempotency survives the refusal: the original claim
    // row is untouched and still held by inst-1.
    let (host, instance, _) = store.delivery_claim(&task_id).await.unwrap().unwrap();
    assert_eq!(
        (host.as_str(), instance.as_str()),
        ("node-work-mac", "inst-1")
    );

    // A fence set after the claim blocks the re-claim too.
    let fence = store
        .set_fence(
            &seeded_hub_delivery_conversation(&store, &task_id).await,
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
            assert!(kinds.contains("pause"), "the gate names the fence: {kinds}");
        }
        other => panic!("a fenced same-instance re-claim must be gated, got {other:?}"),
    }

    // Clearing both gates admits the idempotent re-claim.
    store.clear_fence(&fence, "operator").await.unwrap();
    store
        .release_delivery(&message_id, "codex", "operator")
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await
            .unwrap(),
        wrkspc_core::DeliveryClaimOutcome::AlreadyClaimed
    );
}

/// Reads the conversation id out of a delivery task's envelope.
async fn seeded_hub_delivery_conversation(store: &Store, task_id: &str) -> String {
    let row = store.task_get(task_id).await.unwrap().expect("task row");
    let payload: serde_json::Value = serde_json::from_str(&row.task_json).unwrap();
    payload["metadata"]["delivery"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_string()
}

///  (claim side): a delivery task whose committed message or
/// recipient no longer exists — the room was deleted and a racing
/// fan-out committed its task afterwards — must never be claimable.
#[tokio::test]
async fn orphan_delivery_tasks_are_not_claimable() {
    let store = Store::open_in_memory("orphan-claim-test").await.unwrap();
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
            "orphan-key-1",
            "deleted work",
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

    // The room is deleted BEFORE the racing fan-out commits its task.
    assert!(
        store.delete_conversation(&conversation).await.unwrap(),
        "the room existed and is deleted"
    );

    // The racing fan-out: the delivery-task row has no existence fence
    // back to the message, so it commits after the deletion.
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
            content: "deleted work",
            execution_requested: true,
        })
        .await
        .unwrap();
    assert!(
        store.task_get(&task_id).await.unwrap().is_some(),
        "the racing fan-out committed the orphan task"
    );

    // The defensive claim boundary refuses the orphan: the deleted
    // message and recipient row are gone, and nothing may mirror.
    match store
        .claim_delivery(&task_id, "node-work-mac", "inst-1")
        .await
    {
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => {
            assert!(
                kinds.contains("orphan"),
                "the refusal names the orphan: {kinds}"
            );
        }
        other => panic!("an orphan task must never claim, got {other:?}"),
    }
    assert!(
        store.delivery_claim(&task_id).await.unwrap().is_none(),
        "no claim row may form for an orphan"
    );
}

/// Cancellation racing the fan-out cannot produce
/// executable work — the task insertion carries the canceled-intent
/// fence in the same statement, and the claim gate rejects deliveries
/// whose intent the operator canceled after the task already existed.
#[tokio::test]
async fn cancellation_racing_fanout_and_claim_stays_unexecutable() {
    let store = Store::open_in_memory("cancel-race-test").await.unwrap();
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

    // (a) The interleaving: the fan-out's preflight read the intent as
    // uncanceled; the cancel commits; the fan-out's insert must still
    // be suppressed by the statement-level fence.
    let message_id = match store
        .accept_message(
            &conversation,
            "operator",
            "cancel-race-1",
            "cancel me",
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
    assert!(matches!(
        store.request_cancel(&message_id, "codex", "operator").await,
        Ok(wrkspc_core::CancelOutcome::IntentCanceled)
    ));
    // The fan-out (post-preflight, post-cancel): no task row forms.
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
            content: "cancel me",
            execution_requested: true,
        })
        .await
        .unwrap();
    assert!(
        store.task_get(&task_id).await.unwrap().is_none(),
        "a canceled intent must not fan out"
    );
    assert_eq!(
        store
            .repair_remote_fanout(wrkspc_core::PILOT_HOST_ID)
            .await
            .unwrap(),
        0,
        "the repair pass must not resurrect a canceled delivery"
    );
    assert!(matches!(
        store
            .claim_delivery(&task_id, "node-work-mac", "inst-1")
            .await,
        Err(wrkspc_core::StoreError::NotFound { .. })
    ));

    // (b) The task existed BEFORE the cancel committed: the claim gate
    // must refuse the canceled intent.
    let second = match store
        .accept_message(
            &conversation,
            "operator",
            "cancel-race-2",
            "late cancel",
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
    let second_task = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-work-mac",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &second,
            origin_message_id: None,
            conversation_id: &conversation,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "late cancel",
            execution_requested: true,
        })
        .await
        .unwrap();
    assert!(matches!(
        store.request_cancel(&second, "codex", "operator").await,
        Ok(wrkspc_core::CancelOutcome::IntentCanceled)
    ));
    match store
        .claim_delivery(&second_task, "node-work-mac", "inst-1")
        .await
    {
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => {
            assert!(
                kinds.contains("intent:canceled"),
                "the refusal names the canceled intent: {kinds}"
            );
        }
        other => panic!("a canceled-intent delivery must not claim, got {other:?}"),
    }
    assert!(
        store.delivery_claim(&second_task).await.unwrap().is_none(),
        "no claim row may form for a canceled intent"
    );
}

/// The startup fan-out repair recovers the sender's wire
/// message identity (persisted as the hub acceptance key) and carries
/// it as the envelope origin, so a co-hosted echo maps back onto the
/// original local row — no duplicate message, no duplicate execution.
#[tokio::test]
async fn repair_preserves_the_sender_wire_origin_identity() {
    // The hub: operator on the hub host, codex and agy co-hosted on
    // node-a.
    let hub = Store::open_in_memory("repair-origin-hub").await.unwrap();
    hub.ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    hub.ensure_peer_on_host("codex", "codex", "harness", "node-a")
        .await
        .unwrap();
    hub.ensure_peer_on_host("agy", "agy", "harness", "node-a")
        .await
        .unwrap();
    let conversation = hub
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();

    // The co-hosted node mirrors the hub conversation id.
    let node = Store::open_in_memory("repair-origin-node").await.unwrap();
    node.ensure_peer_on_host("codex", "codex", "harness", "node-a")
        .await
        .unwrap();
    node.ensure_peer_on_host("agy", "agy", "harness", "node-a")
        .await
        .unwrap();
    node.ensure_conversation(&conversation, &["codex", "agy"])
        .await
        .unwrap();
    // The node's original row: its local id IS the wire message id the
    // node propagates upward.
    let local_id = match node
        .accept_message(
            &conversation,
            "codex",
            "local-caller-key",
            "from the node",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected acceptance, got {other:?}"),
    };

    // The hub accepts the node's send under the wire id.
    let hub_message_id = match hub
        .accept_message(
            &conversation,
            "codex",
            &local_id,
            "from the node",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected acceptance, got {other:?}"),
    };
    assert_ne!(hub_message_id, local_id, "the hub allocates its own id");

    // Hub crash before fan-out: the repair rebuilds the envelope WITH
    // the recovered origin.
    assert_eq!(
        hub.repair_remote_fanout(wrkspc_core::PILOT_HOST_ID)
            .await
            .unwrap(),
        1
    );
    let task_id = wrkspc_core::tasks::delivery_task_id(&hub_message_id, "agy");
    let payload: serde_json::Value =
        serde_json::from_str(&hub.task_get(&task_id).await.unwrap().unwrap().task_json).unwrap();
    assert_eq!(
        payload["metadata"]["delivery"]["origin_message_id"],
        local_id.as_str(),
        "the repaired envelope recovers the sender wire identity"
    );
    assert_eq!(
        payload["metadata"]["delivery"]["message_id"],
        hub_message_id.as_str(),
        "the hub message id stays the delivery's idempotency identity"
    );
    // Idempotent: a second repair creates nothing.
    assert_eq!(
        hub.repair_remote_fanout(wrkspc_core::PILOT_HOST_ID)
            .await
            .unwrap(),
        0
    );
    // A later normal replay cannot replace the repaired blob (the
    // deterministic INSERT OR IGNORE) — the origin survives verbatim.
    let _ = hub
        .queue_remote_deliveries(wrkspc_core::RemoteFanoutRequest {
            local_host: wrkspc_core::PILOT_HOST_ID,
            conversation_id: &conversation,
            message_id: &hub_message_id,
            origin_message_id: Some(&local_id),
            sender_peer: "codex",
            content: "from the node",
            execution_requested: true,
            recipients: &["agy".to_string()],
        })
        .await
        .unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&hub.task_get(&task_id).await.unwrap().unwrap().task_json).unwrap();
    assert_eq!(
        payload["metadata"]["delivery"]["origin_message_id"],
        local_id.as_str(),
        "the replay leaves the repaired origin intact"
    );

    // The echo mirror (accept key = origin) replays the ORIGINAL local
    // row — no second message, no second execution intent.
    let replayed = match node
        .accept_message(
            &conversation,
            "codex",
            &local_id,
            "from the node",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
        other => panic!("expected replay, got {other:?}"),
    };
    assert_eq!(replayed, local_id, "the echo reuses the original local row");
    // The negative: mirroring under the hub-derived id (origin lost)
    // mints a SECOND local message — exactly what the repair must not
    // cause.
    let dup = match node
        .accept_message(
            &conversation,
            "codex",
            &hub_message_id,
            "from the node",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected a fresh row under the hub id, got {other:?}"),
    };
    assert_ne!(
        dup, local_id,
        "the hub-derived key cannot replay the original row"
    );

    // Hub-originated posts keep repairing WITHOUT an origin: the
    // operator's acceptance key is a fresh id, not a wire identity.
    let op_message = match hub
        .accept_message(
            &conversation,
            "operator",
            "op-key-1",
            "operator says",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "agy".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        other => panic!("expected acceptance, got {other:?}"),
    };
    assert_eq!(
        hub.repair_remote_fanout(wrkspc_core::PILOT_HOST_ID)
            .await
            .unwrap(),
        1
    );
    let op_task = wrkspc_core::tasks::delivery_task_id(&op_message, "agy");
    let payload: serde_json::Value =
        serde_json::from_str(&hub.task_get(&op_task).await.unwrap().unwrap().task_json).unwrap();
    assert!(
        payload["metadata"]["delivery"]["origin_message_id"].is_null(),
        "a hub-originated post repairs without an origin"
    );
}

/// The unknown-attempt mirror is owned by the (task,
/// host) tuple the reporting node presented — a conflicting re-report
/// is rejected without modifying the row, and only the owning tuple
/// can consume it.
#[tokio::test]
async fn unknown_mirror_rows_are_owned_by_task_and_host() {
    let store = Store::open_in_memory("unknown-ownership").await.unwrap();
    store
        .record_hub_unknown(
            "att-1",
            "task-1",
            "host-a",
            "codex",
            "conv-1",
            "spawn failed",
        )
        .await
        .unwrap();
    // The same owner re-report refreshes the reason.
    store
        .record_hub_unknown(
            "att-1",
            "task-1",
            "host-a",
            "codex",
            "conv-1",
            "spawn failed again",
        )
        .await
        .unwrap();

    // A different TASK with the same attempt id is a conflicting
    // ownership claim: rejected, the stored row untouched.
    let err = store
        .record_hub_unknown("att-1", "task-2", "host-a", "codex", "conv-1", "poison")
        .await
        .unwrap_err();
    assert!(
        matches!(err, wrkspc_core::StoreError::AlreadyExists { .. }),
        "got {err:?}"
    );
    // A different HOST likewise.
    let err = store
        .record_hub_unknown("att-1", "task-1", "host-b", "codex", "conv-1", "poison")
        .await
        .unwrap_err();
    assert!(
        matches!(err, wrkspc_core::StoreError::AlreadyExists { .. }),
        "got {err:?}"
    );
    let listed = store.hub_unknowns().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].stop_reason, "spawn failed again",
        "the original mirror survives"
    );

    // Scoped consumption: a wrong task or host consumes nothing.
    assert!(
        !store
            .consume_hub_unknown("att-1", "task-2", "host-a")
            .await
            .unwrap()
    );
    assert!(
        !store
            .consume_hub_unknown("att-1", "task-1", "host-b")
            .await
            .unwrap()
    );
    assert_eq!(store.hub_unknowns().await.unwrap().len(), 1);

    // The owning tuple consumes the row.
    assert!(
        store
            .consume_hub_unknown("att-1", "task-1", "host-a")
            .await
            .unwrap()
    );
    assert!(store.hub_unknowns().await.unwrap().is_empty());
}
