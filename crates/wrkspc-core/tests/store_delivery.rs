//! The D-6 delivery contract's store half: pollable delivery tasks,
//! append-only host assignments, the pilot cutover, per-instance
//! claims, remote fanout, and the hub outbound ledger.

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
