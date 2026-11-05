use super::*;
use std::sync::Arc;

/// Builds a hub-side delivery task row payload for tests.
fn hub_delivery_row(task_id: &str) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        context_id: "node-test-1".to_string(),
        state: "submitted".to_string(),
        terminal: false,
        version: 1,
        task_json: serde_json::json!({
            "id": task_id,
            "contextId": "node-test-1",
            "status": {"state": "TASK_STATE_SUBMITTED"},
            "metadata": {
                "delivery": {
                    "kind": "delivery",
                    "message_id": "hub-msg-1",
                    "conversation_id": "hub-conv-1",
                    "sender_peer": "operator",
                    "sender_name": "operator",
                    "sender_class": "human",
                    "sender_host": "hub-real-host",
                    "recipient_peer": "codex",
                    "recipient_name": "codex",
                    "recipient_class": "harness",
                    "content": "hello from the hub",
                    "execution_requested": true,
                },
            },
        })
        .to_string(),
        created_at: String::new(),
        updated_at: String::new(),
    }
}

#[tokio::test]
async fn delivery_is_mirrored_dispatched_and_acked() {
    let store = Store::open_in_memory("poll-test").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    // The sender is pre-enrolled under the pilot marker: the mirror
    // must replay the hub's real host assignment instead of leaving
    // the stale projection.
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();

    let row = hub_delivery_row("task-1");
    let mut acked = Vec::new();
    let outcome = process_deliveries(
        &store,
        "node-test-1",
        std::slice::from_ref(&row),
        |_| std::future::ready(true),
        |task_id| {
            acked.push(task_id.to_string());
            std::future::ready(true)
        },
    )
    .await;
    assert_eq!(
        outcome,
        PollOutcome {
            processed: 1,
            failed: 0
        }
    );
    assert_eq!(acked, vec!["task-1"]);

    // The mirror preserved the hub ids: conversation + sender peer
    // exist locally.
    assert!(store.task_get("task-1").await.unwrap().is_some());
    let members = store.conversation_members("hub-conv-1").await.unwrap();
    assert!(members.contains(&"operator".to_string()));
    assert!(members.contains(&"codex".to_string()));
    let sender = store.peer_details("operator").await.unwrap().unwrap();
    assert_eq!(
        sender,
        ("operator".into(), "human".into(), "hub-real-host".into())
    );

    // Exactly one local record; the execution intent it raised is
    // claimable by the local dispatcher.
    let events = store.events_since("hub-conv-1", 0, 100).await.unwrap();
    let accepted: Vec<_> = events
        .iter()
        .filter(|event| event.kind == "message.accepted")
        .collect();
    assert_eq!(accepted.len(), 1);
    let message_id = accepted[0].payload["message_id"]
        .as_str()
        .unwrap()
        .to_string();
    let attempt_id = store.claim_intent(&message_id, "codex").await.unwrap();
    assert!(!attempt_id.is_empty());
}

#[tokio::test]
async fn failed_ack_leaves_delivery_pollable_and_replayable() {
    let store = Store::open_in_memory("poll-retry-test").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();

    let row = hub_delivery_row("task-2");
    let outcome = process_deliveries(
        &store,
        "node-test-1",
        std::slice::from_ref(&row),
        |_| std::future::ready(true),
        |_| std::future::ready(false),
    )
    .await;
    assert_eq!(
        outcome,
        PollOutcome {
            processed: 0,
            failed: 1
        }
    );
    // No processed marker: the hub task stays pollable.
    assert!(store.task_get("task-2").await.unwrap().is_none());

    // A later poll (ack healthy) converges without duplicating the
    // local record.
    let mut acked = Vec::new();
    let outcome = process_deliveries(
        &store,
        "node-test-1",
        &[row],
        |_| std::future::ready(true),
        |task_id| {
            acked.push(task_id.to_string());
            std::future::ready(true)
        },
    )
    .await;
    assert_eq!(
        outcome,
        PollOutcome {
            processed: 1,
            failed: 0
        }
    );
    assert!(store.task_get("task-2").await.unwrap().is_some());
    // The replay path wrote no duplicate local record or intents.
    let events = store.events_since("hub-conv-1", 0, 100).await.unwrap();
    let accepted = events
        .iter()
        .filter(|event| event.kind == "message.accepted")
        .count();
    assert_eq!(accepted, 1);
}

#[tokio::test]
async fn terminal_and_non_delivery_rows_are_skipped() {
    let store = Store::open_in_memory("poll-skip-test").await.unwrap();
    let mut terminal = hub_delivery_row("task-done");
    terminal.terminal = true;
    let not_delivery = TaskRow {
        task_id: "task-other".to_string(),
        context_id: "node-test-1".to_string(),
        state: "submitted".to_string(),
        terminal: false,
        version: 1,
        task_json: r#"{"id":"task-other","contextId":"node-test-1","status":{"state":"TASK_STATE_SUBMITTED"},"metadata":{"other":{}}}"#.to_string(),
        created_at: String::new(),
        updated_at: String::new(),
    };
    let outcome = process_deliveries(
        &store,
        "node-test-1",
        &[terminal, not_delivery],
        |_| std::future::ready(true),
        |_| std::future::ready(true),
    )
    .await;
    assert_eq!(
        outcome,
        PollOutcome {
            processed: 0,
            failed: 0
        }
    );
}
#[tokio::test]
async fn two_nodes_sharing_one_cert_cannot_double_dispatch() {
    // The hub's claim table, simulated as a shared in-flight set:
    // `insert` returns true only for the FIRST claimant.
    let claims: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
        Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));

    // Node A claims and processes the delivery.
    let store_a = Store::open_in_memory("poll-double-a").await.unwrap();
    store_a
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let outcome_a = process_deliveries(
        &store_a,
        "node-test-1",
        &[hub_delivery_row("task-double")],
        |task_id| {
            let claims = Arc::clone(&claims);
            let task_id = task_id.to_string();
            async move { claims.lock().await.insert(task_id) }
        },
        |_| std::future::ready(true),
    )
    .await;
    assert_eq!(outcome_a.processed, 1);

    // Node B (same host id, same cert — the double-attach shape)
    // loses the claim and mirrors NOTHING.
    let store_b = Store::open_in_memory("poll-double-b").await.unwrap();
    store_b
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let outcome_b = process_deliveries(
        &store_b,
        "node-test-1",
        &[hub_delivery_row("task-double")],
        |task_id| {
            let claims = Arc::clone(&claims);
            let task_id = task_id.to_string();
            async move { claims.lock().await.insert(task_id) }
        },
        |_| std::future::ready(true),
    )
    .await;
    assert_eq!(
        outcome_b,
        PollOutcome {
            processed: 0,
            failed: 0
        }
    );
    assert!(store_b.task_get("task-double").await.unwrap().is_none());
    assert!(
        store_b
            .events_since("hub-conv-1", 0, 100)
            .await
            .unwrap()
            .is_empty()
    );
}
/// Seeds a node store with one harness-originated message and
/// returns its id.
async fn seed_local_message(store: &Store) -> String {
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    let conversation = store.create_conversation(&["codex"]).await.unwrap();
    match store
        .accept_message(
            &conversation,
            "codex",
            "k-1",
            "hello from codex",
            false,
            &[],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id }
        | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
    }
}

#[tokio::test]
async fn outbound_messages_reach_the_hub_and_are_ledgered() {
    let node_store = Store::open_in_memory("sync-node").await.unwrap();
    seed_local_message(&node_store).await;
    assert_eq!(
        node_store
            .hub_pending_outbound("node-test-1")
            .await
            .unwrap()
            .len(),
        1
    );

    // A real hub: the A2A surface over a loopback listener. The
    // request is unstamped, so the acceptance middleware passes it
    // through — this test pins the WIRE contract (the SDK's own
    // parser accepts the send) and the ledger behavior.
    let hub_store = Store::open_in_memory("sync-hub").await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let surface = wrkspc_a2a::A2aSurface::new(hub_store.clone());
    let router = surface.into_router("https://hub.test");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });

    let client = reqwest::Client::new();
    let a2a = a2a_client::A2AClient::new(a2a_client::jsonrpc::JsonRpcTransport::new(
        client,
        format!("http://{addr}/jsonrpc"),
    ));
    let sent = sync_outbound(&a2a, &node_store, "node-test-1").await;
    assert_eq!(sent, 1);
    assert!(
        node_store
            .hub_pending_outbound("node-test-1")
            .await
            .unwrap()
            .is_empty()
    );

    // The hub's SDK accepted the send: one task carrying the content.
    let (tasks, total) = hub_store
        .task_list(None, None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert!(tasks[0].task_json.contains("hello from codex"));

    tx.send(()).ok();
    let _ = server.await;
}

#[tokio::test]
async fn failed_hub_send_leaves_the_message_pending() {
    let store = Store::open_in_memory("sync-fail").await.unwrap();
    seed_local_message(&store).await;

    // Nothing listens on port 1: the send fails and the ledger
    // stays untouched for the next tick's retry.
    let client = reqwest::Client::new();
    let a2a = a2a_client::A2AClient::new(a2a_client::jsonrpc::JsonRpcTransport::new(
        client,
        "http://127.0.0.1:1/jsonrpc".to_string(),
    ));
    let sent = sync_outbound(&a2a, &store, "node-test-1").await;
    assert_eq!(sent, 0);
    assert_eq!(
        store
            .hub_pending_outbound("node-test-1")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn roster_sync_and_presence_upload_ride_the_stamped_routes() {
    // Hub side: the discovery routes behind the accept-loop stamp
    // (the middleware writes the same header the TLS accept loop
    // writes per connection).
    let hub_store = Store::open_in_memory("presence-poll-hub").await.unwrap();
    let presence_state = crate::presence::PresenceState::new(
        hub_store.clone(),
        "https://hub.test".to_string(),
        true,
    );
    let router = axum::Router::new()
        .route(
            "/presence",
            axum::routing::post(crate::presence::presence_upsert)
                .with_state(presence_state.clone()),
        )
        .route(
            "/roster",
            axum::routing::get(crate::presence::roster_get).with_state(presence_state),
        )
        .layer(axum::middleware::from_fn(
            |mut request: axum::extract::Request, next: axum::middleware::Next| async move {
                request.headers_mut().insert(
                    "x-wrkspc-peer",
                    axum::http::HeaderValue::from_static("CN=test,SAN=node-test-1"),
                );
                next.run(request).await
            },
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });

    let client = reqwest::Client::new();
    let hub_url = format!("http://{addr}");
    let node_store = Store::open_in_memory("presence-poll-node").await.unwrap();

    // Upload one peer's declaration; the hub enrols it.
    let accepted = upload_presence(
        &client,
        &hub_url,
        &[PresenceDecl {
            peer_id: "codex".to_string(),
            name: "codex".to_string(),
            class: "harness".to_string(),
            state: "connected".to_string(),
        }],
    )
    .await;
    assert_eq!(accepted, 1);
    assert!(hub_store.roster_entry("codex").await.unwrap().is_some());

    // Mirror the hub roster into the node store.
    assert!(sync_roster(&client, &hub_url, &node_store).await);
    let mirror = node_store.roster().await.unwrap();
    assert_eq!(mirror.len(), 1);
    assert_eq!(mirror[0].peer_id, "codex");
    assert_eq!(mirror[0].state, "connected");

    // A dead hub keeps the last-known mirror (freshness decision:
    // stale-but-visible, never a lie-free wipe).
    assert!(!sync_roster(&client, "http://127.0.0.1:1", &node_store).await);
    assert_eq!(node_store.roster().await.unwrap().len(), 1);

    tx.send(()).ok();
    let _ = server.await;
}
