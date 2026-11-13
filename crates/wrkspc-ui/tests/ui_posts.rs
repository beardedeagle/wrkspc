//! Operator posts and unknown-attempt reconciliation, including the
//! hub/node delivery-queueing split.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use wrkspc_core::Store;
use wrkspc_ui::{UiConfig, UiServer};

/// The operator post commits with an execution intent for the other member.
#[tokio::test]
async fn operator_post_delivers_to_members() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;

    let res = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation_id}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "content": "please summarize the repo" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // The message lands with an execution intent for the agent.
    assert_eq!(
        ui.store.due_intents().await.unwrap().len(),
        1,
        "operator post requests execution for other members"
    );
    let messages: Vec<serde_json::Value> = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["sender_id"], "operator");
    let _ = ui.shutdown.send(());
}

/// A directed operator post reaches only the named member: the message is
/// in the shared transcript, but only the addressed peer receives a
/// delivery and an execution intent. Unknown or non-member targets are
/// refused rather than silently dropped.
#[tokio::test]
async fn directed_operator_post_targets_one_member() {
    let ui = spawn_ui().await;
    ui.store
        .ensure_peer("agent2", "agent2", "harness")
        .await
        .unwrap();
    ui.store
        .set_peer_alias("agent2", Some("tester"), "operator")
        .await
        .unwrap();
    let conversation = ui
        .store
        .create_conversation(&["operator", "agent", "agent2"])
        .await
        .unwrap();

    // The alias resolves to the peer; only that member is addressed.
    let res = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({
            "content": "@tester take a look",
            "recipients": ["tester"],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    let intents = ui.store.due_intents().await.unwrap();
    assert_eq!(intents.len(), 1, "exactly one recipient takes a turn");
    assert_eq!(intents[0].recipient_id, "agent2");
    assert_eq!(intents[0].conversation_id, conversation);

    // The message itself stays in the shared transcript for every member.
    let messages: Vec<serde_json::Value> = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"], "@tester take a look");

    // An unknown recipient is a 400, not a silent drop.
    let res = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({
            "content": "hello",
            "recipients": ["ghost"],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::BAD_REQUEST);
    let _ = ui.shutdown.send(());
}

/// Kicking a member is conversation-scoped: the peer leaves this room only,
/// stops receiving deliveries here, and the conversation keeps its operator.
/// The operator is never removable and an unknown member is a 404.
#[tokio::test]
async fn kick_removes_member_from_one_conversation_only() {
    let ui = spawn_ui().await;
    ui.store
        .ensure_peer("agent2", "agent2", "harness")
        .await
        .unwrap();
    let room = ui
        .store
        .create_conversation(&["operator", "agent", "agent2"])
        .await
        .unwrap();
    let other = ui
        .store
        .create_conversation(&["operator", "agent2"])
        .await
        .unwrap();

    let res = ui
        .client()
        .delete(format!(
            "{}/api/conversations/{room}/members/agent2",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // Gone from this room, still a member of the other one.
    let mut members = ui.store.conversation_members(&room).await.unwrap();
    members.sort();
    assert_eq!(members, vec!["agent".to_string(), "operator".to_string()]);
    assert_eq!(
        ui.store.conversation_members(&other).await.unwrap().len(),
        2
    );

    // A post to the room no longer reaches the kicked peer.
    let res = ui
        .client()
        .post(format!("{}/api/conversations/{room}/messages", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "content": "after the kick" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let intents = ui.store.due_intents().await.unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].recipient_id, "agent");

    // The removal is recorded on the conversation feed.
    let events: Vec<serde_json::Value> = ui
        .client()
        .get(format!("{}/api/conversations/{room}/events", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["kind"] == "membership.removed"),
        "the kick appears on the feed"
    );

    // The operator cannot be kicked; an unknown member is a 404.
    for target in ["operator", "ghost"] {
        let res = ui
            .client()
            .delete(format!(
                "{}/api/conversations/{room}/members/{target}",
                ui.base
            ))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND, "{target}");
    }
    let _ = ui.shutdown.send(());
}

/// Unknown attempts surface with full provenance; reconcile retry returns the intent to the due set.
#[tokio::test]
async fn unknown_attempts_surface_and_reconcile() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;
    let message_id = ui.post_message("do the thing").await;

    let attempt_id = ui.store.claim_intent(&message_id, "agent").await.unwrap();
    ui.store.mark_unknown(&attempt_id, "crash").await.unwrap();

    let listed = ui
        .client()
        .get(format!("{}/api/attempts/unknown", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let attempts: Vec<serde_json::Value> = listed.json().await.unwrap();
    assert_eq!(attempts.len(), 1, "unknown is never silent");
    assert_eq!(attempts[0]["attempt_id"], attempt_id);
    assert_eq!(attempts[0]["stop_reason"], "crash");
    assert_eq!(attempts[0]["conversation_id"], conversation_id);

    let reconciled = ui
        .client()
        .post(format!("{}/api/attempts/{attempt_id}/reconcile", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "decision": "retry" }))
        .send()
        .await
        .unwrap();
    assert_eq!(reconciled.status(), reqwest::StatusCode::NO_CONTENT);

    let listed = ui
        .client()
        .get(format!("{}/api/attempts/unknown", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let attempts: Vec<serde_json::Value> = listed.json().await.unwrap();
    assert!(
        attempts.is_empty(),
        "reconciled attempt leaves the unknown list"
    );
    assert_eq!(
        ui.store.due_intents().await.unwrap().len(),
        1,
        "retry returns the intent to the due set"
    );
    let _ = ui.shutdown.send(());
}
/// A directed post whose recipient list exceeds the cap is refused
/// outright: no partial resolution, no fan-out, no committed message
/// (CodeQL `rust/uncontrolled-allocation-size`).
#[tokio::test]
async fn directed_post_rejects_recipient_lists_over_the_cap() {
    let ui = spawn_ui().await;
    let conversation = ui.conversation_id().await;

    let recipients: Vec<String> = (0..33).map(|index| format!("peer{index}")).collect();
    let res = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({
            "content": "directed at everyone",
            "recipients": recipients,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("too many recipients") && error.contains("32")),
        "the refusal names the cap: {body}"
    );

    // Nothing was committed and nothing was queued for execution.
    assert_eq!(ui.store.due_intents().await.unwrap().len(), 0);
    let messages: Vec<serde_json::Value> = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(messages.len(), 0, "no message lands on a refused post");
    let _ = ui.shutdown.send(());
}

/// Hub-mode operator posts queue a pollable delivery task for remote-host recipients.
#[tokio::test]
async fn hub_mode_operator_post_queues_remote_deliveries() {
    let store = Store::open_in_memory("ui-hub-post").await.unwrap();
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

    let cancel: Arc<FakeCancel> = Arc::new(FakeCancel {
        calls: Mutex::new(Vec::new()),
    });
    let ui = UiServer::new(
        store.clone(),
        cancel,
        UiConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            cookie_secure: false,
            token: Some(TOKEN.to_string()),
            queue_remote_deliveries: true,
            hub_role: true,
            hub_connected: false,
        },
        None,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        ui.serve_on(listener, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("ui serves");
    });

    let res = reqwest::Client::new()
        .post(format!(
            "http://{addr}/api/conversations/{conversation}/messages"
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "content": "hello team" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // Hub mode: the remote-host recipient got a pollable delivery.
    let (tasks, total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 1);
    let payload: serde_json::Value = serde_json::from_str(&tasks[0].task_json).unwrap();
    assert_eq!(payload["metadata"]["delivery"]["recipient_peer"], "codex");
    assert_eq!(payload["metadata"]["delivery"]["sender_peer"], "operator");

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// Node-mode operator posts queue no delivery tasks (the hub never sees them).
#[tokio::test]
async fn node_mode_operator_post_queues_nothing() {
    let store = Store::open_in_memory("ui-node-post").await.unwrap();
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

    let cancel: Arc<FakeCancel> = Arc::new(FakeCancel {
        calls: Mutex::new(Vec::new()),
    });
    let ui = UiServer::new(
        store.clone(),
        cancel,
        UiConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            cookie_secure: false,
            token: Some(TOKEN.to_string()),
            queue_remote_deliveries: false,
            hub_role: false,
            hub_connected: false,
        },
        None,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        ui.serve_on(listener, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("ui serves");
    });

    let res = reqwest::Client::new()
        .post(format!(
            "http://{addr}/api/conversations/{conversation}/messages"
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "content": "hello team" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // Node mode: no delivery tasks — the node's dispatcher owns its
    // local peers, and the hub never sees this post.
    let (_, total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 0);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// An operator post with no recipient list fans out to every eligible
/// member: in a room with more than the directed-post ceiling of
/// members, the shared store fence refuses the post with a 400 (not a
/// 500) so a big room never amplifies into unbounded per-peer work.
#[tokio::test]
async fn oversized_fanout_room_post_is_refused_with_400() {
    let ui = spawn_ui().await;
    let mut members: Vec<String> = Vec::new();
    for index in 0..33 {
        let peer_id = format!("peer{index}");
        ui.store
            .ensure_peer(&peer_id, &peer_id, "harness")
            .await
            .unwrap();
        members.push(peer_id);
    }
    members.insert(0, "operator".to_string());
    let member_refs: Vec<&str> = members.iter().map(String::as_str).collect();
    let conversation = ui.store.create_conversation(&member_refs).await.unwrap();

    let res = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "content": "everyone listens" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("too many recipients") && error.contains("33")),
        "the refusal names the fan-out size: {body}"
    );
    let _ = ui.shutdown.send(());
}
