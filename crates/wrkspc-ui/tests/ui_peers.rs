//! The roster, the moderation surface, and hub/node conversation
//! creation roles.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;
use wrkspc_core::Store;
use wrkspc_ui::UiConfig;

/// The peers route returns the roster with peer id and live state.
#[tokio::test]
async fn peers_route_returns_the_roster() {
    let store = Store::open_in_memory("ui-peers").await.unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let ui = spawn_ui_on(
        store,
        UiConfig {
            token: Some(TOKEN.to_string()),
            ..Default::default()
        },
    )
    .await;

    let res = reqwest::Client::new()
        .get(format!("{}/api/peers", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let peers: Vec<serde_json::Value> = res.json().await.unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0]["peer_id"], "codex");
    assert_eq!(peers[0]["state"], "connected");

    let _ = ui.shutdown.send(());
}

/// The moderation surface renames, boots, forgets, and audits; the operator peer is protected.
#[tokio::test]
async fn moderation_surface_renames_boots_forgets_and_audits() {
    let ui = spawn_ui().await;
    // The roster materializes on first presence.
    ui.store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let client = ui.client();
    let url = |path: &str| format!("{}{}", ui.base, path);

    // Rename.
    let res = client
        .patch(url("/api/peers/codex"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "alias": "Rover" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let entry: serde_json::Value = res.json().await.unwrap();
    assert_eq!(entry["alias"], "Rover");

    // Boot.
    let res = client
        .patch(url("/api/peers/codex"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "status": "booted" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let booted: serde_json::Value = res.json().await.unwrap();
    assert_eq!(booted["status"], "booted");

    // The operator peer is protected.
    let res = client
        .patch(url("/api/peers/operator"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "status": "booted" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::BAD_REQUEST);

    // Forget the booted peer: the cohort entry disappears.
    let res = client
        .delete(url("/api/peers/codex"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);
    let peers: Vec<serde_json::Value> = client
        .get(url("/api/peers"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(peers.iter().all(|p| p["peer_id"] != "codex"));

    // Deleting works on an enrolled peer: the operator's complaint was not
    // being able to remove an entry at all, whatever its status. Unknown
    // peers still 404.
    ui.store
        .upsert_roster_presence("claude", "claude", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let res = client
        .delete(url("/api/peers/claude"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        ui.store.roster_entry("claude").await.unwrap().is_none(),
        "the entry is gone after delete"
    );
    let res = client
        .delete(url("/api/peers/ghost"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);

    // The audit log carries every action, newest first.
    let log: Vec<serde_json::Value> = client
        .get(url("/api/moderation"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent_actions: Vec<&str> = log
        .iter()
        .filter(|e| e["peer_id"] == "codex")
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(agent_actions, ["forget", "boot", "alias", "enroll"]);
    assert_eq!(
        log.iter().find(|e| e["action"] == "forget").unwrap()["actor"],
        "operator"
    );

    // The limit parameter caps the feed.
    let one: Vec<serde_json::Value> = client
        .get(url("/api/moderation?limit=1"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    let _ = ui.shutdown.send(());
}

/// Hub role creates conversations (201) and validates members (422 for unknown, no ghost peer).
#[tokio::test]
async fn hub_role_creates_conversations_and_validates_members() {
    let store = Store::open_in_memory("ui-hub-create").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    let ui = spawn_ui_on(
        store,
        UiConfig {
            queue_remote_deliveries: true,
            hub_role: true,
            token: Some(TOKEN.to_string()),
            ..Default::default()
        },
    )
    .await;
    let client = reqwest::Client::new();

    // Unknown member: 422, named member, no ghost peer created.
    let res = client
        .post(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "members": ["operator", "ghost"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["error"], "member not enrolled");
    assert_eq!(body["member"], "ghost");

    // Enrolled members: 201 + id, conversation visible in the list.
    let res = client
        .post(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "members": ["operator", "codex"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);
    let body: serde_json::Value = res.json().await.unwrap();
    let id = body["id"].as_str().unwrap().to_string();
    let res = client
        .get(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let conversations: Vec<serde_json::Value> = res.json().await.unwrap();
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0]["id"], id);

    let _ = ui.shutdown.send(());
}

/// Node role refuses conversation creation (404: no shared-store operator surface).
#[tokio::test]
async fn node_role_refuses_conversation_creation() {
    let store = Store::open_in_memory("ui-node-create").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    let ui = spawn_ui_on(
        store,
        UiConfig {
            token: Some(TOKEN.to_string()),
            ..Default::default()
        },
    )
    .await;

    let res = reqwest::Client::new()
        .post(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "members": ["operator"] }))
        .send()
        .await
        .unwrap();
    // Node role: no shared-store operator surface (a node-created
    // conversation would never reach the hub).
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);

    let _ = ui.shutdown.send(());
}
