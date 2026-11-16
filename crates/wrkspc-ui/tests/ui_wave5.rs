//! Wave-5 UI regressions: alias clearing (UI-09), atomic peer PATCH
//! (UI-10), operator membership at room creation (UI-18), and
//! hub-owned mutations refused on connected nodes (UI-08).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;
use wrkspc_core::Store;
use wrkspc_ui::UiConfig;

/// The role configuration the wave-5 harness boots with: the pinned
/// test token, and delivery queueing following the hub role.
fn wave5_config(hub_role: bool, hub_connected: bool) -> UiConfig {
    UiConfig {
        queue_remote_deliveries: hub_role,
        hub_role,
        hub_connected,
        token: Some(TOKEN.to_string()),
        ..Default::default()
    }
}

fn patch(url: &str, body: serde_json::Value) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .patch(url)
        .bearer_auth(TOKEN)
        .json(&body)
}

/// UI-09: an explicit `{"alias": null}` CLEARS the alias (the normal
/// UI path must be able to remove one), and the roster readback
/// confirms it.
#[tokio::test]
async fn alias_can_be_cleared_with_an_explicit_null() {
    let store = Store::open_in_memory("ui-alias-clear").await.unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    store
        .set_peer_alias("codex", Some("Rover"), "operator")
        .await
        .unwrap();
    let ui = spawn_ui_on(store.clone(), wave5_config(true, false)).await;
    let base = ui.base.clone();

    let res = patch(
        &format!("{base}/api/peers/codex"),
        serde_json::json!({ "alias": null }),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        res.status(),
        reqwest::StatusCode::OK,
        "clearing must succeed"
    );
    let entry = store
        .roster_entry("codex")
        .await
        .unwrap()
        .expect("peer exists");
    assert!(entry.alias.is_none(), "the alias must be gone: {entry:#?}");

    let _ = ui.shutdown.send(());
}

/// UI-10: an invalid status must not leave a partial alias mutation,
/// and an alias-only patch for an unknown peer is a 404 — never a 200.
#[tokio::test]
async fn peer_patch_validates_before_mutating_and_404s_unknown_targets() {
    let store = Store::open_in_memory("ui-patch-atomic").await.unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let ui = spawn_ui_on(store.clone(), wave5_config(true, false)).await;
    let base = ui.base.clone();

    // Valid alias + invalid status: 400 and NO alias change.
    let res = patch(
        &format!("{base}/api/peers/codex"),
        serde_json::json!({ "alias": "changed-on-error", "status": "not-a-status" }),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::BAD_REQUEST);
    let entry = store
        .roster_entry("codex")
        .await
        .unwrap()
        .expect("peer exists");
    assert_ne!(
        entry.alias.as_deref(),
        Some("changed-on-error"),
        "the rejected patch must not mutate: {entry:#?}"
    );

    // Alias-only patch to an unknown peer: 404.
    let res = patch(
        &format!("{base}/api/peers/no-such-peer"),
        serde_json::json!({ "alias": "ghost" }),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);

    let _ = ui.shutdown.send(());
}

/// UI-18: a room the operator is not a member of cannot be created —
/// every UI post is authored as `operator`, so such a room would
/// present a composer that can only fail.
#[tokio::test]
async fn room_creation_requires_the_operator() {
    let store = Store::open_in_memory("ui-operator-room").await.unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    store
        .upsert_roster_presence("agy", "agy", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let ui = spawn_ui_on(store.clone(), wave5_config(true, false)).await;
    let base = ui.base.clone();

    let res = reqwest::Client::new()
        .post(format!("{base}/api/conversations"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "members": ["codex", "agy"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "a room without the operator must be refused"
    );

    let _ = ui.shutdown.send(());
}

/// UI-08: on a hub-connected node, hub-owned mutations (conversation
/// name, peer alias) are refused with 409 instead of reporting success
/// for writes the next authoritative snapshot erases.
#[tokio::test]
async fn connected_node_refuses_hub_owned_mutations() {
    let store = Store::open_in_memory("ui-connected-node").await.unwrap();
    store
        .upsert_roster_presence("operator", "operator", "human", "node-test-1", "connected")
        .await
        .unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let ui = spawn_ui_on(store.clone(), wave5_config(false, true)).await;
    let base = ui.base.clone();

    // Alias: 409 with a clear explanation.
    let res = patch(
        &format!("{base}/api/peers/codex"),
        serde_json::json!({ "alias": "local-rename" }),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CONFLICT);
    let entry = store
        .roster_entry("codex")
        .await
        .unwrap()
        .expect("peer exists");
    assert_ne!(entry.alias.as_deref(), Some("local-rename"));

    // Conversation rename: 409.
    let res = reqwest::Client::new()
        .patch(format!("{base}/api/conversations/{conversation}"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "name": "local-name" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CONFLICT);

    let _ = ui.shutdown.send(());
}
