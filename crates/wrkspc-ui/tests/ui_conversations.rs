//! Room naming over the operator API.
//!
//! A name is display metadata the hub owns: nullable, non-unique, and mirrored
//! to nodes. These tests pin the wire behaviour the UI's inline rename relies on.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;

/// Renaming a room stores the name and returns the updated room.
#[tokio::test]
async fn patch_conversation_sets_the_name() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;

    let renamed = ui
        .client()
        .patch(format!("{}/api/conversations/{conversation_id}", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "name": "  Veritas design  " }))
        .send()
        .await
        .unwrap();
    assert_eq!(renamed.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = renamed.json().await.unwrap();
    assert_eq!(body["name"], "Veritas design");
    assert_eq!(body["id"], conversation_id);

    let listed: Vec<serde_json::Value> = ui
        .client()
        .get(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = listed
        .iter()
        .find(|row| row["id"] == conversation_id)
        .expect("the room is listed");
    assert_eq!(row["name"], "Veritas design");
}

/// A null or blank name clears it: a room is fully usable unnamed.
#[tokio::test]
async fn patch_conversation_clears_the_name() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;

    for body in [
        serde_json::json!({ "name": "temp" }),
        serde_json::json!({ "name": null }),
    ] {
        let response = ui
            .client()
            .patch(format!("{}/api/conversations/{conversation_id}", ui.base))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    let body: serde_json::Value = ui
        .client()
        .patch(format!("{}/api/conversations/{conversation_id}", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "name": "   " }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        body["name"],
        serde_json::Value::Null,
        "a blank name clears it"
    );
}

/// An unknown room is a 404, not a silent no-op.
#[tokio::test]
async fn patch_conversation_refuses_an_unknown_room() {
    let ui = spawn_ui().await;
    let response = ui
        .client()
        .patch(format!("{}/api/conversations/no-such-room", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "name": "anything" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

/// An over-long name is refused rather than truncated.
#[tokio::test]
async fn patch_conversation_refuses_an_over_long_name() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;
    let response = ui
        .client()
        .patch(format!("{}/api/conversations/{conversation_id}", ui.base))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "name": "x".repeat(65) }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    let listed: Vec<serde_json::Value> = ui
        .client()
        .get(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = listed
        .iter()
        .find(|row| row["id"] == conversation_id)
        .expect("the room is listed");
    assert_eq!(
        row["name"],
        serde_json::Value::Null,
        "the refusal changed nothing"
    );
}
