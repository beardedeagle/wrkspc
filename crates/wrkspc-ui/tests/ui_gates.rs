//! Moderation gates: fences, holds, the cancel seam, and guidance.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;

/// Fences create, list, and clear; unknown kinds are rejected.
#[tokio::test]
async fn fences_create_list_and_clear() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;

    let created = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation_id}/fences",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "peer_id": "agent", "kind": "pause" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::OK);
    let fence_id = created.json::<serde_json::Value>().await.unwrap()["fence_id"]
        .as_str()
        .unwrap()
        .to_string();

    let listed = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/fences",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let fences: Vec<serde_json::Value> = listed.json().await.unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0]["kind"], "pause");

    let cleared = ui
        .client()
        .delete(format!("{}/api/fences/{fence_id}", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(cleared.status(), reqwest::StatusCode::NO_CONTENT);

    let listed = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/fences",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let fences: Vec<serde_json::Value> = listed.json().await.unwrap();
    assert!(fences.is_empty(), "cleared fence disappears");

    // Unknown kind is rejected.
    let bad = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation_id}/fences",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "peer_id": "agent", "kind": "nope" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), reqwest::StatusCode::BAD_REQUEST);
    let _ = ui.shutdown.send(());
}

/// Hold moves a delivery to held; release moves it back.
#[tokio::test]
async fn hold_and_release_move_delivery_state() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;
    let message_id = ui.post_message("do the thing").await;

    let held = ui
        .client()
        .post(format!(
            "{}/api/messages/{message_id}/deliveries/agent/hold",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(held.status(), reqwest::StatusCode::NO_CONTENT);

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
    let deliveries = messages[0]["deliveries"].as_array().unwrap();
    assert_eq!(deliveries[0]["state"], "held");

    let released = ui
        .client()
        .post(format!(
            "{}/api/messages/{message_id}/deliveries/agent/release",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(released.status(), reqwest::StatusCode::NO_CONTENT);
    let _ = ui.shutdown.send(());
}

/// Cancel routes through the requester seam with the exact message and recipient.
#[tokio::test]
async fn cancel_routes_through_the_requester_seam() {
    let ui = spawn_ui().await;
    let message_id = ui.post_message("cancel me").await;

    let res = ui
        .client()
        .post(format!(
            "{}/api/messages/{message_id}/deliveries/agent/cancel",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["outcome"], "attempt_interrupt");

    let calls = ui.cancel.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "the requester received the call");
    assert_eq!(calls[0].0, message_id);
    assert_eq!(calls[0].1, "agent");
    let _ = ui.shutdown.send(());
}

/// Guidance lands in the feed with secrets redacted.
#[tokio::test]
async fn guidance_lands_in_the_feed_redacted() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;

    let res = ui
        .client()
        .post(format!(
            "{}/api/conversations/{conversation_id}/guidance",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "text": "use password: hunter2 for staging" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    let events: Vec<serde_json::Value> = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/events?after_seq=0",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let guidance = events
        .iter()
        .find(|e| e["kind"] == "guidance")
        .expect("guidance event");
    let payload = guidance["payload"].to_string();
    assert!(
        !payload.contains("hunter2"),
        "guidance secret must not render: {payload}"
    );
    let _ = ui.shutdown.send(());
}
