//! Read views: conversations, the cursor-based event feed, and
//! transcript redaction.

mod common;
use common::*;

/// Conversations list their members; unknown conversation ids 404.
#[tokio::test]
async fn conversations_and_members_are_listed() {
    let ui = spawn_ui().await;
    let id = ui.conversation_id().await;

    let res = ui
        .client()
        .get(format!("{}/api/conversations/{id}", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let conversation: serde_json::Value = res.json().await.unwrap();
    let members = conversation["members"].as_array().unwrap();
    assert!(members.iter().any(|m| m == "operator"));
    assert!(members.iter().any(|m| m == "agent"));

    let missing = ui
        .client()
        .get(format!("{}/api/conversations/nope", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    let _ = ui.shutdown.send(());
}

/// The event feed resumes from a cursor: new events appear, and nothing after the last seq.
#[tokio::test]
async fn event_feed_is_cursor_based() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;
    ui.post_message("hello").await;

    let first = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/events?after_seq=0",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let events: Vec<serde_json::Value> = first.json().await.unwrap();
    assert!(events.iter().any(|e| e["kind"] == "message.accepted"));
    let last_seq = events.last().unwrap()["seq"].as_i64().unwrap();

    let next = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/events?after_seq={last_seq}",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let more: Vec<serde_json::Value> = next.json().await.unwrap();
    assert!(more.is_empty(), "cursor resumes with nothing new");
    let _ = ui.shutdown.send(());
}

/// Transcript content is redacted on the way out (secrets never render).
#[tokio::test]
async fn transcripts_are_redacted_on_the_way_out() {
    let ui = spawn_ui().await;
    let conversation_id = ui.conversation_id().await;
    ui.post_message("deploy with sk-abcdef1234567890 please")
        .await;

    let res = ui
        .client()
        .get(format!(
            "{}/api/conversations/{conversation_id}/messages",
            ui.base
        ))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let messages: Vec<serde_json::Value> = res.json().await.unwrap();
    let content = messages[0]["content"].as_str().unwrap();
    assert!(
        !content.contains("sk-abcdef1234567890"),
        "secret must not render: {content}"
    );
    assert!(content.contains("[REDACTED key]"), "got: {content}");
    let _ = ui.shutdown.send(());
}
