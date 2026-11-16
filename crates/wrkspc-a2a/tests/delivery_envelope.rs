//! Contract: the task shell core writes IS a canonical A2A Task —
//! this pins the seam so an SDK upgrade that changes the wire shape
//! fails here before a stored blob breaks a reader.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use wrkspc_core::DeliveryEnvelope;

#[test]
fn envelope_shell_parses_as_a2a_task_and_round_trips() {
    let envelope = DeliveryEnvelope {
        kind: "delivery".to_string(),
        message_id: "m-1".to_string(),
        origin_message_id: Some("local-1".to_string()),
        conversation_id: "conv-1".to_string(),
        sender_peer: "operator".to_string(),
        sender_name: "operator".to_string(),
        sender_class: "human".to_string(),
        sender_host: "pilot-host".to_string(),
        recipient_peer: "codex".to_string(),
        recipient_name: "codex".to_string(),
        recipient_class: "harness".to_string(),
        content: "hello".to_string(),
        execution_requested: true,
        guidance: vec![serde_json::json!({
            "id": "g-1",
            "author_id": "operator",
            "text": "stay in the room",
        })],
    };
    let shell = envelope.to_task_json("task-1", "node-host");

    // The stored blob IS the wire Task: a2a must parse it as one.
    let task: a2a::Task = serde_json::from_value(shell.clone())
        .expect("the shell core writes must parse as a2a::Task");
    assert_eq!(task.id, "task-1");
    assert_eq!(task.context_id, "node-host");

    // And the envelope round-trips byte-for-byte through the helper
    // every reader shares.
    let encoded = shell.to_string();
    let decoded = DeliveryEnvelope::from_task_json(&encoded).unwrap();
    assert_eq!(decoded, envelope);
}

#[test]
fn envelope_read_refuses_malformed_blobs() {
    assert!(DeliveryEnvelope::from_task_json("not json").is_none());
    assert!(DeliveryEnvelope::from_task_json("{}").is_none());
    assert!(
        DeliveryEnvelope::from_task_json(r#"{"metadata": {"delivery": {"kind": "delivery"}}}"#)
            .is_none(),
        "missing required fields must not parse"
    );
}
