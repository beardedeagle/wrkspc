//! Node-to-hub message propagation (the reply leg's transport).

use wrkspc_core::Store;

/// Propagates node-local harness messages to the hub via
/// `message/send`, recording each success in the hub-outbound ledger.
///
/// Only messages whose sender is enrolled on this node's own host
/// qualify — hub-mirrored deliveries carry a remote sender host and
/// are never re-sent.
///
/// # Arguments
///
/// * `a2a` — the hub A2A client (this node's mTLS identity).
/// * `store` — the node's local store.
/// * `host_id` — this node's host id.
///
/// # Returns
///
/// The number of messages sent this tick.
pub(crate) async fn sync_outbound(
    a2a: &a2a_client::A2AClient<a2a_client::jsonrpc::JsonRpcTransport>,
    store: &Store,
    host_id: &str,
) -> usize {
    let pending = match store.hub_pending_outbound(host_id).await {
        Ok(pending) => pending,
        Err(error) => {
            tracing::error!(%error, "outbound pending lookup failed");
            return 0;
        }
    };
    let mut sent = 0;
    for message in pending {
        let metadata = std::collections::HashMap::from([(
            "peerId".to_string(),
            serde_json::Value::String(message.sender_peer.clone()),
        )]);
        let request = a2a::SendMessageRequest {
            message: a2a::Message {
                message_id: message.message_id.clone(),
                context_id: Some(message.conversation_id.clone()),
                task_id: None,
                role: a2a::Role::User,
                parts: vec![a2a::Part {
                    content: a2a::PartContent::Text(message.content.clone()),
                    filename: None,
                    media_type: None,
                    metadata: None,
                }],
                metadata: Some(metadata),
                extensions: None,
                reference_task_ids: None,
            },
            configuration: None,
            metadata: None,
            tenant: None,
        };
        match a2a.send_message(&request).await {
            Ok(_) => {
                if let Err(error) = store.mark_hub_sent(&message.message_id).await {
                    tracing::error!(
                        %error,
                        message_id = %message.message_id,
                        "mark_hub_sent failed"
                    );
                }
                sent += 1;
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    message_id = %message.message_id,
                    "hub send failed; retrying next tick"
                );
            }
        }
    }
    sent
}
