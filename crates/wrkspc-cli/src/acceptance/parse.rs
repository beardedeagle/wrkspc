//! `message/send` payload parsing (SDK types, no wire-key guessing)
//! and stamped-identity host extraction.

/// A parsed `message/send` payload.
pub(crate) struct ParsedSend {
    pub(crate) message_id: String,
    pub(crate) conversation_id: String,
    pub(crate) sender_peer: String,
    pub(crate) content: String,
    pub(crate) execution_requested: bool,
    /// The explicit recipient list when the wire message carried a
    /// non-null `recipients` array — including an EMPTY array. `None`
    /// means the field was absent/null (mention inference is the
    /// caller's contract). The distinction is load-bearing: a node's
    /// outbound sync sends its committed recipient set verbatim, and a
    /// committed-EMPTY set is authoritative — the hub must not
    /// re-infer mentions for it.
    pub(crate) recipients: Option<Vec<String>>,
    /// A client-supplied task continuation reference (`taskId`). A
    /// send naming an existing task must be authorized against the
    /// presenting host's own queue BEFORE the SDK handler loads and
    /// mutates that task.
    pub(crate) task_id: Option<String>,
}

/// Parses the `message/send` params with the SDK's own types (no
/// wire-key guessing) and extracts the wrkspc fields from the message
/// metadata.
///
/// `parsed` is the JSON-RPC request value.
///
/// # Returns
///
/// `Ok(Some(parsed))` for a valid send; `Ok(None)` when `params.message`
/// is absent; `Err(reason)` for malformed payloads.
pub(crate) fn parse_send(parsed: &serde_json::Value) -> Result<Option<ParsedSend>, String> {
    let params = parsed.get("params").ok_or("missing params")?;
    let Some(message_value) = params.get("message") else {
        return Ok(None);
    };
    let message: a2a::Message = serde_json::from_value(message_value.clone())
        .map_err(|e| format!("invalid message: {e}"))?;
    let conversation_id = message
        .context_id
        .ok_or("message is missing its contextId")?;
    let sender_peer = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("peerId"))
        .and_then(|value| value.as_str())
        .ok_or("message metadata is missing the sender peerId")?
        .to_string();
    let content = message
        .parts
        .iter()
        .filter_map(|part| match &part.content {
            a2a::PartContent::Text(text) => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    let execution_requested = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("executionRequested"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    // A recipient field, when present and non-null, MUST be an array
    // of strings: malformed explicit recipients are rejected loudly
    // instead of silently discarded. Silently dropping a malformed
    // field can leave zero recipients while execution is requested,
    // and acceptance would then resolve @mentions and execute a turn
    // for a peer the sender never named.
    let recipients = match message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("recipients"))
    {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Array(entries)) => Some(
            entries
                .iter()
                .map(|entry| {
                    entry
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| "recipients must be an array of strings".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => {
            return Err("recipients must be an array of strings".to_string());
        }
    };
    Ok(Some(ParsedSend {
        message_id: message.message_id,
        conversation_id,
        sender_peer,
        content,
        execution_requested,
        recipients,
        task_id: message.task_id,
    }))
}

/// Returns the stamped host id.
///
/// The TLS accept loop stamps the certificate's FIRST DNS SAN alone as
/// `x-wrkspc-peer` (built by `wrkspc_a2a::host_identity` from the parsed
/// SAN extension — never from CN text). The header value is therefore
/// already the authenticated host; this function only validates that it
/// is non-empty.
///
/// `identity` is the stamped identity header value.
///
/// # Returns
///
/// The host id, or `None` when the stamp is empty.
pub(crate) fn host_from_identity(identity: &str) -> Option<String> {
    let host = identity.trim();
    (!host.is_empty()).then(|| host.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send_with_recipients(recipients: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": "m-1",
                    "role": "ROLE_USER",
                    "contextId": "conv-1",
                    "parts": [{"text": "hello"}],
                    "metadata": {
                        "peerId": "node-test-1",
                        "recipients": recipients,
                        "executionRequested": true,
                    },
                },
            },
        })
    }

    /// A well-formed recipients array parses; absent/null yields None.
    #[test]
    fn recipients_parse_strictly() {
        let parsed = parse_send(&send_with_recipients(serde_json::json!(["codex"])))
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.recipients.as_deref(),
            Some(&["codex".to_string()][..])
        );
    }

    /// An explicit EMPTY recipients array is distinct from an absent
    /// (or null) field: a node's outbound sync sends its committed
    /// recipient set verbatim, and a committed-empty list is
    /// authoritative at the hub — the parse must keep the two states
    /// apart so acceptance can route them differently.
    #[test]
    fn explicit_empty_recipients_are_distinct_from_absent() {
        // Explicit empty array → Some(empty).
        let explicit = parse_send(&send_with_recipients(serde_json::json!([])))
            .unwrap()
            .unwrap();
        assert_eq!(explicit.recipients.as_deref(), Some(&[][..]));
        // Null → None (not explicitly supplied).
        let nulled = parse_send(&send_with_recipients(serde_json::Value::Null))
            .unwrap()
            .unwrap();
        assert_eq!(nulled.recipients, None);
        // Absent → None.
        let absent = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": "m-1",
                    "role": "ROLE_USER",
                    "contextId": "conv-1",
                    "parts": [{"text": "hello"}],
                    "metadata": {
                        "peerId": "node-test-1",
                        "executionRequested": true,
                    },
                },
            },
        });
        let parsed = parse_send(&absent).unwrap().unwrap();
        assert_eq!(parsed.recipients, None);
    }

    /// A client-supplied continuation reference (`taskId`) is retained
    /// so the acceptance layer can authorize it against the presenting
    /// host's queue before the SDK handler touches the task.
    #[test]
    fn continuation_task_reference_is_retained() {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": "m-2",
                    "role": "ROLE_USER",
                    "contextId": "conv-1",
                    "taskId": "task-foreign",
                    "parts": [{"text": "continue"}],
                    "metadata": {
                        "peerId": "node-test-1",
                        "executionRequested": true,
                    },
                },
            },
        });
        let parsed = parse_send(&body).unwrap().unwrap();
        assert_eq!(parsed.task_id.as_deref(), Some("task-foreign"));
        let plain = parse_send(&send_with_recipients(serde_json::json!([])))
            .unwrap()
            .unwrap();
        assert_eq!(plain.task_id, None);
    }

    /// Malformed explicit recipients are REJECTED, not silently
    /// discarded: silent discard can leave zero recipients while
    /// execution is requested and wake the wrong peer via @mentions.
    #[test]
    fn malformed_recipients_are_rejected() {
        // Non-array.
        assert!(parse_send(&send_with_recipients(serde_json::json!("codex"))).is_err());
        // Array containing a non-string.
        assert!(parse_send(&send_with_recipients(serde_json::json!(["codex", 7]))).is_err());
        // Array of objects.
        assert!(
            parse_send(&send_with_recipients(
                serde_json::json!([{"peer": "codex"}])
            ))
            .is_err()
        );
    }
}
