//! `message/send` payload parsing (SDK types, no wire-key guessing)
//! and stamped-identity host extraction.

/// A parsed `message/send` payload.
pub(crate) struct ParsedSend {
    pub(crate) message_id: String,
    pub(crate) conversation_id: String,
    pub(crate) sender_peer: String,
    pub(crate) content: String,
    pub(crate) execution_requested: bool,
    pub(crate) recipients: Option<Vec<String>>,
}

/// Parses the `message/send` params with the SDK's own types (no
/// wire-key guessing) and extracts the wrkspc fields from the message
/// metadata.
///
/// # Arguments
///
/// * `parsed` — the JSON-RPC request value.
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
    let recipients = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("recipients"))
        .and_then(|value| value.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        });
    Ok(Some(ParsedSend {
        message_id: message.message_id,
        conversation_id,
        sender_peer,
        content,
        execution_requested,
        recipients,
    }))
}

/// Extracts the host id from a stamped identity string of the form
/// `CN=... SAN=node-<host-id>` (built by `wrkspc_a2a::peer_identity`).
///
/// # Arguments
///
/// * `identity` — the stamped identity header value.
///
/// # Returns
///
/// The first SAN value, or `None` when the identity carries no SAN.
pub(crate) fn host_from_identity(identity: &str) -> Option<String> {
    let san = identity.split("SAN=").nth(1)?;
    san.split(',')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}
