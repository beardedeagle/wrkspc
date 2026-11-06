//! Hub acceptance translation: verified-identity attribution plus the
//! single-transaction store commit for inbound A2A `message/send`.
//!
//! The leg-2 spike proved the transport; this is the D-6 A2 contract's
//! data half: a message accepted on the A2A surface commits hub-side
//! (messages, recipients, intents — one transaction via `accept_message`)
//! and creates pollable delivery tasks for recipients whose host is
//! remote. Sender identity is NEVER taken from the payload: the accept
//! loop stamped the verified certificate identity as `x-wrkspc-peer`,
//! and this layer binds the client-asserted peer id to that host.

use axum::Json;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
pub(crate) use parse::{host_from_identity, parse_send};
use wrkspc_core::{AcceptOutcome, RecipientRequest, RemoteFanoutRequest, Store};

pub(crate) mod parse;
#[cfg(test)]
mod tests;

/// Shared state for the acceptance middleware.
#[derive(Clone)]
pub(crate) struct AcceptanceState {
    store: Store,
    /// The hub's own host id: deliveries to peers on OTHER hosts become
    /// pollable tasks.
    local_host: String,
}

impl AcceptanceState {
    /// Builds the middleware state.
    ///
    /// # Arguments
    ///
    /// * `store` — the hub's shared store.
    /// * `local_host` — the hub's own `host_id`.
    pub(crate) fn new(store: Store, local_host: String) -> Self {
        Self { store, local_host }
    }
}

/// The middleware: attributes the sender from the stamped header, commits
/// accepted sends, and queues remote deliveries.
///
/// Flow: (1) requests without the stamped header pass through untouched
/// (plain-TCP pilot serving); (2) `message/send` requests are parsed
/// up-front and the sender's host binding is validated BEFORE the SDK
/// handler runs — a spoofed peer id is rejected before any acceptance;
/// (3) when the SDK handler accepted (2xx), the store commit and remote
/// delivery tasks run; a commit failure is logged and recorded as a feed
/// event, never silent.
pub(crate) async fn accept_translation(
    State(state): State<AcceptanceState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(identity) = request
        .headers()
        .get("x-wrkspc-peer")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
    else {
        return next.run(request).await;
    };

    // Only the JSON-RPC endpoint is translated; every other route (the
    // agent card, health checks) passes through untouched.
    if request.method() != axum::http::Method::POST || request.uri().path() != "/jsonrpc" {
        return next.run(request).await;
    }

    let (parts, body) = request.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, 1 << 20).await else {
        return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
    };
    let parsed: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "invalid JSON-RPC body").into_response();
        }
    };
    let method = parsed
        .get("method")
        .and_then(|m| m.as_str())
        .map(str::to_string);
    let request = Request::from_parts(parts, Body::from(bytes.clone()));

    // Pre-validate attribution for message/send: the asserted sender
    // peer must be enrolled on the host that presented the certificate.
    // The v1 JSON-RPC surface spells the method `SendMessage` (the
    // slash form is the REST transport's spelling, which never reaches
    // this handler).
    let mut send = None;
    if method.as_deref() == Some("SendMessage") {
        let Some(host) = host_from_identity(&identity) else {
            return (StatusCode::UNAUTHORIZED, "peer identity has no SAN").into_response();
        };
        match parse_send(&parsed) {
            Ok(Some(parsed_send)) => {
                match state.store.peer_details(&parsed_send.sender_peer).await {
                    Ok(Some((_, _, peer_host))) if peer_host == host => {
                        // Ejected (booted/banned) peers are refused even
                        // when their host binding is intact: only the
                        // operator's restore action re-admits them.
                        match state.store.peer_status(&parsed_send.sender_peer).await {
                            Ok(Some(status)) if wrkspc_core::status_is_ejected(&status) => {
                                tracing::warn!(%identity, sender = %parsed_send.sender_peer, %status, "ejected peer send refused");
                                let error = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "id": serde_json::Value::Null,
                                    "error": {
                                        "code": -32001,
                                        "message": format!(
                                            "sender peer {} is {status}; the operator controls its status",
                                            parsed_send.sender_peer
                                        ),
                                    },
                                });
                                return (StatusCode::FORBIDDEN, Json(error)).into_response();
                            }
                            Ok(_) => send = Some((parsed_send, host)),
                            Err(error) => {
                                tracing::error!(%error, "peer status lookup failed");
                                return (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "peer status lookup failed",
                                )
                                    .into_response();
                            }
                        }
                    }
                    Ok(Some(_)) | Ok(None) => {
                        tracing::warn!(%identity, sender = %parsed_send.sender_peer, "sender host binding failed");
                        // A JSON-RPC error envelope: clients must be able
                        // to parse the rejection and distinguish a
                        // permanent attribution failure from a transient
                        // transport failure (a bare 403 text body breaks
                        // every client-side parser and wedges retry loops).
                        let error = serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": serde_json::Value::Null,
                            "error": {
                                "code": -32001,
                                "message": format!(
                                    "sender peer {} is not enrolled on the presenting host",
                                    parsed_send.sender_peer
                                ),
                            },
                        });
                        return (StatusCode::FORBIDDEN, Json(error)).into_response();
                    }
                    Err(error) => {
                        tracing::error!(%error, "peer lookup failed");
                        return (StatusCode::INTERNAL_SERVER_ERROR, "peer lookup failed")
                            .into_response();
                    }
                }
            }
            Ok(None) => {
                return (StatusCode::BAD_REQUEST, "message/send missing message").into_response();
            }
            Err(reason) => {
                return (StatusCode::BAD_REQUEST, reason).into_response();
            }
        }
    }

    let response = next.run(request).await;

    // Commit only what the SDK accepted. The JSON-RPC surface answers
    // every failure inside an HTTP 200 envelope, so the error member is
    // the verdict that matters, not the status line.
    let Some((parsed_send, host)) = send else {
        return response;
    };
    let (parts, body) = response.into_parts();
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let bytes = match axum::body::to_bytes(body, 1 << 20).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unable to buffer the JSON-RPC response",
            )
                .into_response();
        }
    };
    let rpc_error = serde_json::from_slice::<serde_json::Value>(&bytes)
        .map(|value| value.get("error").is_some())
        .unwrap_or(false);
    let response = Response::from_parts(parts, Body::from(bytes));
    if rpc_error {
        // The SDK rejected the send (e.g. an unsupported protocol
        // version): no task was accepted, so there is nothing for the
        // wrkspc store to translate.
        return response;
    }

    // Routing is resolved by the store, never guessed here: explicit
    // recipients win, @mentions in the body fall back in, and a post
    // that names nobody wakes nobody. Pre-filling "all other members"
    // here would silently re-broadcast a directed or unaddressed send
    // (the storm this replaced).
    let recipients = parsed_send.recipients.unwrap_or_default();
    let recipient_requests: Vec<RecipientRequest> = recipients
        .iter()
        .cloned()
        .map(|peer_id| RecipientRequest {
            peer_id,
            execution_requested: parsed_send.execution_requested,
        })
        .collect();

    match state
        .store
        .accept_message(
            &parsed_send.conversation_id,
            &parsed_send.sender_peer,
            &parsed_send.message_id,
            &parsed_send.content,
            parsed_send.execution_requested,
            &recipient_requests,
        )
        .await
    {
        Ok(AcceptOutcome::Accepted { message_id }) => {
            // Fan the commit out to every resolved recipient enrolled on
            // a remote host (core-owned queueing, shared with the hub
            // UI's operator post). The committed recipient rows are the
            // mention-resolved authority; the raw request list is not.
            let resolved_recipients = match state.store.message_recipients(&message_id).await {
                Ok(recipients) => recipients,
                Err(error) => {
                    tracing::error!(%error, message_id = %message_id, "resolved recipient lookup failed after acceptance");
                    return response;
                }
            };
            if let Err(error) = state
                .store
                .queue_remote_deliveries(RemoteFanoutRequest {
                    local_host: &state.local_host,
                    conversation_id: &parsed_send.conversation_id,
                    message_id: &message_id,
                    sender_peer: &parsed_send.sender_peer,
                    content: &parsed_send.content,
                    execution_requested: parsed_send.execution_requested,
                    recipients: &resolved_recipients,
                })
                .await
            {
                tracing::error!(%error, "remote delivery queueing failed");
            }
        }
        Ok(AcceptOutcome::Replay { .. }) => {
            // The hub already committed this send; no duplicate delivery
            // tasks are created.
        }
        Err(error) => {
            // Visible, conservative: the A2A task stands as the client's
            // acceptance record; the operator sees the translation gap in
            // the feed.
            tracing::error!(%error, "acceptance translation failed after SDK acceptance");
            let _ = state
                .store
                .append_event(
                    &parsed_send.conversation_id,
                    "acceptance.translation_failed",
                    &serde_json::json!({
                        "sender_peer": parsed_send.sender_peer,
                        "host": host,
                        "reason": error.to_string(),
                    }),
                )
                .await;
        }
    }
    response
}
