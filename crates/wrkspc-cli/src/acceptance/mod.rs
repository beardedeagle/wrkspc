//! Hub acceptance translation: verified-identity attribution, task-read
//! authorization, and the single-transaction store commit for inbound
//! A2A sends.
//!
//! The leg-2 spike proved the transport; this is the D-6 A2 contract's
//! data half: a message accepted on the A2A surface commits hub-side
//! (messages, recipients, intents — one transaction via `accept_message`)
//! and creates pollable delivery tasks for recipients whose host is
//! remote. Sender identity is NEVER taken from the payload: the accept
//! loop stamped the verified certificate identity as `x-wrkspc-peer`,
//! and this layer binds the client-asserted peer id to that host.
//!
//! Task reads (`GetTask`/`CancelTask`/`SubscribeToTask`/`ListTasks` and
//! their REST twins) are host-queue-scoped: a stamped connection may
//! only address tasks whose queue context is its own host id, so one
//! enrolled host can never read or cancel another host's deliveries.

use axum::Json;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
pub(crate) use parse::{ParsedSend, host_from_identity, parse_send};
use wrkspc_core::{AcceptOutcome, RecipientRequest, RemoteFanoutRequest, Store, StoreError};

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
    /// `store` is the hub's shared store; `local_host` is the hub's own
    /// `host_id`.
    pub(crate) fn new(store: Store, local_host: String) -> Self {
        Self { store, local_host }
    }
}

/// The middleware: authorizes task-scoped reads/cancels, attributes the
/// sender from the stamped header, and commits accepted sends.
///
/// Flow: (1) requests without the stamped header pass through untouched
/// (plain-TCP pilot serving); (2) the REST subtree gets the same
/// authorization + send translation via its own path/query shapes;
/// (3) on `/jsonrpc`, task methods (`GetTask`/`CancelTask`/
/// `SubscribeToTask`/`ListTasks`) are checked against the presenting
/// host's own queue, `message/send` requests are parsed up-front with
/// the sender's host binding AND any referenced continuation task
/// validated BEFORE the SDK handler runs — a spoofed peer id or a
/// foreign task reference is rejected before any acceptance — and
/// streaming sends (`SendStreamingMessage`, `/rest/message:stream`,
/// `/rest/message/stream`) are refused outright: no secure streaming
/// translation exists, so those SDK entry points fail closed instead of
/// reaching the executor unguarded; (4) when the SDK handler accepted
/// (2xx), the store commit and remote delivery tasks run; a commit
/// failure is logged and recorded as a feed event, never silent.
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
    let Some(host) = host_from_identity(&identity) else {
        return (StatusCode::UNAUTHORIZED, "peer identity has no SAN").into_response();
    };

    // The REST (HTTP+JSON) subtree: its task methods and send routes get
    // the same attribution discipline as their JSON-RPC twins.
    if request.uri().path().starts_with("/rest/") {
        return accept_rest(&state, &host, request, next).await;
    }

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

    // Task-scoped methods are host-queue-scoped: a stamped connection
    // may only address tasks in ITS OWN queue. Rejected requests never
    // reach the SDK handler, and the rejection confirms nothing about
    // other hosts' tasks (no existence oracle).
    let mut send = None;
    match method.as_deref() {
        Some("GetTask") | Some("CancelTask") | Some("SubscribeToTask") => {
            let task_id = parsed
                .get("params")
                .and_then(|params| params.get("id"))
                .and_then(|id| id.as_str());
            let Some(task_id) = task_id else {
                return (StatusCode::BAD_REQUEST, "task method is missing its id").into_response();
            };
            if !task_owned_by(&state, task_id, &host).await {
                return rpc_authorization_error("task is not in this host's delivery queue");
            }
        }
        Some("ListTasks") => {
            let context = parsed
                .get("params")
                .and_then(|params| params.get("contextId"))
                .and_then(|id| id.as_str());
            if context != Some(host.as_str()) {
                return rpc_authorization_error("contextId must name this host's own queue");
            }
        }
        Some("SendMessage") => {
            // Pre-validate attribution for message/send: the asserted
            // sender peer must be enrolled on the host that presented
            // the certificate, and a continuation taskId must name a
            // task in THAT host's own queue — before the SDK handler
            // can load or mutate it. The v1 JSON-RPC surface spells
            // the method `SendMessage` (the slash form is the REST
            // transport's spelling, which never reaches this handler).
            match parse_send(&parsed) {
                Ok(Some(parsed_send)) => {
                    match sender_refusal(&state, &identity, &parsed_send, &host).await {
                        Some(response) => return response,
                        None => {
                            if let Some(task_id) = parsed_send.task_id.as_deref()
                                && !task_owned_by(&state, task_id, &host).await
                            {
                                return rpc_authorization_error(
                                    "continuation task is not in this host's delivery queue",
                                );
                            }
                            send = Some(parsed_send);
                        }
                    }
                }
                Ok(None) => {
                    return (StatusCode::BAD_REQUEST, "message/send missing message")
                        .into_response();
                }
                Err(reason) => {
                    return (StatusCode::BAD_REQUEST, reason).into_response();
                }
            }
        }
        // Streaming sends are a REGISTERED SDK entry point with no
        // secure acceptance translation: refuse them before the SDK
        // handler runs rather than letting them reach the executor
        // without sender validation or store commit.
        Some("SendStreamingMessage") => {
            return rpc_authorization_error(
                "streaming message sends are not supported on this surface",
            );
        }
        _ => {}
    }

    let response = next.run(request).await;

    // Commit only what the SDK accepted. The JSON-RPC surface answers
    // every failure inside an HTTP 200 envelope, so the error member is
    // the verdict that matters, not the status line.
    let Some(parsed_send) = send else {
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

    if let Some(error_response) = commit_send(&state, &parsed_send).await {
        return error_response;
    }
    response
}

/// Authorizes and translates the REST (HTTP+JSON) subtree: task reads
/// and cancels are host-queue-scoped exactly like their JSON-RPC twins,
/// and `message/send` (both spellings) runs the same verified-identity
/// acceptance the JSON-RPC surface enforces.
async fn accept_rest(
    state: &AcceptanceState,
    host: &str,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().clone();
    let segments: Vec<&str> = path
        .trim_start_matches("/rest/")
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    match segments.as_slice() {
        // GET /rest/tasks — list by queue (contextId must be this host).
        ["tasks"] => {
            let context = request
                .uri()
                .query()
                .and_then(|query| query_param(query, "contextId"));
            if context.as_deref() != Some(host) {
                return (
                    StatusCode::FORBIDDEN,
                    "contextId must name this host's own queue",
                )
                    .into_response();
            }
            next.run(request).await
        }
        // /rest/tasks/{id} (+ legacy /cancel and /subscribe suffixes):
        // every action on a task is host-queue-scoped. Canonical
        // action spellings (/rest/tasks/{id}:cancel, /rest/tasks/{id}:subscribe)
        // carry the suffix INSIDE the id segment: strip only the two
        // recognized suffixes for the ownership lookup, while the
        // original URI still reaches the SDK (whose own router strips
        // them inside its handlers).
        ["tasks", id, ..] => {
            let id = strip_task_action_suffix(id).to_string();
            if !task_owned_by(state, &id, host).await {
                return (
                    StatusCode::FORBIDDEN,
                    "task is not in this host's delivery queue",
                )
                    .into_response();
            }
            let _ = method;
            next.run(request).await
        }
        // POST /rest/message:send and /rest/message/send: the same
        // verified-identity acceptance as the JSON-RPC surface. The
        // legacy slash spelling splits into two segments — match it as
        // ["message", "send"], never as a single segment containing
        // the unsplit path.
        ["message:send"] | ["message", "send"] => {
            if method != axum::http::Method::POST {
                return next.run(request).await;
            }
            let (parts, body) = request.into_parts();
            let Ok(bytes) = axum::body::to_bytes(body, 1 << 20).await else {
                return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
            };
            let body_value: serde_json::Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "invalid request body").into_response();
                }
            };
            let Some(message_value) = body_value.get("message").cloned() else {
                return (StatusCode::BAD_REQUEST, "message/send missing message").into_response();
            };
            let wrapped = serde_json::json!({ "params": { "message": message_value } });
            let parsed_send = match parse_send(&wrapped) {
                Ok(Some(parsed_send)) => parsed_send,
                Ok(None) => {
                    return (StatusCode::BAD_REQUEST, "message/send missing message")
                        .into_response();
                }
                Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
            };
            let identity = host.to_string();
            if let Some(response) = sender_refusal(state, &identity, &parsed_send, host).await {
                return response;
            }
            // A continuation taskId must name a task in the presenting
            // host's own queue — authorized BEFORE the SDK handler can
            // load or mutate the referenced task.
            if let Some(task_id) = parsed_send.task_id.as_deref()
                && !task_owned_by(state, task_id, host).await
            {
                return rpc_authorization_error(
                    "continuation task is not in this host's delivery queue",
                );
            }
            let response = next
                .run(Request::from_parts(parts, Body::from(bytes)))
                .await;
            if !response.status().is_success() {
                return response;
            }
            if let Some(error_response) = commit_send(state, &parsed_send).await {
                return error_response;
            }
            response
        }
        // Streaming sends (both spellings) are a REGISTERED SDK entry
        // point with no secure acceptance translation: refuse them
        // BEFORE the SDK handler runs. Anything else on these paths
        // (non-POST) is the SDK's own 405.
        ["message:stream"] | ["message", "stream"] => {
            if method != axum::http::Method::POST {
                return next.run(request).await;
            }
            rpc_authorization_error("streaming message sends are not supported on this surface")
        }
        _ => next.run(request).await,
    }
}

/// Strips the SDK's canonical task-action suffixes (`:cancel`,
/// `:subscribe`) for the OWNERSHIP lookup only. The SDK's own router
/// interprets the suffix on the original path, so dispatch must keep
/// the untouched URI; unrecognized text is passed through unchanged
/// and fails closed in the lookup.
fn strip_task_action_suffix(id: &str) -> &str {
    id.strip_suffix(":cancel")
        .or_else(|| id.strip_suffix(":subscribe"))
        .unwrap_or(id)
}

/// Pre-validates sender attribution for a send: the asserted sender
/// peer must be enrolled on the host that presented the certificate,
/// and must not be ejected (booted/banned) — only the operator's
/// restore action re-admits it.
///
/// `state` is the acceptance middleware state; `identity` is the
/// stamped identity (for the refusal log); `parsed_send` is the parsed
/// send payload; `host` is the presenting host (the stamped certificate
/// SAN).
///
/// # Returns
///
/// The response that short-circuits the request, or `None` to proceed.
async fn sender_refusal(
    state: &AcceptanceState,
    identity: &str,
    parsed_send: &ParsedSend,
    host: &str,
) -> Option<Response> {
    match state.store.peer_details(&parsed_send.sender_peer).await {
        Ok(Some((_, _, peer_host))) if peer_host == host => {
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
                    Some((StatusCode::FORBIDDEN, Json(error)).into_response())
                }
                Ok(_) => None,
                Err(error) => {
                    tracing::error!(%error, "peer status lookup failed");
                    Some(
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "peer status lookup failed",
                        )
                            .into_response(),
                    )
                }
            }
        }
        Ok(Some(_)) | Ok(None) => {
            tracing::warn!(%identity, sender = %parsed_send.sender_peer, "sender host binding failed");
            // A JSON-RPC error envelope: clients must be able to parse
            // the rejection and distinguish a permanent attribution
            // failure from a transient transport failure (a bare 403
            // text body breaks every client-side parser and wedges
            // retry loops).
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
            Some((StatusCode::FORBIDDEN, Json(error)).into_response())
        }
        Err(error) => {
            tracing::error!(%error, "peer lookup failed");
            Some((StatusCode::INTERNAL_SERVER_ERROR, "peer lookup failed").into_response())
        }
    }
}

/// Commits an SDK-accepted send to the wrkspc store and queues remote
/// deliveries. Routing is resolved by the store, never guessed here:
/// explicit recipients win, @mentions in the body fall back in, and a
/// post that names nobody wakes nobody.
///
/// The absent-vs-explicit-empty distinction is ROUTED here: an
/// explicitly supplied recipient list — EMPTY included — is committed
/// routing (a node's outbound sync propagates its locally committed
/// set verbatim) and reaches the store's mention-free acceptance path;
/// only an ABSENT field authorizes mention inference for a fresh
/// caller.
///
/// A rejected commit returns the JSON-RPC error envelope that REPLACES
/// the SDK's success response: the sender is told its acceptance did
/// not commit, and the node's outbound ledger never marks it sent.
async fn commit_send(state: &AcceptanceState, parsed_send: &ParsedSend) -> Option<Response> {
    let (recipients, explicit) = match parsed_send.recipients.clone() {
        Some(recipients) => (recipients, true),
        None => (Vec::new(), false),
    };
    let recipient_requests: Vec<RecipientRequest> = recipients
        .iter()
        .cloned()
        .map(|peer_id| RecipientRequest {
            peer_id,
            execution_requested: parsed_send.execution_requested,
        })
        .collect();

    let outcome = if explicit {
        state
            .store
            .accept_message_committed_routing(
                &parsed_send.conversation_id,
                &parsed_send.sender_peer,
                &parsed_send.message_id,
                &parsed_send.content,
                parsed_send.execution_requested,
                &recipient_requests,
            )
            .await
    } else {
        state
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
    };

    match outcome {
        Ok(AcceptOutcome::Accepted { message_id }) => {
            fan_out_accepted(
                state,
                parsed_send,
                &message_id,
                parsed_send.execution_requested,
            )
            .await
        }
        Ok(AcceptOutcome::Replay { message_id }) => {
            // A replay's fan-out authority is the COMMITTED message
            // state, never the retry payload's flag: the first
            // acceptance fixed whether this message may execute, and a
            // retry carrying executionRequested=true must not escalate
            // a mail-only message (or its missing delivery tasks) into
            // executable work.
            let authority = match committed_execution_authority(
                state,
                &parsed_send.conversation_id,
                &message_id,
            )
            .await
            {
                Ok(authority) => authority,
                Err(response) => return Some(*response),
            };
            fan_out_accepted(state, parsed_send, &message_id, authority).await
        }
        // Business-rule rejections are returned to the sender — never
        // reported as a successful acceptance, and the node's outbound
        // ledger never marks a rejected message hub-sent.
        Err(StoreError::PeerEjected { peer_id, status }) => Some(rpc_error_response(
            StatusCode::FORBIDDEN,
            &format!("peer {peer_id} is {status}; the operator controls its status"),
        )),
        Err(StoreError::IdempotencyConflict { key }) => Some(rpc_error_response(
            StatusCode::CONFLICT,
            &format!("idempotency key {key} was already used with different content"),
        )),
        // Mention ambiguity fails closed at the store: the sender must
        // name an exact peer rather than have the hub guess.
        Err(StoreError::AmbiguousPeerRef { reference }) => Some(rpc_error_response(
            StatusCode::BAD_REQUEST,
            &format!("mention `{reference}` is ambiguous; name the peer's exact id"),
        )),
        Err(StoreError::ExecutionAuthorityConflict { message_id }) => Some(rpc_error_response(
            StatusCode::CONFLICT,
            &format!(
                "message {message_id} was accepted without execution; a replay cannot make it executable"
            ),
        )),
        Err(StoreError::NotFound { entity, id }) => Some(rpc_error_response(
            StatusCode::NOT_FOUND,
            &format!("{entity} `{id}` not found"),
        )),
        Err(StoreError::TooManyRecipients { limit, count }) => Some(rpc_error_response(
            StatusCode::BAD_REQUEST,
            &format!("too many recipients: {count} (max {limit})"),
        )),
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
                        "reason": error.to_string(),
                    }),
                )
                .await;
            Some(rpc_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "acceptance translation failed",
            ))
        }
    }
}

/// Fans a committed acceptance out to every resolved recipient
/// enrolled on a remote host (core-owned queueing, shared with the hub
/// UI's operator post). The committed recipient rows are the
/// mention-resolved authority; the raw request list is not. The
/// fan-out runs for REPLAYS too: delivery-task ids are deterministic
/// in (message, recipient), so re-queueing is idempotent and repairs a
/// partially completed fan-out — a successful replay is never evidence
/// that every recipient task exists.
///
/// `execution_requested` is the authority for the created delivery
/// tasks: the freshly committed flag for a new acceptance, the
/// COMMITTED message flag for a replay.
async fn fan_out_accepted(
    state: &AcceptanceState,
    parsed_send: &ParsedSend,
    message_id: &str,
    execution_requested: bool,
) -> Option<Response> {
    let resolved_recipients = match state.store.message_recipients(message_id).await {
        Ok(recipients) => recipients,
        Err(error) => {
            tracing::error!(%error, message_id = %message_id, "resolved recipient lookup failed after acceptance");
            return Some(rpc_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "resolved recipient lookup failed after acceptance",
            ));
        }
    };
    if let Err(error) = state
        .store
        .queue_remote_deliveries(RemoteFanoutRequest {
            local_host: &state.local_host,
            conversation_id: &parsed_send.conversation_id,
            message_id,
            // The sender's own wire id: the recipient node mirrors
            // under it, so a co-hosted echo maps back onto the
            // original local row (no second execution intent).
            origin_message_id: Some(&parsed_send.message_id),
            sender_peer: &parsed_send.sender_peer,
            content: &parsed_send.content,
            execution_requested,
            recipients: &resolved_recipients,
        })
        .await
    {
        tracing::error!(%error, "remote delivery queueing failed");
        return Some(rpc_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote delivery queueing failed",
        ));
    }
    None
}

/// Reads the COMMITTED execution authority of an already-accepted
/// message: the `execution_requested` column fixed at first acceptance
/// is the contract, and the retry payload's flag is not.
///
/// Fails closed: a Replay outcome cannot legitimately lack its
/// committed row, and fan-out never proceeds on the retry payload's
/// untrusted flag.
async fn committed_execution_authority(
    state: &AcceptanceState,
    conversation_id: &str,
    message_id: &str,
) -> Result<bool, Box<Response>> {
    match state.store.list_messages(conversation_id).await {
        Ok(messages) => {
            if let Some(committed) = messages.iter().find(|message| message.id == message_id) {
                return Ok(committed.execution_requested);
            }
            tracing::error!(%message_id, %conversation_id, "committed message row missing on replay");
            Err(Box::new(rpc_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "committed message state unavailable after replay acceptance",
            )))
        }
        Err(error) => {
            tracing::error!(%error, %message_id, "committed message lookup failed");
            Err(Box::new(rpc_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "committed message lookup failed after replay acceptance",
            )))
        }
    }
}

/// Checks that a task exists and belongs to the presenting host's own
/// queue. Missing or unreadable tasks fail CLOSED: the rejection
/// confirms nothing about another host's task ids.
async fn task_owned_by(state: &AcceptanceState, task_id: &str, host: &str) -> bool {
    match state.store.task_get(task_id).await {
        Ok(Some(task)) => task.context_id == host,
        Ok(None) => false,
        Err(error) => {
            tracing::error!(%error, %task_id, "task ownership lookup failed");
            false
        }
    }
}

/// A JSON-RPC authorization rejection: a parseable error envelope so
/// clients can distinguish a permanent authorization failure from a
/// transient transport failure.
fn rpc_authorization_error(message: &str) -> Response {
    rpc_error_response(StatusCode::FORBIDDEN, message)
}

/// A JSON-RPC error envelope with the given HTTP status: the verdict
/// the client must parse is the `error` member — a bare status line
/// breaks every client-side parser and wedges retry loops.
fn rpc_error_response(status: StatusCode, message: &str) -> Response {
    let error = serde_json::json!({
        "jsonrpc": "2.0",
        "id": serde_json::Value::Null,
        "error": {
            "code": -32001,
            "message": message,
        },
    });
    (status, Json(error)).into_response()
}

/// Reads one parameter out of a raw query string.
///
/// Host ids are certificate DNS SANs, which never carry characters
/// that need percent-encoding; a value that WAS encoded would not
/// match the presenting host and is refused fail-closed downstream.
fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == key).then(|| value.to_string())
    })
}
