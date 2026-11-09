//! The operator's message and gate controls: post, fences, holds,
//! releases, cancels, and guidance.

use super::{AppState, store_error};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use wrkspc_core::{CancelOutcome, FenceKind};

/// Removes one member from a conversation (kick from the room). The peer
/// stays enrolled in the hub and in other conversations; already-running
/// deliveries finish. The `operator` member is never removable.
pub(crate) async fn remove_member(
    State(state): State<AppState>,
    Path((id, peer)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state
        .store
        .remove_conversation_member(&id, &peer, "operator")
        .await
        .map_err(store_error)?;
    Ok(Json(json!({ "removed": peer })))
}

#[derive(Deserialize)]
pub(crate) struct PostMessageBody {
    content: String,
    /// Target peers for this message (aliases or peer ids). Omitted or
    /// null = every non-operator member, the historical fan-out. A
    /// directed post still lands in the shared transcript; only the
    /// listed peers receive a delivery and take a turn.
    #[serde(default)]
    recipients: Option<Vec<String>>,
}

pub(crate) async fn post_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<PostMessageBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // The operator posts as a participant; every other member receives the
    // delivery with execution requested (the dispatcher drives it).
    // Ejected (booted/banned) members are skipped: the operator's post
    // never fans out to a peer the operator has ejected.
    let members = state
        .store
        .conversation_members(&id)
        .await
        .map_err(store_error)?;
    // Directed posts (the `@peer` composition path): resolve the requested
    // targets against the conversation's members, failing fast on anything
    // that is not a member rather than silently dropping it.
    let directed: Option<Vec<String>> = match body.recipients {
        Some(requested) => {
            let mut resolved = Vec::with_capacity(requested.len());
            for target in requested {
                let peer_id = state
                    .store
                    .resolve_peer_ref(&target)
                    .await
                    .map_err(|error| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({ "error": error.to_string() })),
                        )
                    })?;
                let Some(peer_id) = peer_id else {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("unknown recipient: {target}") })),
                    ));
                };
                if !members.iter().any(|member| member == &peer_id) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(
                            json!({ "error": format!("not a member of this conversation: {peer_id}") }),
                        ),
                    ));
                }
                if !resolved.contains(&peer_id) {
                    resolved.push(peer_id);
                }
            }
            Some(resolved)
        }
        None => None,
    };
    let mut recipients: Vec<wrkspc_core::RecipientRequest> = Vec::new();
    for member in members
        .iter()
        .filter(|member| member.as_str() != "operator")
    {
        if directed
            .as_ref()
            .is_some_and(|targets| !targets.iter().any(|target| target == member))
        {
            continue;
        }
        let ejected = state
            .store
            .peer_status(member)
            .await
            .map_err(store_error)?
            .is_some_and(|status| wrkspc_core::status_is_ejected(&status));
        if ejected {
            tracing::info!(peer = %member, conversation = %id, "ejected member skipped by operator post");
            continue;
        }
        recipients.push(wrkspc_core::RecipientRequest {
            peer_id: member.clone(),
            execution_requested: true,
        });
    }
    let outcome = state
        .store
        .accept_message(
            &id,
            "operator",
            &uuid::Uuid::new_v4().to_string(),
            &body.content,
            true,
            &recipients,
        )
        .await
        .map_err(store_error)?;
    let message_id = match outcome {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => {
            // Hub mode: fan the operator's post out to recipients on
            // remote hosts as pollable delivery tasks.
            if state.queue_remote_deliveries {
                let recipient_peers: Vec<String> = recipients
                    .iter()
                    .map(|recipient| recipient.peer_id.clone())
                    .collect();
                if let Err(error) = state
                    .store
                    .queue_remote_deliveries(wrkspc_core::RemoteFanoutRequest {
                        local_host: wrkspc_core::PILOT_HOST_ID,
                        conversation_id: &id,
                        message_id: &message_id,
                        // Operator posts have no node-side origin.
                        origin_message_id: None,
                        sender_peer: "operator",
                        content: &body.content,
                        execution_requested: true,
                        recipients: &recipient_peers,
                    })
                    .await
                {
                    tracing::error!(%error, "remote delivery queueing failed");
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "delivery queueing failed" })),
                    ));
                }
            }
            message_id
        }
        wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
    };
    Ok(Json(json!({ "message_id": message_id })))
}

#[derive(Deserialize)]
pub(crate) struct FenceBody {
    peer_id: String,
    kind: String,
}

pub(crate) async fn create_fence(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<FenceBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let kind = FenceKind::from_literal(&body.kind).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("unknown fence kind {}", body.kind) })),
        )
    })?;
    let fence_id = state
        .store
        .set_fence(&id, &body.peer_id, kind, "operator")
        .await
        .map_err(store_error)?;
    Ok(Json(json!({ "fence_id": fence_id })))
}

pub(crate) async fn clear_fence(
    State(state): State<AppState>,
    Path(fence_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    state
        .store
        .clear_fence(&fence_id, "operator")
        .await
        .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn hold_delivery(
    State(state): State<AppState>,
    Path((message_id, peer_id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    state
        .store
        .hold_delivery(&message_id, &peer_id, "operator")
        .await
        .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn release_delivery(
    State(state): State<AppState>,
    Path((message_id, peer_id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    state
        .store
        .release_delivery(&message_id, &peer_id, "operator")
        .await
        .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn cancel_delivery(
    State(state): State<AppState>,
    Path((message_id, peer_id)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let outcome = state
        .cancel
        .request_cancel(&message_id, &peer_id, "operator")
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": error })),
            )
        })?;
    let result = match outcome {
        CancelOutcome::IntentCanceled => json!({ "outcome": "intent_canceled" }),
        CancelOutcome::Attempt { attempt_id } => {
            json!({ "outcome": "attempt_interrupt", "attempt_id": attempt_id })
        }
    };
    Ok(Json(result))
}

#[derive(Deserialize)]
pub(crate) struct GuidanceBody {
    text: String,
}

pub(crate) async fn add_guidance(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<GuidanceBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let guidance_id = state
        .store
        .add_guidance(&id, "operator", &body.text)
        .await
        .map_err(store_error)?;
    Ok(Json(json!({ "guidance_id": guidance_id })))
}
