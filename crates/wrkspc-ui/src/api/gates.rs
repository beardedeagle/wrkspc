//! The operator's message and gate controls: post, fences, holds,
//! releases, cancels, and guidance.

use super::{AppState, store_error};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use wrkspc_core::{CancelOutcome, FenceKind};

/// The ceiling on directed-post recipients per message. The request body
/// itself is bounded by the server's extraction limits, but a near-limit
/// array would still fan out to thousands of per-target store resolves
/// and deliveries; the cap keeps one post from amplifying into unbounded
/// work (CodeQL `rust/uncontrolled-allocation-size`).
const MAX_DIRECTED_RECIPIENTS: usize = 32;

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
            if requested.len() > MAX_DIRECTED_RECIPIENTS {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!(
                        "too many recipients: {} (max {MAX_DIRECTED_RECIPIENTS})",
                        requested.len()
                    ) })),
                ));
            }
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
    let outcome = if state.hub_connected && !state.hub_role {
        // Team mode: the operator post mirrors locally WITHOUT local
        // execution intents — execution waits for hub acceptance (the
        // fences) and this node's claim of the echoed delivery. The
        // outbound sync feeds the hub, which fans out authoritatively.
        state
            .store
            .accept_message_mirror_only(
                &id,
                "operator",
                &uuid::Uuid::new_v4().to_string(),
                &body.content,
                true,
                &recipients,
            )
            .await
            .map_err(store_error)?
    } else {
        // Standalone node, or the hub itself: direct acceptance with
        // local execution intents (the hub never executes — its
        // fan-out below queues remote deliveries instead).
        state
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
            .map_err(store_error)?
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use wrkspc_core::Store;

    struct NoCancel;

    impl crate::api::CancelRequester for NoCancel {
        fn request_cancel(
            &self,
            _message_id: &str,
            _recipient_id: &str,
            _set_by: &str,
        ) -> Pin<Box<dyn Future<Output = Result<wrkspc_core::CancelOutcome, String>> + Send + '_>>
        {
            Box::pin(async { Ok(wrkspc_core::CancelOutcome::IntentCanceled) })
        }
    }

    async fn seeded() -> (Store, String) {
        let store = Store::open_in_memory("gates-defer").await.unwrap();
        store
            .ensure_peer("operator", "operator", "human")
            .await
            .unwrap();
        store
            .ensure_peer("codex", "codex", "harness")
            .await
            .unwrap();
        let conversation = store
            .create_conversation(&["operator", "codex"])
            .await
            .unwrap();
        (store, conversation)
    }

    /// A hub-connected node's operator post mirrors the message WITHOUT
    /// a local execution intent: execution waits for hub acceptance and
    /// this node's claim of the echoed delivery (where fences gate).
    #[tokio::test]
    async fn hub_connected_operator_post_defers_execution() {
        let (store, conversation) = seeded().await;
        let state = AppState::new(store.clone(), Arc::new(NoCancel), false, false, true, None);
        let Json(response) = post_message(
            State(state),
            Path(conversation.clone()),
            Json(PostMessageBody {
                content: "go".to_string(),
                recipients: None,
            }),
        )
        .await
        .unwrap();
        let message_id = response["message_id"].as_str().unwrap().to_string();
        assert_eq!(
            store.message_recipients(&message_id).await.unwrap(),
            vec!["codex".to_string()],
            "the message still commits with its recipients"
        );
        assert!(
            store.claim_intent(&message_id, "codex").await.is_err(),
            "no local intent: the dispatcher has nothing to claim before hub acceptance"
        );
    }

    /// Standalone nodes keep direct acceptance: the operator post still
    /// creates the claimable local intent.
    #[tokio::test]
    async fn standalone_operator_post_still_executes_directly() {
        let (store, conversation) = seeded().await;
        let state = AppState::new(store.clone(), Arc::new(NoCancel), false, false, false, None);
        let Json(response) = post_message(
            State(state),
            Path(conversation.clone()),
            Json(PostMessageBody {
                content: "go".to_string(),
                recipients: None,
            }),
        )
        .await
        .unwrap();
        let message_id = response["message_id"].as_str().unwrap().to_string();
        assert!(
            store.claim_intent(&message_id, "codex").await.is_ok(),
            "standalone posts keep their local intent"
        );
    }
}
