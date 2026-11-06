//! The claim and ack route handlers.

use super::{AckRequest, DeliveryState, delivery_task, stamped_host};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

/// Handles `POST /delivery-claim` (double-attach enforcement: the
/// exclusive claim taken at poll time, BEFORE any mirroring).
///
/// First claim wins; a same-host re-claim is idempotent; a foreign
/// claim returns the claimant. Stuck claims (dead node) are visible in
/// the feed and operator-released — no TTL, by design.
pub(crate) async fn delivery_claim(
    State(state): State<DeliveryState>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<AckRequest>,
) -> Response {
    let host = match stamped_host(&headers) {
        Ok(host) => host,
        Err((code, message)) => return (code, message).into_response(),
    };
    let task = match delivery_task(&state.store, &body.task_id).await {
        Ok(task) => task,
        Err((code, message)) => return (code, message).into_response(),
    };
    if task.context_id != host {
        tracing::warn!(task_id = %body.task_id, %host, "delivery claim host mismatch");
        return (
            StatusCode::FORBIDDEN,
            "host does not own this delivery queue",
        )
            .into_response();
    }
    match state
        .store
        .claim_delivery(&body.task_id, &host, &body.instance_id)
        .await
    {
        Ok(wrkspc_core::DeliveryClaimOutcome::Claimed) => {
            let payload: serde_json::Value =
                serde_json::from_str(&task.task_json).unwrap_or_default();
            if let Some(conversation_id) =
                payload["metadata"]["delivery"]["conversation_id"].as_str()
            {
                let _ = state
                    .store
                    .append_event(
                        conversation_id,
                        "delivery.claimed",
                        &serde_json::json!({
                            "task_id": body.task_id,
                            "host": host,
                            // Which peer's delivery this is. Every agent on a
                            // host presents the same client certificate, so the
                            // host alone cannot say whose mail was consumed.
                            "recipient_peer": payload["metadata"]["delivery"]
                                ["recipient_peer"]
                                .as_str()
                                .unwrap_or_default(),
                        }),
                    )
                    .await;
            }
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({"claimed": true})),
            )
                .into_response()
        }
        Ok(wrkspc_core::DeliveryClaimOutcome::AlreadyClaimed) => (
            StatusCode::OK,
            axum::Json(serde_json::json!({"claimed": true})),
        )
            .into_response(),
        Ok(wrkspc_core::DeliveryClaimOutcome::ForeignClaimed {
            instance_id,
            claimed_at,
        }) => (
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "claimed": false,
                "claimedByInstance": instance_id,
                "claimedAt": claimed_at,
            })),
        )
            .into_response(),
        Err(wrkspc_core::StoreError::TaskTerminal { .. }) => {
            (StatusCode::CONFLICT, "delivery task already terminal").into_response()
        }
        Err(wrkspc_core::StoreError::NotFound { .. }) => {
            (StatusCode::NOT_FOUND, "unknown delivery task").into_response()
        }
        Err(error) => {
            tracing::error!(%error, task_id = %body.task_id, "claim failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("delivery claim failed: {error}"),
            )
                .into_response()
        }
    }
}

/// Handles `POST /delivery-ack`.
///
/// Flow: (1) the stamped header must be present and carry a SAN;
/// (2) the task must exist and be a delivery; (3) the presenting host
/// must own the task's queue context; (4) the presenting host must hold
/// the exclusive claim (double-attach enforcement); (5) the ack
/// transitions the task terminal (idempotent) and marks the hub-side
/// delivery `delivered`, consuming the claim.
pub(crate) async fn delivery_ack(
    State(state): State<DeliveryState>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<AckRequest>,
) -> Response {
    let host = match stamped_host(&headers) {
        Ok(host) => host,
        Err((code, message)) => return (code, message).into_response(),
    };
    let task = match delivery_task(&state.store, &body.task_id).await {
        Ok(task) => task,
        Err((code, message)) => return (code, message).into_response(),
    };
    let payload: serde_json::Value = match serde_json::from_str(&task.task_json) {
        Ok(value) => value,
        Err(_) => {
            return (StatusCode::CONFLICT, "task payload is not valid JSON").into_response();
        }
    };
    let Some(delivery) = payload
        .get("metadata")
        .and_then(|metadata| metadata.get("delivery"))
    else {
        return (StatusCode::CONFLICT, "not a delivery task").into_response();
    };
    if task.context_id != host {
        tracing::warn!(task_id = %body.task_id, %host, "delivery ack host mismatch");
        return (
            StatusCode::FORBIDDEN,
            "host does not own this delivery queue",
        )
            .into_response();
    }
    if task.terminal {
        // The task already reached a terminal state; a repeated ack of a
        // completed delivery is idempotent-success, anything else is a
        // conflict.
        if task.state == "completed" {
            return (
                StatusCode::OK,
                axum::Json(serde_json::json!({"acked": true})),
            )
                .into_response();
        }
        return (StatusCode::CONFLICT, "delivery task already terminal").into_response();
    }

    // The exclusive claim is the fence: only the host that claimed at
    // poll time may ack (double-attach enforcement — a second node with
    // the same cert can never complete, or even mirror, this delivery).
    match state.store.delivery_claim(&body.task_id).await {
        Ok(Some((claimant_host, claimant_instance, _)))
            if claimant_host == host && claimant_instance == body.instance_id => {}
        Ok(Some(_)) => {
            tracing::warn!(task_id = %body.task_id, %host, "ack without the delivery claim");
            return (
                StatusCode::FORBIDDEN,
                "delivery is claimed by another instance",
            )
                .into_response();
        }
        Ok(None) => {
            return (StatusCode::FORBIDDEN, "delivery is not claimed").into_response();
        }
        Err(error) => {
            tracing::error!(%error, task_id = %body.task_id, "claim lookup failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "delivery ack failed").into_response();
        }
    }

    let (Some(message_id), Some(recipient_peer), Some(conversation_id)) = (
        delivery
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        delivery
            .get("recipient_peer")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        delivery
            .get("conversation_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    ) else {
        return (StatusCode::CONFLICT, "delivery payload is incomplete").into_response();
    };

    // The fence: only after the delivery is durably recorded hub-side do
    // we transition the task. mark_delivered is idempotent, so a crashed
    // retry converges.
    if let Err(error) = state
        .store
        .mark_delivered(&message_id, &recipient_peer)
        .await
    {
        tracing::error!(%error, task_id = %body.task_id, "mark_delivered failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "delivery ack failed").into_response();
    }

    // Keep the stored blob consistent with the row: the wire Task must
    // read terminal-completed after the ack.
    let mut acked = payload;
    acked["status"]["state"] = serde_json::Value::String("TASK_STATE_COMPLETED".to_string());
    acked["metadata"]["delivery"]["acked_at"] =
        serde_json::Value::String(chrono::Utc::now().to_rfc3339());
    if let Err(error) = state
        .store
        .task_update(&body.task_id, "completed", true, &acked.to_string())
        .await
    {
        tracing::error!(%error, task_id = %body.task_id, "task completion failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "delivery ack failed").into_response();
    }
    // The claim is consumed: the terminal task IS the delivery record,
    // and the claims table only ever holds in-flight work.
    if let Err(error) = state.store.release_delivery_claim(&body.task_id).await {
        tracing::warn!(%error, task_id = %body.task_id, "claim cleanup failed");
    }
    let _ = state
        .store
        .append_event(
            &conversation_id,
            "delivery.acked",
            &serde_json::json!({
                "task_id": body.task_id,
                "host": host,
                "recipient_peer": recipient_peer,
            }),
        )
        .await;
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"acked": true})),
    )
        .into_response()
}
