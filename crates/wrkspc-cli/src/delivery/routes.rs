//! The claim and ack route handlers.

use super::{AckRequest, DeliveryReport, DeliveryState, delivery_task, stamped_host};
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
        Err(wrkspc_core::StoreError::BlockedByFence { kinds, .. }) => (
            StatusCode::LOCKED,
            axum::Json(serde_json::json!({
                "claimed": false,
                "blocked": kinds,
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
/// the exclusive claim (double-attach enforcement); (5) the ack marks
/// the hub-side delivery `delivered` (mailbox handoff) and transitions
/// the task to the NON-terminal `working` state, consuming the claim.
///
/// An ack is a RECEIPT, not an execution outcome (H-10): an
/// executable delivery transitions to the non-terminal `working` state
/// and becomes terminal only when the node reports the attempt's
/// result through [`delivery_report`]; a mail-only delivery (no
/// execution requested) completes at the ack — receipt is its whole
/// lifecycle. A repeated ack from the owning host is idempotent-
/// success (crash retry).
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
    // Whether the delivery requests execution. Mail-only deliveries
    // (execution_requested=false) create no attempt by design, so for
    // them receipt IS completion; executable deliveries stay working
    // until the node reports the attempt outcome.
    let execution_requested = delivery
        .get("execution_requested")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if task.state == "working" {
        // Idempotent re-ack after a crash between the ack write and the
        // node's local processed marker.
        return (
            StatusCode::OK,
            axum::Json(serde_json::json!({"acked": true})),
        )
            .into_response();
    }
    if task.terminal {
        // A repeated ack of a completed mail-only delivery is
        // idempotent-success; anything else is a conflict.
        if !execution_requested && task.state == "completed" {
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

    // The transition: an executable delivery becomes WORKING (the
    // node has the message and will attempt it — terminal states are
    // reserved for the outcome report), a mail-only delivery becomes
    // COMPLETED (receipt is its whole lifecycle). Either way the task
    // must never read completed before the delivery's own contract
    // allows it (H-10).
    let mut acked = payload;
    acked["metadata"]["delivery"]["acked_at"] =
        serde_json::Value::String(chrono::Utc::now().to_rfc3339());
    if execution_requested {
        acked["status"]["state"] = serde_json::Value::String("TASK_STATE_WORKING".to_string());
        if let Err(error) = state
            .store
            .task_update(&body.task_id, "working", false, &acked.to_string())
            .await
        {
            tracing::error!(%error, task_id = %body.task_id, "task working-transition failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "delivery ack failed").into_response();
        }
    } else {
        acked["status"]["state"] = serde_json::Value::String("TASK_STATE_COMPLETED".to_string());
        if let Err(error) = state
            .store
            .task_update(&body.task_id, "completed", true, &acked.to_string())
            .await
        {
            tracing::error!(%error, task_id = %body.task_id, "task completion failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "delivery ack failed").into_response();
        }
    }
    // The claim is consumed: the working task IS the delivery record,
    // and the claims table only ever holds in-flight handoffs.
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

/// Handles `GET /reconciliation-decisions`: a node polls the
/// operator decisions queued on its mirrored unknown attempts and
/// applies them locally (the resulting terminal state then reports
/// back through [`delivery_report`], consuming the mirror).
pub(crate) async fn reconciliation_decisions(
    State(state): State<DeliveryState>,
    headers: HeaderMap,
) -> Response {
    let host = match stamped_host(&headers) {
        Ok(host) => host,
        Err((code, message)) => return (code, message).into_response(),
    };
    match state.store.hub_pending_reconciliations(&host).await {
        Ok(decisions) => axum::Json(decisions).into_response(),
        Err(error) => {
            tracing::error!(%error, %host, "reconciliation decisions fetch failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "decision fetch failed").into_response()
        }
    }
}

/// Handles `POST /delivery-report` (the H-10 outcome leg): the node
/// reports the terminal result of an attempt for a delivery it
/// mirrored, so the hub's task learns the execution outcome.
///
/// Flow: (1) stamped host, (2) the task must exist and be a delivery,
/// (3) the presenting host must own the task's queue context,
/// (4) the reported state must be a terminal attempt state. The task
/// transitions to the reported terminal state with its record
/// preserved; a repeated report of the SAME state is idempotent.
pub(crate) async fn delivery_report(
    State(state): State<DeliveryState>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<DeliveryReport>,
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
        tracing::warn!(task_id = %body.task_id, %host, "delivery report host mismatch");
        return (
            StatusCode::FORBIDDEN,
            "host does not own this delivery queue",
        )
            .into_response();
    }
    if !matches!(
        body.state.as_str(),
        "completed" | "failed" | "canceled" | "unknown"
    ) {
        return (
            StatusCode::BAD_REQUEST,
            "state must be completed|failed|canceled|unknown",
        )
            .into_response();
    }
    let (Some(recipient_peer), Some(conversation_id)) = (
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

    if body.state == "unknown" {
        // An outcome the node could not determine: mirror it on the
        // hub WITHOUT a task transition (the task stays pending) so
        // the operator sees and reconciles it from the hub UI; the
        // decision routes back through the node's poll loop.
        match state
            .store
            .record_hub_unknown(
                &body.attempt_id,
                &body.task_id,
                &host,
                &recipient_peer,
                &conversation_id,
                &body.stop_reason,
            )
            .await
        {
            Ok(()) => return StatusCode::NO_CONTENT.into_response(),
            Err(error) => {
                tracing::error!(%error, attempt_id = %body.attempt_id, "unknown mirror failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unknown attempt mirror failed",
                )
                    .into_response();
            }
        }
    }

    // Keep the stored blob consistent with the row and preserve the
    // record (H-13): the envelope, metadata, and history survive; only
    // the status transitions.
    let mut reported = payload;
    reported["status"]["state"] = serde_json::Value::String(match body.state.as_str() {
        "completed" => "TASK_STATE_COMPLETED".to_string(),
        "failed" => "TASK_STATE_FAILED".to_string(),
        _ => "TASK_STATE_CANCELED".to_string(),
    });
    reported["metadata"]["attempt"] = serde_json::json!({
        "attempt_id": body.attempt_id,
        "state": body.state,
        "recorded_at": body.recorded_at,
    });
    match state
        .store
        .task_update(&body.task_id, &body.state, true, &reported.to_string())
        .await
    {
        Ok(_) => {}
        Err(wrkspc_core::StoreError::TaskTerminal { state: current, .. }) => {
            // A conflicting outcome for a terminal task: the first
            // terminal state stands (records are append-only), and the
            // reporter drops the report.
            tracing::warn!(
                task_id = %body.task_id,
                current_state = %current,
                reported = %body.state,
                "conflicting outcome report ignored"
            );
            return (
                StatusCode::CONFLICT,
                "delivery task already terminal with a different state",
            )
                .into_response();
        }
        Err(error) => {
            tracing::error!(%error, task_id = %body.task_id, "outcome report failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "delivery report failed").into_response();
        }
    }
    // A final terminal state completes the reconciliation loop: the
    // hub's mirrored unknown row (if any) is consumed.
    let _ = state.store.consume_hub_unknown(&body.attempt_id).await;
    // Guidance consumption ack (hub→node propagation loop): the node
    // reports which authoritative rows rode a successful prompt; the
    // hub marks them delivered. Idempotent — unknown ids (node-local
    // guidance) are no-ops.
    if !body.guidance_delivered.is_empty() {
        let ids: Vec<&str> = body.guidance_delivered.iter().map(String::as_str).collect();
        if let Err(error) = state.store.mark_guidance_delivered(&ids).await {
            tracing::warn!(%error, "guidance consumption ack failed; rows stay pending and re-ride");
        }
    }
    let _ = state
        .store
        .append_event(
            &conversation_id,
            "delivery.attempt",
            &serde_json::json!({
                "task_id": body.task_id,
                "attempt_id": body.attempt_id,
                "state": body.state,
                "recorded_at": body.recorded_at,
                "recipient_peer": recipient_peer,
                "instance_id": body.instance_id,
            }),
        )
        .await;
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"reported": true})),
    )
        .into_response()
}
