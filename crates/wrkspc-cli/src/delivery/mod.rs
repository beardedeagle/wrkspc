//! Hub delivery-ack control surface (D-6 A2 contract, fence-ack leg).
//!
//! A2A v1.0 gives the CLIENT no task-update method — tasks progress via
//! the agent's own updates — so the node's acknowledgment of a durably
//! recorded delivery rides this small hub-local route instead. It is
//! reachable only over the mTLS-stamped connection, and the ack is
//! accepted only from the host that OWNS the delivery queue: the same
//! attribution discipline as acceptance, so a cert can never ack
//! another host's deliveries.

use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
use wrkspc_core::Store;

pub(crate) mod routes;
#[cfg(test)]
mod tests;

pub(crate) use routes::{delivery_ack, delivery_claim, delivery_report, reconciliation_decisions};

/// Shared state for the delivery-ack route.
#[derive(Clone)]
pub(crate) struct DeliveryState {
    store: Store,
}

impl DeliveryState {
    /// Builds the route state.
    ///
    /// `store` is the hub's shared store.
    pub(crate) fn new(store: Store) -> Self {
        Self { store }
    }
}

/// The claim/ack request body.
#[derive(Deserialize)]
pub(crate) struct AckRequest {
    /// The hub delivery task id being claimed or acknowledged.
    #[serde(rename = "taskId")]
    task_id: String,
    /// The node's stable local identity (claim exclusivity is
    /// per-instance: a copied cert on a second machine loses the claim).
    #[serde(rename = "instanceId")]
    instance_id: String,
}

/// The attempt-outcome report body (H-10): the node reports a terminal
/// attempt state for a hub-originated delivery it mirrored, so the
/// hub's task learns the execution result.
#[derive(Deserialize)]
pub(crate) struct DeliveryReport {
    /// The hub delivery task the outcome addresses.
    #[serde(rename = "taskId")]
    task_id: String,
    /// The reporting node's stable local identity.
    #[serde(rename = "instanceId")]
    instance_id: String,
    /// The terminal attempt.
    #[serde(rename = "attemptId")]
    attempt_id: String,
    /// The terminal state: `completed`, `failed`, `canceled`, or
    /// `unknown` (a node-side outcome that could not be determined —
    /// mirrored on the hub for operator reconciliation, never a task
    /// transition).
    state: String,
    /// When the attempt reached the terminal state (RFC 3339).
    #[serde(rename = "recordedAt")]
    recorded_at: String,
    /// The dispatcher-recorded reason for an unknown outcome. Optional.
    #[serde(default, rename = "stopReason")]
    stop_reason: String,
    /// Guidance ids the node consumed (rode a successful prompt): the
    /// hub marks its authoritative rows delivered. Optional, default
    /// empty.
    #[serde(default, rename = "guidanceDelivered")]
    guidance_delivered: Vec<String>,
}

/// Extracts the presenting host from the stamped identity header.
///
/// `headers` is the request headers.
///
/// # Returns
///
/// The host id (certificate SAN), or a `401` response.
pub(crate) fn stamped_host(headers: &HeaderMap) -> Result<String, (StatusCode, &'static str)> {
    let Some(identity) = headers
        .get("x-wrkspc-peer")
        .and_then(|value| value.to_str().ok())
    else {
        return Err((StatusCode::UNAUTHORIZED, "missing stamped peer identity"));
    };
    let Some(host) = crate::acceptance::host_from_identity(identity) else {
        return Err((StatusCode::UNAUTHORIZED, "peer identity has no SAN"));
    };
    Ok(host)
}

/// Loads a delivery task row, enforcing the shared shape checks
/// (existence + `metadata.delivery.kind == "delivery"`).
///
/// `store` is the hub store; `task_id` is the task to load.
///
/// # Returns
///
/// The task row, or a `404`/`409`/`500` response.
pub(crate) async fn delivery_task(
    store: &Store,
    task_id: &str,
) -> Result<wrkspc_core::TaskRow, (StatusCode, &'static str)> {
    let task = match store.task_get(task_id).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            return Err((StatusCode::NOT_FOUND, "unknown delivery task"));
        }
        Err(error) => {
            tracing::error!(%error, "task lookup failed");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "task lookup failed"));
        }
    };
    let payload: serde_json::Value = match serde_json::from_str(&task.task_json) {
        Ok(value) => value,
        Err(_) => {
            return Err((StatusCode::CONFLICT, "task payload is not valid JSON"));
        }
    };
    let Some(delivery) = payload
        .get("metadata")
        .and_then(|metadata| metadata.get("delivery"))
    else {
        return Err((StatusCode::CONFLICT, "not a delivery task"));
    };
    if delivery.get("kind").and_then(|k| k.as_str()) != Some("delivery") {
        return Err((StatusCode::CONFLICT, "not a delivery task"));
    }
    Ok(task)
}
