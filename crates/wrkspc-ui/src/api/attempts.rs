//! Unknown-attempt reconciliation and the stuck-claim release op.

use super::{AppState, store_error};
use crate::redact::redact;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use wrkspc_core::Reconciliation;

pub(crate) async fn unknown_attempts(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Local unknowns (this store owns the attempts) plus the hub's
    // mirrored unknowns (node-side attempts reported upward): one list,
    // one reconciliation surface. The dispatcher writes adapter error
    // text into `stop_reason`; it reaches the browser exactly as
    // stored unless it goes through the same display redaction as
    // every other rendered value.
    let mut merged: Vec<Value> = Vec::new();
    let mut attempts = state.store.unknown_attempts().await.map_err(store_error)?;
    for attempt in &mut attempts {
        let mut value = serde_json::to_value(&*attempt).map_err(store_error_json)?;
        if let Some(reason) = attempt.stop_reason.take() {
            value["stop_reason"] = Value::String(redact(&reason));
        }
        merged.push(value);
    }
    for mirrored in state.store.hub_unknowns().await.map_err(store_error)? {
        merged.push(json!({
            "attempt_id": mirrored.attempt_id,
            "recipient_id": mirrored.recipient_id,
            "conversation_id": mirrored.conversation_id,
            "host_id": mirrored.host_id,
            "stop_reason": redact(&mirrored.stop_reason),
            "decision": mirrored.decision,
        }));
    }
    Ok(Json(Value::Array(merged)))
}

/// Maps a serde failure on a store view to a 500 JSON error.
fn store_error_json(error: serde_json::Error) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": format!("view serialization failed: {error}") })),
    )
}

#[derive(Deserialize)]
pub(crate) struct ReconcileBody {
    decision: String,
    #[serde(default)]
    state: Option<String>,
}

pub(crate) async fn reconcile_attempt(
    State(state): State<AppState>,
    Path(attempt_id): Path<String>,
    Json(body): Json<ReconcileBody>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    let decision = match body.decision.as_str() {
        "retry" => Reconciliation::Retry,
        "close" => {
            let state = match body.state.as_deref() {
                Some("failed") => wrkspc_core::AttemptState::Failed,
                Some("completed") => wrkspc_core::AttemptState::Completed,
                Some("canceled") => wrkspc_core::AttemptState::Canceled,
                _ => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": "close requires state failed|completed|canceled" })),
                    ));
                }
            };
            Reconciliation::Close { state }
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "decision must be retry|close" })),
            ));
        }
    };
    // A hub-mirrored attempt (a node-side unknown reported upward):
    // queue the decision for the owning node's poll loop instead of
    // reconciling a record this store does not own.
    let mirrored = state
        .store
        .hub_unknowns()
        .await
        .map_err(store_error)?
        .into_iter()
        .any(|view| view.attempt_id == attempt_id);
    if mirrored {
        let decision_string = match decision {
            Reconciliation::Retry => "retry".to_string(),
            Reconciliation::Close { state } => format!("close:{}", state.as_str()),
        };
        let updated = state
            .store
            .set_hub_unknown_decision(&attempt_id, &decision_string)
            .await
            .map_err(store_error)?;
        if !updated {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unknown attempt" })),
            ));
        }
        return Ok(StatusCode::NO_CONTENT);
    }
    state
        .store
        .reconcile_unknown(&attempt_id, decision)
        .await
        .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::AppState;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use wrkspc_core::{AcceptOutcome, CancelOutcome, RecipientRequest, Store};

    struct NoCancel;

    impl crate::api::CancelRequester for NoCancel {
        fn request_cancel(
            &self,
            _message_id: &str,
            _recipient_id: &str,
            _set_by: &str,
        ) -> Pin<Box<dyn Future<Output = Result<CancelOutcome, String>> + Send + '_>> {
            Box::pin(async { Ok(CancelOutcome::IntentCanceled) })
        }
    }

    /// Unknown attempts leave the API with `stop_reason` redacted —
    /// the dispatcher writes raw adapter error text there, and the
    /// renderer displays it verbatim.
    #[tokio::test]
    async fn unknown_attempt_stop_reason_is_redacted_on_the_way_out() {
        let store = Store::open_in_memory("attempt-redact").await.unwrap();
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
        let AcceptOutcome::Accepted { message_id } = store
            .accept_message(
                &conversation,
                "operator",
                "k-1",
                "go",
                true,
                &[RecipientRequest {
                    peer_id: "codex".to_string(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap()
        else {
            panic!("expected Accepted");
        };
        let attempt = store.claim_intent(&message_id, "codex").await.unwrap();
        // An adapter error shape that carries a credential.
        store
            .mark_unknown(
                &attempt,
                r#"spawn failed: env FOO="sk-abcdef1234567890" was set"#,
            )
            .await
            .unwrap();

        let state = AppState::new(store, Arc::new(NoCancel), false, true, false, None);
        let Json(views) = unknown_attempts(State(state)).await.unwrap();
        let views = views.as_array().expect("array response");
        assert_eq!(views.len(), 1);
        let reason = views[0].get("stop_reason").and_then(Value::as_str).unwrap();
        assert!(
            !reason.contains("abcdef1234567890"),
            "credential must be masked: {reason:?}"
        );
        assert!(
            reason.contains("spawn failed"),
            "the rest of the text survives: {reason:?}"
        );
    }
}
