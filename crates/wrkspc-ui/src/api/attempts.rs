//! Unknown-attempt reconciliation and the stuck-claim release op.

use super::{AppState, store_error};
use crate::redact::redact;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use wrkspc_core::{Reconciliation, UnknownAttemptView};

pub(crate) async fn unknown_attempts(
    State(state): State<AppState>,
) -> Result<Json<Vec<UnknownAttemptView>>, (StatusCode, Json<Value>)> {
    let mut attempts = state.store.unknown_attempts().await.map_err(store_error)?;
    // The dispatcher writes adapter error text into `stop_reason`; it
    // reaches the browser exactly as stored unless it goes through the
    // same display redaction as every other rendered value.
    for attempt in &mut attempts {
        if let Some(reason) = attempt.stop_reason.take() {
            attempt.stop_reason = Some(redact(&reason));
        }
    }
    Ok(Json(attempts))
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

        let state = AppState::new(store, Arc::new(NoCancel), false, true, None);
        let Json(views) = unknown_attempts(State(state)).await.unwrap();
        assert_eq!(views.len(), 1);
        let reason = views[0].stop_reason.as_deref().unwrap();
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
