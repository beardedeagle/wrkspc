//! Unknown-attempt reconciliation and the stuck-claim release op.

use super::{AppState, store_error};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use wrkspc_core::{Reconciliation, UnknownAttemptView};

pub(crate) async fn unknown_attempts(
    State(state): State<AppState>,
) -> Result<Json<Vec<UnknownAttemptView>>, (StatusCode, Json<Value>)> {
    state
        .store
        .unknown_attempts()
        .await
        .map(Json)
        .map_err(store_error)
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
