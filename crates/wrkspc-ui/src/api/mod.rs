//! The operator API: read views + moderation controls over the committed
//! state, plus the cancel seam into the dispatcher.
//!
//! Every mutation routes through the moderation ops so fences, held
//! deliveries, and events stay on the single committed path. Content and
//! event payloads are redacted on the way out (display path).

use attempts::reconcile_attempt;
use attempts::unknown_attempts;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use conversations::{
    conversation, conversation_events, conversation_fences, conversation_messages,
    create_conversation, delete_conversation, list_conversations,
};
use gates::{
    add_guidance, cancel_delivery, clear_fence, create_fence, hold_delivery, post_message,
    release_delivery, remove_member,
};
use peers::{forget_peer, list_peers, moderation_log, patch_peer};
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use wrkspc_core::{CancelOutcome, Store, StoreError};
use wrkspc_dispatcher::DispatcherHandle;

pub(crate) mod attempts;
pub(crate) mod conversations;
pub(crate) mod gates;
pub(crate) mod peers;

/// Cancellation seam into the dispatcher (the live session interrupt).
pub trait CancelRequester: Send + Sync + 'static {
    /// Requests cancellation of a message's execution for a recipient.
    fn request_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        set_by: &str,
    ) -> Pin<Box<dyn Future<Output = Result<CancelOutcome, String>> + Send + '_>>;
}

impl CancelRequester for DispatcherHandle {
    fn request_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        set_by: &str,
    ) -> Pin<Box<dyn Future<Output = Result<CancelOutcome, String>> + Send + '_>> {
        let handle = self.clone();
        let message_id = message_id.to_string();
        let recipient_id = recipient_id.to_string();
        let set_by = set_by.to_string();
        Box::pin(async move {
            handle
                .request_cancel(&message_id, &recipient_id, &set_by)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    store: Store,
    cancel: Arc<dyn CancelRequester>,
    queue_remote_deliveries: bool,
    hub_role: bool,
    deny: Option<Arc<std::sync::RwLock<std::collections::HashSet<String>>>>,
}

impl AppState {
    /// Builds the state.
    ///
    /// # Arguments
    ///
    /// * `store` — the transactional store.
    /// * `cancel` — the cancel seam (the dispatcher handle in production).
    /// * `queue_remote_deliveries` — hub mode: operator posts queue
    ///   delivery tasks for remote-host recipients.
    /// * `hub_role` — hub mode: the operator surface that owns the
    ///   shared store (conversation creation); the node role returns
    ///   404 for it (a node-created conversation would never reach the
    ///   hub).
    /// * `deny` — the hub's live certificate-serial deny set, when this
    ///   surface is embedded on the mTLS listener.
    pub fn new(
        store: Store,
        cancel: Arc<dyn CancelRequester>,
        queue_remote_deliveries: bool,
        hub_role: bool,
        deny: Option<Arc<std::sync::RwLock<std::collections::HashSet<String>>>>,
    ) -> Self {
        Self {
            store,
            cancel,
            queue_remote_deliveries,
            hub_role,
            deny,
        }
    }
}

/// Maps store errors to HTTP responses.
fn store_error(error: StoreError) -> (StatusCode, Json<Value>) {
    use StoreError::*;
    let status = match error {
        NotFound { .. } => StatusCode::NOT_FOUND,
        TerminalState { .. }
        | IntentTerminal { .. }
        | BlockedByFence { .. }
        | AlreadyExists { .. } => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": error.to_string() })))
}

/// Builds the API router (no auth layer; the server wraps it).
pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/api/health", axum::routing::get(health))
        .route(
            "/api/conversations",
            axum::routing::get(list_conversations).post(create_conversation),
        )
        .route("/api/peers", axum::routing::get(list_peers))
        .route(
            "/api/peers/{id}",
            axum::routing::patch(patch_peer).delete(forget_peer),
        )
        .route("/api/moderation", axum::routing::get(moderation_log))
        .route(
            "/api/conversations/{id}",
            axum::routing::get(conversation).delete(delete_conversation),
        )
        .route(
            "/api/conversations/{id}/events",
            axum::routing::get(conversation_events),
        )
        .route(
            "/api/conversations/{id}/messages",
            axum::routing::get(conversation_messages).post(post_message),
        )
        .route(
            "/api/conversations/{id}/members/{peer}",
            axum::routing::delete(remove_member),
        )
        .route(
            "/api/conversations/{id}/fences",
            axum::routing::get(conversation_fences).post(create_fence),
        )
        .route("/api/fences/{fence_id}", axum::routing::delete(clear_fence))
        .route(
            "/api/messages/{message_id}/deliveries/{peer_id}/hold",
            axum::routing::post(hold_delivery),
        )
        .route(
            "/api/messages/{message_id}/deliveries/{peer_id}/release",
            axum::routing::post(release_delivery),
        )
        .route(
            "/api/messages/{message_id}/deliveries/{peer_id}/cancel",
            axum::routing::post(cancel_delivery),
        )
        .route(
            "/api/conversations/{id}/guidance",
            axum::routing::post(add_guidance),
        )
        .route(
            "/api/attempts/unknown",
            axum::routing::get(unknown_attempts),
        )
        .route(
            "/api/attempts/{attempt_id}/reconcile",
            axum::routing::post(reconcile_attempt),
        )
        .route(
            "/api/deliveries/{task_id}/release-claim",
            axum::routing::post(release_delivery_claim),
        )
        .with_state(state)
}

/// Releases a stuck delivery claim (operator moderation op — claims have
/// no TTL by design, so a dead node's claim blocks the delivery until
/// the operator releases it). Idempotent.
async fn release_delivery_claim(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    state
        .store
        .release_delivery_claim(&task_id)
        .await
        .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
