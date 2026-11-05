//! Hub-side discovery routes (HLD rev0.9): presence ingestion, roster
//! reads, and per-peer agent cards — all on the stamped mTLS surface.
//!
//! Identity rule matches acceptance: the presenting host comes from the
//! `x-wrkspc-peer` stamp written at the accept loop, never from the
//! request body. First presence binds the peer to that host; later
//! cross-host reports are refused.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;
use wrkspc_core::{PresenceOutcome, Store};

/// Shared state for the discovery routes.
#[derive(Clone)]
pub struct PresenceState {
    store: Store,
    base_url: String,
    /// Whether the served transport is the certificate-stamped TLS
    /// surface — per-peer cards mirror the hub card's mTLS declaration.
    mtls: bool,
}

/// The `POST /presence` body: a node report for one peer.
#[derive(Debug, Deserialize)]
pub struct PresenceBody {
    /// The peer id.
    #[serde(rename = "peerId")]
    pub peer_id: String,
    /// The display name (applied at first enrolment only).
    pub name: String,
    /// The peer class (`harness`, `human`, `a2a_native`).
    pub class: String,
    /// The last reported live state.
    pub state: String,
}

impl PresenceState {
    /// Builds the state.
    ///
    /// # Arguments
    ///
    /// * `store` — the hub's transactional store.
    /// * `base_url` — the hub's card base URL (for per-peer cards).
    /// * `mtls` — whether the hub's TLS surface serves the cards.
    pub fn new(store: Store, base_url: String, mtls: bool) -> Self {
        Self {
            store,
            base_url,
            mtls,
        }
    }
}

/// `POST /presence` — upserts a peer's live state under the presenting
/// host's identity.
pub async fn presence_upsert(
    State(state): State<PresenceState>,
    headers: HeaderMap,
    Json(body): Json<PresenceBody>,
) -> impl IntoResponse {
    let host = match crate::delivery::stamped_host(&headers) {
        Ok(host) => host,
        Err(response) => return response.into_response(),
    };
    if !wrkspc_core::is_presence_state(&body.state) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "unknown presence state" })),
        )
            .into_response();
    }
    match state
        .store
        .upsert_roster_presence(&body.peer_id, &body.name, &body.class, &host, &body.state)
        .await
    {
        Ok(PresenceOutcome::Updated) => {
            (StatusCode::OK, Json(serde_json::json!({ "presence": true }))).into_response()
        }
        Ok(PresenceOutcome::HostConflict { owner_host }) => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "peer bound to another host", "ownerHost": owner_host })),
        )
            .into_response(),
        Ok(PresenceOutcome::Ejected { status }) => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": format!("peer is {status}; the operator controls its status") })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, peer = %body.peer_id, "presence upsert failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "presence upsert failed" })),
            )
                .into_response()
        }
    }
}

/// `GET /roster` — the full cohort, for node mirroring.
pub async fn roster_get(
    State(state): State<PresenceState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(response) = crate::delivery::stamped_host(&headers) {
        return response.into_response();
    }
    match state.store.roster().await {
        Ok(entries) => (StatusCode::OK, Json(entries)).into_response(),
        Err(error) => {
            tracing::error!(%error, "roster read failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "roster read failed" })),
            )
                .into_response()
        }
    }
}

/// `GET /peers/{peer}/agent-card.json` — the per-peer discovery card.
pub async fn peer_card(
    State(state): State<PresenceState>,
    headers: HeaderMap,
    Path(peer_id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) = crate::delivery::stamped_host(&headers) {
        return response.into_response();
    }
    match state.store.roster_entry(&peer_id).await {
        Ok(Some(entry)) => (
            StatusCode::OK,
            Json(wrkspc_a2a::build_peer_card(
                &state.base_url,
                &entry,
                state.mtls,
            )),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "peer not in roster" })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, %peer_id, "roster entry read failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "roster entry read failed" })),
            )
                .into_response()
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::header::CONTENT_TYPE;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    /// The discovery routes alone (the stamped identity is injected via
    /// the `x-wrkspc-peer` header, matching the accept loop's stamp).
    fn router_under_test(state: PresenceState) -> axum::Router {
        axum::Router::new()
            .route("/presence", post(presence_upsert).with_state(state.clone()))
            .route("/roster", get(roster_get).with_state(state.clone()))
            .route(
                "/peers/{peer}/agent-card.json",
                get(peer_card).with_state(state),
            )
    }

    fn identity(host: &str) -> String {
        format!("CN=test,SAN={host}")
    }

    async fn post_presence(
        router: &axum::Router,
        host: Option<&str>,
        peer: &str,
        state: &str,
    ) -> axum::http::StatusCode {
        let mut builder = axum::http::Request::builder()
            .method("POST")
            .uri("/presence")
            .header(CONTENT_TYPE, "application/json");
        if let Some(host) = host {
            builder = builder.header("x-wrkspc-peer", identity(host));
        }
        router
            .clone()
            .oneshot(
                builder
                    .body(Body::from(
                        serde_json::json!({
                            "peerId": peer,
                            "name": peer,
                            "class": "harness",
                            "state": state,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn first_presence_enrols_under_the_presenting_host() {
        let store = Store::open_in_memory("presence-route-first").await.unwrap();
        let router = router_under_test(PresenceState::new(
            store.clone(),
            "https://hub.test".to_string(),
            true,
        ));
        assert_eq!(
            post_presence(&router, Some("node-test-1"), "codex", "connected").await,
            StatusCode::OK
        );
        let entry = store.roster_entry("codex").await.unwrap().unwrap();
        assert_eq!(entry.host_id, "node-test-1");
        assert_eq!(entry.state, "connected");
    }

    #[tokio::test]
    async fn cross_host_presence_is_forbidden_and_state_updates_flow() {
        let store = Store::open_in_memory("presence-route-conflict")
            .await
            .unwrap();
        let router = router_under_test(PresenceState::new(
            store.clone(),
            "https://hub.test".to_string(),
            true,
        ));
        assert_eq!(
            post_presence(&router, Some("node-test-1"), "codex", "connected").await,
            StatusCode::OK
        );
        // Same host, new state: accepted.
        assert_eq!(
            post_presence(&router, Some("node-test-1"), "codex", "disconnected").await,
            StatusCode::OK
        );
        assert_eq!(
            store.roster_entry("codex").await.unwrap().unwrap().state,
            "disconnected"
        );
        // A different host claiming the peer: refused, state untouched.
        assert_eq!(
            post_presence(&router, Some("node-other"), "codex", "connected").await,
            StatusCode::FORBIDDEN
        );
        let entry = store.roster_entry("codex").await.unwrap().unwrap();
        assert_eq!(entry.host_id, "node-test-1");
        assert_eq!(entry.state, "disconnected");
    }

    #[tokio::test]
    async fn invalid_state_and_missing_stamp_are_rejected() {
        let store = Store::open_in_memory("presence-route-rejects")
            .await
            .unwrap();
        let router = router_under_test(PresenceState::new(
            store.clone(),
            "https://hub.test".to_string(),
            true,
        ));
        assert_eq!(
            post_presence(&router, Some("node-test-1"), "codex", "busy").await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            post_presence(&router, None, "codex", "connected").await,
            StatusCode::UNAUTHORIZED
        );
        assert!(store.roster_entry("codex").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn roster_get_requires_the_stamp_and_lists_entries() {
        let store = Store::open_in_memory("presence-route-roster")
            .await
            .unwrap();
        store
            .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
            .await
            .unwrap();
        let router = router_under_test(PresenceState::new(
            store.clone(),
            "https://hub.test".to_string(),
            true,
        ));

        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/roster")
                    .header("x-wrkspc-peer", identity("node-test-1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let entries: Vec<wrkspc_core::RosterEntry> = serde_json::from_slice(&body).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].peer_id, "codex");

        // Unstamped reads are refused (mTLS surface invariant).
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/roster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn peer_card_serves_roster_members_only() {
        let store = Store::open_in_memory("presence-route-card").await.unwrap();
        store
            .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
            .await
            .unwrap();
        let router = router_under_test(PresenceState::new(
            store.clone(),
            "https://hub.test".to_string(),
            true,
        ));

        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/peers/codex/agent-card.json")
                    .header("x-wrkspc-peer", identity("node-test-1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let card: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(card["name"], "codex");
        assert!(card["skills"].is_array());

        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/peers/unknown/agent-card.json")
                    .header("x-wrkspc-peer", identity("node-test-1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
