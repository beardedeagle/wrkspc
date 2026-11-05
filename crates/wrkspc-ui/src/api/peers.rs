//! Peer moderation surface: roster list, alias/status patch, forget,
//! and the audit log.

use super::{AppState, store_error};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde_json::{Value, json};

/// `GET /api/peers` — the roster view: hub-authoritative, node-mirrored.
pub(crate) async fn list_peers(
    State(state): State<AppState>,
) -> Result<Json<Vec<wrkspc_core::RosterEntry>>, (StatusCode, Json<Value>)> {
    state.store.roster().await.map(Json).map_err(store_error)
}

/// The `PATCH /api/peers/{id}` body: an operator-moderated alias
/// and/or hub lifecycle status.
#[derive(serde::Deserialize)]
pub(crate) struct PatchPeerBody {
    /// The new display alias (`null` clears it).
    alias: Option<Option<String>>,
    /// The target status (`enrolled`, `booted`, `banned`).
    status: Option<String>,
}

/// `PATCH /api/peers/{id}` — the operator's cohort controls: rename
/// (alias), boot, ban, or restore a peer.
///
/// # Errors
///
/// Returns 400 for invalid bodies, unknown statuses, or an attempt to
/// eject the operator peer; 404 for unknown peers.
pub(crate) async fn patch_peer(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<PatchPeerBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if body.alias.is_none() && body.status.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "nothing to change" })),
        ));
    }
    if let Some(alias) = body.alias.flatten().as_deref() {
        let alias = alias.trim();
        if alias.chars().count() > 64 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "alias too long (max 64 characters)" })),
            ));
        }
        state
            .store
            .set_peer_alias(&id, (!alias.is_empty()).then_some(alias), "operator")
            .await
            .map_err(store_error)?;
    }
    if let Some(status) = body.status.as_deref() {
        let parsed = wrkspc_core::PeerStatus::parse(status).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "unknown status (enrolled, booted, banned)" })),
            )
        })?;
        if id == "operator" && parsed.is_ejected() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "the operator peer cannot be ejected" })),
            ));
        }
        // The serial the peer presented (if any) drives the live deny
        // set on ban, and its removal on restore.
        let serial = state
            .store
            .peer_cert_serial(&id)
            .await
            .map_err(store_error)?;
        let updated = state
            .store
            .set_peer_status(&id, parsed, "operator")
            .await
            .map_err(store_error)?;
        if !updated {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unknown peer" })),
            ));
        }
        if let Some(deny) = &state.deny {
            let mut deny = deny.write().expect("deny lock poisoned");
            match (parsed, serial) {
                (wrkspc_core::PeerStatus::Banned, Some(serial)) => {
                    deny.insert(serial);
                }
                (wrkspc_core::PeerStatus::Enrolled, Some(serial)) => {
                    deny.remove(&serial);
                }
                _ => {}
            }
        }
    }
    let entry = state.store.roster_entry(&id).await.map_err(store_error)?;
    Ok(Json(json!(entry)))
}

/// `DELETE /api/peers/{id}` — forget an ejected peer: the cohort entry
/// disappears (roster row deleted) while message history, the audit
/// trail, and certificate revocations are preserved. Forgotten peers
/// are refused re-enrolment until explicitly restored.
///
/// # Errors
///
/// Returns 400 for the operator peer or a peer that is not ejected
/// (booted or banned); 404 for unknown peers.
pub(crate) async fn forget_peer(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    if id == "operator" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "the operator peer cannot be forgotten" })),
        ));
    }
    let entry = state.store.roster_entry(&id).await.map_err(store_error)?;
    let Some(entry) = entry else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown peer" })),
        ));
    };
    if !wrkspc_core::status_is_ejected(&entry.status) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "only booted or banned peers can be forgotten" })),
        ));
    }
    let forgotten = state
        .store
        .forget_peer(&id, "operator")
        .await
        .map_err(store_error)?;
    if !forgotten {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown peer" })),
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/moderation` — the cohort moderation audit log, newest
/// first. `limit` caps the rows (default 200, max 1000).
pub(crate) async fn moderation_log(
    State(state): State<AppState>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<wrkspc_core::ModerationEvent>>, (StatusCode, Json<Value>)> {
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(200)
        .clamp(1, 1000);
    state
        .store
        .moderation_log(limit)
        .await
        .map(Json)
        .map_err(store_error)
}
