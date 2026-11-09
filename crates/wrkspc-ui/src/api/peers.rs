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
    /// The new display alias. Absent = untouched; explicit `null` =
    /// CLEAR (UI-09 — Serde's default `Option<Option<_>>` collapses
    /// null into "absent", which made clearing impossible); a value =
    /// set.
    #[serde(default, deserialize_with = "deserialize_alias_patch")]
    alias: Option<Option<String>>,
    /// The target status (`enrolled`, `booted`, `banned`).
    status: Option<String>,
}

/// Deserializes the alias patch distinguishing the three shapes:
/// missing field → `None`, explicit `null` → `Some(None)` (clear), a
/// string → `Some(Some(value))`.
fn deserialize_alias_patch<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let inner = <Option<String> as serde::Deserialize>::deserialize(deserializer)?;
    Ok(Some(inner))
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
    // VALIDATE EVERYTHING BEFORE ANY WRITE (UI-10): a bad status must
    // never leave a partial alias mutation committed, and a missing
    // target is 404 — never a 200 with no effect.
    let status = match body.status.as_deref() {
        Some(status) => Some(wrkspc_core::PeerStatus::parse(status).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "unknown status (enrolled, booted, banned)" })),
            )
        })?),
        None => None,
    };
    if id == "operator" && status.is_some_and(|s| s.is_ejected()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "the operator peer cannot be ejected" })),
        ));
    }
    // The alias value is validated up front too (missing = untouched,
    // explicit null = CLEAR, value = set — UI-09 distinguishes them).
    let alias_value = match &body.alias {
        None => None, // field absent: no alias change
        Some(Some(alias)) => {
            let alias = alias.trim();
            if alias.chars().count() > 64 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "alias too long (max 64 characters)" })),
                ));
            }
            Some((!alias.is_empty()).then_some(alias))
        }
        Some(None) => Some(None), // explicit null: clear the alias
    };
    if !state
        .store
        .peer_details(&id)
        .await
        .map_err(store_error)?
        .is_some()
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown peer" })),
        ));
    }
    if let Some(alias) = alias_value {
        // A hub-connected node mirrors hub-owned alias state: the next
        // authoritative snapshot would silently overwrite this write
        // (UI-08), so it is refused loudly instead of reporting
        // success for state that cannot persist.
        if !state.hub_role && state.hub_connected {
            return Err((
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "the alias is hub-owned on a connected node; change it at the hub"
                })),
            ));
        }
        // The target's existence was checked above; the store's
        // silent no-op for a vanished peer is unreachable here except
        // through a forget race, which the audit trail still records.
        state
            .store
            .set_peer_alias(&id, alias, "operator")
            .await
            .map_err(store_error)?;
    }
    if let Some(parsed) = status {
        let updated = state
            .store
            .set_peer_status(&id, parsed, "operator")
            .await
            .map_err(|error| {
                // A 500 body names the store error for the operator;
                // log it server-side too so the diagnosis survives
                // even where the response body is discarded.
                tracing::error!(peer_id = %id, %error, "peer status change failed");
                store_error(error)
            })?;
        if !updated {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unknown peer" })),
            ));
        }
        // The live deny set mirrors the durable state after EVERY
        // status change: bans add the peer's serial, restores remove
        // only that peer's revocation reason — a shared host
        // certificate stays denied while any peer remains banned, and
        // the set can never diverge from the store (H-11). The
        // `--revoke-list` floor is preserved: store re-syncs add to
        // it, never replace it.
        if let Some(deny) = &state.deny {
            match state.store.cert_revocations().await {
                Ok(serials) => {
                    let mut set: std::collections::HashSet<String> =
                        deny.floor.iter().cloned().collect();
                    set.extend(serials);
                    // The set is rebuilt in full below, so recovering a
                    // poisoned guard is safe: any partially-mutated
                    // content is replaced wholesale.
                    let mut live = deny
                        .live
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    live.clear();
                    live.extend(set);
                }
                Err(error) => {
                    tracing::warn!(%error, "deny-set re-sync failed; the live set may lag the store");
                }
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
    // An entry is deletable whatever its status: refusing an enrolled peer is
    // what made the ✕ look broken.
    if state
        .store
        .roster_entry(&id)
        .await
        .map_err(store_error)?
        .is_none()
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown peer" })),
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
