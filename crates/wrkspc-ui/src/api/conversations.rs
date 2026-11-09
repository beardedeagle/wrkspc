//! Conversation views: list, one, events (redacted), messages (redacted),
//! fences, and hub-only creation.

use super::{AppState, store_error};
use crate::redact::{redact, redact_value};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use wrkspc_core::{ConversationView, Fence, MessageView};

pub(crate) async fn list_conversations(
    State(state): State<AppState>,
) -> Result<Json<Vec<ConversationView>>, (StatusCode, Json<Value>)> {
    state
        .store
        .list_conversations()
        .await
        .map(Json)
        .map_err(store_error)
}

pub(crate) async fn conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ConversationView>, (StatusCode, Json<Value>)> {
    let conversation = state
        .store
        .list_conversations()
        .await
        .map_err(store_error)?
        .into_iter()
        .find(|c| c.id == id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("conversation {id} not found") })),
            )
        })?;
    Ok(Json(conversation))
}

/// The `PATCH /api/conversations/{id}` body.
#[derive(Deserialize)]
pub(crate) struct RenameConversationBody {
    /// The new name. Omitted, null, or blank clears it: the name is display
    /// metadata and a room is fully usable unnamed.
    #[serde(default)]
    name: Option<String>,
}

/// Renames a conversation, or clears its name.
///
/// The name is never a key and never has to be unique — two rooms may share one,
/// and the id is what everything else joins on. The hub owns the value and
/// mirrors it to nodes.
///
/// # Arguments
///
/// * `state` — the API state holding the store.
/// * `id` — the conversation to rename.
/// * `body` — the new name, or null to clear it.
///
/// # Returns
///
/// The updated conversation, or a JSON error with the status that caused it.
pub(crate) async fn rename_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RenameConversationBody>,
) -> Result<Json<ConversationView>, (StatusCode, Json<Value>)> {
    /// Longest name accepted, matching the peer alias limit.
    const MAX_NAME_CHARS: usize = 64;
    // A hub-connected node mirrors hub-owned names: the next
    // authoritative snapshot erases a local rename, so it is refused
    // loudly instead of reporting success for state that cannot
    // persist (UI-08).
    if !state.hub_role && state.hub_connected {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "the conversation name is hub-owned on a connected node; change it at the hub"
            })),
        ));
    }
    if let Some(name) = body.name.as_deref()
        && name.trim().chars().count() > MAX_NAME_CHARS
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("conversation names are limited to {MAX_NAME_CHARS} characters")
            })),
        ));
    }
    let updated = state
        .store
        .set_conversation_name(&id, body.name.as_deref())
        .await
        .map_err(store_error)?;
    if !updated {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("conversation {id} not found") })),
        ));
    }
    let conversation = state
        .store
        .list_conversations()
        .await
        .map_err(store_error)?
        .into_iter()
        .find(|row| row.id == id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("conversation {id} not found") })),
            )
        })?;
    Ok(Json(conversation))
}

#[derive(Deserialize)]
pub(crate) struct EventsQuery {
    after_seq: Option<i64>,
    limit: Option<i64>,
}

pub(crate) async fn conversation_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<Vec<Value>>, (StatusCode, Json<Value>)> {
    let events = state
        .store
        .events_since(
            &id,
            query.after_seq.unwrap_or(0),
            query.limit.unwrap_or(500),
        )
        .await
        .map_err(store_error)?;
    let redacted: Vec<Value> = events
        .iter()
        .map(|e| {
            json!({
                "seq": e.seq,
                "conversation_id": e.conversation_id,
                "kind": e.kind,
                "payload": redact_value(&e.payload),
                "created_at": e.created_at,
            })
        })
        .collect();
    Ok(Json(redacted))
}

/// `?window=N` bounds the response to the latest N messages (UI-19):
/// the transcript's initial load stays bounded instead of pulling a
/// whole room into memory; the unbounded form remains for explicit
/// full-history reads.
#[derive(serde::Deserialize)]
pub(crate) struct MessageQuery {
    window: Option<u32>,
}

pub(crate) async fn conversation_messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<MessageQuery>,
) -> Result<Json<Vec<MessageView>>, (StatusCode, Json<Value>)> {
    let mut messages = match query.window {
        Some(window) => state.store.list_messages_window(&id, window).await,
        None => state.store.list_messages(&id).await,
    }
    .map_err(store_error)?;
    for message in &mut messages {
        message.content = redact(&message.content);
    }
    Ok(Json(messages))
}

pub(crate) async fn conversation_fences(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<Fence>>, (StatusCode, Json<Value>)> {
    state
        .store
        .conversation_fences(&id)
        .await
        .map(Json)
        .map_err(store_error)
}

/// The `POST /api/conversations` body: the members to enrol in the new
/// conversation. Every member must already be an enrolled peer — the
/// route never placeholder-enrols at the hub host (a placeholder host
/// binding would make the real node's first presence read as a hostile
/// cross-host claim).
#[derive(Debug, serde::Deserialize)]
pub(crate) struct CreateConversationBody {
    members: Vec<String>,
}

/// `POST /api/conversations` — creates the hub conversation (the
/// loop's first move). Hub-only: the node role returns 404.
pub(crate) async fn create_conversation(
    State(state): State<AppState>,
    Json(body): Json<CreateConversationBody>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    if !state.hub_role {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "conversation creation is hub-only" })),
        ));
    }
    // The operator must be a member by construction (UI-18): every UI
    // post is authored as `operator`, and a room the operator cannot
    // post to presents a working composer that can only fail.
    if !body.members.iter().any(|member| member == "operator") {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "the operator must be a member of every conversation"
            })),
        ));
    }
    for member in &body.members {
        let enrolled = state
            .store
            .peer_details(member)
            .await
            .map_err(store_error)?
            .is_some();
        if !enrolled {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": "member not enrolled", "member": member })),
            ));
        }
    }
    let member_refs: Vec<&str> = body.members.iter().map(String::as_str).collect();
    let id = state
        .store
        .create_conversation(&member_refs)
        .await
        .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(json!({ "id": id }))))
}

/// `DELETE /api/conversations/{id}` — removes the room and everything scoped
/// to it. Works on whichever role the operator is driving: the hub deletes the
/// authoritative copy, a node deletes its mirror.
///
/// # Errors
///
/// Returns 404 when the conversation does not exist.
pub(crate) async fn delete_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state
        .store
        .delete_conversation(&id)
        .await
        .map_err(store_error)?
    {
        true => Ok(Json(json!({ "deleted": id }))),
        false => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "conversation not found" })),
        )),
    }
}
