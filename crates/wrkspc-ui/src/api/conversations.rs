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

pub(crate) async fn conversation_messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<MessageView>>, (StatusCode, Json<Value>)> {
    let mut messages = state.store.list_messages(&id).await.map_err(store_error)?;
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
