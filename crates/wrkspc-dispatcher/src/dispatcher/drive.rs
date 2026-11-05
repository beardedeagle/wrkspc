//! The drive task: one claimed intent through its bound session to a
//! terminal (or unknown) attempt state, with chunk recording.

use super::{DispatchState, InFlight};
use crate::driver::SessionProvider;
use agent_client_protocol::schema::v1::{ContentBlock, TextContent};
use serde_json::json;
use std::sync::{Arc, Mutex};
use wrkspc_acp::stop::stop_reason_str;
use wrkspc_acp::stop_reason_to_attempt_state;
use wrkspc_core::{DueIntent, Store};

/// Records an agent message chunk as a committed `turn.chunk` event.
///
/// Non-text chunks are skipped (the pilot records message output, not
/// tool-call internals). The chunk's ACP `message_id` is kept (null when
/// the agent sends none) so a reader can tell where one agent message
/// ends and the next begins within a turn.
///
/// # Arguments
///
/// * `store` — the transactional store.
/// * `conversation_id` — the owning conversation.
/// * `attempt_id` — the in-flight attempt.
/// * `session_id` — the bound session.
/// * `update` — the session update.
async fn record_chunk(
    store: &Store,
    conversation_id: &str,
    attempt_id: &str,
    session_id: &str,
    update: agent_client_protocol::schema::v1::SessionUpdate,
) {
    if let agent_client_protocol::schema::v1::SessionUpdate::AgentMessageChunk(chunk) = update
        && let ContentBlock::Text(text) = chunk.content
    {
        let _ = store
            .append_event(
                conversation_id,
                "turn.chunk",
                &json!({
                    "attempt_id": attempt_id,
                    "session_id": session_id,
                    "content": text.text,
                    "message_id": chunk.message_id,
                }),
            )
            .await;
    }
}

/// RAII guard removing an attempt from the active set on exit (including
/// panics), so a crashed drive task can be re-claimed.
struct ActiveGuard {
    attempt_id: String,
    session_id: Option<String>,
    state: Arc<Mutex<DispatchState>>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("dispatch state lock");
        state.active.remove(&self.attempt_id);
        state.in_flight.remove(&self.attempt_id);
        if let Some(session_id) = &self.session_id {
            state.busy_sessions.remove(session_id);
        }
    }
}

/// Drives one claimed intent to its terminal state.
pub(crate) async fn drive_intent(
    store: Store,
    provider: Arc<dyn SessionProvider>,
    state: Arc<Mutex<DispatchState>>,
    intent: DueIntent,
    attempt_id: String,
    busy_session: Option<String>,
) {
    let _guard = ActiveGuard {
        attempt_id: attempt_id.clone(),
        session_id: busy_session,
        state: Arc::clone(&state),
    };

    // Pre-dispatch cancel check: a turn the operator already canceled is
    // never dispatched.
    match store.attempt_cancel_requested(&attempt_id).await {
        Ok(true) => {
            store
                .mark_terminal(
                    &attempt_id,
                    wrkspc_core::AttemptState::Canceled,
                    Some("cancelled"),
                    None,
                )
                .await
                .expect("canceled attempt terminal write");
            store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.canceled",
                    &json!({ "attempt_id": attempt_id, "reason": "cancel requested before dispatch" }),
                )
                .await
                .expect("dispatch.canceled event");
            return;
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%error, %attempt_id, "cancel pre-check failed; not dispatching");
            store
                .mark_unknown(&attempt_id, &error.to_string())
                .await
                .expect("unknown attempt write");
            return;
        }
    }

    let Some(binding) = provider.lookup(&intent.recipient_id).await else {
        store
            .mark_unknown(&attempt_id, "no session binding for recipient")
            .await
            .expect("unknown attempt write");
        store
            .append_event(
                &intent.conversation_id,
                "dispatch.unknown",
                &json!({
                    "attempt_id": attempt_id,
                    "recipient_id": intent.recipient_id,
                    "reason": "no session binding",
                }),
            )
            .await
            .expect("dispatch.unknown event");
        return;
    };

    // Guidance rides the next prompt in the conversation.
    let guidance = store
        .undelivered_guidance(&intent.conversation_id)
        .await
        .unwrap_or_default();
    // The prompt carries its own addressing: the conversation id and the
    // recipient's peer id ride every dispatch so the harness can answer via
    // its wrkspc tools without asking the operator for either.
    let mut blocks: Vec<ContentBlock> = vec![ContentBlock::Text(TextContent::new(format!(
        "[wrkspc] conversation: {}\n[wrkspc] you are: {}",
        intent.conversation_id, intent.recipient_id
    )))];
    blocks.extend(guidance.iter().map(|g| {
        ContentBlock::Text(TextContent::new(format!(
            "[operator guidance from {}]: {}",
            g.author_id, g.text
        )))
    }));
    blocks.push(ContentBlock::Text(TextContent::new(intent.content.clone())));

    if let Err(error) = store.mark_dispatched(&attempt_id).await {
        // Never leave a claimed attempt silently undriven: the failure is
        // recorded and the attempt goes unknown for operator
        // reconciliation.
        tracing::warn!(%error, %attempt_id, "mark_dispatched failed; marking unknown");
        store
            .mark_unknown(&attempt_id, &error.to_string())
            .await
            .expect("unknown attempt write");
        store
            .append_event(
                &intent.conversation_id,
                "dispatch.unknown",
                &json!({
                    "attempt_id": attempt_id,
                    "reason": "dispatch write failed",
                    "error": error.to_string(),
                }),
            )
            .await
            .expect("dispatch.unknown event");
        return; // the guard clears tracking
    }
    {
        let mut state_guard = state.lock().expect("dispatch state lock");
        state_guard.in_flight.insert(
            attempt_id.clone(),
            InFlight {
                session_id: binding.session_id.clone(),
                driver: Arc::clone(&binding.driver),
            },
        );
    }
    store
        .append_event(
            &intent.conversation_id,
            "dispatch.started",
            &json!({
                "attempt_id": attempt_id,
                "recipient_id": intent.recipient_id,
                "session_id": binding.session_id,
            }),
        )
        .await
        .expect("dispatch.started event");
    if !guidance.is_empty() {
        let ids: Vec<&str> = guidance.iter().map(|g| g.id.as_str()).collect();
        let _ = store.mark_guidance_delivered(&ids).await;
    }

    let outcome = {
        // Record the turn's streamed output while it runs: agent message
        // chunks become committed `turn.chunk` events attributed to this
        // attempt. One drive task per session (busy gate) makes the
        // attribution unambiguous.
        let prompt_fut = binding.driver.prompt(&binding.session_id, blocks);
        tokio::pin!(prompt_fut);
        let mut events = binding.driver.events();
        let mut events_open = true;
        let outcome = loop {
            tokio::select! {
                outcome = &mut prompt_fut => break outcome,
                event = events.recv(), if events_open => {
                    match event {
                        Ok(event) if event.session_id == binding.session_id => {
                            record_chunk(
                                &store,
                                &intent.conversation_id,
                                &attempt_id,
                                &binding.session_id,
                                event.update,
                            )
                            .await;
                        }
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
                        | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // The prompt outcome still decides the terminal
                            // state; we just stop listening for chunks.
                            events_open = false;
                        }
                    }
                }
            }
        };
        // Drain chunks that raced the completion (emitted in the same poll
        // the turn resolved) so no output is lost.
        while let Ok(event) = events.try_recv() {
            if event.session_id == binding.session_id {
                record_chunk(
                    &store,
                    &intent.conversation_id,
                    &attempt_id,
                    &binding.session_id,
                    event.update,
                )
                .await;
            }
        }
        outcome
    };
    match outcome {
        Ok(reason) => {
            let attempt_state = stop_reason_to_attempt_state(&reason);
            store
                .mark_terminal(
                    &attempt_id,
                    attempt_state,
                    Some(stop_reason_str(&reason)),
                    None,
                )
                .await
                .expect("terminal attempt write");
            store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.stopped",
                    &json!({
                        "attempt_id": attempt_id,
                        "state": attempt_state.as_str(),
                        "stop_reason": stop_reason_str(&reason),
                    }),
                )
                .await
                .expect("dispatch.stopped event");
        }
        Err(error) => {
            store
                .mark_unknown(&attempt_id, &error)
                .await
                .expect("unknown attempt write");
            store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.unknown",
                    &json!({ "attempt_id": attempt_id, "error": error }),
                )
                .await
                .expect("dispatch.unknown event");
        }
    }
}
