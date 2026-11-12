//! The drive task: one claimed intent through its bound session to a
//! terminal (or unknown) attempt state, with chunk recording.

use super::{DispatchState, InFlight};
use crate::driver::{SessionBinding, SessionProvider};
use agent_client_protocol::v1::schema::{ContentBlock, TextContent};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
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
/// * `store` — the transactional store.
/// * `conversation_id` — the owning conversation.
/// * `attempt_id` — the in-flight attempt.
/// * `session_id` — the bound session.
/// * `update` — the session update.
///
/// # Errors
///
/// Returns the store error when the chunk commit fails (the drive
/// counts these into a visible capture gap).
async fn record_chunk(
    store: &Store,
    conversation_id: &str,
    attempt_id: &str,
    session_id: &str,
    update: agent_client_protocol::v1::schema::SessionUpdate,
) -> Result<(), wrkspc_core::StoreError> {
    if let agent_client_protocol::v1::schema::SessionUpdate::AgentMessageChunk(chunk) = update
        && let ContentBlock::Text(text) = chunk.content
    {
        store
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
            .await?;
    }
    Ok(())
}

/// RAII guard removing an attempt from the active set on exit (including
/// panics), so a crashed drive task can be re-claimed.
struct ActiveGuard {
    attempt_id: String,
    /// The busy slot this drive holds (`Some(peer_id)` when the busy
    /// insert succeeded) — keyed by peer id, like the set itself (E08).
    busy_peer: Option<String>,
    state: Arc<Mutex<DispatchState>>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut state = super::lock_state(&self.state);
        state.active.remove(&self.attempt_id);
        state.in_flight.remove(&self.attempt_id);
        if let Some(peer_id) = &self.busy_peer {
            state.busy_peers.remove(peer_id);
        }
    }
}

/// The drive task's shared context: everything a drive needs besides
/// the intent, the attempt, and the selected binding.
pub(crate) struct DriveContext {
    /// The transactional store.
    pub store: Store,
    /// The session provider (the churn re-check uses it).
    pub provider: Arc<dyn SessionProvider>,
    /// Shared dispatch state.
    pub state: Arc<Mutex<DispatchState>>,
    /// Deadline for the churn re-check lookup.
    pub lookup_timeout: Duration,
}

/// Drives one claimed intent to its terminal state.
///
/// * `ctx` — the shared drive context.
/// * `intent` — the claimed intent.
/// * `attempt_id` — the claim.
/// * `binding` — the binding selected at claim time (`None` when the
///   recipient has no session).
/// * `busy_peer` — the busy slot this drive holds (`Some(peer_id)`
///   when the busy insert succeeded).
///
/// # Errors
///
/// Returns the failure reason when the attempt's terminal (or unknown)
/// state could not be durably written — the caller reconciles it.
pub(crate) async fn drive_intent(
    ctx: DriveContext,
    intent: DueIntent,
    attempt_id: String,
    binding: Option<SessionBinding>,
    busy_peer: Option<String>,
) -> Result<(), String> {
    let DriveContext {
        store,
        provider,
        state,
        lookup_timeout,
    } = ctx;
    let _guard = ActiveGuard {
        attempt_id: attempt_id.clone(),
        busy_peer,
        state: Arc::clone(&state),
    };

    // Cancel and moderation are enforced at the dispatch boundary: the
    // final gate immediately before the prompt send re-evaluates both,
    // so a cancel or fence that landed after the claim (or during any
    // of the preparation awaits below) still stops the turn.

    // The binding selected at claim time is retained; the lookup below
    // is only a churn re-check — a second resolve that yields a
    // different session (or hangs) means the binding state changed
    // mid-claim, which is undeterminable (E01/E08).
    let Some(binding) = binding else {
        store
            .mark_unknown(&attempt_id, "no session binding for recipient")
            .await
            .map_err(|e| format!("unknown attempt write failed: {e}"))?;
        let _ = store
            .append_event(
                &intent.conversation_id,
                "dispatch.unknown",
                &json!({
                    "attempt_id": attempt_id,
                    "recipient_id": intent.recipient_id,
                    "reason": "no session binding",
                }),
            )
            .await;
        return Ok(());
    };
    match tokio::time::timeout(lookup_timeout, provider.lookup(&intent.recipient_id)).await {
        // In production the registry resolve is a cache hit: the
        // session already exists. A changed binding is churn.
        Ok(Some(current)) if current.session_id == binding.session_id => {}
        Ok(_) => {
            store
                .mark_unknown(&attempt_id, "session binding churned during claim")
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.unknown",
                    &json!({
                        "attempt_id": attempt_id,
                        "recipient_id": intent.recipient_id,
                        "reason": "session binding churned during claim",
                    }),
                )
                .await;
            return Ok(());
        }
        Err(_) => {
            store
                .mark_unknown(
                    &attempt_id,
                    "session re-check timed out (adapter nonresponsive)",
                )
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.unknown",
                    &json!({
                        "attempt_id": attempt_id,
                        "recipient_id": intent.recipient_id,
                        "reason": "session re-check timed out (adapter nonresponsive)",
                    }),
                )
                .await;
            return Ok(());
        }
    }

    // The prompt carries its own addressing: the conversation id and the
    // recipient's peer id ride every dispatch so the harness can answer via
    // its wrkspc tools without asking the operator for either.
    let mut blocks: Vec<ContentBlock> = vec![ContentBlock::Text(TextContent::new(format!(
        "[wrkspc] conversation: {}\n[wrkspc] you are: {}",
        intent.conversation_id, intent.recipient_id
    )))];
    blocks.push(ContentBlock::Text(TextContent::new(intent.content.clone())));

    if let Err(error) = store.mark_dispatched(&attempt_id).await {
        // Never leave a claimed attempt silently undriven: the failure is
        // recorded and the attempt goes unknown for operator
        // reconciliation.
        tracing::warn!(%error, %attempt_id, "mark_dispatched failed; marking unknown");
        store
            .mark_unknown(&attempt_id, &error.to_string())
            .await
            .map_err(|e| format!("unknown attempt write failed: {e}"))?;
        let _ = store
            .append_event(
                &intent.conversation_id,
                "dispatch.unknown",
                &json!({
                    "attempt_id": attempt_id,
                    "reason": "dispatch write failed",
                    "error": error.to_string(),
                }),
            )
            .await;
        return Ok(()); // the guard clears tracking
    }
    let _ = store
        .append_event(
            &intent.conversation_id,
            "dispatch.started",
            &json!({
                "attempt_id": attempt_id,
                "recipient_id": intent.recipient_id,
                "session_id": binding.session_id,
            }),
        )
        .await;

    // The dispatch boundary — the LAST decision point before the prompt
    // send. A cancel or a moderation gate (fence, hold, ejected peer)
    // that became effective any time after the claim is enforced here.
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
                .map_err(|e| format!("canceled attempt terminal write failed: {e}"))?;
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.canceled",
                    &json!({ "attempt_id": attempt_id, "reason": "cancel requested before dispatch" }),
                )
                .await;
            return Ok(());
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%error, %attempt_id, "cancel check at the dispatch boundary failed");
            store
                .mark_unknown(&attempt_id, &error.to_string())
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            return Ok(());
        }
    }
    if let Err(error) = store
        .delivery_gate(&intent.message_id, &intent.recipient_id)
        .await
    {
        match error {
            wrkspc_core::StoreError::BlockedByFence { kinds, .. } => {
                // The gate stopped this attempt BEFORE the prompt: close
                // it and return the intent to the queue — the delivery
                // stays deliverable once the gate clears (a gate is not
                // an operator cancellation).
                let reason = format!("moderation gate after claim: {kinds}");
                if let Err(gate_error) = store.gate_attempt(&attempt_id, &reason).await {
                    tracing::warn!(%gate_error, %attempt_id, "gate_attempt failed");
                    store
                        .mark_unknown(&attempt_id, &gate_error.to_string())
                        .await
                        .map_err(|e| format!("unknown attempt write failed: {e}"))?;
                    return Ok(());
                }
                let _ = store
                    .append_event(
                        &intent.conversation_id,
                        "dispatch.gated",
                        &json!({
                            "attempt_id": attempt_id,
                            "recipient_id": intent.recipient_id,
                            "reason": reason,
                        }),
                    )
                    .await;
                return Ok(());
            }
            other => {
                tracing::warn!(%other, %attempt_id, "delivery gate check failed; not dispatching");
                store
                    .mark_unknown(&attempt_id, &other.to_string())
                    .await
                    .map_err(|e| format!("unknown attempt write failed: {e}"))?;
                return Ok(());
            }
        }
    }

    // Registered only after the gate: a cancel arriving from here on
    // finds the attempt in flight and interrupts the running turn.
    {
        let mut state_guard = super::lock_state(&state);
        state_guard.in_flight.insert(
            attempt_id.clone(),
            InFlight {
                session_id: binding.session_id.clone(),
                driver: Arc::clone(&binding.driver),
            },
        );
    }

    // Capture accounting: chunk commits can fail or lag behind the
    // adapter's emission rate; either way the transcript must show the
    // gap instead of silently dropping turn output (E06/H-16).
    let mut capture_write_failures: u64 = 0;

    // Guidance rides the next SUCCESSFUL prompt in the conversation
    // (E09) — and it is CLAIMED atomically right before the send (C3):
    // two concurrent turns in the same conversation cannot both ride
    // the same guidance. The claim is released if the attempt does not
    // resolve successfully, so guidance never vanishes into a dead
    // driver or a failed send. A claim failure fails the attempt
    // visibly — guidance is part of the dispatch contract.
    let guidance = match store
        .claim_guidance(&intent.conversation_id, &attempt_id)
        .await
    {
        Ok(guidance) => guidance,
        Err(error) => {
            store
                .mark_terminal(
                    &attempt_id,
                    wrkspc_core::AttemptState::Failed,
                    Some("guidance claim failed"),
                    None,
                )
                .await
                .map_err(|e| format!("terminal attempt write failed: {e}"))?;
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.failed",
                    &json!({
                        "attempt_id": attempt_id,
                        "reason": "guidance claim failed",
                        "error": error.to_string(),
                    }),
                )
                .await;
            return Ok(());
        }
    };
    let guidance_blocks = guidance.iter().map(|g| {
        ContentBlock::Text(TextContent::new(format!(
            "[operator guidance from {}]: {}",
            g.author_id, g.text
        )))
    });
    blocks.splice(1..1, guidance_blocks);

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
                            if let Err(error) = record_chunk(
                                &store,
                                &intent.conversation_id,
                                &attempt_id,
                                &binding.session_id,
                                event.update,
                            )
                            .await
                            {
                                tracing::warn!(%error, %attempt_id, "chunk commit failed");
                                capture_write_failures += 1;
                            }
                        }
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                            // The receiver lagged the adapter: commit a
                            // visible gap and KEEP listening — the
                            // receiver stays valid and future chunks
                            // must not be dropped silently (E06/H-16).
                            record_gap(
                                &store,
                                &intent.conversation_id,
                                &attempt_id,
                                "broadcast lag",
                                dropped,
                            )
                            .await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // The transport is gone; the prompt outcome
                            // still decides the terminal state.
                            events_open = false;
                        }
                    }
                }
            }
        };
        // Drain chunks that raced the completion (emitted in the same poll
        // the turn resolved) so no output is lost.
        loop {
            match events.try_recv() {
                Ok(event) if event.session_id == binding.session_id => {
                    if let Err(error) = record_chunk(
                        &store,
                        &intent.conversation_id,
                        &attempt_id,
                        &binding.session_id,
                        event.update,
                    )
                    .await
                    {
                        tracing::warn!(%error, %attempt_id, "chunk commit failed");
                        capture_write_failures += 1;
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(dropped)) => {
                    record_gap(
                        &store,
                        &intent.conversation_id,
                        &attempt_id,
                        "broadcast lag",
                        dropped,
                    )
                    .await;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            }
        }
        // Real stdio adapters send the terminal response BEFORE their
        // trailing update; a chunk emitted just after the response must
        // still land, not be dropped silently (H-16 completion). The
        // busy gate guarantees the NEXT turn's drive has not started,
        // so attribution stays unambiguous. The window is idle-bounded:
        // 250 ms of silence closes it.
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(250), events.recv()).await {
                Ok(Ok(event)) if event.session_id == binding.session_id => {
                    if let Err(error) = record_chunk(
                        &store,
                        &intent.conversation_id,
                        &attempt_id,
                        &binding.session_id,
                        event.update,
                    )
                    .await
                    {
                        tracing::warn!(%error, %attempt_id, "chunk commit failed");
                        capture_write_failures += 1;
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped))) => {
                    record_gap(
                        &store,
                        &intent.conversation_id,
                        &attempt_id,
                        "broadcast lag",
                        dropped,
                    )
                    .await;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                Err(_elapsed) => break,
            }
        }
        outcome
    };
    // A partial capture is never reported as complete: committed chunk
    // write failures leave their own visible marker.
    if capture_write_failures > 0 {
        record_gap(
            &store,
            &intent.conversation_id,
            &attempt_id,
            "chunk commit failures",
            capture_write_failures,
        )
        .await;
    }

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
                .map_err(|e| format!("terminal attempt write failed: {e}"))?;
            // Guidance is delivered only now: it demonstrably rode a
            // prompt that reached a terminal stop reason (E09).
            if !guidance.is_empty() {
                let ids: Vec<&str> = guidance.iter().map(|g| g.id.as_str()).collect();
                if let Err(error) = store.mark_guidance_delivered(&ids).await {
                    tracing::warn!(%error, %attempt_id, "guidance delivered-mark failed; guidance re-rides the next turn");
                }
            }
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.stopped",
                    &json!({
                        "attempt_id": attempt_id,
                        "state": attempt_state.as_str(),
                        "stop_reason": stop_reason_str(&reason),
                    }),
                )
                .await;
        }
        Err(error) => {
            // Guidance stays undelivered: release this attempt's claim so
            // the next usable turn re-rides it rather than replaying it
            // blindly after an undeterminable outcome (E09/C3).
            if !guidance.is_empty()
                && let Err(claim_error) = store.release_guidance_claim(&attempt_id).await
            {
                tracing::warn!(%claim_error, %attempt_id, "guidance claim release failed; guidance stays claimed until the attempt is reconciled");
            }
            store
                .mark_unknown(&attempt_id, &error)
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.unknown",
                    &json!({ "attempt_id": attempt_id, "error": error }),
                )
                .await;
        }
    }
    Ok(())
}

/// Commits a visible capture-gap marker: turn output was dropped (the
/// receiver lagged the adapter) or chunk commits failed, and the
/// transcript must not read as complete without saying so (E06/H-16).
async fn record_gap(
    store: &Store,
    conversation_id: &str,
    attempt_id: &str,
    reason: &str,
    count: u64,
) {
    let _ = store
        .append_event(
            conversation_id,
            "capture.gap",
            &json!({
                "attempt_id": attempt_id,
                "reason": reason,
                "count": count,
            }),
        )
        .await;
}
