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

/// The transcript contribution of one recorded session update.
enum Recorded {
    /// A text chunk was committed.
    Text,
    /// The update carried no storable text (a non-text chunk or a
    /// non-message update): skipped by design, but counted so a turn
    /// that emitted only non-text output is never mistaken for
    /// silence.
    SkippedNonText,
}

/// Records an agent message chunk as a committed `turn.chunk` event.
///
/// Non-text chunks are skipped (the pilot records message output, not
/// tool-call internals) but reported as [`Recorded::SkippedNonText`]
/// so the drive can mark the gap when a turn produced no storable
/// text at all. The chunk's ACP `message_id` is kept (null when the
/// agent sends none) so a reader can tell where one agent message
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
) -> Result<Recorded, wrkspc_core::StoreError> {
    let agent_client_protocol::v1::schema::SessionUpdate::AgentMessageChunk(chunk) = update else {
        return Ok(Recorded::SkippedNonText);
    };
    let message_id = chunk.message_id;
    let ContentBlock::Text(text) = chunk.content else {
        return Ok(Recorded::SkippedNonText);
    };
    store
        .append_event(
            conversation_id,
            "turn.chunk",
            &json!({
                "attempt_id": attempt_id,
                "session_id": session_id,
                "content": text.text,
                "message_id": message_id,
            }),
        )
        .await?;
    Ok(Recorded::Text)
}

/// RAII guard removing an attempt from the active set on exit (including
/// panics), so a crashed drive task can be re-claimed.
struct ActiveGuard {
    attempt_id: String,
    /// The busy slot this drive holds (`Some(peer_id)` when the busy
    /// insert succeeded) — keyed by peer id, like the set itself (E08).
    busy_peer: Option<String>,
    /// The session the turn ran on: recorded with the quarantine so a
    /// later sweep can release it once the peer binds a different one.
    session_id: Option<String>,
    /// Quarantine: an unconfirmed mid-turn cancel keeps the peer busy
    /// so the session is not reused while the adapter's turn still runs.
    keep_busy: std::cell::Cell<bool>,
    state: Arc<Mutex<DispatchState>>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut state = super::lock_state(&self.state);
        state.active.remove(&self.attempt_id);
        state.in_flight.remove(&self.attempt_id);
        // Every drive exit clears the attempt's parked cancel — an
        // early return before registration must not strand a watch
        // channel for the dispatcher's lifetime.
        state.pending_cancels.remove(&self.attempt_id);
        let Some(peer_id) = &self.busy_peer else {
            return;
        };
        if self.keep_busy.get() {
            // Quarantine: the unconfirmed turn still runs on this
            // session, so the busy slot stays. The protected session is
            // recorded so a later claim can release the slot once the
            // peer binds a DIFFERENT session — the old transport is
            // provably gone, and chunk recording is session-scoped, so
            // no misattribution can follow.
            if let Some(session_id) = &self.session_id {
                state
                    .quarantined
                    .insert(peer_id.clone(), session_id.clone());
            }
        } else {
            state.busy_peers.remove(peer_id);
            state.quarantined.remove(peer_id);
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
    /// Deadline for awaiting the adapter's real outcome after an
    /// operator cancel: beyond it the cancel is unconfirmed and the
    /// attempt goes unknown (see the mid-turn cancel arm).
    pub cancel_timeout: Duration,
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
        cancel_timeout,
    } = ctx;
    let _guard = ActiveGuard {
        attempt_id: attempt_id.clone(),
        busy_peer,
        session_id: binding.as_ref().map(|binding| binding.session_id.clone()),
        keep_busy: std::cell::Cell::new(false),
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
        append_feed_event(
            &store,
            &intent.conversation_id,
            "dispatch.unknown",
            &attempt_id,
            &json!({
                "attempt_id": attempt_id,
                "recipient_id": intent.recipient_id,
                "reason": "no session binding",
            }),
        )
        .await;
        return Ok(());
    };

    // The prompt carries its own addressing: the conversation id and the
    // recipient's peer id ride every dispatch so the harness can answer via
    // its wrkspc tools without asking the operator for either.
    let mut blocks: Vec<ContentBlock> = vec![ContentBlock::Text(TextContent::new(format!(
        "[wrkspc] conversation: {}\n[wrkspc] you are: {}",
        intent.conversation_id, intent.recipient_id
    )))];
    blocks.push(ContentBlock::Text(TextContent::new(intent.content.clone())));

    // The dispatch boundary — the LAST decision point before the prompt
    // send. A cancel or a moderation gate (fence, hold, ejected peer)
    // that became effective any time after the claim is enforced here.

    // Stage 1: binding churn re-check.
    if !stage_recheck_binding(
        &store,
        &provider,
        lookup_timeout,
        &intent,
        &attempt_id,
        &binding,
    )
    .await?
    {
        return Ok(());
    }

    // The prompt carries its own addressing: the conversation id and the
    // recipient's peer id ride every dispatch so the harness can answer via
    // its wrkspc tools without asking the operator for either.
    let mut blocks: Vec<ContentBlock> = vec![ContentBlock::Text(TextContent::new(format!(
        "[wrkspc] conversation: {}\n[wrkspc] you are: {}",
        intent.conversation_id, intent.recipient_id
    )))];
    blocks.push(ContentBlock::Text(TextContent::new(intent.content.clone())));

    // Stage 2: the dispatched write.
    if !stage_mark_dispatched(&store, &intent, &attempt_id).await? {
        return Ok(());
    }
    // Stage 3: the cancel check at the dispatch boundary.
    if !stage_boundary_cancel(&store, &intent, &attempt_id).await? {
        return Ok(());
    }
    // Stage 4: the moderation delivery gate.
    if !stage_delivery_gate(&store, &intent, &attempt_id).await? {
        return Ok(());
    }
    // Stage 5: registration (+ pending-cancel consume).
    let Some((_cancel_flag_tx, mut cancel_flag_rx)) =
        stage_registration(&state, &store, &intent, &attempt_id, &binding).await?
    else {
        return Ok(());
    };
    // Stage 6: the guidance claim + post-claim cancel check.
    let Some(guidance) = stage_guidance(&store, &intent, &attempt_id, &mut cancel_flag_rx).await?
    else {
        return Ok(());
    };

    // Capture accounting: chunk commits can fail or lag behind the
    // adapter's emission rate; either way the transcript must show the
    // gap instead of silently dropping turn output (E06/H-16).
    let mut capture_write_failures: u64 = 0;
    let mut chunks_committed: u64 = 0;
    let mut non_text_updates: u64 = 0;
    // All boundary gates and the registration are behind us and the
    // prompt WILL be sent: only now does the feed record the attempt as
    // started, so a gated or pre-dispatch-canceled attempt is never
    // presented as one that ran.
    append_feed_event(
        &store,
        &intent.conversation_id,
        "dispatch.started",
        &attempt_id,
        &json!({
            "attempt_id": attempt_id,
            "recipient_id": intent.recipient_id,
            "session_id": binding.session_id,
        }),
    )
    .await;

    let guidance_blocks = guidance.iter().map(|g| {
        ContentBlock::Text(TextContent::new(format!(
            "[operator guidance from {}]: {}",
            g.author_id, g.text
        )))
    });
    blocks.splice(1..1, guidance_blocks);

    // Quarantine: an unconfirmed mid-turn cancel retains the peer's
    // busy slot (see the mid-turn cancel arm) so the session is not
    // reused while the adapter's turn still runs.
    let mut quarantine_busy = false;
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
                changed = cancel_flag_rx.changed() => {
                    // An operator cancel flipped the flag mid-turn: the
                    // durable cancellation is recorded and the adapter
                    // interrupt was attempted. The terminal state must
                    // come from the adapter's REAL outcome — a
                    // synthetic Cancelled would release the busy slot
                    // while the ACP request still streams, letting the
                    // next turn attribute the old turn's chunks to
                    // itself. The cancel CALLER stays nonblocking; the
                    // drive alone waits, deadline-bounded.
                    if changed.is_ok() && *cancel_flag_rx.borrow() {
                        match tokio::time::timeout(cancel_timeout, &mut prompt_fut).await {
                            Ok(outcome) => break outcome,
                            Err(_elapsed) => {
                                // The adapter ignored the interrupt:
                                // record Unknown and quarantine the
                                // peer (retain the busy slot) so the
                                // session is not reused while the
                                // unconfirmed turn still runs.
                                quarantine_busy = true;
                                break Err(format!(
                                    "cancel unconfirmed after {cancel_timeout:?}: adapter kept the turn running"
                                ));
                            }
                        }
                    }
                }
                event = events.recv(), if events_open => {
                    match event {
                        Ok(event) if event.session_id == binding.session_id => {
                            match record_chunk(
                                &store,
                                &intent.conversation_id,
                                &attempt_id,
                                &binding.session_id,
                                event.update,
                            )
                            .await
                            {
                                Ok(Recorded::Text) => chunks_committed += 1,
                                Ok(Recorded::SkippedNonText) => non_text_updates += 1,
                                Err(error) => {
                                    tracing::warn!(%error, %attempt_id, "chunk commit failed");
                                    capture_write_failures += 1;
                                }
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
                    match record_chunk(
                        &store,
                        &intent.conversation_id,
                        &attempt_id,
                        &binding.session_id,
                        event.update,
                    )
                    .await
                    {
                        Ok(Recorded::Text) => chunks_committed += 1,
                        Ok(Recorded::SkippedNonText) => non_text_updates += 1,
                        Err(error) => {
                            tracing::warn!(%error, %attempt_id, "chunk commit failed");
                            capture_write_failures += 1;
                        }
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
                    match record_chunk(
                        &store,
                        &intent.conversation_id,
                        &attempt_id,
                        &binding.session_id,
                        event.update,
                    )
                    .await
                    {
                        Ok(Recorded::Text) => chunks_committed += 1,
                        Ok(Recorded::SkippedNonText) => non_text_updates += 1,
                        Err(error) => {
                            tracing::warn!(%error, %attempt_id, "chunk commit failed");
                            capture_write_failures += 1;
                        }
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
    _guard.keep_busy.set(quarantine_busy);
    // A partial capture is never reported as complete: committed chunk
    // write failures leave their own visible marker, and a turn whose
    // output was entirely non-text must not read as silence.
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
    if chunks_committed == 0 && non_text_updates > 0 {
        record_gap(
            &store,
            &intent.conversation_id,
            &attempt_id,
            "non-text output not captured",
            non_text_updates,
        )
        .await;
    }

    match outcome {
        Ok(reason) => {
            let attempt_state = stop_reason_to_attempt_state(&reason);
            // Terminal persistence and guidance consumption commit in
            // ONE store transaction: a failure between the two could
            // otherwise strand guidance claimed by an already-terminal
            // attempt that restart reconciliation can never see.
            let ids: Vec<&str> = guidance.iter().map(|g| g.id.as_str()).collect();
            store
                .complete_attempt(
                    &attempt_id,
                    attempt_state,
                    Some(stop_reason_str(&reason)),
                    &ids,
                )
                .await
                .map_err(|e| format!("terminal attempt write failed: {e}"))?;
            append_feed_event(
                &store,
                &intent.conversation_id,
                "dispatch.stopped",
                &attempt_id,
                &json!({
                    "attempt_id": attempt_id,
                    "state": attempt_state.as_str(),
                    "stop_reason": stop_reason_str(&reason),
                }),
            )
            .await;
        }
        Err(error) => {
            // mark_unknown releases any guidance claim in the SAME
            // transaction as the unknown write (C3): the next usable
            // turn re-rides the guidance instead of a claimed-by-dead
            // attempt row blocking every future claim.
            store
                .mark_unknown(&attempt_id, &error)
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            append_feed_event(
                &store,
                &intent.conversation_id,
                "dispatch.unknown",
                &attempt_id,
                &json!({ "attempt_id": attempt_id, "error": error }),
            )
            .await;
        }
    }
    Ok(())
}

/// Stage 1 of the drive security order: the binding churn re-check — a
/// second resolve that yields a different session (or hangs) means the
/// binding state changed mid-claim, which is undeterminable (E01/E08).
/// Returns `false` after writing the unknown attempt.
async fn stage_recheck_binding(
    store: &Store,
    provider: &Arc<dyn SessionProvider>,
    lookup_timeout: Duration,
    intent: &DueIntent,
    attempt_id: &str,
    binding: &SessionBinding,
) -> Result<bool, String> {
    match tokio::time::timeout(lookup_timeout, provider.lookup(&intent.recipient_id)).await {
        // In production the registry resolve is a cache hit: the
        // session already exists. A changed binding is churn.
        Ok(Some(current))
            if current.session_id == binding.session_id
                && current.peer_id == intent.recipient_id
                && Arc::ptr_eq(&current.driver, &binding.driver) => {}
        Ok(_) => {
            store
                .mark_unknown(attempt_id, "session binding churned during claim")
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            append_feed_event(
                store,
                &intent.conversation_id,
                "dispatch.unknown",
                attempt_id,
                &json!({
                    "attempt_id": attempt_id,
                    "recipient_id": intent.recipient_id,
                    "reason": "session binding churned during claim",
                }),
            )
            .await;
            return Ok(false);
        }
        Err(_) => {
            store
                .mark_unknown(
                    attempt_id,
                    "session re-check timed out (adapter nonresponsive)",
                )
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            append_feed_event(
                store,
                &intent.conversation_id,
                "dispatch.unknown",
                attempt_id,
                &json!({
                    "attempt_id": attempt_id,
                    "recipient_id": intent.recipient_id,
                    "reason": "session re-check timed out (adapter nonresponsive)",
                }),
            )
            .await;
            return Ok(false);
        }
    }
    Ok(true)
}

/// Stage 2: the dispatched write. A failure is recorded and the attempt
/// goes unknown for operator reconciliation — never a silent undriven
/// claim. Returns `false` after writing the unknown attempt.
async fn stage_mark_dispatched(
    store: &Store,
    intent: &DueIntent,
    attempt_id: &str,
) -> Result<bool, String> {
    if let Err(error) = store.mark_dispatched(attempt_id).await {
        // Never leave a claimed attempt silently undriven: the failure is
        // recorded and the attempt goes unknown for operator
        // reconciliation.
        tracing::warn!(%error, %attempt_id, "mark_dispatched failed; marking unknown");
        store
            .mark_unknown(attempt_id, &error.to_string())
            .await
            .map_err(|e| format!("unknown attempt write failed: {e}"))?;
        append_feed_event(
            store,
            &intent.conversation_id,
            "dispatch.unknown",
            attempt_id,
            &json!({
                "attempt_id": attempt_id,
                "reason": "dispatch write failed",
                "error": error.to_string(),
            }),
        )
        .await;
        return Ok(false); // the guard clears tracking
    }
    Ok(true)
}

/// Stage 3: the cancel check at the dispatch boundary — a cancel that
/// became effective any time after the claim is enforced here, before
/// anything else can happen. Returns `false` after closing the attempt.
async fn stage_boundary_cancel(
    store: &Store,
    intent: &DueIntent,
    attempt_id: &str,
) -> Result<bool, String> {
    match store.attempt_cancel_requested(attempt_id).await {
        Ok(true) => {
            store
                .mark_terminal(
                    attempt_id,
                    wrkspc_core::AttemptState::Canceled,
                    Some("cancelled"),
                    None,
                )
                .await
                .map_err(|e| format!("canceled attempt terminal write failed: {e}"))?;
            append_feed_event(
                store,
                &intent.conversation_id,
                "dispatch.canceled",
                attempt_id,
                &json!({ "attempt_id": attempt_id, "reason": "cancel requested before dispatch" }),
            )
            .await;
            return Ok(false);
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%error, %attempt_id, "cancel check at the dispatch boundary failed");
            store
                .mark_unknown(attempt_id, &error.to_string())
                .await
                .map_err(|e| format!("unknown attempt write failed: {e}"))?;
            append_feed_event(
                store,
                &intent.conversation_id,
                "dispatch.unknown",
                attempt_id,
                &json!({
                    "attempt_id": attempt_id,
                    "reason": "cancel check at the dispatch boundary failed",
                    "error": error.to_string(),
                }),
            )
            .await;
            return Ok(false);
        }
    }
    Ok(true)
}

/// Stage 4: the moderation delivery gate — a fence or hold that became
/// effective after the claim gates the attempt before the prompt. The
/// delivery stays deliverable once the gate clears; only the attempt
/// closes. Returns `false` after closing or marking the attempt.
async fn stage_delivery_gate(
    store: &Store,
    intent: &DueIntent,
    attempt_id: &str,
) -> Result<bool, String> {
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
                if let Err(gate_error) = store.gate_attempt(attempt_id, &reason).await {
                    tracing::warn!(%gate_error, %attempt_id, "gate_attempt failed");
                    store
                        .mark_unknown(attempt_id, &gate_error.to_string())
                        .await
                        .map_err(|e| format!("unknown attempt write failed: {e}"))?;
                    append_feed_event(
                        store,
                        &intent.conversation_id,
                        "dispatch.unknown",
                        attempt_id,
                        &json!({
                            "attempt_id": attempt_id,
                            "reason": "gate_attempt failed",
                            "error": gate_error.to_string(),
                        }),
                    )
                    .await;
                    return Ok(false);
                }
                append_feed_event(
                    store,
                    &intent.conversation_id,
                    "dispatch.gated",
                    attempt_id,
                    &json!({
                        "attempt_id": attempt_id,
                        "recipient_id": intent.recipient_id,
                        "reason": reason,
                    }),
                )
                .await;
                return Ok(false);
            }
            other => {
                tracing::warn!(%other, %attempt_id, "delivery gate check failed; not dispatching");
                store
                    .mark_unknown(attempt_id, &other.to_string())
                    .await
                    .map_err(|e| format!("unknown attempt write failed: {e}"))?;
                append_feed_event(
                    store,
                    &intent.conversation_id,
                    "dispatch.unknown",
                    attempt_id,
                    &json!({
                        "attempt_id": attempt_id,
                        "reason": "delivery gate check failed",
                        "error": other.to_string(),
                    }),
                )
                .await;
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Stage 5: registration — the pending-cancel consume and the in-flight
/// registration share ONE lock, so a cancellation durably recorded
/// during the claim-to-registration window is consumed here and the
/// prompt is never sent. Returns the cancel flag pair, or `None` when a
/// pre-registration cancel was consumed and the attempt closed.
async fn stage_registration(
    state: &Arc<Mutex<DispatchState>>,
    store: &Store,
    intent: &DueIntent,
    attempt_id: &str,
    binding: &SessionBinding,
) -> Result<
    Option<(
        tokio::sync::watch::Sender<bool>,
        tokio::sync::watch::Receiver<bool>,
    )>,
    String,
> {
    // Registered only after the gate: a cancel arriving from here on
    // finds the attempt in flight and interrupts the running turn.
    // Registration and the pending-cancel consume share ONE lock: a
    // cancellation durably recorded during the claim-to-registration
    // window is consumed here and the prompt is never sent.
    let (cancel_flag_tx, cancel_flag_rx) = tokio::sync::watch::channel(false);
    // The pending-cancel consume and the registration share ONE lock —
    // and the lock never spans an await (the drive task must stay
    // Send). A cancellation durably recorded during the
    // claim-to-registration window is consumed here and the prompt is
    // never sent.
    let pre_canceled = {
        let mut state_guard = super::lock_state(state);
        if state_guard.pending_cancels.remove(attempt_id).is_some() {
            true
        } else {
            state_guard.in_flight.insert(
                attempt_id.to_string(),
                InFlight {
                    session_id: binding.session_id.clone(),
                    driver: Arc::clone(&binding.driver),
                    cancel_flag: cancel_flag_tx.clone(),
                },
            );
            false
        }
    };
    if pre_canceled {
        // Canceled while we were preparing: close deterministically and
        // never send the prompt.
        store
            .mark_terminal(
                attempt_id,
                wrkspc_core::AttemptState::Canceled,
                Some("cancelled"),
                None,
            )
            .await
            .map_err(|e| format!("canceled attempt terminal write failed: {e}"))?;
        append_feed_event(
            store,
            &intent.conversation_id,
            "dispatch.canceled",
            attempt_id,
            &json!({
                "attempt_id": attempt_id,
                "reason": "cancel requested before dispatch",
            }),
        )
        .await;
        return Ok(None);
    }
    Ok(Some((cancel_flag_tx, cancel_flag_rx)))
}

/// Stage 6: the guidance claim (atomically, right before the send) and
/// the post-claim cancel check — a cancel that landed while claiming
/// releases the claim and never sends the prompt. Returns the claimed
/// guidance, or `None` when the attempt was closed instead.
async fn stage_guidance(
    store: &Store,
    intent: &DueIntent,
    attempt_id: &str,
    cancel_flag_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<Vec<wrkspc_core::Guidance>>, String> {
    // Guidance rides the next SUCCESSFUL prompt in the conversation
    // (E09) — and it is CLAIMED atomically right before the send (C3):
    // two concurrent turns in the same conversation cannot both ride
    // the same guidance. The claim is released if the attempt does not
    // resolve successfully, so guidance never vanishes into a dead
    // driver or a failed send. A claim failure fails the attempt
    // visibly — guidance is part of the dispatch contract.
    let guidance = match store
        .claim_guidance(&intent.conversation_id, attempt_id)
        .await
    {
        Ok(guidance) => guidance,
        Err(error) => {
            store
                .mark_terminal(
                    attempt_id,
                    wrkspc_core::AttemptState::Failed,
                    Some("guidance claim failed"),
                    None,
                )
                .await
                .map_err(|e| format!("terminal attempt write failed: {e}"))?;
            append_feed_event(
                store,
                &intent.conversation_id,
                "dispatch.failed",
                attempt_id,
                &json!({
                    "attempt_id": attempt_id,
                    "reason": "guidance claim failed",
                    "error": error.to_string(),
                }),
            )
            .await;
            return Ok(None);
        }
    };
    // A cancel may have landed while guidance claiming awaited: the
    // flag flips, the claim is released, and the prompt never goes out.
    if *cancel_flag_rx.borrow() {
        if !guidance.is_empty()
            && let Err(claim_error) = store.release_guidance_claim(attempt_id).await
        {
            // The claim must not stay pinned to an attempt about to be
            // closed: propagate instead of writing Canceled, so the
            // drive-failure reconciliation re-runs the release
            // atomically with mark_unknown.
            return Err(format!(
                "guidance claim release failed after pre-send cancel: {claim_error}"
            ));
        }
        store
            .mark_terminal(
                attempt_id,
                wrkspc_core::AttemptState::Canceled,
                Some("cancelled"),
                None,
            )
            .await
            .map_err(|e| format!("canceled attempt terminal write failed: {e}"))?;
        append_feed_event(
            store,
            &intent.conversation_id,
            "dispatch.canceled",
            attempt_id,
            &json!({
                "attempt_id": attempt_id,
                "reason": "cancel requested before dispatch",
            }),
        )
        .await;
        return Ok(None);
    }
    Ok(Some(guidance))
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
    append_feed_event(
        store,
        conversation_id,
        "capture.gap",
        attempt_id,
        &json!({
            "attempt_id": attempt_id,
            "reason": reason,
            "count": count,
        }),
    )
    .await;
}

/// Appends a feed event, warning (never failing the turn) when the
/// append itself fails: a wedged events table must be visible in the
/// logs instead of silently truncating the transcript.
async fn append_feed_event(
    store: &Store,
    conversation_id: &str,
    kind: &str,
    attempt_id: &str,
    payload: &serde_json::Value,
) {
    if let Err(error) = store.append_event(conversation_id, kind, payload).await {
        tracing::warn!(%error, %attempt_id, %kind, "feed event append failed");
    }
}
