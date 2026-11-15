//! The dispatcher: claims eligible intents and drives them through bound
//! sessions, under moderation.
//!
//! Lifecycle per intent: claim (fence-gated, idempotent, unknown-blocking)
//! → pre-dispatch cancel check → mark dispatched (delivery accounting) →
//! prompt (guidance-prefixed) → terminal state from the ACP stop reason.
//! Any driver error marks the attempt unknown — the conservative contract:
//! when the outcome is undeterminable, the session is blocked until the
//! operator reconciles. On startup, open attempts from a previous run are
//! marked unknown for the same reason (the pilot runs one dispatcher per
//! process; a restart means no turns are in flight).
//!
//! The central run loop never awaits adapter work: claim-and-drive tasks
//! run in a [`JoinSet`], so a nonresponsive adapter delays only its own
//! peer's queue while cancel/shutdown keep being serviced (E03/H-14).
//!
//! Reviewed invariant-panic policy (T05): `lock_state` panics ONLY when
//! the dispatch-state mutex is poisoned (a task panicked mid-mutation);
//! recovering possibly torn state would corrupt the busy gate and the
//! in-flight tracking, so the panic is deliberate and scoped to that
//! single site via `#[expect(clippy::panic)]` — any future panic or
//! unwrap in this module re-triggers the lint.

use crate::driver::SessionProvider;
use drive::drive_intent;
use futures::FutureExt;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use wrkspc_core::{CancelOutcome, DueIntent, Store, StoreError};

mod drive;

/// Dispatcher tuning.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Claim-poll interval.
    pub tick: Duration,
    /// Deadline for one session lookup (spawn/initialize/new/model pin):
    /// a nonresponsive adapter must not block the dispatcher beyond this.
    pub lookup_timeout: Duration,
    /// Deadline for one adapter cancel notification: a hung cancel
    /// future must not block the dispatcher's command loop beyond this
    /// (the durable cancellation is already recorded in the store; the
    /// adapter interrupt is best-effort and deadline-bounded).
    pub cancel_timeout: Duration,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            lookup_timeout: Duration::from_secs(45),
            cancel_timeout: Duration::from_secs(30),
        }
    }
}

/// Dispatcher-level errors.
#[derive(Debug, thiserror::Error)]
pub enum DispatcherError {
    /// The dispatcher run loop is gone.
    #[error("dispatcher is not running")]
    Gone,
    /// The underlying store rejected the operation.
    #[error(transparent)]
    Store(wrkspc_core::StoreError),
}

/// Commands the run loop services (from [`DispatcherHandle`]).
enum Command {
    Cancel {
        message_id: String,
        recipient_id: String,
        set_by: String,
        respond: oneshot::Sender<Result<CancelOutcome, DispatcherError>>,
    },
    Shutdown,
}

/// An in-flight turn: which session drives which attempt, plus the
/// drive's cancellation flag (the drive watches it across preparation
/// and the prompt; a cancel that lands in the pre-registration window
/// is parked in [`DispatchState::pending_cancels`] instead).
struct InFlight {
    session_id: String,
    driver: Arc<dyn crate::driver::SessionDriver>,
    cancel_flag: tokio::sync::watch::Sender<bool>,
}

/// A finished claim-and-drive task's output: the attempt id, its
/// conversation, and the drive result. Named fields instead of a
/// positional tuple so the destructuring at reap time cannot mix the
/// two ids up.
struct DriveOutput {
    attempt_id: String,
    conversation_id: String,
    result: Result<(), String>,
}

/// A joined task: the output, or a `JoinError` when the task was aborted
/// (shutdown) or panicked outside the drive's own catch.
type JoinedDrive = Result<DriveOutput, tokio::task::JoinError>;

/// Shared dispatch state between the run loop and drive tasks.
#[derive(Default)]
struct DispatchState {
    /// Attempts with an actively running drive task.
    active: HashSet<String>,
    /// Peers with an in-flight turn (serialized per session: one turn at
    /// a time — also what makes chunk attribution unambiguous).
    ///
    /// Keyed by PEER id, not the adapter-provided session id: session
    /// ids are adapter-chosen strings that independent adapters may
    /// repeat (E08) — a peer's session is unique on this node, so the
    /// peer id is the collision-proof serialization key.
    busy_peers: HashSet<String>,
    /// In-flight turns by attempt id.
    in_flight: HashMap<String, InFlight>,
    /// Quarantined peers: the busy slot an unconfirmed mid-turn cancel
    /// retains, with the session it protects. A later claim releases
    /// the slot once the peer binds a DIFFERENT session (the old
    /// transport is provably gone), so a quarantined peer is never
    /// wedged until restart when the adapter moves on.
    quarantined: HashMap<String, String>,
    /// Lookups currently running for an intent, keyed
    /// `"message_id/recipient_id"`: one claim-and-drive task per intent
    /// at a time, covering the lookup itself. Without it, a lookup
    /// slower than the tick spawns overlapping tasks for one intent —
    /// and a stale task's timeout can mark an attempt unknown that
    /// another drive is actively executing.
    lookup_pending: HashSet<String>,
    /// Cancellations durably recorded for attempts not yet registered
    /// as in-flight (the claim-to-registration window): the drive
    /// consumes its entry and refuses to send.
    pending_cancels: HashMap<String, tokio::sync::watch::Sender<bool>>,
}

/// Handle to a running dispatcher: cancel requests and shutdown.
#[derive(Clone, Debug)]
pub struct DispatcherHandle {
    cmd_tx: mpsc::Sender<Command>,
}

/// Locks the dispatch-state mutex.
///
/// Poison here is an unrecoverable invariant violation — a task
/// panicked while mid-mutation of the busy gate / in-flight tracking
/// — so the lock panics deliberately instead of recovering possibly
/// torn state. This is the dispatcher's single reviewed invariant-
/// panic site (see the module docs).
#[expect(
    clippy::panic,
    reason = "invariant panic: poisoned dispatch-state mutex"
)]
fn lock_state(state: &std::sync::Mutex<DispatchState>) -> std::sync::MutexGuard<'_, DispatchState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => panic!("dispatch state lock poisoned: {poisoned}"),
    }
}

impl DispatcherHandle {
    /// Requests cancellation of a message's execution for a recipient.
    ///
    /// Queued intents are canceled directly; an in-flight attempt is
    /// interrupted via the bound session and confirmed by the turn's
    /// `Cancelled` stop reason.
    ///
    /// * `message_id` — the message whose execution to cancel.
    /// * `recipient_id` — the recipient whose intent to cancel.
    /// * `set_by` — the operator/peer requesting cancellation.
    ///
    /// # Returns
    ///
    /// The cancel outcome.
    ///
    /// # Errors
    ///
    /// Returns [`DispatcherError::Gone`] when the dispatcher is gone, or
    /// [`DispatcherError::Store`] (wrapping [`StoreError`]) when the
    /// store rejects the operation.
    pub async fn request_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        set_by: &str,
    ) -> Result<CancelOutcome, DispatcherError> {
        let (respond, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Cancel {
                message_id: message_id.to_string(),
                recipient_id: recipient_id.to_string(),
                set_by: set_by.to_string(),
                respond,
            })
            .await
            .map_err(|_| DispatcherError::Gone)?;
        rx.await.map_err(|_| DispatcherError::Gone)?
    }

    /// Stops the dispatcher run loop (in-flight drive tasks are aborted).
    ///
    /// # Errors
    ///
    /// Returns [`DispatcherError::Gone`] when the dispatcher is already
    /// gone.
    pub async fn shutdown(&self) -> Result<(), DispatcherError> {
        self.cmd_tx
            .send(Command::Shutdown)
            .await
            .map_err(|_| DispatcherError::Gone)
    }
}

/// The moderation dispatch engine.
pub struct Dispatcher {
    store: Store,
    provider: Arc<dyn SessionProvider>,
    config: DispatcherConfig,
    cmd_rx: mpsc::Receiver<Command>,
    state: Arc<Mutex<DispatchState>>,
}

// The session provider and the dispatch state are trait objects /
// internal bookkeeping without Debug; the summary covers the
// observable shape instead of demanding it from them.
impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher")
            .field("store", &self.store)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Dispatcher {
    /// Builds a dispatcher over a store and session provider.
    ///
    /// * `store` — the transactional store (the dispatcher owns the
    ///   exclusive lock for the pilot).
    /// * `provider` — resolves peers to bound sessions.
    /// * `config` — tuning.
    ///
    /// # Returns
    ///
    /// The dispatcher and its handle.
    pub fn new(
        store: Store,
        provider: Arc<dyn SessionProvider>,
        config: DispatcherConfig,
    ) -> (Self, DispatcherHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(64);
        (
            Self {
                store,
                provider,
                config,
                cmd_rx,
                state: Arc::new(Mutex::new(DispatchState::default())),
            },
            DispatcherHandle { cmd_tx },
        )
    }

    /// Runs the claim loop until `shutdown` resolves or the handle asks it
    /// to stop.
    ///
    /// On entry, open attempts left by a previous run are marked unknown
    /// (restart reconciliation). Claim-and-drive tasks run in a
    /// [`JoinSet`]: completions are reaped continuously (one task record
    /// per finished turn, never one per historical turn — E07) and a
    /// panicked drive is attributed to its attempt and reconciled as
    /// unknown instead of silently freeing the session.
    ///
    /// * `shutdown` — a future that ends the loop when it resolves.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when startup reconciliation fails.
    pub async fn run<F>(mut self, shutdown: F) -> Result<(), StoreError>
    where
        F: std::future::Future<Output = ()>,
    {
        self.reconcile_startup().await?;

        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(self.config.tick);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut tasks = JoinSet::<DriveOutput>::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                command = self.cmd_rx.recv() => {
                    match command {
                        Some(Command::Cancel { message_id, recipient_id, set_by, respond }) => {
                            self.spawn_cancel(&message_id, &recipient_id, &set_by, respond);
                        }
                        Some(Command::Shutdown) | None => break,
                    }
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    self.reap_drive(joined).await;
                }
                _ = tick.tick() => {
                    self.claim_due(&mut tasks).await;
                }
            }
        }

        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }

    /// Startup reconciliation: any open attempt survives a crash with an
    /// undeterminable outcome, so it is marked unknown and blocks until
    /// the operator decides.
    async fn reconcile_startup(&self) -> Result<(), StoreError> {
        for open in self.store.open_attempts().await? {
            self.store
                .mark_unknown(&open.attempt_id, "dispatcher restart during attempt")
                .await?;
            self.store
                .append_event(
                    &open.conversation_id,
                    "dispatch.unknown",
                    &json!({
                        "attempt_id": open.attempt_id,
                        "reason": "dispatcher restart during attempt",
                    }),
                )
                .await?;
            tracing::warn!(
                attempt_id = %open.attempt_id,
                intent_id = %open.intent_id,
                "restart reconciliation: attempt marked unknown",
            );
        }
        Ok(())
    }

    /// Reaps one finished claim-and-drive task: a clean completion needs
    /// nothing (the drive wrote its terminal state); a reported failure
    /// or a panicked task reconciles the attempt as unknown so it is
    /// visible and blocked until the operator decides (E07).
    async fn reap_drive(&self, joined: Option<JoinedDrive>) {
        let Some(joined) = joined else { return };
        match joined {
            Ok(DriveOutput { result: Ok(()), .. }) => {}
            Ok(DriveOutput {
                attempt_id,
                conversation_id,
                result: Err(reason),
            }) => {
                self.reconcile_drive_failure(&attempt_id, &conversation_id, &reason)
                    .await;
            }
            Err(error) => {
                tracing::error!(%error, "drive task panicked; attempt attribution lost");
                // The panic payload cannot name the attempt (JoinError
                // discards the output); the drive task catches panics
                // itself, so this branch is a last resort.
            }
        }
    }

    /// Marks an attempt whose drive task failed (or panicked) unknown,
    /// with a visible feed event.
    async fn reconcile_drive_failure(&self, attempt_id: &str, conversation_id: &str, reason: &str) {
        if let Err(error) = self
            .store
            .mark_unknown(attempt_id, &format!("drive task failed: {reason}"))
            .await
        {
            tracing::warn!(%error, %attempt_id, "unknown reconcile after drive failure failed");
            return;
        }
        let _ = self
            .store
            .append_event(
                conversation_id,
                "dispatch.unknown",
                &json!({
                    "attempt_id": attempt_id,
                    "reason": format!("drive task failed: {reason}"),
                }),
            )
            .await;
        tracing::warn!(%attempt_id, %reason, "drive task failure reconciled as unknown");
    }

    /// One claim sweep: enumerates due intents and spawns a claim-and-
    /// drive task for each. The central loop never awaits adapter work —
    /// each task performs its own deadline-bounded lookup (E03/H-14).
    async fn claim_due(&mut self, tasks: &mut JoinSet<DriveOutput>) {
        let Ok(due) = self
            .store
            .due_intents()
            .await
            .inspect_err(|error| tracing::warn!(%error, "due_intents failed"))
        else {
            return;
        };
        // Due order per peer: at most ONE claim task per recipient per
        // sweep. Due intents come back oldest-first, and the busy slot
        // is claimed inside the spawned task — without this pre-filter
        // a newer intent whose lookup wins the race would take the
        // peer's slot and dispatch siblings out of order.
        let mut swept_peers = HashSet::new();
        for intent in due {
            if !swept_peers.insert(intent.recipient_id.clone()) {
                continue;
            }
            let key = format!("{}/{}", intent.message_id, intent.recipient_id);
            {
                // One lookup per intent at a time: reserve BEFORE the
                // spawn so a slow lookup cannot accumulate overlapping
                // claim tasks (each sweep would otherwise spawn another
                // task for the same still-queued intent).
                let mut state_guard = lock_state(&self.state);
                if !state_guard.lookup_pending.insert(key.clone()) {
                    continue;
                }
            }
            self.spawn_claim(tasks, intent, key);
        }
    }

    /// Spawns the claim-and-drive task for one intent.
    fn spawn_claim(&self, tasks: &mut JoinSet<DriveOutput>, intent: DueIntent, key: String) {
        let store = self.store.clone();
        let provider = Arc::clone(&self.provider);
        let state = Arc::clone(&self.state);
        let lookup_timeout = self.config.lookup_timeout;
        let cancel_timeout = self.config.cancel_timeout;
        tasks.spawn(async move {
            // Panics inside the drive are attributed to the attempt and
            // reconciled as unknown inside `claim_and_drive` (E07): no
            // panicking drive task ever frees its session silently.
            let release_state = Arc::clone(&state);
            let result = claim_and_drive(
                store,
                provider,
                state,
                intent,
                lookup_timeout,
                cancel_timeout,
            )
            .await;
            {
                // Release the reservation on EVERY exit path (success,
                // error, panic) so the intent can be swept again.
                let mut state_guard = lock_state(&release_state);
                state_guard.lookup_pending.remove(&key);
            }
            result
        });
    }

    /// Services a cancel command WITHOUT awaiting any adapter future:
    /// the durable cancellation is recorded in the store (bounded by
    /// the store's own timeouts), the caller is answered, and the
    /// adapter interrupt — when an in-flight turn exists — runs as a
    /// supervised, deadline-bounded task. A cancel that lands while
    /// the drive is still preparing (claimed but not yet registered)
    /// is parked in `pending_cancels` for the drive to consume.
    fn spawn_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        set_by: &str,
        respond: oneshot::Sender<Result<CancelOutcome, DispatcherError>>,
    ) {
        let store = self.store.clone();
        let state = Arc::clone(&self.state);
        let cancel_timeout = self.config.cancel_timeout;
        let message_id = message_id.to_string();
        let recipient_id = recipient_id.to_string();
        let set_by = set_by.to_string();
        tokio::spawn(async move {
            let outcome = match store
                .request_cancel(&message_id, &recipient_id, &set_by)
                .await
            {
                Ok(CancelOutcome::Attempt { attempt_id }) => {
                    {
                        let mut state_guard = lock_state(&state);
                        if let Some(info) = state_guard.in_flight.get(&attempt_id) {
                            let _ = info.cancel_flag.send(true);
                            let session_id = info.session_id.clone();
                            let driver = Arc::clone(&info.driver);
                            let inner_attempt = attempt_id.clone();
                            // Best-effort adapter interrupt, bounded: the
                            // turn's own stop reason confirms the
                            // cancellation even when this hangs.
                            tokio::spawn(async move {
                                let result = tokio::time::timeout(
                                    cancel_timeout,
                                    driver.cancel(&session_id),
                                )
                                .await;
                                match result {
                                    Ok(Ok(())) => {}
                                    Ok(Err(error)) => tracing::warn!(
                                        %error, %inner_attempt, "session cancel failed"
                                    ),
                                    Err(_) => tracing::warn!(
                                        %inner_attempt, "session cancel timed out"
                                    ),
                                }
                            });
                        } else if state_guard.active.contains(&attempt_id) {
                            // Pre-registration window: the drive consumes
                            // this entry and refuses to send the prompt.
                            // Guarded by `active` (under the SAME lock
                            // the drive's exit guard takes) so a late
                            // cancel task cannot park an entry after the
                            // drive already exited.
                            let (flag, _rx) = tokio::sync::watch::channel(true);
                            state_guard.pending_cancels.insert(attempt_id.clone(), flag);
                        }
                    }
                    Ok(CancelOutcome::Attempt { attempt_id })
                }
                other => other.map_err(DispatcherError::Store),
            };
            let _ = respond.send(outcome);
        });
    }
}

/// Claims and drives one intent: deadline-bounded lookup → busy gate →
/// claim → drive. Returns the attempt id so the run loop can attribute
/// a panicked task.
async fn claim_and_drive(
    store: Store,
    provider: Arc<dyn SessionProvider>,
    state: Arc<Mutex<DispatchState>>,
    intent: DueIntent,
    lookup_timeout: Duration,
    cancel_timeout: Duration,
) -> DriveOutput {
    let conversation_id = intent.conversation_id.clone();
    let ok = |attempt_id: String| DriveOutput {
        attempt_id,
        conversation_id: conversation_id.clone(),
        result: Ok(()),
    };

    // Busy gate BEFORE claim: one turn in flight per session. An intent
    // whose session is busy stays queued (retried next tick) — claimed
    // but undriven intents would otherwise wedge behind their own open
    // attempt. The lookup runs outside the state lock (it may spawn a
    // session); the busy insert itself stays lock-serialized. The
    // lookup is deadline-bounded so a nonresponsive adapter cannot
    // hold this task (and only this task) forever (E03/H-14).
    let binding = match tokio::time::timeout(lookup_timeout, provider.lookup(&intent.recipient_id))
        .await
    {
        // A provider that returns another peer's binding (a racing
        // registry) must never prompt the wrong session: the mismatch
        // is rejected as unbound, which the drive records as unknown
        // with a visible feed event.
        Ok(Some(binding)) if binding.peer_id == intent.recipient_id => Some(binding),
        Ok(Some(binding)) => {
            tracing::warn!(
                peer_id = %binding.peer_id,
                recipient_id = %intent.recipient_id,
                "provider returned a binding for another peer; treating the recipient as unbound"
            );
            None
        }
        Ok(None) => None,
        Err(_) => {
            let reason = "session lookup timed out (adapter nonresponsive)";
            // Fresh-only claim: a timed-out lookup must never mark an
            // attempt unknown that ANOTHER drive claimed and is
            // actively executing. With the per-intent reservation only
            // one lookup runs at a time, but a stale sweep could still
            // race a concurrent claim; None means someone else owns it
            // and we leave it alone.
            let claimed = match store
                .claim_intent_if_unclaimed(&intent.message_id, &intent.recipient_id)
                .await
            {
                Ok(claimed) => claimed,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        message_id = %intent.message_id,
                        recipient_id = %intent.recipient_id,
                        "claim failed after lookup timeout",
                    );
                    return ok(format!("{}/{}", intent.message_id, intent.recipient_id));
                }
            };
            let Some(attempt_id) = claimed else {
                return ok(format!("{}/{}", intent.message_id, intent.recipient_id));
            };
            if let Err(error) = store.mark_unknown(&attempt_id, reason).await {
                tracing::warn!(%error, %attempt_id, "unknown write after lookup timeout failed");
            }
            let _ = store
                .append_event(
                    &intent.conversation_id,
                    "dispatch.unknown",
                    &json!({ "attempt_id": attempt_id, "reason": reason }),
                )
                .await;
            return ok(attempt_id);
        }
    };
    let busy_peer = {
        let mut state_guard = lock_state(&state);
        match binding {
            None => None, // no binding: the drive task marks unknown
            Some(binding) => {
                // Keyed by peer id: adapter session ids are not
                // collision-proof across independent adapters (E08).
                let recipient = intent.recipient_id.clone();
                let session_id = binding.session_id.clone();
                if state_guard.busy_peers.insert(recipient.clone()) {
                    Some(binding)
                } else if let Some(quarantined_session) = state_guard.quarantined.get(&recipient)
                    && *quarantined_session != session_id
                {
                    // Quarantine release: the peer now binds a
                    // DIFFERENT session than the one the unconfirmed
                    // turn was left running on. The registry replaces
                    // a session only when the old one closes, so the
                    // old turn's transport is provably gone — and
                    // chunk recording is session-scoped, so nothing
                    // can be misattributed. The slot is released and
                    // the new session proceeds.
                    tracing::warn!(
                        peer_id = %recipient,
                        %session_id,
                        %quarantined_session,
                        "quarantine released: the peer bound a new session"
                    );
                    state_guard.busy_peers.remove(&recipient);
                    state_guard.quarantined.remove(&recipient);
                    state_guard.busy_peers.insert(recipient);
                    Some(binding)
                } else {
                    return ok(format!("{}/{}", intent.message_id, intent.recipient_id));
                }
            }
        }
    };
    let busy_peer_recipient = busy_peer.as_ref().map(|_| intent.recipient_id.clone());

    let claim = match store
        .claim_intent(&intent.message_id, &intent.recipient_id)
        .await
    {
        Ok(attempt_id) => attempt_id,
        Err(error) => {
            // Release the busy slot; the intent was not claimed.
            let mut state_guard = lock_state(&state);
            state_guard.busy_peers.remove(&intent.recipient_id);
            match error {
                StoreError::BlockedByFence { .. }
                | StoreError::BlockedByUnknown { .. }
                | StoreError::IntentTerminal { .. } => {}
                other => tracing::warn!(%other, "claim failed"),
            }
            return ok(format!("{}/{}", intent.message_id, intent.recipient_id));
        }
    };
    {
        let mut state_guard = lock_state(&state);
        if !state_guard.active.insert(claim.clone()) {
            // Already being driven: release the busy slot taken above.
            state_guard.busy_peers.remove(&intent.recipient_id);
            return ok(claim);
        }
    }
    // Panics inside the drive are attributed to the claim and
    // reconciled as unknown (E07): the guard still frees the session,
    // but the attempt is blocked until the operator decides.
    let result = std::panic::AssertUnwindSafe(drive_intent(
        drive::DriveContext {
            store: store.clone(),
            provider,
            state,
            lookup_timeout,
            cancel_timeout,
        },
        intent,
        claim.clone(),
        busy_peer,
        // The busy slot is keyed by PEER id; only Some when the
        // busy insert above succeeded.
        busy_peer_recipient,
    ))
    .catch_unwind()
    .await;
    match result {
        // The drive reports its own reconciliation failures; they ride
        // the output so the run loop can retry the unknown write.
        Ok(inner) => DriveOutput {
            attempt_id: claim.clone(),
            conversation_id: conversation_id.clone(),
            result: inner,
        },
        Err(payload) => {
            let reason = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "drive task panicked".to_string());
            if let Err(error) = store
                .mark_unknown(&claim, &format!("drive task panicked: {reason}"))
                .await
            {
                tracing::warn!(%error, attempt_id = %claim, "unknown reconcile after panic failed");
            }
            let _ = store
                .append_event(
                    &conversation_id,
                    "dispatch.unknown",
                    &json!({
                        "attempt_id": claim,
                        "reason": format!("drive task panicked: {reason}"),
                    }),
                )
                .await;
            DriveOutput {
                attempt_id: claim,
                conversation_id,
                result: Err(format!("drive task panicked: {reason}")),
            }
        }
    }
}
