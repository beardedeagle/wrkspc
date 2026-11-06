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

use crate::driver::SessionProvider;
use drive::drive_intent;
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
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
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

/// An in-flight turn: which session drives which attempt.
struct InFlight {
    session_id: String,
    driver: Arc<dyn crate::driver::SessionDriver>,
}

/// Shared dispatch state between the run loop and drive tasks.
#[derive(Default)]
struct DispatchState {
    /// Attempts with an actively running drive task.
    active: HashSet<String>,
    /// Sessions with an in-flight turn (serialized per session: one turn
    /// at a time — also what makes chunk attribution unambiguous).
    busy_sessions: HashSet<String>,
    /// In-flight turns by attempt id.
    in_flight: HashMap<String, InFlight>,
}

/// Handle to a running dispatcher: cancel requests and shutdown.
#[derive(Clone)]
pub struct DispatcherHandle {
    cmd_tx: mpsc::Sender<Command>,
}

impl DispatcherHandle {
    /// Requests cancellation of a message's execution for a recipient.
    ///
    /// Queued intents are canceled directly; an in-flight attempt is
    /// interrupted via the bound session and confirmed by the turn's
    /// `Cancelled` stop reason.
    ///
    /// # Arguments
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
    /// Returns [`StoreError`] on store failure or a dead dispatcher.
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

impl Dispatcher {
    /// Builds a dispatcher over a store and session provider.
    ///
    /// # Arguments
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
    /// (restart reconciliation). Drive tasks run in a [`JoinSet`] and are
    /// aborted on exit.
    ///
    /// # Arguments
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
        let mut tasks = JoinSet::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                command = self.cmd_rx.recv() => {
                    match command {
                        Some(Command::Cancel { message_id, recipient_id, set_by, respond }) => {
                            self.handle_cancel(&message_id, &recipient_id, &set_by, respond).await;
                        }
                        Some(Command::Shutdown) | None => break,
                    }
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

    /// One claim sweep: enumerates due intents and spawns a drive task for
    /// each newly claimed attempt.
    async fn claim_due(&mut self, tasks: &mut JoinSet<()>) {
        let due = match self.store.due_intents().await {
            Ok(due) => due,
            Err(error) => {
                tracing::warn!(%error, "due_intents failed");
                return;
            }
        };
        for intent in due {
            self.claim_one(tasks, intent).await;
        }
    }

    /// Claims and drives one intent.
    async fn claim_one(&mut self, tasks: &mut JoinSet<()>, intent: DueIntent) {
        // Busy gate BEFORE claim: one turn in flight per session. An intent
        // whose session is busy stays queued (retried next tick) — claimed
        // but undriven intents would otherwise wedge behind their own open
        // attempt. The lookup runs outside the state lock (it may spawn a
        // session); the busy insert itself stays lock-serialized.
        let binding = self.provider.lookup(&intent.recipient_id).await;
        let busy_session = {
            let mut state = self.state.lock().expect("dispatch state lock");
            match binding {
                None => None, // no binding: the drive task marks unknown
                Some(binding) => {
                    if state.busy_sessions.insert(binding.session_id.clone()) {
                        Some(binding.session_id)
                    } else {
                        return;
                    }
                }
            }
        };

        let attempt_id = match self
            .store
            .claim_intent(&intent.message_id, &intent.recipient_id)
            .await
        {
            Ok(attempt_id) => attempt_id,
            Err(error) => {
                // Release the busy slot; the intent was not claimed.
                if let Some(session_id) = &busy_session {
                    let mut state = self.state.lock().expect("dispatch state lock");
                    state.busy_sessions.remove(session_id);
                }
                match error {
                    StoreError::BlockedByFence { .. }
                    | StoreError::BlockedByUnknown { .. }
                    | StoreError::IntentTerminal { .. } => {}
                    other => tracing::warn!(%other, "claim failed"),
                }
                return;
            }
        };
        {
            let mut state = self.state.lock().expect("dispatch state lock");
            if !state.active.insert(attempt_id.clone()) {
                return; // already being driven
            }
        }
        let store = self.store.clone();
        let provider = Arc::clone(&self.provider);
        let state = Arc::clone(&self.state);
        tasks.spawn(async move {
            drive_intent(store, provider, state, intent, attempt_id, busy_session).await;
        });
    }

    /// Services a cancel command.
    async fn handle_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        set_by: &str,
        respond: oneshot::Sender<Result<CancelOutcome, DispatcherError>>,
    ) {
        let outcome = match self
            .store
            .request_cancel(message_id, recipient_id, set_by)
            .await
        {
            Ok(CancelOutcome::Attempt { attempt_id }) => {
                let driver_and_session = {
                    let state = self.state.lock().expect("dispatch state lock");
                    state
                        .in_flight
                        .get(&attempt_id)
                        .map(|info| (info.session_id.clone(), Arc::clone(&info.driver)))
                };
                if let Some((session_id, driver)) = driver_and_session
                    && let Err(error) = driver.cancel(&session_id).await
                {
                    tracing::warn!(%error, %attempt_id, "session cancel failed");
                }
                Ok(CancelOutcome::Attempt { attempt_id })
            }
            other => other.map_err(DispatcherError::Store),
        };
        let _ = respond.send(outcome);
    }
}
