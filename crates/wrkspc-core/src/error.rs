//! wrkspc-core error types.

/// Errors produced by the transactional store.
///
/// Database errors from `sqlx` are carried through via
/// [`StoreError::Db`](Self::Db); every other variant is a fence or
/// lifecycle violation with recovery semantics defined by the HLD.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The idempotency key was already used with different content.
    #[error("idempotency conflict: key `{key}` was already used with different content")]
    IdempotencyConflict { key: String },

    /// The referenced record does not exist.
    #[error("{entity} `{id}` not found")]
    NotFound { entity: &'static str, id: String },

    /// The execution intent is in a state that blocks new work until an
    /// operator reconciles the unknown attempt.
    #[error(
        "execution intent `{intent}` is in state `{state}`; new work is blocked until reconciled"
    )]
    BlockedByUnknown { intent: String, state: String },

    /// A terminal state was already recorded; terminal states are immutable
    /// except idempotent re-emission of the same terminal state.
    #[error("attempt `{attempt}` is terminal ({state}); cannot transition to `{requested}`")]
    TerminalState {
        attempt: String,
        state: String,
        requested: String,
    },

    /// The execution intent has already reached a terminal state; it can
    /// never accept a new attempt.
    #[error("execution intent `{intent}` is terminal ({state})")]
    IntentTerminal { intent: String, state: String },

    /// Another process already owns this store's dispatcher lock.
    #[error("store is locked by owner `{owner}` (acquired {acquired_at})")]
    AlreadyLocked { owner: String, acquired_at: String },

    /// A record with this identifier already exists.
    #[error("{entity} `{id}` already exists")]
    AlreadyExists { entity: &'static str, id: String },

    /// An A2A task row is terminal; terminal states are immutable except
    /// idempotent re-emission of the same terminal state.
    #[error("a2a task `{task_id}` is terminal ({state}); cannot transition to `{requested}`")]
    TaskTerminal {
        task_id: String,
        state: String,
        requested: String,
    },

    /// An active moderation fence blocks this delivery.
    #[error(
        "delivery to `{peer_id}` in conversation `{conversation_id}` is blocked by active fences: {kinds}"
    )]
    BlockedByFence {
        conversation_id: String,
        peer_id: String,
        kinds: String,
    },

    /// Underlying database error.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    /// A peer reference matched more than one peer: routing must fail
    /// closed rather than guess a recipient.
    #[error("ambiguous peer reference: `{reference}` matches more than one peer")]
    AmbiguousPeerRef { reference: String },

    /// Schema migration failure.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// A task failed to join.
    #[error("task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Result alias for store operations.
pub type Result<T> = std::result::Result<T, StoreError>;
