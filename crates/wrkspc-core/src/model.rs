//! wrkspc-core domain model: shared identifiers, states, and constants.
//!
//! The database CHECK constraints are the source of truth for state
//! machines; these enums document the same vocabulary for the Rust side.
//! Keep both in sync deliberately.

/// The host identity stamped on every record. Constant for the single-host
/// pilot (D-6's day-1 requirement); team mode stamps real host identities
/// with zero schema migration.
pub const PILOT_HOST_ID: &str = "wrkspc-pilot-host";

/// The presence live-state vocabulary (HLD rev0.9 first cut; idle/busy
/// arrive with dispatcher in-flight wiring).
pub const PRESENCE_STATES: [&str; 3] = ["connected", "disconnected", "unknown"];

/// Returns `true` when `state` is a known presence live state.
pub fn is_presence_state(state: &str) -> bool {
    PRESENCE_STATES.contains(&state)
}

/// Generated identifiers are UUID v4 strings.
pub type Id = String;

/// Timestamps are RFC 3339 UTC strings.
pub type Timestamp = String;

/// Participant class, matching the HLD's two-class model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerClass {
    /// Speaks A2A directly; the hub coordinates on its behalf.
    A2aNative,
    /// Resides in a harness; wrkspc exposes MCP tools to it and speaks A2A
    /// on its behalf.
    Harness,
    /// Human operator.
    Human,
}

impl PeerClass {
    /// Returns the wire/DB literal for this class.
    pub fn as_str(self) -> &'static str {
        match self {
            PeerClass::A2aNative => "a2a_native",
            PeerClass::Harness => "harness",
            PeerClass::Human => "human",
        }
    }
}

/// Delivery gate state per message recipient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryState {
    /// Committed; the moderation fence has not been evaluated yet.
    Stored,
    /// Moderation-gated; not eligible for delivery.
    Held,
    /// Blocked outright by an operator.
    Blocked,
    /// Cleared for delivery.
    Eligible,
    /// Delivered to the recipient's mailbox.
    Delivered,
    /// Expired under retention policy.
    Expired,
}

impl DeliveryState {
    /// Returns the DB literal for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryState::Stored => "stored",
            DeliveryState::Held => "held",
            DeliveryState::Blocked => "blocked",
            DeliveryState::Eligible => "eligible",
            DeliveryState::Delivered => "delivered",
            DeliveryState::Expired => "expired",
        }
    }
}

/// Execution intent lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentState {
    Queued,
    Claimed,
    Dispatched,
    Completed,
    Failed,
    Canceled,
}

impl IntentState {
    /// Returns the DB literal for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            IntentState::Queued => "queued",
            IntentState::Claimed => "claimed",
            IntentState::Dispatched => "dispatched",
            IntentState::Completed => "completed",
            IntentState::Failed => "failed",
            IntentState::Canceled => "canceled",
        }
    }

    /// Whether this intent state blocks new dispatch work.
    pub fn blocks_new_work(self) -> bool {
        matches!(self, IntentState::Claimed)
    }
}

/// Attempt lifecycle. Terminal states are immutable: re-applying the same
/// terminal state is allowed (idempotent re-emission), a different state is
/// rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    Claimed,
    Dispatched,
    /// Dispatch or completion may have occurred; operator reconciliation
    /// required before the session may run new work.
    Unknown,
    Completed,
    Failed,
    Canceled,
}

impl AttemptState {
    /// Returns the DB literal for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            AttemptState::Claimed => "claimed",
            AttemptState::Dispatched => "dispatched",
            AttemptState::Unknown => "unknown",
            AttemptState::Completed => "completed",
            AttemptState::Failed => "failed",
            AttemptState::Canceled => "canceled",
        }
    }

    /// Whether this attempt state is terminal (immutable).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            AttemptState::Completed | AttemptState::Failed | AttemptState::Canceled
        )
    }
}

/// Operator decision resolving an unknown attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciliation {
    /// The attempt is closed as failed and the intent returns to `queued`,
    /// allowing a fresh attempt under explicit operator authorization.
    Retry,
    /// The attempt is closed with the given terminal state.
    Close { state: AttemptState },
}

/// Moderation fence kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceKind {
    /// Conversation paused: no new delivery, in-flight turns may finish.
    Pause,
    /// Peer blocked: no delivery to this peer at all.
    Block,
    /// Message held: this specific delivery is gated pending operator review.
    Hold,
}

impl FenceKind {
    /// Returns the DB literal for this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            FenceKind::Pause => "pause",
            FenceKind::Block => "block",
            FenceKind::Hold => "hold",
        }
    }

    /// Parses a DB literal back into a fence kind.
    ///
    /// * `literal` — the kind literal (`pause`/`hold`/`block`).
    ///
    /// # Returns
    ///
    /// The parsed kind, or `None` for unknown literals.
    pub fn from_literal(literal: &str) -> Option<Self> {
        match literal {
            "pause" => Some(FenceKind::Pause),
            "block" => Some(FenceKind::Block),
            "hold" => Some(FenceKind::Hold),
            _ => None,
        }
    }
}
