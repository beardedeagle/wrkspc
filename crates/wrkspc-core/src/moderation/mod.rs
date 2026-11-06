//! Moderation-engine store operations: dispatch eligibility, the held
//! queue, cancel requests, operator guidance, and startup reconciliation.
//!
//! Every operation is protocol-agnostic; the dispatcher crate maps these
//! onto sessions. Fences gate delivery here — in the claim path itself —
//! so no caller can bypass moderation.

pub mod cancel;
pub mod guidance;
pub mod holds;

/// A claimable execution intent (the dispatcher's unit of work).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueIntent {
    /// Intent id.
    pub intent_id: String,
    /// Owning message.
    pub message_id: String,
    /// Recipient peer id (the dispatch target).
    pub recipient_id: String,
    /// Owning conversation.
    pub conversation_id: String,
    /// Message content (the prompt text).
    pub content: String,
}

/// The outcome of a cancel request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The intent was still queued (no attempt); it is now canceled.
    IntentCanceled,
    /// An open attempt exists; the request is recorded on it and the
    /// dispatcher interrupts the live session.
    Attempt { attempt_id: String },
}

/// An open (non-terminal) attempt, used for restart reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAttempt {
    /// Attempt id.
    pub attempt_id: String,
    /// Owning intent.
    pub intent_id: String,
    /// Owning message.
    pub message_id: String,
    /// Target peer.
    pub recipient_id: String,
    /// Owning conversation.
    pub conversation_id: String,
    /// Attempt state (`claimed` or `dispatched`).
    pub state: String,
}

/// An operator guidance entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guidance {
    /// Guidance id.
    pub id: String,
    /// Owning conversation.
    pub conversation_id: String,
    /// Authoring peer (operator).
    pub author_id: String,
    /// Guidance text.
    pub text: String,
    /// RFC 3339 UTC creation time.
    pub created_at: String,
}
