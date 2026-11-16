//! Wrkspc lifecycle → A2A task-state mapping.
//!
//! Mirrors HLD Appendix A's lifecycle table exactly. The wire literals are
//! the `TASK_STATE_*` enum strings; `unspecified` is reserved for the zero
//! value and `unknown` is deliberately hub-internal — it is never emitted,
//! so this module returns no state for it (the submitter sees the last
//! durable state with a staleness marker instead).

use a2a::TaskState;

/// The wrkspc-side lifecycle states (HLD Appendix A, document-side names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    /// Message/intent committed, prompt not dispatched.
    Queued,
    /// Committed but moderation-gated, not eligible for delivery.
    Held,
    /// Rejected at acceptance (policy or moderation).
    Blocked,
    /// Attempt persisted before writing the ACP prompt.
    Dispatching,
    /// Attributable session update establishes activity.
    Running,
    /// An outstanding permission decision blocks progress. The wire
    /// state is `input-required`, or `auth-required` when the decision
    /// is auth-bound (HLD Appendix A).
    AwaitingApproval { auth_bound: bool },
    /// The success-evidence predicate holds and results are durably
    /// recorded. A turn ending is never sufficient on its own.
    Completed,
    /// Known terminal failure.
    Failed,
    /// Known terminal cancellation.
    Canceled,
    /// Known terminal expiry (the deadline passed before an outcome).
    /// The A2A wire has no expired state, so the table's
    /// failed/cancelled/expired bucket emits `failed` — report, do not
    /// silently retry (HLD Appendix A).
    Expired,
    /// Dispatch or completion may have occurred — hub-internal, never
    /// emitted to the submitting agent.
    Unknown,
}

/// Maps a wrkspc lifecycle state to the A2A task state the submitting agent
/// sees.
///
/// * `state` — the wrkspc-side lifecycle state.
///
/// # Returns
///
/// The agent-facing `TaskState`, or `None` for [`LifecycleState::Unknown`]
/// (hub-internal; the caller must present the last durable state instead).
pub fn to_task_state(state: LifecycleState) -> Option<TaskState> {
    match state {
        LifecycleState::Queued | LifecycleState::Held | LifecycleState::Dispatching => {
            Some(TaskState::Submitted)
        }
        LifecycleState::Blocked => Some(TaskState::Rejected),
        LifecycleState::Running => Some(TaskState::Working),
        LifecycleState::AwaitingApproval { auth_bound: false } => Some(TaskState::InputRequired),
        LifecycleState::AwaitingApproval { auth_bound: true } => Some(TaskState::AuthRequired),
        LifecycleState::Completed => Some(TaskState::Completed),
        LifecycleState::Failed | LifecycleState::Expired => Some(TaskState::Failed),
        LifecycleState::Canceled => Some(TaskState::Canceled),
        LifecycleState::Unknown => None,
    }
}

/// Returns the wire literal for a task state (the `TASK_STATE_*` strings).
///
/// * `state` — the A2A task state.
pub fn task_state_literal(state: &TaskState) -> &'static str {
    match state {
        TaskState::Unspecified => "TASK_STATE_UNSPECIFIED",
        TaskState::Submitted => "TASK_STATE_SUBMITTED",
        TaskState::Working => "TASK_STATE_WORKING",
        TaskState::InputRequired => "TASK_STATE_INPUT_REQUIRED",
        TaskState::Completed => "TASK_STATE_COMPLETED",
        TaskState::Failed => "TASK_STATE_FAILED",
        TaskState::Canceled => "TASK_STATE_CANCELED",
        TaskState::Rejected => "TASK_STATE_REJECTED",
        TaskState::AuthRequired => "TASK_STATE_AUTH_REQUIRED",
    }
}

/// Parses a wire literal back into a task state.
///
/// * `literal` — the `TASK_STATE_*` string.
///
/// # Returns
///
/// The parsed state, or `None` for unknown literals.
pub fn task_state_from_literal(literal: &str) -> Option<TaskState> {
    match literal {
        "TASK_STATE_UNSPECIFIED" => Some(TaskState::Unspecified),
        "TASK_STATE_SUBMITTED" => Some(TaskState::Submitted),
        "TASK_STATE_WORKING" => Some(TaskState::Working),
        "TASK_STATE_INPUT_REQUIRED" => Some(TaskState::InputRequired),
        "TASK_STATE_COMPLETED" => Some(TaskState::Completed),
        "TASK_STATE_FAILED" => Some(TaskState::Failed),
        "TASK_STATE_CANCELED" => Some(TaskState::Canceled),
        "TASK_STATE_REJECTED" => Some(TaskState::Rejected),
        "TASK_STATE_AUTH_REQUIRED" => Some(TaskState::AuthRequired),
        _ => None,
    }
}
