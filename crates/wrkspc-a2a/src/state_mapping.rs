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
    /// An outstanding permission decision blocks progress.
    AwaitingApproval,
    /// The success-evidence predicate holds and results are durably
    /// recorded. A turn ending is never sufficient on its own.
    Completed,
    /// Known terminal failure.
    Failed,
    /// Known terminal cancellation.
    Canceled,
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
        LifecycleState::AwaitingApproval => Some(TaskState::InputRequired),
        LifecycleState::Completed => Some(TaskState::Completed),
        LifecycleState::Failed => Some(TaskState::Failed),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_lifecycle_per_hld_table() {
        // submitted bucket
        for state in [
            LifecycleState::Queued,
            LifecycleState::Held,
            LifecycleState::Dispatching,
        ] {
            assert_eq!(
                to_task_state(state),
                Some(TaskState::Submitted),
                "{state:?} must map to submitted"
            );
        }
        assert_eq!(
            to_task_state(LifecycleState::Blocked),
            Some(TaskState::Rejected)
        );
        assert_eq!(
            to_task_state(LifecycleState::Running),
            Some(TaskState::Working)
        );
        assert_eq!(
            to_task_state(LifecycleState::AwaitingApproval),
            Some(TaskState::InputRequired)
        );
        assert_eq!(
            to_task_state(LifecycleState::Completed),
            Some(TaskState::Completed)
        );
        assert_eq!(
            to_task_state(LifecycleState::Failed),
            Some(TaskState::Failed)
        );
        assert_eq!(
            to_task_state(LifecycleState::Canceled),
            Some(TaskState::Canceled)
        );
    }

    #[test]
    fn unknown_is_never_emitted() {
        assert_eq!(to_task_state(LifecycleState::Unknown), None);
    }

    #[test]
    fn literals_round_trip_all_nine_states() {
        let states = [
            TaskState::Unspecified,
            TaskState::Submitted,
            TaskState::Working,
            TaskState::InputRequired,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Canceled,
            TaskState::Rejected,
            TaskState::AuthRequired,
        ];
        for state in states {
            let literal = task_state_literal(&state);
            assert_eq!(
                task_state_from_literal(literal),
                Some(state),
                "{literal} must round-trip"
            );
        }
        assert_eq!(task_state_from_literal("TASK_STATE_BOGUS"), None);
    }
}
