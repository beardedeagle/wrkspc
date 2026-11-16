//! The HLD Appendix A lifecycle → A2A task-state table, as a contract
//! test: this is the mapping the submitting agent sees on the wire, so
//! a drift from the documented table fails here before it ships.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use a2a::TaskState;
use wrkspc_a2a::state_mapping::{LifecycleState, task_state_from_literal, task_state_literal, to_task_state};

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
        to_task_state(LifecycleState::AwaitingApproval { auth_bound: false }),
        Some(TaskState::InputRequired)
    );
    // The HLD's auth-bound branch: an approval that requires
    // authentication surfaces as auth-required.
    assert_eq!(
        to_task_state(LifecycleState::AwaitingApproval { auth_bound: true }),
        Some(TaskState::AuthRequired)
    );
    assert_eq!(
        to_task_state(LifecycleState::Completed),
        Some(TaskState::Completed)
    );
    assert_eq!(to_task_state(LifecycleState::Failed), Some(TaskState::Failed));
    assert_eq!(
        to_task_state(LifecycleState::Canceled),
        Some(TaskState::Canceled)
    );
    // The wire has no expired state: the table buckets it with failed.
    assert_eq!(
        to_task_state(LifecycleState::Expired),
        Some(TaskState::Failed)
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
