//! Stop-reason → wrkspc lifecycle mapping.
//!
//! ACP v1 defines exactly five stop reasons (agent-client-protocol-schema
//! 1.9.1, `StopReason`): `EndTurn`, `MaxTokens`, `MaxTurnRequests`,
//! `Refusal`, `Cancelled`. The mapping mirrors HLD Appendix A:
//! `EndTurn` → completed, `Cancelled` → cancelled, everything else → failed
//! with the reason recorded. Note that a completed *turn* is not proof of
//! task success — the D-3 success predicate is a separate check the
//! dispatcher applies.

use agent_client_protocol::v1::schema::StopReason;
use wrkspc_core::AttemptState;

/// Maps an ACP stop reason to the wrkspc attempt state it implies.
///
/// # Arguments
///
/// * `reason` — the stop reason from the `session/prompt` response.
///
/// # Returns
///
/// The attempt state: [`AttemptState::Completed`] for `EndTurn`,
/// [`AttemptState::Canceled`] for `Cancelled`, [`AttemptState::Failed`]
/// otherwise.
pub fn stop_reason_to_attempt_state(reason: &StopReason) -> AttemptState {
    match reason {
        StopReason::EndTurn => AttemptState::Completed,
        StopReason::Cancelled => AttemptState::Canceled,
        StopReason::MaxTokens | StopReason::MaxTurnRequests | StopReason::Refusal => {
            AttemptState::Failed
        }
        // StopReason is non-exhaustive: a future reason is treated
        // conservatively — a turn that did not end with EndTurn is never
        // Completed.
        _ => AttemptState::Failed,
    }
}

/// Returns the DB literal for a stop reason (the attempt `stop_reason`
/// column), so outcome provenance is preserved on every terminal state.
///
/// # Arguments
///
/// * `reason` — the stop reason from the `session/prompt` response.
pub fn stop_reason_str(reason: &StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::Cancelled => "cancelled",
        StopReason::MaxTokens => "max_tokens",
        StopReason::MaxTurnRequests => "max_turn_requests",
        StopReason::Refusal => "refusal",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_all_five_stop_reasons() {
        assert_eq!(
            stop_reason_to_attempt_state(&StopReason::EndTurn),
            AttemptState::Completed
        );
        assert_eq!(
            stop_reason_to_attempt_state(&StopReason::Cancelled),
            AttemptState::Canceled
        );
        for reason in [
            StopReason::MaxTokens,
            StopReason::MaxTurnRequests,
            StopReason::Refusal,
        ] {
            assert_eq!(
                stop_reason_to_attempt_state(&reason),
                AttemptState::Failed,
                "{reason:?} must map to Failed"
            );
        }
    }

    #[test]
    fn stop_reason_literals_are_stable() {
        assert_eq!(stop_reason_str(&StopReason::EndTurn), "end_turn");
        assert_eq!(stop_reason_str(&StopReason::Cancelled), "cancelled");
        assert_eq!(stop_reason_str(&StopReason::MaxTokens), "max_tokens");
        assert_eq!(
            stop_reason_str(&StopReason::MaxTurnRequests),
            "max_turn_requests"
        );
        assert_eq!(stop_reason_str(&StopReason::Refusal), "refusal");
    }
}
