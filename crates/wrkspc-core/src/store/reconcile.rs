//! Operator reconciliation of unknown attempts.

use super::Store;
use crate::error::{Result, StoreError};
use crate::model::AttemptState;
use crate::model::IntentState;
use crate::model::Reconciliation;
use chrono::Utc;
use sqlx::Row;

impl Store {
    /// Resolves an unknown attempt under explicit operator authorization.
    ///
    /// `Retry` closes the attempt as failed (reason `operator-retry`) and
    /// returns the intent to `queued` so a fresh attempt may be claimed.
    /// `Close` records the given terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the
    /// attempt does not exist or is not in the `unknown` state.
    pub async fn reconcile_unknown(
        &self,
        attempt_id: &str,
        decision: Reconciliation,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        // IMMEDIATE: the reconcile is a read-modify-write over attempts
        // and intents — the snapshot must not be promotable under a
        // concurrent writer (deferred reads surface SQLITE_BUSY_SNAPSHOT
        // on the write upgrade).
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let row = sqlx::query(
            "SELECT a.intent_id, a.state, m.conversation_id
             FROM attempts a
             JOIN execution_intents ei ON ei.id = a.intent_id
             JOIN messages m ON m.id = ei.message_id
             WHERE a.id = ?",
        )
        .bind(attempt_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "attempt",
            id: attempt_id.to_string(),
        })?;
        let intent_id: String = row.get("intent_id");
        let current: String = row.get("state");
        let conversation_id: String = row.get("conversation_id");
        if current != "unknown" {
            return Err(StoreError::NotFound {
                entity: "unknown attempt",
                id: attempt_id.to_string(),
            });
        }

        let decision_label = match &decision {
            Reconciliation::Retry => "retry".to_string(),
            Reconciliation::Close { state } => {
                // A non-terminal Close is a caller bug (the operator
                // surface only offers terminal states), but it must be a
                // typed error, never a release-build panic: the
                // debug_assert below evaporates under --release and the
                // unreachable arm would fire in production.
                if !state.is_terminal() {
                    return Err(StoreError::TerminalState {
                        attempt: attempt_id.to_string(),
                        state: "unknown".to_string(),
                        requested: state.as_str().to_string(),
                    });
                }
                format!("close:{}", state.as_str())
            }
        };

        match decision {
            Reconciliation::Retry => {
                sqlx::query(
                    "UPDATE attempts
                     SET state = 'failed', stop_reason = 'operator-retry', terminal_at = ?
                     WHERE id = ?",
                )
                .bind(&now)
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
                sqlx::query("UPDATE execution_intents SET state = 'queued' WHERE id = ?")
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
            }
            Reconciliation::Close { state } => {
                sqlx::query(
                    "UPDATE attempts
                     SET state = ?, stop_reason = 'operator-reconciled', terminal_at = ?
                     WHERE id = ?",
                )
                .bind(state.as_str())
                .bind(&now)
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
                let intent_state = match state {
                    AttemptState::Completed => IntentState::Completed,
                    AttemptState::Failed => IntentState::Failed,
                    AttemptState::Canceled => IntentState::Canceled,
                    // Unreachable by the typed guard above; a defensive
                    // error keeps this a no-panic path regardless.
                    other => {
                        return Err(StoreError::TerminalState {
                            attempt: attempt_id.to_string(),
                            state: "unknown".to_string(),
                            requested: other.as_str().to_string(),
                        });
                    }
                };
                sqlx::query("UPDATE execution_intents SET state = ? WHERE id = ?")
                    .bind(intent_state.as_str())
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        // The recovery action is committed into the feed in the same
        // transaction: closing a visible unknown without a record would
        // leave the resumable history with a gap exactly where the
        // operator's authority was exercised.
        self.append_event_in(
            &mut tx,
            &conversation_id,
            "attempt.reconciled",
            &serde_json::json!({
                "attempt_id": attempt_id,
                "decision": decision_label,
                "by": "operator",
            }),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
