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
        let mut tx = self.pool.begin().await?;

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
            Reconciliation::Close { state } => format!("close:{}", state.as_str()),
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
                debug_assert!(state.is_terminal());
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
                    _ => unreachable!("terminal assertion above"),
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
