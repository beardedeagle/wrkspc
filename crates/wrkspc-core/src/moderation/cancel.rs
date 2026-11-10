//! Cancel requests and open-attempt reconciliation.

use super::{CancelOutcome, OpenAttempt};
use crate::error::{Result, StoreError};
use crate::store::Store;
use chrono::Utc;
use serde_json::json;
use sqlx::Row;

impl Store {
    pub async fn request_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        set_by: &str,
    ) -> Result<CancelOutcome> {
        let now = Utc::now().to_rfc3339();
        // Wait for the WAL writer before reading; a deferred read snapshot
        // cannot be promoted safely if another connection commits first.
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query(
            "SELECT i.id AS intent_id, i.state, m.conversation_id
             FROM execution_intents i
             JOIN messages m ON m.id = i.message_id
             WHERE i.message_id = ? AND i.recipient_id = ?",
        )
        .bind(message_id)
        .bind(recipient_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution intent",
            id: format!("{message_id}/{recipient_id}"),
        })?;
        let intent_id: String = row.get("intent_id");
        let intent_state: String = row.get("state");
        let conversation_id: String = row.get("conversation_id");

        let open = sqlx::query(
            "SELECT id FROM attempts
             WHERE intent_id = ? AND state IN ('claimed', 'dispatched', 'unknown')
             ORDER BY claimed_at DESC LIMIT 1",
        )
        .bind(&intent_id)
        .fetch_optional(&mut *tx)
        .await?;

        let outcome = match (intent_state.as_str(), open) {
            ("completed" | "failed" | "canceled", _) => {
                return Err(StoreError::IntentTerminal {
                    intent: intent_id,
                    state: intent_state,
                });
            }
            ("queued", None) => {
                sqlx::query("UPDATE execution_intents SET state = 'canceled' WHERE id = ?")
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
                self.append_event_in(
                    &mut tx,
                    &conversation_id,
                    "intent.canceled",
                    &json!({
                        "intent_id": intent_id,
                        "message_id": message_id,
                        "recipient_id": recipient_id,
                        "set_by": set_by,
                    }),
                )
                .await?;
                CancelOutcome::IntentCanceled
            }
            (_, Some(row)) => {
                let attempt_id: String = row.get("id");
                sqlx::query(
                    "UPDATE attempts
                     SET cancel_requested = 1,
                         cancel_requested_at = ?,
                         cancel_requested_by = ?
                     WHERE id = ?",
                )
                .bind(&now)
                .bind(set_by)
                .bind(&attempt_id)
                .execute(&mut *tx)
                .await?;
                self.append_event_in(
                    &mut tx,
                    &conversation_id,
                    "cancel.requested",
                    &json!({
                        "attempt_id": attempt_id,
                        "intent_id": intent_id,
                        "set_by": set_by,
                    }),
                )
                .await?;
                CancelOutcome::Attempt { attempt_id }
            }
            (other, None) => {
                return Err(StoreError::BlockedByUnknown {
                    intent: intent_id,
                    state: other.to_string(),
                });
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    /// Lists open attempts (claimed/dispatched) for restart reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn open_attempts(&self) -> Result<Vec<OpenAttempt>> {
        let rows = sqlx::query(
            "SELECT a.id AS attempt_id, a.intent_id, a.state,
                    i.message_id, i.recipient_id, m.conversation_id
             FROM attempts a
             JOIN execution_intents i ON i.id = a.intent_id
             JOIN messages m ON m.id = i.message_id
             WHERE a.state IN ('claimed', 'dispatched')
             ORDER BY a.claimed_at ASC",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| OpenAttempt {
                attempt_id: r.get("attempt_id"),
                intent_id: r.get("intent_id"),
                message_id: r.get("message_id"),
                recipient_id: r.get("recipient_id"),
                conversation_id: r.get("conversation_id"),
                state: r.get("state"),
            })
            .collect())
    }

    /// Whether a cancel request is recorded on an attempt.
    ///
    /// # Arguments
    ///
    /// * `attempt_id` — the attempt to inspect.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the
    /// attempt does not exist, [`StoreError::Db`](StoreError::Db) on
    /// failure.
    pub async fn attempt_cancel_requested(&self, attempt_id: &str) -> Result<bool> {
        let flag: i64 = sqlx::query_scalar("SELECT cancel_requested FROM attempts WHERE id = ?")
            .bind(attempt_id)
            .fetch_one(self.pool())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => StoreError::NotFound {
                    entity: "attempt",
                    id: attempt_id.to_string(),
                },
                other => other.into(),
            })?;
        Ok(flag != 0)
    }
}
