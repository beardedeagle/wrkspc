//! Execution attempts: claim and lifecycle transitions.

use super::Store;
use crate::error::{Result, StoreError};
use crate::model::AttemptState;
use crate::model::IntentState;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

impl Store {
    /// Claims an execution intent for dispatch, creating an attempt row
    /// BEFORE any harness write.
    ///
    /// Idempotent: claiming an intent that already has an open claimed
    /// or dispatched attempt returns that attempt instead of creating
    /// a second one; an unknown attempt blocks until reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::BlockedByUnknown`]
    /// when a prior attempt on this intent is unknown (operator
    /// reconciliation required first), and
    /// [`StoreError::IntentTerminal`] when the
    /// intent already reached a terminal state.
    pub async fn claim_intent(&self, message_id: &str, recipient_id: &str) -> Result<String> {
        Store::retry_on_busy(
            || self.claim_intent_inner(message_id, recipient_id),
            "claim_intent",
        )
        .await
    }

    async fn claim_intent_inner(&self, message_id: &str, recipient_id: &str) -> Result<String> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;

        let intent_row = sqlx::query(
            "SELECT id, state FROM execution_intents
             WHERE message_id = ? AND recipient_id = ?",
        )
        .bind(message_id)
        .bind(recipient_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution intent",
            id: format!("{message_id}/{recipient_id}"),
        })?;
        let intent_id: String = intent_row.get("id");
        let intent_state: String = intent_row.get("state");

        // An open, non-unknown attempt on this intent is returned as-is
        // (idempotent claim). An unknown attempt BLOCKS: its outcome is
        // undetermined and operator reconciliation is required first.
        if let Some(row) = sqlx::query(
            "SELECT id, state FROM attempts
             WHERE intent_id = ? AND state IN ('claimed', 'dispatched', 'unknown')
             ORDER BY claimed_at DESC LIMIT 1",
        )
        .bind(&intent_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let open_id: String = row.get("id");
            let open_state: String = row.get("state");
            match open_state.as_str() {
                "claimed" | "dispatched" => return Ok(open_id),
                "unknown" => {
                    return Err(StoreError::BlockedByUnknown {
                        intent: intent_id,
                        state: "unknown".to_string(),
                    });
                }
                other => {
                    return Err(StoreError::BlockedByUnknown {
                        intent: intent_id,
                        state: other.to_string(),
                    });
                }
            }
        }

        // Session-wide quarantine: an unresolved (unknown) attempt on
        // ANY intent for this recipient blocks new work for the whole
        // recipient — the prior turn's side effects are undeterminable
        // and must not overlap with new execution. The barrier is
        // durable (attempt rows survive restart) and released only by
        // operator reconciliation.
        if let Some(row) = sqlx::query(
            "SELECT i2.id AS intent_id
             FROM attempts a
             JOIN execution_intents i2 ON i2.id = a.intent_id
             WHERE i2.recipient_id = ? AND a.state = 'unknown'
             LIMIT 1",
        )
        .bind(recipient_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            return Err(StoreError::BlockedByUnknown {
                intent: row.get("intent_id"),
                state: "unknown".to_string(),
            });
        }

        match intent_state.as_str() {
            "queued" => {}
            "claimed" => {
                // Intent is claimed but no open attempt row exists — recover
                // by re-attaching: treat as queued (crash between claim and
                // commit can't happen; this closes the window defensively).
            }
            "completed" | "failed" | "canceled" => {
                return Err(StoreError::IntentTerminal {
                    intent: intent_id,
                    state: intent_state,
                });
            }
            "dispatched" => {
                // Dispatched with no open attempt row is an inconsistent
                // store; block rather than double-dispatch.
                return Err(StoreError::BlockedByUnknown {
                    intent: intent_id,
                    state: intent_state,
                });
            }
            other => {
                return Err(StoreError::BlockedByUnknown {
                    intent: intent_id,
                    state: other.to_string(),
                });
            }
        }

        // Gate at the claim — the moderation linearization point: an
        // active pause/hold/block fence, a held/blocked/expired
        // recipient delivery state, or an ejected recipient peer each
        // block the claim in this transaction. Enumeration is not the
        // gate; the claim is, so a hold committed after `due_intents`
        // still stops dispatch here.
        if let Some((conversation_id, kinds)) =
            crate::moderation::holds::delivery_gate_in_tx(&mut tx, message_id, recipient_id).await?
        {
            return Err(StoreError::BlockedByFence {
                conversation_id,
                peer_id: recipient_id.to_string(),
                kinds,
            });
        }

        let attempt_id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO attempts (id, intent_id, state, claimed_at)
             VALUES (?, ?, 'claimed', ?)",
        )
        .bind(&attempt_id)
        .bind(&intent_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE execution_intents SET state = 'claimed' WHERE id = ?")
            .bind(&intent_id)
            .execute(&mut *tx)
            .await?;

        let conversation_id: String =
            sqlx::query("SELECT conversation_id FROM messages WHERE id = ?")
                .bind(message_id)
                .fetch_one(&mut *tx)
                .await?
                .get("conversation_id");
        self.append_event_in(
            &mut tx,
            &conversation_id,
            "attempt.claimed",
            &serde_json::json!({
                "attempt_id": attempt_id,
                "intent_id": intent_id,
                "message_id": message_id,
                "recipient_id": recipient_id,
            }),
        )
        .await?;

        tx.commit().await?;
        Ok(attempt_id)
    }

    /// Records a delivery fence-ack: the recipient's node durably
    /// recorded the delivery locally (D-6 A2 contract — the ack rides
    /// the delivery task to terminal). Idempotent: a delivery already
    /// marked `delivered` acks again without error.
    ///
    /// * `message_id` — the hub-committed message.
    /// * `peer_id` — the recipient whose delivery is acked.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the
    /// recipient row does not exist,
    /// [`StoreError::TerminalState`] when the delivery is already
    /// in a state that cannot be marked delivered, and
    /// [`StoreError::Db`] on failure.
    pub async fn mark_delivered(&self, message_id: &str, peer_id: &str) -> Result<()> {
        Store::retry_on_busy(
            || self.mark_delivered_inner(message_id, peer_id),
            "mark_delivered",
        )
        .await
    }

    async fn mark_delivered_inner(&self, message_id: &str, peer_id: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE message_recipients SET delivery_state = 'delivered'
             WHERE message_id = ? AND peer_id = ?
               AND delivery_state IN ('stored', 'eligible', 'dispatched')",
        )
        .bind(message_id)
        .bind(peer_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            // Distinguish "already delivered" (idempotent ack) from an
            // unknown recipient row.
            let state: Option<String> = sqlx::query(
                "SELECT delivery_state FROM message_recipients
                 WHERE message_id = ? AND peer_id = ?",
            )
            .bind(message_id)
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await?
            .map(|r| r.get("delivery_state"));
            match state.as_deref() {
                Some("delivered") => {}
                Some(other) => {
                    return Err(StoreError::TerminalState {
                        attempt: format!("{message_id}→{peer_id}"),
                        state: other.to_string(),
                        requested: "delivered".to_string(),
                    });
                }
                None => {
                    return Err(StoreError::NotFound {
                        entity: "message recipient",
                        id: format!("{message_id}→{peer_id}"),
                    });
                }
            }
        }
        Ok(())
    }

    /// Marks a claimed attempt as dispatched (the harness write happened).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the
    /// attempt does not exist, and
    /// [`StoreError::TerminalState`] when it is
    /// already terminal.
    pub async fn mark_dispatched(&self, attempt_id: &str) -> Result<()> {
        Store::retry_on_busy(|| self.mark_dispatched_inner(attempt_id), "mark_dispatched").await
    }

    async fn mark_dispatched_inner(&self, attempt_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;

        let (intent_id, state): (String, String) =
            sqlx::query("SELECT intent_id, state FROM attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_optional(&mut *tx)
                .await?
                .map(|r| (r.get("intent_id"), r.get("state")))
                .ok_or_else(|| StoreError::NotFound {
                    entity: "attempt",
                    id: attempt_id.to_string(),
                })?;

        match state.as_str() {
            "dispatched" => return Ok(()), // idempotent
            "claimed" | "unknown" => {}
            other => {
                return Err(StoreError::TerminalState {
                    attempt: attempt_id.to_string(),
                    state: other.to_string(),
                    requested: "dispatched".to_string(),
                });
            }
        }

        sqlx::query("UPDATE attempts SET state = 'dispatched', dispatched_at = ? WHERE id = ?")
            .bind(&now)
            .bind(attempt_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE execution_intents SET state = 'dispatched' WHERE id = ?")
            .bind(&intent_id)
            .execute(&mut *tx)
            .await?;
        // Delivery accounting: the recipient's delivery is now delivered.
        let (message_id, recipient_id): (String, String) =
            sqlx::query("SELECT message_id, recipient_id FROM execution_intents WHERE id = ?")
                .bind(&intent_id)
                .fetch_one(&mut *tx)
                .await
                .map(|r| (r.get("message_id"), r.get("recipient_id")))?;
        sqlx::query(
            "UPDATE message_recipients SET delivery_state = 'delivered'
             WHERE message_id = ? AND peer_id = ?
               AND delivery_state IN ('stored', 'eligible')",
        )
        .bind(&message_id)
        .bind(&recipient_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Marks an attempt unknown: dispatch or completion may have occurred
    /// but the outcome cannot be established. The intent stays claimed, so
    /// new work on the session is blocked until reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the
    /// attempt does not exist, and
    /// [`StoreError::TerminalState`] when it is
    /// already terminal (unknown can't supersede a terminal state).
    pub async fn mark_unknown(&self, attempt_id: &str, reason: &str) -> Result<()> {
        Store::retry_on_busy(
            || self.mark_unknown_inner(attempt_id, reason),
            "mark_unknown",
        )
        .await
    }

    async fn mark_unknown_inner(&self, attempt_id: &str, reason: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let state: Option<String> = sqlx::query("SELECT state FROM attempts WHERE id = ?")
            .bind(attempt_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.get("state"));
        let state = state.ok_or_else(|| StoreError::NotFound {
            entity: "attempt",
            id: attempt_id.to_string(),
        })?;
        if state != "claimed" && state != "dispatched" && state != "unknown" {
            return Err(StoreError::TerminalState {
                attempt: attempt_id.to_string(),
                state,
                requested: "unknown".to_string(),
            });
        }
        // An attempt that went unknown never delivered its guidance:
        // release any claim (C3) so the next usable turn re-rides it.
        sqlx::query("UPDATE guidance SET claimed_attempt = NULL WHERE claimed_attempt = ?")
            .bind(attempt_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE attempts SET state = 'unknown', stop_reason = ? WHERE id = ?")
            .bind(reason)
            .bind(attempt_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Records a terminal state for an attempt and its intent.
    ///
    /// Idempotent re-emission of the same terminal state is allowed; a
    /// different terminal state is rejected.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the
    /// attempt does not exist, and
    /// [`StoreError::TerminalState`] on a state
    /// conflict.
    pub async fn mark_terminal(
        &self,
        attempt_id: &str,
        state: AttemptState,
        stop_reason: Option<&str>,
        result_ref: Option<&str>,
    ) -> Result<()> {
        Store::retry_on_busy(
            || self.mark_terminal_inner(attempt_id, state, stop_reason, result_ref),
            "mark_terminal",
        )
        .await
    }

    async fn mark_terminal_inner(
        &self,
        attempt_id: &str,
        state: AttemptState,
        stop_reason: Option<&str>,
        result_ref: Option<&str>,
    ) -> Result<()> {
        debug_assert!(
            state.is_terminal(),
            "mark_terminal requires a terminal state"
        );
        if !state.is_terminal() {
            return Err(StoreError::TerminalState {
                attempt: attempt_id.to_string(),
                state: "non-terminal".to_string(),
                requested: state.as_str().to_string(),
            });
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query("SELECT intent_id, state FROM attempts WHERE id = ?")
            .bind(attempt_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "attempt",
                id: attempt_id.to_string(),
            })?;
        let intent_id: String = row.get("intent_id");
        let current: String = row.get("state");

        if current == state.as_str() {
            return Ok(()); // idempotent re-emission
        }
        if current == "completed" || current == "failed" || current == "canceled" {
            return Err(StoreError::TerminalState {
                attempt: attempt_id.to_string(),
                state: current,
                requested: state.as_str().to_string(),
            });
        }

        sqlx::query(
            "UPDATE attempts
             SET state = ?, stop_reason = COALESCE(?, stop_reason),
                 result_ref = COALESCE(?, result_ref), terminal_at = ?
             WHERE id = ?",
        )
        .bind(state.as_str())
        .bind(stop_reason)
        .bind(result_ref)
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
        tx.commit().await?;
        Ok(())
    }

    /// Closes an attempt that a moderation gate stopped BEFORE any
    /// prompt: the attempt is recorded canceled (the harness never
    /// ran), and its intent returns to the queue so the delivery stays
    /// deliverable once the gate clears.
    ///
    /// Unlike [`Store::mark_terminal`] with
    /// `Canceled`, the intent is NOT terminal — a gate is not an
    /// operator cancellation, and the delivery must not be lost.
    ///
    /// * `attempt_id` — the gated attempt.
    /// * `reason` — the recorded gate description.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the
    /// attempt does not exist,
    /// [`StoreError::TerminalState`] when it
    /// already left the pre-dispatch states, and
    /// [`StoreError::Db`] on failure.
    pub async fn gate_attempt(&self, attempt_id: &str, reason: &str) -> Result<()> {
        Store::retry_on_busy(
            || self.gate_attempt_inner(attempt_id, reason),
            "gate_attempt",
        )
        .await
    }

    async fn gate_attempt_inner(&self, attempt_id: &str, reason: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT intent_id, state FROM attempts WHERE id = ?")
            .bind(attempt_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "attempt",
                id: attempt_id.to_string(),
            })?;
        let intent_id: String = row.get("intent_id");
        let state: String = row.get("state");
        // Only pre-dispatch attempts may be gated: once the prompt was
        // sent, a later gate can never unsend it.
        if state != "claimed" && state != "dispatched" {
            return Err(StoreError::TerminalState {
                attempt: attempt_id.to_string(),
                state,
                requested: "gated".to_string(),
            });
        }
        sqlx::query(
            "UPDATE attempts SET state = 'canceled', stop_reason = ?, terminal_at = ? WHERE id = ?",
        )
        .bind(reason)
        .bind(&now)
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        let (message_id, recipient_id): (String, String) =
            sqlx::query("SELECT message_id, recipient_id FROM execution_intents WHERE id = ?")
                .bind(&intent_id)
                .fetch_one(&mut *tx)
                .await
                .map(|r| (r.get("message_id"), r.get("recipient_id")))?;
        sqlx::query("UPDATE execution_intents SET state = 'queued' WHERE id = ?")
            .bind(&intent_id)
            .execute(&mut *tx)
            .await?;
        // The dispatch write may already have advanced the delivery;
        // return it to the eligible queue for the next claim.
        sqlx::query(
            "UPDATE message_recipients SET delivery_state = 'eligible'
             WHERE message_id = ? AND peer_id = ?
               AND delivery_state IN ('stored', 'eligible', 'delivered')",
        )
        .bind(&message_id)
        .bind(&recipient_id)
        .execute(&mut *tx)
        .await?;
        let conversation_id: String =
            sqlx::query_scalar("SELECT conversation_id FROM messages WHERE id = ?")
                .bind(&message_id)
                .fetch_one(&mut *tx)
                .await?;
        self.append_event_in(
            &mut tx,
            &conversation_id,
            "attempt.gated",
            &serde_json::json!({
                "attempt_id": attempt_id,
                "intent_id": intent_id,
                "reason": reason,
            }),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
