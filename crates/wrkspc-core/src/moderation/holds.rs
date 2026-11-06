//! Dispatch eligibility and the held-delivery queue: which intents are
//! claimable right now, and the operator's per-message hold/release.

use super::DueIntent;
use crate::error::{Result, StoreError};
use crate::store::Store;
use serde_json::json;
use sqlx::Row;

impl Store {
    pub async fn due_intents(&self) -> Result<Vec<DueIntent>> {
        let rows = sqlx::query(
            "SELECT i.id AS intent_id, i.message_id, i.recipient_id,
                    m.conversation_id, m.content
             FROM execution_intents i
             JOIN messages m ON m.id = i.message_id
             JOIN message_recipients r
               ON r.message_id = i.message_id AND r.peer_id = i.recipient_id
             WHERE i.state = 'queued'
               AND r.delivery_state IN ('stored', 'eligible', 'delivered')
               AND NOT EXISTS (
                 SELECT 1 FROM attempts a
                 WHERE a.intent_id = i.id
                   AND a.state IN ('claimed', 'dispatched', 'unknown')
               )
               AND NOT EXISTS (
                 SELECT 1 FROM fences f
                 WHERE f.active = 1
                   AND f.conversation_id = m.conversation_id
                   AND f.peer_id = i.recipient_id
                   AND f.kind IN ('pause', 'hold', 'block')
               )
             ORDER BY i.created_at ASC",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| DueIntent {
                intent_id: r.get("intent_id"),
                message_id: r.get("message_id"),
                recipient_id: r.get("recipient_id"),
                conversation_id: r.get("conversation_id"),
                content: r.get("content"),
            })
            .collect())
    }

    /// Holds a recipient's delivery (the per-message held queue).
    ///
    /// Idempotent: holding an already-held delivery is a no-op.
    ///
    /// # Arguments
    ///
    /// * `message_id` — the message whose delivery to hold.
    /// * `peer_id` — the recipient.
    /// * `set_by` — the operator/peer requesting the hold.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the
    /// recipient row does not exist or is `delivered`/`expired`,
    /// [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn hold_delivery(&self, message_id: &str, peer_id: &str, set_by: &str) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query(
            "SELECT delivery_state FROM message_recipients
             WHERE message_id = ? AND peer_id = ?",
        )
        .bind(message_id)
        .bind(peer_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "message recipient",
            id: format!("{message_id}/{peer_id}"),
        })?;
        let state: String = row.get("delivery_state");
        match state.as_str() {
            "held" => return Ok(()),
            "stored" | "eligible" => {}
            other => {
                return Err(StoreError::TerminalState {
                    attempt: format!("{message_id}/{peer_id}"),
                    state: other.to_string(),
                    requested: "held".to_string(),
                });
            }
        }
        sqlx::query(
            "UPDATE message_recipients SET delivery_state = 'held'
             WHERE message_id = ? AND peer_id = ?",
        )
        .bind(message_id)
        .bind(peer_id)
        .execute(&mut *tx)
        .await?;
        let conversation_id: String =
            sqlx::query_scalar("SELECT conversation_id FROM messages WHERE id = ?")
                .bind(message_id)
                .fetch_one(&mut *tx)
                .await?;
        self.append_event_in(
            &mut tx,
            &conversation_id,
            "delivery.held",
            &json!({ "message_id": message_id, "peer_id": peer_id, "set_by": set_by }),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Releases a held delivery back into the eligible queue.
    ///
    /// # Arguments
    ///
    /// * `message_id` — the message whose delivery to release.
    /// * `peer_id` — the recipient.
    /// * `set_by` — the operator/peer requesting the release.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the
    /// recipient row does not exist or is not `held`,
    /// [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn release_delivery(
        &self,
        message_id: &str,
        peer_id: &str,
        set_by: &str,
    ) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query(
            "SELECT delivery_state FROM message_recipients
             WHERE message_id = ? AND peer_id = ?",
        )
        .bind(message_id)
        .bind(peer_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "message recipient",
            id: format!("{message_id}/{peer_id}"),
        })?;
        let state: String = row.get("delivery_state");
        if state != "held" {
            return Err(StoreError::NotFound {
                entity: "held delivery",
                id: format!("{message_id}/{peer_id} (state {state})"),
            });
        }
        sqlx::query(
            "UPDATE message_recipients SET delivery_state = 'eligible'
             WHERE message_id = ? AND peer_id = ?",
        )
        .bind(message_id)
        .bind(peer_id)
        .execute(&mut *tx)
        .await?;
        let conversation_id: String =
            sqlx::query_scalar("SELECT conversation_id FROM messages WHERE id = ?")
                .bind(message_id)
                .fetch_one(&mut *tx)
                .await?;
        self.append_event_in(
            &mut tx,
            &conversation_id,
            "delivery.released",
            &json!({ "message_id": message_id, "peer_id": peer_id, "set_by": set_by }),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
