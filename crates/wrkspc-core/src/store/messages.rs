//! Readiness fence: message acceptance and fan-out.

use super::AcceptOutcome;
use super::RecipientRequest;
use super::Store;
use crate::error::{Result, StoreError};
use crate::model::DeliveryState;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

impl Store {
    /// Runs the readiness fence: commits a message, its recipients, and any
    /// requested execution intents in one transaction.
    ///
    /// Sender and recipients must already be conversation members.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::IdempotencyConflict`](StoreError::IdempotencyConflict)
    /// when the key is reused with different content, and
    /// [`StoreError::NotFound`](StoreError::NotFound) for non-members.
    pub async fn accept_message(
        &self,
        conversation_id: &str,
        sender_id: &str,
        idempotency_key: &str,
        content: &str,
        execution_requested: bool,
        recipients: &[RecipientRequest],
    ) -> Result<AcceptOutcome> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;

        // Idempotency: same key + same content = replay, same key + different
        // content = conflict.
        if let Some(row) = sqlx::query(
            "SELECT id, content FROM messages
             WHERE conversation_id = ? AND sender_id = ? AND idempotency_key = ?",
        )
        .bind(conversation_id)
        .bind(sender_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            let existing_id: String = row.get("id");
            let existing_content: String = row.get("content");
            if existing_content == content {
                // A replay means the message is already stored, not that this
                // recipient's work is done. The delivery loop feeds one
                // delivery per recipient under the same idempotency key, so
                // every recipient after the first reaches this branch: enrol it
                // and raise its intent here, or the peer silently never runs.
                self.assert_member(&mut tx, conversation_id, sender_id)
                    .await?;
                for recipient in recipients {
                    self.assert_member(&mut tx, conversation_id, &recipient.peer_id)
                        .await?;
                    sqlx::query(
                        "INSERT OR IGNORE INTO message_recipients
                           (message_id, peer_id, delivery_state)
                         VALUES (?, ?, ?)",
                    )
                    .bind(&existing_id)
                    .bind(&recipient.peer_id)
                    .bind(DeliveryState::Stored.as_str())
                    .execute(&mut *tx)
                    .await?;
                    if recipient.execution_requested {
                        sqlx::query(
                            "INSERT OR IGNORE INTO execution_intents
                               (id, message_id, recipient_id, state, created_at)
                             VALUES (?, ?, ?, 'queued', ?)",
                        )
                        .bind(Uuid::new_v4().to_string())
                        .bind(&existing_id)
                        .bind(&recipient.peer_id)
                        .bind(&now)
                        .execute(&mut *tx)
                        .await?;
                    }
                }
                tx.commit().await?;
                return Ok(AcceptOutcome::Replay {
                    message_id: existing_id,
                });
            }
            return Err(StoreError::IdempotencyConflict {
                key: idempotency_key.to_string(),
            });
        }

        self.assert_member(&mut tx, conversation_id, sender_id)
            .await?;
        for recipient in recipients {
            self.assert_member(&mut tx, conversation_id, &recipient.peer_id)
                .await?;
        }

        let message_id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO messages
               (id, conversation_id, sender_id, idempotency_key, content,
                execution_requested, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&message_id)
        .bind(conversation_id)
        .bind(sender_id)
        .bind(idempotency_key)
        .bind(content)
        .bind(execution_requested)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        for recipient in recipients {
            sqlx::query(
                "INSERT INTO message_recipients (message_id, peer_id, delivery_state)
                 VALUES (?, ?, ?)",
            )
            .bind(&message_id)
            .bind(&recipient.peer_id)
            .bind(DeliveryState::Stored.as_str())
            .execute(&mut *tx)
            .await?;
            if recipient.execution_requested {
                let intent_id = Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO execution_intents
                       (id, message_id, recipient_id, state, created_at)
                     VALUES (?, ?, ?, 'queued', ?)",
                )
                .bind(&intent_id)
                .bind(&message_id)
                .bind(&recipient.peer_id)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
            }
        }

        let event_payload = serde_json::json!({
            "message_id": message_id,
            "sender_id": sender_id,
            "execution_requested": execution_requested,
        });
        self.append_event_in(&mut tx, conversation_id, "message.accepted", &event_payload)
            .await?;

        tx.commit().await?;
        Ok(AcceptOutcome::Accepted { message_id })
    }
}
