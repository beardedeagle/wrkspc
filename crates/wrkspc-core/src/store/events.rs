//! The resumable event feed.

use super::Event;
use super::Store;
use crate::error::Result;
use chrono::Utc;
use sqlx::Row;

impl Store {
    /// Appends an event and returns its cursor position.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn append_event(
        &self,
        conversation_id: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<i64> {
        Store::retry_on_busy(
            || self.append_event_inner(conversation_id, kind, payload),
            "append_event",
        )
        .await
    }

    async fn append_event_inner(
        &self,
        conversation_id: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let seq = self
            .append_event_in(&mut tx, conversation_id, kind, payload)
            .await?;
        tx.commit().await?;
        Ok(seq)
    }

    pub(crate) async fn append_event_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        conversation_id: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "INSERT INTO events (conversation_id, kind, payload, created_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(conversation_id)
        .bind(kind)
        .bind(payload.to_string())
        .bind(&now)
        .execute(&mut **tx)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Reads events after `after_seq` (exclusive) up to `limit`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn events_since(
        &self,
        conversation_id: &str,
        after_seq: i64,
        limit: i64,
    ) -> Result<Vec<Event>> {
        let rows = sqlx::query(
            "SELECT seq, conversation_id, kind, payload, created_at
             FROM events
             WHERE conversation_id = ? AND seq > ?
             ORDER BY seq
             LIMIT ?",
        )
        .bind(conversation_id)
        .bind(after_seq)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Event {
                seq: r.get("seq"),
                conversation_id: r.get("conversation_id"),
                kind: r.get("kind"),
                payload: r
                    .get::<String, _>("payload")
                    .parse()
                    .unwrap_or(serde_json::Value::Null),
                created_at: r.get("created_at"),
            })
            .collect())
    }
}
