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
    /// Returns [`crate::error::StoreError::Db`] on failure.
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

    /// Appends an event, asserting the writer's conversation
    /// membership INSIDE the same transaction as the insert — a peer
    /// kicked between a separate check and the write can never commit
    /// a feed event it is no longer allowed to write.
    ///
    /// Identical row semantics to [`append_event`](Store::append_event).
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::NotFound`] when the
    /// peer is not a member, and [`crate::error::StoreError::Db`] on
    /// failure.
    pub async fn append_event_as_member(
        &self,
        conversation_id: &str,
        peer_id: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<i64> {
        Store::retry_on_busy(
            || self.append_event_as_member_inner(conversation_id, peer_id, kind, payload),
            "append_event_as_member",
        )
        .await
    }

    async fn append_event_as_member_inner(
        &self,
        conversation_id: &str,
        peer_id: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        self.assert_member(&mut tx, conversation_id, peer_id)
            .await?;
        let seq = self
            .append_event_in(&mut tx, conversation_id, kind, payload)
            .await?;
        tx.commit().await?;
        Ok(seq)
    }

    /// Reads events after a cursor, asserting the reader's
    /// conversation membership INSIDE the same transaction as the
    /// read — a peer kicked between a separate check and the read can
    /// never observe one page of events.
    ///
    /// Identical row shape, ordering and clamp to
    /// [`events_since`](Store::events_since).
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::NotFound`] when the
    /// peer is not a member, and [`crate::error::StoreError::Db`] on
    /// failure.
    pub async fn events_since_for_member(
        &self,
        conversation_id: &str,
        peer_id: &str,
        after_seq: i64,
        limit: i64,
    ) -> Result<Vec<Event>> {
        let limit = limit.clamp(1, 1000);
        let mut tx = self.pool.begin().await?;
        self.assert_member(&mut tx, conversation_id, peer_id)
            .await?;
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
        .fetch_all(&mut *tx)
        .await?;
        let mut events: Vec<Event> = Vec::with_capacity(rows.len());
        for r in rows {
            let payload: String = r.get("payload");
            // A corrupt payload must surface as a decode error, never
            // as a genuine-Null feed event.
            let payload: serde_json::Value = payload
                .parse()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            events.push(Event {
                seq: r.get("seq"),
                conversation_id: r.get("conversation_id"),
                kind: r.get("kind"),
                payload,
                created_at: r.get("created_at"),
            });
        }
        Ok(events)
    }

    /// Reads events after `after_seq` (exclusive) up to `limit`.
    ///
    /// The limit is CLAMPED defensively to `1..=1000` (P07): a
    /// negative or oversized caller value must never become an
    /// unbounded `LIMIT -1` fetch. Callers that need the contract
    /// enforced loudly validate before calling.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::Db`] on failure.
    pub async fn events_since(
        &self,
        conversation_id: &str,
        after_seq: i64,
        limit: i64,
    ) -> Result<Vec<Event>> {
        let limit = limit.clamp(1, 1000);
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
        let mut events: Vec<Event> = Vec::with_capacity(rows.len());
        for r in rows {
            let payload: String = r.get("payload");
            // A corrupt payload must surface as a decode error, never
            // as a genuine-Null feed event — silent Null makes
            // corruption indistinguishable from a real null payload
            // downstream.
            let payload: serde_json::Value = payload
                .parse()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            events.push(Event {
                seq: r.get("seq"),
                conversation_id: r.get("conversation_id"),
                kind: r.get("kind"),
                payload,
                created_at: r.get("created_at"),
            });
        }
        Ok(events)
    }
}
