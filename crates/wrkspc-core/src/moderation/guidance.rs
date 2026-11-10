//! Operator guidance: recording, the undelivered queue, and delivery
//! marking.

use super::Guidance;
use crate::error::Result;
use crate::store::Store;
use chrono::Utc;
use serde_json::json;
use sqlx::Row;

impl Store {
    pub async fn add_guidance(
        &self,
        conversation_id: &str,
        author_id: &str,
        text: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool().begin().await?;
        sqlx::query(
            "INSERT INTO guidance (id, conversation_id, author_id, text, delivered, created_at)
             VALUES (?, ?, ?, ?, 0, ?)",
        )
        .bind(&id)
        .bind(conversation_id)
        .bind(author_id)
        .bind(text)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        self.append_event_in(
            &mut tx,
            conversation_id,
            "guidance",
            &json!({ "guidance_id": id, "author_id": author_id, "text": text }),
        )
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Lists undelivered guidance for a conversation, oldest first.
    ///
    /// Read-only view: the UI's pending list reads through here, so the
    /// dispatcher's claim state is deliberately NOT filtered out.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn undelivered_guidance(&self, conversation_id: &str) -> Result<Vec<Guidance>> {
        let rows = sqlx::query(
            "SELECT id, conversation_id, author_id, text, created_at
             FROM guidance
             WHERE conversation_id = ? AND delivered = 0
             ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Guidance {
                id: r.get("id"),
                conversation_id: r.get("conversation_id"),
                author_id: r.get("author_id"),
                text: r.get("text"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// Atomically claims the conversation's pending guidance for an
    /// attempt: the returned rows are exclusively THIS attempt's, so two
    /// concurrent turns in the same conversation cannot both ride the
    /// same guidance. The claim is released (not marked delivered) when
    /// the attempt does not resolve successfully.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation.
    /// * `attempt_id` — the claiming attempt.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn claim_guidance(
        &self,
        conversation_id: &str,
        attempt_id: &str,
    ) -> Result<Vec<Guidance>> {
        // The UPDATE statement IS the claim: acquiring the write lock
        // (with busy_timeout, a concurrent claimer waits and then sees
        // this transaction's committed rows) makes the read set below
        // exactly the set this attempt claimed.
        let mut tx = self.pool().begin().await?;
        let rows = sqlx::query(
            "UPDATE guidance
                SET claimed_attempt = ?
              WHERE conversation_id = ? AND delivered = 0 AND claimed_attempt IS NULL
              RETURNING id, conversation_id, author_id, text, created_at",
        )
        .bind(attempt_id)
        .bind(conversation_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows
            .into_iter()
            .map(|r| Guidance {
                id: r.get("id"),
                conversation_id: r.get("conversation_id"),
                author_id: r.get("author_id"),
                text: r.get("text"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// Releases an attempt's guidance claim: the rows return to the
    /// pending queue and re-ride the next turn (the attempt did not
    /// resolve successfully).
    ///
    /// # Arguments
    ///
    /// * `attempt_id` — the attempt whose claim is released.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn release_guidance_claim(&self, attempt_id: &str) -> Result<()> {
        sqlx::query("UPDATE guidance SET claimed_attempt = NULL WHERE claimed_attempt = ?")
            .bind(attempt_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Ingests hub-originated guidance into the local table (the
    /// hub→node propagation leg): the hub's ids are kept verbatim so
    /// the consumption ack can address the authoritative rows.
    /// Idempotent — a re-delivered envelope does not duplicate rows.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation.
    /// * `entries` — `(id, author_id, text)` triples from the envelope.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn ingest_guidance(
        &self,
        conversation_id: &str,
        entries: &[(String, String, String)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool().begin().await?;
        for (id, author_id, text) in entries {
            sqlx::query(
                "INSERT OR IGNORE INTO guidance (id, conversation_id, author_id, text, delivered, created_at)
                 VALUES (?, ?, ?, ?, 0, ?)",
            )
            .bind(id)
            .bind(conversation_id)
            .bind(author_id)
            .bind(text)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Lists the ids of delivered guidance for a conversation — the
    /// consumption ack set the node reports back to the hub.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn delivered_guidance_ids(&self, conversation_id: &str) -> Result<Vec<String>> {
        let ids = sqlx::query_scalar(
            "SELECT id FROM guidance WHERE conversation_id = ? AND delivered = 1",
        )
        .bind(conversation_id)
        .fetch_all(self.pool())
        .await?;
        Ok(ids)
    }

    /// Marks guidance entries delivered (after they rode a prompt).
    ///
    /// # Arguments
    ///
    /// * `ids` — the guidance ids.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn mark_guidance_delivered(&self, ids: &[&str]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool().begin().await?;
        for id in ids {
            sqlx::query("UPDATE guidance SET delivered = 1, claimed_attempt = NULL WHERE id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
