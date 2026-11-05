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
            sqlx::query("UPDATE guidance SET delivered = 1 WHERE id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
