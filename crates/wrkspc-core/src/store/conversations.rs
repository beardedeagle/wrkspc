//! Conversation creation, enrolment, and membership.

use super::Store;
use crate::error::Result;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

impl Store {
    /// Creates a conversation and enrols `members` in it.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn create_conversation(&self, members: &[&str]) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO conversations (id, created_at) VALUES (?, ?)")
            .bind(&id)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        for member in members {
            sqlx::query(
                "INSERT INTO conversation_members (conversation_id, peer_id, role, joined_at)
                 VALUES (?, ?, 'member', ?)",
            )
            .bind(&id)
            .bind(member)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(id)
    }

    /// Ensures a conversation with a caller-supplied id exists and has
    /// exactly `members` enrolled (node-side mirror of a hub-owned
    /// conversation: the hub id is preserved so records line up across
    /// stores). Idempotent — re-running never duplicates rows or rewrites
    /// membership.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation id to ensure (the hub id
    ///   when mirroring).
    /// * `members` — the peers to enrol.
    ///
    /// # Returns
    ///
    /// `true` when the conversation was created, `false` when it already
    /// existed.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn ensure_conversation(
        &self,
        conversation_id: &str,
        members: &[&str],
    ) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT 1 FROM conversations WHERE id = ?")
            .bind(conversation_id)
            .fetch_optional(&mut *tx)
            .await?;
        let created = row.is_none();
        if created {
            sqlx::query("INSERT INTO conversations (id, created_at) VALUES (?, ?)")
                .bind(conversation_id)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
        }
        for member in members {
            sqlx::query(
                "INSERT OR IGNORE INTO conversation_members (conversation_id, peer_id, role, joined_at)
                 VALUES (?, ?, 'member', ?)",
            )
            .bind(conversation_id)
            .bind(member)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(created)
    }
    /// Lists the peer ids currently enrolled in a conversation.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation to query.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn conversation_members(&self, conversation_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT peer_id FROM conversation_members WHERE conversation_id = ? ORDER BY joined_at",
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.get("peer_id")).collect())
    }

    /// Removes one member from a conversation ("kick from the room").
    ///
    /// The removal is conversation-scoped: the peer stays enrolled in the
    /// hub and in every other conversation. Already-committed deliveries
    /// and their execution intents are deliberately left alone — a turn
    /// the peer may already be running finishes rather than being
    /// interrupted mid-flight. The `operator` member can never be removed
    /// (a conversation without its moderator has no one to arbitrate).
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation to modify.
    /// * `peer_id` — the member to remove.
    /// * `actor` — who performed the removal (moderation provenance).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the
    /// peer is not a member of that conversation.
    pub async fn remove_conversation_member(
        &self,
        conversation_id: &str,
        peer_id: &str,
        actor: &str,
    ) -> Result<()> {
        if peer_id == "operator" {
            return Err(crate::error::StoreError::NotFound {
                entity: "removable member",
                id: peer_id.to_string(),
            });
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            "DELETE FROM conversation_members WHERE conversation_id = ? AND peer_id = ?",
        )
        .bind(conversation_id)
        .bind(peer_id)
        .execute(&mut *tx)
        .await?;
        if deleted.rows_affected() == 0 {
            tx.rollback().await?;
            return Err(crate::error::StoreError::NotFound {
                entity: "conversation member",
                id: peer_id.to_string(),
            });
        }
        sqlx::query(
            "INSERT INTO events (conversation_id, kind, payload, created_at)
             VALUES (?, 'membership.removed', ?, ?)",
        )
        .bind(conversation_id)
        .bind(serde_json::json!({ "peer_id": peer_id, "actor": actor }).to_string())
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
