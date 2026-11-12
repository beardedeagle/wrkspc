//! Moderation fences: set, ack, clear, list.
//!
//! Read-modify-write operations reserve the WAL writer before reading, so a
//! competing commit cannot invalidate the snapshot they need to update.

use super::Fence;
use super::Store;
use crate::error::{Result, StoreError};
use crate::model::FenceKind;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

impl Store {
    /// Sets a moderation fence; idempotent for an identical active fence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`] on failure.
    pub async fn set_fence(
        &self,
        conversation_id: &str,
        peer_id: &str,
        kind: FenceKind,
        set_by: &str,
    ) -> Result<String> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) = sqlx::query(
            "SELECT id FROM fences
             WHERE conversation_id = ? AND peer_id = ? AND kind = ? AND active = 1",
        )
        .bind(conversation_id)
        .bind(peer_id)
        .bind(kind.as_str())
        .fetch_optional(&mut *tx)
        .await?
        {
            let id: String = row.get("id");
            return Ok(id);
        }
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO fences
               (id, conversation_id, peer_id, kind, active, set_by, set_at)
             VALUES (?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&id)
        .bind(conversation_id)
        .bind(peer_id)
        .bind(kind.as_str())
        .bind(set_by)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        self.append_event_in(
            &mut tx,
            conversation_id,
            "fence.set",
            &serde_json::json!({
                "fence_id": id,
                "peer_id": peer_id,
                "kind": kind.as_str(),
                "set_by": set_by,
            }),
        )
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Acknowledges a fence (the team-mode fence-ack seam; the single-host
    /// pilot acks locally).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the fence
    /// does not exist.
    pub async fn ack_fence(&self, fence_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE fences SET acked_at = ? WHERE id = ? AND active = 1 AND acked_at IS NULL",
        )
        .bind(&now)
        .bind(fence_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            // Either nonexistent or already acked — distinguish.
            let exists: Option<String> = sqlx::query("SELECT id FROM fences WHERE id = ?")
                .bind(fence_id)
                .fetch_optional(&self.pool)
                .await?
                .map(|r| r.get("id"));
            if exists.is_none() {
                return Err(StoreError::NotFound {
                    entity: "fence",
                    id: fence_id.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Clears a fence. Idempotent. A `fence.cleared` event is appended in
    /// the same transaction so the committed feed shows the release, not
    /// a fence that reads as applied forever; it is emitted only when an
    /// active fence actually flipped.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the fence
    /// does not exist.
    pub async fn clear_fence(&self, fence_id: &str, cleared_by: &str) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row: Option<(String, String, String)> =
            sqlx::query_as("SELECT conversation_id, peer_id, kind FROM fences WHERE id = ?")
                .bind(fence_id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some((conversation_id, peer_id, kind)) = row else {
            return Err(StoreError::NotFound {
                entity: "fence",
                id: fence_id.to_string(),
            });
        };
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE fences SET active = 0, cleared_by = ?, cleared_at = ?
             WHERE id = ? AND active = 1",
        )
        .bind(cleared_by)
        .bind(&now)
        .bind(fence_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 1 {
            self.append_event_in(
                &mut tx,
                &conversation_id,
                "fence.cleared",
                &serde_json::json!({
                    "fence_id": fence_id,
                    "peer_id": peer_id,
                    "kind": kind,
                    "cleared_by": cleared_by,
                }),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Lists active fences for a conversation/peer pair.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`] on failure.
    pub async fn active_fences(&self, conversation_id: &str, peer_id: &str) -> Result<Vec<Fence>> {
        let rows = sqlx::query(
            "SELECT id, conversation_id, peer_id, kind, set_by, set_at, acked_at
             FROM fences
             WHERE conversation_id = ? AND peer_id = ? AND active = 1
             ORDER BY set_at",
        )
        .bind(conversation_id)
        .bind(peer_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Fence {
                id: r.get("id"),
                conversation_id: r.get("conversation_id"),
                peer_id: r.get("peer_id"),
                kind: r.get("kind"),
                set_by: r.get("set_by"),
                set_at: r.get("set_at"),
                acked_at: r.get("acked_at"),
            })
            .collect())
    }
}
