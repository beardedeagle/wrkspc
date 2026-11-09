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

    /// Ensures a conversation with a caller-supplied id exists and, when
    /// it is being CREATED here, enrols `members` (node-side mirror of a
    /// hub-owned conversation: the hub id is preserved so records line
    /// up across stores). Idempotent — re-running never duplicates rows.
    ///
    /// Membership on an EXISTING room is never re-granted here: the hub
    /// snapshot is the authority, and a stale queued delivery must not
    /// re-enrol a peer the operator removed (H-12). Only the
    /// authoritative mirror ([`Store::mirror_conversations`]) and local
    /// creation add members.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation id to ensure (the hub id
    ///   when mirroring).
    /// * `members` — the peers to enrol (on creation only).
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

    /// Deletes a conversation and everything scoped to it.
    ///
    /// A room the operator cannot remove is a room they cannot escape: every
    /// agent that can see it keeps posting into it, and abandoned rooms keep
    /// waking peers on every poll. Removal is transactional and ordered
    /// children-first so the foreign keys hold.
    ///
    /// The conversation's remote delivery tasks are deleted by the
    /// conversation's committed message ids — never by treating the
    /// requested string as a host queue id (H-8: a host id must not
    /// wipe an unrelated host's queue, and deleting a room must not
    /// leave its remote tasks live).
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the conversation to delete.
    ///
    /// # Returns
    ///
    /// `true` when a conversation was removed, `false` when it did not exist
    /// (a nonexistent id deletes NOTHING — it is not a host queue).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn delete_conversation(&self, conversation_id: &str) -> Result<bool> {
        // Existence FIRST: a nonexistent id deletes nothing, so a host
        // id string can never be mistaken for a room and wipe that
        // host's delivery queue.
        let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM conversations WHERE id = ?")
            .bind(conversation_id)
            .fetch_optional(self.pool())
            .await?;
        if exists.is_none() {
            return Ok(false);
        }

        // The room's delivery-task ids: deterministic for current rows
        // and envelope-matched for legacy random-id rows — both keyed
        // on the conversation's committed message ids.
        let pairs: Vec<(String, String)> = sqlx::query(
            "SELECT r.message_id, r.peer_id
             FROM message_recipients r
             JOIN messages m ON m.id = r.message_id
             WHERE m.conversation_id = ?",
        )
        .bind(conversation_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|r| (r.get("message_id"), r.get("peer_id")))
        .collect();
        let mut message_ids: Vec<String> = Vec::new();
        for (message_id, _) in &pairs {
            if !message_ids.contains(message_id) {
                message_ids.push(message_id.clone());
            }
        }

        let mut tx = self.pool.begin().await?;
        // attempts -> intents -> recipients -> messages -> events/fences/members
        sqlx::query(
            "DELETE FROM attempts WHERE intent_id IN
               (SELECT id FROM execution_intents WHERE message_id IN
                  (SELECT id FROM messages WHERE conversation_id = ?))",
        )
        .bind(conversation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM execution_intents WHERE message_id IN
               (SELECT id FROM messages WHERE conversation_id = ?)",
        )
        .bind(conversation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM message_recipients WHERE message_id IN
               (SELECT id FROM messages WHERE conversation_id = ?)",
        )
        .bind(conversation_id)
        .execute(&mut *tx)
        .await?;
        // The outbound ledger points at messages, and guidance is
        // scoped by conversation even without a foreign key.
        sqlx::query(
            "DELETE FROM hub_outbound WHERE message_id IN
               (SELECT id FROM messages WHERE conversation_id = ?)",
        )
        .bind(conversation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM guidance WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        // The room's delivery tasks (claims first, then rows): the
        // deterministic ids of this room's committed (message,
        // recipient) pairs, plus any legacy row whose envelope names
        // one of the room's messages.
        for (message_id, recipient_peer) in &pairs {
            let task_id = crate::tasks::delivery_task_id(message_id, recipient_peer);
            sqlx::query("DELETE FROM delivery_claims WHERE task_id = ?")
                .bind(&task_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM a2a_tasks WHERE task_id = ?")
                .bind(&task_id)
                .execute(&mut *tx)
                .await?;
        }
        for message_id in &message_ids {
            let envelope = format!("%\"message_id\":\"{message_id}\"%");
            let kind = "%\"kind\":\"delivery\"%";
            sqlx::query(
                "DELETE FROM delivery_claims WHERE task_id IN
                   (SELECT task_id FROM a2a_tasks
                    WHERE task_json LIKE ? AND task_json LIKE ?)",
            )
            .bind(&envelope)
            .bind(kind)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM a2a_tasks WHERE task_json LIKE ? AND task_json LIKE ?")
                .bind(&envelope)
                .bind(kind)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("DELETE FROM messages WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM events WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM fences WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM conversation_members WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        let deleted = sqlx::query("DELETE FROM conversations WHERE id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(deleted.rows_affected() > 0)
    }
}

impl Store {
    /// Sets or clears a conversation's vanity name.
    ///
    /// The name is display metadata: nullable, not unique, and never a key. An
    /// empty or whitespace-only name clears it.
    ///
    /// # Arguments
    ///
    /// * `conversation_id` — the room.
    /// * `name` — the new name, or `None` to clear it.
    ///
    /// # Returns
    ///
    /// Whether a conversation row matched.
    ///
    /// # Errors
    ///
    /// Returns a store error when the write fails.
    pub async fn set_conversation_name(
        &self,
        conversation_id: &str,
        name: Option<&str>,
    ) -> Result<bool> {
        let cleaned = name.map(str::trim).filter(|value| !value.is_empty());
        let updated = sqlx::query("UPDATE conversations SET name = ? WHERE id = ?")
            .bind(cleaned)
            .bind(conversation_id)
            .execute(self.pool())
            .await?
            .rows_affected();
        Ok(updated > 0)
    }

    /// Mirrors the hub's conversation list into this store.
    ///
    /// Nodes learn rooms from deliveries; this fills in the rest — the room set,
    /// its members, and the operator's name — so a room list agrees across
    /// machines. Membership is AUTHORITATIVE: a member the hub omits was
    /// removed and its local read/write grant is revoked here (H-12 —
    /// a successful snapshot is a removal, not a fetch failure). Rooms
    /// the hub omits stay as the node last knew them: a node that
    /// missed one poll must not lose a transcript, and node-local rooms
    /// are never touched.
    ///
    /// # Arguments
    ///
    /// * `conversations` — the hub's conversations, as the hub serves them.
    ///
    /// # Errors
    ///
    /// Returns a store error when the mirror write fails.
    pub async fn mirror_conversations(
        &self,
        conversations: &[crate::views::ConversationView],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for conversation in conversations {
            sqlx::query(
                "INSERT INTO conversations (id, name, created_at) VALUES (?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name",
            )
            .bind(&conversation.id)
            .bind(&conversation.name)
            .bind(&conversation.created_at)
            .execute(&mut *tx)
            .await?;
            for member in &conversation.members {
                // `ON CONFLICT DO NOTHING` rather than `OR IGNORE`: a real
                // constraint failure must surface, while a member this node has
                // never enrolled simply is not added.
                sqlx::query(
                    "INSERT INTO conversation_members
                         (conversation_id, peer_id, role, joined_at)
                     SELECT ?, ?, 'member', ?
                     WHERE EXISTS (SELECT 1 FROM peers WHERE id = ?)
                     ON CONFLICT DO NOTHING",
                )
                .bind(&conversation.id)
                .bind(member)
                .bind(&conversation.created_at)
                .bind(member)
                .execute(&mut *tx)
                .await?;
            }
            // The authoritative reconcile: local member rows the hub
            // snapshot omits are revoked. Only rows this node itself
            // knew are affected (membership rows reference enrolled
            // peers), so unrelated state is never touched.
            if conversation.members.is_empty() {
                sqlx::query("DELETE FROM conversation_members WHERE conversation_id = ?")
                    .bind(&conversation.id)
                    .execute(&mut *tx)
                    .await?;
            } else {
                let placeholders = vec!["?"; conversation.members.len()].join(",");
                let sql = format!(
                    "DELETE FROM conversation_members
                     WHERE conversation_id = ? AND peer_id NOT IN ({placeholders})"
                );
                let mut query = sqlx::query(&sql).bind(&conversation.id);
                for member in &conversation.members {
                    query = query.bind(member);
                }
                query.execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}
