//! Read views for the operator surface: conversations, transcripts
//! (messages with per-recipient delivery state), active fences, and
//! unknown attempts awaiting reconciliation.
//!
//! These are pure queries over the committed state — the UI consumes them
//! read-only; every mutation goes through the moderation ops so fences and
//! events stay on the single committed path.

use crate::error::Result;
use crate::store::Store;
use sqlx::Row;

/// A conversation with its members.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConversationView {
    /// Conversation id.
    pub id: String,
    /// The operator's vanity name for the room, when one is set. Nullable by
    /// design: rooms are usable unnamed, and the id is the key everywhere.
    pub name: Option<String>,
    /// RFC 3339 UTC creation time.
    pub created_at: String,
    /// Member peer ids.
    pub members: Vec<String>,
}

/// A message's delivery to one recipient.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DeliveryView {
    /// Recipient peer id.
    pub peer_id: String,
    /// Delivery state literal (`stored`/`held`/`eligible`/`delivered`/…).
    pub state: String,
}

/// A transcript message with its deliveries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MessageView {
    /// Message id.
    pub id: String,
    /// Sender peer id.
    pub sender_id: String,
    /// Message content.
    pub content: String,
    /// RFC 3339 UTC creation time.
    pub created_at: String,
    /// Whether the message requested execution.
    pub execution_requested: bool,
    /// Per-recipient delivery states.
    pub deliveries: Vec<DeliveryView>,
}

/// An attempt in the `unknown` state, shown for operator reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnknownAttemptView {
    /// Attempt id.
    pub attempt_id: String,
    /// Owning intent.
    pub intent_id: String,
    /// Owning message.
    pub message_id: String,
    /// Target peer.
    pub recipient_id: String,
    /// Owning conversation.
    pub conversation_id: String,
    /// Recorded reason (why the outcome is undeterminable).
    pub stop_reason: Option<String>,
    /// RFC 3339 UTC claim time.
    pub claimed_at: Option<String>,
    /// Whether a cancel request is recorded on the attempt.
    pub cancel_requested: bool,
}

impl Store {
    /// Lists conversations with their members, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::Db`] on failure.
    pub async fn list_conversations(&self) -> Result<Vec<ConversationView>> {
        let rows = sqlx::query(
            "SELECT c.id AS id, c.name AS name, c.created_at AS created_at,
                    GROUP_CONCAT(cm.peer_id) AS members
             FROM conversations c
             JOIN conversation_members cm ON cm.conversation_id = c.id
             GROUP BY c.id
             ORDER BY c.created_at ASC",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| ConversationView {
                id: r.get("id"),
                name: r.get::<Option<String>, _>("name"),
                created_at: r.get("created_at"),
                members: r
                    .get::<Option<String>, _>("members")
                    .unwrap_or_default()
                    .split(',')
                    .filter(|m| !m.is_empty())
                    .map(str::to_string)
                    .collect(),
            })
            .collect())
    }

    /// Lists a conversation's transcript, oldest first, with per-recipient
    /// delivery states.
    ///
    /// * `conversation_id` — the conversation.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::Db`] on failure.
    pub async fn list_messages(&self, conversation_id: &str) -> Result<Vec<MessageView>> {
        let messages = sqlx::query(
            "SELECT id, sender_id, content, created_at, execution_requested
             FROM messages
             WHERE conversation_id = ?
             ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(self.pool())
        .await?;

        // The explicitly requested FULL-history read fetches the full
        // delivery history too; the bounded window below scopes its own
        // fetch to the selected message ids.
        let deliveries = sqlx::query(
            "SELECT r.message_id, r.peer_id, r.delivery_state
             FROM message_recipients r
             JOIN messages m ON m.id = r.message_id
             WHERE m.conversation_id = ?",
        )
        .bind(conversation_id)
        .fetch_all(self.pool())
        .await?;

        self.assemble_messages(messages, deliveries).await
    }

    /// The latest `window` messages of a conversation, oldest first,
    /// with their delivery rows — the transcript's initial bounded
    /// load (UI-19). A client that would otherwise page an entire
    /// room into memory at startup only ever sees this window, and
    /// can request the full list on demand.
    ///
    /// * `conversation_id` — the room.
    /// * `window` — the maximum number of most recent messages.
    pub async fn list_messages_window(
        &self,
        conversation_id: &str,
        window: u32,
    ) -> Result<Vec<MessageView>> {
        if window == 0 {
            return Ok(Vec::new());
        }
        let messages = sqlx::query(
            "SELECT id, sender_id, content, created_at, execution_requested
             FROM (SELECT id, sender_id, content, created_at, execution_requested
                   FROM messages
                   WHERE conversation_id = ?
                   ORDER BY created_at DESC
                   LIMIT ?)
             ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .bind(window)
        .fetch_all(self.pool())
        .await?;

        // The window bounds the DELIVERY fetch too: recipient rows are
        // read for the SELECTED message ids only, so a long-lived room
        // with years of delivery history costs the window, not the whole
        // conversation. (The old conversation-wide fetch built a
        // per-message index for every historical row and then discarded
        // the out-of-window entries — unbounded work behind a bounded
        // API.) The exact selected ids are bound (not a second ORDER/LIMIT
        // projection), so a created_at tie can never fetch rows for a
        // message the outer query did not return. Chunked so a huge
        // window stays under SQLite's variable limit.
        let selected_ids: Vec<String> = messages.iter().map(|row| row.get("id")).collect();
        let mut deliveries: Vec<sqlx::sqlite::SqliteRow> = Vec::new();
        for chunk in selected_ids.chunks(500) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                "SELECT message_id, peer_id, delivery_state
                 FROM message_recipients
                 WHERE message_id IN ({placeholders})"
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            deliveries.extend(query.fetch_all(self.pool()).await?);
        }

        self.assemble_messages(messages, deliveries).await
    }

    /// Joins delivery rows onto fetched message rows. The CALLER decides
    /// the delivery fetch's scope — the full-history list passes the
    /// conversation-wide rows, the bounded window passes only the rows
    /// for its selected ids — so no delivery row outside the caller's
    /// message set is ever materialized here.
    async fn assemble_messages(
        &self,
        messages: Vec<sqlx::sqlite::SqliteRow>,
        deliveries: Vec<sqlx::sqlite::SqliteRow>,
    ) -> Result<Vec<MessageView>> {
        // One pass into a per-message index, then O(1) lookups: the
        // transcript assembly is O(M + R), not O(M × R) — with large
        // rooms the quadratic filter made the view 15× slower (C12).
        let mut deliveries_by_message: std::collections::HashMap<String, Vec<DeliveryView>> =
            std::collections::HashMap::new();
        for row in deliveries {
            deliveries_by_message
                .entry(row.get("message_id"))
                .or_default()
                .push(DeliveryView {
                    peer_id: row.get("peer_id"),
                    state: row.get("delivery_state"),
                });
        }

        Ok(messages
            .into_iter()
            .map(|m| {
                let id: String = m.get("id");
                let deliveries = deliveries_by_message.remove(&id).unwrap_or_default();
                MessageView {
                    id,
                    sender_id: m.get("sender_id"),
                    content: m.get("content"),
                    created_at: m.get("created_at"),
                    execution_requested: m.get("execution_requested"),
                    deliveries,
                }
            })
            .collect())
    }

    /// Lists a conversation's active moderation fences.
    ///
    /// * `conversation_id` — the conversation.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::Db`] on failure.
    pub async fn conversation_fences(&self, conversation_id: &str) -> Result<Vec<crate::Fence>> {
        let rows = sqlx::query(
            "SELECT id, conversation_id, peer_id, kind, set_by, set_at, acked_at
             FROM fences
             WHERE conversation_id = ? AND active = 1
             ORDER BY set_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::Fence {
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

    /// Lists attempts awaiting operator reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StoreError::Db`] on failure.
    pub async fn unknown_attempts(&self) -> Result<Vec<UnknownAttemptView>> {
        let rows = sqlx::query(
            "SELECT a.id AS attempt_id, a.intent_id, a.state, a.stop_reason,
                    a.claimed_at, a.cancel_requested,
                    i.message_id, i.recipient_id, m.conversation_id
             FROM attempts a
             JOIN execution_intents i ON i.id = a.intent_id
             JOIN messages m ON m.id = i.message_id
             WHERE a.state = 'unknown'
             ORDER BY a.claimed_at ASC",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| UnknownAttemptView {
                attempt_id: r.get("attempt_id"),
                intent_id: r.get("intent_id"),
                message_id: r.get("message_id"),
                recipient_id: r.get("recipient_id"),
                conversation_id: r.get("conversation_id"),
                stop_reason: r.get("stop_reason"),
                claimed_at: r.get("claimed_at"),
                cancel_requested: r.get::<i64, _>("cancel_requested") != 0,
            })
            .collect())
    }
}
