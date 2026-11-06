//! Team-mode delivery queueing and node→hub message propagation.
//!
//! Two halves of the cross-machine loop live here:
//!
//! * [`Store::queue_remote_deliveries`] — after a message commits on the
//!   hub, every recipient enrolled on a host other than the hub's own
//!   gets a pollable delivery task (the D-6 A2 contract's data half).
//! * [`Store::hub_pending_outbound`] / [`Store::mark_hub_sent`] — the
//!   node side of the same loop: messages originated by this node's own
//!   harness peers are propagated to the hub via `message/send`, with a
//!   durable ledger so a crash mid-send never duplicates.

use crate::{DeliveryTaskRequest, Result, Store, StoreError};
use sqlx::Row;

/// A local message awaiting propagation to the hub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    /// The message id.
    pub message_id: String,
    /// The conversation id (the hub's conversation, preserved by the
    /// delivery mirror).
    pub conversation_id: String,
    /// The sending peer — enrolled on this node's own host.
    pub sender_peer: String,
    /// The message content.
    pub content: String,
    /// Whether the message requested a harness turn.
    pub execution_requested: bool,
    /// The locally committed recipients (mention-resolved at acceptance).
    /// Carried verbatim so the hub does not re-resolve routing; an empty
    /// list means the post addressed nobody and must wake nobody.
    pub recipients: Vec<String>,
}

impl Store {
    /// Lists messages originated by peers on `host_id` that have not
    /// yet been recorded as delivered to the hub.
    ///
    /// Only messages whose sender is enrolled on this host qualify —
    /// hub-mirrored deliveries carry a remote sender host and are
    /// never re-sent.
    ///
    /// # Arguments
    ///
    /// * `host_id` — this node's own host id (its certificate SAN).
    ///
    /// # Returns
    ///
    /// The pending messages in commit order (oldest first).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn hub_pending_outbound(&self, host_id: &str) -> Result<Vec<OutboundMessage>> {
        let rows = sqlx::query(
            "SELECT m.id, m.conversation_id, m.sender_id, m.content, m.execution_requested
             FROM messages m
             JOIN peers p ON p.id = m.sender_id
             WHERE p.host_id = ?
               AND m.id NOT IN (SELECT message_id FROM hub_outbound)
             ORDER BY m.created_at",
        )
        .bind(host_id)
        .fetch_all(self.pool())
        .await?;
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let message_id: String = row.get("id");
            // The committed recipient list is the routing the hub must
            // preserve: it was resolved once at local acceptance
            // (explicit recipients, then @mentions; a post that named
            // nobody committed no rows and wakes nobody at the hub).
            let recipient_rows = sqlx::query(
                "SELECT peer_id FROM message_recipients WHERE message_id = ? ORDER BY rowid",
            )
            .bind(&message_id)
            .fetch_all(self.pool())
            .await?;
            messages.push(OutboundMessage {
                message_id,
                conversation_id: row.get("conversation_id"),
                sender_peer: row.get("sender_id"),
                content: row.get("content"),
                execution_requested: row.get("execution_requested"),
                recipients: recipient_rows
                    .into_iter()
                    .map(|r| r.get("peer_id"))
                    .collect(),
            });
        }
        Ok(messages)
    }

    /// Records a message as delivered to the hub. Idempotent: a second
    /// call for the same message is a no-op.
    ///
    /// # Arguments
    ///
    /// * `message_id` — the message that was sent to the hub.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn mark_hub_sent(&self, message_id: &str) -> Result<()> {
        sqlx::query("INSERT OR IGNORE INTO hub_outbound (message_id, sent_at) VALUES (?, ?)")
            .bind(message_id)
            .bind(chrono::Utc::now().to_rfc3339())
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Queues a pollable delivery task for every recipient enrolled on
    /// a host other than the request's `local_host`.
    ///
    /// The hub commits an accepted message once; this is the delivery
    /// fan-out that turns the commit into per-host queues. Recipients
    /// on the local host are left to the local dispatcher. Unenrolled
    /// recipients are skipped (acceptance already asserted membership,
    /// so this is a defensive no-op).
    ///
    /// # Arguments
    ///
    /// * `request` — the committed message and the recipients.
    ///
    /// # Returns
    ///
    /// The number of delivery tasks created.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the
    /// sender is not enrolled, and
    /// [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn queue_remote_deliveries(&self, request: RemoteFanoutRequest<'_>) -> Result<usize> {
        let (sender_name, sender_class, sender_host) = self
            .peer_details(request.sender_peer)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "peer",
                id: request.sender_peer.to_string(),
            })?;
        let mut queued = 0;
        for peer_id in request.recipients {
            let Some((recipient_name, recipient_class, peer_host)) =
                self.peer_details(peer_id).await?
            else {
                continue;
            };
            if peer_host == request.local_host {
                continue;
            }
            self.task_create_delivery(DeliveryTaskRequest {
                recipient_host: &peer_host,
                recipient_peer: peer_id,
                recipient_name: &recipient_name,
                recipient_class: &recipient_class,
                message_id: request.message_id,
                conversation_id: request.conversation_id,
                sender_peer: request.sender_peer,
                sender_name: &sender_name,
                sender_class: &sender_class,
                sender_host: &sender_host,
                content: request.content,
                execution_requested: request.execution_requested,
            })
            .await?;
            queued += 1;
        }
        Ok(queued)
    }
}

/// A delivery fan-out request: the committed message plus the
/// recipients to fan out to.
pub struct RemoteFanoutRequest<'a> {
    /// The hub's own host id — recipients on this host are local.
    pub local_host: &'a str,
    /// The committed conversation.
    pub conversation_id: &'a str,
    /// The committed message id.
    pub message_id: &'a str,
    /// The enrolled sender.
    pub sender_peer: &'a str,
    /// The message text.
    pub content: &'a str,
    /// Carried into the delivery metadata.
    pub execution_requested: bool,
    /// Recipient peer ids.
    pub recipients: &'a [String],
}
