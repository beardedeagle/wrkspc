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
            // An operator-canceled intent must not be fanned out (the
            // delivery task is the node's execution channel).
            if self
                .recipient_intent_canceled(request.message_id, peer_id)
                .await?
            {
                continue;
            }
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
                origin_message_id: request.origin_message_id,
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

    /// Repairs a partially completed remote fan-out: for every
    /// committed (message, recipient) pair whose recipient lives on a
    /// remote host, the deterministic delivery task is created when it
    /// does not exist.
    ///
    /// Idempotent and safe to run at startup (or after any replay):
    /// existing tasks — including terminal, already-acked ones — are
    /// never touched, and no duplicate can form because the task id is
    /// a function of (message, recipient).
    ///
    /// # Arguments
    ///
    /// * `local_host` — the hub's own host id; recipients on it are
    ///   local and never get a delivery task.
    ///
    /// # Returns
    ///
    /// The number of delivery tasks (re)created.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn repair_remote_fanout(&self, local_host: &str) -> Result<usize> {
        let rows = sqlx::query(
            "SELECT m.id AS message_id, m.conversation_id, m.sender_id, m.content,
                    m.execution_requested, r.peer_id,
                    p.name AS recipient_name, p.class AS recipient_class,
                    p.host_id AS recipient_host
             FROM message_recipients r
             JOIN messages m ON m.id = r.message_id
             JOIN peers p ON p.id = r.peer_id
             WHERE p.host_id != ?",
        )
        .bind(local_host)
        .fetch_all(self.pool())
        .await?;
        let mut repaired = 0;
        for row in rows {
            let message_id: String = row.get("message_id");
            let recipient_peer: String = row.get("peer_id");
            // An operator-canceled turn must never be re-created by a
            // repair pass (H-10): the fan-out is the execution channel,
            // and a canceled intent means the recipient must not run it.
            if self
                .recipient_intent_canceled(&message_id, &recipient_peer)
                .await?
            {
                continue;
            }
            let task_id = crate::tasks::delivery_task_id(&message_id, &recipient_peer);
            if self.task_get(&task_id).await?.is_some() {
                continue;
            }
            let sender_peer: String = row.get("sender_id");
            let (sender_name, sender_class, sender_host) = self
                .peer_details(&sender_peer)
                .await?
                .ok_or_else(|| StoreError::NotFound {
                entity: "peer",
                id: sender_peer.clone(),
            })?;
            let recipient_host: String = row.get("recipient_host");
            let recipient_name: String = row.get("recipient_name");
            let recipient_class: String = row.get("recipient_class");
            let conversation_id: String = row.get("conversation_id");
            let content: String = row.get("content");
            let execution_requested: bool = row.get("execution_requested");
            self.task_create_delivery(DeliveryTaskRequest {
                recipient_host: &recipient_host,
                recipient_peer: &recipient_peer,
                recipient_name: &recipient_name,
                recipient_class: &recipient_class,
                message_id: &message_id,
                origin_message_id: None,
                conversation_id: &conversation_id,
                sender_peer: &sender_peer,
                sender_name: &sender_name,
                sender_class: &sender_class,
                sender_host: &sender_host,
                content: &content,
                execution_requested,
            })
            .await?;
            repaired += 1;
        }
        Ok(repaired)
    }

    /// Whether the operator canceled the execution intent for a
    /// (message, recipient) pair. A missing intent (a mail-only
    /// message) means "not canceled" — it still delivers.
    ///
    /// # Arguments
    ///
    /// * `message_id` — the message.
    /// * `recipient_peer` — the recipient.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn recipient_intent_canceled(
        &self,
        message_id: &str,
        recipient_peer: &str,
    ) -> Result<bool> {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM execution_intents
             WHERE message_id = ? AND recipient_id = ?",
        )
        .bind(message_id)
        .bind(recipient_peer)
        .fetch_optional(self.pool())
        .await?;
        Ok(state.as_deref() == Some("canceled"))
    }

    /// Lists terminal attempt outcomes that a hub-originated delivery
    /// has not yet reported back to the hub (the H-10 outcome leg).
    ///
    /// Only attempts whose (message, recipient) pair has a mirrored
    /// delivery-task row qualify — those are hub-originated deliveries;
    /// node-local messages have no such row and are never reported.
    ///
    /// # Returns
    ///
    /// The pending reports in completion order (oldest first).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn hub_pending_outcomes(&self) -> Result<Vec<AttemptOutcomeReport>> {
        let rows = sqlx::query(
            "SELECT a.id AS attempt_id, a.state,
                    i.message_id, i.recipient_id, m.conversation_id
             FROM attempts a
             JOIN execution_intents i ON i.id = a.intent_id
             JOIN messages m ON m.id = i.message_id
             WHERE a.state IN ('completed', 'failed', 'canceled', 'unknown')
               AND a.id NOT IN (SELECT attempt_id FROM hub_outcome_reports)
             ORDER BY a.terminal_at ASC",
        )
        .fetch_all(self.pool())
        .await?;
        let mut reports = Vec::with_capacity(rows.len());
        for row in rows {
            let message_id: String = row.get("message_id");
            let recipient_peer: String = row.get("recipient_id");
            let task_id = crate::tasks::delivery_task_id(&message_id, &recipient_peer);
            let mirrored: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM a2a_tasks WHERE task_id = ?")
                    .bind(&task_id)
                    .fetch_optional(self.pool())
                    .await?;
            if mirrored.is_none() {
                // A local message: no hub delivery task exists to
                // report against.
                continue;
            }
            reports.push(AttemptOutcomeReport {
                attempt_id: row.get("attempt_id"),
                task_id,
                message_id,
                recipient_peer,
                conversation_id: row.get("conversation_id"),
                state: row.get("state"),
            });
        }
        Ok(reports)
    }

    /// Records an attempt outcome as reported to the hub. Idempotent.
    ///
    /// # Arguments
    ///
    /// * `attempt_id` — the attempt whose outcome was reported.
    /// * `task_id` — the delivery task the report addressed.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn mark_outcome_reported(&self, attempt_id: &str, task_id: &str) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO hub_outcome_reports (attempt_id, task_id, reported_at)
             VALUES (?, ?, ?)",
        )
        .bind(attempt_id)
        .bind(task_id)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Mirrors a node-reported unknown attempt on the hub. Idempotent:
    /// a re-report refreshes the reason and timestamp but preserves any
    /// queued operator decision.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn record_hub_unknown(
        &self,
        attempt_id: &str,
        task_id: &str,
        host_id: &str,
        recipient_id: &str,
        conversation_id: &str,
        stop_reason: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO hub_unknown_attempts
               (attempt_id, task_id, host_id, recipient_id, conversation_id,
                stop_reason, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(attempt_id) DO UPDATE SET
                stop_reason = excluded.stop_reason,
                updated_at = excluded.updated_at",
        )
        .bind(attempt_id)
        .bind(task_id)
        .bind(host_id)
        .bind(recipient_id)
        .bind(conversation_id)
        .bind(stop_reason)
        .bind(&now)
        .bind(&now)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Every mirrored unknown attempt (the hub UI's reconciliation list).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn hub_unknowns(&self) -> Result<Vec<HubUnknownView>> {
        let rows = sqlx::query(
            "SELECT attempt_id, task_id, host_id, recipient_id, conversation_id,
                    stop_reason, decision, created_at
             FROM hub_unknown_attempts
             ORDER BY created_at ASC",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| HubUnknownView {
                attempt_id: row.get("attempt_id"),
                task_id: row.get("task_id"),
                host_id: row.get("host_id"),
                recipient_id: row.get("recipient_id"),
                conversation_id: row.get("conversation_id"),
                stop_reason: row.get("stop_reason"),
                decision: row.get("decision"),
            })
            .collect())
    }

    /// Mirrored unknowns carrying an operator decision, for one host's
    /// poll loop to apply locally.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn hub_pending_reconciliations(&self, host_id: &str) -> Result<Vec<HubUnknownView>> {
        let rows = sqlx::query(
            "SELECT attempt_id, task_id, host_id, recipient_id, conversation_id,
                    stop_reason, decision, created_at
             FROM hub_unknown_attempts
             WHERE host_id = ? AND decision IS NOT NULL
             ORDER BY created_at ASC",
        )
        .bind(host_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| HubUnknownView {
                attempt_id: row.get("attempt_id"),
                task_id: row.get("task_id"),
                host_id: row.get("host_id"),
                recipient_id: row.get("recipient_id"),
                conversation_id: row.get("conversation_id"),
                stop_reason: row.get("stop_reason"),
                decision: row.get("decision"),
            })
            .collect())
    }

    /// Queues the operator's reconciliation decision on a mirrored
    /// unknown attempt (the hub UI path); the owning node's poll loop
    /// applies it. Returns false when the mirror row is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn set_hub_unknown_decision(&self, attempt_id: &str, decision: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE hub_unknown_attempts
             SET decision = ?, updated_at = ?
             WHERE attempt_id = ?",
        )
        .bind(decision)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(attempt_id)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Consumes the mirror row once the node reports the final terminal
    /// state (the reconciliation is complete).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn consume_hub_unknown(&self, attempt_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM hub_unknown_attempts WHERE attempt_id = ?")
            .bind(attempt_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }
}

/// A hub-mirrored unknown attempt: a node-side attempt whose outcome
/// could not be determined, visible and reconcilable from the hub UI.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct HubUnknownView {
    /// The node-side attempt id (the reconciliation key).
    pub attempt_id: String,
    /// The hub delivery task the attempt addressed.
    pub task_id: String,
    /// The owning node's host id.
    pub host_id: String,
    /// The recipient peer.
    pub recipient_id: String,
    /// The conversation.
    pub conversation_id: String,
    /// The dispatcher-recorded reason (redacted at the display boundary).
    pub stop_reason: String,
    /// The queued operator decision, when one exists.
    pub decision: Option<String>,
}

/// A delivery fan-out request: the committed message plus the
/// recipients to fan out to.
#[derive(Debug)]
pub struct RemoteFanoutRequest<'a> {
    /// The hub's own host id — recipients on this host are local.
    pub local_host: &'a str,
    /// The committed conversation.
    pub conversation_id: &'a str,
    /// The committed message id.
    pub message_id: &'a str,
    /// The sender's own wire message id (the origin) — see
    /// [`DeliveryTaskRequest::origin_message_id`](DeliveryTaskRequest::origin_message_id).
    pub origin_message_id: Option<&'a str>,
    /// The enrolled sender.
    pub sender_peer: &'a str,
    /// The message text.
    pub content: &'a str,
    /// Carried into the delivery metadata.
    pub execution_requested: bool,
    /// Recipient peer ids.
    pub recipients: &'a [String],
}

/// A terminal attempt outcome that must be reported to the hub: the
/// outcome of a hub-originated delivery (H-10 — the hub learns
/// execution results, it does not infer them from the ack).
#[derive(Debug)]
pub struct AttemptOutcomeReport {
    /// The terminal attempt whose outcome is reported.
    pub attempt_id: String,
    /// The hub delivery task the outcome addresses (deterministic id).
    pub task_id: String,
    /// The delivered message.
    pub message_id: String,
    /// The recipient peer (also the task's queue owner).
    pub recipient_peer: String,
    /// The conversation, for the hub-side feed event.
    pub conversation_id: String,
    /// The terminal attempt state: `completed`, `failed`, or
    /// `canceled`.
    pub state: String,
}
