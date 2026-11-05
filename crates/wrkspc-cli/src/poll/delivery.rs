//! Delivery parsing and the claim/mirror/ack pass over one page of
//! hub delivery tasks.

use std::future::Future;
use wrkspc_core::{Store, TaskRow};

/// A parsed delivery payload from a hub task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The hub task id (the ack handle).
    pub task_id: String,
    /// The hub-committed message id (the local idempotency key).
    pub message_id: String,
    /// The hub conversation id (preserved in the local mirror).
    pub conversation_id: String,
    /// The message's author.
    pub sender_peer: String,
    /// The sender's display name.
    pub sender_name: String,
    /// The sender's peer class.
    pub sender_class: String,
    /// The sender's host id.
    pub sender_host: String,
    /// The delivery's destination peer (a peer on this node).
    pub recipient_peer: String,
    /// The recipient's display name.
    pub recipient_name: String,
    /// The recipient's peer class.
    pub recipient_class: String,
    /// The message text.
    pub content: String,
    /// Whether the harness should be prompted.
    pub execution_requested: bool,
}

impl Delivery {
    /// Parses a delivery from a hub task row's payload.
    ///
    /// # Arguments
    ///
    /// * `task` — the hub task row.
    ///
    /// # Returns
    ///
    /// The delivery, or `None` when the row is not a well-formed
    /// delivery payload.
    pub fn from_task_row(task: &TaskRow) -> Option<Self> {
        let payload: serde_json::Value = serde_json::from_str(&task.task_json).ok()?;
        let delivery = payload
            .get("metadata")
            .and_then(|metadata| metadata.get("delivery"))?;
        if delivery.get("kind").and_then(|k| k.as_str()) != Some("delivery") {
            return None;
        }
        let field = |key: &str| {
            delivery
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        Some(Self {
            task_id: task.task_id.clone(),
            message_id: field("message_id")?,
            conversation_id: field("conversation_id")?,
            sender_peer: field("sender_peer")?,
            sender_name: field("sender_name").unwrap_or_default(),
            sender_class: field("sender_class").unwrap_or_default(),
            sender_host: field("sender_host")?,
            recipient_peer: field("recipient_peer")?,
            recipient_name: field("recipient_name").unwrap_or_default(),
            recipient_class: field("recipient_class").unwrap_or_default(),
            content: field("content").unwrap_or_default(),
            execution_requested: delivery
                .get("execution_requested")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        })
    }
}

/// The outcome of one poll pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PollOutcome {
    /// Deliveries durably recorded and acked.
    pub processed: usize,
    /// Deliveries left unacked (retried on the next poll — the hub
    /// keeps them visible as "not enforced").
    pub failed: usize,
}

/// Processes one page of hub delivery tasks against the node's local
/// store.
///
/// Order per delivery: CLAIM (exclusive, before any local write) →
/// mirror peers + conversation → record the message (`accept_message`,
/// idempotent on the hub message id) → ack → local processed marker.
/// A delivery the caller fails to claim belongs to another node and is
/// skipped, not failed. The local marker is written only after the ack,
/// so a crash anywhere in the chain re-runs the whole delivery
/// idempotently on the next poll.
///
/// # Arguments
///
/// * `store` — the node's local store.
/// * `host_id` — this node's host id (its certificate SAN); the
///   recipient peer is enrolled on it.
/// * `tasks` — the hub's delivery task rows for this host.
/// * `claim` — the claim callback (`POST /delivery-claim` over the
///   mTLS client); returns `true` when this node holds the exclusive
///   claim (or already held it), `false` when another host does.
/// * `ack` — the ack callback (`POST /delivery-ack` over the mTLS
///   client); returns `true` on success.
///
/// # Returns
///
/// The pass outcome.
pub async fn process_deliveries<CF, CFut, AF, AFut>(
    store: &Store,
    host_id: &str,
    tasks: &[TaskRow],
    mut claim: CF,
    mut ack: AF,
) -> PollOutcome
where
    CF: FnMut(&str) -> CFut,
    CFut: Future<Output = bool>,
    AF: FnMut(&str) -> AFut,
    AFut: Future<Output = bool>,
{
    let mut outcome = PollOutcome::default();
    for task in tasks {
        if task.terminal {
            continue;
        }
        let Some(delivery) = Delivery::from_task_row(task) else {
            continue;
        };
        // Already processed in a previous pass (the marker doubles as
        // the dedup gate).
        if store
            .task_get(&delivery.task_id)
            .await
            .is_ok_and(|row| row.is_some())
        {
            continue;
        }

        // The exclusive claim comes FIRST: without it, two node
        // processes sharing one cert would both mirror and dispatch.
        if !claim(&delivery.task_id).await {
            tracing::info!(task_id = %delivery.task_id, "delivery claimed by another node; skipping");
            continue;
        }

        // Mirror the peers exactly as the hub enrolled them; the
        // recipient lives on this node.
        if let Err(error) = store
            .ensure_peer_on_host(
                &delivery.sender_peer,
                &delivery.sender_name,
                &delivery.sender_class,
                &delivery.sender_host,
            )
            .await
        {
            tracing::error!(%error, task_id = %delivery.task_id, "sender mirror failed");
            outcome.failed += 1;
            continue;
        }
        // Replay the hub's host assignment when the local projection is
        // stale: the hub is authoritative for peer records (A2
        // contract), and assignment is append-only history.
        if store
            .peer_host(&delivery.sender_peer)
            .await
            .ok()
            .flatten()
            .as_deref()
            != Some(delivery.sender_host.as_str())
            && let Err(error) = store
                .assign_peer_host(&delivery.sender_peer, &delivery.sender_host)
                .await
        {
            tracing::warn!(%error, task_id = %delivery.task_id, "sender host assignment replay failed");
        }
        if let Err(error) = store
            .ensure_peer_on_host(
                &delivery.recipient_peer,
                &delivery.recipient_name,
                &delivery.recipient_class,
                host_id,
            )
            .await
        {
            tracing::error!(%error, task_id = %delivery.task_id, "recipient mirror failed");
            outcome.failed += 1;
            continue;
        }
        if store
            .peer_host(&delivery.recipient_peer)
            .await
            .ok()
            .flatten()
            .as_deref()
            != Some(host_id)
            && let Err(error) = store
                .assign_peer_host(&delivery.recipient_peer, host_id)
                .await
        {
            tracing::warn!(%error, task_id = %delivery.task_id, "recipient host assignment replay failed");
        }
        if let Err(error) = store
            .ensure_conversation(
                &delivery.conversation_id,
                &[&delivery.sender_peer, &delivery.recipient_peer],
            )
            .await
        {
            tracing::error!(%error, task_id = %delivery.task_id, "conversation mirror failed");
            outcome.failed += 1;
            continue;
        }

        // The local record (hub message id as the idempotency key, so
        // re-polls after a crash replay instead of duplicating). With
        // execution requested this also raises the intent the local
        // dispatcher claims.
        let record = store
            .accept_message(
                &delivery.conversation_id,
                &delivery.sender_peer,
                &delivery.message_id,
                &delivery.content,
                delivery.execution_requested,
                &[wrkspc_core::RecipientRequest {
                    peer_id: delivery.recipient_peer.clone(),
                    execution_requested: delivery.execution_requested,
                }],
            )
            .await;
        match record {
            Ok(wrkspc_core::AcceptOutcome::Accepted { .. })
            | Ok(wrkspc_core::AcceptOutcome::Replay { .. }) => {}
            Err(error) => {
                tracing::error!(%error, task_id = %delivery.task_id, "local record failed");
                outcome.failed += 1;
                continue;
            }
        }

        // The fence-ack. Only on success is the processed marker
        // written, so the delivery stays pollable until the hub has
        // confirmed the handoff.
        if !ack(&delivery.task_id).await {
            tracing::warn!(task_id = %delivery.task_id, "delivery ack failed; retrying next poll");
            outcome.failed += 1;
            continue;
        }
        if let Err(error) = store
            .task_create(
                &delivery.task_id,
                host_id,
                "completed",
                true,
                &task.task_json,
            )
            .await
        {
            // The hub already acks; the local marker is only dedup
            // bookkeeping, so the next poll replays the (idempotent)
            // record and re-acks.
            tracing::error!(%error, task_id = %delivery.task_id, "processed marker failed");
            outcome.failed += 1;
            continue;
        }
        tracing::info!(task_id = %delivery.task_id, message_id = %delivery.message_id, "delivery processed");
        outcome.processed += 1;
    }
    outcome
}
