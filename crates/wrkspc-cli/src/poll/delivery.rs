//! Delivery parsing and the claim/mirror/ack pass over one page of
//! hub delivery tasks.

use std::future::Future;
use wrkspc_core::{Store, TaskRow};

/// A parsed delivery payload from a hub task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The hub task id (the ack handle).
    pub task_id: String,
    /// The hub-committed message id (the hub-side record).
    pub message_id: String,
    /// The sender's own wire message id: when the delivery is an echo
    /// of a message THIS node propagated upward, this equals the local
    /// message id and the mirror must map back onto that row (never a
    /// second message/execution intent). `None` for hub-originated
    /// posts.
    pub origin_message_id: Option<String>,
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
    /// Pending hub guidance riding the envelope (hub→node propagation):
    /// `(id, author_id, text)` — the node ingests these before the
    /// delivery's turn is dispatched.
    pub guidance: Vec<(String, String, String)>,
}

impl Delivery {
    /// Parses a delivery from a hub task row's payload.
    ///
    /// `task` is the hub task row.
    ///
    /// # Returns
    ///
    /// The delivery, or `None` when the row is not a well-formed
    /// delivery payload.
    pub fn from_task_row(task: &TaskRow) -> Option<Self> {
        // C2: the typed envelope — a corrupt blob is visible to the
        // operator, never silently skipped like a non-delivery row: a
        // delivery this node should have mirrored is otherwise lost
        // without a trace. A blob without the envelope or the delivery
        // kind is malformed, never a different legitimate task type:
        // log it so a wedged row is distinguishable from an empty
        // queue.
        let Some(envelope) = wrkspc_core::DeliveryEnvelope::from_task_json(&task.task_json) else {
            tracing::warn!(
                task_id = %task.task_id,
                "delivery task payload carries no delivery envelope; skipping",
            );
            return None;
        };
        if envelope.kind != "delivery" {
            tracing::warn!(
                task_id = %task.task_id,
                "delivery task payload carries no delivery kind; skipping",
            );
            return None;
        }
        Some(Self {
            task_id: task.task_id.clone(),
            message_id: envelope.message_id,
            origin_message_id: envelope.origin_message_id,
            conversation_id: envelope.conversation_id,
            sender_peer: envelope.sender_peer,
            sender_name: envelope.sender_name,
            sender_class: envelope.sender_class,
            sender_host: envelope.sender_host,
            recipient_peer: envelope.recipient_peer,
            recipient_name: envelope.recipient_name,
            recipient_class: envelope.recipient_class,
            content: envelope.content,
            execution_requested: envelope.execution_requested,
            guidance: envelope
                .guidance
                .iter()
                .filter_map(|entry| {
                    Some((
                        entry.get("id")?.as_str()?.to_string(),
                        entry.get("author_id")?.as_str()?.to_string(),
                        entry.get("text")?.as_str()?.to_string(),
                    ))
                })
                .collect(),
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
    /// Deliveries the hub refused to claim because a moderation gate
    /// (fence, hold, or peer status) is effective — left untouched and
    /// unacked, so they stay pollable until the gate clears.
    pub gated: usize,
}

/// The hub's verdict for one delivery claim attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This node holds the exclusive claim; mirroring may proceed.
    Claimed,
    /// Another node instance holds the claim; skip without mirroring.
    ForeignClaimed,
    /// A hub moderation gate blocks the claim; skip without mirroring
    /// (the delivery stays pollable until the gate clears).
    Gated,
}

/// Processes one page of hub delivery tasks against the node's local
/// store.
///
/// Order per delivery: CLAIM (exclusive, before any local write) →
/// mirror peers + conversation → record the message (`accept_message`,
/// idempotent on the hub message id) → ack → local processed marker.
/// A delivery the caller fails to claim belongs to another node and is
/// skipped, not failed — and a delivery the hub refuses to claim
/// (moderation gate) is skipped WITHOUT mirroring: the hub is the
/// authority on fences and holds, and nothing may become locally
/// executable against its verdict. The local marker is written only
/// after the ack, so a crash anywhere in the chain re-runs the whole
/// delivery idempotently on the next poll.
///
/// `store` is the node's local store; `host_id` is this node's host id
/// (its certificate SAN), the host the recipient peer is enrolled on;
/// `tasks` is the hub's delivery task rows for this host; `claim` is
/// the claim callback (`POST /delivery-claim` over the mTLS client)
/// returning the hub's claim verdict; `ack` is the ack callback
/// (`POST /delivery-ack` over the mTLS client) returning `true` on
/// success.
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
    CFut: Future<Output = ClaimOutcome>,
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
        // A gate verdict skips WITHOUT mirroring — the hub's moderation
        // is authoritative and nothing becomes locally executable
        // against it.
        match claim(&delivery.task_id).await {
            ClaimOutcome::Claimed => {}
            ClaimOutcome::ForeignClaimed => {
                tracing::info!(
                    task_id = %delivery.task_id,
                    "delivery claim unavailable (foreign instance or transport failure); skipping"
                );
                continue;
            }
            ClaimOutcome::Gated => {
                tracing::info!(
                    task_id = %delivery.task_id,
                    "delivery gated by hub moderation; skipping until the gate clears"
                );
                outcome.gated += 1;
                continue;
            }
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
        // contract), and assignment is append-only history. A store
        // ERROR is not a stale projection — folding it into "mismatch"
        // would trigger a spurious assignment replay; it fails the
        // delivery instead (retried next poll).
        match store.peer_host(&delivery.sender_peer).await {
            Ok(Some(host)) if host == delivery.sender_host.as_str() => {}
            Ok(_) => {
                if let Err(error) = store
                    .assign_peer_host(&delivery.sender_peer, &delivery.sender_host)
                    .await
                {
                    tracing::warn!(%error, task_id = %delivery.task_id, "sender host assignment replay failed");
                }
            }
            Err(error) => {
                tracing::error!(%error, task_id = %delivery.task_id, "sender host lookup failed");
                outcome.failed += 1;
                continue;
            }
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
        // The recipient's host projection is replayed the same way: a
        // store ERROR fails the delivery instead of pretending the
        // projection is stale (see the sender arm above).
        match store.peer_host(&delivery.recipient_peer).await {
            Ok(Some(host)) if host == host_id => {}
            Ok(_) => {
                if let Err(error) = store
                    .assign_peer_host(&delivery.recipient_peer, host_id)
                    .await
                {
                    tracing::warn!(%error, task_id = %delivery.task_id, "recipient host assignment replay failed");
                }
            }
            Err(error) => {
                tracing::error!(%error, task_id = %delivery.task_id, "recipient host lookup failed");
                outcome.failed += 1;
                continue;
            }
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

        // Hub guidance rides in FIRST: the ingest must land before the
        // dispatcher claims the intent this mirror creates, so the
        // hub-UI guidance reaches the very next prompt (idempotent on
        // redelivery — the hub's ids are the keys).
        if !delivery.guidance.is_empty()
            && let Err(error) = store
                .ingest_guidance(&delivery.conversation_id, &delivery.guidance)
                .await
        {
            tracing::error!(%error, task_id = %delivery.task_id, "guidance ingest failed");
            outcome.failed += 1;
            continue;
        }

        // The local record. The idempotency key is the delivery's
        // ORIGIN id: for a message this node propagated upward, the
        // origin is the original LOCAL message id, so a co-hosted echo
        // replays that row instead of creating a second message and
        // execution intent. Hub-originated posts keep the hub id as
        // the key. With execution requested this also raises the
        // intent the local dispatcher claims.
        let accept_key = delivery
            .origin_message_id
            .clone()
            .unwrap_or_else(|| delivery.message_id.clone());
        let record = store
            .accept_message(
                &delivery.conversation_id,
                &delivery.sender_peer,
                &accept_key,
                &delivery.content,
                delivery.execution_requested,
                &[wrkspc_core::RecipientRequest {
                    peer_id: delivery.recipient_peer.clone(),
                    execution_requested: delivery.execution_requested,
                }],
            )
            .await;
        match record {
            Ok(wrkspc_core::AcceptOutcome::Accepted { message_id })
            | Ok(wrkspc_core::AcceptOutcome::Replay { message_id }) => {
                // A mirrored message is not outbound work. Ledger it under its
                // LOCAL id so `hub_pending_outbound` never re-sends it: the
                // outbound query selects messages whose sender is enrolled on
                // this host, and a mirrored row keeps the original sender — so
                // without this a local peer's own message returns from the hub
                // and re-qualifies as outbound, looping once per poll tick.
                //
                // The id must be the local one. The hub id is only the
                // idempotency key; `hub_outbound.message_id` references
                // `messages(id)`, so ledgering the hub id fails the foreign key
                // and leaves the row eligible — the loop survives, silently.
                if let Err(error) = store.mark_hub_sent(&message_id).await {
                    tracing::warn!(
                        %error,
                        task_id = %delivery.task_id,
                        "hub-sent ledger write failed"
                    );
                }
            }
            Err(error) => {
                match &error {
                    // Permanent rejection verdicts (mirrors the acceptance
                    // translation's mapping): no poll retry can change the
                    // local store's answer — the hub-side record conflicts
                    // with committed local state, or policy refuses it.
                    // Warn, write the local skip marker (the next poll's
                    // dedup gate passes over the delivery), and leave the
                    // hub delivery unacked so it stays visible for operator
                    // reconciliation instead of failing every poll with
                    // log noise.
                    wrkspc_core::StoreError::IdempotencyConflict { .. }
                    | wrkspc_core::StoreError::ExecutionAuthorityConflict { .. }
                    | wrkspc_core::StoreError::PeerEjected { .. }
                    | wrkspc_core::StoreError::TooManyRecipients { .. } => {
                        tracing::warn!(
                            %error,
                            task_id = %delivery.task_id,
                            "delivery rejected permanently; skipping without retry"
                        );
                        if let Err(marker_error) = store
                            .task_create(
                                &delivery.task_id,
                                host_id,
                                "rejected",
                                true,
                                &task.task_json,
                            )
                            .await
                        {
                            tracing::error!(
                                %marker_error,
                                task_id = %delivery.task_id,
                                "permanent-rejection marker failed"
                            );
                        }
                    }
                    // Transient (NotFound races on the mirror leg, Db
                    // failures, anything else): the delivery stays pollable
                    // and retries on the next poll — the hub keeps it
                    // visible as "not enforced".
                    _ => {
                        tracing::error!(%error, task_id = %delivery.task_id, "local record failed");
                        outcome.failed += 1;
                    }
                }
                // Neither rejection path reaches the ack leg: the hub
                // delivery is not enforced.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a hub-side delivery task row payload for tests.
    fn hub_delivery_row(task_id: &str) -> TaskRow {
        TaskRow {
            task_id: task_id.to_string(),
            context_id: "node-test-1".to_string(),
            state: "submitted".to_string(),
            terminal: false,
            version: 1,
            task_json: serde_json::json!({
                "id": task_id,
                "contextId": "node-test-1",
                "status": {"state": "TASK_STATE_SUBMITTED"},
                "metadata": {
                    "delivery": {
                        "kind": "delivery",
                        "message_id": "hub-msg-conflict",
                        "conversation_id": "hub-conv-conflict",
                        "sender_peer": "operator",
                        "sender_name": "operator",
                        "sender_class": "human",
                        "sender_host": "hub-real-host",
                        "recipient_peer": "codex",
                        "recipient_name": "codex",
                        "recipient_class": "harness",
                        "content": "hello from the hub",
                        "execution_requested": true,
                    },
                },
            })
            .to_string(),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// A permanent acceptance verdict (idempotency conflict with
    /// committed local state) skips WITHOUT retry: warn-level log, no
    /// failed counter, no ack, and a local skip marker so the next
    /// poll's dedup gate passes the delivery over.
    #[tokio::test]
    async fn permanent_acceptance_rejection_skips_without_retry() {
        let store = Store::open_in_memory("poll-permanent").await.unwrap();
        store
            .ensure_peer("codex", "codex", "harness")
            .await
            .unwrap();
        store
            .ensure_peer("operator", "operator", "human")
            .await
            .unwrap();
        // The delivery's idempotency key (`hub-msg-conflict`, the hub
        // message id — there is no origin id) is already committed
        // locally with DIFFERENT content: the hub-side record conflicts
        // and no retry can change that.
        assert!(
            store
                .ensure_conversation("hub-conv-conflict", &["operator"])
                .await
                .unwrap(),
            "the delivery's conversation is created here"
        );
        store
            .accept_message(
                "hub-conv-conflict",
                "operator",
                "hub-msg-conflict",
                "original local content",
                false,
                &[],
            )
            .await
            .expect("seed conflict");

        let row = hub_delivery_row("task-conflict");
        let mut acked = Vec::new();
        let outcome = process_deliveries(
            &store,
            "node-test-1",
            std::slice::from_ref(&row),
            |_| std::future::ready(ClaimOutcome::Claimed),
            |task_id| {
                acked.push(task_id.to_string());
                std::future::ready(true)
            },
        )
        .await;
        assert_eq!(
            outcome,
            PollOutcome {
                processed: 0,
                failed: 0,
                gated: 0,
            },
            "a permanent rejection is not a retryable failure"
        );
        assert!(
            acked.is_empty(),
            "a rejected delivery is never acked as enforced"
        );
        // The local skip marker exists, so the next poll's dedup gate
        // passes over the delivery — no claim, no mirror, no retry.
        let marker = store
            .task_get("task-conflict")
            .await
            .expect("marker read")
            .expect("marker written");
        assert!(marker.terminal);

        let mut claims = 0;
        let outcome = process_deliveries(
            &store,
            "node-test-1",
            std::slice::from_ref(&row),
            |_| {
                claims += 1;
                std::future::ready(ClaimOutcome::Claimed)
            },
            |_| std::future::ready(true),
        )
        .await;
        assert_eq!(
            outcome,
            PollOutcome {
                processed: 0,
                failed: 0,
                gated: 0,
            }
        );
        assert_eq!(
            claims, 0,
            "the skip marker must short-circuit the next poll before the claim"
        );
    }

    /// A transient acceptance failure (NotFound — the delivery's sender
    /// is not a member of the already-existing local conversation) is
    /// retryable: it counts as failed, is never acked, and no skip
    /// marker is written, so the next poll re-runs it.
    #[tokio::test]
    async fn transient_acceptance_failure_retries_on_the_next_poll() {
        let store = Store::open_in_memory("poll-transient").await.unwrap();
        store
            .ensure_peer("codex", "codex", "harness")
            .await
            .unwrap();
        store
            .ensure_peer("operator", "operator", "human")
            .await
            .unwrap();
        // The conversation already exists locally WITHOUT the sender
        // as a member; `ensure_conversation` enrols members on creation
        // only, so acceptance finds a non-member and fails with
        // NotFound — a transient shape the mirror leg cannot repair
        // itself.
        assert!(
            store
                .ensure_conversation("hub-conv-conflict", &["codex"])
                .await
                .unwrap(),
            "the conversation is created here"
        );

        let row = hub_delivery_row("task-transient");
        let mut acked = Vec::new();
        let outcome = process_deliveries(
            &store,
            "node-test-1",
            std::slice::from_ref(&row),
            |_| std::future::ready(ClaimOutcome::Claimed),
            |task_id| {
                acked.push(task_id.to_string());
                std::future::ready(true)
            },
        )
        .await;
        assert_eq!(
            outcome,
            PollOutcome {
                processed: 0,
                failed: 1,
                gated: 0,
            },
            "a transient acceptance failure stays retryable"
        );
        assert!(acked.is_empty(), "an unaccepted delivery is never acked");
        assert!(
            store.task_get("task-transient").await.unwrap().is_none(),
            "no skip marker for a transient failure"
        );

        // The next poll retries it (the claim runs again): no marker
        // short-circuits the pass.
        let mut claims = 0;
        let outcome = process_deliveries(
            &store,
            "node-test-1",
            std::slice::from_ref(&row),
            |_| {
                claims += 1;
                std::future::ready(ClaimOutcome::Claimed)
            },
            |_| std::future::ready(true),
        )
        .await;
        assert_eq!(outcome.failed, 1);
        assert_eq!(claims, 1, "a transient failure is retried on the next poll");
    }
}
