//! Task persistence for the A2A `TaskStore` seam.
//!
//! wrkspc-core owns every SQL statement in this codebase; the rows carry
//! a canonical A2A Task serialization — the stored blob IS the wire Task,
//! with the wrkspc delivery envelope riding in `metadata.delivery`
//! ([`DeliveryEnvelope`], the one schema the builder and every reader
//! share). `state` is the caller's literal, `terminal` is a plain flag,
//! so the terminal-immutability invariant is enforced here without
//! parsing A2A vocabulary. Pagination is keyset (newest first) over
//! `(updated_at, task_id)`.

pub mod store;

use sqlx::Row;

/// A persisted task row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TaskRow {
    /// Task identifier.
    pub task_id: String,
    /// Logical grouping identifier.
    pub context_id: String,
    /// Caller's state literal.
    pub state: String,
    /// Whether the state is terminal (invariant flag).
    pub terminal: bool,
    /// Caller's serialized task blob.
    pub task_json: String,
    /// Store-owned version counter.
    pub version: i64,
    /// RFC 3339 UTC creation time.
    pub created_at: String,
    /// RFC 3339 UTC last-write time.
    pub updated_at: String,
}

/// A delivery to queue for a remote host (D-6 A2 contract: the hub
/// creates the pollable task; the node executes and reports).
#[derive(Debug, Clone, Copy)]
pub struct DeliveryTaskRequest<'a> {
    /// The recipient peer's `host_id` — the queue context the node polls.
    pub recipient_host: &'a str,
    /// The delivery's destination peer.
    pub recipient_peer: &'a str,
    /// The recipient's display name (for the node-side mirror).
    pub recipient_name: &'a str,
    /// The recipient's peer class string (`human`/`harness`/
    /// `a2a_native`, for the node-side mirror).
    pub recipient_class: &'a str,
    /// The hub-committed message this delivery belongs to.
    pub message_id: &'a str,
    /// The sender's own wire message id (the origin): for a message a
    /// node propagated upward, this is the node's LOCAL message id. The
    /// recipient node mirrors under this id, so a co-hosted echo maps
    /// back onto the original local row instead of creating a second
    /// message and execution intent. `None` for hub-originated posts.
    pub origin_message_id: Option<&'a str>,
    /// The conversation the message belongs to.
    pub conversation_id: &'a str,
    /// The message's author.
    pub sender_peer: &'a str,
    /// The sender's display name (for the node-side mirror).
    pub sender_name: &'a str,
    /// The sender's peer class string (`human`/`harness`/
    /// `a2a_native`, for the node-side mirror).
    pub sender_class: &'a str,
    /// The sender's `host_id` (for the node-side mirror).
    pub sender_host: &'a str,
    /// The message text.
    pub content: &'a str,
    /// Whether the node should prompt the harness.
    pub execution_requested: bool,
}

/// The wrkspc delivery envelope riding in `metadata.delivery` of the
/// A2A task shell core writes: ONE schema for the builder and
/// every reader — a typo is a compile error instead of a silent `None`
/// from string indexing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryEnvelope {
    /// The envelope discriminator — always `delivery`.
    pub kind: String,
    /// The hub-committed message this delivery belongs to.
    pub message_id: String,
    /// The sender's own wire message id (the origin), when one exists.
    pub origin_message_id: Option<String>,
    /// The conversation the message belongs to.
    pub conversation_id: String,
    /// The message's author.
    pub sender_peer: String,
    /// The sender's display name (for the node-side mirror).
    pub sender_name: String,
    /// The sender's peer class string.
    pub sender_class: String,
    /// The sender's host id.
    pub sender_host: String,
    /// The delivery's destination peer.
    pub recipient_peer: String,
    /// The recipient's display name (for the node-side mirror).
    pub recipient_name: String,
    /// The recipient's peer class string.
    pub recipient_class: String,
    /// The message text.
    pub content: String,
    /// Whether the node should prompt the harness.
    pub execution_requested: bool,
    /// The pending operator guidance snapshot riding the envelope.
    pub guidance: Vec<serde_json::Value>,
}

impl DeliveryEnvelope {
    /// Builds the canonical A2A task shell for this envelope — the
    /// stored blob the surface serves as the wire Task.
    ///
    /// * `task_id` — the delivery task id.
    /// * `context_id` — the recipient host's queue context.
    pub fn to_task_json(&self, task_id: &str, context_id: &str) -> serde_json::Value {
        serde_json::json!({
            "id": task_id,
            "contextId": context_id,
            "status": {"state": "TASK_STATE_SUBMITTED"},
            "metadata": {"delivery": self},
        })
    }

    /// Reads the envelope back out of a stored task blob.
    ///
    /// * `task_json` — the stored blob.
    ///
    /// # Returns
    ///
    /// The envelope, or `None` when the blob is malformed or carries no
    /// delivery envelope.
    pub fn from_task_json(task_json: &str) -> Option<Self> {
        let payload: serde_json::Value = serde_json::from_str(task_json).ok()?;
        serde_json::from_value(payload.get("metadata")?.get("delivery")?.clone()).ok()
    }
}

/// Outcome of [`crate::store::Store::claim_delivery`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryClaimOutcome {
    /// This node instance now holds the exclusive claim.
    Claimed,
    /// This node instance already holds the claim (idempotent re-claim,
    /// e.g. a restart mid-delivery).
    AlreadyClaimed,
    /// Another node instance holds the claim (double-attach: a second
    /// process presenting the same cert).
    ForeignClaimed {
        /// The claiming instance.
        instance_id: String,
        /// When the claim was taken.
        claimed_at: String,
    },
}

/// Builds a [`TaskRow`] from a sqlx row.
pub(crate) fn task_row_from_sql(row: &sqlx::sqlite::SqliteRow) -> TaskRow {
    TaskRow {
        task_id: row.get("task_id"),
        context_id: row.get("context_id"),
        state: row.get("state"),
        terminal: row.get("terminal"),
        task_json: row.get("task_json"),
        version: row.get("version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// The deterministic delivery-task id for a (message, recipient) pair.
///
/// Fan-out is idempotent because the id is a function of the committed
/// message and recipient: re-queueing the same pair re-creates the
/// SAME row (never a duplicate), so a partially completed fan-out — or
/// a crash between acceptance and queueing — is repairable by simply
/// re-running the fan-out (the hub does this on every send replay and
/// once at startup).
pub fn delivery_task_id(message_id: &str, recipient_peer: &str) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("{message_id}/{recipient_peer}").as_bytes(),
    )
    .to_string()
}
