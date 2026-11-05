//! Protocol-agnostic task persistence for the A2A `TaskStore` seam.
//!
//! wrkspc-core owns every SQL statement in this codebase; protocol crates
//! map their types onto these rows. `state` is the caller's literal,
//! `terminal` is a plain flag, so the terminal-immutability invariant is
//! enforced here without parsing A2A vocabulary. Pagination is keyset
//! (newest first) over `(updated_at, task_id)`.

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
    /// The recipient's [`PeerClass`] string (for the node-side mirror).
    pub recipient_class: &'a str,
    /// The hub-committed message this delivery belongs to.
    pub message_id: &'a str,
    /// The conversation the message belongs to.
    pub conversation_id: &'a str,
    /// The message's author.
    pub sender_peer: &'a str,
    /// The sender's display name (for the node-side mirror).
    pub sender_name: &'a str,
    /// The sender's [`PeerClass`] string (for the node-side mirror).
    pub sender_class: &'a str,
    /// The sender's `host_id` (for the node-side mirror).
    pub sender_host: &'a str,
    /// The message text.
    pub content: &'a str,
    /// Whether the node should prompt the harness.
    pub execution_requested: bool,
}

/// Outcome of [`Store::claim_delivery`](Store::claim_delivery).
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
pub(crate) fn task_row_from_sql(row: sqlx::sqlite::SqliteRow) -> TaskRow {
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
