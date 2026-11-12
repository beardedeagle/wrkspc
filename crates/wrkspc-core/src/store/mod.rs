//! The transactional store: schema, fence flows, events, moderation state.
//!
//! Every state-changing operation runs inside a single SQLite transaction so
//! acceptance, intent creation, and event emission commit or roll back
//! together — the readiness fence from HLD Appendix A. Terminal attempt
//! states are immutable except idempotent re-emission of the same state.

use crate::error::{Result, StoreError};
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

/// A committed event on the resumable feed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Event {
    /// Monotonic cursor position within the conversation.
    pub seq: i64,
    /// Owning conversation.
    pub conversation_id: String,
    /// Event kind, e.g. `message.accepted`, `attempt.claimed`.
    pub kind: String,
    /// JSON payload; shape per kind.
    pub payload: serde_json::Value,
    /// RFC 3339 UTC creation time.
    pub created_at: String,
}

/// Outcome of [`Store::accept_message`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// A fresh message committed with its recipients and intents.
    Accepted { message_id: String },
    /// The idempotency key matched a previously committed message with equal
    /// content; nothing new was written.
    Replay { message_id: String },
}

/// Requested recipient at acceptance time.
#[derive(Debug, Clone)]
pub struct RecipientRequest {
    /// Recipient peer id.
    pub peer_id: String,
    /// Whether execution was explicitly requested for this recipient.
    pub execution_requested: bool,
}

/// The transactional store.
///
/// Owns the SQLite pool and the exclusive dispatcher lock (an OS-level
/// advisory lock on a sidecar file: only ONE exclusive opener may hold
/// it per database, regardless of role labels, and a crashed process
/// releases it automatically). Dropping the LAST clone closes the lock
/// file and releases the lock; use
/// [`Store::release_lock`] when handing the
/// database to another process without dropping the store.
#[derive(Clone, Debug)]
pub struct Store {
    pool: SqlitePool,
    lock_owner: String,
    /// The held OS lock on the sidecar file (exclusive openers only).
    lock_file: std::sync::Arc<std::sync::Mutex<Option<std::fs::File>>>,
}

/// Description of an active fence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Fence {
    /// Fence id.
    pub id: String,
    /// Conversation it applies to.
    pub conversation_id: String,
    /// Peer it gates.
    pub peer_id: String,
    /// Fence kind.
    pub kind: String,
    /// Peer that set it.
    pub set_by: String,
    /// RFC 3339 UTC time it was set.
    pub set_at: String,
    /// RFC 3339 UTC ack time, `None` until acknowledged.
    pub acked_at: Option<String>,
}

pub mod attempts;
pub mod connect;
pub mod conversations;
pub mod events;
pub mod fences;
pub mod messages;
pub mod peers;
pub mod reconcile;

impl Store {
    /// Returns the inner sqlx pool for sibling core modules (tasks, feed).
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Retries a store operation on transient SQLITE_BUSY (`code 5`).
    ///
    /// WAL allows one writer; concurrent writers busy-wait via the
    /// connection's `busy_timeout`, and contention beyond it surfaces as
    /// `database is locked`. Retrying with backoff is sqlite's sanctioned
    /// handling — observed transient holds have silently wedged dispatcher
    /// turns. A genuine long-held lock still fails (and surfaces visibly)
    /// after the retry budget.
    ///
    /// * `op` — the operation to run; must be safe to re-run (all store
    ///   writes here are idempotent or single-attempt).
    /// * `label` — the operation name, for retry logs.
    pub(crate) async fn retry_on_busy<T, F, Fut>(mut op: F, label: &str) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0u32;
        loop {
            match op().await {
                Err(StoreError::Db(sqlx::Error::Database(db)))
                    if matches!(
                        db.code().as_deref(),
                        // SQLITE_BUSY (5) and SQLITE_BUSY_SNAPSHOT
                        // (517): the snapshot variant is the WAL
                        // reader's contention form and must retry the
                        // same way (C11).
                        Some("5") | Some("517")
                    ) && attempt < 10 =>
                {
                    attempt += 1;
                    let delay = std::time::Duration::from_millis(100 * attempt as u64);
                    tracing::warn!(
                        %label,
                        attempt,
                        ?delay,
                        "sqlite busy; retrying",
                    );
                    tokio::time::sleep(delay).await;
                }
                other => return other,
            }
        }
    }

    pub(crate) async fn assert_member(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        conversation_id: &str,
        peer_id: &str,
    ) -> Result<()> {
        let member: Option<i64> = sqlx::query(
            "SELECT 1 FROM conversation_members
             WHERE conversation_id = ? AND peer_id = ?",
        )
        .bind(conversation_id)
        .bind(peer_id)
        .fetch_optional(&mut **tx)
        .await?
        .map(|r| r.get::<i64, _>(0));
        if member.is_none() {
            return Err(StoreError::NotFound {
                entity: "conversation member",
                id: peer_id.to_string(),
            });
        }
        Ok(())
    }
}
