//! A2A-first discovery (HLD rev0.9): hub-owned presence, a
//! materialized roster, and the node mirror.
//!
//! The hub's `presence` rows are fed by node reports over the stamped
//! transport; `peers` stays authoritative for identity fields
//! (name/class/host), and `roster` is the materialized read shape —
//! derived on the hub from `peers JOIN presence`, mirrored verbatim on
//! nodes so harness-facing reads (MCP `list-peers`, UI) see one shape
//! in both roles.

use crate::{Result, Store};
use sqlx::Row;

pub mod moderation;
pub mod presence;

impl Store {
    /// Appends one moderation audit row inside the caller's transaction.
    pub(crate) async fn log_moderation(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        peer_id: &str,
        actor: &str,
        action: &str,
        before: Option<&str>,
        after: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO moderation_events (peer_id, actor, action, before, after, at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(peer_id)
        .bind(actor)
        .bind(action)
        .bind(before)
        .bind(after)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

/// One roster row: a peer, its harness identity, the host it lives
/// on, its operator-moderated display state, and its last reported
/// live state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RosterEntry {
    /// The peer id.
    pub peer_id: String,
    /// The display name.
    pub name: String,
    /// The operator display alias, when one is set.
    pub alias: Option<String>,
    /// The peer class (`human`, `harness`, `a2a_native`).
    pub class: String,
    /// The host the peer is bound to.
    pub host_id: String,
    /// The hub lifecycle status (`enrolled`, `booted`, `banned`).
    pub status: String,
    /// The last reported live state.
    pub state: String,
    /// When the row last changed (RFC 3339).
    pub updated_at: String,
}

/// The operator-moderated hub lifecycle of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    /// Normal participation.
    Enrolled,
    /// Ejected from the hub; enrolment and participation are refused
    /// until the operator restores the peer.
    Booted,
    /// Ejected and certificate-revoked; the peer's certificate is on
    /// the deny-list and cannot reconnect until the operator restores
    /// it and the revocation is removed.
    Banned,
}

/// One moderation audit row: a cohort action with its actor and
/// before/after values. Deliberately has no foreign key on `peer_id` —
/// the audit must survive a soft-forgotten (or ever-deleted) peer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
pub struct ModerationEvent {
    /// The audit row id.
    pub id: i64,
    /// The peer the action targeted.
    pub peer_id: String,
    /// Who performed it (`operator` for UI actions, `system` for
    /// first-presence enrolment).
    pub actor: String,
    /// The action (`enrol`, `alias`, `boot`, `ban`, `restore`, `forget`).
    pub action: String,
    /// The value before the change, when meaningful.
    pub before: Option<String>,
    /// The value after the change.
    pub after: Option<String>,
    /// When it happened (RFC 3339).
    pub at: String,
}

impl PeerStatus {
    /// Parses a status string, rejecting unknown values.
    ///
    /// # Arguments
    ///
    /// * `value` — the status string (`enrolled`, `booted`, `banned`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "enrolled" => Some(Self::Enrolled),
            "booted" => Some(Self::Booted),
            "banned" => Some(Self::Banned),
            _ => None,
        }
    }

    /// The wire/DB form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enrolled => "enrolled",
            Self::Booted => "booted",
            Self::Banned => "banned",
        }
    }

    /// Whether this status refuses enrolment and participation.
    pub fn is_ejected(self) -> bool {
        matches!(self, Self::Booted | Self::Banned)
    }
}

/// Whether a status string denotes an ejected peer (booted or banned).
///
/// The string form is the store's persisted status; this is the single
/// policy expression every ejection check routes through.
///
/// # Arguments
///
/// * `value` — the status string (`enrolled`, `booted`, `banned`).
pub fn status_is_ejected(value: &str) -> bool {
    PeerStatus::parse(value).is_some_and(PeerStatus::is_ejected)
}

/// The outcome of a presence upsert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceOutcome {
    /// The row was inserted or its state updated.
    Updated,
    /// The peer already belongs to a different host; the presenting
    /// host may not claim it (acceptance's enrolment rule — host
    /// cutover stays the explicit operator path).
    HostConflict { owner_host: String },
    /// The peer has been booted or banned by the operator; enrolment
    /// and presence are refused.
    Ejected { status: String },
}

/// Maps a roster query row onto its struct.
pub(crate) fn roster_entry(row: sqlx::sqlite::SqliteRow) -> RosterEntry {
    RosterEntry {
        peer_id: row.get("peer_id"),
        name: row.get("name"),
        alias: row.get("alias"),
        class: row.get("class"),
        host_id: row.get("host_id"),
        status: row.get("status"),
        state: row.get("state"),
        updated_at: row.get("updated_at"),
    }
}
