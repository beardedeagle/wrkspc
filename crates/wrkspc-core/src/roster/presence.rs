//! Presence upserts (the acceptance enrolment rule), roster reads,
//! and the node-side mirror replacement.

use super::{PresenceOutcome, RosterEntry, roster_entry, status_is_ejected};
use crate::Result;
use crate::store::Store;

impl Store {
    pub async fn upsert_roster_presence(
        &self,
        peer_id: &str,
        name: &str,
        class: &str,
        host_id: &str,
        state: &str,
    ) -> Result<PresenceOutcome> {
        let now = chrono::Utc::now().to_rfc3339();
        let mut tx = self.pool().begin().await?;
        sqlx::query("INSERT OR IGNORE INTO hosts (host_id, name, created_at) VALUES (?, ?, ?)")
            .bind(host_id)
            .bind(host_id)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        let existing: Option<(String,)> = sqlx::query_as("SELECT host_id FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some((owner_host,)) = existing {
            if owner_host != host_id {
                return Ok(PresenceOutcome::HostConflict { owner_host });
            }
            // Ejected peers may not re-enter through presence; only the
            // operator's restore action returns them to `enrolled`.
            //
            // A removed cohort entry is different: the delete was about not
            // being able to tell entries apart, so the peer re-enrols cleanly
            // on its next turn with no alias, instead of being locked out
            // until someone finds it and restores it.
            if let Some((status, forgotten_at)) = sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT status, forgotten_at FROM peers WHERE id = ?",
            )
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?
            {
                if status_is_ejected(&status) {
                    return Ok(PresenceOutcome::Ejected { status });
                }
                if forgotten_at.is_some() {
                    sqlx::query(
                        "UPDATE peers SET forgotten_at = NULL, alias = NULL, name = ? WHERE id = ?",
                    )
                    .bind(name)
                    .bind(peer_id)
                    .execute(&mut *tx)
                    .await?;
                    Self::log_moderation(&mut tx, peer_id, "system", "enrol", None, Some(host_id))
                        .await?;
                }
            }
        } else {
            // First presence enrols the peer onto the presenting host
            // (append-only assignment history, same shape as
            // `ensure_peer_on_host`).
            sqlx::query(
                "INSERT INTO peers (id, host_id, name, class, enrolled_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(peer_id)
            .bind(host_id)
            .bind(name)
            .bind(class)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO peer_host_assignments (peer_id, host_id, assigned_at)
                 VALUES (?, ?, ?)",
            )
            .bind(peer_id)
            .bind(host_id)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            // First enrolment is an audit event (system actor — the
            // operator approved the topology, the presence leg enrolled).
            Self::log_moderation(&mut tx, peer_id, "system", "enrol", None, Some(host_id)).await?;
        }
        sqlx::query(
            "INSERT INTO presence (peer_id, state, updated_at)
             VALUES (?, ?, ?)
             ON CONFLICT(peer_id) DO UPDATE SET
                 state = excluded.state,
                 updated_at = excluded.updated_at",
        )
        .bind(peer_id)
        .bind(state)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        // Materialize the roster row from the authoritative identity
        // fields (peers) joined with the presence state.
        sqlx::query(
            "INSERT INTO roster (peer_id, name, alias, class, host_id, status, state, updated_at)
             SELECT p.id, p.name, p.alias, p.class, p.host_id, p.status, pr.state, pr.updated_at
             FROM peers p JOIN presence pr ON pr.peer_id = p.id
             WHERE p.id = ?
             ON CONFLICT(peer_id) DO UPDATE SET
                 name = excluded.name,
                 alias = excluded.alias,
                 class = excluded.class,
                 host_id = excluded.host_id,
                 status = excluded.status,
                 state = excluded.state,
                 updated_at = excluded.updated_at",
        )
        .bind(peer_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(PresenceOutcome::Updated)
    }

    /// Lists the roster, ordered by peer id.
    ///
    /// One read shape for both roles: the hub's materialized rows and
    /// the node's mirror live in the same table.
    ///
    /// # Returns
    ///
    /// All roster rows.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn roster(&self) -> Result<Vec<RosterEntry>> {
        let rows = sqlx::query(
            "SELECT peer_id, name, alias, class, host_id, status, state, updated_at
             FROM roster
             ORDER BY peer_id",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(roster_entry).collect())
    }

    /// Reads one roster row.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to look up.
    ///
    /// # Returns
    ///
    /// The row, or `None` when the peer has no roster entry.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn roster_entry(&self, peer_id: &str) -> Result<Option<RosterEntry>> {
        let row = sqlx::query(
            "SELECT peer_id, name, alias, class, host_id, status, state, updated_at
             FROM roster
             WHERE peer_id = ?",
        )
        .bind(peer_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(roster_entry))
    }

    /// Replaces the local roster mirror with the hub's rows.
    ///
    /// Node-only: the poll loop fetches the hub roster and mirrors it
    /// here so harness-facing reads see the connected cohort without
    /// hub round-trips. All-or-nothing; a failed fetch keeps the last
    /// known mirror (stale-but-visible, per the freshness decision).
    ///
    /// # Arguments
    ///
    /// * `entries` — the hub's current roster.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn replace_roster(&self, entries: &[RosterEntry]) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("DELETE FROM roster").execute(&mut *tx).await?;
        for entry in entries {
            sqlx::query(
                "INSERT INTO roster (peer_id, name, alias, class, host_id, status, state, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&entry.peer_id)
            .bind(&entry.name)
            .bind(&entry.alias)
            .bind(&entry.class)
            .bind(&entry.host_id)
            .bind(&entry.status)
            .bind(&entry.state)
            .bind(&entry.updated_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
