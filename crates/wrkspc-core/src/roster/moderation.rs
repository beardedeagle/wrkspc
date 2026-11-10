//! Peer lifecycle moderation: alias, status (boot/ban/restore), forget,
//! the audit log, and certificate revocation persistence.

use super::{ModerationEvent, PeerStatus};
use crate::Result;
use crate::error::StoreError;
use crate::store::Store;

impl Store {
    /// Resolves an operator-facing peer reference to the canonical
    /// peer id.
    ///
    /// An EXACT peer id wins first (UI-04): once a client has resolved
    /// a reference to a canonical id, that id must never be re-resolved
    /// as somebody else's alias — `codex` the canonical id always means
    /// the peer `codex`, even when another peer's alias is the string
    /// `codex`. Only when no peer carries the id does the target
    /// resolve as an alias, and an ambiguous alias fails closed.
    ///
    /// # Arguments
    ///
    /// * `target` — the alias or peer id as typed by the operator.
    ///
    /// # Errors
    ///
    /// Returns an error when the reference is an ambiguous alias (two
    /// peers sharing one display name): routing must fail closed rather
    /// than guess a recipient.
    pub async fn resolve_peer_ref(&self, target: &str) -> Result<Option<String>> {
        let by_id: Option<(String,)> = sqlx::query_as("SELECT id FROM peers WHERE id = ?")
            .bind(target)
            .fetch_optional(self.pool())
            .await?;
        if let Some((id,)) = by_id {
            return Ok(Some(id));
        }
        let by_alias: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM peers WHERE alias = ? ORDER BY id LIMIT 2")
                .bind(target)
                .fetch_all(self.pool())
                .await?;
        match by_alias.len() {
            0 => Ok(None),
            1 => Ok(Some(by_alias[0].0.clone())),
            _ => Err(StoreError::AmbiguousPeerRef {
                reference: target.to_string(),
            }),
        }
    }

    pub async fn set_peer_alias(
        &self,
        peer_id: &str,
        alias: Option<&str>,
        actor: &str,
    ) -> Result<()> {
        // IMMEDIATE takes the write lock up front: alias, ban, boot, and
        // forget are read-modify-write paths, and a deferred begin can
        // deadlock another writer mid-upgrade (SQLITE_BUSY surfaces
        // immediately, ignoring busy_timeout).
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        let old: Option<(Option<String>,)> = sqlx::query_as("SELECT alias FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((old,)) = old else {
            tx.rollback().await?;
            return Ok(());
        };
        sqlx::query("UPDATE peers SET alias = ? WHERE id = ?")
            .bind(alias)
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE roster SET alias = ? WHERE peer_id = ?")
            .bind(alias)
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        Self::log_moderation(&mut tx, peer_id, actor, "alias", old.as_deref(), alias).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Moves a peer to a new hub lifecycle status and appends an audit
    /// row.
    ///
    /// `banned` also stamps `revoked_at` and records the peer's stored
    /// certificate serial in `cert_revocations` (the runtime deny-list
    /// source); `enrolled` clears `revoked_at`, drops the revocation,
    /// clears a soft-forget, and re-materializes the roster row (the
    /// forget path deleted it) so the operator's restore is complete.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to moderate.
    /// * `status` — the target status.
    /// * `actor` — who performed the action (audit trail).
    ///
    /// # Returns
    ///
    /// Whether the peer exists and was updated.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn set_peer_status(
        &self,
        peer_id: &str,
        status: PeerStatus,
        actor: &str,
    ) -> Result<bool> {
        // IMMEDIATE: read-modify-write (see set_peer_alias).
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        let exists: Option<(String,)> = sqlx::query_as("SELECT status FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((before,)) = exists else {
            return Ok(false);
        };
        let now = chrono::Utc::now().to_rfc3339();
        match status {
            PeerStatus::Enrolled => {
                sqlx::query("UPDATE peers SET status = 'enrolled', revoked_at = NULL, booted_at = NULL, forgotten_at = NULL WHERE id = ?")
                    .bind(peer_id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM cert_revocations WHERE peer_id = ?")
                    .bind(peer_id)
                    .execute(&mut *tx)
                    .await?;
                // A restored (or un-forgotten) peer needs its cohort row
                // back — the forget path deleted it.
                sqlx::query(
                    "INSERT OR IGNORE INTO roster (peer_id, name, alias, class, host_id, status, state, updated_at)
                     SELECT p.id, p.name, p.alias, p.class, p.host_id, p.status, COALESCE(pr.state, 'unknown'), ?
                     FROM peers p LEFT JOIN presence pr ON pr.peer_id = p.id
                     WHERE p.id = ?",
                )
                .bind(&now)
                .bind(peer_id)
                .execute(&mut *tx)
                .await?;
                Self::log_moderation(
                    &mut tx,
                    peer_id,
                    actor,
                    "restore",
                    Some(&before),
                    Some(status.as_str()),
                )
                .await?;
            }
            PeerStatus::Booted => {
                sqlx::query("UPDATE peers SET status = 'booted', booted_at = ?, forgotten_at = NULL WHERE id = ?")
                    .bind(&now)
                    .bind(peer_id)
                    .execute(&mut *tx)
                    .await?;
                Self::log_moderation(
                    &mut tx,
                    peer_id,
                    actor,
                    "boot",
                    Some(&before),
                    Some(status.as_str()),
                )
                .await?;
            }
            PeerStatus::Banned => {
                sqlx::query("UPDATE peers SET status = 'banned', revoked_at = ?, forgotten_at = NULL WHERE id = ?")
                    .bind(&now)
                    .bind(peer_id)
                    .execute(&mut *tx)
                    .await?;
                Self::log_moderation(
                    &mut tx,
                    peer_id,
                    actor,
                    "ban",
                    Some(&before),
                    Some(status.as_str()),
                )
                .await?;
                // The revocation reason is recorded PER (serial, peer):
                // hosts share one certificate, so a ban must revoke the
                // serial the peer presents now, from the peer's own row
                // or the host-level record (a host that connected before
                // any peer was enrolled still has a serial to revoke).
                let serial_row: Option<(Option<String>,)> =
                    sqlx::query_as("SELECT cert_serial FROM peers WHERE id = ?")
                        .bind(peer_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                let serial = match serial_row.and_then(|(serial,)| serial) {
                    Some(serial) => Some(serial),
                    None => sqlx::query_scalar::<_, Option<String>>(
                        "SELECT h.cert_serial FROM peers p JOIN hosts h ON h.host_id = p.host_id
                             WHERE p.id = ?",
                    )
                    .bind(peer_id)
                    .fetch_optional(&mut *tx)
                    .await?
                    .flatten(),
                };
                if let Some(serial) = serial {
                    sqlx::query(
                        "INSERT OR IGNORE INTO cert_revocations (serial, peer_id, revoked_at)
                         VALUES (?, ?, ?)",
                    )
                    .bind(&serial)
                    .bind(peer_id)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
        sqlx::query("UPDATE roster SET status = ?, alias = (SELECT alias FROM peers WHERE id = ?) WHERE peer_id = ?")
            .bind(status.as_str())
            .bind(peer_id)
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Soft-forgets an ejected peer: stamps `forgotten_at` and deletes
    /// its roster row, so the cohort entry disappears. The `peers` row
    /// survives — message, fence, and attempt foreign keys reference it
    /// — and any certificate revocation is preserved (a banned peer
    /// stays banned at the TLS layer). Forgotten peers are refused
    /// re-enrolment by the presence path.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to forget.
    /// * `actor` — who performed the action (audit trail).
    ///
    /// # Returns
    ///
    /// Whether the peer existed, was ejected, and was forgotten.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn forget_peer(&self, peer_id: &str, actor: &str) -> Result<bool> {
        // IMMEDIATE: read-modify-write (see set_peer_alias).
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((status,)) = row else {
            return Ok(false);
        };
        // A cohort entry may be removed whatever its status: the operator's
        // problem is not ejection, it is not being able to tell the entries
        // apart. Removal clears the alias too, so the peer returns with its
        // plain binding name on its next turn.
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("UPDATE peers SET forgotten_at = ?, alias = NULL WHERE id = ?")
            .bind(&now)
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM presence WHERE peer_id = ?")
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM roster WHERE peer_id = ?")
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        Self::log_moderation(&mut tx, peer_id, actor, "forget", Some(&status), None).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Reads the moderation audit log, newest first.
    ///
    /// # Arguments
    ///
    /// * `limit` — the maximum number of rows to return.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn moderation_log(&self, limit: i64) -> Result<Vec<ModerationEvent>> {
        let rows = sqlx::query_as::<_, ModerationEvent>(
            "SELECT id, peer_id, actor, action, before, after, at
             FROM moderation_events
             ORDER BY id DESC
             LIMIT ?",
        )
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows)
    }

    /// Records the certificate serial a host presented on a stamped
    /// connection, onto every peer enrolled on that host. The handshake
    /// knows only the host identity (one client certificate serves every
    /// peer a node hosts), so the recording keys on `host_id`; the ban
    /// path then finds the serial through the banned peer's own row.
    /// The latest presented serial wins: PKI with short-TTL certs renews
    /// the leaf, and a ban must revoke the serial the peer presents now,
    /// not the first one the hub ever saw.
    ///
    /// # Arguments
    ///
    /// * `host_id` — the presenting host (the certificate's first SAN).
    /// * `serial` — the lowercase hex certificate serial.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn record_host_cert_serial(&self, host_id: &str, serial: &str) -> Result<()> {
        // The host-level record stands independent of peer enrolment: a
        // connection that arrived before any peer was enrolled still
        // leaves the serial a later ban can revoke. The host row is
        // upserted (a first-ever connection creates it), and the latest
        // presented serial wins across short-TTL renewals.
        let now = chrono::Utc::now().to_rfc3339();
        // IMMEDIATE: the upsert is the first write of enrolment and must
        // not race another connection's write upgrade.
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "INSERT INTO hosts (host_id, name, cert_serial, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(host_id) DO UPDATE SET cert_serial = excluded.cert_serial",
        )
        .bind(host_id)
        .bind(host_id)
        .bind(serial)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        // And onto every peer already enrolled on that host (the ban
        // path finds the serial through the banned peer's own row).
        sqlx::query(
            "UPDATE peers SET cert_serial = ?
             WHERE host_id = ? AND cert_serial IS NOT ?",
        )
        .bind(serial)
        .bind(host_id)
        .bind(serial)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Lists the store-persisted certificate revocations (the runtime
    /// ban source, in addition to the `--revoke-list` file).
    ///
    /// The effective deny-set is the union of the remaining per-peer
    /// revocation reasons: restoring one peer on a shared host
    /// certificate never lifts another peer's ban.
    ///
    /// # Returns
    ///
    /// The revoked serials (lowercase hex).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn cert_revocations(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT serial FROM cert_revocations ORDER BY serial")
                .fetch_all(self.pool())
                .await?;
        Ok(rows.into_iter().map(|(serial,)| serial).collect())
    }
}
