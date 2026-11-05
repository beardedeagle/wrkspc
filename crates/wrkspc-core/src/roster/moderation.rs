//! Peer lifecycle moderation: alias, status (boot/ban/restore), forget,
//! the audit log, and certificate revocation persistence.

use super::{ModerationEvent, PeerStatus, status_is_ejected};
use crate::Result;
use crate::error::StoreError;
use crate::store::Store;

impl Store {
    /// Resolves an operator-facing peer reference — an alias first,
    /// falling back to the peer id — to the canonical peer id.
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
        let by_alias: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM peers WHERE alias = ? ORDER BY id LIMIT 2")
                .bind(target)
                .fetch_all(self.pool())
                .await?;
        match by_alias.len() {
            0 => {}
            1 => return Ok(Some(by_alias[0].0.clone())),
            _ => {
                return Err(StoreError::AmbiguousPeerRef {
                    reference: target.to_string(),
                });
            }
        }
        let by_id: Option<(String,)> = sqlx::query_as("SELECT id FROM peers WHERE id = ?")
            .bind(target)
            .fetch_optional(self.pool())
            .await?;
        Ok(by_id.map(|(id,)| id))
    }

    pub async fn set_peer_alias(
        &self,
        peer_id: &str,
        alias: Option<&str>,
        actor: &str,
    ) -> Result<()> {
        let mut tx = self.pool().begin().await?;
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
        let mut tx = self.pool().begin().await?;
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
                let serial_row: Option<(Option<String>,)> =
                    sqlx::query_as("SELECT cert_serial FROM peers WHERE id = ?")
                        .bind(peer_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                if let Some((Some(serial),)) = serial_row {
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
        let mut tx = self.pool().begin().await?;
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((status,)) = row else {
            return Ok(false);
        };
        // Only ejected peers may be forgotten; an enrolled peer must be
        // booted or banned first.
        if !status_is_ejected(&status) {
            return Ok(false);
        }
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("UPDATE peers SET forgotten_at = ? WHERE id = ?")
            .bind(&now)
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

    /// Records the certificate serial a peer presented on a stamped
    /// connection. Idempotent; the first serial wins so a ban always
    /// revokes the identity the hub has seen.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer the certificate belongs to.
    /// * `serial` — the lowercase hex certificate serial.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn record_peer_cert_serial(&self, peer_id: &str, serial: &str) -> Result<()> {
        sqlx::query("UPDATE peers SET cert_serial = ? WHERE id = ? AND cert_serial IS NULL")
            .bind(serial)
            .bind(peer_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Lists the store-persisted certificate revocations (the runtime
    /// ban source, in addition to the `--revoke-list` file).
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
            sqlx::query_as("SELECT serial FROM cert_revocations ORDER BY serial")
                .fetch_all(self.pool())
                .await?;
        Ok(rows.into_iter().map(|(serial,)| serial).collect())
    }
}
