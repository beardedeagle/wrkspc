//! Peer enrolment and lookups (host binding is append-only).

use super::Store;
use crate::error::{Result, StoreError};
use crate::model::PILOT_HOST_ID;
use chrono::Utc;
use sqlx::Row;

impl Store {
    /// Enrols a peer in a conversation, creating both rows if needed.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn ensure_peer(&self, peer_id: &str, name: &str, class: &str) -> Result<()> {
        self.ensure_peer_on_host(peer_id, name, class, PILOT_HOST_ID)
            .await
    }

    /// Enrols a peer on a specific host (team-mode enrolment; the
    /// pilot-only [`ensure_peer`](Store::ensure_peer) delegates here
    /// with [`PILOT_HOST_ID`]). Idempotent: an existing row is left
    /// untouched, so a re-enrolment never rewrites `host_id`.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to enrol.
    /// * `name` — the peer's display name.
    /// * `class` — the peer's [`PeerClass`] string.
    /// * `host_id` — the host that owns the peer.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn ensure_peer_on_host(
        &self,
        peer_id: &str,
        name: &str,
        class: &str,
        host_id: &str,
    ) -> Result<()> {
        // Host registration is part of enrolment: a peer can only live
        // on a known host (`peers.host_id` references `hosts`), and a
        // bare `INSERT OR IGNORE` would silently swallow the FK
        // violation instead of recording the peer.
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO hosts (host_id, name, created_at) VALUES (?, ?, ?)")
            .bind(host_id)
            .bind(host_id)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO peers (id, host_id, name, class, enrolled_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(peer_id)
        .bind(host_id)
        .bind(name)
        .bind(class)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 1 {
            // First enrolment records the first host assignment
            // (append-only history; the projection was just written).
            sqlx::query(
                "INSERT INTO peer_host_assignments (peer_id, host_id, assigned_at)
                 VALUES (?, ?, ?)",
            )
            .bind(peer_id)
            .bind(host_id)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Re-assigns a peer to a host: appends a host-assignment record and
    /// updates the `peers.host_id` projection in the same transaction.
    /// The ONLY writer of host changes (D-6 A2 contract: host assignment
    /// is an explicit event, never a silent upsert —
    /// [`ensure_peer_on_host`](Store::ensure_peer_on_host) stays
    /// immutable for existing peers).
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to re-assign.
    /// * `host_id` — the new host.
    ///
    /// # Returns
    ///
    /// `true` when an assignment was recorded, `false` when the peer
    /// already lives on `host_id` (idempotent no-op).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the
    /// peer is not enrolled, and
    /// [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn assign_peer_host(&self, peer_id: &str, host_id: &str) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let current: Option<String> = sqlx::query("SELECT host_id FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.get("host_id"));
        let Some(current) = current else {
            return Err(StoreError::NotFound {
                entity: "peer",
                id: peer_id.to_string(),
            });
        };
        if current == host_id {
            return Ok(false);
        }
        sqlx::query("INSERT OR IGNORE INTO hosts (host_id, name, created_at) VALUES (?, ?, ?)")
            .bind(host_id)
            .bind(host_id)
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
        sqlx::query("UPDATE peers SET host_id = ? WHERE id = ?")
            .bind(host_id)
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Looks up a peer's `host_id` (the host that owns the peer).
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to look up.
    ///
    /// # Returns
    ///
    /// The peer's host id, or `None` when the peer is not enrolled.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn peer_host(&self, peer_id: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT host_id FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("host_id")))
    }

    /// Looks up a peer's enrolment record.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to look up.
    ///
    /// # Returns
    ///
    /// `Some((name, class, host_id))`, or `None` when the peer is not
    /// enrolled.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn peer_details(&self, peer_id: &str) -> Result<Option<(String, String, String)>> {
        let row = sqlx::query("SELECT name, class, host_id FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| (r.get("name"), r.get("class"), r.get("host_id"))))
    }

    /// Reads a peer's operator-moderated hub status.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to query.
    ///
    /// # Returns
    ///
    /// The status string (`enrolled`, `booted`, `banned`), or `None`
    /// when the peer is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn peer_status(&self, peer_id: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT status FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("status")))
    }

    /// Reads the certificate serial a peer presented on its stamped
    /// connection, when one has been recorded.
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to query.
    ///
    /// # Returns
    ///
    /// The lowercase hex serial, or `None` when the peer is unknown or
    /// has not presented a certificate yet.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn peer_cert_serial(&self, peer_id: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT cert_serial FROM peers WHERE id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("cert_serial")))
    }

    /// Lists a peer's host-assignment history, oldest first
    /// (append-only; the last entry is the current projection).
    ///
    /// # Arguments
    ///
    /// * `peer_id` — the peer to query.
    ///
    /// # Returns
    ///
    /// `(host_id, assigned_at)` pairs in assignment order.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn peer_assignments(&self, peer_id: &str) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT host_id, assigned_at FROM peer_host_assignments
             WHERE peer_id = ? ORDER BY assigned_at",
        )
        .bind(peer_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("host_id"), r.get("assigned_at")))
            .collect())
    }
}
