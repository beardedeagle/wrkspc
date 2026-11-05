//! Store lifecycle: open, migrate, dispatcher lock.
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::Store;
use crate::error::{Result, StoreError};
use crate::model::PILOT_HOST_ID;
use chrono::Utc;
use sqlx::Row;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

impl Store {
    /// Opens (creating if needed) the store at `path`, runs migrations, and
    /// acquires the exclusive dispatcher lock for `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AlreadyLocked`](StoreError::AlreadyLocked) when
    /// another owner holds the lock, and [`StoreError::Db`](StoreError::Db)
    /// for any sqlite failure.
    pub async fn open(path: &Path, owner: &str) -> Result<Self> {
        Self::open_with(path, owner, true).await
    }

    /// Opens the database WITHOUT acquiring the dispatcher lock and
    /// WITHOUT running migrations.
    ///
    /// For components that share the node's database without being the
    /// dispatcher (the MCP bridge serving a harness's tools): the
    /// dispatcher lock is a role lock, not a database lock — SQLite WAL
    /// serializes the actual writes. Schema ownership (migrations) also
    /// belongs to the exclusive opener: running migrations here would
    /// race the node's writer at every bridge spawn (observed: a bridge's
    /// spawn-time migrate contended with the dispatcher and silently
    /// wedged a turn). A shared opener against an unmigrated database
    /// fails loudly on first use instead.
    ///
    /// # Arguments
    ///
    /// * `path` — the database file.
    /// * `owner` — the component's name (bookkeeping only).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on open failure.
    pub async fn open_shared(path: &Path, owner: &str) -> Result<Self> {
        Self::open_with(path, owner, false).await
    }

    /// Opens (and optionally locks + migrates) the database.
    ///
    /// Shared openers skip BOTH migrations and the `journal_mode` pragma:
    /// each is a write-side exclusive concern. WAL mode is a permanent
    /// database property set at creation by the exclusive opener — and
    /// `PRAGMA journal_mode=WAL` requires an exclusive lock that does not
    /// honor `busy_timeout` (sqlx comment; observed as `database is
    /// locked` claim failures racing a bridge spawn).
    async fn open_with(path: &Path, owner: &str, exclusive: bool) -> Result<Self> {
        let mut opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .foreign_keys(true);
        if exclusive {
            opts = opts.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(opts)
            .await?;
        let store = Self {
            pool,
            lock_owner: owner.to_string(),
        };
        if exclusive {
            store.migrate().await?;
            store.acquire_lock().await?;
        }
        Ok(store)
    }

    /// Opens an isolated in-memory store for tests.
    ///
    /// Uses a single pooled connection so migrations and queries all hit the
    /// same in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn open_in_memory(owner: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .in_memory(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        let store = Self {
            pool,
            lock_owner: owner.to_string(),
        };
        store.migrate().await?;
        store.acquire_lock().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        // Seed the constant pilot host (D-6 host_id day-1 requirement).
        let now = Utc::now().to_rfc3339();
        sqlx::query("INSERT OR IGNORE INTO hosts (host_id, name, created_at) VALUES (?, ?, ?)")
            .bind(PILOT_HOST_ID)
            .bind("pilot host (single-machine)")
            .bind(&now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn acquire_lock(&self) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO store_lock (id, owner, acquired_at) VALUES (1, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(&self.lock_owner)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let row = sqlx::query("SELECT owner, acquired_at FROM store_lock WHERE id = 1")
            .fetch_one(&self.pool)
            .await?;
        let owner: String = row.get("owner");
        let acquired_at: String = row.get("acquired_at");
        if owner != self.lock_owner {
            return Err(StoreError::AlreadyLocked { owner, acquired_at });
        }
        Ok(())
    }

    /// Releases the exclusive dispatcher lock if this store holds it.
    ///
    /// Idempotent: releasing a lock this store does not hold is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn release_lock(&self) -> Result<()> {
        sqlx::query("DELETE FROM store_lock WHERE id = 1 AND owner = ?")
            .bind(&self.lock_owner)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
