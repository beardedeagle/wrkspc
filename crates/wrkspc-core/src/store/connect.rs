//! Store lifecycle: open, migrate, dispatcher lock.
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::Store;
use crate::error::{Result, StoreError};
use crate::model::PILOT_HOST_ID;
use chrono::Utc;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

impl Store {
    /// Opens (creating if needed) the store at `path`, runs migrations, and
    /// acquires the exclusive dispatcher lock for `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AlreadyLocked`] when
    /// another owner holds the lock, and [`StoreError::Db`]
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
    /// * `path` — the database file.
    /// * `owner` — the component's name (bookkeeping only).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`] on open failure.
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
            .foreign_keys(true)
            // Concurrent writers (e.g. two guidance claims in one
            // conversation) wait for the write lock instead of failing
            // instantly, then re-evaluate against the committed state.
            .busy_timeout(Duration::from_secs(5));
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
            lock_file: std::sync::Arc::new(std::sync::Mutex::new(None)),
        };
        if exclusive {
            store.migrate().await?;
            store.acquire_lock(path).await?;
        }
        Ok(store)
    }

    /// Opens an isolated in-memory store for tests.
    ///
    /// Uses a single pooled connection so migrations and queries all hit the
    /// same in-memory database. In-memory stores take no OS lock: each is
    /// its own private database, and no two processes can share it.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`] on failure.
    pub async fn open_in_memory(owner: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .in_memory(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        let store = Self {
            pool,
            lock_owner: owner.to_string(),
            lock_file: std::sync::Arc::new(std::sync::Mutex::new(None)),
        };
        store.migrate().await?;
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

    /// Acquires the OS-level exclusive lock on the `<db>.lock` sidecar.
    ///
    /// The lock is exclusive per DATABASE — not per role label: two
    /// processes passing the same owner string (the production case,
    /// `wrkspc-node`/`wrkspc-hub`) cannot both hold it, and a crashed
    /// process releases it automatically when the kernel reaps its
    /// file descriptors (safe crash recovery).
    async fn acquire_lock(&self, path: &Path) -> Result<()> {
        use fs2::FileExt;

        let lock_path = format!("{}.lock", path.display());
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| StoreError::Db(sqlx::Error::Io(e)))?;
        if let Err(_error) = file.try_lock_exclusive() {
            // Someone else holds it: report who, best-effort (the
            // sidecar carries the holder's owner string).
            let holder = std::fs::read_to_string(&lock_path).unwrap_or_default();
            let holder = holder.trim().to_string();
            return Err(StoreError::AlreadyLocked {
                owner: if holder.is_empty() {
                    "another process".to_string()
                } else {
                    holder
                },
                acquired_at: "see the holder's log".to_string(),
            });
        }
        // The sidecar records the holder for diagnostics; the OS lock is
        // the enforcement, never the file contents.
        let owner = self.lock_owner.clone();
        let write = file.set_len(0).and_then(|()| {
            use std::io::Write;
            let mut file = &file;
            file.write_all(owner.as_bytes())
        });
        if write.is_err() {
            tracing::warn!("lock sidecar metadata write failed (lock still held)");
        }
        // A poisoned lock here means a writer panicked while holding it;
        // the stored file handle itself is intact, so std's documented
        // recovery idiom takes the guard and replaces the value.
        let mut guard = self
            .lock_file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(file);
        Ok(())
    }

    /// Releases the exclusive dispatcher lock if this store holds it.
    ///
    /// Idempotent: releasing a lock this store does not hold is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`] on failure.
    pub async fn release_lock(&self) -> Result<()> {
        // `unlock` is the standard-library file-lock API.
        let mut guard = self
            .lock_file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(file) = guard.take()
            && let Err(error) = file.unlock()
        {
            return Err(StoreError::Db(sqlx::Error::Io(error)));
        }
        Ok(())
    }
}
