//! On-demand ACP session registry (team-mode supervision, 1B).
//!
//! Sessions spawn lazily on the first dispatcher claim instead of
//! eagerly at node startup, so a dead local harness degrades execution
//! delivery (visible `unknown` attempts with a feed event) instead of
//! killing the node. Per-peer state machine: cached `PeerSession` while
//! healthy; on spawn failure the peer enters exponential backoff and
//! resolves to `None` — the dispatcher's existing conservative path
//! records the attempt unknown, never silent.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::watch;
use wrkspc_acp::{SessionHandle, StdioTransport};
use wrkspc_core::Store;
use wrkspc_dispatcher::{SessionBinding, SessionProvider};

use crate::bindings::bridge_injection;

/// Spawn material for one binding: everything the registry needs to
/// bring the peer's session up on demand.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// The peer id (matches the binding's `peer_id`).
    pub peer_id: String,
    /// The peer's display name (the harness's own name).
    pub name: String,
    /// The harness adapter command.
    pub command: String,
    /// The session's working directory.
    pub cwd: PathBuf,
    /// Optional model pin (D-4b operating policy).
    pub model: Option<String>,
    /// Whether the peer's tools are registered via the harness's
    /// native HTTP MCP config (agy) instead of ACP `mcpServers`
    /// injection.
    pub mcp_http: bool,
}

/// A live session plus the background tasks that keep it alive.
#[derive(Debug)]
struct PeerSession {
    binding: SessionBinding,
    /// The session handle (its `closed` watch drives dead-session
    /// eviction — E04/H-15).
    handle: SessionHandle,
    /// The loopback HTTP listener task for `mcp_http` peers.
    http_task: Option<ListenerTask>,
}

/// Abort-on-drop ownership of the `mcp_http` listener task.
///
/// Session setup is cancellable at every await: the dispatcher
/// deadline-bounds the whole lookup and drops the future on timeout,
/// so listener cleanup cannot live in error branches. This guard
/// aborts the task whenever its owner is dropped, and ownership moves
/// into the cached [`PeerSession`] only once every awaited setup step
/// has succeeded (E10).
#[derive(Debug)]
struct ListenerTask(Option<tokio::task::JoinHandle<()>>);

impl ListenerTask {
    /// Aborts the listener task, if it is still owned here.
    fn abort(&self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl Drop for ListenerTask {
    fn drop(&mut self) {
        self.abort();
    }
}

impl PeerSession {
    /// Whether the session's transport has ended.
    fn is_closed(&self) -> bool {
        self.handle.closed().borrow().is_some()
    }
}

/// Shared registry state.
#[derive(Debug)]
struct Inner {
    configs: HashMap<String, SessionConfig>,
    sessions: tokio::sync::Mutex<HashMap<String, Arc<PeerSession>>>,
    spawn_locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-peer state: next allowed spawn attempt and consecutive
    /// failure count (exponential backoff, capped).
    backoff: tokio::sync::Mutex<HashMap<String, (Instant, u32)>>,
    store: Store,
    db: PathBuf,
    shutdown: watch::Receiver<bool>,
    /// Whether the node runs against a hub: the injected bridge and the
    /// in-process HTTP MCP surface mirror it, so harness posts never
    /// create local execution intents in team mode.
    hub_connected: bool,
}

/// Spawns ACP sessions on demand with per-peer backoff.
#[derive(Debug, Clone)]
pub struct SessionRegistry {
    inner: Arc<Inner>,
}

impl crate::poll::PresenceSource for SessionRegistry {
    /// Presence declarations for every binding peer: `connected` when
    /// its session is live, `disconnected` otherwise. Peers outside the
    /// binding list are never invented here (the hub accepts them from
    /// other callers; the node reports what it hosts).
    fn decls(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Vec<crate::poll::PresenceDecl>> + Send + '_>,
    > {
        let inner = &self.inner;
        Box::pin(async move {
            let sessions = inner.sessions.lock().await;
            inner
                .configs
                .iter()
                .map(|(peer_id, config)| crate::poll::PresenceDecl {
                    peer_id: peer_id.clone(),
                    name: config.name.clone(),
                    class: "harness".to_string(),
                    state: if sessions
                        .get(peer_id)
                        .is_some_and(|session| !session.is_closed())
                    {
                        "connected".to_string()
                    } else {
                        "disconnected".to_string()
                    },
                })
                .collect()
        })
    }
}

impl SessionRegistry {
    /// Builds the registry from the node's binding configs.
    ///
    /// `configs` has one entry per binding peer; `store` is the node's
    /// store (the MCP server surface needs it); `db` is the database
    /// path (token files + bridge injection); `shutdown` is the node's
    /// shutdown signal, cloned into any HTTP listener task spawned on
    /// demand; `hub_connected` is whether the node runs against a hub —
    /// the MCP surfaces mirror it so harness posts never create local
    /// execution intents in team mode.
    pub fn new(
        configs: Vec<SessionConfig>,
        store: Store,
        db: PathBuf,
        shutdown: watch::Receiver<bool>,
        hub_connected: bool,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                configs: configs
                    .into_iter()
                    .map(|config| (config.peer_id.clone(), config))
                    .collect(),
                sessions: tokio::sync::Mutex::new(HashMap::new()),
                spawn_locks: std::sync::Mutex::new(HashMap::new()),
                backoff: tokio::sync::Mutex::new(HashMap::new()),
                store,
                db,
                shutdown,
                hub_connected,
            }),
        }
    }
}

impl Inner {
    /// Resolves a peer to its binding, spawning the session on demand.
    /// `None` = not enrolled, or unhealthy within backoff.
    async fn resolve(self: &Arc<Self>, peer_id: &str) -> Option<SessionBinding> {
        if !self.configs.contains_key(peer_id) {
            return None;
        }
        let cached = self.sessions.lock().await.get(peer_id).cloned();
        if let Some(session) = cached {
            if session.is_closed() {
                // A dead cached session must be evicted, never
                // re-promoted: every prompt to it would go unknown and
                // presence would keep reporting connected (E04/H-15).
                tracing::warn!(
                    %peer_id,
                    reason = %session.handle.closed().borrow().as_deref().unwrap_or("unknown"),
                    "cached session is dead; evicting",
                );
                if let Some(task) = &session.http_task {
                    task.abort();
                }
                self.sessions.lock().await.remove(peer_id);
            } else {
                return Some(session.binding.clone());
            }
        }
        // Fail fast while a previous spawn failure is still backing off.
        if let Some((deadline, _)) = self.backoff.lock().await.get(peer_id)
            && Instant::now() < *deadline
        {
            return None;
        }
        // One spawn at a time per peer; a concurrent claim that loses
        // the race re-checks the cache inside the lock.
        // The map is insert-only (entries are never mutated after
        // creation — mutation happens under each entry's own lock), so
        // recovering a poisoned guard is safe.
        let spawn_lock = self
            .spawn_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(peer_id.to_string())
            .or_default()
            .clone();
        let _guard = spawn_lock.lock().await;
        if let Some(session) = self.sessions.lock().await.get(peer_id).cloned() {
            return Some(session.binding.clone());
        }
        // The admission decision must be made AFTER the lock: the
        // previous holder may have failed while this lookup was queued
        // and set a backoff deadline that must now govern (E10).
        if let Some((deadline, _)) = self.backoff.lock().await.get(peer_id)
            && Instant::now() < *deadline
        {
            return None;
        }
        // The config was checked above; a missing entry is the
        // conservative outcome (no binding → the dispatcher records
        // unknown), never a panic.
        let config = self.configs.get(peer_id)?.clone();
        match spawn_session(
            &config,
            &self.store,
            &self.db,
            self.shutdown.clone(),
            self.hub_connected,
        )
        .await
        {
            Ok(session) => {
                let binding = session.binding.clone();
                self.sessions
                    .lock()
                    .await
                    .insert(peer_id.to_string(), Arc::new(session));
                self.backoff.lock().await.remove(peer_id);
                Some(binding)
            }
            Err(error) => {
                let mut backoff = self.backoff.lock().await;
                let entry = backoff
                    .entry(peer_id.to_string())
                    .or_insert((Instant::now(), 0));
                entry.1 += 1;
                let delay = Duration::from_secs(1u64 << entry.1.min(6)); // 2s..64s
                entry.0 = Instant::now() + delay;
                tracing::warn!(%peer_id, %error, ?delay, "session spawn failed; backing off");
                None
            }
        }
    }
}

impl SessionProvider for SessionRegistry {
    fn lookup(
        &self,
        peer_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<SessionBinding>> + Send + '_>>
    {
        let inner = Arc::clone(&self.inner);
        let peer_id = peer_id.to_string();
        Box::pin(async move { inner.resolve(&peer_id).await })
    }
}

/// Spawns one peer's session: ACP spawn + MCP wiring (bridge injection
/// or the harness-native HTTP registration) + model pin.
///
/// `config` is the binding's spawn material; `store` is the node store;
/// `db` is the database path; `shutdown` is the node shutdown signal
/// (cloned into the HTTP task).
///
/// # Returns
///
/// The live session.
///
/// # Errors
///
/// Returns the failure reason as a string (spawn failures are degraded
/// state, not fatal errors — the registry backs off and retries).
async fn spawn_session(
    config: &SessionConfig,
    store: &Store,
    db: &Path,
    shutdown: watch::Receiver<bool>,
    hub_connected: bool,
) -> Result<PeerSession, String> {
    tracing::debug!(peer_id = %config.peer_id, "spawning session transport");
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(config.command.clone()));
    let mcp: Vec<agent_client_protocol::v1::schema::McpServer> = if config.mcp_http {
        // The ACP injection is a dead path for adapters without
        // mcpServers handling; the harness-native HTTP config is the
        // route. A fresh loopback listener + bearer token per peer,
        // registered via `agy mcp add` (idempotent add-or-update).
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;
        let token = crate::secrets::mcp_token_for_peer(db, &config.peer_id).await?;
        let server = wrkspc_mcp::McpServer::new(store.clone(), &config.peer_id, hub_connected);
        let http = wrkspc_mcp::HttpListener::new(
            server,
            wrkspc_mcp::HttpListenerConfig {
                bind: addr,
                tokens: std::collections::HashSet::from([token.clone()]),
            },
        );
        let url = format!("http://{addr}/mcp");
        let bearer_header = format!("Authorization: Bearer {}", token);
        let status = tokio::process::Command::new("agy")
            .args([
                "mcp",
                "add",
                "--type",
                "http",
                "--header",
                &bearer_header,
                &format!("wrkspc-{}", config.peer_id),
                &url,
            ])
            .status()
            .await
            .map_err(|e| format!("agy mcp add failed: {e}"))?;
        if !status.success() {
            // A registration that exits nonzero must not be reported as
            // success: the session would look healthy with no usable
            // wrkspc tools (E10).
            return Err(format!(
                "agy mcp add exited {} for {}",
                status, config.peer_id
            ));
        }
        tracing::info!(
            peer_id = %config.peer_id,
            %url,
            "MCP tools registered with the harness (agy mcp config)",
        );
        let mut rx = shutdown.clone();
        let task = tokio::spawn(async move {
            let _ = http
                .serve_on(listener, async move {
                    while !*rx.borrow() {
                        if rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await;
        });
        // From here on every await is a cancellation point — the
        // dispatcher deadline-bounds the whole lookup and drops the
        // future on timeout — so the listener's lifetime rides this
        // guard instead of the error branches below: dropping the
        // future aborts the task and closes the bound port (E10).
        let guard = ListenerTask(Some(task));
        let session_id = match handle
            .new_session_with_mcp(config.cwd.clone(), Vec::new())
            .await
        {
            Ok(session_id) => session_id,
            Err(error) => {
                // The guard aborts the listener on its way out; the
                // failed setup must not leave the bound port behind.
                return Err(format!(
                    "session/new for {} failed: {error}",
                    config.peer_id
                ));
            }
        };
        if let Some(model) = &config.model
            && let Err(error) = pin_model(&handle, &config.peer_id, &session_id, model).await
        {
            // Same contract: the guard closes the listener on the way out.
            return Err(error);
        }
        tracing::info!(peer_id = %config.peer_id, %session_id, "session bound (mcp_http)");
        return Ok(PeerSession {
            binding: SessionBinding {
                peer_id: config.peer_id.clone(),
                session_id,
                driver: Arc::new(handle.clone()),
            },
            handle,
            http_task: Some(guard),
        });
    } else {
        vec![bridge_injection(db, &config.peer_id, hub_connected)]
    };
    let session_id = handle
        .new_session_with_mcp(config.cwd.clone(), mcp)
        .await
        .map_err(|e| format!("session/new for {} failed: {e}", config.peer_id))?;
    if let Some(model) = &config.model {
        pin_model(&handle, &config.peer_id, &session_id, model).await?;
    }
    tracing::info!(peer_id = %config.peer_id, %session_id, "session bound");
    Ok(PeerSession {
        binding: SessionBinding {
            peer_id: config.peer_id.clone(),
            session_id,
            driver: Arc::new(handle.clone()),
        },
        handle,
        http_task: None,
    })
}

/// Pins the session's model (D-4b operating policy).
///
/// # Errors
///
/// Returns the failure reason as a string.
async fn pin_model(
    handle: &SessionHandle,
    peer_id: &str,
    session_id: &str,
    model: &str,
) -> Result<(), String> {
    handle
        .set_config_option(session_id, "model", model)
        .await
        .map_err(|e| format!("model pin for {peer_id} (session {session_id}) failed: {e}"))?;
    tracing::info!(peer_id = %peer_id, %model, "model pinned");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A binding whose command exits immediately without an ACP
    /// handshake (`true` dies before the handshake completes, so the
    /// spawn errors deterministically) exercises the failure/backoff
    /// path.
    fn dead_config() -> SessionConfig {
        SessionConfig {
            peer_id: "codex".to_string(),
            name: "codex".to_string(),
            command: "true".to_string(),
            cwd: PathBuf::from("."),
            model: None,
            mcp_http: false,
        }
    }

    #[tokio::test]
    async fn dead_session_resolves_none_and_backs_off() {
        let store = Store::open_in_memory("registry-test").await.unwrap();
        let (_tx, rx) = watch::channel(false);
        let registry = SessionRegistry::new(
            vec![dead_config()],
            store.clone(),
            PathBuf::from("/nonexistent.db"),
            rx,
            false,
        );

        // First resolve attempts the spawn and fails.
        assert!(registry.lookup("codex").await.is_none());
        // Within backoff, resolves fail fast without re-spawning.
        assert!(registry.lookup("codex").await.is_none());
        // Unenrolled peers resolve to None (dispatcher marks unknown).
        assert!(registry.lookup("agy").await.is_none());
    }

    #[tokio::test]
    async fn unknown_peer_resolves_none_without_spawn_state() {
        let store = Store::open_in_memory("registry-unknown-test")
            .await
            .unwrap();
        let (_tx, rx) = watch::channel(false);
        let registry = SessionRegistry::new(
            Vec::new(),
            store,
            PathBuf::from("/nonexistent.db"),
            rx,
            false,
        );
        assert!(registry.lookup("codex").await.is_none());
    }

    /// Counts the spawn attempts recorded by the marker-writing dead
    /// command below.
    fn spawn_marker_lines(path: &Path) -> usize {
        std::fs::read_to_string(path)
            .map(|contents| contents.lines().count())
            .unwrap_or(0)
    }

    /// A lookup queued behind a failing spawn must re-check backoff
    /// AFTER acquiring the per-peer spawn lock: the first holder fails
    /// and sets the backoff deadline while the second waits, so the
    /// pre-lock admission check was already stale (E10).
    #[tokio::test]
    async fn queued_lookup_rechecks_backoff_after_the_spawn_lock() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-registry-backoff-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join("spawns");
        // Each spawn writes a marker line, then dies after a pause
        // without an ACP handshake: a deterministic failing spawn.
        let command = format!("sh -c 'echo x >> {}; sleep 1; exit 1'", marker.display());
        let store = Store::open_in_memory("registry-backoff-test")
            .await
            .unwrap();
        let (_tx, rx) = watch::channel(false);
        let registry = SessionRegistry::new(
            vec![SessionConfig {
                peer_id: "codex".to_string(),
                name: "codex".to_string(),
                command,
                cwd: PathBuf::from("."),
                model: None,
                mcp_http: false,
            }],
            store.clone(),
            PathBuf::from("/nonexistent.db"),
            rx,
            false,
        );

        // First lookup: the spawn starts (marker line 1) and fails
        // after ~1s, establishing backoff.
        let first = tokio::spawn({
            let registry = registry.clone();
            async move { registry.lookup("codex").await }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while spawn_marker_lines(&marker) < 1 {
            assert!(Instant::now() < deadline, "the first spawn never started");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Second lookup: nothing has failed yet, so it passes the
        // pre-lock backoff check and queues on the spawn lock behind
        // the first.
        let second = tokio::spawn({
            let registry = registry.clone();
            async move { registry.lookup("codex").await }
        });
        // Give the queued lookup time to reach the lock before the
        // first spawn fails and sets the backoff deadline.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(first.await.unwrap().is_none());
        assert!(second.await.unwrap().is_none());
        assert_eq!(
            spawn_marker_lines(&marker),
            1,
            "a lookup queued behind a failing spawn must honor the backoff set while it waited"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Presence must derive connected state from cached session health,
    /// not map membership: a dead cached session advertises
    /// disconnected without waiting for an evicting lookup (E10).
    #[tokio::test]
    async fn dead_cached_session_reports_disconnected_presence() {
        let store = Store::open_in_memory("registry-presence-test")
            .await
            .unwrap();
        let (_tx, rx) = watch::channel(false);
        let registry = SessionRegistry::new(
            vec![
                SessionConfig {
                    peer_id: "codex".to_string(),
                    name: "codex".to_string(),
                    command: "true".to_string(),
                    cwd: PathBuf::from("."),
                    model: None,
                    mcp_http: false,
                },
                SessionConfig {
                    peer_id: "randy".to_string(),
                    name: "randy".to_string(),
                    command: "sleep 10".to_string(),
                    cwd: PathBuf::from("."),
                    model: None,
                    mcp_http: false,
                },
            ],
            store.clone(),
            PathBuf::from("/nonexistent.db"),
            rx,
            false,
        );

        // A cached session whose transport has already ended. The
        // closed watch publishes when the session loop ends; shutdown
        // ends the loop deterministically, and the witness clone then
        // reads the published terminal state.
        let (dead_handle, _events) = SessionHandle::spawn(StdioTransport::new("true".to_string()));
        let dead_witness = dead_handle.clone();
        dead_handle.shutdown().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while dead_witness.closed().borrow().is_none() {
            assert!(
                Instant::now() < deadline,
                "the dead transport never reported closed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // A cached session whose transport is still running.
        let (live_handle, _events) =
            SessionHandle::spawn(StdioTransport::new("sleep 10".to_string()));
        registry.inner.sessions.lock().await.insert(
            "codex".to_string(),
            Arc::new(PeerSession {
                binding: SessionBinding {
                    peer_id: "codex".to_string(),
                    session_id: "fake-dead".to_string(),
                    driver: Arc::new(dead_witness.clone()),
                },
                handle: dead_witness,
                http_task: None,
            }),
        );
        registry.inner.sessions.lock().await.insert(
            "randy".to_string(),
            Arc::new(PeerSession {
                binding: SessionBinding {
                    peer_id: "randy".to_string(),
                    session_id: "fake-live".to_string(),
                    driver: Arc::new(live_handle.clone()),
                },
                handle: live_handle,
                http_task: None,
            }),
        );

        // Reading presence performs no lookup/eviction, yet the dead
        // session must not advertise connected.
        let decls = crate::poll::PresenceSource::decls(&registry).await;
        let state = |peer: &str| {
            decls
                .iter()
                .find(|decl| decl.peer_id == peer)
                .map(|decl| decl.state.as_str())
                .unwrap_or("")
        };
        assert_eq!(
            state("codex"),
            "disconnected",
            "a dead cached session must not advertise connected presence"
        );
        assert_eq!(
            state("randy"),
            "connected",
            "a live cached session stays connected"
        );
    }
}
