//! The wrkspc node: one process running the store, the ACP session
//! supervisor, the harness-facing MCP bridge, the dispatcher, the A2A
//! surface, and the operator UI.
//!
//! Bindings are declared in a TOML file (one entry per harness peer); the
//! MCP bridge is injected into each `session/new` as a stdio server, so
//! harness agents see the wrkspc tools from their first turn.

use crate::bindings::BindingsFile;
use crate::util::{canonical_db_path, stable_token};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use wrkspc_core::Store;
use wrkspc_dispatcher::{Dispatcher, DispatcherConfig};

use crate::poll::{PollLoopConfig, build_hub_client, run_poll_loop};

/// `wrkspc node run` arguments.
#[derive(Debug, clap::Args)]
pub struct NodeRunArgs {
    /// Database file (created if absent).
    #[arg(long)]
    pub db: PathBuf,
    /// Bindings TOML (one [[binding]] per harness peer).
    #[arg(long)]
    pub bindings: PathBuf,
    /// UI bind address (default 127.0.0.1:8081).
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub ui: String,
    /// Optional A2A surface bind address.
    #[arg(long)]
    pub a2a: Option<String>,
    /// Hub URL to poll for deliveries (team mode; node dials the hub).
    /// Requires `--host-id` and the client TLS flags.
    #[arg(long)]
    pub hub: Option<String>,
    /// This node's host id (its certificate SAN); required with `--hub`.
    #[arg(long, requires = "hub")]
    pub host_id: Option<String>,
    /// CA PEM to verify the hub's certificate (required with `--hub`).
    #[arg(long, requires = "hub")]
    pub tls_ca: Option<PathBuf>,
    /// Client certificate PEM identifying this node (required with `--hub`).
    #[arg(long, requires = "hub")]
    pub tls_cert: Option<PathBuf>,
    /// Client private key PEM (required with `--hub`).
    #[arg(long, requires = "hub")]
    pub tls_key: Option<PathBuf>,
    /// Seconds between hub polls.
    #[arg(long, default_value_t = 5)]
    pub poll_interval_secs: u64,
    /// This node's operator-identity name: the Keychain account key for
    /// its UI token (`wrkspc.<name>`). Defaults to the bare host name,
    /// or `default`. Distinct nodes on one machine need distinct names
    /// or they share one operator token.
    #[arg(long)]
    pub node_name: Option<String>,
}

/// The node's keychain identity: `--node-name` when given, otherwise the
/// host id (shortened to the bare hostname), otherwise `default`.
///
/// # Arguments
///
/// * `node_name` — the explicit `--node-name` value.
/// * `host_id` — the node's `--host-id` value.
fn node_identity(node_name: Option<&str>, host_id: Option<&str>) -> String {
    if let Some(name) = node_name.filter(|name| !name.is_empty()) {
        return name.to_string();
    }
    match host_id {
        Some(host) => host
            .trim_end_matches(".mtls.devlocal.home.arpa")
            .to_string(),
        None => "default".to_string(),
    }
}

/// Runs the pilot node until ctrl-c.
///
/// # Arguments
///
/// * `args` — the parsed run arguments.
///
/// # Errors
///
/// Returns a boxed error on any wiring failure.
pub async fn run_node(args: NodeRunArgs) -> Result<(), Box<dyn std::error::Error>> {
    let bindings = BindingsFile::load(&args.bindings)?;
    if bindings.binding.is_empty() {
        return Err("bindings file declares no [[binding]] entries".into());
    }
    let db = canonical_db_path(&args.db)?;
    let store = Store::open(&db, "wrkspc-node").await?;

    // Enrolled peers + the pilot conversation (fresh databases only).
    store.ensure_peer("operator", "operator", "human").await?;
    let mut peer_ids = vec!["operator".to_string()];
    for binding in &bindings.binding {
        store
            .ensure_peer(&binding.peer_id, &binding.harness, "harness")
            .await?;
        peer_ids.push(binding.peer_id.clone());
    }
    if store.list_conversations().await?.is_empty() {
        let refs: Vec<&str> = peer_ids.iter().map(String::as_str).collect();
        let conversation_id = store.create_conversation(&refs).await?;
        tracing::info!(%conversation_id, "created pilot conversation");
    }

    // Components share one shutdown signal; ctrl-c flips it and the
    // process joins each component's task.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_future = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };

    // Team mode: the node's BINDING peers cut over from the pilot host
    // marker to the node's real host id (append-only host assignment;
    // idempotent — the pilot assignment stays as history). The operator
    // is NOT a local peer: its host stays the hub's, or the node would
    // treat every hub-originated operator message as locally owned and
    // re-send it back to the hub forever. Runs before any mirroring so
    // mirrored REMOTE hosts are never rewritten.
    if let Some(host_id) = args.host_id.as_deref() {
        for binding in &bindings.binding {
            let peer_id = &binding.peer_id;
            if store.peer_host(peer_id).await?.as_deref() == Some(wrkspc_core::PILOT_HOST_ID) {
                let assigned = store.assign_peer_host(peer_id, host_id).await?;
                tracing::info!(%peer_id, %host_id, %assigned, "team-mode host cutover");
            }
        }
    }

    // Sessions spawn on demand (team-mode supervision): a dead local
    // harness degrades execution into visible unknown attempts instead
    // of killing the node.
    let configs = bindings
        .binding
        .iter()
        .map(|binding| crate::sessions::SessionConfig {
            peer_id: binding.peer_id.clone(),
            name: binding.harness.clone(),
            command: binding.command.clone(),
            cwd: binding.cwd.clone(),
            model: binding.model.clone(),
            mcp_http: binding.mcp_http,
        })
        .collect();
    let provider = Arc::new(crate::sessions::SessionRegistry::new(
        configs,
        store.clone(),
        db.clone(),
        shutdown_rx.clone(),
        // Team mode: the injected MCP surfaces mirror this so harness
        // posts never create local execution intents — execution waits
        // for hub acceptance and the echoed delivery claim.
        args.hub.is_some(),
    ));
    let (dispatcher, dispatcher_handle) =
        Dispatcher::new(store.clone(), provider.clone(), DispatcherConfig::default());

    // Operator token: WRKSPC_UI_TOKEN env wins; otherwise the platform
    // secret store (macOS Keychain, keyed by the node's host identity;
    // 0600 file elsewhere). The browser session survives node restarts;
    // the token never sits in plaintext on disk on macOS. Retrieval:
    // `wrkspc ui-token` (copies to the clipboard and clears it after
    // 30s — only when it still holds the token).
    let identity = node_identity(args.node_name.as_deref(), args.host_id.as_deref());
    let token = crate::secrets::resolve_ui_token(&identity, &db).await?;
    tracing::info!(
        "operator token ready (platform secret store; set WRKSPC_UI_TOKEN to override; copy it with `wrkspc ui-token`)",
    );

    // The harness skill is how a harness-resident agent learns wrkspc is
    // available and how to use its tool surface. Missing it is not
    // fatal, but it is worth a visible reminder before sessions start.
    if let Some(home) = std::env::var_os("HOME") {
        let home = std::path::PathBuf::from(home);
        for entry in crate::skill::detect(&home) {
            match crate::skill::status(&home, entry) {
                Ok(crate::skill::SkillStatus::Installed) => {}
                Ok(crate::skill::SkillStatus::Stale) => {
                    tracing::warn!(
                        harness = entry.name,
                        "wrkspc skill is stale; run `wrkspc skill install`"
                    );
                }
                Ok(crate::skill::SkillStatus::Missing) => {
                    tracing::warn!(
                        harness = entry.name,
                        "wrkspc skill is not installed; run `wrkspc skill install` before starting conversations"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        harness = entry.name,
                        %error,
                        "wrkspc skill state could not be read"
                    );
                }
            }
        }
    }

    let ui = wrkspc_ui::UiServer::new(
        store.clone(),
        Arc::new(dispatcher_handle),
        wrkspc_ui::UiConfig {
            bind: args.ui.parse()?,
            token: Some(token),
            queue_remote_deliveries: false,
            hub_role: false,
            hub_connected: args.hub.is_some(),
        },
        None,
    );
    let ui_listener = TcpListener::bind(&args.ui).await?;
    let ui_task = tokio::spawn(ui.serve_on(ui_listener, shutdown_future(shutdown_rx.clone())));

    let a2a_task = match args.a2a {
        Some(addr) => {
            // Bind BEFORE announcing readiness: a requested A2A endpoint
            // that cannot bind must not leave the node looking ready
            // (E12).
            let listener = TcpListener::bind(&addr)
                .await
                .map_err(|error| format!("A2A bind on {addr} failed before startup: {error}"))?;
            let store = store.clone();
            let mut rx = shutdown_rx.clone();
            Some(tokio::spawn(async move {
                let surface = wrkspc_a2a::A2aSurface::new(store);
                let _ = surface
                    .serve(listener, async move {
                        while !*rx.borrow() {
                            if rx.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await;
            }))
        }
        None => None,
    };

    let dispatcher_task = tokio::spawn(dispatcher.run(shutdown_future(shutdown_rx.clone())));

    // Team mode: dial the hub over mTLS and pull this host's delivery
    // queue into the local store. Mirrors + records + acks; the local
    // dispatcher then drives execution exactly as in the pilot.
    let poll_task = match &args.hub {
        Some(hub_url) => {
            let host_id = args
                .host_id
                .as_deref()
                .ok_or("--host-id is required with --hub")?
                .to_string();
            let ca = args
                .tls_ca
                .as_deref()
                .ok_or("--tls-ca is required with --hub")?;
            let cert = args
                .tls_cert
                .as_deref()
                .ok_or("--tls-cert is required with --hub")?;
            let key = args
                .tls_key
                .as_deref()
                .ok_or("--tls-key is required with --hub")?;
            let client = build_hub_client(ca, cert, key)?;
            // The node's stable local identity: delivery claims are
            // exclusive per INSTANCE, so a copied cert+key on a second
            // machine loses the claim instead of double-executing.
            let instance_id = stable_token(&db, "node-id").await?;
            let hub_url = hub_url.trim_end_matches('/').to_string();
            let poll_store = store.clone();
            let shutdown = shutdown_rx.clone();
            let interval = std::time::Duration::from_secs(args.poll_interval_secs.max(1));
            let host = host_id.clone();
            let presence: Option<std::sync::Arc<dyn crate::poll::PresenceSource>> =
                Some(provider.clone());
            Some(tokio::spawn(async move {
                run_poll_loop(
                    PollLoopConfig {
                        client,
                        hub_url,
                        store: poll_store,
                        host_id: host,
                        instance_id,
                        interval,
                        presence,
                    },
                    shutdown,
                )
                .await;
            }))
        }
        None => None,
    };

    // Readiness marker: integration tests (and operators) wait for this
    // file instead of parsing logs or polling the UI port. It carries no
    // secret — the token lives in the platform store, never in a file on
    // macOS.
    tokio::fs::write(format!("{}.ready", db.display()), b"ready")
        .await
        .map_err(|e| format!("cannot write readiness marker: {e}"))?;

    tracing::info!("wrkspc node running — ctrl-c to stop");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    let _ = shutdown_tx.send(true);

    // Component supervision at teardown: the dispatcher's result is
    // observed (a restart reconciliation failure is surfaced, never
    // silently discarded — E12), and the readiness marker is removed so
    // a stale marker can never outlive this process.
    match dispatcher_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "dispatcher exited with an error"),
        Err(error) => tracing::warn!(%error, "dispatcher task aborted"),
    }
    let _ = ui_task.await;
    if let Some(task) = poll_task {
        let _ = task.await;
    }
    if let Some(task) = a2a_task {
        let _ = task.await;
    }
    if let Err(error) = tokio::fs::remove_file(format!("{}.ready", db.display())).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(%error, "readiness marker removal failed");
    }
    Ok(())
}
