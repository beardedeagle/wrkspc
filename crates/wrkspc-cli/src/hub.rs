//! The hub role: coordinator without harness control (D-13).
//!
//! Runs the A2A surface over a hub-owned store — plain TCP for pilot
//! parity or mutual TLS (rustls `WebPkiClientVerifier`, optional serial
//! deny-list) for team mode — plus an optional operator UI. No ACP, no
//! dispatcher, no harness spawning: remote coordination, never remote
//! execution.

use std::collections::HashSet;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use clap::Args;
use tokio::net::TcpListener;
use wrkspc_a2a::{A2aSurface, cert_serial_hex};
use wrkspc_core::{CancelOutcome, Store};
use wrkspc_ui::{UiConfig, UiServer};

use crate::util::canonical_db_path;

/// Hub run arguments.
#[derive(Debug, Args)]
pub struct HubRunArgs {
    /// Database file (the hub owns it exclusively).
    #[arg(long)]
    pub db: PathBuf,
    /// A2A surface bind address.
    #[arg(long, default_value = "0.0.0.0:8082")]
    pub a2a: String,
    /// Write the bound A2A address to this file (used by supervisors and
    /// tests to discover ephemeral ports without parsing logs).
    #[arg(long)]
    pub a2a_addr_file: Option<PathBuf>,
    /// Optional UI bind address.
    #[arg(long)]
    pub ui: Option<String>,
    /// External card URL for TLS mode (e.g. `https://hub.example:8443`);
    /// defaults to the bound address.
    #[arg(long)]
    pub card_url: Option<String>,
    /// TLS server certificate chain in PEM. Enables mTLS serving and
    /// requires `--tls-key` and `--tls-ca`.
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    /// TLS server private key in PEM.
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
    /// CA root(s) in PEM; client certificates must chain to this.
    #[arg(long)]
    pub tls_ca: Option<PathBuf>,
    /// Serial deny-list file: one hex serial per line. TLS mode only.
    #[arg(long)]
    pub revoke_list: Option<PathBuf>,
}

/// Runs the hub until ctrl-c.
///
/// # Arguments
///
/// * `args` — the parsed hub arguments.
///
/// # Errors
///
/// Returns a boxed error on store, bind, or TLS-material failures.
pub async fn run_hub(args: HubRunArgs) -> Result<(), Box<dyn std::error::Error>> {
    let db = canonical_db_path(&args.db)?;
    let store = Store::open(&db, "wrkspc-hub").await?;
    store.ensure_peer("operator", "operator", "human").await?;
    // The operator is the one peer with no node: the hub self-reports
    // its presence at startup (HLD rev0.9).
    let _ = store
        .upsert_roster_presence(
            "operator",
            "operator",
            "human",
            wrkspc_core::PILOT_HOST_ID,
            "connected",
        )
        .await?;

    let tls = match (&args.tls_cert, &args.tls_key, &args.tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some((cert, key, ca)),
        (None, None, None) => None,
        _ => return Err("--tls-cert, --tls-key, and --tls-ca must be provided together".into()),
    };
    if args.revoke_list.is_some() && tls.is_none() {
        return Err("--revoke-list requires TLS mode".into());
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_future = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };
    let mut tasks = Vec::new();

    // Operator UI (optional): same surface as the node role; the cancel
    // seam is a no-op here — the hub has no local sessions to interrupt.
    if let Some(bind) = args.ui {
        let token = resolve_ui_token(&db).await?;
        let ui = UiServer::new(
            store.clone(),
            Arc::new(NoOpCancel),
            UiConfig {
                bind: bind.parse()?,
                token: Some(token),
                queue_remote_deliveries: true,
                hub_role: true,
            },
            None,
        );
        let listener = TcpListener::bind(&bind).await?;
        tracing::info!(%bind, "hub UI bound");
        let rx = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move {
            let _ = ui.serve_on(listener, shutdown_future(rx)).await;
        }));
    }

    // A2A surface.
    let listener = TcpListener::bind(&args.a2a).await?;
    let addr = listener.local_addr()?;
    if let Some(path) = &args.a2a_addr_file {
        tokio::fs::write(path, addr.to_string())
            .await
            .map_err(|e| format!("cannot write bound address to {}: {e}", path.display()))?;
    }
    let surface = A2aSurface::new(store.clone()).with_mtls(tls.is_some());
    let a2a_task = match tls {
        Some((cert, key, ca)) => {
            let acceptor = build_acceptor(cert, key, ca)?;
            // The live deny set: the operator's --revoke-list file plus
            // store-persisted runtime bans (the operator UI writes both
            // the store and this set, so revocations bite on the next
            // handshake without a hub restart).
            let mut denied: std::collections::HashSet<String> =
                load_deny_list(args.revoke_list.as_deref())?;
            denied.extend(store.cert_revocations().await?);
            let denied = Arc::new(std::sync::RwLock::new(denied));
            // For the handshake callback's fire-and-forget serial
            // recording.
            let store_for_serial = store.clone();
            let base_url = args
                .card_url
                .clone()
                .unwrap_or_else(|| format!("https://{addr}"));
            let acceptance = crate::acceptance::AcceptanceState::new(
                store.clone(),
                wrkspc_core::PILOT_HOST_ID.to_string(),
            );
            let delivery = crate::delivery::DeliveryState::new(store.clone());
            let presence =
                crate::presence::PresenceState::new(store.clone(), base_url.clone(), tls.is_some());
            // The UI router joins the TLS surface: the browser (no client
            // certificate) reaches `/` and `/api/*` behind the bearer
            // gate, while the A2A routes stay behind the stamp gate.
            let ui_token = resolve_ui_token(&db).await?;
            let ui_app = UiServer::new(
                store.clone(),
                Arc::new(NoOpCancel),
                UiConfig {
                    bind: "127.0.0.1:0".parse().expect("static address parses"),
                    token: Some(ui_token.clone()),
                    queue_remote_deliveries: true,
                    hub_role: true,
                },
                Some(denied.clone()),
            )
            .into_app(ui_token);
            tokio::spawn(async move {
                let _ = surface
                    .serve_tls(
                        listener,
                        acceptor,
                        base_url,
                        shutdown_future(shutdown_rx.clone()),
                        move |certs| {
                            // With the optional verifier a connection may
                            // present no certificate (the browser); allow
                            // it unstamped — the stamp gate keeps it off
                            // the A2A routes. Presented chains that
                            // resolve to a serial on the deny-list are
                            // still dropped. The returned identity is
                            // stamped as `x-wrkspc-peer` on every request
                            // a stamped connection makes.
                            let Some(identity) = certs.first().and_then(wrkspc_a2a::peer_identity)
                            else {
                                return wrkspc_a2a::PeerDecision::Unstamped;
                            };
                            // Remember the presented serial so a later
                            // operator ban can revoke the certificate the
                            // hub has actually seen (first serial wins).
                            if let Some(serial) = certs.first().and_then(cert_serial_hex) {
                                let store = store_for_serial.clone();
                                let identity_for_serial = identity.clone();
                                tokio::spawn(async move {
                                    if let Err(error) = store
                                        .record_peer_cert_serial(&identity_for_serial, &serial)
                                        .await
                                    {
                                        tracing::warn!(%error, %identity_for_serial, "cert serial recording failed");
                                    }
                                });
                            }
                            let denied = certs
                                .first()
                                .and_then(cert_serial_hex)
                                .is_some_and(|serial| {
                                    denied
                                        .read()
                                        .expect("deny lock poisoned")
                                        .contains(&serial)
                                });
                            if denied {
                                wrkspc_a2a::PeerDecision::Deny
                            } else {
                                wrkspc_a2a::PeerDecision::Stamped(identity)
                            }
                        },
                        move |router| {
                            ui_app.merge(
                                router
                                    .layer(axum::middleware::from_fn_with_state(
                                        acceptance,
                                        crate::acceptance::accept_translation,
                                    ))
                                    .route(
                                        "/delivery-claim",
                                        axum::routing::post(crate::delivery::delivery_claim)
                                            .with_state(delivery.clone()),
                                    )
                                    .route(
                                        "/delivery-ack",
                                        axum::routing::post(crate::delivery::delivery_ack)
                                            .with_state(delivery),
                                    )
                                    .route(
                                        "/presence",
                                        axum::routing::post(crate::presence::presence_upsert)
                                            .with_state(presence.clone()),
                                    )
                                    .route(
                                        "/roster",
                                        axum::routing::get(crate::presence::roster_get)
                                            .with_state(presence.clone()),
                                    )
                                    .route(
                                        "/peers/{peer}/agent-card.json",
                                        axum::routing::get(crate::presence::peer_card)
                                            .with_state(presence),
                                    )
                                    // Outermost on the A2A subtree: cert-less
                                    // connections (the browser) must not
                                    // reach any A2A route.
                                    .layer(axum::middleware::from_fn(require_peer_stamp)),
                            )
                        },
                    )
                    .await;
            })
        }
        None => tokio::spawn(async move {
            let _ = surface
                .serve(listener, shutdown_future(shutdown_rx.clone()))
                .await;
        }),
    };
    tasks.push(a2a_task);

    tracing::info!("wrkspc hub running — ctrl-c to stop");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    let _ = shutdown_tx.send(true);
    for task in tasks {
        let _ = task.await;
    }
    Ok(())
}

/// Builds the hub TLS acceptor: server cert + optional client certs
/// chained to `ca`. Client authentication is requested but not
/// required — the browser UI joins the same listener; the A2A routes
/// enforce the client-certificate stamp themselves.
///
/// # Arguments
///
/// * `cert`/`key` — server material in PEM.
/// * `ca` — trusted CA roots in PEM.
///
/// # Returns
///
/// The configured acceptor.
///
/// # Errors
///
/// Returns an error when PEM material cannot be loaded or the rustls
/// config cannot be built.
fn build_acceptor(
    cert: &Path,
    key: &Path,
    ca: &Path,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error>> {
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(cert)?))
            .collect::<Result<_, _>>()?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(key)?))?
        .ok_or("no private key found")?;

    let mut roots = rustls::RootCertStore::empty();
    for root in rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(ca)?)) {
        roots.add(root?)?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
    let verifier = wrkspc_a2a::OptionalClientVerifier::new(verifier);
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(verifier))
        .with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Resolves the operator bearer token: `WRKSPC_UI_TOKEN` when set,
/// otherwise the stable token derived from the hub database.
///
/// # Arguments
///
/// * `db` — the hub database path.
///
/// # Returns
///
/// The token.
///
/// # Errors
///
/// Returns a boxed error when the stable token cannot be derived.
async fn resolve_ui_token(db: &Path) -> Result<String, Box<dyn std::error::Error>> {
    // The hub's identity doubles as its file-store key on non-macOS.
    crate::secrets::resolve_ui_token(&db.display().to_string(), db)
        .await
        .map_err(Into::into)
}

/// Rejects requests without the `x-wrkspc-peer` identity stamp.
///
/// The TLS accept loop stamps every request from a certificate-bearing
/// connection; cert-less connections (the browser UI) carry no stamp
/// and must not reach the A2A surface.
async fn require_peer_stamp(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if request.headers().contains_key("x-wrkspc-peer") {
        next.run(request).await
    } else {
        axum::response::Response::builder()
            .status(axum::http::StatusCode::FORBIDDEN)
            .body(axum::body::Body::from("client certificate required"))
            .expect("static response builds")
    }
}

/// Loads the serial deny-list file (one lowercase hex serial per line).
///
/// # Arguments
///
/// * `path` — optional file path.
///
/// # Returns
///
/// The set of denied serials (empty when `path` is `None`).
///
/// # Errors
///
/// Returns an error when the file exists but cannot be read.
fn load_deny_list(path: Option<&Path>) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(HashSet::new());
    };
    let mut set = HashSet::new();
    for line in std::io::BufReader::new(std::fs::File::open(path)?).lines() {
        let line = line?.trim().to_ascii_lowercase();
        if !line.is_empty() {
            set.insert(line);
        }
    }
    tracing::info!(count = set.len(), "serial deny-list loaded");
    Ok(set)
}

/// Cancel seam for the hub UI: the hub has no local sessions, so
/// cancellation is always refused with an explanation.
#[derive(Debug)]
struct NoOpCancel;

impl wrkspc_ui::CancelRequester for NoOpCancel {
    fn request_cancel(
        &self,
        _message_id: &str,
        _recipient_id: &str,
        _set_by: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<CancelOutcome, String>> + Send + '_>> {
        Box::pin(async {
            Err(
                "the hub role owns no local sessions; cancel the node that executes the attempt"
                    .to_string(),
            )
        })
    }
}
