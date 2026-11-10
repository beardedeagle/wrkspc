//! The serving surface: v1.0 agent card + JSON-RPC/REST routers.
//!
//! The card declares `mtlsSecurityScheme` only when the surface is
//! configured with [`A2aSurface::with_mtls`] (the certificate-stamped
//! TLS transport) — a plaintext listener declares nothing, or clients
//! would attempt mTLS against plaintext.

use crate::PeerDecision;
use crate::WrkspcExecutor;
use crate::WrkspcTaskStore;
use a2a::{AgentCapabilities, AgentCard, AgentInterface, AgentProvider, AgentSkill};
use a2a_server::agent_card::{StaticAgentCard, agent_card_router};
use a2a_server::handler::DefaultRequestHandler;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use wrkspc_core::Store;

/// Builds the pilot's v1.0 agent card.
///
/// # Arguments
///
/// * `base_url` — the externally reachable base URL (scheme + host + port),
///   e.g. `https://hub.example:8443`. Interface URLs derive from it.
/// * `mtls` — whether the served transport is the certificate-stamped TLS
///   surface; when true the card declares `mtlsSecurityScheme` and its
///   security requirement (A2A v1.0 §4.5.6). A plaintext listener must
///   not declare it, or clients will attempt mTLS against plaintext.
///
/// # Returns
///
/// The card: capabilities per v1.0 field structure (`extendedAgentCard`
/// inside `capabilities`), JSON-RPC + HTTP+JSON interfaces, and the
/// mTLS declaration only when `mtls`.
pub fn build_agent_card(base_url: &str, mtls: bool) -> AgentCard {
    let (security_schemes, security_requirements) = if mtls {
        let schemes = std::collections::HashMap::from([(
            "mtlsSecurityScheme".to_string(),
            a2a::SecurityScheme::MutualTls(a2a::MutualTlsSecurityScheme { description: None }),
        )]);
        let requirements = vec![std::collections::HashMap::from([(
            "mtlsSecurityScheme".to_string(),
            Vec::<String>::new(),
        )])];
        (Some(schemes), Some(requirements))
    } else {
        (None, None)
    };
    AgentCard {
        name: "wrkspc".to_string(),
        description:
            "Coordination hub: near-real-time conversations between humans and agents, observed and moderated."
                .to_string(),
        version: "0.1.0".to_string(),
        provider: Some(AgentProvider {
            organization: "wrkspc".to_string(),
            url: "https://github.com/beardedeagle/wrkspc".to_string(),
        }),
        capabilities: AgentCapabilities {
            streaming: Some(true),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: None,
        },
        skills: vec![AgentSkill {
            id: "coordination".to_string(),
            name: "Coordination".to_string(),
            description: "Routes messages and work between enrolled peers.".to_string(),
            tags: vec!["coordination".to_string(), "moderation".to_string()],
            examples: None,
            input_modes: None,
            output_modes: None,
            security_requirements: None,
        }],
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        supported_interfaces: vec![
            AgentInterface::new(
                format!("{base_url}/jsonrpc"),
                a2a::TRANSPORT_PROTOCOL_JSONRPC,
            ),
            AgentInterface::new(
                format!("{base_url}/rest"),
                a2a::TRANSPORT_PROTOCOL_HTTP_JSON,
            ),
        ],
        security_schemes,
        security_requirements,
        documentation_url: None,
        icon_url: None,
        signatures: None,
    }
}

/// Builds the per-peer discovery card (HLD rev0.9): one card per
/// enrolled peer, served by the hub under `/peers/{peer}/agent-card.json`.
///
/// The hub holds the peer's A2A identity, so the card describes the
/// peer but points delegation at the hub itself — same interfaces and
/// same security schemes as the hub card (the `mtlsSecurityScheme`
/// declaration follows the hub card's `mtls` flag). Read-only
/// discovery; capability claims stay minimal.
///
/// # Arguments
///
/// * `base_url` — the hub's base URL (the card's interface target).
/// * `entry` — the roster row for the peer.
/// * `mtls` — whether the hub's TLS surface serves the card.
pub fn build_peer_card(base_url: &str, entry: &wrkspc_core::RosterEntry, mtls: bool) -> AgentCard {
    let (security_schemes, security_requirements) = if mtls {
        let schemes = std::collections::HashMap::from([(
            "mtlsSecurityScheme".to_string(),
            a2a::SecurityScheme::MutualTls(a2a::MutualTlsSecurityScheme { description: None }),
        )]);
        let requirements = vec![std::collections::HashMap::from([(
            "mtlsSecurityScheme".to_string(),
            Vec::<String>::new(),
        )])];
        (Some(schemes), Some(requirements))
    } else {
        (None, None)
    };
    AgentCard {
        name: entry.peer_id.clone(),
        description: format!(
            "wrkspc-enrolled {} peer (hub-held identity; live state: {})",
            entry.class, entry.state
        ),
        version: "0.1.0".to_string(),
        provider: Some(AgentProvider {
            organization: "wrkspc".to_string(),
            url: "https://github.com/beardedeagle/wrkspc".to_string(),
        }),
        capabilities: AgentCapabilities {
            streaming: Some(true),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: None,
        },
        skills: vec![AgentSkill {
            id: format!("peer-{}", entry.class),
            name: entry.name.clone(),
            description: format!(
                "Enrolled {} peer hosted on {} (state: {})",
                entry.class, entry.host_id, entry.state
            ),
            tags: vec!["peer".to_string(), entry.class.clone()],
            examples: None,
            input_modes: None,
            output_modes: None,
            security_requirements: None,
        }],
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        supported_interfaces: vec![
            AgentInterface::new(
                format!("{base_url}/jsonrpc"),
                a2a::TRANSPORT_PROTOCOL_JSONRPC,
            ),
            AgentInterface::new(
                format!("{base_url}/rest"),
                a2a::TRANSPORT_PROTOCOL_HTTP_JSON,
            ),
        ],
        security_schemes,
        security_requirements,
        documentation_url: None,
        icon_url: None,
        signatures: None,
    }
}

/// The serving surface for the coordinator role.
#[derive(Debug)]
pub struct A2aSurface {
    store: Store,
    /// Whether the served transport is the certificate-stamped TLS
    /// surface — the card declares `mtlsSecurityScheme` only then.
    mtls: bool,
}

impl A2aSurface {
    /// Builds the surface over a wrkspc store.
    ///
    /// # Arguments
    ///
    /// * `store` — the transactional store (shared with the dispatcher).
    pub fn new(store: Store) -> Self {
        Self { store, mtls: false }
    }

    /// Declares that the transport serving this surface is the
    /// certificate-stamped TLS surface, so the served agent cards
    /// declare `mtlsSecurityScheme` (A2A v1.0 §4.5.6). Plaintext
    /// serving keeps the default and declares nothing.
    pub fn with_mtls(mut self, mtls: bool) -> Self {
        self.mtls = mtls;
        self
    }

    /// Serves on `listener` until `shutdown` resolves.
    ///
    /// # Arguments
    ///
    /// * `listener` — a bound TCP listener (tests bind port 0 to discover
    ///   the address; production binds the configured addr).
    /// * `shutdown` — a future that ends serving when it resolves.
    ///
    /// # Returns
    ///
    /// The bound address.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` on serve failure.
    pub async fn serve<F>(self, listener: TcpListener, shutdown: F) -> std::io::Result<SocketAddr>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let addr = listener.local_addr()?;
        let base_url = format!("http://{addr}");
        let app = self.into_router(&base_url);

        tracing::info!(%addr, "wrkspc A2A surface bound");
        tokio::select! {
            result = axum::serve(listener, app) => {
                result?;
            }
            _ = shutdown => {}
        }
        Ok(addr)
    }

    /// Serves on `listener` behind a TLS acceptor until `shutdown`
    /// resolves. Each accepted connection completes the TLS handshake;
    /// `resolve_peer` receives the presented peer certificate chain
    /// (empty when the client presented none) and returns a
    /// [`PeerDecision`]: [`PeerDecision::Deny`] drops the connection
    /// before any HTTP, [`PeerDecision::Unstamped`] serves it without a
    /// stamp (for clients that present no certificate, when the acceptor
    /// makes client auth optional), and [`PeerDecision::Stamped`] serves
    /// it and stamps the `x-wrkspc-peer` header on every request the
    /// connection makes. Callers enforce identity/revocation policy in
    /// the closure (rustls verifies chains only); downstream handlers
    /// bind the header back to enrolment records, never trusting
    /// client-asserted identity.
    ///
    /// # Arguments
    ///
    /// * `listener` — a bound TCP listener.
    /// * `acceptor` — a configured [`tokio_rustls::TlsAcceptor`]; require
    ///   client certificates via rustls `WebPkiClientVerifier` for mTLS,
    ///   or use [`OptionalClientVerifier`] to request them without
    ///   requiring them.
    /// * `base_url` — the externally reachable base URL with scheme
    ///   (e.g. `https://hub.example:8443`); the agent card derives from
    ///   it, so it must match the served transport.
    /// * `shutdown` — a future that ends serving when it resolves.
    /// * `resolve_peer` — per-connection policy over the peer
    ///   certificate chain.
    /// * `decorate` — an optional router transformation applied once
    ///   before serving (e.g. the hub's acceptance middleware); pass the
    ///   identity function for the bare surface.
    ///
    /// # Returns
    ///
    /// The bound address.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` on accept-loop failure.
    pub async fn serve_tls<F, G, D>(
        self,
        listener: TcpListener,
        acceptor: tokio_rustls::TlsAcceptor,
        base_url: String,
        shutdown: F,
        resolve_peer: G,
        decorate: D,
    ) -> std::io::Result<SocketAddr>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
        G: Fn(&[rustls_pki_types::CertificateDer<'_>]) -> PeerDecision + Send + Sync + 'static,
        D: FnOnce(axum::Router) -> axum::Router + Send + 'static,
    {
        let addr = listener.local_addr()?;
        let app = decorate(self.into_router(&base_url));
        let resolve_peer = std::sync::Arc::new(resolve_peer);
        let mut shutdown = std::pin::pin!(shutdown);
        // Connection tasks are TRACKED and aborted at shutdown (P09):
        // `serve_tls` returning must not leave connections serving.
        let mut connections = tokio::task::JoinSet::new();

        tracing::info!(%addr, "wrkspc A2A surface bound (TLS)");
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => {
                    let Ok((tcp, peer_addr)) = accepted else { continue };
                    let acceptor = acceptor.clone();
                    let app = app.clone();
                    let resolve_peer = resolve_peer.clone();
                    connections.spawn(async move {
                        // Bound the TLS handshake (P09): a client that
                        // connects and never completes TLS must not pin
                        // a task forever; incomplete handshakes time out.
                        let tls = match tokio::time::timeout(
                            std::time::Duration::from_secs(30),
                            acceptor.accept(tcp),
                        )
                        .await
                        {
                            Ok(Ok(tls)) => tls,
                            Ok(Err(error)) => {
                                tracing::warn!(%peer_addr, %error, "TLS handshake rejected");
                                return;
                            }
                            Err(_) => {
                                tracing::warn!(%peer_addr, "TLS handshake timed out");
                                return;
                            }
                        };
                        let certs: &[rustls_pki_types::CertificateDer<'_>] = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .unwrap_or_default();
                        let identity = match resolve_peer(certs) {
                            PeerDecision::Deny => {
                                let identity_display = certs
                                    .first()
                                    .and_then(peer_identity)
                                    .unwrap_or_else(|| "(no cert)".into());
                                tracing::warn!(%peer_addr, %identity_display, "peer denied by policy");
                                return;
                            }
                            PeerDecision::Unstamped => {
                                tracing::debug!(%peer_addr, "TLS connection accepted (no client certificate)");
                                None
                            }
                            PeerDecision::Stamped(identity) => {
                                tracing::debug!(%peer_addr, %identity, "TLS connection accepted");
                                Some(identity)
                            }
                        };
                        // Stamp the verified identity and serial onto every
                        // request this connection makes; handlers trust these
                        // headers over anything the client asserts in the
                        // payload. Unstamped connections carry neither — and
                        // any client-supplied stamp is removed FIRST, so a
                        // cert-less connection can never forge the peer
                        // identity this surface enforces.
                        let serial = certs.first().and_then(cert_serial_hex);
                        let stamped = tower::ServiceBuilder::new()
                            .map_request(
                                move |mut request: hyper::Request<hyper::body::Incoming>| {
                                    request.headers_mut().remove("x-wrkspc-peer");
                                    request.headers_mut().remove("x-wrkspc-serial");
                                    // The identities come from verified cert
                                    // SANs; a header-parse failure here would
                                    // be a bug, and dropping the header fails
                                    // CLOSED — the request is then anonymous
                                    // and the peer-stamp middleware denies it.
                                    if let Some(identity) = &identity
                                        && let Ok(value) = identity.parse::<hyper::header::HeaderValue>()
                                    {
                                        request.headers_mut().insert(
                                            hyper::header::HeaderName::from_static("x-wrkspc-peer"),
                                            value,
                                        );
                                    }
                                    if let Some(serial) = &serial
                                        && let Ok(value) = serial.parse::<hyper::header::HeaderValue>()
                                    {
                                        request.headers_mut().insert(
                                            hyper::header::HeaderName::from_static(
                                                "x-wrkspc-serial",
                                            ),
                                            value,
                                        );
                                    }
                                    request
                                },
                            )
                            .service(app.clone());
                        let service =
                            hyper_util::service::TowerToHyperService::new(stamped);
                        let io = hyper_util::rt::TokioIo::new(tls);
                        let _ = hyper_util::server::conn::auto::Builder::new(
                            hyper_util::rt::TokioExecutor::new(),
                        )
                        .serve_connection(io, service)
                        .await;
                    });
                }
            }
        }
        // Stop accepting, then stop serving: existing connections are
        // aborted and joined before `serve_tls` returns (P09).
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Ok(addr)
    }

    /// Builds the axum router for the surface. Exposed so callers can
    /// serve the same routers over their own transport (e.g. a TLS
    /// accept loop); consumes the surface.
    ///
    /// # Arguments
    ///
    /// * `base_url` — the externally reachable base URL with scheme; the
    ///   agent card derives its interface URLs from it.
    ///
    /// # Returns
    ///
    /// The router (JSON-RPC + REST + agent card).
    pub fn into_router(self, base_url: &str) -> axum::Router {
        let handler = Arc::new(DefaultRequestHandler::new(
            WrkspcExecutor::new(self.store.clone()),
            WrkspcTaskStore::new(self.store),
        ));
        let card_producer = Arc::new(StaticAgentCard::new(build_agent_card(base_url, self.mtls)));

        axum::Router::new()
            .nest(
                "/jsonrpc",
                a2a_server::jsonrpc::jsonrpc_router(handler.clone()),
            )
            .nest("/rest", a2a_server::rest::rest_router(handler.clone()))
            .merge(agent_card_router(card_producer))
    }
}

/// Extracts `CN` + DNS SANs from a DER certificate for LOG/display
/// attribution only. This string must never participate in identity
/// or authorization decisions — the CN is free text chosen by the CA
/// issuer and can carry `SAN=` markers; the stamped identity is
/// [`host_identity`], built from the parsed SAN extension alone.
///
/// # Arguments
///
/// * `cert` — a leaf certificate in DER form.
///
/// # Returns
///
/// A human-readable identity string, or `None` when the certificate
/// cannot be parsed.
pub fn peer_identity(cert: &rustls_pki_types::CertificateDer<'_>) -> Option<String> {
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    let cn = parsed
        .subject()
        .iter_common_name()
        .next()
        .map(|cn| cn.as_str().unwrap_or("?"))
        .unwrap_or("?");
    let sans: Vec<String> = parsed
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|ext| {
            ext.value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    x509_parser::extensions::GeneralName::DNSName(d) => Some(d.to_string()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    Some(if sans.is_empty() {
        format!("CN={cn}")
    } else {
        format!("CN={cn} SAN={}", sans.join(","))
    })
}

/// Extracts the authorization identity (the host id) from a client
/// certificate: the FIRST DNS SAN of the parsed SAN extension, never
/// anything parsed out of human-readable subject text.
///
/// The TLS accept loop stamps this value alone as `x-wrkspc-peer`;
/// downstream handlers treat the header as the authenticated host.
/// A certificate without a DNS SAN yields `None` — the connection is
/// served unstamped and the stamp gate keeps it off the A2A routes
/// (fail-closed: the CN alone is never an identity).
///
/// # Arguments
///
/// * `cert` — a leaf certificate in DER form.
///
/// # Returns
///
/// The first DNS SAN, or `None` when the certificate cannot be parsed
/// or carries no DNS SAN.
pub fn host_identity(cert: &rustls_pki_types::CertificateDer<'_>) -> Option<String> {
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    parsed
        .subject_alternative_name()
        .ok()
        .flatten()
        .and_then(|ext| {
            ext.value.general_names.iter().find_map(|name| match name {
                x509_parser::extensions::GeneralName::DNSName(d) => {
                    let host = d.to_string();
                    (!host.is_empty()).then_some(host)
                }
                _ => None,
            })
        })
}

/// Reads the leaf serial as lowercase hex, normalized to match revoke
/// records: DER INTEGER serials with the high bit set carry a leading
/// `0x00` that `step`/`openssl` omit, so leading zeros are stripped.
///
/// # Arguments
///
/// * `cert` — a leaf certificate in DER form.
///
/// # Returns
///
/// The normalized hex serial, or `None` when the certificate cannot be
/// parsed.
pub fn cert_serial_hex(cert: &rustls_pki_types::CertificateDer<'_>) -> Option<String> {
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    let mut hex = String::new();
    for byte in parsed.raw_serial() {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    let trimmed = hex.trim_start_matches('0');
    Some(if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::{cert_serial_hex, host_identity, peer_identity};
    use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    use rustls_pki_types::CertificateDer;

    /// Builds a client certificate whose CN carries a crafted
    /// `SAN=` marker while its DNS SAN is the real host.
    fn crafted_cn_cert() -> CertificateParams {
        let mut params = CertificateParams::new(vec!["node-attacker".to_string()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "SAN=node-victim");
        params
    }

    /// Signs `params` under a fresh CA and returns the leaf DER.
    fn sign(params: CertificateParams) -> CertificateDer<'static> {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "wrkspc-test-ca");
        let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();
        let leaf_key = KeyPair::generate().unwrap();
        let leaf = params.signed_by(&leaf_key, &ca).unwrap();
        CertificateDer::from(leaf.der().to_vec())
    }

    /// H-1: the authorization identity is the parsed DNS SAN alone —
    /// CN text carrying a `SAN=` marker can never override it.
    #[test]
    fn host_identity_is_the_parsed_san_never_cn_text() {
        let cert = sign(crafted_cn_cert());
        assert_eq!(
            host_identity(&cert).as_deref(),
            Some("node-attacker"),
            "the DNS SAN is the host, not text parsed out of the CN"
        );
        // The display string still carries the CN (for logs only), and
        // must never be used for attribution.
        let display = peer_identity(&cert).unwrap();
        assert!(
            display.contains("SAN=node-victim"),
            "the display string keeps the raw CN text: {display}"
        );
        assert!(
            cert_serial_hex(&cert).is_some(),
            "serial extraction still works on the crafted leaf"
        );
    }

    /// A certificate without a DNS SAN has no host identity: it is
    /// served unstamped and the stamp gate keeps it off the A2A routes
    /// (fail-closed — the CN alone is never an identity).
    #[test]
    fn cert_without_dns_san_has_no_host_identity() {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "cn-only");
        let cert = sign(params);
        assert_eq!(host_identity(&cert), None);
    }
}
