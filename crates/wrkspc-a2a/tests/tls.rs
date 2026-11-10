//! mTLS serving tests for the A2A surface: the transport seam (F5).
//!
//! Proves, over a real loopback socket with rcgen-issued material:
//!
//! 1. a client presenting a CA-issued certificate completes the handshake
//!    and drives the surface (`SendMessage` → `submitted`);
//! 2. a client without a certificate is rejected at the handshake;
//! 3. the `allow_peer` policy gate drops a denied identity before any
//!    HTTP, while healthy peers pass;
//! 4. `cert_serial_hex` normalizes DER serial padding to match revoke
//!    records (the deny-list comparison contract).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use a2a::{
    GetTaskRequest, Message, Part, Role, SendMessageRequest, SendMessageResponse, TaskState,
};
use a2a_client::A2AClient;
use a2a_client::jsonrpc::JsonRpcTransport;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tokio::net::TcpListener;
use wrkspc_a2a::{A2aSurface, cert_serial_hex};
use wrkspc_core::Store;

/// A self-contained PKI for the test: root CA + one server + one client
/// certificate, all PEM.
struct TestPki {
    ca_pem: Vec<u8>,
    server_cert_der: Vec<u8>,
    server_key_der: Vec<u8>,
    client_pem: Vec<u8>,
}

impl TestPki {
    /// Generates the CA, a server cert (SAN `localhost`), and a client
    /// cert (SAN `node-test-1`).
    fn generate() -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "wrkspc-test-ca");
        let ca_cert = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();

        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        server_params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
        server_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        server_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "hub");
        let server_cert = server_params.signed_by(&server_key, &ca_cert).unwrap();

        let client_key = KeyPair::generate().unwrap();
        let mut client_params = CertificateParams::new(vec!["node-test-1".to_string()]).unwrap();
        client_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        client_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "node-test-1");
        let client_cert = client_params.signed_by(&client_key, &ca_cert).unwrap();

        Self {
            ca_pem: ca_cert.pem().into_bytes(),
            server_cert_der: server_cert.der().to_vec(),
            server_key_der: server_key.serialize_der(),
            client_pem: format!("{}{}", client_cert.pem(), client_key.serialize_pem()).into_bytes(),
        }
    }
}

/// Builds a rustls acceptor requiring client certs chained to the CA.
fn mtls_acceptor(pki: &TestPki) -> tokio_rustls::TlsAcceptor {
    let (server_cert, server_key, roots) = acceptor_material(pki);
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![server_cert], server_key)
        .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

/// Builds a rustls acceptor that requests but does not require client
/// certs (the hub's browser-bearing TLS surface).
fn optional_acceptor(pki: &TestPki) -> tokio_rustls::TlsAcceptor {
    let (server_cert, server_key, roots) = acceptor_material(pki);
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    let verifier = wrkspc_a2a::OptionalClientVerifier::new(verifier);
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(verifier))
        .with_single_cert(vec![server_cert], server_key)
        .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

/// Loads the server cert/key and CA roots shared by the acceptors.
fn acceptor_material(
    pki: &TestPki,
) -> (
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
    rustls::RootCertStore,
) {
    let server_cert = rustls::pki_types::CertificateDer::from(pki.server_cert_der.clone());
    let server_key =
        rustls::pki_types::PrivateKeyDer::try_from(pki.server_key_der.clone()).expect("ec key der");

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pki.ca_pem.as_slice()) {
        roots.add(cert.expect("ca cert")).expect("add root");
    }
    (server_cert, server_key, roots)
}

/// Builds an A2A client over the hub URL with the given identity/CA
/// material.
fn tls_client(hub: &str, pki: &TestPki, identity: Option<&[u8]>) -> A2AClient<JsonRpcTransport> {
    let mut builder = reqwest::Client::builder();
    if let Some(pem) = identity {
        builder = builder.identity(reqwest::Identity::from_pem(pem).unwrap());
    }
    builder = builder.add_root_certificate(reqwest::Certificate::from_pem(&pki.ca_pem).unwrap());
    let transport = JsonRpcTransport::new(builder.build().unwrap(), hub.to_string());
    A2AClient::new(transport)
}

/// Sends one message and asserts the accepted-but-pending contract.
async fn send_ping(client: &A2AClient<JsonRpcTransport>) -> String {
    let response = client
        .send_message(&SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("mtls ping".to_string())]),
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await
        .expect("send");
    match response {
        SendMessageResponse::Task(task) => {
            assert_eq!(task.status.state, TaskState::Submitted);
            task.id
        }
        other => panic!("expected task, got {other:?}"),
    }
}

/// Serves the surface over TLS (required client certs) until
/// `shutdown_tx` fires.
async fn serve_tls(
    store: Store,
    pki: &TestPki,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    resolve_peer: impl Fn(&[rustls_pki_types::CertificateDer<'_>]) -> wrkspc_a2a::PeerDecision
    + Send
    + Sync
    + 'static,
) -> std::net::SocketAddr {
    serve_with(store, pki, mtls_acceptor(pki), shutdown_tx, resolve_peer).await
}

/// Serves the surface over TLS with the given acceptor until
/// `shutdown_tx` fires.
async fn serve_with(
    store: Store,
    _pki: &TestPki,
    acceptor: tokio_rustls::TlsAcceptor,
    mut shutdown_tx: tokio::sync::oneshot::Sender<()>,
    resolve_peer: impl Fn(&[rustls_pki_types::CertificateDer<'_>]) -> wrkspc_a2a::PeerDecision
    + Send
    + Sync
    + 'static,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let surface = A2aSurface::new(store);
    tokio::spawn(async move {
        let _ = surface
            .serve_tls(
                listener,
                acceptor,
                format!("https://{addr}"),
                async move {
                    let _ = shutdown_tx.closed().await;
                },
                resolve_peer,
                |router| router,
            )
            .await;
    });
    addr
}
async fn test_store() -> Store {
    Store::open_in_memory("tls-test").await.unwrap()
}

/// A client presenting a valid certificate completes the mTLS handshake and drives the A2A surface.
#[tokio::test]
async fn mtls_client_with_cert_drives_the_surface() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pki = TestPki::generate();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let addr = serve_tls(test_store().await, &pki, shutdown_tx, |certs| {
        certs
            .first()
            .and_then(wrkspc_a2a::peer_identity)
            .map(wrkspc_a2a::PeerDecision::Stamped)
            .unwrap_or(wrkspc_a2a::PeerDecision::Deny)
    })
    .await;
    let hub = format!("https://{addr}/jsonrpc");

    let client = tls_client(&hub, &pki, Some(&pki.client_pem));
    let task_id = send_ping(&client).await;
    let fetched = client
        .get_task(&GetTaskRequest {
            id: task_id,
            tenant: None,
            history_length: None,
        })
        .await
        .expect("get");
    assert_eq!(fetched.status.state, TaskState::Submitted);

    drop(shutdown_rx);
}

/// A client presenting no certificate is refused at the TLS layer.
#[tokio::test]
async fn client_without_cert_is_rejected() {
    let pki = TestPki::generate();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let addr = serve_tls(test_store().await, &pki, shutdown_tx, |certs| {
        certs
            .first()
            .and_then(wrkspc_a2a::peer_identity)
            .map(wrkspc_a2a::PeerDecision::Stamped)
            .unwrap_or(wrkspc_a2a::PeerDecision::Deny)
    })
    .await;
    let hub = format!("https://{addr}/jsonrpc");

    let client = tls_client(&hub, &pki, None);
    let result = client
        .send_message(&SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("no cert".to_string())]),
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await;
    assert!(
        result.is_err(),
        "a client without a certificate must not be served"
    );

    drop(shutdown_rx);
}

/// Optional client auth serves cert-less connections without the stamped identity (plain-TCP serving mode).
#[tokio::test]
async fn optional_client_auth_serves_cert_less_connections_unstamped() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let pki = TestPki::generate();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    // The optional acceptor requests but does not require a cert; the
    // policy allows cert-less connections unstamped.
    let addr = serve_with(
        test_store().await,
        &pki,
        optional_acceptor(&pki),
        shutdown_tx,
        |certs| {
            certs
                .first()
                .and_then(wrkspc_a2a::peer_identity)
                .map(wrkspc_a2a::PeerDecision::Stamped)
                .unwrap_or(wrkspc_a2a::PeerDecision::Unstamped)
        },
    )
    .await;
    let hub = format!("https://{addr}/jsonrpc");

    // Without a cert the handshake completes and the surface serves the
    // request — unstamped, so the hub's own stamp gate would 403 it;
    // here the bare surface proves the transport seam only.
    let client = tls_client(&hub, &pki, None);
    let task_id = send_ping(&client).await;
    let fetched = client
        .get_task(&GetTaskRequest {
            id: task_id,
            tenant: None,
            history_length: None,
        })
        .await
        .expect("get");
    assert_eq!(fetched.status.state, TaskState::Submitted);

    // A cert-bearing client still gets verified and stamped.
    let client = tls_client(&hub, &pki, Some(&pki.client_pem));
    let _ = send_ping(&client).await;

    drop(shutdown_rx);
}

/// The resolve-peer policy gate denies a listed identity (the ban deny-list shape) before any application traffic.
#[tokio::test]
async fn policy_gate_denies_the_listed_identity() {
    let pki = TestPki::generate();
    // Deny any leaf whose SAN carries `node-test-1`.
    let resolve_peer = |certs: &[rustls_pki_types::CertificateDer<'_>]| {
        let Some(identity) = certs.first().and_then(wrkspc_a2a::peer_identity) else {
            return wrkspc_a2a::PeerDecision::Deny;
        };
        if identity.contains("node-test-1") {
            wrkspc_a2a::PeerDecision::Deny
        } else {
            wrkspc_a2a::PeerDecision::Stamped(identity)
        }
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let addr = serve_tls(test_store().await, &pki, shutdown_tx, resolve_peer).await;
    let hub = format!("https://{addr}/jsonrpc");

    let client = tls_client(&hub, &pki, Some(&pki.client_pem));
    let result = client
        .send_message(&SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("denied".to_string())]),
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await;
    assert!(
        result.is_err(),
        "the policy gate must drop the denied identity"
    );

    drop(shutdown_rx);
}

/// Certificate serials export as lowercase hex matching the revocation record format (high-bit serials included).
#[tokio::test]
async fn serial_hex_matches_revoke_record_format() {
    // rcgen mints a serial with the high bit set (leading DER 0x00 pad)
    // deterministically from this slice.
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.serial_number = Some(rcgen::SerialNumber::from_slice(&[0xBF, 0xED, 0xF5, 0x1C]));
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "padded");
    let cert = params.self_signed(&key).unwrap();

    let der = rustls_pki_types::CertificateDer::from(cert.der().to_vec());
    assert_eq!(
        cert_serial_hex(&der).as_deref(),
        Some("bfedf51c"),
        "leading DER padding must be stripped to match revoke records"
    );
}
