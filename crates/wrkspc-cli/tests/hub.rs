//! Hub-role smoke tests: the `wrkspc hub run` subcommand serves the A2A
//! surface (plain and mTLS) with the deny-list gate wired to the CLI.
//!
//! The TLS handshake contract itself is tested in `wrkspc-a2a`; these
//! tests prove the CLI wiring: flags → acceptor → per-connection policy.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use rustls_pki_types::CertificateDer;

/// A hub child process plus its discovered A2A address.
struct Hub {
    child: Child,
    addr: String,
    dir: PathBuf,
}

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Unique scratch dir for one test run.
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wrkspc-hub-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Spawns the hub and waits for the `--a2a-addr-file` to appear (a
/// deterministic side-channel: log parsing is unreliable because the
/// tracing writer buffers when the child's output is a pipe).
fn spawn_hub(args: &[&str], dir: &PathBuf, db: &str) -> Hub {
    let addr_file = dir.join("a2a-addr");
    let addr_file_s = addr_file.to_str().unwrap().to_string();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wrkspc"));
    // Pin the operator token via the environment override so the
    // platform secret store (macOS Keychain) is never touched by tests.
    cmd.env("WRKSPC_UI_TOKEN", "wrkspc-test-token");
    cmd.args([
        "hub",
        "run",
        "--db",
        db,
        "--a2a",
        "127.0.0.1:0",
        "--a2a-addr-file",
        &addr_file_s,
    ])
    .args(args)
    .current_dir(dir)
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    let child = cmd.spawn().expect("hub spawn");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut addr = None;
    while Instant::now() < deadline {
        if let Ok(content) = std::fs::read_to_string(&addr_file) {
            let trimmed = content.trim().to_string();
            if !trimmed.is_empty() {
                addr = Some(trimmed);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    match addr {
        Some(addr) => Hub {
            child,
            addr,
            dir: dir.clone(),
        },
        None => {
            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            panic!("hub never wrote its bound address");
        }
    }
}

/// A plain (no-mTLS) hub serves the A2A agent card at the well-known route.
#[test]
fn plain_hub_serves_the_agent_card() {
    let dir = scratch_dir("plain");
    let hub = spawn_hub(&[], &dir, "hub.db");

    let url = format!("http://{}/.well-known/agent-card.json", hub.addr);
    let card = reqwest::blocking::get(&url).expect("card fetch");
    assert_eq!(card.status(), 200);
    let body = card.text().unwrap();
    assert!(body.contains("\"name\":\"wrkspc\""), "got: {body}");
    assert!(
        !body.contains("mtlsSecurityScheme"),
        "a plaintext listener must not declare mTLS: {body}"
    );
}

/// Generates a test PKI on disk: CA, server cert/key (SANs for
/// `localhost` + 127.0.0.1), and a client identity bundle.
fn write_pki(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf, PathBuf, String) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "wrkspc-hub-test-ca");
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "hub");
    let server = server_params.signed_by(&server_key, &ca).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(vec!["node-hubtest".to_string()]).unwrap();
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "node-hubtest");
    let client = client_params.signed_by(&client_key, &ca).unwrap();

    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("hub.crt");
    let key_path = dir.join("hub.key");
    let client_path = dir.join("client.pem");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    std::fs::write(&cert_path, server.pem()).unwrap();
    std::fs::write(&key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(
        &client_path,
        format!("{}{}", client.pem(), client_key.serialize_pem()),
    )
    .unwrap();

    let serial = wrkspc_a2a::cert_serial_hex(&CertificateDer::from(client.der().to_vec()))
        .expect("client serial");
    (ca_path, cert_path, key_path, client_path, serial)
}

/// Fetches the card over HTTPS with the given identity (None = no
/// client cert).
fn tls_card(addr: &str, ca: &PathBuf, identity: Option<&PathBuf>) -> Result<u16, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10));
    if let Some(pem) = identity {
        let pem = std::fs::read(pem).unwrap();
        builder = builder.identity(reqwest::Identity::from_pem(&pem).unwrap());
    }
    builder = builder
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca).unwrap()).unwrap());
    let client = builder.build().map_err(|e| e.to_string())?;
    let url = format!("https://{addr}/.well-known/agent-card.json");
    match client.get(&url).send() {
        Ok(response) => Ok(response.status().as_u16()),
        Err(e) => Err(e.to_string()),
    }
}

/// Fetches the agent card over HTTPS with the given identity and
/// returns the body text.
fn tls_card_body(addr: &str, ca: &PathBuf, identity: &PathBuf) -> Result<String, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10));
    let pem = std::fs::read(identity).unwrap();
    builder = builder.identity(reqwest::Identity::from_pem(&pem).unwrap());
    builder = builder
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca).unwrap()).unwrap());
    let client = builder.build().map_err(|e| e.to_string())?;
    let url = format!("https://{addr}/.well-known/agent-card.json");
    client
        .get(&url)
        .send()
        .map_err(|e| e.to_string())?
        .text()
        .map_err(|e| e.to_string())
}

/// Fetches the static UI index over HTTPS with the given identity
/// (None = no client cert); the page itself needs no bearer token.
fn tls_ui_index(addr: &str, ca: &PathBuf, identity: Option<&PathBuf>) -> Result<u16, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10));
    if let Some(pem) = identity {
        let pem = std::fs::read(pem).unwrap();
        builder = builder.identity(reqwest::Identity::from_pem(&pem).unwrap());
    }
    builder = builder
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca).unwrap()).unwrap());
    let client = builder.build().map_err(|e| e.to_string())?;
    let url = format!("https://{addr}/");
    match client.get(&url).send() {
        Ok(response) => Ok(response.status().as_u16()),
        Err(e) => Err(e.to_string()),
    }
}

/// The mTLS hub requests client certs and its route gate rejects a bare (cert-less) client.
#[test]
fn mtls_hub_requests_certs_and_route_gates_the_bare_client() {
    // PKI lives outside the hub scratch dirs so both hub phases share it.
    let pki_dir = scratch_dir("pki");
    let (ca, cert, key, client, serial) = write_pki(&pki_dir);
    let ca_s = ca.to_str().unwrap().to_string();
    let cert_s = cert.to_str().unwrap().to_string();
    let key_s = key.to_str().unwrap().to_string();

    // Phase 1: no deny-list — a cert-bearing client is served, and a
    // bare client completes the handshake (client auth is requested,
    // not required) but is gated off the A2A routes by the stamp gate
    // while the browser UI stays reachable.
    let dir = scratch_dir("mtls");
    let hub = spawn_hub(
        &[
            "--tls-cert",
            &cert_s,
            "--tls-key",
            &key_s,
            "--tls-ca",
            &ca_s,
        ],
        &dir,
        "hub.db",
    );
    assert_eq!(
        tls_card(&hub.addr, &ca, Some(&client)),
        Ok(200),
        "a CA-issued client certificate must be served"
    );
    // The TLS card declares the mTLS scheme (A2A v1.0 §4.5.6) — the
    // plaintext hub must not, and this hub must.
    let card_body = tls_card_body(&hub.addr, &ca, &client).expect("card body");
    assert!(
        card_body.contains("mtlsSecurityScheme"),
        "the TLS hub card must declare mTLS: {card_body}"
    );
    assert_eq!(
        tls_card(&hub.addr, &ca, None),
        Ok(403),
        "a bare client reaches the TLS listener but is stamped out of the A2A routes"
    );
    assert_eq!(
        tls_ui_index(&hub.addr, &ca, None),
        Ok(200),
        "a bare client is served the static UI page"
    );
    drop(hub);

    // Phase 2: the same identity with its serial denied must be dropped.
    let dir = scratch_dir("mtls-denied");
    let deny_path = dir.join("revoked.txt");
    std::fs::write(&deny_path, format!("{serial}\n")).unwrap();
    let hub = spawn_hub(
        &[
            "--tls-cert",
            &cert_s,
            "--tls-key",
            &key_s,
            "--tls-ca",
            &ca_s,
            "--revoke-list",
            deny_path.to_str().unwrap(),
        ],
        &dir,
        "hub.db",
    );
    assert!(
        tls_card(&hub.addr, &ca, Some(&client)).is_err(),
        "the deny-list must drop the listed identity"
    );
}

/// Revocation bites an ESTABLISHED connection: the deny gate is
/// per-request (H-11), so banning a peer denies its NEXT request over
/// the same pooled connection — not just fresh handshakes.
#[test]
fn revocation_denies_the_next_request_on_a_pooled_connection() {
    let pki_dir = scratch_dir("pki");
    // The serial is exercised implicitly: the final 403 proves the
    // deny gate matched THIS certificate's serial against the live
    // revocation set — not merely the peer id.
    let (ca, cert, key, client, _serial) = write_pki(&pki_dir);
    let ca_s = ca.to_str().unwrap().to_string();
    let cert_s = cert.to_str().unwrap().to_string();
    let key_s = key.to_str().unwrap().to_string();

    let dir = scratch_dir("pooled-revocation");
    // Seed the peer row BEFORE the hub starts (a card GET does not
    // enroll): the ban revokes by peer, and the first cert-bearing
    // request records the host-level serial the ban falls back to.
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = wrkspc_core::Store::open(&dir.join("hub.db"), "pooled-seed")
                .await
                .unwrap();
            // The peer enrolls ON the host the certificate presents
            // (the SAN), so the ban's host-level serial fallback can
            // find the serial the handshake recorded.
            store
                .ensure_peer_on_host("node-hubtest", "node-hubtest", "harness", "node-hubtest")
                .await
                .unwrap();
            store.release_lock().await.unwrap();
        });
    }
    let hub = spawn_hub(
        &[
            "--tls-cert",
            &cert_s,
            "--tls-key",
            &key_s,
            "--tls-ca",
            &ca_s,
        ],
        &dir,
        "hub.db",
    );

    // One client for BOTH requests: reqwest keeps the connection
    // pooled, so the second request reuses the first one's TCP/TLS
    // session.
    let pem = std::fs::read(&client).unwrap();
    let client_http = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .identity(reqwest::Identity::from_pem(&pem).unwrap())
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(&ca).unwrap()).unwrap())
        .build()
        .unwrap();
    let url = format!("https://{}/.well-known/agent-card.json", hub.addr);
    assert_eq!(
        client_http.get(&url).send().unwrap().status().as_u16(),
        200,
        "first presence is served"
    );

    // Ban the peer through the operator UI. The hub runs with the
    // pinned test token (spawn_hub sets WRKSPC_UI_TOKEN so the
    // platform secret store is never touched).
    let token = "wrkspc-test-token";
    let ban = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
        .patch(format!("https://{}/api/peers/node-hubtest", hub.addr))
        .bearer_auth(token)
        .json(&serde_json::json!({ "status": "banned" }))
        .send()
        .unwrap();
    // The response body carries the store error text on a 500; assert
    // with it so a flake names its own cause instead of just a status.
    let ban_status = ban.status();
    let ban_body = ban.text().unwrap_or_default();
    assert_eq!(
        ban_status.as_u16(),
        200,
        "ban must succeed: {} {}",
        ban_status,
        ban_body
    );

    // The NEXT request over the pooled connection is denied by the
    // per-request gate — the serial {serial} is in the live deny set.
    assert_eq!(
        client_http.get(&url).send().unwrap().status().as_u16(),
        403,
        "the pooled connection must be denied after the ban"
    );
}

/// Sends a bare (cert-less) GET to the A2A subtree with a forged
/// `x-wrkspc-peer` stamp header.
fn tls_forged_stamp(addr: &str, ca: &PathBuf, forged: &str) -> Result<u16, String> {
    let builder = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca).unwrap()).unwrap());
    let client = builder.build().map_err(|e| e.to_string())?;
    let url = format!("https://{addr}/roster");
    match client.get(&url).header("x-wrkspc-peer", forged).send() {
        Ok(response) => Ok(response.status().as_u16()),
        Err(e) => Err(e.to_string()),
    }
}

/// C-3: a client-supplied `x-wrkspc-peer` header never survives the
/// accept loop — a cert-less connection carrying a forged stamp is
/// refused on the A2A routes exactly like any bare client.
#[test]
fn forged_peer_stamp_is_cleared_for_certless_connections() {
    let pki_dir = scratch_dir("pki-forged");
    let (ca, cert, key, _client, _serial) = write_pki(&pki_dir);
    let ca_s = ca.to_str().unwrap().to_string();
    let cert_s = cert.to_str().unwrap().to_string();
    let key_s = key.to_str().unwrap().to_string();

    let dir = scratch_dir("mtls-forged");
    let hub = spawn_hub(
        &[
            "--tls-cert",
            &cert_s,
            "--tls-key",
            &key_s,
            "--tls-ca",
            &ca_s,
        ],
        &dir,
        "hub.db",
    );
    assert_eq!(
        tls_forged_stamp(&hub.addr, &ca, "node-hubtest"),
        Ok(403),
        "a forged stamp must not pass the stamp gate"
    );
    drop(hub);
}
