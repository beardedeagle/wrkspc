//! Loopback rehearsal of the D-6 A2 delivery loop: a real `wrkspc hub
//! run` process (mTLS) plus the node's real poll client (rustls
//! identity via `build_hub_client`) driving `run_poll_loop` against a
//! local store — poll → mirror → ack over actual TLS between two
//! processes, the exact shape of the cross-machine bar minus the
//! network.
//!
//! Execution hops (dispatcher → harness) are deliberately not driven
//! here (`execution_requested: false`); they are covered by the pilot
//! dispatcher tests. The node binary's CLI arg wiring is covered by the
//! unit tests; driving the binary itself would additionally require an
//! ACP-speaking child process for the bindings, which is not this
//! test's seam.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};

/// A spawned wrkspc hub child, killed on drop.
struct Hub {
    child: Child,
}

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Unique scratch dir for one test run.
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wrkspc-loop-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes a test PKI: CA, hub server cert/key (loopback SANs), and a
/// node client cert whose SAN is the node's host id. Returns
/// (ca, hub cert, hub key, node cert, node key).
fn write_pki(dir: &Path, node_host_id: &str) -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "wrkspc-loop-ca");
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "hub");
    let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

    let node_key = KeyPair::generate().unwrap();
    let mut node_params = CertificateParams::new(vec![node_host_id.to_string()]).unwrap();
    node_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, node_host_id);
    let node = node_params.signed_by(&node_key, &ca, &ca_key).unwrap();

    let ca_path = dir.join("ca.pem");
    let hub_cert = dir.join("hub.crt");
    let hub_key = dir.join("hub.key");
    let node_cert = dir.join("node.crt");
    let node_key_path = dir.join("node.key");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    std::fs::write(&hub_cert, server.pem()).unwrap();
    std::fs::write(&hub_key, server_key.serialize_pem()).unwrap();
    std::fs::write(&node_cert, node.pem()).unwrap();
    std::fs::write(&node_key_path, node_key.serialize_pem()).unwrap();
    (ca_path, hub_cert, hub_key, node_cert, node_key_path)
}

/// The real-world PEM shape that broke the node's client build: cert
/// files without a trailing newline (Vault/openssl artifacts) glued to
/// the key's first line when concatenated. build_hub_client must join
/// on a newline boundary, not raw bytes.
#[test]
fn hub_client_accepts_cert_without_trailing_newline() {
    let dir = std::env::temp_dir().join(format!("wrkspc-pem-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca, _hub_cert, _hub_key, node_cert, node_key) = write_pki(&dir, "node-test-1");
    let mut cert_pem = std::fs::read(&node_cert).unwrap();
    while cert_pem.ends_with(b"\n") {
        cert_pem.pop();
    }
    std::fs::write(&node_cert, cert_pem).unwrap();
    wrkspc_cli::poll::build_hub_client(&ca, &node_cert, &node_key)
        .expect("client must build with a newline-less cert file");
}

/// The poll loop's failure path depends on its requests erroring: a
/// peer that completes TCP but never answers TLS (the live-run wedge:
/// established connection, no ClientHello, no response) must surface
/// as a bounded error, not a silent forever-stall. The client's total
/// request timeout is what turns the wedge into a warn + retry.
#[tokio::test]
async fn hub_client_request_times_out_on_stalled_handshake() {
    let dir = scratch_dir("stall");
    let (ca, _hub_cert, _hub_key, node_cert, node_key) = write_pki(&dir, "node-stall-1");

    // A listener that accepts the TCP connection and then holds it,
    // sending no TLS bytes.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let _held = listener.accept().map(|(stream, _)| stream);
        std::thread::sleep(Duration::from_secs(30));
    });

    let client = wrkspc_cli::poll::build_hub_client_with_timeout(
        &ca,
        &node_cert,
        &node_key,
        Duration::from_millis(500),
    )
    .expect("client must build");

    let started = Instant::now();
    let result = client
        .get(format!("https://{addr}/"))
        .send()
        .await
        .map(|r| r.status().as_u16());
    let elapsed = started.elapsed();

    assert!(
        result.is_err(),
        "a stalled handshake must surface as an error"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the timeout must bound the call (took {elapsed:?})"
    );
}

/// Seeds the hub database: operator (hub host), codex (on the node's
/// host), their conversation, a committed message, and one delivery
/// task in the node's queue. Returns the delivery task id and the
/// conversation id.
async fn seed_hub_db(db: &Path) -> (String, String) {
    let store = wrkspc_core::Store::open(db, "loop-seed").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    let message_id = match store
        .accept_message(
            &conversation,
            "operator",
            "hub-msg-1",
            "hello from the hub",
            false,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: false,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id }
        | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
    };
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-test-1",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &message_id,
            conversation_id: &conversation,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "hello from the hub",
            execution_requested: false,
        })
        .await
        .unwrap();
    // Hand the database to the hub process: release the exclusive
    // dispatcher lock the seed acquired.
    store.release_lock().await.unwrap();
    (task_id, conversation)
}

/// Spawns the hub binary on the seeded DB and waits for its
/// `--a2a-addr-file` side-channel.
fn spawn_hub(dir: &Path, hub_db: &str, ca: &Path, cert: &Path, key: &Path) -> (Hub, String) {
    let addr_file = dir.join("a2a-addr");
    let child = Command::new(env!("CARGO_BIN_EXE_wrkspc"))
        // Pin the operator token via the environment override so the
        // platform secret store (macOS Keychain) is never touched by
        // tests.
        .env("WRKSPC_UI_TOKEN", "wrkspc-test-token")
        .args([
            "hub",
            "run",
            "--db",
            hub_db,
            "--a2a",
            "127.0.0.1:0",
            "--a2a-addr-file",
            addr_file.to_str().unwrap(),
            "--tls-ca",
            ca.to_str().unwrap(),
            "--tls-cert",
            cert.to_str().unwrap(),
            "--tls-key",
            key.to_str().unwrap(),
        ])
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("hub spawn");

    let hub = Hub { child };
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(content) = std::fs::read_to_string(&addr_file) {
            let trimmed = content.trim().to_string();
            if !trimmed.is_empty() {
                return (hub, trimmed);
            }
        }
        assert!(
            Instant::now() < deadline,
            "hub never wrote its bound address"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// End to end over mTLS: the node polls the hub, mirrors the delivery, and acks it back.
#[test]
fn node_poll_mirrors_and_acks_the_hub_over_mtls() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let dir = scratch_dir("poll");
    let (ca, hub_cert, hub_key, node_cert, node_key) = write_pki(&dir, "node-test-1");
    let hub_db = dir.join("hub.db");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (task_id, conversation_id) = runtime.block_on(seed_hub_db(&hub_db));

    let (hub, addr) = spawn_hub(&dir, hub_db.to_str().unwrap(), &ca, &hub_cert, &hub_key);
    let hub_url = format!("https://{addr}");

    // The node's real client identity + poll loop against a local store.
    runtime.block_on(async move {
        let client = wrkspc_cli::poll::build_hub_client(&ca, &node_cert, &node_key).unwrap();
        let node_store = wrkspc_core::Store::open_in_memory("loop-node")
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let poll = tokio::spawn(wrkspc_cli::poll::run_poll_loop(
            wrkspc_cli::poll::PollLoopConfig {
                client,
                hub_url: hub_url.clone(),
                store: node_store.clone(),
                host_id: "node-test-1".to_string(),
                instance_id: "loop-instance".to_string(),
                interval: Duration::from_millis(200),
                presence: None,
            },
            shutdown_rx,
        ));

        // The hub-side fence: the delivery task goes terminal-completed.
        let hub_db = hub_db.clone();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let store = wrkspc_core::Store::open_shared(&hub_db, "loop-check")
                .await
                .unwrap();
            let acked = store
                .task_get(&task_id)
                .await
                .unwrap()
                .is_some_and(|task| task.terminal && task.state == "completed");
            if acked {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "hub never received the delivery ack"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let _ = shutdown_tx.send(true);
        let _ = poll.await;

        // The node-side mirror: same task id marked locally, hub ids
        // preserved, exactly one local record.
        let mirrored = node_store
            .task_get(&task_id)
            .await
            .unwrap()
            .is_some_and(|task| task.terminal);
        let members = node_store
            .conversation_members(&conversation_id)
            .await
            .unwrap();
        let events = node_store
            .events_since(&conversation_id, 0, 100)
            .await
            .unwrap();
        let accepted = events
            .iter()
            .filter(|event| event.kind == "message.accepted")
            .count();
        assert!(mirrored, "node never recorded the processed marker");
        assert!(
            members.contains(&"operator".to_string()) && members.contains(&"codex".to_string()),
            "node mirror missing conversation members: {members:?}"
        );
        assert_eq!(accepted, 1, "node mirror recorded {accepted} messages");
    });

    drop(hub);
    let _ = std::fs::remove_dir_all(&dir);
}
