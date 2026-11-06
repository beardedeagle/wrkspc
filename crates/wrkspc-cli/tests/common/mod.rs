//! Shared harness for the Unit 9 recovery drills (Appendix D,
//! process-level): a real node binary over real stdio, a fake ACP
//! agent, seeded store, and blocking API helpers.
//!
//! The operator token is pinned via the `WRKSPC_UI_TOKEN` env
//! override so the platform secret store (macOS Keychain) is never
//! touched by tests. Scratch dirs survive when `WRKSPC_KEEP_DRILL`
//! is set.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A spawned wrkspc node, killed on drop.
pub struct NodeProc(Child);

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Keeps scratch dirs for post-mortem inspection when the env var is
/// set (drill debugging).
pub fn keep_scratch() -> bool {
    std::env::var_os("WRKSPC_KEEP_DRILL").is_some()
}

/// Removes the scratch dir unless drill debugging keeps it.
pub fn cleanup(dir: &Path) {
    if !keep_scratch() {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Unique scratch dir for one test run.
pub fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wrkspc-drill-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    if keep_scratch() {
        eprintln!("drill scratch kept: {}", dir.display());
    }
    dir
}

/// Locates the fake ACP agent binary, ensuring it is up to date (it is
/// a dependency crate's test-support bin, so `CARGO_BIN_EXE` cannot
/// name it from this package's tests and a stale copy would silently
/// miss new crash knobs). Cargo no-ops when the binary is fresh.
pub fn fake_agent_path() -> PathBuf {
    let status = Command::new("cargo")
        .args([
            "build",
            "--quiet",
            "-p",
            "wrkspc-acp",
            "--bin",
            "fake-acp-agent",
        ])
        .status()
        .expect("cargo build for the fake agent");
    assert!(status.success(), "fake agent build failed: {status}");
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target")
        });
    let profile = option_env!("PROFILE").unwrap_or("debug");
    target_dir.join(profile).join("fake-acp-agent")
}

/// Reserves an ephemeral loopback port for the node's UI listener.
pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Seeds the node database before first spawn: the operator peer, the
/// harness peer, and a conversation holding both (the node role cannot
/// create conversations over its UI; hub-mirrored ones pre-exist the
/// same way). Releases the store lock so the node can take it.
pub fn seed_store(db: &Path) -> String {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = wrkspc_core::Store::open(db, "drill-seed")
            .await
            .expect("seed open");
        store
            .ensure_peer("operator", "operator", "human")
            .await
            .expect("operator peer");
        store
            .ensure_peer("codex", "codex", "harness")
            .await
            .expect("codex peer");
        let id = store
            .create_conversation(&["operator", "codex"])
            .await
            .expect("conversation");
        store.release_lock().await.expect("lock released");
        id
    })
}

/// Spawns the node with the given binding command and waits for its UI
/// (token file + health endpoint). The node's stderr lands in
/// `dir/node.log` for post-mortem inspection.
pub fn spawn_node(dir: &Path, db: &Path, command: &str, port: u16) -> NodeProc {
    let bindings = dir.join("bindings.toml");
    let binding = format!(
        "[[binding]]\npeer_id = \"codex\"\nharness = \"codex\"\ncwd = \".\"\ncommand = {command:?}\n",
    );
    std::fs::write(&bindings, binding).unwrap();
    let log = std::fs::File::create(dir.join("node.log")).expect("node log");

    // The drill pins the operator token via the environment override so
    // the platform secret store (macOS Keychain) is never touched by
    // tests. The readiness marker is cleared first so a stale one from
    // an earlier instance can't satisfy the wait.
    let _ = std::fs::remove_file(format!("{}.ready", db.display()));
    let child = Command::new(env!("CARGO_BIN_EXE_wrkspc"))
        .args([
            "node",
            "run",
            "--db",
            db.to_str().unwrap(),
            "--bindings",
            bindings.to_str().unwrap(),
            "--ui",
            &format!("127.0.0.1:{port}"),
        ])
        // The drill pins the operator token via the environment
        // override so the platform secret store (macOS Keychain) is
        // never touched by tests.
        .env("WRKSPC_UI_TOKEN", DRILL_TOKEN)
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("node spawn");
    NodeProc(child)
}

/// The operator token every drill node runs with (env override).
pub const DRILL_TOKEN: &str = "drill-token";

/// Waits for the node's readiness marker (its startup side channel; the
/// marker carries no secret).
pub fn wait_ready(db: &Path) {
    let path = PathBuf::from(format!("{}.ready", db.display()));
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "node never wrote its readiness marker at {}",
        path.display()
    );
}

/// Blocking HTTP client for the node's UI API.
pub fn api_client(token: &str) -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(5))
        .default_headers(
            std::iter::once((
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            ))
            .collect(),
        )
        .build()
        .unwrap()
}

/// Waits until the UI health endpoint answers.
pub fn wait_ui(client: &reqwest::blocking::Client, port: u16) {
    let url = format!("http://127.0.0.1:{port}/api/health");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if client.get(&url).send().is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "UI never became healthy");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Posts an operator message and returns the accepted message id.
pub fn post(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
    content: &str,
) -> String {
    let url = format!("http://127.0.0.1:{port}/api/conversations/{conversation}/messages");
    let response = client
        .post(&url)
        .json(&serde_json::json!({ "content": content }))
        .send()
        .expect("post");
    assert_eq!(
        response.status(),
        200,
        "post failed: {}",
        response.text().unwrap()
    );
    response.json::<serde_json::Value>().unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Reads the transcript messages view.
pub fn messages(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
) -> Vec<serde_json::Value> {
    let url = format!("http://127.0.0.1:{port}/api/conversations/{conversation}/messages");
    client
        .get(&url)
        .send()
        .expect("messages fetch")
        .json()
        .expect("messages json")
}

/// Polls until the given message's delivery to `codex` reaches `state`.
pub fn wait_delivery(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
    message_id: &str,
    state: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for message in messages(client, port, conversation) {
            if message["id"].as_str() != Some(message_id) {
                continue;
            }
            for delivery in message["deliveries"].as_array().unwrap() {
                if delivery["peer_id"].as_str() == Some("codex")
                    && delivery["state"].as_str() == Some(state)
                {
                    return;
                }
            }
        }
        assert!(Instant::now() < deadline, "delivery never reached {state}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Boots a full drill: seeded store + node + healthy UI.
pub fn boot(dir: &Path, db: &Path, command: &str) -> (NodeProc, u16, String) {
    let port = free_port();
    let node = spawn_node(dir, db, command, port);
    wait_ready(db);
    let token = DRILL_TOKEN.to_string();
    let client = api_client(&token);
    wait_ui(&client, port);
    (node, port, token)
}
