//! Team-mode supervision acceptance: a node whose binding's harness is
//! dead must stay up — sessions spawn on demand, so delivery receipt,
//! the UI, and the dispatcher keep running while execution attempts for
//! that peer degrade into visible unknown state.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A spawned wrkspc node, killed on drop.
struct NodeProc(Child);

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Unique scratch dir for one test run.
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wrkspc-resilience-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A harness that dies before the ACP handshake does not take the node down (regression: 'session task is not running').
#[test]
fn node_survives_a_dead_harness_binding() {
    let dir = scratch_dir("dead-binding");
    let db = dir.join("node.db");
    let bindings = dir.join("bindings.toml");
    // `true` exits without ever answering the ACP handshake: the exact
    // shape of a dead harness. Previously this killed the node at
    // startup ("session task is not running").
    std::fs::write(
        &bindings,
        "[[binding]]\npeer_id = \"codex\"\nharness = \"codex\"\ncwd = \".\"\ncommand = \"true\"\n",
    )
    .unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_wrkspc"))
        .args([
            "node",
            "run",
            "--db",
            db.to_str().unwrap(),
            "--bindings",
            bindings.to_str().unwrap(),
            "--ui",
            "127.0.0.1:0",
        ])
        // Pin the operator token via the environment override so the
        // platform secret store (macOS Keychain) is never touched by
        // tests.
        .env("WRKSPC_UI_TOKEN", "wrkspc-test-token")
        .current_dir(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("node spawn");
    let mut node = NodeProc(child);

    // The node must stay alive across several dispatcher ticks.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(status)) = node.0.try_wait() {
            panic!("node exited with {status} before the observation window");
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
}
