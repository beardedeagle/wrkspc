//! Native-MCP registration failure: a peer whose `mcp_http` binding
//! fails its harness-side registration (`agy mcp add` exiting nonzero)
//! must surface as a visible failure — never a session that looks
//! healthy with no usable wrkspc tools (E10).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// A fake `agy` that exits nonzero: the registration must FAIL, and
/// the failure must be visible, not swallowed.
fn write_fake_agy(dir: &Path) {
    let agy = dir.join("agy");
    std::fs::write(&agy, "#!/bin/sh\nexit 7\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&agy, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// A fake `agy` that records the registration URL (its final
/// positional argument) and exits 0: the registration succeeds, so the
/// listener task is spawned and the setup parks on the ACP handshake
/// (the binding's silent command never completes one).
fn write_recording_agy(dir: &Path) {
    let agy = dir.join("agy");
    std::fs::write(
        &agy,
        "#!/bin/sh\nfor last; do :; done\necho \"$last\" > agy-record.txt\nexit 0\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&agy, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// The MCP HTTP listener must be cancellation-safe: the dispatcher
/// deadline-bounds session lookup and DROPS the pending setup future
/// on timeout, and that cancellation must close the already-bound
/// listener port while the node keeps running (E10).
#[test]
fn canceled_mcp_http_setup_closes_the_bound_listener() {
    let dir = scratch_dir("mcp-cancel");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    write_recording_agy(&dir);
    let port = free_port();

    // The binding: a silent adapter (`sleep` never completes the ACP
    // handshake, so session/new parks) whose tools arrive via native
    // HTTP registration. The listener is bound and its task spawned
    // before that handshake await.
    let bindings = dir.join("bindings.toml");
    std::fs::write(
        &bindings,
        "[[binding]]\npeer_id = \"codex\"\nharness = \"codex\"\ncwd = \".\"\ncommand = \"sleep 60\"\nmcp_http = true\n",
    )
    .unwrap();
    let log = std::fs::File::create(dir.join("node.log")).expect("node log");
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
        .env("WRKSPC_UI_TOKEN", DRILL_TOKEN)
        // The per-peer MCP token is pinned like the UI token so the
        // mcp_http path never writes real Keychain entries.
        .env("WRKSPC_MCP_TOKEN", "wrkspc-test-mcp-token")
        // The fake agy wins over any real one on PATH.
        .env(
            "PATH",
            format!(
                "{}:{}",
                dir.display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .current_dir(&dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log))
        .spawn()
        .expect("node spawn");
    let node = NodeProc(child);

    wait_ready(&db);
    let client = api_client(DRILL_TOKEN);
    wait_ui(&client, port);

    // Deliver work: the lookup starts setup, the fake agy records the
    // bound URL, and the handshake parks.
    let lost = post(&client, port, &conversation, "setup will be canceled");
    let record = dir.join("agy-record.txt");
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && !record.exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        record.exists(),
        "agy never recorded the registration URL; node log:\n{}",
        node_log(&dir)
    );
    let url = std::fs::read_to_string(&record).expect("record read");
    let url = url.trim();
    let listener_port: u16 = url
        .trim_start_matches("http://127.0.0.1:")
        .trim_end_matches("/mcp")
        .parse()
        .expect("listener port");
    // While setup is pending, the bound listener accepts connections.
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", listener_port)).is_ok(),
        "the listener must be bound during pending setup; node log:\n{}",
        node_log(&dir)
    );

    // The dispatcher's lookup deadline (45s default) cancels the
    // pending setup and the attempt surfaces as unknown.
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut found = false;
    while Instant::now() < deadline {
        let attempts: Vec<serde_json::Value> = client
            .get(format!("http://127.0.0.1:{port}/api/attempts/unknown"))
            .send()
            .expect("unknown fetch")
            .json()
            .expect("unknown json");
        if attempts
            .iter()
            .any(|attempt| attempt["message_id"].as_str() == Some(lost.as_str()))
        {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        found,
        "the canceled lookup must surface as an unknown attempt; node log:\n{}",
        node_log(&dir)
    );

    // Cancellation must have closed the listener: connects now refuse.
    // Poll briefly — the abort is scheduled as the timeout drops the
    // future, a moment before the unknown attempt lands.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", listener_port)).is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the listener outlived the canceled setup; node log:\n{}",
            node_log(&dir)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // The node itself stays healthy throughout.
    let health = client
        .get(format!("http://127.0.0.1:{port}/api/health"))
        .send()
        .expect("health");
    assert!(health.status().is_success(), "the node must stay up");
    drop(node);
    cleanup(&dir);
}

/// Reads the node's captured stderr.
fn node_log(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("node.log")).unwrap_or_default()
}

#[test]
fn failed_native_mcp_registration_surfaces_unknown_and_keeps_the_node_up() {
    let dir = scratch_dir("mcp-registration");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    write_fake_agy(&dir);
    let port = free_port();

    // The binding: a working ACP harness (the fake agent) whose tool
    // registration goes through the harness-native HTTP path
    // (mcp_http) — that path runs `agy mcp add`, and the fake agy
    // exits 7.
    let bindings = dir.join("bindings.toml");
    std::fs::write(
        &bindings,
        format!(
            "[[binding]]\npeer_id = \"codex\"\nharness = \"codex\"\ncwd = \".\"\ncommand = \"{}\"\nmcp_http = true\n",
            fake_agent_path().display()
        ),
    )
    .unwrap();
    let log = std::fs::File::create(dir.join("node.log")).expect("node log");
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
        .env("WRKSPC_UI_TOKEN", DRILL_TOKEN)
        // The per-peer MCP token is pinned like the UI token so the
        // mcp_http path never writes real Keychain entries.
        .env("WRKSPC_MCP_TOKEN", "wrkspc-test-mcp-token")
        // The fake agy wins over any real one on PATH.
        .env(
            "PATH",
            format!(
                "{}:{}",
                dir.display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .current_dir(&dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log))
        .spawn()
        .expect("node spawn");
    let node = NodeProc(child);

    wait_ready(&db);
    let client = api_client(DRILL_TOKEN);
    wait_ui(&client, port);

    // Deliver work: the registration fails, the binding resolves to
    // nothing, and the attempt surfaces as unknown — while the node
    // itself stays alive and serving.
    let lost = post(&client, port, &conversation, "mcp registration will fail");
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut found = false;
    while Instant::now() < deadline {
        let attempts: Vec<serde_json::Value> = client
            .get(format!("http://127.0.0.1:{port}/api/attempts/unknown"))
            .send()
            .expect("unknown fetch")
            .json()
            .expect("unknown json");
        if attempts
            .iter()
            .any(|attempt| attempt["message_id"].as_str() == Some(lost.as_str()))
        {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        found,
        "the failed registration must surface as an unknown attempt; node log:\n{}",
        node_log(&dir)
    );
    let health = client
        .get(format!("http://127.0.0.1:{port}/api/health"))
        .send()
        .expect("health");
    assert!(health.status().is_success(), "the node must stay up");
    drop(node);
    cleanup(&dir);
}
