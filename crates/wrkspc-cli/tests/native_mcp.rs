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
