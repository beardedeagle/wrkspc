//! Team-mode supervision acceptance: a node whose binding's harness is
//! dead must stay up AND degrade execution visibly — sessions spawn on
//! demand, so delivery receipt, the UI, and the dispatcher keep running
//! while execution attempts for that peer surface as unknown state.
//!
//! The drills DELIVER REAL WORK against the dead or hung harness:
//! staying alive for a few seconds proves nothing about degraded
//! execution by itself, so each test posts a message and requires the
//! attempt to surface as unknown.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;
use std::time::{Duration, Instant};

/// Reads the unknown-attempts view.
fn unknown_attempts(client: &reqwest::blocking::Client, port: u16) -> Vec<serde_json::Value> {
    let url = format!("http://127.0.0.1:{port}/api/attempts/unknown");
    client
        .get(&url)
        .send()
        .expect("unknown fetch")
        .json()
        .expect("unknown json")
}

/// A harness that dies before the ACP handshake does not take the node
/// down (regression: 'session task is not running'), and the delivery
/// for that peer degrades into a VISIBLE unknown attempt instead of
/// stalling silently — delivery receipt, the UI, and the dispatcher
/// keep working around it.
#[test]
fn node_survives_a_dead_harness_and_surfaces_the_attempt_unknown() {
    let dir = scratch_dir("dead-binding");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let port = free_port();
    // `true` exits without ever answering the ACP handshake: the exact
    // shape of a dead harness. Previously this killed the node at
    // startup ("session task is not running").
    let node = spawn_node(&dir, &db, "true", port);
    wait_ready(&db);
    let client = api_client(DRILL_TOKEN);
    wait_ui(&client, port);

    // Deliver REAL work: the operator posts, the dispatcher claims,
    // the dead harness never answers, and the attempt must surface as
    // unknown — not vanish and not kill the node.
    let lost = post(&client, port, &conversation, "deliver to a dead harness");
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut found = None;
    while Instant::now() < deadline {
        let attempts = unknown_attempts(&client, port);
        found = attempts
            .iter()
            .find(|attempt| attempt["message_id"].as_str() == Some(lost.as_str()))
            .cloned();
        if found.is_some() {
            break;
        }
        // The node must stay alive throughout.
        let _ = &node;
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        found.is_some(),
        "the dead harness's attempt must surface as unknown; node log:\n{}",
        node_log(&dir)
    );
    // And the node is STILL alive after the degraded execution path:
    // receipt + UI keep serving around the dead binding.
    let transcript = messages(&client, port, &conversation);
    assert_eq!(transcript.len(), 1, "the posted message is visible");
    drop(node);
    cleanup(&dir);
}

/// A harness that HANGS at the ACP handshake (never answers, never
/// exits) must not wedge node startup or the dispatcher: readiness is
/// written before any binding is driven (E12), the UI serves, and real
/// work delivered to the hung binding degrades into a VISIBLE unknown
/// attempt once the claim task's own lookup deadline fires — the node
/// stays up around it. Staying alive for a few seconds alone proves
/// nothing: the drill posts work and requires the attempt to surface.
#[test]
fn node_boots_despite_a_hung_harness_binding() {
    let dir = scratch_dir("hung-binding");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let port = free_port();
    // `sleep 60` answers nothing and exits late — the hung-adapter
    // shape (the opposite of the dead harness: it stays alive).
    let node = spawn_node(&dir, &db, "sleep 60", port);
    wait_ready(&db);
    let client = api_client(DRILL_TOKEN);
    wait_ui(&client, port);

    // Deliver REAL work against the hung binding: the claim task's
    // session lookup hangs on the ACP handshake, but every claim task
    // carries its own deadline (45s), so the hung adapter degrades
    // into a VISIBLE unknown attempt instead of a silent stall — and
    // the node keeps serving the whole time.
    let lost = post(&client, port, &conversation, "deliver to a hung harness");
    let deadline = Instant::now() + Duration::from_secs(55);
    let mut found = None;
    while Instant::now() < deadline {
        let attempts = unknown_attempts(&client, port);
        found = attempts
            .iter()
            .find(|attempt| attempt["message_id"].as_str() == Some(lost.as_str()))
            .cloned();
        if found.is_some() {
            break;
        }
        // The node must stay alive throughout.
        let _ = &node;
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        found.is_some(),
        "the hung harness's attempt must surface as unknown; node log:\n{}",
        node_log(&dir)
    );
    let reason = found
        .as_ref()
        .and_then(|attempt| attempt["stop_reason"].as_str());
    assert!(
        reason.is_some_and(|reason| reason.contains("nonresponsive")),
        "the unknown attempt must carry the hung-lookup reason, got: {reason:?}"
    );
    // And the node is STILL alive after the degraded execution path:
    // receipt + UI keep serving around the hung binding.
    let transcript = messages(&client, port, &conversation);
    assert_eq!(transcript.len(), 1, "the posted message is visible");
    let health = client
        .get(format!("http://127.0.0.1:{port}/api/health"))
        .send()
        .expect("health check");
    assert!(health.status().is_success(), "UI must stay healthy");
    drop(node);
    cleanup(&dir);
}

/// Reads the node's captured stderr.
fn node_log(dir: &std::path::Path) -> String {
    std::fs::read_to_string(dir.join("node.log")).unwrap_or_default()
}
