//! Restart-focused recovery drills: delivery replay, harness death,
//! and node kill mid-turn (checks 4, 9, 11, 6, 14).

mod common;
use common::*;
use std::path::Path;
use std::time::{Duration, Instant};

/// Reads the node's captured stderr.
pub fn node_log(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("node.log")).unwrap_or_default()
}

/// Reads the unknown-attempts view.
pub fn unknown_attempts(client: &reqwest::blocking::Client, port: u16) -> Vec<serde_json::Value> {
    let url = format!("http://127.0.0.1:{port}/api/attempts/unknown");
    client
        .get(&url)
        .send()
        .expect("unknown fetch")
        .json()
        .expect("unknown json")
}

/// A delivered message survives a node kill without replay; a new delivery resumes cleanly.
#[test]
fn node_restart_replays_nothing_and_resumes_delivery() {
    let dir = scratch_dir("restart");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let command = fake_agent_path();

    let (node, port, token) = boot(&dir, &db, command.to_str().unwrap());
    let client = api_client(&token);

    let first = post(&client, port, &conversation, "first");
    wait_delivery(&client, port, &conversation, &first, "delivered");

    // Kill the node after the full delivery lifecycle committed, then
    // restart it against the same database.
    drop(node);
    let (node, port, token) = boot(&dir, &db, command.to_str().unwrap());
    let client = api_client(&token);

    let second = post(&client, port, &conversation, "second");
    wait_delivery(&client, port, &conversation, &second, "delivered");

    // Exactly two operator messages, each delivered exactly once, and
    // no attempt was re-claimed or left undeterminable by the restart.
    let transcript = messages(&client, port, &conversation);
    assert_eq!(transcript.len(), 2, "transcript: {transcript:#?}");
    for message in &transcript {
        let deliveries = message["deliveries"].as_array().unwrap();
        assert_eq!(deliveries.len(), 1, "one recipient: {message:#?}");
        assert_eq!(
            deliveries[0]["state"], "delivered",
            "delivered once: {message:#?}"
        );
    }
    assert!(
        unknown_attempts(&client, port).is_empty(),
        "restart must not reconcile any attempt as unknown"
    );
    drop(node);
    cleanup(&dir);
}

/// A harness crash mid-turn surfaces as an unknown attempt; the next turn recovers.
#[test]
fn harness_death_marks_the_attempt_unknown_and_recovers() {
    let dir = scratch_dir("harness-death");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    // Any prompt containing "die" kills the fake harness before it
    // responds — a crash before any ACP write.
    let command = format!("{} --die-on die", fake_agent_path().display());

    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    let lost = post(&client, port, &conversation, "please die now");
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
        std::thread::sleep(Duration::from_millis(100));
    }
    let attempt = found.unwrap_or_else(|| {
        panic!(
            "the lost turn must surface as an unknown attempt; node log:\n{}",
            node_log(&dir)
        )
    });
    assert_eq!(attempt["recipient_id"], "codex");

    // The node stayed alive; the next turn spawns a fresh session and
    // completes.
    let next = post(&client, port, &conversation, "again");
    wait_delivery(&client, port, &conversation, &next, "delivered");
    drop(node);
    cleanup(&dir);
}

/// Killing the node mid-turn reconciles the attempt as unknown on restart; a fresh turn completes.
#[test]
fn node_kill_mid_turn_reconciles_unknown_on_restart() {
    let dir = scratch_dir("node-kill");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let touch = dir.join("prompted");
    let fake = fake_agent_path();
    // "never" holds the turn open until cancel; `--touch` proves the
    // harness received the prompt before the node dies.
    let command = format!("{} --touch {}", fake.display(), touch.display());

    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    let in_flight = post(&client, port, &conversation, "never");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !touch.exists() {
        assert!(
            Instant::now() < deadline,
            "the harness never received the held prompt; node log:\n{}",
            node_log(&dir)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // Kill the node while the turn is held open, then restart: the
    // attempt must be reconciled as unknown, never silently re-run.
    drop(node);
    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut found = None;
    while Instant::now() < deadline {
        let attempts = unknown_attempts(&client, port);
        found = attempts
            .iter()
            .find(|attempt| attempt["message_id"].as_str() == Some(in_flight.as_str()))
            .cloned();
        if found.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        found.is_some(),
        "the in-flight attempt must reconcile as unknown after restart"
    );

    // Delivery of that message stays unresolved (visible, operator-owned),
    // and a fresh turn still completes.
    let next = post(&client, port, &conversation, "next");
    wait_delivery(&client, port, &conversation, &next, "delivered");
    drop(node);
    cleanup(&dir);
}
