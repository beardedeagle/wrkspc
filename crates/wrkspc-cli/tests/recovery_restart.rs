//! Restart-focused recovery drills: delivery replay, harness death,
//! and node kill mid-turn (checks 4, 9, 11, 6, 14).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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
    // The fake logs every prompt it receives: `delivered` alone is a
    // handoff receipt, so the drill ALSO proves each delivery actually
    // reached the harness (receipt + a real turn, not one or the other).
    let prompts_log = dir.join("prompts.log");
    let command = format!(
        "{} --log-prompts {}",
        fake_agent_path().display(),
        prompts_log.display()
    );

    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    let first = post(&client, port, &conversation, "first");
    wait_delivery(&client, port, &conversation, &first, "delivered");
    assert_prompt_count(&prompts_log, 1, "the first delivery's turn ran");
    // The attempt must be TERMINAL before the kill: delivered is a
    // handoff receipt, and killing while the turn is still open would
    // (correctly) reconcile it unknown on restart — the drill's point
    // is a clean kill AFTER completion.
    wait_attempt_terminal(&client, port, &conversation, 1);

    // Kill the node after the full delivery lifecycle committed, then
    // restart it against the same database.
    drop(node);
    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    let second = post(&client, port, &conversation, "second");
    wait_delivery(&client, port, &conversation, &second, "delivered");
    assert_prompt_count(&prompts_log, 2, "the second delivery's turn ran");
    wait_attempt_terminal(&client, port, &conversation, 2);

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

/// Waits until exactly `expected` attempts reached a terminal state
/// (their `dispatch.stopped` feed events), so a clean kill is possible
/// without reconciling an open attempt as unknown.
fn wait_attempt_terminal(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
    expected: usize,
) {
    let url = format!("http://127.0.0.1:{port}/api/conversations/{conversation}/events");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let events: Vec<serde_json::Value> = client
            .get(&url)
            .send()
            .expect("events fetch")
            .json()
            .expect("events json");
        let stopped = events
            .iter()
            .filter(|event| event["kind"].as_str() == Some("dispatch.stopped"))
            .count();
        if stopped == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} terminal attempts, saw {stopped}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Asserts the fake harness received exactly `expected` prompts (its
/// --log-prompts side channel, one line per prompt receipt).
fn assert_prompt_count(path: &Path, expected: usize, why: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let lines = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .count();
        if lines == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{why}: expected {expected} prompts, saw {lines}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A harness crash mid-turn surfaces as an unknown attempt; new work for
/// that recipient is blocked until the operator reconciles the unknown.
///
/// This is the H-6 contract: an undeterminable outcome quarantines the
/// recipient session-wide (the same recipient's queued work must not
/// run until a human reconciles the record), not merely the one intent.
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

    // The node stayed alive. New work for the same recipient is
    // BLOCKED while the unknown is unresolved: the intent queues but
    // no new attempt runs, so its delivery never reaches dispatched
    // state and no second unknown appears.
    let next = post(&client, port, &conversation, "again");
    std::thread::sleep(Duration::from_secs(3));
    let queued = messages(&client, port, &conversation)
        .into_iter()
        .find(|message| message["id"].as_str() == Some(next.as_str()))
        .expect("the queued delivery is visible in the transcript");
    let delivery = queued["deliveries"]
        .as_array()
        .and_then(|deliveries| deliveries.first())
        .expect("one recipient");
    assert_ne!(
        delivery["state"], "delivered",
        "H-6: the recipient is quarantined, so the queued turn must not \
         dispatch until the unknown is reconciled; delivery: {delivery:#?}"
    );
    assert_eq!(
        unknown_attempts(&client, port).len(),
        1,
        "exactly one unknown attempt: the lost turn"
    );

    // Reconciling the lost turn (operator closes it as failed) releases
    // the quarantine; the queued turn then dispatches to a fresh session
    // rather than re-running the lost turn.
    let lost_id = attempt["attempt_id"].as_str().expect("attempt id");
    let status = client
        .post(format!(
            "http://127.0.0.1:{port}/api/attempts/{lost_id}/reconcile"
        ))
        .json(&serde_json::json!({ "decision": "close", "state": "failed" }))
        .send()
        .expect("reconcile")
        .status();
    assert!(status.is_success(), "reconcile must succeed: {status}");
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
    let attempt = found.expect("checked above");

    // Delivery of that message stays unresolved (visible, operator-owned).
    // A fresh turn for the SAME recipient is blocked while the unknown is
    // unresolved (H-6): it queues but must not dispatch.
    let next = post(&client, port, &conversation, "next");
    std::thread::sleep(Duration::from_secs(3));
    let queued = messages(&client, port, &conversation)
        .into_iter()
        .find(|message| message["id"].as_str() == Some(next.as_str()))
        .expect("the queued delivery is visible in the transcript");
    let delivery = queued["deliveries"]
        .as_array()
        .and_then(|deliveries| deliveries.first())
        .expect("one recipient");
    assert_ne!(
        delivery["state"], "delivered",
        "H-6: the recipient is quarantined, so the queued turn must not \
         dispatch until the unknown is reconciled; delivery: {delivery:#?}"
    );

    // Operator closes the undeterminable turn; only then does the queued
    // turn dispatch.
    let lost_id = attempt["attempt_id"].as_str().expect("attempt id");
    let status = client
        .post(format!(
            "http://127.0.0.1:{port}/api/attempts/{lost_id}/reconcile"
        ))
        .json(&serde_json::json!({ "decision": "close", "state": "failed" }))
        .send()
        .expect("reconcile")
        .status();
    assert!(status.is_success(), "reconcile must succeed: {status}");
    wait_delivery(&client, port, &conversation, &next, "delivered");
    drop(node);
    cleanup(&dir);
}
