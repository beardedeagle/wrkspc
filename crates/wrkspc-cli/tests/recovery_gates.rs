//! Moderation-gate recovery drills: serialization, cancel, held
//! durability, and the mid-flight fence race (checks 3, 6, 7, 13).

mod common;
use common::*;
use std::time::{Duration, Instant};

/// Reads the conversation event feed.
fn events(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
) -> Vec<serde_json::Value> {
    let url = format!("http://127.0.0.1:{port}/api/conversations/{conversation}/events?limit=500");
    client
        .get(&url)
        .send()
        .expect("events fetch")
        .json()
        .expect("events json")
}

/// Posts an operator hold on a message delivery (204 = accepted).
fn hold(client: &reqwest::blocking::Client, port: u16, message_id: &str, peer: &str) {
    let url = format!("http://127.0.0.1:{port}/api/messages/{message_id}/deliveries/{peer}/hold");
    let response = client.post(&url).send().expect("hold");
    assert_eq!(
        response.status(),
        204,
        "hold failed: {}",
        response.text().unwrap()
    );
}

/// Posts an operator release on a held delivery (204 = accepted).
fn release(client: &reqwest::blocking::Client, port: u16, message_id: &str, peer: &str) {
    let url =
        format!("http://127.0.0.1:{port}/api/messages/{message_id}/deliveries/{peer}/release");
    let response = client.post(&url).send().expect("release");
    assert_eq!(
        response.status(),
        204,
        "release failed: {}",
        response.text().unwrap()
    );
}

/// Creates a conversation fence and returns its fence id.
fn fence(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
    peer: &str,
    kind: &str,
) -> String {
    let url = format!("http://127.0.0.1:{port}/api/conversations/{conversation}/fences");
    let response = client
        .post(&url)
        .json(&serde_json::json!({ "peer_id": peer, "kind": kind }))
        .send()
        .expect("fence");
    assert_eq!(
        response.status(),
        200,
        "fence failed: {}",
        response.text().unwrap()
    );
    response.json::<serde_json::Value>().unwrap()["fence_id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Clears a conversation fence.
fn clear_fence(client: &reqwest::blocking::Client, port: u16, fence_id: &str) {
    let url = format!("http://127.0.0.1:{port}/api/fences/{fence_id}");
    let response = client.delete(&url).send().expect("fence clear");
    assert_eq!(
        response.status(),
        204,
        "fence clear failed: {}",
        response.text().unwrap()
    );
}

/// Cancels a message delivery through the operator cancel seam.
fn cancel(
    client: &reqwest::blocking::Client,
    port: u16,
    message_id: &str,
    peer: &str,
) -> serde_json::Value {
    let url = format!("http://127.0.0.1:{port}/api/messages/{message_id}/deliveries/{peer}/cancel");
    let response = client.post(&url).send().expect("cancel");
    assert_eq!(
        response.status(),
        200,
        "cancel failed: {}",
        response.text().unwrap()
    );
    response.json().expect("cancel json")
}

/// The current delivery state for one recipient of one message.
fn delivery_state(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
    message_id: &str,
    peer: &str,
) -> Option<String> {
    for message in messages(client, port, conversation) {
        if message["id"].as_str() != Some(message_id) {
            continue;
        }
        for delivery in message["deliveries"].as_array().unwrap() {
            if delivery["peer_id"].as_str() == Some(peer) {
                return delivery["state"].as_str().map(str::to_string);
            }
        }
    }
    None
}

/// Waits until a delivery's state is anything but `not_state`, proving
/// a moderation gate keeps it from dispatching.
fn wait_state_holds(
    client: &reqwest::blocking::Client,
    port: u16,
    conversation: &str,
    message_id: &str,
    peer: &str,
    not_state: &str,
    window: Duration,
) {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        if delivery_state(client, port, conversation, message_id, peer).as_deref()
            == Some(not_state)
        {
            panic!("delivery reached {not_state} while gated");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Three slow turns posted back to back serialize to exactly one active prompt at a time.
#[test]
fn simultaneous_requests_serialize_to_one_active_prompt() {
    let dir = scratch_dir("serialize");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let log = dir.join("prompts.log");
    let fake = fake_agent_path();
    let command = format!("{} --log-prompts {}", fake.display(), log.display());

    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    // Three slow turns posted back to back: the busy gate must queue
    // them so exactly one prompt runs at a time.
    let first = post(&client, port, &conversation, "a-slow");
    let second = post(&client, port, &conversation, "b-slow");
    let third = post(&client, port, &conversation, "c-slow");
    wait_delivery(&client, port, &conversation, &first, "delivered");
    wait_delivery(&client, port, &conversation, &second, "delivered");
    wait_delivery(&client, port, &conversation, &third, "delivered");

    // Three receipts, strictly sequential: each prompt starts after the
    // previous turn's 1.5 s hold completed.
    let raw = std::fs::read_to_string(&log).expect("prompt log");
    let timestamps: Vec<u128> = raw
        .lines()
        .map(|line| line.split('\t').next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(timestamps.len(), 3, "three prompts: {raw}");
    for window in timestamps.windows(2) {
        assert!(
            window[1] - window[0] >= 1400,
            "prompts overlapped or ran back-to-back: {raw}"
        );
    }
    drop(node);
    cleanup(&dir);
}

/// The operator cancel interrupts a held turn and confirms via dispatch.stopped(canceled).
#[test]
fn cancel_during_a_held_turn_confirms_cancellation() {
    let dir = scratch_dir("cancel-hold");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let touch = dir.join("prompted");
    let fake = fake_agent_path();
    let command = format!("{} --touch {}", fake.display(), touch.display());

    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    // The held turn keeps the session busy; the operator cancel must
    // interrupt it (the fake answers the cancel with `Cancelled`).
    let held = post(&client, port, &conversation, "never");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !touch.exists() {
        assert!(
            Instant::now() < deadline,
            "harness never received the prompt"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let outcome = cancel(&client, port, &held, "codex");
    assert_eq!(
        outcome["outcome"], "attempt_interrupt",
        "a held turn must cancel through the attempt: {outcome}"
    );

    // The cancelled attempt lands as a stopped event with the confirmed
    // cancellation carrier.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut stopped = None;
    while Instant::now() < deadline {
        stopped = events(&client, port, &conversation)
            .into_iter()
            .find(|event| {
                event["kind"] == "dispatch.stopped" && event["payload"]["state"] == "canceled"
            });
        if stopped.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        stopped.is_some(),
        "no dispatch.stopped(canceled) event observed"
    );
    drop(node);
    cleanup(&dir);
}

/// A held delivery survives a node kill; release reconciles through the normal queue.
#[test]
fn held_delivery_survives_restart_and_releases_cleanly() {
    let dir = scratch_dir("held-restart");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let fake = fake_agent_path();

    let (node, port, token) = boot(&dir, &db, fake.to_str().unwrap());
    let client = api_client(&token);

    // Hold lands before the next dispatcher tick (250 ms), so the
    // intent never dispatches.
    let held = post(&client, port, &conversation, "held-work");
    hold(&client, port, &held, "codex");
    assert_eq!(
        delivery_state(&client, port, &conversation, &held, "codex").as_deref(),
        Some("held")
    );

    // The hold is durable moderation: after a node kill + restart the
    // delivery stays held and nothing dispatched.
    drop(node);
    let (node, port, token) = boot(&dir, &db, fake.to_str().unwrap());
    let client = api_client(&token);
    assert_eq!(
        delivery_state(&client, port, &conversation, &held, "codex").as_deref(),
        Some("held"),
        "the hold must survive the restart"
    );
    wait_state_holds(
        &client,
        port,
        &conversation,
        &held,
        "codex",
        "delivered",
        Duration::from_secs(3),
    );

    // Release reconciles through the normal queue.
    release(&client, port, &held, "codex");
    wait_delivery(&client, port, &conversation, &held, "delivered");
    drop(node);
    cleanup(&dir);
}

/// A fence raced mid-flight lets in-flight work complete while new deliveries stay blocked until cleared.
#[test]
fn fence_raced_mid_flight_blocks_new_delivery_until_clear() {
    let dir = scratch_dir("fence-race");
    let db = dir.join("node.db");
    let conversation = seed_store(&db);
    let touch = dir.join("prompted");
    let fake = fake_agent_path();
    let command = format!("{} --touch {}", fake.display(), touch.display());

    let (node, port, token) = boot(&dir, &db, &command);
    let client = api_client(&token);

    // A slow turn is confirmed in flight (the harness received the
    // prompt) before the fence lands: in-flight work completes and is
    // reported accurately, while post-fence posts stay blocked until
    // the operator clears the fence.
    let in_flight = post(&client, port, &conversation, "fence-slow");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !touch.exists() {
        assert!(
            Instant::now() < deadline,
            "harness never received the prompt"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let fence_id = fence(&client, port, &conversation, "codex", "pause");
    let blocked = post(&client, port, &conversation, "blocked-work");

    wait_delivery(&client, port, &conversation, &in_flight, "delivered");
    wait_state_holds(
        &client,
        port,
        &conversation,
        &blocked,
        "codex",
        "delivered",
        Duration::from_secs(3),
    );

    clear_fence(&client, port, &fence_id);
    wait_delivery(&client, port, &conversation, &blocked, "delivered");
    drop(node);
    cleanup(&dir);
}
