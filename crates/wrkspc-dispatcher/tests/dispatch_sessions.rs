//! Session behavior: guidance prefixing, missing bindings, chunk
//! attribution, per-session serialization, and parallel peers.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use agent_client_protocol::v1::schema::StopReason;
use common::{FakeDriver, FakeProvider, Mode, payload_has, spawn_harness};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use wrkspc_core::{AcceptOutcome, RecipientRequest, Store};
use wrkspc_dispatcher::{Dispatcher, DispatcherConfig, SessionBinding};

/// Guidance rides the next prompt (prefixed) and is delivered exactly once.
#[tokio::test]
async fn guidance_prefixes_the_next_prompt() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), true).await;
    harness
        .store
        .add_guidance(&harness.conversation_id, "operator", "prefer short answers")
        .await
        .unwrap();

    harness.wait_for_kind("dispatch.stopped").await;
    let prompts = harness.driver.prompt_texts();
    assert_eq!(prompts.len(), 1, "one prompt dispatched");
    assert!(
        prompts[0].contains("[operator guidance from operator]: prefer short answers"),
        "guidance rides the prompt, got: {prompts:?}"
    );
    assert!(
        prompts[0].contains("do the thing"),
        "content follows guidance"
    );
    assert!(
        prompts[0].starts_with(&format!(
            "[wrkspc] conversation: {}\n[wrkspc] you are: agent",
            harness.conversation_id
        )),
        "conversation id and recipient peer id lead the prompt, got: {prompts:?}"
    );

    // Guidance is delivered exactly once.
    assert!(
        harness
            .store
            .undelivered_guidance(&harness.conversation_id)
            .await
            .unwrap()
            .is_empty(),
        "guidance marked delivered"
    );
}

/// A missing session binding marks the attempt unknown without ever prompting.
#[tokio::test]
async fn missing_binding_marks_attempt_unknown() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), false).await;
    harness.wait_for_kind("dispatch.unknown").await;
    let events = harness.stop_and_events().await;

    assert!(payload_has(
        &events,
        "dispatch.unknown",
        "no session binding"
    ),);
    assert!(
        !payload_has(&events, "dispatch.started", "attempt_id"),
        "no prompt without a binding"
    );
}

/// Turn chunks land in the feed attributed to the attempt, before the terminal event.
#[tokio::test]
async fn turn_chunks_land_in_the_feed_attributed_to_the_attempt() {
    let harness = spawn_harness(Mode::Respond(StopReason::EndTurn), true).await;
    harness.wait_for_kind("dispatch.stopped").await;
    let events = harness.stop_and_events().await;

    // The chunk arrives with the attempt id and the reply text, and the
    // sequence shows it before the terminal write.
    let chunk_seq = events
        .iter()
        .position(|(kind, payload)| kind == "turn.chunk" && payload.contains("do the thing"))
        .expect("turn.chunk recorded");
    assert!(
        payload_has(&events, "turn.chunk", "\"attempt_id\""),
        "chunk carries its attempt attribution"
    );
    assert!(
        payload_has(&events, "turn.chunk", "\"message_id\":\"reply-message\""),
        "chunk keeps the agent's message id so message boundaries survive"
    );
    let stopped_seq = events
        .iter()
        .position(|(kind, _)| kind == "dispatch.stopped")
        .expect("stopped recorded");
    assert!(chunk_seq < stopped_seq, "chunks precede the terminal event");
}

/// One session serializes turns across conversations and attributes chunks to the right conversation.
#[tokio::test]
async fn one_session_serializes_turns_and_attributes_chunks_per_conversation() {
    let store = Store::open_in_memory("dispatcher-serial").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    let conv_a = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    let conv_b = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    for (conversation_id, key, content) in [
        (&conv_a, "ka", "message for a"),
        (&conv_b, "kb", "message for b"),
    ] {
        let AcceptOutcome::Accepted { .. } = store
            .accept_message(
                conversation_id,
                "operator",
                key,
                content,
                false,
                &[RecipientRequest {
                    peer_id: "agent".to_string(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap()
        else {
            panic!("fresh accept");
        };
    }

    let driver = FakeDriver::new(Mode::Respond(StopReason::EndTurn));
    let mut bindings = HashMap::new();
    bindings.insert(
        "agent".to_string(),
        SessionBinding {
            peer_id: "agent".to_string(),
            session_id: "sess-agent".to_string(),
            driver: Arc::new(driver.clone()),
        },
    );
    let provider = Arc::new(FakeProvider::new(bindings));
    let (dispatcher, _handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
            lookup_timeout: std::time::Duration::from_secs(10),
            ..Default::default()
        },
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("run");
    });

    // Wait until BOTH conversations reached their terminal write.
    common::wait_until("both turns finish", Duration::from_secs(10), || {
        let store = store.clone();
        let (conv_a, conv_b) = (conv_a.clone(), conv_b.clone());
        Box::pin(async move {
            let a = store.events_since(&conv_a, 0, 100).await.unwrap();
            let b = store.events_since(&conv_b, 0, 100).await.unwrap();
            a.iter().any(|e| e.kind == "dispatch.stopped")
                && b.iter().any(|e| e.kind == "dispatch.stopped")
        })
    })
    .await;

    let events_a = store.events_since(&conv_a, 0, 100).await.unwrap();
    let events_b = store.events_since(&conv_b, 0, 100).await.unwrap();

    // Attribution: each conversation's chunk reflects its own content.
    let chunk_a = events_a
        .iter()
        .find(|e| e.kind == "turn.chunk")
        .expect("chunk in A");
    assert!(
        chunk_a.payload.to_string().contains("message for a"),
        "got: {}",
        chunk_a.payload
    );
    assert!(
        chunk_a.payload.to_string().contains(&conv_a),
        "chunk A carries its conversation context"
    );
    assert!(!chunk_a.payload.to_string().contains("message for b"));
    let chunk_b = events_b
        .iter()
        .find(|e| e.kind == "turn.chunk")
        .expect("chunk in B");
    assert!(chunk_b.payload.to_string().contains("message for b"));
    assert!(
        chunk_b.payload.to_string().contains(&conv_b),
        "chunk B carries its conversation context"
    );
    assert!(!chunk_b.payload.to_string().contains("message for a"));

    // Serialization: one session drives one turn at a time — the driver
    // observed at most ONE prompt in flight across both conversations.
    // A wall-clock event-order proof cannot distinguish same-millisecond
    // events, an overlap counter can.
    assert_eq!(driver.max_overlap(), 1, "one session never overlaps turns");

    let _ = shutdown_tx.send(()).await;
}

/// Two peers on distinct sessions dispatch in parallel and both complete.
#[tokio::test]
async fn two_peers_dispatch_in_parallel_without_wedging() {
    let store = Store::open_in_memory("dispatcher-parallel").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    for peer in ["codex", "agy"] {
        store.ensure_peer(peer, peer, "harness").await.unwrap();
    }
    let conversation_id = store
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();
    // One message with execution intents for BOTH peers.
    let AcceptOutcome::Accepted { .. } = store
        .accept_message(
            &conversation_id,
            "operator",
            "k1",
            "hello both",
            false,
            &[
                RecipientRequest {
                    peer_id: "codex".to_string(),
                    execution_requested: true,
                },
                RecipientRequest {
                    peer_id: "agy".to_string(),
                    execution_requested: true,
                },
            ],
        )
        .await
        .unwrap()
    else {
        panic!("fresh accept");
    };

    // Blocking drivers: each turn holds until release_all(). Both
    // prompts must be OUTSTANDING at once before either completes —
    // immediately-completing fakes would only prove distinct session
    // ids, not overlap.
    let codex = FakeDriver::new(Mode::BlockUntilReleased);
    let agy = FakeDriver::new(Mode::BlockUntilReleased);
    let mut bindings = HashMap::new();
    bindings.insert(
        "codex".to_string(),
        SessionBinding {
            peer_id: "codex".to_string(),
            session_id: "sess-codex".to_string(),
            driver: Arc::new(codex.clone()),
        },
    );
    bindings.insert(
        "agy".to_string(),
        SessionBinding {
            peer_id: "agy".to_string(),
            session_id: "sess-agy".to_string(),
            driver: Arc::new(agy.clone()),
        },
    );
    let provider = Arc::new(FakeProvider::new(bindings));
    let (dispatcher, _handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
            lookup_timeout: std::time::Duration::from_secs(10),
            ..Default::default()
        },
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("run");
    });

    // BOTH prompts must be in flight BEFORE any release: a serialized
    // dispatcher would never reach the second peer while the first
    // turn blocks.
    common::wait_until("both prompts in flight", Duration::from_secs(10), || {
        Box::pin(async { codex.prompt_texts().len() == 1 && agy.prompt_texts().len() == 1 })
    })
    .await;
    assert_eq!(
        codex.prompt_texts().len(),
        1,
        "codex must have its turn in flight before release"
    );
    assert_eq!(
        agy.prompt_texts().len(),
        1,
        "agy must have its turn in flight BEFORE codex completes:          both turns overlap, dispatch is concurrent"
    );
    codex.release_all();
    agy.release_all();

    // BOTH turns must complete.
    common::wait_until("both peers stopped", Duration::from_secs(10), || {
        let store = store.clone();
        let conversation_id = conversation_id.clone();
        Box::pin(async move {
            let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
            events
                .iter()
                .filter(|e| e.kind == "dispatch.stopped")
                .count()
                == 2
        })
    })
    .await;
    let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
    let stopped = events
        .iter()
        .filter(|e| e.kind == "dispatch.stopped")
        .count();
    assert_eq!(stopped, 2, "both peers must complete their turns");
    let chunk_peers: std::collections::HashSet<String> = events
        .iter()
        .filter(|e| e.kind == "turn.chunk")
        .map(|e| e.payload["session_id"].as_str().unwrap_or("?").to_string())
        .collect();
    assert_eq!(chunk_peers.len(), 2, "chunks from both sessions");

    let _ = shutdown_tx.send(()).await;
}

/// Guidance is claimed atomically: two concurrent turns in the SAME
/// conversation partition the pending guidance, so the guidance rides
/// exactly ONE prompt — never both.
#[tokio::test]
async fn concurrent_turns_in_one_conversation_do_not_duplicate_guidance() {
    let store = Store::open_in_memory("dispatcher-guidance-race")
        .await
        .unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    for peer in ["codex", "agy"] {
        store.ensure_peer(peer, peer, "harness").await.unwrap();
    }
    let conversation_id = store
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();
    store
        .add_guidance(&conversation_id, "operator", "only once")
        .await
        .unwrap();
    let AcceptOutcome::Accepted { .. } = store
        .accept_message(
            &conversation_id,
            "operator",
            "k1",
            "hello both",
            false,
            &[
                RecipientRequest {
                    peer_id: "codex".to_string(),
                    execution_requested: true,
                },
                RecipientRequest {
                    peer_id: "agy".to_string(),
                    execution_requested: true,
                },
            ],
        )
        .await
        .unwrap()
    else {
        panic!("fresh accept");
    };

    let codex = FakeDriver::new(Mode::BlockUntilReleased);
    let agy = FakeDriver::new(Mode::BlockUntilReleased);
    let mut bindings = HashMap::new();
    bindings.insert(
        "codex".to_string(),
        SessionBinding {
            peer_id: "codex".to_string(),
            session_id: "sess-codex".to_string(),
            driver: Arc::new(codex.clone()),
        },
    );
    bindings.insert(
        "agy".to_string(),
        SessionBinding {
            peer_id: "agy".to_string(),
            session_id: "sess-agy".to_string(),
            driver: Arc::new(agy.clone()),
        },
    );
    let provider = Arc::new(FakeProvider::new(bindings));
    let (dispatcher, _handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
            lookup_timeout: std::time::Duration::from_secs(10),
            ..Default::default()
        },
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("run");
    });

    // Both turns in flight (both claims attempted), then release.
    common::wait_until(
        "both guidance-race turns in flight",
        Duration::from_secs(10),
        || Box::pin(async { codex.prompt_texts().len() == 1 && agy.prompt_texts().len() == 1 }),
    )
    .await;
    assert_eq!(codex.prompt_texts().len(), 1, "codex turn in flight");
    assert_eq!(agy.prompt_texts().len(), 1, "agy turn in flight");
    let needle = "[operator guidance from operator]: only once";
    let codex_has = codex.prompt_texts()[0].contains(needle);
    let agy_has = agy.prompt_texts()[0].contains(needle);
    assert_ne!(
        codex_has,
        agy_has,
        "guidance must ride exactly ONE of the concurrent prompts; codex: {:?}, agy: {:?}",
        codex.prompt_texts(),
        agy.prompt_texts()
    );
    codex.release_all();
    agy.release_all();

    // Both complete; the guidance is delivered exactly once (the
    // winning turn marked it, the other never saw it).
    common::wait_until(
        "both guidance-race turns stopped",
        Duration::from_secs(10),
        || {
            let store = store.clone();
            let conversation_id = conversation_id.clone();
            Box::pin(async move {
                let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
                events
                    .iter()
                    .filter(|e| e.kind == "dispatch.stopped")
                    .count()
                    == 2
            })
        },
    )
    .await;
    let undelivered = store.undelivered_guidance(&conversation_id).await.unwrap();
    assert!(
        undelivered.is_empty(),
        "guidance must be delivered exactly once; pending: {undelivered:?}"
    );

    let _ = shutdown_tx.send(()).await;
}

/// Real stdio adapters send the terminal response BEFORE their trailing
/// update. A chunk emitted just after the stop reason must still land
/// in the feed — never silently dropped (H-16 completion; the fixture
/// models the real ordering, which the in-memory fake previously
/// reversed).
#[tokio::test]
async fn trailing_chunk_after_the_terminal_response_is_not_dropped() {
    let harness = spawn_harness(Mode::RespondThenChunk(StopReason::EndTurn), true).await;
    harness.wait_for_kind("dispatch.stopped").await;

    // The trailing chunk arrives ~150 ms AFTER the stop reason; the
    // drive task's grace window must still commit it.
    let store = harness.store.clone();
    let conversation_id = harness.conversation_id.clone();
    common::wait_until("the trailing chunk", Duration::from_secs(10), || {
        let store = store.clone();
        let conversation_id = conversation_id.clone();
        Box::pin(async move {
            let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
            events.iter().any(|e| {
                e.kind == "turn.chunk"
                    && e.payload["content"]
                        .as_str()
                        .is_some_and(|content| content.starts_with("trailing:"))
            })
        })
    })
    .await;
}

/// Every security gate precedes the prompt — the dispatched
/// write, the cancel checks, the delivery gate, registration, the
/// guidance claim, and the post-claim cancel check all commit before
/// `dispatch.started` is recorded, and `dispatch.started` precedes
/// the harness prompt. By the time the harness sees the prompt, the
/// started event must already exist in the feed.
#[tokio::test]
async fn every_gate_commits_before_the_prompt() {
    let store = Store::open_in_memory("order-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    let AcceptOutcome::Accepted { .. } = store
        .accept_message(
            &conversation,
            "operator",
            "k-order",
            "work",
            false,
            &[RecipientRequest {
                peer_id: "agent".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    else {
        panic!("fresh accept");
    };

    let driver = FakeDriver::new(Mode::Respond(StopReason::EndTurn));
    let mut bindings = HashMap::new();
    bindings.insert(
        "agent".to_string(),
        SessionBinding {
            peer_id: "agent".to_string(),
            session_id: "sess-agent".to_string(),
            driver: Arc::new(driver.clone()),
        },
    );
    let provider = Arc::new(FakeProvider::new(bindings));
    let (dispatcher, _handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
            ..Default::default()
        },
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("run");
    });

    // The prompt is the terminal marker of the security sequence.
    driver.wait_for_prompt().await;

    // By prompt time the started event must already be committed —
    // every gate runs before it, and it runs before the prompt.
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    assert!(
        events.iter().any(|event| event.kind == "dispatch.started"),
        "dispatch.started must be committed before the prompt reaches the harness"
    );

    shutdown_tx.send(()).await.expect("shutdown send");
}
