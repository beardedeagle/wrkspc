//! Integration tests for the session handle, run against the fake ACP
//! agent binary over real stdio — the same wire path the adapter uses.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use agent_client_protocol::v1::schema::{
    ContentBlock, McpServer, McpServerStdio, RequestPermissionOutcome, SessionConfigOptionValue,
    SessionUpdate, SetSessionConfigOptionRequest, StopReason, TextContent,
};
use std::path::PathBuf;
use std::time::Instant;
use tokio::time::{Duration, sleep};
use wrkspc_acp::{AcpError, SessionEvent, SessionHandle, StdioTransport};

/// Spawns a handle connected to the fake agent.
fn spawn_fake() -> (
    SessionHandle,
    tokio::sync::broadcast::Receiver<SessionEvent>,
) {
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    SessionHandle::spawn(StdioTransport::new(exe))
}

/// session/new injects the wrkspc MCP servers the node configured, before the agent starts.
#[tokio::test]
async fn session_new_injects_mcp_servers() {
    let record = std::env::temp_dir().join(format!("wrkspc-acp-mcp-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&record);
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let command = format!("{exe} --record {}", record.display());
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(command));

    let server = McpServer::Stdio(
        McpServerStdio::new("wrkspc", "wrkspc").args(vec!["mcp-bridge".to_string()]),
    );
    let session = handle
        .new_session_with_mcp(PathBuf::from("."), vec![server])
        .await
        .expect("session/new with mcp injection");
    assert_eq!(session, "fake-1");

    // The fake agent writes the received mcpServers; the wire carried the
    // injection config (check 8's transport half, runtime-proven).
    for _ in 0..50 {
        if record.exists() {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    let json = std::fs::read_to_string(&record).expect("record file written");
    let servers: serde_json::Value = serde_json::from_str(&json).expect("record is JSON");
    let servers = servers.as_array().expect("array");
    assert_eq!(servers.len(), 1);
    // The stdio variant is untagged: the fields serialize flat, without a
    // "type" discriminator.
    assert_eq!(servers[0]["name"], "wrkspc");
    assert_eq!(servers[0]["command"], "wrkspc");
    assert_eq!(servers[0]["args"][0], "mcp-bridge");

    handle.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&record);
}

fn text_block(text: &str) -> ContentBlock {
    ContentBlock::Text(TextContent::new(text.to_string()))
}

/// A prompt round-trips through the fake agent and resolves with the echoed content and the EndTurn stop reason.
#[tokio::test]
async fn prompt_round_trip_delivers_echo_and_end_turn() {
    let (handle, mut events) = spawn_fake();
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    let outcome = handle
        .prompt(session.clone(), vec![text_block("hello")])
        .await
        .expect("prompt");
    assert!(matches!(outcome, StopReason::EndTurn));

    // The echo chunk arrives as a session update event.
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("update must arrive")
        .expect("event stream must stay open");
    assert_eq!(event.session_id, session);
    match event.update {
        SessionUpdate::AgentMessageChunk(chunk) => match chunk.content {
            ContentBlock::Text(t) => assert_eq!(t.text, "echo: hello"),
            other => panic!("unexpected content: {other:?}"),
        },
        other => panic!("unexpected update: {other:?}"),
    }

    handle.shutdown().await.expect("shutdown");
}

/// Reads the fake's `--log-turn` file as `(event, text)` pairs; the
/// trailing millisecond stamp on each line is stripped.
fn read_turn_log(path: &std::path::Path) -> Vec<(String, String)> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            let mut parts = line.splitn(3, '\t');
            let event = parts.next()?;
            let body = parts.next()?;
            Some((event.to_string(), body.to_string()))
        })
        .collect()
}

/// One session services exactly one prompt at a time; a second prompt waits for the first turn.
#[tokio::test]
async fn prompts_are_serialized_per_session() {
    let log = std::env::temp_dir().join(format!("wrkspc-acp-turns-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&log);
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(format!(
        "{exe} --log-turn {}",
        log.display()
    )));
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    // The slow turn takes ~1.5s in the fake agent. The prompts are
    // issued CONCURRENTLY — the slow one starts first, and the fast
    // one is sent while the slow turn is still held open — so the
    // only way the fast turn can start after the slow turn ended is
    // the serialized prompt loop.
    let slow = tokio::spawn({
        let handle = handle.clone();
        let session = session.clone();
        async move { handle.prompt(session, vec![text_block("slow")]).await }
    });
    // The fake's `start slow` log line is deterministic proof the slow
    // prompt reached the agent (a blind sleep raced on slow machines —
    // sending the fast prompt first would prove nothing).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !read_turn_log(&log)
        .iter()
        .any(|(event, text)| event == "start" && text == "slow")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the slow prompt must reach the agent"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let fast = handle
        .prompt(session.clone(), vec![text_block("hello")])
        .await
        .expect("fast prompt");
    let slow = slow.await.expect("slow task").expect("slow prompt");

    assert!(matches!(slow, StopReason::EndTurn));
    assert!(matches!(fast, StopReason::EndTurn));
    // Strict turn order — no wall-clock thresholds: the serialized
    // loop means the fast turn starts only after the slow turn ended.
    assert_eq!(
        read_turn_log(&log),
        vec![
            ("start".to_string(), "slow".to_string()),
            ("end".to_string(), "slow".to_string()),
            ("start".to_string(), "hello".to_string()),
            ("end".to_string(), "hello".to_string()),
        ],
        "turns must be strictly serialized, never interleaved"
    );

    handle.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&log);
}

/// Cancel interrupts a held turn and the confirmation rides the prompt
/// response's stop reason.
#[tokio::test]
async fn cancel_confirms_via_prompt_response() {
    let (handle, mut events) = spawn_fake();
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    // "never" holds the turn open in the fake agent. Spawn the prompt so it
    // actually runs while we cancel from the control path (a pinned-but-
    // never-polled future would never send the command).
    let prompt_task = {
        let handle = handle.clone();
        let session = session.clone();
        tokio::spawn(async move {
            handle
                .prompt(session.clone(), vec![text_block("never")])
                .await
        })
    };

    // Wait for the fake's receipt chunk: deterministic proof the prompt
    // reached the agent before cancel (a blind sleep raced on slow
    // machines — cancel first would no-op and the held turn would hang).
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("the prompt must reach the agent")
        .expect("event stream must stay open");
    assert_eq!(event.session_id, session);
    handle.cancel(session.clone()).await.expect("cancel");

    // Confirmation arrives as the prompt response with stopReason cancelled.
    let outcome = tokio::time::timeout(Duration::from_secs(5), prompt_task)
        .await
        .expect("cancel must resolve the prompt")
        .expect("prompt task must not panic")
        .expect("prompt outcome");
    assert!(matches!(outcome, StopReason::Cancelled));

    handle.shutdown().await.expect("shutdown");
}

/// Commands after shutdown fail with a clean error instead of hanging or panicking.
#[tokio::test]
async fn commands_after_shutdown_fail_cleanly() {
    let (handle, _events) = spawn_fake();
    // shutdown takes &self: the shared handle orders the teardown
    // without a clone and stays usable afterwards.
    handle.shutdown().await.expect("shutdown");

    let err = handle
        .new_session(PathBuf::from("."))
        .await
        .expect_err("command after shutdown must fail");
    assert!(matches!(err, AcpError::NotRunning));
}

/// The serialized prompt queue is BOUNDED (E11): while a turn is in
/// flight, the admission bound is the queue cap — the next prompt is
/// rejected as definitely unsent instead of queueing without limit.
#[tokio::test]
async fn prompt_queue_rejects_beyond_the_admission_bound() {
    let (handle, mut events) = spawn_fake();
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    // "never" holds the turn open; the echo chunk proves it reached
    // the agent and the loop is in flight.
    let held = {
        let handle = handle.clone();
        let session = session.clone();
        tokio::spawn(async move {
            handle
                .prompt(session.clone(), vec![text_block("never")])
                .await
        })
    };
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("the held prompt must reach the agent")
        .expect("event stream must stay open");
    assert_eq!(event.session_id, session);

    // Fill the bounded queue (64 slots) with queued-but-unsent
    // prompts; each waits on its oneshot.
    for _ in 0..64 {
        let handle = handle.clone();
        let session = session.clone();
        tokio::spawn(async move {
            let _ = handle
                .prompt(session.clone(), vec![text_block("queued")])
                .await;
        });
    }
    // No blind sleep: the admission bound is observable through the
    // rejection itself. Commands flow FIFO through the channel, so each
    // attempt either occupies the last pending slot (advancing the queue
    // toward the bound) or is refused with QueueFull. Retry until the
    // refusal lands; a timed-out attempt was queued, not refused.
    let mut overflow = None;
    for _ in 0..80 {
        let attempt_start = Instant::now();
        match tokio::time::timeout(Duration::from_secs(2), {
            let handle = handle.clone();
            let session = session.clone();
            async move {
                handle
                    .prompt(session.clone(), vec![text_block("overflow")])
                    .await
            }
        })
        .await
        {
            Ok(Err(error @ AcpError::QueueFull)) => {
                assert!(
                    attempt_start.elapsed() < Duration::from_secs(1),
                    "the refusal must not wait on the held turn"
                );
                overflow = Some(error);
                break;
            }
            Ok(Err(other)) => panic!("unexpected overflow rejection: {other:?}"),
            Ok(Ok(_)) => panic!("the overflow prompt was accepted; the bound must have held"),
            // Queued behind the held turn: the caller is gone, the loop
            // skips it later, and the next attempt observes the bound.
            Err(_) => continue,
        }
    }
    assert!(
        matches!(overflow, Some(AcpError::QueueFull)),
        "the bound must refuse a prompt as definitely unsent"
    );

    // Tear down: cancel releases the held turn; shutdown drops the
    // queued prompts with NotRunning (their callers are gone).
    handle.cancel(session.clone()).await.expect("cancel");
    let _ = tokio::time::timeout(Duration::from_secs(5), held)
        .await
        .expect("cancel must resolve the held prompt");
    handle.shutdown().await.expect("shutdown");
}

/// An adapter that dies while its session is cached and idle must end
/// the session loop on its own: the closed watch flips with no command,
/// eviction, or shutdown — a dead adapter must never advertise a live
/// presence ( residual).
#[tokio::test]
async fn dead_idle_adapter_flips_closed_without_a_command() {
    // A REAL ACP adapter dying idle: the fake answers initialize, then
    // exits on its own after 300ms of no traffic.
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let (handle, _events) =
        SessionHandle::spawn(StdioTransport::new(format!("{exe} --exit-after-ms 300")));
    assert!(handle.closed().borrow().is_none());
    let reason = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(reason) = handle.closed().borrow().clone() {
                return reason;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("closed watch never fired after the adapter exited");
    // Not just "some termination": an orderly connection end is the
    // only acceptable reason here.
    assert_eq!(reason, "transport ended");
}

/// A transport death while a prompt is in flight is an OUTCOME-UNKNOWN
/// signal, not a protocol refusal: the request may have reached the
/// adapter, so a dispatcher must never treat it as definitely-not-sent.
#[tokio::test]
async fn prompt_death_mid_turn_is_connection_lost() {
    // The fake dies without responding when a prompt contains "boom".
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let command = format!("{exe} --die-on boom");
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(command));
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        handle.prompt(session.clone(), vec![text_block("boom")]),
    )
    .await
    .expect("a dead adapter must fail the prompt, not hang it");
    match outcome {
        Err(AcpError::ConnectionLost(_)) => {}
        other => panic!(
            "a transport death mid-turn must be ConnectionLost (outcome unknown), got {other:?}"
        ),
    }
}

/// The client refuses adapter permission requests with `cancelled`:
/// no permission policy exists yet, so an adapter asking mid-turn must
/// hear the conservative refusal instead of silence.
#[tokio::test]
async fn permission_requests_are_refused_with_cancelled() {
    let outcome_file =
        std::env::temp_dir().join(format!("wrkspc-acp-permission-{}.txt", std::process::id()));
    let _ = std::fs::remove_file(&outcome_file);
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let command = format!("{exe} --request-permission {}", outcome_file.display());
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(command));
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    let outcome = handle
        .prompt(session.clone(), vec![text_block("may I?")])
        .await
        .expect("prompt");
    assert!(matches!(outcome, StopReason::EndTurn));

    // The fake records the client's permission outcome (the request is
    // answered concurrently with the turn, so poll until it lands).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut recorded = None;
    while std::time::Instant::now() < deadline {
        if let Ok(text) = std::fs::read_to_string(&outcome_file) {
            recorded = Some(text);
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    let recorded = recorded.expect("the fake must record the permission outcome");
    let outcome: RequestPermissionOutcome =
        serde_json::from_str(&recorded).expect("the fake must record the outcome as JSON");
    assert_eq!(
        outcome,
        RequestPermissionOutcome::Cancelled,
        "the client must refuse with the cancelled outcome"
    );

    handle.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&outcome_file);
}

/// session/set_config_option round-trips: the exact payload (session id,
/// config id, typed value) the wire carried is asserted, so a wrong
/// payload cannot slip past an ack-only check.
#[tokio::test]
async fn set_config_option_round_trips() {
    let record =
        std::env::temp_dir().join(format!("wrkspc-acp-config-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&record);
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(format!(
        "{exe} --record-config {}",
        record.display()
    )));
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    handle
        .set_config_option(session.clone(), "model", "claude-sonnet-4")
        .await
        .expect("set_config_option");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !record.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the fake must record the config request"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let json = std::fs::read_to_string(&record).expect("record file written");
    let recorded: SetSessionConfigOptionRequest =
        serde_json::from_str(&json).expect("record is JSON");
    assert_eq!(recorded.session_id.to_string(), session);
    assert_eq!(recorded.config_id.to_string(), "model");
    assert_eq!(
        recorded.value,
        SessionConfigOptionValue::value_id("claude-sonnet-4")
    );

    handle.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&record);
}

/// The typed `set_config_option_value` reaches boolean options: the
/// Boolean variant round-trips as `type: "boolean"` on the wire, which
/// the string-only form cannot express.
#[tokio::test]
async fn set_config_option_boolean_value_round_trips() {
    let record = std::env::temp_dir().join(format!(
        "wrkspc-acp-config-bool-{}.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&record);
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new(format!(
        "{exe} --record-config {}",
        record.display()
    )));
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    handle
        .set_config_option_value(
            session.clone(),
            "hints",
            SessionConfigOptionValue::boolean(true),
        )
        .await
        .expect("set_config_option_value");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !record.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the fake must record the config request"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let json = std::fs::read_to_string(&record).expect("record file written");
    let recorded: SetSessionConfigOptionRequest =
        serde_json::from_str(&json).expect("record is JSON");
    assert_eq!(recorded.session_id.to_string(), session);
    assert_eq!(recorded.config_id.to_string(), "hints");
    assert_eq!(recorded.value, SessionConfigOptionValue::boolean(true));

    handle.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&record);
}

/// `StdioTransport::new_with_args` passes argv through unsplit: a
/// record path containing spaces survives as ONE argument (the
/// shell-string ctor would split it and the fake would never write the
/// file).
#[tokio::test]
async fn stdio_transport_new_with_args_preserves_arg_boundaries() {
    let dir = std::env::temp_dir().join(format!("wrkspc-acp space {}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let record = dir.join("config record.json");
    let _ = std::fs::remove_file(&record);
    let exe = env!("CARGO_BIN_EXE_fake-acp-agent");
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new_with_args(
        exe,
        ["--record-config", record.to_str().unwrap()],
    ));
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    handle
        .set_config_option(session.clone(), "model", "claude-sonnet-4")
        .await
        .expect("set_config_option");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !record.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the fake must receive --record-config as one argument and write the file"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let json = std::fs::read_to_string(&record).expect("record file written");
    let recorded: SetSessionConfigOptionRequest =
        serde_json::from_str(&json).expect("record is JSON");
    assert_eq!(
        recorded.value,
        SessionConfigOptionValue::value_id("claude-sonnet-4")
    );

    handle.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A prompt for an unknown session is refused with the adapter's
/// session-not-found error — never silence: a dropped responder would
/// leave the caller awaiting a response until the connection closed.
#[tokio::test]
async fn prompt_for_unknown_session_answers_with_the_protocol_error() {
    let (handle, _events) = spawn_fake();
    handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        handle.prompt("fake-404".to_string(), vec![text_block("hello")]),
    )
    .await
    .expect("an unknown session must be refused promptly, not left hanging");
    match outcome {
        Err(AcpError::Protocol(message)) => {
            assert!(
                message.contains("Resource not found"),
                "the adapter's session-not-found error must surface: {message}"
            );
        }
        other => panic!("expected a protocol refusal, got {other:?}"),
    }

    handle.shutdown().await.expect("shutdown");
}
