//! Integration tests for the session handle, run against the fake ACP
//! agent binary over real stdio — the same wire path the adapter uses.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use agent_client_protocol::v1::schema::{
    ContentBlock, McpServer, McpServerStdio, SessionUpdate, StopReason, TextContent,
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

/// One session services exactly one prompt at a time; a second prompt waits for the first turn.
#[tokio::test]
async fn prompts_are_serialized_per_session() {
    let (handle, _events) = spawn_fake();
    let session = handle
        .new_session(PathBuf::from("."))
        .await
        .expect("session/new");

    // The slow turn takes ~1.5s in the fake agent. The prompts are
    // issued CONCURRENTLY — the slow one starts first, and the fast
    // one is sent while the slow turn is still held open — so the
    // only way the fast response can be delayed past the slow turn's
    // minimum duration is the serialized prompt loop. (The previous
    // shape awaited the slow turn BEFORE creating the fast one, which
    // proved nothing.)
    let slow = tokio::spawn({
        let handle = handle.clone();
        let session = session.clone();
        async move { handle.prompt(session, vec![text_block("slow")]).await }
    });
    // Let the slow prompt reach the fake agent first.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let fast_start = Instant::now();
    let fast = handle
        .prompt(session.clone(), vec![text_block("hello")])
        .await
        .expect("fast prompt");
    let fast_elapsed = fast_start.elapsed();
    let slow = slow.await.expect("slow task").expect("slow prompt");

    assert!(matches!(slow, StopReason::EndTurn));
    assert!(matches!(fast, StopReason::EndTurn));
    assert!(
        fast_elapsed >= Duration::from_millis(1200),
        "the fast prompt must queue behind the in-flight slow turn          (serialized loop), not complete immediately: {fast_elapsed:?}"
    );

    handle.shutdown().await.expect("shutdown");
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
    handle.clone().shutdown().await.expect("shutdown");

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
    // Give the loop a beat to drain the command channel into the
    // pending queue, then the NEXT prompt must be refused promptly.
    sleep(Duration::from_millis(200)).await;
    let overflow_start = Instant::now();
    let overflow = tokio::time::timeout(Duration::from_secs(2), {
        let handle = handle.clone();
        let session = session.clone();
        async move {
            handle
                .prompt(session.clone(), vec![text_block("overflow")])
                .await
        }
    })
    .await
    .expect("the overflow prompt must be refused promptly (not hang)");
    assert!(
        matches!(overflow, Err(AcpError::QueueFull)),
        "the bound must reject as definitely unsent: {overflow:?}"
    );
    assert!(
        overflow_start.elapsed() < Duration::from_secs(1),
        "the refusal must not wait on the held turn"
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
    let (handle, _events) = SessionHandle::spawn(StdioTransport::new("sleep 1"));
    assert!(handle.closed().borrow().is_none());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handle.closed().borrow().is_some() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("closed watch never fired after the adapter exited");
}
