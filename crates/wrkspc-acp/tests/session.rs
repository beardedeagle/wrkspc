//! Integration tests for the session handle, run against the fake ACP
//! agent binary over real stdio — the same wire path the adapter uses.

use agent_client_protocol::schema::v1::{
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

    // The slow turn takes ~1.5s; the second prompt must not be serviced
    // until it completes (same connection, serialized loop).
    let slow_start = Instant::now();
    let slow = handle
        .prompt(session.clone(), vec![text_block("slow")])
        .await
        .expect("slow prompt");
    let slow_done = Instant::now();
    let fast = handle
        .prompt(session.clone(), vec![text_block("hello")])
        .await
        .expect("fast prompt");
    let fast_done = Instant::now();

    assert!(matches!(slow, StopReason::EndTurn));
    assert!(matches!(fast, StopReason::EndTurn));
    assert!(
        slow_done.duration_since(slow_start) >= Duration::from_millis(1500),
        "slow prompt must have taken the full fake-agent delay"
    );
    assert!(
        fast_done >= slow_done,
        "the fast prompt must not finish before the slow one"
    );

    handle.shutdown().await.expect("shutdown");
}

/// Cancel interrupts a held turn and the confirmation rides the prompt response's stop reason.
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
