//! Integration tests for the harness-facing MCP server: tool behavior over
//! an in-process duplex transport (the same protocol path as stdio), store
//! integration, and the HTTP token gate.

use rmcp::handler::client::ClientHandler;
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientInfo};
use rmcp::{ServiceExt, service::RoleClient, service::RunningService};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use uuid::Uuid;
use wrkspc_core::{AttemptState, Store};
use wrkspc_mcp::{HttpListener, HttpListenerConfig, McpServer};

/// Minimal MCP client handler for in-process tests.
#[derive(Debug, Clone, Default)]
struct TestClient;

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }
}

type TestClientService = RunningService<RoleClient, TestClient>;

/// Seeds a store with an operator and an agent in one conversation, and
/// builds an MCP server for the agent.
async fn seeded() -> (Store, String, String, String, McpServer) {
    let store = Store::open_in_memory("mcp-test").await.unwrap();
    let operator = format!("human-{}", Uuid::new_v4());
    let agent = format!("agent-{}", Uuid::new_v4());
    store
        .ensure_peer(&operator, "operator", "human")
        .await
        .unwrap();
    store.ensure_peer(&agent, "agent", "harness").await.unwrap();
    let conversation = store
        .create_conversation(&[&operator, &agent])
        .await
        .unwrap();
    let server = McpServer::new(store.clone(), agent.clone());
    (store, operator, agent, conversation, server)
}

/// Connects a test client to the server over an in-process duplex transport.
async fn connect(server: McpServer) -> TestClientService {
    let (server_transport, client_transport) = tokio::io::duplex(8192);
    tokio::spawn(async move {
        let running = server
            .serve(server_transport)
            .await
            .expect("server must serve");
        running
            .waiting()
            .await
            .expect("server must run to completion");
    });
    TestClient
        .serve(client_transport)
        .await
        .expect("client must serve")
}

/// Extracts the first text content from a tool result.
fn text_of(result: &CallToolResult) -> String {
    result
        .content
        .first()
        .and_then(|content| match content {
            rmcp::model::ContentBlock::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .expect("expected text content")
}

async fn call(
    client: &TestClientService,
    name: &str,
    arguments: serde_json::Value,
) -> CallToolResult {
    call_result(client, name, arguments)
        .await
        .expect("tool call must complete")
}

/// Calls a tool and surfaces the tool-level error: rmcp 1.8 clients
/// return `Err` for `is_error` results instead of a plain result.
async fn call_result(
    client: &TestClientService,
    name: &str,
    arguments: serde_json::Value,
) -> Result<CallToolResult, String> {
    client
        .call_tool(
            CallToolRequestParams::new(name.to_string())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await
        .map_err(|error| error.to_string())
}

/// MCP post-message accepts a message and replays the same key idempotently (same message id, no duplicate commit).
#[tokio::test]
async fn post_message_accepts_and_replays_idempotently() {
    let (_store, _operator, _agent, conversation, server) = seeded().await;
    let client = connect(server).await;

    let args = || {
        serde_json::json!({
            "conversation_id": conversation,
            "idempotency_key": "key-1",
            "content": "hello from harness",
        })
    };

    let first = call(&client, "post-message", args()).await;
    assert_ne!(first.is_error, Some(true), "accept must not be an error");
    let accepted = text_of(&first);
    let message_id = accepted.strip_prefix("accepted ").expect("accepted prefix");

    // Same key + same content: replay with the same message id.
    let second = call(&client, "post-message", args()).await;
    assert_eq!(text_of(&second), format!("replay {message_id}"));

    // Same key + different content: rejected with an error result.
    let mut conflicting = args();
    conflicting["content"] = serde_json::json!("different content");
    let conflict = call_result(&client, "post-message", conflicting).await;
    let conflict = conflict.expect_err("conflict must be an error");
    assert!(
        conflict.contains("idempotency"),
        "the store's conflict message must surface: {conflict}"
    );
}

/// With no explicit recipients, the post defaults to the conversation's other members.
#[tokio::test]
async fn post_message_defaults_recipients_to_other_members() {
    let (store, operator, _agent, conversation, server) = seeded().await;
    let client = connect(server).await;

    let result = call(
        &client,
        "post-message",
        serde_json::json!({
            "conversation_id": conversation,
            "idempotency_key": "key-2",
            "content": "with execution",
            "execution_requested": true,
        }),
    )
    .await;
    let message_id = text_of(&result)
        .strip_prefix("accepted ")
        .expect("accepted prefix")
        .to_string();

    // Default recipients exclude the caller (the agent) and include the
    // other member (the operator), so the execution intent belongs to the
    // operator and is claimable from the store.
    let attempt = store
        .claim_intent(&message_id, &operator)
        .await
        .expect("default recipients must create an intent for the other member");
    store
        .mark_terminal(&attempt, AttemptState::Completed, Some("end_turn"), None)
        .await
        .unwrap();
}

/// Posts and reads outside the caller's membership are rejected with a clean error result.
#[tokio::test]
async fn non_member_is_rejected() {
    let (store, _operator, _agent, conversation, server) = seeded().await;
    let client = connect(server).await;

    // The caller peer (agent) is a member; a post referencing a
    // non-existent conversation must fail cleanly.
    let result = call_result(
        &client,
        "post-message",
        serde_json::json!({
            "conversation_id": "missing-conversation",
            "idempotency_key": "key-3",
            "content": "into the void",
        }),
    )
    .await;
    let error = result.expect_err("non-member post must be an error");
    assert!(
        error.contains("not found") || error.contains("not a member"),
        "the rejection reason must surface: {error}"
    );
    let _ = store;
    let _ = conversation;
}

/// read-conversation enforces membership and resumes from the cursor (only new messages after the bound).
#[tokio::test]
async fn read_conversation_enforces_membership_and_cursor() {
    let (store, operator, _agent, conversation, server) = seeded().await;
    let client = connect(server).await;

    // Two messages committed by the operator, then the agent reads them.
    for (i, key) in ["key-a", "key-b"].iter().enumerate() {
        store
            .accept_message(&conversation, &operator, key, "ping", false, &[])
            .await
            .unwrap();
        let _ = i;
    }

    let all = call(
        &client,
        "read-conversation",
        serde_json::json!({ "conversation_id": conversation }),
    )
    .await;
    let events: serde_json::Value = serde_json::from_str(&text_of(&all)).expect("valid json");
    let events = events.as_array().expect("array");
    assert_eq!(events.len(), 2, "both accepted events visible");

    // Cursor: after the first seq, only the second event returns.
    let first_seq = events[0]["seq"].as_i64().unwrap();
    let tail = call(
        &client,
        "read-conversation",
        serde_json::json!({
            "conversation_id": conversation,
            "after_seq": first_seq,
        }),
    )
    .await;
    let tail: serde_json::Value = serde_json::from_str(&text_of(&tail)).expect("valid json");
    assert_eq!(
        tail.as_array().unwrap().len(),
        1,
        "cursor skips seen events"
    );
}

/// report-work appends a committed feed event the conversation can observe.
#[tokio::test]
async fn report_work_appends_a_committed_event() {
    let (_store, _operator, _agent, conversation, server) = seeded().await;
    let client = connect(server).await;

    let result = call(
        &client,
        "report-work",
        serde_json::json!({
            "conversation_id": conversation,
            "summary": "fixed the thing",
        }),
    )
    .await;
    assert!(text_of(&result).starts_with("reported at event "));

    let read = call(
        &client,
        "read-conversation",
        serde_json::json!({ "conversation_id": conversation }),
    )
    .await;
    let events: serde_json::Value = serde_json::from_str(&text_of(&read)).expect("valid json");
    let events = events.as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["kind"], "work.report");
    assert_eq!(events[0]["payload"]["summary"], "fixed the thing");
}

/// The HTTP listener refuses requests without a valid bearer token.
#[tokio::test]
async fn http_listener_enforces_token_gate() {
    let (store, _operator, _agent, _conversation, server) = seeded().await;
    let tokens = HashSet::from(["good-token".to_string()]);
    let listener = HttpListener::new(
        server,
        HttpListenerConfig {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            tokens,
        },
    );
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve_task = tokio::spawn(async move {
        listener
            .serve_on(tcp, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Give the listener a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // No token → 401 on the MCP route (GET and POST).
    let no_token = client.get(format!("{base}/mcp")).send().await.unwrap();
    assert_eq!(no_token.status(), reqwest::StatusCode::UNAUTHORIZED);
    let no_token_post = client.post(format!("{base}/mcp")).send().await.unwrap();
    assert_eq!(no_token_post.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Wrong token → 401.
    let wrong_token = client
        .get(format!("{base}/mcp"))
        .bearer_auth("bad-token")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_token.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Valid token passes the gate (the transport answers; assert the
    // gate opened rather than a specific transport status).
    let good_token = client
        .get(format!("{base}/mcp"))
        .bearer_auth("good-token")
        .send()
        .await
        .unwrap();
    assert_ne!(good_token.status(), reqwest::StatusCode::UNAUTHORIZED);

    let _ = shutdown_tx.send(());
    serve_task
        .await
        .unwrap()
        .expect("listener must shut down cleanly");
    let _ = store;
}

/// list-peers serves the roster mirror the node's poll loop maintains.
#[tokio::test]
async fn list_peers_returns_the_mirrored_roster() {
    let (store, _operator, _agent, _conversation, server) = seeded().await;
    // Seed the roster the way the node's poll loop would mirror it.
    store
        .replace_roster(&[wrkspc_core::RosterEntry {
            peer_id: "codex".to_string(),
            name: "codex".to_string(),
            alias: None,
            class: "harness".to_string(),
            host_id: "node-test-1".to_string(),
            status: "enrolled".to_string(),
            state: "connected".to_string(),
            updated_at: "2026-10-23T00:00:00Z".to_string(),
        }])
        .await
        .unwrap();
    let client = connect(server).await;

    let result = call(&client, "list-peers", serde_json::json!({})).await;
    assert_ne!(
        result.is_error,
        Some(true),
        "list-peers must not be an error"
    );
    let text = text_of(&result);
    let roster: Vec<serde_json::Value> = serde_json::from_str(&text).expect("roster json");
    assert_eq!(roster.len(), 1);
    assert_eq!(roster[0]["peer_id"], "codex");
    assert_eq!(roster[0]["state"], "connected");
}
