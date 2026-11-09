use super::*;
use axum::http::header::CONTENT_TYPE;
use tower::ServiceExt;
use wrkspc_a2a::A2aSurface;

/// Seeds a store with an operator on the hub (`local`) and a codex
/// peer on a remote host, plus their conversation.
async fn seeded() -> (Store, String) {
    let store = Store::open_in_memory("acceptance-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();
    (store, conversation)
}

/// Builds the router under test: the A2A surface plus the acceptance
/// layer, with the hub as the local host.
async fn router_under_test(store: Store) -> axum::Router {
    let surface = A2aSurface::new(store.clone());
    surface
        .into_router("https://hub.test")
        .layer(axum::middleware::from_fn_with_state(
            AcceptanceState::new(store, wrkspc_core::PILOT_HOST_ID.to_string()),
            accept_translation,
        ))
}

/// Builds a JSON-RPC `message/send` body in the SDK's camelCase
/// wire format.
fn send_body(
    message_id: &str,
    conversation_id: &str,
    peer_id: &str,
    recipients: &[&str],
) -> String {
    send_body_method(
        "SendMessage",
        message_id,
        conversation_id,
        peer_id,
        recipients,
    )
}

fn send_body_method(
    method: &str,
    message_id: &str,
    conversation_id: &str,
    peer_id: &str,
    recipients: &[&str],
) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {
            "message": {
                "messageId": message_id,
                "role": "ROLE_USER",
                "contextId": conversation_id,
                "parts": [{"text": "hello from the node"}],
                "metadata": {
                    "peerId": peer_id,
                    "recipients": recipients,
                    "executionRequested": true,
                },
            },
        },
    })
    .to_string()
}

async fn post(
    router: axum::Router,
    identity: Option<&str>,
    body: &str,
) -> (axum::http::StatusCode, String) {
    post_versioned(router, identity, body, Some("1.0")).await
}

async fn post_versioned(
    router: axum::Router,
    identity: Option<&str>,
    body: &str,
    a2a_version: Option<&str>,
) -> (axum::http::StatusCode, String) {
    let mut builder = axum::http::Request::builder()
        .method("POST")
        .uri("/jsonrpc")
        .header(CONTENT_TYPE, "application/json");
    if let Some(identity) = identity {
        builder = builder.header("x-wrkspc-peer", identity);
    }
    if let Some(version) = a2a_version {
        builder = builder.header("a2a-version", version);
    }
    let response = router
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn accepted_events(store: &Store, conversation: &str) -> usize {
    store
        .events_since(conversation, 0, 1000)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == "message.accepted")
        .count()
}

#[tokio::test]
async fn ejected_peer_send_is_refused_until_restored() {
    let (store, conversation) = seeded().await;
    let identity = Some("node-test-1");
    // Boot codex: its send is refused with a parseable envelope and
    // nothing commits.
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Booted, "operator")
        .await
        .unwrap();
    let router = router_under_test(store.clone()).await;
    let (status, body) = post(
        router,
        identity,
        &send_body("m-1", &conversation, "codex", &["operator"]),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN, "body: {body}");
    let envelope: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(envelope["error"]["code"], -32001);
    assert!(
        envelope["error"]["message"]
            .as_str()
            .unwrap()
            .contains("is booted")
    );
    assert_eq!(accepted_events(&store, &conversation).await, 0);
    // Restore: the same send commits normally.
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Enrolled, "operator")
        .await
        .unwrap();
    let router = router_under_test(store.clone()).await;
    let (status, _) = post(
        router,
        identity,
        &send_body("m-1", &conversation, "codex", &["operator"]),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(accepted_events(&store, &conversation).await, 1);
}

#[tokio::test]
async fn attributed_send_commits_and_queues_remote_delivery() {
    let (store, conversation) = seeded().await;
    let router = router_under_test(store.clone()).await;

    // The operator (hub host) sends to codex on a remote host.
    let (status, body) = post(
        router,
        Some("wrkspc-pilot-host"),
        &send_body("m-1", &conversation, "operator", &["codex"]),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");

    // The send committed as a hub message.
    assert_eq!(accepted_events(&store, &conversation).await, 1);

    // codex lives on node-test-1, so a pollable delivery task exists
    // in that host's queue.
    let (tasks, total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 1);
    let payload: serde_json::Value = serde_json::from_str(&tasks[0].task_json).unwrap();
    let delivery = &payload["metadata"]["delivery"];
    assert_eq!(delivery["kind"], "delivery");
    assert_eq!(delivery["recipient_peer"], "codex");
    assert_eq!(delivery["sender_peer"], "operator");

    // Replaying the same send is idempotent end to end: no new
    // message and no duplicate delivery task.
    let router = router_under_test(store.clone()).await;
    let (status, _) = post(
        router,
        Some("wrkspc-pilot-host"),
        &send_body("m-1", &conversation, "operator", &["codex"]),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(accepted_events(&store, &conversation).await, 1);
    let (_, total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 1);
}

/// An unaddressed send commits but wakes nobody: the pre-fill that
/// used to fan a recipient-less `message/send` out to every other
/// member is gone — routing is mention/explicit only, and a room must
/// not re-broadcast because the node simply didn't say who.
#[tokio::test]
async fn unaddressed_send_commits_and_wakes_nobody() {
    let store = Store::open_in_memory("acceptance-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("codex", "codex", "harness", "node-test-1")
        .await
        .unwrap();
    store
        .ensure_peer_on_host("agy", "agy", "harness", "node-other")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&["operator", "codex", "agy"])
        .await
        .unwrap();
    let router = router_under_test(store.clone()).await;

    let (status, _) = post(
        router,
        Some("node-test-1"),
        &send_body("m-nobody", &conversation, "codex", &[]),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(accepted_events(&store, &conversation).await, 1);
    // The message committed, but no host got a delivery task: agy
    // (node-other) is a member and would have received one under the
    // retired all-members fallback.
    let (_, total) = store
        .task_list(Some("node-other"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 0, "an unaddressed post must not fan out");
}

#[tokio::test]
async fn jsonrpc_error_envelope_is_not_translated() {
    let (store, conversation) = seeded().await;
    let router = router_under_test(store.clone()).await;

    // No `a2a-version` header: the SDK answers HTTP 200 with a
    // JSON-RPC error envelope (version not supported). The middleware
    // must honor that verdict instead of translating the send.
    let (status, body) = post_versioned(
        router,
        Some("wrkspc-pilot-host"),
        &send_body("m-noversion", &conversation, "operator", &["codex"]),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(
        body.contains("\"error\""),
        "the SDK's verdict rides inside HTTP 200, got: {body}"
    );
    assert_eq!(accepted_events(&store, &conversation).await, 0);
}

#[tokio::test]
async fn sender_asserted_from_wrong_host_is_rejected_before_acceptance() {
    let (store, conversation) = seeded().await;
    let router = router_under_test(store.clone()).await;

    // The operator is enrolled on `local`, but this connection
    // presented a certificate for node-test-1.
    let (status, body) = post(
        router,
        Some("node-test-1"),
        &send_body("m-spoof", &conversation, "operator", &["codex"]),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN, "body: {body}");
    // The rejection carries a JSON-RPC error envelope so clients can
    // parse it and tell a permanent attribution failure from a
    // transient transport failure (a bare text 403 wedges the
    // node's outbound retry loop).
    let envelope: serde_json::Value = serde_json::from_str(&body).expect("json envelope");
    assert_eq!(envelope["error"]["code"], -32001, "body: {body}");
    assert_eq!(accepted_events(&store, &conversation).await, 0);
    let (_, total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 0);
}

#[tokio::test]
async fn send_without_sender_metadata_is_rejected() {
    let (store, conversation) = seeded().await;
    let router = router_under_test(store.clone()).await;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "SendMessage",
        "params": {
            "message": {
                "messageId": "m-nometa",
                "role": "ROLE_USER",
                "contextId": conversation,
                "parts": [{"text": "anonymous"}],
            },
        },
    })
    .to_string();
    let (status, _) = post(router, Some("wrkspc-pilot-host"), &body).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(accepted_events(&store, &conversation).await, 0);
}

#[tokio::test]
async fn unstamped_requests_pass_through_without_translation() {
    let (store, conversation) = seeded().await;
    let router = router_under_test(store.clone()).await;

    // No stamped header (plain-TCP serving): the SDK accepts the
    // task, but no store translation runs — sender attribution is
    // only trusted from the verified connection.
    let (status, _) = post(
        router,
        None,
        &send_body("m-plain", &conversation, "operator", &["codex"]),
    )
    .await;
    assert!(status.is_success());
    assert_eq!(accepted_events(&store, &conversation).await, 0);
    let (_, total) = store
        .task_list(Some("node-test-1"), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(total, 0);
}

/// The node's outbound loop marks a message hub-sent only on a
/// parseable success — a re-send the hub already committed must
/// therefore come back as a success, never an error envelope.
/// Live defect: the laptop's agy reply committed on the hub
/// (`c268f601`) while the node kept re-sending its local copy
/// (`3564298e`) with "expected val" parse failures, forever.
#[tokio::test]
async fn replay_send_returns_parseable_success() {
    let (store, conversation) = seeded().await;
    let router = router_under_test(store.clone()).await;
    let body = send_body("m-replay", &conversation, "codex", &["operator"]);

    let (first_status, first_body) = post(router.clone(), Some("node-test-1"), &body).await;
    assert_eq!(first_status, axum::http::StatusCode::OK);
    assert!(
        first_body.contains("\"result\""),
        "first send must carry a result member: {first_body}"
    );

    let (second_status, second_body) = post(router, Some("node-test-1"), &body).await;
    assert_eq!(second_status, axum::http::StatusCode::OK);
    let envelope: serde_json::Value =
        serde_json::from_str(&second_body).expect("replay response must be JSON");
    assert!(
        envelope.get("error").is_none(),
        "replay must not be an error envelope: {second_body}"
    );
    assert!(
        envelope.get("result").is_some(),
        "replay must carry a parseable result: {second_body}"
    );
}

/// Seeds a delivery task in `host`'s queue on the shared store and
/// returns its task id.
async fn seeded_delivery(
    store: &Store,
    conversation: &str,
    host: &str,
    message_id: &str,
) -> String {
    store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: host,
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id,
            origin_message_id: None,
            conversation_id: conversation,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "private delivery content",
            execution_requested: true,
        })
        .await
        .unwrap()
}

/// C-1: task reads and cancels are scoped to the presenting host's own
/// delivery queue. Another host's task — or any task under a foreign
/// or missing context — is refused with a parseable JSON-RPC error
/// envelope before the SDK handler ever sees it, and the refusal
/// confirms nothing about the task's existence.
#[tokio::test]
async fn task_reads_and_cancels_are_scoped_to_the_presenting_host_queue() {
    let (store, conversation) = seeded().await;
    let foreign = seeded_delivery(&store, &conversation, "node-other", "msg-foreign").await;
    let own = seeded_delivery(&store, &conversation, "node-test-1", "msg-own").await;
    let router = router_under_test(store.clone()).await;

    // GetTask/CancelTask/SubscribeToTask on another host's queue: 403
    // with a parseable JSON-RPC error envelope.
    for method in ["GetTask", "CancelTask", "SubscribeToTask"] {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": method,
            "params": { "id": foreign },
        })
        .to_string();
        let (status, text) = post(router.clone(), Some("node-test-1"), &body).await;
        assert_eq!(
            status,
            axum::http::StatusCode::FORBIDDEN,
            "{method} on a foreign host's task"
        );
        let envelope: serde_json::Value =
            serde_json::from_str(&text).expect("rejection must be JSON");
        assert!(
            envelope.get("error").is_some(),
            "{method} rejection must carry an error envelope: {text}"
        );
    }

    // ListTasks with a foreign contextId — and with none at all — is
    // refused: the queue name must be the presenting host.
    for params in [
        serde_json::json!({ "contextId": "node-other" }),
        serde_json::json!({}),
    ] {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "ListTasks",
            "params": params,
        })
        .to_string();
        let (status, _) = post(router.clone(), Some("node-test-1"), &body).await;
        assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    }

    // A nonexistent task id fails closed too: no existence oracle for
    // other hosts' queues.
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 9,
        "method": "GetTask",
        "params": { "id": "ghost-task" },
    })
    .to_string();
    let (status, _) = post(router.clone(), Some("node-test-1"), &body).await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);

    // The presenting host's OWN queue is served normally, and the
    // foreign task remains untouched.
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "GetTask",
        "params": { "id": own },
    })
    .to_string();
    let (status, text) = post(router.clone(), Some("node-test-1"), &body).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let envelope: serde_json::Value = serde_json::from_str(&text).expect("response must be JSON");
    assert!(
        envelope.get("error").is_none(),
        "own task must not error: {text}"
    );
    assert_eq!(envelope["result"]["id"], own);
    assert!(
        store.task_get(&foreign).await.unwrap().is_some(),
        "the refused foreign task was never mutated"
    );

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "ListTasks",
        "params": { "contextId": "node-test-1" },
    })
    .to_string();
    let (status, text) = post(router.clone(), Some("node-test-1"), &body).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(
        text.contains("\"result\""),
        "own queue list must carry a result: {text}"
    );
}

/// C-1 (REST twin): the HTTP+JSON task routes enforce the same
/// host-queue scoping as their JSON-RPC counterparts.
#[tokio::test]
async fn rest_task_routes_are_host_queue_scoped() {
    let (store, conversation) = seeded().await;
    let foreign = seeded_delivery(&store, &conversation, "node-other", "msg-rest-foreign").await;
    let own = seeded_delivery(&store, &conversation, "node-test-1", "msg-rest-own").await;
    let router = router_under_test(store.clone()).await;

    let request = |method: &str, uri: String| {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("x-wrkspc-peer", "node-test-1")
            .body(axum::body::Body::empty())
            .unwrap()
    };

    let response = router
        .clone()
        .oneshot(request(
            "GET",
            "/rest/tasks?contextId=node-other".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);

    let response = router
        .clone()
        .oneshot(request("GET", format!("/rest/tasks/{foreign}")))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);

    let response = router
        .clone()
        .oneshot(request("POST", format!("/rest/tasks/{foreign}/cancel")))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);

    // The own queue passes through to the SDK.
    let response = router
        .clone()
        .oneshot(request("GET", format!("/rest/tasks/{own}")))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}
