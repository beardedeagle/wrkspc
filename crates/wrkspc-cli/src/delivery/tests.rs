use super::*;
use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use tower::ServiceExt;
use wrkspc_a2a::A2aSurface;

/// Seeds a store with a committed message to codex and one delivery
/// task in `node-test-1`'s queue, and returns the task id + the
/// conversation id.
async fn seeded_delivery(store: &Store) -> (String, String) {
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
    let message_id = match store
        .accept_message(
            &conversation,
            "operator",
            "hub-msg-1",
            "hello from the hub",
            true,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    {
        wrkspc_core::AcceptOutcome::Accepted { message_id }
        | wrkspc_core::AcceptOutcome::Replay { message_id } => message_id,
    };
    let task_id = store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-test-1",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &message_id,
            conversation_id: &conversation,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "hello from the hub",
            execution_requested: true,
        })
        .await
        .unwrap();
    (task_id, conversation)
}

async fn router_under_test(store: Store) -> axum::Router {
    let surface = A2aSurface::new(store.clone());
    surface
        .into_router("https://hub.test")
        .route(
            "/delivery-claim",
            axum::routing::post(delivery_claim).with_state(DeliveryState::new(store.clone())),
        )
        .route(
            "/delivery-ack",
            axum::routing::post(delivery_ack).with_state(DeliveryState::new(store)),
        )
}

async fn post_delivery(
    router: axum::Router,
    route: &str,
    identity: Option<&str>,
    task_id: &str,
    instance: &str,
) -> axum::http::StatusCode {
    let mut builder = axum::http::Request::builder()
        .method("POST")
        .uri(route)
        .header(CONTENT_TYPE, "application/json");
    if let Some(identity) = identity {
        builder = builder.header("x-wrkspc-peer", identity);
    }
    let response = router
        .oneshot(
            builder
                .body(Body::from(
                    serde_json::json!({"taskId": task_id, "instanceId": instance}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

#[tokio::test]
async fn owning_host_ack_completes_the_delivery() {
    let store = Store::open_in_memory("ack-test").await.unwrap();
    let (task_id, conversation_id) = seeded_delivery(&store).await;
    let router = router_under_test(store.clone()).await;

    // Unclaimed acks are refused: the exclusive claim is the fence.
    let status = post_delivery(
        router,
        "/delivery-ack",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-a",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);

    // Claim, then ack.
    let router = router_under_test(store.clone()).await;
    let status = post_delivery(
        router,
        "/delivery-claim",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-a",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let router = router_under_test(store.clone()).await;
    let status = post_delivery(
        router,
        "/delivery-ack",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-a",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);

    // The task is terminal-completed and the delivery is marked
    // delivered hub-side.
    let task = store.task_get(&task_id).await.unwrap().unwrap();
    assert!(task.terminal);
    assert_eq!(task.state, "completed");
    let payload: serde_json::Value = serde_json::from_str(&task.task_json).unwrap();
    assert!(payload["metadata"]["delivery"].get("acked_at").is_some());
    assert_eq!(payload["status"]["state"], "TASK_STATE_COMPLETED");

    // The feed records the fence-ack.
    let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
    assert!(events.iter().any(|event| event.kind == "delivery.acked"));

    // A repeated ack is idempotent-success.
    let router = router_under_test(store.clone()).await;
    let status = post_delivery(
        router,
        "/delivery-ack",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-a",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
}

#[tokio::test]
async fn foreign_host_cannot_ack_another_hosts_delivery() {
    let store = Store::open_in_memory("ack-spoof-test").await.unwrap();
    let (task_id, _conversation_id) = seeded_delivery(&store).await;
    let router = router_under_test(store.clone()).await;

    let status = post_delivery(
        router,
        "/delivery-ack",
        Some("CN=node-other SAN=node-other"),
        &task_id,
        "i-other",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    let task = store.task_get(&task_id).await.unwrap().unwrap();
    assert!(!task.terminal);
}

#[tokio::test]
async fn claim_is_first_wins_idempotent_and_visible() {
    let store = Store::open_in_memory("claim-test").await.unwrap();
    let (task_id, conversation_id) = seeded_delivery(&store).await;
    let router = router_under_test(store.clone()).await;

    // First claim wins and lands a visible feed event.
    let status = post_delivery(
        router,
        "/delivery-claim",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-a",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
    let claimed = events
        .iter()
        .find(|event| event.kind == "delivery.claimed")
        .expect("claim lands a feed event");
    // Every agent on a host presents the same client certificate, so the host
    // in the event cannot say whose mail was consumed. The recipient can.
    assert!(
        claimed.payload.get("recipient_peer").is_some(),
        "the claim event must name the recipient: {}",
        claimed.payload
    );

    // A second INSTANCE with the same cert (the double-attach
    // shape) gets 409 with the claimant.
    let router = router_under_test(store.clone()).await;
    let status = post_delivery(
        router,
        "/delivery-claim",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-b",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT);

    // The claiming host's re-claim is idempotent-success.
    let router = router_under_test(store.clone()).await;
    let status = post_delivery(
        router,
        "/delivery-claim",
        Some("CN=node-test-1 SAN=node-test-1"),
        &task_id,
        "i-a",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
}

#[tokio::test]
async fn unstamped_and_unknown_requests_are_rejected() {
    let store = Store::open_in_memory("ack-reject-test").await.unwrap();
    let (task_id, _conversation_id) = seeded_delivery(&store).await;
    let router = router_under_test(store.clone()).await;

    assert_eq!(
        post_delivery(router.clone(), "/delivery-ack", None, &task_id, "i-a").await,
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_delivery(
            router,
            "/delivery-ack",
            Some("CN=node-test-1 SAN=node-test-1"),
            "no-such-task",
            "i-a",
        )
        .await,
        axum::http::StatusCode::NOT_FOUND
    );
}
