//! Integration tests for the A2A surface: the TaskStore adapter over the
//! durable store, and the full wire path (card discovery → six methods)
//! against a real loopback socket with the pinned client.

use a2a::{
    GetTaskRequest, ListTasksRequest, Message, Part, Role, SendMessageRequest, SendMessageResponse,
    Task, TaskState,
};
use a2a_client::agent_card::AgentCardResolver;
use a2a_client::{A2AClient, A2AClientFactory};
use a2a_server::task_store::TaskStore;
use futures::StreamExt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use wrkspc_a2a::{A2aSurface, WrkspcTaskStore};
use wrkspc_core::Store;

fn user_message(text: &str) -> Message {
    Message::new(Role::User, vec![Part::text(text.to_string())])
}

fn task(id: &str, context_id: &str, state: TaskState, status_text: Option<&str>) -> Task {
    let status_message = status_text.map(|text| Message {
        role: Role::Agent,
        message_id: a2a::new_message_id(),
        task_id: Some(id.to_string()),
        context_id: Some(context_id.to_string()),
        parts: vec![Part::text(text.to_string())],
        metadata: None,
        extensions: None,
        reference_task_ids: None,
    });
    Task {
        id: id.to_string(),
        context_id: context_id.to_string(),
        status: a2a::TaskStatus {
            state,
            message: status_message,
            timestamp: Some(chrono::Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: None,
    }
}

// ── TaskStore adapter ──────────────────────────────────────────────────────

/// Adapter CRUD: create returns version 1 and get/update round-trip the row through the TaskStore seam.
#[tokio::test]
async fn adapter_create_get_update_versioning() {
    let store = Store::open_in_memory("a2a-test").await.unwrap();
    let adapter = WrkspcTaskStore::new(store);

    let created = task("t1", "ctx-a", TaskState::Submitted, None);
    let version = adapter.create(created.clone()).await.expect("create");
    assert_eq!(version, 1);

    let fetched = adapter.get("t1").await.expect("get").expect("exists");
    assert_eq!(fetched.status.state, TaskState::Submitted);
    assert_eq!(fetched.context_id, "ctx-a");

    let updated = task("t1", "ctx-a", TaskState::Working, None);
    let version = adapter.update(updated).await.expect("update");
    assert_eq!(version, 2);
    let fetched = adapter.get("t1").await.expect("get").expect("exists");
    assert_eq!(fetched.status.state, TaskState::Working);
}

/// Terminal rows are immutable: a different terminal state is refused while re-emitting the same one stays idempotent.
#[tokio::test]
async fn adapter_terminal_immutability_and_idempotent_reemission() {
    let store = Store::open_in_memory("a2a-test").await.unwrap();
    let adapter = WrkspcTaskStore::new(store);

    adapter
        .create(task("t1", "ctx-a", TaskState::Submitted, None))
        .await
        .unwrap();
    adapter
        .update(task("t1", "ctx-a", TaskState::Completed, None))
        .await
        .unwrap();

    // Different terminal state → rejected.
    let err = adapter
        .update(task("t1", "ctx-a", TaskState::Failed, None))
        .await
        .expect_err("terminal transition must fail");
    assert!(err.to_string().contains("terminal"), "got: {err}");

    // Same terminal state → idempotent, version unchanged.
    let version = adapter
        .update(task("t1", "ctx-a", TaskState::Completed, None))
        .await
        .expect("idempotent re-emission");
    assert_eq!(version, 2);
}

/// Begin-cancel transitions a non-terminal task atomically and refuses an already-terminal one.
#[tokio::test]
async fn adapter_begin_cancel_is_atomic_and_terminal_aware() {
    let store = Store::open_in_memory("a2a-test").await.unwrap();
    let adapter = WrkspcTaskStore::new(store);

    adapter
        .create(task("t1", "ctx-a", TaskState::Submitted, None))
        .await
        .unwrap();
    let canceled = adapter.begin_cancel("t1").await.expect("cancel");
    assert_eq!(canceled.status.state, TaskState::Canceled);

    // Already terminal → not cancelable.
    adapter
        .begin_cancel("t1")
        .await
        .expect_err("second cancel must fail");

    // Unknown task → not found.
    adapter
        .begin_cancel("missing")
        .await
        .expect_err("unknown task must fail");
}

/// List applies the context/state filters and keyset pagination in newest-first order.
#[tokio::test]
async fn adapter_list_filters_and_paginates() {
    let store = Store::open_in_memory("a2a-test").await.unwrap();
    let adapter = WrkspcTaskStore::new(store);

    for (i, state) in [
        TaskState::Submitted,
        TaskState::Working,
        TaskState::Completed,
        TaskState::Submitted,
    ]
    .iter()
    .enumerate()
    {
        adapter
            .create(task(&format!("t{i}"), "ctx-list", (*state).clone(), None))
            .await
            .unwrap();
        // Stagger updates so updated_at ordering is deterministic.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // One task in another context must not appear.
    adapter
        .create(task("other", "ctx-other", TaskState::Submitted, None))
        .await
        .unwrap();

    let all = adapter
        .list(&ListTasksRequest {
            context_id: Some("ctx-list".to_string()),
            status: None,
            page_size: Some(2),
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
            tenant: None,
        })
        .await
        .expect("list");
    assert_eq!(all.total_size, 4, "context filter scopes the count");
    assert_eq!(all.tasks.len(), 2, "page size respected");
    assert!(!all.next_page_token.is_empty(), "more pages exist");

    let page_two = adapter
        .list(&ListTasksRequest {
            context_id: Some("ctx-list".to_string()),
            status: None,
            page_size: Some(2),
            page_token: Some(all.next_page_token),
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
            tenant: None,
        })
        .await
        .expect("page two");
    assert_eq!(page_two.tasks.len(), 2, "remaining rows on page two");
    assert!(page_two.next_page_token.is_empty(), "no third page");
}

// ── Wire path ──────────────────────────────────────────────────────────────

/// Spawns the surface on an ephemeral port and returns (addr, client).
async fn wire_setup() -> (
    SocketAddr,
    A2AClient<Box<dyn a2a_client::Transport>>,
    Store,
    tokio::sync::oneshot::Sender<()>,
) {
    let store = Store::open_in_memory("a2a-wire").await.unwrap();
    let surface = A2aSurface::new(store.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        surface
            .serve(listener, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("surface must serve");
    });
    // Let the server start accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let base = format!("http://{addr}");
    let resolver = AgentCardResolver::new(None);
    let card = resolver.resolve(&base).await.expect("card resolve");
    let factory = A2AClientFactory::builder()
        .preferred_bindings(vec![a2a::TRANSPORT_PROTOCOL_JSONRPC.to_string()])
        .build();
    let client = factory.create_from_card(&card).await.expect("client");
    (addr, client, store, shutdown_tx)
}

/// A message/send accepts on the wire and returns a submitted task without claiming completion.
#[tokio::test]
async fn send_message_accepts_without_claiming_completion() {
    let (_addr, client, _store, _shutdown) = wire_setup().await;

    let response = client
        .send_message(&SendMessageRequest {
            message: user_message("hello wrkspc"),
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await
        .expect("send");
    let task = match response {
        SendMessageResponse::Task(task) => task,
        other => panic!("expected task, got {other:?}"),
    };

    assert_eq!(
        task.status.state,
        TaskState::Submitted,
        "accepted, not completed"
    );
    let status_text = task
        .status
        .message
        .as_ref()
        .and_then(|m| m.parts.first().cloned())
        .map(part_text)
        .unwrap_or_default();
    assert!(
        status_text.contains("awaiting wrkspc routing"),
        "status message must state the honest pending state, got: {status_text}"
    );

    let fetched = client
        .get_task(&GetTaskRequest {
            id: task.id.clone(),
            tenant: None,
            history_length: None,
        })
        .await
        .expect("get");
    assert_eq!(fetched.status.state, TaskState::Submitted);
}

/// The streaming send emits the task object first, before any status events.
#[tokio::test]
async fn streaming_send_starts_with_the_task_object() {
    let (_addr, client, _store, _shutdown) = wire_setup().await;

    let mut stream = client
        .send_streaming_message(&SendMessageRequest {
            message: user_message("stream test"),
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await
        .expect("streaming send");

    let first = stream
        .next()
        .await
        .expect("first stream event")
        .expect("event ok");
    let task = match first {
        a2a::event::StreamResponse::Task(task) => task,
        other => {
            panic!("A2A v1.0 §3.1.2: the stream MUST begin with the Task object, got {other:?}")
        }
    };
    assert_eq!(task.status.state, TaskState::Submitted);
    let status_text = task
        .status
        .message
        .as_ref()
        .and_then(|m| m.parts.first().cloned())
        .map(part_text)
        .unwrap_or_default();
    assert!(
        status_text.contains("awaiting wrkspc routing"),
        "status message must state the honest pending state, got: {status_text}"
    );

    // The executor emits no terminal event: the stream ends here and the
    // task stays submitted.
    assert!(
        stream.next().await.is_none(),
        "stream must end after the submitted snapshot"
    );
}

/// Cancel and terminal-guard semantics hold end to end over the A2A wire.
#[tokio::test]
async fn cancel_and_terminal_guard_on_the_wire() {
    let (_addr, client, _store, _shutdown) = wire_setup().await;

    // Create a task, then cancel it.
    let response = client
        .send_message(&SendMessageRequest {
            message: user_message("cancel me"),
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await
        .expect("send");
    let SendMessageResponse::Task(task) = response else {
        panic!("expected task");
    };
    let canceled = client
        .cancel_task(&a2a::CancelTaskRequest {
            id: task.id.clone(),
            tenant: None,
            metadata: None,
        })
        .await
        .expect("cancel");
    assert_eq!(canceled.status.state, TaskState::Canceled);

    // Sending onto the canceled task id must be rejected.
    let mut message = user_message("resurrect?");
    message.task_id = Some(task.id.clone());
    message.context_id = Some(task.context_id.clone());
    let err = client
        .send_message(&SendMessageRequest {
            message,
            configuration: None,
            metadata: None,
            tenant: None,
        })
        .await
        .expect_err("send onto terminal task must fail");
    let message = err.to_string();
    assert!(
        message.contains("terminal") || message.contains("Unsupported"),
        "rejection must reference the terminal state, got: {message}"
    );
}

/// Extracts text from a part.
fn part_text(part: a2a::Part) -> String {
    match part.content {
        a2a::PartContent::Text(text) => text,
        other => panic!("expected text part, got {other:?}"),
    }
}
