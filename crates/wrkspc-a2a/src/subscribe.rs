//! Store-tailing subscribe: the documented streaming-results
//! transport.
//!
//! The SDK's default subscribe streams only in-process execution events;
//! wrkspc's task transitions are store-driven (the delivery-report leg),
//! so a subscriber would hear nothing. This handler keeps the SDK path
//! for active executions and falls back to tailing the task row's
//! version column: the snapshot, then each version change until the
//! task reaches a terminal state, then the final task.

use a2a::event::StreamResponse;
use a2a::{
    A2AError, CancelTaskRequest, GetExtendedAgentCardRequest, GetTaskPushNotificationConfigRequest,
    GetTaskRequest, ListTaskPushNotificationConfigsRequest,
    ListTaskPushNotificationConfigsResponse, ListTasksRequest, ListTasksResponse,
    SendMessageRequest, SendMessageResponse, SubscribeToTaskRequest, Task,
    TaskPushNotificationConfig, TaskStatusUpdateEvent,
};
use a2a_server::handler::{DefaultRequestHandler, RequestHandler};
use a2a_server::middleware::ServiceParams;
use futures::stream::{self, BoxStream};
use std::sync::Arc;
use std::time::Duration;
use wrkspc_core::Store;

/// Poll cadence for store-tailing: bounds the worst-case subscribe lag
/// without hammering the store with one query per poll per subscriber.
const TAIL_POLL: Duration = Duration::from_millis(250);

/// The wrkspc request handler: the SDK's [`DefaultRequestHandler`] for
/// every method except `subscribe_to_task`, which tail-tails the store
/// when no active execution exists.
#[derive(Clone)]
pub struct WrkspcRequestHandler {
    // Debug is derived on demand by the SDK router's diagnostics; the
    // store handle's own Debug prints its pool, not credentials.
    inner: Arc<DefaultRequestHandler>,
    store: Store,
}

impl std::fmt::Debug for WrkspcRequestHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The SDK's DefaultRequestHandler carries no Debug impl; the
        // wrapper reports its shape instead. The store handle's Debug
        // prints its pool, never credentials.
        f.debug_struct("WrkspcRequestHandler")
            .field("store", &self.store)
            .finish()
    }
}

impl WrkspcRequestHandler {
    /// Builds the handler over the SDK default.
    ///
    /// * `inner` — the SDK's default handler (executor + task store).
    /// * `store` — the transactional store the subscribe tail reads.
    pub fn new(inner: DefaultRequestHandler, store: Store) -> Self {
        Self {
            inner: Arc::new(inner),
            store,
        }
    }
}

/// Maps a store failure to the generic internal error, logging the
/// driver detail server-side (SQLite error text must never reach A2A
/// clients).
fn internal_error(error: wrkspc_core::StoreError) -> A2AError {
    tracing::error!(%error, "storage failure in subscribe tail");
    A2AError::internal("storage failure")
}

/// Tails one task row: the snapshot first, then a status update per
/// version change, ending with the final task when the row goes
/// terminal. The stream owns its store handle, so a client disconnect
/// simply drops it — no watcher registration to unwind.
fn tail_task(
    store: Store,
    task_id: String,
    snapshot: Task,
    version: i64,
) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
    struct Tail {
        store: Store,
        task_id: String,
        snapshot: Option<Task>,
        version: i64,
        done: bool,
    }
    let tail = Tail {
        store,
        task_id,
        snapshot: Some(snapshot),
        version,
        done: false,
    };
    Box::pin(stream::unfold(tail, |mut tail| async move {
        if tail.done {
            return None;
        }
        // The snapshot precedes any polling: the subscriber always
        // starts from the task's current record.
        if let Some(task) = tail.snapshot.take() {
            return Some((Ok(StreamResponse::Task(task)), tail));
        }
        // Poll until the version moves: a change-less poll emits
        // NOTHING — the subscriber hears only transitions.
        loop {
            tokio::time::sleep(TAIL_POLL).await;
            let row = match tail.store.task_get(&tail.task_id).await {
                Ok(Some(row)) => row,
                Ok(None) => {
                    tail.done = true;
                    return Some((Err(A2AError::task_not_found(&tail.task_id)), tail));
                }
                Err(error) => {
                    tail.done = true;
                    return Some((Err(internal_error(error)), tail));
                }
            };
            if row.version == tail.version {
                continue;
            }
            tail.version = row.version;
            let task: Task = match serde_json::from_str(&row.task_json) {
                Ok(task) => task,
                Err(error) => {
                    tail.done = true;
                    return Some((Err(A2AError::internal(error.to_string())), tail));
                }
            };
            if row.terminal {
                tail.done = true;
                return Some((Ok(StreamResponse::Task(task)), tail));
            }
            return Some((
                Ok(StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                    task_id: tail.task_id.clone(),
                    context_id: row.context_id,
                    status: task.status,
                    metadata: None,
                })),
                tail,
            ));
        }
    }))
}

#[async_trait::async_trait]
impl RequestHandler for WrkspcRequestHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        self.inner.send_message(params, req).await
    }

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.inner.send_streaming_message(params, req).await
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.inner.get_task(params, req).await
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.inner.list_tasks(params, req).await
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        self.inner.cancel_task(params, req).await
    }

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        // An active execution streams through the SDK's own path
        // unchanged. Without one, the store is the truth: tail the
        // task row's version.
        match self.inner.subscribe_to_task(params, req.clone()).await {
            Ok(stream) => Ok(stream),
            Err(error)
                if error.code == a2a::error_code::TASK_NOT_FOUND
                    || error.code == a2a::error_code::UNSUPPORTED_OPERATION =>
            {
                let Some(row) = self.store.task_get(&req.id).await.map_err(internal_error)? else {
                    return Err(error);
                };
                if row.terminal {
                    return Err(error);
                }
                let snapshot: Task = serde_json::from_str(&row.task_json)
                    .map_err(|e| A2AError::internal(e.to_string()))?;
                Ok(tail_task(self.store.clone(), req.id, snapshot, row.version))
            }
            Err(error) => Err(error),
        }
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.inner.create_push_config(params, req).await
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.inner.get_push_config(params, req).await
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.inner.list_push_configs(params, req).await
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        req: a2a::DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.inner.delete_push_config(params, req).await
    }

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        req: GetExtendedAgentCardRequest,
    ) -> Result<a2a::AgentCard, A2AError> {
        self.inner.get_extended_agent_card(params, req).await
    }
}
