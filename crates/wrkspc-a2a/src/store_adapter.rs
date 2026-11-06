//! The `TaskStore` adapter: A2A tasks over the wrkspc store's
//! protocol-agnostic task rows.
//!
//! Core owns every SQL statement (see `wrkspc_core::tasks`); this adapter
//! only maps A2A types onto those rows. Terminal-state immutability is
//! enforced in core; the SDK contract (idempotent re-emission of the same
//! terminal state) is preserved there too.

use a2a::{A2AError, ListTasksRequest, ListTasksResponse, Task};
use a2a_server::task_store::{TaskStore, TaskVersion};
use async_trait::async_trait;
use wrkspc_core::Store;

/// Page token: JSON-encoded `{updated_at, task_id}` of the last returned
/// row (keyset pagination, newest first).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PageToken {
    updated_at: String,
    task_id: String,
}

impl PageToken {
    fn encode(&self) -> String {
        serde_json::to_string(self).expect("page token serialization cannot fail")
    }

    fn decode(token: &str) -> Option<Self> {
        serde_json::from_str(token).ok()
    }
}

/// The wrkspc task store adapter.
#[derive(Debug, Clone)]
pub struct WrkspcTaskStore {
    store: Store,
}

impl WrkspcTaskStore {
    /// Builds the adapter over a wrkspc store.
    ///
    /// # Arguments
    ///
    /// * `store` — the transactional store (shared with the dispatcher).
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

/// Serializes a task to the blob column.
fn task_to_json(task: &Task) -> Result<String, A2AError> {
    serde_json::to_string(task).map_err(|e| A2AError::internal(e.to_string()))
}

/// Deserializes a task from the blob column.
fn task_from_json(blob: &str) -> Result<Task, A2AError> {
    serde_json::from_str(blob).map_err(|e| A2AError::internal(e.to_string()))
}

/// Maps a wrkspc store error to the closest A2A error.
fn store_error(error: wrkspc_core::StoreError) -> A2AError {
    match error {
        wrkspc_core::StoreError::NotFound { entity, id } => {
            A2AError::task_not_found(&format!("{entity}/{id}"))
        }
        wrkspc_core::StoreError::TaskTerminal { task_id, state, .. } => A2AError::invalid_request(
            format!("task {task_id} is in terminal state {state} and cannot transition"),
        ),
        wrkspc_core::StoreError::AlreadyExists { entity, id } => {
            A2AError::invalid_request(format!("{entity} `{id}` already exists"))
        }
        other => A2AError::internal(other.to_string()),
    }
}

#[async_trait]
impl TaskStore for WrkspcTaskStore {
    async fn create(&self, task: Task) -> Result<TaskVersion, A2AError> {
        let state = crate::state_mapping::task_state_literal(&task.status.state);
        let blob = task_to_json(&task)?;
        self.store
            .task_create(
                &task.id,
                &task.context_id,
                state,
                task.status.state.is_terminal(),
                &blob,
            )
            .await
            .map_err(store_error)?;
        Ok(1)
    }

    async fn update(&self, task: Task) -> Result<TaskVersion, A2AError> {
        let state = crate::state_mapping::task_state_literal(&task.status.state);
        let blob = task_to_json(&task)?;
        let version = self
            .store
            .task_update(&task.id, state, task.status.state.is_terminal(), &blob)
            .await
            .map_err(store_error)?;
        Ok(version as u64)
    }

    async fn get(&self, task_id: &str) -> Result<Option<Task>, A2AError> {
        let row = self
            .store
            .task_get(task_id)
            .await
            .map_err(|e| A2AError::internal(e.to_string()))?;
        row.map(|r| task_from_json(&r.task_json)).transpose()
    }

    async fn list(&self, req: &ListTasksRequest) -> Result<ListTasksResponse, A2AError> {
        let status = req
            .status
            .as_ref()
            .map(crate::state_mapping::task_state_literal);
        let page = req
            .page_token
            .as_deref()
            .filter(|t| !t.is_empty())
            .map(PageToken::decode);
        let page = match page {
            Some(Some(token)) => Some((token.updated_at, token.task_id)),
            Some(None) => return Err(A2AError::invalid_request("invalid page token")),
            None => None,
        };

        let status_after = req.status_timestamp_after.map(|dt| dt.to_rfc3339());

        // page_size: the handler clamps to MAX_PAGE_SIZE before calling;
        // default 50 per spec when unset. Request one extra row to detect
        // another page.
        let page_size = req.page_size.unwrap_or(50).max(1) as i64;
        let (rows, total) = self
            .store
            .task_list(
                req.context_id.as_deref(),
                status,
                status_after.as_deref(),
                page.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
                page_size + 1,
            )
            .await
            .map_err(|e| A2AError::internal(e.to_string()))?;

        let has_more = rows.len() as i64 > page_size;
        let page_rows = &rows[..rows.len().min(page_size as usize)];

        let tasks = page_rows
            .iter()
            .map(|r| task_from_json(&r.task_json))
            .collect::<Result<Vec<_>, _>>()?;

        let next_page_token = if has_more {
            page_rows
                .last()
                .map(|r| {
                    PageToken {
                        updated_at: r.updated_at.clone(),
                        task_id: r.task_id.clone(),
                    }
                    .encode()
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(ListTasksResponse {
            tasks,
            next_page_token,
            page_size: page_size as i32,
            total_size: total as i32,
        })
    }

    async fn begin_cancel(&self, task_id: &str) -> Result<Task, A2AError> {
        let canceled_state = crate::state_mapping::task_state_literal(&a2a::TaskState::Canceled);
        // The canceled blob is produced by the caller of this trait method's
        // contract: core only needs a serialized replacement. The handler
        // emits the actual TaskStatusUpdateEvent after this returns, so a
        // minimal canceled snapshot suffices here.
        let snapshot = serde_json::json!({
            "id": task_id,
            "contextId": "",
            "status": { "state": "TASK_STATE_CANCELED" },
        });
        let canceled_json =
            serde_json::to_string(&snapshot).map_err(|e| A2AError::internal(e.to_string()))?;
        let row = self
            .store
            .task_begin_cancel(task_id, canceled_state, &canceled_json)
            .await
            .map_err(store_error)?;
        task_from_json(&row.task_json)
    }
}
