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
    /// Serializes the token. The struct is flat strings, so this is
    /// effectively infallible — but the serving path must never panic,
    /// so an unexpected failure surfaces as an internal error instead.
    fn encode(&self) -> Result<String, A2AError> {
        serde_json::to_string(self).map_err(|e| A2AError::internal(e.to_string()))
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

/// Maps a storage failure with no A2A-level shape to a generic internal
/// error, logging the driver detail server-side.
///
/// SQLite error text carries driver paths and schema detail that must
/// never reach A2A clients; the log keeps it for operators while the
/// wire gets the generic shape only.
pub(crate) fn internal_store_error(error: wrkspc_core::StoreError) -> A2AError {
    tracing::error!(%error, "storage failure");
    A2AError::internal("storage failure")
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
        other => internal_store_error(other),
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
            .map_err(internal_store_error)?;
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

        // page_size: resolve through the SDK's shared pagination
        // contract — absent/zero/negative fall back to 50 and the
        // result never exceeds 100 — so direct `TaskStore` callers
        // (tests, embeds) cannot drive an unbounded LIMIT the way
        // the handler's pre-clamp alone would allow. Request one
        // extra row to detect another page.
        let page_size = a2a_server::pagination::resolve_page_size(req.page_size);
        let limit = i64::try_from(page_size)
            .unwrap_or(i64::MAX)
            .saturating_add(1);
        let (rows, total) = self
            .store
            .task_list(
                req.context_id.as_deref(),
                status,
                status_after.as_deref(),
                page.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
                limit,
            )
            .await
            .map_err(internal_store_error)?;

        let has_more = rows.len() > page_size;
        let page_rows = &rows[..rows.len().min(page_size)];

        let tasks = page_rows
            .iter()
            .map(|r| task_from_json(&r.task_json))
            .collect::<Result<Vec<_>, _>>()?;
        // include_artifacts is honored at the response boundary (P11):
        // an explicit `false` strips artifact payloads; anything else
        // (including `None`) returns the stored task as-is.
        let tasks = if req.include_artifacts == Some(false) {
            tasks
                .into_iter()
                .map(|mut task| {
                    task.artifacts = None;
                    task
                })
                .collect()
        } else {
            tasks
        };

        let next_page_token = if has_more {
            match page_rows.last() {
                Some(row) => PageToken {
                    updated_at: row.updated_at.clone(),
                    task_id: row.task_id.clone(),
                }
                .encode()?,
                None => String::new(),
            }
        } else {
            String::new()
        };

        Ok(ListTasksResponse {
            tasks,
            next_page_token,
            page_size: i32::try_from(page_size).unwrap_or(i32::MAX),
            total_size: i32::try_from(total).unwrap_or(i32::MAX),
        })
    }

    async fn begin_cancel(&self, task_id: &str) -> Result<Task, A2AError> {
        // Cancel the WORK first: the delivery's queued intent or
        // in-flight attempt, not just this row (H-cancel propagation —
        // a status flip alone would report cancellation that never
        // reached the execution).
        crate::executor::cancel_enveloped_work(&self.store, task_id).await?;
        // Cancel preserves the task's record: the original context,
        // history, artifacts, and metadata survive; only the status
        // transitions. A cancel that discards the envelope would erase
        // the delivery's audit data (H-13).
        let current = self
            .store
            .task_get(task_id)
            .await
            .map_err(internal_store_error)?
            .ok_or_else(|| A2AError::task_not_found(task_id))?;
        let mut snapshot: serde_json::Value = serde_json::from_str(&current.task_json)
            .map_err(|e| A2AError::internal(e.to_string()))?;
        let snapshot = snapshot
            .as_object_mut()
            .ok_or_else(|| A2AError::internal("task payload is not an object".to_string()))?;
        snapshot.insert(
            "status".to_string(),
            serde_json::json!({
                "state": "TASK_STATE_CANCELED",
                "timestamp": chrono::Utc::now(),
            }),
        );
        let canceled_json =
            serde_json::to_string(snapshot).map_err(|e| A2AError::internal(e.to_string()))?;
        let canceled_state = crate::state_mapping::task_state_literal(&a2a::TaskState::Canceled);
        let row = self
            .store
            .task_begin_cancel(task_id, canceled_state, &canceled_json)
            .await
            .map_err(|error| match error {
                // A terminal task is not cancelable — that is a
                // lifecycle verdict, not a malformed request.
                wrkspc_core::StoreError::TaskTerminal { task_id, state, .. } => {
                    A2AError::task_not_cancelable(&format!(
                        "task {task_id} is terminal ({state}) and cannot be canceled"
                    ))
                }
                other => store_error(other),
            })?;
        task_from_json(&row.task_json)
    }
}
