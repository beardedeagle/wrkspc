//! The pilot executor: accepts inbound A2A work without claiming success.
//!
//! The executor records acceptance as a `submitted` status update and ends
//! its event stream WITHOUT a terminal event, so the task persists in
//! `submitted` state. Execution state is driven by the dispatcher (unit 5):
//! a completed turn is never claimed as task success (D-3 predicate).

use a2a::event::StreamResponse;
use a2a::{A2AError, Message, Part, Role, Task, TaskState, TaskStatus, TaskStatusUpdateEvent};
use a2a_server::{AgentExecutor, ExecutorContext};
use futures::stream;
use futures::stream::BoxStream;
use wrkspc_core::{CancelOutcome, Store};

/// The pilot executor: acceptance only, no completion claims.
#[derive(Debug, Clone)]
pub struct WrkspcExecutor {
    store: Store,
}

impl WrkspcExecutor {
    /// Builds the executor over the shared store.
    ///
    /// # Arguments
    ///
    /// * `store` — the transactional store (shared with the fan-out and
    ///   moderation paths).
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

/// Cancels the wrkspc-side work a task's delivery envelope describes:
/// the A2A task row is not the execution — the cancel must reach the
/// queued intent or the in-flight attempt (H-cancel propagation).
///
/// # Arguments
///
/// * `store` — the store the delivery lives in.
/// * `task_id` — the A2A task being canceled.
///
/// # Returns
///
/// Ok when the cancel reached the work (or there is no envelope/intent
/// to cancel — the row-level cancel still applies), an internal error
/// only when the cancel could not be performed.
pub(crate) async fn cancel_enveloped_work(store: &Store, task_id: &str) -> Result<(), A2AError> {
    let Some(row) = store
        .task_get(task_id)
        .await
        .map_err(|error| A2AError::internal(error.to_string()))?
    else {
        // The row-level cancel below reports the not-found verdict.
        return Ok(());
    };
    let blob: serde_json::Value = serde_json::from_str(&row.task_json)
        .map_err(|error| A2AError::internal(error.to_string()))?;
    let Some(message_id) = blob["metadata"]["delivery"]["message_id"].as_str() else {
        return Ok(()); // no wrkspc delivery envelope: nothing to cancel
    };
    let Some(recipient) = blob["metadata"]["delivery"]["recipient_peer"].as_str() else {
        return Ok(());
    };
    match store
        .request_cancel(message_id, recipient, "a2a-cancel")
        .await
    {
        Ok(CancelOutcome::IntentCanceled) => {
            tracing::info!(task_id, "a2a cancel closed the queued execution intent")
        }
        Ok(CancelOutcome::Attempt { attempt_id }) => tracing::info!(
            task_id,
            %attempt_id,
            "a2a cancel requested on the in-flight attempt"
        ),
        // Already terminal or no intent row: the cancel is idempotent
        // or has nothing to reach — proceed to the row-level cancel.
        Err(wrkspc_core::StoreError::IntentTerminal { .. })
        | Err(wrkspc_core::StoreError::NotFound { .. }) => {}
        Err(error) => {
            return Err(A2AError::internal(format!(
                "cancel propagation failed: {error}"
            )));
        }
    }
    Ok(())
}

impl AgentExecutor for WrkspcExecutor {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        // Never log message text: stderr and supervisor logs are not a
        // secret-safe channel (P08). Byte/part counts carry the
        // operational signal without the content.
        let parts = ctx.message.as_ref().map(|m| m.parts.len()).unwrap_or(0);
        let bytes: usize = ctx
            .message
            .as_ref()
            .map(|m| {
                m.parts
                    .iter()
                    .filter_map(|p| match &p.content {
                        a2a::PartContent::Text(text) => Some(text.len()),
                        _ => None,
                    })
                    .sum()
            })
            .unwrap_or(0);
        tracing::info!(
            task_id = %ctx.task_id,
            parts,
            text_bytes = bytes,
            continuation = ctx.stored_task.is_some(),
            "a2a task accepted; execution pending wrkspc routing"
        );
        // A2A v1.0 §3.1.2: the stream MUST begin with the Task object. The
        // SDK does not synthesize it for fresh executions — the executor
        // emits the snapshot itself, then ends. No terminal event: the
        // task persists in its non-terminal state; the dispatcher drives
        // it onward.
        //
        // A continuation PRESERVES the stored record (P05): metadata
        // (the delivery envelope), artifacts, and history survive, the
        // current input is appended exactly once (deduped by message
        // id), and a progressing task keeps its state instead of being
        // reset to Submitted.
        let stored = ctx.stored_task.clone();
        let mut history = stored
            .as_ref()
            .and_then(|t| t.history.clone())
            .unwrap_or_default();
        if let Some(current) = &ctx.message
            && !history.iter().any(|h| h.message_id == current.message_id)
        {
            history.push(current.clone());
        }
        let state = match stored.as_ref().map(|t| &t.status.state) {
            Some(state) if *state == TaskState::Working => TaskState::Working,
            _ => TaskState::Submitted,
        };
        let task = Task {
            id: ctx.task_id.clone(),
            context_id: ctx.context_id.clone(),
            status: TaskStatus {
                state,
                message: Some(Message {
                    role: Role::Agent,
                    message_id: a2a::new_message_id(),
                    task_id: Some(ctx.task_id.clone()),
                    context_id: Some(ctx.context_id.clone()),
                    parts: vec![Part::text(
                        "accepted; awaiting wrkspc routing (pilot dispatcher, unit 5)".to_string(),
                    )],
                    metadata: None,
                    extensions: None,
                    reference_task_ids: None,
                }),
                timestamp: Some(chrono::Utc::now()),
            },
            artifacts: stored.as_ref().and_then(|t| t.artifacts.clone()),
            history: Some(history),
            metadata: stored.as_ref().and_then(|t| t.metadata.clone()),
        };
        Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let store = self.store.clone();
        let task_id = ctx.task_id.clone();
        let context_id = ctx.context_id.clone();
        tracing::info!(task_id = %ctx.task_id, "a2a task cancel requested");
        Box::pin(stream::once(async move {
            // Cancel the WORK first (queued intent or in-flight
            // attempt): a bare status update would report cancellation
            // that never happened (H-cancel propagation).
            cancel_enveloped_work(&store, &task_id).await?;

            // Then persist the row-level terminal state, preserving the
            // record (envelope, history, metadata survive — H-13).
            let current = store
                .task_get(&task_id)
                .await
                .map_err(|error| A2AError::internal(error.to_string()))?
                .ok_or_else(|| A2AError::task_not_found(&task_id))?;
            let mut snapshot: serde_json::Value = serde_json::from_str(&current.task_json)
                .map_err(|error| A2AError::internal(error.to_string()))?;
            snapshot["status"] = serde_json::json!({
                "state": "TASK_STATE_CANCELED",
                "timestamp": chrono::Utc::now(),
            });
            let canceled_json = serde_json::to_string(&snapshot)
                .map_err(|error| A2AError::internal(error.to_string()))?;
            match store
                .task_begin_cancel(&task_id, "TASK_STATE_CANCELED", &canceled_json)
                .await
            {
                Ok(_) => {}
                // The SDK drives begin_cancel AND this executor cancel:
                // when the store adapter already persisted the canceled
                // state, this leg is idempotent.
                Err(wrkspc_core::StoreError::TaskTerminal { state, .. })
                    if state == "TASK_STATE_CANCELED" => {}
                Err(wrkspc_core::StoreError::TaskTerminal { task_id, state, .. }) => {
                    return Err(A2AError::task_not_cancelable(&format!(
                        "task {task_id} is terminal ({state}) and cannot be canceled"
                    )));
                }
                Err(other) => return Err(A2AError::internal(other.to_string())),
            }
            Ok(StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                task_id,
                context_id,
                status: TaskStatus {
                    state: TaskState::Canceled,
                    message: None,
                    timestamp: Some(chrono::Utc::now()),
                },
                metadata: None,
            }))
        }))
    }
}
