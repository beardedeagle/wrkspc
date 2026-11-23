//! The hub executor: accepts inbound A2A work without claiming success.
//!
//! The executor records acceptance as a `submitted` status update (a
//! continuation keeps its existing non-terminal state) and ends its event
//! stream WITHOUT a terminal event, so the task persists in its
//! non-terminal state. Execution state is driven by the dispatcher
//! (unit 5): a completed turn is never claimed as task success (D-3
//! predicate).

use a2a::event::StreamResponse;
use a2a::{A2AError, Message, Part, Role, Task, TaskState, TaskStatus, TaskStatusUpdateEvent};
use a2a_server::{AgentExecutor, ExecutorContext};
use futures::stream;
use futures::stream::BoxStream;
use wrkspc_core::{CancelOutcome, Store};

/// The hub executor: acceptance only, no completion claims.
#[derive(Debug, Clone)]
pub struct WrkspcExecutor {
    store: Store,
}

impl WrkspcExecutor {
    /// Builds the executor over the shared store.
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
        .map_err(crate::store_adapter::internal_store_error)?
    else {
        // The row-level cancel below reports the not-found verdict.
        return Ok(());
    };
    // The typed envelope — a blob without one carries nothing to
    // cancel.
    let Some(envelope) = wrkspc_core::DeliveryEnvelope::from_task_json(&row.task_json) else {
        return Ok(());
    };
    let message_id = envelope.message_id;
    let recipient = envelope.recipient_peer;
    match store
        .request_cancel(&message_id, &recipient, "a2a-cancel")
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
        Err(
            wrkspc_core::StoreError::IntentTerminal { .. }
            | wrkspc_core::StoreError::NotFound { .. },
        ) => {}
        Err(error) => {
            return Err(crate::store_adapter::internal_store_error(error));
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
        let parts = ctx.message.as_ref().map_or(0, |m| m.parts.len());
        let bytes: usize = ctx.message.as_ref().map_or(0, |m| {
            m.parts
                .iter()
                .filter_map(|p| match &p.content {
                    a2a::PartContent::Text(text) => Some(text.len()),
                    _ => None,
                })
                .sum()
        });
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
        // A continuation preserves only the states a task can actually
        // progress THROUGH — Working, InputRequired (awaiting approval),
        // AuthRequired — instead of resetting a progressing task to
        // Submitted. Everything else falls back to Submitted:
        // `Unspecified` is the reserved zero value the wire mapping
        // must never re-emit (state_mapping.rs), and a terminal record
        // re-emits its own terminal state idempotently through the SDK,
        // never this acceptance leg.
        let state = match stored.as_ref().map(|t| &t.status.state) {
            Some(
                state @ (TaskState::Working | TaskState::InputRequired | TaskState::AuthRequired),
            ) => state.clone(),
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
                    parts: vec![Part::text("accepted; awaiting wrkspc routing".to_string())],
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
                .map_err(crate::store_adapter::internal_store_error)?
                .ok_or_else(|| A2AError::task_not_found(&task_id))?;
            let mut snapshot: serde_json::Value = serde_json::from_str(&current.task_json)
                .map_err(|error| A2AError::internal(error.to_string()))?;
            // A malformed stored blob (not an object) is a store
            // invariant break — error out, never panic on index
            // assignment into a non-object value.
            let snapshot = snapshot
                .as_object_mut()
                .ok_or_else(|| A2AError::internal("task payload is not an object".to_string()))?;
            snapshot.insert(
                "status".to_string(),
                serde_json::json!({
                    "state": crate::state_mapping::task_state_literal(&TaskState::Canceled),
                    "timestamp": chrono::Utc::now(),
                }),
            );
            let canceled_json = serde_json::to_string(&snapshot)
                .map_err(|error| A2AError::internal(error.to_string()))?;
            match store
                .task_begin_cancel(
                    &task_id,
                    crate::state_mapping::task_state_literal(&TaskState::Canceled),
                    &canceled_json,
                )
                .await
            {
                Ok(_) => {}
                // The SDK drives begin_cancel AND this executor cancel:
                // when the store adapter already persisted the canceled
                // state, this leg is idempotent.
                Err(wrkspc_core::StoreError::TaskTerminal { state, .. })
                    if state == crate::state_mapping::task_state_literal(&TaskState::Canceled) => {}
                Err(wrkspc_core::StoreError::TaskTerminal { task_id, state, .. }) => {
                    return Err(A2AError::task_not_cancelable(&format!(
                        "task {task_id} is terminal ({state}) and cannot be canceled"
                    )));
                }
                Err(other) => {
                    return Err(crate::store_adapter::internal_store_error(other));
                }
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
