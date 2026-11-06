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

/// The pilot executor: acceptance only, no completion claims.
#[derive(Debug, Clone)]
pub struct WrkspcExecutor;

/// Extracts the joined text of a message's text parts.
fn message_text(message: &Message) -> String {
    message
        .parts
        .iter()
        .filter_map(|part| {
            if let a2a::PartContent::Text(text) = &part.content {
                Some(text.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

impl AgentExecutor for WrkspcExecutor {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let text = ctx.message.as_ref().map(message_text).unwrap_or_default();
        tracing::info!(
            task_id = %ctx.task_id,
            text = %text,
            "a2a task accepted; execution pending wrkspc routing"
        );
        // A2A v1.0 §3.1.2: the stream MUST begin with the Task object. The
        // SDK does not synthesize it for fresh executions — the executor
        // emits the submitted snapshot itself, then ends. No terminal event:
        // the task persists in `submitted`; the dispatcher drives it onward.
        let task = Task {
            id: ctx.task_id.clone(),
            context_id: ctx.context_id.clone(),
            status: TaskStatus {
                state: TaskState::Submitted,
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
            artifacts: None,
            history: ctx.stored_task.and_then(|stored| stored.history),
            metadata: None,
        };
        Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        tracing::info!(task_id = %ctx.task_id, "a2a task cancel requested");
        let update = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: ctx.task_id.clone(),
            context_id: ctx.context_id.clone(),
            status: TaskStatus {
                state: TaskState::Canceled,
                message: None,
                timestamp: Some(chrono::Utc::now()),
            },
            metadata: None,
        });
        Box::pin(stream::once(async move { Ok(update) }))
    }
}
