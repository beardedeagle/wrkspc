//! The transport seam: `SessionHandle` depends only on [`AcpTransport`].
//!
//! [`StdioTransport`] owns the adapter child process and its pipes (the only
//! implementation today, per D-3's verified transport reality). A future
//! HTTP transport owns a client and SSE stream instead and implements the
//! same trait — the handle does not change.
//!
//! The task argument is a boxed closure returning a boxed future: the ACP
//! SDK's own surface is async-closure native, and this keeps our seam
//! expressible entirely in stable bounds.

use crate::error::AcpError;
use crate::session::SessionEvent;
use agent_client_protocol::schema::v1::{
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SessionNotification,
};
use agent_client_protocol::{AcpAgent, Agent, Client, ConnectionTo};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;

/// The session-control task a transport runs with a live connection.
///
/// Returns `Ok(())` on orderly completion; the error type is the ACP SDK's.
pub type ConnectionTask = Box<
    dyn FnOnce(
            ConnectionTo<Agent>,
        ) -> Pin<
            Box<
                dyn Future<Output = std::result::Result<(), agent_client_protocol::Error>>
                    + Send
                    + 'static,
            >,
        > + Send,
>;

/// A transport that produces live ACP connections.
///
/// Implementors own process/connection lifecycle. `task` runs with a live
/// connection and receives the whole session-control loop from the
/// [`crate::session::SessionHandle`]; when `task` returns, the transport
/// tears down the child/connection.
pub trait AcpTransport: Send + 'static {
    /// Opens a connection, wires session updates and permission requests,
    /// runs `task` against it, and cleans up afterwards.
    ///
    /// # Arguments
    ///
    /// * `updates` — broadcast of `session/update` notifications (multiple
    ///   subscribers: the dispatcher drive task and future UI pumps).
    /// * `task` — the session-control loop; runs with a live connection.
    ///
    /// # Returns
    ///
    /// `Ok(())` when `task` completes normally, [`AcpError`] on failure.
    fn with_connection(
        self,
        updates: tokio::sync::broadcast::Sender<SessionEvent>,
        task: ConnectionTask,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send>>;
}

/// Stdio transport: launches the adapter command and speaks ACP over its
/// stdio pipes. The command is a single shell-style string, e.g.
/// `"npx -y @agentclientprotocol/codex-acp@2.1.1"`.
#[derive(Debug, Clone)]
pub struct StdioTransport {
    command: String,
}

impl StdioTransport {
    /// Builds a transport for the given adapter command.
    ///
    /// # Arguments
    ///
    /// * `command` — the adapter launch command (parsed by the ACP SDK).
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }
}

impl AcpTransport for StdioTransport {
    fn with_connection(
        self,
        updates: tokio::sync::broadcast::Sender<SessionEvent>,
        task: ConnectionTask,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send>> {
        Box::pin(async move {
            let agent =
                AcpAgent::from_str(&self.command).map_err(|e| AcpError::Spawn(e.to_string()))?;
            Client
                .builder()
                .on_receive_notification(
                    async move |notification: SessionNotification, _cx| {
                        let event = SessionEvent {
                            session_id: notification.session_id.to_string(),
                            update: notification.update,
                        };
                        // No subscribers yet (or a lagging one) is fine:
                        // session events are best-effort broadcasts.
                        let _ = updates.send(event);
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |request: RequestPermissionRequest, responder, connection| {
                        // No permission policy exists yet, and the first
                        // adapter-controlled option can carry a persistent
                        // grant — auto-selecting it lets a harness escalate
                        // itself to always-allow. Conservative default:
                        // refuse, loudly. Operator-routed permission
                        // requests are the tracked follow-up; the refusal
                        // is visible to the harness, which can retry with
                        // an operator-approved path.
                        tracing::warn!(
                            session = %request.session_id,
                            options = ?request.options.iter().map(|option| &option.option_id).collect::<Vec<_>>(),
                            "ACP permission request refused (no permission policy configured)"
                        );
                        let _ = connection;
                        responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(agent, async move |connection| task(connection).await)
                .await
                .map_err(|e| AcpError::Protocol(e.to_string()))
        })
    }
}
