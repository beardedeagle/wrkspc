//! The session handle: a tokio task owning one ACP connection and
//! serializing all session operations over it (D-9: direct tasks +
//! `SessionHandle`).
//!
//! Commands flow in over an mpsc channel; `session/update` notifications
//! flow out as [`SessionEvent`]s. Prompts are serialized because the loop
//! awaits each prompt response before servicing the next command — the
//! HLD's "serialize executable delivery per ACP session". Cancellation is
//! the `session/cancel` notification; confirmation arrives as the in-flight
//! prompt's response with `StopReason::Cancelled` (HLD round-2 fix H2).

use crate::error::{AcpError, Result};
use crate::transport::AcpTransport;
use agent_client_protocol::Agent;
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    CancelNotification, ContentBlock, InitializeRequest, McpServer, NewSessionRequest,
    PromptRequest, SessionConfigId, SessionConfigOptionValue, SessionUpdate,
    SetSessionConfigOptionRequest, StopReason,
};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

/// A session update delivered by the adapter (`session/update`).
#[derive(Debug, Clone)]
pub struct SessionEvent {
    /// Owning session.
    pub session_id: String,
    /// The update payload.
    pub update: SessionUpdate,
}

enum Command {
    NewSession {
        cwd: PathBuf,
        mcp_servers: Vec<McpServer>,
        respond: oneshot::Sender<Result<String>>,
    },
    Prompt {
        session_id: String,
        blocks: Vec<ContentBlock>,
        respond: oneshot::Sender<Result<StopReason>>,
    },
    SetConfigOption {
        session_id: String,
        config_id: String,
        value: String,
        respond: oneshot::Sender<Result<()>>,
    },
    Cancel {
        session_id: String,
    },
    Shutdown,
}

/// Owns the adapter connection task; cheap to clone, safe to share.
#[derive(Debug)]
pub struct SessionHandle {
    cmd_tx: mpsc::Sender<Command>,
    ev_rx: tokio::sync::broadcast::Receiver<SessionEvent>,
    /// The transport task's terminal state: `None` while running,
    /// `Some(reason)` once the connection has ended (E04/H-15 — the
    /// registry evicts dead cached sessions on this signal).
    closed: tokio::sync::watch::Receiver<Option<String>>,
}

impl Clone for SessionHandle {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            ev_rx: self.ev_rx.resubscribe(),
            closed: self.closed.clone(),
        }
    }
}

impl SessionHandle {
    /// Spawns the session task on `transport`.
    ///
    /// # Arguments
    ///
    /// * `transport` — the ACP transport (stdio today, HTTP later).
    ///
    /// # Returns
    ///
    /// The handle plus a receiver of [`SessionEvent`]s for the lifetime of
    /// the connection.
    pub fn spawn(
        transport: impl AcpTransport,
    ) -> (Self, tokio::sync::broadcast::Receiver<SessionEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(64);
        let (ev_tx, ev_rx) = tokio::sync::broadcast::channel::<SessionEvent>(256);
        let task: crate::transport::ConnectionTask = Box::new(move |connection| {
            Box::pin(async move {
                run_session_loop(connection, cmd_rx).await;
                Ok(())
            })
        });
        let (closed_tx, closed_rx) = tokio::sync::watch::channel::<Option<String>>(None);
        tokio::spawn(async move {
            let outcome = transport.with_connection(ev_tx, task).await;
            let reason = match outcome {
                Ok(()) => "transport ended".to_string(),
                Err(error) => format!("transport failed: {error}"),
            };
            let _ = closed_tx.send(Some(reason));
        });
        (
            Self {
                cmd_tx,
                ev_rx: ev_rx.resubscribe(),
                closed: closed_rx,
            },
            ev_rx,
        )
    }

    /// Returns a fresh receiver of this session's events (the dispatcher
    /// drive task subscribes per turn).
    ///
    /// # Returns
    ///
    /// A broadcast receiver; it sees only events emitted after
    /// subscription.
    pub fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
        self.ev_rx.resubscribe()
    }

    /// Subscribes to this session's terminal state: `None` while the
    /// transport is alive, `Some(reason)` once it has ended. A session
    /// whose transport died must be evicted, never re-promoted (E04/H-15).
    ///
    /// # Returns
    ///
    /// The terminal-state watch.
    pub fn closed(&self) -> tokio::sync::watch::Receiver<Option<String>> {
        self.closed.clone()
    }

    /// Creates a new session with the given working directory.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Protocol`](AcpError::Protocol) when the adapter
    /// rejects the request and [`AcpError::NotRunning`](AcpError::NotRunning)
    /// when the session task is gone.
    pub async fn new_session(&self, cwd: PathBuf) -> Result<String> {
        self.new_session_with_mcp(cwd, Vec::new()).await
    }

    /// Creates a new session with the given working directory and MCP
    /// server injection (ACP `session/new` `mcpServers` — the harness
    /// sees the wrkspc tools from its first turn).
    ///
    /// # Arguments
    ///
    /// * `cwd` — the session's working directory.
    /// * `mcp_servers` — the MCP servers to inject (stdio today; HTTP when
    ///   the harness advertises `mcp_capabilities.http`).
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Protocol`](AcpError::Protocol) when the adapter
    /// rejects the request and [`AcpError::NotRunning`](AcpError::NotRunning)
    /// when the session task is gone.
    pub async fn new_session_with_mcp(
        &self,
        cwd: PathBuf,
        mcp_servers: Vec<McpServer>,
    ) -> Result<String> {
        let (respond, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::NewSession {
                cwd,
                mcp_servers,
                respond,
            })
            .await
            .map_err(|_| AcpError::NotRunning)?;
        rx.await.map_err(|_| AcpError::NotRunning)?
    }

    /// Sends a prompt and waits for the turn's stop reason.
    ///
    /// # Arguments
    ///
    /// * `session_id` — the target session.
    /// * `blocks` — the prompt content blocks.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Protocol`](AcpError::Protocol) on adapter error
    /// and [`AcpError::NotRunning`](AcpError::NotRunning) when the session
    /// task is gone.
    pub async fn prompt(
        &self,
        session_id: impl Into<String>,
        blocks: Vec<ContentBlock>,
    ) -> Result<StopReason> {
        let (respond, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Prompt {
                session_id: session_id.into(),
                blocks,
                respond,
            })
            .await
            .map_err(|_| AcpError::NotRunning)?;
        rx.await.map_err(|_| AcpError::NotRunning)?
    }

    /// Sets a session configuration option (ACP `session/set_config_option`).
    ///
    /// The antigravity binding uses `config_id = "model"` for D-4b's
    /// mandatory per-session model pinning.
    ///
    /// # Arguments
    ///
    /// * `session_id` — the target session.
    /// * `config_id` — the option's config id (e.g. `"model"`).
    /// * `value` — the option value.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Protocol`](AcpError::Protocol) when the adapter
    /// rejects the request and [`AcpError::NotRunning`](AcpError::NotRunning)
    /// when the session task is gone.
    pub async fn set_config_option(
        &self,
        session_id: impl Into<String>,
        config_id: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<()> {
        let (respond, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SetConfigOption {
                session_id: session_id.into(),
                config_id: config_id.into(),
                value: value.into(),
                respond,
            })
            .await
            .map_err(|_| AcpError::NotRunning)?;
        rx.await.map_err(|_| AcpError::NotRunning)?
    }

    /// Requests cancellation of the session's in-flight turn.
    ///
    /// This is the `session/cancel` notification — it has no response.
    /// Confirmation arrives as the in-flight prompt's response with
    /// `StopReason::Cancelled`; its absence (disconnect before turn end)
    /// leaves the attempt unknown.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::NotRunning`](AcpError::NotRunning) when the
    /// session task is gone.
    pub async fn cancel(&self, session_id: impl Into<String>) -> Result<()> {
        self.cmd_tx
            .send(Command::Cancel {
                session_id: session_id.into(),
            })
            .await
            .map_err(|_| AcpError::NotRunning)
    }

    /// Shuts the session task down and tears down the adapter connection.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::NotRunning`](AcpError::NotRunning) when the
    /// session task is already gone.
    pub async fn shutdown(self) -> Result<()> {
        self.cmd_tx
            .send(Command::Shutdown)
            .await
            .map_err(|_| AcpError::NotRunning)
    }
}

async fn run_session_loop(connection: ConnectionTo<Agent>, mut cmd_rx: mpsc::Receiver<Command>) {
    // Initialize once, before serving any session command. If the adapter
    // cannot initialize, fail every command explicitly rather than hang.
    let init = connection
        .send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await;
    let initialized = init.is_ok();
    if let Err(error) = &init {
        tracing::error!(
            %error,
            "adapter initialize failed; session commands will be refused",
        );
    }

    // Prompts are serialized (one in-flight turn per session) but cancel and
    // shutdown are serviced mid-turn: the prompt runs as a subtask while the
    // loop keeps receiving commands. The pending queue is BOUNDED (E11):
    // beyond the admission bound, a prompt is rejected as definitely unsent
    // instead of growing the queue without limit.
    const MAX_PENDING: usize = 64;
    let mut pending: std::collections::VecDeque<(
        String,
        Vec<ContentBlock>,
        oneshot::Sender<Result<StopReason>>,
    )> = std::collections::VecDeque::new();
    let mut in_flight = false;
    let mut done_rx: Option<oneshot::Receiver<()>> = None;

    loop {
        tokio::select! {
            _ = async {
                if let Some(rx) = &mut done_rx {
                    let _ = rx.await;
                }
            }, if done_rx.is_some() => {
                done_rx = None;
                in_flight = false;
                // Skip queued prompts whose caller has abandoned them:
                // a dropped receiver means the prompt was never sent,
                // so it must not start later (E11).
                while let Some((session_id, blocks, respond)) = pending.pop_front() {
                    if respond.is_closed() {
                        continue;
                    }
                    start_prompt(&connection, &mut done_rx, session_id, blocks, respond);
                    in_flight = true;
                    break;
                }
            }
            command = cmd_rx.recv() => {
                let Some(command) = command else { break };
                match command {
                    Command::Shutdown => break,
                    Command::NewSession {
                        cwd,
                        mcp_servers,
                        respond,
                    } => {
                        let outcome = if initialized {
                            connection
                                .send_request(
                                    NewSessionRequest::new(cwd).mcp_servers(mcp_servers),
                                )
                                .block_task()
                                .await
                                .map(|r| r.session_id.to_string())
                                .map_err(AcpError::from)
                        } else {
                            Err(AcpError::NotRunning)
                        };
                        let _ = respond.send(outcome);
                    }
                    Command::Prompt {
                        session_id,
                        blocks,
                        respond,
                    } => {
                        if !initialized {
                            let _ = respond.send(Err(AcpError::NotRunning));
                        } else if in_flight {
                            if pending.len() >= MAX_PENDING {
                                // Definitely unsent: reject loudly instead
                                // of queueing without bound (E11).
                                let _ = respond.send(Err(AcpError::QueueFull));
                            } else {
                                pending.push_back((session_id, blocks, respond));
                            }
                        } else {
                            start_prompt(&connection, &mut done_rx, session_id, blocks, respond);
                            in_flight = true;
                        }
                    }
                    Command::SetConfigOption {
                        session_id,
                        config_id,
                        value,
                        respond,
                    } => {
                        let outcome = if initialized {
                            connection
                                .send_request(SetSessionConfigOptionRequest::new(
                                    session_id,
                                    SessionConfigId::new(config_id),
                                    SessionConfigOptionValue::value_id(value),
                                ))
                                .block_task()
                                .await
                                .map(|_| ())
                                .map_err(AcpError::from)
                        } else {
                            Err(AcpError::NotRunning)
                        };
                        let _ = respond.send(outcome);
                    }
                    Command::Cancel { session_id } => {
                        if initialized
                            && let Err(error) =
                                connection.send_notification(CancelNotification::new(session_id))
                        {
                            tracing::warn!(%error, "session/cancel notification failed");
                        }
                    }
                }
            }
        }
    }
}

/// Spawns one prompt turn as a subtask. On completion the caller's channel
/// receives the outcome and the serialization slot is released via `done_tx`.
fn start_prompt(
    connection: &ConnectionTo<Agent>,
    done_slot: &mut Option<oneshot::Receiver<()>>,
    session_id: String,
    blocks: Vec<ContentBlock>,
    respond: oneshot::Sender<Result<StopReason>>,
) {
    let connection = connection.clone();
    let (done_tx, done_rx) = oneshot::channel();
    *done_slot = Some(done_rx);
    tokio::spawn(async move {
        let outcome = connection
            .send_request(PromptRequest::new(session_id, blocks))
            .block_task()
            .await
            .map(|r| r.stop_reason)
            .map_err(AcpError::from);
        let _ = respond.send(outcome);
        let _ = done_tx.send(());
    });
}
