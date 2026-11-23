//! wrkspc-acp error types.

/// Errors from the ACP client layer.
///
/// Whether a request was sent matters to a dispatcher: [`AcpError::QueueFull`]
/// and [`AcpError::NotRunning`] mean the request definitely never reached the
/// adapter, [`AcpError::ConnectionLost`] means it may have reached the
/// adapter but the outcome is unknown, and [`AcpError::Protocol`] means the
/// adapter received the request and answered with a refusal.
#[derive(Debug, thiserror::Error)]
pub enum AcpError {
    /// The adapter process could not be launched.
    #[error("adapter spawn failed: {0}")]
    Spawn(String),

    /// The ACP peer received the request and answered with a JSON-RPC
    /// error (a refusal). The request was NOT executed, but it did reach
    /// the peer.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The session task is not running (already shut down or failed);
    /// the request was NOT sent.
    #[error("session task is not running")]
    NotRunning,

    /// The session task failed.
    #[error("session task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// The serialized prompt queue is full (a turn is in flight and
    /// more prompts than the admission bound are already waiting).
    /// The rejected prompt was definitely NOT sent.
    #[error("session prompt queue is full")]
    QueueFull,

    /// The transport ended while a request was in flight. The request may
    /// or may not have reached the adapter, and if it did, the adapter may
    /// have acted on it: the OUTCOME IS UNKNOWN. Unlike
    /// [`AcpError::QueueFull`] and [`AcpError::NotRunning`], this is not a
    /// "definitely not sent" signal — a dispatcher must treat the turn as
    /// indeterminate, not safely retryable.
    #[error("connection lost while a request was in flight (outcome unknown): {0}")]
    ConnectionLost(String),
}

/// Result alias for the ACP layer.
pub type Result<T> = std::result::Result<T, AcpError>;

impl From<agent_client_protocol::Error> for AcpError {
    fn from(value: agent_client_protocol::Error) -> Self {
        // The SDK marks errors it produced locally when the transport ended
        // before a response arrived (clean EOF or hard termination). Those
        // mean "sent, but the outcome is unknown" — never "rejected". Every
        // other error is a peer answer (a refusal the adapter actually sent),
        // which is a definite non-execution.
        if agent_client_protocol::is_connection_terminated(&value)
            || agent_client_protocol::is_incoming_transport_closed(&value)
        {
            AcpError::ConnectionLost(value.to_string())
        } else {
            AcpError::Protocol(value.to_string())
        }
    }
}
