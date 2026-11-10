//! wrkspc-acp error types.

/// Errors from the ACP client layer.
#[derive(Debug, thiserror::Error)]
pub enum AcpError {
    /// The adapter process could not be launched.
    #[error("adapter spawn failed: {0}")]
    Spawn(String),

    /// The ACP peer returned a protocol error.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The session task is not running (already shut down or failed).
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
}

/// Result alias for the ACP layer.
pub type Result<T> = std::result::Result<T, AcpError>;

impl From<agent_client_protocol::Error> for AcpError {
    fn from(value: agent_client_protocol::Error) -> Self {
        AcpError::Protocol(value.to_string())
    }
}
