//! Session abstraction for the dispatcher: a trait over the prompt/cancel
//! verbs the moderation engine needs.
//!
//! The production implementation wraps [`wrkspc_acp::SessionHandle`];
//! tests use fake drivers over channels. The dispatcher never talks to a
//! harness directly — it only sees this seam (the hub role never links the
//! ACP layer; the node role binds real sessions behind it).

use agent_client_protocol::v1::schema::{ContentBlock, StopReason};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use wrkspc_acp::{SessionEvent, SessionHandle};

/// The three verbs dispatch needs from a session: prompt (turn with stop
/// reason), cancel (interrupt the in-flight turn), and events (the turn's
/// streamed output, attributed while in flight).
pub trait SessionDriver: Send + Sync + 'static {
    /// Prompts the session and resolves with the turn's stop reason.
    fn prompt(
        &self,
        session_id: &str,
        blocks: Vec<ContentBlock>,
    ) -> Pin<Box<dyn Future<Output = Result<StopReason, String>> + Send + '_>>;

    /// Requests cancellation of the in-flight turn (notification; the
    /// confirmation rides the prompt's stop reason).
    fn cancel(
        &self,
        session_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;

    /// Subscribes to this session's events. Each call returns a fresh
    /// receiver that sees only events emitted after subscription — one
    /// drive task per session makes attribution unambiguous.
    fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent>;
}

impl SessionDriver for SessionHandle {
    fn prompt(
        &self,
        session_id: &str,
        blocks: Vec<ContentBlock>,
    ) -> Pin<Box<dyn Future<Output = Result<StopReason, String>> + Send + '_>> {
        let handle = self.clone();
        let session_id = session_id.to_string();
        Box::pin(async move {
            handle
                .prompt(session_id, blocks)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn cancel(
        &self,
        session_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        let handle = self.clone();
        let session_id = session_id.to_string();
        Box::pin(async move { handle.cancel(session_id).await.map_err(|e| e.to_string()) })
    }

    fn events(&self) -> tokio::sync::broadcast::Receiver<SessionEvent> {
        self.events()
    }
}

/// A bound session for a peer: the ACP session id plus its driver.
#[derive(Clone)]
pub struct SessionBinding {
    /// The peer this session executes for.
    pub peer_id: String,
    /// The ACP session id.
    pub session_id: String,
    /// The session driver.
    pub driver: Arc<dyn SessionDriver>,
}

// The driver is a trait object without Debug; the summary carries the
// observable identity instead.
impl std::fmt::Debug for SessionBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionBinding")
            .field("peer_id", &self.peer_id)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

/// Resolves a peer to its bound session (the CLI builds this from the
/// bindings table, spawning sessions on demand; tests use static fakes).
pub trait SessionProvider: Send + Sync + 'static {
    /// Returns the binding for a peer, if one is enrolled AND its
    /// session is currently usable. `None` is the conservative
    /// resolution: the dispatcher records the attempt unknown with a
    /// visible feed event rather than silently dropping it.
    fn lookup(
        &self,
        peer_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<SessionBinding>> + Send + '_>>;
}
