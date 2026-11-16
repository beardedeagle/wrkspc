//! wrkspc-a2a — the agent-facing coordination surface.
//!
//! The pilot runs this in-process as the coordinator role. It serves the
//! v1.0 agent card, a `TaskStore` impl over the wrkspc store, and the six
//! core A2A methods (`send_message`, `send_streaming_message`, `get_task`,
//! `list_tasks`, `cancel_task`, `subscribe_to_task`).
//!
//! The executor deliberately does NOT complete tasks: acceptance is
//! recorded as `submitted` with a status message (a continuation keeps
//! its existing non-terminal state), and execution state is driven by
//! the dispatcher (unit 5). A completed *turn* is never claimed as task
//! success (D-3 predicate).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod executor;
pub mod server;
pub mod state_mapping;
pub mod store_adapter;

pub use executor::WrkspcExecutor;
pub use server::{
    A2aSurface, build_agent_card, build_peer_card, cert_serial_hex, host_identity, peer_identity,
};
pub use state_mapping::LifecycleState;
pub use store_adapter::WrkspcTaskStore;


/// The per-connection policy verdict from the serve-loop callback.
///
/// The serve loop resolves the presented certificate chain into one of
/// these before any HTTP is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerDecision {
    /// Drop the connection before any HTTP.
    Deny,
    /// Serve the connection without an `x-wrkspc-peer` stamp. Only
    /// reachable when the acceptor makes client authentication
    /// optional and no certificate was presented.
    Unstamped,
    /// Serve the connection, stamping every request with this
    /// identity.
    Stamped(String),
}
