//! Node-side delivery polling (D-6 A2 contract, poll leg).
//!
//! The node dials the hub (never the reverse) and pulls delivery tasks
//! from its own host queue. Each delivery is mirrored into the local
//! store — peers, the hub-owned conversation id, and the message (the
//! hub message id is the local idempotency key) — which hands execution
//! intent to the local dispatcher. Only after the mirror is durable does
//! the node ack, and only a successful ack leaves the local
//! processed-marker, so an interrupted poll re-runs idempotently.

pub mod client;
pub mod delivery;
pub mod outbound;
pub mod poll_loop;
pub mod presence;
#[cfg(test)]
mod tests;

pub use client::{build_hub_client, build_hub_client_with_timeout};
pub use delivery::{Delivery, PollOutcome, process_deliveries};
pub(crate) use outbound::sync_outbound;
pub use poll_loop::{PollLoopConfig, run_poll_loop};
pub use presence::{PresenceDecl, PresenceSource};
pub(crate) use presence::{sync_conversations, sync_roster, upload_presence};

#[cfg(test)]
use wrkspc_core::{Store, TaskRow};
