//! wrkspc-dispatcher — the moderation dispatch engine.
//!
//! Claims eligible execution intents (fence-gated in the store) and drives
//! them through bound sessions: dispatch → prompt (guidance-prefixed) →
//! terminal state from the ACP stop reason. Driver errors and restarts mark
//! attempts unknown; cancellation interrupts in-flight turns. The intent
//! and attempt lifecycle state machines are enforced in the store — this
//! crate orchestrates them.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod dispatcher;
pub mod driver;

pub use dispatcher::{Dispatcher, DispatcherConfig, DispatcherHandle};
pub use driver::{SessionBinding, SessionDriver, SessionProvider};
