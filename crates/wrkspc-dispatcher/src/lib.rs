//! wrkspc-dispatcher — the moderation dispatch engine.
//!
//! Claims eligible execution intents (fence-gated in the store) and drives
//! them through bound sessions: dispatch → prompt (guidance-prefixed) →
//! terminal state from the ACP stop reason. Driver errors and restarts mark
//! attempts unknown; cancellation interrupts in-flight turns. The nine-state
//! lifecycle mapping is enforced in the store's state machines — this crate
//! orchestrates them.

pub mod dispatcher;
pub mod driver;

pub use dispatcher::{Dispatcher, DispatcherConfig, DispatcherHandle};
pub use driver::{SessionBinding, SessionDriver, SessionProvider};
