//! wrkspc-ui — the operator surface: a loopback bearer-token-gated HTTP
//! API over the committed state, plus the embedded browser UI.
//!
//! The API is the pilot's UI for both roles (D-10): conversations,
//! transcripts (redacted on the way out), the cursor-based event feed,
//! moderation controls (fences, held deliveries, guidance, cancel), and
//! unknown-attempt display with operator reconciliation — never silent.

pub mod api;
pub mod redact;
pub mod server;
pub mod static_ui;

pub use api::CancelRequester;
pub use server::{UiConfig, UiServer};
