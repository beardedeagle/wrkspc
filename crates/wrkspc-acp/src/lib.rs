//! wrkspc-acp — the ACP client: transports, the session handle, and the
//! stop-reason → lifecycle mapping.
//!
//! The HLD's transport seam lives in [`transport::AcpTransport`]:
//! [`transport::StdioTransport`] owns the adapter child process today; a
//! future HTTP transport implements the same trait and
//! [`session::SessionHandle`] does not change.

pub mod error;
pub mod session;
pub mod stop;
pub mod transport;

pub use error::{AcpError, Result};
pub use session::{SessionEvent, SessionHandle};
pub use stop::stop_reason_to_attempt_state;
pub use transport::{AcpTransport, StdioTransport};
