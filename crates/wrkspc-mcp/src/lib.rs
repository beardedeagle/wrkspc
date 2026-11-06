//! wrkspc-mcp — the harness-facing MCP surface.
//!
//! One server instance is one enrolled peer's surface: stdio serving relies
//! on server-process ownership (D-3), HTTP serving requires a bearer token
//! from the listener's allowlist and binds loopback by default. The tools
//! (post/read/report) translate harness tool calls onto the transactional
//! store; the hub role never serves this surface.

pub mod server;

pub use server::{
    HttpListener, HttpListenerConfig, McpServer, PostMessageParams, ReadConversationParams,
    ReportWorkParams,
};
