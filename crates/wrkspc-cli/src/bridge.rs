//! The MCP bridge: a stdio MCP server for one enrolled peer, spawned by
//! the harness's ACP adapter via the injected `session/new` config.
//!
//! Opens the node's database shared (no dispatcher lock — that role
//! belongs to the node) and serves the peer's scoped tools over stdio.

use std::path::PathBuf;

/// `wrkspc mcp-bridge` arguments.
#[derive(Debug, clap::Args)]
pub struct McpBridgeArgs {
    /// The node's database file.
    #[arg(long)]
    pub db: PathBuf,
    /// The peer whose surface to serve.
    #[arg(long)]
    pub peer: String,
}

/// Serves the bridge over stdio until the parent closes the stream.
///
/// # Arguments
///
/// * `args` — the bridge arguments.
///
/// # Errors
///
/// Returns a boxed error on open or serve failure.
pub async fn run_bridge(args: McpBridgeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let store = wrkspc_core::Store::open_shared(&args.db, "wrkspc-mcp-bridge").await?;
    let server = wrkspc_mcp::McpServer::new(store, &args.peer);
    tracing::info!(peer = %args.peer, "mcp bridge serving stdio");
    server.serve_stdio().await?;
    Ok(())
}
