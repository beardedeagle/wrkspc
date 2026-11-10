//! wrkspc binary entrypoint: tracing + CLI dispatch over the library.

use clap::Parser;
use wrkspc_cli::{Cli, Command, HubCommand, NodeCommand};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Before anything reads a key: no core dump may ever capture one.
    let hardened = wrkspc_cli::harden::forbid_core_dumps();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "wrkspc=info".into()),
        )
        // Diagnostics belong on stderr: `mcp-bridge` speaks JSON-RPC on
        // stdout, and a log line there corrupts MCP initialization in any
        // harness that parses stdout strictly.
        .with_writer(std::io::stderr)
        .init();
    if let Err(error) = hardened {
        tracing::warn!(
            %error,
            "core dumps could not be disabled; a crash could write key material to a dump"
        );
    }

    let cli = Cli::parse();
    match cli.command {
        Command::Node {
            node: NodeCommand::Run(args),
        } => wrkspc_cli::node::run_node(args).await,
        Command::Hub {
            hub: HubCommand::Run(args),
        } => wrkspc_cli::hub::run_hub(args).await,
        Command::McpBridge(args) => wrkspc_cli::bridge::run_bridge(args).await,
        Command::Skill { skill } => wrkspc_cli::skill::run_skill(skill).await,
        Command::UiToken { node, db, file } => {
            wrkspc_cli::secrets::run_ui_token(node, db, file).await
        }
    }
}
