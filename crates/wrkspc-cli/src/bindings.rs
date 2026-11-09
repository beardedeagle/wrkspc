//! Bindings configuration: which harness each enrolled peer runs.
//!
//! `command` is a shell-style command line (program + args), matching the
//! ACP stdio transport's spawn shape. The MCP bridge is injected as a
//! stdio `McpServer` pointing at this same binary's `mcp-bridge`
//! subcommand, so the harness sees the wrkspc tools from its first turn.

use agent_client_protocol::schema::v1::{McpServer, McpServerStdio};
use std::path::PathBuf;

/// The bindings file: one entry per enrolled harness peer.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BindingsFile {
    /// The bindings.
    pub binding: Vec<BindingConfig>,
}

/// One harness peer's wiring.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BindingConfig {
    /// Peer id (the identity the store and dispatcher use).
    pub peer_id: String,
    /// Display name.
    pub harness: String,
    /// Session working directory.
    pub cwd: PathBuf,
    /// Shell-style command that spawns the ACP adapter
    /// (e.g. `npx -y @agentclientprotocol/codex-acp@2.1.1`).
    pub command: String,
    /// Optional model pin (D-4b): applied via ACP
    /// `session/set_config_option` with `configId = "model"` right after
    /// `session/new`. Adapters without the method reject the call — the
    /// binding must not carry a pin they cannot honor.
    #[serde(default)]
    pub model: Option<String>,
    /// Serve this peer's wrkspc MCP tools over loopback HTTP and register
    /// them with the harness's native MCP config (agy path). Adapters
    /// without ACP `mcpServers` injection need this: the tools reach the
    /// harness through its own config instead of `session/new`.
    #[serde(default)]
    pub mcp_http: bool,
}

impl BindingsFile {
    /// Parses a bindings TOML file.
    ///
    /// # Arguments
    ///
    /// * `path` — the file to read.
    ///
    /// # Returns
    ///
    /// The parsed bindings.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` on read failure or a `toml` error on parse
    /// failure.
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}

/// Builds the stdio MCP injection config for a peer: this binary's
/// `mcp-bridge` subcommand pointed at the node's database.
///
/// # Arguments
///
/// * `db_path` — the node's database file.
/// * `peer_id` — the peer whose surface the bridge serves.
/// * `hub_connected` — whether the node runs against a hub; the bridge
///   mirrors the flag so hub-mode posts never create local execution
///   intents.
///
/// # Returns
///
/// The ACP `McpServer` to inject at `session/new`.
pub fn bridge_injection(
    db_path: &std::path::Path,
    peer_id: &str,
    hub_connected: bool,
) -> McpServer {
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("wrkspc"));
    let mut args = vec![
        "mcp-bridge".to_string(),
        "--db".to_string(),
        db_path.display().to_string(),
        "--peer".to_string(),
        peer_id.to_string(),
    ];
    if hub_connected {
        args.push("--hub-connected".to_string());
    }
    McpServer::Stdio(McpServerStdio::new("wrkspc", exe).args(args))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bindings_toml() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("wrkspc-bindings-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
[[binding]]
peer_id = "codex"
harness = "codex-acp"
cwd = "/tmp/project"
command = "npx -y @agentclientprotocol/codex-acp@2.1.1"

[[binding]]
peer_id = "agy"
harness = "antigravity-acp"
cwd = "/tmp/project"
command = "npx -y antigravity-acp@1.2.0"
model = "gemini-3.6-flash-medium"
"#,
        )
        .expect("write");
        let bindings = BindingsFile::load(&path).expect("parse");
        assert_eq!(bindings.binding.len(), 2);
        assert_eq!(bindings.binding[0].peer_id, "codex");
        assert_eq!(bindings.binding[0].harness, "codex-acp");
        assert_eq!(
            bindings.binding[0].command,
            "npx -y @agentclientprotocol/codex-acp@2.1.1"
        );
        assert!(bindings.binding[0].model.is_none(), "codex carries no pin");
        assert_eq!(
            bindings.binding[1].model.as_deref(),
            Some("gemini-3.6-flash-medium")
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn injection_points_at_the_bridge_subcommand() {
        let server = bridge_injection(std::path::Path::new("/tmp/wrkspc.db"), "codex", false);
        let McpServer::Stdio(stdio) = server else {
            panic!("expected stdio injection");
        };
        assert_eq!(stdio.name, "wrkspc");
        assert!(stdio.command.to_string_lossy().contains("wrkspc"));
        assert_eq!(
            stdio.args,
            vec!["mcp-bridge", "--db", "/tmp/wrkspc.db", "--peer", "codex"]
        );
    }

    /// Hub-connected nodes inject the flag: the bridge mirrors hub mode,
    /// so harness posts never create local execution intents.
    #[test]
    fn hub_connected_injection_carries_the_flag() {
        let server = bridge_injection(std::path::Path::new("/tmp/wrkspc.db"), "codex", true);
        let McpServer::Stdio(stdio) = server else {
            panic!("expected stdio injection");
        };
        assert_eq!(
            stdio.args,
            vec![
                "mcp-bridge",
                "--db",
                "/tmp/wrkspc.db",
                "--peer",
                "codex",
                "--hub-connected"
            ]
        );
    }
}
