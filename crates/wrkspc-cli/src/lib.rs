//! wrkspc — coordination hub for humans and AI agents.
//!
//! `wrkspc node run` is the pilot (sessions + dispatcher + UI); `wrkspc
//! hub run` is the coordinator role (A2A surface + shared store + UI, no
//! harness control); `mcp-bridge` is spawned by harness ACP adapters via
//! the injected session config.
//!
//! The library target exposes the role modules so integration tests can
//! drive the real poll client and hub wiring; the binary is a thin
//! entrypoint over it.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod acceptance;
pub mod bindings;
pub mod bridge;
pub mod delivery;
pub mod harden;
pub mod hub;
pub mod node;
pub mod poll;
pub mod presence;
pub mod secrets;
pub mod sessions;
pub mod skill;
pub mod util;

use clap::{Parser, Subcommand};

/// Coordination hub for humans and AI agents.
#[derive(Debug, Parser)]
#[command(name = "wrkspc", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Runs the pilot node (store + sessions + dispatcher + UI).
    Node {
        #[command(subcommand)]
        node: NodeCommand,
    },
    /// Runs the coordinator hub (A2A surface + shared store; no harness
    /// control — remote coordination, never remote execution).
    Hub {
        #[command(subcommand)]
        hub: HubCommand,
    },
    /// Serves one peer's MCP tools over stdio (spawned by harnesses).
    McpBridge(bridge::McpBridgeArgs),
    /// Installs, removes, or checks the wrkspc harness skill.
    Skill {
        #[command(subcommand)]
        skill: SkillCommand,
    },
    /// Copies the operator UI token to the clipboard from the platform
    /// secret store (macOS: Keychain; clears the clipboard after 30s,
    /// and only when it still holds the token).
    UiToken {
        /// Names the node explicitly (macOS: the Keychain entry's node
        /// name). Omitted: the single stored token is used.
        #[arg(long)]
        node: Option<String>,
        /// The database path for the file-backed store (non-macOS).
        #[arg(long)]
        db: Option<std::path::PathBuf>,
        /// Reads an explicit token file (the deploy wrapper's tmpfs
        /// handoff) instead of the platform secret store. Wins over the
        /// environment override and the stored token.
        #[arg(long)]
        file: Option<std::path::PathBuf>,
    },
}

/// `wrkspc skill` — the harness skill installer.
#[derive(Debug, Subcommand)]
pub enum SkillCommand {
    /// Installs the skill into detected harnesses (or the ones named
    /// with `--harness`). Idempotent; existing files are only touched
    /// inside wrkspc-marked sections. With no flags the detected
    /// harnesses are listed and the operator picks which to install
    /// into; `--all` installs everywhere without prompting.
    Install {
        /// Harnesses to target (repeatable). Bypasses detection and
        /// the selection prompt.
        #[arg(long)]
        harness: Option<Vec<String>>,
        /// Install into every detected harness without prompting.
        /// Conflicts with `--harness`: the previous silent precedence
        /// (explicit list wins, `--all` ignored) looked like an all-
        /// target install while skipping everything else (CU-12).
        #[arg(long, conflicts_with = "harness")]
        all: bool,
    },
    /// Removes the skill and the wrkspc-marked sections added by
    /// `install`.
    Uninstall {
        /// Harnesses to target (repeatable). Defaults to all detected.
        #[arg(long)]
        harness: Option<Vec<String>>,
    },
    /// Reports the skill's install state per detected harness.
    Status,
}

#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    /// Starts the node and serves until ctrl-c.
    Run(node::NodeRunArgs),
}

#[derive(Debug, Subcommand)]
pub enum HubCommand {
    /// Starts the hub and serves until ctrl-c.
    Run(hub::HubRunArgs),
}
