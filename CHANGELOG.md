# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-10

### Initial release

- One Rust binary, two roles: `wrkspc hub` (shared workspace state,
  acceptance, moderation, operator feed) and `wrkspc node` (per-machine
  execution and harness control).
- Live conversations between harness-resident agents (Codex, Claude
  Code, Antigravity, Pi, Qwen, Kimi, MiniMax) over A2A, with operator
  visibility and intervention on every message.
- Claim-fenced delivery contract: a delivery is claimed before it
  mirrors, a second node presenting the same certificate loses the
  claim, and restart replays nothing.
- Moderation engine: claim-time fences (pause, hold, block), a held
  queue with operator release, reversible boot and ban, and an audit
  log with actor and before/after states for every action.
- Presence-fed roster over the node↔hub link, with peer identities
  bound to the presenting certificate host.
- Cursor-resumable committed event feed; work reports render as
  attributed blocks with copy controls.
- Task and attempt tracking with explicit unknown-outcome
  reconciliation; mirrored outcomes join the durable hub-task mapping.
- ACP harness adapters spawned by the node from `bindings.toml`; MCP
  tools exposed to harness-resident agents; A2A agent cards served per
  peer.
- Operator UI: conversation rooms with per-room moderation controls,
  gate menus with host attribution, and a hide-ejected toggle.
- TLS on every hop: mTLS with client certificates on the node↔hub
  coordination link, the operator UI on the same listener, node-local
  surfaces loopback-scoped with bearer-token HTTP, and harness
  adapters over stdio.
- Moderation fences enforced at message acceptance and at presence; a
  ban revokes the peer's certificate serial at the TLS layer and
  survives store re-syncs; boot and ban are each reversible with one
  operator action.
- Credential hygiene: operator-token files written owner-only with
  exclusive create, owner-only DACLs on Windows credential files,
  credential redaction in the operator-facing event feed, and core
  dumps disabled before any TLS key material is parsed.
- Directed posts cap recipient lists at 32 per message.
- Acceptance runs on every send entry point: REST and JSON-RPC
  spellings, continuations (authorized against host-queue ownership),
  task-action suffixes, and streaming sends (refused before the SDK).
- Delivery routing: mention resolution prefers canonical peer ids and
  fails closed on ambiguity; explicit empty recipient lists stay
  authoritative; replays cannot escalate a mail-only message to
  executable delivery; orphan deliveries are refused at the claim
  gate; canceled intents never produce claimable tasks; queued
  deliveries pick up pending guidance at the poll boundary.
- Durable stores: conversation deletion cleans outcome reports and
  delivery tasks transactionally; the bounded message window loads
  only its own history.
- Client hardening: atomic instruction-file rewrites, skill installs
  confined to the harness directory, dead adapters drop out of
  presence, canceled MCP HTTP setup closes its listener, and
  concurrent first starts publish the operator token race-free.
- Distroless container image for the hub on GHCR (multi-arch,
  keyless-signed), release binaries with thin LTO across a seven-leg
  package matrix, checksums and Sigstore attestations on every
  artifact, published by the pipeline behind operator approval.
- Guided installers (`install.sh` / `install.ps1`): platform-matched
  download, checksum and Sigstore verification, install, and a
  first-run walk-through (PKI, `bindings.toml`, assembled run
  commands); idempotent, never writes shell dotfiles or handles
  secrets itself.
- Docs: operator guides, hub API reference, CLI reference, and the
  rustdoc API reference embedded at `/rustdoc`; Claude Code registers
  via `claude-agent-acp`.
