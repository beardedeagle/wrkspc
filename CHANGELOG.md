# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Single binary with two roles: `wrkspc hub` (shared state, acceptance,
  moderation, feed) and `wrkspc node` (per-machine execution, harness
  control, operator UI).
- Live conversations between harness-resident agents (Codex,
  Antigravity, Pi) over A2A, with the operator able to follow and
  intervene on every message.
- Delivery contract with claim fencing: a delivery is claimed before it
  mirrors, a second node presenting the same certificate loses the
  claim, and restart replays nothing.
- Moderation engine: claim-time fences (pause, hold, block), a held
  queue with operator release, reversible boot, and ban with live
  certificate-serial revocation. Every action lands in an audit log
  with actor, before, and after states.
- Presence-fed roster over the node↔hub link, with peer identities
  bound to the presenting certificate host.
- Cursor-resumable committed event feed for the operator UI.
- Task and attempt tracking with explicit unknown-outcome
  reconciliation.
- ACP harness adapters spawned by the node from `bindings.toml`; MCP
  tools exposed to harness-resident agents; A2A agent cards served per
  peer.
- Operator UI with credential redaction, visible delivery and execution
  status, and conversation-level moderation controls.
- Distroless container image for the hub role, built from source.
- Published container image on GHCR (`ghcr.io/<owner>/wrkspc-hub`):
  multi-arch `linux/amd64` + `linux/arm64`, cosign keyless-signed,
  assembled from the same release binaries as the signed tarballs.
- Release pipeline with an OSV security gate, checksums, and minisign
  signatures across a five-leg package matrix.

### Security

- mTLS with client certificates on every hop: node↔hub, operator UI,
  and the harness MCP surface. No plaintext transport anywhere.
- Booted and banned peers are refused at the TLS layer, at message
  acceptance, and at presence — three independent checks, one operator
  action to reverse.
- Credential redaction in the operator-facing event feed.
