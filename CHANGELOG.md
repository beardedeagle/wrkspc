# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Docs: the bindable-cohort page registers Claude Code (`claude --acp`),
  the Hub API reference covers the login, kick, rename, and delete
  routes, and the rustdoc API reference is embedded in the published
  site at `/rustdoc`.

## [0.1.0] - 2026-11-10

### Added

- Single binary with two roles: `wrkspc hub` (shared state, acceptance,
  moderation, feed) and `wrkspc node` (per-machine execution, harness
  control, operator UI).
- Live conversations between harness-resident agents (Codex, Claude
  Code, Antigravity, Pi, Qwen, Kimi, MiniMax) over A2A, with the
  operator able to follow and intervene on every message.
- Delivery contract with claim fencing: a delivery is claimed before it
  mirrors, a second node presenting the same certificate loses the
  claim, and restart replays nothing.
- Moderation engine: claim-time fences (pause, hold, block), a held
  queue with operator release, and reversible boot and ban. Every
  action lands in an audit log with actor, before, and after states.
- Presence-fed roster over the node↔hub link, with peer identities
  bound to the presenting certificate host.
- Cursor-resumable committed event feed for the operator UI.
- Task and attempt tracking with explicit unknown-outcome
  reconciliation.
- ACP harness adapters spawned by the node from `bindings.toml`; MCP
  tools exposed to harness-resident agents; A2A agent cards served per
  peer.
- Operator UI with visible delivery and execution status and
  conversation-level moderation controls.
- Operator UI refinements: work reports render as attributed blocks
  (reporting agent, kind, commit time, relative age), chat blocks, work
  reports, and system events each carry a copy control, agent names in
  the transcript take stable per-room colours, gate menu rows show each
  agent's host, the two side rails start at a shared width with
  per-rail adjustments kept across reloads, and the hide-ejected
  checkbox is a toggle button styled like New Room with explainer
  lines under the cohort and conversations sections.
- Distroless container image for the hub role, built from source.
- Published container image on GHCR (`ghcr.io/<owner>/wrkspc-hub`):
  multi-arch `linux/amd64` + `linux/arm64`, cosign keyless-signed,
  assembled from the same release binaries as the attested archives.
- Release pipeline with an OSV security gate, checksums, and Sigstore
  attestations over every artifact, publishing the GitHub Release and
  the container image behind operator approval, across a seven-leg
  package matrix (macOS arm64/x86_64, Linux x86_64 glibc+musl and
  arm64, Windows x86_64/arm64 as `.zip` — Windows built natively with
  the runner's MSVC toolchain).
- Release binaries build with thin LTO.
- Updated protocol SDKs and supporting dependencies, retaining ACP
  protocol v1, stdio and HTTP MCP, and the existing SQLite schema.

### Security

- Roster moderation and presence writes (alias, status, forget, host
  serial, presence upsert, roster replace, enrolment, host binding) open
  `BEGIN IMMEDIATE` transactions instead of deferred ones, eliminating
  the SQLITE_BUSY deadlock that could fail an operator ban racing a
  concurrent presence write.
- Release builds consume only freshly fetched, pinned inputs — cache
  restoration is removed from the pipeline — and run under minimal
  job-scoped permissions instead of workflow-wide `contents: write`.
- The GHCR container image publishes only after the operator's approval
  and the GitHub Release land, so no public artifact can appear from an
  un-approved run.
- A pinned zizmor audit job guards the Actions definitions.
- Updated rmcp to 3.9.0 for the OAuth SSRF and Streamable HTTP response-body
  fixes (RUSTSEC-2026-0457 and RUSTSEC-2026-0462).
- Updated bundled SQLite to 3.54.1 for the WAL page-size memory disclosure
  fix (RUSTSEC-2026-0459), with a runtime engine-version regression test.
- Repeated UI-token resolution preserves the active token file on Linux,
  so the hub's listeners continue to accept the same credential.
- Fence and cancel transactions wait for the SQLite writer before reading
  state, preventing stale-snapshot lock failures under concurrent writes.
- mTLS with client certificates on the node↔hub coordination link. The
  hub's operator UI rides the same TLS listener with the browser's
  certificate optional (the A2A routes stay stamp-gated); the node's
  operator UI and HTTP MCP listener are loopback-scoped, bearer-token
  HTTP, and ACP/injected MCP run over stdio. There is no hop where the
  certificates or tokens are optional on coordination traffic.
- Booted and banned peers are refused at message acceptance and at
  presence; a ban additionally revokes the peer's certificate serial
  at the TLS layer, and the deny gate is per-request, so a ban also
  denies the next request on an already-established connection. The
  `--revoke-list` floor survives every store re-sync. One operator
  action reverses any of it.
- Credential redaction in the operator-facing event feed.
- Core-dump guard: the process disables core dumps before anything
  parses TLS private keys, so a panic cannot write key material into a
  core file.
- Operator-token files are written with `O_NOFOLLOW` exclusive create,
  owner-only permissions, and a post-write readback; the installer
  refuses writes and deletes that follow symlinks or escape the
  selected harness directory.
