# Safety and uncertainty

wrkspc's safety story is not "nothing can go wrong." It's "what goes
wrong stays visible and provable."

## Exactly-once, structurally

The delivery contract makes double-execution a contract violation,
not a hope: claims are fenced, acks are fenced by the claim, and a
host can only ever present one identity. The failure you'd have to
engineer to double-run is harder than the one you'd need to
accidentally double-run — the fence is the point.

## Conservative recovery

When the outcome of a turn is uncertain, wrkspc does not retry it,
does not mark it done, and does not paper over it. It records an
unknown attempt and waits for the operator. Re-running an unknown
execution is an operator decision, because only the operator knows
whether "probably ran" is good enough or demands a fresh turn.

## Explicit transport boundaries, no ambient trust

Node↔hub coordination is mTLS with client certificates: identity is
the certificate, and a banned peer's serial sits in a live deny-list
that drops the handshake. Other surfaces carry their own explicit
gates: the hub's browser UI rides HTTPS with the client certificate
optional (the A2A routes stay stamp-gated), the node's operator UI and
HTTP MCP listener are loopback-scoped bearer-token HTTP, and
ACP/injected MCP stay on stdio between the node and the adapters it
spawned. No surface accepts traffic unauthenticated, and credentials
in event content are redacted before they reach the UI. Key material
never outlives a crash either: every role lowers `RLIMIT_CORE` to zero
(and marks the process non-dumpable on Linux) before it parses a TLS
private key, so a panic cannot write a core image containing one.

## The safe-Rust boundary

`unsafe_code` is `forbid` workspace-wide, and every crate opts into
the workspace lint table. The single reviewed exception is
wrkspc-cli's platform FFI — the macOS Keychain integration
(`secrets.rs`), one `geteuid` call (`util.rs`), and the core-dump guard
(`harden.rs`) — which is unsafe
by nature, lives in clearly marked, cfg-gated functions, and is the
only crate whose lint table deviates. The highest-risk modules
(credential redaction, session stop semantics, task-state mapping)
have local Miri coverage — it is a manual check, not an automated
gate — and the release binary is checked to carry no builder paths.
Dependencies are locked, denied by policy, audited, and scanned with
a pinned OSV tool.
