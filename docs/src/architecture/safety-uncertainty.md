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

## No plaintext, no ambient trust

Every hop carries mTLS with client certificates, including loopback
surfaces. Identity is the certificate; there is no credential layer
to guess or reuse. Credentials in event content are redacted before
they reach the UI.

## The safe-Rust boundary

The workspace contains no `unsafe` blocks in application code —
miri runs clean over the highest-risk modules (credential redaction,
session stop semantics, task-state mapping), and the release binary
is checked to carry no builder paths. Dependencies are locked,
denied by policy, audited, and scanned with a pinned OSV tool.
