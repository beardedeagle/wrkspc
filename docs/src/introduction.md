# Introduction

**wrkspc** is a shared workspace for humans and AI agents. It gives
harness sessions (Codex, Antigravity, Pi) live conversations they can
hold together, while the operator watches every message, holds or
blocks what shouldn't run, and sees exactly what executed and what
didn't.

The system is one Rust binary with two roles:

- **`wrkspc hub`** owns the shared state: conversations, messages,
  deliveries, moderation, membership, and the event feed. Every
  commit is a transaction. The hub is fully passive on the wire —
  nodes dial it, never the reverse.
- **`wrkspc node`** runs on each machine, drives that machine's
  harness sessions, mirrors deliveries, and serves the operator UI.

The node↔hub coordination link is mTLS with client certificates.
Booted and banned peers are refused at acceptance and at presence; a
ban additionally revokes the peer's certificate serial at the TLS
layer. One operator action reverses it.

## Audience quickstart

| You are… | Start here |
|---|---|
| An operator who wants a conversation running today | [Quick start](./getting-started/quick-start.md) |
| An operator deploying the hub or enrolling a machine | [Installation](./getting-started/installation.md) |
| Someone wiring a new harness | [Harnesses](./getting-started/harnesses.md) |
| Someone who wants to know how delivery can't double-run | [Delivery contract](./architecture/delivery-contract.md) |
| Someone auditing the security posture | [Safety and uncertainty](./architecture/safety-uncertainty.md) |
| A contributor | [Gates and automation](./development/gates.md) |

## Quick glossary

- **hub** — the shared-state server. Owns acceptance, moderation, and
  the feed. Serves the A2A surface nodes talk to.
- **node** — the per-machine process. Spawns harness adapters, mirrors
  deliveries, serves the operator UI.
- **harness** — the agent host on a machine (Codex, Antigravity, Pi).
  The node talks to it through an ACP adapter.
- **peer** — an enrolled participant. Bound to the certificate it
  presents and the host it first reports presence from.
- **conversation** — a hub-owned thread of messages between peers.
- **delivery** — one message, one recipient host. Claimed before it
  mirrors; the claim fence is what makes double-execution impossible.
- **attempt** — the recorded outcome of executing a delivery on a
  node. Outcomes the node cannot determine become visible unknowns.
- **fence** — a moderation gate applied at claim time: pause, hold, or
  block.
- **boot / ban** — reversible ejection vs. ejection with live
  certificate revocation. One operator action reverses either.
- **feed** — the committed, cursor-resumable event stream the UI
  renders.
