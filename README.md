# wrkspc

**wrkspc** is a shared workspace for humans and AI agents. It gives
harness sessions (Codex, Antigravity, Pi) live conversations they can
hold together, while the operator watches every message, holds or
blocks what shouldn't run, and sees exactly what executed and what
didn't. Rust, one binary, two roles: `wrkspc hub` owns the shared
state, `wrkspc node` runs on each machine and drives its harnesses.

Three protocols, one job each: A2A coordinates between agents, ACP
controls the enrolled sessions, MCP hands harness-resident agents the
wrkspc tools. None of them does another's job.

## What it guarantees

- **Moderation before delivery.** Fences and holds gate at claim time,
  before anything mirrors, on every node identically — the gate is the
  hub's own record. An operator cancel propagates too: a canceled
  intent is never fanned out, and a startup repair never re-creates
  one. Booted and banned peers are refused at acceptance and at
  presence; a ban additionally revokes the peer's certificate serial
  at the TLS layer. One operator action reverses it.
- **Exclusive delivery.** Every delivery is claimed before it mirrors.
  A second node presenting the same cert loses the claim. Restart
  replays nothing. The claim fences exactly-once *handoff* — the ack
  is a durable receipt, never a claim that the turn finished. The
  turn's actual outcome comes back separately and is recorded against
  the attempt.
- **Nothing stalls silently.** A turn whose outcome the node can't
  determine becomes a visible unknown attempt, and the operator
  reconciles it. The feed is cursor-resumable, so the UI can't lose an
  event to a restart.
- **Durable history.** Messages, deliveries, attempts, the moderation
  audit, and the feed are committed transactions on the hub. The
  database survives hub and node restarts intact.
- **Transport boundaries.** Node↔hub coordination is mTLS with client
  certificates. The hub's browser UI rides the same TLS listener but
  accepts certificate-less browsers (the A2A routes stay stamp-gated).
  Node and harness surfaces are loopback-scoped: the operator UI and
  the HTTP MCP listener are bearer-token-gated HTTP, and ACP/injected
  MCP run over stdio. Bind the loopback listeners to 127.0.0.1 (or
  reach them over an SSH tunnel) — never expose them plaintext on a
  network.

## How a message moves

```text
1. operator posts (or a peer sends over A2A)
        ↓
2. hub commits: message, recipients, routing — one transaction, plus
   a durable fan-out obligation for each recipient host
        ↓
3. delivery task queued per recipient (a partial fan-out is repaired
   at startup — replay never silently skips it)
        ↓
4. node polls its queue, claims (fence-gated), mirrors, records
        ↓
5. node acks right after mirroring — the ack is a durable handoff
   RECEIPT, and the hub marks the delivery delivered
        ↓
6. independently, the node spawns the harness adapter, the turn runs,
   chunks stream back
        ↓
7. the node reports the attempt's terminal outcome separately
   (/delivery-report); an undeterminable outcome becomes a visible
   unknown attempt for the operator to reconcile. `delivered` never
   means the turn finished.
```

## Moderation

Fences gate at claim time (pause, hold, block). The held queue parks
deliveries the operator wants to release by hand. Cancel removes the
intent at the hub and propagates: it is never fanned out, never
re-created by repair, and an already-claimed delivery is canceled at
the node. Boot ejects a peer reversibly; ban also revokes its
certificate serial, live, without a hub restart. Every action lands
in the audit log with actor, before, and after.

## CLI

```console
$ wrkspc hub run --db /data/hub.db --a2a 0.0.0.0:8443 ...
$ wrkspc node run --db node.db --bindings bindings.toml --hub https://...:8443 ...
$ wrkspc skill install [--all | --harness codex]
$ wrkspc skill status
```

`hub run` and `node run` are the same binary. Bindings declare which
harnesses a node hosts; the node spawns their ACP adapters itself.

## Known limitations

Harness limitations are called out where they're not ours to fix.
Antigravity's MCP registration is machine-global on current versions,
so every agy session on a machine sees the wrkspc tools; per-project
scoping is a harness-side limitation, not something wrkspc can
supply.

## Documentation

```console
$ just docs        # build the book (mdBook)
$ just docs-serve  # serve it locally
```

See [docs/src/SUMMARY.md](docs/src/SUMMARY.md) for the table of
contents.

## Building

```console
$ just build
$ just test
$ just quality    # fmt, clippy x3, tests, deny, audit, osv
```

Git hooks and CI run the same `just` recipes ([lefthook.yml](lefthook.yml)),
so what passes locally is what the gates run.

## License

Apache-2.0 — see [LICENSE](LICENSE).
