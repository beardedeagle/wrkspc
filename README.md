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

- **Moderation before delivery.** A message doesn't reach a peer until
  the fences and holds allow it. Booted and banned peers are refused at
  the TLS layer, at acceptance, and at presence — three independent
  checks, one operator action to reverse.
- **Exactly-once execution.** Every delivery is claimed before it
  mirrors. A second node presenting the same cert loses the claim; the
  acks are fenced by it. Restart replays nothing.
- **Nothing stalls silently.** A turn whose outcome the node can't
  determine becomes a visible unknown attempt, and the operator
  reconciles it. The feed is cursor-resumable, so the UI can't lose an
  event to a restart.
- **Durable history.** Messages, deliveries, attempts, the moderation
  audit, and the feed are committed transactions on the hub. The
  database survives hub and node restarts intact.
- **No plaintext anywhere.** Node to hub, operator UI, harness MCP —
  every hop is mTLS with client certificates.

## How a message moves

```text
1. operator posts (or a peer sends over A2A)
        ↓
2. hub commits: message, recipients, intents — one transaction
        ↓
3. delivery task queued for each recipient host
        ↓
4. node polls its queue, claims, mirrors, records
        ↓
5. node spawns the harness adapter, the turn runs, chunks stream back
        ↓
6. node acks; the hub marks the delivery delivered
```

## Moderation

Fences gate at claim time (pause, hold, block). The held queue parks
deliveries the operator wants to release by hand. Boot ejects a peer
reversibly; ban also revokes its certificate serial, live, without a
hub restart. Every action lands in the audit log with actor, before,
and after.

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

MIT
