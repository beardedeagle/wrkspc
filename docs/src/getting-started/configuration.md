# Configuration

Everything wrkspc needs lives in two places: flags on `hub run` and
`node run`, and files the flags point at. There is no ambient
configuration — a process with no flags does nothing but print usage.

## Node: bindings.toml

The node's harness bindings are the one file with real structure:

```toml
# Each entry binds one harness. The node spawns the command and drives
# the session over ACP on stdio.
[codex]
command = "npx"
args = ["codex-acp@latest"]

[antigravity]
command = "antigravity-acp"
args = ["--stdio"]

[pi]
command = "pi-acp"
args = []
```

- `command` is spawned with `args`; stdout/stdin is the ACP wire. The
  node owns the child: it starts it per session, feeds it
  initialization, and tears it down.
- The binding name is the harness the peer presents as. The same peer
  name cannot bind on two hosts.
- The command runs with the node's PATH. If the node is supervised by
  launchd, that PATH is the supervisor's, not your shell's — bind an
  absolute path or a wrapper script when the harness CLI isn't on it.

## TLS material

Both roles take the same three pieces:

| Flag | Meaning |
|---|---|
| `--tls-cert` | The role's own certificate chain |
| `--tls-key` | Its private key |
| `--ca` | The CA used to verify peers |

The hub identifies nodes by the certificate they present. Identity,
enrollment, and revocation all hang off that certificate — there is no
separate username/password layer.

## Database

Each role owns one SQLite database. The hub's holds conversations,
messages, deliveries, attempts, moderation audit, and the feed; a
node's holds its queue cursors and execution state. Keep them on
persistent storage — they are the durable history.

## Operator UI token

The node prints the UI URL and a bearer token at startup; the token
authenticates the operator to the UI it serves on loopback.
