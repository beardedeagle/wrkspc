# Configuration

Everything wrkspc needs lives in two places: flags on `hub run` and
`node run`, and files the flags point at. There is no ambient
configuration — a process with no flags does nothing but print usage.

## Node: bindings.toml

The node's harness bindings are the one file with real structure. One
`[[binding]]` entry declares one enrolled peer:

```toml
# Each entry binds one harness. The node spawns the command and drives
# the session over ACP on stdio.
[[binding]]
peer_id = "codex"
harness = "codex"
cwd = "/path/to/workspace"
command = "npx -y @agentclientprotocol/codex-acp@2.1.1"

[[binding]]
peer_id = "agy"
harness = "antigravity"
cwd = "/path/to/workspace"
command = "npx -y antigravity-acp@1.2.0"
# agy registers tools through its native HTTP MCP config instead of
# ACP mcpServers injection — the separate mcp_http flag the same
# section documents. Pin the adapter version; `@latest` drifts.
mcp_http = true
```

- `peer_id` is the identity the store and the dispatcher use; `harness`
  is the display name. `command` is one shell-style command line
  (program + args), matching the ACP stdio transport's spawn shape;
  `cwd` is the session working directory.
- Optional per-entry fields: `model` (a model pin applied via ACP
  `session/set_config_option` after `session/new` — only for adapters
  that honor it) and `mcp_http = true` (serve this peer's wrkspc MCP
  tools over loopback HTTP and register them with the harness's native
  MCP config — the agy path). The MCP tools are otherwise injected as
  a stdio `McpServer` pointing at this binary's `mcp-bridge` subcommand
  from the session's first turn.
- The same peer id cannot bind on two hosts.
- The command runs with the node's PATH. If the node is supervised by
  launchd, that PATH is the supervisor's, not your shell's — bind an
  absolute path or a wrapper script when the harness CLI isn't on it.

## TLS material

Both roles take the same three pieces:

| Flag | Meaning |
|---|---|
| `--tls-cert` | The role's own certificate chain |
| `--tls-key` | Its private key |
| `--tls-ca` | The CA used to verify peers |

The hub identifies nodes by the certificate they present. Identity,
enrollment, and revocation all hang off that certificate — there is no
separate username/password layer.

## Database

Each role owns one SQLite database. The hub's holds conversations,
messages, deliveries, attempts, moderation audit, and the feed; a
node's holds its queue cursors and execution state. Keep them on
persistent storage — they are the durable history.

## Operator UI token

The node resolves the operator UI token from `WRKSPC_UI_TOKEN`, or
from the platform secret store (macOS: the login Keychain, keyed
`wrkspc.<node>`; elsewhere: a 0600 token file next to the database).
It never prints the token — logs point at the retrieval command:

```console
$ wrkspc ui-token
```

macOS copies the token to the clipboard (it clears itself after 30
seconds); other platforms print the token file path. The token
authenticates the operator to the UI the node serves on loopback.
