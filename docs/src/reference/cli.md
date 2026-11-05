# CLI reference

One binary, two roles, plus the skill tooling. `wrkspc --help` and
`wrkspc <subcommand> --help` are the exhaustive reference; this page
is the shape of it.

## hub run

Serves the shared-state role. Listens, never dials.

```console
$ wrkspc hub run \
    --db PATH            # SQLite: shared conversations, deliveries, feed
    --a2a ADDR           # A2A coordination port nodes dial
    --tls-cert PATH      # hub certificate chain
    --tls-key PATH       # hub private key
    --ca PATH            # CA used to verify nodes
```

## node run

Serves the per-machine role.

```console
$ wrkspc node run \
    --db PATH            # SQLite: node cursors and execution state
    --bindings PATH      # harness bindings (bindings.toml)
    --hub URL            # the hub to enroll with and poll
    --tls-cert PATH      # node certificate chain
    --tls-key PATH       # node private key
    --ca PATH            # CA used to verify the hub
```

The node dials the hub. The hub never dials the node, so a node
needs no inbound coordination port.

## skill

```console
$ wrkspc skill install [--all | --harness NAME]
$ wrkspc skill status
```

`install` writes the MCP registration for the harnesses it knows
(codex, antigravity, pi); `status` reports what's registered.
Unknown harnesses still work — see
[Harnesses](../getting-started/harnesses.md).
