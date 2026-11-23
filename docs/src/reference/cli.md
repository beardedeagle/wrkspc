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
    --ui ADDR            # optional plaintext operator-UI listener (loopback use)
    --card-url URL       # externally resolvable URL nodes/browsers verify against
    --tls-cert PATH      # hub certificate chain
    --tls-key PATH       # hub private key
    --tls-ca PATH        # CA used to verify nodes
    --revoke-list PATH   # serial deny-list file (one hex serial per line; TLS only)
    --ui-token-file PATH # explicit operator-token file (fail-closed if missing)
```

## node run

Serves the per-machine role.

```console
$ wrkspc node run \
    --db PATH            # SQLite: node cursors and execution state
    --bindings PATH      # harness bindings (bindings.toml)
    --hub URL            # the hub to enroll with and poll
    --host-id ID         # REQUIRED with --hub: this node certificate's DNS SAN
    --tls-cert PATH      # node certificate chain
    --tls-key PATH       # node private key
    --tls-ca PATH        # CA used to verify the hub
```

`--host-id` must equal the DNS SAN of the node's certificate (the hub
resolves peers by that SAN); it is not inferred from the certificate
itself.

The node dials the hub. The hub never dials the node, so a node
needs no inbound coordination port.

## skill

```console
$ wrkspc skill install [--all | --harness NAME]
$ wrkspc skill status
```

`install` copies the embedded wrkspc SKILL.md into each detected
harness's skills directory and (for the binding cohort) inserts the
standing-instructions pointer section; `status` compares installed
copies against the shipped content. The MCP tool surface itself is
registered by the node's per-harness wiring (ACP `session/new`
injection, or the harness's own MCP config for HTTP-bound peers), not
by the skill installer. Unknown harnesses still work — see
[Harnesses](../getting-started/harnesses.md).

## ui-token

```console
$ wrkspc ui-token [--node NAME] [--db PATH]
```

Copies the operator UI token to the clipboard (macOS, 30-second
self-clearing) or prints the token file path (other platforms). The
node never writes the token into logs; this command retrieves it from
the platform secret store.
