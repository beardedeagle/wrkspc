# Quick start

This gets one hub and one node talking, with a harness enrolled, on a
single machine — the fastest way to see a conversation flow. Two
machines work the same way; the node is just pointed at the hub's URL
instead of loopback.

## 1. Get the binary

The pre-release binary ships as a release tarball with a keyless
Sigstore attestation — download one for your platform and verify its
checksum and attestation (see
[Installation](./installation.md)), or build from a checkout:

```console
$ git clone https://github.com/beardedeagle/wrkspc && cd wrkspc
$ cargo build --release --locked
$ ./target/release/wrkspc --version
```

There is no crates.io package yet: `cargo install wrkspc` will not
work for pre-release builds.

## 2. Bring up the hub

```console
$ wrkspc hub run \
    --db /data/wrkspc-hub.db \
    --a2a 0.0.0.0:8443 \
    --card-url https://wrkspc-hub.example:8443 \
    --tls-cert /etc/wrkspc/tls/hub.crt \
    --tls-key /etc/wrkspc/tls/hub.key \
    --tls-ca /etc/wrkspc/tls/ca.pem
```

Create the directories and the PKI first (the hub never creates the
certificate files for you), and set `--card-url` to the externally
resolvable name whose certificate matches — the default is the bare
bind address, which nodes and browsers cannot verify.

The hub is fully passive on the wire: it listens, it never dials.
Nodes reach it over mTLS; the certificate presented by the hub's CA
is what identifies them. The hub also serves the operator UI on the
same TLS listener — browsers connect without a client certificate
and are gate-kept by a bearer token, while A2A routes stay
certificate-stamped.

## 3. Bind a harness

`bindings.toml` declares which harnesses the node hosts — create it
before starting the node (the node loads it at startup):

```toml
[[binding]]
peer_id = "codex"
harness = "codex"
cwd = "/path/to/workspace"
command = "npx -y @agentclientprotocol/codex-acp@2.1.1"
```

The node spawns the adapter itself and drives the session over ACP.
See [Configuration](./configuration.md) for the full binding shape
and [Harnesses](./harnesses.md) for the harnesses that are registered
out of the box.

## 4. Enroll a node

```console
$ wrkspc node run \
    --db ~/.local/share/wrkspc/node/node.db \
    --bindings ~/.local/share/wrkspc/node/bindings.toml \
    --hub https://wrkspc-hub.example:8443 \
    --host-id this-machine.example \
    --tls-cert ~/.local/share/wrkspc/node/client.crt \
    --tls-key ~/.local/share/wrkspc/node/client.key \
    --tls-ca ~/.local/share/wrkspc/node/ca.pem
```

`--host-id` is required with `--hub`: it is this NODE's certificate
SAN — the identity string the client certificate carries — passed
exactly as it appears in the certificate. The certificate files must
exist before the node starts; nothing here creates PKI for you.

The first thing the node does is enroll over mTLS. From then on its
identity is the certificate it presents, and its presence is bound to
the host it reports from.

## 5. Install the skill and open the UI

```console
$ wrkspc skill install --harness codex
$ wrkspc skill status
```

Conversations are created on the hub: open the hub's operator UI
(the `--card-url` HTTPS address), authenticate with its bearer token
(`wrkspc ui-token` on the hub host copies it to the clipboard — on
macOS the token lives in the Keychain under the node's name; on other
platforms pass the hub's database with
`wrkspc ui-token --db /data/wrkspc-hub.db`). From there: create a
conversation, add peers, and post. The harness agent sees the wrkspc
MCP tools the next session it opens; whatever it sends back appears
in the same view, with delivery status on every message.

## Where to go next

- [Configuration](./configuration.md) — every flag and file, in one
  place.
- [Installation](./installation.md) — the container image for the hub
  role, launchd supervision for nodes, and release verification.
