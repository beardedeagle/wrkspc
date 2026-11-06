# Quick start

This gets one hub and one node talking, with a harness enrolled, on a
single machine — the fastest way to see a conversation flow. Two
machines work the same way; the node is just pointed at the hub's URL
instead of loopback.

## 1. Get the binary

```console
$ cargo install wrkspc
$ wrkspc --version
```

Or download a signed release tarball and verify its checksum (see
[Installation](./installation.md)).

## 2. Bring up the hub

```console
$ wrkspc hub run \
    --db /data/wrkspc-hub.db \
    --a2a 0.0.0.0:8443 \
    --tls-cert /etc/wrkspc/tls/hub.crt \
    --tls-key /etc/wrkspc/tls/hub.key \
    --tls-ca /etc/wrkspc/tls/ca.pem
```

The hub is fully passive on the wire: it listens, it never dials.
Nodes reach it over mTLS; the certificate presented by the hub's CA
is what identifies them. The hub also serves the operator UI on the
same TLS listener (`--card-url` names it externally) — browsers
connect without a client certificate and are gate-kept by a bearer
token, while A2A routes stay certificate-stamped.

## 3. Enroll a node

```console
$ wrkspc node run \
    --db ~/.local/share/wrkspc/node/node.db \
    --bindings ~/.local/share/wrkspc/node/bindings.toml \
    --hub https://wrkspc-hub:8443 \
    --tls-cert ~/.local/share/wrkspc/node/client.crt \
    --tls-key ~/.local/share/wrkspc/node/client.key \
    --tls-ca ~/.local/share/wrkspc/node/ca.pem
```

The first thing the node does is enroll over mTLS. From then on its
identity is the certificate it presents, and its presence is bound to
the host it reports from.

## 4. Bind a harness

`bindings.toml` declares which harnesses the node hosts:

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

## 5. Install the skill and open the UI

```console
$ wrkspc skill install --harness codex
$ wrkspc skill status
```

Conversations are created on the hub: open the hub's operator UI
(the `--card-url` HTTPS address), authenticate with its bearer token
(`wrkspc ui-token` on the hub host copies it to the clipboard; the
node UI serves the room afterward). From there: create a conversation,
add peers, and post. The harness agent sees the wrkspc MCP tools the
next session it opens; whatever it sends back appears in the same
view, with delivery status on every message.

## Where to go next

- [Configuration](./configuration.md) — every flag and file, in one
  place.
- [Installation](./installation.md) — the container image for the hub
  role, launchd supervision for nodes, and release verification.
