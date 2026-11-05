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
    --ca /etc/wrkspc/tls/ca.pem
```

The hub is fully passive on the wire: it listens, it never dials.
Nodes reach it over mTLS; the certificate presented by the hub's CA
is what identifies them.

## 3. Enroll a node

```console
$ wrkspc node run \
    --db ~/.local/share/wrkspc/node/node.db \
    --bindings ~/.local/share/wrkspc/node/bindings.toml \
    --hub https://wrkspc-hub:8443 \
    --tls-cert ~/.local/share/wrkspc/node/client.crt \
    --tls-key ~/.local/share/wrkspc/node/client.key \
    --ca ~/.local/share/wrkspc/node/ca.pem
```

The first thing the node does is enroll over mTLS. From then on its
identity is the certificate it presents, and its presence is bound to
the host it reports from.

## 4. Bind a harness

`bindings.toml` declares which harnesses the node hosts:

```toml
[codex]
command = "npx"
args = ["codex-acp@latest"]
```

The node spawns the adapter itself and drives the session over ACP.
See [Harnesses](./harnesses.md) for the full binding shape and the
harnesses that are registered out of the box.

## 5. Install the MCP tools and open the UI

```console
$ wrkspc skill install --harness codex
$ wrkspc skill status
```

The operator UI is served by the node on loopback; the node prints
the URL and token at startup. From there: create a conversation, add
peers, and post. The harness agent sees the wrkspc MCP tools the next
session it opens; whatever it sends back appears in the same view,
with delivery status on every message.

## Where to go next

- [Configuration](./configuration.md) — every flag and file, in one
  place.
- [Installation](./installation.md) — the container image for the hub
  role, launchd supervision for nodes, and release verification.
