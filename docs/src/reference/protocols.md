# Protocols

Three protocols, one job each. The division is the architecture: each
surface carries exactly the traffic it is for, and no traffic crosses
a surface it wasn't built for.

## A2A — coordination

A2A is the agent-to-agent layer: discovery, task delegation, and
streaming results. The hub serves it; nodes speak it outbound. Agent
cards are served per peer, so an agent can discover who else is here
without anyone poking at a harness directly.

## ACP — session control

ACP is how the node drives the harnesses it hosts. Each binding in
`bindings.toml` is an ACP-capable command; the node spawns it and
controls the session over stdio. ACP stays entirely inside the
node↔harness hop — it never appears on the hub.

## MCP — harness tools

MCP is how harness-resident agents reach wrkspc's tools: the roster,
conversations, messaging. The node serves the MCP surface; the
harness connects to it. What the agent can do is exactly the tool
surface wrkspc registers — no more.

## Transports under it

The node↔hub coordination link is TLS with mandatory client
certificates: a peer's identity on that wire is its certificate. The
other surfaces carry their own, narrower gates — the hub's browser
UI rides HTTPS on the same listener with the client certificate
optional (A2A routes stay stamp-gated, so a certificate-less client
never reaches them), the node's operator UI and the HTTP MCP listener
are loopback-scoped bearer-token HTTP, and ACP plus the injected MCP
bridge are stdio between the node and the adapters it spawned. Each
surface is authenticated; none silently carries another protocol's
traffic.

## SDK and storage dependencies

| Surface | Rust SDK | Protocol or transport |
|---------|----------|-----------------------|
| ACP | `agent-client-protocol 5.1.0` | Stable ACP v1 over stdio |
| MCP | `rmcp 3.9.0` | Stdio and Streamable HTTP |
| A2A | `a2a-lf 0.4.2`, server `0.5.3`, client `0.2.9` | A2A v1.0 over HTTP/TLS |

Storage uses SQLx 0.9.0 at upstream revision
`10462fa4414c6c7f1ed1d39bb31678e3b726878c`, with
`libsqlite3-sys 0.39.1` bundling SQLite 3.54.1. The revision permits the
patched SQLite library; the published SQLx 0.9.0 driver excludes it.
`Cargo.lock` fixes the complete dependency graph. Dependency updates do not
change the database schema or authorize a database reset.
