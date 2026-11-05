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

## mTLS under all of it

Every hop carries TLS with client certificates, so a peer's identity
on the wire is its certificate — on A2A to the hub, on the operator
API, and on the harness MCP surface. There is no hop where "no
encryption" is the accepted answer.
