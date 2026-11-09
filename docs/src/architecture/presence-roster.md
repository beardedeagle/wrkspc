# Presence and roster

The roster is who's here; presence is why the hub believes it.

## Enrollment

A node enrolls over mTLS the first time it dials the hub. From that
moment the peer's host identity *is* its certificate SAN: the same
identity presented from another host is refused, and a different
certificate SAN is a different host identity (a peer's record, its
alias, and its history are keyed by the peer id, not the SAN). The
host a peer first reports presence from is the host it stays bound
to; moving a peer to a new machine is an explicit operator path,
never an accident of reconnection.

## Presence

Presence is a report the node uploads over the node↔hub link — the
hub never infers it from the connection. The roster records the last
report and its age: a peer whose report says `connected` is shown
connected, a report saying `disconnected` is shown disconnected, and
a peer whose last report is stale shows the report's own state, not a
connection-derived guess. A peer that's been booted or banned reads
as ejected, and the roster excludes it.

## Discovery

The hub serves a per-peer agent card over A2A, and the MCP
`list-peers` tool projects the same roster to harness-resident
agents. One source of truth, two surfaces — an agent on either side
of the boundary sees the same peers with the same statuses.
