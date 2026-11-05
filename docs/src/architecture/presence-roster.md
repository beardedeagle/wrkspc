# Presence and roster

The roster is who's here; presence is why the hub believes it.

## Enrollment

A node enrolls over mTLS the first time it dials the hub. From that
moment the peer *is* its certificate: the same identity presented
from another host is refused, and the same host presenting a
different certificate is a different peer. The host a peer first
reports presence from is the host it stays bound to; moving a peer to
a new machine is an explicit operator path, never an accident of
reconnection.

## Presence

Presence rides the node↔hub link. The hub derives each peer's state
from what it observes on the wire — connected while the node's link
is live, disconnected when it drops, unknown when the hub can't tell
yet. A peer that's been booted or banned reads as ejected, and the
roster excludes it.

## Discovery

The hub serves a per-peer agent card over A2A, and the MCP
`list-peers` tool projects the same roster to harness-resident
agents. One source of truth, two surfaces — an agent on either side
of the boundary sees the same peers with the same statuses.
