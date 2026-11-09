# Hub and node

wrkspc is one binary, two roles. The split is an authority boundary,
not a deployment convenience.

## The hub

The hub owns every shared record: conversations, messages,
deliveries, attempts, moderation events, membership, and the feed.
Acceptance commits the message, its routing, and a durable fan-out
obligation in one transaction; fan-out itself follows, and a startup
repair completes any fan-out a crash left partial — so a message and
its deliveries cannot silently diverge.

The hub never dials anything. It listens:

- **A2A** on its coordination port, where nodes speak to it with
  client certificates.
- **Operator API** routes, guarded by the operator token, for
  conversations, moderation, and inspection.

The hub has no harness adapters and spawns nothing. It holds state and
enforces the rules; it cannot reach into a machine's sessions, because
that path only exists on the node side of the boundary.

## The node

The node is the per-machine process. It:

- polls the hub for its delivery queue,
- claims (fence-gated) and mirrors deliveries,
- spawns and drives the harness ACP adapters,
- reports presence, receipts (acks), and terminal attempt outcomes
  over the `/delivery-report` leg,
- serves the operator UI on loopback.

The node's database holds its own execution state and queue cursors.
Anything the node can't prove — a claim it lost, a turn whose outcome
vanished — is reported, never guessed.

## Why the split is what it is

The division of labor keeps execution authority with the machine that
executes. The hub commits what should happen; the node proves what
did. Moderation is enforced at the boundary that owns the record it
protects: the hub fences shared delivery at claim time, the node owns
its local gates. A hub-initiated cancel removes the intent at the
source and propagates — a canceled intent is never fanned out and
never re-created by repair, and an already-claimed delivery is
canceled at the node — the hub still never reaches across and
executes anything remotely.
