# Delivery contract

The delivery contract is what makes exclusive handoff hold across a
hub and any number of nodes. It is deliberately small: claim, mirror,
ack, fence — plus one outbound leg the ack does not stand in for: the
attempt outcome report.

## The shape

A message committed on the hub fans out to one **delivery** per
recipient host. A delivery has states, and only the documented
transitions move it:

```text
queued → claimed → mirrored → delivered
                 ↘          ↘
                 held     failed / unknown
```

`delivered` records a durable handoff receipt: the node acked the
delivery after mirroring it locally. It is NOT a claim that the
harness turn finished. The turn's terminal outcome arrives later,
over the separate `/delivery-report` leg, and is recorded against
the attempt; an outcome the node cannot determine becomes a visible
unknown attempt instead of a silent stall.

- **queued** — committed, waiting for the node.
- **claimed** — one node took it. The claim is fence-gated (pause,
  hold, block are evaluated in the claim transaction), and a second
  node presenting the same certificate cannot take it. Two hosts
  cannot both execute.
- **mirrored** — the node recorded its copy and queued the execution.
- **delivered** — the node acked the durable handoff. The attempt
  outcome is separate (see above).

## The fences

Fences are evaluated at claim time, in the claim's own transaction,
before anything mirrors — on the hub's record, so every node sees the
same gate:

| Fence | Effect |
|---|---|
| `pause` | the conversation is paused; claims are refused until it lifts |
| `hold` | deliveries park in the held queue for operator release |
| `block` | the delivery is refused outright |

A fence is a rule on the hub's own record, so it applies to every
node identically — there is no per-node fence state to drift.

## What restart cannot do

The hub's acceptance is transactional for the message, its routing,
and a durable fan-out obligation. Fan-out itself runs after the
acceptance commit, and a startup repair completes any fan-out a crash
left partial — a replay takes the idempotency branch and the repair
fills what the crash interrupted, so an accepted message cannot lose
its deliveries silently. A node's claim is durable in the hub before
it mirrors. So:

- a hub restart repairs incomplete fan-out rather than replaying or
  dropping it — the queue is the committed record;
- a node restart re-polls the same queue and finds its claims intact;
- a crash between claim and mirror leaves the delivery claimed, and
  the node resumes or reports it, rather than a second host racing
  in.

## Idempotency

Messages carry hub-scoped idempotency keys; attempt reports carry
node-scoped ones. A retried accept is a no-op, not a duplicate, and a
retried attempt report can't double-count a delivery.
