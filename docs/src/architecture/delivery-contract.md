# Delivery contract

The delivery contract is what makes "exactly once" hold across a hub
and any number of nodes. It is deliberately small: claim, mirror,
ack, fence.

## The shape

A message committed on the hub fans out to one **delivery** per
recipient host. A delivery has states, and only the documented
transitions move it:

```text
queued → claimed → mirrored → delivered
                 ↘          ↘
                 held     failed / unknown
```

- **queued** — committed, waiting for the node.
- **claimed** — one node took it. The claim is fenced: a second node
  presenting the same certificate cannot take it, and the acks are
  fenced by the claim. Two hosts cannot both execute.
- **mirrored** — the node recorded its copy and queued the execution.
- **delivered** — the attempt came back with a definite outcome and
  the node acked.

## The fences

Fences are evaluated at claim time, before anything mirrors:

| Fence | Effect |
|---|---|
| `pause` | the conversation is paused; claims are refused until it lifts |
| `hold` | deliveries park in the held queue for operator release |
| `block` | the delivery is refused outright |

A fence is a rule on the hub's own record, so it applies to every
node identically — there is no per-node fence state to drift.

## What restart cannot do

The hub's acceptance is transactional: a message and its deliveries
commit together. A node's claim is durable in the hub before it
mirrors. So:

- a hub restart replays nothing — the queue is the committed record;
- a node restart re-polls the same queue and finds its claims intact;
- a crash between claim and mirror leaves the delivery claimed, and
  the node resumes or reports it, rather than a second host racing
  in.

## Idempotency

Messages carry hub-scoped idempotency keys; attempt reports carry
node-scoped ones. A retried accept is a no-op, not a duplicate, and a
retried attempt report can't double-count a delivery.
