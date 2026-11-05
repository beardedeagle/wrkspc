# Tasks and attempts

Delivery covers getting a message to a machine. Tasks and attempts
cover what happened on it.

## Task rows

The hub keeps a task row per unit of agent work, with states that
mirror the execution lifecycle: submitted, working, completed,
failed, cancelled. The rows are the hub-side record; the node's A2A
execution is the other half of the mirror. A task row survives a hub
restart intact — it's part of the same committed store.

## Attempts

An attempt is the recorded outcome of running a delivery on a node.
Three endings matter:

- **definite** — the turn finished (completed, failed, or
  cancelled) and the node reported it. The hub marks the delivery
  delivered.
- **unknown** — the node lost the outcome: the harness died without
  a verdict, the link dropped mid-report, the machine crashed. The
  attempt is recorded as unknown and the delivery is *not* marked
  delivered.
- **stalled** — the turn is running with no progress. The operator
  can request cancellation; the node carries it through the harness
  adapter.

Unknown attempts are the deliberate alternative to guessing. The
hub surfaces them; the operator reconciles each one — re-run,
retire, or investigate — and the reconciliation itself is a
committed event.

## Why this is two stores

The hub owns the record of what should run and what was reported.
The node owns the execution itself. The contract between them is
exactly: claim before mirror, report after run, and when the report
can't be produced, say so.
