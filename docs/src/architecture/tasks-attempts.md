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
  cancelled) and the node reported it over the `/delivery-report`
  leg. The delivery's `delivered` state is separate and earlier: it
  records the ack, the durable handoff receipt the node sent when it
  mirrored the delivery.
- **unknown** — the node lost the outcome: the harness died without
  a verdict, the link dropped mid-report, the machine crashed. The
  ATTEMPT is recorded as unknown and blocks new work for that
  recipient until the operator reconciles it. The delivery itself
  stays delivered (the receipt was real); what is unknown is the
  turn's outcome.
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
exactly: claim before mirror, ack the handoff receipt, report the
outcome after the run — and when the report can't be produced, say
so (an unknown attempt, blocking new work for that recipient).

## Lifecycle to A2A task states

The submitting agent sees wrkspc's lifecycle through the nine A2A v1.0
task states (HLD Appendix A). The mapping is a contract test
(`crates/wrkspc-a2a/tests/state_mapping.rs`); the wire literals are the
`TASK_STATE_*` strings.

| wrkspc lifecycle | what it means | wire state |
| --- | --- | --- |
| queued | message committed, prompt not dispatched | `submitted` |
| held | committed but moderation-gated | `submitted` with a status marker (never `input-required`) |
| blocked | rejected at acceptance (policy or moderation) | `rejected` |
| dispatching | attempt persisted before writing the ACP prompt | `submitted` (in-progress metadata) |
| running | attributable session update establishes activity | `working` |
| awaiting approval | an outstanding permission decision blocks progress | `input-required`, or `auth-required` when auth-bound |
| completed | results durably recorded AND the success-evidence predicate holds | `completed` |
| failed / cancelled / expired | known terminal outcome | `failed` / `canceled` / `failed` (the wire has no expired state) |
| unknown | dispatch or completion may have occurred | hub-internal; the submitter sees the last durable state with a staleness marker — never `unspecified` |
