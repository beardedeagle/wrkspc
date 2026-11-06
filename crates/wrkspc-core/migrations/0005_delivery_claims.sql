-- Delivery claims (D-6 A2 contract, double-attach enforcement):
-- a delivery may be mirrored+dispatched by exactly ONE node instance.
-- The row is the in-flight exclusive claim, taken at poll time before
-- mirroring and deleted on ack or operator release — the table only
-- ever holds un-acked work. `host_id` binds the queue (cert SAN);
-- `instance_id` is the node's stable local identity, so a second
-- process sharing a copied cert+key loses the claim while the same
-- node's restart re-claims idempotently. No TTL: harness turn durations
-- are unbounded, so any expiry short enough to be useful could hand a
-- live delivery to a second node; stuck claims are visible and
-- operator-released.

CREATE TABLE delivery_claims (
    task_id     TEXT PRIMARY KEY REFERENCES a2a_tasks(task_id),
    host_id     TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    claimed_at  TEXT NOT NULL
);
