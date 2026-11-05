-- Peer host assignments (D-6 A2 contract, host record): append-only
-- history. A row is INSERTED for every assignment and never updated or
-- deleted; `peers.host_id` is the transactional projection of the
-- latest assignment (the store owns both writes, so the projection
-- cannot drift). The pilot→team cutover is an ordinary re-assignment,
-- not a migration.

CREATE TABLE peer_host_assignments (
    peer_id     TEXT NOT NULL REFERENCES peers(id),
    host_id     TEXT NOT NULL REFERENCES hosts(host_id),
    assigned_at TEXT NOT NULL,
    PRIMARY KEY (peer_id, host_id, assigned_at)
);

-- Backfill: every existing peer's current host is its first assignment,
-- timestamped with its enrolment.
INSERT INTO peer_host_assignments (peer_id, host_id, assigned_at)
SELECT id, host_id, enrolled_at FROM peers;
