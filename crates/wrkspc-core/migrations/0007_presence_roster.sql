-- A2A-first discovery (HLD rev0.9, owner-approved 2026-10-23):
-- `presence` is hub-owned, fed by node reports (POST /presence,
-- stamped mTLS). `roster` is the materialized read shape — identical
-- on hub and node so MCP list-peers and the UI read one table in both
-- roles; the hub derives it from peers JOIN presence, nodes mirror the
-- hub's rows via replace_roster.

CREATE TABLE presence (
    peer_id    TEXT PRIMARY KEY REFERENCES peers(id),
    state      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE roster (
    peer_id    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    class      TEXT NOT NULL,
    host_id    TEXT NOT NULL,
    state      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
