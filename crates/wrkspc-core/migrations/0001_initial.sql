-- wrkspc-core initial schema — pilot store, unit 1.
-- Mirrors HLD Appendix A data contracts. host_id exists day 1 (constant for
-- the single-host pilot, D-6 requirement). Fences carry an ack column from
-- day 1 (team-mode fence-ack is the same mechanism as revocation).

CREATE TABLE hosts (
    host_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- Peers: humans and agents of either class (A2A-native, harness-resident).
CREATE TABLE peers (
    id          TEXT PRIMARY KEY,
    host_id     TEXT NOT NULL REFERENCES hosts(host_id),
    name        TEXT NOT NULL,
    class       TEXT NOT NULL CHECK (class IN ('a2a_native', 'harness', 'human')),
    enrolled_at TEXT NOT NULL,
    revoked_at  TEXT
);

CREATE TABLE conversations (
    id          TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL
);

CREATE TABLE conversation_members (
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    peer_id         TEXT NOT NULL REFERENCES peers(id),
    role            TEXT NOT NULL DEFAULT 'member',
    joined_at       TEXT NOT NULL,
    PRIMARY KEY (conversation_id, peer_id)
);

CREATE TABLE messages (
    id                   TEXT PRIMARY KEY,
    conversation_id      TEXT NOT NULL REFERENCES conversations(id),
    sender_id            TEXT NOT NULL REFERENCES peers(id),
    idempotency_key      TEXT NOT NULL,
    content              TEXT NOT NULL,
    execution_requested  INTEGER NOT NULL DEFAULT 0,
    expires_at           TEXT,
    created_at           TEXT NOT NULL,
    UNIQUE(conversation_id, sender_id, idempotency_key)
);

-- Delivery gate state per recipient. 'stored' = committed, fence not yet
-- evaluated; 'held'/'blocked' = moderation gate; 'eligible' = deliverable.
CREATE TABLE message_recipients (
    message_id      TEXT NOT NULL REFERENCES messages(id),
    peer_id         TEXT NOT NULL REFERENCES peers(id),
    delivery_state  TEXT NOT NULL DEFAULT 'stored'
        CHECK (delivery_state IN
            ('stored', 'held', 'eligible', 'delivered', 'blocked', 'expired')),
    gate            TEXT,
    PRIMARY KEY (message_id, peer_id)
);

-- Enrolment bindings: an adapter instance owned by this host for a peer.
CREATE TABLE bindings (
    id                  TEXT PRIMARY KEY,
    peer_id             TEXT NOT NULL REFERENCES peers(id),
    host_id             TEXT NOT NULL REFERENCES hosts(host_id),
    adapter             TEXT NOT NULL,
    adapter_version     TEXT NOT NULL,
    workspace_roots     TEXT NOT NULL DEFAULT '[]',
    acp_session_id      TEXT,
    capability_snapshot TEXT,
    state               TEXT NOT NULL DEFAULT 'enrolled'
        CHECK (state IN ('enrolled', 'disconnected', 'revoked')),
    last_verified_at    TEXT,
    disconnect_reason   TEXT
);

-- Execution intents: one per (message, recipient) where execution was
-- requested at acceptance. binding_id is assigned at dispatch time (unit 2).
CREATE TABLE execution_intents (
    id           TEXT PRIMARY KEY,
    message_id   TEXT NOT NULL REFERENCES messages(id),
    recipient_id TEXT NOT NULL REFERENCES peers(id),
    binding_id   TEXT,
    state        TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN
            ('queued', 'claimed', 'dispatched',
             'completed', 'failed', 'canceled')),
    created_at   TEXT NOT NULL,
    UNIQUE(message_id, recipient_id)
);

-- Attempts: claimed and persisted BEFORE any harness write, so a restart can
-- never recreate dispatched work as fresh work. Unknown blocks new work.
CREATE TABLE attempts (
    id            TEXT PRIMARY KEY,
    intent_id     TEXT NOT NULL REFERENCES execution_intents(id),
    state         TEXT NOT NULL DEFAULT 'claimed'
        CHECK (state IN
            ('claimed', 'dispatched', 'unknown',
             'completed', 'failed', 'canceled')),
    stop_reason   TEXT,
    result_ref    TEXT,
    claimed_at    TEXT NOT NULL,
    dispatched_at TEXT,
    terminal_at   TEXT
);

-- Events: the committed feed; seq is the resumable cursor (D-10 UI, D-6
-- remote UI share this surface).
CREATE TABLE events (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX events_conversation_seq ON events(conversation_id, seq);

-- Moderation fences. Ack column is the team-mode fence-ack seam; single-host
-- pilot acks locally.
CREATE TABLE fences (
    id              TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    peer_id         TEXT NOT NULL REFERENCES peers(id),
    kind            TEXT NOT NULL CHECK (kind IN ('pause', 'block', 'hold')),
    active          INTEGER NOT NULL DEFAULT 1,
    set_by          TEXT NOT NULL REFERENCES peers(id),
    set_at          TEXT NOT NULL,
    acked_at        TEXT,
    cleared_by      TEXT REFERENCES peers(id),
    cleared_at      TEXT
);

CREATE INDEX fences_active ON fences(conversation_id, peer_id, active);

-- Singleton row for the exclusive store lock (Appendix D check 9): another
-- process must not become a second dispatcher against the same database.
CREATE TABLE store_lock (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    owner       TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);
