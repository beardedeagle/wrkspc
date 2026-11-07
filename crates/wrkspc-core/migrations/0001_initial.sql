-- wrkspc-core baseline schema — the single initial migration for the first
-- release. The 11 development migrations were folded into this file before
-- the first PR shipped, since no release has ever run any of them; a fresh
-- database applies this one script and lands on the final schema.

-- Machines that host peers. host_id is the node's certificate SAN.
CREATE TABLE hosts (
    host_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- Peers: humans and agents of either class (A2A-native, harness-resident).
CREATE TABLE peers (
    id           TEXT PRIMARY KEY,
    host_id      TEXT NOT NULL REFERENCES hosts(host_id),
    name         TEXT NOT NULL,
    class        TEXT NOT NULL CHECK (class IN ('a2a_native', 'harness', 'human')),
    enrolled_at  TEXT NOT NULL,
    revoked_at   TEXT,
    alias        TEXT,
    status       TEXT NOT NULL DEFAULT 'enrolled'
        CHECK (status IN ('enrolled', 'booted', 'banned')),
    cert_serial  TEXT,
    booted_at    TEXT,
    forgotten_at TEXT
);

-- Hub-owned rooms; the node mirror preserves the hub conversation id.
CREATE TABLE conversations (
    id         TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    name       TEXT
);

CREATE TABLE conversation_members (
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    peer_id         TEXT NOT NULL REFERENCES peers(id),
    role            TEXT NOT NULL DEFAULT 'member',
    joined_at       TEXT NOT NULL,
    PRIMARY KEY (conversation_id, peer_id)
);

CREATE TABLE messages (
    id                  TEXT PRIMARY KEY,
    conversation_id     TEXT NOT NULL REFERENCES conversations(id),
    sender_id           TEXT NOT NULL REFERENCES peers(id),
    idempotency_key     TEXT NOT NULL,
    content             TEXT NOT NULL,
    execution_requested INTEGER NOT NULL DEFAULT 0,
    expires_at          TEXT,
    created_at          TEXT NOT NULL,
    UNIQUE(conversation_id, sender_id, idempotency_key)
);

-- Delivery gate state per recipient. 'stored' = committed, fence not yet
-- passed; only 'eligible' rows are claimable.
CREATE TABLE message_recipients (
    message_id     TEXT NOT NULL REFERENCES messages(id),
    peer_id        TEXT NOT NULL REFERENCES peers(id),
    delivery_state TEXT NOT NULL DEFAULT 'stored'
        CHECK (delivery_state IN
            ('stored', 'held', 'eligible', 'delivered', 'blocked', 'expired')),
    gate           TEXT,
    PRIMARY KEY (message_id, peer_id)
);

-- One row per harness the node hosts.
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
-- requested. Claimed by the local dispatcher; 'queued' is claimable.
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

-- Attempts: one per claim of an intent. 'unknown' means the outcome is
-- undeterminable and only the operator's reconciliation may close it.
CREATE TABLE attempts (
    id                   TEXT PRIMARY KEY,
    intent_id            TEXT NOT NULL REFERENCES execution_intents(id),
    state                TEXT NOT NULL DEFAULT 'claimed'
        CHECK (state IN
            ('claimed', 'dispatched', 'unknown',
             'completed', 'failed', 'canceled')),
    stop_reason          TEXT,
    result_ref           TEXT,
    claimed_at           TEXT NOT NULL,
    dispatched_at        TEXT,
    terminal_at          TEXT,
    cancel_requested     INTEGER NOT NULL DEFAULT 0,
    cancel_requested_at  TEXT,
    cancel_requested_by  TEXT
);

-- The committed, cursor-resumable event feed (append-only).
CREATE TABLE events (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX events_conversation_seq ON events(conversation_id, seq);

-- Moderation fences: pause, block, or hold evaluated at claim time.
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

-- Single-row table: the exclusive owner of the database (dispatcher lock).
CREATE TABLE store_lock (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    owner       TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

-- A2A task rows: the hub-side record of delivery tasks (context_id = host).
CREATE TABLE a2a_tasks (
    task_id    TEXT PRIMARY KEY,
    context_id TEXT NOT NULL,
    state      TEXT NOT NULL,
    terminal   INTEGER NOT NULL DEFAULT 0,
    task_json  TEXT NOT NULL,
    version    INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX a2a_tasks_context ON a2a_tasks(context_id, updated_at);
CREATE INDEX a2a_tasks_state ON a2a_tasks(state);

-- Operator guidance attached to a conversation's next deliveries.
CREATE TABLE guidance (
    id              TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    author_id       TEXT NOT NULL,
    text            TEXT NOT NULL,
    delivered       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX guidance_undelivered ON guidance(conversation_id, delivered);

-- Append-only peer→host assignments; peers.host_id is the projection.
CREATE TABLE peer_host_assignments (
    peer_id     TEXT NOT NULL REFERENCES peers(id),
    host_id     TEXT NOT NULL REFERENCES hosts(host_id),
    assigned_at TEXT NOT NULL,
    PRIMARY KEY (peer_id, host_id, assigned_at)
);

-- Exclusive per-instance delivery claims (double-attach enforcement).
CREATE TABLE delivery_claims (
    task_id     TEXT PRIMARY KEY REFERENCES a2a_tasks(task_id),
    host_id     TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    claimed_at  TEXT NOT NULL
);

-- Node→hub outbound ledger: messages already propagated, never re-sent.
CREATE TABLE hub_outbound (
    message_id TEXT PRIMARY KEY REFERENCES messages(id),
    sent_at    TEXT NOT NULL
);

-- Per-peer presence state as the hub last observed it.
CREATE TABLE presence (
    peer_id    TEXT PRIMARY KEY REFERENCES peers(id),
    state      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Materialized roster mirror (peers joined with presence).
CREATE TABLE roster (
    peer_id    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    class      TEXT NOT NULL,
    host_id    TEXT NOT NULL,
    state      TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    alias      TEXT,
    status     TEXT NOT NULL DEFAULT 'enrolled'
);

-- Certificate serial revocations: the runtime ban source alongside the
-- operator's --revoke-list file.
CREATE TABLE cert_revocations (
    serial     TEXT PRIMARY KEY,
    peer_id    TEXT NOT NULL,
    revoked_at TEXT NOT NULL
);

-- The moderation audit: actor, action, before/after, newest first.
CREATE TABLE moderation_events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    peer_id TEXT NOT NULL,
    actor   TEXT NOT NULL CHECK (actor IN ('operator', 'system')),
    action  TEXT NOT NULL
        CHECK (action IN ('enroll', 'alias', 'boot', 'ban', 'restore', 'forget')),
    before  TEXT,
    after   TEXT,
    at      TEXT NOT NULL
);

CREATE INDEX idx_moderation_events_peer ON moderation_events(peer_id, at);
