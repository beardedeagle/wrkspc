-- A2A task persistence (unit 4): the TaskStore seam over the wrkspc store.
-- Core owns the rows protocol-agnostically: `state` holds the caller's
-- literal, `terminal` is a plain flag so core can enforce the
-- terminal-immutability invariant WITHOUT parsing protocol vocabulary.
-- `task_json` is the caller's serialized task blob (a2a-lf types, camelCase).
-- version is the store-owned optimistic-concurrency counter (TaskVersion).

CREATE TABLE a2a_tasks (
    task_id     TEXT PRIMARY KEY,
    context_id  TEXT NOT NULL,
    state       TEXT NOT NULL,
    terminal    INTEGER NOT NULL DEFAULT 0,
    task_json   TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX a2a_tasks_context ON a2a_tasks(context_id, updated_at);
CREATE INDEX a2a_tasks_state ON a2a_tasks(state);
