-- Mirrored unknown attempts: the node reports an attempt whose
-- outcome it could not determine; the hub stores the mirror so the
-- operator can see and reconcile it from the hub UI. The decision
-- column carries the queued operator decision (retry/close+state);
-- the node polls pending decisions, applies them locally, and
-- reports the final terminal state, which consumes the row.
CREATE TABLE IF NOT EXISTS hub_unknown_attempts (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    host_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    stop_reason TEXT NOT NULL DEFAULT '',
    decision TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hub_unknown_pending
    ON hub_unknown_attempts (host_id) WHERE decision IS NOT NULL;
