-- Moderation engine (unit 5): durable cancel requests, operator guidance,
-- and the held-delivery queue surface.
--
-- cancel_requested is provenance on the attempt row: the dispatcher's drive
-- task consults it before prompting (a turn the operator already canceled
-- is never dispatched) and the flag survives a crash so recovery sees the
-- request even when the live session is gone.

ALTER TABLE attempts ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0;
ALTER TABLE attempts ADD COLUMN cancel_requested_at TEXT;
ALTER TABLE attempts ADD COLUMN cancel_requested_by TEXT;

-- Operator guidance: undelivered entries are prefixed to the next prompt
-- dispatched in the conversation, then marked delivered.
CREATE TABLE guidance (
    id              TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    author_id       TEXT NOT NULL,
    text            TEXT NOT NULL,
    delivered       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX guidance_undelivered ON guidance(conversation_id, delivered);
