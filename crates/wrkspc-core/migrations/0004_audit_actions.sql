-- Widen the moderation action vocabulary: presence reports now write
-- `rename` and `class drift` audit rows (C5 — a changed binding name
-- shows up with an audit row; a changed class is recorded and not
-- applied). The original CHECK rejected both literals, so the table
-- is rebuilt with the widened constraint and its rows preserved.
-- SQLite has no ALTER CONSTRAINT, hence the 12-step rebuild.
CREATE TABLE moderation_events_rebuilt (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    peer_id TEXT NOT NULL,
    actor   TEXT NOT NULL CHECK (actor IN ('operator', 'system')),
    action  TEXT NOT NULL
        CHECK (action IN ('enroll', 'alias', 'boot', 'ban', 'restore', 'forget',
                          'rename', 'class drift')),
    before  TEXT,
    after   TEXT,
    at      TEXT NOT NULL
);

INSERT INTO moderation_events_rebuilt (id, peer_id, actor, action, before, after, at)
SELECT id, peer_id, actor, action, before, after, at FROM moderation_events;

DROP TABLE moderation_events;
ALTER TABLE moderation_events_rebuilt RENAME TO moderation_events;

CREATE INDEX idx_moderation_events_peer ON moderation_events(peer_id, at);
