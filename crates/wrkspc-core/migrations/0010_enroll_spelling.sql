-- The enrolment audit action was spelled "enrol"; the word is "enroll".
-- SQLite cannot alter a CHECK constraint, so rebuild the audit table and
-- carry every existing row across with the value corrected. No foreign key
-- points at this table by design, so the drop is safe, and ids are preserved
-- so any external reference to an audit row still resolves.

CREATE TABLE moderation_events_new (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    peer_id   TEXT NOT NULL,
    actor     TEXT NOT NULL CHECK (actor IN ('operator', 'system')),
    action    TEXT NOT NULL CHECK (action IN ('enroll', 'alias', 'boot', 'ban', 'restore', 'forget')),
    before    TEXT,
    after     TEXT,
    at        TEXT NOT NULL
);

INSERT INTO moderation_events_new (id, peer_id, actor, action, before, after, at)
SELECT id,
       peer_id,
       actor,
       CASE WHEN action = 'enrol' THEN 'enroll' ELSE action END,
       before,
       after,
       at
FROM moderation_events;

DROP TABLE moderation_events;

ALTER TABLE moderation_events_new RENAME TO moderation_events;

CREATE INDEX idx_moderation_events_peer ON moderation_events(peer_id, at);
