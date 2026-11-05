-- Moderation audit (hardening round, 2026-11-03): every cohort
-- action — first-presence enrolment, alias, boot, ban, restore, and
-- forget — is logged with its actor, before/after values, and time.
-- The log deliberately has NO foreign key on peer_id: a forgotten
-- peer's history must survive even if the peer row ever goes away.
--
-- Booted peers gain a stamp (bans already carry revoked_at). Forget is
-- SOFT: the peers row survives (messages/fences/attempts reference it
-- by foreign key), `forgotten_at` hides it from the roster and refuses
-- re-enrolment, and the roster materialization row is deleted.

CREATE TABLE moderation_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    peer_id   TEXT NOT NULL,
    actor     TEXT NOT NULL CHECK (actor IN ('operator', 'system')),
    action    TEXT NOT NULL CHECK (action IN ('enrol', 'alias', 'boot', 'ban', 'restore', 'forget')),
    before    TEXT,
    after     TEXT,
    at        TEXT NOT NULL
);

CREATE INDEX idx_moderation_events_peer ON moderation_events(peer_id, at);

ALTER TABLE peers ADD COLUMN booted_at TEXT;
ALTER TABLE peers ADD COLUMN forgotten_at TEXT;

