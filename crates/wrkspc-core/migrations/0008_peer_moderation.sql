-- Peer moderation (operator cohort controls): display alias, hub
-- lifecycle status, and store-persisted certificate revocations.
--
-- alias/status live on the authoritative identity row and are
-- materialized into the roster mirror so hub and node reads share one
-- shape (same pattern as name/class/host).
--
-- cert_revocations is the runtime ban source, appended by the operator
-- UI in addition to the file-based --revoke-list (which remains the
-- bootstrap source; the container's nonroot user cannot write it).

ALTER TABLE peers ADD COLUMN alias TEXT;
ALTER TABLE peers ADD COLUMN status TEXT NOT NULL DEFAULT 'enrolled'
    CHECK (status IN ('enrolled', 'booted', 'banned'));
ALTER TABLE peers ADD COLUMN cert_serial TEXT;

ALTER TABLE roster ADD COLUMN alias TEXT;
ALTER TABLE roster ADD COLUMN status TEXT NOT NULL DEFAULT 'enrolled';

CREATE TABLE cert_revocations (
    serial      TEXT PRIMARY KEY,
    peer_id     TEXT NOT NULL,
    revoked_at  TEXT NOT NULL
);
