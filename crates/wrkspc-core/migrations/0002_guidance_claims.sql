-- Guidance claims: an attempt claims its guidance atomically before the
-- prompt is assembled (C3). Two concurrent turns in the same conversation
-- therefore cannot both ride the same guidance; the claimed rows are
-- released when the attempt does not resolve successfully, so guidance
-- never vanishes into a dead driver (E09).
ALTER TABLE guidance ADD COLUMN claimed_attempt TEXT;

CREATE INDEX idx_guidance_pending
    ON guidance (conversation_id)
    WHERE delivered = 0 AND claimed_attempt IS NULL;
