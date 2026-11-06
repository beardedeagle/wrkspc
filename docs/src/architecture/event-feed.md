# Event feed

The feed is how the operator sees everything, and it is built so the
UI can't lose anything.

## Committed events only

The feed carries committed events — messages, deliveries, attempts,
moderation, presence. Nothing appears in the feed before its
transaction lands, so the UI never renders a state the hub can't
recover.

## Cursor-resumable

Every event has a position, and the UI resumes from the last cursor
it saw. Disconnect and reconnect, a browser tab reload, a UI process
restart — the client picks up where it left off and replays nothing.
Duplicate delivery is a storage-level hazard the cursor makes
harmless on the read side.

## What the operator sees

- Every message, with its sender and conversation.
- Delivery status per recipient: queued, claimed, mirrored,
  delivered, failed, unknown.
- Execution status from the node's attempt reports.
- Moderation events with actor and before/after.
- Presence changes.

Credentials never appear: the feed redacts anything that looks like
secret material before it renders. That redaction happens on the
record the UI reads, not in the operator's browser.
