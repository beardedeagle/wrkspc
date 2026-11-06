-- Node → hub propagation ledger (D-6 final mile): messages that
-- originated from THIS node's local harness peers and have been
-- delivered to the hub via a stamped `message/send`. Idempotent: the
-- hub deduplicates on the message id, and this row prevents a re-send
-- after a crash between the send and the local commit.

CREATE TABLE hub_outbound (
    message_id TEXT PRIMARY KEY REFERENCES messages(id),
    sent_at    TEXT NOT NULL
);
