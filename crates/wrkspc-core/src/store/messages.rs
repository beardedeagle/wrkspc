//! Readiness fence: message acceptance and fan-out.

use super::AcceptOutcome;
use super::RecipientRequest;
use super::Store;
use crate::error::{Result, StoreError};
use crate::model::DeliveryState;
use chrono::Utc;
use sqlx::Transaction;
use sqlx::{Row, Sqlite};
use uuid::Uuid;

/// The idempotency key and message text, paired so the shared fence
/// implementation stays under the argument-count lint.
struct AcceptText<'a> {
    key: &'a str,
    content: &'a str,
}

/// The ceiling on directed-post recipients per message, enforced on the
/// shared acceptance path so EVERY caller (operator UI route and MCP
/// post-message tool) is fenced — a near-limit recipient list could
/// otherwise fan out to thousands of per-target store resolves and
/// deliveries (CodeQL `rust/uncontrolled-allocation-size`).
pub const MAX_DIRECTED_RECIPIENTS: usize = 32;

impl Store {
    /// Runs the readiness fence: commits a message, its recipients, and any
    /// requested execution intents in one transaction.
    ///
    /// Sender and recipients must already be conversation members.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::IdempotencyConflict`]
    /// when the key is reused with different content, and
    /// [`StoreError::NotFound`] for non-members.
    pub async fn accept_message(
        &self,
        conversation_id: &str,
        sender_id: &str,
        idempotency_key: &str,
        content: &str,
        execution_requested: bool,
        recipients: &[RecipientRequest],
    ) -> Result<AcceptOutcome> {
        self.accept_message_with(
            conversation_id,
            sender_id,
            AcceptText {
                key: idempotency_key,
                content,
            },
            execution_requested,
            recipients,
            true,
        )
        .await
    }

    /// The hub-connected mirror form of
    /// [`accept_message`](Store::accept_message): identical fence and
    /// commit semantics, but NO local execution intents are created.
    ///
    /// A node running with `--hub` must never execute local work before
    /// hub acceptance: hub fences and the hub-side delivery claim are the
    /// execution gate. The message is stored (the mirror + the outbound
    /// sync feed the hub), and execution happens only when the hub echoes
    /// a delivery the node claims. Standalone nodes keep the direct path.
    ///
    /// # Errors
    ///
    /// The same error set as [`accept_message`](Store::accept_message).
    pub async fn accept_message_mirror_only(
        &self,
        conversation_id: &str,
        sender_id: &str,
        idempotency_key: &str,
        content: &str,
        execution_requested: bool,
        recipients: &[RecipientRequest],
    ) -> Result<AcceptOutcome> {
        self.accept_message_with(
            conversation_id,
            sender_id,
            AcceptText {
                key: idempotency_key,
                content,
            },
            execution_requested,
            recipients,
            false,
        )
        .await
    }

    /// Shared fence implementation; `create_intents` selects whether
    /// execution intents are committed (the execution authority is still
    /// recorded on the message either way, so the wire flag never lies).
    async fn accept_message_with(
        &self,
        conversation_id: &str,
        sender_id: &str,
        text: AcceptText<'_>,
        execution_requested: bool,
        recipients: &[RecipientRequest],
        create_intents: bool,
    ) -> Result<AcceptOutcome> {
        // The ceiling holds on the SHARED path, not per caller: both the
        // operator UI route and the MCP post-message tool funnel through
        // here, and a harness can name as many peers as the protocol lets
        // it (CodeQL rust/uncontrolled-allocation-size).
        if recipients.len() > MAX_DIRECTED_RECIPIENTS {
            return Err(StoreError::TooManyRecipients {
                limit: MAX_DIRECTED_RECIPIENTS,
                count: recipients.len(),
            });
        }
        let AcceptText {
            key: idempotency_key,
            content,
        } = text;
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;

        // Ejection gate at the committed acceptance boundary: a
        // booted/banned peer cannot post through ANY entrypoint (A2A,
        // MCP, mirror) until the operator restores it.
        let sender_status: Option<String> =
            sqlx::query_scalar("SELECT status FROM peers WHERE id = ?")
                .bind(sender_id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(status) = sender_status.as_deref()
            && status != "enrolled"
        {
            return Err(StoreError::PeerEjected {
                peer_id: sender_id.to_string(),
                status: status.to_string(),
            });
        }

        // Idempotency BEFORE routing resolution: a replay must reuse the
        // committed routing and never re-resolve @mentions (an alias
        // move or roster change between retries must not change who a
        // post wakes). The `id = ?` branch covers the co-hosted echo:
        // for a message this node propagated upward, the origin id IS
        // the original local message id.
        if let Some(row) = sqlx::query(
            "SELECT id, content, execution_requested FROM messages
             WHERE conversation_id = ? AND sender_id = ?
               AND (idempotency_key = ? OR id = ?)",
        )
        .bind(conversation_id)
        .bind(sender_id)
        .bind(idempotency_key)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            let existing_id: String = row.get("id");
            let existing_content: String = row.get("content");
            let existing_execution: bool = row.get("execution_requested");
            if existing_content != content {
                return Err(StoreError::IdempotencyConflict {
                    key: idempotency_key.to_string(),
                });
            }
            // A replay means the message is already stored, not that this
            // recipient's work is done. The delivery loop feeds one
            // delivery per recipient under the same origin id, so every
            // recipient after the first reaches this branch: enrol it
            // and raise its intent here, or the peer silently never
            // runs.
            self.assert_member(&mut tx, conversation_id, sender_id)
                .await?;
            // Execution authority is committed at first acceptance: a
            // mail-only message can never become executable via a retry.
            let requests_execution = recipients.iter().any(|r| r.execution_requested);
            if requests_execution && !existing_execution {
                return Err(StoreError::ExecutionAuthorityConflict {
                    message_id: existing_id,
                });
            }
            // The ceiling is CUMULATIVE across replays: each per-recipient
            // delivery loop call lands here with its own small list, but a
            // caller rotating lists under one idempotency key could
            // otherwise accumulate unbounded recipients on one message.
            // Two guards on the count: the incoming list may name the same
            // peer twice (INSERT OR IGNORE would dedupe), and a legacy
            // message committed before the cap may already exceed it — a
            // pure re-ack that adds NOBODY must always pass, or an upgrade
            // strands outstanding deliveries in process_deliveries' replay
            // acks. The refusal fires only when the replay GROWS the set
            // past the ceiling.
            let committed_ids: Vec<String> =
                sqlx::query_scalar("SELECT peer_id FROM message_recipients WHERE message_id = ?")
                    .bind(&existing_id)
                    .fetch_all(&mut *tx)
                    .await?;
            let mut new_unique_ids: Vec<&str> = Vec::new();
            for recipient in recipients {
                if !committed_ids.iter().any(|id| id == &recipient.peer_id)
                    && !new_unique_ids.contains(&recipient.peer_id.as_str())
                {
                    new_unique_ids.push(&recipient.peer_id);
                }
            }
            if !new_unique_ids.is_empty()
                && committed_ids.len() + new_unique_ids.len() > MAX_DIRECTED_RECIPIENTS
            {
                return Err(StoreError::TooManyRecipients {
                    limit: MAX_DIRECTED_RECIPIENTS,
                    count: committed_ids.len() + new_unique_ids.len(),
                });
            }
            for recipient in recipients {
                self.assert_member(&mut tx, conversation_id, &recipient.peer_id)
                    .await?;
                sqlx::query(
                    "INSERT OR IGNORE INTO message_recipients
                       (message_id, peer_id, delivery_state)
                     VALUES (?, ?, ?)",
                )
                .bind(&existing_id)
                .bind(&recipient.peer_id)
                .bind(DeliveryState::Stored.as_str())
                .execute(&mut *tx)
                .await?;
                if create_intents && recipient.execution_requested {
                    sqlx::query(
                        "INSERT OR IGNORE INTO execution_intents
                           (id, message_id, recipient_id, state, created_at)
                         VALUES (?, ?, ?, 'queued', ?)",
                    )
                    .bind(Uuid::new_v4().to_string())
                    .bind(&existing_id)
                    .bind(&recipient.peer_id)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
                }
            }
            // The stored flag reflects the committed intents, never a
            // stale first-acceptance snapshot.
            if requests_execution {
                sqlx::query("UPDATE messages SET execution_requested = 1 WHERE id = ?")
                    .bind(&existing_id)
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
            return Ok(AcceptOutcome::Replay {
                message_id: existing_id,
            });
        }

        // Resolve who this post actually wakes before anything is written: the
        // caller may have named its recipients, or it may only have written
        // `@name` in prose. Both branches below use the same set, so a replay
        // cannot quietly change the routing.
        let recipients = self
            .resolve_recipients(
                &mut tx,
                conversation_id,
                sender_id,
                content,
                execution_requested,
                recipients,
            )
            .await?;
        let recipients = recipients.as_slice();
        // The ceiling also fences the RESOLVED list: a mention-only post
        // arrives with an empty requested list (the early guard above
        // passes), and @mentions can still resolve to dozens of members —
        // the resolved set is what actually fans out.
        if recipients.len() > MAX_DIRECTED_RECIPIENTS {
            return Err(StoreError::TooManyRecipients {
                limit: MAX_DIRECTED_RECIPIENTS,
                count: recipients.len(),
            });
        }

        self.assert_member(&mut tx, conversation_id, sender_id)
            .await?;

        let message_id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO messages
               (id, conversation_id, sender_id, idempotency_key, content,
                execution_requested, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&message_id)
        .bind(conversation_id)
        .bind(sender_id)
        .bind(idempotency_key)
        .bind(content)
        .bind(execution_requested)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        for recipient in recipients {
            sqlx::query(
                "INSERT INTO message_recipients (message_id, peer_id, delivery_state)
                 VALUES (?, ?, ?)",
            )
            .bind(&message_id)
            .bind(&recipient.peer_id)
            .bind(DeliveryState::Stored.as_str())
            .execute(&mut *tx)
            .await?;
            if create_intents && recipient.execution_requested {
                let intent_id = Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO execution_intents
                       (id, message_id, recipient_id, state, created_at)
                     VALUES (?, ?, ?, 'queued', ?)",
                )
                .bind(&intent_id)
                .bind(&message_id)
                .bind(&recipient.peer_id)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
            }
        }

        let event_payload = serde_json::json!({
            "message_id": message_id,
            "sender_id": sender_id,
            "execution_requested": execution_requested,
        });
        self.append_event_in(&mut tx, conversation_id, "message.accepted", &event_payload)
            .await?;

        tx.commit().await?;
        Ok(AcceptOutcome::Accepted { message_id })
    }
}

/// A peer class that never polls for deliveries.
///
/// The operator reads the room in the UI; it has no claim loop, so a delivery
/// row for it can never reach a terminal state and sits `stored` forever.
const HUMAN_CLASS: &str = "human";

/// A conversation member as mention resolution sees it.
struct Candidate {
    peer_id: String,
    alias: Option<String>,
    name: String,
}

impl Store {
    /// Resolves who a post actually wakes.
    ///
    /// Explicit recipients win. When a post names none, `@name` tokens in the
    /// body are resolved against the conversation's members by peer id, alias,
    /// or display name. A post that names nobody still wakes nobody: this is
    /// mention resolution, not fan-out, and fan-out is what turned one room
    /// into a broadcast storm.
    ///
    /// Human peers never take a delivery.
    ///
    /// * `tx` — the acceptance transaction.
    /// * `conversation_id` — the room the post lands in.
    /// * `sender_id` — the author, never a recipient of its own post.
    /// * `content` — the post body, scanned for `@name` tokens when the caller
    ///   named no recipients.
    /// * `execution_requested` — the flag carried onto every resolved
    ///   recipient.
    /// * `requested` — the caller's explicit recipients, possibly empty.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when an explicit recipient is not a
    /// conversation member.
    async fn resolve_recipients(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        conversation_id: &str,
        sender_id: &str,
        content: &str,
        execution_requested: bool,
        requested: &[RecipientRequest],
    ) -> Result<Vec<RecipientRequest>> {
        let candidates = if requested.is_empty() {
            self.mentioned_members(tx, conversation_id, content)
                .await?
                .into_iter()
                .map(|peer_id| RecipientRequest {
                    peer_id,
                    execution_requested,
                })
                .collect::<Vec<_>>()
        } else {
            requested.to_vec()
        };

        let mut resolved: Vec<RecipientRequest> = Vec::with_capacity(candidates.len());
        let explicit = !requested.is_empty();
        for candidate in candidates {
            // An explicit recipient must be a member; the check stays so a
            // typo fails loudly instead of silently reaching nobody.
            self.assert_member(tx, conversation_id, &candidate.peer_id)
                .await?;
            if candidate.peer_id == sender_id
                || resolved.iter().any(|row| row.peer_id == candidate.peer_id)
            {
                continue;
            }
            if self.peer_class(tx, &candidate.peer_id).await?.as_deref() == Some(HUMAN_CLASS) {
                continue;
            }
            // Ejected recipients: an explicit address is refused loudly;
            // a mention never wakes a booted/banned peer (the UI's
            // mention fallback must not re-introduce a banned target).
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM peers WHERE id = ?")
                    .bind(&candidate.peer_id)
                    .fetch_optional(&mut **tx)
                    .await?;
            if let Some(status) = status
                && status != "enrolled"
            {
                if explicit {
                    return Err(StoreError::PeerEjected {
                        peer_id: candidate.peer_id,
                        status,
                    });
                }
                continue;
            }
            resolved.push(candidate);
        }
        Ok(resolved)
    }

    /// Maps the `@name` tokens in a post onto conversation members.
    ///
    /// A token matches a member's peer id, alias, or display name,
    /// case-insensitively, so a peer renamed with an alias stays addressable
    /// under the old name.
    ///
    /// # Errors
    ///
    /// Returns a store error when the membership query fails.
    async fn mentioned_members(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        conversation_id: &str,
        content: &str,
    ) -> Result<Vec<String>> {
        let names = mentioned_names(content);
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT p.id, p.alias, p.name
             FROM conversation_members m JOIN peers p ON p.id = m.peer_id
             WHERE m.conversation_id = ?",
        )
        .bind(conversation_id)
        .fetch_all(&mut **tx)
        .await?;
        let members = rows
            .iter()
            .map(|row| Candidate {
                peer_id: row.get("id"),
                alias: row.get("alias"),
                name: row.get("name"),
            })
            .collect::<Vec<_>>();

        let mut matched: Vec<String> = Vec::new();
        for name in names {
            let hit = members.iter().find(|member| {
                member.peer_id.eq_ignore_ascii_case(&name)
                    || member
                        .alias
                        .as_deref()
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(&name))
                    || member.name.eq_ignore_ascii_case(&name)
            });
            if let Some(member) = hit
                && !matched.contains(&member.peer_id)
            {
                matched.push(member.peer_id.clone());
            }
        }
        Ok(matched)
    }

    /// Reads a peer's class inside a transaction.
    ///
    /// # Errors
    ///
    /// Returns a store error when the query fails.
    async fn peer_class(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        peer_id: &str,
    ) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT class FROM peers WHERE id = ?")
                .bind(peer_id)
                .fetch_optional(&mut **tx)
                .await?,
        )
    }

    /// Lists the peers a message was delivered to.
    ///
    /// This is the post-hoc view of routing: the acceptance path resolves
    /// recipients (including `@name` mentions in prose), and the commit is the
    /// only record of who that turned out to be.
    ///
    /// # Errors
    ///
    /// Returns a store error when the query fails.
    pub async fn message_recipients(&self, message_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT peer_id FROM message_recipients WHERE message_id = ? ORDER BY rowid",
        )
        .bind(message_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|row| row.get::<String, _>(0)).collect())
    }

    /// Reads a peer's class.
    ///
    /// # Errors
    ///
    /// Returns an error when the query fails.
    pub async fn peer_class_of(&self, peer_id: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT class FROM peers WHERE id = ?")
                .bind(peer_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }
}

/// Extracts the `@name` tokens written in a post body.
///
/// A token addresses a peer only when nothing alphanumeric precedes the `@`, so
/// an address such as `user@example.com` is not mistaken for a mention.
fn mentioned_names(content: &str) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for token in content.split_whitespace() {
        let Some(at) = token.find('@') else {
            continue;
        };
        let boundary = token[..at].chars().last();
        if boundary.is_some_and(|c| c.is_alphanumeric()) {
            continue;
        }
        let rest = &token[at + 1..];
        let name = rest
            .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_' && c != '.');
        // A trailing dot belongs to the sentence, never to a peer id.
        let name = name.trim_end_matches('.');
        if name.is_empty() || names.iter().any(|seen| seen.eq_ignore_ascii_case(name)) {
            continue;
        }
        names.push(name.to_string());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::mentioned_names;

    /// Mentions are names, not addresses, and duplicates collapse.
    #[test]
    fn mentions_are_extracted_from_prose() {
        assert_eq!(
            mentioned_names("@codex please review, cc @kestrel."),
            vec!["codex".to_string(), "kestrel".to_string()]
        );
        assert_eq!(
            mentioned_names("(@codex-2) and @CODEX-2 again"),
            vec!["codex-2".to_string()]
        );
        assert!(mentioned_names("mail me at user@example.com").is_empty());
        assert!(mentioned_names("nobody is addressed here").is_empty());
    }
}
