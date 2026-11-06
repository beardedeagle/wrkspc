//! The task-persistence half of the [`Store`](crate::store::Store):
//! task CRUD with keyset pagination, delivery-task queueing, and the
//! delivery claim lifecycle.

use super::{DeliveryClaimOutcome, DeliveryTaskRequest, TaskRow, task_row_from_sql};
use crate::error::{Result, StoreError};
use crate::store::Store;
use chrono::Utc;
use sqlx::Row;

impl Store {
    /// Creates a task row. The initial version is 1.
    ///
    /// # Arguments
    ///
    /// * `task_id` — unique task identifier.
    /// * `context_id` — logical grouping identifier.
    /// * `state` — the caller's state literal (opaque to core).
    /// * `terminal` — whether `state` is a terminal state.
    /// * `task_json` — the caller's serialized task blob (opaque to core).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AlreadyExists`](StoreError::AlreadyExists) when
    /// the task id exists, [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn task_create(
        &self,
        task_id: &str,
        context_id: &str,
        state: &str,
        terminal: bool,
        task_json: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "INSERT INTO a2a_tasks
               (task_id, context_id, state, terminal, task_json, version, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(task_id)
        .bind(context_id)
        .bind(state)
        .bind(terminal)
        .bind(task_json)
        .bind(&now)
        .bind(&now)
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::AlreadyExists {
                entity: "a2a task",
                id: task_id.to_string(),
            });
        }
        Ok(())
    }

    /// Updates a task row, enforcing terminal-state immutability.
    ///
    /// Re-applying the SAME terminal state refreshes the blob/timestamp and
    /// keeps the version (idempotent re-emission); a different state over a
    /// terminal row is rejected.
    ///
    /// # Arguments
    ///
    /// * `task_id` — the task to update.
    /// * `state` — the new state literal.
    /// * `terminal` — whether the new state is terminal.
    /// * `task_json` — the new blob.
    ///
    /// # Returns
    ///
    /// The new version counter.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the task
    /// does not exist, [`StoreError::TaskTerminal`](StoreError::TaskTerminal)
    /// on an illegal transition, [`StoreError::Db`](StoreError::Db) on
    /// failure.
    pub async fn task_update(
        &self,
        task_id: &str,
        state: &str,
        terminal: bool,
        task_json: &str,
    ) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query("SELECT state, terminal, version FROM a2a_tasks WHERE task_id = ?")
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "a2a task",
                id: task_id.to_string(),
            })?;
        let current_state: String = row.get("state");
        let current_terminal: bool = row.get("terminal");
        let current_version: i64 = row.get("version");

        if current_terminal {
            if current_state == state && terminal {
                // Idempotent re-emission of the same terminal state.
                sqlx::query("UPDATE a2a_tasks SET task_json = ?, updated_at = ? WHERE task_id = ?")
                    .bind(task_json)
                    .bind(&now)
                    .bind(task_id)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                return Ok(current_version);
            }
            return Err(StoreError::TaskTerminal {
                task_id: task_id.to_string(),
                state: current_state,
                requested: state.to_string(),
            });
        }

        let new_version = current_version + 1;
        sqlx::query(
            "UPDATE a2a_tasks
             SET state = ?, terminal = ?, task_json = ?, version = ?, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(state)
        .bind(terminal)
        .bind(task_json)
        .bind(new_version)
        .bind(&now)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(new_version)
    }

    /// Fetches a task row.
    ///
    /// # Arguments
    ///
    /// * `task_id` — the task to fetch.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn task_get(&self, task_id: &str) -> Result<Option<TaskRow>> {
        let row = sqlx::query("SELECT * FROM a2a_tasks WHERE task_id = ?")
            .bind(task_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map(task_row_from_sql))
    }

    /// Lists task rows, newest first, with keyset pagination.
    ///
    /// # Arguments
    ///
    /// * `context_id` — optional context filter.
    /// * `state` — optional state-literal filter.
    /// * `status_after` — optional RFC 3339 lower bound on `updated_at`.
    /// * `page` — optional `(updated_at, task_id)` keyset bound (exclusive).
    /// * `limit` — maximum rows returned (no extra-row probing; the caller
    ///   requests limit+1 to detect another page).
    ///
    /// # Returns
    ///
    /// The page rows and the total count under the same filters.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn task_list(
        &self,
        context_id: Option<&str>,
        state: Option<&str>,
        status_after: Option<&str>,
        page: Option<(&str, &str)>,
        limit: i64,
    ) -> Result<(Vec<TaskRow>, i64)> {
        let mut filters: Vec<&str> = Vec::new();
        let mut binds: Vec<String> = Vec::new();

        if let Some(value) = context_id {
            filters.push("context_id = ?");
            binds.push(value.to_string());
        }
        if let Some(value) = state {
            filters.push("state = ?");
            binds.push(value.to_string());
        }
        if let Some(value) = status_after {
            filters.push("updated_at >= ?");
            binds.push(value.to_string());
        }
        if let Some((updated_at, task_id)) = page {
            filters.push("(updated_at < ? OR (updated_at = ? AND task_id < ?))");
            binds.push(updated_at.to_string());
            binds.push(updated_at.to_string());
            binds.push(task_id.to_string());
        }

        let where_clause = if filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", filters.join(" AND "))
        };

        let count_sql = format!("SELECT COUNT(*) FROM a2a_tasks {where_clause}");
        let mut count_query = sqlx::query_scalar(&count_sql);
        for bind in &binds {
            count_query = count_query.bind(bind);
        }
        let total: i64 = count_query.fetch_one(self.pool()).await?;

        let rows_sql = format!(
            "SELECT * FROM a2a_tasks {where_clause}
             ORDER BY updated_at DESC, task_id DESC LIMIT ?"
        );
        let mut rows_query = sqlx::query(&rows_sql);
        for bind in &binds {
            rows_query = rows_query.bind(bind);
        }
        let rows = rows_query.bind(limit).fetch_all(self.pool()).await?;
        Ok((rows.into_iter().map(task_row_from_sql).collect(), total))
    }

    /// Creates a delivery task: a hub-owned A2A task row that a remote
    /// node polls for work addressed to its host (D-6 A2 contract — the
    /// node dials the hub; the hub never calls the node). The row's
    /// `context_id` is the recipient host's queue, `state` is the A2A
    /// `submitted` literal, and `task_json` carries the delivery payload
    /// the node needs to record and dispatch locally.
    ///
    /// # Arguments
    ///
    /// * `request` — the delivery to queue.
    ///
    /// # Returns
    ///
    /// The new task id.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn task_create_delivery(&self, request: DeliveryTaskRequest<'_>) -> Result<String> {
        let task_id = uuid::Uuid::new_v4().to_string();
        // The stored blob is a canonical A2A Task serialization (the
        // surface serves `task_json` as the wire Task), with the
        // wrkspc delivery envelope riding in `metadata.delivery`.
        let payload = serde_json::json!({
            "id": task_id,
            "contextId": request.recipient_host,
            "status": {"state": "TASK_STATE_SUBMITTED"},
            "metadata": {
                "delivery": {
                    "kind": "delivery",
                    "message_id": request.message_id,
                    "conversation_id": request.conversation_id,
                    "sender_peer": request.sender_peer,
                    "sender_name": request.sender_name,
                    "sender_class": request.sender_class,
                    "sender_host": request.sender_host,
                    "recipient_peer": request.recipient_peer,
                    "recipient_name": request.recipient_name,
                    "recipient_class": request.recipient_class,
                    "content": request.content,
                    "execution_requested": request.execution_requested,
                },
            },
        });
        self.task_create(
            &task_id,
            request.recipient_host,
            "submitted",
            false,
            &payload.to_string(),
        )
        .await?;
        Ok(task_id)
    }

    /// Claims a delivery task exclusively for a node instance
    /// (double-attach enforcement: a delivery may be mirrored and
    /// dispatched by exactly one node — the claim is taken at poll
    /// time, BEFORE any mirroring). Idempotent for the claiming
    /// instance; a second instance presenting the same cert gets the
    /// foreign-claim outcome. Terminal tasks cannot be claimed.
    ///
    /// # Arguments
    ///
    /// * `task_id` — the delivery task to claim.
    /// * `host_id` — the claiming host (its certificate SAN).
    /// * `instance_id` — the node's stable local identity (distinguishes
    ///   two processes sharing one copied cert).
    ///
    /// # Returns
    ///
    /// The claim outcome.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) for an
    /// unknown task, [`StoreError::TaskTerminal`](StoreError::TaskTerminal)
    /// for a terminal task, and
    /// [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn claim_delivery(
        &self,
        task_id: &str,
        host_id: &str,
        instance_id: &str,
    ) -> Result<DeliveryClaimOutcome> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query("SELECT terminal FROM a2a_tasks WHERE task_id = ?")
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "a2a task",
                id: task_id.to_string(),
            })?;
        let terminal: bool = row.get("terminal");
        if terminal {
            return Err(StoreError::TaskTerminal {
                task_id: task_id.to_string(),
                state: "terminal".to_string(),
                requested: "claimed".to_string(),
            });
        }
        let existing: Option<String> =
            sqlx::query("SELECT instance_id FROM delivery_claims WHERE task_id = ?")
                .bind(task_id)
                .fetch_optional(&mut *tx)
                .await?
                .map(|r| r.get("instance_id"));
        if let Some(claimant) = existing {
            if claimant == instance_id {
                return Ok(DeliveryClaimOutcome::AlreadyClaimed);
            }
            let claimed_at: String =
                sqlx::query("SELECT claimed_at FROM delivery_claims WHERE task_id = ?")
                    .bind(task_id)
                    .fetch_one(&mut *tx)
                    .await?
                    .get("claimed_at");
            return Ok(DeliveryClaimOutcome::ForeignClaimed {
                instance_id: claimant,
                claimed_at,
            });
        }
        sqlx::query(
            "INSERT INTO delivery_claims (task_id, host_id, instance_id, claimed_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(task_id)
        .bind(host_id)
        .bind(instance_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(DeliveryClaimOutcome::Claimed)
    }

    /// Releases a delivery claim (operator moderation op: a stuck claim
    /// from a dead node blocks the delivery until released — claims have
    /// no TTL by design). Idempotent: releasing an unclaimed task is a
    /// no-op.
    ///
    /// # Arguments
    ///
    /// * `task_id` — the delivery task to release.
    ///
    /// # Returns
    ///
    /// `true` when a claim was removed.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn release_delivery_claim(&self, task_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM delivery_claims WHERE task_id = ?")
            .bind(task_id)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Reads a delivery claim, if one exists.
    ///
    /// # Arguments
    ///
    /// * `task_id` — the delivery task to query.
    ///
    /// # Returns
    ///
    /// `Some((host_id, instance_id, claimed_at))`, or `None` when
    /// unclaimed.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Db`](StoreError::Db) on failure.
    pub async fn delivery_claim(&self, task_id: &str) -> Result<Option<(String, String, String)>> {
        let row = sqlx::query(
            "SELECT host_id, instance_id, claimed_at FROM delivery_claims WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|r| (r.get("host_id"), r.get("instance_id"), r.get("claimed_at"))))
    }

    /// Atomically transitions a non-terminal task to a terminal canceled
    /// state.
    ///
    /// The check and the transition run in one transaction.
    ///
    /// # Arguments
    ///
    /// * `task_id` — the task to cancel.
    /// * `canceled_state` — the caller's canceled-state literal.
    /// * `canceled_json` — the caller's serialized canceled-task blob.
    ///
    /// # Returns
    ///
    /// The updated row.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`](StoreError::NotFound) when the task
    /// does not exist, [`StoreError::TaskTerminal`](StoreError::TaskTerminal)
    /// when it is already terminal, [`StoreError::Db`](StoreError::Db) on
    /// failure.
    pub async fn task_begin_cancel(
        &self,
        task_id: &str,
        canceled_state: &str,
        canceled_json: &str,
    ) -> Result<TaskRow> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query("SELECT * FROM a2a_tasks WHERE task_id = ?")
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "a2a task",
                id: task_id.to_string(),
            })?;
        let current = task_row_from_sql(row);
        if current.terminal {
            return Err(StoreError::TaskTerminal {
                task_id: task_id.to_string(),
                state: current.state,
                requested: canceled_state.to_string(),
            });
        }
        sqlx::query(
            "UPDATE a2a_tasks
             SET state = ?, terminal = 1, task_json = ?, version = version + 1, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(canceled_state)
        .bind(canceled_json)
        .bind(&now)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let updated = self
            .task_get(task_id)
            .await?
            .expect("row committed above must exist");
        Ok(updated)
    }
}
