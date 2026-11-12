//! The poll loop: delivery claim/mirror/ack, outbound propagation,
//! presence upload, and roster mirroring on one tick cadence.

use super::{
    ClaimOutcome, PresenceSource, process_deliveries, sync_conversations, sync_outbound,
    sync_roster, upload_presence,
};
use std::time::Duration;
use wrkspc_core::{Store, TaskRow};

/// The poll loop's wiring: identity, transport, cadence, and the
/// presence source (unit 8b + HLD rev0.9 discovery).
pub struct PollLoopConfig {
    /// The stamped reqwest client (this node's mTLS identity).
    pub client: reqwest::Client,
    /// The hub base URL.
    pub hub_url: String,
    /// The node's local store.
    pub store: Store,
    /// This node's host id (certificate SAN).
    pub host_id: String,
    /// This node's stable instance id.
    pub instance_id: String,
    /// Poll cadence.
    pub interval: Duration,
    /// The presence source (session registry); `None` skips uploads.
    pub presence: Option<std::sync::Arc<dyn PresenceSource>>,
}

// The presence source is a trait object without Debug; the summary
// carries the observable configuration instead.
impl std::fmt::Debug for PollLoopConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollLoopConfig")
            .field("hub_url", &self.hub_url)
            .field("host_id", &self.host_id)
            .field("interval", &self.interval)
            .field("has_presence", &self.presence.is_some())
            .finish_non_exhaustive()
    }
}
/// Runs the node's poll loop: delivery claim/mirror/ack, node → hub
/// message propagation, presence upload, and roster mirroring.
///
/// `config` is the loop's wiring; `shutdown` is the watch channel that
/// ends the loop.
pub async fn run_poll_loop(
    config: PollLoopConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let client = config.client;
    let hub_url = config.hub_url;
    let store = config.store;
    let host_id = config.host_id;
    let instance_id = config.instance_id;
    let interval = config.interval;
    let presence = config.presence;
    let transport =
        a2a_client::jsonrpc::JsonRpcTransport::new(client.clone(), format!("{hub_url}/jsonrpc"));
    let a2a = a2a_client::A2AClient::new(transport);
    let claim_url = format!("{hub_url}/delivery-claim");
    let ack_url = format!("{hub_url}/delivery-ack");
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        }
        let mut tasks: Vec<TaskRow> = Vec::new();
        let mut page_token = String::new();
        // The hub pages the queue (50 tasks newest-first by default) and
        // keeps terminal rows, so a queue deeper than one page must be
        // walked or older pending deliveries stay unreachable forever.
        // Walk until the hub stops returning a page token.
        loop {
            let request = a2a::ListTasksRequest {
                context_id: Some(host_id.clone()),
                status: None,
                page_size: None,
                page_token: (!page_token.is_empty()).then(|| page_token.clone()),
                status_timestamp_after: None,
                history_length: None,
                include_artifacts: None,
                tenant: None,
            };
            match a2a.list_tasks(&request).await {
                Ok(response) => {
                    let rows: Vec<TaskRow> = response
                        .tasks
                        .iter()
                        .map(|task| TaskRow {
                            task_id: task.id.clone(),
                            context_id: task.context_id.clone(),
                            state: wrkspc_a2a::state_mapping::task_state_literal(
                                &task.status.state,
                            )
                            .to_string(),
                            terminal: task.status.state.is_terminal(),
                            task_json: serde_json::to_string(task).unwrap_or_default(),
                            version: 0,
                            created_at: String::new(),
                            updated_at: String::new(),
                        })
                        .collect();
                    tasks.extend(rows);
                    if response.next_page_token.is_empty() {
                        break;
                    }
                    page_token = response.next_page_token;
                }
                Err(error) => {
                    tracing::warn!(%error, "hub poll failed; retrying");
                    break;
                }
            }
        }
        if !tasks.is_empty() {
            let outcome = process_deliveries(
                &store,
                &host_id,
                &tasks,
                |task_id| {
                    let task_id = task_id.to_string();
                    let client = client.clone();
                    let claim_url = claim_url.clone();
                    let instance_id = instance_id.clone();
                    async move {
                        match client
                            .post(&claim_url)
                            .json(&serde_json::json!({
                                "taskId": task_id,
                                "instanceId": instance_id,
                            }))
                            .send()
                            .await
                        {
                            Ok(response) if response.status().is_success() => ClaimOutcome::Claimed,
                            Ok(response) if response.status() == reqwest::StatusCode::LOCKED => {
                                ClaimOutcome::Gated
                            }
                            // Foreign claim (409) and every failure are the
                            // same skip: this node never mirrors work it
                            // could not durably claim.
                            _ => ClaimOutcome::ForeignClaimed,
                        }
                    }
                },
                |task_id| {
                    let task_id = task_id.to_string();
                    let client = client.clone();
                    let ack_url = ack_url.clone();
                    let instance_id = instance_id.clone();
                    async move {
                        client
                            .post(&ack_url)
                            .json(&serde_json::json!({
                                "taskId": task_id,
                                "instanceId": instance_id,
                            }))
                            .send()
                            .await
                            .is_ok_and(|response| response.status().is_success())
                    }
                },
            )
            .await;
            if outcome.processed > 0 || outcome.failed > 0 || outcome.gated > 0 {
                tracing::info!(
                    processed = outcome.processed,
                    failed = outcome.failed,
                    gated = outcome.gated,
                    "hub poll pass",
                );
            }
        }

        // Node → hub propagation: messages originated by this node's
        // harness peers ride a stamped `message/send` back to the hub,
        // which fans them out to their committed recipients (routing and
        // execution intent now travel with the message). Idempotent on
        // both ends: the hub dedups on the message id, the local ledger
        // prevents re-sends.
        let _sent = sync_outbound(&a2a, &store, &host_id).await;

        // A2A-first discovery (HLD rev0.9): upload this node's peer
        // states and mirror the hub's roster for harness reads.
        if let Some(source) = &presence {
            let decls = source.decls().await;
            let _accepted = upload_presence(&client, &hub_url, &decls).await;
        }
        let _refreshed = sync_roster(&client, &hub_url, &store).await;
        let _rooms = sync_conversations(&client, &hub_url, &store).await;

        // Node → hub outcome leg (H-10): terminal attempt results for
        // hub-originated deliveries ride a stamped `delivery-report`
        // back, so the hub's task learns execution outcomes instead of
        // reading completion from the ack. Retried until the hub
        // confirms (or answers "unknown task" — then the report is
        // dropped; there is no task to fold it into).
        for report in store.hub_pending_outcomes().await.unwrap_or_default() {
            // Guidance consumed locally rides the report: the hub marks
            // its authoritative rows delivered (idempotent; unknown ids
            // are no-ops), closing the propagation loop.
            let guidance_delivered = store
                .delivered_guidance_ids(&report.conversation_id)
                .await
                .unwrap_or_default();
            let response = client
                .post(format!("{hub_url}/delivery-report"))
                .json(&serde_json::json!({
                    "taskId": report.task_id,
                    "instanceId": instance_id,
                    "attemptId": report.attempt_id,
                    "state": report.state,
                    "recordedAt": chrono::Utc::now().to_rfc3339(),
                    "guidanceDelivered": guidance_delivered,
                }))
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {}
                // The hub has no task for this pair: a local message,
                // or a repaired ledger — nothing to report against.
                Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {}
                // A conflicting terminal state: the hub already holds
                // an outcome record; the report is satisfied.
                Ok(response) if response.status() == reqwest::StatusCode::CONFLICT => {}
                Ok(response) => {
                    tracing::warn!(
                        status = %response.status(),
                        task_id = %report.task_id,
                        "outcome report rejected; retrying next poll"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, task_id = %report.task_id, "outcome report failed; retrying");
                    continue;
                }
            }
            // Unknown outcomes stay pending in the ledger: the hub
            // mirrors them WITHOUT a task transition, and once the
            // operator's decision is applied locally the terminal
            // state must still report through this leg.
            if report.state != "unknown"
                && let Err(error) = store
                    .mark_outcome_reported(&report.attempt_id, &report.task_id)
                    .await
            {
                tracing::warn!(%error, attempt_id = %report.attempt_id, "outcome ledger write failed");
            }
        }

        // Hub -> node reconciliation leg: operator decisions queued on
        // the hub's mirrored unknown attempts apply to THIS store's
        // attempt rows; the resulting terminal state then reports back
        // through the outcome leg, which consumes the mirror.
        if let Ok(response) = client
            .get(format!("{hub_url}/reconciliation-decisions"))
            .send()
            .await
            && response.status().is_success()
        {
            let decisions: Vec<wrkspc_core::HubUnknownView> = match response.json().await {
                Ok(decisions) => decisions,
                Err(error) => {
                    tracing::warn!(%error, "reconciliation decisions unparseable");
                    Vec::new()
                }
            };
            for decision in decisions {
                let Some(encoded) = decision.decision.as_deref() else {
                    continue;
                };
                let reconciliation = match encoded {
                    "retry" => Some(wrkspc_core::Reconciliation::Retry),
                    "close:failed" => Some(wrkspc_core::Reconciliation::Close {
                        state: wrkspc_core::AttemptState::Failed,
                    }),
                    "close:completed" => Some(wrkspc_core::Reconciliation::Close {
                        state: wrkspc_core::AttemptState::Completed,
                    }),
                    "close:canceled" => Some(wrkspc_core::Reconciliation::Close {
                        state: wrkspc_core::AttemptState::Canceled,
                    }),
                    _ => None,
                };
                let Some(reconciliation) = reconciliation else {
                    tracing::warn!(%encoded, attempt_id = %decision.attempt_id, "unknown reconciliation decision");
                    continue;
                };
                match store
                    .reconcile_unknown(&decision.attempt_id, reconciliation)
                    .await
                {
                    Ok(()) => tracing::info!(
                        attempt_id = %decision.attempt_id,
                        "hub reconciliation decision applied"
                    ),
                    Err(error) => tracing::warn!(
                        %error,
                        attempt_id = %decision.attempt_id,
                        "hub reconciliation decision failed; retrying next poll"
                    ),
                }
            }
        }
    }
}
