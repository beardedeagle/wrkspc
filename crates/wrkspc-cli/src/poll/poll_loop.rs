//! The poll loop: delivery claim/mirror/ack, outbound propagation,
//! presence upload, and roster mirroring on one tick cadence.

use super::{
    PresenceSource, process_deliveries, sync_conversations, sync_outbound, sync_roster,
    upload_presence,
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
/// Runs the node's poll loop: delivery claim/mirror/ack, node → hub
/// message propagation, presence upload, and roster mirroring.
///
/// # Arguments
///
/// * `config` — the loop's wiring.
/// * `shutdown` — the watch channel that ends the loop.
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
        let request = a2a::ListTasksRequest {
            context_id: Some(host_id.clone()),
            status: None,
            page_size: None,
            page_token: None,
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
                        state: wrkspc_a2a::state_mapping::task_state_literal(&task.status.state)
                            .to_string(),
                        terminal: task.status.state.is_terminal(),
                        task_json: serde_json::to_string(task).unwrap_or_default(),
                        version: 0,
                        created_at: String::new(),
                        updated_at: String::new(),
                    })
                    .collect();
                let outcome = process_deliveries(
                    &store,
                    &host_id,
                    &rows,
                    |task_id| {
                        let task_id = task_id.to_string();
                        let client = client.clone();
                        let claim_url = claim_url.clone();
                        let instance_id = instance_id.clone();
                        async move {
                            client
                                .post(&claim_url)
                                .json(&serde_json::json!({
                                    "taskId": task_id,
                                    "instanceId": instance_id,
                                }))
                                .send()
                                .await
                                .is_ok_and(|response| response.status().is_success())
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
                if outcome.processed > 0 || outcome.failed > 0 {
                    tracing::info!(
                        processed = outcome.processed,
                        failed = outcome.failed,
                        "hub poll pass",
                    );
                }
            }
            Err(error) => tracing::warn!(%error, "hub poll failed; retrying"),
        }

        // Node → hub propagation: messages originated by this node's
        // harness peers ride a stamped `message/send` back to the hub,
        // which fans them out to every other member (operator and the
        // other host's peers). Idempotent on both ends: the hub dedups
        // on the message id, the local ledger prevents re-sends.
        let _sent = sync_outbound(&a2a, &store, &host_id).await;

        // A2A-first discovery (HLD rev0.9): upload this node's peer
        // states and mirror the hub's roster for harness reads.
        if let Some(source) = &presence {
            let decls = source.decls().await;
            let _accepted = upload_presence(&client, &hub_url, &decls).await;
        }
        let _refreshed = sync_roster(&client, &hub_url, &store).await;
        let _rooms = sync_conversations(&client, &hub_url, &store).await;
    }
}
