//! Node-side presence declarations and the discovery legs
//! (presence upload, roster mirror).

use wrkspc_core::Store;

/// A node-side presence declaration for one peer (HLD rev0.9): the
/// node reports each peer's live state to the hub on connect and
/// session state change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceDecl {
    /// The peer id.
    pub peer_id: String,
    /// The display name.
    pub name: String,
    /// The peer class (`harness`, `human`, `a2a_native`).
    pub class: String,
    /// The last reported live state.
    pub state: String,
}

/// The seam that computes the node's current presence declarations.
///
/// Implemented by the session registry (session live → `connected`,
/// otherwise `disconnected`); a node with no bindings reports an empty
/// list. Dyn-safe by contract: the method returns a boxed future, never
/// an RPITIT (RPITIT returns are not object-safe).
pub trait PresenceSource: Send + Sync {
    /// Returns the declarations to upload this tick.
    fn decls(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<PresenceDecl>> + Send + '_>>;
}
/// Mirrors the hub's roster into the local store (node-only).
///
/// A failed fetch keeps the last-known mirror — stale-but-visible, per
/// the freshness decision (the UI shows last-seen age).
///
/// # Arguments
///
/// * `client` — the stamped reqwest client (mTLS identity).
/// * `hub_url` — the hub base URL.
/// * `store` — the node's local store.
///
/// # Returns
///
/// `true` when the mirror was refreshed this tick.
pub(crate) async fn sync_roster(client: &reqwest::Client, hub_url: &str, store: &Store) -> bool {
    match client.get(format!("{hub_url}/roster")).send().await {
        Ok(response) if response.status().is_success() => {
            match response.json::<Vec<wrkspc_core::RosterEntry>>().await {
                Ok(entries) => match store.replace_roster(&entries).await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(%error, "roster mirror write failed");
                        false
                    }
                },
                Err(error) => {
                    tracing::warn!(%error, "roster parse failed");
                    false
                }
            }
        }
        Ok(response) => {
            tracing::warn!(status = %response.status(), "roster fetch refused");
            false
        }
        Err(error) => {
            tracing::warn!(%error, "roster fetch failed; keeping last-known mirror");
            false
        }
    }
}

/// Uploads this tick's presence declarations (node → hub).
///
/// Failed uploads are retried next tick — presence is a heartbeat, not
/// a queue, so nothing is buffered here.
///
/// # Arguments
///
/// * `client` — the stamped reqwest client (mTLS identity).
/// * `hub_url` — the hub base URL.
/// * `decls` — the declarations to report.
///
/// # Returns
///
/// The number of declarations the hub accepted this tick.
pub(crate) async fn upload_presence(
    client: &reqwest::Client,
    hub_url: &str,
    decls: &[PresenceDecl],
) -> usize {
    let mut accepted = 0;
    for decl in decls {
        match client
            .post(format!("{hub_url}/presence"))
            .json(&serde_json::json!({
                "peerId": decl.peer_id,
                "name": decl.name,
                "class": decl.class,
                "state": decl.state,
            }))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => accepted += 1,
            Ok(response) => {
                tracing::warn!(
                    status = %response.status(),
                    peer = %decl.peer_id,
                    "presence upload refused; retrying next tick"
                );
            }
            Err(error) => {
                tracing::warn!(%error, peer = %decl.peer_id, "presence upload failed; retrying next tick");
            }
        }
    }
    accepted
}
