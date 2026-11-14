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
/// `client` is the stamped reqwest client (mTLS identity); `hub_url` is
/// the hub base URL; `store` is the node's local store.
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
/// `client` is the stamped reqwest client (mTLS identity); `hub_url` is
/// the hub base URL; `decls` is the declarations to report.
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

/// Mirrors the hub's conversation list into the local store.
///
/// A node learns a room from the first delivery that reaches it, which leaves
/// every other room invisible and every operator-set name unknown. This asks the
/// hub for its rooms each tick and mirrors them, so both machines show the same
/// list. Failures keep the last-known mirror rather than emptying it.
///
/// `client` is the node's stamped mTLS client; `hub_url` is the hub's
/// base URL; `store` is the node's local store.
///
/// # Returns
///
/// Whether the mirror was refreshed.
pub(crate) async fn sync_conversations(
    client: &reqwest::Client,
    hub_url: &str,
    store: &Store,
) -> bool {
    match client.get(format!("{hub_url}/conversations")).send().await {
        Ok(response) if response.status().is_success() => {
            match response.json::<Vec<wrkspc_core::ConversationView>>().await {
                Ok(conversations) => match store.mirror_conversations(&conversations).await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(%error, "conversation mirror write failed");
                        false
                    }
                },
                Err(error) => {
                    tracing::warn!(%error, "conversation list parse failed");
                    false
                }
            }
        }
        Ok(response) => {
            tracing::warn!(status = %response.status(), "conversation list fetch refused");
            false
        }
        Err(error) => {
            tracing::warn!(%error, "conversation list fetch failed; keeping last-known mirror");
            false
        }
    }
}
