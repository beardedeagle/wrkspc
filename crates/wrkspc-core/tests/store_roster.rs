//! Presence and the roster: first-report enrolment, state updates,
//! cross-host refusal, and full mirror replacement.

mod common;

use wrkspc_core::Store;

/// First presence enrols the peer on the presenting host and materializes its roster row.
#[tokio::test]
async fn presence_first_report_enrols_and_materializes_roster() {
    let store = Store::open_in_memory("presence-first").await.unwrap();
    let outcome = store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    assert_eq!(outcome, wrkspc_core::PresenceOutcome::Updated);

    // Enrolment happened under acceptance's rule: the peer exists on
    // the presenting host, and the roster row is materialized from the
    // authoritative identity fields.
    let details = store.peer_details("codex").await.unwrap().unwrap();
    assert_eq!(
        details,
        (
            "codex".to_string(),
            "harness".to_string(),
            "node-test-1".to_string()
        )
    );
    let roster = store.roster().await.unwrap();
    assert_eq!(roster.len(), 1);
    assert_eq!(roster[0].peer_id, "codex");
    assert_eq!(roster[0].host_id, "node-test-1");
    assert_eq!(roster[0].state, "connected");
}

/// A same-host presence report updates only the live state.
#[tokio::test]
async fn presence_same_host_updates_state_only() {
    let store = Store::open_in_memory("presence-update").await.unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let outcome = store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "disconnected")
        .await
        .unwrap();
    assert_eq!(outcome, wrkspc_core::PresenceOutcome::Updated);
    let roster = store.roster().await.unwrap();
    assert_eq!(roster[0].state, "disconnected");
    assert_eq!(roster[0].host_id, "node-test-1");
}

/// A presence report from a different host is refused (HostConflict) and changes nothing.
#[tokio::test]
async fn presence_cross_host_report_is_refused() {
    let store = Store::open_in_memory("presence-conflict").await.unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let outcome = store
        .upsert_roster_presence("codex", "codex", "harness", "node-other", "connected")
        .await
        .unwrap();
    assert_eq!(
        outcome,
        wrkspc_core::PresenceOutcome::HostConflict {
            owner_host: "node-test-1".to_string()
        }
    );
    // The binding and the state are untouched by the refused report.
    let roster = store.roster().await.unwrap();
    assert_eq!(roster[0].host_id, "node-test-1");
    assert_eq!(roster[0].state, "connected");
}

/// Replacing the roster mirror fully replaces the previous rows (no stale entries linger).
#[tokio::test]
async fn roster_mirror_replaces_fully() {
    let store = Store::open_in_memory("roster-mirror").await.unwrap();
    let entries = vec![
        wrkspc_core::RosterEntry {
            peer_id: "codex".to_string(),
            name: "codex".to_string(),
            alias: Some("C-1".to_string()),
            class: "harness".to_string(),
            host_id: "node-test-1".to_string(),
            status: "booted".to_string(),
            state: "connected".to_string(),
            updated_at: "2026-10-23T00:00:00Z".to_string(),
        },
        wrkspc_core::RosterEntry {
            peer_id: "operator".to_string(),
            name: "operator".to_string(),
            alias: None,
            class: "human".to_string(),
            host_id: wrkspc_core::PILOT_HOST_ID.to_string(),
            status: "enrolled".to_string(),
            state: "connected".to_string(),
            updated_at: "2026-10-23T00:00:00Z".to_string(),
        },
    ];
    store.replace_roster(&entries).await.unwrap();
    assert_eq!(store.roster().await.unwrap(), entries);
    // A smaller mirror fully replaces — nothing lingers.
    store.replace_roster(&entries[..1]).await.unwrap();
    let roster = store.roster().await.unwrap();
    assert_eq!(roster.len(), 1);
    assert_eq!(roster[0].peer_id, "codex");
    assert!(store.roster_entry("operator").await.unwrap().is_none());
}

/// Removing a cohort entry hides it immediately, and the peer comes back with
/// a clean identity on its next turn.
///
/// The operator's complaint that motivated this: with several peers sharing a
/// harness name and stale aliases, they could not tell the entries apart and
/// could not remove them. Delete must work on an enrolled peer (not only an
/// ejected one), clear the alias, and let the peer re-enrol itself with its
/// plain binding name.
#[tokio::test]
async fn deleted_cohort_entry_returns_clean_on_next_presence() {
    let store = Store::open_in_memory("cohort-delete").await.unwrap();
    store
        .upsert_roster_presence("claude-1", "claude", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    store
        .set_peer_alias("claude-1", Some("claude-eh"), "operator")
        .await
        .unwrap();
    assert_eq!(
        store.roster().await.unwrap()[0].alias.as_deref(),
        Some("claude-eh")
    );

    // Delete works on an enrolled peer, not just an ejected one.
    assert!(
        store.forget_peer("claude-1", "operator").await.unwrap(),
        "an enrolled peer must be removable"
    );
    assert!(
        store.roster().await.unwrap().is_empty(),
        "the entry disappears from the cohort immediately"
    );

    // Its next turn re-enrols it, with no alias carried over.
    store
        .upsert_roster_presence("claude-1", "claude", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    let roster = store.roster().await.unwrap();
    let entry = roster
        .iter()
        .find(|row| row.peer_id == "claude-1")
        .expect("the peer re-enrols on its next presence");
    assert_eq!(entry.alias, None, "the stale alias must not come back");
    assert_eq!(entry.name, "claude", "the plain binding name is restored");
}
