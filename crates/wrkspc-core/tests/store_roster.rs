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
            updated_at: chrono::Utc::now().to_rfc3339(),
            enrolled_at: "2026-01-01T00:00:00Z".to_string(),
        },
        wrkspc_core::RosterEntry {
            peer_id: "operator".to_string(),
            name: "operator".to_string(),
            alias: None,
            class: "human".to_string(),
            host_id: wrkspc_core::PILOT_HOST_ID.to_string(),
            status: "enrolled".to_string(),
            state: "connected".to_string(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            enrolled_at: "2026-01-01T00:00:00Z".to_string(),
        },
    ];
    store.replace_roster(&entries).await.unwrap();
    // The mirror stores presence only; the enrolment stamp is joined from
    // `peers`, so these unenrolled test ids read back empty.
    let read_back = store.roster().await.unwrap();
    assert_eq!(read_back.len(), entries.len());
    for (row, want) in read_back.iter().zip(&entries) {
        assert_eq!(row.peer_id, want.peer_id);
        assert_eq!(row.name, want.name);
        assert_eq!(row.alias, want.alias);
        assert_eq!(row.status, want.status);
        assert_eq!(row.state, want.state);
        assert!(
            row.enrolled_at.is_empty(),
            "no peers row, no enrolment stamp"
        );
    }
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

    // Its next presence must NOT bring it back: the node re-reports its
    // bindings every few seconds, so re-materialising made delete look broken.
    let outcome = store
        .upsert_roster_presence("claude-1", "claude", "harness", "node-test-1", "connected")
        .await
        .unwrap();
    assert!(
        matches!(outcome, wrkspc_core::PresenceOutcome::Ejected { .. }),
        "a removed entry stays removed while the peer keeps reporting"
    );
    assert!(
        store.roster_entry("claude-1").await.unwrap().is_none(),
        "the cohort entry must not come back on its own"
    );

    // The operator's restore is the only way back, and it returns the peer
    // with its plain binding name and no stale alias.
    store
        .set_peer_status("claude-1", wrkspc_core::PeerStatus::Enrolled, "operator")
        .await
        .unwrap();
    let roster = store.roster().await.unwrap();
    let entry = roster
        .iter()
        .find(|row| row.peer_id == "claude-1")
        .expect("restore re-materialises the entry");
    assert_eq!(entry.alias, None, "the stale alias must not come back");
    assert_eq!(entry.name, "claude", "the plain binding name is restored");
}

/// A peer that stopped reporting reads as disconnected, however green its last
/// report was. Without ageing, every peer that ever ran stays live-looking
/// forever and the cohort fills with entries nobody can account for.
#[tokio::test]
async fn stale_presence_reads_as_disconnected() {
    let store = Store::open_in_memory("stale-presence").await.unwrap();
    store
        .replace_roster(&[wrkspc_core::RosterEntry {
            peer_id: "codex".to_string(),
            name: "codex".to_string(),
            alias: None,
            class: "harness".to_string(),
            host_id: "node-test-1".to_string(),
            status: "enrolled".to_string(),
            state: "connected".to_string(),
            updated_at: "2020-01-01T00:00:00Z".to_string(),
            enrolled_at: "2026-01-01T00:00:00Z".to_string(),
        }])
        .await
        .unwrap();
    assert_eq!(
        store.roster().await.unwrap()[0].state,
        "disconnected",
        "a report from years ago is not a live peer"
    );
}
