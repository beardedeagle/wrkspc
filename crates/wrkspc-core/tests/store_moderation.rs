//! Peer moderation: alias, status transitions, ban revocations,
//! the audit log, soft-forget, and first-presented-serial wins.

mod common;

use wrkspc_core::{PILOT_HOST_ID, Store};

/// Peer alias sets, clears, and mirrors into the roster row.
#[tokio::test]
async fn peer_alias_sets_clears_and_mirrors_roster() {
    let store = Store::open_in_memory("test").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    store
        .set_peer_alias("codex", Some("C-1"), "operator")
        .await
        .unwrap();
    let entry = store.roster_entry("codex").await.unwrap().unwrap();
    assert_eq!(entry.alias.as_deref(), Some("C-1"));
    store
        .set_peer_alias("codex", None, "operator")
        .await
        .unwrap();
    let entry = store.roster_entry("codex").await.unwrap().unwrap();
    assert!(entry.alias.is_none());
}

/// Boot/ban/restore transition the peer status; ban records the presented serial as a revocation and restore clears it.
#[tokio::test]
async fn peer_status_transitions_and_ban_records_revocation() {
    let store = Store::open_in_memory("test").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    store
        .record_peer_cert_serial("codex", "aa11")
        .await
        .unwrap();
    // Boot.
    assert!(
        store
            .set_peer_status("codex", wrkspc_core::PeerStatus::Booted, "operator")
            .await
            .unwrap()
    );
    assert_eq!(
        store.peer_status("codex").await.unwrap().as_deref(),
        Some("booted")
    );
    // A booted peer's presence is refused.
    let outcome = store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        wrkspc_core::PresenceOutcome::Ejected { .. }
    ));
    // Ban: records the presented serial as a revocation.
    assert!(
        store
            .set_peer_status("codex", wrkspc_core::PeerStatus::Banned, "operator")
            .await
            .unwrap()
    );
    assert_eq!(
        store.cert_revocations().await.unwrap(),
        vec!["aa11".to_string()]
    );
    assert_eq!(
        store.peer_status("codex").await.unwrap().as_deref(),
        Some("banned")
    );
    // Restore clears the revocation and re-admits presence.
    assert!(
        store
            .set_peer_status("codex", wrkspc_core::PeerStatus::Enrolled, "operator")
            .await
            .unwrap()
    );
    assert!(store.cert_revocations().await.unwrap().is_empty());
    let outcome = store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    assert!(matches!(outcome, wrkspc_core::PresenceOutcome::Updated));
    // Unknown peers report no change.
    assert!(
        !store
            .set_peer_status("ghost", wrkspc_core::PeerStatus::Booted, "operator")
            .await
            .unwrap()
    );
}

/// The audit log records every transition newest-first with actor and before/after values.
#[tokio::test]
async fn moderation_audit_logs_every_transition() {
    let store = Store::open_in_memory("test").await.unwrap();
    // First presence performs the enrolment itself — a system audit row.
    store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    store
        .set_peer_alias("codex", Some("C-1"), "operator")
        .await
        .unwrap();
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Booted, "operator")
        .await
        .unwrap();
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Banned, "operator")
        .await
        .unwrap();
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Enrolled, "operator")
        .await
        .unwrap();

    let log = store.moderation_log(100).await.unwrap();
    // Newest first: restore, ban, boot, alias, enrol.
    let actions: Vec<&str> = log.iter().map(|e| e.action.as_str()).collect();
    assert_eq!(actions, ["restore", "ban", "boot", "alias", "enrol"]);

    let enrol = log.iter().find(|e| e.action == "enrol").unwrap();
    assert_eq!(enrol.actor, "system");
    assert_eq!(enrol.after.as_deref(), Some(PILOT_HOST_ID));

    let boot = log.iter().find(|e| e.action == "boot").unwrap();
    assert_eq!(boot.actor, "operator");
    assert_eq!(boot.before.as_deref(), Some("enrolled"));
    assert_eq!(boot.after.as_deref(), Some("booted"));

    let alias = log.iter().find(|e| e.action == "alias").unwrap();
    assert_eq!(alias.before.as_deref(), None);
    assert_eq!(alias.after.as_deref(), Some("C-1"));
}

/// Forget soft-hides the cohort entry while history and revocations survive; restore re-materializes.
#[tokio::test]
async fn forget_is_soft_hidden_and_preserves_revocations() {
    let store = Store::open_in_memory("test").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    store
        .record_peer_cert_serial("codex", "aa11")
        .await
        .unwrap();
    // Only ejected peers may be forgotten.
    assert!(!store.forget_peer("codex", "operator").await.unwrap());
    // Unknown peers report no change.
    assert!(!store.forget_peer("ghost", "operator").await.unwrap());

    // Ban, then forget: the revocation must survive.
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Banned, "operator")
        .await
        .unwrap();
    assert!(store.forget_peer("codex", "operator").await.unwrap());

    // The cohort entry disappears…
    assert!(store.roster_entry("codex").await.unwrap().is_none());
    // …while the peers row and its revocation survive (history intact).
    assert_eq!(
        store.peer_status("codex").await.unwrap().as_deref(),
        Some("banned")
    );
    assert_eq!(
        store.cert_revocations().await.unwrap(),
        vec!["aa11".to_string()]
    );
    // Presence is refused, so the node cannot silently resurrect it.
    let outcome = store
        .upsert_roster_presence("codex", "codex", "harness", PILOT_HOST_ID, "connected")
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        wrkspc_core::PresenceOutcome::Ejected { .. }
    ));
    // The audit has the forget row.
    let log = store.moderation_log(100).await.unwrap();
    assert_eq!(log[0].action, "forget");
    assert_eq!(log[0].before.as_deref(), Some("banned"));

    // An explicit restore brings the cohort row back.
    store
        .set_peer_status("codex", wrkspc_core::PeerStatus::Enrolled, "operator")
        .await
        .unwrap();
    let entry = store.roster_entry("codex").await.unwrap().unwrap();
    assert_eq!(entry.status, "enrolled");
    assert!(store.cert_revocations().await.unwrap().is_empty());
}

/// The first presented cert serial wins; later serials are ignored.
#[tokio::test]
async fn first_presented_serial_wins() {
    let store = Store::open_in_memory("test").await.unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    store
        .record_peer_cert_serial("codex", "aa11")
        .await
        .unwrap();
    store
        .record_peer_cert_serial("codex", "bb22")
        .await
        .unwrap();
    assert_eq!(
        store.peer_cert_serial("codex").await.unwrap().as_deref(),
        Some("aa11")
    );
}
