//! Restart durability: crash-before-dispatch, unknown persistence,
//! and the exclusive dispatcher lock across reopens.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::{key, pool_for, seeded, temp_db_path};
use sqlx::Row;
use wrkspc_core::{AcceptOutcome, RecipientRequest, Reconciliation, Store, StoreError};

/// A claimed-but-undispatched attempt survives a crash/reopen and reclaims the same attempt.
#[tokio::test]
async fn crash_before_dispatch_survives_reopen() {
    let path = temp_db_path();
    let owner = "proc-a";

    {
        let store = Store::open(&path, owner).await.unwrap();
        let (operator, agent, conversation) = seeded(&store).await;
        let AcceptOutcome::Accepted { message_id } = store
            .accept_message(
                &conversation,
                &operator,
                &key(),
                "survives restart",
                true,
                &[RecipientRequest {
                    peer_id: agent.clone(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap()
        else {
            panic!("expected Accepted");
        };
        // Claim but never dispatch — the crash point.
        let attempt = store.claim_intent(&message_id, &agent).await.unwrap();
        store.release_lock().await.unwrap();
        // Store A drops here, simulating process death before dispatch.
        let _ = (attempt, message_id, agent);
        // Persist ids across the reopen boundary via the DB itself.
        let _ = store; // dropped
    }

    {
        // A new process opens the same database. The claimed attempt and
        // queued state must be exactly as left.
        let store = Store::open(&path, "proc-b").await.unwrap();
        // The intent is 'claimed' with an open attempt; a new claim returns
        // the SAME attempt (no fresh work invented on restart).
        let pool = pool_for(&path).await;
        let row = sqlx::query(
            "SELECT i.message_id, i.recipient_id, a.id FROM execution_intents i
             JOIN attempts a ON a.intent_id = i.id
             WHERE i.state = 'claimed'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let message_id: String = row.get("message_id");
        let recipient_id: String = row.get("recipient_id");
        let original_attempt: String = row.get("id");
        let reclaimed = store
            .claim_intent(&message_id, &recipient_id)
            .await
            .unwrap();
        assert_eq!(
            reclaimed, original_attempt,
            "restart must not recreate dispatched work"
        );
        let _ = owner;
    }

    std::fs::remove_file(&path).ok();
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

/// Unknown state survives a reopen and keeps blocking; reconciliation unblocks.
#[tokio::test]
async fn unknown_state_persists_across_reopen() {
    let path = temp_db_path();
    let (message_id, agent_id, attempt_id);
    {
        let store = Store::open(&path, "proc-a").await.unwrap();
        let (operator, agent, conversation) = seeded(&store).await;
        let AcceptOutcome::Accepted { message_id: mid } = store
            .accept_message(
                &conversation,
                &operator,
                &key(),
                "unknown test",
                true,
                &[RecipientRequest {
                    peer_id: agent.clone(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap()
        else {
            panic!("expected Accepted");
        };
        let attempt = store.claim_intent(&mid, &agent).await.unwrap();
        store
            .mark_unknown(&attempt, "crash after dispatch")
            .await
            .unwrap();
        store.release_lock().await.unwrap();
        message_id = mid;
        agent_id = agent;
        attempt_id = attempt;
    }

    let store = Store::open(&path, "proc-b").await.unwrap();
    let err = store
        .claim_intent(&message_id, &agent_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::BlockedByUnknown { .. }),
        "unknown must survive restart and still block new work"
    );
    store
        .reconcile_unknown(&attempt_id, Reconciliation::Retry)
        .await
        .unwrap();
    let fresh = store.claim_intent(&message_id, &agent_id).await.unwrap();
    assert_ne!(fresh, attempt_id);

    std::fs::remove_file(&path).ok();
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

/// A second process cannot open the database while the dispatcher lock is held; release hands it over.
#[tokio::test]
async fn second_process_cannot_acquire_lock() {
    let path = temp_db_path();
    let store_a = Store::open(&path, "proc-a").await.unwrap();
    let err = Store::open(&path, "proc-b").await.unwrap_err();
    assert!(matches!(err, StoreError::AlreadyLocked { owner, .. } if owner == "proc-a"));

    // Releasing allows a successor.
    store_a.release_lock().await.unwrap();
    let store_b = Store::open(&path, "proc-b").await.unwrap();
    store_b.release_lock().await.unwrap();

    std::fs::remove_file(&path).ok();
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
