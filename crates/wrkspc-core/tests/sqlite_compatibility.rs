//! Runtime compatibility guards for the bundled SQLite engine and transactions.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::{seeded, temp_db_path};
use sqlx::SqlitePool;
use std::future::Future;
use std::time::Duration;
use wrkspc_core::{CancelOutcome, FenceKind, RecipientRequest, Store, StoreError};

/// The linked engine, not only the lockfile, must contain the WAL page-size fix.
#[tokio::test]
async fn sqlite_engine_includes_wal_page_size_fix() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let version: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let parts: Vec<u32> = version
        .split('.')
        .map(|part| part.parse().expect("numeric SQLite version"))
        .collect();
    let version_number: [u32; 3] = parts.try_into().expect("three-part SQLite version");
    assert!(
        version_number >= [3, 54, 0],
        "SQLite {version} predates the WAL page-size fix (RUSTSEC-2026-0459)"
    );
}

/// Holds a competing WAL writer until the operator transaction is waiting.
async fn after_competing_writer<T>(
    pool: &SqlitePool,
    operation: impl Future<Output = Result<T, StoreError>>,
) -> T {
    let mut writer = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE peers SET name = name || '.'")
        .execute(&mut *writer)
        .await
        .unwrap();
    tokio::pin!(operation);
    let early = tokio::time::timeout(Duration::from_millis(500), &mut operation).await;
    writer.commit().await.unwrap();
    assert!(
        early.is_err(),
        "operator writes must wait for the existing writer"
    );
    operation
        .await
        .expect("operator write succeeds after the competing writer commits")
}

/// Read-before-write moderation must not promote a stale WAL read snapshot.
#[tokio::test]
async fn operator_writes_wait_for_competing_sqlite_writer() {
    let path = temp_db_path();
    let store = Store::open(&path, "sqlite-writer-compatibility")
        .await
        .unwrap();
    let (operator, agent, conversation) = seeded(&store).await;
    let writer = SqlitePool::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();

    let fence = after_competing_writer(
        &writer,
        store.set_fence(&conversation, &agent, FenceKind::Pause, &operator),
    )
    .await;
    after_competing_writer(&writer, store.clear_fence(&fence, &operator)).await;
    assert!(
        store
            .active_fences(&conversation, &agent)
            .await
            .unwrap()
            .is_empty()
    );

    let accepted = store
        .accept_message(
            &conversation,
            &operator,
            "cancel-under-contention",
            "queued work",
            true,
            &[RecipientRequest {
                peer_id: agent.clone(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();
    let wrkspc_core::AcceptOutcome::Accepted { message_id } = accepted else {
        panic!("fresh message must be accepted");
    };
    let outcome = after_competing_writer(
        &writer,
        store.request_cancel(&message_id, &agent, &operator),
    )
    .await;
    assert!(matches!(outcome, CancelOutcome::IntentCanceled));

    writer.close().await;
    store.release_lock().await.unwrap();
    drop(store);
    let _ = std::fs::remove_file(path);
}
