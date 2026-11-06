//! The event feed cursor, the fence set/ack/clear lifecycle, and
//! the seeded pilot host row.

mod common;

use common::{key, pool_for, seeded, temp_db_path};
use sqlx::Row;
use wrkspc_core::{FenceKind, PILOT_HOST_ID, Store};

/// The event feed resumes from a cursor with monotonic sequence numbers.
#[tokio::test]
async fn events_feed_supports_resumable_cursor() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;

    store
        .accept_message(&conversation, &operator, &key(), "one", false, &[])
        .await
        .unwrap();
    store
        .accept_message(&conversation, &operator, &key(), "two", false, &[])
        .await
        .unwrap();

    let events = store.events_since(&conversation, 1, 100).await.unwrap();
    assert_eq!(
        events.len(),
        1,
        "only the second accept event after cursor 1"
    );
    assert_eq!(events[0].kind, "message.accepted");

    let all = store.events_since(&conversation, 0, 100).await.unwrap();
    assert_eq!(all.len(), 2);
    assert!(all[0].seq < all[1].seq, "seq must be monotonic");
    let _ = agent;
}

/// Fence set is idempotent; ack stamps the fence; clear removes it.
#[tokio::test]
async fn fence_set_ack_clear_lifecycle() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;

    let fence = store
        .set_fence(&conversation, &agent, FenceKind::Block, &operator)
        .await
        .unwrap();

    // Idempotent set returns the same fence.
    let again = store
        .set_fence(&conversation, &agent, FenceKind::Block, &operator)
        .await
        .unwrap();
    assert_eq!(fence, again);

    let active = store.active_fences(&conversation, &agent).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].acked_at, None);

    store.ack_fence(&fence).await.unwrap();
    let active = store.active_fences(&conversation, &agent).await.unwrap();
    assert!(active[0].acked_at.is_some(), "ack must be recorded");

    store.clear_fence(&fence, &operator).await.unwrap();
    assert!(
        store
            .active_fences(&conversation, &agent)
            .await
            .unwrap()
            .is_empty()
    );

    // The clear is committed into the feed: the fence no longer reads
    // as applied forever, and the release names who performed it.
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    let cleared: Vec<_> = events
        .iter()
        .filter(|event| event.kind == "fence.cleared")
        .collect();
    assert_eq!(cleared.len(), 1, "one fence.cleared event: {events:?}");
    assert_eq!(cleared[0].payload["fence_id"], fence);
    assert_eq!(cleared[0].payload["cleared_by"], operator);

    // Clearing again is idempotent and emits no second event.
    store.clear_fence(&fence, &operator).await.unwrap();
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "fence.cleared")
            .count(),
        1,
        "re-clearing must not duplicate the event"
    );
}

/// A fresh database seeds the pilot host row.
#[tokio::test]
async fn pilot_host_is_seeded() {
    let path = temp_db_path();
    let store = Store::open(&path, "proc-a").await.unwrap();
    let pool = pool_for(&path).await;
    let row = sqlx::query("SELECT host_id FROM hosts WHERE host_id = ?")
        .bind(PILOT_HOST_ID)
        .fetch_one(&pool)
        .await
        .unwrap();
    let host_id: String = row.get("host_id");
    assert_eq!(host_id, PILOT_HOST_ID);
    store.release_lock().await.unwrap();
    std::fs::remove_file(&path).ok();
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
