#![allow(dead_code)]
//! Shared helpers for the wrkspc-core store integration tests: a
//! seeded two-peer store, unique keys, temp db paths, and a read-only
//! side-channel pool.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use wrkspc_core::Store;

/// Builds a store with two peers enrolled in one conversation. Returns the
/// store plus (operator_id, agent_id, conversation_id).
pub async fn seeded(store: &Store) -> (String, String, String) {
    let operator = format!("human-{}", Uuid::new_v4());
    let agent = format!("agent-{}", Uuid::new_v4());
    store
        .ensure_peer(&operator, "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer(&agent, "test agent", "harness")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&[&operator, &agent])
        .await
        .unwrap();
    (operator, agent, conversation)
}

pub fn key() -> String {
    format!("key-{}", Uuid::new_v4())
}

/// A unique temp-file path for file-backed tests.
pub fn temp_db_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("wrkspc-test-{}-{nanos}.db", Uuid::new_v4()))
}

/// Opens a read-only connection to a file-backed database (WAL allows
/// concurrent readers while the store holds the dispatcher lock).
pub async fn pool_for(path: &std::path::Path) -> sqlx::SqlitePool {
    let opts =
        sqlx::sqlite::SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
            .unwrap()
            .read_only(true);
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .unwrap()
}
