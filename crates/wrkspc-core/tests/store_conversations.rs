//! Room naming and the node-side conversation mirror.
//!
//! A name is display metadata: nullable, non-unique, and never a key. Nodes
//! learn rooms from deliveries, so the mirror is what makes a room set and its
//! names agree across machines.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use wrkspc_core::Store;
use wrkspc_core::views::ConversationView;

/// Enrols a peer, then opens a room with those members.
///
/// Members are foreign keys onto `peers`, so a room can only hold peers that
/// exist.
///
/// # Arguments
///
/// * `name` — the store's file name; in-memory when it does not end in `.db`.
/// * `members` — peer ids to enrol and place in the new room.
///
/// # Returns
///
/// The store and the new room's id.
async fn room(name: &str, members: &[&str]) -> (Store, String) {
    let store = Store::open_in_memory(name).await.unwrap();
    for member in members {
        let class = if *member == "operator" {
            "human"
        } else {
            "harness"
        };
        store.ensure_peer(member, member, class).await.unwrap();
    }
    let id = store.create_conversation(members).await.unwrap();
    (store, id)
}

/// A name is optional, trimmed, clearable, and absent on a fresh room.
#[tokio::test]
async fn conversation_names_are_optional_and_editable() {
    let (store, id) = room("conversation-names", &["codex", "operator"]).await;

    let row = store
        .list_conversations()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == id)
        .expect("the room is listed");
    assert_eq!(row.name, None, "a fresh room is unnamed");

    assert!(
        store
            .set_conversation_name(&id, Some("  Veritas design  "))
            .await
            .unwrap()
    );
    let row = store
        .list_conversations()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == id)
        .unwrap();
    assert_eq!(
        row.name.as_deref(),
        Some("Veritas design"),
        "the name is stored trimmed"
    );

    // A blank name clears it: names are vanity, so an empty field means unnamed.
    assert!(store.set_conversation_name(&id, Some("   ")).await.unwrap());
    let row = store
        .list_conversations()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == id)
        .unwrap();
    assert_eq!(row.name, None);

    assert!(
        store
            .set_conversation_name(&id, Some("again"))
            .await
            .unwrap()
    );
    assert!(store.set_conversation_name(&id, None).await.unwrap());
    let row = store
        .list_conversations()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == id)
        .unwrap();
    assert_eq!(row.name, None, "an explicit null clears the name");

    assert!(
        !store
            .set_conversation_name("no-such-room", Some("x"))
            .await
            .unwrap(),
        "an unknown room reports that nothing matched"
    );
}

/// Two rooms may hold the same name; the id is what identifies them.
#[tokio::test]
async fn conversation_names_are_not_unique() {
    let (store, first) = room("conversation-name-dup", &["codex", "claude-1"]).await;
    let second = store.create_conversation(&["claude-1"]).await.unwrap();

    assert!(
        store
            .set_conversation_name(&first, Some("veritas"))
            .await
            .unwrap()
    );
    assert!(
        store
            .set_conversation_name(&second, Some("veritas"))
            .await
            .unwrap()
    );

    let rows = store.list_conversations().await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.name.as_deref() == Some("veritas"))
            .count(),
        2
    );
}

/// The mirror adds the hub's rooms, updates names, and never deletes a local one.
#[tokio::test]
async fn mirror_conversations_fills_names_and_members_without_deleting() {
    let (store, local) = room("conversation-mirror", &["kestrel", "codex", "operator"]).await;

    store
        .mirror_conversations(&[ConversationView {
            id: "room-hub".to_string(),
            name: Some("Veritas design".to_string()),
            created_at: "2026-11-06T00:00:00Z".to_string(),
            members: vec!["codex".to_string(), "operator".to_string()],
        }])
        .await
        .unwrap();

    let rows = store.list_conversations().await.unwrap();
    let mirrored = rows
        .iter()
        .find(|row| row.id == "room-hub")
        .expect("mirrored");
    assert_eq!(mirrored.name.as_deref(), Some("Veritas design"));
    let mut members = mirrored.members.clone();
    members.sort();
    assert_eq!(members, vec!["codex".to_string(), "operator".to_string()]);
    assert!(
        rows.iter().any(|row| row.id == local),
        "a room the hub omits survives: a node that missed a poll must not lose a transcript"
    );

    // A later mirror carries the operator's rename through and keeps the room.
    store
        .mirror_conversations(&[ConversationView {
            id: "room-hub".to_string(),
            name: None,
            created_at: "2026-11-06T00:00:00Z".to_string(),
            members: vec!["codex".to_string()],
        }])
        .await
        .unwrap();

    let rows = store.list_conversations().await.unwrap();
    let mirrored = rows.iter().find(|row| row.id == "room-hub").expect("kept");
    assert_eq!(
        mirrored.name, None,
        "clearing the hub name clears the mirror"
    );
}

/// UI-19: the transcript's bounded window returns the LATEST messages
/// oldest-first and never more than asked, while the full list is
/// unchanged.
#[tokio::test]
async fn list_messages_window_returns_the_latest_bounded_oldest_first() {
    let store = Store::open_in_memory("window-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let room = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();

    for n in 0..5 {
        store
            .accept_message(
                &room,
                "operator",
                &format!("key-{n}"),
                &format!("m{n}"),
                false,
                &[wrkspc_core::RecipientRequest {
                    peer_id: "codex".to_string(),
                    execution_requested: false,
                }],
            )
            .await
            .unwrap();
    }

    let window = store.list_messages_window(&room, 2).await.unwrap();
    assert_eq!(window.len(), 2, "the window is bounded: {window:#?}");
    assert_eq!(window[0].content, "m3", "oldest first within the window");
    assert_eq!(window[1].content, "m4");
    // Each windowed message still carries its delivery rows.
    assert!(window.iter().all(|m| !m.deliveries.is_empty()));

    let full = store.list_messages(&room).await.unwrap();
    assert_eq!(full.len(), 5, "the unbounded list is untouched");
    assert_eq!(full[0].content, "m0");

    // A window larger than the room degrades to the whole room.
    let all = store.list_messages_window(&room, 50).await.unwrap();
    assert_eq!(all.len(), 5);
}

/// The bounded window bounds the DELIVERY fetch too. A room
/// with a large old delivery history still costs the window, not the
/// whole conversation, while the explicitly requested full-history list
/// stays complete.
#[tokio::test]
async fn bounded_window_scopes_deliveries_to_the_window() {
    let store = Store::open_in_memory("window-deliveries").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let room = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();

    // A large old history: every message has its own delivery row.
    for n in 0..120 {
        store
            .accept_message(
                &room,
                "operator",
                &format!("key-{n}"),
                &format!("m{n}"),
                false,
                &[wrkspc_core::RecipientRequest {
                    peer_id: "codex".to_string(),
                    execution_requested: false,
                }],
            )
            .await
            .unwrap();
    }

    let window = store.list_messages_window(&room, 5).await.unwrap();
    assert_eq!(window.len(), 5, "the window is bounded: {window:#?}");
    assert_eq!(window[0].content, "m115", "oldest first within the window");
    assert_eq!(window[4].content, "m119");
    // Every windowed message carries exactly its own delivery row — no
    // more, no less.
    assert!(
        window.iter().all(|message| message.deliveries.len() == 1),
        "in-window deliveries are joined"
    );

    // The full-history endpoint is unchanged: every historical delivery
    // is still served when explicitly requested.
    let full = store.list_messages(&room).await.unwrap();
    assert_eq!(full.len(), 120, "the unbounded list stays complete");
    assert!(
        full.iter().all(|message| message.deliveries.len() == 1),
        "the full list keeps every delivery row"
    );
}

///  (deletion side): a room's delivery tasks — claim rows and
/// task rows — are cleaned for ALL current messages, so a deleted room
/// can never keep pollable work alive.
#[tokio::test]
async fn delete_conversation_cleans_delivery_tasks_and_claims() {
    let store = Store::open_in_memory("delete-tasks").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let room = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();

    let mut task_ids = Vec::new();
    for n in 0..2 {
        let outcome = store
            .accept_message(
                &room,
                "operator",
                &format!("key-{n}"),
                &format!("work {n}"),
                true,
                &[wrkspc_core::RecipientRequest {
                    peer_id: "codex".to_string(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap();
        let message_id = match outcome {
            wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
            wrkspc_core::AcceptOutcome::Replay { .. } => panic!("expected Accepted"),
        };
        let task_id = store
            .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
                recipient_host: "node-host",
                recipient_peer: "codex",
                recipient_name: "codex",
                recipient_class: "harness",
                message_id: &message_id,
                origin_message_id: None,
                conversation_id: &room,
                sender_peer: "operator",
                sender_name: "operator",
                sender_class: "human",
                sender_host: wrkspc_core::PILOT_HOST_ID,
                content: &format!("work {n}"),
                execution_requested: true,
            })
            .await
            .unwrap();
        // The node claims the delivery before the room is deleted.
        assert!(matches!(
            store
                .claim_delivery(&task_id, "node-host", "instance-1")
                .await
                .unwrap(),
            wrkspc_core::DeliveryClaimOutcome::Claimed
        ));
        task_ids.push(task_id);
    }

    assert!(
        store.delete_conversation(&room).await.unwrap(),
        "the room deletes"
    );

    // Every task for the deleted room is gone: the claim rows cannot
    // survive without their task rows, and the task rows cannot
    // survive the room.
    for task_id in &task_ids {
        let err = store
            .claim_delivery(task_id, "node-host", "instance-1")
            .await
            .unwrap_err();
        assert!(
            matches!(err, wrkspc_core::StoreError::NotFound { .. }),
            "task {task_id} must not outlive its room: {err:?}"
        );
    }
}

///  (deletion side): the deletion's dependency snapshot is
/// gathered INSIDE the write-reserving transaction. A message committed
/// while a deletion is in flight — its writer blocked on the database
/// write lock the deletion now takes up front — is still cleaned: its
/// delivery task cannot outlive the room as executable work.
#[tokio::test]
async fn delete_conversation_cleans_tasks_committed_while_it_holds_the_write_lock() {
    use std::str::FromStr;

    let path = common::temp_db_path();
    let store = Store::open(&path, "delete-interleave").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    let room = store
        .create_conversation(&["operator", "codex"])
        .await
        .unwrap();

    // One message and task committed before the deletion starts.
    let outcome = store
        .accept_message(
            &room,
            "operator",
            "key-before",
            "before",
            false,
            &[wrkspc_core::RecipientRequest {
                peer_id: "codex".to_string(),
                execution_requested: false,
            }],
        )
        .await
        .unwrap();
    let message_before = match outcome {
        wrkspc_core::AcceptOutcome::Accepted { message_id } => message_id,
        wrkspc_core::AcceptOutcome::Replay { .. } => panic!("expected Accepted"),
    };
    store
        .task_create_delivery(wrkspc_core::DeliveryTaskRequest {
            recipient_host: "node-host",
            recipient_peer: "codex",
            recipient_name: "codex",
            recipient_class: "harness",
            message_id: &message_before,
            origin_message_id: None,
            conversation_id: &room,
            sender_peer: "operator",
            sender_name: "operator",
            sender_class: "human",
            sender_host: wrkspc_core::PILOT_HOST_ID,
            content: "before",
            execution_requested: false,
        })
        .await
        .unwrap();
    let task_before = wrkspc_core::tasks::delivery_task_id(&message_before, "codex");

    // A second writer takes the database write lock and holds it while
    // the deletion runs: the deletion blocks (at its first write — or,
    // with the fix, at the write-reserving begin) until the holder
    // commits.
    let side = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
                .unwrap()
                .foreign_keys(true)
                .busy_timeout(std::time::Duration::from_secs(5)),
        )
        .await
        .unwrap();
    let mut hold = side.begin_with("BEGIN IMMEDIATE").await.unwrap();

    let deleting = {
        let store = store.clone();
        let room = room.clone();
        tokio::spawn(async move { store.delete_conversation(&room).await })
    };

    // The deletion is now blocked on the held write lock. Commit a NEW
    // message and its delivery task from the lock holder — the fan-out
    // racing the deletion.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let message_late = uuid::Uuid::new_v4().to_string();
    let task_late = wrkspc_core::tasks::delivery_task_id(&message_late, "codex");
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO messages
           (id, conversation_id, sender_id, idempotency_key, content,
            execution_requested, created_at)
         VALUES (?, ?, ?, 'late-key', 'late', 0, ?)",
    )
    .bind(&message_late)
    .bind(&room)
    .bind("operator")
    .bind(&now)
    .execute(&mut *hold)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO message_recipients (message_id, peer_id, delivery_state)
         VALUES (?, 'codex', 'stored')",
    )
    .bind(&message_late)
    .execute(&mut *hold)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO a2a_tasks
           (task_id, context_id, state, terminal, task_json, version, created_at, updated_at)
         VALUES (?, 'node-host', 'submitted', 0, ?, 1, ?, ?)",
    )
    .bind(&task_late)
    .bind(format!(
        "{{\"metadata\":{{\"delivery\":{{\"kind\":\"delivery\",\"message_id\":\"{message_late}\",\"recipient_peer\":\"codex\"}}}}}}"
    ))
    .bind(&now)
    .bind(&now)
    .execute(&mut *hold)
    .await
    .unwrap();
    hold.commit().await.unwrap();

    let deleted = deleting.await.unwrap().unwrap();
    assert!(deleted, "the room deletes");

    // Both tasks — the one committed before AND the one committed while
    // the deletion was in flight — are gone, and so are the room's
    // messages.
    for task_id in [&task_before, &task_late] {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM a2a_tasks WHERE task_id = ?")
            .bind(task_id)
            .fetch_one(&side)
            .await
            .unwrap();
        assert_eq!(count, 0, "task {task_id} must not outlive its room");
    }
    let messages: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE conversation_id = ?")
            .bind(&room)
            .fetch_one(&side)
            .await
            .unwrap();
    assert_eq!(messages, 0, "the room's messages are gone");
}
