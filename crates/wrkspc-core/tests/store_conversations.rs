//! Room naming and the node-side conversation mirror.
//!
//! A name is display metadata: nullable, non-unique, and never a key. Nodes
//! learn rooms from deliveries, so the mirror is what makes a room set and its
//! names agree across machines.

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
