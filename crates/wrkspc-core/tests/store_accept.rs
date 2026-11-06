//! Acceptance, claim, and terminal-state semantics: membership
//! enforcement, idempotency, unknown-blocking, and immutability.

mod common;

use common::{key, seeded};
use uuid::Uuid;
use wrkspc_core::{
    AcceptOutcome, AttemptState, RecipientRequest, Reconciliation, Store, StoreError,
};

/// Accepting a message commits the message and its recipient intents; the intent is claimable.
#[tokio::test]
async fn accept_commits_message_and_intents() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;

    let outcome = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "hello agent",
            true,
            &[RecipientRequest {
                peer_id: agent.clone(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();
    let message_id = match outcome {
        AcceptOutcome::Accepted { message_id } => message_id,
        AcceptOutcome::Replay { .. } => panic!("expected Accepted"),
    };

    // An execution intent exists and is claimable.
    let attempt = store
        .claim_intent(&message_id, &agent)
        .await
        .expect("intent must be claimable after accept");
    assert!(!attempt.is_empty());
}

/// Re-accepting the same key returns Replay with the original message id (no duplicate commit).
#[tokio::test]
async fn idempotent_replay_returns_same_message() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;
    let idem = key();

    let first = store
        .accept_message(
            &conversation,
            &operator,
            &idem,
            "same content",
            false,
            &[RecipientRequest {
                peer_id: agent.clone(),
                execution_requested: false,
            }],
        )
        .await
        .unwrap();
    let AcceptOutcome::Accepted { message_id } = first else {
        panic!("expected Accepted");
    };

    let second = store
        .accept_message(
            &conversation,
            &operator,
            &idem,
            "same content",
            false,
            &[RecipientRequest {
                peer_id: agent.clone(),
                execution_requested: false,
            }],
        )
        .await
        .unwrap();
    assert_eq!(second, AcceptOutcome::Replay { message_id });
}

/// The same key with different content is refused as an idempotency conflict.
#[tokio::test]
async fn idempotency_conflict_on_different_content() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, _agent, conversation) = seeded(&store).await;
    let idem = key();

    store
        .accept_message(&conversation, &operator, &idem, "v1", false, &[])
        .await
        .unwrap();

    let err = store
        .accept_message(&conversation, &operator, &idem, "v2", false, &[])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::IdempotencyConflict { .. }));
}

/// A non-member cannot accept a message, and a member cannot target a non-member recipient.
#[tokio::test]
async fn non_member_cannot_accept() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;
    let outsider = format!("outsider-{}", Uuid::new_v4());
    store
        .ensure_peer(&outsider, "outsider", "harness")
        .await
        .unwrap();

    let err = store
        .accept_message(&conversation, &outsider, &key(), "sneaky", false, &[])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }));

    // Recipient membership is also enforced.
    let err = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "to outsider",
            false,
            &[RecipientRequest {
                peer_id: outsider,
                execution_requested: false,
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }));
    let _ = agent;
}

/// Claiming the same intent twice returns the same attempt.
#[tokio::test]
async fn claim_is_idempotent() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;

    let AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "run this",
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

    let first = store.claim_intent(&message_id, &agent).await.unwrap();
    let second = store.claim_intent(&message_id, &agent).await.unwrap();
    assert_eq!(first, second, "double claim must return the same attempt");
}

/// An unknown attempt blocks new claims until the operator reconciles (retry creates a new attempt).
#[tokio::test]
async fn unknown_blocks_new_work_until_reconciled() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;

    let AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "run this",
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

    let attempt = store.claim_intent(&message_id, &agent).await.unwrap();
    store.mark_unknown(&attempt, "lost response").await.unwrap();

    let err = store.claim_intent(&message_id, &agent).await.unwrap_err();
    assert!(matches!(err, StoreError::BlockedByUnknown { .. }));

    // Operator authorizes a retry: the attempt closes failed and the intent
    // returns to queued, so a fresh claim succeeds with a NEW attempt.
    store
        .reconcile_unknown(&attempt, Reconciliation::Retry)
        .await
        .unwrap();
    // The reconciliation itself is a committed feed event: closing a
    // visible unknown must not vanish without a record.
    let events = store.events_since(&conversation, 0, 100).await.unwrap();
    let reconciled: Vec<_> = events
        .iter()
        .filter(|event| event.kind == "attempt.reconciled")
        .collect();
    assert_eq!(
        reconciled.len(),
        1,
        "one attempt.reconciled event: {events:?}"
    );
    assert_eq!(reconciled[0].payload["attempt_id"], attempt);
    assert_eq!(reconciled[0].payload["decision"], "retry");
    assert_eq!(reconciled[0].payload["by"], "operator");
    let fresh = store.claim_intent(&message_id, &agent).await.unwrap();
    assert_ne!(fresh, attempt, "retry must produce a new attempt");
}

/// Terminal attempts reject different terminal states, re-emit the same one idempotently, and block new claims.
#[tokio::test]
async fn terminal_state_is_immutable_but_idempotent() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;

    let AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "run this",
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
    let attempt = store.claim_intent(&message_id, &agent).await.unwrap();

    store
        .mark_terminal(&attempt, AttemptState::Completed, Some("end_turn"), None)
        .await
        .unwrap();

    // Same terminal state re-emission is fine (idempotent re-emission).
    store
        .mark_terminal(&attempt, AttemptState::Completed, Some("end_turn"), None)
        .await
        .unwrap();

    // A different terminal state is rejected.
    let err = store
        .mark_terminal(&attempt, AttemptState::Failed, Some("nope"), None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::TerminalState { .. }));

    // The intent is terminal too: claiming fails.
    let err = store.claim_intent(&message_id, &agent).await.unwrap_err();
    assert!(matches!(err, StoreError::IntentTerminal { .. }));
}

/// A second recipient's delivery replays the message but must still raise that
/// recipient's intent.
///
/// The node's delivery loop feeds one delivery per recipient under the same
/// idempotency key, so everyone after the first arrives as a replay. A bare
/// replay dropped the recipient and its intent, leaving every peer but the
/// first silently un-dispatched — visible in the UI as a peer that never runs
/// and never goes green.
#[tokio::test]
async fn replay_raises_the_second_recipients_intent() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;
    store
        .ensure_peer("claude-2", "claude-2", "harness")
        .await
        .unwrap();
    store
        .ensure_conversation(&conversation, &[&operator, &agent, "claude-2"])
        .await
        .unwrap();
    let idem = key();

    let first = store
        .accept_message(
            &conversation,
            &operator,
            &idem,
            "roll call",
            true,
            &[RecipientRequest {
                peer_id: agent.clone(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();
    let message_id = match first {
        AcceptOutcome::Accepted { message_id } => message_id,
        AcceptOutcome::Replay { .. } => panic!("expected Accepted"),
    };

    let second = store
        .accept_message(
            &conversation,
            &operator,
            &idem,
            "roll call",
            true,
            &[RecipientRequest {
                peer_id: "claude-2".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();
    match second {
        AcceptOutcome::Replay {
            message_id: replayed,
        } => assert_eq!(replayed, message_id, "replay must reuse the stored message"),
        AcceptOutcome::Accepted { .. } => panic!("expected Replay"),
    }

    // The regression: without the fix this claim fails and the peer never runs.
    let attempt = store
        .claim_intent(&message_id, "claude-2")
        .await
        .expect("replay must raise the second recipient's intent");
    assert!(!attempt.is_empty(), "second recipient must be claimable");
}

/// Deleting a conversation removes it and everything scoped to it, so an
/// abandoned room cannot keep waking peers on every poll.
#[tokio::test]
async fn delete_conversation_removes_room_and_its_rows() {
    let store = Store::open_in_memory("test").await.unwrap();
    let (operator, agent, conversation) = seeded(&store).await;
    let outcome = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "into the doomed room",
            true,
            &[RecipientRequest {
                peer_id: agent.clone(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap();
    assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));

    assert!(
        store.delete_conversation(&conversation).await.unwrap(),
        "existing conversation reports deleted"
    );
    assert!(
        !store.delete_conversation(&conversation).await.unwrap(),
        "second delete reports nothing removed"
    );
    assert!(
        store
            .conversation_members(&conversation)
            .await
            .unwrap()
            .is_empty(),
        "membership is gone"
    );
}

/// Builds a room with a human operator, a harness peer, and a native peer,
/// each with a single-token name so prose mentions are unambiguous.
async fn room(store: &Store) -> (String, String, String, String) {
    let operator = format!("human-{}", Uuid::new_v4());
    store
        .ensure_peer(&operator, "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("codex", "codex", "harness")
        .await
        .unwrap();
    store
        .ensure_peer("kestrel", "kestrel", "a2a_native")
        .await
        .unwrap();
    let conversation = store
        .create_conversation(&[&operator, "codex", "kestrel"])
        .await
        .unwrap();
    (
        operator,
        "codex".to_string(),
        "kestrel".to_string(),
        conversation,
    )
}

/// A harness post that only names its recipient in prose still reaches it.
/// Deliveries are opt-in, so before this a ping by name woke nobody while the
/// sender was told the post was accepted.
#[tokio::test]
async fn mentions_in_prose_deliver_to_the_named_member() {
    let store = Store::open_in_memory("mention").await.unwrap();
    let (operator, codex, kestrel, conversation) = room(&store).await;

    let outcome = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "@kestrel please review, and @codex-2 is not in this room",
            false,
            &[],
        )
        .await
        .unwrap();
    let message_id = match outcome {
        AcceptOutcome::Accepted { message_id } | AcceptOutcome::Replay { message_id } => message_id,
    };

    let delivered = store.message_recipients(&message_id).await.unwrap();
    assert_eq!(
        delivered,
        vec![kestrel],
        "only the member named in the text is woken"
    );
    assert!(!delivered.contains(&codex));
}

/// Mention resolution is not fan-out: a post that names nobody still wakes
/// nobody, which is what keeps a busy room from turning into a broadcast.
#[tokio::test]
async fn a_post_that_names_nobody_wakes_nobody() {
    let store = Store::open_in_memory("nobody").await.unwrap();
    let (operator, _, _, conversation) = room(&store).await;

    let outcome = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "status update with no addressee",
            true,
            &[],
        )
        .await
        .unwrap();
    let message_id = match outcome {
        AcceptOutcome::Accepted { message_id } | AcceptOutcome::Replay { message_id } => message_id,
    };

    assert!(
        store
            .message_recipients(&message_id)
            .await
            .unwrap()
            .is_empty(),
        "no recipients, no deliveries"
    );
}

/// A peer renamed with an alias stays addressable under the name the room
/// already knows, so an identity change does not orphan the thread.
#[tokio::test]
async fn an_alias_keeps_a_renamed_peer_addressable() {
    let store = Store::open_in_memory("alias").await.unwrap();
    let (operator, _, _, conversation) = room(&store).await;
    store
        .set_peer_alias("codex", Some("codex-veritas-plugins"), "operator")
        .await
        .unwrap();

    let outcome = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "@codex-veritas-plugins status?",
            false,
            &[],
        )
        .await
        .unwrap();
    let message_id = match outcome {
        AcceptOutcome::Accepted { message_id } | AcceptOutcome::Replay { message_id } => message_id,
    };

    assert_eq!(
        store.message_recipients(&message_id).await.unwrap(),
        vec!["codex".to_string()],
        "the alias resolves to the live peer"
    );
}

/// The operator reads the room in the UI and has no claim loop, so addressing
/// it must not create a delivery that can never be acked.
#[tokio::test]
async fn the_operator_takes_no_delivery() {
    let store = Store::open_in_memory("human").await.unwrap();
    let (operator, codex, _, conversation) = room(&store).await;

    let outcome = store
        .accept_message(
            &conversation,
            &operator,
            &key(),
            "@operator and @codex, looks good",
            false,
            &[],
        )
        .await
        .unwrap();
    let message_id = match outcome {
        AcceptOutcome::Accepted { message_id } | AcceptOutcome::Replay { message_id } => message_id,
    };

    let delivered = store.message_recipients(&message_id).await.unwrap();
    assert_eq!(delivered, vec![codex]);
    assert!(!delivered.contains(&operator));
}
