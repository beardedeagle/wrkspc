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
