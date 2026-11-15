//! Restart reconciliation: open attempts are marked unknown, never
//! re-dispatched.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use agent_client_protocol::v1::schema::StopReason;
use common::Mode;
use std::time::Duration;
use wrkspc_core::Store;

/// Restart reconciliation marks open attempts unknown; unknown-blocked intents never re-dispatch.
#[tokio::test]
async fn restart_reconciliation_marks_open_attempts_unknown() {
    // The crash is simulated BETWEEN seeding and spawning: the claim
    // must exist before the dispatcher starts, which is exactly why the
    // harness helpers separate seeding from spawning.
    let store = Store::open_in_memory("dispatcher-restart").await.unwrap();
    let (conversation_id, message_id) = common::seed_fixture(&store).await;
    let _attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();

    let harness = common::spawn_dispatcher_over(
        store.clone(),
        conversation_id,
        Mode::Respond(StopReason::EndTurn),
        true,
    );

    // The open attempt is marked unknown; the intent is never re-dispatched.
    common::wait_until(
        "restart reconciliation marks the attempt unknown",
        Duration::from_secs(10),
        || {
            let store = harness.store.clone();
            let conversation_id = harness.conversation_id.clone();
            Box::pin(async move {
                store
                    .events_since(&conversation_id, 0, 100)
                    .await
                    .unwrap()
                    .iter()
                    .any(|e| {
                        e.kind == "dispatch.unknown"
                            && e.payload.to_string().contains("dispatcher restart")
                    })
            })
        },
    )
    .await;
    assert!(
        harness.driver.prompt_texts().is_empty(),
        "unknown-blocked intents never re-dispatch"
    );
    let _ = harness.shutdown.clone().send(()).await;
}
