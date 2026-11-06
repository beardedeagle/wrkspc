//! Restart reconciliation: open attempts are marked unknown, never
//! re-dispatched.

mod common;

use agent_client_protocol::schema::v1::StopReason;
use common::{FakeDriver, FakeProvider, Mode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use wrkspc_core::{AcceptOutcome, RecipientRequest, Store};
use wrkspc_dispatcher::{Dispatcher, DispatcherConfig, SessionBinding};

/// Restart reconciliation marks open attempts unknown; unknown-blocked intents never re-dispatch.
#[tokio::test]
async fn restart_reconciliation_marks_open_attempts_unknown() {
    let store = Store::open_in_memory("dispatcher-restart").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    let conversation_id = store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();
    let AcceptOutcome::Accepted { message_id } = store
        .accept_message(
            &conversation_id,
            "operator",
            "k1",
            "do the thing",
            false,
            &[RecipientRequest {
                peer_id: "agent".to_string(),
                execution_requested: true,
            }],
        )
        .await
        .unwrap()
    else {
        panic!("fresh accept expected");
    };
    // Simulate a crash between claim and dispatch: an open attempt exists
    // but no dispatcher drove it.
    let _attempt_id = store.claim_intent(&message_id, "agent").await.unwrap();

    let driver = FakeDriver::new(Mode::Respond(StopReason::EndTurn));
    let mut bindings = HashMap::new();
    bindings.insert(
        "agent".to_string(),
        SessionBinding {
            peer_id: "agent".to_string(),
            session_id: "sess-agent".to_string(),
            driver: Arc::new(driver.clone()),
        },
    );
    let provider = Arc::new(FakeProvider { bindings });
    let (dispatcher, _handle) = Dispatcher::new(
        store.clone(),
        provider,
        DispatcherConfig {
            tick: Duration::from_millis(10),
        },
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .expect("run");
    });

    // The open attempt is marked unknown; the intent is never re-dispatched.
    let mut saw_unknown = false;
    for _ in 0..100 {
        let events = store.events_since(&conversation_id, 0, 100).await.unwrap();
        if events.iter().any(|e| {
            e.kind == "dispatch.unknown" && e.payload.to_string().contains("dispatcher restart")
        }) {
            saw_unknown = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = shutdown_tx.send(()).await;
    assert!(
        saw_unknown,
        "restart reconciliation marks the attempt unknown"
    );
    assert!(
        driver.prompt_texts().is_empty(),
        "unknown-blocked intents never re-dispatch"
    );
}
