#![allow(dead_code)]
//! Shared harness for the operator-UI integration tests: an in-memory
//! store, a recording cancel requester, and the UI served on a real
//! loopback socket with a pinned bearer token.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use wrkspc_core::{AcceptOutcome, CancelOutcome, RecipientRequest, Store};
use wrkspc_ui::{CancelRequester, UiConfig, UiServer};

pub const TOKEN: &str = "test-token-1234";

/// A fake cancel requester recording every call.
pub struct FakeCancel {
    pub calls: Mutex<Vec<(String, String)>>,
}

impl CancelRequester for FakeCancel {
    fn request_cancel(
        &self,
        message_id: &str,
        recipient_id: &str,
        _set_by: &str,
    ) -> Pin<Box<dyn Future<Output = Result<CancelOutcome, String>> + Send + '_>> {
        self.calls
            .lock()
            .unwrap()
            .push((message_id.to_string(), recipient_id.to_string()));
        Box::pin(async move {
            Ok(CancelOutcome::Attempt {
                attempt_id: "fake-attempt".to_string(),
            })
        })
    }
}

pub struct TestUi {
    pub store: Store,
    pub base: String,
    pub cancel: Arc<FakeCancel>,
    pub shutdown: oneshot::Sender<()>,
}

pub async fn spawn_ui() -> TestUi {
    spawn_ui_with(false, false).await
}

/// Spawns the UI with hub-mode delivery queueing and the hub-role
/// operator surface on or off.
pub async fn spawn_ui_with(queue_remote_deliveries: bool, hub_role: bool) -> TestUi {
    let store = Store::open_in_memory("ui-test").await.unwrap();
    store
        .ensure_peer("operator", "operator", "human")
        .await
        .unwrap();
    store
        .ensure_peer("agent", "agent", "harness")
        .await
        .unwrap();
    store
        .create_conversation(&["operator", "agent"])
        .await
        .unwrap();

    let cancel = Arc::new(FakeCancel {
        calls: Mutex::new(Vec::new()),
    });
    let ui = UiServer::new(
        store.clone(),
        cancel.clone(),
        UiConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token: Some(TOKEN.to_string()),
            queue_remote_deliveries,
            hub_role,
        },
        None,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        ui.serve_on(listener, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("ui serves");
    });
    TestUi {
        store,
        base: format!("http://{addr}"),
        cancel,
        shutdown: shutdown_tx,
    }
}

impl TestUi {
    pub fn client(&self) -> reqwest::Client {
        reqwest::Client::new()
    }

    pub fn authed(&self) -> reqwest::RequestBuilder {
        self.client()
            .get(format!("{}/api/health", self.base))
            .bearer_auth(TOKEN)
    }

    pub async fn conversation_id(&self) -> String {
        let res = self
            .client()
            .get(format!("{}/api/conversations", self.base))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        let conversations: Vec<serde_json::Value> = res.json().await.unwrap();
        conversations[0]["id"].as_str().unwrap().to_string()
    }

    pub async fn post_message(&self, content: &str) -> String {
        let conversation_id = self.conversation_id().await;
        let AcceptOutcome::Accepted { message_id } = self
            .store
            .accept_message(
                &conversation_id,
                "operator",
                &format!("key-{content}"),
                content,
                false,
                &[RecipientRequest {
                    peer_id: "agent".to_string(),
                    execution_requested: true,
                }],
            )
            .await
            .unwrap()
        else {
            panic!("fresh accept");
        };
        message_id
    }
}
