//! The MCP server implementation: scoped tools over the transactional
//! store, stdio serving, and an authenticated loopback HTTP listener.
//!
//! Tools map directly onto store primitives — no tool invents state the
//! store does not have. Errors from the store surface as MCP error results
//! with the store's message intact, never as silent success.
//!
//! # rmcp adapter contract
//!
//! The pinned SDK is `rmcp` 3.9. `tool_router(server_handler)` generates the
//! tools-only handler. Tool methods return `Result<T, String>`: `Err(String)`
//! becomes a successful JSON-RPC response with `is_error: true`, preserving
//! operational failures as failed tool calls. `ErrorData` instead produces a
//! JSON-RPC protocol error and is reserved for malformed requests.
//!
//! MCP uses stdio or Streamable HTTP at `/mcp`. The HTTP transport's default
//! host allowlist accepts loopback only, in addition to this listener's
//! bearer-token gate.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use wrkspc_core::{AcceptOutcome, Store, StoreError};

/// Parameters for the `post-message` tool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PostMessageParams {
    /// Target conversation id.
    pub conversation_id: String,
    /// Client-generated idempotency key (stable across retries).
    pub idempotency_key: String,
    /// Message content (plain text).
    pub content: String,
    /// Whether execution is explicitly requested. Defaults to false.
    #[serde(default)]
    pub execution_requested: bool,
    /// Recipient peer ids — deliveries are opt-in: name recipients
    /// explicitly here or write `@peer` mentions in the text. A post
    /// that names nobody wakes nobody (never a broadcast to the other
    /// members).
    #[serde(default)]
    pub recipients: Option<Vec<String>>,
}

/// Parameters for the `read-conversation` tool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadConversationParams {
    /// Conversation to read.
    pub conversation_id: String,
    /// Cursor: return events after this sequence number (exclusive).
    #[serde(default)]
    pub after_seq: i64,
    /// Maximum events to return. Defaults to 100.
    #[serde(default = "default_limit")]
    pub limit: i64,
}

/// Default event-read page size.
const fn default_limit() -> i64 {
    100
}

/// Parameters for the `report-work` tool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReportWorkParams {
    /// Conversation the report belongs to.
    pub conversation_id: String,
    /// Free-form work status summary (committed to the event feed).
    pub summary: String,
}

/// The harness-facing MCP server for one enrolled peer.
///
/// All tool calls run under the peer identity baked in at construction:
/// stdio connections are bound by process ownership, HTTP connections by
/// the listener's token allowlist. The store's own membership checks still
/// enforce conversation boundaries.
#[derive(Debug, Clone)]
pub struct McpServer {
    store: Store,
    peer_id: String,
    /// A node running with `--hub` must not execute local work before
    /// hub acceptance: posts mirror locally (no execution intents) and
    /// execution happens only when the hub echoes a delivery this node
    /// claims. Standalone nodes keep direct acceptance.
    hub_connected: bool,
}

impl McpServer {
    /// Builds a server for `peer_id` backed by `store`.
    ///
    /// The store is shared with the dispatcher; `hub_connected` gates
    /// local execution behind hub acceptance and delivery claims.
    pub fn new(store: Store, peer_id: impl Into<String>, hub_connected: bool) -> Self {
        Self {
            store,
            peer_id: peer_id.into(),
            hub_connected,
        }
    }

    /// Serves MCP over this process's stdio (stdin/stdout).
    ///
    /// Authentication is process ownership (D-3): whatever spawned this
    /// process wired its pipes. Blocks until the connection closes.
    ///
    /// # Errors
    ///
    /// Returns the initialize failure or the connection-close reason.
    pub async fn serve_stdio(self) -> Result<(), Box<dyn std::error::Error>> {
        let running = rmcp::serve_server(self, (tokio::io::stdin(), tokio::io::stdout())).await?;
        running.waiting().await?;
        Ok(())
    }

    /// Asserts this peer is a conversation member before read/report access.
    ///
    /// # Errors
    ///
    /// Returns the rejection message when the peer is not a member.
    async fn require_member(&self, conversation_id: &str) -> Result<(), String> {
        let members = self
            .store
            .conversation_members(conversation_id)
            .await
            .map_err(store_error)?;
        if !members.iter().any(|m| m == &self.peer_id) {
            return Err(format!(
                "peer is not a member of conversation {conversation_id}"
            ));
        }
        Ok(())
    }
}

/// Maps a store error to a tool-level failure.
///
/// The SDK contract (P06): `Err(String)` from a tool becomes a
/// successful tools/call envelope with `is_error: true` and this text
/// as content — the harness reads a failed tool call with the reason
/// intact. `Err(ErrorData)` would be a JSON-RPC protocol error
/// (`-32600`), which misclassifies an ordinary business denial as an
/// invalid MCP request. Store errors are operational failures, so they
/// ride the `is_error` envelope, never the protocol channel.
fn store_error(error: StoreError) -> String {
    error.to_string()
}

/// Describes who an accepted post actually reached.
///
/// A post that wakes nobody used to answer with a bare `accepted <id>`, so a
/// harness that addressed a peer in prose saw success while the room stayed
/// silent. The summary names the recipients, says plainly when there are none,
/// and separates the peers that will take a harness turn from those that never
/// will.
///
/// `store` is read for peer classes, `verb` is `accepted` or `replay`,
/// `message_id` is the committed message, `delivered` names the peers the
/// message reached, and `execution_requested` says whether the caller asked
/// for execution.
async fn routing_summary(
    store: &wrkspc_core::Store,
    verb: &str,
    message_id: &str,
    delivered: &[String],
    execution_requested: bool,
) -> String {
    if delivered.is_empty() {
        return format!(
            "{verb} {message_id}; no deliveries raised — nobody was addressed. \
             Pass the recipients parameter or write @peer in the text."
        );
    }
    let mut summary = format!("{verb} {message_id}; delivered to {}", delivered.join(", "));
    if execution_requested {
        let mut harness: Vec<String> = Vec::new();
        let mut other: Vec<String> = Vec::new();
        for peer in delivered {
            match store.peer_class_of(peer).await.ok().flatten().as_deref() {
                Some("harness") => harness.push(peer.clone()),
                Some(class) => other.push(format!("{peer} ({class})")),
                None => other.push(peer.clone()),
            }
        }
        if !harness.is_empty() {
            summary.push_str(&format!("; execution starts for {}", harness.join(", ")));
        }
        if !other.is_empty() {
            summary.push_str(&format!(
                "; no execution for {} — those peers take no harness turn",
                other.join(", ")
            ));
        }
    }
    summary
}

#[tool_router(server_handler)]
impl McpServer {
    /// Posts a message into a conversation under this peer's identity.
    ///
    /// Runs the store's readiness fence: idempotency keys deduplicate
    /// resubmissions, membership is validated, and execution intents are
    /// committed for recipients with `execution_requested`.
    #[tool(
        name = "post-message",
        description = "Post a message into a wrkspc conversation under the caller's peer identity. The same idempotency key with the same content returns the original message id (safe to retry); the same key with different content is rejected."
    )]
    async fn post_message(&self, params: Parameters<PostMessageParams>) -> Result<String, String> {
        let Parameters(params) = params;
        // Deliveries are opt-in. A harness post names its recipients
        // explicitly; a post that names nobody is visible in the room and
        // wakes no one. Fan-out by default turns four agents in one room
        // into a broadcast storm: every reply wakes every peer, every peer
        // replies, and nothing terminates.
        let recipients = params.recipients.unwrap_or_default();
        let recipient_requests = recipients
            .into_iter()
            .map(|peer_id| wrkspc_core::RecipientRequest {
                peer_id,
                execution_requested: params.execution_requested,
            })
            .collect::<Vec<_>>();
        let outcome = if self.hub_connected {
            // Hub mode: no local execution intents. The message mirrors
            // locally and syncs to the hub; execution happens only when
            // the hub accepts it (fences) and this node claims the
            // echoed delivery.
            self.store
                .accept_message_mirror_only(
                    &params.conversation_id,
                    &self.peer_id,
                    &params.idempotency_key,
                    &params.content,
                    params.execution_requested,
                    &recipient_requests,
                )
                .await
                .map_err(store_error)?
        } else {
            self.store
                .accept_message(
                    &params.conversation_id,
                    &self.peer_id,
                    &params.idempotency_key,
                    &params.content,
                    params.execution_requested,
                    &recipient_requests,
                )
                .await
                .map_err(store_error)?
        };
        let (verb, message_id) = match outcome {
            AcceptOutcome::Accepted { message_id } => ("accepted", message_id),
            AcceptOutcome::Replay { message_id } => ("replay", message_id),
        };
        let delivered = self
            .store
            .message_recipients(&message_id)
            .await
            .map_err(store_error)?;
        Ok(routing_summary(
            &self.store,
            verb,
            &message_id,
            &delivered,
            params.execution_requested,
        )
        .await)
    }

    /// Reads committed events from a conversation after a cursor.
    #[tool(
        name = "read-conversation",
        description = "Read committed wrkspc conversation events after a sequence cursor (resumable). Membership required."
    )]
    async fn read_conversation(
        &self,
        params: Parameters<ReadConversationParams>,
    ) -> Result<String, String> {
        let Parameters(params) = params;
        // Bounded reads (P07): the store clamps defensively, but the
        // tool rejects out-of-range values loudly so a harness learns
        // the contract instead of silently getting a clamped page.
        if !(1..=500).contains(&params.limit) {
            return Err(format!(
                "limit must be between 1 and 500 (got {})",
                params.limit
            ));
        }
        self.require_member(&params.conversation_id).await?;
        let events = self
            .store
            .events_since(&params.conversation_id, params.after_seq, params.limit)
            .await
            .map_err(store_error)?;
        serde_json::to_string(&events).map_err(|e| e.to_string())
    }

    /// Lists the connected cohort (HLD rev0.9): the hub's roster
    /// mirrored onto this node — peer id, name, class, host, and last
    /// reported live state. Global and lean; no membership gate.
    #[tool(
        name = "list-peers",
        description = "List the connected wrkspc cohort: peer id, display name, class, host, and last reported live state. Global roster, no membership required."
    )]
    async fn list_peers(&self) -> Result<String, String> {
        let roster = self.store.roster().await.map_err(store_error)?;
        serde_json::to_string(&roster).map_err(|e| e.to_string())
    }

    /// Appends a work-status report to the conversation's committed feed.
    #[tool(
        name = "report-work",
        description = "Append a free-form work status report to a conversation's committed event feed. Membership required."
    )]
    async fn report_work(&self, params: Parameters<ReportWorkParams>) -> Result<String, String> {
        let Parameters(params) = params;
        self.require_member(&params.conversation_id).await?;
        let payload = serde_json::json!({
            "peer_id": self.peer_id,
            "summary": params.summary,
        });
        let seq = self
            .store
            .append_event(&params.conversation_id, "work.report", &payload)
            .await
            .map_err(store_error)?;
        Ok(format!("reported at event {seq}"))
    }
}

/// Configuration for the HTTP listener.
#[derive(Debug, Clone)]
pub struct HttpListenerConfig {
    /// Bind address. Defaults to loopback — the origin check is the
    /// listener itself: only local connections may present tokens.
    pub bind: SocketAddr,
    /// Accepted bearer tokens. A connection must present one of these.
    pub tokens: HashSet<String>,
}

impl Default for HttpListenerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            tokens: HashSet::new(),
        }
    }
}

/// The authenticated HTTP surface for one enrolled peer.
///
/// Serves MCP over Streamable HTTP (the current MCP transport; rmcp no
/// longer ships an SSE server) on the configured address. The axum router
/// is wrapped with a bearer-token gate: requests without a token from
/// `config.tokens` are rejected with `401` before reaching MCP. The
/// transport's own default host allowlist accepts loopback only, so the
/// DNS-rebinding class the rmcp advisory covers is closed twice.
#[derive(Debug)]
pub struct HttpListener {
    config: HttpListenerConfig,
    server: McpServer,
}

impl HttpListener {
    /// Builds a listener for `server` with `config` (bind address and
    /// token allowlist).
    pub fn new(server: McpServer, config: HttpListenerConfig) -> Self {
        Self { config, server }
    }

    /// Binds the configured address and serves until the `shutdown` future
    /// resolves.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` on bind or serve failure.
    pub async fn serve<F>(self, shutdown: F) -> std::io::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind(self.config.bind).await?;
        self.serve_on(listener, shutdown).await
    }

    /// Serves on an already-bound `listener` until the `shutdown` future
    /// resolves.
    ///
    /// Tests bind port 0 themselves to discover the address before calling
    /// this.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` on serve failure.
    pub async fn serve_on<F>(
        self,
        listener: tokio::net::TcpListener,
        shutdown: F,
    ) -> std::io::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let addr = listener.local_addr()?;
        tracing::info!(%addr, peer = %self.server.peer_id, "wrkspc MCP http listener bound");

        // Streamable HTTP: a tower service mounted at /mcp. Older clients
        // keep managed sessions; newer protocol versions are stateless. Each
        // handler clone retains this listener's fixed peer identity. The
        // default config accepts loopback hosts only.
        let server = self.server.clone();
        let http_config =
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default();
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::<
            McpServer,
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
        >::new(
            move || Ok::<_, std::io::Error>(server.clone()),
            Default::default(),
            http_config,
        );
        let router = axum::Router::new().nest_service("/mcp", service);
        let tokens = self.config.tokens.clone();
        let router = router.layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let tokens = tokens.clone();
                async move {
                    let authorized = request
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.strip_prefix("Bearer "))
                        .map(|token| tokens.contains(token))
                        .unwrap_or(false);
                    if !authorized {
                        let response = axum::response::Response::builder()
                            .status(axum::http::StatusCode::UNAUTHORIZED)
                            .body(axum::body::Body::from("unauthorized"));
                        return match response {
                            Ok(response) => response,
                            // A static status and static body cannot fail;
                            // this arm guards against builder misuse.
                            Err(error) => {
                                unreachable!("static unauthorized response failed: {error}")
                            }
                        };
                    }
                    next.run(request).await
                }
            },
        ));

        tokio::select! {
            result = axum::serve(listener, router) => {
                result?;
            }
            _ = shutdown => {}
        }
        Ok(())
    }
}
