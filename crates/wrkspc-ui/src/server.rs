//! The UI server: the operator surface over the committed state.
//!
//! Serves the browser UI (static) and the operator API (`/api/*`) on a
//! loopback-default address. API requests require the bearer token;
//! the UI prompts for it and stores it locally — the token is never
//! injected into served pages.

use crate::api::{AppState, router};
use axum::response::IntoResponse;
use std::net::SocketAddr;
use std::sync::Arc;
use wrkspc_core::Store;

/// The operator login cookie name.
const COOKIE_NAME: &str = "wrkspc_token";

/// The `/api/login` request body: the bearer token pasted from the
/// login card.
#[derive(serde::Deserialize)]
struct LoginBody {
    token: String,
}

/// UI server configuration.
#[derive(Debug, Clone)]
pub struct UiConfig {
    /// Bind address. Defaults to `127.0.0.1:0` (loopback, ephemeral port).
    pub bind: SocketAddr,
    /// The bearer token operators must present. When `None`, a random
    /// token is generated at startup and logged once.
    pub token: Option<String>,
    /// Hub mode: operator posts also queue pollable delivery tasks for
    /// recipients on remote hosts (the hub's own host is
    /// `PILOT_HOST_ID`). The node role leaves this `false` — its
    /// harness peers are local and its deliveries come from the hub.
    pub queue_remote_deliveries: bool,
    /// Hub mode: the operator surface that owns the shared store
    /// (conversation creation). The node role leaves this `false` and
    /// its `POST /api/conversations` returns 404.
    pub hub_role: bool,
    /// Whether this surface dials a hub and mirrors hub-owned state
    /// (conversation names, peer aliases). Hub-owned mutations are
    /// refused with 409 instead of reporting success for writes the
    /// next authoritative snapshot erases (UI-08).
    pub hub_connected: bool,
    /// Marks the login cookie `Secure`. True on TLS listeners (the
    /// browser stores it); false on the plaintext loopback listener
    /// (browsers refuse Secure cookies over http, so the cookie is
    /// simply not stored there).
    pub cookie_secure: bool,
}

// The default bind address is a compile-time constant; the `expect`
// is the reviewed static-value allowance (a regression is caught by
// any test that boots a server).
#[allow(clippy::expect_used)]
impl Default for UiConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:0".parse().expect("static address parses"),
            cookie_secure: false,
            hub_connected: false,
            token: None,
            queue_remote_deliveries: false,
            hub_role: false,
        }
    }
}

/// Constant-time equality for operator credentials: the comparison
/// time does not depend on how many leading bytes match.
fn ct_equals(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b) {
        diff |= left ^ right;
    }
    diff == 0
}

/// The cookie a successful login sets: HttpOnly so page scripts never
/// read it, SameSite=Strict so cross-site requests never carry it, and
/// Secure on TLS listeners.
fn set_cookie_value(token: &str, secure: bool) -> String {
    format!(
        "{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/{}",
        if secure { "; Secure" } else { "" }
    )
}

/// The presented login cookie value, if any.
fn presented_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    let header = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    for part in header.split("; ") {
        if let Some(value) = part.strip_prefix(COOKIE_NAME)
            && let Some(value) = value.strip_prefix('=')
        {
            return Some(value.to_string());
        }
    }
    None
}

/// The UI server.
pub struct UiServer {
    config: UiConfig,
    store: Store,
    cancel: Arc<dyn crate::api::CancelRequester>,
    deny: Option<crate::api::RevocationSet>,
}

// The cancel requester is a trait object without Debug; the summary
// carries the observable configuration instead.
impl std::fmt::Debug for UiServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiServer")
            .field("config", &self.config)
            .field("has_deny_set", &self.deny.is_some())
            .finish_non_exhaustive()
    }
}

impl UiServer {
    /// Builds a UI server.
    ///
    /// `store` is the transactional store; `cancel` is the cancel seam
    /// (the dispatcher handle in production); `config` carries the bind
    /// address and bearer token; `deny` is the hub's live
    /// certificate-serial deny set, when this server is embedded on the
    /// mTLS surface — operator bans update it so revocations take
    /// effect on the next handshake without a hub restart.
    pub fn new(
        store: Store,
        cancel: Arc<dyn crate::api::CancelRequester>,
        config: UiConfig,
        deny: Option<crate::api::RevocationSet>,
    ) -> Self {
        Self {
            config,
            store,
            cancel,
            deny,
        }
    }

    /// Resolves the bearer token (configured or freshly generated).
    /// Public so callers embedding the router on a shared listener
    /// (the hub's TLS surface) resolve the same token the UI prompts
    /// for.
    pub fn resolve_token(&self) -> String {
        match &self.config.token {
            Some(token) => token.clone(),
            None => {
                let token = uuid::Uuid::new_v4().to_string();
                tracing::info!(%token, "wrkspc UI bearer token generated (set WRKSPC_UI_TOKEN to pin it)");
                token
            }
        }
    }

    /// Binds the configured address and serves until `shutdown`
    /// resolves (a future that ends serving when it completes).
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

    /// Builds the served router — the operator API behind the bearer
    /// gate plus the static UI page — for embedding on a shared
    /// listener (e.g. the hub's TLS surface, which routes by path).
    /// [`UiServer::serve_on`] serves exactly this router.
    ///
    /// `token` is the resolved bearer token (see
    /// [`UiServer::resolve_token`]).
    ///
    /// # Returns
    ///
    /// The complete UI router.
    pub fn into_app(self, token: String) -> axum::Router {
        let state = AppState::new(
            self.store,
            self.cancel,
            self.config.queue_remote_deliveries,
            self.config.hub_role,
            self.config.hub_connected,
            self.deny,
        );

        let cookie_secure = self.config.cookie_secure;
        let middleware_token = token.clone();
        let api = router(state).layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let token = middleware_token.clone();
                async move {
                    let presented = presented_cookie(request.headers());
                    let header_ok = request
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.strip_prefix("Bearer "))
                        .map(|value| ct_equals(value, &token))
                        .unwrap_or(false);
                    let cookie_ok = presented
                        .as_deref()
                        .map(|value| ct_equals(value, &token))
                        .unwrap_or(false);
                    if !header_ok && !cookie_ok {
                        // A 401 never clears a cookie: a stale request's
                        // rejection must not clear a newer login, and a
                        // cookie that holds the current token is by
                        // definition authorized. A stale cookie is
                        // superseded by the next successful login's
                        // Set-Cookie.
                        let response = axum::response::Response::builder()
                            .status(axum::http::StatusCode::UNAUTHORIZED)
                            .header("content-type", "text/plain; charset=utf-8")
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

        let login_token = token;
        let login_secure = cookie_secure;
        // The login route sits OUTSIDE the auth layer: it is the only
        // unauthenticated /api route and the only place the cookie is
        // set.
        let login = axum::routing::post(move |axum::Json(body): axum::Json<LoginBody>| {
            let login_token = login_token.clone();
            async move {
                if !ct_equals(&body.token, &login_token) {
                    return (axum::http::StatusCode::UNAUTHORIZED, "unauthorized").into_response();
                }
                let cookie = set_cookie_value(&login_token, login_secure);
                let header = match axum::http::HeaderValue::from_str(&cookie) {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::error!(%error, "login cookie header construction failed");
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "login failed",
                        )
                            .into_response();
                    }
                };
                let mut response = axum::http::StatusCode::NO_CONTENT.into_response();
                response
                    .headers_mut()
                    .insert(axum::http::header::SET_COOKIE, header);
                response
            }
        });

        axum::Router::new()
            .merge(api)
            .route("/", axum::routing::get(static_index))
            .route("/api/login", login)
    }

    /// Serves on an already-bound `listener` until `shutdown` resolves.
    ///
    /// `listener` is a bound TCP listener (tests bind port 0 themselves
    /// to discover the address); `shutdown` is a future that ends
    /// serving when it resolves.
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
        let token = self.resolve_token();
        let app = self.into_app(token);

        tracing::info!(%addr, "wrkspc UI bound");
        tokio::select! {
            result = axum::serve(listener, app) => {
                result?;
            }
            _ = shutdown => {}
        }
        Ok(())
    }
}

/// Serves the static UI page.
///
/// The page is served with `no-store`: it carries the live node's token
/// flow and the embedded UI changes with every deployment. A cached copy
/// would show a stale UI (and a stale token prompt), so the browser must
/// re-fetch on every load.
async fn static_index() -> axum::response::Response {
    match axum::response::Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .header("cache-control", "no-store")
        .body(axum::body::Body::from(crate::static_ui::INDEX_HTML))
    {
        Ok(response) => response,
        // Static headers and a static body cannot fail; this arm guards
        // against builder misuse.
        Err(error) => unreachable!("static index response failed: {error}"),
    }
}
