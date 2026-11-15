//! The bearer-token gate and the static page shell.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::*;

/// The bearer gate rejects missing and wrong tokens and admits the right one.
#[tokio::test]
async fn bearer_gate_rejects_missing_and_wrong_tokens() {
    let ui = spawn_ui().await;

    let no_token = ui
        .client()
        .get(format!("{}/api/health", ui.base))
        .send()
        .await
        .unwrap();
    assert_eq!(no_token.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = ui
        .client()
        .get(format!("{}/api/health", ui.base))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    let ok = ui.authed().send().await.unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
    assert_eq!(
        ok.json::<serde_json::Value>().await.unwrap()["status"],
        "ok"
    );

    let _ = ui.shutdown.send(());
}

/// The root serves the static page shell with an HTML content type,
/// and the shell is no-store: it carries the live node's token flow,
/// so a cached copy would show a stale token prompt.
#[tokio::test]
async fn serves_the_static_ui() {
    let ui = spawn_ui().await;
    let res = ui
        .client()
        .get(format!("{}/", ui.base))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(
        res.headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store",
        "the shell must never be cached"
    );
    let content_type = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(content_type.contains("text/html"), "got {content_type}");
    let body = res.text().await.unwrap();
    assert!(body.contains("<title>wrkspc</title>"), "page shell served");

    // An authenticated request to an unknown SPA route is a 404, not
    // the shell.
    let res = ui
        .client()
        .get(format!("{}/not-a-route", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);

    // Authenticated API payloads are no-store too: cookie-authed
    // transcripts must not be heuristically cached by a shared cache.
    let api = ui.authed().send().await.unwrap();
    assert_eq!(
        api.headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store",
        "authenticated API responses must not be cached"
    );
    let _ = ui.shutdown.send(());
}
