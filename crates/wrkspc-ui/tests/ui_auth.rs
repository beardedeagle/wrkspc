//! The bearer-token gate and the static page shell.

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

/// The root serves the static page shell with an HTML content type.
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
    let content_type = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(content_type.contains("text/html"), "got {content_type}");
    let body = res.text().await.unwrap();
    assert!(body.contains("<title>wrkspc</title>"), "page shell served");
    let _ = ui.shutdown.send(());
}
