//! Operator login-cookie contract: the login route sets the HttpOnly
//! SameSite cookie (Secure only on TLS listeners), the middleware
//! accepts the cookie or the bearer header, and a rejection never
//! carries a clearing Set-Cookie.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::{TOKEN, spawn_ui, spawn_ui_secure};

/// A valid login sets the HttpOnly SameSite cookie (without Secure on
/// the loopback config) and answers 204.
#[tokio::test]
async fn valid_login_sets_the_cookie() {
    let ui = spawn_ui().await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/login", ui.base))
        .json(&serde_json::json!({ "token": TOKEN }))
        .send()
        .await
        .expect("login request");
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    let cookie = response
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .expect("set-cookie present")
        .to_str()
        .expect("set-cookie is text")
        .to_string();
    assert!(
        cookie.starts_with(&format!("wrkspc_token={TOKEN};")),
        "unexpected cookie: {cookie}"
    );
    assert!(cookie.contains("HttpOnly"), "{cookie}");
    assert!(cookie.contains("SameSite=Strict"), "{cookie}");
    assert!(
        !cookie.contains("Secure"),
        "the loopback config must not mark the cookie Secure: {cookie}"
    );
    let _ = ui.shutdown.send(());
}

/// The TLS-listener config marks the cookie Secure so the browser
/// stores it over https.
#[tokio::test]
async fn tls_config_marks_the_cookie_secure() {
    let ui = spawn_ui_secure().await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/login", ui.base))
        .json(&serde_json::json!({ "token": TOKEN }))
        .send()
        .await
        .expect("login request");
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    let cookie = response
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .expect("set-cookie present")
        .to_str()
        .expect("set-cookie is text");
    assert!(cookie.contains("; Secure"), "{cookie}");
    let _ = ui.shutdown.send(());
}

/// A wrong-token login is a 401 that sets no cookie.
#[tokio::test]
async fn wrong_token_login_401s_without_a_cookie() {
    let ui = spawn_ui().await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/login", ui.base))
        .json(&serde_json::json!({ "token": "wrong-token" }))
        .send()
        .await
        .expect("login request");
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        response
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .is_none(),
        "a failed login must never set a cookie"
    );
    let _ = ui.shutdown.send(());
}

/// The login cookie authenticates an API route.
#[tokio::test]
async fn login_cookie_authenticates_the_api() {
    let ui = spawn_ui().await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/conversations", ui.base))
        .header(reqwest::header::COOKIE, format!("wrkspc_token={TOKEN}"))
        .send()
        .await
        .expect("api request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let _ = ui.shutdown.send(());
}

/// The bearer header still authenticates (scripts and healthchecks).
#[tokio::test]
async fn bearer_header_still_authenticates() {
    let ui = spawn_ui().await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/conversations", ui.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("api request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let _ = ui.shutdown.send(());
}

/// An unauthenticated request is a 401 that carries NO clearing
/// Set-Cookie: a stale request's rejection must never clear a newer
/// login's cookie.
#[tokio::test]
async fn unauthenticated_401_carries_no_set_cookie() {
    let ui = spawn_ui().await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/conversations", ui.base))
        .send()
        .await
        .expect("api request");
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        response
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .is_none(),
        "a stale 401 must never clear the remembered cookie"
    );
    let _ = ui.shutdown.send(());
}

/// A wrong-cookie request is a 401 that carries no clearing
/// Set-Cookie (the rotated-credential case: the stale cookie is
/// superseded by the next successful login, not cleared by rejects).
#[tokio::test]
async fn wrong_cookie_401_carries_no_set_cookie() {
    let ui = spawn_ui().await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/conversations", ui.base))
        .header(reqwest::header::COOKIE, "wrkspc_token=stale-value")
        .send()
        .await
        .expect("api request");
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        response
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .is_none(),
        "a stale cookie's 401 must never clear the remembered cookie"
    );
    let _ = ui.shutdown.send(());
}
