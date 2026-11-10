//! Credential redaction for the display path.
//!
//! The store keeps raw content; every value that renders in the operator
//! UI passes through here first. Redaction is display-level: it masks
//! credential-shaped spans (API keys, bearer tokens, PEM blocks,
//! key=value secrets) so a compromised renderer cannot exfiltrate what it
//! never received. It is not a parser — it is a conservative mask.

use serde_json::Value;

/// The static redaction regexes, compiled ONCE (UI-19): the display
/// path redacts every rendered string, so per-call compilation put
/// regex construction on the transcript-poll hot path.
///
/// The `expect` below is a reviewed static-pattern allowance: every
/// pattern is a compile-time constant in this file, so construction
/// cannot fail, and a regression is a unit-test failure, not a
/// runtime path.
#[allow(clippy::expect_used)]
fn regexes() -> &'static Regexes {
    static REGEXES: std::sync::OnceLock<Regexes> = std::sync::OnceLock::new();
    REGEXES.get_or_init(|| Regexes {
        // Most specific first: PEM blocks before the generic key patterns.
        pem: regex::Regex::new(
            r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        )
        .expect("static regex compiles"),
        bearer: regex::Regex::new(r"(?i)\b(bearer)\s+[a-z0-9._~+/=-]{8,}")
            .expect("static regex compiles"),
        // Key=value secrets. Quote-aware on both sides of the separator
        // (UI-07): single- AND double-quoted values mask, escaped
        // quotes inside a double-quoted value do not terminate it
        // early, and an unquoted value stops at the same delimiters as
        // before.
        secret_assign: regex::Regex::new(
            r#"(?i)\b(api[_-]?key|access[_-]?token|secret|password)"?\s*[:=]\s*("[^"\\]*(?:\\.[^"\\]*)*"|'[^']*'|[^\s,;}\]"']+)"#,
        )
        .expect("static regex compiles"),
        // API keys carry hyphens and underscores inside the key material
        // (`«redacted:sk-…»...`, `sk-proj-...`); an alphabet without them
        // stops at the first separator and leaves the suffix exposed.
        api_key: regex::Regex::new(r"(?i)\bsk-[a-z0-9_-]{8,}").expect("static regex compiles"),
    })
}

/// The compiled redaction regexes.
struct Regexes {
    pem: regex::Regex,
    bearer: regex::Regex,
    secret_assign: regex::Regex,
    api_key: regex::Regex,
}

/// Masks credential-shaped spans in a display string.
///
/// # Arguments
///
/// * `text` — the string to redact.
///
/// # Returns
///
/// The redacted string.
pub fn redact(text: &str) -> String {
    let regexes = regexes();
    let redacted = regexes.pem.replace_all(text, "[REDACTED private key]");
    let redacted = regexes.bearer.replace_all(&redacted, "$1 [REDACTED]");
    let redacted = regexes
        .secret_assign
        .replace_all(&redacted, "$1: [REDACTED]");
    regexes
        .api_key
        .replace_all(&redacted, "[REDACTED key]")
        .into_owned()
}

/// Whether an object key names a credential-bearing field: values under
/// these keys mask regardless of their shape (UI-07 — a structured
/// `{"password": "hunter2"}` object must not leak the leaf just
/// because the VALUE alone carries no label).
// Same reviewed static-pattern allowance as `regexes`: the pattern is
// a compile-time constant, and a regression fails the unit tests.
#[allow(clippy::expect_used)]
fn is_sensitive_key(key: &str) -> bool {
    static KEY: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    KEY.get_or_init(|| {
        regex::Regex::new(r"(?i)^(api[_-]?key|access[_-]?token|secret|password)$")
            .expect("static regex compiles")
    })
    .is_match(key)
}

/// Redacts every string leaf in a JSON value (event payloads).
///
/// # Arguments
///
/// * `value` — the payload to redact.
///
/// # Returns
///
/// A redacted copy; structure and non-string leaves are untouched.
pub fn redact_value(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(redact(s)),
        Value::Array(items) => Value::Array(items.iter().map(redact_value).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    // Sensitive keys mask the whole value: the leaf
                    // under `password`/`secret`/`access_token`/`api_key`
                    // never renders, whatever its shape (UI-07).
                    if is_sensitive_key(k) {
                        (k.clone(), Value::String("[REDACTED]".to_string()))
                    } else {
                        (k.clone(), redact_value(v))
                    }
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_api_keys() {
        assert_eq!(
            redact("key is sk-abcdef1234567890 ok"),
            "key is [REDACTED key] ok"
        );
    }

    #[test]
    fn masks_bearer_tokens_keeping_the_scheme() {
        let out = redact("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc");
        assert!(out.contains("Bearer [REDACTED]"), "got: {out}");
        assert!(!out.contains("eyJhbGci"), "token body must be gone: {out}");
    }

    #[test]
    fn masks_pem_blocks_entirely() {
        let text = "before\n-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----\nafter";
        let out = redact(text);
        assert!(out.contains("[REDACTED private key]"), "got: {out}");
        assert!(!out.contains("MIIEow"), "key material must be gone: {out}");
    }

    #[test]
    fn masks_key_value_secrets() {
        let out = redact("api_key=sk-live-123456 secret: hunter2");
        assert!(out.contains("api_key: [REDACTED]"), "got: {out}");
        assert!(out.contains("secret: [REDACTED]"), "got: {out}");
        assert!(!out.contains("hunter2"), "got: {out}");
    }

    #[test]
    fn masks_quoted_json_secrets() {
        // The quote between key and separator must not defeat the mask,
        // and a quoted value may contain spaces.
        let out = redact(r#"{"password":"hunter2"}"#);
        assert!(!out.contains("hunter2"), "got: {out}");
        let out = redact(r#"{"password": "hunter two zero"}"#);
        assert!(!out.contains("hunter"), "got: {out}");
        let out = redact(r#"config: access_token="abc def ghi""#);
        assert!(!out.contains("abc def"), "got: {out}");
    }

    #[test]
    fn masks_hyphenated_api_keys_entirely() {
        // The key alphabet includes `-` and `_`: the whole material must
        // vanish, not an `sk-`-adjacent fragment with the suffix left.
        let out = redact("token leaked: sk-ant-api03-AbCdEfGhIjKlMnOp");
        assert!(!out.contains("AbCdEfGhIjKlMnOp"), "got: {out}");
        assert!(out.contains("[REDACTED key]"), "got: {out}");
        let out = redact("project key sk-proj-1234abcd5678ef90");
        assert!(!out.contains("1234abcd"), "got: {out}");
    }

    #[test]
    fn leaves_prose_untouched() {
        let text = "the session paused at 14:32; hold released for agent";
        assert_eq!(redact(text), text);
    }

    #[test]
    fn leaving_prose_untouched_quoted_keys() {
        // Ordinary quoted prose must not accidentally trip the maskers.
        let text = "the agent said \"password\" twice and kept working";
        assert_eq!(redact(text), text);
    }

    #[test]
    fn masks_single_quoted_and_escaped_assignments() {
        // Single-quoted assignments mask (UI-07).
        let out = redact("password='syntheticvalue'");
        assert!(!out.contains("syntheticvalue"), "got: {out}");
        // Escaped double quotes inside a double-quoted value do not
        // terminate the mask early (UI-07).
        let out = redact(r#"password="hunter2\" suffix""#);
        assert!(!out.contains("hunter2"), "got: {out}");
        assert!(!out.contains("suffix"), "got: {out}");
    }

    #[test]
    fn masks_structured_values_under_sensitive_keys() {
        // A structured object's `password`/`access_token` leaves mask
        // regardless of the value's shape (UI-07).
        let value = serde_json::json!({
            "kind": "tool.invoked",
            "args": { "password": "hunter2", "access_token": "tok-abc", "name": "keep" },
        });
        let out = redact_value(&value);
        assert_eq!(out["args"]["password"], "[REDACTED]");
        assert_eq!(out["args"]["access_token"], "[REDACTED]");
        assert_eq!(out["args"]["name"], "keep");
        let serialized = serde_json::to_string(&out).unwrap();
        assert!(!serialized.contains("hunter2"), "got: {serialized}");
        assert!(!serialized.contains("tok-abc"), "got: {serialized}");
    }

    #[test]
    fn redacts_string_leaves_in_json_without_breaking_structure() {
        let value = serde_json::json!({
            "kind": "message.accepted",
            "payload": { "content": "api_key: abcdef1234567890" },
            "nested": ["keep", "sk-1234567890"],
            "num": 7,
            "flag": true,
        });
        let out = redact_value(&value);
        assert_eq!(out["payload"]["content"], "api_key: [REDACTED]");
        let serialized = serde_json::to_string(&out).expect("still valid JSON");
        assert!(
            !serialized.contains("abcdef1234567890"),
            "got: {serialized}"
        );
        assert_eq!(out["num"], 7);
        assert_eq!(out["flag"], true);
    }
}
