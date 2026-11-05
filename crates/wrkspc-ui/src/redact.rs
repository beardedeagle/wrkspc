//! Credential redaction for the display path.
//!
//! The store keeps raw content; every value that renders in the operator
//! UI passes through here first. Redaction is display-level: it masks
//! credential-shaped spans (API keys, bearer tokens, PEM blocks,
//! key=value secrets) so a compromised renderer cannot exfiltrate what it
//! never received. It is not a parser — it is a conservative mask.

use serde_json::Value;

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
    // Most specific first: PEM blocks before the generic key patterns.
    let pem = regex::Regex::new(
        r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    )
    .expect("static regex compiles");
    let bearer =
        regex::Regex::new(r"(?i)\b(bearer)\s+[a-z0-9._~+/=-]{8,}").expect("static regex compiles");
    let secret_assign = regex::Regex::new(
        r"(?i)\b(api[_-]?key|access[_-]?token|secret|password)\s*[:=]\s*[^\s,;}\]]+",
    )
    .expect("static regex compiles");
    let api_key = regex::Regex::new(r"(?i)\bsk-[a-z0-9]{8,}").expect("static regex compiles");

    let redacted = pem.replace_all(text, "[REDACTED private key]");
    let redacted = bearer.replace_all(&redacted, "$1 [REDACTED]");
    let redacted = secret_assign.replace_all(&redacted, "$1: [REDACTED]");
    api_key
        .replace_all(&redacted, "[REDACTED key]")
        .into_owned()
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
                .map(|(k, v)| (k.clone(), redact_value(v)))
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
    fn leaves_prose_untouched() {
        let text = "the session paused at 14:32; hold released for agent";
        assert_eq!(redact(text), text);
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
