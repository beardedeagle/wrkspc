//! The node's stamped mTLS client for dialing the hub.

use std::path::Path;

/// The node's hub requests are bounded by this total per-request
/// timeout (connect + TLS handshake + response). A wedged handshake
/// must surface as a poll-leg warning and a retry on the next tick,
/// never as a silent forever-stall.
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Builds the node's mTLS client for dialing the hub: the hub's
/// certificate is verified against `ca`, and the node presents its own
/// certificate (`cert` + `key`) as the client identity.
///
/// # Arguments
///
/// * `ca` — CA PEM that signed the hub certificate.
/// * `cert` — this node's client certificate PEM.
/// * `key` — this node's client private key PEM.
///
/// # Returns
///
/// The configured client.
///
/// # Errors
///
/// Returns a boxed error on unreadable or invalid PEM material.
pub fn build_hub_client(
    ca: &Path,
    cert: &Path,
    key: &Path,
) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    build_hub_client_with_timeout(ca, cert, key, DEFAULT_REQUEST_TIMEOUT)
}

/// Builds the hub client with an explicit total request timeout.
///
/// Every hub call (task list, claim, ack, presence, roster, outbound
/// send) rides this client, so the timeout bounds any wedged handshake
/// or stalled response: the call errors, the poll leg logs a warning,
/// and the next tick retries with a fresh connection. Without it, a
/// connection that completes TCP but never finishes TLS wedges the
/// whole tick silently forever — the poll loop has no other clock.
///
/// # Arguments
///
/// * `ca` — CA PEM that signed the hub certificate.
/// * `cert` — this node's client certificate PEM.
/// * `key` — this node's client private key PEM.
/// * `timeout` — total per-request bound (connect + handshake + read).
///
/// # Returns
///
/// The configured client.
///
/// # Errors
///
/// Returns a boxed error on unreadable or invalid PEM material.
pub fn build_hub_client_with_timeout(
    ca: &Path,
    cert: &Path,
    key: &Path,
    timeout: std::time::Duration,
) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let ca_pem = std::fs::read(ca)?;
    // PEM section boundaries: concatenating two buffers directly glues
    // the cert's `-----END CERTIFICATE-----` to the key's next
    // `-----BEGIN ...-----` on one line whenever the cert file lacks a
    // trailing newline (Vault/openssl artifacts do; rcgen's don't
    // always) — rustls-pki-types then sees no sections and fails with
    // "private key or certificate not found". Join on a newline
    // boundary instead of raw bytes.
    let mut identity_pem = std::fs::read(cert)?;
    if !identity_pem.ends_with(b"\n") {
        identity_pem.push(b'\n');
    }
    identity_pem.extend(std::fs::read(key)?);
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca_pem)?)
        .identity(reqwest::Identity::from_pem(&identity_pem)?)
        .timeout(timeout)
        .build()?;
    Ok(client)
}
