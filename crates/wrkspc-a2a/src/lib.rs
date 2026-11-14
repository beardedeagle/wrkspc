//! wrkspc-a2a — the agent-facing coordination surface.
//!
//! The pilot runs this in-process as the coordinator role. It serves the
//! v1.0 agent card, a `TaskStore` impl over the wrkspc store, and the six
//! core A2A methods (`send_message`, `send_streaming_message`, `get_task`,
//! `list_tasks`, `cancel_task`, `subscribe_to_task`).
//!
//! The executor deliberately does NOT complete tasks: acceptance is
//! recorded as `submitted` with a status message (a continuation keeps
//! its existing non-terminal state), and execution state is driven by
//! the dispatcher (unit 5). A completed *turn* is never claimed as task
//! success (D-3 predicate).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod executor;
pub mod server;
pub mod state_mapping;
pub mod store_adapter;

pub use executor::WrkspcExecutor;
pub use server::{
    A2aSurface, build_agent_card, build_peer_card, cert_serial_hex, host_identity, peer_identity,
};
pub use state_mapping::LifecycleState;
pub use store_adapter::WrkspcTaskStore;

use rustls::DistinguishedName;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::ClientCertVerifier;
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::sync::Arc;

/// The per-connection policy verdict from the serve-loop callback.
///
/// The serve loop resolves the presented certificate chain into one of
/// these before any HTTP is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerDecision {
    /// Drop the connection before any HTTP.
    Deny,
    /// Serve the connection without an `x-wrkspc-peer` stamp. Only
    /// reachable when the acceptor makes client authentication
    /// optional and no certificate was presented.
    Unstamped,
    /// Serve the connection, stamping every request with this
    /// identity.
    Stamped(String),
}

/// A client-certificate verifier that requests but does not require a
/// certificate: connections presenting a chain are verified against
/// `inner`'s trust anchors, and connections presenting none complete
/// the handshake unauthenticated.
///
/// The hub uses this so its TLS surface can serve both the mTLS A2A
/// routes and the browser UI; route-level policy (the hub's stamp
/// gate) still decides what an unauthenticated connection may reach.
#[derive(Debug)]
pub struct OptionalClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
}

impl OptionalClientVerifier {
    /// Wraps `inner`, the verifier used for presented chains.
    pub fn new(inner: Arc<dyn ClientCertVerifier>) -> Self {
        Self { inner }
    }
}

impl ClientCertVerifier for OptionalClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        self.inner
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
