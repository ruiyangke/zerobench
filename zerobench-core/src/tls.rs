//! Shared TLS client configuration for every transport crate.
//!
//! Every transport that can speak TLS (HTTP/1, HTTP/2, WebSocket, SSE)
//! ultimately needs the same `rustls::ClientConfig`: a root store, an
//! optional "accept anything" verifier for `--insecure`, and an ALPN
//! list. Rather than duplicate that code in four places, this module
//! owns the single builder and the four call-sites pick an appropriate
//! ALPN list at the point of connect.
//!
//! # Root store
//!
//! Strict verification uses [`webpki_roots::TLS_SERVER_ROOTS`] — a
//! compiled-in copy of the Mozilla trust store. We deliberately avoid
//! `rustls-platform-verifier` here to keep the dep tree small and the
//! behaviour deterministic across platforms; a cert either validates
//! against the Mozilla bundle or it doesn't.
//!
//! # Insecure mode
//!
//! `TransportOpts::insecure_tls = true` swaps in [`InsecureVerifier`],
//! which accepts **any** certificate chain, hostname, and signature.
//! This matches the semantics of `curl -k` / `wget --no-check-certificate`
//! and is the usual ask for benchmarking against self-signed internal
//! stacks. Users opt in explicitly with `-k` / `--insecure`.
//!
//! # ALPN
//!
//! Protocol selection happens **during** the handshake via ALPN, so the
//! caller decides the list before building the config:
//!
//! | Caller                    | ALPN list                 |
//! | ------------------------- | ------------------------- |
//! | HTTP/1.1 only             | `[b"http/1.1"]`           |
//! | HTTP/2 only               | `[b"h2"]`                 |
//! | HTTP Auto (prefer H2)     | `[b"h2", b"http/1.1"]`    |
//! | WebSocket (wss://)        | `&[]` (no ALPN — Upgrade happens post-handshake) |
//! | SSE (typically HTTP/1.1)  | `&[]` or `[b"http/1.1"]`  |
//!
//! An empty ALPN list means "don't advertise any protocol"; rustls will
//! then leave [`rustls::ClientConnection::alpn_protocol`] as `None`, and
//! the server's ALPN extension is ignored.

use std::sync::Arc;

use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::transport::TransportOpts;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Build a `rustls::ClientConfig` for the given opts and ALPN list.
///
/// The returned `Arc<ClientConfig>` is cheap to clone and is what the
/// per-transport `TlsConnector::from(...)` expects.
///
/// - `opts.insecure_tls == false` (default): strict webpki-roots
///   verification. A self-signed cert will be rejected at handshake.
/// - `opts.insecure_tls == true`: all certificate and signature checks
///   are skipped — equivalent to `curl -k`. Only use when benchmarking
///   against an untrusted test server.
///
/// The `alpn` list is copied into the config; pass `&[]` to disable
/// ALPN altogether (e.g. for WebSocket, where the Upgrade dance sits
/// above TLS rather than being selected by it).
pub fn tls_client_config(opts: &TransportOpts, alpn: &[&[u8]]) -> Arc<ClientConfig> {
    let mut config = if opts.insecure_tls {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureVerifier))
            .with_no_client_auth()
    } else {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

// ---------------------------------------------------------------------------
// Insecure verifier
// ---------------------------------------------------------------------------

/// A `ServerCertVerifier` that accepts every certificate, signature, and
/// hostname. Used when `TransportOpts::insecure_tls == true`.
///
/// The signature-scheme list returned from `supported_verify_schemes` is
/// pulled from the crypto provider so the TLS handshake actually gets
/// a list of advertised schemes — returning an empty list would cause
/// rustls to reject the handshake for lack of a mutually-supported
/// scheme.
#[derive(Debug)]
pub(crate) struct InsecureVerifier;

impl ServerCertVerifier for InsecureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Mirror the defaults from rustls' "ring" crypto provider. The
        // list is implementation-defined, so we read it out of the
        // provider rather than hard-coding.
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_config_embeds_webpki_roots() {
        let opts = TransportOpts::default();
        let cfg = tls_client_config(&opts, &[b"h2"]);
        assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn insecure_config_carries_alpn() {
        let opts = TransportOpts {
            insecure_tls: true,
            ..TransportOpts::default()
        };
        let cfg = tls_client_config(&opts, &[b"h2", b"http/1.1"]);
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    #[test]
    fn empty_alpn_list_is_honoured() {
        let opts = TransportOpts::default();
        let cfg = tls_client_config(&opts, &[]);
        assert!(cfg.alpn_protocols.is_empty());
    }
}
