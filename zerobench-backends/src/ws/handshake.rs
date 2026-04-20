//! RFC 6455 §4 HTTP/1.1 Upgrade handshake.
//!
//! Client-side Upgrade handshake — Sec-WebSocket-Key/Accept computation
//! and header validation. RFC 6455 §4.
//!
//! The client half is what we need: generate a Sec-WebSocket-Key, send
//! an Upgrade request, verify the server's 101 + Sec-WebSocket-Accept.
//! The GUID the spec mandates is the string
//! `258EAFA5-E914-47DA-95CA-C5AB0DC85B11` — we compute
//! `base64(SHA-1(key + GUID))` and compare byte-for-byte against the
//! server's `Sec-WebSocket-Accept` header.
//!
//! # What's not here
//!
//! - `Sec-WebSocket-Protocol` (subprotocol) negotiation. Scope note in
//!   the Task 15 plan — we don't advertise, we don't check.
//! - `Sec-WebSocket-Extensions` (permessage-deflate, etc.). Same.
//! - Cookie handling. The client doesn't read or set cookies.
//!
//! The CLI's `-H` flags are forwarded verbatim, so users who need those
//! fields can pass them as raw headers.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use rand::RngCore;
use sha1::{Digest, Sha1};

use zerobench_core::rng::BenchRng;
use zerobench_core::transport::Target;

/// The constant GUID per RFC 6455 §4.2.2 — concatenated with the client
/// key before SHA-1 to produce the server's Accept value. Verified
/// against the RFC text on authorship; a single wrong character here
/// breaks every handshake.
pub(crate) const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Error surface for the handshake layer.
///
/// Kept narrow: the connection layer's `WsError` wraps an `UpgradeFailed`
/// with a short string when any of these fire, so the reporter has a
/// single "bucket" error to count.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HandshakeError {
    /// Server returned a non-101 status line. We carry the status code
    /// so logs can tell "400 Bad Request" from "401 Unauthorized" from
    /// "503 Service Unavailable".
    #[error("server returned {0} (want 101)")]
    NotSwitching(u16),

    /// The `Upgrade` or `Connection` header was absent or had the wrong
    /// token. RFC 6455 §4.1 requires them case-insensitively.
    #[error("missing or malformed {0} header")]
    MissingHeader(&'static str),

    /// The `Sec-WebSocket-Accept` header's value didn't match
    /// `base64(SHA-1(key + GUID))`. Indicates either a hostile server
    /// or a broken intermediary.
    #[error("sec-websocket-accept mismatch")]
    AcceptMismatch,

    /// httparse couldn't parse the status line / headers — the server
    /// sent malformed HTTP, or we're talking to something that isn't
    /// an HTTP server.
    #[error("unparseable response: {0}")]
    UnparseableResponse(String),

    /// Response fit neither "done" nor "needs more bytes" — a catch-all
    /// for the rare cases (e.g. status line >4 KiB) where the input is
    /// syntactically HTTP but not recognisable.
    #[error("response header limit exceeded")]
    HeadersTooBig,
}

/// Generate a fresh 16-byte client nonce and its base64 representation.
///
/// The nonce is random (CSPRNG); the base64 form is what we send in the
/// `Sec-WebSocket-Key` request header and what we'll hash to derive the
/// expected server Accept value.
pub fn generate_key(rng: &mut BenchRng) -> (String, [u8; 16]) {
    let mut key_bytes = [0u8; 16];
    rng.fill_bytes(&mut key_bytes);
    let key_b64 = B64.encode(key_bytes);
    (key_b64, key_bytes)
}

/// Compute the expected value of `Sec-WebSocket-Accept` for a given
/// `Sec-WebSocket-Key`.
///
/// Per RFC 6455 §4.2.2: `base64(SHA-1(key + GUID))`. The spec gives a
/// fixed test vector we assert against below so a regression in base64
/// or SHA-1 crate behaviour surfaces immediately.
pub fn compute_accept(key_b64: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key_b64.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let digest = hasher.finalize();
    B64.encode(digest)
}

/// Build the client's HTTP/1.1 Upgrade request bytes.
///
/// Ordering matches the RFC's example: request-line, Host, Upgrade,
/// Connection, Sec-WebSocket-Key, Sec-WebSocket-Version, user headers,
/// terminating CRLF.
///
/// User-supplied headers from the CLI's `-H` flags are appended last so
/// they can override the defaults (e.g. a custom `User-Agent` or
/// `Origin`). We don't attempt to dedup Host/Upgrade/Connection/Version
/// — if the user passes a conflicting header they get what they asked
/// for; that's curl-style behaviour and matches the rest of the CLI.
pub fn build_request(
    target: &Target,
    path: &str,
    key_b64: &str,
    extra_headers: &[(String, String)],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);

    // Request line.
    out.extend_from_slice(b"GET ");
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");

    // Host.
    out.extend_from_slice(b"Host: ");
    out.extend_from_slice(target.addr().as_bytes());
    out.extend_from_slice(b"\r\n");

    // Upgrade + Connection.
    out.extend_from_slice(b"Upgrade: websocket\r\nConnection: Upgrade\r\n");

    // Sec-WebSocket-Key + Version.
    out.extend_from_slice(b"Sec-WebSocket-Key: ");
    out.extend_from_slice(key_b64.as_bytes());
    out.extend_from_slice(b"\r\nSec-WebSocket-Version: 13\r\n");

    // User headers.
    for (name, value) in extra_headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    // Terminator.
    out.extend_from_slice(b"\r\n");
    out
}

/// Validate a parsed server response against the `Sec-WebSocket-Key`
/// we sent earlier.
///
/// Checks, in order:
/// 1. Status code is 101.
/// 2. `Upgrade` header exists and contains (case-insensitively)
///    `websocket`.
/// 3. `Connection` header exists and contains (case-insensitively)
///    `upgrade`.
/// 4. `Sec-WebSocket-Accept` equals `base64(SHA-1(key + GUID))`.
///
/// The `headers` slice is whatever httparse gave us — the caller has
/// already decided the response's header section is complete (saw
/// `\r\n\r\n`).
pub fn validate_response(
    status: u16,
    headers: &[httparse::Header<'_>],
    sent_key_b64: &str,
) -> Result<(), HandshakeError> {
    if status != 101 {
        return Err(HandshakeError::NotSwitching(status));
    }

    let expected_accept = compute_accept(sent_key_b64);

    let mut has_upgrade = false;
    let mut has_connection = false;
    let mut accept_val: Option<&[u8]> = None;

    for h in headers {
        // httparse gives us `name` as a `&str` and `value` as `&[u8]`.
        // Names are case-insensitive per RFC 7230 §3.2.
        let name = h.name;
        if name.eq_ignore_ascii_case("upgrade") {
            let v = std::str::from_utf8(h.value).unwrap_or("");
            // Upgrade: websocket
            if v.split(',').any(|tok| tok.trim().eq_ignore_ascii_case("websocket")) {
                has_upgrade = true;
            }
        } else if name.eq_ignore_ascii_case("connection") {
            let v = std::str::from_utf8(h.value).unwrap_or("");
            // Connection: Upgrade (may be a comma-separated token list)
            if v.split(',').any(|tok| tok.trim().eq_ignore_ascii_case("upgrade")) {
                has_connection = true;
            }
        } else if name.eq_ignore_ascii_case("sec-websocket-accept") {
            accept_val = Some(h.value);
        }
    }

    if !has_upgrade {
        return Err(HandshakeError::MissingHeader("Upgrade"));
    }
    if !has_connection {
        return Err(HandshakeError::MissingHeader("Connection"));
    }

    let got = match accept_val {
        Some(v) => std::str::from_utf8(v).unwrap_or("").trim(),
        None => return Err(HandshakeError::MissingHeader("Sec-WebSocket-Accept")),
    };
    if got != expected_accept {
        return Err(HandshakeError::AcceptMismatch);
    }
    Ok(())
}

/// Find the byte offset of `"\r\n\r\n"` + 4 in `buf`. Returns `None`
/// when the header terminator hasn't arrived yet.
///
/// Shared with the connection layer's read loop: we need to know where
/// the HTTP response ends and raw WebSocket frames begin so any bytes
/// the server already queued behind the 101 carry over into the recv
/// buffer.
pub fn find_headers_end(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6455 §1.3 test vector: key `dGhlIHNhbXBsZSBub25jZQ==` →
    /// `s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`. If this ever fails either the
    /// base64 crate changed behaviour or someone introduced a bug in
    /// `compute_accept`.
    #[test]
    fn rfc6455_accept_test_vector() {
        let got = compute_accept("dGhlIHNhbXBsZSBub25jZQ==");
        assert_eq!(got, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    /// Well-formed 101 response passes validation.
    #[test]
    fn validate_successful_handshake() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        let headers = [
            httparse::Header {
                name: "Upgrade",
                value: b"websocket",
            },
            httparse::Header {
                name: "Connection",
                value: b"Upgrade",
            },
            httparse::Header {
                name: "Sec-WebSocket-Accept",
                value: accept.as_bytes(),
            },
        ];
        assert!(validate_response(101, &headers, key).is_ok());
    }

    /// Missing Upgrade header is a hard error. We pick this up before
    /// even touching the accept value.
    #[test]
    fn validate_missing_upgrade_header() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        let headers = [
            httparse::Header {
                name: "Connection",
                value: b"Upgrade",
            },
            httparse::Header {
                name: "Sec-WebSocket-Accept",
                value: accept.as_bytes(),
            },
        ];
        assert_eq!(
            validate_response(101, &headers, key).unwrap_err(),
            HandshakeError::MissingHeader("Upgrade"),
        );
    }

    /// Missing Connection header.
    #[test]
    fn validate_missing_connection_header() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        let headers = [
            httparse::Header {
                name: "Upgrade",
                value: b"websocket",
            },
            httparse::Header {
                name: "Sec-WebSocket-Accept",
                value: accept.as_bytes(),
            },
        ];
        assert_eq!(
            validate_response(101, &headers, key).unwrap_err(),
            HandshakeError::MissingHeader("Connection"),
        );
    }

    /// Missing Sec-WebSocket-Accept.
    #[test]
    fn validate_missing_accept_header() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let headers = [
            httparse::Header {
                name: "Upgrade",
                value: b"websocket",
            },
            httparse::Header {
                name: "Connection",
                value: b"Upgrade",
            },
        ];
        assert_eq!(
            validate_response(101, &headers, key).unwrap_err(),
            HandshakeError::MissingHeader("Sec-WebSocket-Accept"),
        );
    }

    /// A wrong Accept value must be rejected as a mismatch.
    #[test]
    fn validate_wrong_accept_value() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let headers = [
            httparse::Header {
                name: "Upgrade",
                value: b"websocket",
            },
            httparse::Header {
                name: "Connection",
                value: b"Upgrade",
            },
            httparse::Header {
                name: "Sec-WebSocket-Accept",
                value: b"bogus",
            },
        ];
        assert_eq!(
            validate_response(101, &headers, key).unwrap_err(),
            HandshakeError::AcceptMismatch,
        );
    }

    /// Non-101 status is immediately rejected.
    #[test]
    fn validate_non_101_status() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let headers = [];
        assert_eq!(
            validate_response(200, &headers, key).unwrap_err(),
            HandshakeError::NotSwitching(200),
        );
    }

    /// Headers are matched case-insensitively (per RFC 7230 §3.2).
    #[test]
    fn header_name_case_insensitive() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        let headers = [
            httparse::Header {
                name: "UPGRADE",
                value: b"WebSocket",
            },
            httparse::Header {
                name: "connection",
                value: b"upgrade",
            },
            httparse::Header {
                name: "Sec-WebSocket-ACCEPT",
                value: accept.as_bytes(),
            },
        ];
        assert!(validate_response(101, &headers, key).is_ok());
    }

    /// `Connection: keep-alive, Upgrade` — multi-token value is fine.
    #[test]
    fn connection_header_can_have_multiple_tokens() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        let headers = [
            httparse::Header {
                name: "Upgrade",
                value: b"websocket",
            },
            httparse::Header {
                name: "Connection",
                value: b"keep-alive, Upgrade",
            },
            httparse::Header {
                name: "Sec-WebSocket-Accept",
                value: accept.as_bytes(),
            },
        ];
        assert!(validate_response(101, &headers, key).is_ok());
    }

    /// The built request includes the required headers in the RFC order.
    #[test]
    fn build_request_shape() {
        let target = Target::parse("ws://example.com:8080").unwrap();
        let req = build_request(
            &target,
            "/chat",
            "dGhlIHNhbXBsZSBub25jZQ==",
            &[("Origin".to_string(), "https://x".to_string())],
        );
        let s = std::str::from_utf8(&req).unwrap();
        assert!(s.starts_with("GET /chat HTTP/1.1\r\n"));
        assert!(s.contains("Host: example.com:8080\r\n"));
        assert!(s.contains("Upgrade: websocket\r\n"));
        assert!(s.contains("Connection: Upgrade\r\n"));
        assert!(s.contains("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"));
        assert!(s.contains("Sec-WebSocket-Version: 13\r\n"));
        assert!(s.contains("Origin: https://x\r\n"));
        assert!(s.ends_with("\r\n\r\n"));
    }

    /// find_headers_end returns the offset just past the terminator.
    #[test]
    fn find_headers_end_basic() {
        let buf = b"HTTP/1.1 101 Switching Protocols\r\n\r\nleftover";
        let end = find_headers_end(buf).unwrap();
        assert_eq!(&buf[end..], b"leftover");
    }

    /// Incomplete terminator → None.
    #[test]
    fn find_headers_end_incomplete() {
        let buf = b"HTTP/1.1 101 Switching Protocols\r\n";
        assert!(find_headers_end(buf).is_none());
    }

    /// Generated keys round-trip through base64 to 16-byte nonces.
    #[test]
    fn generate_key_produces_16_byte_nonce() {
        let mut rng = zerobench_core::rng::from_seed(0xDEAD_BEEF);
        let (key_b64, bytes) = generate_key(&mut rng);
        let decoded = B64.decode(&key_b64).unwrap();
        assert_eq!(decoded.len(), 16);
        assert_eq!(decoded, bytes);
    }

    /// Two consecutive keys should differ (CSPRNG, not a fixed counter).
    #[test]
    fn generate_key_produces_distinct_values() {
        let mut rng = zerobench_core::rng::from_entropy();
        let (a, _) = generate_key(&mut rng);
        let (b, _) = generate_key(&mut rng);
        assert_ne!(a, b);
    }
}
