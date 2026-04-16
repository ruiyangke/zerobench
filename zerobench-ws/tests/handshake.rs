//! Integration tests for the HTTP/1.1 Upgrade handshake.
//!
//! Unit tests cover the happy paths against a hand-built headers array;
//! this file runs the full `build_request` → hypothetical-server → parse
//! → validate pipeline, and also pins the RFC 6455 §1.3 test vector so
//! any regression in the SHA-1 / base64 crates surfaces here.

use zerobench_ws::handshake::{
    build_request, compute_accept, find_headers_end, generate_key, validate_response,
    HandshakeError,
};
use zerobench_core::rng::from_seed;
use zerobench_core::transport::Target;

/// RFC 6455 §1.3 test vector — keep a dedicated test so it's
/// unmistakeably tied to the spec source.
#[test]
fn accept_test_vector_matches_rfc() {
    assert_eq!(
        compute_accept("dGhlIHNhbXBsZSBub25jZQ=="),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
    );
}

/// build_request should emit deterministic bytes for a given key + path.
/// We hard-code the key so the output is reproducible.
#[test]
fn request_bytes_shape() {
    let target = Target::parse("ws://example.com:8080/ignored").unwrap();
    let req = build_request(&target, "/chat", "dGhlIHNhbXBsZSBub25jZQ==", &[]);
    let as_str = std::str::from_utf8(&req).unwrap();

    assert!(as_str.starts_with("GET /chat HTTP/1.1\r\n"));
    assert!(as_str.contains("Host: example.com:8080\r\n"));
    assert!(as_str.contains("Upgrade: websocket\r\n"));
    assert!(as_str.contains("Connection: Upgrade\r\n"));
    assert!(as_str.contains("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"));
    assert!(as_str.contains("Sec-WebSocket-Version: 13\r\n"));
    assert!(as_str.ends_with("\r\n\r\n"));
}

/// `-H` flags are forwarded verbatim.
#[test]
fn request_includes_extra_headers() {
    let target = Target::parse("ws://host").unwrap();
    let req = build_request(
        &target,
        "/",
        "dGhlIHNhbXBsZSBub25jZQ==",
        &[
            ("Origin".to_string(), "https://example.com".to_string()),
            ("Authorization".to_string(), "Bearer token-123".to_string()),
        ],
    );
    let s = std::str::from_utf8(&req).unwrap();
    assert!(s.contains("Origin: https://example.com\r\n"));
    assert!(s.contains("Authorization: Bearer token-123\r\n"));
}

/// Verify we correctly validate a 101 response against a key.
#[test]
fn validate_complete_101_response() {
    let key = "dGhlIHNhbXBsZSBub25jZQ==";
    let accept = compute_accept(key);
    let headers = vec![
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

/// A 400 Bad Request fails with the status embedded in the error.
#[test]
fn non_101_rejects_with_status() {
    let headers = [];
    let err = validate_response(400, &headers, "some-key").unwrap_err();
    assert_eq!(err, HandshakeError::NotSwitching(400));
}

/// Mangled accept value → AcceptMismatch.
#[test]
fn bad_accept_value_rejected() {
    let key = "dGhlIHNhbXBsZSBub25jZQ==";
    let headers = vec![
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
            value: b"wrong-base64==",
        },
    ];
    assert_eq!(
        validate_response(101, &headers, key).unwrap_err(),
        HandshakeError::AcceptMismatch,
    );
}

/// Missing Upgrade header → we surface which header is missing for
/// debuggability.
#[test]
fn missing_upgrade_surfaces_specific_error() {
    let key = "dGhlIHNhbXBsZSBub25jZQ==";
    let accept = compute_accept(key);
    let headers = vec![
        httparse::Header {
            name: "Connection",
            value: b"Upgrade",
        },
        httparse::Header {
            name: "Sec-WebSocket-Accept",
            value: accept.as_bytes(),
        },
    ];
    match validate_response(101, &headers, key).unwrap_err() {
        HandshakeError::MissingHeader(name) => assert_eq!(name, "Upgrade"),
        other => panic!("expected MissingHeader(Upgrade), got {other:?}"),
    }
}

/// `find_headers_end` locates `\r\n\r\n`.
#[test]
fn find_headers_end_at_expected_offset() {
    let body = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\nREST_OF_DATA";
    let end = find_headers_end(body).unwrap();
    assert_eq!(&body[end..], b"REST_OF_DATA");
}

/// Keys are 16 bytes of randomness, base64-encoded to 24 chars (always
/// with `=` padding since 16 isn't a multiple of 3).
#[test]
fn generated_keys_have_correct_shape() {
    use base64::Engine as _;
    let mut rng = from_seed(42);
    let (k, bytes) = generate_key(&mut rng);
    assert_eq!(bytes.len(), 16);
    // 24 chars including padding.
    assert_eq!(k.len(), 24);
    assert!(k.ends_with('='));
    // Base64 decodes back to our 16 bytes.
    let decoded = base64::engine::general_purpose::STANDARD.decode(&k).unwrap();
    assert_eq!(decoded, bytes);
}

/// Two keys from the same RNG stream must differ (the key function
/// advances the RNG internally).
#[test]
fn generated_keys_are_distinct() {
    let mut rng = from_seed(42);
    let (a, _) = generate_key(&mut rng);
    let (b, _) = generate_key(&mut rng);
    assert_ne!(a, b);
}

/// End-to-end: build a request → have a fake "server" compute the
/// accept value → validate. Mirrors the real handshake path minus the
/// socket IO.
#[test]
fn end_to_end_key_to_accept_validation() {
    let mut rng = from_seed(0xCAFE_F00D);
    let (key_b64, _) = generate_key(&mut rng);
    let target = Target::parse("ws://example.com").unwrap();
    let req = build_request(&target, "/", &key_b64, &[]);

    // Pull the key out of the request — the server would do this via
    // its HTTP parser.
    let req_str = std::str::from_utf8(&req).unwrap();
    let sent_key = req_str
        .lines()
        .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
        .expect("Sec-WebSocket-Key in request");
    assert_eq!(sent_key, key_b64);

    // Compute what the server should respond with.
    let accept = compute_accept(sent_key);

    // Server reply headers → validate against our original key.
    let headers = vec![
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
    validate_response(101, &headers, &key_b64).expect("handshake must validate");
}
