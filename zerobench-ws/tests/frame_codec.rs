//! Integration tests for the RFC 6455 frame codec.
//!
//! Unit tests covering the common happy paths live in
//! `zerobench_ws::frame::tests`; this file exercises the round-trip
//! shape (client-encode → server-unmask → server-echo → client-decode)
//! plus the edge cases around extended payload lengths.

use zerobench_ws::frame::{decode_frame, encode_close, encode_frame, parse_close_payload, Opcode, WsError};

/// Simulate a server receiving a client frame: strip the MASK bit,
/// unmask the payload in place, and drop the 4-byte mask so
/// `decode_frame` (which rejects masked inputs) can parse it.
///
/// The real server side in `tests/ws_smoke.rs` does the same thing.
fn unmask_client_frame(frame: &[u8]) -> Vec<u8> {
    assert!(frame.len() >= 2, "frame must have a header");
    let short_len = frame[1] & 0x7f;
    let header_ext_bytes = match short_len {
        0..=125 => 0,
        126 => 2,
        127 => 8,
        _ => unreachable!(),
    };
    let header_total = 2 + header_ext_bytes;
    let mask = &frame[header_total..header_total + 4];

    let mut out = Vec::with_capacity(frame.len() - 4);
    out.extend_from_slice(&frame[..header_total]);
    // Clear MASK bit in byte 1.
    out[1] &= 0x7f;

    let payload_start = header_total + 4;
    let payload_end = frame.len();
    for (i, b) in frame[payload_start..payload_end].iter().enumerate() {
        out.push(b ^ mask[i & 3]);
    }
    out
}

#[test]
fn round_trip_short_text_frame() {
    // Client encodes → server unmasks → decode parses.
    let mut client_buf = Vec::new();
    encode_frame(Opcode::Text, b"hello", [0x12, 0x34, 0x56, 0x78], &mut client_buf);

    let server_view = unmask_client_frame(&client_buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.opcode, Opcode::Text);
    assert!(hdr.fin);
    assert_eq!(hdr.payload_len, 5);

    let payload = &server_view[hdr.payload_start..hdr.payload_start + hdr.payload_len];
    assert_eq!(payload, b"hello");
}

#[test]
fn round_trip_16_bit_length_payload() {
    let payload = vec![0xA5u8; 1000]; // triggers 126-length path
    let mut client_buf = Vec::new();
    encode_frame(Opcode::Binary, &payload, [0; 4], &mut client_buf);

    let server_view = unmask_client_frame(&client_buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.opcode, Opcode::Binary);
    assert_eq!(hdr.payload_len, 1000);
    assert_eq!(
        &server_view[hdr.payload_start..hdr.payload_start + hdr.payload_len],
        &payload[..]
    );
}

#[test]
fn round_trip_64_bit_length_payload() {
    // Cross the 2^16 threshold: 70_000 bytes exercises the 127-length
    // path. We keep the payload small enough that test memory stays
    // trivial.
    let payload = vec![0x33u8; 70_000];
    let mut client_buf = Vec::new();
    encode_frame(Opcode::Binary, &payload, [0x99, 0xAA, 0xBB, 0xCC], &mut client_buf);

    let server_view = unmask_client_frame(&client_buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.opcode, Opcode::Binary);
    assert_eq!(hdr.payload_len, 70_000);
    assert_eq!(
        &server_view[hdr.payload_start..hdr.payload_start + hdr.payload_len],
        &payload[..]
    );
}

/// RFC 6455 §5.1: a server-sent frame with MASK=1 is a protocol error.
/// We verify the decoder rejects it.
#[test]
fn decode_rejects_masked_server_frame() {
    let bad = [0x81u8, 0x82, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00];
    let err = decode_frame(&bad).unwrap_err();
    assert!(matches!(err, WsError::ServerFrameMasked));
}

/// Empty payload round-trip. Edge case — 0-length text frames are legal
/// and appear as "keepalive" pings in some WS libraries.
#[test]
fn round_trip_empty_payload() {
    let mut client_buf = Vec::new();
    encode_frame(Opcode::Text, b"", [0x01, 0x02, 0x03, 0x04], &mut client_buf);
    let server_view = unmask_client_frame(&client_buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.payload_len, 0);
}

/// Exactly 125 bytes stays in the short-length (7-bit) path.
#[test]
fn round_trip_125_byte_payload() {
    let payload = vec![0x42u8; 125];
    let mut client_buf = Vec::new();
    encode_frame(Opcode::Binary, &payload, [0; 4], &mut client_buf);
    // Header should be exactly 2 bytes + 4-byte mask (no extended len).
    assert_eq!(client_buf[1] & 0x7f, 125);
    let server_view = unmask_client_frame(&client_buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.payload_len, 125);
}

/// Ping with a 32-byte payload is a legal control frame (<= 125).
#[test]
fn round_trip_ping_payload() {
    let payload = b"keepalive-token-0123456789abcdef";
    let mut client_buf = Vec::new();
    encode_frame(Opcode::Ping, payload, [0xAA; 4], &mut client_buf);
    let server_view = unmask_client_frame(&client_buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.opcode, Opcode::Ping);
    assert_eq!(hdr.payload_len, payload.len());
    assert_eq!(
        &server_view[hdr.payload_start..hdr.payload_start + hdr.payload_len],
        payload,
    );
}

/// Close frame + reason round-trips through encode + parse.
#[test]
fn close_frame_round_trip() {
    let mut buf = Vec::new();
    encode_close(1000, "goodbye", [0; 4], &mut buf);
    let server_view = unmask_client_frame(&buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.opcode, Opcode::Close);
    let payload = &server_view[hdr.payload_start..hdr.payload_start + hdr.payload_len];
    let (code, reason) = parse_close_payload(payload);
    assert_eq!(code, 1000);
    assert_eq!(reason, "goodbye");
}

/// A too-long reason gets truncated to 123 bytes so the total close
/// payload (2 code + 123 reason) fits in the 125-byte control-frame cap.
#[test]
fn close_frame_truncates_long_reason() {
    let long = "x".repeat(500);
    let mut buf = Vec::new();
    encode_close(1002, &long, [0; 4], &mut buf);
    let server_view = unmask_client_frame(&buf);
    let hdr = decode_frame(&server_view).unwrap();
    assert_eq!(hdr.opcode, Opcode::Close);
    // 2-byte code + 123-byte reason = 125 bytes total.
    assert_eq!(hdr.payload_len, 125);
}
