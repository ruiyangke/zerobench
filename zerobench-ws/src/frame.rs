//! RFC 6455 §5.2 frame codec.
//!
//! This module implements the wire format for WebSocket frames — the
//! client-side view, with two thin APIs:
//!
//! - [`encode_frame`] — produce a masked client frame ready to write on
//!   the socket. Always FIN=1 (we don't emit fragmented frames from the
//!   client), always MASK=1 (RFC 6455 §5.3 mandates masking for
//!   client→server traffic).
//! - [`decode_frame`] — parse one frame from a buffer. Returns
//!   [`FrameHeader`] + payload range + the total bytes consumed so the
//!   caller can advance its read buffer.
//!
//! ## Masks
//!
//! Masks are 4 bytes of CSPRNG output (see [`crate::conn::WsConnection`]
//! for where the RNG is seeded). RFC 6455 §10.3 — the mask defends
//! against cache-poisoning attacks that exploit intermediaries which
//! confuse WebSocket traffic for HTTP. A weak mask (e.g. a counter) lets
//! an attacker predict future mask values and craft bytes that will
//! XOR into valid HTTP when seen by a naive proxy. We use Xoshiro256++
//! seeded from OS entropy (via `BenchRng::from_entropy`) which is more
//! than enough for this defence.
//!
//! ## What's not here
//!
//! - Fragmentation on the send side. We only emit FIN=1 frames.
//! - `permessage-deflate` extension. No RSV bits ever set.
//! - Close-code framing sugar. [`encode_close`] builds the payload, but
//!   it's still a regular binary-ish frame with opcode 8; the high-level
//!   close semantics (echo server's close, etc.) live in [`crate::conn`].

use bytes::BytesMut;

/// RFC 6455 §5.2 opcode values. We treat the enum as authoritative; any
/// opcode byte outside this set surfaces as [`FrameError::ProtocolOther`]
/// at decode time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    /// A continuation of a fragmented message.
    Continuation = 0x0,
    /// A UTF-8 text frame.
    Text = 0x1,
    /// A binary frame.
    Binary = 0x2,
    /// A connection-close frame (may carry a 2-byte code + UTF-8 reason).
    Close = 0x8,
    /// An unsolicited heartbeat — server asks us to reply with Pong.
    Ping = 0x9,
    /// A reply to a Ping we sent, or an unsolicited keepalive.
    Pong = 0xA,
}

impl Opcode {
    /// Decode a raw 4-bit opcode. Returns `None` for unassigned opcodes
    /// (0x3..=0x7 and 0xB..=0xF per the IANA registry).
    pub fn from_bits(b: u8) -> Option<Self> {
        Some(match b & 0x0f {
            0x0 => Opcode::Continuation,
            0x1 => Opcode::Text,
            0x2 => Opcode::Binary,
            0x8 => Opcode::Close,
            0x9 => Opcode::Ping,
            0xA => Opcode::Pong,
            _ => return None,
        })
    }

    /// The 4-bit wire value for this opcode.
    pub fn bits(self) -> u8 {
        self as u8
    }

    /// Control frames (opcode 0x8..=0xF) MUST NOT be fragmented and MUST
    /// have a payload ≤ 125 bytes (RFC 6455 §5.4 + §5.5). We use this
    /// check on decode so a misbehaving server is caught early.
    pub fn is_control(self) -> bool {
        matches!(self, Opcode::Close | Opcode::Ping | Opcode::Pong)
    }
}

/// The header + metadata of a decoded frame.
///
/// Kept separate from the payload so [`decode_frame`] can report
/// "this frame would take N bytes, but the buffer is only M" without
/// allocating — the caller keeps reading and re-calls.
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    /// True when this is the final fragment of a message.
    pub fin: bool,
    /// The opcode — continuation, text, binary, close, ping, or pong.
    pub opcode: Opcode,
    /// Length of the payload in bytes.
    pub payload_len: usize,
    /// Byte offset in the source buffer where the payload begins.
    pub payload_start: usize,
    /// Total bytes consumed by this frame (header + mask + payload).
    pub total_len: usize,
}

/// Errors produced by [`decode_frame`] and [`encode_frame`].
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// Not enough bytes in the buffer to decode a whole frame yet.
    /// Non-fatal — caller should read more and retry.
    #[error("need more data ({needed} bytes)")]
    NeedMore {
        /// A lower bound on additional bytes required before `decode_frame`
        /// could possibly succeed. Callers may read more than this.
        needed: usize,
    },

    /// An RSV bit was set but we didn't negotiate an extension. RFC 6455
    /// §5.2: the receiver MUST fail the connection in this case.
    #[error("reserved bit set (RSV={0:03b})")]
    ReservedBitSet(u8),

    /// The opcode byte was outside the assigned range.
    #[error("unknown opcode: 0x{0:x}")]
    UnknownOpcode(u8),

    /// A server-sent frame had the MASK bit set. RFC 6455 §5.1:
    /// "a server MUST NOT mask any frames that it sends to the client".
    #[error("server frame was masked")]
    ServerFrameMasked,

    /// Control frame with payload length > 125. RFC 6455 §5.5.
    #[error("control frame too large: {0} bytes")]
    ControlFrameTooLarge(usize),

    /// Control frame with FIN=0 (control frames MUST NOT be fragmented).
    #[error("fragmented control frame")]
    FragmentedControlFrame,

    /// The frame's declared payload length exceeded a sanity limit. We
    /// cap at 64 MiB to avoid absurd allocations from a malicious server.
    #[error("payload too large: {0} bytes (cap 64 MiB)")]
    PayloadTooLarge(usize),

    /// Generic protocol-level error — message / reason in the string.
    /// Used for cases where we want a specific, human-readable error
    /// rather than a fixed enum variant.
    #[error("protocol error: {0}")]
    ProtocolOther(String),
}

/// Hard upper bound on accepted frame payload. WebSocket allows up to
/// 2^63 bytes, but accepting a "here's an 8 EiB frame" header would let
/// a server kill us via `BytesMut::reserve`. 64 MiB is larger than any
/// realistic message (LLM tokens, chat payloads) and prevents pathology.
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

/// Encode a client frame.
///
/// Always emits FIN=1, RSV=0, MASK=1. The mask is four raw bytes
/// (typically from a CSPRNG — see module-level doc). The caller is
/// responsible for supplying an opcode that makes sense (Text, Binary,
/// Ping, Pong, Close). Continuation is legal per the spec but we don't
/// fragment on send, so callers should never pass it.
///
/// Returns the byte count written. The output buffer is *appended to*,
/// not overwritten, so callers can batch multiple frames if they want.
pub fn encode_frame(
    opcode: Opcode,
    payload: &[u8],
    mask: [u8; 4],
    out: &mut Vec<u8>,
) -> usize {
    let len = payload.len();
    let start = out.len();

    // Byte 0: FIN=1, RSV=0, opcode.
    out.push(0x80 | opcode.bits());

    // Byte 1+: MASK=1 and the length field.
    if len < 126 {
        out.push(0x80 | (len as u8));
    } else if len < 65536 {
        out.push(0x80 | 126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0x80 | 127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }

    // Masking key (4 bytes).
    out.extend_from_slice(&mask);

    // Masked payload — `byte[i] XOR mask[i % 4]`.
    //
    // We XOR byte-by-byte. A 4-byte-at-a-time implementation could halve
    // CPU time on large payloads, but for a bench tool whose payloads
    // are typically <1 KiB "ping" messages the win isn't worth the
    // unsafe / alignment handling it would bring.
    out.reserve(len);
    for (i, &b) in payload.iter().enumerate() {
        out.push(b ^ mask[i & 3]);
    }

    out.len() - start
}

/// Encode a Close frame with a 2-byte status code + UTF-8 reason.
///
/// The status code is prepended to the reason; together they form the
/// Close frame's payload per RFC 6455 §5.5.1. `code = 1000` (normal
/// closure) is the happy path.
pub fn encode_close(code: u16, reason: &str, mask: [u8; 4], out: &mut Vec<u8>) -> usize {
    // 2-byte code + up to 123 bytes of UTF-8 reason = 125 byte ceiling
    // for a Close frame (it's a control frame). We truncate rather than
    // erroring — callers rarely check the return, and a bench tool
    // sending a 200-byte reason is a bug we should hide gracefully.
    let reason_bytes = reason.as_bytes();
    let reason_len = reason_bytes.len().min(123);

    let mut payload: BytesMut = BytesMut::with_capacity(2 + reason_len);
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(&reason_bytes[..reason_len]);

    encode_frame(Opcode::Close, &payload, mask, out)
}

/// Decode one frame from `buf`.
///
/// On success, returns a [`FrameHeader`] describing the frame's shape.
/// The actual payload lives at `buf[hdr.payload_start..hdr.payload_start
/// + hdr.payload_len]`.
///
/// Server frames MUST NOT be masked (RFC 6455 §5.1); if the MASK bit is
/// set on input this returns [`FrameError::ServerFrameMasked`].
pub fn decode_frame(buf: &[u8]) -> Result<FrameHeader, FrameError> {
    // Every frame is at least 2 bytes (header + length byte).
    if buf.len() < 2 {
        return Err(FrameError::NeedMore {
            needed: 2 - buf.len(),
        });
    }

    let b0 = buf[0];
    let b1 = buf[1];

    let fin = (b0 & 0x80) != 0;
    let rsv = (b0 & 0x70) >> 4;
    if rsv != 0 {
        return Err(FrameError::ReservedBitSet(rsv));
    }

    let opcode = Opcode::from_bits(b0).ok_or(FrameError::UnknownOpcode(b0 & 0x0f))?;

    let masked = (b1 & 0x80) != 0;
    if masked {
        return Err(FrameError::ServerFrameMasked);
    }

    let short_len = (b1 & 0x7f) as u64;
    let (payload_len, header_len) = match short_len {
        0..=125 => (short_len as usize, 2usize),
        126 => {
            if buf.len() < 4 {
                return Err(FrameError::NeedMore {
                    needed: 4 - buf.len(),
                });
            }
            let n = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            (n, 4)
        }
        127 => {
            if buf.len() < 10 {
                return Err(FrameError::NeedMore {
                    needed: 10 - buf.len(),
                });
            }
            let n = u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]);
            // RFC 6455 §5.2: the 64-bit length's most-significant bit
            // MUST be 0. That's enforced by the `> MAX_PAYLOAD` check
            // below since MAX_PAYLOAD (64 MiB) is far below 2^63.
            if n > MAX_PAYLOAD as u64 {
                return Err(FrameError::PayloadTooLarge(n as usize));
            }
            (n as usize, 10)
        }
        _ => unreachable!("7-bit length can't exceed 127"),
    };

    if opcode.is_control() {
        if !fin {
            return Err(FrameError::FragmentedControlFrame);
        }
        if payload_len > 125 {
            return Err(FrameError::ControlFrameTooLarge(payload_len));
        }
    }

    // Server frames are unmasked, so no masking-key bytes after the
    // length field — the payload starts immediately.
    let payload_start = header_len;
    let total_len = payload_start + payload_len;

    if buf.len() < total_len {
        return Err(FrameError::NeedMore {
            needed: total_len - buf.len(),
        });
    }

    Ok(FrameHeader {
        fin,
        opcode,
        payload_len,
        payload_start,
        total_len,
    })
}

/// Parse a Close frame's payload into `(code, reason)`.
///
/// Per RFC 6455 §5.5.1: an empty payload is legal and equivalent to
/// code=1005 (no code present). A 1-byte payload is malformed per the
/// spec but we tolerate it as "no code" to keep the bench tool robust
/// against buggy servers.
pub fn parse_close_payload(payload: &[u8]) -> (u16, String) {
    if payload.len() < 2 {
        return (1005, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    // Reason is SHOULD be UTF-8 per the spec. If the server sent
    // non-UTF-8, use the lossy decode rather than erroring — we've
    // already seen the Close, the connection is going away anyway.
    let reason = String::from_utf8_lossy(&payload[2..]).into_owned();
    (code, reason)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The "most basic" encode: a 5-byte text payload.
    #[test]
    fn encode_text_hello() {
        let mut out = Vec::new();
        let mask = [0xA1, 0xB2, 0xC3, 0xD4];
        let n = encode_frame(Opcode::Text, b"hello", mask, &mut out);

        // 2-byte header + 4-byte mask + 5-byte payload
        assert_eq!(n, 11);
        assert_eq!(out.len(), 11);

        // Byte 0: FIN=1 | RSV=0 | opcode=1 (text) → 0x81
        assert_eq!(out[0], 0x81);
        // Byte 1: MASK=1 | len=5 → 0x85
        assert_eq!(out[1], 0x85);
        // Bytes 2..6: the mask
        assert_eq!(&out[2..6], &mask);
        // Bytes 6..11: payload XOR mask
        let expected: Vec<u8> =
            b"hello".iter().enumerate().map(|(i, b)| b ^ mask[i & 3]).collect();
        assert_eq!(&out[6..11], expected.as_slice());
    }

    /// 126-byte payload uses the 16-bit extended length.
    #[test]
    fn encode_extended_16bit_length() {
        let payload = vec![0xAB; 126];
        let mut out = Vec::new();
        encode_frame(Opcode::Binary, &payload, [0; 4], &mut out);

        // 2 header + 2 length + 4 mask + 126 payload = 134
        assert_eq!(out.len(), 134);
        // Byte 0: FIN + Binary opcode
        assert_eq!(out[0], 0x82);
        // Byte 1: MASK=1 | len=126
        assert_eq!(out[1], 0x80 | 126);
        // Bytes 2..4: 16-bit length in big-endian
        assert_eq!(&out[2..4], &(126u16).to_be_bytes());
    }

    /// 70_000-byte payload uses the 64-bit extended length.
    #[test]
    fn encode_extended_64bit_length() {
        let payload = vec![0x11; 70_000];
        let mut out = Vec::new();
        encode_frame(Opcode::Binary, &payload, [0; 4], &mut out);

        // 2 header + 8 length + 4 mask + 70_000 payload = 70_014
        assert_eq!(out.len(), 70_014);
        assert_eq!(out[1], 0x80 | 127);
        assert_eq!(&out[2..10], &(70_000u64).to_be_bytes());
    }

    /// Round-trip: encode a client frame, server-side unmask it, verify
    /// we recover the original payload.
    #[test]
    fn round_trip_text_frame() {
        // Encode a client frame.
        let mut client_out = Vec::new();
        let mask = [0x01, 0x02, 0x03, 0x04];
        encode_frame(Opcode::Text, b"hello world", mask, &mut client_out);

        // Server would unmask — we manually replicate here. Since
        // `decode_frame` rejects masked input (per RFC 6455 §5.1's
        // "server MUST NOT mask"), we can't decode a client frame
        // directly; we simulate a server that has unmasked it first.
        let mut server_view = client_out.clone();
        // Byte 1 has the MASK bit set; strip it and drop the 4-byte
        // mask. Payload starts at byte 2 + 4 (mask) = 6.
        server_view[1] &= 0x7f;
        let mask_slice = [
            server_view[2], server_view[3], server_view[4], server_view[5],
        ];
        // Unmask payload in place:
        for (i, b) in server_view[6..].iter_mut().enumerate() {
            *b ^= mask_slice[i & 3];
        }
        // Rebuild the "server" frame without the 4 mask bytes.
        let mut unmasked = Vec::with_capacity(server_view.len() - 4);
        unmasked.extend_from_slice(&server_view[..2]); // header
        unmasked.extend_from_slice(&server_view[6..]); // payload

        let hdr = decode_frame(&unmasked).unwrap();
        assert_eq!(hdr.opcode, Opcode::Text);
        assert!(hdr.fin);
        assert_eq!(hdr.payload_len, 11);
        let payload = &unmasked[hdr.payload_start..hdr.payload_start + hdr.payload_len];
        assert_eq!(payload, b"hello world");
    }

    /// A masked server frame is a protocol error — server MUST NOT
    /// mask. We should reject with `ServerFrameMasked` rather than
    /// silently accepting.
    #[test]
    fn decode_rejects_masked_server_frame() {
        // Bytes: FIN+text | MASK+len=2 | mask(4) | payload(2)
        let bad = [0x81u8, 0x82, 0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x00];
        let err = decode_frame(&bad).unwrap_err();
        assert!(matches!(err, FrameError::ServerFrameMasked));
    }

    /// Short buffer → NeedMore with a correct "bytes remaining" hint.
    #[test]
    fn decode_need_more_for_header() {
        let err = decode_frame(&[0x81]).unwrap_err();
        match err {
            FrameError::NeedMore { needed } => assert_eq!(needed, 1),
            _ => panic!("expected NeedMore"),
        }
    }

    /// 16-bit extended length requires 4 header bytes; short buffer →
    /// NeedMore.
    #[test]
    fn decode_need_more_for_16bit_length() {
        // FIN+text, len=126, partial header
        let err = decode_frame(&[0x81, 0x7E, 0x00]).unwrap_err();
        match err {
            FrameError::NeedMore { needed } => assert_eq!(needed, 1),
            _ => panic!("expected NeedMore"),
        }
    }

    /// 64-bit extended length requires 10 header bytes.
    #[test]
    fn decode_need_more_for_64bit_length() {
        // FIN+text, len=127, no payload bytes yet
        let err = decode_frame(&[0x81, 0x7F, 0, 0, 0, 0]).unwrap_err();
        match err {
            FrameError::NeedMore { needed } => assert_eq!(needed, 4),
            _ => panic!("expected NeedMore"),
        }
    }

    /// Ping + small payload decoded correctly as a control frame.
    #[test]
    fn decode_ping() {
        // Server Ping with payload "hi"
        let frame = [0x89u8, 0x02, b'h', b'i'];
        let hdr = decode_frame(&frame).unwrap();
        assert_eq!(hdr.opcode, Opcode::Ping);
        assert!(hdr.fin);
        assert_eq!(hdr.payload_len, 2);
        assert_eq!(&frame[hdr.payload_start..][..2], b"hi");
    }

    /// Close frame with code 1000 + reason "bye".
    #[test]
    fn decode_close_payload() {
        // Server Close: FIN=1, opcode=8, len=5, code=1000 (big-endian), "bye"
        let code: u16 = 1000;
        let mut frame = vec![0x88, 0x05];
        frame.extend_from_slice(&code.to_be_bytes());
        frame.extend_from_slice(b"bye");
        let hdr = decode_frame(&frame).unwrap();
        assert_eq!(hdr.opcode, Opcode::Close);
        let payload = &frame[hdr.payload_start..hdr.payload_start + hdr.payload_len];
        let (c, reason) = parse_close_payload(payload);
        assert_eq!(c, 1000);
        assert_eq!(reason, "bye");
    }

    /// Fragmented control frame (FIN=0 on Ping) is a protocol error.
    #[test]
    fn decode_rejects_fragmented_control() {
        // Opcode=9 (ping), FIN=0, len=0
        let frame = [0x09u8, 0x00];
        assert!(matches!(
            decode_frame(&frame).unwrap_err(),
            FrameError::FragmentedControlFrame
        ));
    }

    /// Control frame with payload > 125 is a protocol error.
    #[test]
    fn decode_rejects_huge_control() {
        // FIN+Ping, len=126 → invalid per RFC 6455 §5.5
        let mut frame = vec![0x89, 0x7E, 0x00, 0x7E];
        frame.extend_from_slice(&vec![0u8; 126]);
        assert!(matches!(
            decode_frame(&frame).unwrap_err(),
            FrameError::ControlFrameTooLarge(126)
        ));
    }

    /// Unknown opcode (0x3 is reserved for data, unassigned).
    #[test]
    fn decode_rejects_unknown_opcode() {
        let frame = [0x83u8, 0x00];
        assert!(matches!(
            decode_frame(&frame).unwrap_err(),
            FrameError::UnknownOpcode(0x3)
        ));
    }

    /// RSV bits set → protocol error.
    #[test]
    fn decode_rejects_rsv() {
        // FIN=1, RSV1=1, opcode=text → 0xC1
        let frame = [0xC1u8, 0x00];
        assert!(matches!(
            decode_frame(&frame).unwrap_err(),
            FrameError::ReservedBitSet(_)
        ));
    }

    /// Close helper packs code + reason correctly.
    #[test]
    fn encode_close_round_trip() {
        let mut out = Vec::new();
        encode_close(1000, "bye", [0; 4], &mut out);
        // Opcode=8 (close), FIN=1 → 0x88
        assert_eq!(out[0], 0x88);
        // MASK=1, len=5 → 0x85
        assert_eq!(out[1], 0x85);
        // Bytes 6..8: code 1000 in big-endian
        assert_eq!(&out[6..8], &(1000u16).to_be_bytes());
        // Bytes 8..11: "bye" XOR zero-mask = "bye"
        assert_eq!(&out[8..11], b"bye");
    }

    /// Payload length beyond 64 MiB is rejected at decode.
    #[test]
    fn decode_rejects_payload_over_cap() {
        // 127-length variant with 100 MiB
        let big = (100u64 * 1024 * 1024).to_be_bytes();
        let mut frame = vec![0x82u8, 0x7F];
        frame.extend_from_slice(&big);
        assert!(matches!(
            decode_frame(&frame).unwrap_err(),
            FrameError::PayloadTooLarge(_)
        ));
    }

    /// Opcode round-trip through the enum.
    #[test]
    fn opcode_bits_roundtrip() {
        for op in [
            Opcode::Continuation,
            Opcode::Text,
            Opcode::Binary,
            Opcode::Close,
            Opcode::Ping,
            Opcode::Pong,
        ] {
            assert_eq!(Opcode::from_bits(op.bits()), Some(op));
        }
        assert!(Opcode::from_bits(0x3).is_none());
        assert!(Opcode::from_bits(0xF).is_none());
    }

    #[test]
    fn control_opcodes_identified() {
        assert!(Opcode::Close.is_control());
        assert!(Opcode::Ping.is_control());
        assert!(Opcode::Pong.is_control());
        assert!(!Opcode::Text.is_control());
        assert!(!Opcode::Binary.is_control());
        assert!(!Opcode::Continuation.is_control());
    }

    #[test]
    fn parse_close_empty_payload() {
        let (code, reason) = parse_close_payload(&[]);
        assert_eq!(code, 1005);
        assert!(reason.is_empty());
    }

    #[test]
    fn parse_close_single_byte_tolerated() {
        // 1-byte payload is malformed per spec; we return 1005 for safety.
        let (code, _) = parse_close_payload(&[0x03]);
        assert_eq!(code, 1005);
    }
}
