//! A single client-side WebSocket connection (mio/synchronous).
//!
//! Wraps a [`MioStream`] (plain TCP or TLS) plus the recv-buffer and
//! the per-connection mask RNG. The connection layer sits above the
//! frame codec and below the benchmark loop:
//!
//! ```text
//!     run_ws_echo_rtt_from_plan_threaded
//!       └── WsConnection ← this module
//!             └── frame  ← crate::ws::frame
//!                 └── MioStream (plain or TLS)
//! ```
//!
//! # TLS / wss://
//!
//! `wss://` runs the same handshake + frame codec on top of a
//! `MioTlsStream` wrapped inside `MioStream::Tls`. The `MioStream`
//! enum implements `Read + Write`, so the connection code is agnostic
//! to the transport variant.
//!
//! # Control-frame handling
//!
//! `recv` auto-replies to Pings with Pongs (RFC 6455 §5.5.2) and closes
//! cleanly on a server Close frame. Data frames (text/binary) propagate
//! up to the caller. Continuation frames for fragmented messages are
//! concatenated transparently — the caller sees one completed
//! Text/Binary per message, never a `Continuation`.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use rustls::ClientConfig;

use zerobench_core::rng::BenchRng;
use zerobench_core::transport::{Target, TransportOpts};

use crate::http::mio_tls::{MioStream, MioTlsStream};

use crate::ws::frame::{self, FrameHeader, Opcode};
use crate::ws::handshake::{self, find_headers_end, HandshakeError};

/// The high-level error type the connection layer surfaces.
///
/// Lifted to the benchmark loop; it chooses which [`WsStats`] counter
/// to bump for each variant. See `classify_error` in the ws backend.
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    /// The HTTP/1.1 Upgrade handshake failed. Carries a short string
    /// with the underlying [`HandshakeError`] for logging.
    #[error("handshake: {0}")]
    Handshake(#[from] HandshakeError),

    /// A frame decode problem — bad RSV bits, masked server frame,
    /// oversized control frame, etc. Connection is fatal after this.
    #[error("frame: {0}")]
    Frame(#[from] frame::FrameError),

    /// A close frame arrived. Not really an error — the caller decides
    /// whether to count it, but the variant exists so the recv loop
    /// has a clean way to signal "EOF by protocol".
    #[error("remote closed (code={code}, reason={reason:?})")]
    Closed {
        /// Close code as received from the server (RFC 6455 §7.4).
        code: u16,
        /// UTF-8 reason string (lossy decoded from the payload).
        reason: String,
    },

    /// Raw socket IO error. Bubble through `?` via the `#[from]`.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// TLS handshake failed — certificate rejection, hostname mismatch,
    /// signature failure, or an unexpected EOF before the handshake
    /// finished. Message carries the underlying rustls error.
    #[error("tls: {0}")]
    Tls(String),
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A single established WebSocket connection.
///
/// Lifecycle:
/// 1. [`WsConnection::connect`] — TCP + optional TLS + RFC 6455 §4 Upgrade.
/// 2. Repeated [`WsConnection::send_text`] / [`WsConnection::recv`].
/// 3. Optional [`WsConnection::close`] before drop.
///
/// `send_text` always emits FIN=1 frames — we don't fragment outbound
/// messages. `recv` transparently handles Ping/Pong and reassembles
/// Continuation fragments before handing a completed
/// [`DataFrame`] up to the caller.
pub struct WsConnection {
    stream: MioStream,
    poll: Poll,
    events: Events,
    token: Token,
    recv_buf: BytesMut,
    mask_rng: BenchRng,
    /// Running accumulator for fragmented inbound messages.
    /// `(opcode, payload)` where `opcode` is the first frame's opcode
    /// (Text or Binary). Cleared when a FIN arrives.
    fragment: Option<(Opcode, Vec<u8>)>,
    /// Tracks whether we've sent a Close frame. If the server sends
    /// Close first, we send a reply (RFC 6455 §5.5.1 "the endpoint
    /// MUST send a Close frame in response") and set this so we don't
    /// double-send on [`WsConnection::close`].
    close_sent: bool,
}

/// A frame handed to the caller. Text / Binary are message-level
/// (fragmented messages are re-assembled before dispatch); Pong is
/// surfaced directly because `CorrelateStrategy::PingPong` in
/// `WsEchoRtt` needs to read the pong payload to correlate a Ping.
///
/// Ping is NOT surfaced: the connection auto-replies with Pong
/// internally before returning to the caller (see `handle_frame`).
#[derive(Debug, Clone)]
pub enum DataFrame {
    /// A complete text message (caller's responsibility to validate
    /// UTF-8 if they care; the decoder does not).
    Text(bytes::Bytes),
    /// A complete binary message.
    Binary(bytes::Bytes),
    /// An inbound Pong control frame's payload. Surfaced so the
    /// echo-RTT ping-pong correlation strategy can measure RTT from
    /// the Ping send to the matching Pong receipt.
    Pong(bytes::Bytes),
}

impl DataFrame {
    /// Length of the message payload in bytes.
    pub fn len(&self) -> usize {
        match self {
            DataFrame::Text(b) | DataFrame::Binary(b) | DataFrame::Pong(b) => b.len(),
        }
    }

    /// `true` when the payload is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the raw bytes regardless of variant.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            DataFrame::Text(b) | DataFrame::Binary(b) | DataFrame::Pong(b) => b,
        }
    }
}

impl WsConnection {
    /// Unified connect: picks plain TCP or TLS based on `target.tls`,
    /// performs the TCP connect + optional TLS handshake + WS Upgrade.
    ///
    /// Returns a ready-to-use connection on success.
    pub fn connect(
        target: &Target,
        opts: &TransportOpts,
        path: &str,
        extra_headers: &[(String, String)],
        mut mask_rng: BenchRng,
        tls_config: Option<&Arc<ClientConfig>>,
    ) -> Result<Self, WsError> {
        // `Target::resolve` now honours `opts.resolve_overrides` and
        // `target.addr_family` — the override path skips DNS entirely,
        // the family preference breaks resolver-order ties on dual-stack
        // hosts like `localhost`.
        let addr: SocketAddr = target.resolve(opts).map_err(WsError::Io)?;

        let mut poll = Poll::new().map_err(WsError::Io)?;
        let mut events = Events::with_capacity(64);
        let token = Token(0);

        // --- TCP connect (non-blocking via mio) ---
        let mut tcp = TcpStream::connect(addr).map_err(|e| {
            WsError::Io(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("{addr}: {e}"),
            ))
        })?;
        let _ = tcp.set_nodelay(true);

        poll.registry()
            .register(&mut tcp, token, Interest::READABLE | Interest::WRITABLE)
            .map_err(WsError::Io)?;

        // --- Optional TLS handshake ---
        //
        // For TLS targets, `MioTlsStream::complete_handshake` handles
        // both waiting for TCP connect to complete and driving
        // the TLS state machine. We skip `wait_for_tcp_connect`
        // to avoid consuming the mio edge event the connect step needs.
        //
        // For plain TCP, we wait for the connect event ourselves before
        // writing the WS upgrade request.
        let mut stream = if target.tls {
            let config = tls_config.ok_or_else(|| {
                WsError::Tls("wss:// target but no TLS config provided".into())
            })?;
            let sni = target.sni_name().to_string();
            let mut tls = MioTlsStream::new(tcp, Arc::clone(config), &sni)
                .map_err(|e| WsError::Tls(format!("tls init: {e}")))?;
            tls.complete_handshake(&mut poll, token, opts.connect_timeout)
                .map_err(|e| WsError::Tls(format!("tls handshake: {e}")))?;
            poll.registry()
                .reregister(tls.tcp_stream_mut(), token, Interest::READABLE | Interest::WRITABLE)
                .map_err(WsError::Io)?;
            MioStream::Tls(tls)
        } else {
            wait_for_tcp_connect(&mut poll, &mut events, token, &tcp, opts.connect_timeout)?;
            MioStream::Plain(tcp)
        };

        // --- WS Upgrade handshake ---
        let (key_b64, _key_bytes) = handshake::generate_key(&mut mask_rng);
        let req = handshake::build_request(target, path, &key_b64, extra_headers);

        // Write request bytes
        write_all_mio(&mut stream, &req, &mut poll, &mut events, token)?;
        stream.flush_tls();

        // Read response headers
        let mut raw = Vec::<u8>::with_capacity(1024);
        const MAX_HEADER_BYTES: usize = 16 * 1024;

        let headers_end_pos = loop {
            if let Some(end) = find_headers_end(&raw) {
                break end;
            }
            if raw.len() >= MAX_HEADER_BYTES {
                return Err(WsError::Handshake(HandshakeError::HeadersTooBig));
            }
            read_some_mio(&mut stream, &mut raw, &mut poll, &mut events)?;
        };

        // Parse with httparse
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut resp = httparse::Response::new(&mut headers);
        let parse_result = resp
            .parse(&raw[..headers_end_pos])
            .map_err(|e| WsError::Handshake(HandshakeError::UnparseableResponse(e.to_string())))?;

        match parse_result {
            httparse::Status::Complete(_) => {}
            httparse::Status::Partial => {
                return Err(WsError::Handshake(HandshakeError::UnparseableResponse(
                    "httparse: partial after \\r\\n\\r\\n seen".into(),
                )));
            }
        }
        let status = resp.code.unwrap_or(0);
        handshake::validate_response(status, resp.headers, &key_b64)?;

        // Anything after the \r\n\r\n is the start of the WebSocket
        // frame stream.
        let mut recv_buf = BytesMut::with_capacity(4096);
        if raw.len() > headers_end_pos {
            recv_buf.extend_from_slice(&raw[headers_end_pos..]);
        }

        Ok(Self {
            stream,
            poll,
            events,
            token,
            recv_buf,
            mask_rng,
            fragment: None,
            close_sent: false,
        })
    }

    /// Send a Text frame.
    pub fn send_text(&mut self, payload: &[u8]) -> Result<(), WsError> {
        self.send_frame(Opcode::Text, payload)
    }

    /// Send a Binary frame.
    pub fn send_binary(&mut self, payload: &[u8]) -> Result<(), WsError> {
        self.send_frame(Opcode::Binary, payload)
    }

    /// Send a masked frame of the given opcode.
    fn send_frame(&mut self, opcode: Opcode, payload: &[u8]) -> Result<(), WsError> {
        let mask = generate_mask(&mut self.mask_rng);
        let mut buf = Vec::with_capacity(14 + payload.len());
        frame::encode_frame(opcode, payload, mask, &mut buf);
        write_all_mio(&mut self.stream, &buf, &mut self.poll, &mut self.events, self.token)?;
        self.stream.flush_tls();
        Ok(())
    }

    /// Send a Pong frame with the given payload.
    fn send_pong(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if payload.len() > 125 {
            return Err(WsError::Frame(frame::FrameError::ControlFrameTooLarge(
                payload.len(),
            )));
        }
        self.send_frame(Opcode::Pong, payload)
    }

    /// Send a Ping frame (RFC 6455 §5.5.2) with the given payload.
    /// Compliant servers auto-reply with Pong. Used by `WsHold`
    /// heartbeats where the client wants to keep a proxy's idle
    /// timeout at bay.
    ///
    /// RFC 6455 §5.5 caps control-frame payloads at 125 bytes. An
    /// oversized ping is rejected locally rather than put on the wire:
    /// any RFC 6455 server that read it would tear the connection down
    /// on the client's behalf, producing a hard-to-diagnose "transport
    /// closed" error on the next recv.
    pub fn send_ping(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if payload.len() > 125 {
            return Err(WsError::Frame(frame::FrameError::ControlFrameTooLarge(
                payload.len(),
            )));
        }
        self.send_frame(Opcode::Ping, payload)
    }

    /// Wait for the next data message.
    ///
    /// Handles control frames transparently:
    /// - `Ping` -> auto-reply with `Pong` and keep reading.
    /// - `Pong` -> ignored (we don't issue keepalive pings).
    /// - `Close` -> reply with close (if we haven't already) and
    ///   return [`WsError::Closed`] so the caller's loop exits.
    ///
    /// Reassembles fragmented messages (RFC 6455 §5.4) into a single
    /// `DataFrame` before returning.
    pub fn recv(&mut self) -> Result<DataFrame, WsError> {
        loop {
            // Try to decode the next frame from what's already buffered.
            let hdr_result = frame::decode_frame(&self.recv_buf);
            match hdr_result {
                Ok(hdr) => {
                    let frame_bytes = self.recv_buf.split_to(hdr.total_len);
                    match self.handle_frame(hdr, frame_bytes.freeze())? {
                        Some(f) => return Ok(f),
                        None => continue,
                    }
                }
                Err(frame::FrameError::NeedMore { needed }) => {
                    self.fill_recv_buf(needed)?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Like [`recv`] but returns `Ok(None)` once `timeout` elapses with
    /// no data frame arriving. Control frames (Ping/Pong/Close) are
    /// still handled transparently, so this also keeps auto-Pong alive
    /// for callers that need bounded waits (e.g. `WsHold` between
    /// heartbeats; `WsServerPushRtt` deadline polling).
    pub fn try_recv(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<DataFrame>, WsError> {
        let deadline = Instant::now() + timeout;
        loop {
            match frame::decode_frame(&self.recv_buf) {
                Ok(hdr) => {
                    let frame_bytes = self.recv_buf.split_to(hdr.total_len);
                    match self.handle_frame(hdr, frame_bytes.freeze())? {
                        Some(f) => return Ok(Some(f)),
                        None => continue,
                    }
                }
                Err(frame::FrameError::NeedMore { .. }) => {}
                Err(e) => return Err(e.into()),
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            match read_some_timed(
                &mut self.stream,
                &mut self.recv_buf,
                &mut self.poll,
                &mut self.events,
                deadline - now,
            ) {
                Ok(true) => continue,
                Ok(false) => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }

    /// Dispatch a single decoded frame.
    fn handle_frame(
        &mut self,
        hdr: FrameHeader,
        frame_bytes: bytes::Bytes,
    ) -> Result<Option<DataFrame>, WsError> {
        let payload = bytes::Bytes::slice(
            &frame_bytes,
            hdr.payload_start..hdr.payload_start + hdr.payload_len,
        );

        match hdr.opcode {
            Opcode::Text | Opcode::Binary => {
                if self.fragment.is_some() {
                    return Err(WsError::Frame(frame::FrameError::ProtocolOther(
                        "new data frame without finishing previous fragment".into(),
                    )));
                }
                if hdr.fin {
                    return Ok(Some(match hdr.opcode {
                        Opcode::Text => DataFrame::Text(payload),
                        Opcode::Binary => DataFrame::Binary(payload),
                        _ => unreachable!(),
                    }));
                }
                self.fragment = Some((hdr.opcode, payload.to_vec()));
                Ok(None)
            }
            Opcode::Continuation => {
                let (opcode, buf) = match self.fragment.as_mut() {
                    Some(f) => f,
                    None => {
                        return Err(WsError::Frame(frame::FrameError::ProtocolOther(
                            "continuation frame without preceding text/binary".into(),
                        )));
                    }
                };
                buf.extend_from_slice(&payload);
                if hdr.fin {
                    let opcode = *opcode;
                    let (_op, complete) = self.fragment.take().expect("just matched as Some");
                    let bytes = bytes::Bytes::from(complete);
                    return Ok(Some(match opcode {
                        Opcode::Text => DataFrame::Text(bytes),
                        Opcode::Binary => DataFrame::Binary(bytes),
                        _ => unreachable!(
                            "fragment opcode is always Text or Binary — checked at first frame"
                        ),
                    }));
                }
                Ok(None)
            }
            Opcode::Ping => {
                self.send_pong(&payload)?;
                Ok(None)
            }
            Opcode::Pong => {
                // Surface to the caller so ping-pong correlation
                // strategies can read the pong payload. Callers that
                // don't care (hold / server_push / fanout) match only
                // Text / Binary in their recv loop; the Pong frame
                // falls through their match arms harmlessly.
                Ok(Some(DataFrame::Pong(payload)))
            }
            Opcode::Close => {
                let (code, reason) = frame::parse_close_payload(&payload);
                if !self.close_sent {
                    let mut buf = Vec::with_capacity(8);
                    let mask = generate_mask(&mut self.mask_rng);
                    frame::encode_close(code, "", mask, &mut buf);
                    let _ = write_all_mio(
                        &mut self.stream,
                        &buf,
                        &mut self.poll,
                        &mut self.events,
                        self.token,
                    );
                    self.stream.flush_tls();
                    self.close_sent = true;
                }
                Err(WsError::Closed { code, reason })
            }
        }
    }

    /// Read at least `min_new` bytes into `recv_buf`.
    fn fill_recv_buf(&mut self, min_new: usize) -> Result<(), WsError> {
        let before = self.recv_buf.len();
        while self.recv_buf.len() - before < min_new.max(1) {
            read_some_bytes_into(
                &mut self.stream,
                &mut self.recv_buf,
                &mut self.poll,
                &mut self.events,
            )?;
        }
        Ok(())
    }

    /// Send a Close frame with the given code and reason.
    pub fn close(&mut self, code: u16, reason: &str) -> Result<(), WsError> {
        if self.close_sent {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(8 + reason.len());
        let mask = generate_mask(&mut self.mask_rng);
        frame::encode_close(code, reason, mask, &mut buf);
        self.close_sent = true;
        write_all_mio(&mut self.stream, &buf, &mut self.poll, &mut self.events, self.token)?;
        self.stream.flush_tls();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Mio IO helpers
// ---------------------------------------------------------------------------

/// Wait for the TCP connect to complete (mio connect is non-blocking).
fn wait_for_tcp_connect(
    poll: &mut Poll,
    events: &mut Events,
    token: Token,
    tcp: &TcpStream,
    timeout: Duration,
) -> Result<(), WsError> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(WsError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "TCP connect timeout",
            )));
        }
        let _ = poll.poll(events, Some(remaining.min(Duration::from_millis(100))));
        for event in events.iter() {
            if event.token() == token {
                if event.is_error() {
                    if let Some(e) = tcp.take_error()? {
                        return Err(WsError::Io(e));
                    }
                    return Err(WsError::Io(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "connect error",
                    )));
                }
                if event.is_writable() || event.is_readable() {
                    // Verify the connection actually succeeded.
                    match tcp.peer_addr() {
                        Ok(_) => return Ok(()),
                        Err(ref e) if e.kind() == io::ErrorKind::NotConnected => continue,
                        Err(e) => return Err(WsError::Io(e)),
                    }
                }
            }
        }
    }
}

/// Write all bytes to the stream, handling WouldBlock via mio poll.
fn write_all_mio(
    stream: &mut MioStream,
    data: &[u8],
    poll: &mut Poll,
    events: &mut Events,
    _token: Token,
) -> Result<(), WsError> {
    let mut written = 0;
    while written < data.len() {
        match stream.write(&data[written..]) {
            Ok(0) => {
                return Err(WsError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned 0",
                )));
            }
            Ok(n) => written += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let _ = poll.poll(events, Some(Duration::from_millis(100)));
                stream.flush_tls();
            }
            Err(e) => return Err(WsError::Io(e)),
        }
    }
    Ok(())
}

/// Read some bytes from the stream into a Vec (for handshake headers).
/// At least one non-zero read must succeed; returns error on EOF.
fn read_some_mio(
    stream: &mut MioStream,
    buf: &mut Vec<u8>,
    poll: &mut Poll,
    events: &mut Events,
) -> Result<(), WsError> {
    let mut tmp = [0u8; 4096];
    loop {
        stream.flush_tls();
        match stream.read(&mut tmp) {
            Ok(0) => {
                return Err(WsError::Handshake(HandshakeError::UnparseableResponse(
                    "server closed before sending handshake response".into(),
                )));
            }
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                return Ok(());
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let _ = poll.poll(events, Some(Duration::from_millis(100)));
                stream.flush_tls();
            }
            Err(e) => return Err(WsError::Io(e)),
        }
    }
}

/// Read some bytes from the stream into a BytesMut (for frame data).
fn read_some_bytes_into(
    stream: &mut MioStream,
    buf: &mut BytesMut,
    poll: &mut Poll,
    events: &mut Events,
) -> Result<(), WsError> {
    let mut tmp = [0u8; 4096];
    loop {
        stream.flush_tls();
        match stream.read(&mut tmp) {
            Ok(0) => {
                return Err(WsError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed before frame completed",
                )));
            }
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                return Ok(());
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let _ = poll.poll(events, Some(Duration::from_millis(100)));
                stream.flush_tls();
            }
            Err(e) => return Err(WsError::Io(e)),
        }
    }
}

/// Like [`read_some_bytes_into`] but bounded by a caller-supplied
/// timeout. Returns `Ok(true)` when some bytes were read, `Ok(false)`
/// on timeout, and `Err` on EOF or hard I/O failure.
fn read_some_timed(
    stream: &mut MioStream,
    buf: &mut BytesMut,
    poll: &mut Poll,
    events: &mut Events,
    timeout: Duration,
) -> Result<bool, WsError> {
    let deadline = Instant::now() + timeout;
    let mut tmp = [0u8; 4096];
    loop {
        stream.flush_tls();
        match stream.read(&mut tmp) {
            Ok(0) => {
                return Err(WsError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed before frame completed",
                )));
            }
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                return Ok(true);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(false);
                }
                let wait = (deadline - now).min(Duration::from_millis(100));
                let _ = poll.poll(events, Some(wait));
                stream.flush_tls();
            }
            Err(e) => return Err(WsError::Io(e)),
        }
    }
}

/// Generate a 4-byte mask from the per-connection CSPRNG.
fn generate_mask(rng: &mut BenchRng) -> [u8; 4] {
    use rand::RngCore;
    let mut m = [0u8; 4];
    rng.fill_bytes(&mut m);
    m
}

// ---------------------------------------------------------------------------
// Tests (unit tests — full E2E is in tests/ws_smoke.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_is_4_bytes_from_rng() {
        let mut rng = zerobench_core::rng::from_seed(12345);
        let m = generate_mask(&mut rng);
        assert!(m.iter().any(|&b| b != 0));
    }

    #[test]
    fn mask_differs_per_call_on_same_rng() {
        let mut rng = zerobench_core::rng::from_seed(777);
        let a = generate_mask(&mut rng);
        let b = generate_mask(&mut rng);
        assert_ne!(a, b);
    }

    #[test]
    fn data_frame_accessors() {
        let d = DataFrame::Text(bytes::Bytes::from_static(b"hi"));
        assert_eq!(d.len(), 2);
        assert!(!d.is_empty());
        assert_eq!(d.as_bytes(), b"hi");
        let empty = DataFrame::Binary(bytes::Bytes::new());
        assert!(empty.is_empty());
    }
}
