//! A single client-side WebSocket connection.
//!
//! Wraps a plain TCP socket (plus the recv-buffer and the per-connection
//! mask RNG). The connection layer sits above the frame codec and below
//! the benchmark loop:
//!
//! ```text
//!     run_ws_saturate
//!       └── WsConnection ← this module
//!             └── frame  ← zerobench_ws::frame
//!                 └── TcpStream
//! ```
//!
//! # TLS / wss://
//!
//! `wss://` runs the same handshake + frame codec on top of a
//! `compio_tls::TlsStream<TcpStream>` instead of a plain `TcpStream`.
//! The layered design falls out of [`WsStream`] being a blanket trait
//! for anything that is `AsyncRead + AsyncWrite + Unpin + 'static`:
//! `WsConnection<TcpStream>` and `WsConnection<TlsStream<TcpStream>>`
//! coexist without any per-variant code below the connect path.
//! We deliberately do **not** advertise ALPN here — the WebSocket
//! Upgrade handshake lives above TLS, so ALPN has no role.
//!
//! # Control-frame handling
//!
//! `recv` auto-replies to Pings with Pongs (RFC 6455 §5.5.2) and closes
//! cleanly on a server Close frame. Data frames (text/binary) propagate
//! up to the caller. Continuation frames for fragmented messages are
//! concatenated transparently — the caller sees one completed
//! Text/Binary per message, never a `Continuation`.

use std::io;

use bytes::BytesMut;
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
// AsyncWrite's `flush` is an inherent method, but callers need to
// invoke it explicitly after write_all on TLS streams (plain TCP
// ignores it as a no-op).
use compio::net::TcpStream;
use compio_tls::TlsStream;

use zerobench_core::rng::BenchRng;
use zerobench_core::tls::tls_client_config;
use zerobench_core::transport::{Target, TransportOpts};

use crate::frame::{self, FrameHeader, Opcode};
use crate::handshake::{self, find_headers_end, HandshakeError};

/// The high-level error type the connection layer surfaces.
///
/// Lifted to the benchmark loop; it chooses which [`WsStats`] counter
/// to bump for each variant. See [`crate::classify_error`].
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    /// The HTTP/1.1 Upgrade handshake failed. Carries a short string
    /// with the underlying [`HandshakeError`] for logging.
    #[error("handshake: {0}")]
    Handshake(#[from] HandshakeError),

    /// A frame decode problem — bad RSV bits, masked server frame,
    /// oversized control frame, etc. Connection is fatal after this.
    #[error("frame: {0}")]
    Frame(#[from] frame::WsError),

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
// A trait abstracting the underlying transport so TLS can be dropped in
// later. Declared pub(crate) because the public API only exposes the
// concrete TcpStream variant for now.
// ---------------------------------------------------------------------------

/// Marker trait for WS-capable byte streams. `TcpStream` satisfies it
/// for `ws://`; `TlsStream<TcpStream>` satisfies it for `wss://`.
pub trait WsStream: AsyncRead + AsyncWrite + Unpin + 'static {}
impl WsStream for TcpStream {}
impl WsStream for TlsStream<TcpStream> {}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A single established WebSocket connection.
///
/// Lifecycle:
/// 1. [`WsConnection::handshake`] — TCP + RFC 6455 §4 Upgrade.
/// 2. Repeated [`WsConnection::send`] / [`WsConnection::recv`].
/// 3. Optional [`WsConnection::close`] before drop.
///
/// `send` always emits FIN=1 frames — we don't fragment outbound
/// messages. `recv` transparently handles Ping/Pong and reassembles
/// Continuation fragments before handing a completed
/// [`DataFrame`] up to the caller.
pub struct WsConnection<S: WsStream> {
    stream: S,
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

/// A data frame handed to the caller — messages only (no control
/// frames). Fragmented messages are re-assembled before this appears.
#[derive(Debug, Clone)]
pub enum DataFrame {
    /// A complete text message (caller's responsibility to validate
    /// UTF-8 if they care; the decoder does not).
    Text(bytes::Bytes),
    /// A complete binary message.
    Binary(bytes::Bytes),
}

impl DataFrame {
    /// Length of the message payload in bytes.
    pub fn len(&self) -> usize {
        match self {
            DataFrame::Text(b) | DataFrame::Binary(b) => b.len(),
        }
    }

    /// `true` when the payload is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the raw bytes regardless of Text/Binary variant.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            DataFrame::Text(b) | DataFrame::Binary(b) => b,
        }
    }
}

impl WsConnection<TcpStream> {
    /// Open a plain-TCP `ws://` connection and perform the RFC 6455 §4
    /// Upgrade handshake.
    ///
    /// Returns a ready-to-use connection on success. Partial reads of
    /// the response are handled — we keep pulling from the socket until
    /// a `\r\n\r\n` terminator is found (capped at 16 KiB to prevent
    /// resource exhaustion from a malicious server).
    ///
    /// TLS targets are rejected here — the caller should route `wss://`
    /// through [`WsConnection::connect`] which dispatches to the TLS
    /// path. Kept for backward compatibility with existing tests; new
    /// call sites should prefer the unified [`connect`] wrapper below.
    pub async fn connect_tcp(
        target: &Target,
        opts: &TransportOpts,
        path: &str,
        extra_headers: &[(String, String)],
        mut mask_rng: BenchRng,
    ) -> Result<Self, WsError> {
        if target.tls {
            return Err(WsError::Tls(
                "wss:// target passed to connect_tcp; use WsConnection::connect for TLS support".into(),
            ));
        }

        let addr = target.addr();
        let stream = compio::time::timeout(opts.connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| WsError::Io(io::Error::new(io::ErrorKind::TimedOut, format!("{addr}: connect timeout"))))?
            .map_err(|e| {
                WsError::Io(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("{addr}: {e}"),
                ))
            })?;
        // Best-effort NODELAY; same as the HTTP transport. A failure
        // here doesn't justify aborting.
        let _ = stream.set_nodelay(true);

        Self::handshake_over(stream, target, path, extra_headers, &mut mask_rng).await
    }
}

impl WsConnection<TlsStream<TcpStream>> {
    /// Open a TLS-wrapped `wss://` connection and perform the Upgrade
    /// handshake on top of it.
    ///
    /// Uses [`zerobench_core::tls::tls_client_config`] for the rustls
    /// config. We deliberately don't advertise ALPN — the WebSocket
    /// Upgrade lives *above* TLS, so there's no protocol for ALPN to
    /// pick between.
    pub async fn connect_tls(
        target: &Target,
        opts: &TransportOpts,
        path: &str,
        extra_headers: &[(String, String)],
        mut mask_rng: BenchRng,
    ) -> Result<Self, WsError> {
        if !target.tls {
            // Called from a unified wrapper that's already verified
            // this — belt-and-braces.
            return Err(WsError::Tls(
                "ws:// target passed to connect_tls; use connect_tcp for plain".into(),
            ));
        }

        let addr = target.addr();
        let tcp = compio::time::timeout(opts.connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| WsError::Io(io::Error::new(io::ErrorKind::TimedOut, format!("{addr}: connect timeout"))))?
            .map_err(|e| {
                WsError::Io(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("{addr}: {e}"),
                ))
            })?;
        let _ = tcp.set_nodelay(true);

        // Empty ALPN list — WebSocket handshake is entirely over TLS
        // record layer, no ALPN negotiation to make.
        let cfg = tls_client_config(opts, &[]);
        let connector = compio_tls::TlsConnector::from(cfg);
        let server_name = target.sni_name().to_string();
        let tls = compio::time::timeout(opts.connect_timeout, connector.connect(&server_name, tcp))
            .await
            .map_err(|_| WsError::Tls(format!("{addr}: tls handshake timeout")))?
            .map_err(|e| WsError::Tls(format!("handshake: {e}")))?;

        Self::handshake_over(tls, target, path, extra_headers, &mut mask_rng).await
    }
}

/// Unified `ws://` + `wss://` connection.
///
/// A single enum so the runner can hold "a WebSocket connection"
/// without propagating a stream-type generic through every function.
/// Method calls dispatch through the enum with a runtime match; the
/// two variants don't share a vtable because the inner types differ
/// enough (TLS stream is boxed for size parity) that an enum is clearer
/// than a trait object.
pub enum AnyWsConnection {
    /// Plain `ws://` — TCP directly.
    Plain(WsConnection<TcpStream>),
    /// `wss://` — TLS over TCP.
    Tls(WsConnection<TlsStream<TcpStream>>),
}

impl AnyWsConnection {
    /// Dispatch `send_text`.
    pub async fn send_text(&mut self, payload: &[u8]) -> Result<(), WsError> {
        match self {
            AnyWsConnection::Plain(c) => c.send_text(payload).await,
            AnyWsConnection::Tls(c) => c.send_text(payload).await,
        }
    }

    /// Dispatch `send_binary`.
    pub async fn send_binary(&mut self, payload: &[u8]) -> Result<(), WsError> {
        match self {
            AnyWsConnection::Plain(c) => c.send_binary(payload).await,
            AnyWsConnection::Tls(c) => c.send_binary(payload).await,
        }
    }

    /// Dispatch `recv`.
    pub async fn recv(&mut self) -> Result<DataFrame, WsError> {
        match self {
            AnyWsConnection::Plain(c) => c.recv().await,
            AnyWsConnection::Tls(c) => c.recv().await,
        }
    }

    /// Dispatch `close`.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), WsError> {
        match self {
            AnyWsConnection::Plain(c) => c.close(code, reason).await,
            AnyWsConnection::Tls(c) => c.close(code, reason).await,
        }
    }
}

impl WsConnection<TcpStream> {
    /// Unified connect that picks plain or TLS based on `target.tls`.
    ///
    /// This is the entry point the runner uses. Existing callers that
    /// still go through [`WsConnection::connect_tcp`] (tests, v1 compat)
    /// keep working — the inherent method lives on `WsConnection<TcpStream>`
    /// and still returns a concrete type.
    pub async fn connect(
        target: &Target,
        opts: &TransportOpts,
        path: &str,
        extra_headers: &[(String, String)],
        mask_rng: BenchRng,
    ) -> Result<AnyWsConnection, WsError> {
        if target.tls {
            let c = WsConnection::<TlsStream<TcpStream>>::connect_tls(
                target,
                opts,
                path,
                extra_headers,
                mask_rng,
            )
            .await?;
            Ok(AnyWsConnection::Tls(c))
        } else {
            let c = WsConnection::<TcpStream>::connect_tcp(
                target,
                opts,
                path,
                extra_headers,
                mask_rng,
            )
            .await?;
            Ok(AnyWsConnection::Plain(c))
        }
    }
}

impl<S: WsStream> WsConnection<S> {
    /// Perform the handshake on a pre-opened stream.
    ///
    /// Extracted so a future TLS variant can call it after the TLS
    /// handshake succeeds. The `mask_rng` parameter seeds the
    /// per-connection mask CSPRNG and is also used for the
    /// Sec-WebSocket-Key nonce.
    pub async fn handshake_over(
        mut stream: S,
        target: &Target,
        path: &str,
        extra_headers: &[(String, String)],
        mask_rng: &mut BenchRng,
    ) -> Result<Self, WsError> {
        let (key_b64, _key_bytes) = handshake::generate_key(mask_rng);
        let req = handshake::build_request(target, path, &key_b64, extra_headers);
        stream.write_all(req).await.0?;
        // Flush is critical on TLS streams — compio-tls buffers
        // plaintext through rustls' record layer and only empties the
        // buffer on an explicit flush. Plain TCP treats this as a
        // no-op, so the call is cheap on the hot path.
        stream.flush().await?;

        // Pull the response header section. Cap at 16 KiB so a misbehaving
        // server can't pin us on endless reads.
        const MAX_HEADER_BYTES: usize = 16 * 1024;
        let mut raw = Vec::<u8>::with_capacity(1024);

        let headers_end_pos = loop {
            if let Some(end) = find_headers_end(&raw) {
                break end;
            }
            if raw.len() >= MAX_HEADER_BYTES {
                return Err(WsError::Handshake(HandshakeError::HeadersTooBig));
            }
            let buf = Vec::with_capacity(1024);
            let BufResult(res, returned) = stream.read(buf).await;
            let n = res?;
            if n == 0 {
                return Err(WsError::Handshake(HandshakeError::UnparseableResponse(
                    "server closed before sending handshake response".into(),
                )));
            }
            raw.extend_from_slice(&returned[..n]);
        };

        // Parse with httparse — it's zero-copy and already a workspace
        // dep. `max_header_count=64` is well above what real servers send.
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut resp = httparse::Response::new(&mut headers);
        let parse_result = resp
            .parse(&raw[..headers_end_pos])
            .map_err(|e| WsError::Handshake(HandshakeError::UnparseableResponse(e.to_string())))?;

        match parse_result {
            httparse::Status::Complete(_) => {}
            httparse::Status::Partial => {
                // Shouldn't happen — find_headers_end already saw \r\n\r\n.
                return Err(WsError::Handshake(HandshakeError::UnparseableResponse(
                    "httparse: partial after \\r\\n\\r\\n seen".into(),
                )));
            }
        }
        let status = resp.code.unwrap_or(0);

        handshake::validate_response(status, resp.headers, &key_b64)?;

        // Anything after the `\r\n\r\n` is the start of the WebSocket
        // frame stream — servers sometimes send the 101 and immediately
        // push their first frame in the same TCP chunk.
        let mut recv_buf = BytesMut::with_capacity(4096);
        if raw.len() > headers_end_pos {
            recv_buf.extend_from_slice(&raw[headers_end_pos..]);
        }

        Ok(Self {
            stream,
            recv_buf,
            mask_rng: mask_rng.clone(),
            fragment: None,
            close_sent: false,
        })
    }

    /// Send a Text frame.
    pub async fn send_text(&mut self, payload: &[u8]) -> Result<(), WsError> {
        self.send_frame(Opcode::Text, payload).await
    }

    /// Send a Binary frame.
    pub async fn send_binary(&mut self, payload: &[u8]) -> Result<(), WsError> {
        self.send_frame(Opcode::Binary, payload).await
    }

    /// Send a masked frame of the given opcode. Callers should pass
    /// `Text`, `Binary`, `Ping`, or `Pong`. Close frames should go
    /// through [`WsConnection::close`].
    async fn send_frame(&mut self, opcode: Opcode, payload: &[u8]) -> Result<(), WsError> {
        let mask = generate_mask(&mut self.mask_rng);
        // Build into a fresh Vec so `write_all`'s buffer-ownership
        // contract (compio takes the buffer by value) is satisfied.
        let mut buf = Vec::with_capacity(14 + payload.len());
        frame::encode_frame(opcode, payload, mask, &mut buf);
        self.stream.write_all(buf).await.0?;
        // Flush for TLS streams — see the handshake path's comment.
        // Plain TCP's flush is a no-op, so the cost is negligible.
        self.stream.flush().await?;
        Ok(())
    }

    /// Send a Pong frame with the given payload. Used to reply to a
    /// server Ping per RFC 6455 §5.5.2.
    async fn send_pong(&mut self, payload: &[u8]) -> Result<(), WsError> {
        self.send_frame(Opcode::Pong, payload).await
    }

    /// Wait for the next data message.
    ///
    /// Handles control frames transparently:
    /// - `Ping` → auto-reply with `Pong` and keep reading.
    /// - `Pong` → ignored (we don't issue keepalive pings).
    /// - `Close` → reply with close (if we haven't already) and
    ///   return [`WsError::Closed`] so the caller's loop exits.
    ///
    /// Reassembles fragmented messages (RFC 6455 §5.4) into a single
    /// `DataFrame` before returning.
    pub async fn recv(&mut self) -> Result<DataFrame, WsError> {
        loop {
            // Try to decode the next frame from what's already buffered.
            let hdr_result = frame::decode_frame(&self.recv_buf);
            match hdr_result {
                Ok(hdr) => {
                    let frame_bytes = self.recv_buf.split_to(hdr.total_len);
                    match self.handle_frame(hdr, frame_bytes.freeze()).await? {
                        Some(f) => return Ok(f),
                        None => continue,
                    }
                }
                Err(frame::WsError::NeedMore { needed }) => {
                    self.fill_recv_buf(needed).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Dispatch a single decoded frame. Returns `Some(DataFrame)` when
    /// the caller should get a message back (possibly after accumulating
    /// fragments), `None` when the recv loop should spin again (e.g.
    /// after auto-replying to a Ping).
    async fn handle_frame(
        &mut self,
        hdr: FrameHeader,
        frame_bytes: bytes::Bytes,
    ) -> Result<Option<DataFrame>, WsError> {
        // Qualified call — `Bytes::slice` returns `Bytes`, but
        // `compio::buf::IoBuf::slice` (a blanket impl covers `Bytes`)
        // shadows the method name, so we use `Buf`'s inherent form.
        let payload = bytes::Bytes::slice(
            &frame_bytes,
            hdr.payload_start..hdr.payload_start + hdr.payload_len,
        );

        match hdr.opcode {
            Opcode::Text | Opcode::Binary => {
                if self.fragment.is_some() {
                    return Err(WsError::Frame(frame::WsError::ProtocolOther(
                        "new data frame without finishing previous fragment".into(),
                    )));
                }
                if hdr.fin {
                    // Single complete message, no fragmentation.
                    return Ok(Some(match hdr.opcode {
                        Opcode::Text => DataFrame::Text(payload),
                        Opcode::Binary => DataFrame::Binary(payload),
                        _ => unreachable!(),
                    }));
                }
                // Start a fragmented message.
                self.fragment = Some((hdr.opcode, payload.to_vec()));
                Ok(None)
            }
            Opcode::Continuation => {
                let (opcode, buf) = match self.fragment.as_mut() {
                    Some(f) => f,
                    None => {
                        return Err(WsError::Frame(frame::WsError::ProtocolOther(
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
                // Per §5.5.2: reply with Pong carrying identical payload.
                self.send_pong(&payload).await?;
                Ok(None)
            }
            Opcode::Pong => {
                // Unsolicited pong — ignore. We don't issue pings so
                // there's no request to match up.
                Ok(None)
            }
            Opcode::Close => {
                let (code, reason) = frame::parse_close_payload(&payload);
                // RFC 6455 §5.5.1: "Upon receipt of such a frame, the
                // other peer sends a Close frame in response, if it
                // hasn't already sent one."
                if !self.close_sent {
                    // Best-effort echo. Ignore errors: the socket may
                    // already be half-closed.
                    let mut buf = Vec::with_capacity(8);
                    let mask = generate_mask(&mut self.mask_rng);
                    frame::encode_close(code, "", mask, &mut buf);
                    let _ = self.stream.write_all(buf).await.0;
                    let _ = self.stream.flush().await;
                    self.close_sent = true;
                }
                Err(WsError::Closed { code, reason })
            }
        }
    }

    /// Read at least `min_new` bytes into `recv_buf`.
    ///
    /// Compio's `AsyncRead::read` takes buffer ownership, so we allocate
    /// a fresh Vec per read and copy into our persistent BytesMut. A
    /// BytesMut re-use would require compio's `IoBufMut` to work with
    /// an owned view of the tail — possible but not worth the plumbing
    /// for what's typically a single read per message on loopback.
    async fn fill_recv_buf(&mut self, min_new: usize) -> Result<(), WsError> {
        let want = min_new.max(4096);
        let before = self.recv_buf.len();
        while self.recv_buf.len() - before < min_new.max(1) {
            let buf = Vec::with_capacity(want);
            let BufResult(res, returned) = self.stream.read(buf).await;
            let n = res?;
            if n == 0 {
                return Err(WsError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed before frame completed",
                )));
            }
            self.recv_buf.extend_from_slice(&returned[..n]);
        }
        Ok(())
    }

    /// Send a Close frame with the given code and reason.
    ///
    /// Best-effort: if the socket write fails (e.g. the server closed
    /// already), the error is swallowed and the connection should just
    /// be dropped. We still set `close_sent` so callers that invoke
    /// [`Self::close`] after having seen a server Close don't emit a
    /// second Close frame.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), WsError> {
        if self.close_sent {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(8 + reason.len());
        let mask = generate_mask(&mut self.mask_rng);
        frame::encode_close(code, reason, mask, &mut buf);
        self.close_sent = true;
        self.stream.write_all(buf).await.0?;
        let _ = self.stream.flush().await;
        Ok(())
    }
}

/// Generate a 4-byte mask from the per-connection CSPRNG.
///
/// Uses `BenchRng::fill_bytes` (Xoshiro256++ seeded from OS entropy —
/// see `zerobench_core::rng`). RFC 6455 §10.3 requires the mask be
/// "chosen from the set of allowed 32-bit values at random", which
/// Xoshiro meets with enormous margin for the cache-poisoning threat
/// model: even a narrow-period PRNG would suffice as long as it's not
/// predictable to an attacker; we use a CSPRNG-equivalent
/// [`BenchRng::from_entropy`] seed so predicting the mask requires
/// reading the worker's memory.
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
        // Deterministic for a seeded Xoshiro — verify we actually wrote
        // 4 bytes and that they're not all zero (near-impossible for
        // any reasonable seed).
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
