//! TLS wrapper for mio's `TcpStream` using `rustls` directly.
//!
//! No async runtime, no compio-tls, no tokio-rustls. Just the raw
//! `rustls::ClientConnection` state machine driven by mio socket
//! readiness events.
//!
//! The wrapper provides:
//!
//! - **`MioTlsStream`**: a TLS-wrapped `TcpStream` that implements
//!   `Read + Write`. TLS record encryption/decryption is transparent
//!   to callers.
//! - **`build_tls_config`**: constructs a `rustls::ClientConfig` with
//!   webpki-roots (strict) or an accept-all verifier (`--insecure`).
//! - **`MioStream`**: an enum over plain `TcpStream` and `MioTlsStream`
//!   that unifies the two paths so callers don't need to branch on
//!   every read/write.
//!
//! # Non-blocking handshake
//!
//! `complete_handshake` drives the TLS state machine to completion by
//! polling the mio event loop between each `read_tls` / `write_tls`
//! round-trip. The caller must have already registered the underlying
//! TCP socket with mio before calling this method.
//!
//! # WouldBlock handling
//!
//! rustls buffers both plaintext and ciphertext internally. A `Read`
//! on `MioTlsStream` first drives pending I/O (`write_tls` then
//! `read_tls`), processes new TLS records, and then reads decrypted
//! bytes from the internal buffer. If no decrypted data is available,
//! the call returns `WouldBlock` — the mio event loop will wake us
//! when the socket is readable again.

use std::io::{self, Read, Write};
use std::sync::Arc;

use mio::net::TcpStream;
use rustls::ClientConfig;

use zerobench_core::tls::tls_client_config;
use zerobench_core::transport::TransportOpts;

// ---------------------------------------------------------------------------
// MioTlsStream — TLS over mio TcpStream
// ---------------------------------------------------------------------------

/// A TLS-wrapped mio `TcpStream`. Handles non-blocking TLS handshake
/// and transparent encrypt/decrypt on read/write.
pub struct MioTlsStream {
    tls: rustls::ClientConnection,
    tcp: TcpStream,
}

impl MioTlsStream {
    /// Create a new TLS connection over an already-connected TCP stream.
    /// Does NOT perform the handshake — call `complete_handshake` after
    /// registering the underlying TCP stream with mio.
    pub fn new(
        tcp: TcpStream,
        config: Arc<ClientConfig>,
        server_name: &str,
    ) -> io::Result<Self> {
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad SNI: {e}")))?;
        let tls = rustls::ClientConnection::new(config, server_name)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("tls: {e}")))?;
        Ok(Self { tls, tcp })
    }

    /// Drive the TLS handshake to completion. Blocks (with mio poll)
    /// until the handshake succeeds or fails.
    ///
    /// The underlying TCP stream must already be registered with `poll`
    /// at `token` with `Interest::READABLE | Interest::WRITABLE`.
    ///
    /// Since mio's `TcpStream::connect` is non-blocking, the TCP 3-way
    /// handshake may still be in progress. This method first waits for
    /// the socket to become writable (TCP connect completed), then
    /// drives the TLS state machine.
    pub fn complete_handshake(
        &mut self,
        poll: &mut mio::Poll,
        token: mio::Token,
    ) -> io::Result<()> {
        let mut events = mio::Events::with_capacity(64);

        // Phase 1: wait for TCP connect to complete.
        // mio signals the socket as writable when connect() finishes.
        // Check for connect errors via `peer_addr()` or `take_error()`.
        loop {
            poll.poll(&mut events, Some(std::time::Duration::from_secs(5)))?;
            let mut connected = false;
            for event in events.iter() {
                if event.token() == token {
                    if event.is_writable() || event.is_readable() {
                        connected = true;
                    }
                    if event.is_error() {
                        // Check the actual error.
                        if let Some(e) = self.tcp.take_error()? {
                            return Err(e);
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            "connect error",
                        ));
                    }
                }
            }
            if connected {
                // Verify the connection actually succeeded.
                match self.tcp.peer_addr() {
                    Ok(_) => break,
                    Err(ref e) if e.kind() == io::ErrorKind::NotConnected => {
                        // Not yet connected — keep polling.
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // Phase 2: drive TLS handshake.
        let mut rounds = 0u32;
        loop {
            rounds += 1;
            if !self.tls.is_handshaking() {
                return Ok(());
            }
            // Drive TLS I/O — may need multiple rounds as the handshake
            // involves several message flights (ClientHello -> ServerHello
            // + Cert + Done -> ClientFinished -> ServerFinished).
            //
            // We loop on do_tls_io until neither side wants I/O or we
            // hit WouldBlock, then poll for more socket readiness.
            loop {
                let wrote = if self.tls.wants_write() {
                    match self.tls.write_tls(&mut self.tcp) {
                        Ok(n) => n > 0,
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => false,
                        Err(e) => return Err(e),
                    }
                } else {
                    false
                };
                let read = if self.tls.wants_read() {
                    match self.tls.read_tls(&mut self.tcp) {
                        Ok(0) => {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "tls eof during handshake",
                            ));
                        }
                        Ok(n) => {
                            self.tls.process_new_packets().map_err(|e| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("tls: {e}"),
                                )
                            })?;
                            n > 0
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => false,
                        Err(e) => return Err(e),
                    }
                } else {
                    false
                };
                if !wrote && !read {
                    break;
                }
                if !self.tls.is_handshaking() {
                    return Ok(());
                }
            }
            if !self.tls.is_handshaking() {
                return Ok(());
            }
            if rounds > 200 {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "tls handshake timed out",
                ));
            }
            // Wait for socket readiness
            poll.poll(&mut events, Some(std::time::Duration::from_millis(100)))?;
        }
    }

    /// `true` if the rustls state machine has not yet finished the
    /// handshake. Use in a multiplexing loop to decide when to
    /// transition out of the handshake state.
    pub fn is_handshaking(&self) -> bool {
        self.tls.is_handshaking()
    }

    /// Perform pending TLS reads/writes on the underlying TCP stream.
    /// Call this whenever mio reports the socket is readable or writable.
    pub fn do_tls_io(&mut self) -> io::Result<()> {
        // Write pending TLS data to TCP
        if self.tls.wants_write() {
            match self.tls.write_tls(&mut self.tcp) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
        // Read TLS data from TCP
        if self.tls.wants_read() {
            match self.tls.read_tls(&mut self.tcp) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "tls eof"));
                }
                Ok(_) => {
                    self.tls.process_new_packets().map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("tls: {e}"))
                    })?;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Get the negotiated ALPN protocol (e.g. `b"h2"` or `b"http/1.1"`).
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.tls.alpn_protocol()
    }

    /// Get a reference to the underlying TCP stream (for mio registration).
    pub fn tcp_stream(&self) -> &TcpStream {
        &self.tcp
    }

    /// Get a mutable reference to the underlying TCP stream.
    pub fn tcp_stream_mut(&mut self) -> &mut TcpStream {
        &mut self.tcp
    }
}

impl Read for MioTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Drive TLS I/O first, then try to read decrypted data.
        self.do_tls_io()?;
        match self.tls.reader().read(buf) {
            Ok(n) => Ok(n),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No decrypted data available — drive I/O once more in
                // case new ciphertext arrived between the two calls.
                self.do_tls_io()?;
                self.tls.reader().read(buf)
            }
            Err(e) => Err(e),
        }
    }
}

impl Write for MioTlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.tls.writer().write(buf)?;
        // Flush all pending TLS records to TCP. Loop until rustls has
        // no more output or the socket returns WouldBlock. This ensures
        // the encrypted request bytes actually reach the wire before
        // we transition to reading the response.
        while self.tls.wants_write() {
            match self.tls.write_tls(&mut self.tcp) {
                Ok(0) => break,
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.tls.writer().flush()?;
        while self.tls.wants_write() {
            match self.tls.write_tls(&mut self.tcp) {
                Ok(0) => break,
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        self.tcp.flush()
    }
}

// ---------------------------------------------------------------------------
// MioStream — plain or TLS, unified Read + Write
// ---------------------------------------------------------------------------

/// Unified stream type for the mio backends. Callers read/write through
/// this enum and don't need to branch on TLS vs plain TCP.
pub enum MioStream {
    /// Plain (unencrypted) TCP stream.
    Plain(TcpStream),
    /// TLS-wrapped TCP stream.
    Tls(MioTlsStream),
}

impl MioStream {
    /// Get a reference to the underlying TCP stream for mio registration.
    pub fn tcp_stream(&self) -> &TcpStream {
        match self {
            Self::Plain(s) => s,
            Self::Tls(s) => s.tcp_stream(),
        }
    }

    /// Get a mutable reference to the underlying TCP stream.
    pub fn tcp_stream_mut(&mut self) -> &mut TcpStream {
        match self {
            Self::Plain(s) => s,
            Self::Tls(s) => s.tcp_stream_mut(),
        }
    }

    /// Flush any pending TLS output to the underlying TCP socket.
    /// No-op for plain TCP streams. Call this on writable events
    /// to ensure encrypted data reaches the wire even when the
    /// application layer is in a reading state.
    pub fn flush_tls(&mut self) {
        if let Self::Tls(s) = self {
            let _ = s.do_tls_io();
        }
    }

    /// Drive one round of TLS I/O against the underlying TCP socket.
    /// Plain streams return `Ok(())` unconditionally. Use in a
    /// multiplexing event loop where multiple TLS connections share
    /// one Poll and need to drive handshakes incrementally.
    pub fn drive_tls_io(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(_) => Ok(()),
            Self::Tls(s) => s.do_tls_io(),
        }
    }

    /// `true` if this is a TLS stream and its handshake is still in
    /// progress. Plain streams always return `false`.
    pub fn is_handshaking(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Tls(s) => s.is_handshaking(),
        }
    }
}

impl Read for MioStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for MioStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

// ---------------------------------------------------------------------------
// TLS config builder — delegates to zerobench-core's shared builder
// ---------------------------------------------------------------------------

/// Build a `rustls::ClientConfig` suitable for the mio backends.
///
/// Delegates to `zerobench_core::tls::tls_client_config`, which handles
/// strict webpki-roots verification vs `--insecure` accept-all mode.
/// The `alpn` list is passed through to the config — callers should set
/// `[b"http/1.1"]` for H1 or `[b"h2"]` for H2.
pub fn build_tls_config(opts: &TransportOpts, alpn: &[&[u8]]) -> Arc<ClientConfig> {
    tls_client_config(opts, alpn)
}
