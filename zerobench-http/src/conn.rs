//! Shared TCP / TLS connection helpers used by every HTTP transport.
//!
//! Opens a raw TCP socket against a [`Target`], optionally negotiates
//! TLS via `compio-tls` + `rustls`, and wraps the final socket in
//! [`CountingStream`] so wire bytes are tracked regardless of the
//! transport that will use the connection.
//!
//! The returned [`Connected`] enum is `match`ed by the caller so each
//! transport can build a hyper / h3 / ws stack on top. We don't box the
//! stream into a trait object — keeping it concrete avoids dynamic
//! dispatch on the hot path and, more practically, sidesteps the thorny
//! `Send + Sync + Unpin + 'static` bound soup `dyn AsyncRead + AsyncWrite`
//! would require.
//!
//! # TLS stack
//!
//! ```text
//!     hyper::client::conn::http{1,2}
//!         └── HyperStream            (compio ↔ hyper IO bridge)
//!             └── TlsStream          (compio-tls wrapper over rustls)
//!                 └── CountingStream (on-wire byte counters)
//!                     └── TcpStream  (compio-net)
//! ```
//!
//! The counters sit *under* TLS so the numbers reflect what `tcpdump`
//! would see: encrypted bytes on the wire, TLS framing included.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use compio::net::TcpStream;
use compio_tls::TlsStream;
use zerobench_core::tls::tls_client_config;
use zerobench_core::transport::{Target, TransportError, TransportOpts};

use crate::counting_stream::CountingStream;

/// A freshly-opened connection, already wrapped in [`CountingStream`].
///
/// Plain HTTP connections return [`Connected::Plain`]; HTTPS connections
/// return [`Connected::Tls`] after negotiating TLS over the counted
/// socket.
///
/// Both variants expose the same `(read, written)` counter pair via
/// [`Connected::counts`]; the HTTP/1 pool snapshots these around each
/// `send_request` call to compute per-request bytes on wire. The TLS
/// variant keeps a separate handle to the underlying counters because
/// `compio-tls::TlsStream` doesn't surface its inner stream directly.
///
/// The `Tls` variant also carries `negotiated_alpn` so the caller can
/// inspect what the server chose — used by `HttpTransport::build_client`
/// to decide between H2 and H1 after the handshake.
pub enum Connected {
    /// Plain TCP with no TLS. `CountingStream` sits directly on the
    /// compio `TcpStream`.
    Plain {
        /// The counted TCP stream itself, ready for handing to hyper.
        stream: CountingStream<TcpStream>,
        /// Shared read-bytes counter.
        read_ctr: Arc<AtomicU64>,
        /// Shared written-bytes counter.
        written_ctr: Arc<AtomicU64>,
    },
    /// HTTPS — `TlsStream` on top of a counted `TcpStream`. TLS encrypt
    /// happens above the counters, so they reflect encrypted wire bytes.
    /// The `counts` pair is held separately because `TlsStream` doesn't
    /// surface its inner stream.
    Tls {
        /// Boxed because `TlsStream` is ~1 KiB (state machine + buffers)
        /// and we return it from an `async fn` — boxing keeps the enum
        /// variant compact and avoids a big move when pattern-matching.
        stream: Box<TlsStream<CountingStream<TcpStream>>>,
        /// Shared read-bytes counter. Same handle as the one inside the
        /// TLS stream's inner `CountingStream`.
        read_ctr: Arc<AtomicU64>,
        /// Shared written-bytes counter.
        written_ctr: Arc<AtomicU64>,
        /// ALPN protocol selected by the server, or `None` if ALPN was
        /// not negotiated (empty `alpn_protocols` on the client, or the
        /// server didn't advertise a match).
        negotiated_alpn: Option<Vec<u8>>,
    },
}

impl Connected {
    /// Shared handles to the (bytes_read, bytes_written) counters.
    ///
    /// Both variants return the counters from the underlying
    /// `CountingStream`; TLS framing (~20-30 bytes per record) shows up
    /// in these numbers because the wrapper sits below the TLS layer.
    pub fn counts(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        match self {
            Connected::Plain {
                read_ctr,
                written_ctr,
                ..
            } => (read_ctr.clone(), written_ctr.clone()),
            Connected::Tls {
                read_ctr,
                written_ctr,
                ..
            } => (read_ctr.clone(), written_ctr.clone()),
        }
    }

    /// ALPN protocol negotiated during the TLS handshake, or `None` for
    /// plain HTTP, for TLS connections that passed an empty ALPN list,
    /// or for servers that didn't pick anything.
    pub fn negotiated_alpn(&self) -> Option<&[u8]> {
        match self {
            Connected::Plain { .. } => None,
            Connected::Tls {
                negotiated_alpn, ..
            } => negotiated_alpn.as_deref(),
        }
    }
}

/// Open one connection against `target` honouring `opts`.
///
/// Steps:
/// 1. Resolve + TCP connect, honouring `connect_timeout`.
/// 2. Apply TCP_NODELAY per `opts.tcp_nodelay`.
/// 3. Wrap in [`CountingStream`].
/// 4. If `target.tls`, perform a rustls handshake via `compio-tls` with
///    the supplied `alpn` list. The server's ALPN choice is surfaced on
///    the returned `Connected::Tls.negotiated_alpn`.
///
/// `alpn` is ignored for non-TLS targets. Typical values:
///
/// - `&[b"h2", b"http/1.1"]` — HTTP `Auto`, prefer H2, fall back to H1.
/// - `&[b"http/1.1"]`        — HTTP force-H1.
/// - `&[b"h2"]`              — HTTP force-H2.
/// - `&[]`                   — WebSocket (wss://) and SSE; no ALPN.
pub async fn open(
    target: &Target,
    opts: &TransportOpts,
    alpn: &[&[u8]],
) -> Result<Connected, TransportError> {
    let addr = target.addr();

    // TCP connect with deadline. compio's `time::timeout` drops the
    // connect future on expiry, which aborts the underlying io_uring
    // op via Drop (io_uring cancel).
    let tcp = compio::time::timeout(opts.connect_timeout, TcpStream::connect(&addr))
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::Connect(format!("{addr}: {e}")))?;

    if opts.tcp_nodelay {
        // Failure here is benign — best-effort NODELAY doesn't justify
        // aborting the connection.
        let _ = tcp.set_nodelay(true);
    }

    let counted = CountingStream::new(tcp);
    let (read_ctr, written_ctr) = counted.counts();

    if target.tls {
        // Build the client config with the caller-supplied ALPN list.
        let cfg = tls_client_config(opts, alpn);
        let connector = compio_tls::TlsConnector::from(cfg);
        // SNI hostname: explicit override if present, otherwise the
        // connect hostname. rustls accepts DNS names and IP literals
        // uniformly via `ServerName::try_from(&str)` internally (called
        // inside `compio-tls`); no extra branching needed here.
        let server_name = target.sni_name().to_string();

        // Handshake with a deadline — use the same `connect_timeout`
        // that we applied to the TCP connect. A TLS handshake is a
        // small number of RTTs; if it outruns the TCP budget the
        // target is almost certainly unreachable anyway.
        let tls_stream = compio::time::timeout(
            opts.connect_timeout,
            connector.connect(&server_name, counted),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::Tls(format!("handshake: {e}")))?;

        let negotiated_alpn = tls_stream
            .negotiated_alpn()
            .map(|cow| cow.into_owned());

        Ok(Connected::Tls {
            stream: Box::new(tls_stream),
            read_ctr,
            written_ctr,
            negotiated_alpn,
        })
    } else {
        Ok(Connected::Plain {
            stream: counted,
            read_ctr,
            written_ctr,
        })
    }
}
