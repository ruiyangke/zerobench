//! Shared TCP / TLS connection helpers used by every HTTP transport.
//!
//! Opens a raw TCP socket against a [`Target`], optionally negotiates
//! TLS via `compio-tls`, and wraps the final socket in
//! [`CountingStream`] so wire bytes are tracked regardless of the
//! transport that will use the connection.
//!
//! The returned [`Connected`] enum is `match`ed by the caller so each
//! transport can build a hyper / h3 / ws stack on top. We don't box the
//! stream into a trait object — keeping it concrete avoids dynamic
//! dispatch on the hot path and, more practically, sidesteps the thorny
//! `Send + Sync + Unpin + 'static` bound soup `dyn AsyncRead + AsyncWrite`
//! would require.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use compio::net::TcpStream;
use compio_tls::TlsStream;
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
/// `compio-tls::TlsStream` doesn't expose its inner stream.
pub enum Connected {
    /// Plain TCP with no TLS. `CountingStream` sits directly on the
    /// compio `TcpStream`.
    Plain(CountingStream<TcpStream>),
    /// HTTPS — `TlsStream` on top of a counted `TcpStream`. TLS encrypt
    /// happens above the counters, so they reflect encrypted wire bytes.
    /// The `counts` pair is held separately because `TlsStream` doesn't
    /// surface its inner stream.
    Tls {
        stream: TlsStream<CountingStream<TcpStream>>,
        read_ctr: Arc<AtomicU64>,
        written_ctr: Arc<AtomicU64>,
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
            Connected::Plain(s) => s.counts(),
            Connected::Tls {
                read_ctr,
                written_ctr,
                ..
            } => (read_ctr.clone(), written_ctr.clone()),
        }
    }
}

/// Open one connection against `target` honouring `opts`.
///
/// Steps:
/// 1. Resolve + TCP connect, honouring `connect_timeout`.
/// 2. Apply TCP_NODELAY per `opts.tcp_nodelay`.
/// 3. Wrap in [`CountingStream`].
/// 4. Negotiate TLS if `target.tls`.
///
/// TLS path is stubbed for v0.0.1 Phase B — it returns a clear error
/// pointing at the task that will wire rustls config. Plain HTTP is
/// fully functional.
pub async fn open(target: &Target, opts: &TransportOpts) -> Result<Connected, TransportError> {
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

    if target.tls {
        // TLS is deferred to a future phase — compio-tls wants a
        // `rustls::ClientConfig` and we haven't wired the config
        // plumbing (insecure toggle, client certs, SNI override) yet.
        // Plain HTTP is fully supported in this release.
        let _ = target.sni_name(); // suppress "unused" warning if we ever drop sni_name
        Err(TransportError::Tls(
            "TLS support not wired in Phase B; use http:// targets for now".into(),
        ))
    } else {
        Ok(Connected::Plain(counted))
    }
}
