//! Tokio-native TCP / TLS connection helpers.
//!
//! Mirrors the compio `conn.rs` but uses `tokio::net::TcpStream` and
//! `tokio-rustls` — no `HyperStream` bridge, no `CountingStream`.
//!
//! # Byte counting
//!
//! v1 of the tokio backend skips per-request byte counting. The compio
//! backend wraps a `CountingStream` below TLS; doing the same on tokio
//! would require an `AsyncRead + AsyncWrite` wrapper that satisfies
//! hyper's `hyper::rt::Read + hyper::rt::Write` via `hyper-util`'s
//! `TokioIo` bridge. Wiring that is straightforward but deferred — the
//! primary goal is latency / throughput measurement, not byte accounting.

use tokio::net::TcpStream;
use zerobench_core::tls::tls_client_config;
use zerobench_core::transport::{Target, TransportError, TransportOpts};

/// A freshly-opened tokio connection, ready to hand to hyper.
pub enum ConnectedTokio {
    /// Plain TCP (no TLS).
    Plain(TcpStream),
    /// HTTPS — `tokio_rustls::client::TlsStream` over `TcpStream`.
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

/// Open one TCP (+TLS) connection using tokio.
pub async fn open_tokio(
    target: &Target,
    opts: &TransportOpts,
    alpn: &[&[u8]],
) -> Result<ConnectedTokio, TransportError> {
    let addr = target.addr();

    let tcp = tokio::time::timeout(
        opts.connect_timeout,
        TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(|e| TransportError::Connect(format!("{addr}: {e}")))?;

    if opts.tcp_nodelay {
        let _ = tcp.set_nodelay(true);
    }

    if target.tls {
        let cfg = tls_client_config(opts, alpn);
        let connector = tokio_rustls::TlsConnector::from(cfg);
        let server_name = rustls::pki_types::ServerName::try_from(target.sni_name().to_string())
            .map_err(|e| TransportError::Tls(format!("invalid server name: {e}")))?;

        let tls_stream = tokio::time::timeout(
            opts.connect_timeout,
            connector.connect(server_name, tcp),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::Tls(format!("handshake: {e}")))?;

        Ok(ConnectedTokio::Tls(tls_stream))
    } else {
        Ok(ConnectedTokio::Plain(tcp))
    }
}
