//! Transport runtime error taxonomy.
//!
//! `Target`, `TransportOpts`, `HttpVersionPref`, `AddrFamily`, and
//! `TargetError` are plan-adjacent vocabulary and live in
//! `zerobench_core::transport`. Only `TransportError` — the *runtime*
//! failure taxonomy every wire-layer transport produces — lives here.
//!
//! Variants map one-to-one onto `zerobench_core::stats::ErrorKind`
//! counters so the dispatcher can roll them up without re-inspecting
//! the error (see `ARCH(error-unify)` in `docs/ARCH-REVIEW-2026-04-20.md`
//! §4.7).
//!
//! A follow-up phase introduces `classify(&TransportError) -> ErrorKind`
//! here; for now each backend maps variants inline.

/// The error type every transport backend uses.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// TCP/TLS connect failed. The contained string carries the
    /// underlying cause (DNS failure, ECONNREFUSED, TLS handshake
    /// reject, etc) — we don't split them further because the
    /// benchmark reporter collapses them into a single counter anyway.
    #[error("connect failed: {0}")]
    Connect(String),

    /// A deadline fired. Covers both connect-timeout and
    /// request-timeout per `zerobench_core::transport::TransportOpts`.
    #[error("timeout")]
    Timeout,

    /// Protocol-level error — header parsing, frame decode, invalid
    /// Content-Length, etc.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Bare IO error bubbled up from the socket. Autoconverted from
    /// `std::io::Error` so transport impls can use `?` freely.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Template expansion, header construction, or body encoding
    /// produced an invalid request before anything reached the wire.
    /// Treated as fatal — a broken plan isn't retryable.
    #[error("request build failed: {0}")]
    RequestBuild(String),

    /// TLS-specific failure (certificate rejection, ALPN mismatch,
    /// handshake abort). Split from [`TransportError::Connect`] so the
    /// reporter can surface TLS issues distinctly when we care to.
    #[error("tls error: {0}")]
    Tls(String),
}
