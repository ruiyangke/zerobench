//! Transport runtime error taxonomy.
//!
//! `Target`, `TransportOpts`, `HttpVersionPref`, `AddrFamily`, and
//! `TargetError` are plan-adjacent vocabulary and live in
//! `zerobench_core::transport`. Only `TransportError` — the *runtime*
//! failure taxonomy every wire-layer transport produces — lives here.
//!
//! Every backend op path funnels into one of the seven variants below,
//! and [`classify`] is the single canonical mapping from a
//! `TransportError` to the upstream [`ErrorKind`] counter taxonomy used
//! by `TaskStats`/`Recorder`. No protocol crate rolls its own match →
//! ErrorKind arm; they all call [`classify`].
//!
//! See `docs/ARCH-REVIEW-2026-04-20.md` §4.7.

use zerobench_core::stats::ErrorKind;

/// The error type every transport backend uses.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// TCP / DNS / socket-open failed. The contained string carries the
    /// underlying cause (DNS failure, ECONNREFUSED, bind error, etc).
    #[error("connect failed: {0}")]
    Connect(String),

    /// TLS-specific failure (certificate rejection, ALPN mismatch,
    /// handshake abort). Split from [`TransportError::Connect`] so the
    /// reporter can surface TLS issues distinctly; `classify` folds
    /// them back into [`ErrorKind::Connect`] because a failed TLS
    /// handshake is, from the user's perspective, a failure to
    /// establish the connection.
    #[error("tls error: {0}")]
    Tls(String),

    /// A deadline fired at any phase — connect, handshake, write, read.
    /// Covers `TransportOpts::connect_timeout` and
    /// `TransportOpts::request_timeout`.
    #[error("timeout")]
    Timeout,

    /// Socket write failed after connect. Carries the underlying
    /// `io::Error` rendered as a string so callers don't need to match
    /// on an `io::ErrorKind` variant.
    #[error("write failed: {0}")]
    Write(String),

    /// Socket read failed after connect. Same string-only rendering as
    /// [`TransportError::Write`].
    #[error("read failed: {0}")]
    Read(String),

    /// Response framing / protocol violation — bad HTTP/1 headers, bad
    /// WebSocket frame, malformed SSE line, unexpected EOF before
    /// headers, etc. Distinct from [`TransportError::Read`] because the
    /// underlying socket is fine; it's the peer that misbehaved.
    /// `classify` folds to [`ErrorKind::Read`] (the server-side side of
    /// the read).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Pre-flight request build failed — template expansion, invalid
    /// URL, header construction. Client-side fault; treated as fatal
    /// because a broken plan isn't retryable. `classify` folds to
    /// [`ErrorKind::Connect`] as a conservative default for
    /// phase-unclear failures.
    #[error("request build failed: {0}")]
    RequestBuild(String),
}

/// Canonical mapping from transport-level errors to the upstream
/// counter taxonomy.
///
/// Every backend uses this; there are no hand-rolled `match` →
/// `ErrorKind` arms in the protocol crates. Keep the mapping stable —
/// the reporter surfaces these counters and dashboards downstream depend
/// on the categorisation.
pub fn classify(e: &TransportError) -> ErrorKind {
    match e {
        // TLS is part of "establish the connection" from the caller's
        // perspective — fold to Connect.
        TransportError::Connect(_) | TransportError::Tls(_) => ErrorKind::Connect,
        TransportError::Timeout => ErrorKind::Timeout,
        TransportError::Write(_) => ErrorKind::Write,
        // Protocol framing violations show up on the read path — the
        // socket is fine but the peer sent garbage. Fold to Read.
        TransportError::Read(_) | TransportError::Protocol(_) => ErrorKind::Read,
        // Client-side pre-flight — no wire activity yet — conservative
        // default is Connect.
        TransportError::RequestBuild(_) => ErrorKind::Connect,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_connect() {
        assert_eq!(
            classify(&TransportError::Connect("refused".into())),
            ErrorKind::Connect
        );
    }

    #[test]
    fn classify_tls_is_connect() {
        assert_eq!(
            classify(&TransportError::Tls("cert reject".into())),
            ErrorKind::Connect
        );
    }

    #[test]
    fn classify_timeout() {
        assert_eq!(classify(&TransportError::Timeout), ErrorKind::Timeout);
    }

    #[test]
    fn classify_write() {
        assert_eq!(
            classify(&TransportError::Write("broken pipe".into())),
            ErrorKind::Write
        );
    }

    #[test]
    fn classify_read() {
        assert_eq!(
            classify(&TransportError::Read("reset".into())),
            ErrorKind::Read
        );
    }

    #[test]
    fn classify_protocol_is_read() {
        assert_eq!(
            classify(&TransportError::Protocol("bad headers".into())),
            ErrorKind::Read
        );
    }

    #[test]
    fn classify_request_build_is_connect() {
        assert_eq!(
            classify(&TransportError::RequestBuild("invalid url".into())),
            ErrorKind::Connect
        );
    }
}
