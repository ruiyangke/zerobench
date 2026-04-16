//! The transport abstraction.
//!
//! A [`Transport`] knows how to turn a [`crate::plan::RequestPlan`] into a
//! real wire exchange. Concrete implementations live in sibling crates
//! (`zerobench-http` for HTTP/1/2, `zerobench-ws` for WebSocket,
//! `zerobench-sse` for Server-Sent Events); they share this trait so the
//! engine layer can dispatch uniformly.
//!
//! # Design
//!
//! - [`Target`] describes the remote endpoint (host, port, TLS).
//! - [`TransportOpts`] carries the knobs shared across all transports
//!   (timeouts, pool size, TCP_NODELAY, TLS-insecure toggle).
//! - [`Response`] is a `Transport`'s return value — status, headers, a
//!   body (buffered or streamed), and the four numbers every benchmark
//!   ultimately cares about: bytes sent, bytes received, TTFB, total
//!   duration.
//! - [`ScenarioContext`] is re-exported here from
//!   [`crate::scenario_context`] for ergonomic imports.
//!
//! # `Send` and `async fn` in traits
//!
//! We use `async fn` in trait (stabilised in Rust 1.75) and **do not**
//! require the returned future to be `Send`. Rationale: compio's runtime
//! is strictly single-threaded per worker — each worker thread runs its
//! own runtime and its own per-thread `Transport::Client` — so futures
//! never cross threads. The [`Transport::Client`] itself *is* bounded as
//! `Clone + Send + 'static` so the plan-build step (on a control thread)
//! can move a client handle into each worker.
//!
//! If a future transport backend needs a multi-threaded runtime, we can
//! add a `Send` bound to `exchange` at that point — but `async fn` in
//! trait makes that addition a non-breaking change for existing impls.

use std::fmt;
use std::time::Duration;

use http::HeaderMap;

use crate::plan::RequestPlan;
use crate::scenario_context::ScenarioContext;

// ---------------------------------------------------------------------------
// Target
// ---------------------------------------------------------------------------

/// Where to open connections.
///
/// Constructed from a URL-ish string via [`Target::parse`], or built
/// directly by front-ends that already have the parts (Rhai scripts,
/// request-file parser). The URL's path / query / fragment are *not*
/// part of the target — those belong on [`RequestPlan::url`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// Server hostname or IP literal (no surrounding brackets on IPv6).
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Negotiate TLS on connect.
    pub tls: bool,
    /// SNI override. Defaults to `host` when unset; front-ends set this
    /// when the TCP host and the TLS certificate subject differ (e.g.
    /// connecting to an internal load-balancer by IP but validating
    /// against a public certificate).
    pub sni: Option<String>,
}

impl Target {
    /// Parse a URL-ish string into a target.
    ///
    /// Accepted shapes:
    ///
    /// - `http://host`                      — port defaults to 80
    /// - `http://host:port`
    /// - `https://host`                     — port defaults to 443
    /// - `https://host:port/path?query`     — path/query ignored
    ///
    /// The path, query, and fragment are deliberately discarded — those
    /// belong on the [`RequestPlan`], not on the connection target.
    pub fn parse(url: &str) -> Result<Self, TargetError> {
        // Scheme.
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| TargetError::InvalidUrl(url.to_string()))?;
        let tls = match scheme {
            "http" | "ws" => false,
            "https" | "wss" => true,
            other => {
                return Err(TargetError::InvalidUrl(format!(
                    "unsupported scheme: {other}"
                )));
            }
        };

        // Strip path/query/fragment — the authority is whatever precedes
        // the first '/', '?', or '#'.
        let authority_end = rest
            .find(|c: char| c == '/' || c == '?' || c == '#')
            .unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        if authority.is_empty() {
            return Err(TargetError::MissingHost);
        }

        // Split host:port, handling IPv6 literals in brackets.
        let (host_str, port_str) = if let Some(stripped) = authority.strip_prefix('[') {
            // IPv6 literal — `[addr]` or `[addr]:port`.
            let close = stripped
                .find(']')
                .ok_or_else(|| TargetError::InvalidUrl(url.to_string()))?;
            let host = &stripped[..close];
            let after = &stripped[close + 1..];
            let port = after
                .strip_prefix(':')
                .map(Some)
                .unwrap_or(None)
                .map(|p| p.to_string());
            (host.to_string(), port)
        } else {
            // IPv4 or DNS name — split on the last ':' (only one expected).
            match authority.rsplit_once(':') {
                Some((h, p)) => (h.to_string(), Some(p.to_string())),
                None => (authority.to_string(), None),
            }
        };

        if host_str.is_empty() {
            return Err(TargetError::MissingHost);
        }

        let port = match port_str {
            Some(s) => s
                .parse::<u16>()
                .map_err(|_| TargetError::InvalidPort(s))?,
            None => {
                if tls {
                    443
                } else {
                    80
                }
            }
        };

        Ok(Self {
            host: host_str,
            port,
            tls,
            sni: None,
        })
    }

    /// Returns the authority portion `host:port` suitable for a `Host`
    /// header or as the address argument to `TcpStream::connect`. IPv6
    /// hosts are bracketed to match standard URL/authority syntax.
    pub fn addr(&self) -> String {
        if self.host.contains(':') {
            // IPv6 literal — bracket per RFC 3986 §3.2.2.
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// SNI hostname to use when performing the TLS handshake. Returns
    /// the override if set, otherwise the connect hostname.
    pub fn sni_name(&self) -> &str {
        self.sni.as_deref().unwrap_or(&self.host)
    }
}

/// Errors returned by [`Target::parse`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TargetError {
    /// The URL couldn't be parsed — missing `://`, unsupported scheme,
    /// malformed IPv6 bracket, etc.
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    /// The URL had a scheme but no authority (e.g. `http:///path`).
    #[error("missing host")]
    MissingHost,
    /// The port component wouldn't parse as a `u16`.
    #[error("invalid port: {0}")]
    InvalidPort(String),
}

// ---------------------------------------------------------------------------
// TransportOpts
// ---------------------------------------------------------------------------

/// Tunables shared across all transport implementations.
///
/// Concrete transports may accept additional, protocol-specific options
/// (e.g. `Http1Pool` will want `h1_max_headers`), but these are the five
/// that every transport honours.
#[derive(Debug, Clone)]
pub struct TransportOpts {
    /// Maximum time to wait for a single TCP (and, if applicable, TLS)
    /// connect to complete.
    pub connect_timeout: Duration,
    /// Per-request deadline, applied by the dispatcher. Transports may
    /// use it too (e.g. for the HTTP/2 stream-open timeout).
    pub request_timeout: Duration,
    /// Maximum concurrent connections in the pool (HTTP/1) or maximum
    /// concurrent streams (HTTP/2 multiplexed over one connection).
    pub max_conns: usize,
    /// Disable Nagle's algorithm. Default `true` because latency
    /// benchmarks want minimum write latency.
    pub tcp_nodelay: bool,
    /// Accept invalid TLS certificates (self-signed, expired, hostname
    /// mismatch). Only wired in through `compio-tls` / `rustls` — users
    /// who need it opt in explicitly with `-k` / `--insecure`.
    pub insecure_tls: bool,
    /// Preferred HTTP protocol version for the client side.
    ///
    /// For v0.0.1 this is honoured by `HttpTransport` only. On plain
    /// HTTP, [`HttpVersionPref::Auto`] picks H1 (H2 cleartext requires
    /// explicit opt-in because servers don't advertise). On HTTPS, Auto
    /// will try to negotiate H2 via ALPN and fall back to H1 if the
    /// server only offers it — but TLS + ALPN wiring is deferred beyond
    /// Phase E and Auto-on-HTTPS today resolves to H1 until the TLS
    /// path lands.
    pub http_version: HttpVersionPref,
}

impl Default for TransportOpts {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_conns: 100,
            tcp_nodelay: true,
            insecure_tls: false,
            http_version: HttpVersionPref::Auto,
        }
    }
}

/// User preference for the HTTP wire protocol version.
///
/// The enum carries *all* variants regardless of feature flags so
/// downstream code (the CLI parser, front-ends) can use a single type;
/// `HttpTransport::build_client` returns [`TransportError::Protocol`] if
/// the request variant isn't wired up in the current build (e.g. `Http2`
/// was requested but the `h2` feature wasn't enabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpVersionPref {
    /// Let the transport pick. HTTP → H1, HTTPS → ALPN negotiation
    /// (currently resolves to H1 until TLS+ALPN lands).
    #[default]
    Auto,
    /// Force HTTP/1.1. Always available.
    Http1,
    /// Force HTTP/2. Available when the `h2` feature is compiled into
    /// `zerobench-http`; the dispatcher surfaces a clear error if the
    /// feature is absent rather than silently downgrading.
    Http2,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// A completed exchange.
///
/// Every field is populated before `exchange` returns. `bytes_sent` and
/// `bytes_received` are the **on-wire** counts (post-TLS-encrypt /
/// pre-TLS-decrypt if applicable), measured by a `CountingStream`
/// sitting beneath hyper.
pub struct Response {
    /// Numeric status code — pulled directly from `http::StatusCode`
    /// so callers don't need a dependency on `http` just to check it.
    pub status: u16,
    /// Response headers verbatim. Hyper guarantees header names are
    /// lowercased, but values are preserved as received.
    pub headers: HeaderMap,
    /// Response body — buffered for short bodies, streamed for SSE/WS.
    pub body: ResponseBody,
    /// On-wire bytes written for this exchange (request line + headers
    /// + body + any framing).
    pub bytes_sent: u64,
    /// On-wire bytes read for this exchange (status line + headers +
    /// body + any framing).
    pub bytes_received: u64,
    /// Time from sending the first byte to receiving the first byte of
    /// the response's status line.
    pub ttfb: Duration,
    /// Total time from sending the first byte to the last byte of the
    /// response body being consumed.
    pub total: Duration,
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("body", &self.body)
            .field("bytes_sent", &self.bytes_sent)
            .field("bytes_received", &self.bytes_received)
            .field("ttfb", &self.ttfb)
            .field("total", &self.total)
            .finish()
    }
}

/// Response body — either fully collected bytes or a stream of chunks.
pub enum ResponseBody {
    /// Fully collected body, read into memory before `exchange` returns.
    /// This is what all v0.0.1 HTTP transports produce; SSE and WS
    /// transports will use [`ResponseBody::Stream`].
    Buffered(bytes::Bytes),
    /// Incrementally produced body chunks. The stream is `!Send` because
    /// it typically wraps compio IO types, which aren't `Send`. compio
    /// runs a dedicated runtime per worker thread; the stream is produced
    /// and consumed on the same thread, so a `Send` bound would only
    /// rule out correct implementations.
    Stream(
        std::pin::Pin<
            Box<dyn futures_util::Stream<Item = std::io::Result<bytes::Bytes>>>,
        >,
    ),
}

impl ResponseBody {
    /// `true` iff this is a [`ResponseBody::Buffered`] variant with zero
    /// bytes. Streaming bodies return `false` regardless of whether the
    /// stream will ultimately yield any chunks — we can't know without
    /// consuming it.
    pub fn is_empty(&self) -> bool {
        match self {
            ResponseBody::Buffered(b) => b.is_empty(),
            ResponseBody::Stream(_) => false,
        }
    }

    /// Length of a [`ResponseBody::Buffered`] body, or `None` for a
    /// streaming body (Content-Length may not be authoritative on
    /// chunked transfer-encoding).
    pub fn len(&self) -> Option<usize> {
        match self {
            ResponseBody::Buffered(b) => Some(b.len()),
            ResponseBody::Stream(_) => None,
        }
    }
}

impl fmt::Debug for ResponseBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseBody::Buffered(b) => f
                .debug_struct("ResponseBody::Buffered")
                .field("len", &b.len())
                .finish(),
            ResponseBody::Stream(_) => f
                .debug_struct("ResponseBody::Stream")
                .finish_non_exhaustive(),
        }
    }
}

// ---------------------------------------------------------------------------
// Transport trait
// ---------------------------------------------------------------------------

/// The abstraction every concrete transport implements.
///
/// Implementors:
///
/// - [`HttpTransport`](../../zerobench_http/struct.HttpTransport.html)
///   for HTTP/1 (and, gated behind `h2`, HTTP/2).
/// - `WsTransport` and `SseTransport` in their respective crates.
///
/// Each transport's `Client` is a cheap-to-clone handle (typically
/// `Arc<Pool>`) holding protocol-specific state. The dispatcher clones
/// one handle per worker; workers share the pool but own their own
/// [`ScenarioContext`].
pub trait Transport: Send + 'static {
    /// Cheap-to-clone per-thread client handle. Bounded `Clone + Send +
    /// 'static` so it can be moved onto worker threads; the underlying
    /// data structures are free to be thread-local (typical: `Arc<Pool>`
    /// where `Pool` lives in a single thread's runtime).
    type Client: Clone + Send + 'static;

    /// Pre-open the pool (or otherwise prepare to send requests) against
    /// `target` with the given `opts`. Must not return until the client
    /// is ready to accept an `exchange` call.
    ///
    /// Returns [`TransportError::Connect`] on the first fatal connect
    /// failure — partial pools (some slots failed) are an implementation
    /// choice left to each concrete transport.
    fn build_client(
        target: &Target,
        opts: &TransportOpts,
    ) -> impl std::future::Future<Output = Result<Self::Client, TransportError>>;

    /// Execute one exchange: send the request described by `plan`,
    /// receive the response, and populate a [`Response`]. `ctx` is
    /// threaded through template expansion (URL, headers, body) and
    /// is available to extractors if the transport performs any.
    fn exchange(
        client: &Self::Client,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> impl std::future::Future<Output = Result<Response, TransportError>>;
}

// ---------------------------------------------------------------------------
// TransportError
// ---------------------------------------------------------------------------

/// The error type every `Transport` uses.
///
/// Variants map one-to-one onto the [`crate::stats::ErrorKind`] counters
/// so the dispatcher can roll them up without re-inspecting the error.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// TCP/TLS connect failed. The contained string carries the
    /// underlying cause (DNS failure, ECONNREFUSED, TLS handshake
    /// reject, etc) — we don't split them further because the
    /// benchmark reporter collapses them into a single counter anyway.
    #[error("connect failed: {0}")]
    Connect(String),

    /// A deadline fired. Covers both connect-timeout and
    /// request-timeout per [`TransportOpts`].
    #[error("timeout")]
    Timeout,

    /// Protocol-level error from hyper / h3 / compio-ws — header
    /// parsing, frame decode, invalid Content-Length, etc.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_localhost_with_port() {
        let t = Target::parse("http://localhost:8080").unwrap();
        assert_eq!(t.host, "localhost");
        assert_eq!(t.port, 8080);
        assert!(!t.tls);
        assert_eq!(t.sni_name(), "localhost");
    }

    #[test]
    fn parse_https_default_port() {
        let t = Target::parse("https://api.example.com/foo").unwrap();
        assert_eq!(t.host, "api.example.com");
        assert_eq!(t.port, 443);
        assert!(t.tls);
    }

    #[test]
    fn parse_http_default_port() {
        let t = Target::parse("http://example.com/foo").unwrap();
        assert_eq!(t.host, "example.com");
        assert_eq!(t.port, 80);
        assert!(!t.tls);
    }

    #[test]
    fn parse_https_host_only_with_port() {
        let t = Target::parse("https://x:9000").unwrap();
        assert_eq!(t.host, "x");
        assert_eq!(t.port, 9000);
        assert!(t.tls);
    }

    #[test]
    fn parse_ignores_path_and_query() {
        let t = Target::parse("http://h.example:1234/a/b?c=d#e").unwrap();
        assert_eq!(t.host, "h.example");
        assert_eq!(t.port, 1234);
    }

    #[test]
    fn parse_ws_and_wss() {
        let ws = Target::parse("ws://h:8080").unwrap();
        assert!(!ws.tls);
        assert_eq!(ws.port, 8080);

        let wss = Target::parse("wss://h").unwrap();
        assert!(wss.tls);
        assert_eq!(wss.port, 443);
    }

    #[test]
    fn parse_ipv6_literal() {
        let t = Target::parse("http://[::1]:8080").unwrap();
        assert_eq!(t.host, "::1");
        assert_eq!(t.port, 8080);
        assert_eq!(t.addr(), "[::1]:8080");
    }

    #[test]
    fn parse_rejects_missing_scheme() {
        assert!(matches!(
            Target::parse("not-a-url"),
            Err(TargetError::InvalidUrl(_))
        ));
    }

    #[test]
    fn parse_rejects_unsupported_scheme() {
        assert!(matches!(
            Target::parse("ftp://example.com"),
            Err(TargetError::InvalidUrl(_))
        ));
    }

    #[test]
    fn parse_rejects_empty_authority() {
        assert!(matches!(
            Target::parse("http:///path"),
            Err(TargetError::MissingHost)
        ));
    }

    #[test]
    fn parse_rejects_non_numeric_port() {
        assert!(matches!(
            Target::parse("http://host:abc"),
            Err(TargetError::InvalidPort(_))
        ));
    }

    #[test]
    fn parse_rejects_port_overflow() {
        assert!(matches!(
            Target::parse("http://host:99999"),
            Err(TargetError::InvalidPort(_))
        ));
    }

    #[test]
    fn addr_formats_host_port() {
        let t = Target {
            host: "example.com".into(),
            port: 443,
            tls: true,
            sni: None,
        };
        assert_eq!(t.addr(), "example.com:443");
    }

    #[test]
    fn sni_override_is_honoured() {
        let t = Target {
            host: "10.0.0.5".into(),
            port: 443,
            tls: true,
            sni: Some("api.example.com".into()),
        };
        assert_eq!(t.sni_name(), "api.example.com");
    }

    #[test]
    fn transport_opts_default_values() {
        let o = TransportOpts::default();
        assert_eq!(o.connect_timeout, Duration::from_secs(5));
        assert_eq!(o.request_timeout, Duration::from_secs(30));
        assert_eq!(o.max_conns, 100);
        assert!(o.tcp_nodelay);
        assert!(!o.insecure_tls);
        assert_eq!(o.http_version, HttpVersionPref::Auto);
    }

    #[test]
    fn http_version_pref_default_is_auto() {
        assert_eq!(HttpVersionPref::default(), HttpVersionPref::Auto);
    }

    #[test]
    fn response_body_buffered_len_and_empty() {
        let b = ResponseBody::Buffered(bytes::Bytes::from_static(b"hello"));
        assert_eq!(b.len(), Some(5));
        assert!(!b.is_empty());

        let e = ResponseBody::Buffered(bytes::Bytes::new());
        assert_eq!(e.len(), Some(0));
        assert!(e.is_empty());
    }

    #[test]
    fn response_body_stream_len_is_none() {
        let stream = futures_util::stream::empty::<std::io::Result<bytes::Bytes>>();
        let b = ResponseBody::Stream(Box::pin(stream));
        assert_eq!(b.len(), None);
        // Streams can't report empty because we don't want to consume them.
        assert!(!b.is_empty());
    }
}
