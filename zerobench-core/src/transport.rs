//! Transport vocabulary — target, options, address-family preference.
//!
//! - [`Target`] describes the remote endpoint (host, port, TLS).
//! - [`TransportOpts`] carries the knobs shared across all transports
//!   (timeouts, pool size, TCP_NODELAY, TLS-insecure toggle).
//! - [`TargetError`] is returned by [`Target::parse`] only — it's a
//!   plan-construction error, not a runtime error.
//!
//! The *runtime* error taxonomy every backend produces
//! (`TransportError`: connect/timeout/protocol/io/build/tls) lives in
//! `zerobench_runtime::transport` because it's a runtime concern and
//! `zerobench-core` is type-vocabulary only.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Target
// ---------------------------------------------------------------------------

/// Preferred address family when resolving a hostname.
///
/// Dual-stack hosts like `localhost` resolve to both `127.0.0.1` and `::1`;
/// the order of the returned list is resolver- and libc-dependent, so we
/// let callers pin the family explicitly when it matters.
///
/// * `Any` — take whichever address the resolver returned first (current
///   behaviour; reasonable default).
/// * `V4` — return the first IPv4 address, error if none.
/// * `V6` — return the first IPv6 address, error if none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AddrFamily {
    /// No preference — first address the resolver returned wins.
    #[default]
    Any,
    /// Only IPv4 addresses are acceptable.
    V4,
    /// Only IPv6 addresses are acceptable.
    V6,
}

/// Where to open connections.
///
/// Constructed from a URL-ish string via [`Target::parse`], or built
/// directly by front-ends that already have the parts (Rhai scripts,
/// request-file parser). The URL's path / query / fragment are *not*
/// part of the target — those belong on [`crate::plan::RequestPlan::url`].
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
    /// Preferred address family for DNS resolution. Defaults to
    /// [`AddrFamily::Any`]; set to [`AddrFamily::V4`] / [`AddrFamily::V6`]
    /// to pin the family when the resolver returns a dual-stack mix.
    pub addr_family: AddrFamily,
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
    /// belong on the [`crate::plan::RequestPlan`], not on the connection target.
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
            addr_family: AddrFamily::Any,
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

    /// Resolve the target to a `SocketAddr`, honouring the optional
    /// `--resolve HOST:PORT:ADDR` overrides from `opts` and the
    /// address-family preference on `self`.
    ///
    /// Resolution order:
    ///
    /// 1. If `opts.resolve_overrides` contains a tuple matching
    ///    `(self.host, self.port)`, the `addr` string from that tuple is
    ///    parsed as an IP literal and used directly — no system DNS call.
    ///    This mirrors `curl --resolve` and is the primary escape hatch
    ///    for benchmarking behind split-horizon DNS.
    /// 2. Otherwise the system resolver is consulted and the first
    ///    address matching `self.addr_family` is returned.
    ///
    /// Returns [`std::io::ErrorKind::NotFound`] / [`std::io::ErrorKind::AddrNotAvailable`]
    /// when no address matches the request — callers map this to
    /// `zerobench_runtime::transport::TransportError::Connect` at the wire layer.
    pub fn resolve(&self, opts: &TransportOpts) -> std::io::Result<SocketAddr> {
        use std::net::ToSocketAddrs;

        // 1. Check resolve_overrides first — curl-style --resolve.
        for (host, port, addr) in opts.resolve_overrides.iter() {
            if host.eq_ignore_ascii_case(&self.host) && *port == self.port {
                let ip: IpAddr = addr.parse().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid --resolve address literal: {addr}"),
                    )
                })?;
                return Ok(SocketAddr::new(ip, self.port));
            }
        }

        // 2. Fall back to system resolution with address-family filter.
        let addrs = self.addr().to_socket_addrs()?;
        let picked = match self.addr_family {
            AddrFamily::Any => addrs.into_iter().next(),
            AddrFamily::V4 => addrs.into_iter().find(|a| a.is_ipv4()),
            AddrFamily::V6 => addrs.into_iter().find(|a| a.is_ipv6()),
        };
        picked.ok_or_else(|| {
            let kind = match self.addr_family {
                AddrFamily::Any => std::io::ErrorKind::AddrNotAvailable,
                _ => std::io::ErrorKind::NotFound,
            };
            std::io::Error::new(
                kind,
                format!(
                    "DNS resolution returned no {} addresses for {}",
                    match self.addr_family {
                        AddrFamily::Any => "",
                        AddrFamily::V4 => "IPv4 ",
                        AddrFamily::V6 => "IPv6 ",
                    },
                    self.addr()
                ),
            )
        })
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
    /// mismatch). Only wired through `rustls` — users
    /// who need it opt in explicitly with `-k` / `--insecure`.
    pub insecure_tls: bool,
    /// Preferred HTTP protocol version for the client side.
    ///
    /// Honoured by `HttpTransport` only. On plain HTTP,
    /// [`HttpVersionPref::Auto`] picks H1 (H2 cleartext requires
    /// explicit opt-in because servers don't advertise). On HTTPS, Auto
    /// performs an ALPN probe (`h2, http/1.1`) and picks the protocol
    /// the server chose — H2 if offered, H1 otherwise.
    pub http_version: HttpVersionPref,
    /// curl-compatible `--resolve HOST:PORT:ADDR` overrides.
    ///
    /// Each tuple `(host, port, addr)` tells the transport "when asked
    /// to connect to `host:port`, skip DNS and use `addr` instead".
    /// Empty when no overrides were passed. The resolver lookup is
    /// performed in the transport layer; the CLI only parses and
    /// forwards the list.
    pub resolve_overrides: Vec<(String, u16, String)>,
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
            resolve_overrides: Vec::new(),
        }
    }
}

/// User preference for the HTTP wire protocol version.
///
/// The enum carries *all* variants regardless of feature flags so
/// downstream code (the CLI parser, front-ends) can use a single type;
/// `HttpTransport::build_client` returns a `TransportError::Protocol` if
/// the request variant isn't wired up in the current build (e.g. `Http2`
/// was requested but the `h2` feature wasn't enabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpVersionPref {
    /// Let the transport pick. HTTP → H1, HTTPS → ALPN negotiation
    /// (`h2` if the server offers it, else `http/1.1`).
    #[default]
    Auto,
    /// Force HTTP/1.1. Always available.
    Http1,
    /// Force HTTP/2. HTTP/2 support is always compiled in via
    /// `zerobench-backends::http::mio_h2`.
    Http2,
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
            addr_family: AddrFamily::Any,
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
            addr_family: AddrFamily::Any,
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
    fn addr_family_default_is_any() {
        assert_eq!(AddrFamily::default(), AddrFamily::Any);
        let t = Target::parse("http://h").unwrap();
        assert_eq!(t.addr_family, AddrFamily::Any);
    }

    #[test]
    fn resolve_uses_override_v4() {
        let mut t = Target::parse("http://example.com:8080").unwrap();
        t.addr_family = AddrFamily::Any;
        let opts = TransportOpts {
            resolve_overrides: vec![(
                "example.com".to_string(),
                8080,
                "10.0.0.1".to_string(),
            )],
            ..TransportOpts::default()
        };
        let addr = t.resolve(&opts).unwrap();
        assert_eq!(addr.ip(), "10.0.0.1".parse::<std::net::IpAddr>().unwrap());
        assert_eq!(addr.port(), 8080);
    }

    #[test]
    fn resolve_uses_override_v6() {
        let t = Target::parse("http://example.com:443").unwrap();
        let opts = TransportOpts {
            resolve_overrides: vec![(
                "example.com".to_string(),
                443,
                "::1".to_string(),
            )],
            ..TransportOpts::default()
        };
        let addr = t.resolve(&opts).unwrap();
        assert!(addr.is_ipv6());
        assert_eq!(addr.port(), 443);
    }

    #[test]
    fn resolve_override_is_case_insensitive_host() {
        let t = Target::parse("http://ExAmPlE.CoM:80").unwrap();
        let opts = TransportOpts {
            resolve_overrides: vec![(
                "example.com".to_string(),
                80,
                "10.0.0.5".to_string(),
            )],
            ..TransportOpts::default()
        };
        let addr = t.resolve(&opts).unwrap();
        assert_eq!(addr.ip(), "10.0.0.5".parse::<std::net::IpAddr>().unwrap());
    }

    #[test]
    fn resolve_override_port_must_match() {
        let t = Target::parse("http://example.com:8080").unwrap();
        let opts = TransportOpts {
            // Wrong port — must fall through to DNS (which will fail
            // deterministically here because example.com is arbitrary).
            resolve_overrides: vec![(
                "example.com".to_string(),
                9999,
                "10.0.0.1".to_string(),
            )],
            ..TransportOpts::default()
        };
        // Without a matching override we go to real DNS. On a sandbox
        // without network, this may fail; we only assert the override
        // didn't hijack the lookup.
        let addr = t.resolve(&opts);
        if let Ok(a) = addr {
            assert_ne!(a.ip(), "10.0.0.1".parse::<std::net::IpAddr>().unwrap());
        }
    }

    #[test]
    fn resolve_override_invalid_ip_is_error() {
        let t = Target::parse("http://example.com:80").unwrap();
        let opts = TransportOpts {
            resolve_overrides: vec![(
                "example.com".to_string(),
                80,
                "not-an-ip".to_string(),
            )],
            ..TransportOpts::default()
        };
        let err = t.resolve(&opts).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn resolve_loopback_v4_via_override() {
        // Using an override avoids DNS dependence.
        let mut t = Target::parse("http://localhost:1234").unwrap();
        t.addr_family = AddrFamily::V4;
        let opts = TransportOpts {
            resolve_overrides: vec![(
                "localhost".to_string(),
                1234,
                "127.0.0.1".to_string(),
            )],
            ..TransportOpts::default()
        };
        let addr = t.resolve(&opts).unwrap();
        assert!(addr.is_ipv4());
    }

    #[test]
    fn resolve_ipv4_literal_as_target_any() {
        // Numeric hosts bypass the resolver cleanly on every platform.
        let t = Target::parse("http://127.0.0.1:1234").unwrap();
        let opts = TransportOpts::default();
        let addr = t.resolve(&opts).unwrap();
        assert_eq!(addr, "127.0.0.1:1234".parse().unwrap());
    }

    #[test]
    fn resolve_ipv6_literal_as_target_any() {
        let t = Target::parse("http://[::1]:1234").unwrap();
        let opts = TransportOpts::default();
        let addr = t.resolve(&opts).unwrap();
        assert!(addr.is_ipv6());
        assert_eq!(addr.port(), 1234);
    }

    #[test]
    fn resolve_ipv4_only_with_v4_family_pass() {
        let mut t = Target::parse("http://127.0.0.1:80").unwrap();
        t.addr_family = AddrFamily::V4;
        let opts = TransportOpts::default();
        assert!(t.resolve(&opts).unwrap().is_ipv4());
    }

    #[test]
    fn resolve_ipv4_only_with_v6_family_fails() {
        let mut t = Target::parse("http://127.0.0.1:80").unwrap();
        t.addr_family = AddrFamily::V6;
        let opts = TransportOpts::default();
        let err = t.resolve(&opts).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

}
