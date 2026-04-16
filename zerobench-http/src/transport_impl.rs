//! The [`Transport`] trait impl for HTTP.
//!
//! v0.0.1 has two wire protocols available: HTTP/1 via
//! [`crate::Http1Pool`] (always compiled in) and HTTP/2 via
//! [`crate::Http2Client`] (feature-gated behind `h2`). A single dispatch
//! enum — [`HttpClient`] — wraps whichever one the user chose, so the
//! rest of the engine only has to know about one `Transport::Client`.
//!
//! ## Protocol choice
//!
//! [`HttpTransport::build_client`] picks between H1 and H2 using
//! [`TransportOpts::http_version`]:
//!
//! - [`HttpVersionPref::Http1`] → always [`Http1Pool`].
//! - [`HttpVersionPref::Http2`] → always [`Http2Client`]. Errors with
//!   [`TransportError::Protocol`] when the `h2` feature isn't compiled
//!   in, so users aren't silently downgraded.
//! - [`HttpVersionPref::Auto`] → H1 on plain HTTP. On HTTPS, an ALPN
//!   probe is performed: we open one TLS connection advertising
//!   `h2, http/1.1` and look at what the server chose. `h2` → build an
//!   `Http2Client`; anything else → build an `Http1Pool`. This probe
//!   connection is closed and not reused — the pool/H2 client opens its
//!   own connections with the appropriate single-protocol ALPN list
//!   when it builds its first slot. One extra connect at startup is
//!   negligible for a benchmark that runs for ≥1s.
//!
//! The `build_client` future itself is feature-gated only in terms of
//! which arms compile — the `HttpClient` enum is always defined with
//! both variants so match exhaustiveness is stable across builds; the
//! `Http2` variant is conditionally compiled, and callers handle it via
//! `cfg(feature = "h2")` on the arm.

use std::sync::Arc;

use zerobench_core::plan::RequestPlan;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::transport::{
    HttpVersionPref, Response, Target, Transport, TransportError, TransportOpts,
};

use crate::h1::{Http1Pool, StreamingResponse};

/// A cheap-to-clone dispatch handle over either an H1 pool or an H2
/// client.
///
/// Both variants wrap their inner type in [`Arc`] so cloning the enum
/// is a pointer-copy per variant; the dispatcher clones one of these
/// per worker.
#[derive(Clone, Debug)]
pub enum HttpClient {
    /// HTTP/1.1 — pre-opened pool of TCP connections, each serialising
    /// one request at a time. `-c N` = pool size.
    Http1(Arc<Http1Pool>),
    /// HTTP/2 — single TCP connection, many concurrent streams. `-c N`
    /// = `max_concurrent_streams` hint. Only compiled when the crate is
    /// built with the `h2` feature.
    #[cfg(feature = "h2")]
    Http2(Arc<crate::Http2Client>),
}

impl HttpClient {
    /// Target the client was opened against. Cheap — a borrow of the
    /// [`Target`] stored at construction time, regardless of whether
    /// the underlying client is H1 or H2.
    pub fn target(&self) -> &Target {
        match self {
            HttpClient::Http1(p) => p.target(),
            #[cfg(feature = "h2")]
            HttpClient::Http2(c) => c.target(),
        }
    }

    /// Dispatch a single exchange to whichever variant is in use.
    pub async fn exchange(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        match self {
            HttpClient::Http1(p) => p.exchange(plan, ctx).await,
            #[cfg(feature = "h2")]
            HttpClient::Http2(c) => c.exchange(plan, ctx).await,
        }
    }

    /// Open a streaming exchange against the underlying client.
    ///
    /// Only HTTP/1 supports streaming in v0.0.1 — the SSE runner builds
    /// its clients with `HttpVersionPref::Http1` explicitly, and this
    /// method surfaces a clear `Protocol` error if called on the H2
    /// variant (which has a different framing story that v0.0.1 doesn't
    /// wire through).
    ///
    /// The caller owns the returned [`StreamingResponse`] and is
    /// responsible for draining the body. Dropping it releases the
    /// slot back to the pool; see [`StreamingResponse::invalidate`] for
    /// error-path cleanup semantics.
    pub async fn exchange_streaming(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<StreamingResponse, TransportError> {
        match self {
            HttpClient::Http1(p) => p.exchange_streaming(plan, ctx).await,
            #[cfg(feature = "h2")]
            HttpClient::Http2(_) => Err(TransportError::Protocol(
                "streaming exchange is only implemented for HTTP/1 in v0.0.1".into(),
            )),
        }
    }
}

/// Zero-sized type that carries the [`Transport`] impl.
pub struct HttpTransport;

impl Transport for HttpTransport {
    type Client = HttpClient;

    async fn build_client(
        target: &Target,
        opts: &TransportOpts,
    ) -> Result<Self::Client, TransportError> {
        match opts.http_version {
            HttpVersionPref::Http1 => {
                let pool = Http1Pool::new(target, opts).await?;
                Ok(HttpClient::Http1(Arc::new(pool)))
            }
            HttpVersionPref::Http2 => build_h2(target, opts).await,
            HttpVersionPref::Auto => build_auto(target, opts).await,
        }
    }

    async fn exchange(
        client: &Self::Client,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        client.exchange(plan, ctx).await
    }
}

/// `HttpVersionPref::Auto` dispatch.
///
/// Plain HTTP: always H1 (H2 cleartext requires explicit opt-in because
/// no browser / curl uses it and servers don't advertise it).
///
/// HTTPS: probe ALPN with `h2, http/1.1`, pick the pool type based on
/// what the server chose. The probe connection is closed; the pool opens
/// fresh connections on its own. This adds one connect worth of latency
/// at startup (roughly 1-3 RTTs including TLS), which is noise for any
/// bench that runs ≥1s. We trade simplicity for the negligible cost —
/// threading the probe connection into the first slot would require new
/// `from_conn` constructors on both pool types and some care around
/// ownership.
async fn build_auto(
    target: &Target,
    opts: &TransportOpts,
) -> Result<HttpClient, TransportError> {
    // Plain HTTP → H1.
    if !target.tls {
        let pool = Http1Pool::new(target, opts).await?;
        return Ok(HttpClient::Http1(Arc::new(pool)));
    }

    // TLS but H2 feature absent → H1.
    #[cfg(not(feature = "h2"))]
    {
        let pool = Http1Pool::new(target, opts).await?;
        return Ok(HttpClient::Http1(Arc::new(pool)));
    }

    #[cfg(feature = "h2")]
    {
        // Probe: open one TLS connection advertising both protocols.
        let alpn: &[&[u8]] = &[b"h2", b"http/1.1"];
        let probe = crate::conn::open(target, opts, alpn).await?;
        let negotiated = probe.negotiated_alpn().map(|s| s.to_vec());
        // Drop the probe — the pool/H2 client opens its own connection.
        drop(probe);

        match negotiated.as_deref() {
            Some(b"h2") => build_h2(target, opts).await,
            _ => {
                // `Some(b"http/1.1")`, `None`, or anything else → H1.
                let pool = Http1Pool::new(target, opts).await?;
                Ok(HttpClient::Http1(Arc::new(pool)))
            }
        }
    }
}

/// Build an H2 client when the feature is present, or return a clear
/// error otherwise. Kept as a separate function so the `cfg` gates are
/// localised rather than sprinkled through `build_client`.
#[cfg(feature = "h2")]
async fn build_h2(
    target: &Target,
    opts: &TransportOpts,
) -> Result<HttpClient, TransportError> {
    let client = crate::Http2Client::new(target, opts).await?;
    Ok(HttpClient::Http2(Arc::new(client)))
}

/// Stub called when the user asked for `--http-version h2` but the
/// binary wasn't compiled with the `h2` feature. We surface a clear
/// `Protocol` error so the failure mode is "you built zerobench without
/// H2 support" rather than "silently downgraded to H1".
#[cfg(not(feature = "h2"))]
async fn build_h2(
    _target: &Target,
    _opts: &TransportOpts,
) -> Result<HttpClient, TransportError> {
    Err(TransportError::Protocol(
        "HTTP/2 requested but zerobench-http was built without the `h2` feature"
            .into(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(feature = "h2")))]
mod tests {
    use super::*;

    #[compio::test]
    async fn http_version_h2_without_feature_errors_cleanly() {
        // Without the feature, selecting H2 must produce a clear
        // Protocol error. We don't even need to reach a real server
        // because the dispatch short-circuits before open().
        let target = Target::parse("http://127.0.0.1:1").expect("target");
        let opts = TransportOpts {
            http_version: HttpVersionPref::Http2,
            ..TransportOpts::default()
        };
        let err = HttpTransport::build_client(&target, &opts)
            .await
            .expect_err("should fail without h2 feature");
        match err {
            TransportError::Protocol(msg) => {
                assert!(
                    msg.contains("h2"),
                    "error message should mention `h2`: {msg}"
                );
            }
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }
}
