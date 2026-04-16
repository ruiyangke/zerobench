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
//! - [`HttpVersionPref::Auto`] → H1 on plain HTTP. On HTTPS this would
//!   ideally attempt ALPN-negotiated H2, but TLS + ALPN support is
//!   deferred beyond Phase E; Auto on HTTPS currently resolves to H1.
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
            HttpVersionPref::Auto => {
                // TODO(alpn): when TLS is wired, attempt ALPN `h2` on
                // HTTPS targets and fall back to H1 on `http/1.1`. For
                // now, Auto everywhere means H1.
                let pool = Http1Pool::new(target, opts).await?;
                Ok(HttpClient::Http1(Arc::new(pool)))
            }
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
