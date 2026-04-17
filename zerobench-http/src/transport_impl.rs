//! The [`Transport`] trait impl for HTTP.
//!
//! # Runtime backends
//!
//! - `runtime-compio`: Uses `Http1Pool` (compio + HyperStream bridge) and
//!   `Http2Client`. The `HttpClient` dispatch enum wraps both.
//! - `runtime-tokio`: Uses `Http1PoolTokio` (native tokio + hyper). H2 on
//!   tokio is not yet wired — `HttpVersionPref::Http2` returns a clean error.
//!
//! In both cases, the public `HttpTransport` type implements `Transport`.

use std::sync::Arc;

use zerobench_core::plan::RequestPlan;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::transport::{
    HttpVersionPref, Response, Target, Transport, TransportError, TransportOpts,
};

// =========================================================================
// Compio backend
// =========================================================================

#[cfg(feature = "runtime-compio")]
use crate::h1::Http1Pool;
#[cfg(all(feature = "runtime-compio", feature = "h1"))]
use crate::h1::StreamingResponse;

/// A cheap-to-clone dispatch handle over either an H1 pool or an H2
/// client. Only available on the compio backend.
#[cfg(feature = "runtime-compio")]
#[derive(Clone, Debug)]
pub enum HttpClient {
    Http1(Arc<Http1Pool>),
    #[cfg(feature = "h2")]
    Http2(Arc<crate::Http2Client>),
}

#[cfg(feature = "runtime-compio")]
impl HttpClient {
    pub fn target(&self) -> &Target {
        match self {
            HttpClient::Http1(p) => p.target(),
            #[cfg(feature = "h2")]
            HttpClient::Http2(c) => c.target(),
        }
    }

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

// =========================================================================
// Tokio backend
// =========================================================================

#[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
use crate::h1_tokio::Http1PoolTokio;

/// Tokio client handle — wraps `Arc<Http1PoolTokio>`.
#[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
#[derive(Clone, Debug)]
pub struct HttpClientTokio(Arc<Http1PoolTokio>);

// =========================================================================
// Transport impl — dispatches based on active backend
// =========================================================================

/// Zero-sized type that carries the [`Transport`] impl.
pub struct HttpTransport;

#[cfg(feature = "runtime-compio")]
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

#[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
impl Transport for HttpTransport {
    type Client = HttpClientTokio;

    async fn build_client(
        target: &Target,
        opts: &TransportOpts,
    ) -> Result<Self::Client, TransportError> {
        match opts.http_version {
            HttpVersionPref::Http1 | HttpVersionPref::Auto => {
                let pool = Http1PoolTokio::new(target, opts).await?;
                Ok(HttpClientTokio(Arc::new(pool)))
            }
            HttpVersionPref::Http2 => Err(TransportError::Protocol(
                "HTTP/2 is not yet supported with the tokio runtime backend".into(),
            )),
        }
    }

    async fn exchange(
        client: &Self::Client,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        client.0.exchange(plan, ctx).await
    }
}

// =========================================================================
// Compio-only helpers
// =========================================================================

#[cfg(all(feature = "runtime-compio", feature = "h2"))]
async fn build_auto(
    target: &Target,
    opts: &TransportOpts,
) -> Result<HttpClient, TransportError> {
    if !target.tls {
        let pool = Http1Pool::new(target, opts).await?;
        return Ok(HttpClient::Http1(Arc::new(pool)));
    }

    let alpn: &[&[u8]] = &[b"h2", b"http/1.1"];
    let probe = crate::conn::open(target, opts, alpn).await?;
    let negotiated = probe.negotiated_alpn().map(|s| s.to_vec());
    drop(probe);

    match negotiated.as_deref() {
        Some(b"h2") => build_h2(target, opts).await,
        _ => {
            let pool = Http1Pool::new(target, opts).await?;
            Ok(HttpClient::Http1(Arc::new(pool)))
        }
    }
}

#[cfg(all(feature = "runtime-compio", not(feature = "h2")))]
async fn build_auto(
    target: &Target,
    opts: &TransportOpts,
) -> Result<HttpClient, TransportError> {
    let pool = Http1Pool::new(target, opts).await?;
    Ok(HttpClient::Http1(Arc::new(pool)))
}

#[cfg(all(feature = "runtime-compio", feature = "h2"))]
async fn build_h2(
    target: &Target,
    opts: &TransportOpts,
) -> Result<HttpClient, TransportError> {
    let client = crate::Http2Client::new(target, opts).await?;
    Ok(HttpClient::Http2(Arc::new(client)))
}

#[cfg(all(feature = "runtime-compio", not(feature = "h2")))]
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

#[cfg(all(test, feature = "runtime-compio", not(feature = "h2")))]
mod tests {
    use super::*;

    #[compio::test]
    async fn http_version_h2_without_feature_errors_cleanly() {
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
