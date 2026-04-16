//! The `Transport` trait impl for HTTP.
//!
//! Keeps the impl separate from [`Http1Pool`] because the pool is
//! useful on its own (lower-level tests, future debug tooling) and the
//! trait impl is only relevant when plugging into the dispatcher.

use std::sync::Arc;

use zerobench_core::plan::RequestPlan;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::transport::{
    Response, Target, Transport, TransportError, TransportOpts,
};

use crate::h1::Http1Pool;

/// HTTP transport — wraps [`Http1Pool`] and exposes it via the
/// [`Transport`] trait.
///
/// The `h2` feature will swap in an HTTP/2 pool inside `build_client`
/// when the target's ALPN negotiates `h2`; the exchange API stays the
/// same because hyper's `SendRequest` type parameter is the body.
pub struct HttpTransport;

impl Transport for HttpTransport {
    type Client = Arc<Http1Pool>;

    async fn build_client(
        target: &Target,
        opts: &TransportOpts,
    ) -> Result<Self::Client, TransportError> {
        let pool = Http1Pool::new(target, opts).await?;
        Ok(Arc::new(pool))
    }

    async fn exchange(
        client: &Self::Client,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        client.exchange(plan, ctx).await
    }
}
