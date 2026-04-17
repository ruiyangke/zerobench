//! HTTP/2 client built on `hyper::client::conn::http2`.
//!
//! Unlike HTTP/1, HTTP/2 multiplexes many concurrent requests over a
//! single connection. [`Http2Client`] therefore owns **one** TCP (or, in
//! the future, TLS) connection and a single cloneable
//! `SendRequest<Full<Bytes>>`. Every call to [`Http2Client::exchange`]
//! clones that sender, so multiple in-flight requests ride the same
//! underlying connection as distinct streams.
//!
//! # `-c N` semantics (vs H1)
//!
//! - HTTP/1: `N` TCP connections, each serialising one request at a time.
//! - HTTP/2: `1` TCP connection, up to `N` concurrent streams. The value
//!   is forwarded to the hyper builder via `initial_max_send_streams`,
//!   which bounds the number of streams **we** (the client) may have
//!   in flight at once. Note: hyper's `max_concurrent_streams` is the
//!   *opposite* — it caps streams the server is allowed to initiate
//!   (server push), not our own — so it is the wrong knob here.
//!   The peer's `SETTINGS_MAX_CONCURRENT_STREAMS` is still respected
//!   independently by h2 at the protocol layer.
//!
//! # Failure model
//!
//! If the single connection driver task exits (server sent GOAWAY, TCP
//! reset, etc.), every subsequent `send_request` will return
//! `TransportError::Protocol`. v0.0.1 does not reconnect — this matches
//! Http1Pool's "don't lazy-reopen" policy and avoids hiding real issues
//! behind magic retries. A future iteration may add a "reopen on first
//! failure" mode.
//!
//! # Byte counting
//!
//! Because there is only one socket under an H2 client, the
//! [`CountingStream`] wrapper installed by [`crate::conn::open`] already
//! counts every byte sent/received by every stream. For per-exchange
//! byte counts we take `(read, written)` snapshots before the request
//! and after the response body completes; on a busy connection these
//! deltas *mingle* framing bytes from other streams, which is documented
//! on the `Response::bytes_*` fields. Mingling matches what an external
//! observer (wrk, `ss -ti`, tcpdump) would attribute to the same time
//! window, so the reporter averages still tell the truth; they just
//! shouldn't be interpreted as "this individual request's cost" in
//! contended settings.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::runtime::spawn;
use cyper_core::{CompioExecutor, CompioTimer, HyperStream};
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2::{self, SendRequest};
use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::ExpandCtx;
use zerobench_core::transport::{
    Response, ResponseBody, Target, TransportError, TransportOpts,
};

use crate::conn::{self, Connected};

/// HTTP/2 single-connection client.
///
/// `Arc<Http2Client>` is the [`crate::HttpClient::Http2`] variant.
pub struct Http2Client {
    /// Cheap-to-clone sender handle. H2 streams fan out through this; a
    /// `clone` is a lightweight operation over a tokio-style mpsc, so we
    /// don't bother with a per-request lock.
    sender: SendRequest<Full<Bytes>>,
    /// Destination authority, used to fill the `Host` header (which is
    /// still required by H2 in practice — hyper pulls `:authority` from
    /// the URI, but our URIs are origin-form, so we set Host explicitly
    /// to match H1's behaviour).
    target: Target,
    /// Per-socket byte counters. Shared with the `CountingStream` that
    /// sits below hyper; snapshots around each exchange give a best-
    /// effort per-request byte count (see module docs on mingling).
    read_ctr: Arc<AtomicU64>,
    written_ctr: Arc<AtomicU64>,
    /// Per-request deadline copied from [`TransportOpts`].
    request_timeout: Duration,
}

impl std::fmt::Debug for Http2Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2Client")
            .field("target", &self.target)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl Http2Client {
    /// Open a single connection and perform the HTTP/2 handshake.
    ///
    /// Plain-HTTP ("h2c") is the supported path for v0.0.1 — see the
    /// task notes in `docs/plans/2026-04-16-v0.0.1-impl.md`. TLS + ALPN
    /// support is wired in [`crate::conn::open`] when that phase lands;
    /// once a TLS connection arrives here, we accept it unconditionally
    /// and trust the caller's ALPN negotiation decision.
    pub async fn new(
        target: &Target,
        opts: &TransportOpts,
    ) -> Result<Self, TransportError> {
        // max_conns has a different meaning for H2 (concurrent streams),
        // but 0 remains nonsensical in both cases.
        if opts.max_conns == 0 {
            return Err(TransportError::Connect(
                "max_conns must be > 0".into(),
            ));
        }

        // HTTP/2 requires ALPN `h2` when running over TLS. We ask for
        // that exact protocol here; if the server doesn't speak H2, the
        // hyper handshake below will fail with a protocol error — the
        // `HttpTransport::build_client` ALPN probe is already the
        // belt-and-braces check that prevents us from ever reaching this
        // path when the server only offered `http/1.1`.
        let alpn: &[&[u8]] = if target.tls { &[b"h2"] } else { &[] };
        let connected = conn::open(target, opts, alpn).await?;
        let (read_ctr, written_ctr) = connected.counts();

        // H2 handshake wants a hyper-compatible IO handle. `HyperStream`
        // bridges compio's Async{Read,Write} into hyper's equivalents;
        // the counting has already been applied below HyperStream.
        //
        // Hyper's `http2::Builder::handshake` returns a `Connection<Io,
        // B, E>` whose IO type parameter differs between the plain and
        // TLS arms. We spawn the driver inside each arm and only return
        // the `SendRequest` (which has a uniform type) out of the match.
        let sender = match connected {
            Connected::Plain { stream, .. } => {
                let io = HyperStream::new(stream);
                let mut builder = http2::Builder::new(CompioExecutor);
                builder.timer(CompioTimer);
                // Cap the number of streams *we* initiate at once.
                // `initial_max_send_streams` is the client-side knob;
                // `max_concurrent_streams` (what we previously used)
                // governs streams the *server* is allowed to open via
                // push, not our outgoing streams. Using the wrong one
                // silently left outgoing streams at hyper's default
                // ceiling (100) regardless of `-c N`.
                //
                // The peer may advertise its own, lower cap via
                // `SETTINGS_MAX_CONCURRENT_STREAMS`; h2 honours that
                // independently, so our value is an *upper bound* on
                // what we attempt.
                builder.initial_max_send_streams(opts.max_conns);

                let (sender, conn) = builder
                    .handshake::<_, Full<Bytes>>(io)
                    .await
                    .map_err(|e| {
                        TransportError::Protocol(format!("h2 handshake: {e}"))
                    })?;
                spawn(async move {
                    let _ = conn.await;
                })
                .detach();
                sender
            }
            Connected::Tls {
                stream,
                negotiated_alpn,
                ..
            } => {
                // Confirm ALPN picked `h2`. An HTTPS server that only
                // supports HTTP/1.1 will surface as `Some(b"http/1.1")`
                // here — reject with a Protocol error because the caller
                // asked for H2 explicitly. This path is normally
                // short-circuited by `HttpTransport::build_client`'s
                // probe; keeping the check here makes `Http2Client::new`
                // safe to call in isolation.
                match negotiated_alpn.as_deref() {
                    Some(b"h2") => {}
                    Some(other) => {
                        return Err(TransportError::Protocol(format!(
                            "ALPN negotiated {}, expected h2",
                            String::from_utf8_lossy(other)
                        )));
                    }
                    None => {
                        // No ALPN negotiated at all. h2 over TLS without
                        // ALPN is non-standard; hyper's h2 handshake will
                        // almost certainly fail. We still attempt it for
                        // parity with loopback test servers that may not
                        // bother advertising.
                    }
                }

                let io = HyperStream::new(*stream);
                let mut builder = http2::Builder::new(CompioExecutor);
                builder.timer(CompioTimer);
                builder.initial_max_send_streams(opts.max_conns);
                let (sender, conn) = builder
                    .handshake::<_, Full<Bytes>>(io)
                    .await
                    .map_err(|e| {
                        TransportError::Protocol(format!("h2 handshake: {e}"))
                    })?;
                spawn(async move {
                    let _ = conn.await;
                })
                .detach();
                sender
            }
        };

        Ok(Self {
            sender,
            target: target.clone(),
            read_ctr,
            written_ctr,
            request_timeout: opts.request_timeout,
        })
    }

    /// Send one request over the multiplexed connection.
    ///
    /// Each call clones the `SendRequest` — cheap — so concurrent
    /// callers of `exchange` each get their own handle and run on
    /// separate H2 streams. No per-caller serialisation.
    pub async fn exchange(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        let req = build_request(&self.target, plan, ctx)?;

        // Snapshot counters *before* clone+send so we can compute a
        // per-request delta. Counters are connection-wide; see the
        // module docs on mingling under concurrent load.
        let r_before = self.read_ctr.load(Ordering::Relaxed);
        let w_before = self.written_ctr.load(Ordering::Relaxed);

        let t0 = Instant::now();
        let mut sender = self.sender.clone();

        let send_fut = sender.send_request(req);
        let use_timeout = self.request_timeout < Duration::from_secs(300);

        let res = if use_timeout {
            match compio::time::timeout(self.request_timeout, send_fut).await {
                Ok(Ok(res)) => res,
                Ok(Err(e)) => {
                    return Err(TransportError::Protocol(format!("send_request: {e}")));
                }
                Err(_) => {
                    return Err(TransportError::Timeout);
                }
            }
        } else {
            match send_fut.await {
                Ok(res) => res,
                Err(e) => {
                    return Err(TransportError::Protocol(format!("send_request: {e}")));
                }
            }
        };

        let ttfb = t0.elapsed();

        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let mut body = res.into_body();

        // Drain the body without collecting — same rationale as H1.
        // The benchmark hot path never inspects the body; draining
        // avoids the BytesMut allocation + memmove per frame.
        if use_timeout {
            let remaining = self.request_timeout
                .saturating_sub(ttfb)
                .max(Duration::from_millis(1));
            loop {
                match compio::time::timeout(remaining, body.frame()).await {
                    Ok(Some(Ok(_frame))) => {}
                    Ok(Some(Err(e))) => {
                        return Err(TransportError::Protocol(format!("body: {e}")));
                    }
                    Ok(None) => break,
                    Err(_) => {
                        return Err(TransportError::Timeout);
                    }
                }
            }
        } else {
            loop {
                match body.frame().await {
                    Some(Ok(_frame)) => {}
                    Some(Err(e)) => {
                        return Err(TransportError::Protocol(format!("body: {e}")));
                    }
                    None => break,
                }
            }
        }

        let total = t0.elapsed();

        let r_after = self.read_ctr.load(Ordering::Relaxed);
        let w_after = self.written_ctr.load(Ordering::Relaxed);

        Ok(Response {
            status,
            headers,
            body: ResponseBody::Buffered(Bytes::new()),
            bytes_sent: w_after.saturating_sub(w_before),
            bytes_received: r_after.saturating_sub(r_before),
            ttfb,
            total,
        })
    }

    /// Target this client was opened against.
    pub fn target(&self) -> &Target {
        &self.target
    }
}

// ---------------------------------------------------------------------------
// Helpers — request builder
// ---------------------------------------------------------------------------
//
// This is structurally identical to `h1.rs::build_request`. We duplicate
// it rather than lifting it into `conn.rs` or a shared helper because
// the H1 code carries a couple of quirks (origin-form URI normalisation,
// Host header injection) that are specifically right for H1 — and the
// H2 path, while also benefiting from them today, may diverge once we
// start leaning on `:authority` pseudo-headers or absolute-form URIs.
// Keeping the two copies lets each evolve without spooky action at a
// distance.

fn build_request(
    target: &Target,
    plan: &RequestPlan,
    ctx: &mut ScenarioContext,
) -> Result<Request<Full<Bytes>>, TransportError> {
    let mut ectx = ExpandCtx {
        rng: &mut ctx.rng,
        counter: &ctx.counter,
        scenario_vars: &ctx.vars,
    };

    ctx.url_buf.clear();
    plan.url.expand_into(&mut ctx.url_buf, &mut ectx);
    let url_str = std::str::from_utf8(&ctx.url_buf)
        .map_err(|e| TransportError::RequestBuild(format!("url not utf-8: {e}")))?;

    let path_and_query = extract_path_and_query(url_str);

    let mut builder = Request::builder()
        .method(plan.method.clone())
        .uri(path_and_query.as_ref());

    builder = builder.header(http::header::HOST, target.addr());

    for (name_tpl, val_tpl) in &plan.headers {
        ctx.hdr_name_buf.clear();
        ctx.hdr_val_buf.clear();
        name_tpl.expand_into(&mut ctx.hdr_name_buf, &mut ectx);
        val_tpl.expand_into(&mut ctx.hdr_val_buf, &mut ectx);

        let name = http::HeaderName::from_bytes(&ctx.hdr_name_buf)
            .map_err(|e| TransportError::RequestBuild(format!("header name: {e}")))?;
        let value = HeaderValue::from_bytes(&ctx.hdr_val_buf)
            .map_err(|e| TransportError::RequestBuild(format!("header value: {e}")))?;
        builder = builder.header(name, value);
    }

    let body_bytes = match &plan.body {
        None => Bytes::new(),
        Some(BodySource::Static(b)) => b.clone(),
        Some(BodySource::Template(t)) => {
            ctx.body_buf.clear();
            t.expand_into(&mut ctx.body_buf, &mut ectx);
            Bytes::copy_from_slice(&ctx.body_buf)
        }
    };

    builder
        .body(Full::new(body_bytes))
        .map_err(|e| TransportError::RequestBuild(format!("build: {e}")))
}

/// Same origin-form extraction as H1's version. See
/// [`crate::h1`] for the rationale; the rules are unchanged here because
/// hyper's H2 client happily accepts an origin-form URI and does the
/// right thing with `:authority`.
fn extract_path_and_query(url: &str) -> std::borrow::Cow<'_, str> {
    use std::borrow::Cow;

    let (rest, absolute) = if let Some(pos) = url.find("://") {
        let after_scheme = &url[pos + 3..];
        match after_scheme.find(|c: char| c == '/' || c == '?' || c == '#') {
            Some(i) => (&after_scheme[i..], true),
            None => return Cow::Borrowed("/"),
        }
    } else {
        (url, false)
    };

    let without_fragment = match rest.find('#') {
        Some(i) => &rest[..i],
        None => rest,
    };

    if without_fragment.is_empty() {
        return Cow::Borrowed("/");
    }

    if absolute && without_fragment.starts_with('?') {
        return Cow::Owned(format!("/{without_fragment}"));
    }

    Cow::Borrowed(without_fragment)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_path_and_query_slash_defaults() {
        assert_eq!(extract_path_and_query("http://h:80"), "/");
        assert_eq!(extract_path_and_query(""), "/");
        assert_eq!(extract_path_and_query("http://h:80#frag"), "/");
    }

    #[test]
    fn extract_path_and_query_preserves_query() {
        assert_eq!(extract_path_and_query("http://h:80?q=1"), "/?q=1");
        assert_eq!(
            extract_path_and_query("http://h:80/foo?q=1"),
            "/foo?q=1"
        );
        assert_eq!(
            extract_path_and_query("/relative?q=1"),
            "/relative?q=1"
        );
    }

    #[test]
    fn extract_path_and_query_strips_fragment() {
        assert_eq!(extract_path_and_query("http://h:80/foo#frag"), "/foo");
        assert_eq!(extract_path_and_query("/path#frag"), "/path");
    }
}
