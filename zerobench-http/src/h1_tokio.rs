//! Tokio-native HTTP/1 connection pool.
//!
//! Mirrors `h1.rs` but uses `tokio::net::TcpStream` + `hyper-util`'s
//! `TokioIo` bridge to give hyper a native tokio executor. No
//! `HyperStream` (the compio↔hyper IO bridge) is involved — hyper runs
//! on tokio's own poll-based IO, eliminating the 5% bridge overhead
//! measured in profiling.
//!
//! # Byte counting
//!
//! v1 skips per-request wire byte counting (bytes_sent / bytes_received
//! are reported as 0). The core benchmark metrics (latency, TTFB, RPS,
//! status codes) are fully functional.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use tokio::sync::Mutex;

use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::ExpandCtx;
use zerobench_core::transport::{
    Response, ResponseBody, Target, TransportError, TransportOpts,
};

use crate::conn_tokio::{self, ConnectedTokio};

/// A single slot in the tokio pool.
struct Slot {
    sender: Option<SendRequest<Full<Bytes>>>,
}

/// Pre-opened HTTP/1 connection pool using tokio + hyper natively.
///
/// `Arc<Http1PoolTokio>` is the `Transport::Client` for the tokio
/// backend. The pool shape mirrors `Http1Pool` — pre-opened slots,
/// round-robin acquire, per-slot mutex — but the IO types are native
/// tokio, not compio.
pub struct Http1PoolTokio {
    slots: Box<[Arc<Mutex<Slot>>]>,
    next_idx: AtomicUsize,
    target: Target,
    request_timeout: Duration,
}

impl Http1PoolTokio {
    /// Open `opts.max_conns` connections in parallel.
    pub async fn new(target: &Target, opts: &TransportOpts) -> Result<Self, TransportError> {
        if opts.max_conns == 0 {
            return Err(TransportError::Connect("max_conns must be > 0".into()));
        }

        let mut pending = Vec::with_capacity(opts.max_conns);
        for _ in 0..opts.max_conns {
            pending.push(open_one_tokio(target, opts));
        }
        let results = futures_util::future::join_all(pending).await;

        let mut slots: Vec<Arc<Mutex<Slot>>> = Vec::with_capacity(opts.max_conns);
        for res in results {
            let slot = res?;
            slots.push(Arc::new(Mutex::new(slot)));
        }

        Ok(Self {
            slots: slots.into_boxed_slice(),
            next_idx: AtomicUsize::new(0),
            target: target.clone(),
            request_timeout: opts.request_timeout,
        })
    }

    /// Target this pool was opened against.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// Send one request through the pool.
    pub async fn exchange(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        let req = build_request(&self.target, plan, ctx)?;

        // Acquire a slot (round-robin with try_lock fast path).
        let start_idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        let n = self.slots.len();
        let mut guard_opt = None;
        for offset in 0..n {
            let idx = (start_idx + offset) % n;
            if let Ok(g) = self.slots[idx].try_lock() {
                guard_opt = Some(g);
                break;
            }
        }
        let mut guard = match guard_opt {
            Some(g) => g,
            None => self.slots[start_idx % n].lock().await,
        };

        let t0 = Instant::now();
        let sender = guard
            .sender
            .as_mut()
            .ok_or_else(|| TransportError::Connect("slot unavailable".into()))?;

        // Send + await response. Skip creating a timer per request when
        // the timeout is very large (>= 5 min).
        let use_timeout = self.request_timeout < Duration::from_secs(300);

        let res = if use_timeout {
            match tokio::time::timeout(self.request_timeout, sender.send_request(req)).await {
                Ok(Ok(res)) => res,
                Ok(Err(e)) => {
                    guard.sender = None;
                    return Err(TransportError::Protocol(format!("send_request: {e}")));
                }
                Err(_) => {
                    guard.sender = None;
                    return Err(TransportError::Timeout);
                }
            }
        } else {
            match sender.send_request(req).await {
                Ok(res) => res,
                Err(e) => {
                    guard.sender = None;
                    return Err(TransportError::Protocol(format!("send_request: {e}")));
                }
            }
        };

        let ttfb = t0.elapsed();
        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let mut body = res.into_body();

        // Drain the body without collecting.
        if use_timeout {
            let remaining = self.request_timeout.saturating_sub(ttfb).max(Duration::from_millis(1));
            loop {
                match tokio::time::timeout(remaining, body.frame()).await {
                    Ok(Some(Ok(_frame))) => {}
                    Ok(Some(Err(e))) => {
                        guard.sender = None;
                        return Err(TransportError::Protocol(format!("body: {e}")));
                    }
                    Ok(None) => break,
                    Err(_) => {
                        guard.sender = None;
                        return Err(TransportError::Timeout);
                    }
                }
            }
        } else {
            loop {
                match body.frame().await {
                    Some(Ok(_frame)) => {}
                    Some(Err(e)) => {
                        guard.sender = None;
                        return Err(TransportError::Protocol(format!("body: {e}")));
                    }
                    None => break,
                }
            }
        }

        let total = t0.elapsed();
        drop(guard);

        Ok(Response {
            status,
            headers,
            body: ResponseBody::Buffered(Bytes::new()),
            bytes_sent: 0,
            bytes_received: 0,
            ttfb,
            total,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Open one TCP+TLS connection and perform the HTTP/1 handshake using
/// tokio + hyper natively.
async fn open_one_tokio(target: &Target, opts: &TransportOpts) -> Result<Slot, TransportError> {
    let alpn: &[&[u8]] = if target.tls { &[b"http/1.1"] } else { &[] };
    let connected = conn_tokio::open_tokio(target, opts, alpn).await?;

    let (sender, conn_driver): (SendRequest<Full<Bytes>>, _) = match connected {
        ConnectedTokio::Plain(tcp) => {
            let io = TokioIo::new(tcp);
            let (sender, conn) = http1::handshake(io)
                .await
                .map_err(|e| TransportError::Protocol(format!("handshake: {e}")))?;
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(async move { let _ = conn.await; });
            (sender, fut)
        }
        ConnectedTokio::Tls(tls) => {
            let io = TokioIo::new(tls);
            let (sender, conn) = http1::handshake(io)
                .await
                .map_err(|e| TransportError::Protocol(format!("handshake: {e}")))?;
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(async move { let _ = conn.await; });
            (sender, fut)
        }
    };

    tokio::spawn(conn_driver);

    Ok(Slot {
        sender: Some(sender),
    })
}

/// Build an `http::Request<Full<Bytes>>` from a `RequestPlan`.
///
/// Identical to the compio backend's `build_request` — the request
/// construction is runtime-agnostic.
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

/// Extract the origin-form path+query from a URL-ish string.
/// (Same logic as `h1.rs::extract_path_and_query`.)
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

impl std::fmt::Debug for Http1PoolTokio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1PoolTokio")
            .field("slots", &self.slots.len())
            .field("target", &self.target)
            .finish()
    }
}
