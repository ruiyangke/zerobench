//! HTTP/1 connection pool built on `hyper::client::conn::http1`.
//!
//! [`Http1Pool`] opens `opts.max_conns` persistent TCP (+TLS, once
//! wired) connections up-front. Each connection becomes a hyper
//! `SendRequest<Full<Bytes>>` handle whose driver task is spawned on
//! the compio runtime. Requests acquire a free slot in round-robin
//! order, send one request, await the full response, and release the
//! slot — HTTP/1 can't pipeline concurrent requests on one connection
//! anyway, so the per-slot lock is effectively a serialising channel.
//!
//! # Why this shape
//!
//! - **Pre-opened pool**: connect cost is excluded from per-request
//!   latency. Benchmarks measure the server, not the kernel's SYN-ACK
//!   path.
//! - **Round-robin, not FIFO**: cheap, doesn't require a queue, and
//!   reduces head-of-line blocking when one slow response holds up
//!   a slot.
//! - **Per-slot byte counters**: each connection's `CountingStream`
//!   exposes its own `Arc<AtomicU64>` pair. Per-request bytes = delta
//!   between snapshot-before and snapshot-after on the slot we used,
//!   so concurrent requests on different slots don't interfere.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::runtime::spawn;
use cyper_core::HyperStream;
use futures_util::lock::Mutex;
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::{self, SendRequest};
use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::transport::{
    Response, ResponseBody, Target, TransportError, TransportOpts,
};

use crate::conn::{self, Connected};

/// A single slot in the pool.
///
/// `sender` is `None` only when that slot's connection failed to open
/// during [`Http1Pool::new`] *and* we chose not to abort the whole pool
/// (we currently do abort, but the field shape stays `Option` so a
/// future "lazy reopen on death" policy is a one-line change).
///
/// `read_ctr` / `written_ctr` are the `CountingStream` handles for
/// this slot's socket, used to compute per-request wire bytes.
#[derive(Debug)]
struct Slot {
    sender: Option<SendRequest<Full<Bytes>>>,
    read_ctr: Arc<AtomicU64>,
    written_ctr: Arc<AtomicU64>,
}

/// Pre-opened HTTP/1 connection pool.
///
/// `Arc<Http1Pool>` is the `Transport::Client` for `HttpTransport`.
#[derive(Debug)]
pub struct Http1Pool {
    /// One mutex per slot. `futures_util::lock::Mutex` is chosen for
    /// the async-await API and fairness; `async-lock` or
    /// `parking_lot::Mutex` + manual parking would also work but add a
    /// dep or hand-rolled machinery.
    slots: Box<[Mutex<Slot>]>,
    /// Round-robin cursor. We take `fetch_add` mod `slots.len()` as
    /// the starting slot; if it's busy, we spin to the next.
    next_idx: AtomicUsize,
    /// Used to build the `Host` header in each request.
    target: Target,
    /// Per-request deadline from the original opts.
    request_timeout: Duration,
}

impl Http1Pool {
    /// Open `opts.max_conns` connections in parallel.
    ///
    /// Returns [`TransportError::Connect`] on the first failure — we
    /// don't ship partial pools. Rationale: if 1/100 connections
    /// failed, workers would hit the missing slot and throw spurious
    /// errors; better to surface the problem up front.
    pub async fn new(target: &Target, opts: &TransportOpts) -> Result<Self, TransportError> {
        if opts.max_conns == 0 {
            return Err(TransportError::Connect(
                "max_conns must be > 0".into(),
            ));
        }

        // Open all connections concurrently. Each future resolves to a
        // ready slot or an error.
        let mut pending = Vec::with_capacity(opts.max_conns);
        for _ in 0..opts.max_conns {
            pending.push(open_one(target, opts));
        }
        let results = futures_util::future::join_all(pending).await;

        let mut slots: Vec<Mutex<Slot>> = Vec::with_capacity(opts.max_conns);
        for res in results {
            let slot = res?;
            slots.push(Mutex::new(slot));
        }

        Ok(Self {
            slots: slots.into_boxed_slice(),
            next_idx: AtomicUsize::new(0),
            target: target.clone(),
            request_timeout: opts.request_timeout,
        })
    }

    /// Number of slots in the pool.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// `true` if the pool has zero slots (cannot happen for pools
    /// returned from [`Self::new`]; provided for lint-friendly APIs).
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Send one request through the pool.
    ///
    /// Expands the URL / header / body templates using `ctx`, acquires
    /// a free slot round-robin, fires the request, awaits the response
    /// body, and records on-wire byte deltas.
    pub async fn exchange(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        // --- Build the request (template expansion happens here) ----
        let req = build_request(&self.target, plan, ctx)?;

        // --- Acquire a slot ---------------------------------------------
        let start_idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        let n = self.slots.len();
        // Round-robin starting point. We loop over all slots with
        // `try_lock`; if every slot is busy we fall back to an `await`
        // on the one we picked first. This gives us low contention
        // under burst while still bounding waiters.
        let mut guard_opt = None;
        for offset in 0..n {
            let idx = (start_idx + offset) % n;
            if let Some(g) = self.slots[idx].try_lock() {
                guard_opt = Some((idx, g));
                break;
            }
        }
        // If all slots busy, block on the first one we'd have picked.
        let (slot_idx, mut guard) = match guard_opt {
            Some(g) => g,
            None => {
                let idx = start_idx % n;
                let g = self.slots[idx].lock().await;
                (idx, g)
            }
        };

        // --- Snapshot counters before the exchange ----------------------
        let r_before = guard.read_ctr.load(Ordering::Relaxed);
        let w_before = guard.written_ctr.load(Ordering::Relaxed);

        // --- Send + await response --------------------------------------
        let t0 = Instant::now();
        let sender = guard
            .sender
            .as_mut()
            .ok_or_else(|| TransportError::Connect(format!("slot {slot_idx} unavailable")))?;

        let send_fut = sender.send_request(req);
        // Enforce request_timeout around headers *and* body.
        let res = compio::time::timeout(self.request_timeout, send_fut)
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|e| {
                // Connection died — mark slot empty so future users don't
                // hit the same dead sender (conservative: we don't try
                // to reopen here; that's a later task).
                guard.sender = None;
                TransportError::Protocol(format!("send_request: {e}"))
            })?;

        let ttfb = t0.elapsed();

        // Split status + headers from the body first; collecting body
        // consumes `res`.
        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let body = res.into_body();

        let collected = compio::time::timeout(
            self.request_timeout.saturating_sub(ttfb).max(Duration::from_millis(1)),
            body.collect(),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| {
            guard.sender = None;
            TransportError::Protocol(format!("body: {e}"))
        })?;

        let bytes = collected.to_bytes();
        let total = t0.elapsed();

        // --- Snapshot counters after ------------------------------------
        let r_after = guard.read_ctr.load(Ordering::Relaxed);
        let w_after = guard.written_ctr.load(Ordering::Relaxed);

        // Drop the guard so another task can use the slot. The two
        // snapshots and the `req` future all live on one thread, so
        // there's no torn-read concern even with Relaxed.
        drop(guard);

        Ok(Response {
            status,
            headers,
            body: ResponseBody::Buffered(bytes),
            bytes_sent: w_after.saturating_sub(w_before),
            bytes_received: r_after.saturating_sub(r_before),
            ttfb,
            total,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers — connection open + request building
// ---------------------------------------------------------------------------

/// Open one TCP+TLS connection and perform the HTTP/1 handshake,
/// spawning the driver task on the compio runtime.
async fn open_one(target: &Target, opts: &TransportOpts) -> Result<Slot, TransportError> {
    let connected = conn::open(target, opts).await?;
    let (read_ctr, written_ctr) = connected.counts();

    match connected {
        Connected::Plain(stream) => {
            let io = HyperStream::new(stream);
            let (sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
                .await
                .map_err(|e| TransportError::Protocol(format!("handshake: {e}")))?;

            // Spawn the connection driver. We don't hold a handle — the
            // task detaches and runs until the server (or pool Drop)
            // closes the stream. Errors here just end the task; the
            // SendRequest side learns via poll_ready returning Err.
            spawn(async move {
                let _ = conn.await;
            })
            .detach();

            Ok(Slot {
                sender: Some(sender),
                read_ctr,
                written_ctr,
            })
        }
        Connected::Tls { .. } => {
            // Reachable once conn::open gains TLS support.
            Err(TransportError::Tls(
                "TLS not wired in Phase B; conn::open returns error earlier".into(),
            ))
        }
    }
}

/// Build an `http::Request<Full<Bytes>>` from a `RequestPlan`,
/// expanding URL / header / body templates with `ctx`.
fn build_request(
    target: &Target,
    plan: &RequestPlan,
    ctx: &mut ScenarioContext,
) -> Result<Request<Full<Bytes>>, TransportError> {
    // URL — we only send the origin form (path+query) on the wire,
    // per hyper's convention for non-proxy HTTP/1.
    let mut url_buf: Vec<u8> = Vec::with_capacity(plan.url.estimated_size());
    plan.url.expand_into(&mut url_buf, &mut ctx.expand_ctx());
    let url_str = std::str::from_utf8(&url_buf)
        .map_err(|e| TransportError::RequestBuild(format!("url not utf-8: {e}")))?;

    // Strip scheme + authority if the template produced an absolute URL.
    let path_and_query = extract_path_and_query(url_str);
    let path_and_query = if path_and_query.is_empty() {
        "/"
    } else {
        path_and_query
    };

    let mut builder = Request::builder()
        .method(plan.method.clone())
        .uri(path_and_query);

    // Headers: start with `Host` since hyper requires it, then layer
    // user-supplied headers which can override it.
    builder = builder.header(http::header::HOST, target.addr());

    let mut hdr_name_buf: Vec<u8> = Vec::with_capacity(32);
    let mut hdr_val_buf: Vec<u8> = Vec::with_capacity(128);
    for (name_tpl, val_tpl) in &plan.headers {
        hdr_name_buf.clear();
        hdr_val_buf.clear();
        name_tpl.expand_into(&mut hdr_name_buf, &mut ctx.expand_ctx());
        val_tpl.expand_into(&mut hdr_val_buf, &mut ctx.expand_ctx());

        let name = http::HeaderName::from_bytes(&hdr_name_buf)
            .map_err(|e| TransportError::RequestBuild(format!("header name: {e}")))?;
        let value = HeaderValue::from_bytes(&hdr_val_buf)
            .map_err(|e| TransportError::RequestBuild(format!("header value: {e}")))?;
        builder = builder.header(name, value);
    }

    // Body.
    let body_bytes = match &plan.body {
        None => Bytes::new(),
        Some(BodySource::Static(b)) => b.clone(),
        Some(BodySource::Template(t)) => {
            let mut buf = Vec::with_capacity(t.estimated_size());
            t.expand_into(&mut buf, &mut ctx.expand_ctx());
            Bytes::from(buf)
        }
    };

    builder
        .body(Full::new(body_bytes))
        .map_err(|e| TransportError::RequestBuild(format!("build: {e}")))
}

/// Extract the path+query portion from a URL-ish string, falling back
/// to the whole input if no scheme is present.
///
/// Used because plan URLs are most often relative (`/api/x?k=v`) but
/// front-ends occasionally hand us absolute URLs from a CLI flag.
fn extract_path_and_query(url: &str) -> &str {
    if let Some(pos) = url.find("://") {
        // Absolute. Skip past "://authority/" — find the next '/' after
        // the authority start.
        let after_scheme = &url[pos + 3..];
        match after_scheme.find('/') {
            Some(i) => &after_scheme[i..],
            // No path → origin form is just "/"
            None => "/",
        }
    } else {
        // Already relative; strip any accidental fragment.
        match url.find('#') {
            Some(i) => &url[..i],
            None => url,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_path_and_query_from_absolute_url() {
        assert_eq!(
            extract_path_and_query("http://h:8080/api/x?k=v"),
            "/api/x?k=v"
        );
        assert_eq!(extract_path_and_query("http://h:8080"), "/");
        assert_eq!(extract_path_and_query("http://h:8080/"), "/");
    }

    #[test]
    fn extract_path_and_query_from_relative_url() {
        assert_eq!(extract_path_and_query("/api/x?k=v"), "/api/x?k=v");
        assert_eq!(extract_path_and_query("/api#frag"), "/api");
        assert_eq!(extract_path_and_query("/"), "/");
    }
}
