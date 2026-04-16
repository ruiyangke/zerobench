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
use futures_util::lock::{Mutex, OwnedMutexGuard};
use http::{HeaderMap, HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
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
///
/// Slots are held as `Arc<Mutex<Slot>>` (not plain `Mutex<Slot>`) so the
/// streaming path ([`Http1Pool::exchange_streaming`]) can return an
/// owned lock guard that outlives the immediate `&self` borrow — the
/// stream body must own the slot until the consumer finishes draining.
#[derive(Debug)]
pub struct Http1Pool {
    /// One mutex per slot. `futures_util::lock::Mutex` is chosen for
    /// the async-await API and fairness; `async-lock` or
    /// `parking_lot::Mutex` + manual parking would also work but add a
    /// dep or hand-rolled machinery.
    slots: Box<[Arc<Mutex<Slot>>]>,
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

    /// Number of slots in the pool.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// `true` if the pool has zero slots (cannot happen for pools
    /// returned from [`Self::new`]; provided for lint-friendly APIs).
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Target this pool was opened against. Cheap — returns a borrow of
    /// the [`Target`] stored at construction time.
    pub fn target(&self) -> &Target {
        &self.target
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
        // Enforce request_timeout around headers *and* body. Every exit
        // path that leaves the slot in an undefined state (timeout *or*
        // connection error) must null the sender: the driver task has
        // either been cancelled mid-write or seen the connection die,
        // so the next request on this slot would otherwise hit an
        // out-of-sync SendRequest.
        let res = match compio::time::timeout(self.request_timeout, send_fut).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                // Connection died — mark slot empty so future users don't
                // hit the same dead sender (conservative: we don't try
                // to reopen here; that's a later task).
                guard.sender = None;
                return Err(TransportError::Protocol(format!("send_request: {e}")));
            }
            Err(_) => {
                // Timeout — the send_request future is being dropped, which
                // leaves the underlying hyper connection in an indeterminate
                // state. Invalidate the slot so subsequent exchanges fail
                // cleanly instead of hanging or reading stale bytes.
                guard.sender = None;
                return Err(TransportError::Timeout);
            }
        };

        let ttfb = t0.elapsed();

        // Split status + headers from the body first; collecting body
        // consumes `res`.
        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let body = res.into_body();

        let collected = match compio::time::timeout(
            self.request_timeout.saturating_sub(ttfb).max(Duration::from_millis(1)),
            body.collect(),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                guard.sender = None;
                return Err(TransportError::Protocol(format!("body: {e}")));
            }
            Err(_) => {
                // Body-collection timeout — same reasoning as above. The
                // body future is cancelled mid-read; invalidate the slot.
                guard.sender = None;
                return Err(TransportError::Timeout);
            }
        };

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

    /// Send one request through the pool and return the response headers
    /// plus a handle to the streaming body.
    ///
    /// Unlike [`Self::exchange`], the response body is **not** collected
    /// into memory before the method returns. The caller gets access to
    /// the raw [`hyper::body::Incoming`] (pre-dechunked by hyper) and can
    /// poll frames at its own pace. The slot's mutex is held by the
    /// returned [`StreamingResponse`] until it is dropped, so the
    /// connection is reserved for as long as the caller needs to consume
    /// the stream. This is the entry point used by the SSE runner to
    /// time per-chunk latency.
    ///
    /// Errors are surfaced the same way as [`Self::exchange`]: connect,
    /// timeout, protocol, etc. Timeouts here cover only the "send request
    /// → first response frame" window; the caller is responsible for
    /// enforcing any overall stream deadline it cares about.
    pub async fn exchange_streaming(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<StreamingResponse, TransportError> {
        // --- Build the request (template expansion happens here) ----
        let req = build_request(&self.target, plan, ctx)?;

        // --- Acquire an owned slot guard --------------------------------
        //
        // The streaming path returns after response headers arrive but
        // *before* the body is drained. We need to keep the slot locked
        // for the duration of the stream, which outlives the `&self`
        // borrow — hence `lock_owned` (needs `Arc<Mutex<Slot>>` on our
        // side), and the caller holds the returned `StreamingResponse`.
        let start_idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        let n = self.slots.len();
        let mut guard_opt: Option<(usize, OwnedMutexGuard<Slot>)> = None;
        for offset in 0..n {
            let idx = (start_idx + offset) % n;
            if let Some(g) = self.slots[idx].clone().try_lock_owned() {
                guard_opt = Some((idx, g));
                break;
            }
        }
        let (slot_idx, mut guard) = match guard_opt {
            Some(g) => g,
            None => {
                let idx = start_idx % n;
                let g = self.slots[idx].clone().lock_owned().await;
                (idx, g)
            }
        };

        // --- Send request + await response headers -----------------------
        let t0 = Instant::now();
        let sender = guard
            .sender
            .as_mut()
            .ok_or_else(|| TransportError::Connect(format!("slot {slot_idx} unavailable")))?;

        let send_fut = sender.send_request(req);
        let res = match compio::time::timeout(self.request_timeout, send_fut).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                guard.sender = None;
                return Err(TransportError::Protocol(format!("send_request: {e}")));
            }
            Err(_) => {
                guard.sender = None;
                return Err(TransportError::Timeout);
            }
        };

        let ttfb = t0.elapsed();

        // Detach headers from the response so `body` owns its own frame
        // stream without being tied to the response envelope.
        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let body = res.into_body();

        Ok(StreamingResponse {
            status,
            headers,
            ttfb,
            body,
            read_ctr: Arc::clone(&guard.read_ctr),
            written_ctr: Arc::clone(&guard.written_ctr),
            _guard: guard,
        })
    }
}

/// Response returned from [`Http1Pool::exchange_streaming`].
///
/// Owns the response's streaming body plus the slot guard that backs it.
/// When the `StreamingResponse` is dropped the slot is released back to
/// the pool. If the connection died mid-stream, the slot's internal
/// `sender` must be nulled by the *caller* before drop (via
/// [`Self::invalidate`]) — the pool has no way to know the stream ended
/// in error otherwise, and a subsequent `exchange` on the same slot
/// would try to reuse a broken `SendRequest`.
///
/// `body` is hyper's post-dechunked frame stream: `body.frame().await`
/// yields data bytes already stripped of HTTP/1.1 chunked-encoding
/// framing. See the zerobench-sse crate for the line-level SSE parsing
/// that consumes these bytes.
pub struct StreamingResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: HeaderMap,
    /// Time from sending the first byte to receiving the first byte of
    /// the status line.
    pub ttfb: Duration,
    /// Streaming body. Call `.frame().await` (via `BodyExt`) to pull
    /// frames; hyper de-chunks HTTP/1 `Transfer-Encoding: chunked`
    /// internally.
    pub body: Incoming,
    /// Shared handle to the slot's read-bytes counter. The caller can
    /// snapshot before/after draining the stream to compute on-wire
    /// bytes received.
    pub read_ctr: Arc<AtomicU64>,
    /// Shared handle to the slot's written-bytes counter. Snapshot
    /// before the exchange starts to compute per-request write bytes.
    pub written_ctr: Arc<AtomicU64>,
    /// Owned slot guard; released on drop.
    _guard: OwnedMutexGuard<Slot>,
}

impl StreamingResponse {
    /// Invalidate the underlying slot.
    ///
    /// Call this if the stream errored or you dropped it mid-frame;
    /// otherwise a subsequent `exchange` on the same slot would try to
    /// reuse a sender whose connection state is undefined. Harmless to
    /// call on a cleanly-drained stream — the slot is invalidated either
    /// way, and v0.0.1 doesn't attempt lazy reconnect.
    pub fn invalidate(mut self) {
        self._guard.sender = None;
        drop(self._guard);
    }
}

impl std::fmt::Debug for StreamingResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("ttfb", &self.ttfb)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Helpers — connection open + request building
// ---------------------------------------------------------------------------

/// Open one TCP+TLS connection and perform the HTTP/1 handshake,
/// spawning the driver task on the compio runtime.
async fn open_one(target: &Target, opts: &TransportOpts) -> Result<Slot, TransportError> {
    // HTTP/1 pool always negotiates `http/1.1` when the target is TLS;
    // a server that insists on `h2` will be handled up a layer via the
    // ALPN probe in `HttpTransport::build_client`, not here.
    let alpn: &[&[u8]] = if target.tls { &[b"http/1.1"] } else { &[] };
    let connected = conn::open(target, opts, alpn).await?;
    let (read_ctr, written_ctr) = connected.counts();

    let (sender, conn_driver): (SendRequest<Full<Bytes>>, _) = match connected {
        Connected::Plain { stream, .. } => {
            let io = HyperStream::new(stream);
            let (sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
                .await
                .map_err(|e| TransportError::Protocol(format!("handshake: {e}")))?;
            // Box the driver future so both arms produce the same type.
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> =
                Box::pin(async move {
                    let _ = conn.await;
                });
            (sender, fut)
        }
        Connected::Tls { stream, .. } => {
            // `TlsStream<CountingStream<TcpStream>>` satisfies compio's
            // AsyncRead/AsyncWrite via compio-tls — HyperStream bridges
            // it into hyper's trait world the same as the plain path.
            let io = HyperStream::new(*stream);
            let (sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
                .await
                .map_err(|e| TransportError::Protocol(format!("handshake: {e}")))?;
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> =
                Box::pin(async move {
                    let _ = conn.await;
                });
            (sender, fut)
        }
    };

    // Spawn the connection driver. We don't hold a handle — the task
    // detaches and runs until the server (or pool Drop) closes the
    // stream. Errors here just end the task; the SendRequest side learns
    // via poll_ready returning Err.
    spawn(conn_driver).detach();

    Ok(Slot {
        sender: Some(sender),
        read_ctr,
        written_ctr,
    })
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
    // `extract_path_and_query` is guaranteed to return a non-empty slice
    // and handles query/fragment rules consistently across absolute and
    // relative inputs. Only owned in the edge case of an absolute URL
    // whose authority is immediately followed by '?' (needs a synthetic
    // leading '/').
    let path_and_query = extract_path_and_query(url_str);

    let mut builder = Request::builder()
        .method(plan.method.clone())
        .uri(path_and_query.as_ref());

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

/// Extract the origin-form path+query from a URL-ish string.
///
/// Rules:
/// - `#fragment` is always stripped (it's client-side and must not
///   appear on the wire).
/// - `?query` is always preserved, even when there's no path — an
///   absolute URL like `http://h:80?q=1` maps to `"/?q=1"`.
/// - A missing path is replaced with `"/"`. If the whole input has no
///   path, no query, and no fragment, the result is `"/"`.
///
/// Returns a `Cow<str>` — borrowed for the common case where we can
/// return a suffix of the input unchanged, owned only when we must
/// prepend `/` (absolute URL with query but no path).
///
/// Used because plan URLs are most often relative (`/api/x?k=v`) but
/// front-ends occasionally hand us absolute URLs from a CLI flag.
fn extract_path_and_query(url: &str) -> std::borrow::Cow<'_, str> {
    use std::borrow::Cow;

    // Narrow to the part after scheme://authority if the URL is
    // absolute; otherwise operate on the input as-is. Track whether
    // we entered the absolute branch so we can prepend "/" when the
    // authority is followed directly by '?'.
    let (rest, absolute) = if let Some(pos) = url.find("://") {
        let after_scheme = &url[pos + 3..];
        // Authority ends at the first '/', '?', or '#'.
        match after_scheme.find(|c: char| c == '/' || c == '?' || c == '#') {
            Some(i) => (&after_scheme[i..], true),
            // No path / query / fragment at all — "/" is the only
            // sensible origin-form.
            None => return Cow::Borrowed("/"),
        }
    } else {
        (url, false)
    };

    // Strip fragment in both branches. Anything after '#' is
    // client-side and not part of the origin-form request.
    let without_fragment = match rest.find('#') {
        Some(i) => &rest[..i],
        None => rest,
    };

    if without_fragment.is_empty() {
        // "#frag" alone (and theoretically empty input) — default.
        return Cow::Borrowed("/");
    }

    // Absolute URL where the authority was followed directly by '?' has
    // no explicit path; prepend "/" so the origin-form is well-formed.
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

    // --- Absolute URLs ------------------------------------------------------

    #[test]
    fn absolute_no_path_no_query_defaults_to_slash() {
        assert_eq!(extract_path_and_query("http://h:80"), "/");
    }

    #[test]
    fn absolute_bare_slash_stays_slash() {
        assert_eq!(extract_path_and_query("http://h:80/"), "/");
    }

    #[test]
    fn absolute_query_without_path_preserves_query() {
        // Regression: the old implementation located the path by searching
        // for the next '/' after the authority and silently dropped any
        // query that appeared before one — i.e. when there was no path.
        assert_eq!(extract_path_and_query("http://h:80?q=1"), "/?q=1");
    }

    #[test]
    fn absolute_path_with_query_preserves_both() {
        assert_eq!(extract_path_and_query("http://h:80/foo?q=1"), "/foo?q=1");
    }

    #[test]
    fn absolute_fragment_is_stripped() {
        assert_eq!(extract_path_and_query("http://h:80/foo#frag"), "/foo");
    }

    #[test]
    fn absolute_query_plus_fragment_keeps_query_drops_fragment() {
        assert_eq!(
            extract_path_and_query("http://h:80/foo?q=1#frag"),
            "/foo?q=1"
        );
    }

    #[test]
    fn absolute_fragment_only_after_authority_defaults_to_slash() {
        // "http://h:80#f" has no path, no query, only a fragment — the
        // fragment is client-side so we strip it and fall back to "/".
        assert_eq!(extract_path_and_query("http://h:80#frag"), "/");
    }

    // --- Relative URLs ------------------------------------------------------

    #[test]
    fn relative_path_with_query_preserves_query() {
        assert_eq!(
            extract_path_and_query("/relative/path?q=1"),
            "/relative/path?q=1"
        );
    }

    #[test]
    fn relative_fragment_is_stripped() {
        assert_eq!(extract_path_and_query("/path#frag"), "/path");
    }

    #[test]
    fn relative_bare_slash_stays_slash() {
        assert_eq!(extract_path_and_query("/"), "/");
    }

    #[test]
    fn relative_path_query_fragment_strips_only_fragment() {
        assert_eq!(
            extract_path_and_query("/path?q=1#frag"),
            "/path?q=1"
        );
    }

    #[test]
    fn relative_empty_falls_back_to_slash() {
        assert_eq!(extract_path_and_query(""), "/");
    }
}
