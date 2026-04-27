//! Mio-based HTTP/2 benchmark client — single TCP connection per thread with
//! N concurrent H2 streams, driven by manual future polling from the
//! synchronous mio event loop.
//!
//! Uses the `h2` crate directly (not hyper). The h2 futures are polled
//! manually from the mio event loop using a no-op waker — mio socket
//! events drive re-polling, not waker notifications.
//!
//! ```text
//! Thread i:
//!   mio::Poll + 1 TCP connection + N H2 streams
//!
//!   loop {
//!       poll.poll(&mut events)?;
//!       // 1. Drive the h2 connection (processes SETTINGS, WINDOW_UPDATE, etc.)
//!       // 2. Check for completed response streams
//!       // 3. Start new requests on idle stream slots
//!   }
//! ```
//!
//! # Modes
//!
//! Same as mio-h1:
//! - **Saturate (closed-loop)**: `target_rps = None`. Immediately starts a
//!   new request when a stream completes.
//! - **Open-loop (constant-rate)**: `target_rps = Some(rps)`. Token-bucket
//!   scheduling, coordinated-omission-free latency measurement.
//!
//! # Limitations
//!
//! - **TLS**: supported via `MioTlsStream` when the target is `https://`
//!   and an ALPN-configured `ClientConfig` is passed. Plain `h2c` is
//!   used on `http://` — the plain socket skips the handshake entirely.
//! - **No per-request template expansion**: request metadata is built once.
//! - **Single scenario only**: uses `plan.scenarios[0].steps[0]`.
//! - **No tokio runtime**: only tokio's `io-util` traits are used (via h2).

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use h2::client::{self, ResponseFuture, SendRequest};
use h2::RecvStream;
use mio::net::TcpStream;
use mio::{Events, Interest, Token};
use rustls::ClientConfig;

use rand::Rng;
use zerobench_core::plan::{Plan, Protocol, Step};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stats::{ErrorKind, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_runtime::{LiveSnapshot, Recorder, Sample};

use super::mio_tls::{MioStream, MioTlsStream};

// ---------------------------------------------------------------------------
// Connect helpers — split resolve / connect so we can re-resolve on failure
// ---------------------------------------------------------------------------

/// Attempt a single non-blocking TCP connect to `addr`.
#[inline]
fn connect_once(addr: SocketAddr) -> io::Result<TcpStream> {
    TcpStream::connect(addr)
}

/// Connect to `target`, re-resolving once on transient failure.
///
/// See `mio_h1::connect_with_retry` for the full rationale. Short version:
/// rolling deploys flip DNS; a stale first resolution is often the real
/// cause of ECONNREFUSED / EHOSTUNREACH. One retry is enough without
/// masking genuine server-down failures.
fn connect_with_retry(
    target: &Target,
    opts: &TransportOpts,
) -> io::Result<(TcpStream, SocketAddr)> {
    let addr = target.resolve(opts)?;
    match connect_once(addr) {
        Ok(s) => Ok((s, addr)),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::HostUnreachable
                    | io::ErrorKind::NetworkUnreachable
                    | io::ErrorKind::TimedOut
            ) =>
        {
            let fresh = target.resolve(opts)?;
            connect_once(fresh).map(|s| (s, fresh))
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Per-thread stream distribution — same shape as H1's distribute_conns.
// ---------------------------------------------------------------------------

/// Distribute `total` HTTP/2 streams across `threads` workers exactly.
///
/// Unlike the H1 case, each worker in H2 runs a single TCP connection
/// multiplexing the streams — so the "conns" we split here are really
/// streams. The arithmetic is identical and we delegate to the same
/// formula: first `total % threads` threads take the ceiling, the rest
/// take the floor. `threads` is clamped to `[1, total]`.
fn distribute_streams(total: usize, threads: usize) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }
    let threads = threads.max(1).min(total);
    let base = total / threads;
    let remainder = total % threads;
    let mut out = Vec::with_capacity(threads);
    for i in 0..threads {
        out.push(if i < remainder { base + 1 } else { base });
    }
    debug_assert_eq!(out.iter().sum::<usize>(), total);
    out
}

// ---------------------------------------------------------------------------
// MioAsyncAdapter — bridges mio TcpStream to tokio I/O traits
// ---------------------------------------------------------------------------

/// Wraps a `MioStream` (plain TCP or TLS) to implement tokio's
/// `AsyncRead` / `AsyncWrite`.
///
/// All operations are non-blocking — returns `Poll::Pending` on `WouldBlock`.
/// No tokio runtime involved; polling is driven by the mio event loop.
struct MioAsyncAdapter {
    stream: MioStream,
}

impl tokio::io::AsyncRead for MioAsyncAdapter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.stream.read(buf.initialize_unfilled()) {
            Ok(n) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl tokio::io::AsyncWrite for MioAsyncAdapter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.stream.write(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.stream.flush() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// No-op waker — mio events drive our polling, not waker notifications
// ---------------------------------------------------------------------------

/// Waker wired to mio's eventfd. When h2's internal state transitions
/// call `wake()`, this writes to an eventfd that wakes `poll.poll()` —
/// so the event loop re-polls immediately instead of waiting for the
/// next socket event or timeout.
struct MioWaker {
    inner: mio::Waker,
}

impl Wake for MioWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.inner.wake();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.inner.wake();
    }
}

/// Token for the mio::Waker registration. We ignore events with this
/// token — they just serve to break poll() out of its timeout.
const WAKER_TOKEN: Token = Token(usize::MAX);

/// Create a Waker that wakes the given mio::Poll via eventfd.
fn mio_waker(poll: &mio::Poll) -> Waker {
    let mio_waker = mio::Waker::new(poll.registry(), WAKER_TOKEN).expect("mio::Waker::new");
    Waker::from(Arc::new(MioWaker { inner: mio_waker }))
}

// ---------------------------------------------------------------------------
// H2 connection state
// ---------------------------------------------------------------------------

/// An in-flight H2 stream awaiting a response.
struct H2Stream {
    /// Future that resolves to the response headers.
    response_fut: ResponseFuture,
    /// Wall-clock instant when `send_request` was called.
    t0: Instant,
    /// For CO-free latency: the time this request was *scheduled* to start.
    intended_start: Instant,
    /// Once headers arrive, we drain the body here. `None` means we're
    /// still waiting for headers.
    body: Option<RecvStream>,
    /// Which scenario this stream belongs to — for per-scenario stats.
    scenario_id: u16,
    /// Approximate wire-size of the request: method + path + pseudo-
    /// headers + user headers. The HPACK encoder compresses this on
    /// the wire, but we track the pre-compression size so the report
    /// remains comparable with HTTP/1 (which doesn't have HPACK).
    request_bytes: u64,
    /// Sum of DATA-frame payloads received on this stream.
    response_bytes: u64,
    /// HTTP status code captured from the response future on arrival.
    /// Zero until the future resolves (never dispatched with 0 since
    /// we only finalise streams once body drain completes, which
    /// requires headers to have already arrived).
    status: u16,
    /// Response headers captured on future arrival, in the same
    /// `(lowercased-name, value)` format `apply_extractions` expects.
    /// Empty until headers arrive.
    extracted_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

/// State for a single H2 connection (one per worker thread).
struct H2Conn {
    /// The h2 connection future — must be polled to process frames.
    connection: client::Connection<MioAsyncAdapter, Bytes>,
    /// Clone-cheap handle for sending new requests.
    send_request: SendRequest<Bytes>,
    /// Active streams awaiting responses.
    streams: Vec<H2Stream>,
    /// Number of stream slots available for new requests.
    idle_slots: usize,
}

impl H2Conn {
    /// Drive the H2 connection (processes incoming frames: SETTINGS,
    /// WINDOW_UPDATE, PING, GOAWAY, etc.) and check for completed
    /// response streams.
    ///
    /// Returns `true` if the connection is still alive.
    #[allow(clippy::too_many_arguments)]
    fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        now: Instant,
        recorder: &mut Recorder<'_>,
        request_plan: &zerobench_core::plan::RequestPlan,
        ctx: &mut zerobench_core::scenario_context::ScenarioContext,
    ) -> bool {
        // 1. Drive the connection — this processes incoming frames and
        //    unblocks any pending send operations.
        match Pin::new(&mut self.connection).poll(cx) {
            Poll::Ready(Ok(())) => {
                // Connection closed cleanly (GOAWAY received).
                return false;
            }
            Poll::Ready(Err(_)) => {
                return false;
            }
            Poll::Pending => {
                // Normal — connection is alive, waiting for more frames.
            }
        }

        // 2. Check for completed response streams.
        let mut i = 0;
        while i < self.streams.len() {
            let stream = &mut self.streams[i];

            // If we don't have the body yet, poll the response future.
            if stream.body.is_none() {
                match Pin::new(&mut stream.response_fut).poll(cx) {
                    Poll::Ready(Ok(response)) => {
                        stream.status = response.status().as_u16();
                        // Capture headers in the lower-cased form
                        // `apply_extractions` expects. H2 pseudo-
                        // headers (:status / :scheme / :authority /
                        // :path) arrive with the `:` prefix; we keep
                        // them so scripts can extract them by name
                        // if they want (e.g. `.extract_header(":status")`).
                        let mut hdrs = Vec::with_capacity(response.headers().len());
                        for (name, value) in response.headers() {
                            let name_bytes = name.as_str().as_bytes();
                            let lower: Vec<u8> =
                                name_bytes.iter().map(|b| b.to_ascii_lowercase()).collect();
                            hdrs.push((lower, value.as_bytes().to_vec()));
                        }
                        stream.extracted_headers = hdrs;
                        stream.body = Some(response.into_body());
                        // Fall through to drain the body below.
                    }
                    Poll::Ready(Err(_)) => {
                        let sid = self.streams[i].scenario_id;
                        self.streams.swap_remove(i);
                        self.idle_slots += 1;
                        recorder.record_error(sid, ErrorKind::Read);
                        continue;
                    }
                    Poll::Pending => {
                        i += 1;
                        continue;
                    }
                }
            }

            // Drain the response body (DATA frames). Each successful
            // chunk contributes its length to `response_bytes` so the
            // end-of-stream record carries an accurate byte count
            // rather than hard-coded 0 (which lied in the report).
            let body = stream.body.as_mut().unwrap();
            let body_done = loop {
                match body.poll_data(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        let n = chunk.len();
                        stream.response_bytes = stream.response_bytes.saturating_add(n as u64);
                        // Release flow-control capacity so the sender can
                        // continue. Without this, the connection stalls once
                        // the window fills up.
                        let _ = body.flow_control().release_capacity(n);
                    }
                    Poll::Ready(Some(Err(_))) => {
                        break false; // error
                    }
                    Poll::Ready(None) => {
                        break true; // body fully drained
                    }
                    Poll::Pending => {
                        // More body data expected — will resume on next poll.
                        break false;
                    }
                }
            };

            if body_done {
                let stream = self.streams.swap_remove(i);
                let co_free_latency = now.duration_since(stream.intended_start);
                let ttfb = now.duration_since(stream.t0);
                let sid = stream.scenario_id;
                let bs = stream.request_bytes;
                let br = stream.response_bytes;
                recorder.record(
                    sid,
                    Sample {
                        latency: co_free_latency,
                        ttfb,
                        bytes_sent: bs,
                        bytes_recv: br,
                    },
                );
                // 4xx/5xx classification + user assertions +
                // extractions. Identical semantics to the mio_h1 +
                // cold_connect paths so `.expect_status(...)` /
                // `.extract_header(...)` from the DSL behave the
                // same under `--http-version h2`.
                if (400..500).contains(&stream.status) {
                    recorder.record_error(sid, ErrorKind::Status4xx);
                } else if (500..600).contains(&stream.status) {
                    recorder.record_error(sid, ErrorKind::Status5xx);
                }
                let assertion_failures = crate::http::raw_h1_common::check_assertions(
                    request_plan,
                    stream.status,
                    co_free_latency,
                );
                for _ in 0..assertion_failures {
                    recorder.record_error(sid, ErrorKind::AssertionFailed);
                }
                crate::http::raw_h1_common::apply_extractions(
                    request_plan,
                    stream.status,
                    &stream.extracted_headers,
                    ctx,
                );
                ctx.clear_all();
                self.idle_slots += 1;
                // Don't increment i — swap_remove moved the last element here.
            } else if stream.body.is_some() {
                // Check if body errored — if poll_data returned Err, we
                // handle it below. Otherwise it's Pending, move on.
                i += 1;
            } else {
                i += 1;
            }
        }

        true
    }

    /// Start a new request on an idle stream slot. Returns `true` on
    /// success.
    fn start_request(
        &mut self,
        request: &http::Request<()>,
        intended_start: Instant,
        scenario_id: u16,
    ) -> bool {
        if self.idle_slots == 0 {
            return false;
        }

        // Clone the request — http::Request<()> is cheap.
        let req = request.clone();

        // Estimate the request's logical wire size (method + path +
        // authority + headers, with 2 bytes of separator accounting).
        // HPACK compresses this on the actual wire, but the estimate
        // gives the report a non-zero bytes_sent figure that's in the
        // same units as the HTTP/1 backend reports.
        let request_bytes = estimate_request_bytes(&req);

        // `send_request` returns a `ResponseFuture` and a `SendStream`.
        // `true` = end of stream (no body to send for benchmarks).
        match self.send_request.send_request(req, true) {
            Ok((response_fut, _send_stream)) => {
                self.streams.push(H2Stream {
                    response_fut,
                    t0: Instant::now(),
                    intended_start,
                    body: None,
                    scenario_id,
                    request_bytes,
                    response_bytes: 0,
                    status: 0,
                    extracted_headers: Vec::new(),
                });
                self.idle_slots -= 1;
                true
            }
            Err(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// H2 handshake — blocking wrapper using mio events
// ---------------------------------------------------------------------------

/// Perform the HTTP/2 client handshake by manually polling the h2 future,
/// waiting for socket readiness via mio between polls.
fn handshake_blocking(
    adapter: MioAsyncAdapter,
    poll: &mut mio::Poll,
    events: &mut Events,
    waker: &Waker,
) -> io::Result<(
    SendRequest<Bytes>,
    client::Connection<MioAsyncAdapter, Bytes>,
)> {
    let mut cx = Context::from_waker(waker);
    let mut fut = Box::pin(client::handshake(adapter));

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok((sr, conn))) => return Ok((sr, conn)),
            Poll::Ready(Err(e)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("h2 handshake: {e}"),
                ))
            }
            Poll::Pending => {
                // Wait for socket readiness via mio, then re-poll.
                let _ = poll.poll(events, Some(Duration::from_millis(50)));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Single-thread H2 worker
// ---------------------------------------------------------------------------

/// Run the mio + H2 event loop on the calling thread. Blocks until `stop`
/// is set. Returns per-thread stats.
pub fn run_mio_h2_worker(
    plan: &Plan,
    target: &Target,
    opts: &TransportOpts,
    max_streams: usize,
    stop: &AtomicBool,
    target_rps: Option<f64>,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<&LiveSnapshot>,
) -> TaskStats {
    let mut poll = mio::Poll::new().expect("mio::Poll::new");
    let mut events = Events::with_capacity(256);
    let num_scenarios = plan.scenarios.len();
    let mut stats = TaskStats::new(num_scenarios);
    let mut ctx = ScenarioContext::new(plan.vars.len(), zerobench_core::rng::from_entropy());

    // Filter out SSE/WS scenarios — H2 only serves HTTP.
    let http_indices: Vec<usize> = plan
        .scenarios
        .iter()
        .enumerate()
        .filter_map(|(i, s)| (s.protocol() == Protocol::Http).then_some(i))
        .collect();
    if http_indices.is_empty() {
        return stats;
    }

    fn pick_scenario(http_indices: &[usize], ctx: &mut ScenarioContext) -> usize {
        if http_indices.len() <= 1 {
            http_indices[0]
        } else {
            http_indices[ctx.rng.gen_range(0..http_indices.len())]
        }
    }

    // Connect TCP. `connect_with_retry` re-resolves once on transient
    // failures so rolling-deploy DNS flips don't fail the whole worker.
    let mut tcp = match connect_with_retry(target, opts) {
        Ok((s, _)) => s,
        Err(_) => {
            // Scope the Recorder so its `&mut stats` borrow drops
            // before the early `return stats;` below.
            Recorder::new(&mut stats, live).record_error(0, ErrorKind::Connect);
            return stats;
        }
    };
    tcp.set_nodelay(opts.tcp_nodelay).ok();

    let token = Token(0);
    poll.registry()
        .register(&mut tcp, token, Interest::READABLE | Interest::WRITABLE)
        .expect("mio register");

    // Wrap in TLS if needed.
    let stream = if let Some(ref config) = tls_config {
        let sni_name = target.sni_name();
        let mut tls_stream = match MioTlsStream::new(tcp, Arc::clone(config), sni_name) {
            Ok(s) => s,
            Err(_) => {
                Recorder::new(&mut stats, live).record_error(0, ErrorKind::Connect);
                return stats;
            }
        };
        if tls_stream
            .complete_handshake(&mut poll, token, opts.connect_timeout)
            .is_err()
        {
            Recorder::new(&mut stats, live).record_error(0, ErrorKind::Connect);
            return stats;
        }
        // Re-register after handshake: mio uses edge-triggered epoll
        // (EPOLLET), so events consumed during the handshake won't
        // re-fire. Reregister forces fresh notification delivery.
        poll.registry()
            .reregister(
                tls_stream.tcp_stream_mut(),
                token,
                Interest::READABLE | Interest::WRITABLE,
            )
            .expect("mio reregister after TLS handshake");
        MioStream::Tls(tls_stream)
    } else {
        MioStream::Plain(tcp)
    };

    // Create a mio::Waker so h2's internal wake() calls break us out
    // of poll.poll() immediately — no waiting for socket events.
    let waker = mio_waker(&poll);

    // H2 handshake (blocks until complete, using mio events).
    let adapter = MioAsyncAdapter { stream };
    let (send_request, connection) =
        match handshake_blocking(adapter, &mut poll, &mut events, &waker) {
            Ok(pair) => pair,
            Err(_) => {
                Recorder::new(&mut stats, live).record_error(0, ErrorKind::Connect);
                return stats;
            }
        };

    let mut h2 = H2Conn {
        connection,
        send_request,
        streams: Vec::with_capacity(max_streams),
        idle_slots: max_streams,
    };

    // mio_h2 supports only one HTTP scenario today (documented
    // limitation). Grab its RequestPlan once so `poll_progress` can
    // pass assertion + extraction lists when a response completes.
    let request_plan_for_assertions = plan.scenarios[http_indices[0]]
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Request(r) => Some(r.clone()),
            _ => None,
        })
        .expect("mio-h2 HTTP scenario must contain at least one Request step");

    // Unified sink for the main loop — every completed op or error
    // fans out to both TaskStats and LiveSnapshot through one call.
    let mut recorder = Recorder::new(&mut stats, live);

    // Token scheduling (same as H1).
    let open_loop = target_rps.is_some();
    let token_interval = target_rps.map(|rps| Duration::from_secs_f64(1.0 / rps));
    let started_at = Instant::now();
    let mut next_token_at = started_at;
    let mut pending_tokens: VecDeque<Instant> = VecDeque::with_capacity(max_streams * 2);
    let max_pending = max_streams * 2;

    // If saturate mode, fill all stream slots immediately.
    if !open_loop {
        let now = Instant::now();
        for _ in 0..max_streams {
            let sid = pick_scenario(&http_indices, &mut ctx) as u16;
            let req = build_h2_request_from_plan(plan, sid as usize, target, &mut ctx);
            if !h2.start_request(&req, now, sid) {
                break;
            }
            ctx.clear_all();
        }
    }

    while !stop.load(Ordering::Relaxed) {
        // --- Open-loop: generate tokens into the pending queue ----------
        if let Some(interval) = token_interval {
            let now = Instant::now();
            while now >= next_token_at {
                pending_tokens.push_back(next_token_at);
                next_token_at += interval;
            }
            // Cap the queue — excess tokens are keepup drops.
            while pending_tokens.len() > max_pending {
                pending_tokens.pop_front();
                recorder.record_error(0, ErrorKind::Keepup);
            }
        }

        // --- Assign pending tokens to idle stream slots -----------------
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if h2.idle_slots > 0 {
                    pending_tokens.pop_front();
                    let sid = pick_scenario(&http_indices, &mut ctx) as u16;
                    let req = build_h2_request_from_plan(plan, sid as usize, target, &mut ctx);
                    h2.start_request(&req, intended, sid);
                    ctx.clear_all();
                } else {
                    break;
                }
            }
        }

        // --- Calculate poll timeout ------------------------------------
        let poll_timeout = if open_loop {
            if !pending_tokens.is_empty() {
                Some(Duration::ZERO)
            } else {
                let until_next = next_token_at.saturating_duration_since(Instant::now());
                Some(until_next.min(Duration::from_millis(1)))
            }
        } else {
            Some(Duration::from_millis(1))
        };

        let _ = poll.poll(&mut events, poll_timeout);

        let batch_now = Instant::now();

        // --- Drive H2 connection + check completed streams -------------
        let mut cx = Context::from_waker(&waker);

        let alive = h2.poll_progress(
            &mut cx,
            batch_now,
            &mut recorder,
            &request_plan_for_assertions,
            &mut ctx,
        );
        if !alive {
            // Connection died — record error and exit.
            recorder.record_error(0, ErrorKind::Read);
            break;
        }

        // --- Saturate: refill stream slots after completions -----------
        if !open_loop {
            while h2.idle_slots > 0 {
                let sid = pick_scenario(&http_indices, &mut ctx) as u16;
                let req = build_h2_request_from_plan(plan, sid as usize, target, &mut ctx);
                if !h2.start_request(&req, batch_now, sid) {
                    break;
                }
                ctx.clear_all();
            }
        }

        // --- Post-event token assignment (open-loop) -------------------
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if h2.idle_slots > 0 {
                    pending_tokens.pop_front();
                    let sid = pick_scenario(&http_indices, &mut ctx) as u16;
                    let req = build_h2_request_from_plan(plan, sid as usize, target, &mut ctx);
                    h2.start_request(&req, intended, sid);
                    ctx.clear_all();
                } else {
                    break;
                }
            }
        }
    }

    // Drop the recorder so `stats` is no longer borrowed.
    drop(recorder);
    stats
}

// ---------------------------------------------------------------------------
// Multi-threaded driver
// ---------------------------------------------------------------------------

/// Spawn up to `num_threads` OS threads, each with its own TCP connection
/// and mio event loop driving an exact share of `total_streams`. Blocks
/// until `duration` elapses.
///
/// Stream distribution mirrors the H1 fix: first `total_streams % threads`
/// workers take `floor + 1` streams, the rest take the floor. `num_threads`
/// is clamped to `min(num_threads, total_streams)` so we never spin up
/// workers that own zero streams.
pub fn run_mio_h2_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    num_threads: usize,
    total_streams: usize,
    duration: Duration,
    target_rps: Option<f64>,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<Arc<LiveSnapshot>>,
    stop_flag: Option<Arc<AtomicBool>>,
) -> Vec<TaskStats> {
    // When an external stop flag is provided (e.g. from the TUI),
    // use it directly — the caller manages the timer. Otherwise
    // create our own timer thread.
    let stop = stop_flag.unwrap_or_else(|| {
        let flag = Arc::new(AtomicBool::new(false));
        let timer = flag.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer.store(true, Ordering::Relaxed);
        });
        flag
    });
    let per_thread_streams = distribute_streams(total_streams, num_threads);
    let active_threads = per_thread_streams.len();
    let per_thread_rps = if active_threads == 0 {
        None
    } else {
        target_rps.map(|rps| rps / active_threads as f64)
    };
    let plan = Arc::new(plan.clone());

    // Spawn worker threads.
    let handles: Vec<_> = per_thread_streams
        .into_iter()
        .map(|streams| {
            let target = target.clone();
            let opts = opts.clone();
            let plan = plan.clone();
            let stop = stop.clone();
            let tls_config = tls_config.clone();
            let live = live.clone();

            std::thread::spawn(move || {
                run_mio_h2_worker(
                    &plan,
                    &target,
                    &opts,
                    streams,
                    &stop,
                    per_thread_rps,
                    tls_config,
                    live.as_deref(),
                )
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().expect("mio-h2 worker thread panicked"))
        .collect()
}

// ---------------------------------------------------------------------------
// Request builder — expand once at startup
// ---------------------------------------------------------------------------

/// Build an HTTP/2 request from a specific scenario with per-request
/// template expansion. `scenario_idx` selects the scenario;
/// `ctx` provides the RNG, counter, and var state.
///
/// Uses [`ScenarioContext::take_url_buf`] / [`ScenarioContext::return_url_buf`]
/// and [`ScenarioContext::take_header_bufs`] / [`ScenarioContext::return_header_bufs`]
/// so we don't reach into the buffer fields directly — those are now
/// `#[doc(hidden)]` and the accessors are the supported API.
fn build_h2_request_from_plan(
    plan: &Plan,
    scenario_idx: usize,
    target: &Target,
    ctx: &mut ScenarioContext,
) -> http::Request<()> {
    // Walk the scenario's steps to find the first HTTP Request step,
    // skipping leading Pause/PauseRandom. Post-Tier-1 the caller already
    // filters non-HTTP scenarios (see `http_indices` in the worker), so
    // a Request must exist — if not, the plan is malformed.
    let request_plan = plan.scenarios[scenario_idx]
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Request(r) => Some(r),
            _ => None,
        })
        .expect("mio-h2 HTTP scenario must contain at least one Request step");

    // Expand the URL template into the context's reusable URL buffer
    // using the closure-style accessor. `with_url_buf` clears the buffer
    // before we write into it and re-installs it on return.
    let url_buf = {
        let mut taken = ctx.take_url_buf();
        taken.clear();
        request_plan
            .url
            .expand_into(&mut taken, &mut ctx.expand_ctx());
        taken
    };
    let url_str = std::str::from_utf8(&url_buf).unwrap_or("/");
    let path = extract_path_and_query(url_str);

    let mut builder = http::Request::builder()
        .method(request_plan.method.as_str())
        .uri(path.as_ref())
        .header("host", target.addr());

    // Expand header templates. `take_header_bufs` hands us both scratch
    // `Vec<u8>`s; we return them together once all headers are consumed.
    let (mut hdr_name, mut hdr_val) = ctx.take_header_bufs();
    for (name_tpl, val_tpl) in &request_plan.headers {
        hdr_name.clear();
        hdr_val.clear();
        // Re-borrow ExpandCtx fresh for each header — rng is unique so
        // we can't hold it across the header loop iterations.
        let mut ectx = ctx.expand_ctx();
        name_tpl.expand_into(&mut hdr_name, &mut ectx);
        val_tpl.expand_into(&mut hdr_val, &mut ectx);
        if let (Ok(name), Ok(val)) = (
            std::str::from_utf8(&hdr_name),
            std::str::from_utf8(&hdr_val),
        ) {
            builder = builder.header(name, val);
        }
    }
    // Hand both buffer families back to the context for the next request.
    ctx.return_url_buf(url_buf);
    ctx.return_header_bufs(hdr_name, hdr_val);

    builder.body(()).expect("failed to build H2 request")
}

/// Best-effort pre-HPACK wire-size estimate for an `http::Request<()>`.
///
/// Used by the per-stream stats so `bytes_sent` in the final report
/// is not hard-coded 0. Accounts for method, authority, path, scheme,
/// and user headers; ignores HPACK dynamic-table dedup savings and
/// frame framing overhead. The result is an upper bound that stays
/// comparable with the HTTP/1 backend's request-size reporting.
fn estimate_request_bytes(req: &http::Request<()>) -> u64 {
    let mut n: u64 = 0;
    // Method + space + path.
    n = n.saturating_add(req.method().as_str().len() as u64);
    n = n.saturating_add(1);
    n = n.saturating_add(
        req.uri()
            .path_and_query()
            .map(|p| p.as_str().len())
            .unwrap_or(1) as u64,
    );
    // Authority (":authority" pseudo-header).
    if let Some(a) = req.uri().authority() {
        n = n.saturating_add(a.as_str().len() as u64);
    }
    // Each header contributes `name + ": " + value + CRLF`.
    for (name, value) in req.headers() {
        n = n.saturating_add(name.as_str().len() as u64);
        n = n.saturating_add(2);
        n = n.saturating_add(value.as_bytes().len() as u64);
        n = n.saturating_add(2);
    }
    n
}

/// Extract origin-form path+query from a potentially absolute URL.
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
    fn mio_waker_does_not_panic() {
        let poll = mio::Poll::new().unwrap();
        let waker = mio_waker(&poll);
        waker.wake_by_ref();
        // Just verifying it doesn't crash.
    }

    #[test]
    fn extract_path_and_query_works() {
        assert_eq!(extract_path_and_query("http://h:80"), "/");
        assert_eq!(extract_path_and_query("http://h:80/foo"), "/foo");
        assert_eq!(extract_path_and_query("http://h:80/foo?q=1"), "/foo?q=1");
        assert_eq!(extract_path_and_query("/bar"), "/bar");
        assert_eq!(extract_path_and_query(""), "/");
    }

    // ------------------------------------------------------------------
    // distribute_streams — mirrors the H1 distribute_conns fix
    // ------------------------------------------------------------------

    #[test]
    fn distribute_streams_small_total_large_threads() {
        let per = distribute_streams(20, 32);
        assert_eq!(per.iter().sum::<usize>(), 20);
        assert_eq!(per.len(), 20);
    }

    #[test]
    fn distribute_streams_even() {
        let per = distribute_streams(100, 4);
        assert_eq!(per.iter().sum::<usize>(), 100);
        assert_eq!(per.len(), 4);
        assert!(per.iter().all(|&n| n == 25));
    }

    #[test]
    fn distribute_streams_uneven() {
        let per = distribute_streams(7, 3);
        assert_eq!(per.iter().sum::<usize>(), 7);
        assert_eq!(per, vec![3, 2, 2]);
    }

    #[test]
    fn distribute_streams_one_stream_many_threads() {
        let per = distribute_streams(1, 8);
        assert_eq!(per, vec![1]);
    }

    #[test]
    fn distribute_streams_zero_total() {
        assert!(distribute_streams(0, 4).is_empty());
    }

    #[test]
    fn distribute_streams_many_pairs_exact_total() {
        for (total, threads) in [(20, 32), (100, 4), (7, 3), (1, 8), (0, 4), (1000, 17)] {
            let per = distribute_streams(total, threads);
            assert_eq!(
                per.iter().sum::<usize>(),
                total,
                "({total}, {threads}) distribution lost count"
            );
        }
    }
}
