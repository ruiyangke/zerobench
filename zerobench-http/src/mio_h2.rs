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
//! - **No TLS**: plain-text h2c only. Pass `https://` and it panics.
//! - **No per-request template expansion**: request metadata is built once.
//! - **Single scenario only**: uses `plan.scenarios[0].steps[0]`.
//! - **No tokio runtime**: only tokio's `io-util` traits are used (via h2).

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use h2::client::{self, ResponseFuture, SendRequest};
use h2::RecvStream;
use mio::net::TcpStream;
use mio::{Events, Interest, Token};

use zerobench_core::plan::{Plan, Step};
use zerobench_core::stats::{ErrorKind, TaskStats};
use zerobench_core::transport::Target;

// ---------------------------------------------------------------------------
// MioAsyncAdapter — bridges mio TcpStream to tokio I/O traits
// ---------------------------------------------------------------------------

/// Wraps mio's `TcpStream` to implement tokio's `AsyncRead` / `AsyncWrite`.
///
/// All operations are non-blocking — returns `Poll::Pending` on `WouldBlock`.
/// No tokio runtime involved; polling is driven by the mio event loop.
struct MioAsyncAdapter {
    stream: TcpStream,
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

/// No-op waker. When h2 internally stores this waker and calls `wake()`, it
/// does nothing. We re-poll on every mio event anyway.
///
/// The h2 crate uses `atomic-waker` internally and will call `wake()` when
/// new frames arrive or flow-control windows change. Since our mio event
/// loop already re-polls on every socket readiness event, these waker
/// notifications are redundant — the socket event and the waker event
/// coincide.
struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

/// Thread-local no-op waker. Created once per thread, leaked into a
/// `'static` reference. Extremely cheap — one `Arc<NoopWaker>` per thread.
fn noop_waker() -> &'static Waker {
    static WAKER: OnceLock<Waker> = OnceLock::new();
    WAKER.get_or_init(|| Waker::from(Arc::new(NoopWaker)))
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
    fn poll_progress(&mut self, cx: &mut Context<'_>, now: Instant, stats: &mut TaskStats) -> bool {
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
                        let _status = response.status().as_u16();
                        stream.body = Some(response.into_body());
                        // Fall through to drain the body below.
                    }
                    Poll::Ready(Err(_)) => {
                        self.streams.swap_remove(i);
                        self.idle_slots += 1;
                        stats.record_error(0, ErrorKind::Read);
                        continue;
                    }
                    Poll::Pending => {
                        i += 1;
                        continue;
                    }
                }
            }

            // Drain the response body (DATA frames).
            let body = stream.body.as_mut().unwrap();
            let body_done = loop {
                match body.poll_data(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        // Release flow-control capacity so the sender can
                        // continue. Without this, the connection stalls once
                        // the window fills up.
                        let _ = body.flow_control().release_capacity(chunk.len());
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
                stats.record(0, co_free_latency, ttfb, 0, 0);
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
    ) -> bool {
        if self.idle_slots == 0 {
            return false;
        }

        // Clone the request — http::Request<()> is cheap.
        let req = request.clone();

        // `send_request` returns a `ResponseFuture` and a `SendStream`.
        // `true` = end of stream (no body to send for benchmarks).
        match self.send_request.send_request(req, true) {
            Ok((response_fut, _send_stream)) => {
                // No body to send — we passed `true` for end_of_stream.
                self.streams.push(H2Stream {
                    response_fut,
                    t0: Instant::now(),
                    intended_start,
                    body: None,
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
) -> io::Result<(SendRequest<Bytes>, client::Connection<MioAsyncAdapter, Bytes>)> {
    let waker = noop_waker();
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
    target: &Target,
    request: &http::Request<()>,
    max_streams: usize,
    stop: &AtomicBool,
    num_scenarios: usize,
    target_rps: Option<f64>,
) -> TaskStats {
    let addr: SocketAddr = target.addr().parse().expect("valid socket address");
    let mut poll = mio::Poll::new().expect("mio::Poll::new");
    let mut events = Events::with_capacity(256);
    let mut stats = TaskStats::new(num_scenarios);

    // Connect TCP.
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(_) => {
            stats.record_error(0, ErrorKind::Connect);
            return stats;
        }
    };
    stream.set_nodelay(true).ok();

    let token = Token(0);
    poll.registry()
        .register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
        .expect("mio register");

    // H2 handshake (blocks until complete, using mio events).
    let adapter = MioAsyncAdapter { stream };
    let (send_request, connection) = match handshake_blocking(adapter, &mut poll, &mut events) {
        Ok(pair) => pair,
        Err(_) => {
            stats.record_error(0, ErrorKind::Connect);
            return stats;
        }
    };

    let mut h2 = H2Conn {
        connection,
        send_request,
        streams: Vec::with_capacity(max_streams),
        idle_slots: max_streams,
    };

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
            if !h2.start_request(request, now) {
                break;
            }
        }
    }

    let waker = noop_waker();

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
                stats.record_error(0, ErrorKind::Keepup);
            }
        }

        // --- Assign pending tokens to idle stream slots -----------------
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if h2.idle_slots > 0 {
                    pending_tokens.pop_front();
                    h2.start_request(request, intended);
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
        let mut cx = Context::from_waker(waker);

        let alive = h2.poll_progress(&mut cx, batch_now, &mut stats);
        if !alive {
            // Connection died — record error and exit.
            stats.record_error(0, ErrorKind::Read);
            break;
        }

        // --- Saturate: refill stream slots after completions -----------
        if !open_loop {
            while h2.idle_slots > 0 {
                if !h2.start_request(request, batch_now) {
                    break;
                }
            }
        }

        // --- Post-event token assignment (open-loop) -------------------
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if h2.idle_slots > 0 {
                    pending_tokens.pop_front();
                    h2.start_request(request, intended);
                } else {
                    break;
                }
            }
        }
    }

    stats
}

// ---------------------------------------------------------------------------
// Multi-threaded driver
// ---------------------------------------------------------------------------

/// Spawn `num_threads` OS threads, each with its own TCP connection and mio
/// event loop driving `streams_per_thread` H2 streams. Blocks until
/// `duration` elapses.
pub fn run_mio_h2_threaded(
    target: &Target,
    plan: &Plan,
    num_threads: usize,
    total_streams: usize,
    duration: Duration,
    target_rps: Option<f64>,
) -> Vec<TaskStats> {
    assert!(
        !target.tls,
        "mio-h2 mode does not support TLS (https://). Use http:// or remove --mio."
    );

    let request = build_h2_request(plan, target);
    let stop = Arc::new(AtomicBool::new(false));
    let streams_per_thread = total_streams.div_ceil(num_threads);
    let per_thread_rps = target_rps.map(|rps| rps / num_threads as f64);

    // Timer thread — signals stop after `duration`.
    let stop_timer = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(duration);
        stop_timer.store(true, Ordering::Relaxed);
    });

    // Spawn worker threads.
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let target = target.clone();
            let request = request.clone();
            let stop = stop.clone();
            let num_scenarios = plan.scenarios.len();

            std::thread::spawn(move || {
                run_mio_h2_worker(
                    &target,
                    &request,
                    streams_per_thread,
                    &stop,
                    num_scenarios,
                    per_thread_rps,
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

/// Pre-build the HTTP/2 request from the first scenario's first step.
/// Templates are expanded once (not per-request).
fn build_h2_request(plan: &Plan, target: &Target) -> http::Request<()> {
    use zerobench_core::rng;
    use zerobench_core::scenario_context::ScenarioContext;

    let step = plan
        .scenarios
        .first()
        .and_then(|s| s.steps.first())
        .expect("plan must have at least one scenario with one step");

    let request_plan = match step {
        Step::Request(r) => r,
        _ => panic!("mio-h2 mode requires the first step to be a Request"),
    };

    let mut ctx = ScenarioContext::new(plan.vars.len(), rng::from_entropy());
    let mut url_buf = Vec::with_capacity(256);
    let mut ectx = ctx.expand_ctx();
    request_plan.url.expand_into(&mut url_buf, &mut ectx);
    let url_str = std::str::from_utf8(&url_buf).unwrap_or("/");

    // Extract path + query from the expanded URL.
    let path = extract_path_and_query(url_str);

    // H2 requests use :method, :path, :scheme, :authority pseudo-headers.
    // The `http::Request` builder sets these when given a URI + method.
    http::Request::builder()
        .method(request_plan.method.as_str())
        .uri(path.as_ref())
        .header("host", target.addr())
        .body(())
        .expect("failed to build H2 request")
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
    fn noop_waker_does_not_panic() {
        let waker = noop_waker();
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
}
