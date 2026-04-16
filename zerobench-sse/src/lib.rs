//! zerobench-sse — Server-Sent Events benchmarking runner.
//!
//! Unlike `zerobench-http`, SSE doesn't cleanly slot into the
//! [`Transport`](zerobench_core::Transport) trait: that trait returns a
//! single-shot [`Response`](zerobench_core::Response), but an SSE
//! exchange emits many chunks over time and we want per-chunk latency
//! metrics, not one number at the end. The right shape for SSE is a
//! dedicated runner that the CLI picks when `--sse` is set.
//!
//! # Architecture
//!
//! - [`SseRunner::run_iteration`] — runs one SSE request to completion
//!   (stream closed by server, `data: [DONE]` sentinel, or deadline
//!   reached) and folds metrics into an [`SseStats`].
//! - [`SseStats`] — HDR histograms for TTFB and inter-chunk latency,
//!   plus counters (chunks, bytes, completed streams, errors).
//! - [`run_sse_saturate`] — spawns N concurrent workers, each calling
//!   `run_iteration` in a loop until the shared `StopSignal` trips.
//!
//! # One connection per iteration
//!
//! SSE connections are long-lived and single-use: once a stream closes,
//! the connection is done. Rather than going through the shared
//! `Http1Pool` (whose slots would be invalidated after the first stream
//! and produce spurious "slot unavailable" errors on every subsequent
//! iteration), the runner opens a fresh TCP + HTTP/1 connection for each
//! iteration. This mirrors v1's intent without v1's round-robin bug:
//! N workers × 1 connection each × 1 stream per iteration, all
//! concurrent.
//!
//! hyper already de-chunks `Transfer-Encoding: chunked`, so the runner
//! sees raw SSE bytes. The v1 bench had a substring-search-for-
//! `0\r\n\r\n` in the chunked framing; this implementation leans on
//! hyper's proper decoder and parses events via
//! [`SseLineParser`](line_parser::SseLineParser).
//!
//! # What's not here
//!
//! - Reconnection with exponential backoff + `Last-Event-ID`. v0.0.1
//!   runs one stream per iteration; if it fails, the error is counted.
//! - Event-name filtering (`event: foo`). The runner only counts
//!   `data:` events.
//! - `retry:` field. Ignored.
//! - TLS. [`Target::tls`] == `true` surfaces the same error as the
//!   HTTP transport until TLS wiring lands for the whole stack.

pub mod line_parser;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use cyper_core::HyperStream;
use hdrhistogram::Histogram;
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Incoming};
use hyper::client::conn::http1;

use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stop::StopSignal;
use zerobench_core::transport::{Target, TransportError, TransportOpts};
use zerobench_http::Connected;

pub use line_parser::{SseEvent, SseLineParser};

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Histogram bounds for SSE metrics — `[1, 60_000_000_000]` ns, 3 sig
/// figs. Same as the core stats crate; keeps the reporter's percentile
/// math uniform across protocols.
const HIST_LO_NS: u64 = 1;
const HIST_HI_NS: u64 = 60_000_000_000;
const HIST_SIG: u8 = 3;

/// Per-worker SSE statistics.
///
/// Lives locally on each worker task; one instance per worker. Merged
/// into an [`SseSummary`] at end-of-run.
#[derive(Debug, Clone)]
pub struct SseStats {
    /// Time from request-send to first body byte, in nanoseconds.
    pub ttfb: Histogram<u64>,
    /// Inter-chunk latency in nanoseconds — time between successive
    /// `data:` events. First chunk of a stream is not recorded here
    /// (it's the TTFB).
    pub chunk_latency: Histogram<u64>,
    /// Total `data:` events received across all streams.
    pub chunks: u64,
    /// Total on-wire body bytes received (post-dechunk).
    pub bytes_received: u64,
    /// Streams that ended cleanly (either `[DONE]` or server closed
    /// without error).
    pub completed: u64,
    /// Streams that started (got a TTFB) — successful or not.
    pub streams: u64,
    /// Failures to establish the stream (connect / TLS / send-request).
    pub errors_connect: u64,
    /// Mid-stream read/decode errors.
    pub errors_read: u64,
}

impl Default for SseStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SseStats {
    /// Fresh stats with empty histograms and zero counters.
    pub fn new() -> Self {
        Self {
            ttfb: new_hist(),
            chunk_latency: new_hist(),
            chunks: 0,
            bytes_received: 0,
            completed: 0,
            streams: 0,
            errors_connect: 0,
            errors_read: 0,
        }
    }

    /// Record a TTFB measurement. Durations outside the histogram
    /// bounds are clamped; we don't drop samples.
    pub fn record_ttfb(&mut self, d: Duration) {
        let _ = self.ttfb.record(duration_to_hist_ns(d));
    }

    /// Record an inter-chunk gap.
    pub fn record_chunk_gap(&mut self, d: Duration) {
        let _ = self.chunk_latency.record(duration_to_hist_ns(d));
    }

    /// Merge another stats bucket into this one. Histograms add; all
    /// counters sum field-wise.
    pub fn merge(&mut self, other: &Self) {
        let _ = self.ttfb.add(&other.ttfb);
        let _ = self.chunk_latency.add(&other.chunk_latency);
        self.chunks += other.chunks;
        self.bytes_received += other.bytes_received;
        self.completed += other.completed;
        self.streams += other.streams;
        self.errors_connect += other.errors_connect;
        self.errors_read += other.errors_read;
    }
}

/// End-of-run SSE summary — merged from all worker [`SseStats`].
#[derive(Debug, Clone)]
pub struct SseSummary {
    /// Combined TTFB across all streams.
    pub ttfb: Histogram<u64>,
    /// Combined inter-chunk latency.
    pub chunk_latency: Histogram<u64>,
    /// Total `data:` events received.
    pub chunks: u64,
    /// Total body bytes received.
    pub bytes_received: u64,
    /// Streams that ended cleanly.
    pub completed: u64,
    /// Streams that started (completed + errored).
    pub streams: u64,
    /// Connect-phase failures.
    pub errors_connect: u64,
    /// Mid-stream read failures.
    pub errors_read: u64,
    /// Wall-clock duration of the benchmark (excluding warmup).
    pub duration: Duration,
}

impl SseSummary {
    /// Merge per-worker stats into a single summary.
    pub fn merge(stats: Vec<SseStats>, duration: Duration) -> Self {
        let mut out = SseSummary {
            ttfb: new_hist(),
            chunk_latency: new_hist(),
            chunks: 0,
            bytes_received: 0,
            completed: 0,
            streams: 0,
            errors_connect: 0,
            errors_read: 0,
            duration,
        };
        for s in stats {
            let _ = out.ttfb.add(&s.ttfb);
            let _ = out.chunk_latency.add(&s.chunk_latency);
            out.chunks += s.chunks;
            out.bytes_received += s.bytes_received;
            out.completed += s.completed;
            out.streams += s.streams;
            out.errors_connect += s.errors_connect;
            out.errors_read += s.errors_read;
        }
        out
    }

    /// Chunks per second across the whole run.
    pub fn chunks_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.chunks as f64 / secs
        }
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// A single SSE iteration: open a fresh connection, issue one GET,
/// consume the streaming body, record metrics, return.
///
/// The runner is stateless — [`Self::run_iteration`] takes the target +
/// opts and opens a dedicated connection per call. No pool, no slot
/// reuse: this avoids the "slot invalidated after first stream" problem
/// the pooled `exchange_streaming` path has, and matches the design
/// intent of "each worker task gets its own connection".
pub struct SseRunner;

impl SseRunner {
    /// Run one SSE request and drain its streaming body.
    ///
    /// Terminates on any of:
    /// - Server closes the connection cleanly (→ `stats.completed += 1`).
    /// - Server sends `data: [DONE]` (→ `stats.completed += 1`). We
    ///   still drain the tail for completeness; in practice OpenAI-style
    ///   servers close immediately after, so the drain exits quickly.
    /// - `deadline` passes (→ caller's deadline expired).
    /// - IO / protocol error (→ `stats.errors_read += 1`).
    ///
    /// Never panics. All error paths are recorded in `stats`.
    pub async fn run_iteration(
        target: &Target,
        opts: &TransportOpts,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
        stats: &mut SseStats,
        deadline: Instant,
    ) {
        // --- Open a fresh connection for this stream --------------------
        //
        // We don't go through `Http1Pool` because SSE connections are
        // single-use: once the stream closes, the connection is done.
        // A pooled model would leave dead slots behind after the first
        // iteration, producing spurious "slot unavailable" errors on
        // every subsequent iteration. One fresh TCP per iteration is
        // both simpler and produces the numbers the benchmark claims.
        let connected = match zerobench_http::open(target, opts).await {
            Ok(c) => c,
            Err(e) => {
                classify_stream_open_error(&e, stats);
                return;
            }
        };

        let stream = match connected {
            Connected::Plain(s) => s,
            Connected::Tls { .. } => {
                // TLS is not wired through this path yet — the rest of
                // the stack returns TransportError::Tls on HTTPS too, so
                // this branch shouldn't be reachable in practice for
                // v0.0.1. Classify as connect-phase failure for parity
                // with the HTTP transport's error surface.
                stats.errors_connect += 1;
                return;
            }
        };

        let (read_ctr, _written_ctr) = stream.counts();
        let r_before = read_ctr.load(std::sync::atomic::Ordering::Relaxed);

        let io = HyperStream::new(stream);
        let (mut sender, conn) = match http1::handshake::<_, Full<Bytes>>(io).await {
            Ok(pair) => pair,
            Err(e) => {
                stats.errors_connect += 1;
                let _ = e;
                return;
            }
        };

        // Spawn the connection driver on the compio runtime. When the
        // stream ends (server closes or we drop the response body), the
        // driver future resolves and the task exits — no leak.
        compio::runtime::spawn(async move {
            let _ = conn.await;
        })
        .detach();

        // --- Build + send the request ------------------------------------
        let req = match build_request(target, plan, ctx) {
            Ok(r) => r,
            Err(e) => {
                classify_stream_open_error(&e, stats);
                return;
            }
        };

        let t0 = Instant::now();
        let res = match compio::time::timeout(opts.request_timeout, sender.send_request(req))
            .await
        {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                stats.errors_connect += 1;
                let _ = e;
                return;
            }
            Err(_) => {
                stats.errors_connect += 1;
                return;
            }
        };
        let ttfb = t0.elapsed();

        let _status = res.status().as_u16();
        let _headers = res.headers().clone();
        let mut body: Incoming = res.into_body();

        stats.streams += 1;
        stats.record_ttfb(ttfb);

        // --- Drain the body frame-by-frame --------------------------------
        //
        // Each `body.frame().await` yields a dechunked frame from hyper
        // (or None when the stream ends). We wrap the await in a
        // deadline check so a slow server doesn't block us past the
        // bench duration.
        let mut last_chunk_at: Option<Instant> = None;
        let mut parser = SseLineParser::new();
        let mut done_seen = false;

        // Tracks whether we've already counted a clean completion for
        // this stream. `[DONE]` sentinels, explicit end-of-stream from
        // hyper, and `frame() -> None` are all valid "the server closed
        // cleanly" signals — and OpenAI-style servers reliably emit
        // two of them in a row (`[DONE]` then server close). We want
        // `completed += 1` per stream, not per close signal.
        let mut counted_completion = false;

        loop {
            if Instant::now() >= deadline {
                drain_parser_flush(
                    &mut parser,
                    &mut last_chunk_at,
                    stats,
                    &mut done_seen,
                    &mut counted_completion,
                );
                break;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drain_parser_flush(
                    &mut parser,
                    &mut last_chunk_at,
                    stats,
                    &mut done_seen,
                    &mut counted_completion,
                );
                break;
            }

            let frame_fut = body.frame();
            let frame = match compio::time::timeout(remaining, frame_fut).await {
                Ok(Some(Ok(f))) => f,
                Ok(Some(Err(_e))) => {
                    // Network / protocol error mid-stream. Break rather
                    // than early-return so the post-loop
                    // `bytes_received` accumulation still runs — bytes
                    // read before the error are real traffic and
                    // belong in the stats.
                    stats.errors_read += 1;
                    break;
                }
                Ok(None) => {
                    // Server closed the stream.
                    drain_parser_flush(
                        &mut parser,
                        &mut last_chunk_at,
                        stats,
                        &mut done_seen,
                        &mut counted_completion,
                    );
                    if !counted_completion {
                        stats.completed += 1;
                    }
                    break;
                }
                Err(_) => {
                    // Deadline.
                    drain_parser_flush(
                        &mut parser,
                        &mut last_chunk_at,
                        stats,
                        &mut done_seen,
                        &mut counted_completion,
                    );
                    break;
                }
            };

            // Only data frames carry bytes. Trailers (HTTP/2 only) are
            // ignored.
            let Some(data) = frame.data_ref() else {
                continue;
            };

            parser.feed(data.as_ref(), |ev| {
                handle_event(ev, &mut last_chunk_at, stats, &mut done_seen);
            });

            if done_seen {
                // `[DONE]` sentinel — record completion and keep reading
                // the tail. Dedup via `counted_completion` so a
                // subsequent "server closed" isn't counted twice.
                if !counted_completion {
                    stats.completed += 1;
                    counted_completion = true;
                }
                done_seen = false;
            }

            if body.is_end_stream() {
                // Short-circuit: if hyper tells us the body is done, we
                // can skip waiting for the next `None` poll.
                drain_parser_flush(
                    &mut parser,
                    &mut last_chunk_at,
                    stats,
                    &mut done_seen,
                    &mut counted_completion,
                );
                if !counted_completion {
                    stats.completed += 1;
                }
                break;
            }
        }

        let r_after = read_ctr.load(std::sync::atomic::Ordering::Relaxed);
        stats.bytes_received += r_after.saturating_sub(r_before);
    }
}

/// Dispatch one SSE event to the stats counters.
///
/// Records inter-chunk latency using the wall-clock instant of this
/// call (i.e. the instant we finished parsing the event — close enough
/// to "event arrived" for a bench tool, and cheaper than one
/// `Instant::now()` per byte).
fn handle_event(
    ev: SseEvent<'_>,
    last_chunk_at: &mut Option<Instant>,
    stats: &mut SseStats,
    done_seen: &mut bool,
) {
    match ev {
        SseEvent::Data(_payload) => {
            let now = Instant::now();
            if let Some(prev) = *last_chunk_at {
                stats.record_chunk_gap(now - prev);
            }
            *last_chunk_at = Some(now);
            stats.chunks += 1;
        }
        SseEvent::Done => {
            *done_seen = true;
        }
        SseEvent::Ignored => {
            // Non-data field or comment — not counted against chunk
            // throughput. We could expose a separate counter if users
            // start wanting visibility, but for v0.0.1 the benchmarks
            // we care about (LLM streaming, event streams) have data
            // as the dominant field.
        }
    }
}

/// Map a transport error during stream-open into the right SSE counter.
fn classify_stream_open_error(e: &TransportError, stats: &mut SseStats) {
    match e {
        TransportError::Connect(_)
        | TransportError::Tls(_)
        | TransportError::Timeout
        | TransportError::RequestBuild(_) => stats.errors_connect += 1,
        TransportError::Protocol(_) | TransportError::Io(_) => stats.errors_read += 1,
    }
}

/// Flush any pending event from the parser at end-of-stream. Records
/// the final chunk (if any) into stats as a regular `Data` event, and
/// counts a completion if the parser had a pending `[DONE]` — but only
/// if the caller hasn't already counted one via `counted_completion`.
fn drain_parser_flush(
    parser: &mut SseLineParser,
    last_chunk_at: &mut Option<Instant>,
    stats: &mut SseStats,
    done_seen: &mut bool,
    counted_completion: &mut bool,
) {
    parser.flush(|ev| handle_event(ev, last_chunk_at, stats, done_seen));
    if *done_seen && !*counted_completion {
        stats.completed += 1;
        *counted_completion = true;
    }
    *done_seen = false;
}

// ---------------------------------------------------------------------------
// Request builder
// ---------------------------------------------------------------------------
//
// Minimal counterpart to the HTTP transport's request builder. We inline
// it here rather than exposing it from `zerobench_http` because:
//   - The SSE use case is narrow: always GET, always a known set of
//     headers (Accept: text/event-stream, plus whatever the plan
//     carries). The HTTP builder handles more cases.
//   - The HTTP builder is pub(crate) — making it pub would be API
//     surface for something trivially reproducible.

/// Build an HTTP/1 `Request<Full<Bytes>>` for the SSE plan. Expands URL,
/// header, and body templates using `ctx`.
fn build_request(
    target: &Target,
    plan: &RequestPlan,
    ctx: &mut ScenarioContext,
) -> Result<Request<Full<Bytes>>, TransportError> {
    let mut url_buf: Vec<u8> = Vec::with_capacity(plan.url.estimated_size());
    plan.url.expand_into(&mut url_buf, &mut ctx.expand_ctx());
    let url_str = std::str::from_utf8(&url_buf)
        .map_err(|e| TransportError::RequestBuild(format!("url not utf-8: {e}")))?;

    // Origin-form path+query.
    let path_and_query = extract_path_and_query(url_str);

    let mut builder = Request::builder()
        .method(plan.method.clone())
        .uri(path_and_query.as_ref());
    builder = builder.header(http::header::HOST, target.addr());
    // SSE default headers — overridable by user-supplied plan headers.
    builder = builder.header(http::header::ACCEPT, "text/event-stream");
    builder = builder.header(http::header::CACHE_CONTROL, "no-cache");

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

    // Body is optional even for SSE (POST-with-body servers exist, e.g.
    // Anthropic's stream-with-prompt pattern).
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

/// Extract the origin-form `path+query` from a URL-ish string. Mirrors
/// the helper in `zerobench_http::h1` — kept local to avoid adding a
/// cross-crate `pub` just for this narrow helper.
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
// Saturation dispatcher
// ---------------------------------------------------------------------------

/// Run a closed-loop SSE benchmark.
///
/// Spawns `max_tasks` concurrent worker coroutines; each one calls
/// [`SseRunner::run_iteration`] in a loop until `stop` trips. Every
/// worker owns its own [`SseStats`]; on shutdown we collect and return
/// them all for the caller to merge via [`SseSummary::merge`].
///
/// Each iteration opens a fresh TCP+HTTP/1 connection against `target`.
/// N workers in parallel → N concurrent SSE streams, as the bench flag
/// `-c N` advertises.
pub async fn run_sse_saturate(
    target: Target,
    opts: TransportOpts,
    plan: Arc<RequestPlan>,
    max_tasks: usize,
    stop: StopSignal,
) -> Vec<SseStats> {
    if max_tasks == 0 {
        return Vec::new();
    }

    let target = Arc::new(target);
    let opts = Arc::new(opts);

    let mut handles = Vec::with_capacity(max_tasks);
    for _ in 0..max_tasks {
        let target = target.clone();
        let opts = opts.clone();
        let plan = plan.clone();
        let stop = stop.clone();
        let handle = compio::runtime::spawn(async move {
            worker_sse_saturate(target, opts, plan, stop).await
        });
        handles.push(handle);
    }

    let mut out = Vec::with_capacity(max_tasks);
    for h in handles {
        match h.await {
            Ok(stats) => out.push(stats),
            Err(_panic) => out.push(SseStats::new()),
        }
    }
    out
}

async fn worker_sse_saturate(
    target: Arc<Target>,
    opts: Arc<TransportOpts>,
    plan: Arc<RequestPlan>,
    stop: StopSignal,
) -> SseStats {
    let mut stats = SseStats::new();
    let num_vars: usize = 0;
    let mut ctx = ScenarioContext::new(num_vars, zerobench_core::rng::from_entropy());

    // Very far-future deadline: the `stop` signal is the real terminator.
    // A finite value (not `Instant::MAX`) keeps `compio::time::timeout`
    // deterministic — compio schedules a timer for the value we pass in,
    // and we don't want to rely on implementation-defined behaviour for
    // "infinitely far future".
    let long_deadline = Instant::now() + Duration::from_secs(60 * 60 * 24 * 365);

    while !stop.is_stopped() {
        let errors_before = stats.errors_connect + stats.errors_read;
        SseRunner::run_iteration(
            &target,
            &opts,
            &plan,
            &mut ctx,
            &mut stats,
            long_deadline,
        )
        .await;
        ctx.clear_all();

        let errors_after = stats.errors_connect + stats.errors_read;
        if errors_after > errors_before {
            // Synchronous error — yield so the stop-signal timer (and
            // any other worker) gets a chance to run before we retry.
            YieldNow::default().await;
        }
    }

    stats
}

/// Cooperatively yield once to the runtime — same pattern as the core
/// dispatcher (see `zerobench_core::dispatcher`). compio 0.18 has no
/// built-in `yield_now`, so we hand-roll an 8-line future.
///
/// Returns `Pending` on first poll (scheduling its own waker), `Ready`
/// on the second — letting the runtime drain other ready tasks between.
#[derive(Debug, Default)]
struct YieldNow {
    yielded: bool,
}

impl std::future::Future for YieldNow {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.yielded {
            std::task::Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Histogram helpers
// ---------------------------------------------------------------------------

fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO_NS, HIST_HI_NS, HIST_SIG)
        .expect("HDR bounds are valid compile-time constants")
}

fn duration_to_hist_ns(d: Duration) -> u64 {
    let ns = d.as_nanos();
    if ns < HIST_LO_NS as u128 {
        HIST_LO_NS
    } else if ns > HIST_HI_NS as u128 {
        HIST_HI_NS
    } else {
        ns as u64
    }
}

