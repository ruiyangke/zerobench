//! zerobench-sse — Server-Sent Events benchmarking runner (mio/epoll).
//!
//! Synchronous, zero-async SSE benchmark runner. Each worker thread
//! opens a fresh TCP+HTTP/1.1 connection per iteration, writes the
//! request, reads the streaming response (handling chunked
//! transfer-encoding), and parses SSE events via
//! [`SseLineParser`](line_parser::SseLineParser).
//!
//! # Architecture
//!
//! - [`run_sse_threaded`] — spawns N OS threads, each running
//!   [`run_sse_worker`] in a loop until the shared stop flag trips.
//! - [`SseStats`] — HDR histograms for TTFB and inter-chunk latency,
//!   plus counters (chunks, bytes, completed streams, errors).
//! - [`SseLineParser`] — streaming SSE event framer (sync, no IO).
//!
//! # One connection per iteration
//!
//! SSE connections are long-lived and single-use: once a stream closes,
//! the connection is done. The runner opens a fresh TCP + HTTP/1
//! connection for each iteration. N workers × 1 connection each, all
//! concurrent.

pub mod line_parser;

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use rand::Rng;
use rustls::ClientConfig;

use zerobench_core::plan::{Plan, Protocol, RequestPlan, SsePlan, Step};
use zerobench_core::stats::TaskStats;
use zerobench_core::transport::{Target, TransportOpts};

use zerobench_core::histogram::{duration_to_hist_ns, new_hist};
use zerobench_http::mio_tls::{MioStream, MioTlsStream};

pub use line_parser::{SseEvent, SseLineParser};

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Per-worker SSE statistics.
#[derive(Debug, Clone)]
pub struct SseStats {
    pub ttfb: Histogram<u64>,
    pub chunk_latency: Histogram<u64>,
    pub chunks: u64,
    pub bytes_received: u64,
    pub completed: u64,
    pub streams: u64,
    pub errors_connect: u64,
    pub errors_read: u64,
}

impl Default for SseStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SseStats {
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

    pub fn record_ttfb(&mut self, d: Duration) {
        let _ = self.ttfb.record(duration_to_hist_ns(d));
    }

    pub fn record_chunk_gap(&mut self, d: Duration) {
        let _ = self.chunk_latency.record(duration_to_hist_ns(d));
    }

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

/// End-of-run SSE summary — merged from all worker stats.
#[derive(Debug, Clone)]
pub struct SseSummary {
    pub ttfb: Histogram<u64>,
    pub chunk_latency: Histogram<u64>,
    pub chunks: u64,
    pub bytes_received: u64,
    pub completed: u64,
    pub streams: u64,
    pub errors_connect: u64,
    pub errors_read: u64,
    pub duration: Duration,
}

impl SseSummary {
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

    pub fn chunks_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 { 0.0 } else { self.chunks as f64 / secs }
    }
}

// ---------------------------------------------------------------------------
// Chunked transfer-encoding decoder
// ---------------------------------------------------------------------------

/// Incremental HTTP chunked transfer-encoding decoder.
///
/// Feed raw bytes from the socket; the decoder strips chunk framing and
/// appends only payload data to the output buffer.
struct ChunkDecoder {
    state: ChunkState,
    size_buf: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    /// Accumulating hex chunk-size digits until `\r\n`.
    Size,
    /// Reading `remaining` payload bytes.
    Data { remaining: usize },
    /// Expecting `\r\n` after chunk data.
    TrailerCr,
    TrailerLf,
    /// Terminal `0\r\n\r\n` reached.
    Done,
}

impl ChunkDecoder {
    fn new() -> Self {
        Self {
            state: ChunkState::Size,
            size_buf: Vec::with_capacity(16),
        }
    }

    /// Decode `input`, appending de-chunked payload to `out`.
    /// Returns `true` when the terminal 0-length chunk is reached.
    fn decode(&mut self, input: &[u8], out: &mut Vec<u8>) -> bool {
        let mut i = 0;
        while i < input.len() {
            match self.state {
                ChunkState::Size => {
                    let b = input[i];
                    i += 1;
                    if b == b'\n' {
                        // Parse hex size (ignore trailing \r in size_buf).
                        let s = if self.size_buf.last() == Some(&b'\r') {
                            &self.size_buf[..self.size_buf.len() - 1]
                        } else {
                            &self.size_buf[..]
                        };
                        let size = parse_hex(s);
                        self.size_buf.clear();
                        if size == 0 {
                            self.state = ChunkState::Done;
                            return true;
                        }
                        self.state = ChunkState::Data { remaining: size };
                    } else {
                        self.size_buf.push(b);
                    }
                }
                ChunkState::Data { remaining } => {
                    let avail = input.len() - i;
                    let take = avail.min(remaining);
                    out.extend_from_slice(&input[i..i + take]);
                    i += take;
                    let left = remaining - take;
                    if left == 0 {
                        self.state = ChunkState::TrailerCr;
                    } else {
                        self.state = ChunkState::Data { remaining: left };
                    }
                }
                ChunkState::TrailerCr => {
                    i += 1; // skip \r
                    self.state = ChunkState::TrailerLf;
                }
                ChunkState::TrailerLf => {
                    i += 1; // skip \n
                    self.state = ChunkState::Size;
                }
                ChunkState::Done => return true,
            }
        }
        false
    }
}

fn parse_hex(s: &[u8]) -> usize {
    // Strip optional chunk-extension (`;ext=val`)
    let hex = match memchr::memchr(b';', s) {
        Some(i) => &s[..i],
        None => s,
    };
    let hex_str = std::str::from_utf8(hex).unwrap_or("0");
    usize::from_str_radix(hex_str.trim(), 16).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// SSE request builder
// ---------------------------------------------------------------------------

/// Build raw HTTP/1.1 request bytes for an SSE endpoint from a
/// `RequestPlan` (the `--sse` CLI shortcut uses this after converting
/// its positional URL into a Request step with `expect_streaming`).
///
/// Adds `Accept: text/event-stream` and `Cache-Control: no-cache`
/// headers that the SSE spec expects.
fn build_sse_request_from_request_plan(
    plan: &Plan,
    target: &Target,
    request_plan: &RequestPlan,
) -> Vec<u8> {
    use zerobench_core::rng;
    use zerobench_core::scenario_context::ScenarioContext;

    let mut ctx = ScenarioContext::new(plan.vars.len(), rng::from_entropy());
    let mut buf = Vec::with_capacity(512);

    zerobench_http::raw_h1_common::build_raw_request(request_plan, &mut ctx, target, &mut buf)
        .expect("failed to build SSE request bytes");

    inject_sse_headers(&mut buf);
    buf
}

/// Build raw HTTP/1.1 request bytes for a Rhai-authored `SsePlan`.
///
/// The Rhai `SSE(url)` builder produces an `SsePlan { url, headers,
/// expect_chunks }` — we synthesize a `RequestPlan::get(url)` with the
/// SSE plan's extra headers, then feed the same `raw_h1_common::build_raw_request`
/// path so the HTTP/1.1 framing, host resolution, and template
/// expansion stay identical to the HTTP path.
fn build_sse_request_from_sse_plan(plan: &Plan, target: &Target, sse: &SsePlan) -> Vec<u8> {
    use zerobench_core::rng;
    use zerobench_core::scenario_context::ScenarioContext;

    let mut req = RequestPlan::get(sse.url.clone());
    for (name, value) in &sse.headers {
        req.headers.push((name.clone(), value.clone()));
    }

    let mut ctx = ScenarioContext::new(plan.vars.len(), rng::from_entropy());
    let mut buf = Vec::with_capacity(512);

    zerobench_http::raw_h1_common::build_raw_request(&req, &mut ctx, target, &mut buf)
        .expect("failed to build SSE request bytes");

    inject_sse_headers(&mut buf);
    buf
}

/// Insert `Accept: text/event-stream` and `Cache-Control: no-cache` into
/// `buf` before the final `\r\n\r\n` if they're not already present.
fn inject_sse_headers(buf: &mut Vec<u8>) {
    if !buf
        .windows(b"Accept:".len())
        .any(|w| w.eq_ignore_ascii_case(b"Accept:"))
    {
        if let Some(pos) = memchr::memmem::find(buf, b"\r\n\r\n") {
            let sse_headers = b"Accept: text/event-stream\r\nCache-Control: no-cache\r\n";
            buf.splice(pos..pos, sse_headers.iter().copied());
        }
    }
}

/// Legacy shim — wraps `build_sse_request_from_request_plan` so the
/// `--sse` CLI path can keep its callsite. The CLI feeds a `Plan` whose
/// first scenario's first step is a `Request`; we walk that same path.
fn build_sse_request(plan: &Plan, target: &Target) -> Vec<u8> {
    let step = plan
        .scenarios
        .first()
        .and_then(|s| s.steps.first())
        .expect("plan must have at least one scenario with one step");

    match step {
        Step::Request(r) => build_sse_request_from_request_plan(plan, target, r),
        Step::SseStream(s) => build_sse_request_from_sse_plan(plan, target, s),
        _ => panic!("SSE mode requires the first step to be a Request or SseStream"),
    }
}

// ---------------------------------------------------------------------------
// Worker — one stream iteration
// ---------------------------------------------------------------------------

/// Run one SSE stream: connect, send request, read streaming response,
/// parse events, record metrics. Returns when the stream ends, errors,
/// or stop fires.
fn run_one_stream(
    target: &Target,
    opts: &TransportOpts,
    request_bytes: &[u8],
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
    stats: &mut SseStats,
) {
    // `Target::resolve` now takes `&TransportOpts` so we can honour
    // `opts.resolve_overrides` (curl-style `--resolve`) and the per-target
    // `addr_family` preference on dual-stack hosts.
    let addr: SocketAddr = target.resolve(opts).expect("DNS resolution failed");
    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(_) => { stats.errors_connect += 1; return; }
    };
    let mut events = Events::with_capacity(64);
    let token = Token(0);

    // --- Connect TCP -------------------------------------------------------
    let mut tcp = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(_) => { stats.errors_connect += 1; return; }
    };
    tcp.set_nodelay(true).ok();

    poll.registry()
        .register(&mut tcp, token, Interest::READABLE | Interest::WRITABLE)
        .expect("mio register");

    // --- TLS handshake (optional) ------------------------------------------
    let mut stream = if let Some(config) = tls_config {
        let sni = target.sni_name().to_string();
        let mut tls = match MioTlsStream::new(tcp, Arc::clone(config), &sni) {
            Ok(s) => s,
            Err(_) => { stats.errors_connect += 1; return; }
        };
        if tls.complete_handshake(&mut poll, token).is_err() {
            stats.errors_connect += 1;
            return;
        }
        poll.registry()
            .reregister(tls.tcp_stream_mut(), token, Interest::READABLE | Interest::WRITABLE)
            .expect("mio reregister");
        MioStream::Tls(tls)
    } else {
        MioStream::Plain(tcp)
    };

    // --- Write request -----------------------------------------------------
    let mut written = 0usize;
    loop {
        if stop.load(Ordering::Relaxed) { return; }
        let _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        for _ev in events.iter() {
            while written < request_bytes.len() {
                match stream.write(&request_bytes[written..]) {
                    Ok(0) => { stats.errors_connect += 1; return; }
                    Ok(n) => written += n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => { stats.errors_connect += 1; return; }
                }
            }
        }
        if written >= request_bytes.len() { break; }
    }
    stream.flush_tls();

    let t0 = Instant::now();

    // --- Read response headers ---------------------------------------------
    let mut hdr_buf = vec![0u8; 8192];
    let mut hdr_len = 0usize;
    let header_end;

    loop {
        if stop.load(Ordering::Relaxed) { return; }
        let _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        stream.flush_tls();
        for _ev in events.iter() {
            match stream.read(&mut hdr_buf[hdr_len..]) {
                Ok(0) => { stats.errors_connect += 1; return; }
                Ok(n) => hdr_len += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => { stats.errors_connect += 1; return; }
            }
        }
        if let Some(end) = memchr::memmem::find(&hdr_buf[..hdr_len], b"\r\n\r\n") {
            header_end = end + 4;
            break;
        }
        if hdr_len >= hdr_buf.len() {
            // Headers too large.
            stats.errors_connect += 1;
            return;
        }
    }

    // Record TTFB.
    let ttfb = t0.elapsed();
    stats.streams += 1;
    stats.record_ttfb(ttfb);

    // Parse headers to check for chunked encoding.
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut headers);
    let _ = resp.parse(&hdr_buf[..header_end]);
    let chunked = resp.headers.iter().any(|h| {
        h.name.eq_ignore_ascii_case("transfer-encoding")
            && h.value.eq_ignore_ascii_case(b"chunked")
    });

    // --- Read streaming body -----------------------------------------------
    let mut parser = SseLineParser::new();
    let mut chunk_decoder = ChunkDecoder::new();
    let mut last_chunk_at: Option<Instant> = None;
    let mut done_seen = false;
    let mut counted_completion = false;
    let mut body_bytes = 0u64;

    // Process any body data that arrived with the headers.
    let leftover = &hdr_buf[header_end..hdr_len];
    if !leftover.is_empty() {
        let mut decoded = Vec::with_capacity(leftover.len());
        if chunked {
            let terminal = chunk_decoder.decode(leftover, &mut decoded);
            body_bytes += decoded.len() as u64;
            feed_parser(&mut parser, &decoded, &mut last_chunk_at, stats, &mut done_seen);
            if terminal {
                if !counted_completion { stats.completed += 1; }
                return;
            }
        } else {
            body_bytes += leftover.len() as u64;
            feed_parser(&mut parser, leftover, &mut last_chunk_at, stats, &mut done_seen);
        }
        check_done(&mut done_seen, &mut counted_completion, stats);
    }

    let mut read_buf = [0u8; 8192];
    let mut decoded_buf = Vec::with_capacity(4096);

    loop {
        if stop.load(Ordering::Relaxed) { break; }
        let _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        stream.flush_tls();

        let mut got_data = false;
        for _ev in events.iter() {
            loop {
                match stream.read(&mut read_buf) {
                    Ok(0) => {
                        // Server closed connection.
                        parser.flush(|ev| handle_event(ev, &mut last_chunk_at, stats, &mut done_seen));
                        if !counted_completion {
                            stats.completed += 1;
                        }
                        stats.bytes_received += body_bytes;
                        return;
                    }
                    Ok(n) => {
                        got_data = true;
                        if chunked {
                            decoded_buf.clear();
                            let terminal = chunk_decoder.decode(&read_buf[..n], &mut decoded_buf);
                            body_bytes += decoded_buf.len() as u64;
                            feed_parser(&mut parser, &decoded_buf, &mut last_chunk_at, stats, &mut done_seen);
                            check_done(&mut done_seen, &mut counted_completion, stats);
                            if terminal {
                                stats.bytes_received += body_bytes;
                                return;
                            }
                        } else {
                            body_bytes += n as u64;
                            feed_parser(&mut parser, &read_buf[..n], &mut last_chunk_at, stats, &mut done_seen);
                            check_done(&mut done_seen, &mut counted_completion, stats);
                        }
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        stats.errors_read += 1;
                        stats.bytes_received += body_bytes;
                        return;
                    }
                }
            }
        }

        if !got_data {
            // Timeout — flush TLS and check stop.
            stream.flush_tls();
        }
    }

    stats.bytes_received += body_bytes;
}

fn feed_parser(
    parser: &mut SseLineParser,
    data: &[u8],
    last_chunk_at: &mut Option<Instant>,
    stats: &mut SseStats,
    done_seen: &mut bool,
) {
    parser.feed(data, |ev| handle_event(ev, last_chunk_at, stats, done_seen));
}

fn handle_event(
    ev: SseEvent<'_>,
    last_chunk_at: &mut Option<Instant>,
    stats: &mut SseStats,
    done_seen: &mut bool,
) {
    match ev {
        SseEvent::Data(_) => {
            let now = Instant::now();
            if let Some(prev) = *last_chunk_at {
                stats.record_chunk_gap(now - prev);
            }
            *last_chunk_at = Some(now);
            stats.chunks += 1;
        }
        SseEvent::Done => { *done_seen = true; }
        SseEvent::Ignored => {}
    }
}

fn check_done(done_seen: &mut bool, counted: &mut bool, stats: &mut SseStats) {
    if *done_seen && !*counted {
        stats.completed += 1;
        *counted = true;
        *done_seen = false;
    }
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

/// Single worker — reconnects per iteration until stop fires.
fn run_sse_worker(
    target: &Target,
    opts: &TransportOpts,
    request_bytes: &[u8],
    stop: &AtomicBool,
    tls_config: Option<Arc<ClientConfig>>,
) -> SseStats {
    let mut stats = SseStats::new();
    while !stop.load(Ordering::Relaxed) {
        run_one_stream(target, opts, request_bytes, stop, tls_config.as_ref(), &mut stats);
    }
    stats
}

/// Multi-scenario worker — picks a random SSE scenario per iteration
/// from `pre_built` (list of `(scenario_id, request_bytes)` pairs),
/// runs one stream against the chosen scenario's request, and
/// attributes per-stream metrics to the right `ScenarioStats` slot.
///
/// Returns a `TaskStats` with `sse` extras populated per scenario_id.
fn run_sse_worker_multi(
    target: &Target,
    opts: &TransportOpts,
    pre_built: &[(u16, Vec<u8>)],
    num_scenarios: usize,
    stop: &AtomicBool,
    tls_config: Option<Arc<ClientConfig>>,
) -> TaskStats {
    let mut task = TaskStats::new(num_scenarios);
    if pre_built.is_empty() {
        return task;
    }

    // Dedicated RNG for scenario selection — keeps the SSE worker's
    // picks independent of any core `ScenarioContext` (we don't need
    // templates here, the bytes are pre-built).
    let mut rng = zerobench_core::rng::from_entropy();

    while !stop.load(Ordering::Relaxed) {
        // Uniform pick across configured SSE scenarios.
        let idx = if pre_built.len() == 1 {
            0
        } else {
            rng.gen_range(0..pre_built.len())
        };
        let (scenario_id, request_bytes) = &pre_built[idx];
        let mut per_stream = SseStats::new();
        let t_iter_start = Instant::now();
        run_one_stream(
            target,
            opts,
            request_bytes,
            stop,
            tls_config.as_ref(),
            &mut per_stream,
        );
        let iter_latency = t_iter_start.elapsed();

        // Fold per-stream counters into the right scenario's slot +
        // task-level aggregate.
        let sid = *scenario_id;
        if let Some(sc) = task.per_scenario.get_mut(sid as usize) {
            // Count every started stream as a "request" (operation).
            // `requests` doubles as "streams attempted" in the SSE
            // report — aligns with HTTP where 1 request == 1
            // operation regardless of success.
            sc.requests += per_stream.streams;
            // Task-level mirror for the top-line operation count.
            task.requests += per_stream.streams;
            // Stream-duration latency; caps at the configured HDR bound.
            if per_stream.streams > 0 {
                let _ = sc.latency.record(duration_to_hist_ns(iter_latency));
                let _ = task.latency.record(duration_to_hist_ns(iter_latency));
            }
            let extras = sc.sse_mut();
            extras.merge(&SseExtras {
                ttfb: per_stream.ttfb.clone(),
                chunk_gap: per_stream.chunk_latency.clone(),
                chunks: per_stream.chunks,
                streams_completed: per_stream.completed,
                bytes_received: per_stream.bytes_received,
            });
            // Connect / read errors attributed to the scenario.
            for _ in 0..per_stream.errors_connect {
                sc.errors.incr(zerobench_core::ErrorKind::Connect);
                task.errors.incr(zerobench_core::ErrorKind::Connect);
            }
            for _ in 0..per_stream.errors_read {
                sc.errors.incr(zerobench_core::ErrorKind::Read);
                task.errors.incr(zerobench_core::ErrorKind::Read);
            }
            // Bytes received also roll up to the task level — the
            // terminal report's top-line "transfer" line counts all
            // SSE+HTTP+WS bytes regardless of protocol.
            task.bytes_recv += per_stream.bytes_received;
        }
    }
    task
}

use zerobench_core::stats::SseExtras;

// ---------------------------------------------------------------------------
// Multi-threaded driver
// ---------------------------------------------------------------------------

/// Spawn `num_workers` OS threads, each running one SSE stream at a
/// time in a reconnect loop. Blocks until `duration` elapses.
///
/// `opts` carries the curl-style `--resolve` overrides and address-family
/// preference used by `Target::resolve`. Callers that don't care about
/// DNS hijacking can pass `&TransportOpts::default()`.
pub fn run_sse_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    num_workers: usize,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
) -> Vec<SseStats> {
    let request_bytes = build_sse_request(plan, target);
    let stop = Arc::new(AtomicBool::new(false));

    // Timer thread.
    let stop_timer = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(duration);
        stop_timer.store(true, Ordering::Relaxed);
    });

    let handles: Vec<_> = (0..num_workers)
        .map(|_| {
            let target = target.clone();
            let opts = opts.clone();
            let request_bytes = request_bytes.clone();
            let stop = stop.clone();
            let tls_config = tls_config.clone();

            std::thread::spawn(move || {
                run_sse_worker(&target, &opts, &request_bytes, &stop, tls_config)
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().expect("SSE worker panicked"))
        .collect()
}

/// Drive SSE scenarios from a multi-protocol `Plan` — the surface the
/// Tier-1 unified CLI dispatcher calls when a plan has one or more
/// `Step::SseStream` scenarios.
///
/// Returns per-worker [`TaskStats`] with `sse` extras populated per
/// scenario_id. Scenarios whose protocol isn't SSE are silently
/// skipped. An empty SSE-scenario list returns empty TaskStats for
/// each worker so the caller can still merge without special-casing.
///
/// Worker distribution: each worker runs one stream at a time and
/// picks a random SSE scenario per iteration. `num_workers` is the
/// concurrency dial — mirrors the `-c N` CLI flag semantics.
pub fn run_sse_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    num_workers: usize,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    stop_flag: Option<Arc<AtomicBool>>,
) -> Vec<TaskStats> {
    let num_scenarios = plan.scenarios.len();

    // Pre-build request bytes for every SSE scenario in the plan.
    // Scenarios with no SSE step contribute nothing to `pre_built`;
    // the worker uniformly picks across the remaining entries.
    let pre_built: Vec<(u16, Vec<u8>)> = plan
        .scenarios
        .iter()
        .enumerate()
        .filter_map(|(i, sc)| {
            if sc.protocol() != Protocol::Sse {
                return None;
            }
            sc.steps.iter().find_map(|step| match step {
                Step::SseStream(s) => {
                    let bytes = build_sse_request_from_sse_plan(plan, target, s);
                    Some((i as u16, bytes))
                }
                _ => None,
            })
        })
        .collect();

    if pre_built.is_empty() {
        // Nothing to do — return empty per-worker stats so the CLI
        // dispatcher can merge without branching.
        return (0..num_workers)
            .map(|_| TaskStats::new(num_scenarios))
            .collect();
    }

    let stop = stop_flag.unwrap_or_else(|| {
        let flag = Arc::new(AtomicBool::new(false));
        let timer = flag.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer.store(true, Ordering::Relaxed);
        });
        flag
    });

    let handles: Vec<_> = (0..num_workers)
        .map(|_| {
            let target = target.clone();
            let opts = opts.clone();
            let pre_built = pre_built.clone();
            let stop = stop.clone();
            let tls_config = tls_config.clone();
            std::thread::spawn(move || {
                run_sse_worker_multi(
                    &target,
                    &opts,
                    &pre_built,
                    num_scenarios,
                    &stop,
                    tls_config,
                )
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().expect("SSE worker panicked"))
        .collect()
}

