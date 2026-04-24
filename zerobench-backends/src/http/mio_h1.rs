//! Mio-based raw HTTP/1.1 benchmark client — synchronous epoll event loop,
//! zero async overhead.
//!
//! Each worker thread runs its own [`mio::Poll`] instance with N/T connections.
//! No futures, no wakers, no task scheduling — just a tight event loop:
//!
//! ```text
//! Thread i:
//!   mio::Poll + 38 connections (300 total / 8 threads)
//!
//!   loop {
//!       poll.poll(&mut events)?;
//!       for event in &events {
//!           match conn.state {
//!               Writing  → conn.stream.write(&req_bytes) → state = ReadingHeaders
//!               Reading  → conn.stream.read(&mut buf) → parse headers →
//!                          if complete: record_stats() → state = Writing (next req)
//!           }
//!       }
//!   }
//! ```
//!
//! # Modes
//!
//! - **Saturate (closed-loop)**: `target_rps = None`. Each connection fires
//!   the next request immediately after a response. Classic closed-loop.
//! - **Open-loop (constant-rate)**: `target_rps = Some(rps)`. Requests are
//!   scheduled at a fixed interval via a token-bucket. Latency is measured
//!   from `intended_start` (when the token was generated), not from the
//!   actual write — this is coordinated-omission-free measurement.
//!
//! # TLS support
//!
//! When the target is `https://`, connections are wrapped in a
//! [`MioTlsStream`] backed by `rustls`.
//! The TLS handshake is driven to completion via mio poll events before
//! the request loop starts. The `--insecure` flag disables certificate
//! verification (self-signed, expired, hostname mismatch).
//!
//! # Per-request template expansion
//!
//! Each request expands `{{uuid}}`, `{{counter}}`, `{{rand_*}}` etc.
//! freshly via `ScenarioContext` and `build_raw_request`. Templates are
//! expanded into each connection's `write_buf` before every send.
//!
//! # Multi-scenario
//!
//! Each iteration picks a random scenario from `plan.scenarios` (uniform
//! random), executes its first Request step. `scenario_id` is passed to
//! `stats.record()` and `stats.record_error()`.
//!
//! # Response assertions and extraction
//!
//! After parsing response headers, checks `RequestPlan.checks` (StatusEq,
//! StatusIn, LatencyUnder). Failed assertions increment
//! `ErrorKind::AssertionFailed`. Extractions (`Extract::Header`,
//! `Extract::StatusCode`) populate `ScenarioContext.vars` for subsequent
//! template expansions.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use rand::Rng;
use rustls::ClientConfig;

use zerobench_core::plan::{Plan, Protocol, RequestPlan, Step};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stats::{ErrorKind, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_runtime::{LiveSnapshot, Recorder, Sample};

use super::mio_tls::{MioStream, MioTlsStream};
use super::raw_h1_common::{
    build_raw_request, find_connection_close, find_content_length_raw,
    find_transfer_encoding_chunked, ChunkProgress, ChunkedDecoder, ConnectionMode,
    ContentLength,
};

// ---------------------------------------------------------------------------
// Connect helpers — split resolve / connect so we can re-resolve on failure
// ---------------------------------------------------------------------------

/// Attempt a single non-blocking TCP connect to `addr`. Returns whatever
/// `TcpStream::connect` returned, unwrapped at the caller.
#[inline]
fn connect_once(addr: SocketAddr) -> io::Result<TcpStream> {
    TcpStream::connect(addr)
}

/// Connect to `target`, re-resolving once on transient failure.
///
/// Rationale: rolling deployments flip DNS mid-run. The first resolution
/// from the worker may be stale by the time we attempt the connect, and a
/// single ECONNREFUSED / EHOSTUNREACH is much more likely to clear after
/// another DNS lookup than after a blind retry to the same dead address.
///
/// We deliberately only re-resolve **once** — retrying further would hide
/// genuine server-down failures and delay error reporting.
fn connect_with_retry(
    target: &Target,
    opts: &TransportOpts,
) -> io::Result<(TcpStream, SocketAddr)> {
    let addr = target.resolve(opts)?;
    match connect_once(addr) {
        Ok(s) => Ok((s, addr)),
        Err(e) if matches!(
            e.kind(),
            io::ErrorKind::ConnectionRefused | io::ErrorKind::HostUnreachable
                | io::ErrorKind::NetworkUnreachable | io::ErrorKind::TimedOut
        ) => {
            // One re-resolve + retry. If the second attempt fails, bubble
            // the original error kind up — callers classify it as Connect.
            let fresh = target.resolve(opts)?;
            connect_once(fresh).map(|s| (s, fresh))
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Per-thread connection distribution
// ---------------------------------------------------------------------------

/// Distribute `total` connections across `threads` workers so that the
/// *exact* total is preserved — the first `total % threads` workers get
/// one extra connection, the rest take the floor.
///
/// This is the fix for the `-c N -t T` bug where `div_ceil` could
/// over-allocate by up to `threads - 1` connections when `total` didn't
/// divide evenly.
///
/// `threads` is clamped to `max(1, min(threads, total))` before distribution
/// — spinning up threads that own zero connections is pure overhead.
/// When `total == 0`, returns an empty vector.
fn distribute_conns(total: usize, threads: usize) -> Vec<usize> {
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

/// Find the end of HTTP headers (`\r\n\r\n`). Returns the byte offset
/// just past the terminator. Uses `memchr::memmem` (SIMD-accelerated
/// via AVX2/SSE4.2 — ~10x faster than `windows(4).position()`).
#[inline]
fn find_header_end(buf: &[u8]) -> Option<usize> {
    memchr::memmem::find(buf, b"\r\n\r\n").map(|p| p + 4)
}

// ---------------------------------------------------------------------------
// Scenario selection helper — resolves the first Request step
// ---------------------------------------------------------------------------

/// Resolved scenario selection: which scenario to execute, and a
/// reference to its first `RequestPlan`.
struct SelectedScenario<'a> {
    scenario_id: u16,
    request_plan: &'a RequestPlan,
}

/// Pick a scenario from the given list of HTTP scenario indices.
///
/// `http_indices` contains only the indices into `plan.scenarios` of
/// scenarios whose protocol is [`Protocol::Http`] — SSE/WS scenarios
/// are handled by other backends. When only one HTTP scenario exists,
/// returns it without an RNG call.
fn pick_scenario<'a>(
    plan: &'a Plan,
    http_indices: &[usize],
    ctx: &mut ScenarioContext,
) -> SelectedScenario<'a> {
    let pick = if http_indices.len() <= 1 {
        http_indices[0]
    } else {
        http_indices[ctx.rng.gen_range(0..http_indices.len())]
    };
    let scenario = &plan.scenarios[pick];
    let request_plan = scenario
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Request(r) => Some(r),
            _ => None,
        })
        .expect("HTTP scenario must have at least one Request step");
    SelectedScenario {
        scenario_id: pick as u16,
        request_plan,
    }
}

/// Collect indices of HTTP scenarios from `plan`.
///
/// Scenarios whose protocol is SSE or WS are filtered out — those are
/// served by other backends. Returns indices (not references) so the
/// worker can still treat `scenario_id` as an index into
/// `plan.scenarios` without renumbering.
fn http_scenario_indices(plan: &Plan) -> Vec<usize> {
    plan.scenarios
        .iter()
        .enumerate()
        .filter_map(|(i, s)| (s.protocol() == Protocol::Http).then_some(i))
        .collect()
}

// ---------------------------------------------------------------------------
// Connection state machine
// ---------------------------------------------------------------------------

/// State machine for a single HTTP/1.1 keep-alive connection.
struct Conn {
    stream: MioStream,
    state: ConnState,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    /// Persistent read scratch buffer — initialized once, reused across
    /// reads. Avoids zeroing 8 KiB on the stack on every `try_read` call
    /// (was 6.7% of CPU in the profiler).
    tmp_read: Box<[u8; 8192]>,
    /// How many bytes of `write_buf` have been flushed to the socket.
    write_offset: usize,
    /// Wall-clock instant when the current request was started (write began).
    t0: Instant,
    /// For open-loop mode: the time this request was SCHEDULED to start
    /// (may be earlier than actual write time if the connection was busy).
    /// For saturate mode: same as `t0`.
    intended_start: Instant,
    /// Time-to-first-byte captured when we finish parsing response headers.
    /// Stored here so it survives the ReadingHeaders -> ReadingBody transition.
    ttfb: Duration,
    /// HTTP status code from the response headers.
    status: u16,
    /// Which scenario this connection is currently executing.
    scenario_id: u16,
    /// Captured response header values needed by `Extract::Header`.
    /// Populated during `check_headers`; consumed after response completes.
    /// Key = lowercased header name bytes, Value = header value bytes.
    extracted_headers: Vec<(Vec<u8>, Vec<u8>)>,
    /// Skip header capture + extraction in the static fast path.
    skip_header_capture: bool,
}

#[derive(Debug)]
enum ConnState {
    /// Ready to write a new request.
    Idle,
    /// Writing request bytes (may need multiple writes for large bodies).
    Writing,
    /// Waiting for / reading response headers.
    ReadingHeaders,
    /// Reading remaining body bytes (Content-Length known).
    ReadingBody {
        header_len: usize,
        content_length: usize,
    },
    /// Reading a `Transfer-Encoding: chunked` body. The decoder
    /// tracks chunk framing across multiple reads; `header_len` is
    /// the offset in `read_buf` where the body begins.
    ReadingChunkedBody {
        header_len: usize,
        decoder: ChunkedDecoder,
    },
    /// Connection is dead — skip this slot for the rest of the run.
    Dead,
}

impl Conn {
    fn new(stream: MioStream) -> Self {
        let now = Instant::now();
        Self {
            stream,
            state: ConnState::Idle,
            read_buf: Vec::with_capacity(8192),
            write_buf: Vec::with_capacity(512),
            tmp_read: Box::new([0u8; 8192]),
            write_offset: 0,
            t0: now,
            intended_start: now,
            ttfb: Duration::ZERO,
            status: 0,
            scenario_id: 0,
            extracted_headers: Vec::new(),
            skip_header_capture: false,
        }
    }

    /// Load `request_bytes` into the write buffer and transition to Writing.
    ///
    /// `intended_start` is the scheduled time for this request. In saturate
    /// mode, pass `Instant::now()`. In open-loop mode, pass the token
    /// timestamp so latency is measured CO-free.
    ///
    /// Static fast path — copies pre-built bytes without template expansion.
    /// Used when the plan is fully static (no `{{...}}` parts).
    fn prepare_request(&mut self, request_bytes: &[u8], intended_start: Instant) {
        self.write_buf.clear();
        self.write_buf.extend_from_slice(request_bytes);
        self.write_offset = 0;
        self.read_buf.clear();
        self.intended_start = intended_start;
        self.t0 = Instant::now();
        self.ttfb = Duration::ZERO;
        self.status = 0;
        self.scenario_id = 0;
        self.extracted_headers.clear();
        self.state = ConnState::Writing;
    }

    /// Build request from a template, expanding dynamic parts via
    /// `ScenarioContext`, and transition to Writing.
    ///
    /// `intended_start` is the scheduled time for this request.
    fn prepare_request_from_template(
        &mut self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
        target: &Target,
        intended_start: Instant,
        scenario_id: u16,
    ) {
        self.write_buf.clear();
        build_raw_request(plan, ctx, target, ConnectionMode::KeepAlive, &mut self.write_buf)
            .expect("failed to build request bytes");
        self.write_offset = 0;
        self.read_buf.clear();
        self.intended_start = intended_start;
        self.t0 = Instant::now();
        self.ttfb = Duration::ZERO;
        self.status = 0;
        self.scenario_id = scenario_id;
        self.extracted_headers.clear();
        self.state = ConnState::Writing;
    }

    /// Attempt to flush pending request bytes. Returns `true` when all bytes
    /// have been written and the connection should transition to reading.
    fn try_write(&mut self) -> io::Result<bool> {
        while self.write_offset < self.write_buf.len() {
            match self.stream.write(&self.write_buf[self.write_offset..]) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero"));
                }
                Ok(n) => self.write_offset += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }

    /// Attempt to read response data. Returns `Some((status, ttfb, total))`
    /// when the full response (headers + body) has been received.
    ///
    /// `now` is a cached `Instant::now()` from the top of the poll batch —
    /// avoids a separate clock_gettime per event (was 14.8% of CPU).
    fn try_read(&mut self, now: Instant) -> io::Result<Option<(u16, Duration, Duration)>> {
        loop {
            match self.stream.read(&mut *self.tmp_read) {
                Ok(0) => {
                    self.state = ConnState::Dead;
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed by server",
                    ));
                }
                Ok(n) => {
                    self.read_buf.extend_from_slice(&self.tmp_read[..n]);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        match self.state {
            ConnState::ReadingHeaders => self.check_headers(now),
            ConnState::ReadingBody {
                header_len,
                content_length,
            } => {
                let body_received = self.read_buf.len() - header_len;
                if body_received >= content_length {
                    let total = now.duration_since(self.t0);
                    self.state = ConnState::Idle;
                    return Ok(Some((self.status, self.ttfb, total)));
                }
                Ok(None)
            }
            ConnState::ReadingChunkedBody { header_len, ref mut decoder } => {
                let body = &self.read_buf[header_len..];
                match decoder.advance(body) {
                    ChunkProgress::NeedMore => Ok(None),
                    ChunkProgress::Done { .. } => {
                        let total = now.duration_since(self.t0);
                        self.state = ConnState::Idle;
                        Ok(Some((self.status, self.ttfb, total)))
                    }
                    ChunkProgress::Err(msg) => {
                        self.state = ConnState::Dead;
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("chunked response: {msg}"),
                        ))
                    }
                }
            }
            _ => Ok(None),
        }
    }

    /// Parse response headers using httparse (full validation + AVX2 SIMD).
    /// The SIMD `find_header_end` via memchr finds the `\r\n\r\n` terminator;
    /// httparse then parses the status line + headers with full validation.
    ///
    /// Also captures header values needed by `Extract::Header` into
    /// `self.extracted_headers` so they survive the `read_buf` being
    /// cleared on the next request.
    fn check_headers(&mut self, now: Instant) -> io::Result<Option<(u16, Duration, Duration)>> {
        let buf = &self.read_buf;

        let header_end = match find_header_end(buf) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut resp = httparse::Response::new(&mut headers);
        match resp.parse(&buf[..header_end]) {
            Ok(httparse::Status::Complete(hdr_len)) => {
                let status = resp.code.unwrap_or(0);
                let is_chunked = find_transfer_encoding_chunked(resp.headers);
                // Content-Length is ignored when Transfer-Encoding: chunked
                // is present per RFC 9112 §6.1 (TE overrides CL).
                let cl = find_content_length_raw(resp.headers);
                let ttfb = now.duration_since(self.t0);
                let keep_alive = !find_connection_close(resp.headers);

                // Capture response headers for extraction (skip in static fast path).
                if !self.skip_header_capture {
                    self.extracted_headers.clear();
                    for h in resp.headers.iter() {
                        if h.name.is_empty() {
                            break;
                        }
                        let name_lower: Vec<u8> =
                            h.name.as_bytes().iter().map(|b| b.to_ascii_lowercase()).collect();
                        self.extracted_headers
                            .push((name_lower, h.value.to_vec()));
                    }
                }

                if is_chunked {
                    // Chunked body — run the decoder immediately in case
                    // the whole body already arrived in this read.
                    let mut decoder = ChunkedDecoder::new();
                    match decoder.advance(&buf[hdr_len..]) {
                        ChunkProgress::Done { .. } => {
                            let total = now.duration_since(self.t0);
                            if keep_alive {
                                self.state = ConnState::Idle;
                            } else {
                                self.state = ConnState::Dead;
                            }
                            return Ok(Some((status, ttfb, total)));
                        }
                        ChunkProgress::NeedMore => {
                            self.ttfb = ttfb;
                            self.status = status;
                            self.state = ConnState::ReadingChunkedBody {
                                header_len: hdr_len,
                                decoder,
                            };
                            return Ok(None);
                        }
                        ChunkProgress::Err(msg) => {
                            self.state = ConnState::Dead;
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("chunked response: {msg}"),
                            ));
                        }
                    }
                }

                let content_length = match cl {
                    ContentLength::Present(n) => n,
                    ContentLength::Missing => {
                        // No CL, no chunked. RFC 9110 §8.6 "close-delimited"
                        // fallback: body ends at EOF, so we must not reuse
                        // this connection even if keep-alive was claimed.
                        self.state = ConnState::Dead;
                        0
                    }
                    ContentLength::Malformed => {
                        // Present-but-malformed CL: unrecoverable.
                        // We cannot tell where the body ends, so the
                        // next bytes on this socket may be body
                        // continuation or a new response. The safe
                        // (and spec-mandated) response is to kill
                        // the connection and surface a read error.
                        self.state = ConnState::Dead;
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "malformed Content-Length in response",
                        ));
                    }
                };

                let body_received = buf.len() - hdr_len;
                if body_received >= content_length {
                    let total = now.duration_since(self.t0);
                    // Dead state from Missing-CL above takes
                    // precedence over keep-alive because we cannot
                    // trust the framing beyond this response.
                    if keep_alive && !matches!(self.state, ConnState::Dead) {
                        self.state = ConnState::Idle;
                    } else if !keep_alive {
                        self.state = ConnState::Dead;
                    }
                    Ok(Some((status, ttfb, total)))
                } else {
                    self.ttfb = ttfb;
                    self.status = status;
                    self.state = ConnState::ReadingBody {
                        header_len: hdr_len,
                        content_length,
                    };
                    Ok(None)
                }
            }
            Ok(httparse::Status::Partial) => Ok(None),
            Err(_) => {
                self.state = ConnState::Dead;
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP response parse error",
                ))
            }
        }
    }
}

// Response post-processing — `check_assertions`, `apply_extractions`,
// `capture_headers` live in `raw_h1_common` so cold_connect + mio_h2
// reuse the same logic. Re-imported under the original names below
// so the mio_h1 call sites don't churn.
use crate::http::raw_h1_common::{apply_extractions, check_assertions};

// ---------------------------------------------------------------------------
// Single-thread worker
// ---------------------------------------------------------------------------

/// Run the mio event loop on the calling thread. Blocks until `stop` is
/// set. Returns per-thread stats.
///
/// Each connection is registered with `READABLE | WRITABLE` once. The state
/// machine filters spurious wakeups (e.g. writable while reading) — this
/// avoids 2 `reregister` syscalls per request at the cost of occasional
/// no-op event handling, which is cheaper at high req/s.
///
/// # Open-loop mode
///
/// When `target_rps` is `Some(rps)`, requests are dispatched at a fixed
/// rate using a token-bucket scheduler. Latency is measured from the
/// token's `intended_start` (coordinated-omission-free). Connections that
/// complete a response are returned to an idle pool rather than immediately
/// starting the next request; they wait for the next token.
///
/// When all connections are busy and tokens pile up, the surplus is counted
/// as `keepup` errors — requests that *would* have been sent but couldn't.
pub fn run_mio_worker(
    plan: &Plan,
    target: &Target,
    opts: &TransportOpts,
    num_conns: usize,
    stop: &AtomicBool,
    target_rps: Option<f64>,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<&LiveSnapshot>,
) -> TaskStats {
    let mut poll = Poll::new().expect("mio::Poll::new");
    let mut events = Events::with_capacity(1024);
    let num_scenarios = plan.scenarios.len();
    let mut stats = TaskStats::new(num_scenarios);

    // Filter out SSE/WS scenarios — those are served by other backends.
    // A multi-protocol plan may have zero HTTP scenarios (pure-SSE /
    // pure-WS scripts); in that case this worker exits immediately
    // without opening any sockets.
    let http_indices = http_scenario_indices(plan);
    if http_indices.is_empty() {
        return stats;
    }

    // Per-worker ScenarioContext for template expansion + extraction.
    let mut ctx = ScenarioContext::new(plan.vars.len(), zerobench_core::rng::from_entropy());

    // Unified sink — one call per op fans out to TaskStats + LiveSnapshot
    // + per-scenario live slot. Kills the former triple-record antipattern.
    let mut recorder = Recorder::new(&mut stats, live);

    // --- Static fast-path detection ---
    // When every scenario is a single static request with no extractions
    // or assertions, pre-build the wire bytes once and skip per-request
    // template expansion entirely. This recovers the ~1.7M req/s peak
    // that the old pre-built-bytes approach achieved.
    //
    // After Tier-1 unification, "only one HTTP scenario" (instead of
    // "only one scenario total") is the relevant precondition — a
    // mixed-protocol plan with 1 HTTP + N other-protocol scenarios is
    // still a static fast-path candidate.
    let single_scenario = http_indices.len() == 1;
    let first_req_plan = plan.scenarios[http_indices[0]]
        .steps
        .iter()
        .find_map(|s| match s { Step::Request(r) => Some(r), _ => None })
        .expect("HTTP scenario must have at least one Request step");
    let use_static = single_scenario
        && first_req_plan.is_static()
        && first_req_plan.extract.is_empty()
        && first_req_plan.checks.is_empty();

    let static_bytes: Option<Vec<u8>> = if use_static {
        let mut buf = Vec::with_capacity(512);
        build_raw_request(
            first_req_plan,
            &mut ctx,
            target,
            ConnectionMode::KeepAlive,
            &mut buf,
        )
        .expect("failed to build static request bytes");
        Some(buf)
    } else {
        None
    };

    // Scenario ID for the static fast-path — the single HTTP scenario's
    // index in `plan.scenarios`. Not always 0 when SSE/WS scenarios are
    // declared first.
    let static_scenario_id: u16 = http_indices[0] as u16;

    // Macro-like helper to dispatch the right prepare_request call.
    // When static bytes are available, use the fast memcpy path.
    // Otherwise expand templates per-request.
    macro_rules! prepare_conn {
        ($conn:expr, $intended:expr, $plan:expr, $ctx:expr, $target:expr, $sid:expr) => {
            if let Some(ref bytes) = static_bytes {
                $conn.prepare_request(bytes, $intended);
                $conn.scenario_id = static_scenario_id;
            } else {
                $conn.prepare_request_from_template($plan, $ctx, $target, $intended, $sid);
            }
        };
    }

    let open_loop = target_rps.is_some();
    let token_interval = target_rps.map(|rps| Duration::from_secs_f64(1.0 / rps));
    let started_at = Instant::now();
    let mut next_token_at = started_at;
    // Pre-allocate idle list with capacity; filled below for open-loop.
    let mut idle_conns: Vec<usize> = Vec::with_capacity(num_conns);

    let sni_name = target.sni_name().to_string();

    // Open connections and register with mio. `connect_with_retry` may
    // re-resolve once on transient failures (ECONNREFUSED, etc.) so
    // rolling-deploy DNS flips don't kill the first batch of connects.
    let mut connections: Vec<Conn> = Vec::with_capacity(num_conns);
    for i in 0..num_conns {
        let mut tcp = match connect_with_retry(target, opts) {
            Ok((s, _)) => s,
            Err(_) => {
                recorder.record_error(0, ErrorKind::Connect);
                continue;
            }
        };
        tcp.set_nodelay(opts.tcp_nodelay).ok();

        // Register the raw TCP stream with mio first (needed for both
        // plain and TLS paths — mio watches the socket fd, not the
        // TLS wrapper).
        poll.registry()
            .register(
                &mut tcp,
                Token(i),
                Interest::READABLE | Interest::WRITABLE,
            )
            .expect("mio register");

        let stream = if let Some(ref config) = tls_config {
            let mut tls_stream = match MioTlsStream::new(tcp, Arc::clone(config), &sni_name) {
                Ok(s) => s,
                Err(_) => {
                    recorder.record_error(0, ErrorKind::Connect);
                    continue;
                }
            };
            // Drive the TLS handshake to completion using mio poll
            // events. Wall-clock bounded by `opts.connect_timeout` —
            // that's the same budget the prior TCP connect already
            // respects, so a slow TLS peer can't extend the total
            // handshake past what the user asked for.
            if tls_stream
                .complete_handshake(&mut poll, Token(i), opts.connect_timeout)
                .is_err()
            {
                recorder.record_error(0, ErrorKind::Connect);
                continue;
            }
            // Re-register after handshake: mio uses edge-triggered epoll
            // (EPOLLET), so events consumed during the handshake won't
            // re-fire. Reregister forces fresh notification delivery.
            poll.registry()
                .reregister(
                    tls_stream.tcp_stream_mut(),
                    Token(i),
                    Interest::READABLE | Interest::WRITABLE,
                )
                .expect("mio reregister after TLS handshake");
            MioStream::Tls(tls_stream)
        } else {
            MioStream::Plain(tcp)
        };

        let mut conn = Conn::new(stream);
        conn.skip_header_capture = use_static;
        connections.push(conn);
    }

    // Cache the currently active RequestPlan per connection. For the
    // initial send (saturate mode), we need to pick a scenario right away.
    // We store references via indices into plan.scenarios rather than
    // pointers, so we can look them up for post-response processing.

    // Per-connection scenario index tracker — maps conn index to the index
    // into plan.scenarios that was last assigned to it.
    let mut conn_scenario_idx: Vec<usize> = vec![0; connections.len()];

    if open_loop {
        // All connections start idle; tokens will assign them work.
        for i in 0..connections.len() {
            idle_conns.push(i);
        }
    } else {
        // Saturate mode: start all connections immediately.
        let now = Instant::now();
        for (i, conn) in connections.iter_mut().enumerate() {
            let sel = pick_scenario(plan, &http_indices, &mut ctx);
            conn_scenario_idx[i] = sel.scenario_id as usize;
            prepare_conn!(conn, now, sel.request_plan, &mut ctx, target, sel.scenario_id);
        }
    }

    // Pending token queue — tokens wait here briefly for a connection
    // to become idle instead of being immediately dropped as keepup.
    // Bounded at 2x connection count to prevent unbounded growth.
    let mut pending_tokens: std::collections::VecDeque<Instant> =
        std::collections::VecDeque::with_capacity(num_conns * 2);
    let max_pending = num_conns * 2;

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

        // --- Assign pending tokens to idle connections ------------------
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if let Some(conn_idx) = idle_conns.pop() {
                    pending_tokens.pop_front();
                    let sel = pick_scenario(plan, &http_indices, &mut ctx);
                    conn_scenario_idx[conn_idx] = sel.scenario_id as usize;
                    let conn = &mut connections[conn_idx];
                    prepare_conn!(conn, intended, sel.request_plan, &mut ctx, target, sel.scenario_id);
                    match conn.try_write() {
                        Ok(true) => conn.state = ConnState::ReadingHeaders,
                        Ok(false) => {}
                        Err(_) => {
                            conn.state = ConnState::Dead;
                            recorder.record_error(sel.scenario_id, ErrorKind::Write);
                        }
                    }
                } else {
                    break; // no idle conns — tokens wait in queue
                }
            }
        }

        // --- Calculate poll timeout ------------------------------------
        let poll_timeout = if open_loop {
            if !pending_tokens.is_empty() {
                // Tokens waiting — wake up ASAP to check for completions.
                Some(Duration::ZERO)
            } else {
                // No pending tokens — wake at next token time.
                let until_next = next_token_at.saturating_duration_since(Instant::now());
                Some(until_next.min(Duration::from_millis(1)))
            }
        } else {
            Some(Duration::from_millis(100))
        };

        if poll.poll(&mut events, poll_timeout).is_err() {
            break;
        }

        // Cache Instant::now() once per poll batch — shared across all
        // events in this iteration. Saves ~50% of clock_gettime calls
        // (was 14.8% of CPU). Sub-us precision loss between events in
        // the same batch is acceptable for a benchmark tool.
        let batch_now = Instant::now();

        for event in events.iter() {
            let idx = event.token().0;
            if idx >= connections.len() {
                continue;
            }
            let conn = &mut connections[idx];

            if matches!(conn.state, ConnState::Dead) {
                continue;
            }

            // --- Handle writable (Writing or Idle) ---------------------
            if event.is_writable() {
                match conn.state {
                    ConnState::Writing => match conn.try_write() {
                        Ok(true) => {
                            conn.state = ConnState::ReadingHeaders;
                        }
                        Ok(false) => {} // would-block
                        Err(_) => {
                            conn.state = ConnState::Dead;
                            recorder.record_error(conn.scenario_id, ErrorKind::Write);
                        }
                    },
                    ConnState::Idle if !open_loop => {
                        // Saturate: start next request immediately.
                        // CO-free discipline: `intended_start` is the
                        // moment the previous response completed, which
                        // we approximate with `batch_now` (the Instant
                        // captured just before processing this event
                        // batch). Using a fresh `Instant::now()` here
                        // would undercount the latency of the next
                        // response by the time we spent processing
                        // prior events in this same batch.
                        let sel = pick_scenario(plan, &http_indices, &mut ctx);
                        conn_scenario_idx[idx] = sel.scenario_id as usize;
                        prepare_conn!(conn, batch_now, sel.request_plan, &mut ctx, target, sel.scenario_id);
                        match conn.try_write() {
                            Ok(true) => {
                                conn.state = ConnState::ReadingHeaders;
                            }
                            Ok(false) => {}
                            Err(_) => {
                                conn.state = ConnState::Dead;
                                recorder.record_error(sel.scenario_id, ErrorKind::Write);
                            }
                        }
                    }
                    // For TLS, writable events while reading are NOT
                    // spurious — pending encrypted output needs to be
                    // flushed before the server can respond.
                    _ => conn.stream.flush_tls(),
                }
            }

            // --- Handle readable (ReadingHeaders or ReadingBody) -------
            if event.is_readable()
                && matches!(
                    conn.state,
                    ConnState::ReadingHeaders
                        | ConnState::ReadingBody { .. }
                        | ConnState::ReadingChunkedBody { .. }
                )
            {
                match conn.try_read(batch_now) {
                    Ok(Some((status, ttfb, _total))) => {
                        let scenario_id = conn.scenario_id;
                        let bytes_sent = conn.write_buf.len() as u64;
                        let bytes_recv = conn.read_buf.len() as u64;
                        // CO-free latency: measured from intended_start.
                        let co_free_latency = batch_now.duration_since(conn.intended_start);

                        // --- Status-class error tracking ---
                        if (400..500).contains(&status) {
                            recorder.record_error(scenario_id, ErrorKind::Status4xx);
                        } else if (500..600).contains(&status) {
                            recorder.record_error(scenario_id, ErrorKind::Status5xx);
                        }

                        // --- Record stats ---
                        recorder.record(
                            scenario_id,
                            Sample {
                                latency: co_free_latency,
                                ttfb,
                                bytes_sent,
                                bytes_recv,
                            },
                        );

                        // --- Apply extractions + assertions (skip in static fast path) ---
                        if !use_static {
                            let scen_idx = conn_scenario_idx[idx];
                            let request_plan = plan.scenarios[scen_idx]
                                .steps
                                .iter()
                                .find_map(|s| match s {
                                    Step::Request(r) => Some(r),
                                    _ => None,
                                })
                                .expect("scenario must have a Request step");

                            apply_extractions(
                                request_plan,
                                status,
                                &conn.extracted_headers,
                                &mut ctx,
                            );

                            let assertion_failures =
                                check_assertions(request_plan, status, co_free_latency);
                            for _ in 0..assertion_failures {
                                recorder.record_error(scenario_id, ErrorKind::AssertionFailed);
                            }

                            ctx.clear_all();
                        }

                        if matches!(conn.state, ConnState::Idle) {
                            if open_loop {
                                // Return connection to the idle pool;
                                // the token scheduler will assign the next request.
                                idle_conns.push(idx);
                            } else {
                                // Saturate: pipeline next request immediately.
                                let sel = pick_scenario(plan, &http_indices, &mut ctx);
                                conn_scenario_idx[idx] = sel.scenario_id as usize;
                                prepare_conn!(conn, batch_now, sel.request_plan, &mut ctx, target, sel.scenario_id);
                                match conn.try_write() {
                                    Ok(true) => {
                                        conn.state = ConnState::ReadingHeaders;
                                    }
                                    Ok(false) => {}
                                    Err(_) => {
                                        conn.state = ConnState::Dead;
                                        recorder.record_error(
                                            sel.scenario_id,
                                            ErrorKind::Write,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {} // incomplete — keep reading
                    Err(_) => {
                        let scenario_id = conn.scenario_id;
                        conn.state = ConnState::Dead;
                        recorder.record_error(scenario_id, ErrorKind::Read);
                    }
                }
            }
        }

        // --- Post-event token assignment --------------------------------
        // Connections that just went idle can immediately pick up queued
        // tokens. Without this second pass, idle conns would wait until
        // the next outer-loop iteration — wasting an entire poll cycle.
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if let Some(conn_idx) = idle_conns.pop() {
                    pending_tokens.pop_front();
                    let sel = pick_scenario(plan, &http_indices, &mut ctx);
                    conn_scenario_idx[conn_idx] = sel.scenario_id as usize;
                    let conn = &mut connections[conn_idx];
                    prepare_conn!(conn, intended, sel.request_plan, &mut ctx, target, sel.scenario_id);
                    match conn.try_write() {
                        Ok(true) => conn.state = ConnState::ReadingHeaders,
                        Ok(false) => {}
                        Err(_) => {
                            conn.state = ConnState::Dead;
                            recorder.record_error(sel.scenario_id, ErrorKind::Write);
                        }
                    }
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

/// Spawn up to `num_threads` OS threads, each with its own mio event loop,
/// sharing `total_conns` such that the *exact* requested total is honoured.
/// Blocks until `duration` elapses, then joins all threads and returns
/// their stats.
///
/// `target_rps`: `None` for saturate (closed-loop), `Some(rps)` for
/// open-loop constant-rate mode. The total rate is split evenly across
/// the *actual* number of active threads — which is clamped to
/// `min(num_threads, total_conns)` so we don't spawn workers that own
/// zero connections.
///
/// Fixes the `-c N -t T` bug where `div_ceil(total_conns, num_threads) *
/// num_threads` over-allocated by up to `num_threads - 1` connections
/// when `total_conns` didn't divide evenly.
pub fn run_mio_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    num_threads: usize,
    total_conns: usize,
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

    // Exact distribution: first `total % threads` threads get one more
    // connection, the rest get the floor. Clamps to `total` so we never
    // launch more threads than connections.
    let per_thread_conns = distribute_conns(total_conns, num_threads);
    let active_threads = per_thread_conns.len();
    let per_thread_rps = if active_threads == 0 {
        None
    } else {
        target_rps.map(|rps| rps / active_threads as f64)
    };

    // Spawn worker threads.
    let handles: Vec<_> = per_thread_conns
        .into_iter()
        .map(|conns| {
            let target = target.clone();
            let opts = opts.clone();
            let plan = plan.clone();
            let stop = stop.clone();
            let tls_config = tls_config.clone();
            let live = live.clone();

            std::thread::spawn(move || {
                run_mio_worker(
                    &plan,
                    &target,
                    &opts,
                    conns,
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
        .map(|h| h.join().expect("mio worker thread panicked"))
        .collect()
}

// ---------------------------------------------------------------------------
// Test helpers (public but doc-hidden)
// ---------------------------------------------------------------------------

/// Build raw request bytes from a `RequestPlan`. Exposed for integration
/// tests that need to construct the same wire bytes the mio worker uses.
#[doc(hidden)]
pub fn __test_build_request(
    plan: &zerobench_core::plan::RequestPlan,
    ctx: &mut zerobench_core::scenario_context::ScenarioContext,
    target: &Target,
    out: &mut Vec<u8>,
) {
    super::raw_h1_common::build_raw_request(
        plan,
        ctx,
        target,
        ConnectionMode::KeepAlive,
        out,
    )
    .expect("build request");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use zerobench_core::plan::{Assertion, Extract};

    /// Verify `Conn` state transitions on a synthetic buffer that contains
    /// a complete HTTP response in one chunk.
    #[test]
    fn conn_check_headers_complete_response() {
        // Simulate a connection that has already read a full response.
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nhello";

        // Test memchr-accelerated find_header_end + httparse.
        assert!(find_header_end(response).is_some());
        let hdr_end = find_header_end(response).unwrap();
        assert_eq!(&response[hdr_end..], b"hello");

        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut resp = httparse::Response::new(&mut headers);
        let status = resp.parse(&response[..hdr_end]);
        assert!(status.is_ok());
        assert_eq!(resp.code, Some(200));
        assert_eq!(find_content_length_raw(resp.headers), ContentLength::Present(5));
        assert!(!find_connection_close(resp.headers));
    }

    #[test]
    fn conn_check_headers_partial() {
        let partial = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
        assert!(find_header_end(partial).is_none());
    }

    #[test]
    fn memchr_find_header_end_simd() {
        let full = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(find_header_end(full), Some(full.len()));

        let partial = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n";
        assert_eq!(find_header_end(partial), None);

        let empty_headers = b"HTTP/1.1 200 OK\r\n\r\n";
        assert_eq!(find_header_end(empty_headers), Some(empty_headers.len()));
    }

    #[test]
    fn check_assertions_status_eq_pass() {
        let plan = RequestPlan {
            method: http::Method::GET,
            url: zerobench_core::template::Template::literal("/"),
            headers: Default::default(),
            body: None,
            extract: Vec::new(),
            checks: vec![Assertion::StatusEq(200)],
            expect_streaming: false,
        };
        assert_eq!(check_assertions(&plan, 200, Duration::from_millis(1)), 0);
        assert_eq!(check_assertions(&plan, 404, Duration::from_millis(1)), 1);
    }

    #[test]
    fn check_assertions_latency_under() {
        let plan = RequestPlan {
            method: http::Method::GET,
            url: zerobench_core::template::Template::literal("/"),
            headers: Default::default(),
            body: None,
            extract: Vec::new(),
            checks: vec![Assertion::LatencyUnder(Duration::from_millis(100))],
            expect_streaming: false,
        };
        assert_eq!(check_assertions(&plan, 200, Duration::from_millis(50)), 0);
        assert_eq!(check_assertions(&plan, 200, Duration::from_millis(200)), 1);
    }

    #[test]
    fn apply_extractions_status_code() {
        let mut ctx = ScenarioContext::new(1, zerobench_core::rng::from_seed(1));
        let plan = RequestPlan {
            method: http::Method::GET,
            url: zerobench_core::template::Template::literal("/"),
            headers: Default::default(),
            body: None,
            extract: vec![Extract::StatusCode {
                into: zerobench_core::var::VarSlot(0),
            }],
            checks: Vec::new(),
            expect_streaming: false,
        };
        apply_extractions(&plan, 418, &[], &mut ctx);
        assert_eq!(
            ctx.get_var(zerobench_core::var::VarSlot(0))
                .map(|b| b.as_ref()),
            Some(b"418".as_ref()),
        );
    }

    #[test]
    fn apply_extractions_header() {
        let mut ctx = ScenarioContext::new(1, zerobench_core::rng::from_seed(1));
        let plan = RequestPlan {
            method: http::Method::GET,
            url: zerobench_core::template::Template::literal("/"),
            headers: Default::default(),
            body: None,
            extract: vec![Extract::Header {
                name: http::HeaderName::from_static("x-req-id"),
                into: zerobench_core::var::VarSlot(0),
            }],
            checks: Vec::new(),
            expect_streaming: false,
        };
        let captured = vec![(b"x-req-id".to_vec(), b"abc123".to_vec())];
        apply_extractions(&plan, 200, &captured, &mut ctx);
        assert_eq!(
            ctx.get_var(zerobench_core::var::VarSlot(0))
                .map(|b| b.as_ref()),
            Some(b"abc123".as_ref()),
        );
    }

    #[test]
    fn apply_extractions_header_missing_clears_slot() {
        let mut ctx = ScenarioContext::new(1, zerobench_core::rng::from_seed(1));
        // Pre-set the slot.
        ctx.set_var(
            zerobench_core::var::VarSlot(0),
            Bytes::from_static(b"old"),
        );
        let plan = RequestPlan {
            method: http::Method::GET,
            url: zerobench_core::template::Template::literal("/"),
            headers: Default::default(),
            body: None,
            extract: vec![Extract::Header {
                name: http::HeaderName::from_static("x-missing"),
                into: zerobench_core::var::VarSlot(0),
            }],
            checks: Vec::new(),
            expect_streaming: false,
        };
        apply_extractions(&plan, 200, &[], &mut ctx);
        assert!(ctx.get_var(zerobench_core::var::VarSlot(0)).is_none());
    }

    // ------------------------------------------------------------------
    // distribute_conns — the `-c N -t T` fix
    //
    // The buggy behaviour (`div_ceil(total, threads) * threads`) was
    // silently over-allocating connections when `total % threads != 0`,
    // e.g. `(20, 32)` would yield `1 * 32 = 32` real connections. We
    // want exactly `total` every time.
    // ------------------------------------------------------------------

    #[test]
    fn distribute_small_total_large_threads() {
        // 20 conns across 32 threads — the old bug produced 32.
        let per = distribute_conns(20, 32);
        assert_eq!(per.iter().sum::<usize>(), 20);
        assert_eq!(per.len(), 20); // threads clamped down to total
        assert!(per.iter().all(|&n| n == 1));
    }

    #[test]
    fn distribute_even() {
        let per = distribute_conns(100, 4);
        assert_eq!(per.iter().sum::<usize>(), 100);
        assert_eq!(per.len(), 4);
        assert!(per.iter().all(|&n| n == 25));
    }

    #[test]
    fn distribute_uneven_puts_remainder_in_first_threads() {
        let per = distribute_conns(7, 3);
        assert_eq!(per.iter().sum::<usize>(), 7);
        assert_eq!(per.len(), 3);
        // 7 = 3 + 2 + 2 (remainder goes first)
        assert_eq!(per[0], 3);
        assert_eq!(per[1], 2);
        assert_eq!(per[2], 2);
    }

    #[test]
    fn distribute_one_conn_many_threads() {
        let per = distribute_conns(1, 8);
        assert_eq!(per.iter().sum::<usize>(), 1);
        assert_eq!(per.len(), 1);
    }

    #[test]
    fn distribute_zero_total() {
        let per = distribute_conns(0, 4);
        assert!(per.is_empty());
        assert_eq!(per.iter().sum::<usize>(), 0);
    }

    #[test]
    fn distribute_zero_threads_is_defensive() {
        // Pathological input — clamp to at least 1 thread if any conns
        // were requested. Matches `num_threads = num_threads.max(1)` in
        // the top-level driver.
        let per = distribute_conns(5, 0);
        assert_eq!(per.iter().sum::<usize>(), 5);
        assert_eq!(per.len(), 1);
        assert_eq!(per[0], 5);
    }

    #[test]
    fn distribute_many_pairs_exact_total() {
        for (total, threads) in [(20, 32), (100, 4), (7, 3), (1, 8), (0, 4), (1000, 17)] {
            let per = distribute_conns(total, threads);
            assert_eq!(
                per.iter().sum::<usize>(),
                total,
                "({total}, {threads}) distribution lost count"
            );
        }
    }
}
