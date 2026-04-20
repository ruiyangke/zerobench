//! HTTP cold-connect benchmark — fresh TCP+TLS+HTTP connection per op.
//!
//! Implements `docs/design-v0.1.0.md` §3.1 and `docs/PHILOSOPHY.md` §4.2.
//! Where `mio_h1` measures steady-state pool throughput, cold-connect
//! measures **connection establishment cost**: accept queue, TCP 3WHS,
//! TLS handshake, first request/response. One op = one fresh
//! connection and one request/response cycle; the socket closes
//! afterwards.
//!
//! # Threading
//!
//! `connections` worker threads, each running a tight serial loop over
//! fresh connections. No pool — that's the whole point. Each thread
//! uses its own small `mio::Poll` so TCP/TLS setup is non-blocking.
//! Token-bucket pacing is shared across workers via an
//! `AtomicU64` elapsed-ns counter (open-loop, CO-free).
//!
//! # Metrics
//!
//! Records handshake time (connect-start → request-written), TTFB
//! (request-written → first-byte), and total (connect-start →
//! response-complete) as three distinct latencies. All HDR-bounded.

use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::net::TcpStream as MioTcp;
use mio::{Events, Interest, Poll, Token};
use rustls::ClientConfig;

use zerobench_core::plan::{Plan, Protocol, RequestPlan, Step};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stats::{ErrorKind, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_runtime::transport::{classify, TransportError};
use zerobench_runtime::{LiveSnapshot, Recorder, Sample};

use crate::http::mio_tls::{MioStream, MioTlsStream};
use crate::http::raw_h1_common::{
    apply_extractions, build_raw_request, capture_headers, check_assertions, ConnectionMode,
};

const POLL_TOKEN: Token = Token(0);

/// Drive all `Step::HttpColdConnect` scenarios in `plan` for `duration`.
///
/// `connections` is the degree of parallelism (number of worker
/// threads). `target_rps` bounds global throughput; `None` means
/// saturate (fire as fast as each thread can).
#[allow(clippy::too_many_arguments)]
pub fn run_cold_connect_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    connections: u32,
    duration: Duration,
    target_rps: Option<f64>,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<Arc<LiveSnapshot>>,
    stop_flag: Option<Arc<AtomicBool>>,
) -> Vec<TaskStats> {
    let num_scenarios = plan.scenarios.len();
    let mut out: Vec<TaskStats> = Vec::new();

    let cold_indices: Vec<usize> = plan
        .scenarios
        .iter()
        .enumerate()
        .filter_map(|(i, s)| {
            (s.protocol() == Protocol::Http
                && s.steps.iter().any(|st| matches!(st, Step::HttpColdConnect(_))))
            .then_some(i)
        })
        .collect();

    if cold_indices.is_empty() {
        return out;
    }

    let stop = stop_flag.unwrap_or_else(|| {
        let s = Arc::new(AtomicBool::new(false));
        let timer_stop = s.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer_stop.store(true, Ordering::Relaxed);
        });
        s
    });

    // Global intended-elapsed counter for open-loop pacing. Workers
    // atomically take-a-slot by incrementing this, giving each op a
    // deterministic `intended_start` even under contention.
    let intended_elapsed_ns: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let base = Instant::now();
    let deadline = base + duration;
    let interval_ns: u64 = match target_rps {
        Some(r) if r > 0.0 => (1_000_000_000.0 / r) as u64,
        _ => 0,
    };

    let num_vars = plan.vars.len();
    for &sid in &cold_indices {
        let scenario = &plan.scenarios[sid];
        let cold_plan = scenario
            .steps
            .iter()
            .find_map(|s| match s {
                Step::HttpColdConnect(p) => Some(p.clone()),
                _ => None,
            })
            .expect("HttpColdConnect scenario must have a HttpColdConnect step");

        let conns = connections.max(1) as usize;
        let handles: Vec<_> = (0..conns)
            .map(|worker_id| {
                let target = target.clone();
                let opts = opts.clone();
                let req_plan = cold_plan.request.clone();
                let stop = Arc::clone(&stop);
                let elapsed = Arc::clone(&intended_elapsed_ns);
                let tls = tls_config.clone();
                let live = live.clone();
                std::thread::Builder::new()
                    .name(format!("zerobench-cold-{worker_id}"))
                    .spawn(move || {
                        run_worker(
                            worker_id,
                            &target,
                            &opts,
                            &req_plan,
                            base,
                            deadline,
                            &stop,
                            &elapsed,
                            interval_ns,
                            tls.as_ref(),
                            live.as_deref(),
                            num_vars,
                            num_scenarios,
                            sid as u16,
                        )
                    })
                    .expect("spawn cold-connect worker")
            })
            .collect();

        // Merge worker stats into one TaskStats per scenario.
        let mut task = TaskStats::new(num_scenarios);
        for h in handles {
            let ws = h.join().expect("cold-connect worker panicked");
            task.merge(&ws);
        }
        out.push(task);
    }

    out
}

/// One worker thread — serial fresh-connect-per-op loop.
#[allow(clippy::too_many_arguments)]
fn run_worker(
    _worker_id: usize,
    target: &Target,
    opts: &TransportOpts,
    req_plan: &RequestPlan,
    base: Instant,
    deadline: Instant,
    stop: &AtomicBool,
    intended_elapsed_ns: &AtomicU64,
    interval_ns: u64,
    tls_config: Option<&Arc<ClientConfig>>,
    live: Option<&LiveSnapshot>,
    num_vars: usize,
    num_scenarios: usize,
    scenario_id: u16,
) -> TaskStats {
    let mut task = TaskStats::new(num_scenarios);
    let rng = zerobench_core::rng::from_entropy();
    let mut ctx = ScenarioContext::new(num_vars, rng);
    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(_) => return task,
    };
    let mut events = Events::with_capacity(4);
    let mut req_buf: Vec<u8> = Vec::with_capacity(512);

    // M3: Resolve once per worker thread (cached for the lifetime
    // of the worker) rather than per-op. Per-op resolve is wrong at
    // high rates — at 10K+ ops/s DNS becomes the bottleneck and we
    // measure resolver throughput instead of server cold-connect
    // latency. Workers still see distinct addresses when the user
    // passes multiple --resolve overrides because each worker gets
    // independent Target::resolve sticking (round-robin / family
    // preference). On rolling-DNS deployments we trade off for the
    // cache-hit path: 1 DNS call per worker per run, not per op.
    let cached_addr = target.resolve(opts).ok();

    loop {
        let now = Instant::now();
        if stop.load(Ordering::Relaxed) || now >= deadline {
            break;
        }

        // Open-loop pacing. When interval_ns > 0, reserve our slot and
        // sleep until it's our turn. When 0 (saturate), skip pacing.
        let intended_start = if interval_ns == 0 {
            now
        } else {
            let my_slot_ns =
                intended_elapsed_ns.fetch_add(interval_ns, Ordering::Relaxed);
            let target_at = base + Duration::from_nanos(my_slot_ns);
            if target_at > now {
                let sleep_for = target_at - now;
                if sleep_for > Duration::from_micros(5) {
                    std::thread::sleep(
                        sleep_for.min(Duration::from_millis(100)),
                    );
                }
            } else {
                // Falling behind — record keep-up miss.
                Recorder::new(&mut task, live)
                    .record_error(scenario_id, ErrorKind::Keepup);
            }
            target_at
        };

        let Some(addr) = cached_addr else {
            Recorder::new(&mut task, live)
                .record_error(scenario_id, ErrorKind::Connect);
            continue;
        };

        match do_one_op(
            &mut poll,
            &mut events,
            &mut req_buf,
            &mut ctx,
            addr,
            target,
            opts,
            req_plan,
            tls_config,
        ) {
            Ok(outcome) => {
                // PHILOSOPHY §1 / P6: measure from the intended start
                // of the op — the token-bucket slot we reserved, not
                // when we actually began executing after the pacing
                // sleep. This keeps the histogram CO-free under
                // open-loop pacing. In saturate mode intended_start ==
                // now-at-top-of-iteration, so the numbers are
                // equivalent.
                let total = outcome
                    .completed_at
                    .saturating_duration_since(intended_start);
                // Record the "full-cold" TTFB = handshake + wait-for-first-byte.
                // This is the meaningful signal for cold-connect: the server's
                // accept + TLS + first-write latency together.
                let full_ttfb = outcome.handshake + outcome.ttfb;
                {
                    // Scope the recorder so its `&mut task` borrow is released
                    // before the error-path blocks below (which still use
                    // `&mut task` directly — see note below).
                    let mut recorder = Recorder::new(&mut task, live);
                    recorder.record(
                        scenario_id,
                        Sample {
                            latency: total,
                            ttfb: full_ttfb,
                            bytes_sent: outcome.request_bytes,
                            bytes_recv: outcome.response_bytes,
                        },
                    );
                }
                // Classify 4xx/5xx.
                if (400..500).contains(&outcome.status) {
                    Recorder::new(&mut task, live)
                        .record_error(scenario_id, ErrorKind::Status4xx);
                } else if (500..600).contains(&outcome.status) {
                    Recorder::new(&mut task, live)
                        .record_error(scenario_id, ErrorKind::Status5xx);
                }

                // Run user-declared assertions + extractions. Matches
                // the behaviour of `mio_h1` so a script's
                // `.expect_status(200)` / `.extract_header(...)`
                // enforces / populates identically regardless of the
                // HTTP backend the CLI routed to. Extractions write
                // into `ctx.vars`; cold-connect clears the context
                // after each op so extractions are effectively op-
                // scoped (no cross-op var carry, unlike keep-alive
                // pool reuse in mio_h1).
                let assertion_failures =
                    check_assertions(req_plan, outcome.status, total);
                for _ in 0..assertion_failures {
                    Recorder::new(&mut task, live)
                        .record_error(scenario_id, ErrorKind::AssertionFailed);
                }
                apply_extractions(
                    req_plan,
                    outcome.status,
                    &outcome.extracted_headers,
                    &mut ctx,
                );
                ctx.clear_all();
            }
            Err(e) => {
                let kind = classify(&e);
                Recorder::new(&mut task, live).record_error(scenario_id, kind);
            }
        }
    }

    task
}

/// Result of one successful cold-connect op.
struct OpOutcome {
    handshake: Duration,
    ttfb: Duration,
    completed_at: Instant,
    status: u16,
    request_bytes: u64,
    response_bytes: u64,
    /// Lowercased-name → value tuples captured from the response
    /// headers. Fed to `apply_extractions` so the scenario's
    /// `.extract_header(...)` slots get populated.
    extracted_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

/// RAII guard that deregisters a stream from mio's registry on drop.
///
/// Protects against dangling registrations when an op's error path
/// abandons the stream mid-loop. `armed` lets callers opt into the
/// deregister only after a successful `register` call — if
/// registration never happened, dropping is a no-op.
///
/// Holds the `Registry` by owned clone (`try_clone`) rather than a
/// borrow so the guard doesn't pin `poll` for its entire lifetime —
/// the op body needs `&mut Poll` for `wait_for` and handshake driving.
struct StreamGuard {
    registry: mio::Registry,
    stream: MioStream,
    armed: bool,
}

impl StreamGuard {
    fn new(registry: mio::Registry, stream: MioStream) -> Self {
        Self { registry, stream, armed: false }
    }
    fn arm(&mut self) {
        self.armed = true;
    }
    fn register(&mut self, token: Token, interest: Interest) -> io::Result<()> {
        self.registry
            .register(self.stream.tcp_stream_mut(), token, interest)
    }
    fn stream_mut(&mut self) -> &mut MioStream {
        &mut self.stream
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.registry.deregister(self.stream.tcp_stream_mut());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn do_one_op(
    poll: &mut Poll,
    events: &mut Events,
    req_buf: &mut Vec<u8>,
    ctx: &mut ScenarioContext,
    addr: std::net::SocketAddr,
    target: &Target,
    opts: &TransportOpts,
    req_plan: &RequestPlan,
    tls_config: Option<&Arc<ClientConfig>>,
) -> Result<OpOutcome, TransportError> {
    let connect_start = Instant::now();

    // --- TCP connect (no registration yet) ---
    let tcp = MioTcp::connect(addr).map_err(|e| TransportError::Connect(e.to_string()))?;
    let _ = tcp.set_nodelay(true);

    // --- TLS wrap BEFORE registration ---
    // Building `MioTlsStream` does no I/O — it just constructs the
    // rustls state machine — so doing it here is safe. On wrap failure
    // `tcp` is dropped and its fd is closed; there is no registration
    // to leak. This ordering guarantees: if the function returns Err
    // before the guard arms, no mio registration exists.
    let stream = if target.tls {
        let config = tls_config.ok_or_else(|| {
            TransportError::Tls(
                "https:// target but no TLS config was provided by the caller"
                    .into(),
            )
        })?;
        let sni = target.sni_name().to_string();
        let tls = MioTlsStream::new(tcp, Arc::clone(config), &sni)
            .map_err(|e| TransportError::Tls(format!("init: {e}")))?;
        MioStream::Tls(tls)
    } else {
        MioStream::Plain(tcp)
    };

    // --- Register under a drop guard ---
    // From here onward, any return path — success, error, panic —
    // drops `guard` which deregisters automatically. No manual
    // deregister calls needed inside the op body. The cloned
    // registry detaches guard's lifetime from `poll` so `wait_for`
    // and `poll.poll(...)` can take `&mut poll` freely below.
    let registry = poll
        .registry()
        .try_clone()
        .map_err(|e| TransportError::Connect(e.to_string()))?;
    let mut guard = StreamGuard::new(registry, stream);
    guard
        .register(POLL_TOKEN, Interest::READABLE | Interest::WRITABLE)
        .map_err(|e| TransportError::Connect(e.to_string()))?;
    guard.arm();

    // Wait for writable (TCP connect done).
    wait_for(
        poll,
        events,
        opts.connect_timeout,
        |ev| ev.token() == POLL_TOKEN && ev.is_writable(),
    )
    .map_err(|e| TransportError::Connect(e.to_string()))?;
    if let Err(e) = guard.stream_mut().tcp_stream_mut().peer_addr() {
        return Err(TransportError::Connect(e.to_string()));
    }

    // Drive TLS handshake to completion.
    if guard.stream_mut().is_handshaking() {
        let hs_deadline = Duration::from_millis(5_000);
        let hs_start = Instant::now();
        while guard.stream_mut().is_handshaking() {
            if hs_start.elapsed() > hs_deadline {
                return Err(TransportError::Timeout);
            }
            guard
                .stream_mut()
                .drive_tls_io()
                .map_err(|e| TransportError::Tls(format!("handshake: {e}")))?;
            if !guard.stream_mut().is_handshaking() {
                break;
            }
            poll.poll(events, Some(Duration::from_millis(200)))
                .map_err(|e| TransportError::Connect(e.to_string()))?;
        }
    }

    // --- Write request ---
    req_buf.clear();
    // Cold-connect semantics require the server to close the
    // connection after the response. Without `Connection: close`,
    // servers returning Transfer-Encoding: chunked responses (common
    // in modern stacks) keep the connection alive and we'd hang
    // until `request_timeout` for every op. The builder emits
    // `Connection: close` directly; a user-provided Connection
    // header (e.g. `upgrade` for WS handshakes) wins.
    build_raw_request(req_plan, ctx, target, ConnectionMode::Close, req_buf)?;
    let mut write_pos = 0;
    while write_pos < req_buf.len() {
        match guard.stream_mut().write(&req_buf[write_pos..]) {
            Ok(0) => {
                return Err(TransportError::Write(
                    "peer closed during request write".into(),
                ))
            }
            Ok(n) => write_pos += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait_for(poll, events, opts.request_timeout, |ev| {
                    ev.token() == POLL_TOKEN && ev.is_writable()
                })
                .map_err(|_| TransportError::Timeout)?;
            }
            Err(e) => return Err(TransportError::Write(e.to_string())),
        }
    }
    let request_bytes = req_buf.len() as u64;
    let write_done = Instant::now();
    let handshake = write_done.duration_since(connect_start);

    // --- Read response ---
    let mut read_buf: Vec<u8> = Vec::with_capacity(8192);
    let mut scratch = [0u8; 8192];
    let mut first_byte_at: Option<Instant> = None;
    let mut header_end: Option<usize> = None;
    let mut status: u16 = 0;
    let mut content_length: usize = 0;
    let mut have_content_length = false;
    let mut captured_headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    let read_deadline = Instant::now() + opts.request_timeout;
    loop {
        if Instant::now() >= read_deadline {
            return Err(TransportError::Timeout);
        }
        match guard.stream_mut().read(&mut scratch) {
            Ok(0) => {
                // EOF — if we haven't parsed headers, that's a read error.
                if header_end.is_none() {
                    return Err(TransportError::Protocol(
                        "EOF before response headers received".into(),
                    ));
                }
                // Otherwise EOF after headers is valid Connection: close.
                break;
            }
            Ok(n) => {
                if first_byte_at.is_none() {
                    first_byte_at = Some(Instant::now());
                }
                read_buf.extend_from_slice(&scratch[..n]);

                if header_end.is_none() {
                    if let Some(pos) =
                        memchr::memmem::find(&read_buf, b"\r\n\r\n").map(|p| p + 4)
                    {
                        header_end = Some(pos);
                        let mut headers = [httparse::EMPTY_HEADER; 32];
                        let mut resp = httparse::Response::new(&mut headers);
                        match resp.parse(&read_buf[..pos]) {
                            Ok(httparse::Status::Complete(_)) => {
                                status = resp.code.unwrap_or(0);
                                for h in resp.headers.iter() {
                                    if h.name.eq_ignore_ascii_case("content-length") {
                                        if let Ok(s) = std::str::from_utf8(h.value) {
                                            if let Ok(n) = s.trim().parse::<usize>() {
                                                content_length = n;
                                                have_content_length = true;
                                            }
                                        }
                                    }
                                }
                                // Capture for later .extract_header(...)
                                // / .extract_status(...) processing.
                                captured_headers = capture_headers(&resp);
                            }
                            _ => {
                                return Err(TransportError::Protocol(
                                    "malformed HTTP/1 response headers".into(),
                                ))
                            }
                        }
                    }
                }

                if let Some(hdr) = header_end {
                    if have_content_length
                        && read_buf.len() - hdr >= content_length
                    {
                        break;
                    }
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let left = read_deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(TransportError::Timeout);
                }
                wait_for(poll, events, left, |ev| {
                    ev.token() == POLL_TOKEN && ev.is_readable()
                })
                .map_err(|_| TransportError::Timeout)?;
            }
            Err(e) => return Err(TransportError::Read(e.to_string())),
        }
    }

    let completed_at = Instant::now();
    let ttfb = first_byte_at
        .unwrap_or(completed_at)
        .duration_since(write_done);
    let response_bytes = read_buf.len() as u64;

    // `guard` drops here: deregister then close the underlying fd.

    Ok(OpOutcome {
        handshake,
        ttfb,
        completed_at,
        status,
        request_bytes,
        response_bytes,
        extracted_headers: captured_headers,
    })
}

/// Wait for any event matching `pred` with timeout. Returns Ok if one
/// fires, Err on timeout or poll error.
fn wait_for(
    poll: &mut Poll,
    events: &mut Events,
    timeout: Duration,
    pred: impl Fn(&mio::event::Event) -> bool,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "cold-connect wait timeout",
            ));
        }
        poll.poll(events, Some(deadline - now))?;
        for ev in events.iter() {
            if pred(ev) {
                return Ok(());
            }
        }
    }
}
