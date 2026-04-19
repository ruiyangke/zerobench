//! SSE reconnect-storm benchmark — `docs/design-v0.1.0.md` §3.2
//! `SseReconnectStorm` and `docs/PHILOSOPHY.md` §4.3.
//!
//! N SSE subscribers; each is killed at `kill_rate_per_s` then
//! reconnects with the `Last-Event-ID` header set to the most recent
//! `id:` value it saw (per WHATWG EventSource §9.2). Measures
//! reconnect success + Last-Event-ID propagation.
//!
//! # Scope
//!
//! Thread-per-subscriber blocking I/O — same scaling caveat as
//! `hold.rs`. Kill scheduling uses an exponential-interval per
//! subscriber (memoryless) so the aggregate kill rate matches
//! `kill_rate_per_s`.

use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use mio::net::TcpStream as MioTcp;
use mio::{Events, Interest, Poll, Token};
use rand::Rng;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist};
use zerobench_core::plan::{Plan, Protocol, SseReconnectStormPlan, Step};
use zerobench_core::stats::{SseExtras, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_http::mio_tls::MioStream;

use crate::line_parser::{SseEvent, SseLineParser};

const POLL_TOKEN: Token = Token(0);

/// Per-subscriber rollup.
#[derive(Debug, Clone)]
struct SubStats {
    events: u64,
    bytes_received: u64,
    reconnects_attempted: u64,
    reconnects_succeeded: u64,
    /// Reconnects where the post-reconnect response advanced past
    /// the previous `Last-Event-ID` — i.e. the server honored the
    /// resume protocol. A strict subset of `reconnects_succeeded`.
    reconnects_resumed: u64,
    errors_connect: u64,
    errors_read: u64,
}

impl SubStats {
    fn new() -> Self {
        Self {
            events: 0,
            bytes_received: 0,
            reconnects_attempted: 0,
            reconnects_succeeded: 0,
            reconnects_resumed: 0,
            errors_connect: 0,
            errors_read: 0,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run_sse_reconnect_storm_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    // Per-session event recording would flood the per-second snapshot
    // with non-latency values during kill/reconnect churn; deferred
    // for a sharper integration that surfaces reconnect_succeeded /
    // reconnect_resumed as dedicated counters. For now: accepted for
    // API symmetry with sibling backends.
    _live: Option<Arc<zerobench_core::LiveSnapshot>>,
    stop_flag: Option<Arc<AtomicBool>>,
) -> Vec<TaskStats> {
    let stop = stop_flag.unwrap_or_else(|| {
        let s = Arc::new(AtomicBool::new(false));
        let timer_stop = s.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer_stop.store(true, Ordering::Relaxed);
        });
        s
    });
    let num_scenarios = plan.scenarios.len();
    let mut out: Vec<TaskStats> = Vec::new();

    for (sid, scenario) in plan.scenarios.iter().enumerate() {
        if scenario.protocol() != Protocol::Sse {
            continue;
        }
        let storm_plan = scenario.steps.iter().find_map(|s| match s {
            Step::SseReconnectStorm(p) => Some(p.clone()),
            _ => None,
        });
        let Some(storm_plan) = storm_plan else { continue };

        let wall_deadline = Instant::now()
            + duration.min(if storm_plan.subscribers.hold_for.is_zero() {
                duration
            } else {
                storm_plan.subscribers.hold_for
            });

        let sub_count = storm_plan.subscribers.subscribers.max(1) as usize;
        let kill_rate = if storm_plan.kill_rate_per_s <= 0.0 {
            0.0
        } else {
            storm_plan.kill_rate_per_s
        };

        let handles: Vec<_> = (0..sub_count)
            .map(|_| {
                let target = target.clone();
                let opts = opts.clone();
                let plan = storm_plan.clone();
                let stop = Arc::clone(&stop);
                let tls = tls_config.clone();
                std::thread::Builder::new()
                    .name("zerobench-sse-storm".into())
                    .spawn(move || {
                        run_one_subscriber(
                            &target,
                            &opts,
                            &plan,
                            wall_deadline,
                            &stop,
                            kill_rate,
                            tls.as_ref(),
                        )
                    })
                    .expect("spawn storm subscriber")
            })
            .collect();

        let mut rollup = SubStats::new();
        let mut ttfb_hist: Histogram<u64> = new_hist();
        let mut reconnect_gap: Histogram<u64> = new_hist();
        for h in handles {
            let (s, ttfb, gaps) = h.join().expect("storm subscriber panicked");
            rollup.events += s.events;
            rollup.bytes_received += s.bytes_received;
            rollup.reconnects_attempted += s.reconnects_attempted;
            rollup.reconnects_succeeded += s.reconnects_succeeded;
            rollup.reconnects_resumed += s.reconnects_resumed;
            rollup.errors_connect += s.errors_connect;
            rollup.errors_read += s.errors_read;
            if let Some(t) = ttfb {
                let _ = ttfb_hist.record(duration_to_hist_ns(t));
            }
            for g in gaps {
                let _ = reconnect_gap.record(duration_to_hist_ns(g));
            }
        }

        // Only the resumed count is counted as a hard error; connect
        // failures during reconnect are accepted for storm loads.
        let resume_failures =
            rollup.reconnects_succeeded.saturating_sub(rollup.reconnects_resumed);

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            sc.requests = rollup.events;
            task.requests = rollup.events;
            task.bytes_recv = rollup.bytes_received;
            sc.errors.connect = rollup.errors_connect;
            sc.errors.read = rollup.errors_read
                + if storm_plan.verify_last_event_id { resume_failures } else { 0 };
            task.errors.connect += rollup.errors_connect;
            task.errors.read += rollup.errors_read
                + if storm_plan.verify_last_event_id { resume_failures } else { 0 };
            *sc.sse_mut() = SseExtras {
                ttfb: ttfb_hist,
                chunk_gap: reconnect_gap,
                chunks: rollup.events,
                streams_completed: rollup.reconnects_succeeded,
                bytes_received: rollup.bytes_received,
                broadcast_rtt: new_hist(),
            };
        }
        out.push(task);
    }
    out
}

/// Run one subscriber — connect, read events, die at exponential
/// intervals then reconnect with Last-Event-ID. Returns (stats, ttfb
/// of first connect, reconnect-gap samples).
#[allow(clippy::too_many_arguments)]
fn run_one_subscriber(
    target: &Target,
    opts: &TransportOpts,
    plan: &SseReconnectStormPlan,
    deadline: Instant,
    stop: &AtomicBool,
    kill_rate: f64,
    tls_config: Option<&Arc<ClientConfig>>,
) -> (SubStats, Option<Duration>, Vec<Duration>) {
    let mut stats = SubStats::new();
    let mut first_ttfb: Option<Duration> = None;
    let mut reconnect_gaps: Vec<Duration> = Vec::new();
    let mut rng = zerobench_core::rng::from_entropy();
    let mut last_event_id: Option<String> = None;
    let mut first_connect = true;
    // Instant at which the previous session ended. Used to compute
    // the honest reconnect-latency: new-session-first-byte minus
    // prior-session-end. None on the first iteration (no reconnect
    // has happened yet).
    let mut prior_session_end: Option<Instant> = None;

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let kill_duration = if kill_rate > 0.0 {
            // Exponential with rate kill_rate per second: -ln(U)/lambda.
            let u: f64 = rng.gen_range(1e-9..1.0);
            Duration::from_secs_f64((-u.ln()) / kill_rate)
        } else {
            deadline.saturating_duration_since(Instant::now())
        };
        let session_deadline = (Instant::now() + kill_duration).min(deadline);

        let prior_id = if first_connect { None } else { last_event_id.clone() };
        if !first_connect {
            stats.reconnects_attempted += 1;
        }
        match run_one_session(
            target,
            opts,
            plan,
            session_deadline,
            stop,
            prior_id.as_deref(),
            tls_config,
        ) {
            Ok(session) => {
                if first_ttfb.is_none() {
                    first_ttfb = session.ttfb;
                }
                stats.events += session.events;
                stats.bytes_received += session.bytes_received;
                if let Some(id) = session.last_event_id {
                    last_event_id = Some(id);
                }
                if !first_connect {
                    stats.reconnects_succeeded += 1;
                    // C5: reconnect gap = prior-session-end →
                    // new-session-first-byte. Caveat per the
                    // post-fix audit item #13: on servers with
                    // long event intervals, first_byte_at includes
                    // the HTTP 200 response line + potentially the
                    // first retry: / comment but NOT the first
                    // data event — SSE servers that emit response
                    // headers immediately give a clean
                    // reconnect-latency signal; servers that only
                    // flush bytes when they have real events
                    // inflate the histogram with their own event
                    // cadence. The histogram label is "chunk_gap"
                    // in the archive, which is deliberate aliasing
                    // — a dedicated "reconnect_connect_latency"
                    // slot is a future SseExtras extension.
                    if let (Some(prior_end), Some(first_byte)) =
                        (prior_session_end, session.first_byte_at)
                    {
                        reconnect_gaps
                            .push(first_byte.saturating_duration_since(prior_end));
                    }
                    // C6: Resume-honoured heuristic. We count a
                    // reconnect as "resumed" only when (a) the server
                    // did NOT re-send the event carrying
                    // prior_last_event_id (verified by tracking that
                    // specific ID through the new session) AND (b)
                    // new events actually arrived. A non-resuming
                    // server that starts from scratch and assigns new
                    // IDs would re-send the prior ID (within the
                    // buffered window) and trip saw_prior_id. A
                    // server that skips past prior and only emits new
                    // IDs does NOT trip saw_prior_id → counted as
                    // resumed.
                    if prior_id.is_some()
                        && session.events > 0
                        && !session.saw_prior_id
                    {
                        stats.reconnects_resumed += 1;
                    }
                }
                // Only track end-of-session on successful completions.
                // A failed connect or mid-session read error never had
                // a stable "session end" instant; measuring from the
                // failure-retry boundary would bias `reconnect_gaps`
                // toward artificially large values on flaky servers.
                prior_session_end = Some(Instant::now());
            }
            Err(SessionErr::Connect) => {
                stats.errors_connect += 1;
                // Leave prior_session_end as-is: the next successful
                // reconnect will compute its gap relative to the last
                // *good* session, not relative to a connect-failure
                // pit that contains no useful timing signal.
            }
            Err(SessionErr::Read) => {
                stats.errors_read += 1;
            }
        }
        first_connect = false;
    }
    (stats, first_ttfb, reconnect_gaps)
}

enum SessionErr {
    Connect,
    Read,
}

struct SessionOutcome {
    events: u64,
    bytes_received: u64,
    ttfb: Option<Duration>,
    /// Monotonic instant of the first response byte for this session.
    /// Used by the caller to compute the honest reconnect gap.
    first_byte_at: Option<Instant>,
    last_event_id: Option<String>,
    /// `true` iff any `id:` line seen in this session matches the
    /// `expected_prior_id` the caller supplied. Set only when the
    /// caller asks us to watch for a specific prior id.
    saw_prior_id: bool,
}

#[allow(clippy::too_many_arguments)]
fn run_one_session(
    target: &Target,
    opts: &TransportOpts,
    plan: &SseReconnectStormPlan,
    deadline: Instant,
    stop: &AtomicBool,
    expected_prior_id: Option<&str>,
    tls_config: Option<&Arc<ClientConfig>>,
) -> Result<SessionOutcome, SessionErr> {
    let addr = target.resolve(opts).map_err(|_| SessionErr::Connect)?;
    let mut poll = Poll::new().map_err(|_| SessionErr::Connect)?;
    let mut events = Events::with_capacity(4);
    let mut tcp = MioTcp::connect(addr).map_err(|_| SessionErr::Connect)?;
    let _ = tcp.set_nodelay(true);
    poll.registry()
        .register(&mut tcp, POLL_TOKEN, Interest::READABLE | Interest::WRITABLE)
        .map_err(|_| SessionErr::Connect)?;
    wait_for(&mut poll, &mut events, opts.connect_timeout, |e| {
        e.token() == POLL_TOKEN && e.is_writable()
    })
    .map_err(|_| SessionErr::Connect)?;
    if tcp.peer_addr().is_err() {
        return Err(SessionErr::Connect);
    }
    let mut stream = if target.tls {
        let cfg = tls_config.ok_or(SessionErr::Connect)?;
        let sni = target.sni_name().to_string();
        let tls = zerobench_http::mio_tls::MioTlsStream::new(tcp, Arc::clone(cfg), &sni)
            .map_err(|_| SessionErr::Connect)?;
        let mut s = MioStream::Tls(tls);
        let hs_start = Instant::now();
        while s.is_handshaking() {
            if hs_start.elapsed() > Duration::from_secs(5) {
                return Err(SessionErr::Connect);
            }
            s.drive_tls_io().map_err(|_| SessionErr::Connect)?;
            if !s.is_handshaking() {
                break;
            }
            let _ = poll.poll(&mut events, Some(Duration::from_millis(200)));
        }
        s
    } else {
        MioStream::Plain(tcp)
    };

    let request = build_request(target, plan, expected_prior_id);
    let mut write_pos = 0;
    while write_pos < request.len() {
        match stream.write(&request[write_pos..]) {
            Ok(0) => return Err(SessionErr::Connect),
            Ok(n) => write_pos += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait_for(&mut poll, &mut events, opts.request_timeout, |e| {
                    e.token() == POLL_TOKEN && e.is_writable()
                })
                .map_err(|_| SessionErr::Connect)?;
            }
            Err(_) => return Err(SessionErr::Connect),
        }
    }
    let t_sent = Instant::now();

    let mut outcome = SessionOutcome {
        events: 0,
        bytes_received: 0,
        ttfb: None,
        first_byte_at: None,
        last_event_id: expected_prior_id.map(|s| s.to_string()),
        saw_prior_id: false,
    };

    let mut buf = [0u8; 8192];
    let mut pre_body: Vec<u8> = Vec::new();
    let mut header_done = false;
    let mut parser = SseLineParser::default();
    let mut decoder = crate::hold::ChunkDecoder::new();

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let now = Instant::now();
                if outcome.first_byte_at.is_none() {
                    outcome.first_byte_at = Some(now);
                    outcome.ttfb = Some(now.saturating_duration_since(t_sent));
                }
                outcome.bytes_received += n as u64;

                let slice = &buf[..n];
                let body_slice = if header_done {
                    slice
                } else {
                    pre_body.extend_from_slice(slice);
                    match memchr::memmem::find(&pre_body, b"\r\n\r\n").map(|p| p + 4) {
                        Some(hdr_end) => {
                            let grown = pre_body.len() - n;
                            header_done = true;
                            &slice[hdr_end.saturating_sub(grown)..]
                        }
                        None => continue,
                    }
                };

                let mut decoded: Vec<u8> = Vec::with_capacity(body_slice.len());
                let _ = decoder.decode(body_slice, &mut decoded);
                if !decoded.is_empty() {
                    let events_ref = &mut outcome.events;
                    let last_id_ref = &mut outcome.last_event_id;
                    let saw_prior_ref = &mut outcome.saw_prior_id;
                    // Route Id extraction through the line parser so
                    // `id:` lines split across TCP reads are stitched
                    // back together — the parser owns line buffering
                    // and handles CRLF / LF / bare-CR boundaries. A
                    // chunk-boundary `id:\r\n12` + `345\r\n` previously
                    // lost the `12` on the first read and produced
                    // `345` instead of `12345`.
                    parser.feed(&decoded, |ev| match ev {
                        SseEvent::Data(_) => *events_ref += 1,
                        SseEvent::Id(val) => {
                            if let Ok(s) = std::str::from_utf8(&val) {
                                let trimmed = s.trim();
                                if let Some(expected) = expected_prior_id {
                                    if trimmed == expected {
                                        *saw_prior_ref = true;
                                    }
                                }
                                *last_id_ref = Some(trimmed.to_string());
                            }
                        }
                        SseEvent::Done | SseEvent::Ignored => {}
                    });
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No data yet — wait up to 100ms for readability or
                // the deadline, whichever comes first.
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let budget =
                    (deadline - now).min(Duration::from_millis(100));
                let _ = poll.poll(&mut events, Some(budget));
            }
            Err(_) => {
                return Err(SessionErr::Read);
            }
        }
    }
    Ok(outcome)
}

/// Poll until an event matching `pred` fires, or `timeout` elapses.
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
            return Err(io::Error::new(io::ErrorKind::TimedOut, "reconnect-storm wait"));
        }
        poll.poll(events, Some(deadline - now))?;
        for ev in events.iter() {
            if pred(ev) {
                return Ok(());
            }
        }
    }
}

fn build_request(
    target: &Target,
    plan: &SseReconnectStormPlan,
    last_event_id: Option<&str>,
) -> Vec<u8> {
    let url_str = render_template(&plan.subscribers.url);
    let path = match url_str.find("://").and_then(|i| url_str[i + 3..].find('/').map(|j| i + 3 + j)) {
        Some(p) => url_str[p..].to_string(),
        None => "/".to_string(),
    };
    let host = if (target.tls && target.port == 443) || (!target.tls && target.port == 80) {
        target.host.clone()
    } else {
        format!("{}:{}", target.host, target.port)
    };
    let mut s = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n",
    );
    if let Some(id) = last_event_id {
        s.push_str(&format!("Last-Event-ID: {id}\r\n"));
    }
    s.push_str("\r\n");
    s.into_bytes()
}

fn render_template(tpl: &zerobench_core::Template) -> String {
    let mut buf = Vec::with_capacity(256);
    let mut rng = zerobench_core::rng::from_entropy();
    let mut ctx = zerobench_core::ExpandCtx {
        rng: &mut rng,
        counter: &std::rc::Rc::new(std::cell::Cell::new(0)),
        scenario_vars: &[],
    };
    tpl.expand_into(&mut buf, &mut ctx);
    String::from_utf8_lossy(&buf).to_string()
}
