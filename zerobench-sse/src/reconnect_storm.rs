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

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::Rng;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist};
use zerobench_core::plan::{Plan, Protocol, SseReconnectStormPlan, Step};
use zerobench_core::stats::{SseExtras, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};

use crate::line_parser::{SseEvent, SseLineParser};

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

pub fn run_sse_reconnect_storm_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    _tls_config: Option<Arc<ClientConfig>>,
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
                std::thread::Builder::new()
                    .name("zerobench-sse-storm".into())
                    .spawn(move || {
                        run_one_subscriber(&target, &opts, &plan, wall_deadline, &stop, kill_rate)
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
            };
        }
        out.push(task);
    }
    out
}

/// Run one subscriber — connect, read events, die at exponential
/// intervals then reconnect with Last-Event-ID. Returns (stats, ttfb
/// of first connect, reconnect-gap samples).
fn run_one_subscriber(
    target: &Target,
    opts: &TransportOpts,
    plan: &SseReconnectStormPlan,
    deadline: Instant,
    stop: &AtomicBool,
    kill_rate: f64,
) -> (SubStats, Option<Duration>, Vec<Duration>) {
    let mut stats = SubStats::new();
    let mut first_ttfb: Option<Duration> = None;
    let mut reconnect_gaps: Vec<Duration> = Vec::new();
    let mut rng = zerobench_core::rng::from_entropy();
    let mut last_event_id: Option<String> = None;
    let mut prior_last_event_id: Option<String> = None;
    let mut first_connect = true;

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let kill_duration = if kill_rate > 0.0 {
            // Exponential with rate kill_rate per second: -ln(U)/lambda.
            let u: f64 = rng.gen_range(1e-9..1.0);
            Duration::from_secs_f64((-u.ln()) / kill_rate)
        } else {
            deadline.saturating_duration_since(Instant::now())
        };
        let session_deadline = (Instant::now() + kill_duration).min(deadline);

        let connect_start = Instant::now();
        if !first_connect {
            stats.reconnects_attempted += 1;
            prior_last_event_id = last_event_id.clone();
        }
        match run_one_session(
            target,
            opts,
            plan,
            session_deadline,
            stop,
            last_event_id.as_deref(),
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
                    reconnect_gaps.push(connect_start.elapsed());
                    // Heuristic: assume resume happened if the post-
                    // reconnect session saw at least one event AND the
                    // final last_event_id differs from the prior one
                    // we sent on the resume request.
                    if session.events > 0 && last_event_id != prior_last_event_id {
                        stats.reconnects_resumed += 1;
                    }
                }
            }
            Err(SessionErr::Connect) => {
                stats.errors_connect += 1;
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
    last_event_id: Option<String>,
}

fn run_one_session(
    target: &Target,
    opts: &TransportOpts,
    plan: &SseReconnectStormPlan,
    deadline: Instant,
    stop: &AtomicBool,
    last_event_id: Option<&str>,
) -> Result<SessionOutcome, SessionErr> {
    let addr = target.resolve(opts).map_err(|_| SessionErr::Connect)?;
    let mut stream =
        TcpStream::connect_timeout(&addr, opts.connect_timeout).map_err(|_| SessionErr::Connect)?;
    let _ = stream.set_nodelay(true);
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(|_| SessionErr::Read)?;
    stream
        .set_write_timeout(Some(opts.request_timeout))
        .map_err(|_| SessionErr::Read)?;

    let request = build_request(target, plan, last_event_id);
    stream
        .write_all(&request)
        .map_err(|_| SessionErr::Connect)?;
    let t_sent = Instant::now();

    let mut outcome = SessionOutcome {
        events: 0,
        bytes_received: 0,
        ttfb: None,
        last_event_id: last_event_id.map(|s| s.to_string()),
    };

    let mut buf = [0u8; 8192];
    let mut pre_body: Vec<u8> = Vec::new();
    let mut header_done = false;
    let mut parser = SseLineParser::default();
    let mut decoder = crate::hold::ChunkDecoder::new();
    let mut first_byte: Option<Instant> = None;

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let now = Instant::now();
                if first_byte.is_none() {
                    first_byte = Some(now);
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
                    // Also capture `id:` values for Last-Event-ID
                    // propagation. SseLineParser swallows id lines
                    // (reports Ignored) so we also grep manually.
                    for line in decoded.split(|&b| b == b'\n') {
                        if line.starts_with(b"id:") {
                            let val = line[3..]
                                .trim_ascii_start()
                                .trim_ascii_end();
                            if let Ok(s) = std::str::from_utf8(val) {
                                outcome.last_event_id = Some(s.to_string());
                            }
                        }
                    }
                    parser.feed(&decoded, |ev| match ev {
                        SseEvent::Data(_) => *events_ref += 1,
                        SseEvent::Done | SseEvent::Ignored => {}
                    });
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => {
                let _ = stream.shutdown(Shutdown::Both);
                return Err(SessionErr::Read);
            }
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    Ok(outcome)
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
