//! SSE broadcast-latency benchmark — `docs/design-v0.1.0.md` §3.2
//! `SseFanout` and `docs/PHILOSOPHY.md` §4.3.
//!
//! N SSE subscribers + a periodic external trigger. On each trigger
//! firing we record the send instant; every subscriber records its
//! event arrival instants. Post-hoc we correlate: for each trigger,
//! the first event observed by each subscriber after the trigger
//! send is the broadcast; `arrival - trigger_sent` is the
//! per-subscriber broadcast RTT.
//!
//! # Scope
//!
//! v0.1.0 implements:
//! - `TriggerSpec::HttpPost` — the tool fires HTTP POST triggers.
//! - `FanoutMode::TriggerRtt` — proxy latency as described above.
//!
//! `TriggerSpec::DedicatedWsConnection` and `FanoutMode::Timestamp`
//! return a runtime error (not yet implemented). The TriggerRtt mode
//! gives a usable broadcast-latency signal against any compliant SSE
//! server without server cooperation.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use mio::net::TcpStream as MioTcp;
use mio::{Events, Interest, Poll, Token};
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist, HIST_HI_NS, HIST_LO_NS};
use zerobench_core::plan::{
    FanoutMode, Plan, Protocol, SseFanoutPlan, Step, TriggerSpec,
};
use zerobench_core::stats::{SseExtras, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_http::mio_tls::MioStream;

use crate::line_parser::{SseEvent, SseLineParser};

const POLL_TOKEN: Token = Token(0);
const TRIGGER_INTERVAL_MS: u64 = 500; // 2/s triggers — balances accuracy vs server load

/// Event arrival instant for one subscriber.
type EventTime = Instant;

/// Per-subscriber state.
struct SubscriberStats {
    events: Vec<EventTime>,
    ttfb: Option<Duration>,
    errors_connect: u64,
    errors_read: u64,
    bytes_received: u64,
}

impl SubscriberStats {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            ttfb: None,
            errors_connect: 0,
            errors_read: 0,
            bytes_received: 0,
        }
    }
}

pub fn run_sse_fanout_from_plan_threaded(
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
        let fanout_plan = scenario.steps.iter().find_map(|s| match s {
            Step::SseFanout(p) => Some(p.clone()),
            _ => None,
        });
        let Some(fanout_plan) = fanout_plan else { continue };

        // Only the documented subset is supported today.
        if !matches!(fanout_plan.mode, FanoutMode::TriggerRtt) {
            eprintln!(
                "[sse_fanout] mode {:?} not yet implemented; falling back to TriggerRtt",
                fanout_plan.mode
            );
        }
        let trigger_url = match &fanout_plan.trigger {
            TriggerSpec::HttpPost { url, .. } => url.clone(),
            TriggerSpec::DedicatedWsConnection { .. } => {
                eprintln!(
                    "[sse_fanout] DedicatedWsConnection trigger not yet implemented; scenario skipped"
                );
                continue;
            }
        };

        let wall_deadline = Instant::now()
            + duration.min(if fanout_plan.subscribers.hold_for.is_zero() {
                duration
            } else {
                fanout_plan.subscribers.hold_for
            });

        let triggers: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::new()));
        let sub_count = fanout_plan.subscribers.subscribers.max(1) as usize;
        let request_bytes = build_subscribe_request(target, &fanout_plan);

        // Start subscribers first so they're ready when triggers fire.
        let sub_handles: Vec<_> = (0..sub_count)
            .map(|_| {
                let target = target.clone();
                let opts = opts.clone();
                let req = request_bytes.clone();
                let stop = Arc::clone(&stop);
                std::thread::Builder::new()
                    .name("zerobench-sse-fanout-sub".into())
                    .spawn(move || {
                        run_one_subscriber(&target, &opts, &req, wall_deadline, &stop)
                    })
                    .expect("spawn subscriber")
            })
            .collect();

        // Give subscribers a short head-start to connect + subscribe.
        std::thread::sleep(Duration::from_millis(100));

        // Trigger thread.
        let trigger_stop = Arc::clone(&stop);
        let trigger_triggers = Arc::clone(&triggers);
        let trigger_target = target.clone();
        let trigger_opts = opts.clone();
        let trigger_url_str = render_template(&trigger_url);
        let trigger_handle = std::thread::Builder::new()
            .name("zerobench-sse-fanout-trigger".into())
            .spawn(move || {
                run_trigger_loop(
                    &trigger_target,
                    &trigger_opts,
                    &trigger_url_str,
                    wall_deadline,
                    &trigger_stop,
                    &trigger_triggers,
                );
            })
            .expect("spawn trigger");

        let subs: Vec<SubscriberStats> = sub_handles
            .into_iter()
            .map(|h| h.join().expect("subscriber panicked"))
            .collect();
        let _ = trigger_handle.join();

        // Correlate events with triggers.
        let trigger_times = triggers.lock().expect("triggers mutex").clone();
        let mut rtt_hist: Histogram<u64> = new_hist();
        let mut total_events: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut errors_connect: u64 = 0;
        let mut errors_read: u64 = 0;
        let mut ttfb_hist: Histogram<u64> = new_hist();
        for s in &subs {
            total_events += s.events.len() as u64;
            total_bytes += s.bytes_received;
            errors_connect += s.errors_connect;
            errors_read += s.errors_read;
            if let Some(ttfb) = s.ttfb {
                let _ = ttfb_hist.record(duration_to_hist_ns(ttfb));
            }
            // For each trigger, the first event observed by this
            // subscriber at or after the trigger's send-instant is the
            // broadcast response. CONSUME that event after recording so
            // the next trigger matches a LATER event — otherwise a
            // slow-firing subscriber would map the same event to
            // multiple consecutive triggers and double-count.
            let mut ev_iter = s.events.iter().peekable();
            for &t_sent in &trigger_times {
                while let Some(&&ev) = ev_iter.peek() {
                    if ev >= t_sent {
                        let delta = duration_to_hist_ns(ev.saturating_duration_since(t_sent))
                            .clamp(HIST_LO_NS, HIST_HI_NS);
                        let _ = rtt_hist.record(delta);
                        ev_iter.next();
                        break;
                    }
                    ev_iter.next();
                }
            }
        }

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            sc.requests = total_events;
            task.requests = total_events;
            task.bytes_recv = total_bytes;
            sc.errors.connect = errors_connect;
            sc.errors.read = errors_read;
            task.errors.connect += errors_connect;
            task.errors.read += errors_read;
            *sc.sse_mut() = SseExtras {
                ttfb: ttfb_hist,
                chunk_gap: rtt_hist,
                chunks: total_events,
                streams_completed: trigger_times.len() as u64,
                bytes_received: total_bytes,
            };
        }
        out.push(task);
    }
    out
}

/// Fire HTTP POST triggers at `TRIGGER_INTERVAL_MS`, recording each
/// send instant into `triggers`.
fn run_trigger_loop(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &str,
    deadline: Instant,
    stop: &AtomicBool,
    triggers: &Mutex<Vec<Instant>>,
) {
    let interval = Duration::from_millis(TRIGGER_INTERVAL_MS);
    let mut next = Instant::now() + interval;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let now = Instant::now();
        if now < next {
            std::thread::sleep((next - now).min(Duration::from_millis(100)));
            continue;
        }
        let t = Instant::now();
        if fire_trigger(target, opts, trigger_url).is_ok() {
            triggers.lock().expect("triggers mutex").push(t);
        }
        next = Instant::now() + interval;
    }
}

/// Send one blocking HTTP POST to `trigger_url` and wait for any
/// response or EOF. Returns Ok on a completed exchange, Err otherwise.
fn fire_trigger(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &str,
) -> std::io::Result<()> {
    // Parse path from the trigger URL (assumes same target host/port).
    let path = match trigger_url.find("://").and_then(|i| trigger_url[i + 3..].find('/')) {
        Some(rel) => {
            let abs_idx = trigger_url.find("://").map(|i| i + 3).unwrap_or(0) + rel;
            &trigger_url[abs_idx..]
        }
        None => "/",
    };

    let addr = target
        .resolve(opts)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{e}")))?;
    let mut stream = std::net::TcpStream::connect_timeout(&addr, opts.connect_timeout)?;
    let _ = stream.set_nodelay(true);
    stream.set_read_timeout(Some(opts.request_timeout))?;
    stream.set_write_timeout(Some(opts.request_timeout))?;

    let host = if (target.tls && target.port == 443) || (!target.tls && target.port == 80) {
        target.host.clone()
    } else {
        format!("{}:{}", target.host, target.port)
    };
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    stream.write_all(req.as_bytes())?;
    let mut sink = [0u8; 512];
    loop {
        match stream.read(&mut sink) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Render a static template to a String. Fanout triggers can't use
/// per-iteration variables today (no scenario context at trigger time).
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

/// Build the subscribe request (re-uses hold's request format).
fn build_subscribe_request(
    target: &Target,
    plan: &SseFanoutPlan,
) -> Vec<u8> {
    let url = render_template(&plan.subscribers.url);
    let path = match url.find("://").and_then(|i| url[i + 3..].find('/').map(|j| i + 3 + j)) {
        Some(p) => url[p..].to_string(),
        None => "/".to_string(),
    };
    let host = if (target.tls && target.port == 443) || (!target.tls && target.port == 80) {
        target.host.clone()
    } else {
        format!("{}:{}", target.host, target.port)
    };
    format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    )
    .into_bytes()
}

/// Run one subscriber until deadline — mio single-conn state machine
/// that records event arrival instants.
fn run_one_subscriber(
    target: &Target,
    opts: &TransportOpts,
    request: &[u8],
    deadline: Instant,
    stop: &AtomicBool,
) -> SubscriberStats {
    let mut stats = SubscriberStats::new();
    let addr = match target.resolve(opts) {
        Ok(a) => a,
        Err(_) => {
            stats.errors_connect += 1;
            return stats;
        }
    };
    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(_) => {
            stats.errors_connect += 1;
            return stats;
        }
    };
    let mut events = Events::with_capacity(4);
    let mut tcp = match MioTcp::connect(addr) {
        Ok(s) => s,
        Err(_) => {
            stats.errors_connect += 1;
            return stats;
        }
    };
    let _ = tcp.set_nodelay(true);
    if poll
        .registry()
        .register(&mut tcp, POLL_TOKEN, Interest::READABLE | Interest::WRITABLE)
        .is_err()
    {
        stats.errors_connect += 1;
        return stats;
    }
    let mut stream = MioStream::Plain(tcp);

    // Wait for connect.
    if !wait_event(&mut poll, &mut events, opts.connect_timeout, |e| {
        e.is_writable() && e.token() == POLL_TOKEN
    }) {
        stats.errors_connect += 1;
        return stats;
    }

    // Write request.
    let mut write_pos = 0;
    while write_pos < request.len() {
        match stream.write(&request[write_pos..]) {
            Ok(0) => {
                stats.errors_connect += 1;
                return stats;
            }
            Ok(n) => write_pos += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if !wait_event(&mut poll, &mut events, Duration::from_secs(5), |e| {
                    e.is_writable() && e.token() == POLL_TOKEN
                }) {
                    stats.errors_connect += 1;
                    return stats;
                }
            }
            Err(_) => {
                stats.errors_connect += 1;
                return stats;
            }
        }
    }
    let t_sent = Instant::now();

    // Read loop with SSE parser.
    let mut pre_body: Vec<u8> = Vec::new();
    let mut header_done = false;
    let mut parser = SseLineParser::default();
    let mut decoder = crate::hold::ChunkDecoder::new();
    let mut buf = [0u8; 8192];
    let mut first_byte: Option<Instant> = None;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        match stream.read(&mut buf) {
            Ok(0) => return stats,
            Ok(n) => {
                let now = Instant::now();
                if first_byte.is_none() {
                    first_byte = Some(now);
                    stats.ttfb = Some(now.saturating_duration_since(t_sent));
                }
                stats.bytes_received = stats.bytes_received.saturating_add(n as u64);

                let slice = &buf[..n];
                let body_slice = if header_done {
                    slice
                } else {
                    pre_body.extend_from_slice(slice);
                    match memchr::memmem::find(&pre_body, b"\r\n\r\n").map(|p| p + 4) {
                        Some(hdr_end) => {
                            let grown_before = pre_body.len() - n;
                            let body_start_in_slice =
                                hdr_end.saturating_sub(grown_before);
                            header_done = true;
                            &slice[body_start_in_slice..]
                        }
                        None => continue,
                    }
                };

                let mut decoded: Vec<u8> = Vec::with_capacity(body_slice.len());
                let _ = decoder.decode(body_slice, &mut decoded);
                if !decoded.is_empty() {
                    let events_ref = &mut stats.events;
                    parser.feed(&decoded, |ev| match ev {
                        SseEvent::Data(_) => events_ref.push(Instant::now()),
                        SseEvent::Done | SseEvent::Ignored => {}
                    });
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if !wait_event(&mut poll, &mut events, Duration::from_millis(100), |_| true) {
                    continue;
                }
            }
            Err(_) => {
                stats.errors_read += 1;
                return stats;
            }
        }
    }
    stats
}

fn wait_event(
    poll: &mut Poll,
    events: &mut Events,
    timeout: Duration,
    pred: impl Fn(&mio::event::Event) -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        if poll.poll(events, Some(deadline - now)).is_err() {
            return false;
        }
        for ev in events.iter() {
            if pred(ev) {
                return true;
            }
        }
    }
}
