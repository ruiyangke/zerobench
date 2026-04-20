//! SSE broadcast-latency benchmark
//!
//! Trigger-side helpers live in [`crate::fanout_core`]. This file
//! holds only the SSE-specific subscriber logic + the `SseFanoutPlan`
//! handler.
//!
//! See `docs/design-v0.1.0.md` §3.2 `SseFanout` and `docs/PHILOSOPHY.md` §4.3.
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

use std::io::{Read, Write as _};
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
use crate::http::mio_tls::MioStream;
use zerobench_runtime::LiveSnapshot;

use crate::sse::line_parser::{SseEvent, SseLineParser};

const POLL_TOKEN: Token = Token(0);

/// Event arrival instant for one subscriber.
/// One inbound broadcast event observed by a subscriber.
///
/// `emit_ns` is the payload-embedded server timestamp parsed per
/// `FanoutMode::Timestamp { emit_field }`. `None` when the mode is
/// `TriggerRtt` (we don't scan the payload) or when the field is
/// missing / unparseable — Timestamp mode then falls back to the
/// trigger-RTT delta for that particular event.
#[derive(Debug, Clone, Copy)]
struct EventTime {
    received_at: Instant,
    emit_ns: Option<u64>,
}

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

#[allow(clippy::too_many_arguments)]
pub fn run_sse_fanout_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    // LiveSnapshot plumbing is accepted for API symmetry but not
    // populated yet — broadcast-RTT is computed post-hoc via
    // trigger/event correlation, and per-event live recording would
    // double-count because the fanout op-semantic is "one RTT per
    // trigger per subscriber", not "one op per event". Wiring this
    // cleanly is a follow-up; it doesn't fit the per-second window.
    _live: Option<Arc<LiveSnapshot>>,
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

        // Mode dispatch:
        //   - TriggerRtt: record each broadcast's received Instant
        //     and diff against the trigger's send Instant.
        //   - Timestamp: scan each payload for the server-supplied
        //     `emit_ns` field and diff against a wall-clock `now` at
        //     reception. Server and client must share a synced wall
        //     clock (NTP is usually good enough for the milliseconds
        //     regime this benchmark cares about).
        let emit_field: Option<String> = match &fanout_plan.mode {
            FanoutMode::TriggerRtt => None,
            FanoutMode::Timestamp { emit_field } => Some(emit_field.clone()),
        };
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
                let tls = tls_config.clone();
                let emit_field = emit_field.clone();
                std::thread::Builder::new()
                    .name("zerobench-sse-fanout-sub".into())
                    .spawn(move || {
                        run_one_subscriber(
                            &target,
                            &opts,
                            &req,
                            wall_deadline,
                            &stop,
                            tls.as_ref(),
                            emit_field.as_deref(),
                        )
                    })
                    .expect("spawn subscriber")
            })
            .collect();

        // Give subscribers a short head-start to connect + subscribe.
        std::thread::sleep(Duration::from_millis(100));

        // Trigger thread. Owns a Template (not a pre-rendered string)
        // so `{{counter}}` and `{{uuid}}` in the trigger URL advance
        // per firing — closing M2 from the audit. TLS config flows
        // through so https:// trigger URLs work.
        let trigger_stop = Arc::clone(&stop);
        let trigger_triggers = Arc::clone(&triggers);
        let trigger_target = target.clone();
        let trigger_opts = opts.clone();
        let trigger_url_tpl = trigger_url.clone();
        let trigger_tls = tls_config.clone();
        let trigger_handle = std::thread::Builder::new()
            .name("zerobench-sse-fanout-trigger".into())
            .spawn(move || {
                crate::fanout_core::run_trigger_loop(
                    &trigger_target,
                    &trigger_opts,
                    &trigger_url_tpl,
                    wall_deadline,
                    &trigger_stop,
                    &trigger_triggers,
                    trigger_tls.as_ref(),
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
            if emit_field.is_some() {
                // Timestamp mode: each event's RTT is
                //   (wall-clock-at-reception - server_emit_ns).
                // We approximate "wall-clock-at-reception" by
                // remembering that `ev.received_at` is monotonic
                // relative to this worker's wall-clock anchor
                // captured once per scenario below. Clock skew shows
                // up as a constant offset in the whole histogram —
                // percentile shapes are preserved.
                for ev in &s.events {
                    let Some(emit_ns) = ev.emit_ns else {
                        // Event payload didn't carry the configured
                        // `emit_ns` field — count as an error so the
                        // verdict surfaces server-side misconfiguration
                        // instead of silently dropping the sample.
                        errors_read = errors_read.saturating_add(1);
                        continue;
                    };
                    let now_unix_ns = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    // Derive the received-at-unix-ns from the
                    // monotonic Instant delta. Recording at record-
                    // time (instead of here) would be more accurate
                    // but is a bigger restructure; the delta is
                    // small because rollup runs immediately after the
                    // subscribers join.
                    let rx_monotonic_ago = ev.received_at.elapsed().as_nanos() as u64;
                    let rx_unix_ns = now_unix_ns.saturating_sub(rx_monotonic_ago);
                    let delta_ns = rx_unix_ns.saturating_sub(emit_ns);
                    let clamped = delta_ns.clamp(HIST_LO_NS, HIST_HI_NS);
                    let _ = rtt_hist.record(clamped);
                }
                continue;
            }

            // TriggerRtt mode (default): for each trigger, the first
            // event observed by this subscriber at or after the
            // trigger's send-instant is the broadcast response.
            // CONSUME that event after recording so the next trigger
            // matches a LATER event — otherwise a slow-firing
            // subscriber would map the same event to multiple
            // consecutive triggers and double-count.
            let mut ev_iter = s.events.iter().peekable();
            for &t_sent in &trigger_times {
                let mut matched = false;
                while let Some(&&ev) = ev_iter.peek() {
                    if ev.received_at >= t_sent {
                        let delta = duration_to_hist_ns(
                            ev.received_at.saturating_duration_since(t_sent),
                        )
                        .clamp(HIST_LO_NS, HIST_HI_NS);
                        let _ = rtt_hist.record(delta);
                        ev_iter.next();
                        matched = true;
                        break;
                    }
                    ev_iter.next();
                }
                if !matched {
                    // Trigger produced no broadcast for this subscriber
                    // (subscriber disconnected early, or broadcast was
                    // lost). Count as a read error so the verdict
                    // reflects missing data rather than silently
                    // dropping it — this shows up in errors.read on
                    // the scenario and in the exit code.
                    errors_read = errors_read.saturating_add(1);
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
                // chunk_gap is the "inter-event" slot used by
                // SseHold / SseReconnectStorm; leave empty here so
                // consumers don't conflate broadcast latency with
                // event pacing. broadcast_rtt is the dedicated
                // fanout slot.
                chunk_gap: new_hist(),
                chunks: total_events,
                streams_completed: trigger_times.len() as u64,
                bytes_received: total_bytes,
                broadcast_rtt: rtt_hist,
            };
        }
        out.push(task);
    }
    out
}

/// Build the subscribe request (re-uses hold's request format).
fn build_subscribe_request(
    target: &Target,
    plan: &SseFanoutPlan,
) -> Vec<u8> {
    let url = crate::fanout_core::render_template(&plan.subscribers.url);
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
// `emit_field` — Some iff `FanoutMode::Timestamp { emit_field }` was
// selected. Each inbound event's payload is scanned for
// `"<emit_field>":N` and the parsed N is carried into the post-run
// RTT pass.
#[allow(clippy::too_many_arguments)]
fn run_one_subscriber(
    target: &Target,
    opts: &TransportOpts,
    request: &[u8],
    deadline: Instant,
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
    emit_field: Option<&str>,
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

    // Wait for TCP connect.
    if !wait_event(&mut poll, &mut events, opts.connect_timeout, |e| {
        e.is_writable() && e.token() == POLL_TOKEN
    }) {
        stats.errors_connect += 1;
        return stats;
    }

    // Wrap in TLS when the target is https://. The MioStream enum
    // hides plain vs TLS from the rest of the subscriber loop.
    let mut stream = if target.tls {
        let Some(cfg) = tls_config else {
            stats.errors_connect += 1;
            return stats;
        };
        let sni = target.sni_name().to_string();
        let tls = match crate::http::mio_tls::MioTlsStream::new(tcp, Arc::clone(cfg), &sni) {
            Ok(t) => t,
            Err(_) => {
                stats.errors_connect += 1;
                return stats;
            }
        };
        let mut s = MioStream::Tls(tls);
        // Drive the handshake to completion with a bounded deadline.
        let hs_start = Instant::now();
        while s.is_handshaking() {
            if hs_start.elapsed() > Duration::from_secs(5) {
                stats.errors_connect += 1;
                return stats;
            }
            if s.drive_tls_io().is_err() {
                stats.errors_connect += 1;
                return stats;
            }
            if !s.is_handshaking() {
                break;
            }
            let _ = poll.poll(&mut events, Some(Duration::from_millis(200)));
        }
        s
    } else {
        MioStream::Plain(tcp)
    };

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
    let mut decoder = crate::sse::hold::ChunkDecoder::new();
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
                        SseEvent::Data(payload) => {
                            let emit_ns = emit_field.and_then(|f| {
                                zerobench_runtime::json_scan::find_json_u64_field(
                                    &payload,
                                    f.as_bytes(),
                                )
                            });
                            events_ref.push(EventTime {
                                received_at: Instant::now(),
                                emit_ns,
                            });
                        }
                        SseEvent::Done | SseEvent::Id(_) | SseEvent::Ignored => {}
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
