//! WebSocket broadcast-latency benchmark
//!
//! ARCH(fanout-core): HEAVY DUPLICATION with sse/fanout.rs. Extract:
//!   - run_trigger_loop    (mirror of sse/fanout::run_trigger_loop)
//!   - fire_http_trigger   (mirror of sse/fanout::fire_trigger)
//!   - render_template     (identical)
//!   - post-run trigger↔frame correlation pass
//! All four go to zerobench-backends::fanout_core. This file keeps only
//! WS-specific subscriber logic + the WsFanoutPlan handler.
//!
//! See docs/ARCH-REVIEW-2026-04-20.md §4.6, §B1, §7. — `docs/design-v0.1.0.md` §3.3
//! `WsFanout`.
//!
//! Same shape as `SseFanout`: N held WS subscribers + a periodic
//! external trigger. For each trigger firing we record the send
//! instant; subscribers record frame arrivals; post-hoc correlation
//! yields a broadcast-RTT distribution.
//!
//! Scope mirrors `SseFanout` — only `TriggerSpec::HttpPost` and
//! `FanoutMode::TriggerRtt` are wired today.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::SeedableRng;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist, HIST_HI_NS, HIST_LO_NS};
use zerobench_core::plan::{
    FanoutMode, Plan, Protocol, Step, TriggerSpec, WsFanoutPlan,
};
use zerobench_core::stats::{TaskStats, WsExtras};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::BenchRng;

use crate::ws::conn::{DataFrame, WsConnection};

const TRIGGER_INTERVAL_MS: u64 = 500;

/// One broadcast frame observed by a subscriber. `emit_ns` is
/// populated only under `FanoutMode::Timestamp` via the SSE crate's
/// payload scanner (re-exported for use here — WS broadcast payloads
/// typically carry JSON too).
#[derive(Debug, Clone, Copy)]
struct FrameTime {
    received_at: Instant,
    emit_ns: Option<u64>,
}

struct SubscriberStats {
    frames: Vec<FrameTime>,
    handshake: Option<Duration>,
    bytes_recv: u64,
    errors_connect: u64,
    errors_read: u64,
}

#[allow(clippy::too_many_arguments)]
pub fn run_ws_fanout_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    // See SseFanout for rationale on deferring live recording.
    _live: Option<Arc<zerobench_runtime::LiveSnapshot>>,
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
        if scenario.protocol() != Protocol::Ws {
            continue;
        }
        let fan: Option<WsFanoutPlan> = scenario.steps.iter().find_map(|s| match s {
            Step::WsFanout(p) => Some(p.clone()),
            _ => None,
        });
        let Some(fan) = fan else { continue };

        // Mode dispatch: TriggerRtt diffs against the trigger's
        // send instant; Timestamp parses a server-embedded
        // `emit_ns` field out of each broadcast payload and diffs
        // against wall-clock at reception. See sse/fanout.rs for the
        // extended rationale.
        let emit_field: Option<String> = match &fan.mode {
            FanoutMode::TriggerRtt => None,
            FanoutMode::Timestamp { emit_field } => Some(emit_field.clone()),
        };
        let trigger_url = match &fan.trigger {
            TriggerSpec::HttpPost { url, .. } => url.clone(),
            TriggerSpec::DedicatedWsConnection { .. } => {
                eprintln!(
                    "[ws_fanout] DedicatedWsConnection trigger not yet implemented; scenario skipped"
                );
                continue;
            }
        };

        let wall_deadline = Instant::now()
            + duration.min(if fan.subscribers.hold_for.is_zero() {
                duration
            } else {
                fan.subscribers.hold_for
            });
        let triggers: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::new()));
        let sub_count = fan.subscribers.connections.max(1) as usize;

        let path = extract_path(&fan.subscribers.url);
        let sub_handles: Vec<_> = (0..sub_count)
            .map(|_| {
                let target = target.clone();
                let opts = opts.clone();
                let stop = Arc::clone(&stop);
                let tls = tls_config.clone();
                let path = path.clone();
                let emit_field = emit_field.clone();
                std::thread::Builder::new()
                    .name("zerobench-ws-fanout-sub".into())
                    .spawn(move || {
                        run_one_subscriber(
                            &target,
                            &opts,
                            &path,
                            wall_deadline,
                            &stop,
                            tls.as_ref(),
                            emit_field.as_deref(),
                        )
                    })
                    .expect("spawn ws fanout subscriber")
            })
            .collect();

        std::thread::sleep(Duration::from_millis(100));

        let trigger_stop = Arc::clone(&stop);
        let trigger_triggers = Arc::clone(&triggers);
        let trigger_target = target.clone();
        let trigger_opts = opts.clone();
        let trigger_url_tpl = trigger_url.clone();
        let trigger_tls = tls_config.clone();
        let trigger_handle = std::thread::Builder::new()
            .name("zerobench-ws-fanout-trigger".into())
            .spawn(move || {
                run_trigger_loop(
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

        let trigger_times = triggers.lock().expect("triggers mutex").clone();
        let mut rtt_hist: Histogram<u64> = new_hist();
        let mut total_frames: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut errors_connect: u64 = 0;
        let mut errors_read: u64 = 0;
        let mut handshake_hist: Histogram<u64> = new_hist();
        for s in &subs {
            total_frames += s.frames.len() as u64;
            total_bytes += s.bytes_recv;
            errors_connect += s.errors_connect;
            errors_read += s.errors_read;
            if let Some(h) = s.handshake {
                let _ = handshake_hist.record(duration_to_hist_ns(h));
            }
            if emit_field.is_some() {
                // Timestamp mode — read server-embedded emit_ns per
                // frame. See sse/fanout.rs for the skew caveat.
                for f in &s.frames {
                    let Some(emit_ns) = f.emit_ns else {
                        errors_read = errors_read.saturating_add(1);
                        continue;
                    };
                    let now_unix_ns = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    let rx_ago = f.received_at.elapsed().as_nanos() as u64;
                    let rx_unix_ns = now_unix_ns.saturating_sub(rx_ago);
                    let delta = rx_unix_ns
                        .saturating_sub(emit_ns)
                        .clamp(HIST_LO_NS, HIST_HI_NS);
                    let _ = rtt_hist.record(delta);
                }
                continue;
            }

            // See the SseFanout version for the consume-after-match
            // rationale (prevents a single frame being credited to
            // multiple consecutive triggers).
            let mut frame_iter = s.frames.iter().peekable();
            for &t_sent in &trigger_times {
                let mut matched = false;
                while let Some(&&ev) = frame_iter.peek() {
                    if ev.received_at >= t_sent {
                        let delta = duration_to_hist_ns(
                            ev.received_at.saturating_duration_since(t_sent),
                        )
                        .clamp(HIST_LO_NS, HIST_HI_NS);
                        let _ = rtt_hist.record(delta);
                        frame_iter.next();
                        matched = true;
                        break;
                    }
                    frame_iter.next();
                }
                if !matched {
                    errors_read = errors_read.saturating_add(1);
                }
            }
        }

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            sc.requests = total_frames;
            task.requests = total_frames;
            task.bytes_recv = total_bytes;
            sc.errors.connect = errors_connect;
            sc.errors.read = errors_read;
            task.errors.connect += errors_connect;
            task.errors.read += errors_read;
            *sc.ws_mut() = WsExtras {
                handshake: handshake_hist,
                // rtt holds echo / inter-message gap for other WS
                // backends; fanout's broadcast latency lives in its
                // own slot so result.json readers never have to guess.
                rtt: new_hist(),
                messages_sent: 0,
                messages_recv: total_frames,
                bytes_sent: 0,
                bytes_recv: total_bytes,
                broadcast_rtt: rtt_hist,
            };
        }
        out.push(task);
    }
    out
}

// `emit_field` — Some iff the caller selected FanoutMode::Timestamp.
// Each broadcast frame's payload is scanned for the JSON field
// `"<emit_field>":N`; the parsed N lands in the per-frame record for
// the post-run RTT pass.
#[allow(clippy::too_many_arguments)]
fn run_one_subscriber(
    target: &Target,
    opts: &TransportOpts,
    path: &str,
    deadline: Instant,
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
    emit_field: Option<&str>,
) -> SubscriberStats {
    let mut stats = SubscriberStats {
        frames: Vec::new(),
        handshake: None,
        bytes_recv: 0,
        errors_connect: 0,
        errors_read: 0,
    };
    let handshake_start = Instant::now();
    let rng = BenchRng::seed_from_u64(handshake_start.elapsed().as_nanos() as u64);
    let mut conn = match WsConnection::connect(target, opts, path, &[], rng, tls_config) {
        Ok(c) => c,
        Err(_) => {
            stats.errors_connect += 1;
            return stats;
        }
    };
    stats.handshake = Some(handshake_start.elapsed());

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Bounded recv — must cap the wait so the deadline/stop
        // check fires even when the server pushes nothing between
        // triggers. Without this a low-broadcast-rate fanout would
        // sit inside WsConnection::recv forever (same C2 bug that
        // WsServerPushRtt had).
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(250));
        if remaining.is_zero() {
            break;
        }
        match conn.try_recv(remaining) {
            Ok(Some(DataFrame::Text(b))) | Ok(Some(DataFrame::Binary(b))) => {
                let emit_ns = emit_field.and_then(|f| {
                    zerobench_runtime::json_scan::find_json_u64_field(&b, f.as_bytes())
                });
                stats.frames.push(FrameTime {
                    received_at: Instant::now(),
                    emit_ns,
                });
                stats.bytes_recv = stats.bytes_recv.saturating_add(b.len() as u64);
            }
            Ok(Some(DataFrame::Pong(_))) => {
                // Spurious pong from a keep-alive ping we didn't
                // send — ignore. Fanout subscribers don't correlate
                // control frames.
            }
            Ok(None) => {}
            Err(crate::ws::conn::WsError::Closed { .. }) => {
                // Clean close from the server — the fanout subscriber's
                // session has ended. Not an error; the stats we have
                // are the stats we report.
                break;
            }
            Err(_) => {
                stats.errors_read += 1;
                return stats;
            }
        }
    }
    let _ = conn.close(1000, "bye");
    stats
}

fn run_trigger_loop(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &zerobench_core::Template,
    deadline: Instant,
    stop: &AtomicBool,
    triggers: &Mutex<Vec<Instant>>,
    tls_config: Option<&Arc<ClientConfig>>,
) {
    let interval = Duration::from_millis(TRIGGER_INTERVAL_MS);
    let mut next = Instant::now() + interval;
    let mut rng = zerobench_core::rng::from_entropy();
    let counter = std::rc::Rc::new(std::cell::Cell::new(0u64));
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let now = Instant::now();
        if now < next {
            std::thread::sleep((next - now).min(Duration::from_millis(100)));
            continue;
        }
        let mut url_buf: Vec<u8> = Vec::with_capacity(128);
        {
            let mut ctx = zerobench_core::ExpandCtx {
                rng: &mut rng,
                counter: &counter,
                scenario_vars: &[],
            };
            trigger_url.expand_into(&mut url_buf, &mut ctx);
        }
        let url_str = String::from_utf8_lossy(&url_buf).to_string();
        let t = Instant::now();
        if fire_http_trigger(target, opts, &url_str, tls_config).is_ok() {
            triggers.lock().expect("triggers mutex").push(t);
        }
        next = Instant::now() + interval;
    }
}

fn fire_http_trigger(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &str,
    tls_config: Option<&Arc<ClientConfig>>,
) -> std::io::Result<()> {
    // Delegated to crate::http::simple_post::fire_http_post — pure
    // mio, no blocking std::net::TcpStream on the client side. The
    // trigger URL typically points at the same host (an HTTP
    // /broadcast endpoint alongside the WS /subscribe). Cross-host
    // triggers aren't supported here yet (requires a second Target).
    let path = match trigger_url.find("://").and_then(|i| trigger_url[i + 3..].find('/')) {
        Some(rel) => {
            let abs_idx = trigger_url.find("://").map(|i| i + 3).unwrap_or(0) + rel;
            &trigger_url[abs_idx..]
        }
        None => "/",
    };
    crate::http::simple_post::fire_http_post(target, opts, path, &[], tls_config)
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

fn extract_path(url: &zerobench_core::Template) -> String {
    let s = render_template(url);
    if let Some(path_start) = s.find("://").and_then(|i| s[i + 3..].find('/').map(|j| i + 3 + j)) {
        s[path_start..].to_string()
    } else {
        "/".to_string()
    }
}
