//! WebSocket broadcast-latency benchmark — `docs/design-v0.1.0.md` §3.3
//! `WsFanout`.
//!
//! Same shape as `SseFanout`: N held WS subscribers + a periodic
//! external trigger. For each trigger firing we record the send
//! instant; subscribers record frame arrivals; post-hoc correlation
//! yields a broadcast-RTT distribution.
//!
//! Scope mirrors `SseFanout` — only `TriggerSpec::HttpPost` and
//! `FanoutMode::TriggerRtt` are wired today.

use std::io::{Read, Write};
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

use crate::conn::{DataFrame, WsConnection, WsError};

const TRIGGER_INTERVAL_MS: u64 = 500;

struct SubscriberStats {
    frames: Vec<Instant>,
    handshake: Option<Duration>,
    bytes_recv: u64,
    errors_connect: u64,
    errors_read: u64,
}

pub fn run_ws_fanout_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
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

        if !matches!(fan.mode, FanoutMode::TriggerRtt) {
            eprintln!(
                "[ws_fanout] mode {:?} not yet implemented; falling back to TriggerRtt",
                fan.mode
            );
        }
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
                std::thread::Builder::new()
                    .name("zerobench-ws-fanout-sub".into())
                    .spawn(move || {
                        run_one_subscriber(&target, &opts, &path, wall_deadline, &stop, tls.as_ref())
                    })
                    .expect("spawn ws fanout subscriber")
            })
            .collect();

        std::thread::sleep(Duration::from_millis(100));

        let trigger_stop = Arc::clone(&stop);
        let trigger_triggers = Arc::clone(&triggers);
        let trigger_target = target.clone();
        let trigger_opts = opts.clone();
        let trigger_url_str = render_template(&trigger_url);
        let trigger_handle = std::thread::Builder::new()
            .name("zerobench-ws-fanout-trigger".into())
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
            // See the SseFanout version for the consume-after-match
            // rationale (prevents a single frame being credited to
            // multiple consecutive triggers).
            let mut frame_iter = s.frames.iter().peekable();
            for &t_sent in &trigger_times {
                while let Some(&&ev) = frame_iter.peek() {
                    if ev >= t_sent {
                        let delta = duration_to_hist_ns(ev.saturating_duration_since(t_sent))
                            .clamp(HIST_LO_NS, HIST_HI_NS);
                        let _ = rtt_hist.record(delta);
                        frame_iter.next();
                        break;
                    }
                    frame_iter.next();
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
                rtt: rtt_hist,
                messages_sent: 0,
                messages_recv: total_frames,
                bytes_sent: 0,
                bytes_recv: total_bytes,
            };
        }
        out.push(task);
    }
    out
}

fn run_one_subscriber(
    target: &Target,
    opts: &TransportOpts,
    path: &str,
    deadline: Instant,
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
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
        match conn.recv() {
            Ok(DataFrame::Text(b)) | Ok(DataFrame::Binary(b)) => {
                stats.frames.push(Instant::now());
                stats.bytes_recv = stats.bytes_recv.saturating_add(b.len() as u64);
            }
            Err(WsError::Io(e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // Loop
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
        if fire_http_trigger(target, opts, trigger_url).is_ok() {
            triggers.lock().expect("triggers mutex").push(t);
        }
        next = Instant::now() + interval;
    }
}

fn fire_http_trigger(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &str,
) -> std::io::Result<()> {
    // The trigger URL typically points at the same host (e.g. an HTTP
    // /broadcast endpoint alongside the WS /subscribe). For cross-host
    // triggers the user can set a different hostname in the URL.
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
