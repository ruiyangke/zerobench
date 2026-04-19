//! WebSocket server-push RTT benchmark — `docs/design-v0.1.0.md` §3.3
//! `WsServerPushRtt`.
//!
//! Open N persistent connections and read inbound text/binary frames
//! only; record the inter-message arrival gap as the primary latency
//! axis. No correlation logic — the receiver assumes every data frame
//! is a push to count.
//!
//! # Stall detection
//!
//! If `expected_rate_per_conn > 0.0`, the scenario flags a stall when
//! the observed-rate falls below half the expected rate for a whole
//! connection. Stalls increment `errors.read` on the scenario.
//!
//! # Threading
//!
//! One OS thread per connection. Same scaling caveat as `hold.rs`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::SeedableRng;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist, HIST_HI_NS, HIST_LO_NS};
use zerobench_core::plan::{Plan, Protocol, Step, WsServerPushRttPlan};
use zerobench_core::stats::{TaskStats, WsExtras};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::BenchRng;

use crate::conn::{DataFrame, WsConnection};

/// Per-connection rollup.
#[derive(Debug, Clone)]
struct PushStats {
    handshake: Option<Duration>,
    gap: Histogram<u64>,
    messages_recv: u64,
    bytes_recv: u64,
    errors_connect: u64,
    errors_read: u64,
    stalled: bool,
}

impl PushStats {
    fn new() -> Self {
        Self {
            handshake: None,
            gap: new_hist(),
            messages_recv: 0,
            bytes_recv: 0,
            errors_connect: 0,
            errors_read: 0,
            stalled: false,
        }
    }
}

fn run_one_push(
    target: &Target,
    opts: &TransportOpts,
    plan: &WsServerPushRttPlan,
    deadline: Instant,
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
) -> PushStats {
    let mut stats = PushStats::new();
    let path = extract_path(&plan.url);
    let handshake_start = Instant::now();
    let rng = BenchRng::seed_from_u64(handshake_start.elapsed().as_nanos() as u64);
    let mut conn = match WsConnection::connect(target, opts, &path, &[], rng, tls_config) {
        Ok(c) => c,
        Err(_) => {
            stats.errors_connect += 1;
            return stats;
        }
    };
    stats.handshake = Some(handshake_start.elapsed());

    let mut last_at: Option<Instant> = None;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Bounded recv — must cap the wait so the deadline/stop check
        // above fires even when the server pushes nothing. Without
        // this, a low-rate push scenario would sit inside recv()
        // forever because WsConnection::recv loops internally on
        // WouldBlock with no external timeout.
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(250));
        if remaining.is_zero() {
            break;
        }
        match conn.try_recv(remaining) {
            Ok(Some(DataFrame::Text(b))) | Ok(Some(DataFrame::Binary(b))) => {
                let now = Instant::now();
                if let Some(prev) = last_at {
                    let gap =
                        duration_to_hist_ns(now.saturating_duration_since(prev))
                            .clamp(HIST_LO_NS, HIST_HI_NS);
                    let _ = stats.gap.record(gap);
                }
                last_at = Some(now);
                stats.messages_recv += 1;
                stats.bytes_recv = stats.bytes_recv.saturating_add(b.len() as u64);
            }
            Ok(None) => {
                // Timeout — loop back to check deadline/stop.
            }
            Err(_) => {
                stats.errors_read += 1;
                return stats;
            }
        }
    }

    // Stall detection vs. expected rate.
    if plan.expected_rate_per_conn > 0.0 && !plan.hold_for.is_zero() {
        let elapsed_s = plan.hold_for.as_secs_f64().max(f64::EPSILON);
        let observed_rate = stats.messages_recv as f64 / elapsed_s;
        if observed_rate < plan.expected_rate_per_conn * 0.5 {
            stats.stalled = true;
        }
    }
    let _ = conn.close(1000, "bye");
    stats
}

pub fn run_ws_server_push_rtt_from_plan_threaded(
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
        let push_plan = scenario.steps.iter().find_map(|s| match s {
            Step::WsServerPushRtt(p) => Some(p.clone()),
            _ => None,
        });
        let Some(push_plan) = push_plan else { continue };

        let wall_deadline = Instant::now()
            + duration.min(if push_plan.hold_for.is_zero() {
                duration
            } else {
                push_plan.hold_for
            });

        let handles: Vec<_> = (0..push_plan.connections.max(1))
            .map(|_| {
                let target = target.clone();
                let opts = opts.clone();
                let plan = push_plan.clone();
                let stop = Arc::clone(&stop);
                let tls = tls_config.clone();
                std::thread::Builder::new()
                    .name("zerobench-ws-push".into())
                    .spawn(move || {
                        run_one_push(&target, &opts, &plan, wall_deadline, &stop, tls.as_ref())
                    })
                    .expect("spawn ws-push worker")
            })
            .collect();

        let mut rollup_handshake: Histogram<u64> = new_hist();
        let mut rollup_gap: Histogram<u64> = new_hist();
        let mut total_recv: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut errors_connect: u64 = 0;
        let mut errors_read: u64 = 0;
        let mut stalls: u64 = 0;
        for h in handles {
            let s = h.join().expect("ws-push worker panicked");
            if let Some(hs) = s.handshake {
                let _ = rollup_handshake.record(duration_to_hist_ns(hs));
            }
            let _ = rollup_gap.add(&s.gap);
            total_recv += s.messages_recv;
            total_bytes += s.bytes_recv;
            errors_connect += s.errors_connect;
            errors_read += s.errors_read;
            if s.stalled {
                stalls += 1;
            }
        }

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            sc.requests = total_recv;
            task.requests = total_recv;
            task.bytes_recv = total_bytes;
            sc.errors.connect = errors_connect;
            sc.errors.read = errors_read + stalls;
            task.errors.connect += errors_connect;
            task.errors.read += errors_read + stalls;
            *sc.ws_mut() = WsExtras {
                handshake: rollup_handshake,
                rtt: rollup_gap, // gap histogram lives in the rtt slot
                messages_sent: 0,
                messages_recv: total_recv,
                bytes_sent: 0,
                bytes_recv: total_bytes,
            };
        }
        out.push(task);
    }
    out
}

fn extract_path(url: &zerobench_core::Template) -> String {
    let mut buf = Vec::with_capacity(256);
    let mut rng = zerobench_core::rng::from_entropy();
    let mut ctx = zerobench_core::ExpandCtx {
        rng: &mut rng,
        counter: &std::rc::Rc::new(std::cell::Cell::new(0)),
        scenario_vars: &[],
    };
    url.expand_into(&mut buf, &mut ctx);
    let s = String::from_utf8_lossy(&buf).to_string();
    if let Some(path_start) = s.find("://").and_then(|i| s[i + 3..].find('/').map(|j| i + 3 + j)) {
        s[path_start..].to_string()
    } else {
        "/".to_string()
    }
}
