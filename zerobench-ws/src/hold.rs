//! WebSocket idle-capacity benchmark — `docs/PHILOSOPHY.md` §4.4 and
//! `docs/design-v0.1.0.md` §3.3 `WsHold`.
//!
//! Holds N persistent WebSocket connections open for `hold_for`,
//! sending a periodic heartbeat (Ping by default; app-level Text as a
//! fallback) so proxies don't evict the connections for idleness.
//! Primary metric: handshake latency distribution + conn-drop count.
//!
//! # Threading
//!
//! One OS thread per held connection. Low-overhead because each
//! thread only wakes on heartbeat or inbound frame arrival. For
//! ≥10k connections a future rewrite should multiplex all connections
//! on one mio::Poll per runner (mirroring the SSE `hold` design);
//! today's cap is ~2k connections before OS-thread overhead dominates.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::SeedableRng;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist};
use zerobench_core::plan::{HeartbeatFrame, Plan, Protocol, Step, WsHoldPlan};
use zerobench_core::stats::{TaskStats, WsExtras};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::{BenchRng, LiveSnapshot};

use crate::conn::WsConnection;

/// Per-connection rollup.
#[derive(Debug, Clone)]
struct HoldStats {
    handshake: Option<Duration>,
    bytes_sent: u64,
    bytes_recv: u64,
    heartbeats_sent: u64,
    frames_recv: u64,
    errors_connect: u64,
    errors_read: u64,
    errors_write: u64,
    /// True if the connection stayed alive through the whole hold
    /// window; false if it dropped / errored early.
    completed: bool,
}

impl HoldStats {
    fn new() -> Self {
        Self {
            handshake: None,
            bytes_sent: 0,
            bytes_recv: 0,
            heartbeats_sent: 0,
            frames_recv: 0,
            errors_connect: 0,
            errors_read: 0,
            errors_write: 0,
            completed: false,
        }
    }
}

/// Run one connection — connect, heartbeat loop, close on deadline.
#[allow(clippy::too_many_arguments)]
fn run_one_hold(
    target: &Target,
    opts: &TransportOpts,
    plan: &WsHoldPlan,
    deadline: Instant,
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
    live: Option<&LiveSnapshot>,
    scenario_id: u16,
) -> HoldStats {
    let mut stats = HoldStats::new();
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

    // M8: heartbeat == 0 is interpreted as "use the default" rather
    // than "never heartbeat" because a WsHold without heartbeats gets
    // dropped by proxy idle timeouts within ~30-60s, which poisons
    // the measurement the verb exists to produce. If a user actually
    // wants zero heartbeats they should pick a very long interval
    // (e.g. 9999s) so the intent is explicit in the archived plan.
    let heartbeat_interval = if plan.heartbeat.is_zero() {
        Duration::from_secs(25)
    } else {
        plan.heartbeat
    };
    let heartbeat_payload = b"zb-hb".to_vec();
    let mut next_hb = Instant::now() + heartbeat_interval;

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let now = Instant::now();
        let sleep_until = next_hb.min(deadline);
        if sleep_until > now {
            // Between heartbeats we call the bounded try_recv instead
            // of sleeping. Two reasons:
            // 1. WsConnection::try_recv transparently auto-pongs
            //    inbound Pings — required so RFC 6455 servers that
            //    expect timely Pong responses don't drop the
            //    connection. Plain sleep would starve the control
            //    frame path entirely.
            // 2. Pure blocking sleep was the previous behaviour and
            //    broke the hold semantics (the whole point of
            //    WsHold is idle-capacity: if servers drop us due to
            //    our own missed Pongs the measurement is poisoned).
            let nap = (sleep_until - now).min(Duration::from_millis(500));
            match conn.try_recv(nap) {
                Ok(Some(frame)) => {
                    stats.frames_recv += 1;
                    let blen = frame.len() as u64;
                    stats.bytes_recv = stats.bytes_recv.saturating_add(blen);
                    if let Some(live) = live {
                        // Op-count semantic for WsHold: each server-
                        // side frame is a unit of work the server
                        // delivered to us. Latency slot carries 0 —
                        // there's no meaningful RTT axis for hold.
                        live.record(1, 0, blen);
                        live.record_scenario(scenario_id, 1, 0, blen);
                    }
                }
                Ok(None) => {}
                Err(crate::conn::WsError::Closed { .. }) => {
                    // Clean RFC 6455 Close handshake from the server —
                    // WsHold's definition of "held" ends here. Not an
                    // error; exit the inner loop so the worker returns
                    // the stats it has.
                    return stats;
                }
                Err(_) => {
                    stats.errors_read += 1;
                    return stats;
                }
            }
            continue;
        }
        let send_result = match plan.heartbeat_frame {
            HeartbeatFrame::Ping => conn.send_ping(&heartbeat_payload),
            HeartbeatFrame::TextApp => conn.send_text(&heartbeat_payload),
        };
        match send_result {
            Ok(()) => {
                stats.heartbeats_sent += 1;
                stats.bytes_sent =
                    stats.bytes_sent.saturating_add(heartbeat_payload.len() as u64);
            }
            Err(_) => {
                stats.errors_write += 1;
                return stats;
            }
        }
        next_hb = Instant::now() + heartbeat_interval;
    }

    stats.completed = true;
    let _ = conn.close(1000, "bye");
    stats
}

/// Drive all `WsHold` scenarios in `plan`.
#[allow(clippy::too_many_arguments)]
pub fn run_ws_hold_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<Arc<LiveSnapshot>>,
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
        let hold_plan = scenario.steps.iter().find_map(|s| match s {
            Step::WsHold(p) => Some(p.clone()),
            _ => None,
        });
        let Some(hold_plan) = hold_plan else { continue };

        let wall_deadline = Instant::now()
            + duration.min(if hold_plan.hold_for.is_zero() {
                duration
            } else {
                hold_plan.hold_for
            });

        let handles: Vec<_> = (0..hold_plan.connections.max(1))
            .map(|_| {
                let target = target.clone();
                let opts = opts.clone();
                let plan = hold_plan.clone();
                let stop = Arc::clone(&stop);
                let tls = tls_config.clone();
                let sid_u16 = sid as u16;
                let live = live.clone();
                std::thread::Builder::new()
                    .name("zerobench-ws-hold".into())
                    .spawn(move || {
                        run_one_hold(
                            &target,
                            &opts,
                            &plan,
                            wall_deadline,
                            &stop,
                            tls.as_ref(),
                            live.as_deref(),
                            sid_u16,
                        )
                    })
                    .expect("spawn ws-hold worker")
            })
            .collect();

        let mut rollup_handshake: Histogram<u64> = new_hist();
        let mut total_sent: u64 = 0;
        let mut total_recv: u64 = 0;
        let mut total_frames: u64 = 0;
        let mut errors_connect: u64 = 0;
        let mut errors_read: u64 = 0;
        let mut errors_write: u64 = 0;
        let mut completed: u64 = 0;
        for h in handles {
            let s = h.join().expect("ws-hold worker panicked");
            if let Some(hs) = s.handshake {
                let _ = rollup_handshake.record(duration_to_hist_ns(hs));
            }
            total_sent += s.bytes_sent;
            total_recv += s.bytes_recv;
            total_frames += s.frames_recv;
            errors_connect += s.errors_connect;
            errors_read += s.errors_read;
            errors_write += s.errors_write;
            if s.completed {
                completed += 1;
            }
        }

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            // For WsHold the meaningful "op count" is held-seconds, but we
            // don't track that here — use completed-connections as a proxy.
            sc.requests = completed;
            task.requests = completed;
            task.bytes_sent = total_sent;
            task.bytes_recv = total_recv;
            sc.errors.connect = errors_connect;
            sc.errors.read = errors_read;
            sc.errors.write = errors_write;
            task.errors.connect += errors_connect;
            task.errors.read += errors_read;
            task.errors.write += errors_write;
            *sc.ws_mut() = WsExtras {
                handshake: rollup_handshake,
                rtt: new_hist(),
                messages_sent: 0,
                messages_recv: total_frames,
                bytes_sent: total_sent,
                bytes_recv: total_recv,
                broadcast_rtt: new_hist(),
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
