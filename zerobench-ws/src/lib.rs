//! zerobench-ws — RFC 6455 WebSocket transport (mio/epoll, zero async).
//!
//! Synchronous, zero-async WebSocket benchmark runner. Each worker
//! thread opens a connection, performs the HTTP/1.1 Upgrade handshake,
//! then loops send->recv until the shared stop flag trips.
//!
//! # Architecture
//!
//! - [`frame`] — RFC 6455 §5.2 wire-format encoder/decoder.
//! - [`handshake`] — the HTTP/1.1 §4 Upgrade exchange + Accept-key
//!   validation.
//! - [`conn::WsConnection`] — one established connection (MioStream +
//!   recv buffer + per-connection mask CSPRNG).
//! - [`run_ws_threaded`] — N OS threads, each owning one connection,
//!   looping send->recv until the stop signal trips.
//!
//! # Fixing the v1 benchmark's bugs
//!
//! The reference implementation at `tools/bench/src/ws.rs` in the
//! zeroship repo had three known issues we correct here:
//!
//! 1. **Mask was a Weyl atomic counter, not CSPRNG**. RFC 6455 §10.3
//!    says "MUST be chosen from the set of allowed 32-bit values at
//!    random". We use [`BenchRng`](zerobench_core::rng::BenchRng)
//!    seeded from OS entropy.
//! 2. **Round-robin sequential per-connection handling**. v1 rotated
//!    through N slots in one task, so N connections were serviced
//!    one-at-a-time. We use thread-per-connection.
//! 3. **Byte-by-byte masking XOR**. Kept — for payloads typically
//!    <1 KiB the 4-byte-at-a-time variant is a micro-optimisation that
//!    isn't worth the complexity.

pub mod conn;
pub mod frame;
pub mod handshake;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use rand::Rng;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist};
use zerobench_core::live_snapshot::LiveSnapshot;
use zerobench_core::plan::{Plan, Protocol, Step, WsRoundPlan};
use zerobench_core::rng::from_entropy;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stats::{TaskStats, WsExtras};
use zerobench_core::transport::{Target, TargetError, TransportOpts};

pub use conn::{DataFrame, WsConnection, WsError};
pub use frame::{FrameHeader, Opcode};

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Per-worker WebSocket statistics. One instance per worker; merged into
/// a [`WsSummary`] at end-of-run.
#[derive(Debug, Clone)]
pub struct WsStats {
    /// Handshake time in nanoseconds — from TCP connect complete through
    /// 101 response received + Accept verified.
    pub handshake: Histogram<u64>,
    /// Per-message round-trip time in nanoseconds — from when we
    /// finished writing a frame to when we parsed the corresponding
    /// response frame.
    pub rtt: Histogram<u64>,
    /// Count of Text/Binary frames we sent.
    pub messages_sent: u64,
    /// Count of Text/Binary frames received.
    pub messages_recvd: u64,
    /// Total payload bytes sent (excluding frame headers).
    pub bytes_sent: u64,
    /// Total payload bytes received.
    pub bytes_recvd: u64,
    /// TCP connect / DNS failures.
    pub errors_connect: u64,
    /// Handshake protocol failures.
    pub errors_upgrade: u64,
    /// Read / write / framing errors mid-session.
    pub errors_io: u64,
    /// Errors emitting or processing a Close frame.
    pub errors_close: u64,
}

impl Default for WsStats {
    fn default() -> Self {
        Self::new()
    }
}

impl WsStats {
    /// Fresh stats bucket with empty histograms and zero counters.
    pub fn new() -> Self {
        Self {
            handshake: new_hist(),
            rtt: new_hist(),
            messages_sent: 0,
            messages_recvd: 0,
            bytes_sent: 0,
            bytes_recvd: 0,
            errors_connect: 0,
            errors_upgrade: 0,
            errors_io: 0,
            errors_close: 0,
        }
    }

    /// Record a handshake duration.
    pub fn record_handshake(&mut self, d: Duration) {
        let _ = self.handshake.record(duration_to_hist_ns(d));
    }

    /// Record a round-trip sample.
    pub fn record_rtt(&mut self, d: Duration) {
        let _ = self.rtt.record(duration_to_hist_ns(d));
    }

    /// Merge another stats bucket. Histograms add; counters sum.
    pub fn merge(&mut self, other: &Self) {
        let _ = self.handshake.add(&other.handshake);
        let _ = self.rtt.add(&other.rtt);
        self.messages_sent += other.messages_sent;
        self.messages_recvd += other.messages_recvd;
        self.bytes_sent += other.bytes_sent;
        self.bytes_recvd += other.bytes_recvd;
        self.errors_connect += other.errors_connect;
        self.errors_upgrade += other.errors_upgrade;
        self.errors_io += other.errors_io;
        self.errors_close += other.errors_close;
    }
}

/// End-of-run WebSocket summary — merged from all worker [`WsStats`].
#[derive(Debug, Clone)]
pub struct WsSummary {
    /// Combined handshake-time histogram.
    pub handshake: Histogram<u64>,
    /// Combined round-trip-time histogram.
    pub rtt: Histogram<u64>,
    /// Total messages sent across all workers.
    pub messages_sent: u64,
    /// Total messages received.
    pub messages_recvd: u64,
    /// Total payload bytes sent.
    pub bytes_sent: u64,
    /// Total payload bytes received.
    pub bytes_recvd: u64,
    /// Connect failures.
    pub errors_connect: u64,
    /// Handshake failures.
    pub errors_upgrade: u64,
    /// Mid-session IO / framing failures.
    pub errors_io: u64,
    /// Close-frame failures.
    pub errors_close: u64,
    /// Wall-clock duration of the benchmark (excluding warmup).
    pub duration: Duration,
}

impl WsSummary {
    /// Merge a vector of per-worker stats into a single summary.
    pub fn merge(stats: Vec<WsStats>, duration: Duration) -> Self {
        let mut out = WsSummary {
            handshake: new_hist(),
            rtt: new_hist(),
            messages_sent: 0,
            messages_recvd: 0,
            bytes_sent: 0,
            bytes_recvd: 0,
            errors_connect: 0,
            errors_upgrade: 0,
            errors_io: 0,
            errors_close: 0,
            duration,
        };
        for s in stats {
            let _ = out.handshake.add(&s.handshake);
            let _ = out.rtt.add(&s.rtt);
            out.messages_sent += s.messages_sent;
            out.messages_recvd += s.messages_recvd;
            out.bytes_sent += s.bytes_sent;
            out.bytes_recvd += s.bytes_recvd;
            out.errors_connect += s.errors_connect;
            out.errors_upgrade += s.errors_upgrade;
            out.errors_io += s.errors_io;
            out.errors_close += s.errors_close;
        }
        out
    }

    /// Messages per second (received) across the whole run.
    pub fn messages_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.messages_recvd as f64 / secs
        }
    }
}

// ---------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------

/// Everything the WebSocket runner needs, hoisted out of
/// [`zerobench_core::plan::Plan`] so we don't stretch the shared types
/// to accommodate protocol-specific knobs.
#[derive(Debug, Clone)]
pub struct WsPlan {
    /// TCP endpoint (host + port + TLS flag).
    pub target: Target,
    /// HTTP path to request in the Upgrade (e.g. `/echo`).
    pub path: String,
    /// Extra HTTP headers to include in the Upgrade request.
    pub headers: Vec<(String, String)>,
    /// Payload to send on each iteration.
    pub message: Bytes,
    /// Transport options — `insecure_tls`, `connect_timeout`, etc.
    pub opts: TransportOpts,
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Classify a handshake/connect error into the right stats counter.
fn classify_open_error(
    e: &WsError,
    stats: &mut WsStats,
    live: Option<&LiveSnapshot>,
) {
    match e {
        WsError::Io(_) => {
            stats.errors_connect += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Handshake(_) => {
            stats.errors_upgrade += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Tls(_) => {
            stats.errors_connect += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Frame(_) => {
            stats.errors_upgrade += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Closed { .. } => {
            stats.errors_upgrade += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
    }
}

/// Classify a mid-session error.
fn classify_io_error(_e: &WsError, stats: &mut WsStats, live: Option<&LiveSnapshot>) {
    stats.errors_io += 1;
    if let Some(l) = live {
        l.record_error(zerobench_core::stats::ErrorKind::Read);
    }
}

/// Close-path error.
fn classify_close_error(_e: &WsError, stats: &mut WsStats) {
    stats.errors_close += 1;
}

/// Run one worker's lifecycle end-to-end: connect, handshake, loop
/// send/recv until `stop` trips, optionally close.
fn run_worker(
    plan: &WsPlan,
    stop: &AtomicBool,
    live: Option<&Arc<LiveSnapshot>>,
    tls_config: Option<&Arc<ClientConfig>>,
) -> WsStats {
    let mut stats = WsStats::new();
    let rng = from_entropy();

    // --- Connect + Handshake ---
    let t_connect_start = Instant::now();
    let conn_result = WsConnection::connect(
        &plan.target,
        &plan.opts,
        &plan.path,
        &plan.headers,
        rng,
        tls_config,
    );
    let mut conn = match conn_result {
        Ok(c) => {
            stats.record_handshake(t_connect_start.elapsed());
            c
        }
        Err(e) => {
            classify_open_error(&e, &mut stats, live.map(|a| a.as_ref()));
            return stats;
        }
    };

    // --- Benchmark loop ---
    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();

        if let Err(e) = conn.send_text(&plan.message) {
            classify_io_error(&e, &mut stats, live.map(|a| a.as_ref()));
            return stats;
        }
        stats.messages_sent += 1;
        stats.bytes_sent += plan.message.len() as u64;

        let frame = match conn.recv() {
            Ok(f) => f,
            Err(WsError::Closed { code: _, reason: _ }) => {
                return stats;
            }
            Err(e) => {
                classify_io_error(&e, &mut stats, live.map(|a| a.as_ref()));
                return stats;
            }
        };

        let rtt = t0.elapsed();
        stats.record_rtt(rtt);
        stats.messages_recvd += 1;
        stats.bytes_recvd += frame.len() as u64;
    }

    // --- Close ---
    if let Err(e) = conn.close(1000, "") {
        classify_close_error(&e, &mut stats);
    }

    stats
}

/// Run a WebSocket benchmark with `num_workers` OS threads, each
/// owning one [`WsConnection`] and looping send->recv until `stop`
/// trips.
///
/// Per-worker stats are collected at end-of-run and returned for the
/// caller to merge via [`WsSummary::merge`].
///
/// `live`, when `Some`, receives per-message RTT samples so the TUI
/// and JSONL streamers update in real time.
pub fn run_ws_threaded(
    plan: WsPlan,
    num_workers: usize,
    stop: Arc<AtomicBool>,
    live: Option<Arc<LiveSnapshot>>,
    tls_config: Option<Arc<ClientConfig>>,
) -> Vec<WsStats> {
    if num_workers == 0 {
        return Vec::new();
    }

    let plan = Arc::new(plan);

    let handles: Vec<_> = (0..num_workers)
        .map(|_| {
            let plan = plan.clone();
            let stop = stop.clone();
            let live = live.clone();
            let tls_config = tls_config.clone();

            std::thread::spawn(move || {
                run_worker(&plan, &stop, live.as_ref(), tls_config.as_ref())
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().expect("WS worker panicked"))
        .collect()
}

// ---------------------------------------------------------------------------
// Multi-scenario (Plan-driven) runner
// ---------------------------------------------------------------------------

/// Precomputed per-scenario WS plan: the `scenario_id` the caller needs
/// for stats attribution, plus the same shape as [`WsPlan`] minus the
/// shared target/opts.
struct WsScenarioRun {
    scenario_id: u16,
    target: Target,
    path: String,
    headers: Vec<(String, String)>,
    message: Bytes,
}

/// Extract host/port/path from a URL string and build a [`Target`].
fn parse_ws_url(url: &str) -> Result<(Target, String), TargetError> {
    // Find the authority end — first `/`, `?`, or `#` after `://`.
    let after_scheme = url.find("://").map(|i| i + 3).unwrap_or(0);
    let authority_end = url[after_scheme..]
        .find(|c: char| c == '/' || c == '?' || c == '#')
        .map(|i| after_scheme + i)
        .unwrap_or(url.len());
    let authority = &url[..authority_end];
    let target = Target::parse(authority)?;

    let path = match url[authority_end..].find('#') {
        Some(i) => url[authority_end..][..i].to_string(),
        None => url[authority_end..].to_string(),
    };
    let path = if path.is_empty() { "/".to_string() } else { path };
    Ok((target, path))
}

/// Run one WS-scenario worker — per iteration, picks a random WS
/// scenario, opens a connection, sends one message, receives one,
/// records per-scenario stats, then closes.
///
/// A new connection is opened per round-trip (mirrors the one-shot
/// semantics of `Step::WsRound`: each iteration is one round). To
/// benchmark long-lived connections with many rounds, use the
/// single-plan `run_ws_threaded` path.
fn run_ws_worker_multi(
    scenarios: &[WsScenarioRun],
    opts: &TransportOpts,
    num_scenarios: usize,
    stop: &AtomicBool,
    tls_config: Option<Arc<ClientConfig>>,
) -> TaskStats {
    let mut task = TaskStats::new(num_scenarios);
    if scenarios.is_empty() {
        return task;
    }
    let mut rng = zerobench_core::rng::from_entropy();

    while !stop.load(Ordering::Relaxed) {
        let idx = if scenarios.len() == 1 {
            0
        } else {
            rng.gen_range(0..scenarios.len())
        };
        let run = &scenarios[idx];
        let sid = run.scenario_id as usize;
        let mask_rng = from_entropy();

        let t_open = Instant::now();
        let conn = WsConnection::connect(
            &run.target,
            opts,
            &run.path,
            &run.headers,
            mask_rng,
            tls_config.as_ref(),
        );
        let mut conn = match conn {
            Ok(c) => {
                let hs = t_open.elapsed();
                if let Some(sc) = task.per_scenario.get_mut(sid) {
                    let _ = sc.ws_mut().handshake.record(duration_to_hist_ns(hs));
                }
                c
            }
            Err(_) => {
                if let Some(sc) = task.per_scenario.get_mut(sid) {
                    sc.errors.incr(zerobench_core::ErrorKind::Connect);
                    task.errors.incr(zerobench_core::ErrorKind::Connect);
                }
                continue;
            }
        };

        // One round: send one message, await one reply. The round is
        // the Tier-1 "operation"; multi-round conversations aren't
        // expressible in `Step::WsRound` yet (Tier 2).
        let t_rt = Instant::now();
        if conn.send_text(&run.message).is_err() {
            if let Some(sc) = task.per_scenario.get_mut(sid) {
                sc.errors.incr(zerobench_core::ErrorKind::Write);
                task.errors.incr(zerobench_core::ErrorKind::Write);
            }
            let _ = conn.close(1000, "");
            continue;
        }
        let frame = match conn.recv() {
            Ok(f) => f,
            Err(_) => {
                if let Some(sc) = task.per_scenario.get_mut(sid) {
                    sc.errors.incr(zerobench_core::ErrorKind::Read);
                    task.errors.incr(zerobench_core::ErrorKind::Read);
                }
                let _ = conn.close(1000, "");
                continue;
            }
        };
        let rtt = t_rt.elapsed();

        // Update task + scenario stats for the completed round.
        let bytes_sent = run.message.len() as u64;
        let bytes_recv = frame.len() as u64;
        task.record(run.scenario_id, rtt, Duration::ZERO, bytes_sent, bytes_recv);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            let extras = sc.ws_mut();
            let _ = extras.rtt.record(duration_to_hist_ns(rtt));
            extras.messages_sent += 1;
            extras.messages_recv += 1;
            extras.bytes_sent += bytes_sent;
            extras.bytes_recv += bytes_recv;
        }

        // Close politely — errors here don't count against the round
        // we just completed.
        let _ = conn.close(1000, "");
    }

    task
}

/// Drive all WS scenarios declared in a multi-protocol `Plan` — the
/// surface the Tier-1 unified CLI dispatcher calls when a plan has
/// one or more `Step::WsRound` scenarios.
///
/// Returns per-worker [`TaskStats`] with `ws` extras populated per
/// scenario_id.
pub fn run_ws_from_plan_threaded(
    opts: &TransportOpts,
    plan: &Plan,
    num_workers: usize,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    stop_flag: Option<Arc<AtomicBool>>,
) -> Vec<TaskStats> {
    let num_scenarios = plan.scenarios.len();

    // Collect the static per-scenario runs. We compile each
    // `WsRoundPlan` template in-place — the Rhai DSL produces
    // templated URLs/headers/messages that need per-scenario
    // expansion. For Tier 1 we expand once against an empty context,
    // matching the existing `--ws` CLI shortcut (templates are
    // declarative; dynamic values would require per-iteration expand
    // which is Tier 2).
    let scenarios: Vec<WsScenarioRun> = plan
        .scenarios
        .iter()
        .enumerate()
        .filter_map(|(i, sc)| {
            if sc.protocol() != Protocol::Ws {
                return None;
            }
            sc.steps.iter().find_map(|step| match step {
                Step::WsRound(w) => Some(compile_ws_scenario(plan, i as u16, w)),
                _ => None,
            })
        })
        .collect();

    if scenarios.is_empty() {
        return (0..num_workers)
            .map(|_| TaskStats::new(num_scenarios))
            .collect();
    }

    let stop = stop_flag.unwrap_or_else(|| {
        let flag = Arc::new(AtomicBool::new(false));
        let timer = flag.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer.store(true, Ordering::Relaxed);
        });
        flag
    });

    let handles: Vec<_> = (0..num_workers)
        .map(|_| {
            let scenarios = scenarios
                .iter()
                .map(|s| WsScenarioRun {
                    scenario_id: s.scenario_id,
                    target: s.target.clone(),
                    path: s.path.clone(),
                    headers: s.headers.clone(),
                    message: s.message.clone(),
                })
                .collect::<Vec<_>>();
            let opts = opts.clone();
            let stop = stop.clone();
            let tls_config = tls_config.clone();
            std::thread::spawn(move || {
                run_ws_worker_multi(&scenarios, &opts, num_scenarios, &stop, tls_config)
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().expect("WS worker panicked"))
        .collect()
}

/// Compile one `WsRoundPlan` into a runnable `WsScenarioRun`.
///
/// Templates are expanded once against a fresh `ScenarioContext` seeded
/// from entropy; values that depend on per-iteration randomness
/// (`{{uuid}}`, `{{counter}}`) resolve to their first sampled value.
/// Per-round re-expansion is a Tier-2 feature; for Tier 1 it's
/// sufficient that literal URLs and static message bodies work.
fn compile_ws_scenario(plan: &Plan, scenario_id: u16, ws: &WsRoundPlan) -> WsScenarioRun {
    let mut ctx = ScenarioContext::new(plan.vars.len(), zerobench_core::rng::from_entropy());

    // Expand URL -> String -> (Target, path).
    let mut url_buf = Vec::with_capacity(128);
    ws.url.expand_into(&mut url_buf, &mut ctx.expand_ctx());
    let url_str = std::str::from_utf8(&url_buf).unwrap_or("/").to_string();
    let (target, path) = parse_ws_url(&url_str)
        .expect("WS scenario URL must parse into a valid Target");

    // Expand each header.
    let mut headers: Vec<(String, String)> = Vec::with_capacity(ws.headers.len());
    for (n, v) in &ws.headers {
        let mut nb = Vec::with_capacity(32);
        let mut vb = Vec::with_capacity(64);
        n.expand_into(&mut nb, &mut ctx.expand_ctx());
        v.expand_into(&mut vb, &mut ctx.expand_ctx());
        headers.push((
            std::str::from_utf8(&nb).unwrap_or("").to_string(),
            std::str::from_utf8(&vb).unwrap_or("").to_string(),
        ));
    }

    // Expand the outbound message.
    let mut mb = Vec::with_capacity(64);
    ws.message.expand_into(&mut mb, &mut ctx.expand_ctx());
    let message = Bytes::from(mb);

    // Keep a reference to the extras for the caller — we return the
    // compiled run, scenario_id retained so stats roll up cleanly.
    let _ = WsExtras::default(); // no-op; signals field wiring path

    WsScenarioRun {
        scenario_id,
        target,
        path,
        headers,
        message,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_merge_sums_counters() {
        let mut a = WsStats::new();
        a.messages_sent = 10;
        a.bytes_sent = 100;
        a.errors_io = 1;
        a.record_rtt(Duration::from_micros(500));

        let mut b = WsStats::new();
        b.messages_sent = 5;
        b.bytes_sent = 50;
        b.errors_connect = 2;
        b.record_rtt(Duration::from_micros(1_500));

        a.merge(&b);
        assert_eq!(a.messages_sent, 15);
        assert_eq!(a.bytes_sent, 150);
        assert_eq!(a.errors_io, 1);
        assert_eq!(a.errors_connect, 2);
        assert_eq!(a.rtt.len(), 2);
    }

    #[test]
    fn summary_merge_aggregates_correctly() {
        let mut s1 = WsStats::new();
        s1.messages_recvd = 10;
        s1.bytes_recvd = 100;
        s1.record_handshake(Duration::from_millis(5));
        s1.record_rtt(Duration::from_micros(500));

        let mut s2 = WsStats::new();
        s2.messages_recvd = 20;
        s2.bytes_recvd = 200;
        s2.record_handshake(Duration::from_millis(3));

        let sum = WsSummary::merge(vec![s1, s2], Duration::from_secs(2));
        assert_eq!(sum.messages_recvd, 30);
        assert_eq!(sum.bytes_recvd, 300);
        assert_eq!(sum.handshake.len(), 2);
        assert_eq!(sum.rtt.len(), 1);
        assert!((sum.messages_per_sec() - 15.0).abs() < 1e-9);
    }

    #[test]
    fn empty_summary_messages_per_sec_is_zero() {
        let sum = WsSummary::merge(Vec::new(), Duration::ZERO);
        assert_eq!(sum.messages_per_sec(), 0.0);
    }

    #[test]
    fn duration_to_hist_ns_clamps() {
        use zerobench_core::histogram::{HIST_HI_NS, HIST_LO_NS};
        assert_eq!(duration_to_hist_ns(Duration::from_nanos(0)), HIST_LO_NS);
        let huge = Duration::from_secs(999);
        assert_eq!(duration_to_hist_ns(huge), HIST_HI_NS);
        assert_eq!(
            duration_to_hist_ns(Duration::from_micros(500)),
            500_000,
        );
    }
}
