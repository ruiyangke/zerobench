//! Open-loop rate scheduling.
//!
//! A [`Token`] is a timestamped authorization to fire one scenario
//! iteration. A per-scenario scheduler task emits tokens at the rate
//! specified by the scenario's [`RateProfile`]; a pool of worker tasks
//! drains the shared channel and executes the iteration.
//!
//! # Coordinated-omission-free latency
//!
//! Workers record latency against `now - token.intended_start` rather
//! than `now - send_start`. Under backpressure — e.g. a slow 10th
//! response blocks the worker and the scheduler queues more tokens —
//! later tokens see their queue time folded into the latency sample,
//! which is what CO-free measurement means.
//!
//! # Keepup accounting
//!
//! When every worker is busy and the channel fills, the scheduler's
//! `try_send` returns `Full`. We increment a shared `KeepupCounter`
//! (atomic) and continue. At end-of-run the dispatcher folds the
//! keepup count into `Summary::errors.keepup`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::Rng;

use crate::live_snapshot::LiveSnapshot;
use crate::plan::{Plan, RateProfile, Step};
use crate::rng::from_entropy;
use crate::scenario_context::ScenarioContext;
use crate::stats::{ErrorKind, TaskStats};
use crate::step_exec::{
    classify_transport_error, process_response, record_transport_error_live,
};
use crate::stop::StopSignal;
use crate::transport::{Response, Transport};

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

/// A unit of work emitted by a rate scheduler.
#[derive(Debug, Clone, Copy)]
pub struct Token {
    /// Which scenario in the plan to execute.
    pub scenario_id: u16,
    /// The wall-clock time at which this token *should* have been
    /// emitted. Workers compute latency as `now - intended_start` so
    /// backpressure shows up in the numbers.
    pub intended_start: Instant,
}

// ---------------------------------------------------------------------------
// Keepup counter
// ---------------------------------------------------------------------------

/// Shared atomic counter for "token dropped because workers couldn't
/// keep up". Clone the inner `Arc` to hand it to schedulers; read via
/// [`KeepupCounter::take`] at end-of-run.
#[derive(Clone, Debug, Default)]
pub struct KeepupCounter {
    inner: Arc<AtomicU64>,
}

impl KeepupCounter {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn inc(&self) {
        self.inner.fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.inner.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Scheduler — Constant rate
// ---------------------------------------------------------------------------

/// Drive one scenario's token emission until `stop` fires.
///
/// Precise scheduling: intended times are computed as
/// `started_at + i / rps`, not `Instant::now()`. If emission drifts
/// behind (worker starvation, slow schedulers, etc.), we catch up by
/// emitting multiple tokens back-to-back rather than waiting — which
/// is what keeps the target rate honest.
///
/// `live` is the optional shared [`LiveSnapshot`] used by the JSONL
/// streaming path; when present, each token dropped due to channel
/// saturation increments `errors.keepup` on the next tick so real-time
/// monitors see backpressure when it happens, not just at end-of-run.
pub async fn run_scheduler(
    scenario_id: u16,
    profile: RateProfile,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
    live: Option<Arc<LiveSnapshot>>,
) {
    match profile {
        RateProfile::Constant(rps) => {
            run_constant(scenario_id, rps, sender, started_at, stop, keepup, live).await
        }
        RateProfile::Ramp { from, to, over } => {
            run_ramp(
                scenario_id,
                from,
                to,
                over,
                sender,
                started_at,
                stop,
                keepup,
                live,
            )
            .await
        }
        RateProfile::Stepped(steps) => {
            run_stepped(scenario_id, steps, sender, started_at, stop, keepup, live).await
        }
        RateProfile::Saturate { .. } => {
            // Saturate mode doesn't use the scheduler — the dispatcher
            // short-circuits to run_saturate. If we somehow land here,
            // just idle until stop.
            while !stop.is_stopped() {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Bump the keepup counter and, if a `LiveSnapshot` is attached, record
/// a per-window `ErrorKind::Keepup` so the next JSONL tick reflects the
/// drop. Called from every scheduler on `TrySendError::Full`.
fn record_keepup_drop(keepup: &KeepupCounter, live: Option<&Arc<LiveSnapshot>>) {
    keepup.inc();
    if let Some(l) = live {
        l.record_error(ErrorKind::Keepup);
    }
}

async fn run_constant(
    scenario_id: u16,
    rps: f64,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
    live: Option<Arc<LiveSnapshot>>,
) {
    if rps <= 0.0 {
        return;
    }
    let period_ns = (1e9 / rps) as u64;
    let period = Duration::from_nanos(period_ns.max(1));

    let mut i: u64 = 0;
    while !stop.is_stopped() {
        let intended = started_at + period * (i as u32);
        // Sleep until the intended emission time. If we're already
        // behind (catch-up), sleep_until returns immediately.
        compio::time::sleep_until(intended).await;
        match sender.try_send(Token {
            scenario_id,
            intended_start: intended,
        }) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(_)) => {
                record_keepup_drop(&keepup, live.as_ref());
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                break;
            }
        }
        i = i.wrapping_add(1);
    }
}

async fn run_ramp(
    scenario_id: u16,
    from: f64,
    to: f64,
    over: Duration,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
    live: Option<Arc<LiveSnapshot>>,
) {
    // r(t) = from + (to-from)*t/over. Integrate to get
    // N(t) = from*t + (to-from)*t²/(2*over) during the ramp, and
    // N(t) = N(over) + to*(t-over) after.
    //
    // To emit token `n` at its exact intended time, we need `t` such
    // that `N(t) = n`. Sleep until that `t`, then enqueue. This gives
    // CO-free latency measurement with no polling jitter.
    let over_s = over.as_secs_f64().max(1e-9);
    let mut i: u64 = 0;
    while !stop.is_stopped() {
        let intended_offset = intended_offset_for(from, to, over_s, i as f64);
        let intended = started_at + Duration::from_secs_f64(intended_offset.max(0.0));
        // Sleep until the intended emission time. If we're already
        // behind (catch-up), sleep_until returns immediately.
        compio::time::sleep_until(intended).await;

        match sender.try_send(Token {
            scenario_id,
            intended_start: intended,
        }) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(_)) => {
                record_keepup_drop(&keepup, live.as_ref());
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                break;
            }
        }
        i = i.wrapping_add(1);
    }
}

/// Inverse of the cumulative-count function `N(t)` for a linear ramp
/// `from → to` over `over_s` seconds. Returns the offset (in seconds
/// from start) at which the `n`-th token should be emitted.
///
/// # Math
///
/// During the ramp (t ≤ over_s):
///   N(t) = from·t + (to-from)·t²/(2·over_s) = n
///   → a·t² + b·t − n = 0, where a = (to-from)/(2·over_s), b = from.
///   Positive root: t = (−b + √(b² + 4an)) / (2a).
///
/// After the ramp (t > over_s), the rate is constant at `to`:
///   N(t) = N(over_s) + to·(t − over_s)
///   → t = over_s + (n − N(over_s)) / to.
///
/// # Edge cases
///
/// - `from == to` (degenerate ramp): fall back to the linear case
///   `t = n / from`.
/// - `from == 0 && to == 0`: no tokens; returns 0 (scheduler won't
///   emit because the `target_count` stays 0).
/// - `over_s` is guaranteed ≥ 1e-9 by the caller.
fn intended_offset_for(from: f64, to: f64, over_s: f64, n: f64) -> f64 {
    // Count emitted by the end of the ramp phase.
    let n_end = (from + to) * over_s / 2.0;

    // After-ramp: constant rate `to`.
    if n > n_end {
        if to <= 0.0 {
            return over_s;
        }
        return over_s + (n - n_end) / to;
    }

    // Degenerate: constant rate throughout — skip the quadratic.
    if (to - from).abs() < f64::EPSILON {
        if from <= 0.0 {
            return 0.0;
        }
        return n / from;
    }

    // Ramp phase: solve the quadratic (to-from)/(2*over_s)·t² + from·t − n = 0.
    let a = (to - from) / (2.0 * over_s);
    let b = from;
    // disc = b² + 4·a·n (since we're solving a·t² + b·t − n = 0,
    // the discriminant is b² − 4·a·(−n) = b² + 4an).
    let disc = b * b + 4.0 * a * n;
    let disc_sqrt = disc.max(0.0).sqrt();
    // Positive root. When a > 0 (ramp up), this is `(-b + sqrt)/2a`.
    // When a < 0 (ramp down), the same formula gives the relevant root
    // in the `[0, over_s]` domain because disc ≥ 0 and n ≤ n_end.
    (-b + disc_sqrt) / (2.0 * a)
}

async fn run_stepped(
    scenario_id: u16,
    mut steps: Vec<(Duration, f64)>,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
    live: Option<Arc<LiveSnapshot>>,
) {
    steps.sort_by_key(|(t, _)| *t);
    // Walk the step schedule. Between steps, behave like `run_constant`.
    let mut cursor = 0usize;
    let mut current_rps: f64 = 0.0;
    let mut segment_index: u64 = 0;
    let mut segment_start = started_at;

    while !stop.is_stopped() {
        // Advance the step cursor.
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(started_at);
        while cursor < steps.len() && steps[cursor].0 <= elapsed {
            current_rps = steps[cursor].1;
            cursor += 1;
            segment_index = 0;
            segment_start = now;
        }
        if current_rps <= 0.0 {
            // Idle until the next step or stop.
            let next = steps.get(cursor).map(|(t, _)| started_at + *t);
            let wait = match next {
                Some(n) => n.saturating_duration_since(Instant::now()),
                None => Duration::from_millis(50),
            };
            let wait = wait.min(Duration::from_millis(50));
            compio::time::sleep(wait).await;
            continue;
        }
        let period = Duration::from_nanos((1e9 / current_rps) as u64).max(Duration::from_nanos(1));
        let intended = segment_start + period * (segment_index as u32);
        let now = Instant::now();
        if intended > now {
            compio::time::sleep(intended - now).await;
        }
        match sender.try_send(Token {
            scenario_id,
            intended_start: intended,
        }) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(_)) => {
                record_keepup_drop(&keepup, live.as_ref());
            }
            Err(flume::TrySendError::Disconnected(_)) => break,
        }
        segment_index = segment_index.wrapping_add(1);
    }
}

// ---------------------------------------------------------------------------
// Open-loop dispatcher
// ---------------------------------------------------------------------------

/// Run an open-loop benchmark.
///
/// For each scenario with a non-`Saturate` rate profile, spawn a
/// scheduler task. Spawn `max_conns` worker tasks that pull tokens
/// from a shared bounded MPMC channel. Workers execute the scenario
/// and record latency as `now - token.intended_start` (CO-free).
///
/// Scenarios whose profile is `Saturate` are skipped in open-loop mode
/// — the caller is expected to route those to [`run_saturate`].
pub async fn run_open_loop<T: Transport>(
    plan: &Plan,
    client: T::Client,
    max_conns: usize,
    stop: StopSignal,
    live: Option<Arc<LiveSnapshot>>,
) -> Vec<TaskStats> {
    if plan.scenarios.is_empty() || max_conns == 0 {
        return Vec::new();
    }

    let plan = Arc::new(plan.clone());
    let num_scenarios = plan.scenarios.len();

    // Bounded channel — capacity = 4 * max_conns. The rationale: we want
    // enough slack that a sudden batch of intended-start times can be
    // absorbed without spurious keepup errors, but not so much that the
    // queue time dominates. 4x the worker count is a common wrk-style
    // heuristic.
    let capacity = (max_conns * 4).max(max_conns + 1);
    let (tx, rx) = flume::bounded::<Token>(capacity);
    // One keepup counter per scenario, so we can attribute drops back to
    // the scheduler that saw them. The overall Summary gets the sum.
    let scenario_keepup: Vec<KeepupCounter> = (0..num_scenarios)
        .map(|_| KeepupCounter::new())
        .collect();
    let started_at = Instant::now();

    // Spawn one scheduler per non-Saturate scenario. Each scheduler
    // gets a clone of the shared `LiveSnapshot` so its `keepup` drops
    // land in the next per-second JSONL tick (not only in the final
    // summary).
    let mut scheduler_handles = Vec::new();
    for (i, scenario) in plan.scenarios.iter().enumerate() {
        if matches!(scenario.rate, RateProfile::Saturate { .. }) {
            continue;
        }
        let id = i as u16;
        let tx = tx.clone();
        let stop = stop.clone();
        let keepup = scenario_keepup[i].clone();
        let profile = scenario.rate.clone();
        let live_for_scheduler = live.clone();
        let h = compio::runtime::spawn(async move {
            run_scheduler(
                id,
                profile,
                tx,
                started_at,
                stop,
                keepup,
                live_for_scheduler,
            )
            .await;
        });
        scheduler_handles.push(h);
    }

    // If NO scenarios had a rate profile, return empty stats — nothing
    // to do. Drop the original sender to let workers finish draining.
    if scheduler_handles.is_empty() {
        drop(tx);
        return Vec::new();
    }

    // Spawn worker tasks.
    let mut worker_handles = Vec::with_capacity(max_conns);
    for _ in 0..max_conns {
        let plan = plan.clone();
        let client = client.clone();
        let rx = rx.clone();
        let stop = stop.clone();
        let live = live.clone();
        let h = compio::runtime::spawn(async move {
            worker_open_loop::<T>(plan, client, rx, stop, num_scenarios, live).await
        });
        worker_handles.push(h);
    }

    // Close our end of the channel so workers get a Disconnected when
    // the last scheduler drops theirs. The workers cloned receivers;
    // once all senders drop, receivers return RecvError::Disconnected.
    drop(tx);
    drop(rx);

    // Wait for schedulers to finish (they exit on stop).
    for h in scheduler_handles {
        let _ = h.await;
    }

    // Collect worker stats.
    let mut out = Vec::with_capacity(max_conns);
    for h in worker_handles {
        match h.await {
            Ok(stats) => out.push(stats),
            Err(_) => out.push(TaskStats::new(num_scenarios)),
        }
    }

    // Fold per-scenario keepup into task 0's slots so that:
    //   * `Summary::errors.keepup` carries the run-wide total (via the
    //     task-level counter, summed during merge), and
    //   * `Summary::per_scenario[i].errors.keepup` carries the
    //     correctly-attributed per-scenario count.
    // Attributing to task 0 keeps the count from being multiplied across
    // every worker's merge.
    if let Some(first) = out.first_mut() {
        let mut total: u64 = 0;
        for (i, counter) in scenario_keepup.iter().enumerate() {
            let n = counter.get();
            total += n;
            if let Some(sc) = first.per_scenario.get_mut(i) {
                sc.errors.keepup += n;
            }
        }
        first.errors.keepup += total;
    }
    out
}

async fn worker_open_loop<T: Transport>(
    plan: Arc<Plan>,
    client: T::Client,
    rx: flume::Receiver<Token>,
    stop: StopSignal,
    num_scenarios: usize,
    live: Option<Arc<LiveSnapshot>>,
) -> TaskStats {
    let mut stats = TaskStats::new(num_scenarios);
    let mut ctx = ScenarioContext::new(plan.vars.len(), from_entropy());

    loop {
        if stop.is_stopped() {
            break;
        }
        // We want to wake up on either a token or stop. flume doesn't
        // have a select; poll with a short timeout so the loop exits
        // even if the channel never disconnects.
        let token = match compio::time::timeout(
            Duration::from_millis(50),
            rx.recv_async(),
        )
        .await
        {
            Ok(Ok(t)) => t,
            Ok(Err(_)) => break, // channel closed
            Err(_) => continue,  // timeout — re-check stop
        };
        let scenario = &plan.scenarios[token.scenario_id as usize];
        execute_steps_open_loop::<T>(
            &client,
            token.scenario_id,
            &scenario.steps,
            &mut ctx,
            &mut stats,
            token.intended_start,
            live.as_ref(),
        )
        .await;
        ctx.clear_all();
    }

    stats
}

/// Step executor that records latency against `intended_start` for the
/// first [`Step::Request`] in the iteration. Later requests in the same
/// iteration record against the actual completion time — a chained
/// scenario's "step 2" shouldn't have step 1's queue time double-counted.
async fn execute_steps_open_loop<T: Transport>(
    client: &T::Client,
    scenario_id: u16,
    steps: &[Step],
    ctx: &mut ScenarioContext,
    stats: &mut TaskStats,
    intended_start: Instant,
    live: Option<&Arc<LiveSnapshot>>,
) {
    let mut first_request = true;
    for step in steps {
        match step {
            Step::Request(req) => match T::exchange(client, req, ctx).await {
                Ok(resp) => {
                    if first_request {
                        // Patch total to capture queue time + service time.
                        let patched = Instant::now().saturating_duration_since(intended_start);
                        let resp = Response {
                            total: patched,
                            ..resp
                        };
                        process_response(scenario_id, req, &resp, ctx, stats, live);
                        first_request = false;
                    } else {
                        process_response(scenario_id, req, &resp, ctx, stats, live);
                    }
                }
                Err(e) => {
                    let kind = classify_transport_error(&e);
                    stats.record_error(scenario_id, kind);
                    if let Some(l) = live {
                        record_transport_error_live(l, kind);
                    }
                    break;
                }
            },
            Step::Pause(d) => compio::time::sleep(*d).await,
            Step::PauseRandom { min, max } => {
                let d = if min == max {
                    *min
                } else {
                    let lo = min.as_nanos() as u64;
                    let hi = max.as_nanos() as u64;
                    Duration::from_nanos(ctx.rng.gen_range(lo..=hi))
                };
                compio::time::sleep(d).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (unit)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepup_counter_starts_zero_and_increments() {
        let k = KeepupCounter::new();
        assert_eq!(k.get(), 0);
        k.inc();
        k.inc();
        assert_eq!(k.get(), 2);
    }

    #[test]
    fn intended_offset_degenerate_constant_rate() {
        // from == to ramp — linear.
        // 100 rps → token 50 at t=0.5s.
        let t = intended_offset_for(100.0, 100.0, 1.0, 50.0);
        assert!((t - 0.5).abs() < 1e-9, "got {t}");
    }

    #[test]
    fn intended_offset_steep_ramp_500th_near_0_707s() {
        // 0 → 1000 rps over 1s: the k-th token lands at
        //   N(t) = 0·t + 1000·t²/(2·1) = 500·t² = k
        //   → t = sqrt(k/500)
        // For k = 500: t = sqrt(1) = 1.0s.
        // For k = 250: t = sqrt(0.5) ≈ 0.7071s.
        // Task doc says "500th token of a 0→1000 ramp at ≈0.707s" — that
        // corresponds to k=250 (i.e. the halfway point in count). Verify
        // the exact math holds.
        let t_250 = intended_offset_for(0.0, 1000.0, 1.0, 250.0);
        assert!(
            (t_250 - 0.5_f64.sqrt()).abs() < 1e-6,
            "k=250 should be sqrt(0.5)s, got {t_250}"
        );
        // The old approximation would put token k=250 at t = 250/500 = 0.5s.
        // Our quadratic form must be strictly greater than 0.5 at k=250,
        // and at least a few ms away from 0.5.
        assert!(
            t_250 - 0.5 > 0.005,
            "quadratic solution should be well above linear approx; got t_250={t_250}"
        );

        let t_500 = intended_offset_for(0.0, 1000.0, 1.0, 500.0);
        assert!(
            (t_500 - 1.0).abs() < 1e-6,
            "last ramp token (k=500) should be at 1s, got {t_500}"
        );
    }

    #[test]
    fn intended_offset_endpoints_match() {
        // Endpoint: N(0) = 0, so k=0 → t=0.
        let t0 = intended_offset_for(100.0, 500.0, 2.0, 0.0);
        assert!(t0.abs() < 1e-9, "got {t0}");
        // Endpoint: N(over) = (from+to)*over/2 = 600. k=600 → t=2.0.
        let t_end = intended_offset_for(100.0, 500.0, 2.0, 600.0);
        assert!((t_end - 2.0).abs() < 1e-6, "got {t_end}");
    }

    #[test]
    fn intended_offset_after_ramp_uses_to_rate() {
        // 0 → 100 over 1s; post-ramp at 100 rps.
        // N(1) = 50, then N(2) = 50 + 100*1 = 150.
        // So token 100 lands at t = 1 + (100-50)/100 = 1.5s.
        let t = intended_offset_for(0.0, 100.0, 1.0, 100.0);
        assert!((t - 1.5).abs() < 1e-6, "got {t}");
    }

    #[test]
    fn intended_offset_ramp_down_also_works() {
        // 1000 → 0 rps over 1s — symmetric to the ramp-up case.
        // N(t) = 1000·t − 500·t². Total over the ramp = 500 tokens.
        // Midpoint-by-count (k=250): 500·t² − 1000·t + 250 = 0
        //   → t² − 2t + 0.5 = 0 → t = 1 − sqrt(0.5) ≈ 0.293s.
        let t = intended_offset_for(1000.0, 0.0, 1.0, 250.0);
        let expected = 1.0 - 0.5_f64.sqrt();
        assert!(
            (t - expected).abs() < 1e-6,
            "ramp-down k=250 should be ≈{expected}, got {t}"
        );
    }
}
