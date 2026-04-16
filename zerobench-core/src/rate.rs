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

use crate::plan::{Plan, RateProfile, Step};
use crate::rng::from_entropy;
use crate::scenario_context::ScenarioContext;
use crate::stats::TaskStats;
use crate::step_exec::{classify_transport_error, process_response};
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
pub async fn run_scheduler(
    scenario_id: u16,
    profile: RateProfile,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
) {
    match profile {
        RateProfile::Constant(rps) => {
            run_constant(scenario_id, rps, sender, started_at, stop, keepup).await
        }
        RateProfile::Ramp { from, to, over } => {
            run_ramp(scenario_id, from, to, over, sender, started_at, stop, keepup).await
        }
        RateProfile::Stepped(steps) => {
            run_stepped(scenario_id, steps, sender, started_at, stop, keepup).await
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

async fn run_constant(
    scenario_id: u16,
    rps: f64,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
) {
    if rps <= 0.0 {
        return;
    }
    let period_ns = (1e9 / rps) as u64;
    let period = Duration::from_nanos(period_ns.max(1));

    let mut i: u64 = 0;
    while !stop.is_stopped() {
        let intended = started_at + period * (i as u32);
        // If intended is already behind, don't sleep — emit immediately
        // and move on (the "catch-up" path).
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
                keepup.inc();
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
) {
    // Integrate r(t) = from + (to-from)*t/over to get target count over
    // time: N(t) = from*t + (to-from)*t^2/(2*over). We emit tokens when
    // N(elapsed) >= next_index + 1.
    //
    // After `over` elapses, continue at rate `to`.
    let over_s = over.as_secs_f64().max(1e-9);
    let mut emitted: u64 = 0;
    while !stop.is_stopped() {
        let elapsed = Instant::now().saturating_duration_since(started_at);
        let t = elapsed.as_secs_f64();
        let target = if t < over_s {
            from * t + (to - from) * t * t / (2.0 * over_s)
        } else {
            let end_target = from * over_s + (to - from) * over_s / 2.0;
            end_target + to * (t - over_s)
        };
        let target_count = target.max(0.0) as u64;
        while emitted < target_count {
            let intended = started_at + Duration::from_secs_f64(emitted as f64 / rate_at(from, to, over_s, emitted as f64).max(1e-9));
            match sender.try_send(Token {
                scenario_id,
                intended_start: intended,
            }) {
                Ok(()) => emitted += 1,
                Err(flume::TrySendError::Full(_)) => {
                    keepup.inc();
                    emitted += 1;
                }
                Err(flume::TrySendError::Disconnected(_)) => return,
            }
        }
        compio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Instantaneous rate at emission index `n` under a linear ramp
/// `from → to` over `over_s` seconds. Used only to compute
/// `intended_start`; we accept a minor rounding inaccuracy for the sake
/// of simple math.
fn rate_at(from: f64, to: f64, over_s: f64, n: f64) -> f64 {
    // Assume linear-in-time r(t); approximate the corresponding `t`
    // for emission n by inverting N(t). For a uniform average the
    // approximation `t ≈ n / ((from + to) / 2)` suffices at this scale.
    let avg = (from + to) / 2.0;
    if avg <= 0.0 {
        return from.max(1.0);
    }
    let t = (n / avg).min(over_s);
    from + (to - from) * t / over_s
}

async fn run_stepped(
    scenario_id: u16,
    mut steps: Vec<(Duration, f64)>,
    sender: flume::Sender<Token>,
    started_at: Instant,
    stop: StopSignal,
    keepup: KeepupCounter,
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
            Err(flume::TrySendError::Full(_)) => keepup.inc(),
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
    let keepup = KeepupCounter::new();
    let started_at = Instant::now();

    // Spawn one scheduler per non-Saturate scenario.
    let mut scheduler_handles = Vec::new();
    for (i, scenario) in plan.scenarios.iter().enumerate() {
        if matches!(scenario.rate, RateProfile::Saturate { .. }) {
            continue;
        }
        let id = i as u16;
        let tx = tx.clone();
        let stop = stop.clone();
        let keepup = keepup.clone();
        let profile = scenario.rate.clone();
        let h = compio::runtime::spawn(async move {
            run_scheduler(id, profile, tx, started_at, stop, keepup).await;
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
        let h = compio::runtime::spawn(async move {
            worker_open_loop::<T>(plan, client, rx, stop, num_scenarios).await
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

    // Fold keepup into every task's errors — attribute to task 0 so the
    // merged Summary reflects the total without double-counting per
    // scenario.
    if let Some(first) = out.first_mut() {
        first.errors.keepup += keepup.get();
    }
    out
}

async fn worker_open_loop<T: Transport>(
    plan: Arc<Plan>,
    client: T::Client,
    rx: flume::Receiver<Token>,
    stop: StopSignal,
    num_scenarios: usize,
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
                        process_response(scenario_id, req, &resp, ctx, stats);
                        first_request = false;
                    } else {
                        process_response(scenario_id, req, &resp, ctx, stats);
                    }
                }
                Err(e) => {
                    stats.record_error(scenario_id, classify_transport_error(&e));
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
    fn rate_at_constant_rate_is_constant() {
        // from == to degenerate case.
        assert!((rate_at(100.0, 100.0, 1.0, 50.0) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn rate_at_linear_ramp_midpoint() {
        // ramp 0 → 200 over 1s; at n=100 (elapsed ≈ 1s) rate should be ≈ 200.
        let r = rate_at(0.0, 200.0, 1.0, 100.0);
        assert!((r - 200.0).abs() < 1e-6, "got {r}");
    }
}
