//! Scenario dispatch and the worker loop.
//!
//! This module turns a [`Plan`] + a [`Transport::Client`] into a stream of
//! real HTTP exchanges, runs them to completion, and collects per-task
//! statistics for the reporter.
//!
//! Two entry points:
//!
//! - [`run_saturate`] — closed-loop. Spawns `max_tasks` worker coroutines,
//!   each loops "pick scenario → execute steps → record stats" until the
//!   shared [`StopSignal`] trips.
//! - [`run_open_loop`] (Task 10) — token-driven; one scheduler per
//!   scenario emits timestamped tokens into a shared MPMC channel that
//!   the workers drain.
//!
//! Both modes share the same per-iteration step-runner
//! ([`execute_steps`]), so assertions / extracts / template expansion
//! have one well-tested implementation.
//!
//! # Scheduling
//!
//! Workers run as `compio::runtime::spawn` tasks on the current
//! single-threaded runtime. `exchange` futures are `!Send` (they bottom
//! out in compio IO handles) but that's fine — every worker stays on
//! the runtime's thread.
//!
//! # Cancellation
//!
//! A tripped [`StopSignal`] only prevents the worker from picking up a
//! new iteration; any in-flight `exchange` is allowed to finish. That's
//! the correct behaviour for a bench tool — dropping the future on the
//! timeout boundary would invalidate the slot (see `Http1Pool::exchange`
//! timeout handling) and skew stats with spurious errors.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rand::Rng;

use crate::live_snapshot::LiveSnapshot;
use crate::plan::{Plan, Step};
use crate::rng::from_entropy;
use crate::runtime::runtime_sleep;
use crate::scenario_context::ScenarioContext;
use crate::stats::TaskStats;
use crate::step_exec::{
    classify_transport_error, process_response, record_transport_error_live,
};
use crate::stop::StopSignal;
use crate::transport::Transport;

/// Cooperatively yield once to the runtime.
///
/// Returns `Pending` on first poll (after arranging for its own waker
/// to be scheduled), then `Ready` on the second. This is the minimal
/// "let other tasks run before I continue" primitive.
///
/// # Why not `compio::time::sleep(Duration::ZERO)`?
///
/// `compio::time::sleep` bottoms out in `TimerRuntime::insert`, which
/// **drops** timers whose deadline is already in the past and returns
/// synchronously without ever yielding. So `sleep(Duration::ZERO)` is a
/// no-op — exactly the opposite of what we want in the error branch of
/// the worker loop, where we need to surrender the thread so
/// `StopSignal::after`'s own timer task can fire.
///
/// # Why not `compio::runtime::yield_now`?
///
/// compio 0.18 doesn't export a `yield_now`. A hand-rolled 8-line
/// future is the simplest dependency-free fix.
struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            // Wake ourselves so the runtime re-polls us after draining
            // other ready tasks. Without this, a cooperative-only
            // scheduler could park us indefinitely.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Return a future that yields control to the runtime exactly once.
fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

// ---------------------------------------------------------------------------
// Public API — saturate-mode dispatcher
// ---------------------------------------------------------------------------

/// Run a closed-loop saturation benchmark.
///
/// Spawns `max_tasks` concurrent worker coroutines. Each worker
/// iterates:
///
/// 1. Pick a scenario (weighted-random; Phase C uses equal weights
///    pending the Task-10 rate-profile work).
/// 2. Walk through its [`Step`]s in order, executing [`Step::Request`]s
///    through `client` and sleeping for [`Step::Pause`] /
///    [`Step::PauseRandom`].
/// 3. Record latency/TTFB/bytes into the worker's [`TaskStats`].
/// 4. Loop until `stop` trips.
///
/// Returns one [`TaskStats`] per worker; the caller merges via
/// [`crate::stats::Summary::merge`].
///
/// Empty plan (no scenarios) returns an empty `Vec` without spawning
/// anything.
pub async fn run_saturate<T: Transport>(
    plan: &Plan,
    client: T::Client,
    max_tasks: usize,
    stop: StopSignal,
    live: Option<Arc<LiveSnapshot>>,
) -> Vec<TaskStats> {
    if plan.scenarios.is_empty() || max_tasks == 0 {
        return Vec::new();
    }

    let plan = Arc::new(plan.clone());
    let num_scenarios = plan.scenarios.len();

    #[cfg(feature = "runtime-compio")]
    {
        let mut handles = Vec::with_capacity(max_tasks);
        for _ in 0..max_tasks {
            let plan = plan.clone();
            let client = client.clone();
            let stop = stop.clone();
            let live = live.clone();
            let handle = compio::runtime::spawn(async move {
                worker_saturate::<T>(plan, client, stop, num_scenarios, live).await
            });
            handles.push(handle);
        }

        let mut out = Vec::with_capacity(max_tasks);
        for h in handles {
            match h.await {
                Ok(stats) => out.push(stats),
                Err(_panic) => {
                    out.push(TaskStats::new(num_scenarios));
                }
            }
        }
        out
    }

    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    {
        // Tokio path: use a LocalSet to avoid the Send requirement on
        // Transport futures (compio IO types are !Send, and the generic
        // Transport trait doesn't bound Send on exchange's return).
        // tokio::task::spawn_local works on a LocalSet and doesn't
        // require Send.
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut handles = Vec::with_capacity(max_tasks);
            for _ in 0..max_tasks {
                let plan = plan.clone();
                let client = client.clone();
                let stop = stop.clone();
                let live = live.clone();
                handles.push(tokio::task::spawn_local(async move {
                    worker_saturate::<T>(plan, client, stop, num_scenarios, live).await
                }));
            }

            let mut out = Vec::with_capacity(max_tasks);
            for h in handles {
                match h.await {
                    Ok(stats) => out.push(stats),
                    Err(_panic) => {
                        out.push(TaskStats::new(num_scenarios));
                    }
                }
            }
            out
        }).await
    }
}

// ---------------------------------------------------------------------------
// Public API — multi-threaded saturate dispatcher
// ---------------------------------------------------------------------------

/// Multi-threaded saturate dispatcher. Spawns `num_threads` OS threads,
/// each with its own compio runtime and connection pool. Each thread
/// runs `conns_per_thread` concurrent worker tasks. Stats are collected
/// per-thread and returned as a flat `Vec` for merging.
///
/// When `num_threads <= 1`, takes a single-thread fast path with no
/// thread-spawn overhead.
pub fn run_saturate_threaded<T: Transport>(
    plan: Arc<Plan>,
    target: Arc<crate::transport::Target>,
    opts: Arc<crate::transport::TransportOpts>,
    num_threads: usize,
    total_connections: usize,
    stop: StopSignal,
    live: Option<Arc<LiveSnapshot>>,
) -> Vec<TaskStats>
where
    T::Client: Send + 'static,
{
    #[cfg(feature = "runtime-compio")]
    {
        if num_threads <= 1 {
            return compio::runtime::Runtime::new()
                .expect("compio runtime")
                .block_on(async {
                    let client = T::build_client(&target, &opts)
                        .await
                        .expect("build_client");
                    run_saturate::<T>(&plan, client, total_connections, stop, live).await
                });
        }

        let conns_per_thread = (total_connections + num_threads - 1) / num_threads;

        let handles: Vec<_> = (0..num_threads)
            .map(|_thread_id| {
                let plan = plan.clone();
                let target = target.clone();
                let opts = opts.clone();
                let stop = stop.clone();
                let live = live.clone();

                std::thread::spawn(move || {
                    compio::runtime::Runtime::new()
                        .expect("compio runtime")
                        .block_on(async {
                            let client = T::build_client(&target, &opts)
                                .await
                                .expect("build_client");
                            run_saturate::<T>(&plan, client, conns_per_thread, stop, live).await
                        })
                })
            })
            .collect();

        handles
            .into_iter()
            .flat_map(|h| h.join().expect("worker thread panicked"))
            .collect()
    }

    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    {
        run_saturate_threaded_tokio::<T>(plan, target, opts, num_threads, total_connections, stop, live)
    }
}

/// Tokio threaded dispatcher. Spawns N OS threads, each with its own
/// `current_thread` tokio runtime (mirroring the compio model). This
/// avoids the `Send` requirement on Transport futures while still giving
/// each thread its own connection pool and reactor.
#[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
fn run_saturate_threaded_tokio<T: Transport>(
    plan: Arc<Plan>,
    target: Arc<crate::transport::Target>,
    opts: Arc<crate::transport::TransportOpts>,
    num_threads: usize,
    total_connections: usize,
    stop: StopSignal,
    live: Option<Arc<LiveSnapshot>>,
) -> Vec<TaskStats>
where
    T::Client: Send + 'static,
{
    if num_threads <= 1 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        return rt.block_on(async {
            let client = T::build_client(&target, &opts)
                .await
                .expect("build_client");
            run_saturate::<T>(&plan, client, total_connections, stop, live).await
        });
    }

    let conns_per_thread = (total_connections + num_threads - 1) / num_threads;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let plan = plan.clone();
            let target = target.clone();
            let opts = opts.clone();
            let stop = stop.clone();
            let live = live.clone();

            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime");
                rt.block_on(async {
                    let client = T::build_client(&target, &opts)
                        .await
                        .expect("build_client");
                    run_saturate::<T>(&plan, client, conns_per_thread, stop, live).await
                })
            })
        })
        .collect();

    handles
        .into_iter()
        .flat_map(|h| h.join().expect("worker thread panicked"))
        .collect()
}

// ---------------------------------------------------------------------------
// Worker — saturate mode
// ---------------------------------------------------------------------------

async fn worker_saturate<T: Transport>(
    plan: Arc<Plan>,
    client: T::Client,
    stop: StopSignal,
    num_scenarios: usize,
    live: Option<Arc<LiveSnapshot>>,
) -> TaskStats {
    let mut stats = TaskStats::new(num_scenarios);
    let mut ctx = ScenarioContext::new(plan.vars.len(), from_entropy());

    while !stop.is_stopped() {
        // Pick a scenario. Phase C uses uniform random selection;
        // proper weighted-random lands with the Task-10 rate profile.
        let scenario_id = pick_scenario(num_scenarios, &mut ctx);
        let scenario = &plan.scenarios[scenario_id];

        execute_steps::<T>(
            &client,
            scenario_id as u16,
            &scenario.steps,
            &mut ctx,
            &mut stats,
            live.as_ref(),
        )
        .await;

        // Fresh slate between iterations so a prior iteration's extracts
        // don't leak into the next.
        ctx.clear_all();
    }

    stats
}

// ---------------------------------------------------------------------------
// Shared step executor
// ---------------------------------------------------------------------------

/// Execute one iteration of `steps`. On transport error inside a
/// [`Step::Request`], skip the remaining steps of this iteration but
/// still count the error against the task/scenario.
///
/// Latency/bytes for successful responses are folded into `stats`;
/// extractors populate `ctx` so templates in later steps can interpolate
/// the extracted values.
pub(crate) async fn execute_steps<T: Transport>(
    client: &T::Client,
    scenario_id: u16,
    steps: &[Step],
    ctx: &mut ScenarioContext,
    stats: &mut TaskStats,
    live: Option<&Arc<LiveSnapshot>>,
) {
    for step in steps {
        match step {
            Step::Request(req) => {
                match T::exchange(client, req, ctx).await {
                    Ok(resp) => {
                        process_response(scenario_id, req, &resp, ctx, stats, live);
                    }
                    Err(e) => {
                        let kind = classify_transport_error(&e);
                        stats.record_error(scenario_id, kind);
                        if let Some(l) = live {
                            record_transport_error_live(l, scenario_id, kind);
                        }
                        // If every slot in the pool is dead (e.g. H1-only
                        // client against an H2-only server), `T::exchange`
                        // returns `Err` *synchronously* without hitting a
                        // real `.await` on a socket. On a single-threaded
                        // runtime, that starves every other task — notably
                        // `StopSignal::after`'s timer — so the worker loop
                        // spins forever. A cooperative yield here restores
                        // forward progress for the rest of the runtime
                        // without changing behaviour on the happy path
                        // (real IO already yields).
                        yield_now().await;
                        // Don't execute further steps — their templates
                        // may rely on an extract we didn't get to run.
                        break;
                    }
                }
            }
            Step::Pause(d) => runtime_sleep(*d).await,
            Step::PauseRandom { min, max } => {
                let d = if min == max {
                    *min
                } else {
                    let lo = min.as_nanos() as u64;
                    let hi = max.as_nanos() as u64;
                    let pick = ctx.rng.gen_range(lo..=hi);
                    std::time::Duration::from_nanos(pick)
                };
                runtime_sleep(d).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario selection
// ---------------------------------------------------------------------------

/// Pick a scenario index. Phase C strategy: uniform random.
///
/// Task 10 replaces this with weighted-random based on the rate profile
/// (for `Constant(r)` each scenario's weight is `r`; for `Saturate` all
/// scenarios receive equal weight). Keeping the selection logic behind
/// a helper means the open-loop code can specialise without restructuring
/// the worker body.
fn pick_scenario(num_scenarios: usize, ctx: &mut ScenarioContext) -> usize {
    if num_scenarios <= 1 {
        0
    } else {
        ctx.rng.gen_range(0..num_scenarios)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_scenario_uniform_in_range() {
        let mut ctx = ScenarioContext::new(0, crate::rng::from_seed(7));
        for _ in 0..100 {
            let s = pick_scenario(5, &mut ctx);
            assert!(s < 5);
        }
        // With one scenario, always 0.
        assert_eq!(pick_scenario(1, &mut ctx), 0);
        assert_eq!(pick_scenario(0, &mut ctx), 0);
    }
}
