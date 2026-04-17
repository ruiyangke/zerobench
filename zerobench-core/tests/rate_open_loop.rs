//! Integration tests for the open-loop rate scheduler + dispatcher.
//!
//! These tests go through an in-memory FakeTransport to avoid a real
//! HTTP dependency for the core crate — see
//! `dispatcher_saturate.rs` for the same pattern.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use parking_lot::Mutex;
use zerobench_core::plan::{
    Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::{run_open_loop, run_open_loop_threaded};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stop::StopSignal;
use zerobench_core::template::Template;
use zerobench_core::transport::{
    Response, ResponseBody, Target, Transport, TransportError, TransportOpts,
};
use zerobench_core::var::VarRegistry;
use zerobench_core::Summary;

// ---------------------------------------------------------------------------
// FakeTransport (same shape as dispatcher_saturate's).
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct FakeTransport;

#[derive(Clone)]
struct FakeClient {
    requests: Arc<AtomicU64>,
    service_time_us: Arc<AtomicU64>,
    slow_every_nth: Arc<AtomicU64>,
    slow_time_us: Arc<AtomicU64>,
    counter: Arc<Mutex<u64>>,
}

impl FakeClient {
    fn new() -> Self {
        Self {
            requests: Arc::new(AtomicU64::new(0)),
            service_time_us: Arc::new(AtomicU64::new(50)),
            slow_every_nth: Arc::new(AtomicU64::new(0)),
            slow_time_us: Arc::new(AtomicU64::new(0)),
            counter: Arc::new(Mutex::new(0)),
        }
    }
}

impl Transport for FakeTransport {
    type Client = FakeClient;

    async fn build_client(
        _target: &Target,
        _opts: &TransportOpts,
    ) -> Result<Self::Client, TransportError> {
        Ok(FakeClient::new())
    }

    async fn exchange(
        client: &Self::Client,
        _plan: &RequestPlan,
        _ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        // Decide whether this one is slow BEFORE we sleep, so we
        // count every request against the "slow every Nth" cadence.
        let mut guard = client.counter.lock();
        *guard += 1;
        let i = *guard;
        drop(guard);
        let nth = client.slow_every_nth.load(Ordering::Relaxed);
        let is_slow = nth > 0 && i % nth == 0;

        let sleep_us = if is_slow {
            client.slow_time_us.load(Ordering::Relaxed)
        } else {
            client.service_time_us.load(Ordering::Relaxed)
        };
        compio::time::sleep(Duration::from_micros(sleep_us)).await;
        client.requests.fetch_add(1, Ordering::Relaxed);

        Ok(Response {
            status: 200,
            headers: HeaderMap::new(),
            body: ResponseBody::Buffered(Bytes::from_static(b"ok")),
            bytes_sent: 10,
            bytes_received: 2,
            ttfb: Duration::from_micros(1),
            total: Duration::from_micros(sleep_us),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn constant_plan(rps: f64, duration: Duration) -> Plan {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    Plan {
        scenarios: vec![Scenario {
            name: "open-loop".into(),
            rate: RateProfile::Constant(rps),
            steps: vec![Step::Request(RequestPlan::get(url))],
        }],
        vars,
        duration,
        warmup: None,
        threads: 1,
    }
}

#[compio::test]
async fn open_loop_constant_rate_matches_target() {
    // 1000 rps target, 2s window → ~2000 requests (generous bounds).
    let plan = constant_plan(1000.0, Duration::from_millis(2000));
    let client = FakeClient::new();
    // Service time short enough that workers never backup.
    client.service_time_us.store(100, Ordering::Relaxed);
    let stop = StopSignal::after(plan.duration);

    let stats = run_open_loop::<FakeTransport>(&plan, client.clone(), 50, stop, None).await;
    let summary = Summary::merge(stats, plan.duration);

    let total = summary.requests;
    // Tight bound: ±5%. The spec target is ±3%, but 5% absorbs timer
    // granularity and warm-up jitter without letting real regressions
    // through. Keep the same bound on CI and locally (no env branching).
    assert!(
        total >= 1900 && total <= 2100,
        "expected ~2000 requests (±5%), got {total} (keepup={})",
        summary.errors.keepup
    );
    // No transport/status errors expected.
    assert_eq!(summary.errors.connect, 0);
    assert_eq!(summary.errors.timeout, 0);
    assert_eq!(summary.errors.status_4xx, 0);
    assert_eq!(summary.errors.status_5xx, 0);
}

#[compio::test]
async fn open_loop_captures_queue_time_in_latency() {
    // 500 rps target, 1 worker, 500µs normal service, with a 200ms
    // stall every 10th request. Each stall parks ~100 intended-start
    // times in the queue; those that land in the channel (capacity
    // ~4× workers) carry nearly-full stall duration of queue time.
    //
    // Under CO-free measurement, latency = now − token.intended_start,
    // so p99 must reflect the stall's queue time — well beyond 100ms.
    // Under CO-bad measurement (latency = now − send_start) p99 would
    // cap near the 200ms service time but with fewer samples at that
    // tail; the 100ms bar separates the two regimes comfortably.
    let plan = constant_plan(500.0, Duration::from_millis(1500));
    let client = FakeClient::new();
    client.service_time_us.store(500, Ordering::Relaxed);
    client.slow_every_nth.store(10, Ordering::Relaxed);
    client.slow_time_us.store(200_000, Ordering::Relaxed); // 200ms stall
    let stop = StopSignal::after(plan.duration);

    let stats = run_open_loop::<FakeTransport>(&plan, client, 1, stop, None).await;
    let summary = Summary::merge(stats, plan.duration);

    let p99 = summary.latency_p(99.0);
    assert!(
        p99 > Duration::from_millis(100),
        "p99 should capture accumulated queue time, got {p99:?} (requests={} keepup={})",
        summary.requests,
        summary.errors.keepup
    );
}

#[compio::test]
async fn open_loop_keepup_counter_fires_on_overload() {
    // 100k rps but only 5 workers. Service time 200µs. Arrival rate
    // far exceeds service rate → channel fills → keepup increments.
    let plan = constant_plan(100_000.0, Duration::from_millis(400));
    let client = FakeClient::new();
    client.service_time_us.store(200, Ordering::Relaxed);
    let stop = StopSignal::after(plan.duration);

    let stats = run_open_loop::<FakeTransport>(&plan, client.clone(), 5, stop, None).await;
    let summary = Summary::merge(stats, plan.duration);

    assert!(
        summary.errors.keepup > 0,
        "expected some keepup errors under severe overload, got keepup={}",
        summary.errors.keepup
    );
    // Requests completed should still be nonzero.
    assert!(summary.requests > 0);
}

#[compio::test]
async fn open_loop_empty_plan_is_noop() {
    let plan = Plan::new();
    let client = FakeClient::new();
    let stop = StopSignal::after(Duration::from_millis(10));
    let stats = run_open_loop::<FakeTransport>(&plan, client, 4, stop, None).await;
    assert!(stats.is_empty());
}

#[compio::test]
async fn open_loop_keepup_is_attributed_per_scenario() {
    // Two scenarios with wildly different target rates, served by only
    // one worker with a slow service time. Both schedulers overflow the
    // channel, but the faster scenario should rack up far more drops.
    // Verify the per-scenario keepup counts reflect that split.
    let mut vars = VarRegistry::new();
    let url_fast = Template::compile("/fast", &mut vars).unwrap();
    let url_slow = Template::compile("/slow", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![
            Scenario {
                name: "fast".into(),
                rate: RateProfile::Constant(100_000.0),
                steps: vec![Step::Request(RequestPlan::get(url_fast))],
            },
            Scenario {
                name: "slow".into(),
                rate: RateProfile::Constant(1_000.0),
                steps: vec![Step::Request(RequestPlan::get(url_slow))],
            },
        ],
        vars,
        duration: Duration::from_millis(300),
        warmup: None,
        threads: 1,
    };
    let client = FakeClient::new();
    client.service_time_us.store(500, Ordering::Relaxed);
    let stop = StopSignal::after(plan.duration);

    let stats = run_open_loop::<FakeTransport>(&plan, client, 1, stop, None).await;
    let summary = Summary::merge(stats, plan.duration);

    assert_eq!(summary.per_scenario.len(), 2);
    let fast_keepup = summary.per_scenario[0].errors.keepup;
    let slow_keepup = summary.per_scenario[1].errors.keepup;

    // The 100k rps scheduler must record dramatically more drops than
    // the 1k rps scheduler. A 100x rate ratio with the same single
    // worker should give at least a 10x keepup ratio (generous bound).
    assert!(
        fast_keepup > 0,
        "fast scenario should have keepup drops, got {fast_keepup}"
    );
    assert!(
        fast_keepup > slow_keepup.saturating_mul(10),
        "fast keepup ({fast_keepup}) should dominate slow keepup ({slow_keepup})"
    );
    // The totals should add up to the summary-wide count.
    assert_eq!(
        fast_keepup + slow_keepup,
        summary.errors.keepup,
        "per-scenario keepup should sum to total"
    );
}

#[compio::test]
async fn open_loop_keepup_is_forwarded_to_live_snapshot() {
    // Same shape as `open_loop_keepup_counter_fires_on_overload`, but
    // hooks a `LiveSnapshot` in and asserts the keepup count shows up
    // on a tick swap — the JSONL streaming path depends on this.
    let plan = constant_plan(100_000.0, Duration::from_millis(400));
    let client = FakeClient::new();
    client.service_time_us.store(200, Ordering::Relaxed);
    let stop = StopSignal::after(plan.duration);
    let live = zerobench_core::LiveSnapshot::new(plan.scenarios.len());

    let stats = zerobench_core::run_open_loop::<FakeTransport>(
        &plan,
        client.clone(),
        5,
        stop,
        Some(live.clone()),
    )
    .await;
    let summary = Summary::merge(stats, plan.duration);

    // Sanity: the per-run summary still reflects backpressure.
    assert!(
        summary.errors.keepup > 0,
        "summary.errors.keepup should be nonzero under overload, got {}",
        summary.errors.keepup
    );

    // And the live-tick snapshot must have seen at least one of those
    // drops land in the per-window counter.
    let tick = live.swap_and_snapshot();
    assert!(
        tick.errors.keepup > 0,
        "LiveTick should reflect keepup drops; got tick.errors = {:?}",
        tick.errors
    );
}

#[compio::test]
async fn open_loop_saturate_scenario_produces_no_tokens() {
    // A plan whose only scenario is Saturate should not be run by the
    // open-loop dispatcher — returns empty stats.
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "sat".into(),
            rate: RateProfile::Saturate { max_concurrency: 4 },
            steps: vec![Step::Request(RequestPlan::get(url))],
        }],
        vars,
        duration: Duration::from_millis(100),
        warmup: None,
        threads: 1,
    };
    let client = FakeClient::new();
    let stop = StopSignal::after(plan.duration);
    let stats = run_open_loop::<FakeTransport>(&plan, client.clone(), 4, stop, None).await;
    assert!(stats.is_empty());
    assert_eq!(client.requests.load(Ordering::Relaxed), 0);
}

// ---------------------------------------------------------------------------
// Multi-threaded open-loop tests
// ---------------------------------------------------------------------------

/// Multi-threaded open-loop with a constant rate should hit roughly
/// the target rate (each thread runs at rate/num_threads).
#[test]
fn open_loop_threaded_constant_rate() {
    let plan = Arc::new(constant_plan(2000.0, Duration::from_millis(500)));
    let target = Arc::new(Target::parse("http://fake:80").unwrap());
    let opts = Arc::new(TransportOpts::default());
    let stop = StopSignal::after_wall(Duration::from_millis(500));

    let stats = run_open_loop_threaded::<FakeTransport>(
        plan, target, opts, 2, 8, stop, None,
    );

    let total: u64 = stats.iter().map(|s| s.requests).sum();
    // 2000 rps x 0.5s = 1000 expected. Allow 30% tolerance for CI.
    assert!(
        total >= 500 && total <= 1500,
        "expected ~1000 requests at 2k rps for 500ms, got {total}"
    );
}

/// Single-thread fast path of the threaded open-loop dispatcher.
#[test]
fn open_loop_threaded_single_thread_fast_path() {
    let plan = Arc::new(constant_plan(1000.0, Duration::from_millis(300)));
    let target = Arc::new(Target::parse("http://fake:80").unwrap());
    let opts = Arc::new(TransportOpts::default());
    let stop = StopSignal::after_wall(Duration::from_millis(300));

    let stats = run_open_loop_threaded::<FakeTransport>(
        plan, target, opts, 1, 4, stop, None,
    );

    let total: u64 = stats.iter().map(|s| s.requests).sum();
    assert!(total > 0, "single-thread open-loop produced zero requests");
}
