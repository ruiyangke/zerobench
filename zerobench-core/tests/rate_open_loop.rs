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
use zerobench_core::run_open_loop;
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

    let stats = run_open_loop::<FakeTransport>(&plan, client.clone(), 50, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    let total = summary.requests;
    // Allow ±5% for scheduler drift + compio's timer granularity.
    // Be conservative: test CI can be noisy.
    assert!(
        total >= 1500 && total <= 2400,
        "expected ~2000 requests, got {total} (keepup={})",
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
    // 1000 rps target with service time of 100µs normally. Every 10th
    // request sleeps 5ms — enough to build backlog under open-loop
    // queue semantics. Workers = 1 so the queue is the only backpressure
    // mechanism.
    //
    // Under CO-free measurement, the latency histogram must include
    // queue time — p99 should be much larger than the 5ms slow request
    // because tokens queued behind the slow one accumulate queue time.
    let plan = constant_plan(500.0, Duration::from_millis(1500));
    let client = FakeClient::new();
    client.service_time_us.store(500, Ordering::Relaxed);
    client.slow_every_nth.store(10, Ordering::Relaxed);
    client.slow_time_us.store(20_000, Ordering::Relaxed); // 20ms
    let stop = StopSignal::after(plan.duration);

    let stats = run_open_loop::<FakeTransport>(&plan, client, 1, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    // We expect a massively inflated p99 because every 10th token
    // blocks behind a 20ms response, and tokens behind it accumulate
    // up to seconds of queue time (a single worker can't keep up with
    // 500rps when one in 10 responses takes 20ms — arrival rate >>
    // service rate, so queue grows unboundedly).
    //
    // Assert p99 is substantially larger than the slow request itself
    // (20ms). Under CO-bad measurement (no queue time) we'd see p99
    // close to 20ms.
    let p99 = summary.latency_p(99.0);
    assert!(
        p99 > Duration::from_millis(20),
        "p99 should capture queue time, got {p99:?}"
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

    let stats = run_open_loop::<FakeTransport>(&plan, client.clone(), 5, stop).await;
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
    let stats = run_open_loop::<FakeTransport>(&plan, client, 4, stop).await;
    assert!(stats.is_empty());
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
    };
    let client = FakeClient::new();
    let stop = StopSignal::after(plan.duration);
    let stats = run_open_loop::<FakeTransport>(&plan, client.clone(), 4, stop).await;
    assert!(stats.is_empty());
    assert_eq!(client.requests.load(Ordering::Relaxed), 0);
}
