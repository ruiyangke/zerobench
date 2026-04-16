//! Integration tests for [`zerobench_core::run_saturate`].
//!
//! These spin up an in-process hyper test server on the current compio
//! runtime and drive `run_saturate` against it, then assert on the
//! collected `TaskStats`.
//!
//! Why keep the fake transport in-crate rather than pulling in
//! zerobench-http for tests: the core dispatcher is transport-agnostic,
//! and a hand-rolled in-memory [`Transport`] impl both exercises the
//! trait bounds (Client: Clone + Send + 'static; exchange: async-in-
//! trait, !Send future) and keeps the core test suite independent from
//! the HTTP crate's compile-test surface. One real-HTTP smoke test
//! lives in `zerobench-cli/tests/cli_smoke.rs` (Task 9).

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use zerobench_core::plan::{
    Assertion, Extract, Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stop::StopSignal;
use zerobench_core::template::Template;
use zerobench_core::transport::{
    Response, ResponseBody, Target, Transport, TransportError, TransportOpts,
};
use zerobench_core::var::VarRegistry;
use zerobench_core::{run_saturate, Summary};

// ---------------------------------------------------------------------------
// FakeTransport — an in-memory transport for testing the dispatcher.
// ---------------------------------------------------------------------------
//
// Rationale: the dispatcher must work against *any* Transport impl. Using
// a fake lets us:
//  - Drive the request counter without a real server in the loop.
//  - Inject custom responses (e.g. different status codes per URL).
//  - Verify template expansion actually reached the transport.

#[derive(Clone)]
struct FakeTransport;

#[derive(Clone)]
struct FakeClient {
    requests: Arc<AtomicU64>,
    // Count per distinct URL so multi-scenario tests can assert each
    // scenario's scenario actually got exercised.
    urls: Arc<parking_lot::Mutex<Vec<String>>>,
    // Status code we respond with (varies per test).
    status: Arc<AtomicU32>,
    // When set, response echoes this header name back in the response
    // with the current request count as value — exercises extract+chain.
    echo_status_header: Option<HeaderName>,
}

impl FakeClient {
    fn new(status: u16) -> Self {
        Self {
            requests: Arc::new(AtomicU64::new(0)),
            urls: Arc::new(parking_lot::Mutex::new(Vec::new())),
            status: Arc::new(AtomicU32::new(status as u32)),
            echo_status_header: None,
        }
    }
}

impl Transport for FakeTransport {
    type Client = FakeClient;

    async fn build_client(
        _target: &Target,
        _opts: &TransportOpts,
    ) -> Result<Self::Client, TransportError> {
        Ok(FakeClient::new(200))
    }

    async fn exchange(
        client: &Self::Client,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        // Expand the URL to record which URL was requested and to
        // verify templates work.
        let mut url_buf: Vec<u8> = Vec::with_capacity(plan.url.estimated_size());
        plan.url.expand_into(&mut url_buf, &mut ctx.expand_ctx());
        let url = String::from_utf8_lossy(&url_buf).into_owned();
        client.urls.lock().push(url);
        client.requests.fetch_add(1, Ordering::Relaxed);

        // Real transports await socket IO; we must too, otherwise the
        // single-threaded compio runtime never polls the stop-signal
        // timer nor other worker tasks (pure cooperative multitasking).
        // A 1µs sleep is enough to hit the timer wheel.
        compio::time::sleep(Duration::from_micros(1)).await;

        let status = client.status.load(Ordering::Relaxed) as u16;
        let mut headers = HeaderMap::new();
        if let Some(name) = &client.echo_status_header {
            let val = format!("{status}");
            headers.insert(
                name.clone(),
                HeaderValue::from_bytes(val.as_bytes()).unwrap(),
            );
        }

        Ok(Response {
            status,
            headers,
            body: ResponseBody::Buffered(Bytes::from_static(b"pong")),
            bytes_sent: 42,
            bytes_received: 4,
            ttfb: Duration::from_micros(100),
            total: Duration::from_micros(250),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// One scenario, one step (GET /), saturate for a short window and
/// verify throughput + stats consistency.
#[compio::test]
async fn saturate_single_scenario_fires_many_requests() {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "bench".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![Step::Request(RequestPlan::get(url))],
        }],
        vars,
        duration: Duration::from_millis(300),
        warmup: None,
    };

    let client = FakeClient::new(200);
    let stop = StopSignal::after(plan.duration);
    let stats = run_saturate::<FakeTransport>(&plan, client.clone(), 4, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    // Transport-level request counter matches the stats total.
    let transport_requests = client.requests.load(Ordering::Relaxed);
    assert_eq!(summary.requests, transport_requests);
    // Should be substantial — FakeTransport::exchange is cheap, so even
    // on a contended CI box 300ms * 4 workers completes thousands.
    assert!(summary.requests > 50, "too few requests: {}", summary.requests);
    // No errors — FakeTransport returns 200 without assertions.
    assert_eq!(summary.errors.total(), 0);
    // Latency histogram non-empty.
    assert_eq!(summary.latency_p(50.0).is_zero(), false);
    // Per-scenario aligned.
    assert_eq!(summary.per_scenario.len(), 1);
    assert_eq!(summary.per_scenario[0].requests, summary.requests);
}

/// Two scenarios — both should see some traffic under uniform selection.
#[compio::test]
async fn saturate_multi_scenario_exercises_every_scenario() {
    let mut vars = VarRegistry::new();
    let url_a = Template::compile("/a", &mut vars).unwrap();
    let url_b = Template::compile("/b", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![
            Scenario {
                name: "a".into(),
                rate: RateProfile::Saturate { max_concurrency: 50 },
                steps: vec![Step::Request(RequestPlan::get(url_a))],
            },
            Scenario {
                name: "b".into(),
                rate: RateProfile::Saturate { max_concurrency: 50 },
                steps: vec![Step::Request(RequestPlan::get(url_b))],
            },
        ],
        vars,
        duration: Duration::from_millis(300),
        warmup: None,
    };

    let client = FakeClient::new(200);
    let stop = StopSignal::after(plan.duration);
    let stats = run_saturate::<FakeTransport>(&plan, client.clone(), 8, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    assert_eq!(summary.per_scenario.len(), 2);
    assert!(summary.per_scenario[0].requests > 0, "scenario a empty");
    assert!(summary.per_scenario[1].requests > 0, "scenario b empty");

    // URL routing: every URL we saw should be one of /a or /b.
    let urls = client.urls.lock();
    for u in urls.iter() {
        assert!(u == "/a" || u == "/b", "unexpected url: {u}");
    }
    assert_eq!(
        urls.len() as u64,
        summary.requests,
        "url count should match request count"
    );
}

/// Assertion failure: server returns 200 but plan expects 404. Both the
/// request count and assertion-failed counter should be positive.
#[compio::test]
async fn saturate_counts_assertion_failures_but_still_counts_requests() {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let mut req = RequestPlan::get(url);
    req.checks = vec![Assertion::StatusEq(404)];

    let plan = Plan {
        scenarios: vec![Scenario {
            name: "expect-404".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![Step::Request(req)],
        }],
        vars,
        duration: Duration::from_millis(250),
        warmup: None,
    };

    let client = FakeClient::new(200);
    let stop = StopSignal::after(plan.duration);
    let stats = run_saturate::<FakeTransport>(&plan, client.clone(), 4, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    assert!(summary.requests > 0);
    assert!(
        summary.errors.assertion_failed > 0,
        "expected some assertion failures"
    );
    // Every successful response should have bumped assertion_failed
    // because the server never produces 404.
    assert_eq!(summary.errors.assertion_failed, summary.requests);
}

/// Extract + chained template: first step extracts the status code into
/// a var; second step's URL interpolates the var. Verify the observed
/// URL reflects the interpolation.
#[compio::test]
async fn saturate_extract_status_propagates_through_chained_url() {
    let mut vars = VarRegistry::new();
    let url_first = Template::compile("/first", &mut vars).unwrap();
    let url_second = Template::compile("/second?status={{var:last_status}}", &mut vars).unwrap();
    let status_slot = vars.allocate("last_status").unwrap();

    let mut first = RequestPlan::get(url_first);
    first.extract = vec![Extract::StatusCode { into: status_slot }];

    let plan = Plan {
        scenarios: vec![Scenario {
            name: "chain".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![
                Step::Request(first),
                Step::Request(RequestPlan::get(url_second)),
            ],
        }],
        vars,
        duration: Duration::from_millis(200),
        warmup: None,
    };

    let mut client = FakeClient::new(200);
    // A bit ugly — we don't need the echo header, but we do want the
    // default 200 status. Status::new default already sets 200.
    client.echo_status_header = None;
    let stop = StopSignal::after(plan.duration);
    let _stats = run_saturate::<FakeTransport>(&plan, client.clone(), 4, stop).await;

    // Every "second" URL observed must carry the 200 that the first
    // request emitted via Extract::StatusCode.
    let urls = client.urls.lock();
    let second_urls: Vec<_> = urls.iter().filter(|u| u.starts_with("/second")).collect();
    assert!(!second_urls.is_empty(), "no second-step URLs observed");
    for u in second_urls {
        assert_eq!(
            u.as_str(),
            "/second?status=200",
            "second-step URL should interpolate the extracted status"
        );
    }
}

/// Stop signal trips immediately — dispatcher should return with
/// zero (or near-zero) requests.
#[compio::test]
async fn saturate_respects_prefired_stop_signal() {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "bench".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![Step::Request(RequestPlan::get(url))],
        }],
        vars,
        duration: Duration::from_millis(100),
        warmup: None,
    };

    let client = FakeClient::new(200);
    let stop = StopSignal::new();
    stop.stop(); // trip before spawning

    let stats = run_saturate::<FakeTransport>(&plan, client.clone(), 2, stop).await;
    let summary = Summary::merge(stats, plan.duration);
    // Workers check the flag before their first iteration, so requests
    // should be 0. The guard lives at the top of the loop; a tripped
    // signal aborts before picking a scenario.
    assert_eq!(summary.requests, 0);
}

/// Empty plan (no scenarios) — no workers spawned, returns an empty
/// stats vec.
#[compio::test]
async fn saturate_empty_plan_is_noop() {
    let plan = Plan::new();
    let client = FakeClient::new(200);
    let stop = StopSignal::after(Duration::from_millis(10));
    let stats = run_saturate::<FakeTransport>(&plan, client, 4, stop).await;
    assert!(stats.is_empty());
}

/// Saturate with `max_tasks = 0` — also a noop.
#[compio::test]
async fn saturate_zero_max_tasks_is_noop() {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![Scenario::new(
            "scn",
            vec![Step::Request(RequestPlan::get(url))],
        )],
        vars,
        duration: Duration::from_millis(50),
        warmup: None,
    };
    let client = FakeClient::new(200);
    let stop = StopSignal::after(plan.duration);
    let stats = run_saturate::<FakeTransport>(&plan, client, 0, stop).await;
    assert!(stats.is_empty());
}

/// Pause step — workers should actually sleep and throughput should
/// reflect the pause.
#[compio::test]
async fn saturate_pause_step_slows_throughput() {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "paused".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![
                Step::Request(RequestPlan::get(url)),
                Step::Pause(Duration::from_millis(20)),
            ],
        }],
        vars,
        duration: Duration::from_millis(250),
        warmup: None,
    };

    let client = FakeClient::new(200);
    let stop = StopSignal::after(plan.duration);
    let stats = run_saturate::<FakeTransport>(&plan, client.clone(), 1, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    // With a 20ms pause per iteration and a 250ms window, a single
    // worker should have run between ~8 and ~14 iterations. Stay loose
    // to avoid CI flakes.
    assert!(
        summary.requests >= 4 && summary.requests <= 25,
        "unexpected request count with 20ms pause: {}",
        summary.requests
    );
}

// ---------------------------------------------------------------------------
// Use-count sanity — suppress unused-crate warnings from Rc/Cell imports.
// These are not referenced by tests above, but keeping the imports in the
// header block keeps the test file shape consistent with future tests
// that will need a single-threaded shared handle (Task 10).
// ---------------------------------------------------------------------------
#[allow(dead_code)]
fn _shape_check() {
    let _ = Rc::new(Cell::new(0u64));
}
