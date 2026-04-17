//! Integration tests for JSONL streaming + Prometheus output.

use std::time::Duration;

use zerobench_core::{
    plan::{Plan, RateProfile, RequestPlan, Scenario, Step},
    print_jsonl_tick, print_prometheus, ErrorKind, LiveSnapshot, Summary, TaskStats,
    Template, VarRegistry,
};

// ---------------------------------------------------------------------------
// LiveSnapshot round-trip
// ---------------------------------------------------------------------------

#[test]
fn live_snapshot_record_and_swap_counts_samples() {
    let live = LiveSnapshot::new(0);
    live.record(100_000, 5, 10);
    live.record(200_000, 7, 14);
    live.record_error(ErrorKind::Connect);
    live.record_error(ErrorKind::Status5xx);

    let tick = live.swap_and_snapshot();
    assert_eq!(tick.requests, 2);
    assert_eq!(tick.bytes_sent, 12);
    assert_eq!(tick.bytes_recv, 24);
    assert_eq!(tick.errors.connect, 1);
    assert_eq!(tick.errors.status_5xx, 1);
    // After swap, counters reset.
    let tick2 = live.swap_and_snapshot();
    assert_eq!(tick2.requests, 0);
    assert!(tick2.latency.is_empty());
}

// ---------------------------------------------------------------------------
// JSONL writer
// ---------------------------------------------------------------------------

#[test]
fn jsonl_tick_is_valid_single_line_json() {
    let live = LiveSnapshot::new(0);
    // Seed some samples across a few orders of magnitude so percentiles
    // are meaningful.
    for _ in 0..50 {
        live.record(100_000, 1, 2); // 100µs
    }
    for _ in 0..10 {
        live.record(500_000, 1, 2); // 500µs
    }
    live.record(2_100_000, 1, 2); // 2.1ms

    let tick = live.swap_and_snapshot();

    let mut buf = Vec::new();
    print_jsonl_tick(&tick, &mut buf).unwrap();

    // Single line → exactly one `\n` at the end.
    let s = std::str::from_utf8(&buf).unwrap();
    assert_eq!(s.matches('\n').count(), 1, "expected one newline: {s}");

    // Parses as valid JSON with expected fields.
    let v: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
    assert!(v.get("t").is_some(), "missing 't' field: {v}");
    assert!(v.get("rps").is_some(), "missing 'rps': {v}");
    assert!(v.get("requests_delta").is_some());
    assert!(v.get("bytes_sent").is_some());
    assert!(v.get("bytes_recv").is_some());
    assert!(v.get("p50_ns").is_some());
    assert!(v.get("p99_ns").is_some());
    assert!(v.get("p99_9_ns").is_some());
    assert!(v.get("errors").and_then(|e| e.get("connect")).is_some());

    assert_eq!(v["requests_delta"].as_u64().unwrap(), 61);
    assert!(v["rps"].as_u64().unwrap() > 0);
    // p50 should be ≤ p99.
    let p50 = v["p50_ns"].as_u64().unwrap();
    let p99 = v["p99_ns"].as_u64().unwrap();
    assert!(p50 <= p99, "p50={p50} p99={p99}");
}

// ---------------------------------------------------------------------------
// Prometheus writer
// ---------------------------------------------------------------------------

fn make_summary_and_plan() -> (Summary, Plan) {
    // Build a TaskStats with a small set of samples, then merge into
    // a Summary. The plan only needs to exist to pass as the second
    // argument; its contents don't affect the Prometheus output.
    let mut ts = TaskStats::new(1);
    for _ in 0..100 {
        ts.record(0, Duration::from_micros(120), Duration::from_micros(50), 80, 160);
    }
    ts.record(0, Duration::from_millis(10), Duration::from_millis(5), 80, 160);
    ts.errors.connect = 0;
    ts.errors.status_5xx = 2;
    let summary = Summary::merge(vec![ts], Duration::from_secs(30));

    let mut vars = VarRegistry::new();
    let url = Template::compile("http://example.com/", &mut vars).unwrap();
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "s".into(),
            rate: RateProfile::Constant(100.0),
            steps: vec![Step::Request(RequestPlan::get(url))],
        }],
        vars,
        duration: Duration::from_secs(30),
        warmup: None,
        threads: 1,
    };
    (summary, plan)
}

#[test]
fn prometheus_output_has_expected_metric_families() {
    let (summary, plan) = make_summary_and_plan();
    let mut buf = Vec::new();
    print_prometheus(&summary, &plan, &mut buf).unwrap();
    let s = std::str::from_utf8(&buf).expect("utf-8");

    // Each metric family has a HELP and TYPE line.
    assert!(
        s.contains("# HELP zerobench_requests_total"),
        "missing requests HELP:\n{s}"
    );
    assert!(s.contains("# TYPE zerobench_requests_total counter"));
    assert!(s.contains("zerobench_requests_total 101"));

    assert!(s.contains("# HELP zerobench_latency_seconds"));
    assert!(s.contains("# TYPE zerobench_latency_seconds summary"));
    assert!(s.contains("zerobench_latency_seconds{quantile=\"0.5\"}"));
    assert!(s.contains("zerobench_latency_seconds{quantile=\"0.9\"}"));
    assert!(s.contains("zerobench_latency_seconds{quantile=\"0.99\"}"));
    assert!(s.contains("zerobench_latency_seconds{quantile=\"0.999\"}"));
    assert!(s.contains("zerobench_latency_seconds_sum"));
    assert!(s.contains("zerobench_latency_seconds_count 101"));

    assert!(s.contains("# HELP zerobench_errors_total"));
    assert!(s.contains("# TYPE zerobench_errors_total counter"));
    assert!(s.contains("zerobench_errors_total{category=\"connect\"} 0"));
    assert!(s.contains("zerobench_errors_total{category=\"status_5xx\"} 2"));

    assert!(s.contains("# HELP zerobench_bytes_sent_total"));
    assert!(s.contains("zerobench_bytes_sent_total"));
    assert!(s.contains("# HELP zerobench_bytes_received_total"));
    assert!(s.contains("zerobench_bytes_received_total"));
}

#[test]
fn prometheus_output_is_clean_ascii_text() {
    // Prometheus textfile format is strict about ASCII; make sure we
    // don't accidentally emit Unicode via the formatter.
    let (summary, plan) = make_summary_and_plan();
    let mut buf = Vec::new();
    print_prometheus(&summary, &plan, &mut buf).unwrap();
    let s = std::str::from_utf8(&buf).expect("utf-8");
    assert!(
        s.chars().all(|c| c.is_ascii()),
        "non-ASCII byte in Prometheus output:\n{s}"
    );
}
