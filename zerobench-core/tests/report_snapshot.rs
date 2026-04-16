//! Integration tests for [`zerobench_core::report`].
//!
//! The tests stuff a known set of latencies / error counts / request
//! counts into a [`TaskStats`], merge into a [`Summary`], render via
//! the reporter, and assert on string substrings.
//!
//! We use `contains`-based assertions rather than full snapshots:
//! tiny formatting tweaks (trailing whitespace, paint byte order)
//! shouldn't break the test, but the high-signal strings (p50, request
//! count, `schema_version`) must all appear.

use std::time::Duration;

use serde_json::Value;
use zerobench_core::plan::{
    Assertion, Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::stats::{ErrorKind, Summary, TaskStats};
use zerobench_core::template::Template;
use zerobench_core::var::VarRegistry;
use zerobench_core::{print_json, print_terminal, ColorChoice};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Single-scenario plan with one request and a StatusEq(200) check.
fn sample_plan(duration: Duration) -> Plan {
    let mut vars = VarRegistry::new();
    let url = Template::compile("/", &mut vars).unwrap();
    let mut req = RequestPlan::get(url);
    req.checks = vec![Assertion::StatusEq(200)];
    Plan {
        scenarios: vec![Scenario {
            name: "bench".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![Step::Request(req)],
        }],
        vars,
        duration,
        warmup: None,
    }
}

/// Two-scenario plan for the multi-scenario rendering branch.
fn two_scenario_plan(duration: Duration) -> Plan {
    let mut vars = VarRegistry::new();
    let a = Template::compile("/a", &mut vars).unwrap();
    let b = Template::compile("/b", &mut vars).unwrap();
    Plan {
        scenarios: vec![
            Scenario {
                name: "purchase-flow".into(),
                rate: RateProfile::Saturate { max_concurrency: 50 },
                steps: vec![Step::Request(RequestPlan::get(a))],
            },
            Scenario {
                name: "browse-only".into(),
                rate: RateProfile::Saturate { max_concurrency: 50 },
                steps: vec![Step::Request(RequestPlan::get(b))],
            },
        ],
        vars,
        duration,
        warmup: None,
    }
}

/// Seed a summary with a known distribution so percentile assertions
/// land on predictable values.
fn build_summary(num_scenarios: usize, duration: Duration) -> Summary {
    let mut stats = TaskStats::new(num_scenarios);
    // 1000 samples of 120µs — establishes a p50 at ~120µs.
    for _ in 0..1000 {
        stats.record(
            0,
            Duration::from_micros(120),
            Duration::from_micros(80),
            100,
            50,
        );
    }
    // 50 samples at 2.1ms to push p99.
    for _ in 0..50 {
        stats.record(
            0,
            Duration::from_micros(2_100),
            Duration::from_micros(2_000),
            100,
            50,
        );
    }
    // Put a few samples into scenario 1 if present (two-scenario plan).
    if num_scenarios > 1 {
        for _ in 0..500 {
            stats.record(
                1,
                Duration::from_micros(450),
                Duration::from_micros(300),
                100,
                50,
            );
        }
    }
    // One assertion failure to exercise the nonzero-error rendering.
    stats.record_error(0, ErrorKind::AssertionFailed);

    Summary::merge(vec![stats], duration)
}

// ---------------------------------------------------------------------------
// Terminal reporter
// ---------------------------------------------------------------------------

#[test]
fn terminal_single_scenario_contains_expected_fields() {
    let plan = sample_plan(Duration::from_secs(30));
    let summary = build_summary(1, Duration::from_secs(30));

    let mut out = Vec::new();
    print_terminal(&summary, &plan, ColorChoice::Never, false, &mut out)
        .expect("terminal render");
    let s = String::from_utf8(out).expect("utf8");

    // Header labels.
    assert!(s.contains("target rate"), "missing 'target rate':\n{s}");
    assert!(s.contains("actual rate"), "missing 'actual rate':\n{s}");
    assert!(s.contains("duration"), "missing 'duration':\n{s}");
    assert!(s.contains("latency"), "missing 'latency':\n{s}");
    assert!(s.contains("throughput"), "missing 'throughput':\n{s}");
    assert!(s.contains("errors"), "missing 'errors':\n{s}");
    assert!(s.contains("assertions"), "missing 'assertions':\n{s}");

    // Latency percentiles labels are present.
    assert!(s.contains("p50="), "missing p50:\n{s}");
    assert!(s.contains("p90="), "missing p90:\n{s}");
    assert!(s.contains("p99="), "missing p99:\n{s}");
    assert!(s.contains("p99.9="), "missing p99.9:\n{s}");
    assert!(s.contains("max="), "missing max:\n{s}");

    // Total request count (1000 + 50 = 1050) should render with the
    // thousands-separator formatter.
    assert!(s.contains("1,050"), "expected '1,050' in output:\n{s}");

    // Duration rendered as "30.00s".
    assert!(s.contains("30.00s"), "expected '30.00s' in output:\n{s}");

    // No per-scenario block for a single-scenario plan.
    assert!(!s.contains("scenarios\n"), "unexpected per-scenario block:\n{s}");

    // Color-off: no ANSI codes.
    assert!(!s.contains('\x1b'), "ANSI escape present with ColorChoice::Never:\n{s}");
}

#[test]
fn terminal_multi_scenario_renders_per_scenario_block() {
    let plan = two_scenario_plan(Duration::from_secs(10));
    let summary = build_summary(2, Duration::from_secs(10));

    let mut out = Vec::new();
    print_terminal(&summary, &plan, ColorChoice::Never, false, &mut out)
        .expect("terminal render");
    let s = String::from_utf8(out).expect("utf8");

    // "scenarios" header appears.
    assert!(s.contains("scenarios\n"), "missing 'scenarios' block:\n{s}");
    assert!(s.contains("purchase-flow"), "missing scenario name:\n{s}");
    assert!(s.contains("browse-only"), "missing scenario name:\n{s}");
    // The second scenario's 500 requests should appear in the output.
    assert!(s.contains("500"), "missing request count:\n{s}");
}

#[test]
fn terminal_color_always_produces_ansi_codes() {
    let plan = sample_plan(Duration::from_secs(1));
    let summary = build_summary(1, Duration::from_secs(1));

    let mut out = Vec::new();
    print_terminal(&summary, &plan, ColorChoice::Always, false, &mut out)
        .expect("terminal render");
    let s = String::from_utf8(out).expect("utf8");
    assert!(s.contains('\x1b'), "expected ANSI escape with ColorChoice::Always");
}

// ---------------------------------------------------------------------------
// JSON reporter
// ---------------------------------------------------------------------------

#[test]
fn json_reporter_includes_schema_version_and_core_metrics() {
    let plan = sample_plan(Duration::from_secs(30));
    let summary = build_summary(1, Duration::from_secs(30));

    let mut out = Vec::new();
    print_json(&summary, &plan, &mut out).expect("json render");
    let text = std::str::from_utf8(&out).expect("utf8");
    let v: Value = serde_json::from_str(text).expect("parse json");

    assert_eq!(v["schema_version"], Value::from(1));
    assert_eq!(v["duration_ms"], Value::from(30_000u64));
    assert_eq!(v["requests"], Value::from(1050u64));
    assert_eq!(v["bytes_sent"], Value::from(100 * 1050u64));
    assert_eq!(v["bytes_received"], Value::from(50 * 1050u64));

    // Latency object has all expected keys.
    let lat = &v["latency_ns"];
    assert!(lat["p50"].is_number());
    assert!(lat["p90"].is_number());
    assert!(lat["p99"].is_number());
    assert!(lat["p99_9"].is_number());
    assert!(lat["max"].is_number());

    // Errors object shape.
    let e = &v["errors"];
    assert_eq!(e["assertion_failed"], Value::from(1u64));
    assert_eq!(e["connect"], Value::from(0u64));
    assert_eq!(e["timeout"], Value::from(0u64));
    assert_eq!(e["keepup"], Value::from(0u64));

    // Scenarios array.
    let scenarios = v["scenarios"].as_array().unwrap();
    assert_eq!(scenarios.len(), 1);
    assert_eq!(scenarios[0]["name"], Value::from("bench"));
    assert_eq!(scenarios[0]["requests"], Value::from(1050u64));
}

#[test]
fn json_reporter_target_rate_is_null_in_phase_c() {
    let plan = sample_plan(Duration::from_secs(1));
    let summary = build_summary(1, Duration::from_secs(1));

    let mut out = Vec::new();
    print_json(&summary, &plan, &mut out).expect("json render");
    let v: Value = serde_json::from_slice(&out).expect("parse json");

    // Phase C has no rate profile wiring; the field is present but null.
    assert_eq!(v["target_rate"], Value::Null);
}

#[test]
fn json_reporter_two_scenarios_emits_both() {
    let plan = two_scenario_plan(Duration::from_secs(1));
    let summary = build_summary(2, Duration::from_secs(1));

    let mut out = Vec::new();
    print_json(&summary, &plan, &mut out).expect("json render");
    let v: Value = serde_json::from_slice(&out).expect("parse json");

    let s = v["scenarios"].as_array().unwrap();
    assert_eq!(s.len(), 2);
    assert_eq!(s[0]["name"], Value::from("purchase-flow"));
    assert_eq!(s[1]["name"], Value::from("browse-only"));
}
