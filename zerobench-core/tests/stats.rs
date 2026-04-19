//! Integration tests for the stats primitives.
//!
//! These ensure that nanosecond precision, per-scenario roll-up, and
//! merge semantics all behave as the reporter will eventually rely on.

use std::time::Duration;

use zerobench_core::{ErrorCounters, ErrorKind, Summary, TaskStats};

#[test]
fn records_sub_microsecond_samples_without_rounding_up() {
    // 500ns samples must not round to 1µs — this is the core HDR
    // precision guarantee for local-loopback benchmarking.
    let mut s = TaskStats::new(1);
    for _ in 0..10_000 {
        s.record(0, Duration::from_nanos(500), Duration::from_nanos(500), 0, 0);
    }
    let p50 = s.latency.value_at_percentile(50.0);
    // With 3 significant figures and a value of 500ns, HDR's precision
    // window is ±0.5 of the significand; 500 is exactly representable.
    assert!(
        (495..=505).contains(&p50),
        "p50 drifted: expected ~500ns, got {p50}ns",
    );
    assert_eq!(s.requests, 10_000);
}

#[test]
fn merge_3_tasks_sums_requests() {
    let a = {
        let mut t = TaskStats::new(1);
        for _ in 0..1000 {
            t.record(0, Duration::from_micros(100), Duration::from_micros(10), 10, 20);
        }
        t
    };
    let b = {
        let mut t = TaskStats::new(1);
        for _ in 0..1000 {
            t.record(0, Duration::from_micros(200), Duration::from_micros(20), 5, 15);
        }
        t
    };
    let c = {
        let mut t = TaskStats::new(1);
        for _ in 0..1000 {
            t.record(0, Duration::from_micros(300), Duration::from_micros(30), 1, 3);
        }
        t
    };

    let sum = Summary::merge(vec![a, b, c], Duration::from_secs(10));
    assert_eq!(sum.requests, 3000);
    assert_eq!(sum.bytes_sent, 1000 * 10 + 1000 * 5 + 1000 * 1);
    assert_eq!(sum.bytes_recv, 1000 * 20 + 1000 * 15 + 1000 * 3);
    // Per-scenario roll-up.
    assert_eq!(sum.per_scenario[0].requests, 3000);
}

#[test]
fn requests_per_sec_matches_duration() {
    let mut t = TaskStats::new(1);
    for _ in 0..500 {
        t.record(0, Duration::from_micros(100), Duration::from_micros(10), 0, 0);
    }
    let sum = Summary::merge(vec![t], Duration::from_millis(500));
    // 500 requests in 500ms = 1000 rps.
    let rps = sum.requests_per_sec();
    assert!((rps - 1000.0).abs() < 1e-6, "rps={rps}");
}

#[test]
fn requests_per_sec_is_zero_for_zero_duration() {
    let sum = Summary::merge(vec![], Duration::ZERO);
    assert_eq!(sum.requests_per_sec(), 0.0);
}

#[test]
fn error_counters_default_is_zero() {
    let e = ErrorCounters::default();
    assert_eq!(e.connect, 0);
    assert_eq!(e.read, 0);
    assert_eq!(e.write, 0);
    assert_eq!(e.timeout, 0);
    assert_eq!(e.keepup, 0);
    assert_eq!(e.status_4xx, 0);
    assert_eq!(e.status_5xx, 0);
    assert_eq!(e.assertion_failed, 0);
    assert_eq!(e.total(), 0);
}

#[test]
fn error_counters_merge_sums_all_fields() {
    let mut a = ErrorCounters::default();
    a.connect = 1;
    a.read = 2;
    a.write = 3;
    a.timeout = 4;
    a.keepup = 5;
    a.status_4xx = 6;
    a.status_5xx = 7;
    a.assertion_failed = 8;

    let mut b = ErrorCounters::default();
    b.connect = 10;
    b.read = 20;
    b.write = 30;
    b.timeout = 40;
    b.keepup = 50;
    b.status_4xx = 60;
    b.status_5xx = 70;
    b.assertion_failed = 80;

    let mut m = a.clone();
    m.merge(&b);

    assert_eq!(m.connect, 11);
    assert_eq!(m.read, 22);
    assert_eq!(m.write, 33);
    assert_eq!(m.timeout, 44);
    assert_eq!(m.keepup, 55);
    assert_eq!(m.status_4xx, 66);
    assert_eq!(m.status_5xx, 77);
    assert_eq!(m.assertion_failed, 88);
    assert_eq!(m.total(), 11 + 22 + 33 + 44 + 55 + 66 + 77 + 88);
}

#[test]
fn record_error_updates_task_and_scenario_buckets() {
    let mut t = TaskStats::new(2);
    t.record_error(0, ErrorKind::Connect);
    t.record_error(0, ErrorKind::Timeout);
    t.record_error(1, ErrorKind::Status5xx);

    assert_eq!(t.errors.connect, 1);
    assert_eq!(t.errors.timeout, 1);
    assert_eq!(t.errors.status_5xx, 1);
    assert_eq!(t.errors.total(), 3);

    assert_eq!(t.per_scenario[0].errors.connect, 1);
    assert_eq!(t.per_scenario[0].errors.timeout, 1);
    assert_eq!(t.per_scenario[0].errors.status_5xx, 0);
    assert_eq!(t.per_scenario[1].errors.connect, 0);
    assert_eq!(t.per_scenario[1].errors.status_5xx, 1);
}

#[test]
fn per_scenario_stats_roll_up_on_merge() {
    let mut a = TaskStats::new(2);
    for _ in 0..100 {
        a.record(0, Duration::from_micros(50), Duration::from_micros(5), 0, 0);
    }
    for _ in 0..50 {
        a.record(1, Duration::from_micros(500), Duration::from_micros(50), 0, 0);
    }

    let mut b = TaskStats::new(2);
    for _ in 0..200 {
        b.record(0, Duration::from_micros(60), Duration::from_micros(6), 0, 0);
    }
    for _ in 0..25 {
        b.record(1, Duration::from_micros(600), Duration::from_micros(60), 0, 0);
    }

    let sum = Summary::merge(vec![a, b], Duration::from_secs(1));
    assert_eq!(sum.per_scenario.len(), 2);
    assert_eq!(sum.per_scenario[0].requests, 300);
    assert_eq!(sum.per_scenario[1].requests, 75);
    assert_eq!(sum.requests, 375);
}

#[test]
fn latency_p_returns_zero_for_empty_summary() {
    let sum = Summary::merge(vec![], Duration::from_secs(1));
    assert_eq!(sum.latency_p(50.0), Duration::ZERO);
    assert_eq!(sum.latency_p(99.9), Duration::ZERO);
}

#[test]
fn latency_p_returns_nanosecond_durations() {
    let mut t = TaskStats::new(1);
    for _ in 0..10_000 {
        t.record(0, Duration::from_nanos(1_000), Duration::from_nanos(100), 0, 0);
    }
    let sum = Summary::merge(vec![t], Duration::from_secs(1));
    let p50 = sum.latency_p(50.0);
    // Within HDR's 3-sig-fig precision: 1000ns ±0.5.
    let p50_ns = p50.as_nanos() as u64;
    assert!(
        (990..=1010).contains(&p50_ns),
        "p50 drifted: {p50_ns}ns",
    );
}

#[test]
fn clamp_high_samples_to_max_instead_of_dropping() {
    // Extreme latency: 10 minutes. Must not panic; gets clamped to max.
    let mut t = TaskStats::new(1);
    t.record(
        0,
        Duration::from_secs(600),
        Duration::from_secs(600),
        0,
        0,
    );
    assert_eq!(t.requests, 1);
    // Max recorded value must reflect the clamp.
    let max_ns = t.latency.max();
    assert!(max_ns > 0, "expected clamped max, got {max_ns}");
}

#[test]
fn merge_of_zero_tasks_yields_empty_summary() {
    let sum = Summary::merge(vec![], Duration::from_secs(1));
    assert_eq!(sum.requests, 0);
    assert_eq!(sum.bytes_sent, 0);
    assert_eq!(sum.bytes_recv, 0);
    assert_eq!(sum.errors.total(), 0);
    assert!(sum.per_scenario.is_empty());
}

#[test]
fn record_zero_duration_clamps_to_hist_lo_without_panic() {
    // Duration::ZERO is below HDR's minimum recordable value (1ns); it
    // must clamp up to 1 rather than panic or drop the sample.
    let mut s = TaskStats::new(1);
    s.record(0, Duration::ZERO, Duration::ZERO, 0, 0);
    assert_eq!(s.requests, 1);
    // The min recorded value reflects the clamp (HIST_LO_NS == 1).
    assert_eq!(s.latency.min(), 1);
    assert_eq!(s.ttfb.min(), 1);
    // Per-scenario bucket incremented too.
    assert_eq!(s.per_scenario[0].requests, 1);
}

#[test]
fn record_error_with_out_of_range_scenario_id_does_not_panic() {
    // Task-level counter must still increment; per-scenario buckets are
    // silently skipped for unknown scenario IDs (no panic, no bounds
    // growth).
    let mut s = TaskStats::new(2);
    s.record_error(99, ErrorKind::Connect);
    s.record_error(u16::MAX, ErrorKind::Timeout);
    assert_eq!(s.errors.connect, 1);
    assert_eq!(s.errors.timeout, 1);
    assert_eq!(s.errors.total(), 2);
    // Scenario buckets remain untouched.
    assert_eq!(s.per_scenario.len(), 2);
    assert_eq!(s.per_scenario[0].errors.total(), 0);
    assert_eq!(s.per_scenario[1].errors.total(), 0);
}
