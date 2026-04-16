//! Integration tests for [`zerobench_tui::state::DashboardState`].
//!
//! The unit tests inside `state.rs` exercise basic construction and
//! ingest bookkeeping. This file focuses on the derived-value APIs
//! the render layer depends on: rolling histogram merges, sparkline
//! windowing, delta indicator, and bounded-ring eviction.

use std::time::Duration;

use hdrhistogram::Histogram;
use zerobench_core::live_snapshot::LiveTick;
use zerobench_core::stats::ErrorCounters;
use zerobench_tui::state::{
    DashboardState, DELTA_LOOKBACK, MAX_TICKS, ROLLING_LATENCY_WINDOW,
};

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn fresh_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

/// Build a LiveTick with `requests` samples all at `latency_ns`.
fn tick(elapsed_s: u64, requests: u64, latency_ns: u64) -> LiveTick {
    let mut h = fresh_hist();
    for _ in 0..requests {
        let _ = h.record(latency_ns);
    }
    LiveTick {
        elapsed: Duration::from_secs(elapsed_s),
        requests,
        bytes_sent: 0,
        bytes_recv: 0,
        errors: ErrorCounters::default(),
        latency: h,
    }
}

fn tick_with_err(
    elapsed_s: u64,
    requests: u64,
    latency_ns: u64,
    errors: ErrorCounters,
) -> LiveTick {
    let mut base = tick(elapsed_s, requests, latency_ns);
    base.errors = errors;
    base
}

// ---------------------------------------------------------------------------
// Totals / RPS / progress
// ---------------------------------------------------------------------------

#[test]
fn totals_accumulate_across_ticks() {
    let mut s = DashboardState::new(
        Some(1000.0),
        Duration::from_secs(10),
        "http://api".into(),
    );
    s.ingest(tick(1, 500, 100_000));
    s.ingest(tick(2, 600, 120_000));
    s.ingest(tick(3, 700, 140_000));
    assert_eq!(s.total_requests, 1800);
    // rps is the *last* tick's request count.
    assert_eq!(s.requests_per_sec(), 700.0);
}

#[test]
fn errors_accumulate_across_ticks() {
    let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
    let mut e1 = ErrorCounters::default();
    e1.connect = 2;
    e1.timeout = 1;
    let mut e2 = ErrorCounters::default();
    e2.connect = 3;
    e2.status_5xx = 5;

    s.ingest(tick_with_err(1, 10, 1_000, e1));
    s.ingest(tick_with_err(2, 10, 1_000, e2));

    assert_eq!(s.total_errors.connect, 5);
    assert_eq!(s.total_errors.timeout, 1);
    assert_eq!(s.total_errors.status_5xx, 5);
    assert_eq!(s.total_errors.total(), 11);
}

#[test]
fn actual_vs_target_pct() {
    let mut s = DashboardState::new(
        Some(1000.0),
        Duration::from_secs(10),
        "x".into(),
    );
    s.ingest(tick(1, 994, 100_000));
    let pct = s.actual_vs_target_pct().unwrap();
    assert!((pct - 99.4).abs() < 0.01, "got {pct}");
}

#[test]
fn actual_vs_target_pct_none_for_saturate() {
    let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
    s.ingest(tick(1, 994, 100_000));
    assert!(s.actual_vs_target_pct().is_none());
}

// ---------------------------------------------------------------------------
// Sparkline data
// ---------------------------------------------------------------------------

#[test]
fn sparkline_returns_data_newest_last() {
    let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
    s.ingest(tick(1, 100, 1_000));
    s.ingest(tick(2, 200, 1_000));
    s.ingest(tick(3, 300, 1_000));
    s.ingest(tick(4, 400, 1_000));

    let all = s.sparkline_data(10);
    assert_eq!(all, vec![100, 200, 300, 400]);

    // Capped — newest two preserved.
    let cap2 = s.sparkline_data(2);
    assert_eq!(cap2, vec![300, 400]);
}

// ---------------------------------------------------------------------------
// Rolling latency window
// ---------------------------------------------------------------------------

#[test]
fn rolling_latency_merges_last_window_ticks() {
    // Feed ROLLING_LATENCY_WINDOW + 2 ticks; oldest two are outside
    // the window and shouldn't affect the merged histogram.
    let mut s = DashboardState::new(None, Duration::from_secs(60), "x".into());

    // Ticks 1..=2: high-latency outliers that must be evicted from
    // the rolling window. If they leaked in, our p99 would be huge.
    s.ingest(tick(1, 100, 1_000_000_000)); // 1s latency
    s.ingest(tick(2, 100, 1_000_000_000));
    // Ticks 3..=7: ROLLING_LATENCY_WINDOW = 5 recent ticks at 1ms.
    for i in 0..ROLLING_LATENCY_WINDOW {
        s.ingest(tick(3 + i as u64, 100, 1_000_000));
    }

    let rolling = s.rolling_latency().unwrap();
    let p99 = rolling.value_at_percentile(99.0);
    // Should be ~1ms (1_000_000 ns), not near 1s.
    assert!(
        p99 < 2_000_000,
        "rolling p99 leaked outlier ticks: got {p99}ns",
    );
    // Total samples in rolling window = 100 * 5.
    assert_eq!(rolling.len(), 500);
}

#[test]
fn rolling_latency_none_when_empty() {
    let s = DashboardState::new(None, Duration::from_secs(10), "x".into());
    assert!(s.rolling_latency().is_none());
}

// ---------------------------------------------------------------------------
// Delta indicator
// ---------------------------------------------------------------------------

#[test]
fn p99_9_delta_reports_regression_after_lookback() {
    let mut s = DashboardState::new(None, Duration::from_secs(60), "x".into());

    // Fewer than DELTA_LOOKBACK ticks — baseline not yet captured.
    for i in 0..(DELTA_LOOKBACK - 1) {
        s.ingest(tick(i as u64, 100, 1_000_000));
    }
    assert!(s.p99_9_delta_pct().is_none());

    // DELTA_LOOKBACK-th ingest captures the baseline from the oldest
    // retained tick's cached rolling p99.9.
    s.ingest(tick(DELTA_LOOKBACK as u64 - 1, 100, 1_000_000));
    let prev = s.prev_p99_9_ns.unwrap_or(0);
    assert!(prev > 0, "baseline should be captured");

    // Push several ticks at 10ms so the rolling window regresses. We
    // need at least ROLLING_LATENCY_WINDOW so the current rolling
    // window is entirely inside the new high-latency regime.
    for i in 0..(ROLLING_LATENCY_WINDOW + 2) {
        s.ingest(tick(
            DELTA_LOOKBACK as u64 + i as u64,
            100,
            10_000_000,
        ));
    }
    let delta = s.p99_9_delta_pct().unwrap();
    // Rolling p99.9 jumped from ~1ms to ~10ms — expect ~+900%.
    assert!(
        delta > 500.0,
        "expected large positive delta, got {delta}%",
    );
}

#[test]
fn p99_9_delta_uses_symmetric_rolling_windows() {
    // Seed 5 early ticks at 1ms then 5 later ticks at 10ms (with a
    // ROLLING_LATENCY_WINDOW-sized gap between them so neither window
    // is contaminated by the other). Verify the delta reflects the
    // rolling-vs-rolling comparison, not a single-tick spike vs
    // rolling merge.
    assert_eq!(ROLLING_LATENCY_WINDOW, 5);
    assert_eq!(DELTA_LOOKBACK, 10);

    let mut s = DashboardState::new(None, Duration::from_secs(60), "x".into());

    // 10 ticks at 1ms. Once len == DELTA_LOOKBACK, the delta baseline
    // becomes the cached rolling p99.9 of ticks[0], which was itself
    // only a 1-tick window (~1ms). That's fine for this test — the
    // rolling-vs-rolling guarantee is that *both sides* of the ratio
    // are cached values, no longer mismatching 1-tick vs 5-tick.
    for i in 0..DELTA_LOOKBACK {
        s.ingest(tick(i as u64, 100, 1_000_000));
    }

    // Now push 5 more ticks at 10ms. After these, the current rolling
    // window sits entirely at 10ms and the baseline comes from the
    // cached rolling p99.9 of ticks[DELTA_LOOKBACK - 1 + 5 - 10] =
    // ticks[4], whose rolling window was entirely at 1ms.
    for i in 0..ROLLING_LATENCY_WINDOW {
        s.ingest(tick(
            DELTA_LOOKBACK as u64 + i as u64,
            100,
            10_000_000,
        ));
    }

    let baseline = s.prev_p99_9_ns.expect("baseline should be set");
    let current = s.rolling_p99_9_ns();
    assert!(
        baseline < 2_000_000,
        "baseline should reflect the 1ms rolling window, got {baseline}ns",
    );
    assert!(
        current > 5_000_000,
        "current rolling p99.9 should reflect the 10ms regime, got {current}ns",
    );
    let delta = s.p99_9_delta_pct().unwrap();
    assert!(
        delta > 500.0 && delta < 2_000.0,
        "expected rolling-vs-rolling delta near +900%, got {delta}%",
    );
}

// ---------------------------------------------------------------------------
// Bounded ring eviction
// ---------------------------------------------------------------------------

#[test]
fn ring_evicts_oldest_past_max_ticks() {
    let mut s = DashboardState::new(None, Duration::from_secs(10_000), "x".into());
    // Push MAX_TICKS + 5 ticks — len should stabilise at MAX_TICKS.
    for i in 0..(MAX_TICKS + 5) {
        s.ingest(tick(i as u64, i as u64, 1_000));
    }
    assert_eq!(s.ticks.len(), MAX_TICKS);

    // Oldest retained tick should be the one at offset `5`
    // (earlier were evicted), which we can sanity-check via its
    // `requests` field.
    let oldest = s.ticks.front().unwrap();
    assert_eq!(oldest.requests, 5);
    let newest = s.ticks.back().unwrap();
    assert_eq!(newest.requests, (MAX_TICKS + 4) as u64);

    // total_requests is cumulative — not truncated.
    let expected_total: u64 = (0..(MAX_TICKS + 5) as u64).sum();
    assert_eq!(s.total_requests, expected_total);
}
