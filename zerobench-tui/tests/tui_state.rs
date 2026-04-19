//! Integration tests for [`zerobench_tui::state::DashboardState`].
//!
//! The unit tests inside `state.rs` exercise basic construction and
//! ingest bookkeeping. This file focuses on the derived-value APIs
//! the render layer depends on: rolling histogram merges, sparkline
//! windowing, delta indicator, bounded-ring eviction, tab switching,
//! peak/min tracking, and cumulative bytes.

use std::time::Duration;

use hdrhistogram::Histogram;
use zerobench_core::live_snapshot::LiveTick;
use zerobench_core::stats::ErrorCounters;
use zerobench_tui::state::{
    DashboardState, RunMode, Tab, TransportInfo, DELTA_LOOKBACK, MAX_TICKS,
    ROLLING_LATENCY_WINDOW,
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
        per_scenario: Vec::new(),
    }
}

fn tick_with_bytes(
    elapsed_s: u64,
    requests: u64,
    latency_ns: u64,
    bytes_sent: u64,
    bytes_recv: u64,
) -> LiveTick {
    let mut t = tick(elapsed_s, requests, latency_ns);
    t.bytes_sent = bytes_sent;
    t.bytes_recv = bytes_recv;
    t
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

fn fixture_transport() -> TransportInfo {
    TransportInfo {
        mode: RunMode::Saturate(100),
        connections: 100,
        protocol: "H2".into(),
        tls: true,
        alpn: Some("h2".into()),
    }
}

fn fresh_state(target_rate: Option<f64>, dur_s: u64) -> DashboardState {
    DashboardState::new(
        target_rate,
        Duration::from_secs(dur_s),
        "http://api".into(),
        fixture_transport(),
        vec![],
    )
}

// ---------------------------------------------------------------------------
// Totals / RPS / progress
// ---------------------------------------------------------------------------

#[test]
fn totals_accumulate_across_ticks() {
    let mut s = fresh_state(Some(1000.0), 10);
    s.ingest(tick(1, 500, 100_000));
    s.ingest(tick(2, 600, 120_000));
    s.ingest(tick(3, 700, 140_000));
    assert_eq!(s.total_requests, 1800);
    // rps is the *last* tick's request count.
    assert_eq!(s.requests_per_sec(), 700.0);
}

#[test]
fn errors_accumulate_across_ticks() {
    let mut s = fresh_state(None, 10);
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
    let mut s = fresh_state(Some(1000.0), 10);
    s.ingest(tick(1, 994, 100_000));
    let pct = s.actual_vs_target_pct().unwrap();
    assert!((pct - 99.4).abs() < 0.01, "got {pct}");
}

#[test]
fn actual_vs_target_pct_none_for_saturate() {
    let mut s = fresh_state(None, 10);
    s.ingest(tick(1, 994, 100_000));
    assert!(s.actual_vs_target_pct().is_none());
}

// ---------------------------------------------------------------------------
// Sparkline data
// ---------------------------------------------------------------------------

#[test]
fn sparkline_returns_data_newest_last() {
    let mut s = fresh_state(None, 10);
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
    let mut s = fresh_state(None, 60);

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
    let s = fresh_state(None, 10);
    assert!(s.rolling_latency().is_none());
}

// ---------------------------------------------------------------------------
// Delta indicator
// ---------------------------------------------------------------------------

#[test]
fn p99_9_delta_reports_regression_after_lookback() {
    let mut s = fresh_state(None, 60);

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

    let mut s = fresh_state(None, 60);

    for i in 0..DELTA_LOOKBACK {
        s.ingest(tick(i as u64, 100, 1_000_000));
    }

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
    let mut s = fresh_state(None, 10_000);
    // Push MAX_TICKS + 5 ticks — len should stabilise at MAX_TICKS.
    for i in 0..(MAX_TICKS + 5) {
        s.ingest(tick(i as u64, i as u64, 1_000));
    }
    assert_eq!(s.ticks.len(), MAX_TICKS);

    let oldest = s.ticks.front().unwrap();
    assert_eq!(oldest.requests, 5);
    let newest = s.ticks.back().unwrap();
    assert_eq!(newest.requests, (MAX_TICKS + 4) as u64);

    // total_requests is cumulative — not truncated.
    let expected_total: u64 = (0..(MAX_TICKS + 5) as u64).sum();
    assert_eq!(s.total_requests, expected_total);
}

// ---------------------------------------------------------------------------
// Peak / min rps tracking
// ---------------------------------------------------------------------------

#[test]
fn peak_tracks_highest_observed_rps() {
    let mut s = fresh_state(None, 30);
    s.ingest(tick(1, 100, 1_000));
    s.ingest(tick(2, 500, 1_000));
    s.ingest(tick(3, 300, 1_000));
    assert_eq!(s.peak_rps, 500.0);
}

#[test]
fn min_skips_zero_ticks_during_warmup() {
    // An empty "ramp up" shouldn't pin min_rps to 0 forever.
    let mut s = fresh_state(None, 30);
    s.ingest(tick(0, 0, 1_000));
    s.ingest(tick(1, 0, 1_000));
    assert!(s.min_rps.is_none());

    s.ingest(tick(2, 200, 1_000));
    s.ingest(tick(3, 400, 1_000));
    assert_eq!(s.min_rps, Some(200.0));
}

#[test]
fn reset_peaks_clears_both_trackers() {
    let mut s = fresh_state(None, 30);
    s.ingest(tick(1, 200, 1_000));
    s.ingest(tick(2, 500, 1_000));
    assert_eq!(s.peak_rps, 500.0);
    assert_eq!(s.min_rps, Some(200.0));

    s.reset_peaks();
    assert_eq!(s.peak_rps, 0.0);
    assert!(s.min_rps.is_none());
}

// ---------------------------------------------------------------------------
// Cumulative bytes
// ---------------------------------------------------------------------------

#[test]
fn cumulative_bytes_accumulate() {
    let mut s = fresh_state(None, 30);
    s.ingest(tick_with_bytes(1, 100, 1_000, 1_000, 10_000));
    s.ingest(tick_with_bytes(2, 100, 1_000, 2_000, 20_000));
    s.ingest(tick_with_bytes(3, 100, 1_000, 3_000, 30_000));
    assert_eq!(s.cumulative_bytes_sent, 6_000);
    assert_eq!(s.cumulative_bytes_recv, 60_000);
}

// ---------------------------------------------------------------------------
// TickRecord percentile cache
// ---------------------------------------------------------------------------

#[test]
fn tickrecord_caches_p50_p90_p99_p99_9() {
    // Build a tick with enough spread for the percentiles to diverge.
    // 100 samples at 1ms, 10 at 10ms, 1 at 100ms.
    let mut h = fresh_hist();
    for _ in 0..100 {
        let _ = h.record(1_000_000);
    }
    for _ in 0..10 {
        let _ = h.record(10_000_000);
    }
    let _ = h.record(100_000_000);
    let live = LiveTick {
        elapsed: Duration::from_secs(1),
        requests: 111,
        bytes_sent: 0,
        bytes_recv: 0,
        errors: ErrorCounters::default(),
        latency: h,
        per_scenario: Vec::new(),
    };

    let mut s = fresh_state(None, 30);
    s.ingest(live);
    let t = s.ticks.back().unwrap();
    // p50 should be in the 1ms cluster.
    assert!(t.p50_ns <= 1_500_000, "p50 = {}", t.p50_ns);
    // p99.9 should be the 100ms outlier (hdrhist rounds into its
    // bucket; exact value is ~100ms).
    assert!(t.p99_9_ns >= 10_000_000, "p99.9 = {}", t.p99_9_ns);
    // p90 / p99 should sit between.
    assert!(t.p90_ns >= t.p50_ns);
    assert!(t.p99_ns >= t.p90_ns);
}

// ---------------------------------------------------------------------------
// Tab enum helpers
// ---------------------------------------------------------------------------

#[test]
fn tab_next_and_prev_wrap() {
    assert_eq!(Tab::Overview.next(), Tab::Latency);
    assert_eq!(Tab::Latency.next(), Tab::Throughput);
    assert_eq!(Tab::Throughput.next(), Tab::Errors);
    assert_eq!(Tab::Errors.next(), Tab::Overview);

    assert_eq!(Tab::Overview.prev(), Tab::Errors);
    assert_eq!(Tab::Errors.prev(), Tab::Throughput);
}

#[test]
fn tab_from_digit_valid_range() {
    assert_eq!(Tab::from_digit(1), Some(Tab::Overview));
    assert_eq!(Tab::from_digit(2), Some(Tab::Latency));
    assert_eq!(Tab::from_digit(3), Some(Tab::Throughput));
    assert_eq!(Tab::from_digit(4), Some(Tab::Errors));
    assert_eq!(Tab::from_digit(0), None);
    assert_eq!(Tab::from_digit(5), None);
    assert_eq!(Tab::from_digit(255), None);
}

#[test]
fn initial_tab_is_overview() {
    let s = fresh_state(None, 30);
    assert_eq!(s.current_tab, Tab::Overview);
    assert!(!s.help_visible);
}

// ---------------------------------------------------------------------------
// Avg rps
// ---------------------------------------------------------------------------

#[test]
fn avg_rps_is_arithmetic_mean_of_tick_requests() {
    let mut s = fresh_state(None, 30);
    s.ingest(tick(1, 100, 1_000));
    s.ingest(tick(2, 200, 1_000));
    s.ingest(tick(3, 300, 1_000));
    assert!((s.avg_rps() - 200.0).abs() < 0.001);
}

#[test]
fn avg_rps_returns_zero_on_empty_ring() {
    let s = fresh_state(None, 30);
    assert_eq!(s.avg_rps(), 0.0);
}
