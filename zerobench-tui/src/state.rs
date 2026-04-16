//! Dashboard state — folds [`LiveTick`]s into a bounded ring of
//! per-second snapshots and exposes the derived figures the renderer
//! needs (sparkline data, rolling-window latency, progress ratio,
//! delta indicators).
//!
//! # Why a ring of ticks rather than a t-digest?
//!
//! An earlier draft of the design mentioned a streaming t-digest; we
//! deliberately keep the simpler structure:
//!
//! - The HDR histograms already live in the tick and cost <30 KiB each.
//! - Merging 5 HDR histograms on every render (10 Hz) is cheaper than
//!   the overhead of a new `tdigest` dep and the fixed work it does
//!   per-sample inside the hot path.
//! - The TUI only needs *recent* percentiles — merging the last N
//!   ticks gives an exact answer for the window.
//!
//! # Eviction
//!
//! `ticks` is a plain `Vec` capped at [`MAX_TICKS`]. On overflow the
//! oldest entry is removed (O(N) amortised by the small cap — 300
//! entries max at a 5-min run). At our tick rate (1 Hz) this is
//! trivial; no need for a specialised ring buffer.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use zerobench_core::live_snapshot::LiveTick;
use zerobench_core::stats::ErrorCounters;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/// How many per-second ticks to keep in the ring. 300 is 5 minutes —
/// more than enough for any live run the TUI is intended to render.
pub const MAX_TICKS: usize = 300;

/// Number of recent ticks the "latency (last 5s)" panel merges over.
pub const ROLLING_LATENCY_WINDOW: usize = 5;

/// Number of ticks back to compare against for the p99.9 delta
/// indicator. The design spec calls it "10s ago"; at 1 tick/s that is
/// 10 ticks back.
pub const DELTA_LOOKBACK: usize = 10;

/// HDR histogram bounds — must match `LiveSnapshot`'s so cloning +
/// merging works without bucket resizing.
const HIST_LO_NS: u64 = 1;
const HIST_HI_NS: u64 = 60_000_000_000;
const HIST_SIG: u8 = 3;

fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO_NS, HIST_HI_NS, HIST_SIG)
        .expect("HDR histogram bounds are valid compile-time constants")
}

// ---------------------------------------------------------------------------
// TickRecord
// ---------------------------------------------------------------------------

/// One per-second window's worth of data retained for the dashboard.
///
/// This is a compact-enough projection of [`LiveTick`] to keep 300 of
/// them resident without worrying about memory. We retain the full
/// latency histogram so the rolling-window merge is exact.
#[derive(Debug, Clone)]
pub struct TickRecord {
    /// Wall-clock offset from run start.
    pub elapsed: Duration,
    /// Requests completed in this 1s window.
    pub requests: u64,
    /// p50 of the window in nanoseconds (cached off the histogram for
    /// cheap renderer access).
    pub p50_ns: u64,
    /// p99 of the window in nanoseconds.
    pub p99_ns: u64,
    /// Errors recorded in this window.
    pub errors: ErrorCounters,
    /// Full window histogram — needed for the rolling-5s merge.
    pub latency: Histogram<u64>,
}

impl TickRecord {
    fn from_live(tick: LiveTick) -> Self {
        let p50_ns = if tick.latency.is_empty() {
            0
        } else {
            tick.latency.value_at_percentile(50.0)
        };
        let p99_ns = if tick.latency.is_empty() {
            0
        } else {
            tick.latency.value_at_percentile(99.0)
        };
        Self {
            elapsed: tick.elapsed,
            requests: tick.requests,
            p50_ns,
            p99_ns,
            errors: tick.errors,
            latency: tick.latency,
        }
    }
}

// ---------------------------------------------------------------------------
// DashboardState
// ---------------------------------------------------------------------------

/// Mutable state owned by the TUI render loop.
///
/// Grown by feeding [`LiveTick`]s from the shared `LiveSnapshot` via
/// [`DashboardState::ingest`]; read each frame by the `ui` module.
pub struct DashboardState {
    /// Wall-clock instant at which the TUI loop started.
    pub started_at: Instant,
    /// Target rate in req/s, if the run was open-loop; `None` for
    /// saturate runs.
    pub target_rate: Option<f64>,
    /// Total run duration (plan.duration). Drives the progress bar.
    pub total_duration: Duration,
    /// Human-friendly target URL for the header bar.
    pub url_label: String,

    /// Recent per-second ticks, newest last. Capped at [`MAX_TICKS`].
    pub ticks: VecDeque<TickRecord>,
    /// Cumulative requests across the entire run (summed from ticks).
    pub total_requests: u64,
    /// Cumulative errors across the entire run.
    pub total_errors: ErrorCounters,

    /// p99.9 from ~`DELTA_LOOKBACK` ticks ago, cached for rendering the
    /// delta indicator. `None` until we have enough history.
    pub prev_p99_9_ns: Option<u64>,

    /// User toggled via `p` — renderer skips `terminal.draw` when set.
    pub paused_rendering: bool,
    /// User toggled via `l` — renderer shows a "log (stub)" panel when
    /// set. Phase v0.0.1 has no real log content; this is a
    /// scaffolded toggle for future use.
    pub log_visible: bool,
    /// `q` pressed — main loop breaks on the next iteration.
    pub exit_requested: bool,
}

impl DashboardState {
    /// Fresh state with no ticks.
    pub fn new(
        target_rate: Option<f64>,
        total_duration: Duration,
        url_label: String,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            target_rate,
            total_duration,
            url_label,
            ticks: VecDeque::with_capacity(MAX_TICKS),
            total_requests: 0,
            total_errors: ErrorCounters::default(),
            prev_p99_9_ns: None,
            paused_rendering: false,
            log_visible: false,
            exit_requested: false,
        }
    }

    /// Fold a [`LiveTick`] into state. Updates cumulative counters,
    /// appends to the ring (evicting the oldest when full), and
    /// refreshes the p99.9 delta baseline.
    pub fn ingest(&mut self, tick: LiveTick) {
        // Snapshot the "DELTA_LOOKBACK ticks ago" p99.9 *before* we
        // push the new tick so the baseline stays aligned with the
        // current-window number the UI will compute for this frame.
        if self.ticks.len() >= DELTA_LOOKBACK {
            // Tick at index `len - DELTA_LOOKBACK` is our reference.
            let idx = self.ticks.len() - DELTA_LOOKBACK;
            if let Some(ref_tick) = self.ticks.get(idx) {
                self.prev_p99_9_ns = if ref_tick.latency.is_empty() {
                    Some(0)
                } else {
                    Some(ref_tick.latency.value_at_percentile(99.9))
                };
            }
        }

        self.total_requests += tick.requests;
        self.total_errors.merge(&tick.errors);

        let rec = TickRecord::from_live(tick);
        self.ticks.push_back(rec);
        while self.ticks.len() > MAX_TICKS {
            self.ticks.pop_front();
        }
    }

    /// Requests in the most recent 1s tick. Returns 0 before any tick
    /// has been ingested.
    pub fn requests_per_sec(&self) -> f64 {
        self.ticks
            .back()
            .map(|t| t.requests as f64)
            .unwrap_or(0.0)
    }

    /// Instantaneous rate as a fraction of the target, if a target was
    /// set. `None` for saturate runs. Clamped to [0.0, 2.0] so a
    /// transient overshoot doesn't blow up the gauge width.
    pub fn actual_vs_target_pct(&self) -> Option<f64> {
        let target = self.target_rate?;
        if target <= 0.0 {
            return None;
        }
        let actual = self.requests_per_sec();
        let pct = actual / target * 100.0;
        Some(pct.clamp(0.0, 200.0))
    }

    /// Elapsed wall-clock time since the TUI started.
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Progress ratio in `[0.0, 1.0]`. Saturates at 1.0 once the run
    /// exceeds its planned duration (which happens during the
    /// graceful-shutdown window).
    pub fn progress(&self) -> f64 {
        let total = self.total_duration.as_secs_f64();
        if total <= 0.0 {
            return 1.0;
        }
        (self.elapsed().as_secs_f64() / total).clamp(0.0, 1.0)
    }

    /// Requests-per-tick values in order, oldest first. Suitable for
    /// feeding directly into `Sparkline::data`. Caps at the chart
    /// width the caller provides (so we don't hand the widget a
    /// 300-element vector when the chart is 40 cells wide).
    pub fn sparkline_data(&self, max_points: usize) -> Vec<u64> {
        let n = self.ticks.len().min(max_points);
        self.ticks
            .iter()
            .rev()
            .take(n)
            .map(|t| t.requests)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Merge the last [`ROLLING_LATENCY_WINDOW`] ticks' latency
    /// histograms into a fresh histogram. Returns `None` when no
    /// samples have been recorded yet.
    ///
    /// Cheap: we clone the most recent tick then `add` the others
    /// into it. A full 5-tick merge is ~5 × 30 KiB and runs at most
    /// 10× per second.
    pub fn rolling_latency(&self) -> Option<Histogram<u64>> {
        if self.ticks.is_empty() {
            return None;
        }
        let mut hist = new_hist();
        let start = self.ticks.len().saturating_sub(ROLLING_LATENCY_WINDOW);
        for tick in self.ticks.iter().skip(start) {
            let _ = hist.add(&tick.latency);
        }
        if hist.is_empty() {
            None
        } else {
            Some(hist)
        }
    }

    /// Current-frame p99.9 in nanoseconds, derived from the rolling
    /// window. Returns 0 when no samples exist.
    pub fn rolling_p99_9_ns(&self) -> u64 {
        self.rolling_latency()
            .map(|h| h.value_at_percentile(99.9))
            .unwrap_or(0)
    }

    /// Percent delta of the rolling p99.9 vs `prev_p99_9_ns`.
    /// Positive values = latency regressed (worse), negative = improved.
    /// Returns `None` until we have both a baseline and current
    /// samples.
    pub fn p99_9_delta_pct(&self) -> Option<f64> {
        let baseline = self.prev_p99_9_ns?;
        if baseline == 0 {
            return None;
        }
        let current = self.rolling_p99_9_ns();
        Some((current as f64 - baseline as f64) / baseline as f64 * 100.0)
    }

    /// Error counters from the most recent tick, for the "errors"
    /// panel. Returns `ErrorCounters::default()` when no ticks have
    /// been ingested so the renderer always has something to show.
    pub fn last_tick_errors(&self) -> ErrorCounters {
        self.ticks
            .back()
            .map(|t| t.errors.clone())
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Unit tests — additional integration tests live in tests/tui_state.rs
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tick(
        elapsed_s: u64,
        requests: u64,
        lat_ns: &[u64],
        errors: ErrorCounters,
    ) -> LiveTick {
        let mut lat = new_hist();
        for &n in lat_ns {
            let _ = lat.record(n);
        }
        LiveTick {
            elapsed: Duration::from_secs(elapsed_s),
            requests,
            bytes_sent: 0,
            bytes_recv: 0,
            errors,
            latency: lat,
        }
    }

    #[test]
    fn new_state_is_empty() {
        let s = DashboardState::new(None, Duration::from_secs(30), "x".into());
        assert!(s.ticks.is_empty());
        assert_eq!(s.total_requests, 0);
        assert!(s.rolling_latency().is_none());
        assert_eq!(s.requests_per_sec(), 0.0);
    }

    #[test]
    fn ingest_updates_totals_and_ring() {
        let mut s = DashboardState::new(None, Duration::from_secs(30), "x".into());
        s.ingest(make_tick(1, 100, &[1_000, 2_000], ErrorCounters::default()));
        s.ingest(make_tick(2, 200, &[1_500, 2_500], ErrorCounters::default()));
        assert_eq!(s.ticks.len(), 2);
        assert_eq!(s.total_requests, 300);
        assert_eq!(s.requests_per_sec(), 200.0);
    }

    #[test]
    fn progress_clamped_to_one() {
        let mut s = DashboardState::new(
            None,
            Duration::from_millis(1),
            "x".into(),
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!((s.progress() - 1.0).abs() < 1e-9);
        s.started_at = Instant::now();
        assert!(s.progress() < 0.5);
    }
}
