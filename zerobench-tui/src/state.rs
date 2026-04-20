//! ARCH STATUS: KEEP
//!
//! Consumes LiveTick values produced by the sharded LiveSnapshot
//! (post-move: zerobench-runtime). No architectural changes.
//! See ARCH-REVIEW §7.
//!
//! ----------------------------------------------------------------------
//!
//! Dashboard state — folds [`LiveTick`]s into a bounded ring of
//! per-second snapshots and exposes the derived figures the renderer
//! needs (sparkline data, rolling-window latency, progress ratio,
//! delta indicators, tab selection, peak/min trackers, transport info).
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
//! `ticks` is a plain `VecDeque` capped at [`MAX_TICKS`]. At our tick
//! rate (1 Hz) a 300-element ring covers 5 minutes — more than enough
//! for any live run the TUI is intended to render.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use ratatui::symbols::Marker;
use zerobench_core::stats::ErrorCounters;
use zerobench_runtime::live_snapshot::LiveTick;

/// Maximum number of log entries retained in the dashboard.
const MAX_LOG_ENTRIES: usize = 100;

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
// Log entries
// ---------------------------------------------------------------------------

/// A single log event surfaced in the TUI's log pane. Currently no
/// actual events are emitted from the dispatchers; the structure is
/// here so that when we DO wire log events (assertion failures,
/// connection errors), they appear with zero renderer changes.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Elapsed time since the benchmark started.
    pub timestamp: Duration,
    /// Event category — `"assert"`, `"connect"`, `"timeout"`, etc.
    pub category: String,
    /// Human-readable detail message.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Tabs
// ---------------------------------------------------------------------------

/// Top-level tab — user navigates with `1`/`2`/`3`/`4`, `Tab`, or
/// `Shift-Tab`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Latency,
    Throughput,
    Errors,
}

impl Tab {
    /// Short label used in the tab bar.
    pub const fn label(self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Latency => "Latency",
            Tab::Throughput => "Throughput",
            Tab::Errors => "Errors",
        }
    }

    /// Zero-based index into the ordered tab list — used by the
    /// keyboard handler and the renderer's underline position.
    pub const fn index(self) -> usize {
        match self {
            Tab::Overview => 0,
            Tab::Latency => 1,
            Tab::Throughput => 2,
            Tab::Errors => 3,
        }
    }

    /// Tab numbered 1-4 (as typed on the keyboard). Returns `None` for
    /// out-of-range inputs so the key handler can ignore stray digits.
    pub const fn from_digit(d: u8) -> Option<Self> {
        match d {
            1 => Some(Tab::Overview),
            2 => Some(Tab::Latency),
            3 => Some(Tab::Throughput),
            4 => Some(Tab::Errors),
            _ => None,
        }
    }

    /// All tabs in display order. Used by the tab bar renderer and by
    /// `Tab` / `Shift-Tab` cycling.
    pub const ALL: [Tab; 4] = [Tab::Overview, Tab::Latency, Tab::Throughput, Tab::Errors];

    /// Next tab, wrapping from Errors → Overview.
    pub const fn next(self) -> Self {
        match self {
            Tab::Overview => Tab::Latency,
            Tab::Latency => Tab::Throughput,
            Tab::Throughput => Tab::Errors,
            Tab::Errors => Tab::Overview,
        }
    }

    /// Previous tab, wrapping from Overview → Errors.
    pub const fn prev(self) -> Self {
        match self {
            Tab::Overview => Tab::Errors,
            Tab::Latency => Tab::Overview,
            Tab::Throughput => Tab::Latency,
            Tab::Errors => Tab::Throughput,
        }
    }
}

// ---------------------------------------------------------------------------
// RunMode + TransportInfo
// ---------------------------------------------------------------------------

/// Run-mode classifier for the header subtitle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RunMode {
    /// Closed-loop saturate with the given connection count.
    Saturate(usize),
    /// Open-loop rate-targeted — f64 req/s.
    Rate(f64),
}

/// Snapshot of the transport configuration, shown in the header bar.
///
/// Built once by `main.rs` before the TUI starts; the TUI doesn't
/// mutate it.
#[derive(Debug, Clone)]
pub struct TransportInfo {
    /// Saturate or open-loop rate.
    pub mode: RunMode,
    /// Connection count — mirrored on `Saturate(n)` but also meaningful
    /// for open-loop where it bounds concurrency.
    pub connections: usize,
    /// Protocol label — `"H1"`, `"H2"`, `"H3"`, `"WS"`, `"SSE"`.
    pub protocol: String,
    /// TLS enabled on this run.
    pub tls: bool,
    /// ALPN choice — `"h2"`, `"http/1.1"`, or `None` if the run didn't
    /// negotiate ALPN (plain HTTP, raw WS).
    pub alpn: Option<String>,
}

impl Default for TransportInfo {
    fn default() -> Self {
        Self {
            mode: RunMode::Saturate(1),
            connections: 1,
            protocol: "H1".into(),
            tls: false,
            alpn: None,
        }
    }
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
    /// Bytes sent on-wire in this 1s window.
    pub bytes_sent: u64,
    /// Bytes received on-wire in this 1s window.
    pub bytes_recv: u64,
    /// p50 of the window in nanoseconds (cached off the histogram for
    /// cheap renderer access).
    pub p50_ns: u64,
    /// p90 of the window in nanoseconds.
    pub p90_ns: u64,
    /// p99 of the window in nanoseconds.
    pub p99_ns: u64,
    /// p99.9 of the window in nanoseconds — drives the latency
    /// time-series chart on the Latency tab.
    pub p99_9_ns: u64,
    /// Errors recorded in this window.
    pub errors: ErrorCounters,
    /// Full window histogram — needed for the rolling-5s merge.
    pub latency: Histogram<u64>,
    /// Rolling p99.9 over the `ROLLING_LATENCY_WINDOW` ticks ending at
    /// this tick. Cached at ingest-time so the delta indicator can
    /// compare two same-shaped rolling windows (baseline vs current)
    /// without recomputing the baseline on every frame.
    pub rolling_p99_9_ns: u64,
    /// Per-scenario breakdown for this window. Index = scenario_id.
    pub per_scenario: Vec<ScenarioTickRecord>,
}

/// Per-scenario counters for one tick window — the TUI-facing
/// projection of [`ScenarioTick`]. Percentiles are pre-computed to
/// avoid histogram queries on the render path.
#[derive(Debug, Clone)]
pub struct ScenarioTickRecord {
    pub requests: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub errors: ErrorCounters,
}

impl TickRecord {
    fn from_live(tick: LiveTick) -> Self {
        let (p50_ns, p90_ns, p99_ns, p99_9_ns) = if tick.latency.is_empty() {
            (0, 0, 0, 0)
        } else {
            (
                tick.latency.value_at_percentile(50.0),
                tick.latency.value_at_percentile(90.0),
                tick.latency.value_at_percentile(99.0),
                tick.latency.value_at_percentile(99.9),
            )
        };
        let per_scenario = tick
            .per_scenario
            .iter()
            .map(|s| {
                let (p50, p99) = if s.latency.is_empty() {
                    (0, 0)
                } else {
                    (
                        s.latency.value_at_percentile(50.0),
                        s.latency.value_at_percentile(99.0),
                    )
                };
                ScenarioTickRecord {
                    requests: s.requests,
                    bytes_sent: s.bytes_sent,
                    bytes_recv: s.bytes_recv,
                    p50_ns: p50,
                    p99_ns: p99,
                    errors: s.errors.clone(),
                }
            })
            .collect();
        Self {
            elapsed: tick.elapsed,
            requests: tick.requests,
            bytes_sent: tick.bytes_sent,
            bytes_recv: tick.bytes_recv,
            p50_ns,
            p90_ns,
            p99_ns,
            p99_9_ns,
            errors: tick.errors,
            latency: tick.latency,
            // Set by `DashboardState::ingest` once the tick has been
            // pushed and the rolling window can be merged.
            rolling_p99_9_ns: 0,
            per_scenario,
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
    /// Transport metadata — protocol, connection count, TLS, ALPN.
    pub transport: TransportInfo,

    /// Recent per-second ticks, newest last. Capped at [`MAX_TICKS`].
    pub ticks: VecDeque<TickRecord>,
    /// Cumulative requests across the entire run (summed from ticks).
    pub total_requests: u64,
    /// Cumulative errors across the entire run.
    pub total_errors: ErrorCounters,

    /// Cumulative bytes sent on-wire across the full run.
    pub cumulative_bytes_sent: u64,
    /// Cumulative bytes received on-wire across the full run.
    pub cumulative_bytes_recv: u64,

    /// Peak rps observed since start (or since last `reset_peaks`).
    pub peak_rps: f64,
    /// Min rps observed since start (or since last `reset_peaks`).
    /// `None` until at least one non-zero tick has been ingested — a
    /// warm-up full of zeroes otherwise pins the min at 0 forever.
    pub min_rps: Option<f64>,

    /// Rolling-window p99.9 from `DELTA_LOOKBACK` ticks ago, cached for
    /// rendering the delta indicator. `None` until we have enough
    /// history. This is the *rolling* p99.9 snapshotted at that tick
    /// (not the single-tick p99.9), so it is directly comparable to the
    /// most recent rolling_p99_9_ns for the current frame.
    pub prev_p99_9_ns: Option<u64>,

    /// Current tab — toggled by `1`-`4`, `Tab`, `Shift-Tab`.
    pub current_tab: Tab,
    /// Help overlay visible — toggled by `?`.
    pub help_visible: bool,

    /// User toggled via `p` — renderer skips `terminal.draw` when set.
    pub paused_rendering: bool,
    /// User toggled via `l` — renderer shows a log panel when set.
    /// Currently a placeholder until per-op log lines flow through
    /// `LiveSnapshot`.
    pub log_visible: bool,
    /// `q` pressed — main loop breaks on the next iteration.
    pub exit_requested: bool,
    /// Benchmark run has finished — TUI stays open for inspection.
    /// Set by the main loop when `StopSignal` fires. While true,
    /// the status pill shows "done" and no new ticks are ingested.
    pub run_completed: bool,

    /// User-controlled Y-axis scale multiplier. 1.0 = auto (fit data).
    /// < 1.0 = zoomed in, > 1.0 = zoomed out.
    pub y_scale: f64,

    /// Chart marker style — toggled with `m` between Braille and Dot.
    pub marker: Marker,

    /// Bounded ring of log events shown in the log pane. Capped at
    /// [`MAX_LOG_ENTRIES`]. Currently no events are emitted; the
    /// structure is scaffolded for wiring.
    pub log_entries: VecDeque<LogEntry>,
    /// Scroll offset for the log pane's `List` widget.
    pub log_scroll: usize,

    /// Path of the last saved report — shown in the footer for 3 seconds.
    pub last_save_path: Option<String>,
    /// When the last save happened — drives the 3s fade-out.
    pub last_save_at: Option<Instant>,
    /// Whether to auto-export JSON when the run completes.
    pub auto_export: bool,
    /// User-provided output directory/file for exports.
    pub export_path: Option<std::path::PathBuf>,

    /// Scenario names — set once from the Plan at TUI startup.
    pub scenario_names: Vec<String>,
    /// Per-scenario cumulative request counts.
    pub scenario_total_requests: Vec<u64>,
    /// Per-scenario cumulative errors.
    pub scenario_total_errors: Vec<ErrorCounters>,
}

impl DashboardState {
    /// Fresh state with no ticks.
    pub fn new(
        target_rate: Option<f64>,
        total_duration: Duration,
        url_label: String,
        transport: TransportInfo,
        scenario_names: Vec<String>,
    ) -> Self {
        let n = scenario_names.len();
        Self {
            started_at: Instant::now(),
            target_rate,
            total_duration,
            url_label,
            transport,
            ticks: VecDeque::with_capacity(MAX_TICKS),
            total_requests: 0,
            total_errors: ErrorCounters::default(),
            cumulative_bytes_sent: 0,
            cumulative_bytes_recv: 0,
            peak_rps: 0.0,
            min_rps: None,
            prev_p99_9_ns: None,
            current_tab: Tab::Overview,
            help_visible: false,
            paused_rendering: false,
            log_visible: false,
            exit_requested: false,
            run_completed: false,
            y_scale: 1.0,
            marker: Marker::Braille,
            log_entries: VecDeque::with_capacity(MAX_LOG_ENTRIES),
            log_scroll: 0,
            last_save_path: None,
            last_save_at: None,
            auto_export: true,
            export_path: None,
            scenario_names,
            scenario_total_requests: vec![0u64; n],
            scenario_total_errors: (0..n).map(|_| ErrorCounters::default()).collect(),
        }
    }

    /// Fold a [`LiveTick`] into state. Updates cumulative counters,
    /// appends to the ring (evicting the oldest when full), caches the
    /// rolling p99.9 on the just-pushed tick, refreshes the p99.9
    /// delta baseline, and updates peak/min rps.
    ///
    /// The delta baseline is taken from the *rolling* p99.9 stored on
    /// the tick at `len - DELTA_LOOKBACK` (which was itself computed
    /// over the same [`ROLLING_LATENCY_WINDOW`]-sized window). That
    /// keeps baseline and current both apples-to-apples rolling
    /// windows, instead of comparing a single-tick baseline against a
    /// multi-tick current.
    pub fn ingest(&mut self, tick: LiveTick) {
        self.total_requests += tick.requests;
        self.total_errors.merge(&tick.errors);
        self.cumulative_bytes_sent += tick.bytes_sent;
        self.cumulative_bytes_recv += tick.bytes_recv;

        // Track peak / min rps using this tick's delta.
        let rps = tick.requests as f64;
        if rps > self.peak_rps {
            self.peak_rps = rps;
        }
        // Only update min once a non-zero tick has arrived — otherwise
        // the warm-up window pins min at 0 permanently.
        if rps > 0.0 {
            self.min_rps = Some(match self.min_rps {
                Some(prev) => prev.min(rps),
                None => rps,
            });
        }

        let rec = TickRecord::from_live(tick);
        // Accumulate per-scenario totals.
        for (i, st) in rec.per_scenario.iter().enumerate() {
            if i < self.scenario_total_requests.len() {
                self.scenario_total_requests[i] += st.requests;
                self.scenario_total_errors[i].merge(&st.errors);
            }
        }
        self.ticks.push_back(rec);
        while self.ticks.len() > MAX_TICKS {
            self.ticks.pop_front();
        }

        // Compute the rolling-window p99.9 *ending at the just-pushed
        // tick* and store it on the record. This is the same
        // calculation `rolling_latency` performs; we duplicate a bit
        // of code here to avoid borrow-checker issues around mutating
        // the tick we just pushed while iterating the ring. Work is
        // O(ROLLING_LATENCY_WINDOW) histogram merges once per second —
        // trivial.
        let rolling_p99_9 = self.compute_rolling_p99_9_ns();
        if let Some(last) = self.ticks.back_mut() {
            last.rolling_p99_9_ns = rolling_p99_9;
        }

        // Refresh the delta baseline: look `DELTA_LOOKBACK` ticks back
        // and pull the already-cached rolling p99.9 (which was itself a
        // rolling window at that time). That gives us a symmetric
        // comparison — rolling-now vs rolling-then.
        if self.ticks.len() >= DELTA_LOOKBACK {
            let idx = self.ticks.len() - DELTA_LOOKBACK;
            if let Some(ref_tick) = self.ticks.get(idx) {
                self.prev_p99_9_ns = Some(ref_tick.rolling_p99_9_ns);
            }
        }
    }

    /// Reset peak / min rps trackers. Bound to the `r` keybind so the
    /// user can re-arm them after a warm-up or a load transition.
    pub fn reset_peaks(&mut self) {
        self.peak_rps = 0.0;
        self.min_rps = None;
    }

    /// Internal helper: merge the rolling window of histograms and
    /// return the p99.9 in nanoseconds. Returns 0 when the window is
    /// empty. Used at ingest time to cache the value on `TickRecord`.
    fn compute_rolling_p99_9_ns(&self) -> u64 {
        if self.ticks.is_empty() {
            return 0;
        }
        let mut hist = new_hist();
        let start = self.ticks.len().saturating_sub(ROLLING_LATENCY_WINDOW);
        for tick in self.ticks.iter().skip(start) {
            let _ = hist.add(&tick.latency);
        }
        if hist.is_empty() {
            0
        } else {
            hist.value_at_percentile(99.9)
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

    /// Average rps across all ticks in the ring. Returns 0 when empty.
    /// Used by the Throughput tab's summary panel.
    pub fn avg_rps(&self) -> f64 {
        if self.ticks.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.ticks.iter().map(|t| t.requests).sum();
        sum as f64 / self.ticks.len() as f64
    }

    /// Instantaneous rate as a fraction of the target, if a target was
    /// set. `None` for saturate runs. Clamped to [0.0, 200.0] so a
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
    ///
    /// After `ingest` has run, the last tick's `rolling_p99_9_ns` is
    /// already the value we want — no need to remerge at render time.
    pub fn rolling_p99_9_ns(&self) -> u64 {
        self.ticks.back().map(|t| t.rolling_p99_9_ns).unwrap_or(0)
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

    /// Per-second byte rates from the most recent tick. Returns
    /// `(bytes_sent, bytes_recv)` or `(0, 0)` before any tick.
    pub fn last_tick_bytes(&self) -> (u64, u64) {
        self.ticks
            .back()
            .map(|t| (t.bytes_sent, t.bytes_recv))
            .unwrap_or((0, 0))
    }

    /// Per-scenario rps from the most recent tick.
    pub fn scenario_rps(&self, idx: usize) -> f64 {
        self.ticks
            .back()
            .and_then(|t| t.per_scenario.get(idx))
            .map(|s| s.requests as f64)
            .unwrap_or(0.0)
    }

    /// Per-scenario p99 from the most recent tick (nanoseconds).
    pub fn scenario_p99_ns(&self, idx: usize) -> u64 {
        self.ticks
            .back()
            .and_then(|t| t.per_scenario.get(idx))
            .map(|s| s.p99_ns)
            .unwrap_or(0)
    }

    /// Append a log entry to the ring, evicting the oldest when full.
    pub fn push_log(&mut self, entry: LogEntry) {
        self.log_entries.push_back(entry);
        while self.log_entries.len() > MAX_LOG_ENTRIES {
            self.log_entries.pop_front();
        }
    }

    /// Scroll the log pane up by one line (toward older entries).
    pub fn log_scroll_up(&mut self) {
        self.log_scroll = self.log_scroll.saturating_add(1);
    }

    /// Scroll the log pane down by one line (toward newer entries).
    pub fn log_scroll_down(&mut self) {
        self.log_scroll = self.log_scroll.saturating_sub(1);
    }

    /// Jump the log pane to the oldest entry.
    pub fn log_scroll_top(&mut self) {
        if !self.log_entries.is_empty() {
            self.log_scroll = self.log_entries.len().saturating_sub(1);
        }
    }

    /// Jump the log pane to the newest entry.
    pub fn log_scroll_bottom(&mut self) {
        self.log_scroll = 0;
    }
}

// ---------------------------------------------------------------------------
// Unit tests — additional integration tests live in tests/tui_state.rs
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_transport() -> TransportInfo {
        TransportInfo {
            mode: RunMode::Saturate(100),
            connections: 100,
            protocol: "H2".into(),
            tls: true,
            alpn: Some("h2".into()),
        }
    }

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
            per_scenario: Vec::new(),
        }
    }

    #[test]
    fn new_state_is_empty() {
        let s = DashboardState::new(
            None,
            Duration::from_secs(30),
            "x".into(),
            fixture_transport(),
            vec![],
        );
        assert!(s.ticks.is_empty());
        assert_eq!(s.total_requests, 0);
        assert!(s.rolling_latency().is_none());
        assert_eq!(s.requests_per_sec(), 0.0);
        assert_eq!(s.peak_rps, 0.0);
        assert!(s.min_rps.is_none());
        assert_eq!(s.current_tab, Tab::Overview);
        assert!(!s.help_visible);
    }

    #[test]
    fn ingest_updates_totals_and_ring() {
        let mut s = DashboardState::new(
            None,
            Duration::from_secs(30),
            "x".into(),
            fixture_transport(),
            vec![],
        );
        s.ingest(make_tick(1, 100, &[1_000, 2_000], ErrorCounters::default()));
        s.ingest(make_tick(2, 200, &[1_500, 2_500], ErrorCounters::default()));
        assert_eq!(s.ticks.len(), 2);
        assert_eq!(s.total_requests, 300);
        assert_eq!(s.requests_per_sec(), 200.0);
        assert_eq!(s.peak_rps, 200.0);
        assert_eq!(s.min_rps, Some(100.0));
    }

    #[test]
    fn progress_clamped_to_one() {
        let mut s = DashboardState::new(
            None,
            Duration::from_millis(1),
            "x".into(),
            fixture_transport(),
            vec![],
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!((s.progress() - 1.0).abs() < 1e-9);
        s.started_at = Instant::now();
        assert!(s.progress() < 0.5);
    }

    #[test]
    fn tab_cycling_wraps() {
        assert_eq!(Tab::Overview.next(), Tab::Latency);
        assert_eq!(Tab::Errors.next(), Tab::Overview);
        assert_eq!(Tab::Overview.prev(), Tab::Errors);
        assert_eq!(Tab::Errors.prev(), Tab::Throughput);
    }

    #[test]
    fn tab_from_digit_rejects_out_of_range() {
        assert_eq!(Tab::from_digit(0), None);
        assert_eq!(Tab::from_digit(1), Some(Tab::Overview));
        assert_eq!(Tab::from_digit(4), Some(Tab::Errors));
        assert_eq!(Tab::from_digit(5), None);
    }

    #[test]
    fn reset_peaks_clears_both() {
        let mut s = DashboardState::new(
            None,
            Duration::from_secs(30),
            "x".into(),
            fixture_transport(),
            vec![],
        );
        s.ingest(make_tick(1, 100, &[1_000], ErrorCounters::default()));
        s.ingest(make_tick(2, 500, &[1_000], ErrorCounters::default()));
        assert_eq!(s.peak_rps, 500.0);
        assert_eq!(s.min_rps, Some(100.0));

        s.reset_peaks();
        assert_eq!(s.peak_rps, 0.0);
        assert!(s.min_rps.is_none());
    }

    #[test]
    fn per_scenario_accumulates_across_ticks() {
        use zerobench_runtime::live_snapshot::ScenarioTick;

        fn make_scenario_tick(
            elapsed_s: u64,
            requests: u64,
            per_scenario: Vec<ScenarioTick>,
        ) -> LiveTick {
            let mut lat = new_hist();
            for _ in 0..requests {
                let _ = lat.record(1_000);
            }
            LiveTick {
                elapsed: Duration::from_secs(elapsed_s),
                requests,
                bytes_sent: 0,
                bytes_recv: 0,
                errors: ErrorCounters::default(),
                latency: lat,
                per_scenario,
            }
        }

        fn scenario_tick(requests: u64, lat_ns: u64, errors: ErrorCounters) -> ScenarioTick {
            let mut lat = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
            for _ in 0..requests {
                let _ = lat.record(lat_ns);
            }
            ScenarioTick {
                requests,
                bytes_sent: requests * 10,
                bytes_recv: requests * 20,
                errors,
                latency: lat,
            }
        }

        let mut s = DashboardState::new(
            None,
            Duration::from_secs(30),
            "x".into(),
            fixture_transport(),
            vec!["login".into(), "browse".into()],
        );

        // Tick 1: scenario 0 has 100 reqs, scenario 1 has 50.
        s.ingest(make_scenario_tick(
            1,
            150,
            vec![
                scenario_tick(100, 1_000, ErrorCounters::default()),
                scenario_tick(50, 2_000, ErrorCounters::default()),
            ],
        ));
        assert_eq!(s.scenario_total_requests[0], 100);
        assert_eq!(s.scenario_total_requests[1], 50);
        assert_eq!(s.scenario_rps(0), 100.0);
        assert_eq!(s.scenario_rps(1), 50.0);

        // Tick 2: scenario 0 has 200, scenario 1 has 80 + 1 error.
        let mut errs = ErrorCounters::default();
        errs.status_4xx = 1;
        s.ingest(make_scenario_tick(
            2,
            280,
            vec![
                scenario_tick(200, 1_500, ErrorCounters::default()),
                scenario_tick(80, 2_500, errs),
            ],
        ));
        assert_eq!(s.scenario_total_requests[0], 300);
        assert_eq!(s.scenario_total_requests[1], 130);
        assert_eq!(s.scenario_total_errors[1].status_4xx, 1);
        assert_eq!(s.scenario_rps(0), 200.0);
        assert_eq!(s.scenario_rps(1), 80.0);
        assert!(s.scenario_p99_ns(0) > 0);
    }
}
