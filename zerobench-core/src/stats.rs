//! Per-task statistics and end-of-run summary.
//!
//! Each worker owns a [`TaskStats`]; there is no contention on the hot
//! path. On shutdown, the dispatcher collects every task's stats and
//! merges into a [`Summary`].
//!
//! # Histogram bounds
//!
//! Latency/TTFB histograms cover `[1, 60_000_000_000]` nanoseconds — 1ns
//! to 60s — with 3 significant figures. That keeps memory small (~30 KiB
//! per histogram) while preserving sub-microsecond precision, which
//! matters: a 500ns loopback request must not round to 1µs.
//!
//! # Layering
//!
//! [`ScenarioStats`] repeats the task-level structure per scenario so
//! reports can break out results by scenario without re-scanning samples.

use std::time::Duration;

use hdrhistogram::Histogram;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/// Lower bound on recordable latency. HDR requires ≥ 1.
const HIST_LO_NS: u64 = 1;
/// Upper bound on recordable latency — 60 seconds in ns. Samples above
/// this cap are clamped to the max (so an infinite hang still produces a
/// valid histogram).
const HIST_HI_NS: u64 = 60_000_000_000;
/// Significant figures — controls memory vs precision tradeoff.
const HIST_SIG: u8 = 3;

// ---------------------------------------------------------------------------
// ErrorCounters
// ---------------------------------------------------------------------------

/// Category counts; populated by the dispatcher when a transport fails
/// or an assertion fires. Used for the report's "errors" panel.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ErrorCounters {
    /// TCP connect failed (refused, unreachable, DNS, etc).
    pub connect: u64,
    /// Read from the socket failed mid-response.
    pub read: u64,
    /// Write to the socket failed mid-request.
    pub write: u64,
    /// Per-request deadline exceeded.
    pub timeout: u64,
    /// Workers couldn't keep up; token dropped by the rate scheduler.
    pub keepup: u64,
    /// Response status in 400-499.
    pub status_4xx: u64,
    /// Response status in 500-599.
    pub status_5xx: u64,
    /// A user-supplied assertion returned failure.
    pub assertion_failed: u64,
}

impl ErrorCounters {
    /// Add `other` into `self`, field-wise.
    pub fn merge(&mut self, other: &Self) {
        self.connect += other.connect;
        self.read += other.read;
        self.write += other.write;
        self.timeout += other.timeout;
        self.keepup += other.keepup;
        self.status_4xx += other.status_4xx;
        self.status_5xx += other.status_5xx;
        self.assertion_failed += other.assertion_failed;
    }

    /// Total error count across all categories.
    pub fn total(&self) -> u64 {
        self.connect
            + self.read
            + self.write
            + self.timeout
            + self.keepup
            + self.status_4xx
            + self.status_5xx
            + self.assertion_failed
    }

    /// Increment the counter for a given [`ErrorKind`].
    pub fn incr(&mut self, kind: ErrorKind) {
        match kind {
            ErrorKind::Connect => self.connect += 1,
            ErrorKind::Read => self.read += 1,
            ErrorKind::Write => self.write += 1,
            ErrorKind::Timeout => self.timeout += 1,
            ErrorKind::Keepup => self.keepup += 1,
            ErrorKind::Status4xx => self.status_4xx += 1,
            ErrorKind::Status5xx => self.status_5xx += 1,
            ErrorKind::AssertionFailed => self.assertion_failed += 1,
        }
    }
}

/// The kinds of errors tracked in [`ErrorCounters`]. Used as the tag for
/// [`TaskStats::record_error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Connect,
    Read,
    Write,
    Timeout,
    Keepup,
    Status4xx,
    Status5xx,
    AssertionFailed,
}

// ---------------------------------------------------------------------------
// ScenarioStats
// ---------------------------------------------------------------------------

/// Per-scenario slice of the task's statistics.
///
/// One [`ScenarioStats`] per scenario per task; after merge, one per
/// scenario in the final [`Summary`].
#[derive(Debug, Clone)]
pub struct ScenarioStats {
    /// Index into `Plan::scenarios`.
    pub scenario_id: u16,
    /// Completed requests attributed to this scenario.
    pub requests: u64,
    /// Latency histogram in nanoseconds.
    pub latency: Histogram<u64>,
    /// Errors attributed to this scenario.
    pub errors: ErrorCounters,
}

impl ScenarioStats {
    /// Construct an empty stats bucket for `scenario_id`.
    pub fn new(scenario_id: u16) -> Self {
        Self {
            scenario_id,
            requests: 0,
            latency: new_hist(),
            errors: ErrorCounters::default(),
        }
    }

    /// Add `other` into `self`. Both must share the same `scenario_id`;
    /// caller is responsible for that invariant.
    pub fn merge(&mut self, other: &Self) {
        debug_assert_eq!(self.scenario_id, other.scenario_id);
        self.requests += other.requests;
        // `add` returns an error only if `other`'s bounds don't fit
        // `self`'s. Since all our histograms share the same config, this
        // cannot fail; convert a theoretical failure into "skip sample".
        let _ = self.latency.add(&other.latency);
        self.errors.merge(&other.errors);
    }
}

// ---------------------------------------------------------------------------
// TaskStats
// ---------------------------------------------------------------------------

/// Per-worker statistics. One instance per worker task, merged into a
/// [`Summary`] at end-of-run.
#[derive(Debug, Clone)]
pub struct TaskStats {
    /// End-to-end request latency in nanoseconds.
    pub latency: Histogram<u64>,
    /// Time-to-first-byte in nanoseconds.
    pub ttfb: Histogram<u64>,
    /// Total successful requests this task completed.
    pub requests: u64,
    /// Bytes written on-wire (pre-TLS count).
    pub bytes_sent: u64,
    /// Bytes read on-wire (pre-TLS count).
    pub bytes_recv: u64,
    /// Task-level error roll-up across all scenarios.
    pub errors: ErrorCounters,
    /// Per-scenario detail, indexed by `scenario_id`. Length fixed at
    /// construction so updates are O(1) without bounds growth.
    pub per_scenario: Vec<ScenarioStats>,
}

impl TaskStats {
    /// Construct fresh stats for a task that will handle `num_scenarios`
    /// distinct scenarios. Scenario IDs 0..num_scenarios.
    pub fn new(num_scenarios: usize) -> Self {
        Self {
            latency: new_hist(),
            ttfb: new_hist(),
            requests: 0,
            bytes_sent: 0,
            bytes_recv: 0,
            errors: ErrorCounters::default(),
            per_scenario: (0..num_scenarios)
                .map(|i| ScenarioStats::new(i as u16))
                .collect(),
        }
    }

    /// Record a completed request.
    ///
    /// Durations above the histogram's upper bound are clamped to the
    /// max rather than dropped.
    pub fn record(
        &mut self,
        scenario_id: u16,
        latency: Duration,
        ttfb: Duration,
        bytes_sent: u64,
        bytes_recv: u64,
    ) {
        let lat_ns = duration_to_hist_ns(latency);
        let ttfb_ns = duration_to_hist_ns(ttfb);

        // `record` can only fail on values outside configured bounds.
        // `duration_to_hist_ns` clamps, so this never errs, but we
        // ignore the result defensively.
        let _ = self.latency.record(lat_ns);
        let _ = self.ttfb.record(ttfb_ns);

        self.requests += 1;
        self.bytes_sent += bytes_sent;
        self.bytes_recv += bytes_recv;

        if let Some(s) = self.per_scenario.get_mut(scenario_id as usize) {
            s.requests += 1;
            let _ = s.latency.record(lat_ns);
        }
    }

    /// Record an error against the task and (if `scenario_id` is in
    /// range) the scenario.
    pub fn record_error(&mut self, scenario_id: u16, kind: ErrorKind) {
        self.errors.incr(kind);
        if let Some(s) = self.per_scenario.get_mut(scenario_id as usize) {
            s.errors.incr(kind);
        }
    }
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

/// Aggregate statistics for the whole run.
///
/// Produced by [`Summary::merge`]; consumed by the reporter (terminal,
/// JSON, Prometheus, diff tool).
#[derive(Debug, Clone)]
pub struct Summary {
    /// Full-run latency histogram (all scenarios, all tasks).
    pub latency: Histogram<u64>,
    /// Full-run TTFB histogram.
    pub ttfb: Histogram<u64>,
    /// Total requests completed.
    pub requests: u64,
    /// Total bytes written across all connections.
    pub bytes_sent: u64,
    /// Total bytes read across all connections.
    pub bytes_recv: u64,
    /// Category-count error totals.
    pub errors: ErrorCounters,
    /// Per-scenario breakdown, indexed by scenario ID.
    pub per_scenario: Vec<ScenarioStats>,
    /// Measurement duration (excludes warmup).
    pub duration: Duration,
}

impl Summary {
    /// Merge a list of [`TaskStats`] into a single [`Summary`].
    ///
    /// `duration` is the measured wall-clock duration (typically
    /// `plan.duration`, or a shorter window if the user hit ctrl-C).
    ///
    /// Returns a summary with empty histograms if `stats` is empty.
    pub fn merge(stats: Vec<TaskStats>, duration: Duration) -> Self {
        let num_scenarios = stats.first().map(|s| s.per_scenario.len()).unwrap_or(0);

        let mut out = Summary {
            latency: new_hist(),
            ttfb: new_hist(),
            requests: 0,
            bytes_sent: 0,
            bytes_recv: 0,
            errors: ErrorCounters::default(),
            per_scenario: (0..num_scenarios)
                .map(|i| ScenarioStats::new(i as u16))
                .collect(),
            duration,
        };

        for s in stats {
            let _ = out.latency.add(&s.latency);
            let _ = out.ttfb.add(&s.ttfb);
            out.requests += s.requests;
            out.bytes_sent += s.bytes_sent;
            out.bytes_recv += s.bytes_recv;
            out.errors.merge(&s.errors);

            for (i, sc) in s.per_scenario.into_iter().enumerate() {
                if let Some(dst) = out.per_scenario.get_mut(i) {
                    dst.merge(&sc);
                }
            }
        }

        out
    }

    /// Average requests per second over the measurement window.
    ///
    /// Returns `0.0` if `duration` is zero (prevents NaN/infinity in
    /// downstream formatting).
    pub fn requests_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.requests as f64 / secs
        }
    }

    /// Latency at percentile `pct` (e.g. `99.9`), as a [`Duration`].
    ///
    /// Returns `Duration::ZERO` if no samples have been recorded.
    pub fn latency_p(&self, pct: f64) -> Duration {
        if self.latency.is_empty() {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.latency.value_at_percentile(pct))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn new_hist() -> Histogram<u64> {
    // Bounds are compile-time constants known valid; `expect` never fires.
    Histogram::<u64>::new_with_bounds(HIST_LO_NS, HIST_HI_NS, HIST_SIG)
        .expect("HDR histogram bounds are valid compile-time constants")
}

fn duration_to_hist_ns(d: Duration) -> u64 {
    let ns = d.as_nanos();
    if ns < HIST_LO_NS as u128 {
        HIST_LO_NS
    } else if ns > HIST_HI_NS as u128 {
        HIST_HI_NS
    } else {
        ns as u64
    }
}

// ---------------------------------------------------------------------------
// Unit tests (integration tests in tests/stats.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_clamps_below_lo() {
        assert_eq!(duration_to_hist_ns(Duration::from_nanos(0)), HIST_LO_NS);
    }

    #[test]
    fn duration_clamps_above_hi() {
        assert_eq!(
            duration_to_hist_ns(Duration::from_secs(120)),
            HIST_HI_NS
        );
    }

    #[test]
    fn duration_preserves_mid_range() {
        assert_eq!(
            duration_to_hist_ns(Duration::from_nanos(500)),
            500
        );
    }

    #[test]
    fn error_counters_total() {
        let mut e = ErrorCounters::default();
        e.connect = 1;
        e.timeout = 2;
        e.status_4xx = 3;
        assert_eq!(e.total(), 6);
    }
}
