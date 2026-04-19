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
use serde::{Deserialize, Serialize};

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
    /// Completed requests/operations attributed to this scenario.
    ///
    /// The unit depends on the scenario's [`Protocol`](crate::plan::Protocol):
    /// - HTTP: completed request/response pairs
    /// - SSE: completed streams (not each chunk — use `sse.chunks` for that)
    /// - WS: completed message round-trips
    pub requests: u64,
    /// Latency histogram in nanoseconds.
    ///
    /// Per protocol:
    /// - HTTP: end-to-end request → response latency
    /// - SSE: full stream duration (open → close)
    /// - WS: per-message round-trip time
    pub latency: Histogram<u64>,
    /// Errors attributed to this scenario.
    pub errors: ErrorCounters,
    /// SSE-specific counters and histograms, `Some` iff this scenario's
    /// protocol is [`Protocol::Sse`](crate::plan::Protocol::Sse).
    pub sse: Option<SseExtras>,
    /// WebSocket-specific counters and histograms, `Some` iff this
    /// scenario's protocol is [`Protocol::Ws`](crate::plan::Protocol::Ws).
    pub ws: Option<WsExtras>,
}

/// SSE-specific per-scenario metrics that don't fit the generic
/// request/latency layout.
#[derive(Debug, Clone)]
pub struct SseExtras {
    /// Time-to-first-byte for the SSE response — from request-write
    /// completion to the first response byte.
    pub ttfb: Histogram<u64>,
    /// Inter-chunk gap between successive data events within a stream.
    pub chunk_gap: Histogram<u64>,
    /// Total number of data events received across all streams.
    pub chunks: u64,
    /// Streams that saw either a `[DONE]` terminator or a clean server
    /// close. Cheaper than `requests` when you want to exclude streams
    /// that errored mid-flight.
    pub streams_completed: u64,
    /// Payload bytes received on SSE streams (post-chunked-decoding).
    pub bytes_received: u64,
}

impl Default for SseExtras {
    fn default() -> Self {
        Self {
            ttfb: new_hist(),
            chunk_gap: new_hist(),
            chunks: 0,
            streams_completed: 0,
            bytes_received: 0,
        }
    }
}

impl SseExtras {
    /// Merge `other` into `self`. Histograms add; counters sum.
    pub fn merge(&mut self, other: &Self) {
        let _ = self.ttfb.add(&other.ttfb);
        let _ = self.chunk_gap.add(&other.chunk_gap);
        self.chunks += other.chunks;
        self.streams_completed += other.streams_completed;
        self.bytes_received += other.bytes_received;
    }
}

/// WebSocket-specific per-scenario metrics.
#[derive(Debug, Clone)]
pub struct WsExtras {
    /// Handshake time — TCP connect to 101 Switching Protocols.
    pub handshake: Histogram<u64>,
    /// Per-message round-trip time.
    pub rtt: Histogram<u64>,
    /// Text/Binary frames sent from this client.
    pub messages_sent: u64,
    /// Text/Binary frames received from the server.
    pub messages_recv: u64,
    /// Payload bytes sent (excluding frame headers).
    pub bytes_sent: u64,
    /// Payload bytes received.
    pub bytes_recv: u64,
}

impl Default for WsExtras {
    fn default() -> Self {
        Self {
            handshake: new_hist(),
            rtt: new_hist(),
            messages_sent: 0,
            messages_recv: 0,
            bytes_sent: 0,
            bytes_recv: 0,
        }
    }
}

impl WsExtras {
    /// Merge `other` into `self`. Histograms add; counters sum.
    pub fn merge(&mut self, other: &Self) {
        let _ = self.handshake.add(&other.handshake);
        let _ = self.rtt.add(&other.rtt);
        self.messages_sent += other.messages_sent;
        self.messages_recv += other.messages_recv;
        self.bytes_sent += other.bytes_sent;
        self.bytes_recv += other.bytes_recv;
    }
}

impl ScenarioStats {
    /// Construct an empty stats bucket for `scenario_id`.
    pub fn new(scenario_id: u16) -> Self {
        Self {
            scenario_id,
            requests: 0,
            latency: new_hist(),
            errors: ErrorCounters::default(),
            sse: None,
            ws: None,
        }
    }

    /// Ensure this scenario has SSE extras, creating defaults on demand.
    ///
    /// Called by the SSE backend on first event it attributes to this
    /// scenario. Lets us keep the `Option<_>` hygiene — bytes-only /
    /// HTTP scenarios get `None` and don't allocate histograms.
    pub fn sse_mut(&mut self) -> &mut SseExtras {
        self.sse.get_or_insert_with(SseExtras::default)
    }

    /// Ensure this scenario has WS extras, creating defaults on demand.
    pub fn ws_mut(&mut self) -> &mut WsExtras {
        self.ws.get_or_insert_with(WsExtras::default)
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
        if let Some(o) = &other.sse {
            self.sse_mut().merge(o);
        }
        if let Some(o) = &other.ws {
            self.ws_mut().merge(o);
        }
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

    /// Merge `other` into `self`. The two must share the same
    /// `per_scenario.len()` — i.e. they come from runs that built
    /// `TaskStats::new` with the same scenario count. Used by the CLI
    /// dispatcher to combine per-backend stats into one task.
    pub fn merge(&mut self, other: &Self) {
        let _ = self.latency.add(&other.latency);
        let _ = self.ttfb.add(&other.ttfb);
        self.requests += other.requests;
        self.bytes_sent += other.bytes_sent;
        self.bytes_recv += other.bytes_recv;
        self.errors.merge(&other.errors);
        for (i, sc) in other.per_scenario.iter().enumerate() {
            if let Some(dst) = self.per_scenario.get_mut(i) {
                dst.merge(sc);
            }
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
// Export — serde-compatible projection of Summary for result.json.
//
// HDR histograms don't implement Serialize; the canonical archival
// format is an `.histlog` V2 compressed log (§PHILOSOPHY P3). For
// Phase 5b we emit JSON-friendly percentiles + counts — sufficient
// for the diff / replay fast paths. Phase 5c will add the .histlog
// alongside (both derived from the same Histogram source of truth).
// ---------------------------------------------------------------------------

/// Serialisable projection of [`Summary`] — the content of
/// `result.json` in the archive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryExport {
    /// Schema version — bumps on rename / retype / remove.
    pub schema_version: u32,
    /// Measurement duration in nanoseconds. Excludes warmup.
    pub duration_ns: u64,
    /// Completed operations count (HTTP req/resp, SSE stream, WS
    /// round). See protocol-specific meaning on [`ScenarioStats`].
    pub requests: u64,
    /// Average rate over the measurement window (req/s).
    pub rate_per_s: f64,
    /// Bytes written across all connections.
    pub bytes_sent: u64,
    /// Bytes read across all connections.
    pub bytes_recv: u64,
    /// Overall latency percentile breakdown.
    pub latency: LatencyExport,
    /// Overall TTFB percentile breakdown (HTTP only — zeros for
    /// non-HTTP runs).
    pub ttfb: LatencyExport,
    /// Error category counts.
    pub errors: ErrorCountersExport,
    /// Per-scenario breakdown.
    pub scenarios: Vec<ScenarioExport>,
    /// Per-run metric vectors, one entry per individual run when
    /// `--runs N > 1`. Empty when the caller merged all runs into a
    /// single aggregate (e.g. `probe`, `--runs 1`). Bootstrap CI
    /// (§9.3 `run-bootstrap` strategy) resamples these values when
    /// both sides of a `compare` have `per_run.len() ≥ 3`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_run: Vec<PerRunMetrics>,
}

/// Metrics captured from a single run — the elementary unit the
/// run-level bootstrap resamples over. One entry per `--runs` loop
/// iteration in `measure`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerRunMetrics {
    /// Run index (0-based) within this `measure` invocation.
    pub index: u32,
    /// Achieved rate (req/s) over the run's duration.
    pub rate_per_s: f64,
    /// Run-level request count.
    pub requests: u64,
    /// Run-level error total (sum across categories).
    pub errors_total: u64,
    /// Run-level latency percentiles. Extracted from the run's
    /// HDR histogram before it was folded into the aggregate.
    pub latency: LatencyExport,
}

impl SummaryExport {
    /// Schema version for `result.json`.
    pub const SCHEMA_VERSION: u32 = 1;
}

/// Percentile breakdown extracted from an HDR histogram.
///
/// All values are in nanoseconds. Zero means "no samples recorded" —
/// a histogram with zero samples returns zero at every percentile per
/// hdrhistogram semantics. Not `Eq` because `mean_ns` / `stddev_ns`
/// are `f64`; use [`PartialEq`] for exact matching or compare fields
/// within tolerance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyExport {
    /// Sample count.
    pub count: u64,
    /// Minimum observed.
    pub min_ns: u64,
    /// 50th percentile.
    pub p50_ns: u64,
    /// 90th percentile.
    pub p90_ns: u64,
    /// 99th percentile.
    pub p99_ns: u64,
    /// 99.9th percentile.
    pub p99_9_ns: u64,
    /// 99.99th percentile.
    pub p99_99_ns: u64,
    /// Maximum observed.
    pub max_ns: u64,
    /// Arithmetic mean.
    pub mean_ns: f64,
    /// Standard deviation.
    pub stddev_ns: f64,
}

impl LatencyExport {
    /// Extract percentiles from an HDR histogram. An empty histogram
    /// yields zeros across the board.
    pub fn from_hist(h: &Histogram<u64>) -> Self {
        Self {
            count: h.len(),
            min_ns: h.min(),
            p50_ns: h.value_at_percentile(50.0),
            p90_ns: h.value_at_percentile(90.0),
            p99_ns: h.value_at_percentile(99.0),
            p99_9_ns: h.value_at_percentile(99.9),
            p99_99_ns: h.value_at_percentile(99.99),
            max_ns: h.max(),
            mean_ns: h.mean(),
            stddev_ns: h.stdev(),
        }
    }
}

/// Flat error-counter snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCountersExport {
    /// TCP connect failures.
    pub connect: u64,
    /// Read failures.
    pub read: u64,
    /// Write failures.
    pub write: u64,
    /// Per-request deadline exceeded.
    pub timeout: u64,
    /// Scheduler couldn't keep up (token dropped).
    pub keepup: u64,
    /// Status 4xx.
    pub status_4xx: u64,
    /// Status 5xx.
    pub status_5xx: u64,
    /// Assertion failures.
    pub assertion_failed: u64,
}

impl ErrorCountersExport {
    /// Project an [`ErrorCounters`] into the serialisable form.
    pub fn from_counters(e: &ErrorCounters) -> Self {
        Self {
            connect: e.connect,
            read: e.read,
            write: e.write,
            timeout: e.timeout,
            keepup: e.keepup,
            status_4xx: e.status_4xx,
            status_5xx: e.status_5xx,
            assertion_failed: e.assertion_failed,
        }
    }
}

/// Per-scenario row in `result.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioExport {
    /// Index into `Plan::scenarios`.
    pub scenario_id: u16,
    /// Completed operations attributed to this scenario.
    pub requests: u64,
    /// Latency breakdown.
    pub latency: LatencyExport,
    /// Errors attributed here.
    pub errors: ErrorCountersExport,
    /// SSE-specific metrics, present iff the scenario speaks SSE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse: Option<SseExtrasExport>,
    /// WS-specific metrics, present iff the scenario speaks WS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws: Option<WsExtrasExport>,
}

/// SSE extras in serialisable form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SseExtrasExport {
    /// Time-to-first-byte histogram.
    pub ttfb: LatencyExport,
    /// Inter-chunk gap histogram.
    pub chunk_gap: LatencyExport,
    /// Total data events received.
    pub chunks: u64,
    /// Streams that saw a clean close / `[DONE]`.
    pub streams_completed: u64,
    /// Payload bytes received.
    pub bytes_received: u64,
}

impl SseExtrasExport {
    fn from(s: &SseExtras) -> Self {
        Self {
            ttfb: LatencyExport::from_hist(&s.ttfb),
            chunk_gap: LatencyExport::from_hist(&s.chunk_gap),
            chunks: s.chunks,
            streams_completed: s.streams_completed,
            bytes_received: s.bytes_received,
        }
    }
}

/// WS extras in serialisable form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsExtrasExport {
    /// Handshake latency histogram.
    pub handshake: LatencyExport,
    /// Round-trip histogram.
    pub rtt: LatencyExport,
    /// Messages sent.
    pub messages_sent: u64,
    /// Messages received.
    pub messages_recv: u64,
    /// Bytes sent.
    pub bytes_sent: u64,
    /// Bytes received.
    pub bytes_recv: u64,
}

impl WsExtrasExport {
    fn from(w: &WsExtras) -> Self {
        Self {
            handshake: LatencyExport::from_hist(&w.handshake),
            rtt: LatencyExport::from_hist(&w.rtt),
            messages_sent: w.messages_sent,
            messages_recv: w.messages_recv,
            bytes_sent: w.bytes_sent,
            bytes_recv: w.bytes_recv,
        }
    }
}

impl Summary {
    /// Project this summary into the archive-ready JSON shape.
    ///
    /// Percentiles are sampled at the standard ladder
    /// `[min, p50, p90, p99, p99.9, p99.99, max]` plus mean + stddev.
    /// Per-scenario extras (SSE / WS) are included when the underlying
    /// `ScenarioStats` carries them.
    pub fn to_export(&self) -> SummaryExport {
        SummaryExport {
            schema_version: SummaryExport::SCHEMA_VERSION,
            duration_ns: self.duration.as_nanos().min(u128::from(u64::MAX)) as u64,
            requests: self.requests,
            rate_per_s: self.requests_per_sec(),
            bytes_sent: self.bytes_sent,
            bytes_recv: self.bytes_recv,
            latency: LatencyExport::from_hist(&self.latency),
            ttfb: LatencyExport::from_hist(&self.ttfb),
            errors: ErrorCountersExport::from_counters(&self.errors),
            scenarios: self
                .per_scenario
                .iter()
                .map(|s| ScenarioExport {
                    scenario_id: s.scenario_id,
                    requests: s.requests,
                    latency: LatencyExport::from_hist(&s.latency),
                    errors: ErrorCountersExport::from_counters(&s.errors),
                    sse: s.sse.as_ref().map(SseExtrasExport::from),
                    ws: s.ws.as_ref().map(WsExtrasExport::from),
                })
                .collect(),
            per_run: Vec::new(),
        }
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

    // -----------------------------------------------------------------
    // SSE/WS extras
    // -----------------------------------------------------------------

    #[test]
    fn scenario_stats_default_has_no_extras() {
        let s = ScenarioStats::new(0);
        assert!(s.sse.is_none());
        assert!(s.ws.is_none());
    }

    #[test]
    fn sse_extras_merge_sums_counters_and_histograms() {
        let mut a = SseExtras::default();
        a.chunks = 10;
        a.streams_completed = 1;
        a.bytes_received = 100;
        let _ = a.ttfb.record(1_000);
        let _ = a.chunk_gap.record(500);

        let mut b = SseExtras::default();
        b.chunks = 5;
        b.streams_completed = 2;
        b.bytes_received = 50;
        let _ = b.ttfb.record(2_000);

        a.merge(&b);
        assert_eq!(a.chunks, 15);
        assert_eq!(a.streams_completed, 3);
        assert_eq!(a.bytes_received, 150);
        assert_eq!(a.ttfb.len(), 2);
        assert_eq!(a.chunk_gap.len(), 1);
    }

    #[test]
    fn ws_extras_merge_sums_counters_and_histograms() {
        let mut a = WsExtras::default();
        a.messages_sent = 10;
        a.messages_recv = 9;
        a.bytes_sent = 100;
        a.bytes_recv = 90;
        let _ = a.handshake.record(5_000_000);
        let _ = a.rtt.record(500);

        let mut b = WsExtras::default();
        b.messages_sent = 5;
        b.messages_recv = 5;
        b.bytes_sent = 50;
        b.bytes_recv = 50;
        let _ = b.rtt.record(1_500);

        a.merge(&b);
        assert_eq!(a.messages_sent, 15);
        assert_eq!(a.messages_recv, 14);
        assert_eq!(a.bytes_sent, 150);
        assert_eq!(a.bytes_recv, 140);
        assert_eq!(a.handshake.len(), 1);
        assert_eq!(a.rtt.len(), 2);
    }

    #[test]
    fn scenario_stats_merge_preserves_sse_extras() {
        let mut a = ScenarioStats::new(0);
        a.sse_mut().chunks = 10;
        let _ = a.sse_mut().ttfb.record(1_000);

        let mut b = ScenarioStats::new(0);
        b.sse_mut().chunks = 5;
        let _ = b.sse_mut().ttfb.record(2_000);

        a.merge(&b);
        let sse = a.sse.as_ref().unwrap();
        assert_eq!(sse.chunks, 15);
        assert_eq!(sse.ttfb.len(), 2);
    }

    #[test]
    fn scenario_stats_merge_preserves_ws_extras() {
        let mut a = ScenarioStats::new(1);
        a.ws_mut().messages_sent = 10;

        let mut b = ScenarioStats::new(1);
        b.ws_mut().messages_sent = 5;

        a.merge(&b);
        let ws = a.ws.as_ref().unwrap();
        assert_eq!(ws.messages_sent, 15);
    }

    #[test]
    fn task_stats_merge_combines_per_scenario_extras() {
        let mut a = TaskStats::new(2);
        a.per_scenario[0].sse_mut().chunks = 3;
        a.per_scenario[1].ws_mut().messages_sent = 7;

        let mut b = TaskStats::new(2);
        b.per_scenario[0].sse_mut().chunks = 2;
        b.per_scenario[1].ws_mut().messages_sent = 3;

        a.merge(&b);
        assert_eq!(a.per_scenario[0].sse.as_ref().unwrap().chunks, 5);
        assert_eq!(a.per_scenario[1].ws.as_ref().unwrap().messages_sent, 10);
    }
}
