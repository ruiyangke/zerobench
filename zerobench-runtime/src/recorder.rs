//! Recorder — collapses the triple-record antipattern.
//!
//! Every completed op touches three places today: `TaskStats.record`,
//! `LiveSnapshot.record`, `LiveSnapshot.record_scenario`. Plus the
//! error equivalent is another three. Recorder holds the shared state
//! and exposes one `record` / `record_error` per op.
//!
//! See `docs/ARCH-REVIEW-2026-04-20.md` §4.3, §7.

use std::time::Duration;

use zerobench_core::histogram::duration_to_hist_ns;
use zerobench_core::stats::{ErrorKind, TaskStats};

use crate::live_snapshot::LiveSnapshot;

/// A completed op, as seen by the recorder.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Wall latency of the op (request round-trip, RTT, or event gap).
    pub latency: Duration,
    /// Time-to-first-byte. Zero for protocols where it's not applicable
    /// (WS RTT, SSE event gap).
    pub ttfb: Duration,
    /// Bytes written to the wire for this op.
    pub bytes_sent: u64,
    /// Bytes read from the wire for this op.
    pub bytes_recv: u64,
}

/// Aggregates per-task persistent stats (`TaskStats`) and the optional
/// shared live-snapshot sink. Constructed per worker/task.
pub struct Recorder<'a> {
    task_stats: Option<&'a mut TaskStats>,
    live: Option<&'a LiveSnapshot>,
}

impl<'a> Recorder<'a> {
    /// The HTTP pattern: TaskStats is the persistent accumulator,
    /// LiveSnapshot is the optional TUI feed.
    pub fn new(task_stats: &'a mut TaskStats, live: Option<&'a LiveSnapshot>) -> Self {
        Self {
            task_stats: Some(task_stats),
            live,
        }
    }

    /// SSE/WS event loops keep their own per-subscriber stats that
    /// aren't `TaskStats`-shaped. They still want live-snapshot writes
    /// for the TUI.
    pub fn live_only(live: Option<&'a LiveSnapshot>) -> Self {
        Self {
            task_stats: None,
            live,
        }
    }

    /// Record one completed op — writes TaskStats (if present) and
    /// both LiveSnapshot slots (aggregate + per-scenario).
    #[inline(always)]
    pub fn record(&mut self, sid: u16, sample: Sample) {
        if let Some(ts) = self.task_stats.as_deref_mut() {
            ts.record(
                sid,
                sample.latency,
                sample.ttfb,
                sample.bytes_sent,
                sample.bytes_recv,
            );
        }
        if let Some(live) = self.live {
            let ns = duration_to_hist_ns(sample.latency);
            live.record(ns, sample.bytes_sent, sample.bytes_recv);
            live.record_scenario(sid, ns, sample.bytes_sent, sample.bytes_recv);
        }
    }

    /// Like `record` but takes a pre-computed nanosecond latency. Used
    /// by WS/SSE where the histogram ns is already computed (to avoid
    /// `Duration`→ns round-trip drift on very small values).
    #[inline(always)]
    pub fn record_ns(&mut self, sid: u16, latency_ns: u64, bytes_sent: u64, bytes_recv: u64) {
        if let Some(ts) = self.task_stats.as_deref_mut() {
            ts.record(
                sid,
                Duration::from_nanos(latency_ns),
                Duration::ZERO,
                bytes_sent,
                bytes_recv,
            );
        }
        if let Some(live) = self.live {
            live.record(latency_ns, bytes_sent, bytes_recv);
            live.record_scenario(sid, latency_ns, bytes_sent, bytes_recv);
        }
    }

    /// Record one error — writes TaskStats.record_error (if present)
    /// plus both LiveSnapshot error counters.
    #[inline(always)]
    pub fn record_error(&mut self, sid: u16, kind: ErrorKind) {
        if let Some(ts) = self.task_stats.as_deref_mut() {
            ts.record_error(sid, kind);
        }
        if let Some(live) = self.live {
            live.record_error(kind);
            live.record_scenario_error(sid, kind);
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_with_task_stats_writes_both_sides() {
        let mut stats = TaskStats::new(2);
        let live = LiveSnapshot::new(2);
        {
            let mut rec = Recorder::new(&mut stats, Some(&live));
            rec.record(
                1,
                Sample {
                    latency: Duration::from_micros(250),
                    ttfb: Duration::from_micros(40),
                    bytes_sent: 100,
                    bytes_recv: 200,
                },
            );
        }

        // TaskStats side.
        assert_eq!(stats.requests, 1);
        assert_eq!(stats.bytes_sent, 100);
        assert_eq!(stats.bytes_recv, 200);
        assert_eq!(stats.latency.len(), 1);
        assert_eq!(stats.ttfb.len(), 1);
        assert_eq!(stats.per_scenario[1].requests, 1);
        assert_eq!(stats.per_scenario[0].requests, 0);

        // LiveSnapshot side — tick swap to read.
        let tick = live.swap_and_snapshot();
        assert_eq!(tick.requests, 1);
        assert_eq!(tick.bytes_sent, 100);
        assert_eq!(tick.bytes_recv, 200);
        assert_eq!(tick.latency.len(), 1);
        assert_eq!(tick.per_scenario[1].requests, 1);
        assert_eq!(tick.per_scenario[0].requests, 0);
    }

    #[test]
    fn record_without_task_stats_only_writes_live() {
        let live = LiveSnapshot::new(1);
        {
            let mut rec = Recorder::live_only(Some(&live));
            rec.record(
                0,
                Sample {
                    latency: Duration::from_millis(5),
                    ttfb: Duration::ZERO,
                    bytes_sent: 1,
                    bytes_recv: 2,
                },
            );
        }
        let tick = live.swap_and_snapshot();
        assert_eq!(tick.requests, 1);
        assert_eq!(tick.bytes_sent, 1);
        assert_eq!(tick.bytes_recv, 2);
        assert_eq!(tick.per_scenario[0].requests, 1);
    }

    #[test]
    fn record_without_live_only_writes_task_stats() {
        let mut stats = TaskStats::new(1);
        {
            let mut rec = Recorder::new(&mut stats, None);
            rec.record(
                0,
                Sample {
                    latency: Duration::from_micros(100),
                    ttfb: Duration::from_micros(10),
                    bytes_sent: 7,
                    bytes_recv: 11,
                },
            );
        }
        assert_eq!(stats.requests, 1);
        assert_eq!(stats.bytes_sent, 7);
        assert_eq!(stats.bytes_recv, 11);
        assert_eq!(stats.latency.len(), 1);
    }

    #[test]
    fn record_ns_bypasses_duration_roundtrip() {
        let mut stats = TaskStats::new(1);
        let live = LiveSnapshot::new(1);
        {
            let mut rec = Recorder::new(&mut stats, Some(&live));
            rec.record_ns(0, 12_345, 3, 4);
        }
        assert_eq!(stats.requests, 1);
        assert_eq!(stats.bytes_sent, 3);
        assert_eq!(stats.bytes_recv, 4);
        assert_eq!(stats.latency.len(), 1);

        let tick = live.swap_and_snapshot();
        assert_eq!(tick.requests, 1);
        assert_eq!(tick.bytes_sent, 3);
        assert_eq!(tick.bytes_recv, 4);
        assert_eq!(tick.latency.len(), 1);
        assert_eq!(tick.per_scenario[0].requests, 1);
    }

    #[test]
    fn record_ns_live_only_writes_live() {
        let live = LiveSnapshot::new(2);
        {
            let mut rec = Recorder::live_only(Some(&live));
            rec.record_ns(1, 42_000, 0, 16);
        }
        let tick = live.swap_and_snapshot();
        assert_eq!(tick.requests, 1);
        assert_eq!(tick.bytes_sent, 0);
        assert_eq!(tick.bytes_recv, 16);
        assert_eq!(tick.per_scenario[0].requests, 0);
        assert_eq!(tick.per_scenario[1].requests, 1);
    }

    #[test]
    fn record_error_with_both_writes_both_sides() {
        let mut stats = TaskStats::new(2);
        let live = LiveSnapshot::new(2);
        {
            let mut rec = Recorder::new(&mut stats, Some(&live));
            rec.record_error(1, ErrorKind::Connect);
            rec.record_error(1, ErrorKind::Status5xx);
        }
        assert_eq!(stats.errors.connect, 1);
        assert_eq!(stats.errors.status_5xx, 1);
        assert_eq!(stats.per_scenario[1].errors.connect, 1);
        assert_eq!(stats.per_scenario[1].errors.status_5xx, 1);
        assert_eq!(stats.per_scenario[0].errors.total(), 0);

        let tick = live.swap_and_snapshot();
        assert_eq!(tick.errors.connect, 1);
        assert_eq!(tick.errors.status_5xx, 1);
        assert_eq!(tick.per_scenario[1].errors.connect, 1);
        assert_eq!(tick.per_scenario[1].errors.status_5xx, 1);
        assert_eq!(tick.per_scenario[0].errors.total(), 0);
    }

    #[test]
    fn record_error_without_live_only_writes_task_stats() {
        let mut stats = TaskStats::new(1);
        {
            let mut rec = Recorder::new(&mut stats, None);
            rec.record_error(0, ErrorKind::Read);
        }
        assert_eq!(stats.errors.read, 1);
        assert_eq!(stats.per_scenario[0].errors.read, 1);
    }

    #[test]
    fn record_error_live_only_writes_live() {
        let live = LiveSnapshot::new(2);
        {
            let mut rec = Recorder::live_only(Some(&live));
            rec.record_error(0, ErrorKind::Write);
            rec.record_error(1, ErrorKind::Timeout);
            rec.record_error(1, ErrorKind::Timeout);
        }
        let tick = live.swap_and_snapshot();
        assert_eq!(tick.errors.write, 1);
        assert_eq!(tick.errors.timeout, 2);
        assert_eq!(tick.per_scenario[0].errors.write, 1);
        assert_eq!(tick.per_scenario[1].errors.timeout, 2);
    }

    #[test]
    fn record_error_with_neither_is_noop() {
        // Neither sink — Recorder silently drops. This is the degenerate
        // path (no TaskStats, no live) and should not panic.
        let mut rec = Recorder::live_only(None);
        rec.record_error(0, ErrorKind::Keepup);
        rec.record_ns(0, 1_000, 1, 1);
        rec.record(
            0,
            Sample {
                latency: Duration::from_micros(1),
                ttfb: Duration::ZERO,
                bytes_sent: 0,
                bytes_recv: 0,
            },
        );
    }
}
