//! zerobench-ws — RFC 6455 WebSocket transport.
//!
//! Like [`zerobench_sse`], WebSocket doesn't fit the single-shot
//! [`Transport`](zerobench_core::Transport) trait: a connection is
//! long-lived and bidirectional, and the useful metric is per-message
//! round-trip time rather than per-request latency. So this crate
//! exposes its own runner — [`run_ws_saturate`] — which the CLI picks
//! when `--ws` is set.
//!
//! # Architecture
//!
//! - [`frame`] — RFC 6455 §5.2 wire-format encoder/decoder.
//! - [`handshake`] — the HTTP/1.1 §4 Upgrade exchange + Accept-key
//!   validation.
//! - [`conn::WsConnection`] — one established connection (stream +
//!   recv buffer + per-connection mask CSPRNG).
//! - [`run_ws_saturate`] — N concurrent worker tasks, each owning one
//!   connection, looping send→recv until `StopSignal` trips.
//!
//! # Fixing the v1 benchmark's bugs
//!
//! The reference implementation at `tools/bench/src/ws.rs` in the
//! zeroship repo had three known issues we correct here:
//!
//! 1. **Mask was a Weyl atomic counter, not CSPRNG**. RFC 6455 §10.3
//!    says "MUST be chosen from the set of allowed 32-bit values at
//!    random". We use [`BenchRng`](zerobench_core::rng::BenchRng)
//!    seeded from OS entropy — predictable-only if someone reads the
//!    worker's memory, which defeats the cache-poisoning threat model
//!    the mask defends against.
//! 2. **Round-robin sequential per-connection handling**. v1 rotated
//!    through N slots in one task, so N connections were serviced
//!    one-at-a-time. We use task-per-connection (same fix as the
//!    HTTP/SSE crates).
//! 3. **Byte-by-byte masking XOR**. Kept — for payloads typically
//!    <1 KiB the 4-byte-at-a-time variant is a micro-optimisation that
//!    isn't worth the complexity.

pub mod conn;
pub mod frame;
pub mod handshake;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;

use zerobench_core::live_snapshot::LiveSnapshot;
use zerobench_core::rng::from_entropy;
use zerobench_core::stop::StopSignal;
use zerobench_core::transport::Target;

pub use conn::{DataFrame, WsConnection, WsError};
pub use frame::{FrameHeader, Opcode};

// ---------------------------------------------------------------------------
// Histograms
// ---------------------------------------------------------------------------

/// Histogram bounds — `[1 ns, 60 s]`, 3 sig figs. Matches `SseStats` so
/// percentile math in the reporter is uniform across protocols.
const HIST_LO_NS: u64 = 1;
const HIST_HI_NS: u64 = 60_000_000_000;
const HIST_SIG: u8 = 3;

fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO_NS, HIST_HI_NS, HIST_SIG)
        .expect("HDR bounds are valid compile-time constants")
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
// Stats
// ---------------------------------------------------------------------------

/// Per-worker WebSocket statistics. One instance per worker; merged into
/// a [`WsSummary`] at end-of-run.
#[derive(Debug, Clone)]
pub struct WsStats {
    /// Handshake time in nanoseconds — from TCP connect complete through
    /// 101 response received + Accept verified.
    pub handshake: Histogram<u64>,
    /// Per-message round-trip time in nanoseconds — from when we
    /// finished writing a frame to when we parsed the corresponding
    /// response frame.
    pub rtt: Histogram<u64>,
    /// Count of Text/Binary frames we sent. Does not include control
    /// frames (Ping auto-replies are tracked separately if we ever
    /// need them).
    pub messages_sent: u64,
    /// Count of Text/Binary frames received. Should normally equal
    /// `messages_sent` for an echo server; a gap means the server
    /// dropped messages or closed mid-response.
    pub messages_recvd: u64,
    /// Total payload bytes sent (excluding frame headers). Matches
    /// the wire-visible message body, not the on-wire byte count —
    /// that would add ~6 bytes of header per message.
    pub bytes_sent: u64,
    /// Total payload bytes received. Same convention as `bytes_sent`.
    pub bytes_recvd: u64,
    /// TCP connect / DNS failures.
    pub errors_connect: u64,
    /// Handshake protocol failures (101 not received, Accept mismatch,
    /// missing Upgrade/Connection header).
    pub errors_upgrade: u64,
    /// Read / write / framing errors mid-session.
    pub errors_io: u64,
    /// Errors emitting or processing a Close frame.
    pub errors_close: u64,
}

impl Default for WsStats {
    fn default() -> Self {
        Self::new()
    }
}

impl WsStats {
    /// Fresh stats bucket with empty histograms and zero counters.
    pub fn new() -> Self {
        Self {
            handshake: new_hist(),
            rtt: new_hist(),
            messages_sent: 0,
            messages_recvd: 0,
            bytes_sent: 0,
            bytes_recvd: 0,
            errors_connect: 0,
            errors_upgrade: 0,
            errors_io: 0,
            errors_close: 0,
        }
    }

    /// Record a handshake duration.
    pub fn record_handshake(&mut self, d: Duration) {
        let _ = self.handshake.record(duration_to_hist_ns(d));
    }

    /// Record a round-trip sample.
    pub fn record_rtt(&mut self, d: Duration) {
        let _ = self.rtt.record(duration_to_hist_ns(d));
    }

    /// Merge another stats bucket. Histograms add; counters sum.
    pub fn merge(&mut self, other: &Self) {
        let _ = self.handshake.add(&other.handshake);
        let _ = self.rtt.add(&other.rtt);
        self.messages_sent += other.messages_sent;
        self.messages_recvd += other.messages_recvd;
        self.bytes_sent += other.bytes_sent;
        self.bytes_recvd += other.bytes_recvd;
        self.errors_connect += other.errors_connect;
        self.errors_upgrade += other.errors_upgrade;
        self.errors_io += other.errors_io;
        self.errors_close += other.errors_close;
    }
}

/// End-of-run WebSocket summary — merged from all worker [`WsStats`].
#[derive(Debug, Clone)]
pub struct WsSummary {
    /// Combined handshake-time histogram.
    pub handshake: Histogram<u64>,
    /// Combined round-trip-time histogram.
    pub rtt: Histogram<u64>,
    /// Total messages sent across all workers.
    pub messages_sent: u64,
    /// Total messages received.
    pub messages_recvd: u64,
    /// Total payload bytes sent.
    pub bytes_sent: u64,
    /// Total payload bytes received.
    pub bytes_recvd: u64,
    /// Connect failures.
    pub errors_connect: u64,
    /// Handshake failures.
    pub errors_upgrade: u64,
    /// Mid-session IO / framing failures.
    pub errors_io: u64,
    /// Close-frame failures.
    pub errors_close: u64,
    /// Wall-clock duration of the benchmark (excluding warmup).
    pub duration: Duration,
}

impl WsSummary {
    /// Merge a vector of per-worker stats into a single summary.
    pub fn merge(stats: Vec<WsStats>, duration: Duration) -> Self {
        let mut out = WsSummary {
            handshake: new_hist(),
            rtt: new_hist(),
            messages_sent: 0,
            messages_recvd: 0,
            bytes_sent: 0,
            bytes_recvd: 0,
            errors_connect: 0,
            errors_upgrade: 0,
            errors_io: 0,
            errors_close: 0,
            duration,
        };
        for s in stats {
            let _ = out.handshake.add(&s.handshake);
            let _ = out.rtt.add(&s.rtt);
            out.messages_sent += s.messages_sent;
            out.messages_recvd += s.messages_recvd;
            out.bytes_sent += s.bytes_sent;
            out.bytes_recvd += s.bytes_recvd;
            out.errors_connect += s.errors_connect;
            out.errors_upgrade += s.errors_upgrade;
            out.errors_io += s.errors_io;
            out.errors_close += s.errors_close;
        }
        out
    }

    /// Messages per second (received) across the whole run.
    pub fn messages_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.messages_recvd as f64 / secs
        }
    }
}

// ---------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------

/// Everything the WebSocket runner needs, hoisted out of
/// [`zerobench_core::plan::Plan`] so we don't stretch the shared types
/// to accommodate protocol-specific knobs (like "what text payload to
/// send per iteration").
#[derive(Debug, Clone)]
pub struct WsPlan {
    /// TCP endpoint (host + port + TLS flag).
    pub target: Target,
    /// HTTP path to request in the Upgrade (e.g. `/echo`). Parsed from
    /// the `ws://.../` URL by the CLI.
    pub path: String,
    /// Extra HTTP headers to include in the Upgrade request. From
    /// `-H` flags on the CLI — e.g. `Origin`, auth cookies.
    pub headers: Vec<(String, String)>,
    /// Payload to send on each iteration. Treated as a text-frame
    /// body; servers that echo return the same bytes. Defaults to
    /// `b"ping"` in the CLI.
    pub message: Bytes,
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// The benchmark's per-connection loop.
///
/// Not much state — the runner's "state" mostly lives in the
/// [`WsConnection`] it owns and the [`WsStats`] it folds into.
pub struct WsRunner;

impl WsRunner {
    /// Run one worker's lifecycle end-to-end: connect, handshake, loop
    /// send/recv until `stop` trips, optionally close.
    ///
    /// Errors never panic. Every failure path is counted in `stats`.
    async fn run_worker(plan: Arc<WsPlan>, stop: StopSignal, live: Option<Arc<LiveSnapshot>>) -> WsStats {
        let mut stats = WsStats::new();
        let rng = from_entropy();

        // --- Connect ----------------------------------------------------
        let t_connect_start = Instant::now();
        let conn_result =
            WsConnection::connect_tcp(&plan.target, &plan.path, &plan.headers, rng).await;
        let mut conn = match conn_result {
            Ok(c) => {
                stats.record_handshake(t_connect_start.elapsed());
                c
            }
            Err(e) => {
                classify_open_error(&e, &mut stats, live.as_deref());
                return stats;
            }
        };

        // --- Benchmark loop --------------------------------------------
        //
        // We sample RTT per message: t0 just before send → t1 after
        // parsing the server's response frame. Control frames (Ping/
        // Pong/Close) are handled inside `recv` and don't bump the
        // RTT histogram.
        while !stop.is_stopped() {
            let t0 = Instant::now();

            if let Err(e) = conn.send_text(&plan.message).await {
                classify_io_error(&e, &mut stats, live.as_deref());
                return stats;
            }
            stats.messages_sent += 1;
            stats.bytes_sent += plan.message.len() as u64;

            let frame = match conn.recv().await {
                Ok(f) => f,
                Err(WsError::Closed { code: _, reason: _ }) => {
                    // Server closed cleanly. Not an error — exit the
                    // loop and let the (skipped) close-frame branch at
                    // the bottom finalise cleanly.
                    return stats;
                }
                Err(e) => {
                    classify_io_error(&e, &mut stats, live.as_deref());
                    return stats;
                }
            };

            let rtt = t0.elapsed();
            stats.record_rtt(rtt);
            stats.messages_recvd += 1;
            stats.bytes_recvd += frame.len() as u64;
        }

        // --- Close ------------------------------------------------------
        //
        // RFC 6455 §5.5.1: code 1000 is "normal closure". We don't care
        // about the reason byte for a bench tool; leave it empty.
        if let Err(e) = conn.close(1000, "").await {
            // A close-send error at the very end is cosmetic — the
            // server might have already closed the socket. Log it but
            // don't otherwise propagate.
            classify_close_error(&e, &mut stats);
        }

        stats
    }
}

/// Classify a handshake/connect error into the right stats counter.
///
/// Only reached during connection-open; subsequent IO errors go through
/// [`classify_io_error`].
fn classify_open_error(
    e: &WsError,
    stats: &mut WsStats,
    live: Option<&LiveSnapshot>,
) {
    match e {
        WsError::Io(_) => {
            stats.errors_connect += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Handshake(_) => {
            stats.errors_upgrade += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Tls(_) => {
            stats.errors_connect += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Frame(_) => {
            // Frame errors during open are vanishingly unlikely (would
            // require the server to prepend bytes to its 101 response
            // and fail decoding) but keep the match exhaustive.
            stats.errors_upgrade += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
        WsError::Closed { .. } => {
            // Server closed during handshake — count it as upgrade
            // failure since no message ever flowed.
            stats.errors_upgrade += 1;
            if let Some(l) = live {
                l.record_error(zerobench_core::stats::ErrorKind::Connect);
            }
        }
    }
}

/// Classify a mid-session error. All non-clean-close errors land here.
fn classify_io_error(_e: &WsError, stats: &mut WsStats, live: Option<&LiveSnapshot>) {
    stats.errors_io += 1;
    if let Some(l) = live {
        l.record_error(zerobench_core::stats::ErrorKind::Read);
    }
}

/// Close-path error — a failure to *send* the close frame at shutdown.
fn classify_close_error(_e: &WsError, stats: &mut WsStats) {
    stats.errors_close += 1;
}

/// Run a WebSocket benchmark by saturating `max_tasks` concurrent
/// connections.
///
/// Each task owns one [`WsConnection`] and loops send→recv until
/// `stop` trips. Per-worker stats are collected at end-of-run and
/// returned for the caller to merge via [`WsSummary::merge`].
///
/// `live`, when `Some`, receives per-message RTT samples so the TUI
/// and JSONL streamers update in real time. Same integration style as
/// the HTTP/SSE runners.
pub async fn run_ws_saturate(
    plan: WsPlan,
    max_tasks: usize,
    stop: StopSignal,
    live: Option<Arc<LiveSnapshot>>,
) -> Vec<WsStats> {
    if max_tasks == 0 {
        return Vec::new();
    }

    let plan = Arc::new(plan);
    let mut handles = Vec::with_capacity(max_tasks);

    for _ in 0..max_tasks {
        let plan = plan.clone();
        let stop = stop.clone();
        let live = live.clone();
        let handle = compio::runtime::spawn(async move {
            WsRunner::run_worker(plan, stop, live).await
        });
        handles.push(handle);
    }

    let mut out = Vec::with_capacity(max_tasks);
    for h in handles {
        match h.await {
            Ok(s) => out.push(s),
            Err(_panic) => out.push(WsStats::new()),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_merge_sums_counters() {
        let mut a = WsStats::new();
        a.messages_sent = 10;
        a.bytes_sent = 100;
        a.errors_io = 1;
        a.record_rtt(Duration::from_micros(500));

        let mut b = WsStats::new();
        b.messages_sent = 5;
        b.bytes_sent = 50;
        b.errors_connect = 2;
        b.record_rtt(Duration::from_micros(1_500));

        a.merge(&b);
        assert_eq!(a.messages_sent, 15);
        assert_eq!(a.bytes_sent, 150);
        assert_eq!(a.errors_io, 1);
        assert_eq!(a.errors_connect, 2);
        assert_eq!(a.rtt.len(), 2);
    }

    #[test]
    fn summary_merge_aggregates_correctly() {
        let mut s1 = WsStats::new();
        s1.messages_recvd = 10;
        s1.bytes_recvd = 100;
        s1.record_handshake(Duration::from_millis(5));
        s1.record_rtt(Duration::from_micros(500));

        let mut s2 = WsStats::new();
        s2.messages_recvd = 20;
        s2.bytes_recvd = 200;
        s2.record_handshake(Duration::from_millis(3));

        let sum = WsSummary::merge(vec![s1, s2], Duration::from_secs(2));
        assert_eq!(sum.messages_recvd, 30);
        assert_eq!(sum.bytes_recvd, 300);
        assert_eq!(sum.handshake.len(), 2);
        assert_eq!(sum.rtt.len(), 1);
        assert!((sum.messages_per_sec() - 15.0).abs() < 1e-9);
    }

    #[test]
    fn empty_summary_messages_per_sec_is_zero() {
        let sum = WsSummary::merge(Vec::new(), Duration::ZERO);
        assert_eq!(sum.messages_per_sec(), 0.0);
    }

    #[test]
    fn duration_to_hist_ns_clamps() {
        // Below lower bound → clamped up
        assert_eq!(duration_to_hist_ns(Duration::from_nanos(0)), HIST_LO_NS);
        // Above upper bound → clamped down
        let huge = Duration::from_secs(999);
        assert_eq!(duration_to_hist_ns(huge), HIST_HI_NS);
        // Within bounds → exact
        assert_eq!(
            duration_to_hist_ns(Duration::from_micros(500)),
            500_000,
        );
    }
}
