//! Per-second live snapshot for JSONL streaming output.
//!
//! CROWN JEWEL — the 16-shard sharded-mutex design is the 1M+ req/s
//! enabler. Do NOT rewrite the sharding without benchmarking.
//!
//! Workers call [`LiveSnapshot::record`] (or [`LiveSnapshot::record_error`])
//! on every completed sample. A dedicated per-second ticker task calls
//! [`LiveSnapshot::swap_and_snapshot`] every 1s to atomically swap in an
//! empty histogram, reset counters, and hand the previous bucket to the
//! reporter.
//!
//! # Design: sharded HDR histograms
//!
//! The latency histogram is sharded across `LATENCY_SHARDS` mutexes
//! keyed by a per-thread hash. A single-mutex design was serialising
//! every worker at ~1M req/s; each thread sticking to its own shard
//! reduces expected contention to `1/SHARDS` of what a round-robin or
//! single-mutex layout would produce.
//!
//! On tick, the ticker walks all shards, swaps each one for a fresh
//! `Histogram`, and merges the previous buckets into one result. The
//! merge cost (~16 HDR adds per second) is negligible next to the
//! per-sample lock-contention savings on the worker path.
//!
//! **Why not t-digest?** Per-second HDR buckets give *exact*
//! percentiles for the window; t-digest is an approximation we don't
//! need when the samples fit in memory — which they always do for a
//! one-second bucket.

use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use parking_lot::Mutex;

use zerobench_core::stats::{ErrorCounters, ErrorKind};

// ---------------------------------------------------------------------------
// Sharding — number of latency-histogram mutexes.
// ---------------------------------------------------------------------------

/// Number of shards for the aggregate and per-scenario latency
/// histograms. Must be a power of two — the shard-picker masks with
/// `SHARDS - 1` instead of a modulo for branch-free indexing.
///
/// 16 is enough to cover typical bench worker counts (4..32) without
/// contention; larger counts share shards but under load the sample
/// bursts still spread out because different threads generally hash
/// to different shards.
const LATENCY_SHARDS: usize = 16;
const LATENCY_SHARDS_MASK: usize = LATENCY_SHARDS - 1;

/// Pick a per-thread shard index deterministically from the thread's
/// id. Cached in a thread-local so each thread computes the hash once
/// and reuses it for every `record` call.
///
/// Using `ThreadId`'s stable hash keeps the workload balanced across
/// shards regardless of scheduler whim: thread A always writes to the
/// same shard, thread B to a different one, so two hot threads never
/// contend unless they happen to land on the same slot (1-in-SHARDS
/// collision).
fn pick_shard() -> usize {
    thread_local! {
        static SHARD: std::cell::Cell<Option<usize>> = const {
            std::cell::Cell::new(None)
        };
    }
    SHARD.with(|cell| {
        if let Some(s) = cell.get() {
            return s;
        }
        // `DefaultHasher` is std's FxHash-equivalent — cheap and
        // high-quality for the single hash-per-thread we do here.
        let tid = std::thread::current().id();
        let mut h = BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default()
            .build_hasher();
        std::hash::Hash::hash(&tid, &mut h);
        let s = (h.finish() as usize) & LATENCY_SHARDS_MASK;
        cell.set(Some(s));
        s
    })
}

// ---------------------------------------------------------------------------
// Histogram bounds — match TaskStats' so the bucket sizes align.
// ---------------------------------------------------------------------------

const HIST_LO_NS: u64 = 1;
const HIST_HI_NS: u64 = 60_000_000_000;
const HIST_SIG: u8 = 3;

fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO_NS, HIST_HI_NS, HIST_SIG)
        .expect("HDR histogram bounds are valid compile-time constants")
}

// ---------------------------------------------------------------------------
// Atomic error counters
// ---------------------------------------------------------------------------

/// Lock-free error counters — one `AtomicU64` per category. Feeds into
/// an [`ErrorCounters`] at snapshot time.
#[derive(Debug, Default)]
struct AtomicLiveErrors {
    connect: AtomicU64,
    read: AtomicU64,
    write: AtomicU64,
    timeout: AtomicU64,
    keepup: AtomicU64,
    status_4xx: AtomicU64,
    status_5xx: AtomicU64,
    assertion_failed: AtomicU64,
}

impl AtomicLiveErrors {
    fn incr(&self, kind: ErrorKind) {
        let slot = match kind {
            ErrorKind::Connect => &self.connect,
            ErrorKind::Read => &self.read,
            ErrorKind::Write => &self.write,
            ErrorKind::Timeout => &self.timeout,
            ErrorKind::Keepup => &self.keepup,
            ErrorKind::Status4xx => &self.status_4xx,
            ErrorKind::Status5xx => &self.status_5xx,
            ErrorKind::AssertionFailed => &self.assertion_failed,
        };
        slot.fetch_add(1, Ordering::Relaxed);
    }

    /// Swap every slot to zero, returning the previous values.
    fn swap_all(&self) -> ErrorCounters {
        ErrorCounters {
            connect: self.connect.swap(0, Ordering::Relaxed),
            read: self.read.swap(0, Ordering::Relaxed),
            write: self.write.swap(0, Ordering::Relaxed),
            timeout: self.timeout.swap(0, Ordering::Relaxed),
            keepup: self.keepup.swap(0, Ordering::Relaxed),
            status_4xx: self.status_4xx.swap(0, Ordering::Relaxed),
            status_5xx: self.status_5xx.swap(0, Ordering::Relaxed),
            assertion_failed: self.assertion_failed.swap(0, Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// LiveSnapshot
// ---------------------------------------------------------------------------

/// Shared aggregator for per-second JSONL streaming.
///
/// Construct once via [`LiveSnapshot::new`], share across workers via
/// `Arc::clone`. Workers call [`Self::record`] / [`Self::record_error`]
/// on every completed iteration; a dedicated ticker calls
/// [`Self::swap_and_snapshot`] every second to hand off the prior
/// bucket to the reporter.
pub struct LiveSnapshot {
    /// Wall-clock instant at which this aggregator was created. Ticks
    /// report their `t` relative to this anchor.
    start: Instant,
    /// Total requests recorded since the last swap (reset on tick).
    requests: AtomicU64,
    /// Total bytes sent since the last swap.
    bytes_sent: AtomicU64,
    /// Total bytes received since the last swap.
    bytes_recv: AtomicU64,
    /// Error counters since the last swap.
    errors: AtomicLiveErrors,
    /// Sharded latency histograms. Writers hash their thread id to
    /// `LATENCY_SHARDS_MASK` and acquire only that shard's mutex;
    /// the ticker merges across shards on swap. Boxed to keep the
    /// per-`LiveSnapshot` footprint bounded and allocated once at
    /// construction.
    latency_shards: Box<[Mutex<Histogram<u64>>; LATENCY_SHARDS]>,
    /// Per-scenario live counters. Initialized with `num_scenarios` slots.
    /// Index = scenario_id. Each slot tracks its own requests + errors
    /// + latency histogram (sharded, same pattern as the aggregate).
    scenario_counters: Vec<ScenarioLiveSlot>,
}

/// Per-scenario atomic counters — one slot per scenario in the plan.
struct ScenarioLiveSlot {
    requests: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    errors: AtomicLiveErrors,
    /// Sharded histogram, matching the aggregate layout. `Box`ed so the
    /// per-scenario memory (~16 · 30 KiB ≈ 480 KiB) lives on the heap;
    /// the per-`LiveSnapshot` footprint grows as `N_scenarios · 480 KiB`.
    latency_shards: Box<[Mutex<Histogram<u64>>; LATENCY_SHARDS]>,
}

fn new_shard_array() -> Box<[Mutex<Histogram<u64>>; LATENCY_SHARDS]> {
    // `array::from_fn` can't be used with a closure that captures state
    // AND produces a `Mutex<Histogram>` in a no-alloc way, so we build
    // the array via a vector and unwrap into a boxed array. The boxing
    // avoids a 480 KiB stack object at construction time.
    let vec: Vec<Mutex<Histogram<u64>>> =
        (0..LATENCY_SHARDS).map(|_| Mutex::new(new_hist())).collect();
    vec.into_boxed_slice()
        .try_into()
        .expect("LATENCY_SHARDS vec always has the right length")
}

impl LiveSnapshot {
    /// Build an empty snapshot anchored at `now`. `num_scenarios` sets
    /// the number of per-scenario counter slots; pass `plan.scenarios.len()`.
    /// Wrap in `Arc` and share with workers.
    pub fn new(num_scenarios: usize) -> Arc<Self> {
        let scenario_counters = (0..num_scenarios)
            .map(|_| ScenarioLiveSlot {
                requests: AtomicU64::new(0),
                bytes_sent: AtomicU64::new(0),
                bytes_recv: AtomicU64::new(0),
                errors: AtomicLiveErrors::default(),
                latency_shards: new_shard_array(),
            })
            .collect();
        Arc::new(Self {
            start: Instant::now(),
            requests: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            errors: AtomicLiveErrors::default(),
            latency_shards: new_shard_array(),
            scenario_counters,
        })
    }

    /// Record a completed sample. `latency_ns` must already be clamped
    /// to the histogram's configured bounds (workers use the same
    /// `duration_to_hist_ns` helper as `TaskStats::record`, reachable
    /// indirectly through this method's caller).
    ///
    /// The sample is appended to this thread's shard — under realistic
    /// loads with `threads >> LATENCY_SHARDS` each shard sees roughly
    /// `threads / LATENCY_SHARDS` concurrent writers, not `threads`,
    /// which is where the S3 contention win comes from.
    pub fn record(&self, latency_ns: u64, bytes_sent: u64, bytes_recv: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes_sent, Ordering::Relaxed);
        self.bytes_recv.fetch_add(bytes_recv, Ordering::Relaxed);
        let ns = clamp_ns(latency_ns);
        let shard = &self.latency_shards[pick_shard()];
        let mut bucket = shard.lock();
        // `record` can only fail for out-of-range; we just clamped.
        let _ = bucket.record(ns);
    }

    /// Increment the counter for `kind`.
    pub fn record_error(&self, kind: ErrorKind) {
        self.errors.incr(kind);
    }

    /// Record a completed sample for a specific scenario. Call alongside
    /// [`Self::record`] — this feeds the per-scenario breakdown the TUI
    /// scenarios table shows.
    pub fn record_scenario(
        &self,
        scenario_id: u16,
        latency_ns: u64,
        bytes_sent: u64,
        bytes_recv: u64,
    ) {
        if let Some(slot) = self.scenario_counters.get(scenario_id as usize) {
            slot.requests.fetch_add(1, Ordering::Relaxed);
            slot.bytes_sent.fetch_add(bytes_sent, Ordering::Relaxed);
            slot.bytes_recv.fetch_add(bytes_recv, Ordering::Relaxed);
            let ns = clamp_ns(latency_ns);
            let shard = &slot.latency_shards[pick_shard()];
            let mut h = shard.lock();
            let _ = h.record(ns);
        }
    }

    /// Increment the per-scenario error counter for `kind`.
    pub fn record_scenario_error(&self, scenario_id: u16, kind: ErrorKind) {
        if let Some(slot) = self.scenario_counters.get(scenario_id as usize) {
            slot.errors.incr(kind);
        }
    }

    /// Swap in an empty histogram + reset the counters, returning the
    /// prior bucket as a [`LiveTick`]. Call once per second from a
    /// dedicated ticker task.
    pub fn swap_and_snapshot(&self) -> LiveTick {
        let elapsed = self.start.elapsed();
        let requests = self.requests.swap(0, Ordering::Relaxed);
        let bytes_sent = self.bytes_sent.swap(0, Ordering::Relaxed);
        let bytes_recv = self.bytes_recv.swap(0, Ordering::Relaxed);
        let errors = self.errors.swap_all();
        let latency = merge_shards(&self.latency_shards);
        let per_scenario: Vec<ScenarioTick> = self
            .scenario_counters
            .iter()
            .map(|slot| ScenarioTick {
                requests: slot.requests.swap(0, Ordering::Relaxed),
                bytes_sent: slot.bytes_sent.swap(0, Ordering::Relaxed),
                bytes_recv: slot.bytes_recv.swap(0, Ordering::Relaxed),
                errors: slot.errors.swap_all(),
                latency: merge_shards(&slot.latency_shards),
            })
            .collect();
        LiveTick {
            elapsed,
            requests,
            bytes_sent,
            bytes_recv,
            errors,
            latency,
            per_scenario,
        }
    }

    /// Start instant — exposed so callers can compute custom offsets.
    pub fn start(&self) -> Instant {
        self.start
    }
}

// ---------------------------------------------------------------------------
// LiveTick
// ---------------------------------------------------------------------------

/// One per-second window's worth of data, produced by
/// [`LiveSnapshot::swap_and_snapshot`] and consumed by the JSONL
/// writer.
#[derive(Debug)]
pub struct LiveTick {
    /// Time elapsed from the snapshot's creation to this swap.
    pub elapsed: Duration,
    /// Requests completed in this window (delta from the prior tick).
    pub requests: u64,
    /// Bytes written on-wire this window.
    pub bytes_sent: u64,
    /// Bytes read on-wire this window.
    pub bytes_recv: u64,
    /// Errors recorded this window.
    pub errors: ErrorCounters,
    /// Latency samples captured this window. Feeds the percentile
    /// fields on the JSONL line.
    pub latency: Histogram<u64>,
    /// Per-scenario breakdown for this window. Index = scenario_id.
    /// Empty when the snapshot was created with `num_scenarios == 0`.
    pub per_scenario: Vec<ScenarioTick>,
}

/// Per-scenario counters for one tick window.
#[derive(Debug)]
pub struct ScenarioTick {
    pub requests: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub errors: ErrorCounters,
    pub latency: Histogram<u64>,
}

impl LiveTick {
    /// Latency at percentile `pct`, in nanoseconds. Returns 0 when the
    /// bucket had no samples.
    pub fn latency_p_ns(&self, pct: f64) -> u64 {
        if self.latency.is_empty() {
            0
        } else {
            self.latency.value_at_percentile(pct)
        }
    }
}

fn clamp_ns(ns: u64) -> u64 {
    if ns < HIST_LO_NS {
        HIST_LO_NS
    } else if ns > HIST_HI_NS {
        HIST_HI_NS
    } else {
        ns
    }
}

/// Atomically swap each shard with a fresh empty histogram and merge
/// the previous buckets into one. Runs once per tick; per-shard mutex
/// hold is strictly shorter than the record path, so writers never
/// observe the merge window.
fn merge_shards(shards: &[Mutex<Histogram<u64>>; LATENCY_SHARDS]) -> Histogram<u64> {
    let mut merged = new_hist();
    for shard in shards.iter() {
        let prev = {
            let mut guard = shard.lock();
            std::mem::replace(&mut *guard, new_hist())
        };
        // `add` can fail only on incompatible bounds — every shard is
        // built with the same `new_hist` so this is unreachable.
        let _ = merged.add(&prev);
    }
    merged
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_then_swap_returns_counts() {
        let live = LiveSnapshot::new(0);
        live.record(100_000, 10, 20);
        live.record(200_000, 11, 21);
        live.record(300_000, 12, 22);
        let tick = live.swap_and_snapshot();
        assert_eq!(tick.requests, 3);
        assert_eq!(tick.bytes_sent, 33);
        assert_eq!(tick.bytes_recv, 63);
        assert_eq!(tick.latency.len(), 3);
        assert!(tick.per_scenario.is_empty());
    }

    #[test]
    fn swap_resets_counters() {
        let live = LiveSnapshot::new(0);
        live.record(100_000, 1, 2);
        let _ = live.swap_and_snapshot();
        let t2 = live.swap_and_snapshot();
        assert_eq!(t2.requests, 0);
        assert_eq!(t2.bytes_sent, 0);
        assert!(t2.latency.is_empty());
    }

    #[test]
    fn record_error_categorises_correctly() {
        let live = LiveSnapshot::new(0);
        live.record_error(ErrorKind::Connect);
        live.record_error(ErrorKind::Status5xx);
        live.record_error(ErrorKind::Status5xx);
        let tick = live.swap_and_snapshot();
        assert_eq!(tick.errors.connect, 1);
        assert_eq!(tick.errors.status_5xx, 2);
        assert_eq!(tick.errors.read, 0);
    }

    #[test]
    fn clamp_ns_bounds() {
        assert_eq!(clamp_ns(0), HIST_LO_NS);
        assert_eq!(clamp_ns(500), 500);
        assert_eq!(clamp_ns(u64::MAX), HIST_HI_NS);
    }

    #[test]
    fn many_threads_record_concurrently_all_samples_survive() {
        // Regression cover for the S3 sharding refactor: N threads
        // each record M samples. After swap the total sample count
        // and request counter must equal N * M, regardless of how
        // the hash-based shard assignment played out.
        let live = LiveSnapshot::new(1);
        const THREADS: usize = 32;
        const PER_THREAD: u64 = 2_000;

        let mut handles = Vec::with_capacity(THREADS);
        for t in 0..THREADS {
            let live = Arc::clone(&live);
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    // Spread latencies across shards so the merged
                    // histogram's percentiles would visibly break
                    // if a shard were dropped.
                    let lat_ns = 10_000 + ((t as u64) * 1000) + i;
                    live.record(lat_ns, 10, 20);
                    live.record_scenario(0, lat_ns, 10, 20);
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panicked");
        }

        let tick = live.swap_and_snapshot();
        let expected = (THREADS as u64) * PER_THREAD;
        assert_eq!(tick.requests, expected);
        assert_eq!(tick.latency.len(), expected);
        assert_eq!(tick.per_scenario[0].requests, expected);
        assert_eq!(tick.per_scenario[0].latency.len(), expected);
    }

    #[test]
    fn pick_shard_is_stable_per_thread() {
        // Calling `pick_shard` repeatedly from the same thread must
        // return the same slot — the thread-local caching is what
        // keeps hash cost out of the record hot path.
        let first = pick_shard();
        for _ in 0..1_000 {
            assert_eq!(pick_shard(), first);
        }
    }

    #[test]
    fn per_scenario_record_and_swap() {
        let live = LiveSnapshot::new(2);

        // Scenario 0: 2 requests.
        live.record_scenario(0, 100_000, 10, 20);
        live.record_scenario(0, 200_000, 11, 21);
        // Scenario 1: 1 request.
        live.record_scenario(1, 300_000, 5, 8);
        // Scenario 1 error.
        live.record_scenario_error(1, ErrorKind::Status4xx);

        // Out-of-range scenario_id silently ignored.
        live.record_scenario(99, 100_000, 1, 1);
        live.record_scenario_error(99, ErrorKind::Connect);

        let tick = live.swap_and_snapshot();
        assert_eq!(tick.per_scenario.len(), 2);

        let s0 = &tick.per_scenario[0];
        assert_eq!(s0.requests, 2);
        assert_eq!(s0.bytes_sent, 21);
        assert_eq!(s0.bytes_recv, 41);
        assert_eq!(s0.latency.len(), 2);
        assert_eq!(s0.errors.total(), 0);

        let s1 = &tick.per_scenario[1];
        assert_eq!(s1.requests, 1);
        assert_eq!(s1.bytes_sent, 5);
        assert_eq!(s1.bytes_recv, 8);
        assert_eq!(s1.latency.len(), 1);
        assert_eq!(s1.errors.status_4xx, 1);

        // After swap, scenario counters are reset.
        let t2 = live.swap_and_snapshot();
        assert_eq!(t2.per_scenario[0].requests, 0);
        assert_eq!(t2.per_scenario[1].requests, 0);
        assert!(t2.per_scenario[0].latency.is_empty());
    }
}
