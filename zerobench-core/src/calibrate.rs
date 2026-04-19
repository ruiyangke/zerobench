//! Client-side calibration — P5 / §9.6.2 / §5.1 of the v0.1.0 docs.
//!
//! Before every `measure` / `compare` / `curve` / `soak` run, zerobench
//! verifies the client can sustain the offered rate by generating
//! synthetic traffic against an in-process TCP echo server on the
//! loopback interface. If the client can't sustain ≥99% of the
//! requested rate against loopback, it certainly can't sustain that
//! rate against any real target (loopback is the cheapest possible
//! target — no NIC, no wire, no driver IRQ coalescing).
//!
//! There is **no cache** (§15 Q3 RESOLVED): calibration is cheap
//! (a few seconds), client-local, and every invalidation axis we'd
//! add (tool version, machine fingerprint, concurrency flags, CPU
//! governor) is a way for a stale calibration to lie silently.
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//! use zerobench_core::calibrate::ClientSelfCheck;
//!
//! let check = ClientSelfCheck::spawn()?;
//! let result = check.check(50_000.0, Duration::from_secs(1))?;
//! match result.verdict {
//!     Verdict::Pass => println!("sustained {:.1}% — OK", result.sustained_pct * 100.0),
//!     Verdict::Refuse => eprintln!("client cannot sustain {} req/s here", result.offered_rate),
//!     _ => {}
//! }
//! ```

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::histogram::{new_hist, HIST_HI_NS, HIST_LO_NS, HIST_SIG};

// ---------------------------------------------------------------------------
// LoopbackEcho — a minimal blocking TCP echo server bound to 127.0.0.1:<ephemeral>.
//
// Each accepted connection spawns a small handler thread that echoes
// whatever it reads back to the writer. Intended for calibration only —
// do not use for load benchmarks (one thread per connection doesn't scale).
//
// The listener socket is set to non-blocking mode so the acceptor loop
// can check the stop flag at ~1ms cadence and exit promptly on drop.
// ---------------------------------------------------------------------------

/// A minimal in-process TCP echo server for client-side calibration.
///
/// Binds to `127.0.0.1:0` (kernel-chosen port). Each connection is
/// handled by a dedicated OS thread that echoes read bytes verbatim.
/// Dropping the `LoopbackEcho` signals all threads to stop and joins
/// the acceptor.
pub struct LoopbackEcho {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LoopbackEcho {
    /// Spawn the echo server on a kernel-chosen ephemeral port.
    pub fn spawn() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let handle = thread::Builder::new()
            .name("zerobench-echo-accept".into())
            .spawn(move || Self::accept_loop(listener, stop_c))?;

        Ok(Self {
            addr,
            stop,
            handle: Some(handle),
        })
    }

    /// The bound loopback address; use this as the calibration target.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn accept_loop(listener: TcpListener, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let stop_c = stop.clone();
                    // One handler thread per connection. Fine for ≤16
                    // connections (the calibration pool size). Not for
                    // production workloads.
                    let _ = thread::Builder::new()
                        .name("zerobench-echo-conn".into())
                        .spawn(move || Self::echo_loop(stream, stop_c));
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }
    }

    fn echo_loop(mut stream: TcpStream, stop: Arc<AtomicBool>) {
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
        let mut buf = [0u8; 4096];

        while !stop.load(Ordering::Relaxed) {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if stream.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Read timeout fired — give the stop flag a chance
                    // to be observed, then retry.
                    continue;
                }
                Err(_) => break,
            }
        }
    }
}

impl Drop for LoopbackEcho {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// ClientSelfCheck — drives synthetic traffic against LoopbackEcho.
// ---------------------------------------------------------------------------

/// Outcome of a single calibration run.
#[derive(Debug)]
pub struct SelfCheckResult {
    /// Offered rate (req/s) requested by the caller.
    pub offered_rate: f64,
    /// Actual achieved rate = completed requests / elapsed seconds.
    pub achieved_rate: f64,
    /// `achieved_rate / offered_rate`, clamped to [0.0, 1.0].
    pub sustained_pct: f64,
    /// Distribution of completed-request round-trip latency (ns, HDR).
    pub latency: Histogram<u64>,
    /// Distribution of absolute scheduler drift in nanoseconds
    /// (`|actual_start − intended_start|`). The client's own scheduler
    /// variance; cross-reference against P10's 5µs p99 floor.
    pub jitter: Histogram<u64>,
    /// Number of requests that completed in the window.
    pub completed: u64,
    /// How long the check actually ran for.
    pub elapsed: Duration,
    /// Machine-readable outcome: does the client pass the P5 gate?
    pub verdict: Verdict,
}

/// Pass/fail outcome of the self-check against loopback.
///
/// Thresholds from `docs/PHILOSOPHY.md` §9.6.2 (self-refusal gate):
/// - ≥ 99.0% sustained → `Pass` (proceed with the real run)
/// - 95.0% – 99.0%     → `Marginal` (warn, but allow)
/// - < 95.0%           → `Refuse` (tool refuses without `--force-overload`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Client sustained ≥99% of the requested rate. Safe to proceed.
    Pass,
    /// 95–99% — report the gap but don't block.
    Marginal,
    /// <95% — refuse to run without `--force-overload`.
    Refuse,
}

impl Verdict {
    fn from_sustained(sustained_pct: f64) -> Self {
        if sustained_pct >= 0.99 {
            Self::Pass
        } else if sustained_pct >= 0.95 {
            Self::Marginal
        } else {
            Self::Refuse
        }
    }
}

/// Client-side self-check driver. Owns a `LoopbackEcho` and issues
/// synthetic traffic against it at a caller-specified rate.
pub struct ClientSelfCheck {
    echo: LoopbackEcho,
}

impl ClientSelfCheck {
    /// Spawn the echo server and return a ready-to-drive self-check.
    pub fn spawn() -> std::io::Result<Self> {
        Ok(Self {
            echo: LoopbackEcho::spawn()?,
        })
    }

    /// Run a calibration probe at `rate_rps` for `duration`.
    ///
    /// The caller supplies the pool size via `pool_size` — this is the
    /// number of persistent loopback connections opened and reused
    /// round-robin. Default (`None`) is 8, which is fine for rates up
    /// to ~200k req/s on a modern CPU. Larger rates may need a larger
    /// pool to avoid per-connection serialisation bottlenecks.
    pub fn check(
        &self,
        rate_rps: f64,
        duration: Duration,
        pool_size: Option<usize>,
    ) -> std::io::Result<SelfCheckResult> {
        assert!(rate_rps > 0.0, "rate_rps must be positive");
        assert!(!duration.is_zero(), "duration must be non-zero");

        let pool_size = pool_size.unwrap_or(8).max(1);

        // Open the pool. Every connection is persistent; keep-alive is
        // implicit because we only send on one side at a time.
        let mut pool: Vec<TcpStream> = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let s = TcpStream::connect(self.echo.addr)?;
            s.set_nodelay(true)?;
            s.set_read_timeout(Some(Duration::from_millis(500)))?;
            pool.push(s);
        }

        let interval_ns = (1_000_000_000.0 / rate_rps) as u64;
        let start = Instant::now();
        let end = start + duration;

        let mut latency = new_hist();
        let mut jitter = new_hist();

        let mut completed: u64 = 0;
        let mut intended_elapsed_ns: u64 = 0;
        let mut pool_idx: usize = 0;
        let mut req_id: u64 = 0;
        let mut buf = [0u8; 8];

        loop {
            let now = Instant::now();
            if now >= end {
                break;
            }

            // Open-loop scheduling: compute the intended start time for
            // this request relative to the run origin, sleep if we're
            // ahead, proceed if we're behind.
            let intended_start = start + Duration::from_nanos(intended_elapsed_ns);
            if intended_start > now {
                let sleep_for = intended_start - now;
                // thread::sleep has µs-ish resolution on Linux; for
                // short sleeps we accept the granularity cost.
                if sleep_for > Duration::from_micros(1) {
                    thread::sleep(sleep_for);
                }
            }

            let actual_start = Instant::now();
            let drift_ns = actual_start
                .duration_since(start)
                .as_nanos()
                .saturating_sub(intended_elapsed_ns as u128) as u64;
            // Clamp to the HDR range. Drift 0 becomes 1 (HDR can't
            // record 0).
            let drift_sample = drift_ns.clamp(HIST_LO_NS, HIST_HI_NS);
            let _ = jitter.record(drift_sample);

            // Round-robin through the pool. Sending+reading serially
            // on one connection is fine because each request is tiny.
            let conn = &mut pool[pool_idx];
            pool_idx = (pool_idx + 1) % pool_size;
            req_id += 1;
            let payload = req_id.to_le_bytes();

            // Any failure here ends the check — the caller sees a
            // short run and can interpret via the verdict + elapsed.
            if conn.write_all(&payload).is_err() {
                break;
            }
            if conn.read_exact(&mut buf).is_err() {
                break;
            }

            let lat_ns = actual_start.elapsed().as_nanos() as u64;
            let lat_sample = lat_ns.clamp(HIST_LO_NS, HIST_HI_NS);
            let _ = latency.record(lat_sample);
            completed += 1;
            intended_elapsed_ns = intended_elapsed_ns.saturating_add(interval_ns);
        }

        let elapsed = start.elapsed();
        let elapsed_s = elapsed.as_secs_f64().max(f64::EPSILON);
        let achieved = completed as f64 / elapsed_s;
        let sustained = (achieved / rate_rps).min(1.0);

        Ok(SelfCheckResult {
            offered_rate: rate_rps,
            achieved_rate: achieved,
            sustained_pct: sustained,
            latency,
            jitter,
            completed,
            elapsed,
            verdict: Verdict::from_sustained(sustained),
        })
    }
}

// Silence unused-import warning when `new_hist` isn't referenced in a
// feature-gated build; we always use it here, but keep the re-export
// list consistent with other core modules.
#[allow(dead_code)]
const _HDR_CONSTS: (u64, u64, u8) = (HIST_LO_NS, HIST_HI_NS, HIST_SIG);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn echo_spawns_and_accepts() {
        let echo = LoopbackEcho::spawn().expect("spawn echo");
        let mut conn = TcpStream::connect(echo.addr()).expect("connect");
        conn.set_nodelay(true).ok();
        conn.set_read_timeout(Some(Duration::from_millis(500))).ok();

        let sent = b"hello-world-1234";
        conn.write_all(sent).expect("write");
        let mut buf = [0u8; 16];
        conn.read_exact(&mut buf).expect("read");
        assert_eq!(&buf, sent);

        drop(conn);
        drop(echo); // Drop signals stop to the acceptor + any handler.
    }

    #[test]
    fn echo_handles_many_sequential_connections() {
        let echo = LoopbackEcho::spawn().expect("spawn");
        for i in 0u32..50 {
            let mut c = TcpStream::connect(echo.addr()).expect("connect");
            c.set_nodelay(true).ok();
            c.set_read_timeout(Some(Duration::from_millis(500))).ok();
            let payload = i.to_le_bytes();
            c.write_all(&payload).expect("write");
            let mut buf = [0u8; 4];
            c.read_exact(&mut buf).expect("read");
            assert_eq!(buf, payload);
        }
    }

    #[test]
    fn self_check_low_rate_is_easy_to_sustain() {
        let check = ClientSelfCheck::spawn().expect("spawn");
        // 1 000 req/s for 500 ms. Trivially achievable on any modern
        // box; regressions here would point at a serious scheduler bug.
        let result = check
            .check(1_000.0, Duration::from_millis(500), Some(4))
            .expect("check");

        assert_eq!(
            result.verdict,
            Verdict::Pass,
            "expected Pass at 1k req/s; got sustained_pct={}",
            result.sustained_pct,
        );
        assert!(
            result.sustained_pct >= 0.99,
            "sustained_pct={}",
            result.sustained_pct
        );
        assert!(result.completed >= 400, "completed={}", result.completed);
    }

    #[test]
    fn self_check_refuses_impossible_rate() {
        let check = ClientSelfCheck::spawn().expect("spawn");
        // 1 billion req/s is unachievable on any hardware; the verdict
        // must be `Refuse`. This exercises the gate-on-impossible path.
        let result = check
            .check(1_000_000_000.0, Duration::from_millis(200), Some(8))
            .expect("check");

        assert_eq!(
            result.verdict,
            Verdict::Refuse,
            "expected Refuse at 1G req/s; got sustained_pct={}",
            result.sustained_pct,
        );
        assert!(result.sustained_pct < 0.95);
    }

    #[test]
    fn verdict_thresholds_match_philosophy() {
        assert_eq!(Verdict::from_sustained(1.00), Verdict::Pass);
        assert_eq!(Verdict::from_sustained(0.99), Verdict::Pass);
        assert_eq!(Verdict::from_sustained(0.9899), Verdict::Marginal);
        assert_eq!(Verdict::from_sustained(0.95), Verdict::Marginal);
        assert_eq!(Verdict::from_sustained(0.9499), Verdict::Refuse);
        assert_eq!(Verdict::from_sustained(0.0), Verdict::Refuse);
    }
}
