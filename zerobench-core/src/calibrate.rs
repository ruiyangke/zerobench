//! ARCH STATUS: MOVE → zerobench-runtime::calibrate
//!
//! Calibration spawns an in-process echo server — that's runtime behaviour,
//! not a core type. Moves wholesale; no rewrite. See
//! docs/ARCH-REVIEW-2026-04-20.md §7, docs/ARCH-TAGS.md.
//!
//! ----------------------------------------------------------------------
//!
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
//! # I/O model
//!
//! Pure mio / non-blocking sockets on both sides. The echo server
//! runs in a background thread with its own [`mio::Poll`]
//! multiplexing the listener and all accepted connections. The
//! client self-check holds one [`mio::Poll`] with a pool of
//! persistent connections; per-request write + readable-wait +
//! read-exact proceed under non-blocking semantics.
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

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use mio::{Events, Interest, Poll, Token};

use crate::histogram::{new_hist, HIST_HI_NS, HIST_LO_NS, HIST_SIG};

// ---------------------------------------------------------------------------
// LoopbackEcho — mio-multiplexed TCP echo server on 127.0.0.1:<ephemeral>.
//
// One background thread owns a single `mio::Poll` driving the listener
// and every accepted connection through non-blocking reads/writes. No
// thread-per-connection, no blocking I/O. Dropping the `LoopbackEcho`
// signals stop and joins the thread.
// ---------------------------------------------------------------------------

/// A minimal in-process TCP echo server for client-side calibration.
///
/// Binds to `127.0.0.1:0` (kernel-chosen port). Every connection is
/// multiplexed on a single mio `Poll` — non-blocking throughout.
/// Dropping the `LoopbackEcho` signals the background thread to stop
/// and joins it.
pub struct LoopbackEcho {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LoopbackEcho {
    /// Spawn the echo server on a kernel-chosen ephemeral port.
    pub fn spawn() -> io::Result<Self> {
        let bind: SocketAddr = "127.0.0.1:0".parse().expect("valid literal");
        let mut listener = MioTcpListener::bind(bind)?;
        let addr = listener.local_addr()?;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let handle = thread::Builder::new()
            .name("zerobench-echo".into())
            .spawn(move || {
                if let Err(_e) = run_echo_loop(&mut listener, &stop_c) {
                    // Socket I/O error during shutdown is fine; anything
                    // mid-run would be surprising but we have no channel
                    // to surface it. Intentionally swallowed.
                }
            })?;

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
}

const LISTENER_TOKEN: Token = Token(usize::MAX);

/// Per-connection echo state: a small read buffer plus whatever
/// bytes are queued for write back to the peer.
struct EchoConn {
    stream: MioTcpStream,
    pending_write: Vec<u8>,
    interests: Interest,
}

impl EchoConn {
    fn new(stream: MioTcpStream) -> Self {
        Self {
            stream,
            pending_write: Vec::new(),
            interests: Interest::READABLE,
        }
    }
}

fn run_echo_loop(listener: &mut MioTcpListener, stop: &AtomicBool) -> io::Result<()> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);
    poll.registry()
        .register(listener, LISTENER_TOKEN, Interest::READABLE)?;

    let mut conns: HashMap<Token, EchoConn> = HashMap::new();
    let mut next_token: usize = 0;

    while !stop.load(Ordering::Relaxed) {
        if let Err(e) = poll.poll(&mut events, Some(Duration::from_millis(100))) {
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        for event in events.iter() {
            if event.token() == LISTENER_TOKEN {
                // Drain the accept queue — edge-triggered friendly.
                loop {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nodelay(true);
                            let token = Token(next_token);
                            next_token = next_token.wrapping_add(1);
                            if poll
                                .registry()
                                .register(&mut stream, token, Interest::READABLE)
                                .is_err()
                            {
                                continue;
                            }
                            conns.insert(token, EchoConn::new(stream));
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
                continue;
            }

            let Some(conn) = conns.get_mut(&event.token()) else { continue };

            let mut drop_conn = false;
            if event.is_readable() {
                drop_conn |= echo_read(conn, poll.registry(), event.token()).is_err();
            }
            if !drop_conn && event.is_writable() {
                drop_conn |= echo_write(conn, poll.registry(), event.token()).is_err();
            }
            if drop_conn {
                if let Some(mut c) = conns.remove(&event.token()) {
                    let _ = poll.registry().deregister(&mut c.stream);
                }
            }
        }
    }

    // Graceful teardown: deregister everything so the Drop path is
    // predictable in nested test harnesses.
    for (_, mut c) in conns.drain() {
        let _ = poll.registry().deregister(&mut c.stream);
    }
    let _ = poll.registry().deregister(listener);
    Ok(())
}

/// Read as much as the kernel has buffered into the pending-write
/// queue. On EOF or fatal error, returns Err so the caller drops the
/// connection.
fn echo_read(
    conn: &mut EchoConn,
    registry: &mio::Registry,
    token: Token,
) -> io::Result<()> {
    let mut buf = [0u8; 8192];
    loop {
        match conn.stream.read(&mut buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")),
            Ok(n) => conn.pending_write.extend_from_slice(&buf[..n]),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    // If we have bytes to echo, try writing immediately; any remainder
    // waits for a writable event.
    if !conn.pending_write.is_empty() {
        echo_write(conn, registry, token)?;
    }
    Ok(())
}

/// Flush as much of the pending-write queue as the kernel accepts.
/// Toggles interest to include WRITABLE when bytes remain.
fn echo_write(
    conn: &mut EchoConn,
    registry: &mio::Registry,
    token: Token,
) -> io::Result<()> {
    while !conn.pending_write.is_empty() {
        match conn.stream.write(&conn.pending_write) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "zero write")),
            Ok(n) => {
                conn.pending_write.drain(..n);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    let want_writable = !conn.pending_write.is_empty();
    let desired = if want_writable {
        Interest::READABLE | Interest::WRITABLE
    } else {
        Interest::READABLE
    };
    if desired != conn.interests {
        registry.reregister(&mut conn.stream, token, desired)?;
        conn.interests = desired;
    }
    Ok(())
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
    pub fn spawn() -> io::Result<Self> {
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
    ) -> io::Result<SelfCheckResult> {
        assert!(rate_rps > 0.0, "rate_rps must be positive");
        assert!(!duration.is_zero(), "duration must be non-zero");

        let pool_size = pool_size.unwrap_or(8).max(1);

        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(pool_size.max(16));

        // Open the pool. Non-blocking connect — register all sockets
        // with WRITABLE and wait for each to signal writable (TCP
        // connect complete).
        let mut pool: Vec<MioTcpStream> = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            let mut s = MioTcpStream::connect(self.echo.addr)?;
            s.set_nodelay(true)?;
            poll.registry().register(
                &mut s,
                Token(i),
                Interest::READABLE | Interest::WRITABLE,
            )?;
            pool.push(s);
        }
        wait_connected(&mut poll, &mut events, &mut pool)?;

        let interval_ns = (1_000_000_000.0 / rate_rps) as u64;
        let start = Instant::now();
        let end = start + duration;

        let mut latency = new_hist();
        let mut jitter = new_hist();

        let mut completed: u64 = 0;
        let mut intended_elapsed_ns: u64 = 0;
        let mut pool_idx: usize = 0;
        let mut req_id: u64 = 0;
        let mut recv_buf = [0u8; 8];

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
                if sleep_for > Duration::from_micros(1) {
                    thread::sleep(sleep_for);
                }
            }

            let actual_start = Instant::now();
            let drift_ns = actual_start
                .duration_since(start)
                .as_nanos()
                .saturating_sub(intended_elapsed_ns as u128) as u64;
            let drift_sample = drift_ns.clamp(HIST_LO_NS, HIST_HI_NS);
            let _ = jitter.record(drift_sample);

            let conn = &mut pool[pool_idx];
            let token = Token(pool_idx);
            pool_idx = (pool_idx + 1) % pool_size;
            req_id += 1;
            let payload = req_id.to_le_bytes();

            // Send. Any failure here ends the check — the caller sees
            // a short run and can interpret via the verdict + elapsed.
            if nb_write_all(&mut poll, &mut events, conn, token, &payload).is_err() {
                break;
            }
            if nb_read_exact(&mut poll, &mut events, conn, token, &mut recv_buf).is_err() {
                break;
            }

            let lat_ns = actual_start.elapsed().as_nanos() as u64;
            let lat_sample = lat_ns.clamp(HIST_LO_NS, HIST_HI_NS);
            let _ = latency.record(lat_sample);
            completed += 1;
            intended_elapsed_ns = intended_elapsed_ns.saturating_add(interval_ns);
        }

        // Deregister everything before returning so repeated calls on
        // the same ClientSelfCheck don't leak kernel state.
        for s in pool.iter_mut() {
            let _ = poll.registry().deregister(s);
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

/// Wait until every connection in `pool` reports writable (TCP connect
/// complete). Returns an error if any connection fails to connect
/// within 5s.
fn wait_connected(
    poll: &mut Poll,
    events: &mut Events,
    pool: &mut [MioTcpStream],
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut ready = vec![false; pool.len()];
    let mut ready_count = 0;

    while ready_count < pool.len() {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "loopback connect timeout",
            ));
        }
        poll.poll(events, Some(deadline - now))?;
        for event in events.iter() {
            let idx = event.token().0;
            if idx >= pool.len() || ready[idx] {
                continue;
            }
            if event.is_error() {
                if let Ok(Some(e)) = pool[idx].take_error() {
                    return Err(e);
                }
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "connect error",
                ));
            }
            if event.is_writable() {
                match pool[idx].peer_addr() {
                    Ok(_) => {
                        ready[idx] = true;
                        ready_count += 1;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::NotConnected => {}
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Ok(())
}

/// Non-blocking write-all: loops `stream.write()` until every byte in
/// `buf` is sent, polling for writable events when the kernel buffer
/// fills. Returns when done or on fatal error.
fn nb_write_all(
    poll: &mut Poll,
    events: &mut Events,
    stream: &mut MioTcpStream,
    token: Token,
    buf: &[u8],
) -> io::Result<()> {
    let mut sent = 0;
    while sent < buf.len() {
        match stream.write(&buf[sent..]) {
            Ok(0) => {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "zero write"));
            }
            Ok(n) => sent += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait_for_interest(poll, events, stream, token, Interest::WRITABLE)?;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Non-blocking read-exact: loops `stream.read()` until `buf` is full,
/// polling for readable events between partial reads.
fn nb_read_exact(
    poll: &mut Poll,
    events: &mut Events,
    stream: &mut MioTcpStream,
    token: Token,
    buf: &mut [u8],
) -> io::Result<()> {
    let mut read = 0;
    while read < buf.len() {
        match stream.read(&mut buf[read..]) {
            Ok(0) => {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
            }
            Ok(n) => read += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait_for_interest(poll, events, stream, token, Interest::READABLE)?;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Poll with a short timeout until `stream`'s `token` signals
/// `desired` readiness. Bounded so a misbehaving server can't hang
/// calibration forever.
fn wait_for_interest(
    poll: &mut Poll,
    events: &mut Events,
    _stream: &mut MioTcpStream,
    token: Token,
    desired: Interest,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_millis(500);
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "calibration poll timeout",
            ));
        }
        poll.poll(events, Some(deadline - now))?;
        for event in events.iter() {
            if event.token() != token {
                continue;
            }
            if event.is_error() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "socket error during calibration",
                ));
            }
            let readable_ok =
                desired == Interest::READABLE && event.is_readable();
            let writable_ok =
                desired == Interest::WRITABLE && event.is_writable();
            if readable_ok || writable_ok {
                return Ok(());
            }
        }
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

    /// Open a blocking-style client against `addr` using std::net — the
    /// tests exercise LoopbackEcho from outside, so a plain blocking
    /// stream is the clearest way to assert the echo behaviour. The
    /// server itself is still pure mio.
    fn client_round_trip(addr: SocketAddr, payload: &[u8]) -> std::io::Result<Vec<u8>> {
        use std::net::TcpStream;
        let mut s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(Duration::from_millis(500)))?;
        s.write_all(payload)?;
        let mut buf = vec![0u8; payload.len()];
        s.read_exact(&mut buf)?;
        Ok(buf)
    }

    #[test]
    fn echo_spawns_and_accepts() {
        let echo = LoopbackEcho::spawn().expect("spawn echo");
        let sent = b"hello-world-1234";
        let got = client_round_trip(echo.addr(), sent).expect("round trip");
        assert_eq!(&got[..], sent);
        drop(echo);
    }

    #[test]
    fn echo_handles_many_sequential_connections() {
        let echo = LoopbackEcho::spawn().expect("spawn");
        for i in 0u32..50 {
            let payload = i.to_le_bytes();
            let got = client_round_trip(echo.addr(), &payload).expect("round trip");
            assert_eq!(got, payload);
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
