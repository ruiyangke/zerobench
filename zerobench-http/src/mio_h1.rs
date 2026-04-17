//! Mio-based raw HTTP/1.1 benchmark client — synchronous epoll event loop,
//! zero async overhead.
//!
//! Each worker thread runs its own [`mio::Poll`] instance with N/T connections.
//! No futures, no wakers, no task scheduling — just a tight event loop:
//!
//! ```text
//! Thread i:
//!   mio::Poll + 38 connections (300 total / 8 threads)
//!
//!   loop {
//!       poll.poll(&mut events)?;
//!       for event in &events {
//!           match conn.state {
//!               Writing  → conn.stream.write(&req_bytes) → state = ReadingHeaders
//!               Reading  → conn.stream.read(&mut buf) → parse headers →
//!                          if complete: record_stats() → state = Writing (next req)
//!           }
//!       }
//!   }
//! ```
//!
//! # Modes
//!
//! - **Saturate (closed-loop)**: `target_rps = None`. Each connection fires
//!   the next request immediately after a response. Classic closed-loop.
//! - **Open-loop (constant-rate)**: `target_rps = Some(rps)`. Requests are
//!   scheduled at a fixed interval via a token-bucket. Latency is measured
//!   from `intended_start` (when the token was generated), not from the
//!   actual write — this is coordinated-omission-free measurement.
//!
//! # Limitations
//!
//! - **No TLS**: mio mode only supports `http://` targets. Pass an `https://`
//!   URL and it will panic at startup with a clear message.
//! - **No per-request template expansion**: request bytes are pre-built once
//!   at startup. `{{uuid}}` gets one UUID for the entire run. This is the
//!   same trade-off `wrk` makes — fixed wire bytes for maximum throughput.
//! - **Single scenario only**: uses `plan.scenarios[0].steps[0]`.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};

use zerobench_core::plan::{Plan, Step};
use zerobench_core::stats::{ErrorKind, TaskStats};
use zerobench_core::transport::Target;

use super::raw_h1_common::{find_connection_close, find_content_length_raw, find_header_end};

// ---------------------------------------------------------------------------
// Connection state machine
// ---------------------------------------------------------------------------

/// State machine for a single HTTP/1.1 keep-alive connection.
struct Conn {
    stream: TcpStream,
    state: ConnState,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    /// How many bytes of `write_buf` have been flushed to the socket.
    write_offset: usize,
    /// Wall-clock instant when the current request was started (write began).
    t0: Instant,
    /// For open-loop mode: the time this request was SCHEDULED to start
    /// (may be earlier than actual write time if the connection was busy).
    /// For saturate mode: same as `t0`.
    intended_start: Instant,
    /// Time-to-first-byte captured when we finish parsing response headers.
    /// Stored here so it survives the ReadingHeaders -> ReadingBody transition.
    ttfb: Duration,
    /// HTTP status code from the response headers.
    status: u16,
}

#[derive(Debug)]
enum ConnState {
    /// Ready to write a new request.
    Idle,
    /// Writing request bytes (may need multiple writes for large bodies).
    Writing,
    /// Waiting for / reading response headers.
    ReadingHeaders,
    /// Reading remaining body bytes (Content-Length known).
    ReadingBody {
        header_len: usize,
        content_length: usize,
    },
    /// Connection is dead — skip this slot for the rest of the run.
    Dead,
}

impl Conn {
    fn new(stream: TcpStream) -> Self {
        let now = Instant::now();
        Self {
            stream,
            state: ConnState::Idle,
            read_buf: Vec::with_capacity(8192),
            write_buf: Vec::with_capacity(512),
            write_offset: 0,
            t0: now,
            intended_start: now,
            ttfb: Duration::ZERO,
            status: 0,
        }
    }

    /// Load `request_bytes` into the write buffer and transition to Writing.
    ///
    /// `intended_start` is the scheduled time for this request. In saturate
    /// mode, pass `Instant::now()`. In open-loop mode, pass the token
    /// timestamp so latency is measured CO-free.
    fn prepare_request(&mut self, request_bytes: &[u8], intended_start: Instant) {
        self.write_buf.clear();
        self.write_buf.extend_from_slice(request_bytes);
        self.write_offset = 0;
        self.read_buf.clear();
        self.intended_start = intended_start;
        self.t0 = Instant::now();
        self.ttfb = Duration::ZERO;
        self.status = 0;
        self.state = ConnState::Writing;
    }

    /// Attempt to flush pending request bytes. Returns `true` when all bytes
    /// have been written and the connection should transition to reading.
    fn try_write(&mut self) -> io::Result<bool> {
        while self.write_offset < self.write_buf.len() {
            match self.stream.write(&self.write_buf[self.write_offset..]) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero"));
                }
                Ok(n) => self.write_offset += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }

    /// Attempt to read response data. Returns `Some((status, ttfb, total))`
    /// when the full response (headers + body) has been received.
    fn try_read(&mut self) -> io::Result<Option<(u16, Duration, Duration)>> {
        let mut tmp = [0u8; 8192];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => {
                    self.state = ConnState::Dead;
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed by server",
                    ));
                }
                Ok(n) => {
                    self.read_buf.extend_from_slice(&tmp[..n]);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        match self.state {
            ConnState::ReadingHeaders => self.check_headers(),
            ConnState::ReadingBody {
                header_len,
                content_length,
            } => {
                let body_received = self.read_buf.len() - header_len;
                if body_received >= content_length {
                    let total = self.t0.elapsed();
                    self.state = ConnState::Idle;
                    return Ok(Some((self.status, self.ttfb, total)));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Parse response headers from `read_buf`. If complete, either returns
    /// the final result (headers + body already in buffer) or transitions
    /// to `ReadingBody`.
    fn check_headers(&mut self) -> io::Result<Option<(u16, Duration, Duration)>> {
        let header_end = match find_header_end(&self.read_buf) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut resp = httparse::Response::new(&mut headers);
        match resp.parse(&self.read_buf[..header_end]) {
            Ok(httparse::Status::Complete(hdr_len)) => {
                let status = resp.code.unwrap_or(0);
                let content_length = find_content_length_raw(resp.headers);
                let ttfb = self.t0.elapsed();
                let keep_alive = !find_connection_close(resp.headers);

                let body_received = self.read_buf.len() - hdr_len;
                if body_received >= content_length {
                    // Full response already in buffer.
                    let total = self.t0.elapsed();
                    if keep_alive {
                        self.state = ConnState::Idle;
                    } else {
                        self.state = ConnState::Dead;
                    }
                    Ok(Some((status, ttfb, total)))
                } else {
                    // Need more body bytes — stash TTFB and status for later.
                    self.ttfb = ttfb;
                    self.status = status;
                    self.state = ConnState::ReadingBody {
                        header_len: hdr_len,
                        content_length,
                    };
                    Ok(None)
                }
            }
            Ok(httparse::Status::Partial) => Ok(None),
            Err(_) => {
                self.state = ConnState::Dead;
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP response parse error",
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Single-thread worker
// ---------------------------------------------------------------------------

/// Run the mio event loop on the calling thread. Blocks until `stop` is
/// set. Returns per-thread stats.
///
/// Each connection is registered with `READABLE | WRITABLE` once. The state
/// machine filters spurious wakeups (e.g. writable while reading) — this
/// avoids 2 `reregister` syscalls per request at the cost of occasional
/// no-op event handling, which is cheaper at high req/s.
///
/// # Open-loop mode
///
/// When `target_rps` is `Some(rps)`, requests are dispatched at a fixed
/// rate using a token-bucket scheduler. Latency is measured from the
/// token's `intended_start` (coordinated-omission-free). Connections that
/// complete a response are returned to an idle pool rather than immediately
/// starting the next request; they wait for the next token.
///
/// When all connections are busy and tokens pile up, the surplus is counted
/// as `keepup` errors — requests that *would* have been sent but couldn't.
pub fn run_mio_worker(
    target: &Target,
    request_bytes: &[u8],
    num_conns: usize,
    stop: &AtomicBool,
    num_scenarios: usize,
    target_rps: Option<f64>,
) -> TaskStats {
    let addr: SocketAddr = target.addr().parse().expect("valid socket address");
    let mut poll = Poll::new().expect("mio::Poll::new");
    let mut events = Events::with_capacity(1024);
    let mut stats = TaskStats::new(num_scenarios);

    let open_loop = target_rps.is_some();
    let token_interval = target_rps.map(|rps| Duration::from_secs_f64(1.0 / rps));
    let started_at = Instant::now();
    let mut next_token_at = started_at;
    // Pre-allocate idle list with capacity; filled below for open-loop.
    let mut idle_conns: Vec<usize> = Vec::with_capacity(num_conns);

    // Open connections and register with mio.
    let mut connections: Vec<Conn> = Vec::with_capacity(num_conns);
    for i in 0..num_conns {
        let mut stream = match TcpStream::connect(addr) {
            Ok(s) => s,
            Err(_) => {
                stats.record_error(0, ErrorKind::Connect);
                continue;
            }
        };
        stream.set_nodelay(true).ok();
        poll.registry()
            .register(
                &mut stream,
                Token(i),
                Interest::READABLE | Interest::WRITABLE,
            )
            .expect("mio register");
        let conn = Conn::new(stream);
        connections.push(conn);
    }

    if open_loop {
        // All connections start idle; tokens will assign them work.
        for i in 0..connections.len() {
            idle_conns.push(i);
        }
    } else {
        // Saturate mode: start all connections immediately.
        let now = Instant::now();
        for conn in &mut connections {
            conn.prepare_request(request_bytes, now);
        }
    }

    // Pending token queue — tokens wait here briefly for a connection
    // to become idle instead of being immediately dropped as keepup.
    // Bounded at 2× connection count to prevent unbounded growth.
    let mut pending_tokens: std::collections::VecDeque<Instant> =
        std::collections::VecDeque::with_capacity(num_conns * 2);
    let max_pending = num_conns * 2;

    while !stop.load(Ordering::Relaxed) {
        // --- Open-loop: generate tokens into the pending queue ----------
        if let Some(interval) = token_interval {
            let now = Instant::now();
            while now >= next_token_at {
                pending_tokens.push_back(next_token_at);
                next_token_at += interval;
            }
            // Cap the queue — excess tokens are keepup drops.
            while pending_tokens.len() > max_pending {
                pending_tokens.pop_front();
                stats.record_error(0, ErrorKind::Keepup);
            }
        }

        // --- Assign pending tokens to idle connections ------------------
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if let Some(conn_idx) = idle_conns.pop() {
                    pending_tokens.pop_front();
                    let conn = &mut connections[conn_idx];
                    conn.prepare_request(request_bytes, intended);
                    match conn.try_write() {
                        Ok(true) => conn.state = ConnState::ReadingHeaders,
                        Ok(false) => {}
                        Err(_) => {
                            conn.state = ConnState::Dead;
                            stats.record_error(0, ErrorKind::Write);
                        }
                    }
                } else {
                    break; // no idle conns — tokens wait in queue
                }
            }
        }

        // --- Calculate poll timeout ------------------------------------
        let poll_timeout = if open_loop {
            if !pending_tokens.is_empty() {
                // Tokens waiting — wake up ASAP to check for completions.
                Some(Duration::ZERO)
            } else {
                // No pending tokens — wake at next token time.
                let until_next = next_token_at.saturating_duration_since(Instant::now());
                Some(until_next.min(Duration::from_millis(1)))
            }
        } else {
            Some(Duration::from_millis(100))
        };

        if poll.poll(&mut events, poll_timeout).is_err() {
            break;
        }

        for event in events.iter() {
            let idx = event.token().0;
            if idx >= connections.len() {
                continue;
            }
            let conn = &mut connections[idx];

            if matches!(conn.state, ConnState::Dead) {
                continue;
            }

            // --- Handle writable (Writing or Idle) ---------------------
            if event.is_writable() {
                match conn.state {
                    ConnState::Writing => match conn.try_write() {
                        Ok(true) => {
                            conn.state = ConnState::ReadingHeaders;
                        }
                        Ok(false) => {} // would-block
                        Err(_) => {
                            conn.state = ConnState::Dead;
                            stats.record_error(0, ErrorKind::Write);
                        }
                    },
                    ConnState::Idle if !open_loop => {
                        // Saturate: start next request immediately.
                        conn.prepare_request(request_bytes, Instant::now());
                        match conn.try_write() {
                            Ok(true) => {
                                conn.state = ConnState::ReadingHeaders;
                            }
                            Ok(false) => {}
                            Err(_) => {
                                conn.state = ConnState::Dead;
                                stats.record_error(0, ErrorKind::Write);
                            }
                        }
                    }
                    _ => {} // spurious writable while reading, or idle in open-loop — ignore
                }
            }

            // --- Handle readable (ReadingHeaders or ReadingBody) -------
            if event.is_readable()
                && matches!(
                    conn.state,
                    ConnState::ReadingHeaders | ConnState::ReadingBody { .. }
                )
            {
                match conn.try_read() {
                    Ok(Some((_status, ttfb, _total))) => {
                        let bytes_sent = conn.write_buf.len() as u64;
                        let bytes_recv = conn.read_buf.len() as u64;
                        // CO-free latency: measured from intended_start.
                        let co_free_latency = conn.intended_start.elapsed();
                        stats.record(0, co_free_latency, ttfb, bytes_sent, bytes_recv);

                        if matches!(conn.state, ConnState::Idle) {
                            if open_loop {
                                // Return connection to the idle pool;
                                // the token scheduler will assign the next request.
                                idle_conns.push(idx);
                            } else {
                                // Saturate: pipeline next request immediately.
                                conn.prepare_request(request_bytes, Instant::now());
                                match conn.try_write() {
                                    Ok(true) => {
                                        conn.state = ConnState::ReadingHeaders;
                                    }
                                    Ok(false) => {}
                                    Err(_) => {
                                        conn.state = ConnState::Dead;
                                        stats.record_error(0, ErrorKind::Write);
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {} // incomplete — keep reading
                    Err(_) => {
                        conn.state = ConnState::Dead;
                        stats.record_error(0, ErrorKind::Read);
                    }
                }
            }
        }

        // --- Post-event token assignment --------------------------------
        // Connections that just went idle can immediately pick up queued
        // tokens. Without this second pass, idle conns would wait until
        // the next outer-loop iteration — wasting an entire poll cycle.
        if open_loop {
            while let Some(&intended) = pending_tokens.front() {
                if let Some(conn_idx) = idle_conns.pop() {
                    pending_tokens.pop_front();
                    let conn = &mut connections[conn_idx];
                    conn.prepare_request(request_bytes, intended);
                    match conn.try_write() {
                        Ok(true) => conn.state = ConnState::ReadingHeaders,
                        Ok(false) => {}
                        Err(_) => {
                            conn.state = ConnState::Dead;
                            stats.record_error(0, ErrorKind::Write);
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }

    stats
}

// ---------------------------------------------------------------------------
// Multi-threaded driver
// ---------------------------------------------------------------------------

/// Spawn `num_threads` OS threads, each with its own mio event loop and an
/// even share of `total_conns`. Blocks until `duration` elapses, then
/// joins all threads and returns their stats.
///
/// `target_rps`: `None` for saturate (closed-loop), `Some(rps)` for
/// open-loop constant-rate mode. The total rate is split evenly across
/// threads (each thread gets `rps / num_threads`).
pub fn run_mio_threaded(
    target: &Target,
    plan: &Plan,
    num_threads: usize,
    total_conns: usize,
    duration: Duration,
    target_rps: Option<f64>,
) -> Vec<TaskStats> {
    assert!(
        !target.tls,
        "mio-h1 mode does not support TLS (https://). Use http:// or remove --mio."
    );

    let request_bytes = build_static_request(plan, target);
    let stop = Arc::new(AtomicBool::new(false));
    let conns_per_thread = total_conns.div_ceil(num_threads);
    let per_thread_rps = target_rps.map(|rps| rps / num_threads as f64);

    // Timer thread — signals stop after `duration`.
    let stop_timer = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(duration);
        stop_timer.store(true, Ordering::Relaxed);
    });

    // Spawn worker threads.
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let target = target.clone();
            let request_bytes = request_bytes.clone();
            let stop = stop.clone();
            let num_scenarios = plan.scenarios.len();

            std::thread::spawn(move || {
                run_mio_worker(
                    &target,
                    &request_bytes,
                    conns_per_thread,
                    &stop,
                    num_scenarios,
                    per_thread_rps,
                )
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().expect("mio worker thread panicked"))
        .collect()
}

// ---------------------------------------------------------------------------
// Request builder — expand once at startup
// ---------------------------------------------------------------------------

/// Pre-build the HTTP/1.1 request wire bytes from the first scenario's
/// first step. Templates are expanded once (not per-request).
fn build_static_request(plan: &Plan, target: &Target) -> Vec<u8> {
    use zerobench_core::rng;
    use zerobench_core::scenario_context::ScenarioContext;

    let step = plan
        .scenarios
        .first()
        .and_then(|s| s.steps.first())
        .expect("plan must have at least one scenario with one step");

    let request_plan = match step {
        Step::Request(r) => r,
        _ => panic!("mio mode requires the first step to be a Request"),
    };

    let mut ctx = ScenarioContext::new(plan.vars.len(), rng::from_entropy());
    let mut buf = Vec::with_capacity(512);
    super::raw_h1_common::build_raw_request(request_plan, &mut ctx, target, &mut buf)
        .expect("failed to build request bytes");
    buf
}

// ---------------------------------------------------------------------------
// Test helpers (public but doc-hidden)
// ---------------------------------------------------------------------------

/// Build raw request bytes from a `RequestPlan`. Exposed for integration
/// tests that need to construct the same wire bytes the mio worker uses.
#[doc(hidden)]
pub fn __test_build_request(
    plan: &zerobench_core::plan::RequestPlan,
    ctx: &mut zerobench_core::scenario_context::ScenarioContext,
    target: &Target,
    out: &mut Vec<u8>,
) {
    super::raw_h1_common::build_raw_request(plan, ctx, target, out)
        .expect("build request");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify `Conn` state transitions on a synthetic buffer that contains
    /// a complete HTTP response in one chunk.
    #[test]
    fn conn_check_headers_complete_response() {
        // Simulate a connection that has already read a full response.
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nhello";

        // We can't create a Conn without a real socket, so test the
        // underlying helpers directly.
        assert!(find_header_end(response).is_some());
        let hdr_end = find_header_end(response).unwrap();
        assert_eq!(&response[hdr_end..], b"hello");

        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut resp = httparse::Response::new(&mut headers);
        let status = resp.parse(&response[..hdr_end]);
        assert!(status.is_ok());
        assert_eq!(resp.code, Some(200));
        assert_eq!(find_content_length_raw(resp.headers), 5);
        assert!(!find_connection_close(resp.headers));
    }

    #[test]
    fn conn_check_headers_partial() {
        let partial = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
        assert!(find_header_end(partial).is_none());
    }
}
