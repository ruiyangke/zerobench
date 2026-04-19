//! SSE `hold` mode — the protocol-native SSE workload.
//!
//! Implements `docs/PHILOSOPHY.md` §4.3 and `docs/design-v0.1.0.md`
//! §3.2: open N concurrent subscribers, hold them for `hold_for`,
//! count individual events (not streams) as the op, measure
//! inter-event gap as the primary latency axis.
//!
//! Answers the production question: "how many concurrent subscribers
//! can the server sustain at what event rate and chunk-gap p99?"
//!
//! # I/O model
//!
//! Per-scenario: one OS thread owns one [`mio::Poll`] multiplexing
//! all `N` subscribers. Each subscriber is a token-indexed state
//! machine (`ConnectingTcp → TlsHandshaking → WritingRequest →
//! ReadingHeaders → ReadingBody`). Plain and TLS subscribers share
//! the same loop via [`MioStream`]. No blocking `std::net` anywhere
//! on the client side.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use mio::net::TcpStream as MioTcp;
use mio::{Events, Interest, Poll, Token};
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist, HIST_HI_NS, HIST_LO_NS};
use zerobench_core::plan::{Plan, Protocol, SseHoldPlan, Step};
use zerobench_core::stats::{SseExtras, TaskStats};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_http::mio_tls::{MioStream, MioTlsStream};

use crate::line_parser::{SseEvent, SseLineParser};

// ---------------------------------------------------------------------------
// Per-subscriber statistics
// ---------------------------------------------------------------------------

/// Stats captured by one subscriber worker thread over its hold
/// lifetime. Folded into the scenario's [`SseExtras`] on merge.
#[derive(Debug, Clone)]
struct HoldStats {
    /// Time-to-first-byte — from request write completion to the
    /// first response byte.
    ttfb: Option<Duration>,
    /// Inter-event gap histogram. Primary latency axis for Hold mode.
    event_gap: Histogram<u64>,
    /// Number of SSE data events observed.
    events: u64,
    /// Payload bytes observed (post-chunked-decoding).
    bytes_received: u64,
    /// `true` if the subscriber saw `[DONE]` before time ran out.
    saw_done: bool,
    /// Connect-path errors (DNS, TCP, TLS).
    errors_connect: u64,
    /// Mid-stream read errors.
    errors_read: u64,
}

impl HoldStats {
    fn new() -> Self {
        Self {
            ttfb: None,
            event_gap: new_hist(),
            events: 0,
            bytes_received: 0,
            saw_done: false,
            errors_connect: 0,
            errors_read: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-subscriber state machine
// ---------------------------------------------------------------------------

/// One subscriber multiplexed on the scenario's shared [`Poll`].
///
/// The `state` field advances monotonically from `ConnectingTcp`
/// through `ReadingBody`; `Dead` is terminal. The driver inspects
/// mio event readiness and the current state to decide what I/O to
/// perform, then updates state accordingly.
struct Subscriber {
    stream: MioStream,
    state: SubState,
    /// Progress through the request bytes during `WritingRequest`.
    write_pos: usize,
    /// Accumulator for HTTP response bytes received before the
    /// `\r\n\r\n` header terminator is seen.
    pre_body: Vec<u8>,
    decoder: ChunkDecoder,
    parser: SseLineParser,
    stats: HoldStats,
    /// Set when the full request has flushed to the wire — used as
    /// the TTFB baseline.
    t_request_sent: Option<Instant>,
    first_byte_at: Option<Instant>,
    last_event_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubState {
    ConnectingTcp,
    TlsHandshaking,
    WritingRequest,
    ReadingHeaders,
    ReadingBody,
    Dead,
}

/// Result of driving a subscriber through one mio event.
enum DriveOutcome {
    /// Subscriber still active — keep polling it.
    Alive,
    /// Subscriber terminated (connect error, read error, EOF). The
    /// caller marks it dead and drops it from the live count.
    Dead,
}

/// Attempt TCP connect + TLS wrap (if required) + register with
/// `poll`. Returns a subscriber in `ConnectingTcp` state ready to be
/// driven by events, or an error to count as a connect failure.
fn create_subscriber(
    addr: SocketAddr,
    target: &Target,
    tls_config: Option<&Arc<ClientConfig>>,
    token: Token,
    registry: &mio::Registry,
) -> io::Result<Subscriber> {
    let tcp = MioTcp::connect(addr)?;
    let _ = tcp.set_nodelay(true);

    let stream = if target.tls {
        let config = tls_config.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "https:// target but no TLS config provided",
            )
        })?;
        let sni = target.sni_name().to_string();
        let tls = MioTlsStream::new(tcp, Arc::clone(config), &sni)?;
        MioStream::Tls(tls)
    } else {
        MioStream::Plain(tcp)
    };

    Ok(Subscriber {
        stream,
        state: SubState::ConnectingTcp,
        write_pos: 0,
        pre_body: Vec::new(),
        decoder: ChunkDecoder::new(),
        parser: SseLineParser::default(),
        stats: HoldStats::new(),
        t_request_sent: None,
        first_byte_at: None,
        last_event_at: None,
    })
    .and_then(|mut sub| {
        registry.register(
            sub.stream.tcp_stream_mut(),
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;
        Ok(sub)
    })
}

/// Drive one subscriber through one mio event. Loops through
/// states transitioning synchronously when possible (e.g.
/// `ConnectingTcp → WritingRequest` after a successful TCP connect
/// reveals no TLS is required), returning to the caller only on
/// `WouldBlock` or terminal events.
fn drive_subscriber(sub: &mut Subscriber, request: &[u8]) -> DriveOutcome {
    loop {
        match sub.state {
            SubState::ConnectingTcp => {
                // mio reports writable once the TCP 3WHS is done.
                // Confirm by probing peer_addr — returns NotConnected
                // while the SYN-ACK is still pending.
                let tcp = sub.stream.tcp_stream();
                match tcp.peer_addr() {
                    Ok(_) => {}
                    Err(ref e) if e.kind() == io::ErrorKind::NotConnected => {
                        return DriveOutcome::Alive;
                    }
                    Err(_) => {
                        sub.stats.errors_connect =
                            sub.stats.errors_connect.saturating_add(1);
                        return DriveOutcome::Dead;
                    }
                }
                sub.state = if sub.stream.is_handshaking() {
                    SubState::TlsHandshaking
                } else {
                    SubState::WritingRequest
                };
            }
            SubState::TlsHandshaking => {
                if let Err(_e) = sub.stream.drive_tls_io() {
                    sub.stats.errors_connect =
                        sub.stats.errors_connect.saturating_add(1);
                    return DriveOutcome::Dead;
                }
                if sub.stream.is_handshaking() {
                    return DriveOutcome::Alive;
                }
                sub.state = SubState::WritingRequest;
            }
            SubState::WritingRequest => loop {
                let remaining = &request[sub.write_pos..];
                if remaining.is_empty() {
                    sub.t_request_sent = Some(Instant::now());
                    sub.state = SubState::ReadingHeaders;
                    break;
                }
                match sub.stream.write(remaining) {
                    Ok(0) => {
                        sub.stats.errors_connect =
                            sub.stats.errors_connect.saturating_add(1);
                        return DriveOutcome::Dead;
                    }
                    Ok(n) => {
                        sub.write_pos = sub.write_pos.saturating_add(n);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return DriveOutcome::Alive;
                    }
                    Err(_) => {
                        sub.stats.errors_connect =
                            sub.stats.errors_connect.saturating_add(1);
                        return DriveOutcome::Dead;
                    }
                }
            },
            SubState::ReadingHeaders => {
                let mut buf = [0u8; 4096];
                match sub.stream.read(&mut buf) {
                    Ok(0) => {
                        sub.stats.errors_read =
                            sub.stats.errors_read.saturating_add(1);
                        return DriveOutcome::Dead;
                    }
                    Ok(n) => {
                        let now = Instant::now();
                        if sub.first_byte_at.is_none() {
                            sub.first_byte_at = Some(now);
                            if let Some(t_sent) = sub.t_request_sent {
                                sub.stats.ttfb =
                                    Some(now.saturating_duration_since(t_sent));
                            }
                        }
                        sub.stats.bytes_received =
                            sub.stats.bytes_received.saturating_add(n as u64);
                        sub.pre_body.extend_from_slice(&buf[..n]);
                        if let Some(hdr_end) = find_header_end(&sub.pre_body) {
                            let body_tail: Vec<u8> =
                                sub.pre_body[hdr_end..].to_vec();
                            sub.pre_body.clear();
                            sub.state = SubState::ReadingBody;
                            if !body_tail.is_empty() {
                                feed_body(sub, &body_tail);
                            }
                        }
                        // Loop again — more header bytes may already
                        // be buffered; falls through to read more.
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return DriveOutcome::Alive;
                    }
                    Err(_) => {
                        sub.stats.errors_read =
                            sub.stats.errors_read.saturating_add(1);
                        return DriveOutcome::Dead;
                    }
                }
            }
            SubState::ReadingBody => {
                let mut buf = [0u8; 8192];
                match sub.stream.read(&mut buf) {
                    Ok(0) => return DriveOutcome::Dead,
                    Ok(n) => {
                        sub.stats.bytes_received =
                            sub.stats.bytes_received.saturating_add(n as u64);
                        feed_body(sub, &buf[..n]);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return DriveOutcome::Alive;
                    }
                    Err(_) => {
                        sub.stats.errors_read =
                            sub.stats.errors_read.saturating_add(1);
                        return DriveOutcome::Dead;
                    }
                }
            }
            SubState::Dead => return DriveOutcome::Dead,
        }
    }
}

/// Feed decoded body bytes through the chunked decoder + SSE line
/// parser, updating `sub.stats` and `sub.last_event_at`.
///
/// Splitting field-level borrows explicitly so the parser callback
/// can mutate `stats` and `last_event_at` without aliasing `parser`
/// or `decoder`.
fn feed_body(sub: &mut Subscriber, input: &[u8]) {
    let mut decoded: Vec<u8> = Vec::with_capacity(input.len());
    let _ended = sub.decoder.decode(input, &mut decoded);
    if decoded.is_empty() {
        return;
    }
    let parser = &mut sub.parser;
    let stats = &mut sub.stats;
    let last_event_at = &mut sub.last_event_at;
    parser.feed(&decoded, |ev| match ev {
        SseEvent::Data(_payload) => {
            let now = Instant::now();
            if let Some(prev) = *last_event_at {
                let gap_ns =
                    duration_to_hist_ns(now.saturating_duration_since(prev));
                let gap_ns = gap_ns.clamp(HIST_LO_NS, HIST_HI_NS);
                let _ = stats.event_gap.record(gap_ns);
            }
            *last_event_at = Some(now);
            stats.events = stats.events.saturating_add(1);
        }
        SseEvent::Done => {
            // PHILOSOPHY §4.3 SseHold: do NOT terminate on [DONE].
            // Hold mode cares about subscriber lifetime, not logical
            // stream end — `[DONE]` is typically an application
            // signal, and real chat / notification servers never
            // emit it. Continue reading until deadline.
            stats.saw_done = true;
        }
        SseEvent::Ignored => {
            // event: / id: / retry: — counters for these land with
            // reconnect-storm in a later commit.
        }
    });
}

/// Returns the byte index after the `\r\n\r\n` header terminator if
/// present, or `None` if the headers don't end in the provided buffer.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .or_else(|| buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2))
}

// ---------------------------------------------------------------------------
// Chunked transfer-encoding decoder
//
// Copied verbatim from lib.rs to keep hold.rs standalone for Phase 6a.
// Phase 6b refactor will pull the decoder into a shared submodule.
// ---------------------------------------------------------------------------

pub(crate) struct ChunkDecoder {
    state: ChunkState,
    size_buf: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    Size,
    Data { remaining: usize },
    TrailerCr,
    TrailerLf,
    Done,
}

impl ChunkDecoder {
    pub(crate) fn new() -> Self {
        Self {
            state: ChunkState::Size,
            size_buf: Vec::with_capacity(16),
        }
    }

    pub(crate) fn decode(&mut self, input: &[u8], out: &mut Vec<u8>) -> bool {
        let mut i = 0;
        while i < input.len() {
            match self.state {
                ChunkState::Size => {
                    let b = input[i];
                    i += 1;
                    if b == b'\n' {
                        let s = if self.size_buf.last() == Some(&b'\r') {
                            &self.size_buf[..self.size_buf.len() - 1]
                        } else {
                            &self.size_buf[..]
                        };
                        let size = parse_hex(s);
                        self.size_buf.clear();
                        if size == 0 {
                            self.state = ChunkState::Done;
                            return true;
                        }
                        self.state = ChunkState::Data { remaining: size };
                    } else {
                        self.size_buf.push(b);
                    }
                }
                ChunkState::Data { remaining } => {
                    let avail = input.len() - i;
                    let take = avail.min(remaining);
                    out.extend_from_slice(&input[i..i + take]);
                    i += take;
                    let left = remaining - take;
                    if left == 0 {
                        self.state = ChunkState::TrailerCr;
                    } else {
                        self.state = ChunkState::Data { remaining: left };
                    }
                }
                ChunkState::TrailerCr => {
                    i += 1;
                    self.state = ChunkState::TrailerLf;
                }
                ChunkState::TrailerLf => {
                    i += 1;
                    self.state = ChunkState::Size;
                }
                ChunkState::Done => return true,
            }
        }
        false
    }
}

fn parse_hex(s: &[u8]) -> usize {
    let mut out: usize = 0;
    for &b in s {
        let d = match b {
            b'0'..=b'9' => (b - b'0') as usize,
            b'a'..=b'f' => (b - b'a' + 10) as usize,
            b'A'..=b'F' => (b - b'A' + 10) as usize,
            _ => break, // chunk extensions start with `;`; stop at first non-hex
        };
        out = out * 16 + d;
    }
    out
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Drive `SseHold` scenarios from a multi-protocol Plan.
///
/// For each `Step::SseHold` scenario, spawns `subscribers` worker
/// threads, each running [`run_one_hold`] for the minimum of
/// `hold_for` (from the plan) and `duration` (from the caller).
///
/// Returns a `Vec<TaskStats>` with one entry per scenario, containing
/// the scenario's aggregated SSE extras. Non-SSE scenarios are
/// silently skipped — callers combine this with the HTTP / WS
/// dispatchers for mixed-protocol runs.
pub fn run_sse_hold_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    stop_flag: Option<Arc<AtomicBool>>,
) -> Vec<TaskStats> {
    let stop = stop_flag.unwrap_or_else(|| {
        let s = Arc::new(AtomicBool::new(false));
        let timer_stop = s.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer_stop.store(true, Ordering::Relaxed);
        });
        s
    });

    let num_scenarios = plan.scenarios.len();
    let mut out: Vec<TaskStats> = Vec::new();

    for (sid, scenario) in plan.scenarios.iter().enumerate() {
        if scenario.protocol() != Protocol::Sse {
            continue;
        }

        // Find the SseHold step; skip scenarios using other protocols.
        let hold_plan = scenario.steps.iter().find_map(|s| match s {
            Step::SseHold(p) => Some(p.clone()),
            _ => None,
        });
        let Some(hold_plan) = hold_plan else { continue };

        let per_scenario_stats =
            run_hold_scenario(target, opts, &hold_plan, duration, &stop, tls_config.clone());

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            sc.requests = per_scenario_stats.events;
            task.requests = per_scenario_stats.events;
            task.bytes_recv = per_scenario_stats.bytes_received;
            sc.errors.connect = per_scenario_stats.errors_connect;
            sc.errors.read = per_scenario_stats.errors_read;
            task.errors.connect += per_scenario_stats.errors_connect;
            task.errors.read += per_scenario_stats.errors_read;
            *sc.sse_mut() = SseExtras {
                ttfb: per_scenario_stats.ttfb,
                chunk_gap: per_scenario_stats.event_gap,
                chunks: per_scenario_stats.events,
                streams_completed: per_scenario_stats.streams_completed,
                bytes_received: per_scenario_stats.bytes_received,
                broadcast_rtt: new_hist(),
            };
        }
        out.push(task);
    }

    out
}

/// Per-scenario rollup produced by fanning out workers and merging
/// their [`HoldStats`].
struct ScenarioRollup {
    ttfb: Histogram<u64>,
    event_gap: Histogram<u64>,
    events: u64,
    bytes_received: u64,
    streams_completed: u64,
    errors_connect: u64,
    errors_read: u64,
}

fn run_hold_scenario(
    target: &Target,
    opts: &TransportOpts,
    hold_plan: &SseHoldPlan,
    duration: Duration,
    stop: &Arc<AtomicBool>,
    tls_config: Option<Arc<ClientConfig>>,
) -> ScenarioRollup {
    let wall_deadline = Instant::now()
        + duration.min(if hold_plan.hold_for.is_zero() {
            duration
        } else {
            hold_plan.hold_for
        });

    let request = build_hold_request(target, hold_plan);
    let n_subs = hold_plan.subscribers.max(1) as usize;

    let mut rollup = ScenarioRollup {
        ttfb: new_hist(),
        event_gap: new_hist(),
        events: 0,
        bytes_received: 0,
        streams_completed: 0,
        errors_connect: 0,
        errors_read: 0,
    };

    // Resolve once — all subscribers share the same target.
    let addr = match target.resolve(opts) {
        Ok(a) => a,
        Err(_) => {
            rollup.errors_connect = n_subs as u64;
            return rollup;
        }
    };

    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(_) => {
            rollup.errors_connect = n_subs as u64;
            return rollup;
        }
    };
    let mut events = Events::with_capacity(n_subs.clamp(64, 1024));

    // Create + register all subscribers. Creation failures count as
    // connect errors but don't abort the scenario — other subscribers
    // continue independently.
    let mut subs: Vec<Option<Subscriber>> = Vec::with_capacity(n_subs);
    for i in 0..n_subs {
        match create_subscriber(
            addr,
            target,
            tls_config.as_ref(),
            Token(i),
            poll.registry(),
        ) {
            Ok(sub) => subs.push(Some(sub)),
            Err(_) => {
                rollup.errors_connect = rollup.errors_connect.saturating_add(1);
                subs.push(None);
            }
        }
    }

    let mut alive = subs.iter().filter(|s| s.is_some()).count();

    // Event loop: single thread multiplexes all subscribers. The
    // 100ms poll timeout bounds how long we wait before re-checking
    // `stop` and `deadline`.
    while alive > 0 {
        let now = Instant::now();
        if stop.load(Ordering::Relaxed) || now >= wall_deadline {
            break;
        }
        let budget = (wall_deadline - now).min(Duration::from_millis(100));
        if poll.poll(&mut events, Some(budget)).is_err() {
            break;
        }
        for event in events.iter() {
            let idx = event.token().0;
            let Some(slot) = subs.get_mut(idx) else { continue };
            let Some(sub) = slot.as_mut() else { continue };
            if sub.state == SubState::Dead {
                continue;
            }

            // Socket-level error → fail fast.
            if event.is_error() {
                if let Ok(Some(_e)) = sub.stream.tcp_stream_mut().take_error() {
                    // Classify as connect error if we hadn't yet sent
                    // the request; otherwise as read error.
                    if sub.t_request_sent.is_none() {
                        sub.stats.errors_connect =
                            sub.stats.errors_connect.saturating_add(1);
                    } else {
                        sub.stats.errors_read =
                            sub.stats.errors_read.saturating_add(1);
                    }
                }
                sub.state = SubState::Dead;
                alive -= 1;
                continue;
            }

            // For TLS, writable events may also need to flush pending
            // cipher output even when the app layer is in a reading
            // state.
            if event.is_writable() {
                sub.stream.flush_tls();
            }

            if let DriveOutcome::Dead = drive_subscriber(sub, &request) {
                sub.state = SubState::Dead;
                alive -= 1;
            }
        }
    }

    // Merge per-subscriber stats into rollup.
    for s in subs.into_iter().flatten() {
        if let Some(ttfb) = s.stats.ttfb {
            let _ = rollup.ttfb.record(duration_to_hist_ns(ttfb));
        }
        let _ = rollup.event_gap.add(&s.stats.event_gap);
        rollup.events = rollup.events.saturating_add(s.stats.events);
        rollup.bytes_received =
            rollup.bytes_received.saturating_add(s.stats.bytes_received);
        if s.stats.saw_done {
            rollup.streams_completed = rollup.streams_completed.saturating_add(1);
        }
        rollup.errors_connect =
            rollup.errors_connect.saturating_add(s.stats.errors_connect);
        rollup.errors_read = rollup.errors_read.saturating_add(s.stats.errors_read);
    }

    rollup
}

/// Build the HTTP/1.1 request bytes for an SSE Hold subscriber.
/// Templates aren't expanded for Phase 6a — the URL must be a static
/// literal. Templated URLs land when the shared scenario-context path
/// is wired in Phase 6b.
fn build_hold_request(target: &Target, plan: &SseHoldPlan) -> Vec<u8> {
    let path = extract_path(&plan.url);
    let host = if (target.tls && target.port == 443) || (!target.tls && target.port == 80) {
        target.host.clone()
    } else {
        format!("{}:{}", target.host, target.port)
    };
    format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Accept: text/event-stream\r\n\
         Cache-Control: no-cache\r\n\
         Connection: keep-alive\r\n\
         \r\n",
    )
    .into_bytes()
}

/// Extract the path+query from a compiled [`zerobench_core::Template`].
/// For Phase 6a we expect a fully-static template; templated URLs
/// yield an empty expansion (falls back to `"/"`).
fn extract_path(url: &zerobench_core::Template) -> String {
    let mut buf = Vec::with_capacity(256);
    let mut rng = zerobench_core::rng::from_entropy();
    let mut ctx = zerobench_core::ExpandCtx {
        rng: &mut rng,
        counter: &std::rc::Rc::new(std::cell::Cell::new(0)),
        scenario_vars: &[],
    };
    url.expand_into(&mut buf, &mut ctx);
    let s = String::from_utf8_lossy(&buf).to_string();
    if let Some(path_start) = s.find("://").and_then(|i| s[i + 3..].find('/').map(|j| i + 3 + j)) {
        s[path_start..].to_string()
    } else {
        "/".to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::net::{Shutdown, TcpListener};
    use std::sync::Arc;

    /// Spawn a minimal SSE stub server that loop-accepts and, per
    /// connection, emits one event every `interval` for `n` events,
    /// then holds the connection open. Returns the bound address.
    fn spawn_stub_sse(events: usize, interval: Duration) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || loop {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            std::thread::spawn(move || {
                let mut buf = [0u8; 1024];
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = stream.read(&mut buf);

                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\n\
                      Content-Type: text/event-stream\r\n\
                      Transfer-Encoding: chunked\r\n\
                      Connection: keep-alive\r\n\
                      Cache-Control: no-cache\r\n\
                      \r\n",
                );

                for i in 0..events {
                    let body = format!("data: event-{i}\n\n");
                    let chunk = format!("{:x}\r\n{}\r\n", body.len(), body);
                    if stream.write_all(chunk.as_bytes()).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                    if interval > Duration::ZERO {
                        std::thread::sleep(interval);
                    }
                }

                std::thread::sleep(Duration::from_secs(2));
                let _ = stream.shutdown(Shutdown::Both);
            });
        });
        std::thread::sleep(Duration::from_millis(50));
        addr
    }

    fn single_hold_plan(addr: SocketAddr, subs: u32, hold_for: Duration) -> (Plan, Target) {
        use smallvec::SmallVec;
        use zerobench_core::plan::{Mode, RateProfile, Scenario};
        use zerobench_core::var::VarRegistry;
        use zerobench_core::Template;

        let mut vars = VarRegistry::new();
        let url =
            Template::compile(&format!("http://{addr}/stream"), &mut vars).unwrap();

        let hold_plan = SseHoldPlan {
            url,
            headers: SmallVec::new(),
            subscribers: subs,
            hold_for,
            reconnect: false,
        };

        let plan = Plan {
            scenarios: vec![Scenario {
                name: "sse-hold-test".into(),
                rate: RateProfile::Saturate { max_concurrency: subs as usize },
                steps: vec![Step::SseHold(hold_plan)],
            }],
            vars,
            duration: Duration::from_secs(2),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::Measure,
            name: "sse-hold-test".into(),
        };

        let target = Target {
            host: "127.0.0.1".to_string(),
            port: addr.port(),
            tls: false,
            sni: None,
            addr_family: zerobench_core::transport::AddrFamily::V4,
        };
        (plan, target)
    }

    #[test]
    fn hold_counts_events_from_single_subscriber() {
        let addr = spawn_stub_sse(10, Duration::from_millis(5));
        let (plan, target) = single_hold_plan(addr, 1, Duration::from_millis(500));
        let opts = TransportOpts::default();
        let stop = Arc::new(AtomicBool::new(false));
        // Short cutoff — enough to see all 10 events.
        let stop_timer = stop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(800));
            stop_timer.store(true, Ordering::Relaxed);
        });

        let stats = run_sse_hold_from_plan_threaded(
            &target,
            &opts,
            &plan,
            Duration::from_millis(500),
            None,
            Some(stop),
        );

        assert_eq!(stats.len(), 1, "one scenario");
        let ts = &stats[0];
        let sse = ts.per_scenario[0].sse.as_ref().expect("sse extras");
        assert!(
            sse.chunks >= 8,
            "expected ≥8 events (server emits 10); got {}",
            sse.chunks
        );
        assert!(
            sse.bytes_received >= 50,
            "expected ≥50 bytes; got {}",
            sse.bytes_received
        );
        // event_gap should have recorded (events - 1) samples.
        assert!(
            sse.chunk_gap.len() >= 7,
            "expected ≥7 event_gap samples; got {}",
            sse.chunk_gap.len()
        );
    }

    #[test]
    fn hold_with_multiple_subscribers_aggregates_events() {
        let addr = spawn_stub_sse(5, Duration::from_millis(10));
        let (plan, target) = single_hold_plan(addr, 2, Duration::from_millis(500));
        let opts = TransportOpts::default();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_timer = stop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            stop_timer.store(true, Ordering::Relaxed);
        });

        let stats = run_sse_hold_from_plan_threaded(
            &target,
            &opts,
            &plan,
            Duration::from_millis(500),
            None,
            Some(stop),
        );

        // With 2 subscribers × 5 events each, expect ≥ 6 total events
        // counted (some slack for slow-start).
        assert_eq!(stats.len(), 1);
        let sse = stats[0].per_scenario[0].sse.as_ref().unwrap();
        assert!(
            sse.chunks >= 6,
            "expected ≥6 events with 2 subs × 5; got {}",
            sse.chunks
        );
    }

    #[test]
    fn hold_stops_on_deadline_even_without_done() {
        // Server emits 100 events with a 50ms interval — more than the
        // 300ms deadline allows. Client must stop on deadline, not
        // wait for all events.
        let addr = spawn_stub_sse(100, Duration::from_millis(50));
        let (plan, target) = single_hold_plan(addr, 1, Duration::from_millis(300));
        let opts = TransportOpts::default();
        let stop = Arc::new(AtomicBool::new(false));

        let start = Instant::now();
        let _stats = run_sse_hold_from_plan_threaded(
            &target,
            &opts,
            &plan,
            Duration::from_millis(300),
            None,
            Some(stop),
        );
        let elapsed = start.elapsed();

        // Should finish within ~400ms — deadline of 300 + a little
        // slack for thread teardown.
        assert!(
            elapsed < Duration::from_millis(800),
            "hold ran too long: {:?}",
            elapsed
        );
    }

    #[test]
    fn hold_records_zero_errors_on_clean_run() {
        let addr = spawn_stub_sse(5, Duration::from_millis(10));
        let (plan, target) = single_hold_plan(addr, 1, Duration::from_millis(300));
        let opts = TransportOpts::default();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_timer = stop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            stop_timer.store(true, Ordering::Relaxed);
        });

        let stats = run_sse_hold_from_plan_threaded(
            &target,
            &opts,
            &plan,
            Duration::from_millis(300),
            None,
            Some(stop),
        );
        let ts = &stats[0];
        assert_eq!(ts.errors.connect, 0);
        assert_eq!(ts.errors.read, 0);
    }

    #[test]
    fn hold_reports_connect_error_when_server_absent() {
        // Point at a port nothing listens on.
        let phantom_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (plan, target) =
            single_hold_plan(phantom_addr, 2, Duration::from_millis(100));
        let mut opts = TransportOpts::default();
        opts.connect_timeout = Duration::from_millis(200);
        let stop = Arc::new(AtomicBool::new(false));

        let stats = run_sse_hold_from_plan_threaded(
            &target,
            &opts,
            &plan,
            Duration::from_millis(300),
            None,
            Some(stop),
        );
        let ts = &stats[0];
        assert!(ts.errors.connect >= 1, "expected connect errors; got {:?}", ts.errors);
        let sse = ts.per_scenario[0].sse.as_ref().unwrap();
        assert_eq!(sse.chunks, 0, "no events when connect fails");
    }
}
