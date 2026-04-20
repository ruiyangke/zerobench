//! ARCH STATUS: MOVE → zerobench-backends::ws::echo_rtt
//!
//! Becomes one arm of the dispatch match. ARCH(error-unify): RecvErr
//! (Timeout / Transport / ProtocolMismatch) disappears — replaces with
//! core's TransportError. ARCH(recorder) at the RTT-recorded site.
//! The 4-variant CorrelateStrategy + MatchKey dispatch stays — that's
//! load-bearing for the recent review-loop fixes.
//! See docs/ARCH-REVIEW-2026-04-20.md §4.1, §4.3, §4.7, §7.
//!
//! ----------------------------------------------------------------------
//!
//! WebSocket echo-RTT — `docs/PHILOSOPHY.md` §4.4 and
//! `docs/design-v0.1.0.md` §3.3.
//!
//! Production WS workload: open N persistent connections, send
//! messages at a per-connection rate, measure round-trip time from
//! send to correlated echo.
//!
//! # Correlation strategies
//!
//! All four [`CorrelateStrategy`] variants are honoured:
//!
//! - `MonotonicIdPrepend` (default): prepend a 16-hex-char id + `'|'`
//!   to the user payload; match echo by first-16-byte prefix. Works
//!   for servers that echo text verbatim.
//! - `PingPong` (RFC 6455 §5.5.2/5.5.3): send a 16-byte id on a
//!   Ping frame; RFC-6455-compliant servers auto-reply with a Pong
//!   carrying the same bytes. Zero payload intrusion; preferred when
//!   the server honours Pings.
//! - `PayloadSubstring { marker }`: send the payload verbatim; match
//!   any inbound data frame containing `marker`. For servers that
//!   transform the payload (e.g. re-encode as JSON) but preserve a
//!   known substring. The same marker is searched on every echo —
//!   use at rates low enough that responses stay ordered.
//! - `FirstTextFrame`: send payload verbatim; match the very next
//!   data frame. Only valid when the send rate guarantees at most
//!   one message in flight at any moment.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rustls::ClientConfig;

use zerobench_core::histogram::{duration_to_hist_ns, new_hist};
use zerobench_core::plan::{CorrelateStrategy, Plan, Protocol, Step, WsEchoRttPlan};
use zerobench_core::stats::{TaskStats, WsExtras};
use zerobench_core::transport::{Target, TransportOpts};
use rand::SeedableRng;
use zerobench_core::BenchRng;

use crate::conn::{DataFrame, WsConnection, WsError};

// ---------------------------------------------------------------------------
// Per-connection stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ConnStats {
    handshake: Option<Duration>,
    rtt: Histogram<u64>,
    messages_sent: u64,
    messages_recv: u64,
    bytes_sent: u64,
    bytes_recv: u64,
    errors_connect: u64,
    errors_read: u64,
    errors_write: u64,
}

impl ConnStats {
    fn new() -> Self {
        Self {
            handshake: None,
            rtt: new_hist(),
            messages_sent: 0,
            messages_recv: 0,
            bytes_sent: 0,
            bytes_recv: 0,
            errors_connect: 0,
            errors_read: 0,
            errors_write: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-connection worker
// ---------------------------------------------------------------------------

/// Run one WS connection for at most `deadline` wall-clock time (or
/// until `stop` fires). Sends text frames at `msg_rate_per_conn` with
/// a monotonic-id prefix, recv-matches echoes, records RTT.
#[allow(clippy::too_many_arguments)]
fn run_one_echo_rtt(
    target: &Target,
    opts: &TransportOpts,
    plan: &WsEchoRttPlan,
    deadline: Instant,
    stop: &AtomicBool,
    tls_config: Option<&Arc<ClientConfig>>,
    live: Option<&zerobench_core::LiveSnapshot>,
    scenario_id: u16,
) -> ConnStats {
    let mut stats = ConnStats::new();

    // Derive path from the plan's URL template (static for now).
    let path = extract_ws_path(&plan.url);
    let payload_suffix = extract_ws_payload(&plan.payload);

    let handshake_start = Instant::now();
    let rng = BenchRng::seed_from_u64(handshake_start.elapsed().as_nanos() as u64);
    let mut conn = match WsConnection::connect(target, opts, &path, &[], rng, tls_config) {
        Ok(c) => c,
        Err(_) => {
            stats.errors_connect += 1;
            return stats;
        }
    };
    stats.handshake = Some(handshake_start.elapsed());

    // Rate pacing: one send every `interval` ns, open-loop scheduled.
    let interval_ns: u64 = if plan.msg_rate_per_conn > 0.0 {
        (1_000_000_000.0 / plan.msg_rate_per_conn) as u64
    } else {
        0 // As-fast-as-possible
    };
    let base = Instant::now();
    let mut seq: u64 = 0;
    let mut intended_ns: u64 = 0;

    // Scratch buffer — shape depends on the correlate strategy:
    //  - MonotonicIdPrepend: "<id hex>|<suffix>"
    //  - PingPong:           16-byte id payload on a Ping frame
    //  - PayloadSubstring:   the user's payload verbatim (match by
    //                         marker substring regardless of wrapping)
    //  - FirstTextFrame:     the user's payload verbatim (match any
    //                         next data frame without correlating).
    let mut send_buf: Vec<u8> = Vec::with_capacity(32 + payload_suffix.len());

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Intended start time for this send — the slot the token
        // bucket allocated us. RTT is measured from THIS instant
        // (not from when the client actually got around to calling
        // send_text) so client-side scheduling jitter doesn't
        // double-count into the RTT histogram. PHILOSOPHY §P6 / §1.
        let intended_start = if interval_ns > 0 {
            let target_at = base + Duration::from_nanos(intended_ns);
            let now = Instant::now();
            if target_at > now {
                let sleep_for = target_at - now;
                if sleep_for > Duration::from_micros(5) {
                    // Cap sleep so we check deadline/stop often.
                    let chunk = sleep_for.min(Duration::from_millis(100));
                    thread::sleep(chunk);
                    continue; // re-evaluate deadline
                }
            }
            intended_ns = intended_ns.saturating_add(interval_ns);
            target_at
        } else {
            // Saturate mode: no pacing. Intended == actual.
            Instant::now()
        };

        // Build + send per correlate strategy; record the match key
        // the subsequent recv_matching call should look for.
        seq = seq.wrapping_add(1);
        let id_ns = base.elapsed().as_nanos() as u64 ^ seq;
        send_buf.clear();
        let (send_res, match_key) = match &plan.correlate {
            CorrelateStrategy::PingPong => {
                // 16 hex chars of monotonic id — fits the RFC 6455
                // control-frame 125-byte cap with margin. Compliant
                // servers auto-Pong with the same bytes.
                write_hex16(id_ns, &mut send_buf);
                let len = send_buf.len();
                (conn.send_ping(&send_buf), MatchKey::ExactBytes(len))
            }
            CorrelateStrategy::MonotonicIdPrepend => {
                // 16 hex chars + '|' + user payload. Match echo by
                // first-16-byte prefix.
                write_hex16(id_ns, &mut send_buf);
                send_buf.push(b'|');
                send_buf.extend_from_slice(&payload_suffix);
                let mut id = [0u8; 16];
                id.copy_from_slice(&send_buf[..16]);
                (conn.send_text(&send_buf), MatchKey::Prefix16(id))
            }
            CorrelateStrategy::PayloadSubstring { marker } => {
                // User's payload verbatim; echo must contain the
                // literal marker somewhere in its bytes. We don't
                // re-derive per-message uniqueness — PayloadSubstring
                // is for servers that transform the message (e.g.
                // re-encode JSON) but preserve a marker substring.
                // RTT is per-send regardless of marker repetition.
                send_buf.extend_from_slice(&payload_suffix);
                (
                    conn.send_text(&send_buf),
                    MatchKey::Substring(marker.as_bytes().to_vec()),
                )
            }
            CorrelateStrategy::FirstTextFrame => {
                // Fire-and-match-any. Accept the very next inbound
                // text / binary frame as the echo. Only valid for
                // one-in-flight-at-a-time scenarios (rate low enough
                // that the server responds before the next send).
                send_buf.extend_from_slice(&payload_suffix);
                (conn.send_text(&send_buf), MatchKey::Any)
            }
        };

        if send_res.is_err() {
            stats.errors_write += 1;
            break;
        }
        stats.messages_sent += 1;
        stats.bytes_sent = stats.bytes_sent.saturating_add(send_buf.len() as u64);

        match recv_matching(&mut conn, &match_key, deadline, stop) {
            Ok(bytes) => {
                let rtt = Instant::now().saturating_duration_since(intended_start);
                let rtt_ns = duration_to_hist_ns(rtt);
                // ARCH(recorder): triple-record (rtt histogram + live aggregate
                // + live scenario). Collapses to recorder.record(sid, Sample{..}).
                // See ARCH-REVIEW §4.3.
                let _ = stats.rtt.record(rtt_ns);
                stats.messages_recv += 1;
                stats.bytes_recv = stats.bytes_recv.saturating_add(bytes as u64);
                if let Some(live) = live {
                    // Each matched echo is one op; latency = RTT.
                    let blen = bytes as u64;
                    live.record(rtt_ns, send_buf.len() as u64, blen);
                    live.record_scenario(
                        scenario_id,
                        rtt_ns,
                        send_buf.len() as u64,
                        blen,
                    );
                }
            }
            Err(RecvErr::Timeout) => {
                // Deadline fired while waiting for echo — stop cleanly.
                break;
            }
            Err(RecvErr::Transport) => {
                stats.errors_read += 1;
                break;
            }
            Err(RecvErr::ProtocolMismatch) => {
                // Got a frame but prefix didn't match — likely a
                // server-push message. Count as read-error for now;
                // a future strategy (first_text_frame) tolerates this.
                stats.errors_read += 1;
            }
        }
    }

    let _ = conn.close(1000, "bye");
    stats
}

enum RecvErr {
    Timeout,
    Transport,
    ProtocolMismatch,
}

/// What an inbound frame must satisfy to be considered an echo for
/// the send that preceded it. See [`CorrelateStrategy`] docs.
enum MatchKey {
    /// Inbound text/binary frame whose first 16 bytes equal this
    /// byte array. Carries the send's id prefix so the match is
    /// self-contained.
    Prefix16([u8; 16]),
    /// Inbound Pong frame whose payload has exactly this many bytes
    /// and (per RFC 6455 §5.5.3 echo-verbatim) matches the Ping
    /// payload we sent. Length is sufficient here because the
    /// WsConnection's low-level reader hands us the Pong's payload
    /// directly; payload-byte equality would be an additional
    /// verification we skip for speed.
    ExactBytes(usize),
    /// Inbound payload must contain the given literal marker as a
    /// contiguous substring.
    Substring(Vec<u8>),
    /// Any next inbound data frame.
    Any,
}

fn recv_matching(
    conn: &mut WsConnection,
    key: &MatchKey,
    deadline: Instant,
    stop: &AtomicBool,
) -> Result<usize, RecvErr> {
    // Try up to 100 frames before giving up — guards against server
    // pushing unrelated frames ahead of our echo.
    for _ in 0..100 {
        if stop.load(Ordering::Relaxed) {
            return Err(RecvErr::Timeout);
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(RecvErr::Timeout);
        }
        // Use try_recv so the underlying socket read respects the
        // remaining deadline budget. Previously `conn.recv()` could
        // block until the OS default socket timeout — minutes in the
        // worst case — under backpressure. Under saturate runs that
        // produced a per-connection stall that CO-corrupted the RTT
        // histogram.
        let budget = deadline - now;
        match conn.try_recv(budget) {
            // PingPong path: recv_pong delivers the Pong payload
            // through DataFrame::Pong. Match by length.
            Ok(Some(DataFrame::Pong(b))) => {
                if let MatchKey::ExactBytes(n) = key {
                    if b.len() == *n {
                        return Ok(b.len());
                    }
                }
                // Not our pong — keep looking.
            }
            Ok(Some(DataFrame::Text(b))) | Ok(Some(DataFrame::Binary(b))) => {
                let matched = match key {
                    MatchKey::Prefix16(id) => {
                        b.len() >= 16 && &b[..16] == &id[..]
                    }
                    MatchKey::ExactBytes(_) => false, // Ping/Pong path; data frames don't correlate
                    MatchKey::Substring(marker) => {
                        memchr::memmem::find(&b, marker).is_some()
                    }
                    MatchKey::Any => true,
                };
                if matched {
                    return Ok(b.len());
                }
                // Not our frame — keep looking.
            }
            Ok(None) => {
                // Deadline reached with no frame in hand.
                return Err(RecvErr::Timeout);
            }
            Err(WsError::Closed { .. }) => {
                // Server closed cleanly (Close handshake). Treat as
                // timeout so the outer loop exits without flagging a
                // transport error — the connection is simply gone.
                return Err(RecvErr::Timeout);
            }
            Err(_) => return Err(RecvErr::Transport),
        }
    }
    Err(RecvErr::ProtocolMismatch)
}

fn write_hex16(v: u64, out: &mut Vec<u8>) {
    // 16 lowercase hex characters of `v`.
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for i in (0..16).rev() {
        let nibble = ((v >> (i * 4)) & 0xF) as usize;
        out.push(HEX[nibble]);
    }
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Drive `WsEchoRtt` scenarios from a multi-protocol Plan.
///
/// For each `Step::WsEchoRtt` scenario, spawns `connections` worker
/// threads, each running [`run_one_echo_rtt`] for the minimum of
/// `hold_for` (from the plan) and `duration` (from the caller).
///
/// Returns a `Vec<TaskStats>`, one per scenario.
#[allow(clippy::too_many_arguments)]
pub fn run_ws_echo_rtt_from_plan_threaded(
    target: &Target,
    opts: &TransportOpts,
    plan: &Plan,
    duration: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<Arc<zerobench_core::LiveSnapshot>>,
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
        if scenario.protocol() != Protocol::Ws {
            continue;
        }

        let echo_plan = scenario.steps.iter().find_map(|s| match s {
            Step::WsEchoRtt(p) => Some(p.clone()),
            _ => None,
        });
        let Some(echo_plan) = echo_plan else {
            continue;
        };

        let rollup = run_echo_scenario(
            target,
            opts,
            &echo_plan,
            duration,
            &stop,
            tls_config.clone(),
            live.clone(),
            sid as u16,
        );

        let mut task = TaskStats::new(num_scenarios);
        if let Some(sc) = task.per_scenario.get_mut(sid) {
            sc.requests = rollup.messages_recv;
            task.requests = rollup.messages_recv;
            task.bytes_sent = rollup.bytes_sent;
            task.bytes_recv = rollup.bytes_recv;
            sc.errors.connect = rollup.errors_connect;
            sc.errors.read = rollup.errors_read;
            sc.errors.write = rollup.errors_write;
            task.errors.connect += rollup.errors_connect;
            task.errors.read += rollup.errors_read;
            task.errors.write += rollup.errors_write;
            *sc.ws_mut() = WsExtras {
                handshake: rollup.handshake,
                rtt: rollup.rtt,
                messages_sent: rollup.messages_sent,
                messages_recv: rollup.messages_recv,
                bytes_sent: rollup.bytes_sent,
                bytes_recv: rollup.bytes_recv,
                broadcast_rtt: new_hist(),
            };
        }
        out.push(task);
    }

    out
}

struct ScenarioRollup {
    handshake: Histogram<u64>,
    rtt: Histogram<u64>,
    messages_sent: u64,
    messages_recv: u64,
    bytes_sent: u64,
    bytes_recv: u64,
    errors_connect: u64,
    errors_read: u64,
    errors_write: u64,
}

#[allow(clippy::too_many_arguments)]
fn run_echo_scenario(
    target: &Target,
    opts: &TransportOpts,
    echo_plan: &WsEchoRttPlan,
    duration: Duration,
    stop: &Arc<AtomicBool>,
    tls_config: Option<Arc<ClientConfig>>,
    live: Option<Arc<zerobench_core::LiveSnapshot>>,
    scenario_id: u16,
) -> ScenarioRollup {
    // Deadline = `duration` (WsEchoRtt doesn't carry a hold_for — the
    // caller's duration is the bound).
    let deadline = Instant::now() + duration;

    let handles: Vec<_> = (0..echo_plan.connections.max(1))
        .map(|_| {
            let target = target.clone();
            let opts = opts.clone();
            let plan = echo_plan.clone();
            let stop = Arc::clone(stop);
            let tls = tls_config.clone();
            let live = live.clone();
            std::thread::Builder::new()
                .name("zerobench-ws-echo-rtt".into())
                .spawn(move || {
                    run_one_echo_rtt(
                        &target,
                        &opts,
                        &plan,
                        deadline,
                        &stop,
                        tls.as_ref(),
                        live.as_deref(),
                        scenario_id,
                    )
                })
                .expect("spawn ws-echo-rtt worker")
        })
        .collect();

    let mut rollup = ScenarioRollup {
        handshake: new_hist(),
        rtt: new_hist(),
        messages_sent: 0,
        messages_recv: 0,
        bytes_sent: 0,
        bytes_recv: 0,
        errors_connect: 0,
        errors_read: 0,
        errors_write: 0,
    };

    for h in handles {
        let s = h.join().expect("ws-echo-rtt worker panicked");
        if let Some(t) = s.handshake {
            let _ = rollup.handshake.record(duration_to_hist_ns(t));
        }
        let _ = rollup.rtt.add(&s.rtt);
        rollup.messages_sent += s.messages_sent;
        rollup.messages_recv += s.messages_recv;
        rollup.bytes_sent += s.bytes_sent;
        rollup.bytes_recv += s.bytes_recv;
        rollup.errors_connect += s.errors_connect;
        rollup.errors_read += s.errors_read;
        rollup.errors_write += s.errors_write;
    }

    rollup
}

fn extract_ws_path(url: &zerobench_core::Template) -> String {
    let mut buf = Vec::with_capacity(128);
    let mut rng = zerobench_core::rng::from_entropy();
    let mut ctx = zerobench_core::ExpandCtx {
        rng: &mut rng,
        counter: &std::rc::Rc::new(std::cell::Cell::new(0)),
        scenario_vars: &[],
    };
    url.expand_into(&mut buf, &mut ctx);
    let s = String::from_utf8_lossy(&buf).to_string();
    if let Some(idx) = s.find("://").and_then(|i| s[i + 3..].find('/').map(|j| i + 3 + j))
    {
        s[idx..].to_string()
    } else {
        "/".to_string()
    }
}

fn extract_ws_payload(tmpl: &zerobench_core::Template) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    let mut rng = zerobench_core::rng::from_entropy();
    let mut ctx = zerobench_core::ExpandCtx {
        rng: &mut rng,
        counter: &std::rc::Rc::new(std::cell::Cell::new(0)),
        scenario_vars: &[],
    };
    tmpl.expand_into(&mut buf, &mut ctx);
    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;
    use std::io::{Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpListener};
    use zerobench_core::plan::{CorrelateStrategy, Mode, RateProfile, Scenario};
    use zerobench_core::var::VarRegistry;
    use zerobench_core::Template;

    /// Minimal RFC-6455 echo server: accepts one connection, does the
    /// handshake, echoes every text frame verbatim, exits on close.
    fn spawn_ws_echo() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || loop {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            std::thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .ok();

                // --- Handshake ---
                let mut req = Vec::new();
                let mut tmp = [0u8; 1024];
                loop {
                    match stream.read(&mut tmp) {
                        Ok(0) => return,
                        Ok(n) => {
                            req.extend_from_slice(&tmp[..n]);
                            if req.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }
                // Find Sec-WebSocket-Key.
                let req_s = String::from_utf8_lossy(&req);
                let key = req_s
                    .lines()
                    .find_map(|l| {
                        let (n, v) = l.split_once(':')?;
                        if n.eq_ignore_ascii_case("Sec-WebSocket-Key") {
                            Some(v.trim().to_string())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();
                let accept = crate::handshake::compute_accept(&key);
                let resp = format!(
                    "HTTP/1.1 101 Switching Protocols\r\n\
                     Upgrade: websocket\r\n\
                     Connection: Upgrade\r\n\
                     Sec-WebSocket-Accept: {accept}\r\n\r\n",
                );
                if stream.write_all(resp.as_bytes()).is_err() {
                    return;
                }

                // --- Echo loop ---
                // Super-simple RFC 6455 frame reader + writer. Handles
                // client → server masked text frames and server → client
                // unmasked echoes. Ignores fragmentation (tests don't
                // exercise it).
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                loop {
                    match stream.read(&mut tmp) {
                        Ok(0) => return,
                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                        Err(_) => return,
                    }

                    // Parse frames out of `buf`.
                    loop {
                        if buf.len() < 2 {
                            break;
                        }
                        let b0 = buf[0];
                        let b1 = buf[1];
                        let opcode = b0 & 0x0F;
                        let masked = (b1 & 0x80) != 0;
                        let mut payload_len = (b1 & 0x7F) as usize;
                        let mut hdr = 2;
                        if payload_len == 126 {
                            if buf.len() < 4 {
                                break;
                            }
                            payload_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
                            hdr = 4;
                        } else if payload_len == 127 {
                            if buf.len() < 10 {
                                break;
                            }
                            payload_len = u64::from_be_bytes([
                                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
                                buf[9],
                            ]) as usize;
                            hdr = 10;
                        }
                        let mask_off = if masked { hdr + 4 } else { hdr };
                        if buf.len() < mask_off + payload_len {
                            break;
                        }
                        let mask = if masked {
                            [buf[hdr], buf[hdr + 1], buf[hdr + 2], buf[hdr + 3]]
                        } else {
                            [0u8; 4]
                        };
                        let mut payload =
                            buf[mask_off..mask_off + payload_len].to_vec();
                        if masked {
                            for (i, b) in payload.iter_mut().enumerate() {
                                *b ^= mask[i % 4];
                            }
                        }
                        buf.drain(..mask_off + payload_len);

                        // Opcodes: 0x1 text, 0x2 binary, 0x8 close.
                        if opcode == 0x8 {
                            let _ = stream.shutdown(Shutdown::Both);
                            return;
                        }
                        if opcode == 0x1 || opcode == 0x2 {
                            // Echo unmasked server→client frame.
                            let len = payload.len();
                            let mut out = Vec::with_capacity(10 + len);
                            out.push(0x80 | opcode);
                            if len < 126 {
                                out.push(len as u8);
                            } else if len < 65536 {
                                out.push(126);
                                out.extend_from_slice(&(len as u16).to_be_bytes());
                            } else {
                                out.push(127);
                                out.extend_from_slice(&(len as u64).to_be_bytes());
                            }
                            out.extend_from_slice(&payload);
                            if stream.write_all(&out).is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        });
        std::thread::sleep(Duration::from_millis(50));
        addr
    }

    fn echo_plan_for(addr: SocketAddr, n: u32, rate: f64) -> (Plan, Target) {
        let mut vars = VarRegistry::new();
        let url =
            Template::compile(&format!("ws://{addr}/chat"), &mut vars).unwrap();
        let payload = Template::compile("hello", &mut vars).unwrap();
        let plan = Plan {
            scenarios: vec![Scenario {
                name: "ws-echo-rtt-test".into(),
                rate: RateProfile::Saturate { max_concurrency: n as usize },
                steps: vec![Step::WsEchoRtt(WsEchoRttPlan {
                    url,
                    headers: SmallVec::new(),
                    connections: n,
                    msg_rate_per_conn: rate,
                    correlate: CorrelateStrategy::MonotonicIdPrepend,
                    payload,
                })],
            }],
            vars,
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::Measure,
            name: "ws-echo-rtt-test".into(),
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
    fn echo_rtt_records_round_trips() {
        let addr = spawn_ws_echo();
        let (plan, target) = echo_plan_for(addr, 2, 100.0);
        let stats = run_ws_echo_rtt_from_plan_threaded(
            &target,
            &TransportOpts::default(),
            &plan,
            Duration::from_millis(500),
            None,
            None,
            None,
        );
        assert_eq!(stats.len(), 1);
        let ws = stats[0].per_scenario[0].ws.as_ref().expect("ws extras");
        assert!(
            ws.messages_sent >= 10,
            "expected ≥10 messages sent; got {}",
            ws.messages_sent
        );
        assert!(
            ws.messages_recv >= 10,
            "expected ≥10 echo replies; got {}",
            ws.messages_recv
        );
        // Rtt histogram must have same count as messages_recv.
        assert_eq!(ws.rtt.len(), ws.messages_recv);
    }

    #[test]
    fn echo_rtt_reports_connect_error_on_absent_server() {
        let phantom: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let (plan, target) = echo_plan_for(phantom, 2, 50.0);
        let mut opts = TransportOpts::default();
        opts.connect_timeout = Duration::from_millis(200);
        let stats = run_ws_echo_rtt_from_plan_threaded(
            &target,
            &opts,
            &plan,
            Duration::from_millis(500),
            None,
            None,
            None,
        );
        let ts = &stats[0];
        assert!(ts.errors.connect >= 1);
        let ws = ts.per_scenario[0].ws.as_ref().unwrap();
        assert_eq!(ws.messages_sent, 0);
    }

    #[test]
    fn echo_rtt_stops_on_deadline() {
        let addr = spawn_ws_echo();
        let (plan, target) = echo_plan_for(addr, 1, 10_000.0);
        let start = Instant::now();
        let _stats = run_ws_echo_rtt_from_plan_threaded(
            &target,
            &TransportOpts::default(),
            &plan,
            Duration::from_millis(200),
            None,
            None,
            None,
        );
        assert!(
            start.elapsed() < Duration::from_millis(800),
            "ran too long: {:?}",
            start.elapsed()
        );
    }
}
