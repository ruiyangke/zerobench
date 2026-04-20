//! S8: integration smoke tests for WS step variants that previously
//! had no end-to-end coverage: WsHold, WsServerPushRtt, WsFanout.
//!
//! Each test spins up a minimal RFC 6455 stub server (no TLS), runs
//! the appropriate `run_*_from_plan_threaded` entry point for a short
//! duration, and asserts that the resulting `TaskStats` shape matches
//! the scenario's promise — e.g. WsHold sends at least one heartbeat,
//! WsServerPushRtt records at least one inbound frame, WsFanout
//! records frames after a trigger fires.
//!
//! The stub servers deliberately do the minimum framing needed: only
//! fin + opcode + short payload, no fragmentation, no masks on server
//! → client. That matches how the real backends exercise them and
//! keeps the tests focused on the dispatcher-level behaviour the
//! other tests don't cover.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use smallvec::SmallVec;
use zerobench_core::plan::{
    FanoutMode, HeartbeatFrame, Mode, Plan, RateProfile, Scenario, Step, TriggerSpec,
    WsFanoutPlan, WsHoldPlan, WsServerPushRttPlan,
};
use zerobench_core::template::Template;
use zerobench_core::transport::{AddrFamily, Target, TransportOpts};
use zerobench_core::var::VarRegistry;

// ---------------------------------------------------------------------------
// Stub-server helpers (shared across WsHold / WsServerPushRtt / WsFanout)
// ---------------------------------------------------------------------------

/// Read the HTTP/1.1 upgrade request off `stream` and write a valid
/// RFC 6455 101 response. Returns the `Sec-WebSocket-Key` the client
/// sent, useful for test assertions (though the backends don't care).
fn do_ws_handshake(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut req = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before handshake",
            ));
        }
        req.extend_from_slice(&tmp[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
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
    let accept = zerobench_backends::ws::handshake::compute_accept(&key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n",
    );
    stream.write_all(resp.as_bytes())?;
    Ok(key)
}

/// Read one complete client → server frame. Returns
/// `(opcode, payload)` on success, `None` on clean close / EOF.
///
/// Handles the extended-length and masking that RFC 6455 §5.3
/// requires on client-originated frames. Not fragmentation-aware —
/// the backends only send single-frame control + text/binary here.
fn read_masked_frame(stream: &mut TcpStream) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 2];
    if read_exact_or_eof(stream, &mut header)? == 0 {
        return Ok(None);
    }
    let opcode = header[0] & 0x0F;
    let masked = (header[1] & 0x80) != 0;
    let mut len = (header[1] & 0x7F) as usize;
    if len == 126 {
        let mut ext = [0u8; 2];
        stream.read_exact(&mut ext)?;
        len = u16::from_be_bytes(ext) as usize;
    } else if len == 127 {
        let mut ext = [0u8; 8];
        stream.read_exact(&mut ext)?;
        len = u64::from_be_bytes(ext) as usize;
    }
    let mask = if masked {
        let mut k = [0u8; 4];
        stream.read_exact(&mut k)?;
        k
    } else {
        [0u8; 4]
    };
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload)?;
    }
    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    Ok(Some((opcode, payload)))
}

fn read_exact_or_eof(stream: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return Ok(filled),
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Write an unmasked server → client frame with the given opcode +
/// payload. FIN bit always set — no fragmentation.
fn write_server_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut out = Vec::with_capacity(10 + payload.len());
    out.push(0x80 | opcode);
    if payload.len() < 126 {
        out.push(payload.len() as u8);
    } else if payload.len() < 65536 {
        out.push(126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    out.extend_from_slice(payload);
    stream.write_all(&out)
}

// ---------------------------------------------------------------------------
// Plan / Target builders
// ---------------------------------------------------------------------------

fn ws_target(addr: SocketAddr) -> Target {
    Target {
        host: "127.0.0.1".into(),
        port: addr.port(),
        tls: false,
        sni: None,
        addr_family: AddrFamily::V4,
    }
}

fn make_plan(name: &str, step: Step) -> Plan {
    Plan {
        scenarios: vec![Scenario {
            name: name.into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![step],
        }],
        vars: VarRegistry::new(),
        duration: Duration::from_millis(500),
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::Measure,
        name: name.into(),
    }
}

// ---------------------------------------------------------------------------
// WsHold
// ---------------------------------------------------------------------------

/// Stub that accepts a single WS client, responds to client ping
/// frames (opcode 0x9) with pongs, and counts the pings seen so the
/// test can assert the client actually sent heartbeats.
fn spawn_ws_hold_stub(ping_count: Arc<Mutex<u32>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .ok();
            if do_ws_handshake(&mut stream).is_err() {
                return;
            }
            loop {
                match read_masked_frame(&mut stream) {
                    Ok(Some((opcode, payload))) => match opcode {
                        0x9 => {
                            // Client ping → server pong with same payload.
                            *ping_count.lock().unwrap() += 1;
                            if write_server_frame(&mut stream, 0xA, &payload).is_err() {
                                return;
                            }
                        }
                        0x1 | 0x2 => {
                            // Text / binary heartbeat — just count as a ping.
                            *ping_count.lock().unwrap() += 1;
                        }
                        0x8 => return,
                        _ => {}
                    },
                    _ => return,
                }
            }
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn ws_hold_sends_heartbeats_and_records_stats() {
    let pings = Arc::new(Mutex::new(0u32));
    let addr = spawn_ws_hold_stub(Arc::clone(&pings));

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("ws://{addr}/"), &mut vars).unwrap();
    let hold = WsHoldPlan {
        url,
        headers: SmallVec::new(),
        connections: 1,
        heartbeat: Duration::from_millis(100),
        heartbeat_frame: HeartbeatFrame::Ping,
        hold_for: Duration::from_millis(400),
    };
    let mut plan = make_plan("ws-hold-smoke", Step::WsHold(hold));
    plan.vars = vars;

    let stop = Arc::new(AtomicBool::new(false));
    let stop_timer = Arc::clone(&stop);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(600));
        stop_timer.store(true, Ordering::Relaxed);
    });

    let stats = zerobench_backends::ws::run_ws_hold_from_plan_threaded(
        &ws_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(500),
        None,
        None,
        Some(stop),
    );
    assert_eq!(stats.len(), 1, "one scenario");
    // At least one heartbeat should have reached the server in 400ms at 100ms cadence.
    let seen = *pings.lock().unwrap();
    assert!(
        seen >= 2,
        "expected ≥2 heartbeat pings at stub; got {seen}"
    );
}

// ---------------------------------------------------------------------------
// WsServerPushRtt
// ---------------------------------------------------------------------------

/// Stub that accepts a WS client, performs the handshake, then pushes
/// text frames at roughly `rate_hz` for `duration`. Measures whether
/// the client records the frames via `run_ws_server_push_rtt_*`.
fn spawn_ws_push_stub(rate_hz: u32, duration: Duration) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            if do_ws_handshake(&mut stream).is_err() {
                return;
            }
            let interval = Duration::from_millis(1000 / rate_hz.max(1) as u64);
            let deadline = std::time::Instant::now() + duration;
            let mut seq: u64 = 0;
            while std::time::Instant::now() < deadline {
                let payload = format!("push-{seq}");
                if write_server_frame(&mut stream, 0x1, payload.as_bytes()).is_err() {
                    return;
                }
                seq += 1;
                std::thread::sleep(interval);
            }
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn ws_server_push_rtt_records_inbound_frames() {
    let addr = spawn_ws_push_stub(50, Duration::from_millis(600));

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("ws://{addr}/"), &mut vars).unwrap();
    let push = WsServerPushRttPlan {
        url,
        headers: SmallVec::new(),
        connections: 1,
        expected_rate_per_conn: 0.0,
        hold_for: Duration::from_millis(500),
    };
    let mut plan = make_plan("ws-push-smoke", Step::WsServerPushRtt(push));
    plan.vars = vars;

    let stats = zerobench_backends::ws::run_ws_server_push_rtt_from_plan_threaded(
        &ws_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(500),
        None,
        None,
        None,
    );
    assert_eq!(stats.len(), 1);
    let ws = stats[0].per_scenario[0]
        .ws
        .as_ref()
        .expect("ws extras present");
    assert!(
        ws.messages_recv >= 5,
        "expected ≥5 pushed frames at 50 Hz for 500ms; got {}",
        ws.messages_recv
    );
    assert!(ws.bytes_recv > 0, "some bytes must have been received");
}

// ---------------------------------------------------------------------------
// WsFanout
// ---------------------------------------------------------------------------

/// Combined WS-subscriber + HTTP-trigger stub on a single port.
///
/// The WsFanout backend assumes `trigger_url` lives on the same
/// target as the WS subscriber URL (see the same-host comment in
/// `zerobench_backends::ws::fanout::fire_http_trigger`), so we run both sides
/// on one `TcpListener` and dispatch per-connection based on the
/// request line: `GET /` with an `Upgrade` header → WebSocket
/// subscriber; `POST /broadcast` → trigger + broadcast.
fn spawn_ws_fanout_stubs(triggers_seen: Arc<std::sync::atomic::AtomicU32>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Shared subscriber table. Each entry is a write-half clone held
    // behind a Mutex so the trigger side can broadcast without
    // racing a subscriber parking thread.
    let subs: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));

    std::thread::spawn(move || loop {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let subs = Arc::clone(&subs);
        let triggers_seen = Arc::clone(&triggers_seen);
        std::thread::spawn(move || {
            // Read until end-of-headers.
            let mut req = Vec::new();
            let mut tmp = [0u8; 2048];
            let mut body_len = 0usize;
            let mut header_end = None;
            while header_end.is_none() {
                let n = match stream.read(&mut tmp) {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };
                req.extend_from_slice(&tmp[..n]);
                header_end = req.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4);
            }
            let end = header_end.unwrap();
            let req_s = String::from_utf8_lossy(&req[..end - 4]);
            // Identify upgrade vs POST.
            let has_upgrade = req_s
                .lines()
                .any(|l| l.to_ascii_lowercase().starts_with("upgrade:"));
            if has_upgrade {
                // Parse Sec-WebSocket-Key from the already-read headers.
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
                let accept = zerobench_backends::ws::handshake::compute_accept(&key);
                let resp = format!(
                    "HTTP/1.1 101 Switching Protocols\r\n\
                     Upgrade: websocket\r\n\
                     Connection: Upgrade\r\n\
                     Sec-WebSocket-Accept: {accept}\r\n\r\n",
                );
                if stream.write_all(resp.as_bytes()).is_err() {
                    return;
                }
                // Register as a subscriber and park the reader.
                if let Ok(clone) = stream.try_clone() {
                    subs.lock().unwrap().push(clone);
                }
                loop {
                    match read_masked_frame(&mut stream) {
                        Ok(Some(_)) => continue,
                        _ => return,
                    }
                }
            }
            // --- HTTP trigger path ---
            for line in req_s.lines() {
                if let Some(v) = line
                    .to_ascii_lowercase()
                    .strip_prefix("content-length:")
                {
                    body_len = v.trim().parse().unwrap_or(0);
                }
            }
            // Drain body if content-length says there's more.
            let already_in_body = req.len() - end;
            let mut need = body_len.saturating_sub(already_in_body);
            while need > 0 {
                let n = match stream.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => return,
                };
                need = need.saturating_sub(n);
            }
            triggers_seen.fetch_add(1, Ordering::Relaxed);
            let _ = stream.write_all(
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            // Broadcast to every live subscriber.
            let payload = b"broadcast";
            let mut guard = subs.lock().unwrap();
            guard.retain_mut(|s| write_server_frame(s, 0x1, payload).is_ok());
        });
    });

    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn ws_fanout_receives_broadcasts_after_triggers() {
    let trigger_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let addr = spawn_ws_fanout_stubs(Arc::clone(&trigger_count));
    let ws_addr = addr;

    let mut vars = VarRegistry::new();
    let ws_url = Template::compile(&format!("ws://{ws_addr}/"), &mut vars).unwrap();
    // Trigger must live on the SAME host:port as the subscribers —
    // zerobench_backends::ws::fanout::fire_http_trigger reuses the WS Target
    // for the POST (same-host assumption documented in fanout.rs).
    let trigger_url =
        Template::compile(&format!("http://{ws_addr}/broadcast"), &mut vars).unwrap();
    let fanout = WsFanoutPlan {
        subscribers: WsHoldPlan {
            url: ws_url,
            headers: SmallVec::new(),
            connections: 2,
            heartbeat: Duration::from_secs(60),
            heartbeat_frame: HeartbeatFrame::Ping,
            hold_for: Duration::from_millis(2_000),
        },
        trigger: TriggerSpec::HttpPost {
            url: trigger_url,
            body: None,
        },
        mode: FanoutMode::TriggerRtt,
    };
    let mut plan = make_plan("ws-fanout-smoke", Step::WsFanout(fanout));
    plan.vars = vars;
    plan.duration = Duration::from_millis(2_000);

    // The fanout trigger fires every 500ms (TRIGGER_INTERVAL_MS in
    // zerobench_backends::ws::fanout), so the test window needs ≥ 1.5s to
    // observe at least two firings even under slow CI scheduling.
    let stats = zerobench_backends::ws::run_ws_fanout_from_plan_threaded(
        &ws_target(ws_addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(2_000),
        None,
        None,
        None,
    );
    assert_eq!(stats.len(), 1);
    // Triggers fire at a fixed cadence inside the backend — we just
    // need to observe that at least one fired end-to-end and that
    // subscribers received at least one broadcast.
    let triggers = trigger_count.load(Ordering::Relaxed);
    assert!(
        triggers >= 1,
        "expected ≥1 HTTP trigger from the backend; got {triggers}"
    );
    let ws = stats[0].per_scenario[0]
        .ws
        .as_ref()
        .expect("ws extras present");
    assert!(
        ws.messages_recv >= 1,
        "expected ≥1 broadcast frame across subscribers; got {}",
        ws.messages_recv
    );
}

// ---------------------------------------------------------------------------
// WsEchoRtt correlate-strategy coverage
//
// Each test spins up a stub that matches the strategy's on-wire
// expectations: echo text verbatim for Prefix16, auto-Pong for
// PingPong, etc. — and asserts that RTTs are actually recorded. A
// failure means the strategy is registered on the Rhai surface but
// not honoured by the backend (which was the pre-fix state for
// everything except MonotonicIdPrepend).
// ---------------------------------------------------------------------------

use zerobench_core::plan::{CorrelateStrategy, WsEchoRttPlan};

fn spawn_ws_echo_text_verbatim() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            std::thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .ok();
                if do_ws_handshake(&mut stream).is_err() {
                    return;
                }
                loop {
                    match read_masked_frame(&mut stream) {
                        Ok(Some((0x1, payload))) | Ok(Some((0x2, payload))) => {
                            let _ = write_server_frame(&mut stream, 0x1, &payload);
                        }
                        Ok(Some((0x9, payload))) => {
                            // RFC 6455 auto-Pong.
                            let _ = write_server_frame(&mut stream, 0xA, &payload);
                        }
                        Ok(Some((0x8, _))) | Ok(None) | Err(_) => return,
                        _ => {}
                    }
                }
            });
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    addr
}

fn echo_plan(addr: SocketAddr, correlate: CorrelateStrategy, payload: &str) -> Plan {
    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("ws://{addr}/"), &mut vars).unwrap();
    let payload_tpl = Template::compile(payload, &mut vars).unwrap();
    let plan_step = Step::WsEchoRtt(WsEchoRttPlan {
        url,
        headers: SmallVec::new(),
        connections: 1,
        msg_rate_per_conn: 100.0,
        correlate,
        payload: payload_tpl,
    });
    let mut plan = make_plan("ws-echo-correlate-smoke", plan_step);
    plan.vars = vars;
    plan
}

#[test]
fn ws_echo_rtt_correlate_pingpong_records_rtt() {
    let addr = spawn_ws_echo_text_verbatim();
    let plan = echo_plan(addr, CorrelateStrategy::PingPong, "ignored-ping-payload");
    let stats = zerobench_backends::ws::run_ws_echo_rtt_from_plan_threaded(
        &ws_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(300),
        None,
        None,
        None,
    );
    let ws = stats[0].per_scenario[0].ws.as_ref().expect("ws extras");
    assert!(
        ws.messages_recv >= 5,
        "pingpong strategy should match ≥5 pongs in 300ms; got {}",
        ws.messages_recv
    );
    assert!(ws.rtt.len() == ws.messages_recv);
}

#[test]
fn ws_echo_rtt_correlate_substring_records_rtt() {
    let addr = spawn_ws_echo_text_verbatim();
    // Payload carries a unique marker; verbatim echo trivially
    // contains it, so every reply correlates.
    let plan = echo_plan(
        addr,
        CorrelateStrategy::PayloadSubstring {
            marker: "zb-substring-marker".into(),
        },
        r#"{"marker":"zb-substring-marker","payload":"x"}"#,
    );
    let stats = zerobench_backends::ws::run_ws_echo_rtt_from_plan_threaded(
        &ws_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(300),
        None,
        None,
        None,
    );
    let ws = stats[0].per_scenario[0].ws.as_ref().expect("ws extras");
    assert!(
        ws.messages_recv >= 3,
        "substring strategy should match ≥3 echoes in 300ms; got {}",
        ws.messages_recv
    );
}

#[test]
fn ws_echo_rtt_correlate_first_text_frame_records_rtt() {
    let addr = spawn_ws_echo_text_verbatim();
    let plan = echo_plan(
        addr,
        CorrelateStrategy::FirstTextFrame,
        "whatever-the-server-echoes",
    );
    let stats = zerobench_backends::ws::run_ws_echo_rtt_from_plan_threaded(
        &ws_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(300),
        None,
        None,
        None,
    );
    let ws = stats[0].per_scenario[0].ws.as_ref().expect("ws extras");
    assert!(
        ws.messages_recv >= 3,
        "first_text_frame should match ≥3 echoes in 300ms; got {}",
        ws.messages_recv
    );
}
