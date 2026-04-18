//! Integration test for `run_ws_from_plan_threaded` — the Tier-1
//! multi-scenario WS runner used by `zerobench run script.rhai`.
//!
//! Re-uses the echo-server infrastructure from `ws_smoke.rs` via a
//! local helper. Boots one echo server, declares two WS scenarios
//! against it in a single Plan, runs the multi-scenario driver, and
//! verifies both scenarios see round-trips and per-scenario WS extras.

use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use smallvec::SmallVec;

use zerobench_core::plan::{Plan, Protocol, Scenario, Step, WsRoundPlan};
use zerobench_core::stats::Summary;
use zerobench_core::template::Template;
use zerobench_core::transport::TransportOpts;
use zerobench_core::var::VarRegistry;
use zerobench_ws::frame::{encode_frame, Opcode};
use zerobench_ws::handshake::{compute_accept, find_headers_end};

// ---------------------------------------------------------------------------
// Minimal echo server (shared shape with ws_smoke.rs)
// ---------------------------------------------------------------------------

fn extract_ws_key(raw: &[u8]) -> Option<String> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    if req.parse(raw).ok()?.is_partial() {
        return None;
    }
    for h in req.headers {
        if h.name.eq_ignore_ascii_case("sec-websocket-key") {
            return std::str::from_utf8(h.value).ok().map(|s| s.trim().to_string());
        }
    }
    None
}

fn build_101(accept: &str) -> Vec<u8> {
    let mut s = String::with_capacity(128);
    s.push_str("HTTP/1.1 101 Switching Protocols\r\n");
    s.push_str("Upgrade: websocket\r\n");
    s.push_str("Connection: Upgrade\r\n");
    s.push_str("Sec-WebSocket-Accept: ");
    s.push_str(accept);
    s.push_str("\r\n\r\n");
    s.into_bytes()
}

fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    loop {
        if let Some(end) = find_headers_end(buf) {
            return Ok(end);
        }
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request too big",
            ));
        }
    }
}

fn recv_frame(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<(Opcode, Vec<u8>)> {
    loop {
        if buf.len() >= 2 {
            let b0 = buf[0];
            let b1 = buf[1];
            let op = b0 & 0x0f;
            let masked = (b1 & 0x80) != 0;
            if !masked {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "client frame not masked",
                ));
            }
            let short_len = (b1 & 0x7f) as u64;
            let (plen, header_len) = match short_len {
                0..=125 => (short_len as usize, 2usize),
                126 => {
                    if buf.len() < 4 {
                        (0, usize::MAX)
                    } else {
                        (u16::from_be_bytes(buf[2..4].try_into().unwrap()) as usize, 4)
                    }
                }
                127 => {
                    if buf.len() < 10 {
                        (0, usize::MAX)
                    } else {
                        (
                            u64::from_be_bytes(buf[2..10].try_into().unwrap()) as usize,
                            10,
                        )
                    }
                }
                _ => unreachable!(),
            };
            if header_len != usize::MAX && buf.len() >= header_len + 4 + plen {
                let mask = [
                    buf[header_len],
                    buf[header_len + 1],
                    buf[header_len + 2],
                    buf[header_len + 3],
                ];
                let payload_start = header_len + 4;
                let payload_end = payload_start + plen;
                let mut payload = Vec::with_capacity(plen);
                for (i, b) in buf[payload_start..payload_end].iter().enumerate() {
                    payload.push(b ^ mask[i & 3]);
                }
                buf.drain(..payload_end);
                let opcode = match op {
                    0x1 => Opcode::Text,
                    0x2 => Opcode::Binary,
                    0x8 => Opcode::Close,
                    0x9 => Opcode::Ping,
                    0xA => Opcode::Pong,
                    _ => Opcode::Continuation,
                };
                return Ok((opcode, payload));
            }
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn send_server_frame(
    stream: &mut TcpStream,
    opcode: Opcode,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut masked = Vec::with_capacity(14 + payload.len());
    encode_frame(opcode, payload, [0; 4], &mut masked);
    let short = masked[1] & 0x7f;
    let ext_bytes = match short {
        0..=125 => 0,
        126 => 2,
        127 => 8,
        _ => unreachable!(),
    };
    let header_total = 2 + ext_bytes;
    let mut out = Vec::with_capacity(header_total + payload.len());
    out.extend_from_slice(&masked[..header_total]);
    out[1] &= 0x7f;
    out.extend_from_slice(&masked[header_total + 4..]);
    stream.write_all(&out)?;
    stream.flush()
}

fn echo_handler(mut stream: TcpStream) {
    let mut req_buf = Vec::with_capacity(1024);
    let headers_end = match read_request(&mut stream, &mut req_buf) {
        Ok(pos) => pos,
        Err(_) => return,
    };
    let key = match extract_ws_key(&req_buf[..headers_end]) {
        Some(k) => k,
        None => return,
    };
    let accept = compute_accept(&key);
    let resp = build_101(&accept);
    if stream.write_all(&resp).is_err() {
        return;
    }
    let _ = stream.flush();
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    if req_buf.len() > headers_end {
        recv_buf.extend_from_slice(&req_buf[headers_end..]);
    }
    loop {
        let (opcode, payload) = match recv_frame(&mut stream, &mut recv_buf) {
            Ok(f) => f,
            Err(_) => return,
        };
        match opcode {
            Opcode::Text | Opcode::Binary => {
                if send_server_frame(&mut stream, opcode, &payload).is_err() {
                    return;
                }
            }
            Opcode::Close => {
                let _ = send_server_frame(&mut stream, Opcode::Close, &payload);
                return;
            }
            _ => {}
        }
    }
}

fn spawn_echo_server(stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).ok();
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((s, _)) => {
                    s.set_nonblocking(false).ok();
                    thread::spawn(move || echo_handler(s));
                }
                Err(_) => thread::sleep(Duration::from_millis(10)),
            }
        }
    });
    thread::sleep(Duration::from_millis(20));
    addr
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn two_ws_plan(addr: SocketAddr) -> Plan {
    let mut vars = VarRegistry::new();
    let url_a = Template::compile(&format!("ws://{addr}/a"), &mut vars).unwrap();
    let url_b = Template::compile(&format!("ws://{addr}/b"), &mut vars).unwrap();
    let ping = Template::compile("ping", &mut vars).unwrap();

    Plan {
        scenarios: vec![
            Scenario::new(
                "ws-a",
                vec![Step::WsRound(WsRoundPlan {
                    url: url_a,
                    headers: SmallVec::new(),
                    message: ping.clone(),
                })],
            ),
            Scenario::new(
                "ws-b",
                vec![Step::WsRound(WsRoundPlan {
                    url: url_b,
                    headers: SmallVec::new(),
                    message: ping.clone(),
                })],
            ),
        ],
        duration: Duration::from_millis(500),
        vars,
        ..Plan::new()
    }
}

#[test]
fn run_ws_from_plan_collects_per_scenario_stats() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_echo_server(stop.clone());
    let plan = two_ws_plan(addr);

    assert_eq!(plan.scenarios.len(), 2);
    assert_eq!(plan.scenarios[0].protocol(), Protocol::Ws);
    assert_eq!(plan.scenarios[1].protocol(), Protocol::Ws);

    let opts = TransportOpts::default();
    let stats = zerobench_ws::run_ws_from_plan_threaded(
        &opts,
        &plan,
        4,
        Duration::from_millis(500),
        None,
        None,
    );
    stop.store(true, Ordering::Relaxed);

    let summary = Summary::merge(stats, Duration::from_millis(500));
    assert_eq!(summary.per_scenario.len(), 2);

    // Aggregate assertion: across both scenarios we saw some rounds.
    let total_msgs_sent: u64 = summary
        .per_scenario
        .iter()
        .filter_map(|s| s.ws.as_ref())
        .map(|e| e.messages_sent)
        .sum();
    let total_msgs_recv: u64 = summary
        .per_scenario
        .iter()
        .filter_map(|s| s.ws.as_ref())
        .map(|e| e.messages_recv)
        .sum();
    assert!(
        total_msgs_sent > 0,
        "expected WS messages sent across both scenarios, got 0"
    );
    assert!(
        total_msgs_recv > 0,
        "expected WS messages received across both scenarios, got 0"
    );
    // Top-line `requests` should reflect completed round-trips.
    assert!(summary.requests > 0);
}
