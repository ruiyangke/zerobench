//! End-to-end smoke tests for the WebSocket runner.
//!
//! Spins up a minimal echo server that handshakes HTTP/1.1 Upgrade and
//! echoes text frames (built entirely with our own frame codec — "eat
//! your own dogfood"), then points [`run_ws_saturate`] at it and
//! verifies:
//!
//! 1. N concurrent connections all make progress — no round-robin
//!    serialisation bug.
//! 2. RTT is sub-10 ms on loopback.
//! 3. `errors_connect` fires on a dead address.
//! 4. Close handshake exits cleanly.

use std::convert::TryInto;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener as CompioTcpListener, TcpStream};

use zerobench_core::stop::StopSignal;
use zerobench_core::transport::Target;
use zerobench_ws::frame::{encode_frame, Opcode};
use zerobench_ws::handshake::{compute_accept, find_headers_end};
use zerobench_ws::{run_ws_saturate, WsPlan, WsSummary};

// ---------------------------------------------------------------------------
// Minimal WS echo server — handshake + echo loop, using our codec.
// ---------------------------------------------------------------------------

/// Minimal HTTP/1 handshake response parser: pulls `Sec-WebSocket-Key`
/// out of the request so we can compute the accept key. Uses httparse
/// so we don't roll our own header parsing.
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

/// Build the server's 101 response.
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

/// Read from `stream` until we see `\r\n\r\n`; accumulate bytes in `buf`.
async fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    loop {
        if let Some(end) = find_headers_end(buf) {
            return Ok(end);
        }
        let chunk: Vec<u8> = Vec::with_capacity(1024);
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before full request",
            ));
        }
        buf.extend_from_slice(&returned[..n]);
        if buf.len() > 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request too big",
            ));
        }
    }
}

/// Server-side reader: pull bytes from the socket until one full frame
/// is buffered. Returns `(opcode, payload, fin)` for the completed frame.
async fn recv_frame(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<(Opcode, Vec<u8>, bool)> {
    loop {
        // Try parsing what's already buffered.
        //
        // Client-sent frames MUST be masked; our `decode_frame` rejects
        // masked input, so we do a simplified header parse + unmask
        // here inline.
        if buf.len() >= 2 {
            let b0 = buf[0];
            let b1 = buf[1];
            let fin = (b0 & 0x80) != 0;
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
                        // fall through to read more
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
                    0x0 => Opcode::Continuation,
                    0x1 => Opcode::Text,
                    0x2 => Opcode::Binary,
                    0x8 => Opcode::Close,
                    0x9 => Opcode::Ping,
                    0xA => Opcode::Pong,
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown opcode {op}"),
                        ))
                    }
                };
                return Ok((opcode, payload, fin));
            }
        }
        // Read more.
        let chunk: Vec<u8> = Vec::with_capacity(4096);
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before full frame",
            ));
        }
        buf.extend_from_slice(&returned[..n]);
    }
}

/// Server side: given a text/binary payload, echo it back as a server
/// (unmasked) frame using our codec.
async fn send_server_frame(
    stream: &mut TcpStream,
    opcode: Opcode,
    payload: &[u8],
) -> std::io::Result<()> {
    // Build a masked-by-zero frame, then strip the mask bytes + clear
    // the MASK bit. The client `decode_frame` rejects masked inputs —
    // this produces the "unmasked server frame" shape correctly.
    let mut masked = Vec::with_capacity(14 + payload.len());
    encode_frame(opcode, payload, [0; 4], &mut masked);

    // Locate the header size.
    let short = masked[1] & 0x7f;
    let ext_bytes = match short {
        0..=125 => 0,
        126 => 2,
        127 => 8,
        _ => unreachable!(),
    };
    let header_total = 2 + ext_bytes;

    // Build the server-shape frame: header bytes (sans MASK bit) + payload.
    let mut out = Vec::with_capacity(header_total + payload.len());
    out.extend_from_slice(&masked[..header_total]);
    out[1] &= 0x7f; // clear MASK bit
    out.extend_from_slice(&masked[header_total + 4..]); // skip the 4 mask bytes; payload is XOR-with-0 i.e. unchanged
    stream.write_all(out).await.0
}

/// Handle one client: accept the Upgrade, then echo every text/binary
/// frame until the client closes.
async fn echo_handler(mut stream: TcpStream) {
    // --- Handshake ---
    let mut req_buf = Vec::with_capacity(1024);
    let headers_end = match read_request(&mut stream, &mut req_buf).await {
        Ok(pos) => pos,
        Err(_) => return,
    };
    let key = match extract_ws_key(&req_buf[..headers_end]) {
        Some(k) => k,
        None => return,
    };
    let accept = compute_accept(&key);
    let resp = build_101(&accept);
    if stream.write_all(resp).await.0.is_err() {
        return;
    }

    // Any bytes after the header terminator are the start of the WS
    // stream. Preserve them.
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    if req_buf.len() > headers_end {
        recv_buf.extend_from_slice(&req_buf[headers_end..]);
    }

    // --- Echo loop ---
    loop {
        let (opcode, payload, _fin) = match recv_frame(&mut stream, &mut recv_buf).await {
            Ok(f) => f,
            Err(_) => return,
        };

        match opcode {
            Opcode::Text | Opcode::Binary => {
                if send_server_frame(&mut stream, opcode, &payload).await.is_err() {
                    return;
                }
            }
            Opcode::Ping => {
                // Pong with same payload.
                let _ = send_server_frame(&mut stream, Opcode::Pong, &payload).await;
            }
            Opcode::Pong => {
                // ignore
            }
            Opcode::Close => {
                // Echo close.
                let _ = send_server_frame(&mut stream, Opcode::Close, &payload).await;
                return;
            }
            Opcode::Continuation => {
                // Our client doesn't fragment, so treat this as a protocol error.
                return;
            }
        }
    }
}

/// Boot a WebSocket echo server on an ephemeral port. Returns the bound
/// address; the server keeps running on its own thread until the test
/// process exits.
fn spawn_echo_server() -> SocketAddr {
    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind); // release the port for compio to rebind

    let (ready_tx, ready_rx): (Sender<()>, _) = channel();

    thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = CompioTcpListener::bind(addr).await.unwrap();
            let _ = ready_tx.send(());
            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                compio::runtime::spawn(async move {
                    echo_handler(socket).await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("echo server never bound");
    addr
}

/// Build a plan against the given address with a small payload.
fn ws_plan_for(addr: SocketAddr, payload: &str) -> WsPlan {
    let target = Target::parse(&format!("ws://{addr}")).unwrap();
    WsPlan {
        target,
        path: "/echo".to_string(),
        headers: Vec::new(),
        message: Bytes::copy_from_slice(payload.as_bytes()),

    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Single connection + N round-trips succeed, RTT is sub-10 ms on loopback.
#[compio::test]
async fn single_connection_many_round_trips() {
    let addr = spawn_echo_server();
    let plan = ws_plan_for(addr, "ping");

    // Run 1 worker for 500 ms — plenty of time for many round-trips.
    let stop = StopSignal::after(Duration::from_millis(500));
    let stats = run_ws_saturate(plan, 1, stop, None).await;
    let summary = WsSummary::merge(stats, Duration::from_millis(500));

    assert!(
        summary.messages_sent > 10,
        "expected many round-trips, got {}",
        summary.messages_sent
    );
    assert_eq!(
        summary.messages_sent, summary.messages_recvd,
        "all messages should round-trip",
    );
    assert_eq!(summary.errors_connect, 0);
    assert_eq!(summary.errors_upgrade, 0);
    assert_eq!(summary.errors_io, 0);

    // RTT p99 should be way under 10 ms on loopback. 10 ms = 10M ns.
    let p99 = summary.rtt.value_at_percentile(99.0);
    assert!(p99 < 10_000_000, "RTT p99 {p99}ns exceeds 10ms on loopback");
}

/// Multiple connections make progress in parallel — not the v1
/// round-robin bug where 50 concurrent connections are serviced
/// one-at-a-time. With a fast loopback echo server and 50 workers we
/// should see all 50 complete many RTs in a short run.
#[compio::test]
async fn many_connections_run_in_parallel() {
    let addr = spawn_echo_server();
    let plan = ws_plan_for(addr, "ping");

    let stop = StopSignal::after(Duration::from_millis(500));
    let stats = run_ws_saturate(plan, 50, stop, None).await;

    // Per-connection, every worker should have done at least one
    // message — if even one worker had zero, it'd suggest a hang or
    // a round-robin bug.
    let nonzero_workers = stats_len_nonzero(&stats);
    let summary = WsSummary::merge(stats, Duration::from_millis(500));

    assert!(
        summary.messages_recvd >= 50,
        "expected many messages across 50 connections, got {}",
        summary.messages_recvd,
    );
    assert!(
        nonzero_workers >= 40,
        "expected ≥ 40 workers with non-zero messages, got {nonzero_workers}",
    );
}

/// Connect to a port that's (almost certainly) not listening. Expect
/// `errors_connect` to fire and no messages to flow.
#[compio::test]
async fn refused_connection_counts_connect_error() {
    // Bind + immediately drop to get a port that was briefly used; on
    // Linux the next bind attempt tends to get TIME_WAIT back and
    // succeeds, which means connect() may succeed or may not on the
    // way out. Simpler: pick port 1 which is reserved and always refuses.
    let target = Target::parse("ws://127.0.0.1:1").unwrap();
    let plan = WsPlan {
        target,
        path: "/".to_string(),
        headers: Vec::new(),
        message: Bytes::from_static(b"x"),

    };

    let stop = StopSignal::after(Duration::from_millis(300));
    let stats = run_ws_saturate(plan, 1, stop, None).await;
    let summary = WsSummary::merge(stats, Duration::from_millis(300));

    assert_eq!(summary.messages_sent, 0);
    assert_eq!(summary.messages_recvd, 0);
    assert_eq!(summary.errors_connect, 1);
    assert_eq!(summary.errors_upgrade, 0);
}

/// The handshake histogram records at least one sample on a successful
/// open.
#[compio::test]
async fn handshake_is_recorded() {
    let addr = spawn_echo_server();
    let plan = ws_plan_for(addr, "ping");
    let stop = StopSignal::after(Duration::from_millis(200));
    let stats = run_ws_saturate(plan, 1, stop, None).await;
    let sum = WsSummary::merge(stats, Duration::from_millis(200));
    assert_eq!(sum.handshake.len(), 1);
}

/// Close handshake exits cleanly: when `stop` fires each worker sends
/// a Close(1000) and the server echoes it. No IO errors should be
/// recorded.
#[compio::test]
async fn close_handshake_clean_exit() {
    let addr = spawn_echo_server();
    let plan = ws_plan_for(addr, "x");
    let start = Instant::now();
    let stop = StopSignal::after(Duration::from_millis(120));
    let stats = run_ws_saturate(plan, 2, stop, None).await;
    let sum = WsSummary::merge(stats, start.elapsed());

    assert!(sum.messages_sent > 0);
    assert_eq!(sum.errors_io, 0);
    // `errors_close` may be 0 in the usual case. If the server raced
    // us on closing first we'd still be fine — `close()` is a no-op
    // after a server Close was already seen. So we don't assert a
    // specific errors_close value.
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Count workers that received at least one message.
fn stats_len_nonzero(stats: &[zerobench_ws::WsStats]) -> usize {
    stats.iter().filter(|s| s.messages_recvd > 0).count()
}

