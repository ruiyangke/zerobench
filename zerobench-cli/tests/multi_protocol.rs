//! End-to-end CLI test: `zerobench run <script>` with a multi-protocol
//! Rhai fixture. Spawns in-process HTTP/SSE/WS stub servers, points the
//! fixture at them via env vars, invokes the CLI, and parses the
//! rendered report to confirm per-protocol rows appear.
//!
//! The fixture lives at `../zerobench-rhai/tests/multi_protocol.rhai`.

#![cfg(feature = "script")]
#![cfg(feature = "sse")]
#![cfg(feature = "ws")]

use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn zerobench_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zerobench")
}

fn fixture_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("zerobench-rhai")
        .join("tests")
        .join("multi_protocol.rhai")
}

// ---------------------------------------------------------------------------
// HTTP stub: returns 200 "ok" on any path.
// ---------------------------------------------------------------------------

fn spawn_http_stub(stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).ok();
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(5));
                continue;
            };
            stream.set_nonblocking(false).ok();
            thread::spawn(move || {
                // Drain one HTTP request per connection, then close
                // (keep-alive not required for this smoke test).
                loop {
                    let mut buf = [0u8; 2048];
                    let n = match stream.read(&mut buf) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    if !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                        continue;
                    }
                    let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
                    if stream.write_all(resp).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                    // Loop to serve the next pipelined request.
                }
            });
        }
    });
    thread::sleep(Duration::from_millis(30));
    addr
}

// ---------------------------------------------------------------------------
// SSE stub: streams 5 events then closes.
// ---------------------------------------------------------------------------

fn spawn_sse_stub(stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).ok();
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(5));
                continue;
            };
            stream.set_nonblocking(false).ok();
            thread::spawn(move || {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let headers = "HTTP/1.1 200 OK\r\n\
                               Content-Type: text/event-stream\r\n\
                               Cache-Control: no-cache\r\n\
                               Transfer-Encoding: chunked\r\n\
                               \r\n";
                if stream.write_all(headers.as_bytes()).is_err() {
                    return;
                }
                for i in 0..5 {
                    let payload = format!("data: e-{i}\n\n");
                    let chunk = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                    if stream.write_all(chunk.as_bytes()).is_err() {
                        return;
                    }
                }
                let _ = stream.write_all(b"0\r\n\r\n");
            });
        }
    });
    thread::sleep(Duration::from_millis(30));
    addr
}

// ---------------------------------------------------------------------------
// WS stub: minimal handshake + echo.
// ---------------------------------------------------------------------------

fn spawn_ws_stub(stop: Arc<AtomicBool>) -> SocketAddr {
    use sha1::{Digest, Sha1};
    const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).ok();
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(5));
                continue;
            };
            stream.set_nonblocking(false).ok();
            thread::spawn(move || {
                // Read handshake.
                let mut buf = Vec::with_capacity(1024);
                loop {
                    let mut chunk = [0u8; 1024];
                    let n = match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if buf.len() > 16 * 1024 {
                        return;
                    }
                }
                // Extract key.
                let key = {
                    let mut headers = [httparse::EMPTY_HEADER; 32];
                    let mut req = httparse::Request::new(&mut headers);
                    let _ = req.parse(&buf);
                    let mut found = None;
                    for h in req.headers.iter() {
                        if h.name.eq_ignore_ascii_case("sec-websocket-key") {
                            found = std::str::from_utf8(h.value)
                                .ok()
                                .map(|s| s.trim().to_string());
                            break;
                        }
                    }
                    match found {
                        Some(k) => k,
                        None => return,
                    }
                };
                let accept = {
                    let mut h = Sha1::new();
                    h.update(key.as_bytes());
                    h.update(WS_GUID.as_bytes());
                    let digest = h.finalize();
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.encode(digest)
                };
                let resp = format!(
                    "HTTP/1.1 101 Switching Protocols\r\n\
                     Upgrade: websocket\r\n\
                     Connection: Upgrade\r\n\
                     Sec-WebSocket-Accept: {accept}\r\n\r\n"
                );
                if stream.write_all(resp.as_bytes()).is_err() {
                    return;
                }

                // Echo loop — read one frame, reply with unmasked copy.
                let mut rbuf: Vec<u8> = Vec::with_capacity(4096);
                loop {
                    // Parse one masked text frame at a time.
                    loop {
                        if rbuf.len() >= 2 {
                            let op = rbuf[0] & 0x0f;
                            let short_len = (rbuf[1] & 0x7f) as usize;
                            let masked = (rbuf[1] & 0x80) != 0;
                            if !masked {
                                return;
                            }
                            let (plen, hdr_len) = match short_len {
                                0..=125 => (short_len, 2usize),
                                126 => {
                                    if rbuf.len() < 4 {
                                        break;
                                    }
                                    (
                                        u16::from_be_bytes(rbuf[2..4].try_into().unwrap())
                                            as usize,
                                        4,
                                    )
                                }
                                127 => {
                                    if rbuf.len() < 10 {
                                        break;
                                    }
                                    (
                                        u64::from_be_bytes(rbuf[2..10].try_into().unwrap())
                                            as usize,
                                        10,
                                    )
                                }
                                _ => return,
                            };
                            if rbuf.len() >= hdr_len + 4 + plen {
                                let mask = [
                                    rbuf[hdr_len],
                                    rbuf[hdr_len + 1],
                                    rbuf[hdr_len + 2],
                                    rbuf[hdr_len + 3],
                                ];
                                let payload: Vec<u8> = rbuf
                                    [hdr_len + 4..hdr_len + 4 + plen]
                                    .iter()
                                    .enumerate()
                                    .map(|(i, b)| b ^ mask[i & 3])
                                    .collect();
                                rbuf.drain(..hdr_len + 4 + plen);

                                if op == 0x8 {
                                    // Close — reply and exit.
                                    let mut out = vec![0x88, payload.len() as u8];
                                    out.extend_from_slice(&payload);
                                    let _ = stream.write_all(&out);
                                    return;
                                }
                                if op == 0x1 || op == 0x2 {
                                    // Echo as unmasked text frame.
                                    let mut out = Vec::with_capacity(2 + payload.len());
                                    let opcode_byte = 0x80 | op;
                                    out.push(opcode_byte);
                                    let plen_u8 = payload.len();
                                    if plen_u8 <= 125 {
                                        out.push(plen_u8 as u8);
                                    } else if plen_u8 <= 0xFFFF {
                                        out.push(126);
                                        out.extend_from_slice(
                                            &(plen_u8 as u16).to_be_bytes(),
                                        );
                                    } else {
                                        out.push(127);
                                        out.extend_from_slice(
                                            &(plen_u8 as u64).to_be_bytes(),
                                        );
                                    }
                                    out.extend_from_slice(&payload);
                                    if stream.write_all(&out).is_err() {
                                        return;
                                    }
                                }
                                continue;
                            }
                        }
                        break;
                    }
                    // Need more.
                    let mut chunk = [0u8; 4096];
                    let n = match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    rbuf.extend_from_slice(&chunk[..n]);
                }
            });
        }
    });
    thread::sleep(Duration::from_millis(30));
    addr
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[test]
fn cli_run_multi_protocol_rhai_produces_unified_report() {
    let stop = Arc::new(AtomicBool::new(false));
    let http_addr = spawn_http_stub(stop.clone());
    let sse_addr = spawn_sse_stub(stop.clone());
    let ws_addr = spawn_ws_stub(stop.clone());

    let out = Command::new(zerobench_bin())
        .arg("run")
        .arg(fixture_path())
        .env("TEST_HTTP_URL", format!("http://{http_addr}/ping"))
        .env("TEST_SSE_URL", format!("http://{sse_addr}/events"))
        .env("TEST_WS_URL", format!("ws://{ws_addr}/ws"))
        .env("TEST_DURATION", "300ms")
        // `--parallel` produces one unified report (old default).
        // The new default is serial-per-scenario; asserted by a
        // sibling test.
        .args(["-c", "4", "--color", "never", "--parallel"])
        .output()
        .expect("run zerobench run");

    stop.store(true, Ordering::Relaxed);

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let all = format!("{stdout}\n{stderr}");

    // Should have succeeded (exit 0) if all three backends completed
    // some operations.
    assert!(
        out.status.success(),
        "expected zerobench run to succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Per-scenario block should list all three scenarios with their
    // protocol badges.
    assert!(all.contains("http-ping"), "missing http scenario row:\n{all}");
    assert!(all.contains("sse-events"), "missing sse scenario row:\n{all}");
    assert!(all.contains("ws-echo"), "missing ws scenario row:\n{all}");
    assert!(all.contains("HTTP"), "missing HTTP badge:\n{all}");
    assert!(all.contains("SSE"), "missing SSE badge:\n{all}");
    assert!(all.contains("WS"), "missing WS badge:\n{all}");

    // Top-line label should switch to "operations" since the plan is
    // mixed.
    assert!(
        all.contains("operations"),
        "expected 'operations' label for mixed plan:\n{all}"
    );
    assert!(all.contains("ops/s"), "expected 'ops/s' label:\n{all}");
}

/// Default mode is now serial — each scenario runs in its own isolated
/// block (own pool, own duration, own report). Verify the per-scenario
/// section dividers appear and each scenario's report renders separately.
#[test]
fn cli_run_multi_protocol_rhai_serial_mode_default() {
    let stop = Arc::new(AtomicBool::new(false));
    let http_addr = spawn_http_stub(stop.clone());
    let sse_addr = spawn_sse_stub(stop.clone());
    let ws_addr = spawn_ws_stub(stop.clone());

    let out = Command::new(zerobench_bin())
        .arg("run")
        .arg(fixture_path())
        .env("TEST_HTTP_URL", format!("http://{http_addr}/ping"))
        .env("TEST_SSE_URL", format!("http://{sse_addr}/events"))
        .env("TEST_WS_URL", format!("ws://{ws_addr}/ws"))
        .env("TEST_DURATION", "200ms")
        .args(["-c", "4", "--color", "never"])
        .output()
        .expect("run zerobench run");

    stop.store(true, Ordering::Relaxed);

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let all = format!("{stdout}\n{stderr}");

    assert!(
        out.status.success(),
        "expected serial zerobench run to succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Serial output contains per-scenario section headers with
    // scenario index: "scenario 1/3", "scenario 2/3", "scenario 3/3".
    assert!(
        all.contains("scenario 1/3") && all.contains("scenario 3/3"),
        "missing serial-mode section headers:\n{all}"
    );
    // Each scenario name appears.
    assert!(all.contains("http-ping"));
    assert!(all.contains("sse-events"));
    assert!(all.contains("ws-echo"));
}
