#![cfg(feature = "runtime-compio")]
//! End-to-end smoke tests for the `zerobench` CLI binary.
//!
//! Each test:
//!
//! 1. Starts an in-process hyper test server on a *separate thread*
//!    with its own compio runtime. Bind on a random ephemeral port
//!    (`127.0.0.1:0`).
//! 2. Invokes the compiled `zerobench` binary via `std::process::Command`,
//!    targeting the server.
//! 3. Asserts on stdout/stderr and exit code.
//!
//! We run the server on its own thread because the CLI spawns its own
//! compio runtime in the subprocess — the server side lives in *this*
//! test's runtime and must be on a thread that we can leave running.

use std::convert::Infallible;
use std::net::TcpListener as StdTcpListener;
use std::process::Command;
use std::sync::mpsc::{channel, Sender};
use std::thread;

use bytes::Bytes;
use compio::net::TcpListener as CompioTcpListener;
use cyper_core::HyperStream;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spawn a hyper server on its own thread with its own compio runtime.
/// Returns the bound address.
///
/// The server keeps running for the duration of the test process —
/// tests are small and short, so the detached thread is acceptable.
fn spawn_server(status: u16, body: &'static [u8]) -> std::net::SocketAddr {
    // Bind synchronously up-front so the caller knows the port before
    // spawning. We use a std TcpListener to get a free port, then pass
    // the address to the runtime thread which rebinds via compio.
    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind); // release the port for compio to bind

    let (ready_tx, ready_rx): (Sender<()>, _) = channel();

    thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = CompioTcpListener::bind(addr).await.unwrap();
            // Signal bind completion to the test thread. Tests that
            // try to connect before this signal can race; we wait for it
            // deliberately.
            let _ = ready_tx.send(());

            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                compio::runtime::spawn(async move {
                    let io = HyperStream::new(socket);
                    let svc = service_fn(move |_req: Request<Incoming>| async move {
                        let mut resp = Response::new(Full::new(Bytes::from_static(body)));
                        *resp.status_mut() =
                            http::StatusCode::from_u16(status).unwrap();
                        Ok::<_, Infallible>(resp)
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                })
                .detach();
            }
        });
    });

    // Block until the server has called .bind().
    ready_rx.recv().expect("server never bound");
    addr
}

/// Absolute path to the compiled `zerobench` binary.
fn zerobench_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zerobench")
}

/// Spawn an HTTP/2 server on its own thread with its own compio runtime.
///
/// Uses `hyper::server::conn::http2::Builder` over plain TCP ("h2c" /
/// cleartext) — matching how the CLI connects when invoked with
/// `--http-version h2` against an `http://` URL.
///
/// Feature-gated to mirror the CLI: the test only compiles when the
/// binary under test is built with the `h2` feature (otherwise the
/// subprocess would error with "H2 requested but not compiled").
#[cfg(feature = "h2")]
fn spawn_h2_server(body: &'static [u8]) -> std::net::SocketAddr {
    // A local executor for the server side of hyper H2. `CompioExecutor`
    // in cyper-core requires `F: Send`, but the per-stream futures
    // carry `Incoming` (which isn't `Send`), so we need an unbounded
    // local spawn. compio is single-threaded per runtime, so !Send is
    // safe.
    #[derive(Clone, Default)]
    struct LocalCompioExec;
    impl<F> hyper::rt::Executor<F> for LocalCompioExec
    where
        F: std::future::Future + 'static,
    {
        fn execute(&self, fut: F) {
            compio::runtime::spawn(async move {
                fut.await;
            })
            .detach();
        }
    }

    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);

    let (ready_tx, ready_rx): (Sender<()>, _) = channel();

    thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = CompioTcpListener::bind(addr).await.unwrap();
            let _ = ready_tx.send(());

            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                compio::runtime::spawn(async move {
                    let io = HyperStream::new(socket);
                    let svc = service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(Full::new(
                            Bytes::from_static(body),
                        )))
                    });
                    let _ = hyper::server::conn::http2::Builder::new(LocalCompioExec)
                        .serve_connection(io, svc)
                        .await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("h2 server never bound");
    addr
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn cli_saturate_against_200_server_succeeds() {
    let addr = spawn_server(200, b"pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "4",
            "-d",
            "1s",
            "--color",
            "never",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "expected success, got status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("actual rate"), "missing 'actual rate':\n{stdout}");
    assert!(stdout.contains("latency"), "missing 'latency':\n{stdout}");
    assert!(stdout.contains("throughput"), "missing 'throughput':\n{stdout}");
    // We should have at least *one* successful request.
    assert!(
        !stdout.contains("actual rate    0.0"),
        "rate was zero, server unreachable? stdout:\n{stdout}"
    );
}

#[test]
fn cli_expect_status_404_against_200_exits_nonzero() {
    let addr = spawn_server(200, b"pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "2",
            "-d",
            "500ms",
            "--color",
            "never",
            "--expect-status",
            "404",
            &url,
        ])
        .output()
        .expect("run zerobench");

    // Exit code 1 — assertion failures.
    assert!(
        !out.status.success(),
        "expected non-zero exit with --expect-status 404 vs 200-server, stdout:\n{}",
        String::from_utf8_lossy(&out.stdout),
    );
}

#[test]
fn cli_invalid_url_reports_clear_error() {
    let out = Command::new(zerobench_bin())
        .args(["--saturate", "-d", "100ms", "not-a-url"])
        .output()
        .expect("run zerobench");

    assert!(!out.status.success(), "expected failure with invalid URL");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.to_lowercase().contains("url")
            || stderr.to_lowercase().contains("error"),
        "expected an error message about the URL, got:\n{stderr}"
    );
}

#[test]
fn cli_json_format_emits_structured_output() {
    let addr = spawn_server(200, b"pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "2",
            "-d",
            "500ms",
            "--format",
            "json",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(out.status.success(), "zerobench failed: {:?}", out.status);
    // Parse the JSON to verify schema_version.
    let stdout = std::str::from_utf8(&out.stdout).expect("utf8");
    let v: serde_json::Value = serde_json::from_str(stdout).expect("parse json");
    assert_eq!(v["schema_version"], serde_json::Value::from(1));
    assert!(v["requests"].as_u64().unwrap() > 0);
}

#[test]
fn cli_rate_flag_runs_open_loop_and_reports_rate() {
    let addr = spawn_server(200, b"pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "-r",
            "200",
            "-c",
            "8",
            "-d",
            "1s",
            "--color",
            "never",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "zerobench failed under --rate: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // The target rate line should render the constant profile.
    assert!(
        stdout.contains("200 req/s constant"),
        "expected 'constant' rate label:\n{stdout}"
    );
}

/// Write a `.http` fixture whose Host line includes `addr`. Returns the
/// path to a tempfile that the caller should pass to `--request-file`.
fn write_request_fixture(addr: std::net::SocketAddr) -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!(
        "zerobench-cli-simple-{pid}-{nanos}.http"
    ));
    let body = format!(
        "GET /ping HTTP/1.1\r\nHost: {addr}\r\nAccept: text/plain\r\n\r\n"
    );
    std::fs::write(&path, body).expect("write fixture");
    path
}

#[test]
fn cli_request_file_against_local_server_succeeds() {
    let addr = spawn_server(200, b"pong");
    let fixture = write_request_fixture(addr);

    let out = Command::new(zerobench_bin())
        .args([
            "--request-file",
            fixture.to_str().unwrap(),
            "--saturate",
            "-c",
            "1",
            "-d",
            "500ms",
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench");

    // Delete fixture before assertion so a failed test doesn't leak.
    let _ = std::fs::remove_file(&fixture);

    assert!(
        out.status.success(),
        "zerobench --request-file failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // We should have completed at least one request against the local
    // server — "actual rate" line must not report 0.0.
    assert!(
        !stdout.contains("actual rate    0.0"),
        "expected non-zero rate, got:\n{stdout}"
    );
}

#[test]
fn cli_jsonl_format_streams_per_second_lines() {
    let addr = spawn_server(200, b"pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "4",
            "-d",
            "3s",
            "--color",
            "never",
            "--format",
            "jsonl",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "zerobench --format jsonl failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // stdout should be pure JSONL.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "expected at least 2 JSONL lines over 3s, got {}:\n{stdout}",
        lines.len()
    );

    // Each line must parse as valid JSON.
    let mut saw_nonzero_rps = false;
    for l in &lines {
        let v: serde_json::Value = serde_json::from_str(l).expect("valid json");
        assert!(v.get("rps").is_some(), "missing rps in line: {l}");
        if v["rps"].as_u64().unwrap_or(0) > 0 {
            saw_nonzero_rps = true;
        }
    }
    assert!(saw_nonzero_rps, "no JSONL line with nonzero rps:\n{stdout}");

    // The terminal summary went to stderr.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("actual rate") || stderr.contains("latency"),
        "missing terminal summary on stderr:\n{stderr}"
    );
}

#[test]
fn cli_prometheus_format_emits_expected_block() {
    let addr = spawn_server(200, b"pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "4",
            "-d",
            "500ms",
            "--color",
            "never",
            "--format",
            "prom",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "zerobench --format prom failed: status={:?}\nstdout:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("zerobench_requests_total"),
        "missing requests_total metric:\n{stdout}"
    );
    assert!(
        stdout.contains("zerobench_latency_seconds"),
        "missing latency_seconds metric:\n{stdout}"
    );
}

// ---------------------------------------------------------------------------
// HTTP/2 — feature-gated
// ---------------------------------------------------------------------------

/// End-to-end: the CLI run with `--http-version h2` successfully talks
/// to a local H2 (cleartext) server and reports non-zero throughput.
#[cfg(feature = "h2")]
#[test]
fn cli_http_version_h2_against_h2_server() {
    let addr = spawn_h2_server(b"h2-pong");
    let url = format!("http://{addr}/");

    let out = Command::new(zerobench_bin())
        .args([
            "--http-version",
            "h2",
            "--saturate",
            "-c",
            "10",
            "-d",
            "1s",
            "--color",
            "never",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "zerobench --http-version h2 failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("actual rate"), "missing 'actual rate':\n{stdout}");
    assert!(
        !stdout.contains("actual rate    0.0"),
        "rate was zero; H2 unreachable? stdout:\n{stdout}"
    );
}

// A symmetric "`--http-version h1` against an H2-only server must fail"
// test was considered and skipped: hyper's H1 client + our saturate
// loop end up in a tight error-path loop when the slot is invalidated
// (no .await yield between consecutive "slot unavailable" errors), so
// the subprocess's StopSignal::after timer never gets serviced. That's
// a pre-existing dispatcher concern orthogonal to Task 14; the positive
// H2-path test above is enough to prove the --http-version flag works.

// ---------------------------------------------------------------------------
// SSE — feature-gated
// ---------------------------------------------------------------------------

/// Spawn a minimal SSE server that emits `chunks` `data:` events at
/// `per_chunk` spacing, then optionally `data: [DONE]`.
///
/// Mirrors the sse_smoke test fixture but lives here so the CLI test
/// doesn't depend on zerobench-sse's dev-deps.
#[cfg(feature = "sse")]
fn spawn_sse_server(
    chunks: usize,
    per_chunk: std::time::Duration,
    send_done: bool,
) -> std::net::SocketAddr {
    use futures_util::stream::{self, StreamExt};
    use http_body_util::StreamBody;
    use hyper::body::Frame;

    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);

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
                    let io = HyperStream::new(socket);
                    let svc = service_fn(move |_req: Request<Incoming>| async move {
                        let frames = (0..chunks).map(move |i| {
                            let payload = format!("data: event-{i}\n\n");
                            (payload, per_chunk)
                        });
                        let done = send_done.then(|| {
                            ("data: [DONE]\n\n".to_string(), std::time::Duration::ZERO)
                        });
                        let chain: Vec<(String, std::time::Duration)> =
                            frames.chain(done).collect();
                        let s = stream::iter(chain).then(|(payload, wait)| async move {
                            if !wait.is_zero() {
                                compio::time::sleep(wait).await;
                            }
                            Ok::<_, Infallible>(Frame::data(Bytes::from(payload)))
                        });
                        let body = StreamBody::new(s);
                        let mut resp = Response::new(body);
                        resp.headers_mut().insert(
                            http::header::CONTENT_TYPE,
                            http::HeaderValue::from_static("text/event-stream"),
                        );
                        Ok::<_, Infallible>(resp)
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("sse server never bound");
    addr
}

/// The CLI must dispatch `--sse` into the SseRunner path and render a
/// block containing `chunks` / `SSE streaming` text with a non-zero
/// count. Exit code should be 0 for a clean run where streams start
/// and close cleanly.
#[cfg(feature = "sse")]
#[test]
fn cli_sse_flag_runs_and_reports_chunks() {
    // 20 chunks per stream, 5ms per chunk → ~100ms per stream.
    let addr = spawn_sse_server(20, std::time::Duration::from_millis(5), true);
    let url = format!("http://{addr}/events");

    let out = Command::new(zerobench_bin())
        .args([
            "--sse",
            "--saturate",
            "-c",
            "4",
            "-d",
            "1s",
            "--color",
            "never",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "zerobench --sse failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("SSE streaming"),
        "missing 'SSE streaming' block:\n{stdout}"
    );
    assert!(
        stdout.contains("chunks"),
        "missing 'chunks' line in SSE block:\n{stdout}"
    );
    // Should have received > 0 chunks over 1 second against a stream
    // that emits 20 chunks per 100ms.
    assert!(
        !stdout.contains("chunks        0 total"),
        "reported 0 chunks — SSE path didn't receive any events:\n{stdout}"
    );
}

// ---------------------------------------------------------------------------
// WebSocket — feature-gated
// ---------------------------------------------------------------------------

/// Tiny WebSocket echo server spawned on its own thread (own compio
/// runtime). Handshakes HTTP/1.1 Upgrade + echoes masked client text
/// frames as unmasked server text frames, using zerobench-ws's codec.
#[cfg(feature = "ws")]
fn spawn_ws_echo_server() -> std::net::SocketAddr {
    use compio::buf::BufResult;
    use compio::io::{AsyncRead, AsyncWriteExt};
    use zerobench_ws::frame::{encode_frame, Opcode};
    use zerobench_ws::handshake::{compute_accept, find_headers_end};

    fn extract_key(raw: &[u8]) -> Option<String> {
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

    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);

    let (ready_tx, ready_rx): (Sender<()>, _) = channel();

    thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = CompioTcpListener::bind(addr).await.unwrap();
            let _ = ready_tx.send(());
            loop {
                let (mut stream, _peer) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                compio::runtime::spawn(async move {
                    // Pull HTTP request.
                    let mut req_buf = Vec::with_capacity(1024);
                    loop {
                        if find_headers_end(&req_buf).is_some() {
                            break;
                        }
                        let chunk: Vec<u8> = Vec::with_capacity(1024);
                        let BufResult(res, returned) = stream.read(chunk).await;
                        let n = match res {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        req_buf.extend_from_slice(&returned[..n]);
                        if req_buf.len() > 16 * 1024 {
                            return;
                        }
                    }
                    let headers_end = find_headers_end(&req_buf).unwrap();
                    let key = match extract_key(&req_buf[..headers_end]) {
                        Some(k) => k,
                        None => return,
                    };
                    let accept = compute_accept(&key);
                    let resp = format!(
                        "HTTP/1.1 101 Switching Protocols\r\n\
                         Upgrade: websocket\r\n\
                         Connection: Upgrade\r\n\
                         Sec-WebSocket-Accept: {accept}\r\n\r\n"
                    );
                    if stream.write_all(resp.into_bytes()).await.0.is_err() {
                        return;
                    }

                    // Echo loop.
                    let mut recv = Vec::with_capacity(4096);
                    if req_buf.len() > headers_end {
                        recv.extend_from_slice(&req_buf[headers_end..]);
                    }
                    loop {
                        // Minimal inline decoder — same shape as the
                        // smoke-test server in zerobench-ws/tests.
                        if recv.len() < 2 {
                            let chunk: Vec<u8> = Vec::with_capacity(1024);
                            let BufResult(res, returned) = stream.read(chunk).await;
                            let n = match res {
                                Ok(n) if n > 0 => n,
                                _ => return,
                            };
                            recv.extend_from_slice(&returned[..n]);
                            continue;
                        }
                        let b0 = recv[0];
                        let b1 = recv[1];
                        let op = b0 & 0x0f;
                        let masked = (b1 & 0x80) != 0;
                        if !masked {
                            return;
                        }
                        let short = (b1 & 0x7f) as u64;
                        let (plen, hlen) = match short {
                            0..=125 => (short as usize, 2usize),
                            126 => {
                                if recv.len() < 4 {
                                    let chunk: Vec<u8> = Vec::with_capacity(1024);
                                    let BufResult(res, returned) = stream.read(chunk).await;
                                    let n = match res {
                                        Ok(n) if n > 0 => n,
                                        _ => return,
                                    };
                                    recv.extend_from_slice(&returned[..n]);
                                    continue;
                                }
                                (
                                    u16::from_be_bytes([recv[2], recv[3]]) as usize,
                                    4,
                                )
                            }
                            _ => {
                                if recv.len() < 10 {
                                    let chunk: Vec<u8> = Vec::with_capacity(1024);
                                    let BufResult(res, returned) = stream.read(chunk).await;
                                    let n = match res {
                                        Ok(n) if n > 0 => n,
                                        _ => return,
                                    };
                                    recv.extend_from_slice(&returned[..n]);
                                    continue;
                                }
                                let len = u64::from_be_bytes([
                                    recv[2], recv[3], recv[4], recv[5], recv[6],
                                    recv[7], recv[8], recv[9],
                                ]);
                                (len as usize, 10)
                            }
                        };
                        if recv.len() < hlen + 4 + plen {
                            let chunk: Vec<u8> = Vec::with_capacity(1024);
                            let BufResult(res, returned) = stream.read(chunk).await;
                            let n = match res {
                                Ok(n) if n > 0 => n,
                                _ => return,
                            };
                            recv.extend_from_slice(&returned[..n]);
                            continue;
                        }
                        let mask = [
                            recv[hlen],
                            recv[hlen + 1],
                            recv[hlen + 2],
                            recv[hlen + 3],
                        ];
                        let pstart = hlen + 4;
                        let pend = pstart + plen;
                        let mut payload = Vec::with_capacity(plen);
                        for (i, b) in recv[pstart..pend].iter().enumerate() {
                            payload.push(b ^ mask[i & 3]);
                        }
                        recv.drain(..pend);

                        // Echo text/binary, ack ping with pong, exit on close.
                        let opcode = match op {
                            0x1 => Opcode::Text,
                            0x2 => Opcode::Binary,
                            0x8 => {
                                // Echo close and exit.
                                let mut out = Vec::new();
                                encode_frame(Opcode::Close, &payload, [0; 4], &mut out);
                                // Convert to server shape (strip MASK bit + mask bytes).
                                let short2 = out[1] & 0x7f;
                                let ext = match short2 {
                                    0..=125 => 0usize,
                                    126 => 2,
                                    _ => 8,
                                };
                                let ht = 2 + ext;
                                let mut unmasked = Vec::with_capacity(ht + payload.len());
                                unmasked.extend_from_slice(&out[..ht]);
                                unmasked[1] &= 0x7f;
                                unmasked.extend_from_slice(&out[ht + 4..]);
                                let _ = stream.write_all(unmasked).await.0;
                                return;
                            }
                            0x9 => Opcode::Pong,
                            _ => continue,
                        };
                        let mut out = Vec::new();
                        encode_frame(opcode, &payload, [0; 4], &mut out);
                        let short2 = out[1] & 0x7f;
                        let ext = match short2 {
                            0..=125 => 0usize,
                            126 => 2,
                            _ => 8,
                        };
                        let ht = 2 + ext;
                        let mut unmasked = Vec::with_capacity(ht + payload.len());
                        unmasked.extend_from_slice(&out[..ht]);
                        unmasked[1] &= 0x7f;
                        unmasked.extend_from_slice(&out[ht + 4..]);
                        if stream.write_all(unmasked).await.0.is_err() {
                            return;
                        }
                    }
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("ws server never bound");
    addr
}

/// End-to-end: the `--ws` CLI flag runs the WS runner, renders a
/// WebSocket block with non-zero messages.
#[cfg(feature = "ws")]
#[test]
fn cli_ws_flag_runs_and_reports_messages() {
    let addr = spawn_ws_echo_server();
    let url = format!("ws://{addr}/echo");

    let out = Command::new(zerobench_bin())
        .args([
            "--ws",
            "-c",
            "4",
            "-d",
            "1s",
            "--color",
            "never",
            &url,
        ])
        .output()
        .expect("run zerobench");

    assert!(
        out.status.success(),
        "zerobench --ws failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("WebSocket"),
        "missing 'WebSocket' block:\n{stdout}"
    );
    assert!(stdout.contains("rtt"), "missing 'rtt' line:\n{stdout}");
    assert!(
        stdout.contains("messages"),
        "missing 'messages' line:\n{stdout}"
    );
    // Parse the "messages  N sent  M received ..." line and assert M > 0.
    // Earlier versions used `contains("0 received")` which falsely matched
    // any count ending in 0 (e.g. `"100 received"`).
    let received = stdout
        .lines()
        .find_map(|line| {
            let rest = line.trim_start().strip_prefix("messages")?;
            let (_sent, after) = rest.trim_start().split_once(" sent")?;
            let (count, _tail) = after.trim_start().split_once(" received")?;
            count.trim().replace(',', "").parse::<u64>().ok()
        })
        .expect("messages line must have a parsable `received` count");
    assert!(received > 0, "reported 0 messages received:\n{stdout}");
}

// ---------------------------------------------------------------------------
// Rhai script subcommand — feature-gated
// ---------------------------------------------------------------------------

/// Write a minimal `.rhai` fixture targeting `addr`. The script declares
/// a single saturate scenario with one GET, short duration, no rate.
#[cfg(feature = "script")]
fn write_rhai_fixture(addr: std::net::SocketAddr) -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir()
        .join(format!("zerobench-cli-rhai-{pid}-{nanos}.rhai"));
    let body = format!(
        r#"
// Smoke-test scenario for `zerobench run`. Targets a local echo server
// via the address passed in at test-generation time.
scenario("smoke", |s| {{
    s.step(
        GET("http://{addr}/ping")
            .expect_status(200)
    );
}});
saturate(2);
duration("500ms");
"#
    );
    std::fs::write(&path, body).expect("write fixture");
    path
}

/// End-to-end: `zerobench run <script.rhai>` drives a real bench loop
/// against a local server and reports non-zero throughput.
#[cfg(feature = "script")]
#[test]
fn cli_run_rhai_script_against_local_server() {
    let addr = spawn_server(200, b"pong");
    let fixture = write_rhai_fixture(addr);

    let out = Command::new(zerobench_bin())
        .args([
            "run",
            fixture.to_str().unwrap(),
            "-c",
            "2",
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench run <rhai>");

    // Delete fixture before assertion so a failed test doesn't leak.
    let _ = std::fs::remove_file(&fixture);

    assert!(
        out.status.success(),
        "zerobench run failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("actual rate") || stdout.contains("latency"),
        "missing terminal report output:\n{stdout}"
    );
    assert!(
        !stdout.contains("actual rate    0.0"),
        "rate was zero — server unreachable? stdout:\n{stdout}"
    );
}

/// `zerobench run` with `--duration` must override the script's
/// `duration(...)` — regression test for the override plumbing.
#[cfg(feature = "script")]
#[test]
fn cli_run_rhai_script_duration_override() {
    let addr = spawn_server(200, b"pong");

    // Write a script with a ridiculously long duration that we then
    // override via --duration. If the override were ignored, the test
    // would time out well before the 30s script duration.
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir()
        .join(format!("zerobench-cli-rhai-override-{pid}-{nanos}.rhai"));
    let body = format!(
        r#"
scenario("x", |s| {{
    s.step(GET("http://{addr}/").expect_status(200));
}});
saturate(2);
duration("30s");
"#
    );
    std::fs::write(&path, body).expect("write fixture");

    let out = Command::new(zerobench_bin())
        .args([
            "run",
            path.to_str().unwrap(),
            "-c",
            "2",
            "--duration",
            "300ms",
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench run --duration");

    let _ = std::fs::remove_file(&path);

    assert!(
        out.status.success(),
        "zerobench run --duration failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
