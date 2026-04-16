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

