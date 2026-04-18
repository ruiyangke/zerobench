//! Integration smoke tests for the mio-based SSE runner.
//!
//! Boots a minimal SSE server using raw `std::net` (no async runtime),
//! points the runner at it, and verifies chunks are received.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zerobench_core::plan::{Plan, RequestPlan, Scenario, Step};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_sse::SseSummary;

// ---------------------------------------------------------------------------
// Raw std::net SSE test server
// ---------------------------------------------------------------------------

/// Spawn a blocking SSE server that emits `chunks` events then closes.
fn spawn_sse_server(
    chunks: usize,
    interval: Duration,
    send_done: bool,
    stop: Arc<AtomicBool>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            listener.set_nonblocking(false).ok();
            let _ = listener.set_ttl(1);
            // Accept with a timeout so the server can check stop.
            listener
                .set_nonblocking(true)
                .ok();
            let stream = match listener.accept() {
                Ok((s, _)) => s,
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
            };
            let stop = stop.clone();

            std::thread::spawn(move || {
                let mut stream = stream;
                // Read the request (we don't care about contents).
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);

                // Write SSE response headers.
                let headers = "HTTP/1.1 200 OK\r\n\
                               Content-Type: text/event-stream\r\n\
                               Cache-Control: no-cache\r\n\
                               Transfer-Encoding: chunked\r\n\
                               \r\n";
                let _ = stream.write_all(headers.as_bytes());

                // Send chunks.
                for i in 0..chunks {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let payload = format!("data: event-{i}\n\n");
                    let chunk = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                    if stream.write_all(chunk.as_bytes()).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                    if !interval.is_zero() {
                        std::thread::sleep(interval);
                    }
                }

                if send_done {
                    let done = "data: [DONE]\n\n";
                    let chunk = format!("{:x}\r\n{}\r\n", done.len(), done);
                    let _ = stream.write_all(chunk.as_bytes());
                }

                // Terminal chunk.
                let _ = stream.write_all(b"0\r\n\r\n");
                let _ = stream.flush();
            });
        }
    });

    // Give the server thread a moment to bind.
    std::thread::sleep(Duration::from_millis(50));
    addr
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_plan(addr: SocketAddr) -> (Plan, Target) {
    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("http://{addr}/events"), &mut vars).unwrap();
    let mut req = RequestPlan::get(url);
    req.expect_streaming = true;

    let plan = Plan {
        scenarios: vec![Scenario::new(String::from("sse"), vec![Step::Request(req)])],
        duration: Duration::from_secs(2),
        vars,
        ..Plan::new()
    };
    let target = Target::parse(&format!("http://{addr}")).unwrap();
    (plan, target)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn runner_reads_all_chunks() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_sse_server(100, Duration::from_millis(5), false, stop.clone());
    let (plan, target) = make_plan(addr);

    let opts = TransportOpts::default();
    let stats = zerobench_sse::run_sse_threaded(&target, &opts, &plan, 1, Duration::from_secs(3), None);
    stop.store(true, Ordering::Relaxed);
    let summary = SseSummary::merge(stats, Duration::from_secs(3));

    assert!(summary.streams >= 1, "expected at least 1 stream, got {}", summary.streams);
    assert!(summary.chunks >= 100, "expected >= 100 chunks, got {}", summary.chunks);
    assert!(summary.bytes_received > 0);
}

#[test]
fn runner_records_done_sentinel() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_sse_server(50, Duration::from_millis(1), true, stop.clone());
    let (plan, target) = make_plan(addr);

    let opts = TransportOpts::default();
    let stats = zerobench_sse::run_sse_threaded(&target, &opts, &plan, 1, Duration::from_secs(2), None);
    stop.store(true, Ordering::Relaxed);
    let summary = SseSummary::merge(stats, Duration::from_secs(2));

    assert!(summary.streams >= 1);
    assert!(summary.completed >= 1, "expected at least 1 completion, got {}", summary.completed);
}

#[test]
fn threaded_runs_connections_concurrently() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_sse_server(5, Duration::from_millis(50), false, stop.clone());
    let (plan, target) = make_plan(addr);

    let opts = TransportOpts::default();
    let stats = zerobench_sse::run_sse_threaded(&target, &opts, &plan, 10, Duration::from_secs(2), None);
    stop.store(true, Ordering::Relaxed);
    let summary = SseSummary::merge(stats, Duration::from_secs(2));

    assert!(
        summary.streams >= 10,
        "expected >= 10 streams started, got {}",
        summary.streams
    );
    assert!(summary.chunks > 0);
}
