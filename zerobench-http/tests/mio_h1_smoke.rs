#![cfg(feature = "mio-h1")]
//! Smoke test for the mio-based raw HTTP/1.1 transport.
//!
//! Spins up a minimal `std::net::TcpListener` server (no async, no hyper)
//! that replies with a fixed 200 OK + body, then runs `run_mio_worker`
//! for a short burst and asserts that requests were recorded.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zerobench_core::plan::{Plan, RateProfile, RequestPlan, Scenario, Step};
// TaskStats used by type inference only
use zerobench_core::template::Template;
use zerobench_core::transport::Target;
use zerobench_core::var::VarRegistry;

// ---------------------------------------------------------------------------
// Minimal HTTP/1.1 echo server (std::net, blocking)
// ---------------------------------------------------------------------------

/// Spawn a blocking TCP server that reads one HTTP request and replies
/// with a fixed 200 OK response per connection. Loops until `stop` is set.
/// Returns the listen address.
fn spawn_server(stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(false).unwrap();
    // Short accept timeout so we can check the stop flag.
    listener
        .set_nonblocking(false)
        .unwrap();

    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\npong";

    std::thread::spawn(move || {
        // Set a short timeout on accept so the loop can check `stop`.
        // On Linux, use SO_RCVTIMEO via the socket itself.
        listener
            .set_nonblocking(true)
            .unwrap();

        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let stop = stop.clone();
                    let response = response.to_vec();
                    std::thread::spawn(move || {
                        let mut buf = [0u8; 4096];
                        stream.set_nodelay(true).ok();
                        // Keep-alive loop.
                        while !stop.load(Ordering::Relaxed) {
                            match stream.read(&mut buf) {
                                Ok(0) => break,
                                Ok(_) => {
                                    // Check if we have a complete request (\r\n\r\n).
                                    // For simplicity, reply immediately after any read.
                                    if stream.write_all(&response).is_err() {
                                        break;
                                    }
                                }
                                Err(ref e)
                                    if e.kind() == std::io::ErrorKind::WouldBlock
                                        || e.kind() == std::io::ErrorKind::TimedOut =>
                                {
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                                Err(_) => break,
                            }
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });

    // Give the server a moment to start accepting.
    std::thread::sleep(Duration::from_millis(50));
    addr
}

// ---------------------------------------------------------------------------
// Helper: build a simple Plan
// ---------------------------------------------------------------------------

fn simple_plan(addr: SocketAddr) -> (Plan, Target) {
    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("http://{addr}/bench"), &mut vars).unwrap();
    let req = RequestPlan::get(url);
    let scenario = Scenario {
        name: "mio-smoke".into(),
        rate: RateProfile::Saturate {
            max_concurrency: 10,
        },
        steps: vec![Step::Request(req)],
    };
    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: Duration::from_secs(2),
        warmup: None,
        threads: 1,
    };
    let target = Target::parse(&format!("http://{addr}")).unwrap();
    (plan, target)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn mio_worker_records_requests() {
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = stop.clone();
    let addr = spawn_server(server_stop);

    let (plan, target) = simple_plan(addr);
    let request_bytes = {
        use zerobench_core::rng;
        use zerobench_core::scenario_context::ScenarioContext;

        let step = plan.scenarios[0].steps.first().unwrap();
        let rp = match step {
            Step::Request(r) => r,
            _ => panic!("expected request step"),
        };
        let mut ctx = ScenarioContext::new(plan.vars.len(), rng::from_entropy());
        let mut buf = Vec::new();
        zerobench_http::mio_h1::__test_build_request(rp, &mut ctx, &target, &mut buf);
        buf
    };

    // Run for a short burst.
    let worker_stop = Arc::new(AtomicBool::new(false));
    let ws = worker_stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        ws.store(true, Ordering::Relaxed);
    });

    let stats = zerobench_http::mio_h1::run_mio_worker(
        &target,
        &request_bytes,
        4, // 4 connections
        &worker_stop,
        plan.scenarios.len(),
    );

    // Stop the server.
    stop.store(true, Ordering::Relaxed);

    assert!(
        stats.requests > 0,
        "expected at least some requests, got {}",
        stats.requests
    );
    assert!(
        stats.bytes_sent > 0,
        "expected bytes_sent > 0, got {}",
        stats.bytes_sent
    );
    assert!(
        stats.bytes_recv > 0,
        "expected bytes_recv > 0, got {}",
        stats.bytes_recv
    );
}

#[test]
fn mio_threaded_records_requests() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_server(stop.clone());

    let (mut plan, target) = simple_plan(addr);
    plan.duration = Duration::from_secs(1);

    let all_stats = zerobench_http::mio_h1::run_mio_threaded(
        &target,
        &plan,
        2,  // 2 threads
        8,  // 8 total connections
        plan.duration,
    );

    stop.store(true, Ordering::Relaxed);

    assert_eq!(all_stats.len(), 2, "expected 2 thread stats");
    let total_requests: u64 = all_stats.iter().map(|s| s.requests).sum();
    assert!(
        total_requests > 0,
        "expected at least some requests across threads, got {total_requests}",
    );
}

#[test]
fn mio_rejects_tls_target() {
    let result = std::panic::catch_unwind(|| {
        let target = Target::parse("https://127.0.0.1:443").unwrap();
        let mut vars = VarRegistry::new();
        let url = Template::compile("https://127.0.0.1:443/bench", &mut vars).unwrap();
        let req = RequestPlan::get(url);
        let scenario = Scenario {
            name: "tls".into(),
            rate: RateProfile::Saturate {
                max_concurrency: 1,
            },
            steps: vec![Step::Request(req)],
        };
        let plan = Plan {
            scenarios: vec![scenario],
            vars,
            duration: Duration::from_secs(1),
            warmup: None,
            threads: 1,
        };
        zerobench_http::mio_h1::run_mio_threaded(&target, &plan, 1, 1, Duration::from_secs(1));
    });
    assert!(result.is_err(), "expected panic for TLS target");
}
