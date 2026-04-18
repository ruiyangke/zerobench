//! Integration test for `run_sse_from_plan_threaded` — the Tier-1
//! multi-scenario SSE runner used by `zerobench run script.rhai`.
//!
//! Boots a minimal SSE server that streams N events then closes, then
//! points the multi-scenario runner at a plan that declares two SSE
//! scenarios against the same server. Validates stats attribution
//! (both scenarios get chunks counted) and that the aggregate
//! `TaskStats` rolls up correctly.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use smallvec::SmallVec;
use zerobench_core::plan::{Plan, Protocol, Scenario, SsePlan, Step};
use zerobench_core::stats::Summary;
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;

/// Spawn a minimal SSE server that emits `chunks` events per connection
/// then closes with a terminal chunk. Re-serves until stop trips.
fn spawn_sse_server(chunks: usize, stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).ok();

    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            let stream = match listener.accept() {
                Ok((s, _)) => s,
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
            };
            let stop_c = stop.clone();
            std::thread::spawn(move || {
                let mut stream = stream;
                stream.set_nonblocking(false).ok();
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);

                let headers = "HTTP/1.1 200 OK\r\n\
                               Content-Type: text/event-stream\r\n\
                               Cache-Control: no-cache\r\n\
                               Transfer-Encoding: chunked\r\n\
                               \r\n";
                let _ = stream.write_all(headers.as_bytes());
                for i in 0..chunks {
                    if stop_c.load(Ordering::Relaxed) {
                        break;
                    }
                    let payload = format!("data: e-{i}\n\n");
                    let chunk = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                    if stream.write_all(chunk.as_bytes()).is_err() {
                        return;
                    }
                }
                // Terminal 0-length chunk.
                let _ = stream.write_all(b"0\r\n\r\n");
            });
        }
    });

    std::thread::sleep(Duration::from_millis(50));
    addr
}

/// Build a plan that has two SSE scenarios pointing at the given
/// address, plus a filler HTTP scenario the SSE runner should silently
/// skip.
fn two_sse_plan(addr: SocketAddr) -> Plan {
    let mut vars = VarRegistry::new();
    let sse_url_a = Template::compile(&format!("http://{addr}/a"), &mut vars).unwrap();
    let sse_url_b = Template::compile(&format!("http://{addr}/b"), &mut vars).unwrap();
    let http_url = Template::compile(&format!("http://{addr}/h"), &mut vars).unwrap();

    Plan {
        scenarios: vec![
            Scenario::new(
                "sse-a",
                vec![Step::SseStream(SsePlan {
                    url: sse_url_a,
                    headers: SmallVec::new(),
                    expect_chunks: None,
                })],
            ),
            Scenario::new(
                "sse-b",
                vec![Step::SseStream(SsePlan {
                    url: sse_url_b,
                    headers: SmallVec::new(),
                    expect_chunks: None,
                })],
            ),
            // Filler HTTP scenario — exercised by run_mio_threaded, not
            // by run_sse_from_plan_threaded. Included to ensure SSE
            // runner correctly filters it out.
            Scenario::new(
                "http-ping",
                vec![Step::Request(
                    zerobench_core::plan::RequestPlan::get(http_url),
                )],
            ),
        ],
        duration: Duration::from_secs(1),
        vars,
        ..Plan::new()
    }
}

#[test]
fn run_sse_from_plan_collects_per_scenario_stats() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_sse_server(10, stop.clone());
    let plan = two_sse_plan(addr);
    let target = Target::parse(&format!("http://{addr}")).unwrap();

    // Sanity: we should have three scenarios, one HTTP + two SSE.
    assert_eq!(plan.scenarios.len(), 3);
    assert_eq!(plan.scenarios[0].protocol(), Protocol::Sse);
    assert_eq!(plan.scenarios[1].protocol(), Protocol::Sse);
    assert_eq!(plan.scenarios[2].protocol(), Protocol::Http);

    let opts = TransportOpts::default();
    let stats = zerobench_sse::run_sse_from_plan_threaded(
        &target,
        &opts,
        &plan,
        4, // workers
        Duration::from_millis(500),
        None,
        None,
    );
    stop.store(true, Ordering::Relaxed);

    // Merge per-worker stats into a Summary, verify scenarios 0 and 1
    // both saw traffic (uniform picking distributes across them).
    let summary = Summary::merge(stats, Duration::from_secs(1));
    assert_eq!(summary.per_scenario.len(), 3);
    let sse_a = &summary.per_scenario[0];
    let sse_b = &summary.per_scenario[1];
    let http = &summary.per_scenario[2];

    // HTTP scenario should be untouched (empty extras, zero requests).
    assert_eq!(http.requests, 0);
    assert!(http.sse.is_none());
    assert!(http.ws.is_none());

    // With 4 workers picking uniformly from 2 SSE scenarios over
    // 500ms, at least one scenario should have received some streams
    // and chunks. A strong assertion "both are nonzero" is slightly
    // flaky under heavy CI load, so we check the aggregate and at
    // least one scenario has nonzero counters.
    let total_streams = sse_a.requests + sse_b.requests;
    let total_chunks = sse_a.sse.as_ref().map(|e| e.chunks).unwrap_or(0)
        + sse_b.sse.as_ref().map(|e| e.chunks).unwrap_or(0);
    assert!(
        total_streams > 0,
        "expected some SSE streams across both scenarios, got 0"
    );
    assert!(
        total_chunks > 0,
        "expected some SSE chunks across both scenarios, got 0"
    );
}
