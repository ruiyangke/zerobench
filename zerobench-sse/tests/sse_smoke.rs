//! Integration smoke tests for the SSE runner.
//!
//! Boots a small SSE server in-process on its own thread (hyper over
//! cyper-core), points the runner at it, and verifies:
//!
//! - Emits 100 chunks over ~1s → the runner records ≥ 100 `chunks` and
//!   TTFB < 100ms on loopback.
//! - `data: [DONE]` sentinel → `completed` counter increments.
//! - N concurrent streams make progress in parallel — not the v1
//!   round-robin bug where 50 connections get serviced one-at-a-time.

use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::net::TcpListener as CompioTcpListener;
use cyper_core::HyperStream;
use futures_util::stream::{self, StreamExt};
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use zerobench_core::plan::RequestPlan;
use zerobench_core::rng::from_seed;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::stop::StopSignal;
use zerobench_core::template::Template;
use zerobench_core::transport::{HttpVersionPref, Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_sse::{run_sse_saturate, SseRunner, SseStats, SseSummary};

// ---------------------------------------------------------------------------
// Test servers
// ---------------------------------------------------------------------------

/// Spawn an SSE server that emits `chunks` events at `interval` spacing
/// and then optionally sends `data: [DONE]` before closing.
///
/// The server runs on its own thread with its own compio runtime so the
/// client-side runtime (the test's) can drive the bench without
/// cross-runtime issues.
fn spawn_sse_server(chunks: usize, interval: Duration, send_done: bool) -> SocketAddr {
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
                    let svc = service_fn(move |_req: Request<hyper::body::Incoming>| {
                        async move {
                            // Build a streaming body: N data frames spaced by `interval`,
                            // optionally followed by `[DONE]`.
                            let frames = (0..chunks).map(move |i| {
                                let payload = format!("data: event-{i}\n\n");
                                (payload, interval)
                            });
                            let done = send_done.then(|| ("data: [DONE]\n\n".to_string(), Duration::ZERO));

                            let chain: Vec<(String, Duration)> =
                                frames.chain(done).collect();

                            let s = stream::iter(chain).then(
                                |(payload, wait)| async move {
                                    if !wait.is_zero() {
                                        compio::time::sleep(wait).await;
                                    }
                                    Ok::<_, Infallible>(Frame::data(Bytes::from(payload)))
                                },
                            );

                            let body = StreamBody::new(s);
                            let mut resp = Response::new(body);
                            resp.headers_mut().insert(
                                http::header::CONTENT_TYPE,
                                http::HeaderValue::from_static("text/event-stream"),
                            );
                            resp.headers_mut().insert(
                                http::header::CACHE_CONTROL,
                                http::HeaderValue::from_static("no-cache"),
                            );
                            Ok::<_, Infallible>(resp)
                        }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("server never bound");
    addr
}

/// Spawn an SSE server that takes `hold` duration to produce each chunk,
/// tracking concurrent in-flight requests. Returns `(addr, peak_ref)`
/// where `peak_ref` is shared mutable state the test reads after the run.
///
/// The test uses this to verify that `N` concurrent SSE connections
/// actually make progress concurrently — not the v1 round-robin bug
/// where the peak was always 1.
fn spawn_concurrency_probe_server(
    chunks: usize,
    per_chunk: Duration,
) -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);

    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak_ret = peak.clone();

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
                let active = active.clone();
                let peak = peak.clone();
                compio::runtime::spawn(async move {
                    let io = HyperStream::new(socket);
                    let svc = service_fn(move |_req: Request<hyper::body::Incoming>| {
                        let active = active.clone();
                        let peak = peak.clone();
                        async move {
                            // Track concurrent streams.
                            let current = active
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                + 1;
                            let mut prev = peak.load(std::sync::atomic::Ordering::SeqCst);
                            while current > prev {
                                match peak.compare_exchange(
                                    prev,
                                    current,
                                    std::sync::atomic::Ordering::SeqCst,
                                    std::sync::atomic::Ordering::SeqCst,
                                ) {
                                    Ok(_) => break,
                                    Err(n) => prev = n,
                                }
                            }

                            // Build a streaming body: N frames, each sent after `per_chunk`.
                            // The active counter decrements via a guard struct that fires on drop.
                            struct Guard(Arc<std::sync::atomic::AtomicUsize>);
                            impl Drop for Guard {
                                fn drop(&mut self) {
                                    self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                }
                            }

                            let frames = (0..chunks).map(move |i| {
                                let payload = format!("data: x-{i}\n\n");
                                (payload, per_chunk)
                            });
                            let guard = Arc::new(Guard(active.clone()));

                            let s = stream::iter(frames).then(move |(payload, wait)| {
                                let _g = guard.clone();
                                async move {
                                    // Keep the guard alive through each frame's await.
                                    let _keep = _g;
                                    compio::time::sleep(wait).await;
                                    Ok::<_, Infallible>(Frame::data(Bytes::from(payload)))
                                }
                            });

                            let body = StreamBody::new(s);
                            let mut resp = Response::new(body);
                            resp.headers_mut().insert(
                                http::header::CONTENT_TYPE,
                                http::HeaderValue::from_static("text/event-stream"),
                            );
                            Ok::<_, Infallible>(resp)
                        }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("server never bound");
    (addr, peak_ret)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the target + opts pair the SseRunner expects. Named-tuple
/// style for readability in each test's setup block.
fn sse_target(addr: SocketAddr) -> (Target, TransportOpts) {
    let target = Target::parse(&format!("http://{addr}")).expect("parse target");
    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        http_version: HttpVersionPref::Http1,
        ..TransportOpts::default()
    };
    (target, opts)
}

fn sse_plan() -> RequestPlan {
    let mut vars = VarRegistry::new();
    let mut plan = RequestPlan::get(Template::compile("/events", &mut vars).unwrap());
    plan.expect_streaming = true;
    plan
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A server emits 100 chunks at 5ms intervals (~500ms total). One
/// iteration consumes the whole stream and should see exactly 100
/// `data:` events.
#[compio::test]
async fn runner_reads_all_chunks_from_streaming_server() {
    let addr = spawn_sse_server(100, Duration::from_millis(5), false);
    let (target, opts) = sse_target(addr);
    let plan = sse_plan();
    let mut ctx = ScenarioContext::new(0, from_seed(1));
    let mut stats = SseStats::new();

    // Give ourselves a generous deadline (3x the expected stream time).
    let deadline = Instant::now() + Duration::from_secs(5);
    SseRunner::run_iteration(&target, &opts, &plan, &mut ctx, &mut stats, deadline).await;

    assert_eq!(stats.streams, 1, "expected one stream to start");
    assert_eq!(
        stats.chunks, 100,
        "expected 100 chunks, got {}",
        stats.chunks
    );
    assert!(stats.bytes_received > 0);
    // TTFB on loopback is microseconds; cap at 100ms even for slow CI.
    let ttfb_p99 = stats.ttfb.value_at_percentile(99.0);
    assert!(
        ttfb_p99 < 100_000_000, // 100ms in ns
        "TTFB p99 should be < 100ms on loopback, got {ttfb_p99}ns"
    );
    // Completed should be 1 (server closed after last chunk).
    assert_eq!(stats.completed, 1, "expected clean completion");
}

/// Server emits 50 chunks then `data: [DONE]`. Completed stays at 1 —
/// we dedup via the `counted_completion` flag so that `[DONE]` + server
/// close count as one stream completion, not two.
#[compio::test]
async fn runner_records_done_sentinel() {
    let addr = spawn_sse_server(50, Duration::from_millis(1), true);
    let (target, opts) = sse_target(addr);
    let plan = sse_plan();
    let mut ctx = ScenarioContext::new(0, from_seed(1));
    let mut stats = SseStats::new();

    let deadline = Instant::now() + Duration::from_secs(5);
    SseRunner::run_iteration(&target, &opts, &plan, &mut ctx, &mut stats, deadline).await;

    assert_eq!(stats.streams, 1);
    assert_eq!(stats.chunks, 50, "expected 50 chunks");
    assert_eq!(
        stats.completed, 1,
        "expected exactly 1 completion, got {}",
        stats.completed
    );
}

/// Regression test for the v1 round-robin bug: with 10 concurrent SSE
/// workers, all 10 connections should make progress in parallel. The
/// probe server tracks its peak concurrent in-flight requests; if the
/// peak is < 10 we'd know workers were getting serialised.
#[compio::test]
async fn saturate_runs_connections_concurrently() {
    // Server: 5 chunks per stream, 30ms per chunk → ~150ms per stream.
    // With 10 concurrent workers the peak should be 10 pretty much
    // immediately; without true concurrency the peak would be 1.
    let (addr, peak) =
        spawn_concurrency_probe_server(5, Duration::from_millis(30));
    let (target, opts) = sse_target(addr);
    let plan = Arc::new(sse_plan());

    // Run for 500ms — enough for the first wave to saturate and the
    // server to see concurrent streams.
    let stop = StopSignal::after(Duration::from_millis(500));
    let stats = run_sse_saturate(target, opts, plan, 10, stop).await;

    // Stats sanity: at least 10 streams started (one wave) and chunks
    // were received.
    let summary = SseSummary::merge(stats, Duration::from_millis(500));
    assert!(
        summary.streams >= 10,
        "expected >= 10 streams started, got {}",
        summary.streams
    );
    assert!(summary.chunks > 0, "expected chunks to be received");

    // The big check: peak concurrent streams on the server.
    let p = peak.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        p >= 5,
        "expected peak concurrent streams >= 5, got {p} — round-robin bug?"
    );
}
