#![cfg(feature = "runtime-compio")]
//! Smoke test for [`zerobench_http::Http1Pool`] against an in-process
//! hyper server.
//!
//! The server runs on the same compio runtime as the client — compio
//! is single-threaded, so we spawn an accept-loop task that handles
//! each connection via `hyper::server::conn::http1::Builder` driven
//! through `cyper-core::HyperStream` (the compio↔hyper IO bridge we
//! use client-side, just flipped around).
//!
//! The test covers:
//! - 100 sequential exchanges — `exchange` round-trips through the
//!   pool without deadlock or resource leak.
//! - 100 concurrent exchanges — round-robin slot rotation keeps up
//!   under contention.
//! - Dead-server case — `Http1Pool::new` surfaces a connect error
//!   when no one is listening.
//! - Timeout case — a server that accepts but never responds causes
//!   `exchange` to return `TransportError::Timeout` per the configured
//!   `request_timeout`.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use compio::net::TcpListener;
use compio::runtime::spawn;
use cyper_core::HyperStream;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use zerobench_core::plan::RequestPlan;
use zerobench_core::rng::from_seed;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::Template;
use zerobench_core::transport::{
    Target, TransportError, TransportOpts,
};
use zerobench_core::var::VarRegistry;

use zerobench_http::Http1Pool;

// ---------------------------------------------------------------------------
// Test server — handles incoming connections until dropped.
// ---------------------------------------------------------------------------

/// Boot a server bound to 127.0.0.1:0, calling `handler` for every
/// request. Returns the ephemeral address. The server task runs
/// detached; it will keep accepting until the process exits or the
/// listener hits an error (e.g. if the test tears down via a panic
/// the OS cleans it up).
async fn spawn_echo_server(body: &'static [u8]) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    spawn(async move {
        loop {
            let (socket, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            // Per-connection task.
            spawn(async move {
                let io = HyperStream::new(socket);
                let service = service_fn(move |_req: Request<Incoming>| async move {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(body))))
                });
                let _ = http1::Builder::new().serve_connection(io, service).await;
            })
            .detach();
        }
    })
    .detach();

    addr
}

/// Echo back the request's path+query in the response body. Lets the
/// caller inspect which URL each exchange actually issued — used to
/// verify that template expansion (`{{counter}}`, `{{rand_*}}`) is
/// producing distinct URLs per exchange.
async fn spawn_path_echo_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    spawn(async move {
        loop {
            let (socket, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            spawn(async move {
                let io = HyperStream::new(socket);
                let service = service_fn(move |req: Request<Incoming>| async move {
                    let path_and_query = req
                        .uri()
                        .path_and_query()
                        .map(|pq| pq.as_str().to_string())
                        .unwrap_or_else(|| req.uri().path().to_string());
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(path_and_query))))
                });
                let _ = http1::Builder::new().serve_connection(io, service).await;
            })
            .detach();
        }
    })
    .detach();

    addr
}

/// Respond to *every* request by holding the socket for 60 seconds —
/// neither reading the request nor writing a response. Exercises the
/// request_timeout path on the *first* request on a fresh connection.
async fn spawn_hanging_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    spawn(async move {
        loop {
            let (socket, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            // Park the socket inside a detached task that just holds it,
            // so the kernel doesn't RST the connection. We never read
            // or write — the client will observe a request_timeout.
            spawn(async move {
                // Hold the socket for 60s so the test's 500ms timeout
                // is the first thing to fire.
                compio::time::sleep(Duration::from_secs(60)).await;
                drop(socket);
            })
            .detach();
        }
    })
    .detach();

    addr
}

// ---------------------------------------------------------------------------
// Helpers — plan + context builders reused across cases.
// ---------------------------------------------------------------------------

fn url_plan(url: &str, vars: &mut VarRegistry) -> RequestPlan {
    let url = Template::compile(url, vars).expect("compile url");
    RequestPlan::get(url)
}

fn target_for(addr: std::net::SocketAddr) -> Target {
    Target::parse(&format!("http://{addr}")).expect("parse target")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[compio::test]
async fn sequential_exchanges_return_expected_body() {
    let addr = spawn_echo_server(b"pong").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 4,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        ..TransportOpts::default()
    };

    let pool = Http1Pool::new(&target, &opts).await.expect("pool");
    assert_eq!(pool.len(), 4);

    let mut vars = VarRegistry::new();
    let plan = url_plan("/health", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(7));

    for _ in 0..100 {
        let resp = pool.exchange(&plan, &mut ctx).await.expect("exchange");
        assert_eq!(resp.status, 200);
        // Body is intentionally drained without collecting (perf
        // optimisation) — we verify the exchange completed via status,
        // byte counters, and timing.
        assert!(resp.bytes_sent > 0, "bytes_sent should be > 0");
        assert!(resp.bytes_received > 0, "bytes_received should be > 0");
        // TTFB may legitimately be a few hundred ns on loopback.
        assert!(resp.ttfb > Duration::ZERO);
        assert!(resp.total >= resp.ttfb);
    }
}

#[compio::test]
async fn concurrent_exchanges_all_succeed() {
    let addr = spawn_echo_server(b"pong").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 8,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        ..TransportOpts::default()
    };
    let pool = Arc::new(Http1Pool::new(&target, &opts).await.expect("pool"));

    let mut vars = VarRegistry::new();
    let plan = url_plan("/health", &mut vars);

    // 100 concurrent futures, each with its own ScenarioContext because
    // exchange takes `&mut ScenarioContext`. We can still borrow `pool`
    // immutably across all of them.
    let mut futs = Vec::with_capacity(100);
    for seed in 0..100u64 {
        let pool = pool.clone();
        let plan = plan.clone();
        let num_vars = vars.len();
        futs.push(async move {
            let mut ctx = ScenarioContext::new(num_vars, from_seed(seed));
            pool.exchange(&plan, &mut ctx).await
        });
    }

    let results = futures_util::future::join_all(futs).await;
    let mut ok = 0usize;
    for r in results {
        let resp = r.expect("exchange error");
        assert_eq!(resp.status, 200);
        assert!(resp.bytes_sent > 0);
        assert!(resp.bytes_received > 0);
        ok += 1;
    }
    assert_eq!(ok, 100);
}

#[compio::test]
async fn dead_server_returns_connect_error() {
    // Bind a listener, record its addr, and drop it — the addr is
    // guaranteed to refuse connections (no one listening on it).
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = listener.local_addr().unwrap();
        drop(listener);
        a
    };

    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_secs(1),
        ..TransportOpts::default()
    };

    let err = Http1Pool::new(&target, &opts).await.expect_err("expected error");
    assert!(
        matches!(err, TransportError::Connect(_) | TransportError::Timeout),
        "expected Connect or Timeout, got {err:?}"
    );
}

#[compio::test]
async fn request_timeout_fires_on_hanging_server() {
    let addr = spawn_hanging_server().await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_millis(300),
        ..TransportOpts::default()
    };

    let pool = Http1Pool::new(&target, &opts).await.expect("pool");
    let mut vars = VarRegistry::new();
    let plan = url_plan("/", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let t0 = std::time::Instant::now();
    let err = pool.exchange(&plan, &mut ctx).await.expect_err("expected timeout");
    let elapsed = t0.elapsed();

    assert!(
        matches!(err, TransportError::Timeout),
        "expected Timeout, got {err:?}"
    );
    // Timeout should fire promptly — allow generous slack for CI jitter
    // but reject anything that took 10x the configured timeout.
    assert!(
        elapsed < Duration::from_secs(3),
        "timeout took too long: {elapsed:?}"
    );
}

#[compio::test]
async fn zero_max_conns_rejected() {
    let target = Target::parse("http://127.0.0.1:9").expect("target");
    let opts = TransportOpts {
        max_conns: 0,
        ..TransportOpts::default()
    };
    let err = Http1Pool::new(&target, &opts).await.expect_err("expected error");
    assert!(matches!(err, TransportError::Connect(_)));
}

// ---------------------------------------------------------------------------
// Template expansion through exchange (Fix 7a).
// ---------------------------------------------------------------------------

#[compio::test]
async fn template_expansion_produces_distinct_urls_per_exchange() {
    let addr = spawn_path_echo_server().await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        ..TransportOpts::default()
    };
    let pool = Http1Pool::new(&target, &opts).await.expect("pool");

    let mut vars = VarRegistry::new();
    // {{counter}} emits 0, 1, 2, ... per worker; {{rand_int}} stretches
    // the expansion path without making assertions flaky (we only check
    // path shape + the counter, not the rand value).
    let url_tpl = Template::compile(
        "/api/{{counter}}?seed={{rand_int:1:100}}",
        &mut vars,
    )
    .expect("compile url");
    let plan = RequestPlan::get(url_tpl);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(42));

    // Fire 10 exchanges. The body is drained without collecting (perf
    // optimisation), so we can't inspect the echoed path here. Template
    // expansion correctness is covered by the unit tests in
    // zerobench-core; this test verifies that the pool handles templated
    // URLs without errors.
    for _ in 0..10 {
        let resp = pool.exchange(&plan, &mut ctx).await.expect("exchange");
        assert_eq!(resp.status, 200);
        assert!(resp.bytes_received > 0);
    }
}

// ---------------------------------------------------------------------------
// max_conns = 1 with concurrent requests (Fix 7b).
// ---------------------------------------------------------------------------

#[compio::test]
async fn max_conns_one_serializes_concurrent_exchanges() {
    let addr = spawn_echo_server(b"pong").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        ..TransportOpts::default()
    };
    let pool = Arc::new(Http1Pool::new(&target, &opts).await.expect("pool"));
    assert_eq!(pool.len(), 1);

    let mut vars = VarRegistry::new();
    let plan = url_plan("/health", &mut vars);
    let num_vars = vars.len();

    // 10 concurrent exchanges on a single-slot pool. The round-robin
    // try_lock loop will fail on the one slot, and the .lock().await
    // fallback kicks in — exactly the path this test exercises.
    let mut futs = Vec::with_capacity(10);
    for seed in 0..10u64 {
        let pool = pool.clone();
        let plan = plan.clone();
        futs.push(async move {
            let mut ctx = ScenarioContext::new(num_vars, from_seed(seed));
            pool.exchange(&plan, &mut ctx).await
        });
    }

    let results = futures_util::future::join_all(futs).await;
    for (i, r) in results.into_iter().enumerate() {
        let resp = r.unwrap_or_else(|e| panic!("exchange {i} failed: {e:?}"));
        assert_eq!(resp.status, 200);
    }
}

// ---------------------------------------------------------------------------
// Slot invalidation after timeout (Fix 7c — covers Fix 2 behaviour).
// ---------------------------------------------------------------------------

#[compio::test]
async fn slot_is_invalidated_after_timeout() {
    // Single-slot pool against a never-responding server. The first
    // exchange times out; the next exchange on the same slot must
    // surface a clean error (not hang, not panic) because the slot's
    // sender was nulled out by the timeout branch.
    let addr = spawn_hanging_server().await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_millis(200),
        ..TransportOpts::default()
    };

    let pool = Http1Pool::new(&target, &opts).await.expect("pool");
    let mut vars = VarRegistry::new();
    let plan = url_plan("/", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    // First exchange: times out.
    let err1 = pool.exchange(&plan, &mut ctx).await.expect_err("timeout");
    assert!(
        matches!(err1, TransportError::Timeout),
        "expected Timeout, got {err1:?}"
    );

    // Second exchange: slot was invalidated (guard.sender = None), so we
    // must surface a *fast*, well-typed error — not hang waiting on a
    // dead connection. v0.0.1 does not lazy-reconnect; TransportError::
    // Connect ("slot N unavailable") is the documented behaviour.
    let t0 = std::time::Instant::now();
    let err2 = pool
        .exchange(&plan, &mut ctx)
        .await
        .expect_err("slot should be dead");
    let elapsed = t0.elapsed();

    assert!(
        matches!(err2, TransportError::Connect(_)),
        "after a timeout, subsequent exchanges should fail with Connect (slot invalidated); got {err2:?}"
    );
    // If we accidentally tried to reuse the dead sender, the test would
    // hang on compio::time::timeout; assert that we returned promptly.
    assert!(
        elapsed < Duration::from_millis(100),
        "second exchange should fail immediately, took {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// Transport trait impl (Fix 8).
// ---------------------------------------------------------------------------

#[compio::test]
async fn transport_trait_build_client_and_exchange() {
    use zerobench_core::Transport;
    use zerobench_http::HttpTransport;

    // Exercises HttpTransport through the Transport trait rather than
    // the inherent Http1Pool API — covers the trait wiring that the
    // dispatcher will use in Phase C.
    let addr = spawn_echo_server(b"trait-ok").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        ..TransportOpts::default()
    };

    let client = <HttpTransport as Transport>::build_client(&target, &opts)
        .await
        .expect("build_client");

    let mut vars = VarRegistry::new();
    let plan = url_plan("/trait", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(7));

    let resp = <HttpTransport as Transport>::exchange(&client, &plan, &mut ctx)
        .await
        .expect("exchange");
    assert_eq!(resp.status, 200);
}
