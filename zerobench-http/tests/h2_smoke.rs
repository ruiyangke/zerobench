//! Smoke test for [`zerobench_http::Http2Client`] against an in-process
//! hyper HTTP/2 server over plain TCP ("h2c" / cleartext).
//!
//! The test covers:
//! - Basic exchange: a GET yields status 200 and the expected body.
//! - Concurrent exchanges: 100 in-flight requests all succeed,
//!   demonstrating that one connection multiplexes streams correctly.
//! - Real concurrency: against a server that sleeps 50ms per handler,
//!   100 concurrent requests complete in ≲ a few times 50ms — much
//!   faster than 100 × 50ms (5s), which is what H1 would produce if
//!   the pool were size 1.
//! - Dead-server case: `Http2Client::new` surfaces a connect error
//!   when no one is listening on the port.
//!
//! Hyper server APIs:
//!   `hyper::server::conn::http2::Builder::new(CompioExecutor)
//!      .serve_connection(HyperStream, service)` — drives H2 server
//!   side over our compio IO. The `Builder` needs a timer for keep-
//!   alive, etc., but tests don't exercise those paths so we omit it.

#![cfg(feature = "h2")]

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::net::TcpListener;
use compio::runtime::spawn;
use cyper_core::HyperStream;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response};

// Local executor that spawns onto compio without requiring Send bounds —
// hyper's H2 server side spawns per-stream tasks whose futures contain
// `Incoming` (not Send), so cyper-core's Send-bounded CompioExecutor
// can't be used for the server. compio is single-threaded per runtime
// so `!Send` is fine.
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
use zerobench_core::plan::RequestPlan;
use zerobench_core::rng::from_seed;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::Template;
use zerobench_core::transport::{
    HttpVersionPref, ResponseBody, Target, TransportError, TransportOpts,
};
use zerobench_core::var::VarRegistry;

use zerobench_http::Http2Client;

// ---------------------------------------------------------------------------
// Test server helpers
// ---------------------------------------------------------------------------

/// Boot an in-process H2 server that replies with `body` and status 200.
///
/// Runs on the same compio runtime as the client — compio is strictly
/// single-threaded per runtime, so we `detach` the accept loop and let
/// it live for the test's lifetime.
async fn spawn_h2_server(body: &'static [u8]) -> std::net::SocketAddr {
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
                let svc = service_fn(move |_req: Request<Incoming>| async move {
                    Ok::<_, Infallible>(Response::new(Full::new(
                        Bytes::from_static(body),
                    )))
                });
                // The H2 server side needs an executor to fork streams —
                // `CompioExecutor` spawns each per-stream task onto the
                // current compio runtime.
                let _ = http2::Builder::new(LocalCompioExec)
                    .serve_connection(io, svc)
                    .await;
            })
            .detach();
        }
    })
    .detach();

    addr
}

/// H2 server that sleeps `per_request` before replying. Used to prove
/// that multiple requests do indeed interleave on a single H2 conn.
async fn spawn_h2_sleeping_server(
    per_request: Duration,
    body: &'static [u8],
) -> std::net::SocketAddr {
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
                let svc = service_fn(move |_req: Request<Incoming>| async move {
                    compio::time::sleep(per_request).await;
                    Ok::<_, Infallible>(Response::new(Full::new(
                        Bytes::from_static(body),
                    )))
                });
                let _ = http2::Builder::new(LocalCompioExec)
                    .serve_connection(io, svc)
                    .await;
            })
            .detach();
        }
    })
    .detach();

    addr
}

// ---------------------------------------------------------------------------
// Plan / context builders
// ---------------------------------------------------------------------------

fn url_plan(url: &str, vars: &mut VarRegistry) -> RequestPlan {
    let url = Template::compile(url, vars).expect("compile url");
    RequestPlan::get(url)
}

fn target_for(addr: std::net::SocketAddr) -> Target {
    Target::parse(&format!("http://{addr}")).expect("parse target")
}

fn h2_opts(max_streams: usize) -> TransportOpts {
    TransportOpts {
        max_conns: max_streams,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        http_version: HttpVersionPref::Http2,
        ..TransportOpts::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[compio::test]
async fn single_exchange_returns_expected_body() {
    let addr = spawn_h2_server(b"h2-ok").await;
    let target = target_for(addr);
    let opts = h2_opts(10);

    let client = Http2Client::new(&target, &opts).await.expect("h2 client");

    let mut vars = VarRegistry::new();
    let plan = url_plan("/health", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let resp = client.exchange(&plan, &mut ctx).await.expect("exchange");
    assert_eq!(resp.status, 200);
    match &resp.body {
        ResponseBody::Buffered(b) => assert_eq!(b.as_ref(), b"h2-ok"),
        _ => panic!("expected buffered body"),
    }
    assert!(resp.bytes_sent > 0, "bytes_sent > 0");
    assert!(resp.bytes_received > 0, "bytes_received > 0");
    assert!(resp.ttfb > Duration::ZERO);
    assert!(resp.total >= resp.ttfb);
}

#[compio::test]
async fn concurrent_exchanges_all_succeed_on_single_connection() {
    let addr = spawn_h2_server(b"pong").await;
    let target = target_for(addr);
    let opts = h2_opts(100);
    let client = Arc::new(Http2Client::new(&target, &opts).await.expect("h2 client"));

    let mut vars = VarRegistry::new();
    let plan = url_plan("/concurrent", &mut vars);
    let num_vars = vars.len();

    let mut futs = Vec::with_capacity(100);
    for seed in 0..100u64 {
        let client = client.clone();
        let plan = plan.clone();
        futs.push(async move {
            let mut ctx = ScenarioContext::new(num_vars, from_seed(seed));
            client.exchange(&plan, &mut ctx).await
        });
    }

    let results = futures_util::future::join_all(futs).await;
    let mut ok = 0usize;
    for (i, r) in results.into_iter().enumerate() {
        let resp = r.unwrap_or_else(|e| panic!("exchange {i} failed: {e:?}"));
        assert_eq!(resp.status, 200);
        match resp.body {
            ResponseBody::Buffered(b) => assert_eq!(b.as_ref(), b"pong"),
            _ => panic!("expected buffered body"),
        }
        ok += 1;
    }
    assert_eq!(ok, 100);
}

#[compio::test]
async fn multiplexing_beats_serialisation() {
    // Prove that H2 really does multiplex: against a server that sleeps
    // 50ms per request, 100 concurrent requests must finish in ≪ 5s.
    //
    // The bound is generous (≤ 2s) to tolerate CI jitter on a single-
    // threaded compio runtime, but still far from the 5s a serialising
    // pool (H1 max_conns=1) would produce.
    let addr = spawn_h2_sleeping_server(Duration::from_millis(50), b"slow").await;
    let target = target_for(addr);
    let opts = h2_opts(100);
    let client = Arc::new(Http2Client::new(&target, &opts).await.expect("h2 client"));

    let mut vars = VarRegistry::new();
    let plan = url_plan("/mux", &mut vars);
    let num_vars = vars.len();

    let t0 = Instant::now();
    let mut futs = Vec::with_capacity(100);
    for seed in 0..100u64 {
        let client = client.clone();
        let plan = plan.clone();
        futs.push(async move {
            let mut ctx = ScenarioContext::new(num_vars, from_seed(seed));
            client.exchange(&plan, &mut ctx).await
        });
    }
    let results = futures_util::future::join_all(futs).await;
    let elapsed = t0.elapsed();

    for (i, r) in results.into_iter().enumerate() {
        let resp = r.unwrap_or_else(|e| panic!("exchange {i} failed: {e:?}"));
        assert_eq!(resp.status, 200);
    }

    assert!(
        elapsed < Duration::from_secs(2),
        "100 concurrent 50ms-sleeping requests should finish in < 2s on H2, \
         took {elapsed:?} — is multiplexing actually happening?"
    );
}

#[compio::test]
async fn dead_server_returns_connect_error() {
    // Bind, capture the addr, drop the listener — anything connecting
    // after this point is guaranteed refusal.
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = listener.local_addr().unwrap();
        drop(listener);
        a
    };

    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 10,
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_secs(1),
        http_version: HttpVersionPref::Http2,
        ..TransportOpts::default()
    };

    let err = Http2Client::new(&target, &opts)
        .await
        .expect_err("expected connect error");
    assert!(
        matches!(err, TransportError::Connect(_) | TransportError::Timeout),
        "expected Connect or Timeout, got {err:?}"
    );
}

#[compio::test]
async fn zero_max_conns_rejected() {
    let target = Target::parse("http://127.0.0.1:9").expect("target");
    let opts = TransportOpts {
        max_conns: 0,
        http_version: HttpVersionPref::Http2,
        ..TransportOpts::default()
    };
    let err = Http2Client::new(&target, &opts)
        .await
        .expect_err("expected error");
    assert!(matches!(err, TransportError::Connect(_)));
}

// ---------------------------------------------------------------------------
// Transport trait dispatch
// ---------------------------------------------------------------------------

#[compio::test]
async fn transport_dispatches_to_http2_when_requested() {
    use zerobench_core::Transport;
    use zerobench_http::{HttpClient, HttpTransport};

    let addr = spawn_h2_server(b"via-trait").await;
    let target = target_for(addr);
    let opts = h2_opts(4);

    let client = <HttpTransport as Transport>::build_client(&target, &opts)
        .await
        .expect("build_client");

    // Confirm the dispatcher picked the H2 variant — if it silently
    // downgraded to H1 we'd fail below because the server doesn't speak
    // HTTP/1.
    assert!(
        matches!(client, HttpClient::Http2(_)),
        "expected HttpClient::Http2 variant, got {client:?}"
    );

    let mut vars = VarRegistry::new();
    let plan = url_plan("/trait", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let resp = <HttpTransport as Transport>::exchange(&client, &plan, &mut ctx)
        .await
        .expect("exchange");
    assert_eq!(resp.status, 200);
    match resp.body {
        ResponseBody::Buffered(b) => assert_eq!(b.as_ref(), b"via-trait"),
        _ => panic!("expected buffered body"),
    }
}
