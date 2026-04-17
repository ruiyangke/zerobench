#![cfg(all(feature = "raw-h1", feature = "runtime-compio"))]
//! Smoke test for [`zerobench_http::RawH1Pool`] against an in-process
//! hyper server.
//!
//! Mirrors `h1_smoke.rs` but exercises the raw HTTP/1.1 transport that
//! bypasses hyper on the client side. The server is still hyper-based
//! (it's the standard echo server reused across all tests).
//!
//! Tests cover:
//! - Sequential exchanges (100 iterations on a 4-slot pool).
//! - Concurrent exchanges (50 concurrent futures on an 8-slot pool).
//! - Dead-server detection (connect error).
//! - POST with a static body.

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
use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::rng::from_seed;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportError, TransportOpts};
use zerobench_core::var::VarRegistry;

use zerobench_http::RawH1Pool;

// ---------------------------------------------------------------------------
// Test server — handles incoming connections until dropped.
// ---------------------------------------------------------------------------

/// Boot a server bound to 127.0.0.1:0, responding with a fixed body.
async fn spawn_echo_server(body: &'static [u8]) -> std::net::SocketAddr {
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

/// Boot a server that echoes the request body back in the response.
async fn spawn_body_echo_server() -> std::net::SocketAddr {
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
                let service = service_fn(|req: Request<Incoming>| async move {
                    use http_body_util::BodyExt;
                    let body_bytes = req.collect().await.unwrap().to_bytes();
                    Ok::<_, Infallible>(Response::new(Full::new(body_bytes)))
                });
                let _ = http1::Builder::new().serve_connection(io, service).await;
            })
            .detach();
        }
    })
    .detach();

    addr
}

// ---------------------------------------------------------------------------
// Helpers
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
async fn raw_h1_sequential_requests() {
    let addr = spawn_echo_server(b"pong").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 4,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        ..TransportOpts::default()
    };

    let pool = RawH1Pool::new(&target, &opts).await.expect("pool");
    assert_eq!(pool.len(), 4);

    let mut vars = VarRegistry::new();
    let plan = url_plan("/health", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(7));

    for _ in 0..100 {
        let resp = pool.exchange(&plan, &mut ctx).await.expect("exchange");
        assert_eq!(resp.status, 200);
        assert!(resp.bytes_sent > 0, "bytes_sent should be > 0");
        assert!(resp.bytes_received > 0, "bytes_received should be > 0");
        assert!(resp.ttfb > Duration::ZERO);
        assert!(resp.total >= resp.ttfb);
    }
}

#[compio::test]
async fn raw_h1_concurrent_requests() {
    let addr = spawn_echo_server(b"pong").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 8,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        ..TransportOpts::default()
    };
    let pool = Arc::new(RawH1Pool::new(&target, &opts).await.expect("pool"));

    let mut vars = VarRegistry::new();
    let plan = url_plan("/health", &mut vars);

    let mut futs = Vec::with_capacity(50);
    for seed in 0..50u64 {
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
    assert_eq!(ok, 50);
}

#[compio::test]
async fn raw_h1_dead_server() {
    // Bind a listener, record its addr, and drop it.
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

    let err = RawH1Pool::new(&target, &opts)
        .await
        .expect_err("expected error");
    assert!(
        matches!(err, TransportError::Connect(_) | TransportError::Timeout),
        "expected Connect or Timeout, got {err:?}"
    );
}

#[compio::test]
async fn raw_h1_with_body() {
    let addr = spawn_body_echo_server().await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        ..TransportOpts::default()
    };
    let pool = RawH1Pool::new(&target, &opts).await.expect("pool");

    let mut vars = VarRegistry::new();
    let url = Template::compile("/echo", &mut vars).expect("compile url");
    let plan = RequestPlan {
        method: http::Method::POST,
        url,
        headers: Default::default(),
        body: Some(BodySource::Static(Bytes::from_static(b"hello world"))),
        extract: Vec::new(),
        checks: Vec::new(),
        expect_streaming: false,
    };
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let resp = pool.exchange(&plan, &mut ctx).await.expect("exchange");
    assert_eq!(resp.status, 200);
    assert!(resp.bytes_sent > 0);
    assert!(resp.bytes_received > 0);
}

#[compio::test]
async fn raw_h1_zero_conns_rejected() {
    let target = Target::parse("http://127.0.0.1:9").expect("target");
    let opts = TransportOpts {
        max_conns: 0,
        ..TransportOpts::default()
    };
    let err = RawH1Pool::new(&target, &opts)
        .await
        .expect_err("expected error");
    assert!(matches!(err, TransportError::Connect(_)));
}

#[compio::test]
async fn raw_h1_tls_rejected() {
    let target = Target::parse("https://127.0.0.1:443").expect("target");
    let opts = TransportOpts::default();
    let err = RawH1Pool::new(&target, &opts)
        .await
        .expect_err("expected error");
    assert!(
        matches!(err, TransportError::Protocol(_)),
        "expected Protocol error for TLS, got {err:?}"
    );
}

#[compio::test]
async fn raw_h1_transport_trait_wiring() {
    use zerobench_core::Transport;
    use zerobench_http::RawH1Transport;

    let addr = spawn_echo_server(b"trait-ok").await;
    let target = target_for(addr);
    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        ..TransportOpts::default()
    };

    let client = <RawH1Transport as Transport>::build_client(&target, &opts)
        .await
        .expect("build_client");

    let mut vars = VarRegistry::new();
    let plan = url_plan("/trait", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(7));

    let resp = <RawH1Transport as Transport>::exchange(&client, &plan, &mut ctx)
        .await
        .expect("exchange");
    assert_eq!(resp.status, 200);
}
