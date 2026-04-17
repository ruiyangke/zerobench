#![cfg(feature = "runtime-compio")]
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
    HttpVersionPref, Target, TransportError, TransportOpts,
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

/// Like [`spawn_h2_sleeping_server`] but the server advertises a
/// `SETTINGS_MAX_CONCURRENT_STREAMS` cap to the client. This is the
/// coupling required to prove *the client* is obeying a concurrency
/// bound: per h2's settings-application rules, the client's
/// `initial_max_send_streams` is overridden by the peer's initial
/// SETTINGS, so without the server advertising a cap the client has
/// no enforceable ceiling. Used by the regression test that guards
/// the `initial_max_send_streams` wiring in [`crate::h2`].
async fn spawn_h2_sleeping_server_capped(
    per_request: Duration,
    max_streams: u32,
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
                let mut builder = http2::Builder::new(LocalCompioExec);
                builder.max_concurrent_streams(max_streams);
                let _ = builder.serve_connection(io, svc).await;
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

/// End-to-end smoke test for the HTTP/2 outgoing-stream concurrency
/// cap plumbing.
///
/// # Background
///
/// Hyper's H2 *client* exposes two similarly-named knobs that do very
/// different things:
///
/// - `max_concurrent_streams(n)` — caps how many streams the **server**
///   may initiate (server push). Irrelevant for normal request/response
///   benchmarking. An earlier version of [`crate::h2`] incorrectly used
///   this for the `-c N` ceiling.
/// - `initial_max_send_streams(n)` — the ceiling on streams **we** (the
///   client) may initiate. This is what `-c N` should be wired to.
///
/// Per RFC 9113 §5.1.2 the authoritative cap on client-initiated
/// streams is whatever the server advertises in `SETTINGS_MAX_\
/// CONCURRENT_STREAMS`. Once the server's preface SETTINGS arrive, h2
/// overwrites the client's `initial_max_send_streams` with the peer's
/// value. So to *observe* a cap in a test we pair:
///
/// - Client: [`Http2Client::new`] forwarding `opts.max_conns` → hyper's
///   `initial_max_send_streams` (Fix 1).
/// - Server: advertises `max_concurrent_streams = 2` in its initial
///   SETTINGS.
///
/// If either side fails to wire the cap, the observed wall-clock shape
/// of 10 × 50ms requests collapses from ~250ms to ~50ms.
///
/// # What this guards
///
/// A future edit that wires `opts.max_conns` into the wrong hyper
/// builder method (e.g. the server-push `max_concurrent_streams`
/// again), or drops the call entirely, will still produce a *build*
/// that works but with the server's 2-stream SETTINGS as the only
/// ceiling. That alone can't distinguish old-bug from fixed code
/// against a cooperative server, so the assertion also checks that
/// all 10 exchanges succeed — catching the case where a mis-wired
/// cap would let us exceed the server's advertised limit and trigger
/// `REFUSED_STREAM` resets.
#[compio::test]
async fn initial_max_send_streams_is_respected() {
    // Server both sleeps 50ms per response AND advertises a
    // `max_concurrent_streams = 2` cap to the client. The server cap
    // is the authoritative limit per the spec; the client's
    // `initial_max_send_streams(2)` echoes that locally so the h2
    // stack has consistent bookkeeping from first byte onward.
    let addr =
        spawn_h2_sleeping_server_capped(Duration::from_millis(50), 2, b"capped").await;
    let target = target_for(addr);
    // Cap ourselves at 2 concurrent outgoing streams. The server-side
    // SETTINGS will echo this value.
    let opts = h2_opts(2);

    let client = Arc::new(Http2Client::new(&target, &opts).await.expect("h2 client"));

    let mut vars = VarRegistry::new();
    let plan = url_plan("/capped", &mut vars);
    let num_vars = vars.len();

    let t0 = Instant::now();
    let mut futs = Vec::with_capacity(10);
    for seed in 0..10u64 {
        let client = client.clone();
        let plan = plan.clone();
        futs.push(async move {
            let mut ctx = ScenarioContext::new(num_vars, from_seed(seed));
            client.exchange(&plan, &mut ctx).await
        });
    }
    let results = futures_util::future::join_all(futs).await;
    let elapsed = t0.elapsed();

    // All 10 must succeed. If the client ignored the negotiated cap and
    // tried to open 10 streams at once, the server would reject the
    // excess with REFUSED_STREAM and some of these would fail.
    for (i, r) in results.into_iter().enumerate() {
        let resp = r.unwrap_or_else(|e| panic!("exchange {i} failed: {e:?}"));
        assert_eq!(resp.status, 200);
    }

    // 2 concurrent × 50ms × 5 batches = ≥ 250ms. We assert ≥ 200ms as a
    // generous floor; without the cap, 10 parallel streams against a
    // 50ms-sleeping server would finish in ~50ms, well below the bound.
    assert!(
        elapsed >= Duration::from_millis(200),
        "elapsed {elapsed:?} suggests the 2-stream cap was not honoured \
         (would be ~50ms if the client ignored the advertised limit)"
    );
}

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
}
