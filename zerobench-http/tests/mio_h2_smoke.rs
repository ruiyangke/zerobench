#![cfg(feature = "mio-h2")]
//! Smoke test for the mio-based HTTP/2 transport.
//!
//! Spins up an in-process H2 server (tokio + hyper on a separate thread)
//! that replies with a fixed 200 OK + body, then runs `run_mio_h2_worker`
//! for a short burst and asserts that requests were recorded.
//!
//! The test server uses tokio; the client under test uses mio. This proves
//! the no-async-runtime mio+h2 approach works against a real H2 server.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

use zerobench_core::plan::{Plan, RateProfile, RequestPlan, Scenario, Step};
use zerobench_core::template::Template;
use zerobench_core::transport::Target;
use zerobench_core::var::VarRegistry;

// ---------------------------------------------------------------------------
// H2 test server (tokio-based, runs on a dedicated thread)
// ---------------------------------------------------------------------------

/// Spawn a tokio-based H2 server on a new OS thread. Returns the listen
/// address. The server replies with a fixed 200 OK + `body` to every
/// request, supporting keep-alive and concurrent streams.
fn spawn_h2_server(body: &'static [u8]) -> SocketAddr {
    let (tx, rx) = std::sync::mpsc::channel::<SocketAddr>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();

            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                stream.set_nodelay(true).ok();
                let io = TokioIo::new(stream);

                tokio::task::spawn_local(async move {
                    let svc = service_fn(move |_req: Request<Incoming>| {
                        let body = body;
                        async move {
                            Ok::<_, Infallible>(Response::new(Full::new(
                                Bytes::from_static(body),
                            )))
                        }
                    });
                    let _ = http2::Builder::new(TokioExec)
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });
    });

    let addr = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // Give the server a moment to start accepting.
    std::thread::sleep(Duration::from_millis(50));
    addr
}

/// Minimal executor that spawns onto the current tokio local set.
/// hyper's H2 server side spawns per-stream tasks whose futures contain
/// `Incoming` (not Send), so we use `spawn_local` rather than `spawn`.
#[derive(Clone, Copy)]
struct TokioExec;

impl<F> hyper::rt::Executor<F> for TokioExec
where
    F: std::future::Future + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn_local(fut);
    }
}

// ---------------------------------------------------------------------------
// Helper: build a simple Plan + Target
// ---------------------------------------------------------------------------

fn simple_plan(addr: SocketAddr) -> (Plan, Target) {
    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("http://{addr}/bench"), &mut vars).unwrap();
    let req = RequestPlan::get(url);
    let scenario = Scenario {
        name: "mio-h2-smoke".into(),
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

/// Build the H2 request that `run_mio_h2_worker` expects.
fn build_request(plan: &Plan, target: &Target) -> http::Request<()> {
    use zerobench_core::plan::Step;
    use zerobench_core::rng;
    use zerobench_core::scenario_context::ScenarioContext;

    let step = plan.scenarios[0].steps.first().unwrap();
    let rp = match step {
        Step::Request(r) => r,
        _ => panic!("expected request step"),
    };

    let mut ctx = ScenarioContext::new(plan.vars.len(), rng::from_entropy());
    let mut url_buf = Vec::with_capacity(256);
    let mut ectx = ctx.expand_ctx();
    rp.url.expand_into(&mut url_buf, &mut ectx);
    let url_str = std::str::from_utf8(&url_buf).unwrap_or("/");

    // Extract path from the full URL.
    let path = if let Some(pos) = url_str.find("://") {
        let after_scheme = &url_str[pos + 3..];
        match after_scheme.find('/') {
            Some(i) => &after_scheme[i..],
            None => "/",
        }
    } else {
        url_str
    };

    http::Request::builder()
        .method(rp.method.as_str())
        .uri(path)
        .header("host", target.addr())
        .body(())
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn mio_h2_worker_records_requests() {
    let addr = spawn_h2_server(b"h2ok");

    let (plan, target) = simple_plan(addr);
    let request = build_request(&plan, &target);

    // Run for a short burst.
    let stop = Arc::new(AtomicBool::new(false));
    let ws = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(2));
        ws.store(true, Ordering::Relaxed);
    });

    let stats = zerobench_http::mio_h2::run_mio_h2_worker(
        &target,
        &request,
        10, // 10 concurrent streams
        &stop,
        plan.scenarios.len(),
        None, // saturate mode
        None, // no TLS
    );

    assert!(
        stats.requests > 0,
        "expected at least some requests, got {}",
        stats.requests
    );
}

#[test]
fn mio_h2_threaded_records_requests() {
    let addr = spawn_h2_server(b"h2ok");

    let (mut plan, target) = simple_plan(addr);
    plan.duration = Duration::from_secs(2);

    let all_stats = zerobench_http::mio_h2::run_mio_h2_threaded(
        &target,
        &plan,
        2,  // 2 threads
        10, // 10 total streams
        plan.duration,
        None, // saturate mode
        None, // no TLS
    );

    assert_eq!(all_stats.len(), 2, "expected 2 thread stats");
    let total_requests: u64 = all_stats.iter().map(|s| s.requests).sum();
    assert!(
        total_requests > 0,
        "expected at least some requests across threads, got {total_requests}",
    );
}

/// Passing an HTTPS target no longer panics -- TLS is supported.
/// Connecting to a non-existent TLS server produces connect errors.
#[test]
fn mio_h2_https_without_server_records_connect_errors() {
    let target = Target::parse("https://127.0.0.1:19443").unwrap();
    let mut vars = VarRegistry::new();
    let url = Template::compile("https://127.0.0.1:19443/bench", &mut vars).unwrap();
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
    let opts = zerobench_core::transport::TransportOpts {
        insecure_tls: true,
        ..Default::default()
    };
    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"h2"]));
    let stats = zerobench_http::mio_h2::run_mio_h2_threaded(
        &target, &plan, 1, 1, Duration::from_secs(1), None, tls_config,
    );
    // No panic -- graceful handling.
    let total: u64 = stats.iter().map(|s| s.requests).sum();
    let total_errors: u64 = stats.iter().map(|s| s.errors.total()).sum();
    assert!(total > 0 || total_errors > 0, "expected connect attempts");
}

// ---------------------------------------------------------------------------
// TLS smoke tests
// ---------------------------------------------------------------------------

/// Spawn a tokio-based H2+TLS server with a self-signed certificate.
fn spawn_h2_tls_server(body: &'static [u8]) -> SocketAddr {
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::ServerConfig;

    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("self-signed cert");
    let cert_der: CertificateDer<'static> = cert.into();
    let key_der: PrivatePkcs8KeyDer<'static> =
        PrivatePkcs8KeyDer::from(key_pair.serialize_der());

    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))
        .expect("server config");
    config.alpn_protocols = vec![b"h2".to_vec()];
    let config = Arc::new(config);

    let (tx, rx) = std::sync::mpsc::channel::<SocketAddr>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();

            let acceptor = tokio_rustls::TlsAcceptor::from(config);

            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                stream.set_nodelay(true).ok();
                let acceptor = acceptor.clone();

                tokio::task::spawn_local(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let io = TokioIo::new(tls_stream);

                    let svc = service_fn(move |_req: Request<Incoming>| {
                        let body = body;
                        async move {
                            Ok::<_, Infallible>(Response::new(Full::new(
                                Bytes::from_static(body),
                            )))
                        }
                    });
                    let _ = http2::Builder::new(TokioExec)
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });
    });

    let addr = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn mio_h2_tls_with_alpn() {
    let addr = spawn_h2_tls_server(b"h2-tls-ok");

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("https://127.0.0.1:{}/bench", addr.port()), &mut vars)
        .unwrap();
    let req = RequestPlan::get(url);
    let scenario = Scenario {
        name: "mio-h2-tls-smoke".into(),
        rate: RateProfile::Saturate { max_concurrency: 10 },
        steps: vec![Step::Request(req)],
    };
    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: Duration::from_secs(2),
        warmup: None,
        threads: 1,
    };
    let mut target = Target::parse(&format!("https://127.0.0.1:{}", addr.port())).unwrap();
    target.sni = Some("localhost".into());

    let request = build_request(&plan, &target);

    // Build TLS config with insecure verifier + ALPN h2.
    let opts = zerobench_core::transport::TransportOpts {
        insecure_tls: true,
        ..Default::default()
    };
    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"h2"]));

    let stop = Arc::new(AtomicBool::new(false));
    let ws = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(2));
        ws.store(true, Ordering::Relaxed);
    });

    let stats = zerobench_http::mio_h2::run_mio_h2_worker(
        &target,
        &request,
        10, // 10 concurrent streams
        &stop,
        plan.scenarios.len(),
        None, // saturate mode
        tls_config,
    );

    assert!(
        stats.requests > 0,
        "expected at least some H2+TLS requests, got {} (errors: connect={}, read={}, write={})",
        stats.requests, stats.errors.connect, stats.errors.read, stats.errors.write,
    );
}
