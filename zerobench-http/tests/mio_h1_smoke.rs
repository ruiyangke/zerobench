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

    // Run for a short burst.
    let worker_stop = Arc::new(AtomicBool::new(false));
    let ws = worker_stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        ws.store(true, Ordering::Relaxed);
    });

    let topts = zerobench_core::transport::TransportOpts::default();
    let stats = zerobench_http::mio_h1::run_mio_worker(
        &plan,
        &target,
        &topts,
        4, // 4 connections
        &worker_stop,
        None, // saturate mode
        None, // no TLS
        None, // no LiveSnapshot
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

    let topts = zerobench_core::transport::TransportOpts::default();
    let all_stats = zerobench_http::mio_h1::run_mio_threaded(
        &target,
        &topts,
        &plan,
        2,  // 2 threads
        8,  // 8 total connections
        plan.duration,
        None, // saturate mode
        None, // no TLS
        None, // no LiveSnapshot
        None, // no external stop
    );

    stop.store(true, Ordering::Relaxed);

    assert_eq!(all_stats.len(), 2, "expected 2 thread stats");
    let total_requests: u64 = all_stats.iter().map(|s| s.requests).sum();
    assert!(
        total_requests > 0,
        "expected at least some requests across threads, got {total_requests}",
    );
}

/// Passing an HTTPS target no longer panics -- TLS is supported. But
/// connecting to a non-existent TLS server produces connect errors (no
/// panic, graceful stats).
#[test]
fn mio_https_without_server_records_connect_errors() {
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
    // With TLS config but no server -- should not panic, just record errors.
    let opts = zerobench_core::transport::TransportOpts {
        insecure_tls: true,
        ..Default::default()
    };
    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]));
    let stats = zerobench_http::mio_h1::run_mio_threaded(
        &target, &opts, &plan, 1, 1, Duration::from_secs(1), None, tls_config, None, None,
    );
    // No panic -- that's the key assertion. Stats may have connect errors.
    let total: u64 = stats.iter().map(|s| s.requests).sum();
    let total_errors: u64 = stats.iter().map(|s| s.errors.total()).sum();
    // Either requests or errors must be > 0 (connection was attempted).
    assert!(total > 0 || total_errors > 0, "expected connect attempts");
}

#[test]
fn mio_open_loop_respects_target_rate() {
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = stop.clone();
    let addr = spawn_server(server_stop);

    let (plan, target) = simple_plan(addr);

    // Target 1000 req/s for 2 seconds -> expect ~2000 requests (+/-20%).
    let target_rps = 1000.0;
    let run_secs = 2;

    let worker_stop = Arc::new(AtomicBool::new(false));
    let ws = worker_stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(run_secs));
        ws.store(true, Ordering::Relaxed);
    });

    let topts = zerobench_core::transport::TransportOpts::default();
    let stats = zerobench_http::mio_h1::run_mio_worker(
        &plan,
        &target,
        &topts,
        10, // plenty of connections
        &worker_stop,
        Some(target_rps),
        None, // no TLS
        None, // no LiveSnapshot
    );

    stop.store(true, Ordering::Relaxed);

    let expected = (target_rps * run_secs as f64) as u64;
    let lo = (expected as f64 * 0.80) as u64;
    let hi = (expected as f64 * 1.20) as u64;
    assert!(
        stats.requests >= lo && stats.requests <= hi,
        "expected ~{expected} requests (range {lo}..{hi}), got {}",
        stats.requests
    );
}

#[test]
fn mio_open_loop_records_keepup_on_overload() {
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = stop.clone();
    let addr = spawn_server(server_stop);

    let (plan, target) = simple_plan(addr);

    // Target 1M req/s with only 2 connections against a blocking server.
    // The token scheduler will outpace connection capacity, producing keepup drops.
    let worker_stop = Arc::new(AtomicBool::new(false));
    let ws = worker_stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        ws.store(true, Ordering::Relaxed);
    });

    let topts = zerobench_core::transport::TransportOpts::default();
    let stats = zerobench_http::mio_h1::run_mio_worker(
        &plan,
        &target,
        &topts,
        2, // only 2 connections -- bottleneck
        &worker_stop,
        Some(1_000_000.0), // 1M req/s -- way more than 2 connections can handle
        None, // no TLS
        None, // no LiveSnapshot
    );

    stop.store(true, Ordering::Relaxed);

    assert!(
        stats.errors.keepup > 0,
        "expected keepup drops > 0, got {}",
        stats.errors.keepup
    );
}

// ---------------------------------------------------------------------------
// TLS smoke tests
// ---------------------------------------------------------------------------

/// Spawn a blocking TLS server using rustls + std::net. Returns the
/// listen address.
fn spawn_tls_server(stop: Arc<AtomicBool>) -> SocketAddr {
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::ServerConfig;

    // Generate self-signed cert for localhost.
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
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let config = Arc::new(config);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();

    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: keep-alive\r\n\r\ntls-ok";

    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((tcp_stream, _)) => {
                    let stop = stop.clone();
                    let config = config.clone();
                    let response = response.to_vec();
                    std::thread::spawn(move || {
                        tcp_stream.set_nodelay(true).ok();
                        tcp_stream.set_nonblocking(false).ok();
                        let conn = rustls::ServerConnection::new(config).unwrap();
                        let mut tls = rustls::StreamOwned::new(conn, tcp_stream);

                        let mut buf = [0u8; 4096];
                        while !stop.load(Ordering::Relaxed) {
                            match tls.read(&mut buf) {
                                Ok(0) => break,
                                Ok(_) => {
                                    if tls.write_all(&response).is_err() {
                                        break;
                                    }
                                    if tls.flush().is_err() {
                                        break;
                                    }
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

    std::thread::sleep(Duration::from_millis(50));
    addr
}

/// Low-level test: manually drive a single MioTlsStream through
/// handshake + HTTP request + response to validate the TLS wrapper
/// works correctly with mio.
#[test]
fn mio_tls_stream_low_level() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_tls_server(stop.clone());

    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut poll = mio::Poll::new().unwrap();
    let saddr: std::net::SocketAddr = addr;
    let mut tcp = mio::net::TcpStream::connect(saddr).unwrap();
    tcp.set_nodelay(true).ok();
    let token = mio::Token(0);
    poll.registry()
        .register(&mut tcp, token, mio::Interest::READABLE | mio::Interest::WRITABLE)
        .unwrap();

    let opts = zerobench_core::transport::TransportOpts {
        insecure_tls: true,
        ..Default::default()
    };
    let config = zerobench_http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]);
    let mut tls = zerobench_http::mio_tls::MioTlsStream::new(tcp, config, "localhost").unwrap();
    tls.complete_handshake(&mut poll, token).unwrap();

    // Write HTTP request
    let req = b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let n = tls.write(req).unwrap();
    assert_eq!(n, req.len());
    tls.flush().unwrap();

    // Read response (poll until readable)
    let mut events = mio::Events::with_capacity(64);
    let mut response = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for TLS response");
        }
        poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
        let mut buf = [0u8; 4096];
        match tls.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("read error: {e}"),
        }
        // Check if we have a complete response.
        if response.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);

    let resp_str = String::from_utf8_lossy(&response);
    assert!(
        resp_str.contains("200 OK"),
        "expected 200 OK in response, got: {resp_str}"
    );
}

#[test]
fn mio_h1_tls_with_self_signed_cert() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_tls_server(stop.clone());

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("https://127.0.0.1:{}/bench", addr.port()), &mut vars)
        .unwrap();
    let req = RequestPlan::get(url);
    let scenario = Scenario {
        name: "mio-tls-smoke".into(),
        rate: RateProfile::Saturate { max_concurrency: 4 },
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

    // Build TLS config with insecure verifier (self-signed cert).
    let opts = zerobench_core::transport::TransportOpts {
        insecure_tls: true,
        ..Default::default()
    };
    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]));

    let worker_stop = Arc::new(AtomicBool::new(false));
    let ws = worker_stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(2));
        ws.store(true, Ordering::Relaxed);
    });

    let stats = zerobench_http::mio_h1::run_mio_worker(
        &plan,
        &target,
        &opts,
        4, // 4 connections
        &worker_stop,
        None, // saturate mode
        tls_config,
        None, // no LiveSnapshot
    );

    stop.store(true, Ordering::Relaxed);

    assert!(
        stats.requests > 0,
        "expected at least some TLS requests, got {} (errors: connect={}, read={}, write={})",
        stats.requests, stats.errors.connect, stats.errors.read, stats.errors.write,
    );
    assert!(
        stats.bytes_sent > 0,
        "expected bytes_sent > 0 over TLS, got {}",
        stats.bytes_sent,
    );
    assert!(
        stats.bytes_recv > 0,
        "expected bytes_recv > 0 over TLS, got {}",
        stats.bytes_recv,
    );
}
