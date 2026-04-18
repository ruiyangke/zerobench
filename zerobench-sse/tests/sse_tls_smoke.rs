//! TLS smoke test for the mio-based SSE runner.
//!
//! Boots an HTTPS SSE server using raw std::net + rustls (no async),
//! self-signed cert, and runs the SSE runner against it.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;

use zerobench_core::plan::{Plan, RequestPlan, Scenario, Step};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_sse::SseSummary;

fn make_server_config() -> Arc<ServerConfig> {
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
    Arc::new(config)
}

fn spawn_https_sse_server(chunks: usize, stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = make_server_config();

    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            listener.set_nonblocking(true).ok();
            let tcp = match listener.accept() {
                Ok((s, _)) => s,
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
            };
            tcp.set_nonblocking(false).ok();
            let config = config.clone();

            std::thread::spawn(move || {
                let conn = match rustls::ServerConnection::new(config) {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let mut tls = rustls::StreamOwned::new(conn, tcp);

                // Read request.
                let mut buf = [0u8; 4096];
                let _ = tls.read(&mut buf);

                // Write SSE response with chunked encoding.
                let headers = "HTTP/1.1 200 OK\r\n\
                               Content-Type: text/event-stream\r\n\
                               Transfer-Encoding: chunked\r\n\
                               \r\n";
                let _ = tls.write_all(headers.as_bytes());

                for i in 0..chunks {
                    let payload = format!("data: tls-event-{i}\n\n");
                    let chunk = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                    if tls.write_all(chunk.as_bytes()).is_err() {
                        return;
                    }
                    let _ = tls.flush();
                }

                // [DONE] + terminal chunk.
                let done = "data: [DONE]\n\n";
                let chunk = format!("{:x}\r\n{}\r\n", done.len(), done);
                let _ = tls.write_all(chunk.as_bytes());
                let _ = tls.write_all(b"0\r\n\r\n");
                let _ = tls.flush();
            });
        }
    });

    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn sse_https_insecure_streams_data() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_https_sse_server(10, stop.clone());
    let mut target = Target::parse(&format!("https://{addr}")).unwrap();
    target.sni = Some("localhost".into());

    let opts = TransportOpts {
        insecure_tls: true,
        ..TransportOpts::default()
    };

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("https://{addr}/stream"), &mut vars).unwrap();
    let mut req = RequestPlan::get(url);
    req.expect_streaming = true;

    let plan = Plan {
        scenarios: vec![Scenario::new(String::from("sse-tls"), vec![Step::Request(req)])],
        duration: Duration::from_secs(2),
        vars,
        ..Plan::new()
    };

    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]));
    let stats = zerobench_sse::run_sse_threaded(&target, &opts, &plan, 1, Duration::from_secs(2), tls_config);
    stop.store(true, Ordering::Relaxed);
    let summary = SseSummary::merge(stats, Duration::from_secs(2));

    assert!(summary.streams >= 1, "expected >= 1 stream, got {}", summary.streams);
    assert!(summary.completed >= 1, "expected completion");
    assert!(summary.chunks >= 10, "expected >= 10 chunks, got {}", summary.chunks);
}

#[test]
fn sse_https_strict_rejects_self_signed() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_https_sse_server(5, stop.clone());
    let mut target = Target::parse(&format!("https://{addr}")).unwrap();
    target.sni = Some("localhost".into());

    let opts = TransportOpts {
        insecure_tls: false,
        ..TransportOpts::default()
    };

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("https://{addr}/stream"), &mut vars).unwrap();
    let req = RequestPlan::get(url);

    let plan = Plan {
        scenarios: vec![Scenario::new(String::from("sse-tls"), vec![Step::Request(req)])],
        duration: Duration::from_secs(1),
        vars,
        ..Plan::new()
    };

    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]));
    let stats = zerobench_sse::run_sse_threaded(&target, &opts, &plan, 1, Duration::from_secs(1), tls_config);
    stop.store(true, Ordering::Relaxed);
    let summary = SseSummary::merge(stats, Duration::from_secs(1));

    assert_eq!(summary.streams, 0, "TLS handshake should not succeed");
    assert!(summary.errors_connect > 0, "expected connect errors");
}
