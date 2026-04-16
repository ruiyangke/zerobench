//! End-to-end TLS smoke test for the SSE runner.
//!
//! Boots an HTTPS SSE server with a self-signed cert and runs the
//! SSE runner against it with `insecure_tls = true`. Verifies that
//! data frames round-trip over the encrypted transport.

use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use compio::net::TcpListener as CompioTcpListener;
use compio_tls::TlsAcceptor;
use cyper_core::HyperStream;
use futures_util::stream::{self, StreamExt};
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;

use zerobench_core::plan::RequestPlan;
use zerobench_core::rng::from_seed;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::Template;
use zerobench_core::transport::{HttpVersionPref, Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_sse::{SseRunner, SseStats};

// ---------------------------------------------------------------------------
// Self-signed server setup
// ---------------------------------------------------------------------------

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
    // SSE is HTTP/1.1; advertise only that via ALPN.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(config)
}

/// Boot an HTTPS SSE server that emits `chunks` data events then `[DONE]`.
fn spawn_https_sse_server(chunks: usize) -> SocketAddr {
    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);

    let (ready_tx, ready_rx): (Sender<()>, _) = channel();
    let config = make_server_config();

    thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = CompioTcpListener::bind(addr).await.unwrap();
            let acceptor = TlsAcceptor::from(config);
            let _ = ready_tx.send(());

            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let acceptor = acceptor.clone();
                compio::runtime::spawn(async move {
                    let tls = match acceptor.accept(socket).await {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let io = HyperStream::new(tls);
                    let svc = service_fn(move |_req: Request<hyper::body::Incoming>| async move {
                        let frames: Vec<_> = (0..chunks)
                            .map(move |i| format!("data: tls-event-{i}\n\n"))
                            .chain(std::iter::once("data: [DONE]\n\n".to_string()))
                            .collect();
                        let s = stream::iter(frames).then(|payload| async move {
                            Ok::<_, Infallible>(Frame::data(Bytes::from(payload)))
                        });
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
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("sse https server never bound");
    addr
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[compio::test]
async fn sse_https_insecure_streams_data_events() {
    let addr = spawn_https_sse_server(10);
    let mut target = Target::parse(&format!("https://{addr}")).unwrap();
    target.sni = Some("localhost".into());

    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(10),
        insecure_tls: true,
        http_version: HttpVersionPref::Http1,
        ..TransportOpts::default()
    };

    let mut vars = VarRegistry::new();
    let url = Template::compile("/stream", &mut vars).unwrap();
    let plan = RequestPlan::get(url);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let mut stats = SseStats::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    SseRunner::run_iteration(&target, &opts, &plan, &mut ctx, &mut stats, deadline).await;

    assert_eq!(
        stats.errors_connect, 0,
        "unexpected connect errors: {stats:?}"
    );
    assert_eq!(stats.errors_read, 0, "unexpected read errors: {stats:?}");
    assert_eq!(stats.streams, 1, "should have seen one stream");
    assert_eq!(stats.completed, 1, "stream should have completed");
    assert!(
        stats.chunks >= 10,
        "expected ≥10 data events, got {}",
        stats.chunks
    );
    assert!(
        stats.bytes_received > 0,
        "encrypted bytes on wire must be > 0"
    );
}

#[compio::test]
async fn sse_https_strict_rejects_self_signed() {
    let addr = spawn_https_sse_server(5);
    let mut target = Target::parse(&format!("https://{addr}")).unwrap();
    target.sni = Some("localhost".into());

    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        insecure_tls: false,
        http_version: HttpVersionPref::Http1,
        ..TransportOpts::default()
    };

    let mut vars = VarRegistry::new();
    let url = Template::compile("/stream", &mut vars).unwrap();
    let plan = RequestPlan::get(url);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let mut stats = SseStats::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    SseRunner::run_iteration(&target, &opts, &plan, &mut ctx, &mut stats, deadline).await;

    assert_eq!(stats.streams, 0, "handshake should not have succeeded");
    assert!(
        stats.errors_connect > 0,
        "strict TLS verification must reject self-signed: {stats:?}"
    );
}
