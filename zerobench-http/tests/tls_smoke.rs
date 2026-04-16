//! End-to-end TLS smoke tests for the HTTP transport.
//!
//! Spins up a local HTTPS server with a rcgen-generated self-signed
//! certificate, then points `Http1Pool` / `Http2Client` / `HttpTransport`
//! at it with various ALPN settings. Verifies three things:
//!
//! 1. `--insecure` (accept-all verifier) lets the handshake complete.
//! 2. Strict verification (webpki-roots) correctly rejects a self-signed
//!    cert even when ALPN would otherwise be fine.
//! 3. `HttpVersionPref::Auto` on HTTPS probes ALPN, picks H2 when the
//!    server advertises it, falls back to H1 when it doesn't.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use compio::net::TcpListener;
use compio::runtime::spawn;
use compio_tls::TlsAcceptor;
use cyper_core::{CompioExecutor, HyperStream};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use zerobench_core::plan::RequestPlan;
use zerobench_core::rng::from_seed;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::Template;
use zerobench_core::transport::{
    HttpVersionPref, ResponseBody, Target, TransportError, TransportOpts,
};
use zerobench_core::var::VarRegistry;

use zerobench_http::{Http1Pool, HttpClient, HttpTransport};

// ---------------------------------------------------------------------------
// Self-signed cert + rustls server config
// ---------------------------------------------------------------------------

/// Generate a fresh self-signed cert for `localhost` + `127.0.0.1`
/// together with a rustls `ServerConfig` advertising the given ALPN
/// protocols. Returns `(cert_der, server_config)` — the cert is kept in
/// case a future test wants to pin it, though today we only use it
/// inside the config.
fn make_server_config(alpn: &[&[u8]]) -> Arc<ServerConfig> {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("self-signed cert");

    // CertificateDer<'static> is an `Into` from rcgen's `Certificate`.
    let cert_der: CertificateDer<'static> = cert.into();
    let key_der: PrivatePkcs8KeyDer<'static> =
        PrivatePkcs8KeyDer::from(key_pair.serialize_der());

    // Install the ring provider for rustls. Idempotent; tests may run
    // many times in the same process.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))
        .expect("server config");
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

/// Boot an HTTPS (HTTP/1.1) echo server on an ephemeral port. The
/// `body` is returned verbatim for every request. ALPN advertises
/// `http/1.1`.
async fn spawn_https_h1_server(body: &'static [u8]) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = make_server_config(&[b"http/1.1"]);
    let acceptor = TlsAcceptor::from(config);

    spawn(async move {
        loop {
            let (socket, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            spawn(async move {
                // TLS handshake.
                let tls = match acceptor.accept(socket).await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let io = HyperStream::new(tls);
                let service =
                    service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(body))))
                    });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            })
            .detach();
        }
    })
    .detach();

    addr
}

/// Boot an HTTPS server that advertises both `h2` and `http/1.1` via
/// ALPN and serves the chosen protocol accordingly. The body is
/// returned verbatim for every request.
async fn spawn_https_auto_server(body: &'static [u8]) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = make_server_config(&[b"h2", b"http/1.1"]);
    let acceptor = TlsAcceptor::from(config);

    spawn(async move {
        loop {
            let (socket, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            spawn(async move {
                let tls = match acceptor.accept(socket).await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                // Branch on ALPN for which hyper server builder to use.
                let alpn = tls.negotiated_alpn().map(|c| c.to_vec());
                let io = HyperStream::new(tls);
                let service =
                    service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(body))))
                    });
                match alpn.as_deref() {
                    Some(b"h2") => {
                        let _ = hyper::server::conn::http2::Builder::new(CompioExecutor)
                            .serve_connection(io, service)
                            .await;
                    }
                    _ => {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service)
                            .await;
                    }
                }
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

fn https_target(addr: std::net::SocketAddr) -> Target {
    // SNI must be set to `localhost` — our test certificate only lists
    // `localhost` and `127.0.0.1` as SANs, and the connect target is an
    // IP literal. rustls accepts IPs, but we set SNI explicitly to be
    // robust across versions.
    let mut t = Target::parse(&format!("https://{addr}")).unwrap();
    t.sni = Some("localhost".into());
    t
}

fn get_plan(url: &str, vars: &mut VarRegistry) -> RequestPlan {
    let url = Template::compile(url, vars).expect("compile");
    RequestPlan::get(url)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[compio::test]
async fn h1_with_insecure_verifier_succeeds_over_self_signed_tls() {
    let addr = spawn_https_h1_server(b"tls-ok").await;
    let target = https_target(addr);

    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        insecure_tls: true,
        http_version: HttpVersionPref::Http1,
        ..TransportOpts::default()
    };

    let pool = Http1Pool::new(&target, &opts).await.expect("pool");
    let mut vars = VarRegistry::new();
    let plan = get_plan("/health", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(1));

    let resp = pool.exchange(&plan, &mut ctx).await.expect("exchange");
    assert_eq!(resp.status, 200);
    match resp.body {
        ResponseBody::Buffered(b) => assert_eq!(b.as_ref(), b"tls-ok"),
        _ => panic!("expected buffered body"),
    }
    // TLS adds overhead vs plain HTTP, but the counter deltas still
    // must be positive.
    assert!(resp.bytes_sent > 0, "bytes_sent should be > 0");
    assert!(resp.bytes_received > 0, "bytes_received should be > 0");
}

#[compio::test]
async fn h1_strict_verification_rejects_self_signed() {
    let addr = spawn_https_h1_server(b"unused").await;
    let target = https_target(addr);

    // Default `insecure_tls: false` — strict webpki-roots verification.
    // The self-signed cert has no issuer in the Mozilla trust store, so
    // the handshake must fail.
    let opts = TransportOpts {
        max_conns: 1,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        insecure_tls: false,
        http_version: HttpVersionPref::Http1,
        ..TransportOpts::default()
    };

    let err = Http1Pool::new(&target, &opts)
        .await
        .expect_err("self-signed cert must be rejected");
    match err {
        TransportError::Tls(msg) => {
            // The exact rustls message varies by version; just make
            // sure we got the right variant.
            let _ = msg;
        }
        other => panic!("expected Tls error, got {other:?}"),
    }
}

#[compio::test]
#[cfg(feature = "h2")]
async fn auto_negotiates_h2_via_alpn_on_https() {
    let addr = spawn_https_auto_server(b"via-h2").await;
    let target = https_target(addr);

    let opts = TransportOpts {
        max_conns: 4,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        insecure_tls: true,
        http_version: HttpVersionPref::Auto,
        ..TransportOpts::default()
    };

    use zerobench_core::Transport;
    let client = <HttpTransport as Transport>::build_client(&target, &opts)
        .await
        .expect("build_client");
    assert!(
        matches!(client, HttpClient::Http2(_)),
        "Auto + HTTPS + server offering h2 must produce an H2 client"
    );

    // Dispatch one request to confirm the H2 path actually works e2e.
    let mut vars = VarRegistry::new();
    let plan = get_plan("/h2", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(3));
    let resp = <HttpTransport as Transport>::exchange(&client, &plan, &mut ctx)
        .await
        .expect("exchange");
    assert_eq!(resp.status, 200);
    match resp.body {
        ResponseBody::Buffered(b) => assert_eq!(b.as_ref(), b"via-h2"),
        _ => panic!("expected buffered body"),
    }
}

#[compio::test]
async fn auto_falls_back_to_h1_when_server_only_offers_h1() {
    let addr = spawn_https_h1_server(b"via-h1").await;
    let target = https_target(addr);

    let opts = TransportOpts {
        max_conns: 2,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        insecure_tls: true,
        http_version: HttpVersionPref::Auto,
        ..TransportOpts::default()
    };

    use zerobench_core::Transport;
    let client = <HttpTransport as Transport>::build_client(&target, &opts)
        .await
        .expect("build_client");
    assert!(
        matches!(client, HttpClient::Http1(_)),
        "Auto + HTTPS + server offering only http/1.1 must fall back to H1"
    );

    let mut vars = VarRegistry::new();
    let plan = get_plan("/fallback", &mut vars);
    let mut ctx = ScenarioContext::new(vars.len(), from_seed(4));
    let resp = <HttpTransport as Transport>::exchange(&client, &plan, &mut ctx)
        .await
        .expect("exchange");
    assert_eq!(resp.status, 200);
    match resp.body {
        ResponseBody::Buffered(b) => assert_eq!(b.as_ref(), b"via-h1"),
        _ => panic!("expected buffered body"),
    }
}
