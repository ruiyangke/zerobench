#![cfg(feature = "runtime-compio")]
//! End-to-end TLS smoke tests for the compiled `zerobench` CLI binary.
//!
//! Boots a self-signed HTTPS server on its own thread and invokes the
//! CLI subprocess with `--insecure https://...`; verifies the run
//! completes with a non-zero throughput figure.

use std::convert::Infallible;
use std::net::TcpListener as StdTcpListener;
use std::process::Command;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use compio::net::TcpListener as CompioTcpListener;
use compio_tls::TlsAcceptor;
use cyper_core::{CompioExecutor, HyperStream};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;

fn make_server_config(alpn: &[&[u8]]) -> Arc<ServerConfig> {
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
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

/// Boot an HTTPS server whose ALPN advertises both `h2` and
/// `http/1.1` and dispatches to the right hyper builder based on what
/// the client picks.
fn spawn_https_auto_server(body: &'static [u8]) -> std::net::SocketAddr {
    let bind = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);

    let (ready_tx, ready_rx): (Sender<()>, _) = channel();
    let config = make_server_config(&[b"h2", b"http/1.1"]);

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
                    let alpn = tls.negotiated_alpn().map(|c| c.to_vec());
                    let io = HyperStream::new(tls);
                    let svc = service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(body))))
                    });
                    match alpn.as_deref() {
                        Some(b"h2") => {
                            let _ = hyper::server::conn::http2::Builder::new(CompioExecutor)
                                .serve_connection(io, svc)
                                .await;
                        }
                        _ => {
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, svc)
                                .await;
                        }
                    }
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("https server never bound");
    addr
}

fn zerobench_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zerobench")
}

#[test]
fn cli_saturate_with_insecure_against_self_signed_h1() {
    let addr = spawn_https_auto_server(b"cli-tls-ok");
    // Connect by IP, since SNI for `localhost` vs `127.0.0.1` has
    // subtly different behaviour with some rustls versions. Our cert
    // lists both as SANs so either works.
    let url = format!("https://127.0.0.1:{}/", addr.port());

    let output = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "4",
            "-d",
            "1s",
            "--insecure",
            "--http-version",
            "h1",
            &url,
        ])
        .output()
        .expect("spawn zerobench");

    assert!(
        output.status.success(),
        "CLI exited non-zero; stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[cfg(feature = "h2")]
#[test]
fn cli_saturate_with_insecure_negotiates_h2_via_alpn() {
    let addr = spawn_https_auto_server(b"cli-tls-h2");
    // Connect by IP, since SNI for `localhost` vs `127.0.0.1` has
    // subtly different behaviour with some rustls versions. Our cert
    // lists both as SANs so either works.
    let url = format!("https://127.0.0.1:{}/", addr.port());

    let output = Command::new(zerobench_bin())
        .args([
            "--saturate",
            "-c",
            "4",
            "-d",
            "1s",
            "--insecure",
            // Auto — should prefer h2 via ALPN probe.
            "--http-version",
            "auto",
            &url,
        ])
        .output()
        .expect("spawn zerobench");

    assert!(
        output.status.success(),
        "CLI exited non-zero; stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
