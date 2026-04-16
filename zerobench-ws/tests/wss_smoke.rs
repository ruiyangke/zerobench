//! End-to-end TLS smoke test for the WebSocket runner.
//!
//! Spins up a rcgen-self-signed `wss://` echo server and runs
//! `run_ws_saturate` against it with `insecure_tls = true`. Verifies
//! that the handshake + frame exchange work over TLS, and that strict
//! verification rejects the self-signed cert.

use std::convert::TryInto;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::{TcpListener as CompioTcpListener, TcpStream};
use compio_tls::{TlsAcceptor, TlsStream};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;

use zerobench_core::stop::StopSignal;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_ws::frame::{encode_frame, Opcode};
use zerobench_ws::handshake::{compute_accept, find_headers_end};
use zerobench_ws::{run_ws_saturate, WsPlan, WsSummary};

// ---------------------------------------------------------------------------
// Echo server — copied from ws_smoke.rs but generic over the IO type so
// we can hand it a `TlsStream<TcpStream>`.
// ---------------------------------------------------------------------------

fn extract_ws_key(raw: &[u8]) -> Option<String> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    if req.parse(raw).ok()?.is_partial() {
        return None;
    }
    for h in req.headers {
        if h.name.eq_ignore_ascii_case("sec-websocket-key") {
            return std::str::from_utf8(h.value).ok().map(|s| s.trim().to_string());
        }
    }
    None
}

fn build_101(accept: &str) -> Vec<u8> {
    let mut s = String::with_capacity(128);
    s.push_str("HTTP/1.1 101 Switching Protocols\r\n");
    s.push_str("Upgrade: websocket\r\n");
    s.push_str("Connection: Upgrade\r\n");
    s.push_str("Sec-WebSocket-Accept: ");
    s.push_str(accept);
    s.push_str("\r\n\r\n");
    s.into_bytes()
}

async fn read_request<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    loop {
        if let Some(end) = find_headers_end(buf) {
            return Ok(end);
        }
        let chunk: Vec<u8> = Vec::with_capacity(1024);
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before full request",
            ));
        }
        buf.extend_from_slice(&returned[..n]);
        if buf.len() > 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request too big",
            ));
        }
    }
}

/// Server-side: read a complete masked client frame and return the
/// unmasked payload + opcode. Duplicates the ws_smoke implementation
/// so this test file stays self-contained (the original is a sibling
/// file, not a public module).
async fn recv_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
) -> std::io::Result<(Opcode, Vec<u8>, bool)> {
    loop {
        if buf.len() >= 2 {
            let b0 = buf[0];
            let b1 = buf[1];
            let fin = (b0 & 0x80) != 0;
            let opcode = match b0 & 0x0F {
                0x0 => Opcode::Continuation,
                0x1 => Opcode::Text,
                0x2 => Opcode::Binary,
                0x8 => Opcode::Close,
                0x9 => Opcode::Ping,
                0xA => Opcode::Pong,
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "bad opcode",
                    ))
                }
            };
            let masked = (b1 & 0x80) != 0;
            if !masked {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unmasked client frame",
                ));
            }
            let short = b1 & 0x7F;
            let (payload_len, hdr_size) = match short {
                0..=125 => (short as usize, 2),
                126 => {
                    if buf.len() < 4 {
                        (0usize, usize::MAX)
                    } else {
                        (u16::from_be_bytes([buf[2], buf[3]]) as usize, 4)
                    }
                }
                127 => {
                    if buf.len() < 10 {
                        (0usize, usize::MAX)
                    } else {
                        let mut eight = [0u8; 8];
                        eight.copy_from_slice(&buf[2..10]);
                        (u64::from_be_bytes(eight) as usize, 10)
                    }
                }
                _ => unreachable!(),
            };

            if hdr_size != usize::MAX {
                let mask_start = hdr_size;
                let frame_end = mask_start + 4 + payload_len;
                if buf.len() >= frame_end {
                    let mask: [u8; 4] = buf[mask_start..mask_start + 4].try_into().unwrap();
                    let mut payload = buf[mask_start + 4..frame_end].to_vec();
                    for (i, b) in payload.iter_mut().enumerate() {
                        *b ^= mask[i % 4];
                    }
                    // Drain consumed bytes from buf.
                    buf.drain(..frame_end);
                    return Ok((opcode, payload, fin));
                }
            }
        }

        let chunk: Vec<u8> = Vec::with_capacity(1024);
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed mid-frame",
            ));
        }
        buf.extend_from_slice(&returned[..n]);
    }
}

async fn send_server_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    opcode: Opcode,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut masked = Vec::with_capacity(14 + payload.len());
    encode_frame(opcode, payload, [0; 4], &mut masked);
    let short = masked[1] & 0x7f;
    let ext_bytes = match short {
        0..=125 => 0,
        126 => 2,
        127 => 8,
        _ => unreachable!(),
    };
    let header_total = 2 + ext_bytes;
    let mut out = Vec::with_capacity(header_total + payload.len());
    out.extend_from_slice(&masked[..header_total]);
    out[1] &= 0x7f;
    out.extend_from_slice(&masked[header_total + 4..]);
    stream.write_all(out).await.0?;
    // TLS writes need an explicit flush; plain TCP treats it as a
    // no-op. Keep behavioural parity with the client side.
    stream.flush().await
}

async fn echo_handler<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S) {
    let mut req_buf = Vec::with_capacity(1024);
    let headers_end = match read_request(&mut stream, &mut req_buf).await {
        Ok(pos) => pos,
        Err(_) => return,
    };
    let key = match extract_ws_key(&req_buf[..headers_end]) {
        Some(k) => k,
        None => return,
    };
    let accept = compute_accept(&key);
    let resp = build_101(&accept);
    if stream.write_all(resp).await.0.is_err() {
        return;
    }
    // TLS record-layer flush. No-op on plain TCP.
    if stream.flush().await.is_err() {
        return;
    }

    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    if req_buf.len() > headers_end {
        recv_buf.extend_from_slice(&req_buf[headers_end..]);
    }

    loop {
        let (opcode, payload, _fin) = match recv_frame(&mut stream, &mut recv_buf).await {
            Ok(f) => f,
            Err(_) => return,
        };

        match opcode {
            Opcode::Text | Opcode::Binary => {
                if send_server_frame(&mut stream, opcode, &payload).await.is_err() {
                    return;
                }
            }
            Opcode::Ping => {
                let _ = send_server_frame(&mut stream, Opcode::Pong, &payload).await;
            }
            Opcode::Pong => {}
            Opcode::Close => {
                let _ = send_server_frame(&mut stream, Opcode::Close, &payload).await;
                return;
            }
            Opcode::Continuation => return,
        }
    }
}

// ---------------------------------------------------------------------------
// TLS server config
// ---------------------------------------------------------------------------

fn make_server_config() -> Arc<ServerConfig> {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("self-signed cert");
    let cert_der: CertificateDer<'static> = cert.into();
    let key_der: PrivatePkcs8KeyDer<'static> =
        PrivatePkcs8KeyDer::from(key_pair.serialize_der());

    let _ = rustls::crypto::ring::default_provider().install_default();

    // No ALPN for WS — the Upgrade happens above TLS.
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))
        .expect("server config");
    Arc::new(config)
}

fn spawn_tls_echo_server() -> SocketAddr {
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
                    let tls: TlsStream<TcpStream> = match acceptor.accept(socket).await {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    echo_handler(tls).await;
                })
                .detach();
            }
        });
    });

    ready_rx.recv().expect("tls server never bound");
    addr
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn wss_plan(addr: SocketAddr, insecure: bool) -> WsPlan {
    let mut target = Target::parse(&format!("wss://{addr}")).unwrap();
    // Our self-signed cert only covers `localhost` / `127.0.0.1`; set
    // SNI explicitly to `localhost` so strict verification (when used)
    // can at least pass the name match.
    target.sni = Some("localhost".into());

    WsPlan {
        target,
        path: "/echo".to_string(),
        headers: Vec::new(),
        message: Bytes::from_static(b"ping-tls"),
        opts: TransportOpts {
            insecure_tls: insecure,
            connect_timeout: Duration::from_secs(5),
            ..TransportOpts::default()
        },
    }
}

#[compio::test]
async fn wss_insecure_round_trips() {
    let addr = spawn_tls_echo_server();
    let plan = wss_plan(addr, true);

    let stop = StopSignal::after(Duration::from_millis(500));
    let stats = run_ws_saturate(plan, 1, stop, None).await;
    let summary = WsSummary::merge(stats, Duration::from_millis(500));

    assert!(
        summary.messages_sent > 5,
        "expected round-trips over TLS, got {}",
        summary.messages_sent
    );
    assert_eq!(summary.messages_sent, summary.messages_recvd);
    assert_eq!(summary.errors_connect, 0);
    assert_eq!(summary.errors_upgrade, 0);
    assert_eq!(summary.errors_io, 0);
}

#[compio::test]
async fn wss_strict_verification_rejects_self_signed() {
    let addr = spawn_tls_echo_server();
    let plan = wss_plan(addr, false);

    let stop = StopSignal::after(Duration::from_millis(500));
    let stats = run_ws_saturate(plan, 1, stop, None).await;
    let summary = WsSummary::merge(stats, Duration::from_millis(500));

    // Handshake must fail — no round-trips. The error gets classified
    // as "connect error" by the runner (TLS failures bubble through
    // `WsError::Tls`, which `classify_open_error` logs under
    // `errors_connect`).
    assert_eq!(summary.messages_sent, 0);
    assert_eq!(summary.messages_recvd, 0);
    assert!(
        summary.errors_connect > 0,
        "expected at least one TLS connect error, got {:?}",
        summary
    );
}
