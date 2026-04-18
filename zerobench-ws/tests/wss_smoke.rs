//! End-to-end TLS smoke test for the WebSocket runner (mio, zero async).
//!
//! Spins up a rcgen-self-signed `wss://` echo server using raw
//! `std::net::TcpListener` + rustls `StreamOwned`, then runs
//! `run_ws_threaded` against it with `insecure_tls = true`. Verifies
//! that the handshake + frame exchange work over TLS, and that strict
//! verification rejects the self-signed cert.

use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;

use zerobench_core::transport::{Target, TransportOpts};
use zerobench_http::mio_tls::build_tls_config;
use zerobench_ws::frame::{encode_frame, Opcode};
use zerobench_ws::handshake::{compute_accept, find_headers_end};
use zerobench_ws::{run_ws_threaded, WsPlan, WsSummary};

// ---------------------------------------------------------------------------
// Echo server — TLS-wrapped using rustls::StreamOwned for simplicity.
//
// rustls::StreamOwned wraps a blocking TcpStream and handles the TLS
// state machine transparently on Read/Write. No deadlock risk since
// the TcpStream is blocking and StreamOwned drives write_tls/read_tls
// internally.
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

fn read_request(stream: &mut impl Read, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    loop {
        if let Some(end) = find_headers_end(buf) {
            return Ok(end);
        }
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before full request",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request too big",
            ));
        }
    }
}

fn recv_frame(
    stream: &mut impl Read,
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
                    buf.drain(..frame_end);
                    return Ok((opcode, payload, fin));
                }
            }
        }

        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed mid-frame",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn send_server_frame(
    stream: &mut impl Write,
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
    stream.write_all(&out)?;
    stream.flush()
}

fn echo_handler(mut stream: impl Read + Write) {
    let mut req_buf = Vec::with_capacity(1024);
    let headers_end = match read_request(&mut stream, &mut req_buf) {
        Ok(pos) => pos,
        Err(_) => return,
    };
    let key = match extract_ws_key(&req_buf[..headers_end]) {
        Some(k) => k,
        None => return,
    };
    let accept = compute_accept(&key);
    let resp = build_101(&accept);
    if stream.write_all(&resp).is_err() {
        return;
    }
    let _ = stream.flush();

    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    if req_buf.len() > headers_end {
        recv_buf.extend_from_slice(&req_buf[headers_end..]);
    }

    loop {
        let (opcode, payload, _fin) = match recv_frame(&mut stream, &mut recv_buf) {
            Ok(f) => f,
            Err(_) => return,
        };

        match opcode {
            Opcode::Text | Opcode::Binary => {
                if send_server_frame(&mut stream, opcode, &payload).is_err() {
                    return;
                }
            }
            Opcode::Ping => {
                let _ = send_server_frame(&mut stream, Opcode::Pong, &payload);
            }
            Opcode::Pong => {}
            Opcode::Close => {
                let _ = send_server_frame(&mut stream, Opcode::Close, &payload);
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

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))
        .expect("server config");
    Arc::new(config)
}

fn spawn_tls_echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = make_server_config();

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(tcp) => {
                    let config = config.clone();
                    thread::spawn(move || {
                        // Use rustls::StreamOwned which wraps a blocking
                        // TcpStream and handles the TLS state machine
                        // transparently. The handshake happens
                        // automatically on first read/write.
                        let server_conn = match rustls::ServerConnection::new(config) {
                            Ok(c) => c,
                            Err(_) => return,
                        };
                        let tls_stream = rustls::StreamOwned::new(server_conn, tcp);
                        echo_handler(tls_stream);
                    });
                }
                Err(_) => break,
            }
        }
    });

    thread::sleep(Duration::from_millis(10));
    addr
}

/// Create a stop flag that trips after the given duration.
fn stop_after(d: Duration) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let f = flag.clone();
    thread::spawn(move || {
        thread::sleep(d);
        f.store(true, Ordering::Relaxed);
    });
    flag
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn wss_plan(addr: SocketAddr, insecure: bool) -> WsPlan {
    let mut target = Target::parse(&format!("wss://{addr}")).unwrap();
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

#[test]
fn wss_insecure_round_trips() {
    let addr = spawn_tls_echo_server();
    let plan = wss_plan(addr, true);
    let tls_config = Some(build_tls_config(&plan.opts, &[]));

    let stop = stop_after(Duration::from_millis(500));
    let stats = run_ws_threaded(plan, 1, stop, None, tls_config);
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

#[test]
fn wss_strict_verification_rejects_self_signed() {
    let addr = spawn_tls_echo_server();
    let plan = wss_plan(addr, false);
    let tls_config = Some(build_tls_config(&plan.opts, &[]));

    let stop = stop_after(Duration::from_millis(500));
    let stats = run_ws_threaded(plan, 1, stop, None, tls_config);
    let summary = WsSummary::merge(stats, Duration::from_millis(500));

    assert_eq!(summary.messages_sent, 0);
    assert_eq!(summary.messages_recvd, 0);
    assert!(
        summary.errors_connect > 0,
        "expected at least one TLS connect error, got {:?}",
        summary
    );
}
