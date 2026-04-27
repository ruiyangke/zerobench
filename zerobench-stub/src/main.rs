//! `zerobench-stub` -- ultra-fast stub server for benchmarking every
//! protocol zerobench supports.
//!
//! Built on raw `httparse` + compio (no hyper on the server side) for
//! maximum throughput and zero body-type / upgrade-lifecycle complexity.
//! Multi-worker via OS threads + SO_REUSEPORT.
//!
//! Protocols served on a single port with path-based routing:
//!
//! | Path                      | Protocol  | Behaviour                            |
//! |---------------------------|-----------|--------------------------------------|
//! | `GET /`                   | HTTP      | 200 OK, body `"pong"` (4 bytes)      |
//! | `GET /health`             | HTTP      | 200 OK, body `{"status":"ok"}`       |
//! | `POST /echo`              | HTTP      | 200 OK, echoes request body          |
//! | `GET /delay/:ms`          | HTTP      | 200 OK after sleeping `:ms` ms       |
//! | `GET /status/:code`       | HTTP      | Responds with the given status code  |
//! | `GET /bytes/:n`           | HTTP      | 200 OK, `n` bytes of `0x42`          |
//! | `GET /sse?chunks=&delay_ms=&size=` | SSE | `text/event-stream` N events   |
//! | `GET /ws` (Upgrade)       | WebSocket | RFC 6455 echo server                 |
//!
//! TLS (`--tls`): generates a self-signed cert via `rcgen`, serves HTTPS
//! with ALPN `h2, http/1.1`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::TcpListener as CompioTcpListener;
use compio::net::TcpStream;
use compio_tls::{TlsAcceptor, TlsStream};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use sha1::{Digest, Sha1};

// ---------------------------------------------------------------------------
// CLI argument parsing (no clap -- keep the binary tiny)
// ---------------------------------------------------------------------------

struct Args {
    port: u16,
    workers: usize,
    tls: bool,
    verbose: bool,
}

fn parse_args() -> Args {
    let mut port: u16 = 8080;
    let mut workers: usize = 1;
    let mut tls = false;
    let mut verbose = false;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-p" | "--port" => {
                i += 1;
                port = args[i].parse().expect("invalid --port value");
            }
            "-w" | "--workers" => {
                i += 1;
                workers = args[i].parse().expect("invalid --workers value");
            }
            "--tls" => tls = true,
            "-v" | "--verbose" => verbose = true,
            "-h" | "--help" => {
                eprintln!(
                    "zerobench-stub [OPTIONS]\n\n\
                     Options:\n  \
                       -p, --port <PORT>     Listen port [default: 8080]\n  \
                       -w, --workers <N>     Worker threads [default: 1]\n  \
                       --tls                 Enable TLS with self-signed cert\n  \
                       -v, --verbose         Log requests to stderr\n  \
                       -h, --help            Show this help"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("[zerobench-stub] unknown flag: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    Args {
        port,
        workers,
        tls,
        verbose,
    }
}

// ---------------------------------------------------------------------------
// TLS setup
// ---------------------------------------------------------------------------

fn generate_tls_config() -> Arc<ServerConfig> {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("self-signed cert");

    let cert_der: CertificateDer<'static> = cert.into();
    let key_der: PrivatePkcs8KeyDer<'static> = PrivatePkcs8KeyDer::from(key_pair.serialize_der());

    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))
        .expect("server config");
    // Only advertise HTTP/1.1 — the stub uses raw httparse, not hyper,
    // so it cannot serve HTTP/2 frames. Clients that negotiate h2 via
    // ALPN would get garbage. Advertising only http/1.1 forces even
    // h2-capable clients to fall back.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(config)
}

// ---------------------------------------------------------------------------
// SO_REUSEPORT listener
// ---------------------------------------------------------------------------

fn create_reuseport_listener(port: u16) -> std::net::TcpListener {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
    socket.set_reuse_port(true).unwrap();
    socket.set_reuse_address(true).unwrap();
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
    socket.bind(&addr.into()).unwrap();
    socket.listen(1024).unwrap();
    socket.set_nonblocking(true).unwrap();
    socket.into()
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let args = parse_args();

    let tls_config = if args.tls {
        Some(generate_tls_config())
    } else {
        None
    };

    // Print banner
    if args.tls {
        eprintln!("[zerobench-stub] TLS enabled (self-signed, ALPN: http/1.1)");
    }
    eprintln!(
        "[zerobench-stub] {} worker{} on port {}",
        args.workers,
        if args.workers == 1 { "" } else { "s" },
        args.port
    );
    eprintln!(
        "[zerobench-stub] routes:\n  \
         GET  /                    -> 200 \"pong\"\n  \
         GET  /health              -> 200 {{\"status\":\"ok\"}}\n  \
         POST /echo                -> 200 (echo body)\n  \
         GET  /delay/:ms           -> 200 after :ms delay\n  \
         GET  /status/:code        -> :code response\n  \
         GET  /bytes/:n            -> 200 with n bytes\n  \
         GET  /sse?chunks=N&delay_ms=M -> SSE stream\n  \
         GET  /ws (Upgrade)        -> WebSocket echo"
    );
    let scheme = if args.tls { "https" } else { "http" };
    eprintln!("[zerobench-stub] {scheme}://0.0.0.0:{}", args.port);

    let verbose = args.verbose;

    if args.workers <= 1 {
        run_worker(args.port, false, tls_config, verbose);
    } else {
        let handles: Vec<_> = (0..args.workers)
            .map(|_| {
                let tls = tls_config.clone();
                std::thread::spawn(move || {
                    run_worker(args.port, true, tls, verbose);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}

fn run_worker(port: u16, use_reuseport: bool, tls: Option<Arc<ServerConfig>>, verbose: bool) {
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let listener = if use_reuseport {
            let std_listener = create_reuseport_listener(port);
            CompioTcpListener::from_std(std_listener).unwrap()
        } else {
            CompioTcpListener::bind(format!("0.0.0.0:{port}"))
                .await
                .unwrap()
        };

        let acceptor = tls.map(TlsAcceptor::from);

        loop {
            let (stream, _addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => continue,
            };

            let acceptor = acceptor.clone();
            compio::runtime::spawn(async move {
                match acceptor {
                    Some(acc) => {
                        let tls_stream: TlsStream<TcpStream> = match acc.accept(stream).await {
                            Ok(s) => s,
                            Err(_) => return,
                        };
                        handle_connection(tls_stream, verbose).await;
                    }
                    None => {
                        handle_connection(stream, verbose).await;
                    }
                }
            })
            .detach();
        }
    });
}

// ---------------------------------------------------------------------------
// Connection handler -- generic over plain TCP and TLS streams
// ---------------------------------------------------------------------------

async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S, verbose: bool) {
    let mut data = Vec::with_capacity(8192);

    loop {
        // Read bytes into data until we have a full HTTP request header.
        let headers_end = match read_until_headers(&mut stream, &mut data).await {
            Some(end) => end,
            None => return, // connection closed or error
        };

        // Parse the request headers with httparse.
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        let status = match req.parse(&data[..headers_end]) {
            Ok(s) => s,
            Err(_) => {
                let _ = write_response(&mut stream, 400, &[], b"bad request").await;
                return;
            }
        };
        if status.is_partial() {
            let _ = write_response(&mut stream, 400, &[], b"bad request").await;
            return;
        }

        let method = req.method.unwrap_or("GET");
        let path = req.path.unwrap_or("/");
        let path_owned = path.to_string();
        let method_owned = method.to_string();

        if verbose {
            eprintln!("[stub] {method} {path}");
        }

        // Determine Content-Length for POST body.
        let content_length = find_content_length(req.headers);

        // Check for WebSocket upgrade.
        if is_ws_upgrade(req.headers) {
            // Read any trailing body bytes already in `data` past headers.
            let leftover = data[headers_end..].to_vec();
            handle_ws_upgrade(&mut stream, req.headers, leftover).await;
            return; // WS takes over the connection
        }

        // For SSE: handle specially (takes over the connection for streaming).
        if method_owned == "GET" && path_owned.starts_with("/sse") {
            data.drain(..headers_end);
            handle_sse_stream(&mut stream, &path_owned).await;
            return;
        }

        // Read request body if Content-Length > 0.
        let body = if content_length > 0 {
            // We may already have part of the body after the headers.
            let already_read = data.len() - headers_end;
            let remaining = content_length.saturating_sub(already_read);
            if remaining > 0
                && read_exact_into(&mut stream, &mut data, remaining)
                    .await
                    .is_none()
            {
                return;
            }
            data[headers_end..headers_end + content_length].to_vec()
        } else {
            Vec::new()
        };

        // Route and respond.
        let responded = match (method_owned.as_str(), path_owned.as_str()) {
            ("GET", "/") => {
                write_response(&mut stream, 200, &[("Content-Type", "text/plain")], b"pong").await
            }
            ("GET", "/health") => {
                write_response(
                    &mut stream,
                    200,
                    &[("Content-Type", "application/json")],
                    b"{\"status\":\"ok\"}",
                )
                .await
            }
            ("POST", "/echo") => {
                write_response(
                    &mut stream,
                    200,
                    &[("Content-Type", "application/octet-stream")],
                    &body,
                )
                .await
            }
            ("GET", p) if p.starts_with("/delay/") => {
                let ms: u64 = p[7..].parse().unwrap_or(0);
                compio::time::sleep(Duration::from_millis(ms)).await;
                write_response(&mut stream, 200, &[("Content-Type", "text/plain")], b"ok").await
            }
            ("GET", p) if p.starts_with("/status/") => {
                let code: u16 = p[8..].parse().unwrap_or(200);
                write_response(&mut stream, code, &[], b"").await
            }
            ("GET", p) if p.starts_with("/bytes/") => {
                let n: usize = p[7..].parse().unwrap_or(0).min(10_000_000);
                let payload = vec![0x42u8; n];
                write_response(
                    &mut stream,
                    200,
                    &[("Content-Type", "application/octet-stream")],
                    &payload,
                )
                .await
            }
            _ => write_response(&mut stream, 404, &[], b"not found").await,
        };

        if responded.is_err() {
            return;
        }

        // Keep-alive: clear consumed data and loop for next request.
        data.clear();
    }
}

// ---------------------------------------------------------------------------
// HTTP response writer
// ---------------------------------------------------------------------------

async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<()> {
    let reason = status_reason(status);
    let mut out = Vec::with_capacity(256 + body.len());
    out.extend_from_slice(
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\n",
            body.len()
        )
        .as_bytes(),
    );
    for (name, value) in headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);

    stream.write_all(out).await.0?;
    stream.flush().await
}

fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------------------
// IO helpers
// ---------------------------------------------------------------------------

/// Read from `stream` into `data` until `\r\n\r\n` is found. Returns the
/// byte offset just past the header terminator, or `None` on EOF / error.
async fn read_until_headers<S: AsyncRead + Unpin>(
    stream: &mut S,
    data: &mut Vec<u8>,
) -> Option<usize> {
    loop {
        if let Some(pos) = find_headers_end(data) {
            return Some(pos);
        }
        let chunk: Vec<u8> = Vec::with_capacity(8192);
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = match res {
            Ok(n) => n,
            Err(_) => return None,
        };
        if n == 0 {
            return None;
        }
        data.extend_from_slice(&returned[..n]);
        if data.len() > 64 * 1024 {
            return None; // headers too large
        }
    }
}

/// Read exactly `count` more bytes from `stream`, appending to `data`.
async fn read_exact_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    data: &mut Vec<u8>,
    mut count: usize,
) -> Option<()> {
    while count > 0 {
        let chunk: Vec<u8> = Vec::with_capacity(count.min(65536));
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = match res {
            Ok(n) => n,
            Err(_) => return None,
        };
        if n == 0 {
            return None;
        }
        data.extend_from_slice(&returned[..n]);
        count = count.saturating_sub(n);
    }
    Some(())
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn find_content_length(headers: &[httparse::Header<'_>]) -> usize {
    for h in headers {
        if h.name.eq_ignore_ascii_case("content-length") {
            if let Ok(s) = std::str::from_utf8(h.value) {
                return s.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

fn find_header<'a>(headers: &[httparse::Header<'a>], name: &str) -> Option<&'a [u8]> {
    for h in headers {
        if h.name.eq_ignore_ascii_case(name) {
            return Some(h.value);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

/// RFC 6455 GUID for computing Sec-WebSocket-Accept.
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

fn is_ws_upgrade(headers: &[httparse::Header<'_>]) -> bool {
    let mut has_upgrade = false;
    let mut has_connection = false;
    for h in headers {
        if h.name.eq_ignore_ascii_case("upgrade") {
            let v = std::str::from_utf8(h.value).unwrap_or("");
            if v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("websocket"))
            {
                has_upgrade = true;
            }
        } else if h.name.eq_ignore_ascii_case("connection") {
            let v = std::str::from_utf8(h.value).unwrap_or("");
            if v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
            {
                has_connection = true;
            }
        }
    }
    has_upgrade && has_connection
}

fn compute_ws_accept(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.trim().as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let digest = hasher.finalize();
    B64.encode(digest)
}

async fn handle_ws_upgrade<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    headers: &[httparse::Header<'_>],
    leftover: Vec<u8>,
) {
    let key = match find_header(headers, "sec-websocket-key") {
        Some(v) => match std::str::from_utf8(v) {
            Ok(s) => s.to_string(),
            Err(_) => return,
        },
        None => return,
    };

    let accept = compute_ws_accept(&key);

    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\
         \r\n"
    );

    if stream.write_all(response.into_bytes()).await.0.is_err() {
        return;
    }
    if stream.flush().await.is_err() {
        return;
    }

    // Start WebSocket echo loop with any leftover bytes.
    ws_echo_loop(stream, leftover).await;
}

/// WebSocket echo loop: read masked client frames, echo unmasked.
async fn ws_echo_loop<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, mut buf: Vec<u8>) {
    loop {
        // Try to decode a frame from the buffer.
        match try_decode_client_frame(&buf) {
            FrameResult::Complete {
                opcode,
                payload,
                consumed,
            } => {
                buf.drain(..consumed);

                match opcode {
                    0x1 | 0x2 => {
                        // Text or Binary -- echo back unmasked.
                        if send_server_frame(stream, opcode, &payload).await.is_err() {
                            return;
                        }
                    }
                    0x8 => {
                        // Close -- echo the close frame and exit.
                        let _ = send_server_frame(stream, 0x8, &payload).await;
                        return;
                    }
                    0x9 => {
                        // Ping -- respond with Pong.
                        if send_server_frame(stream, 0xA, &payload).await.is_err() {
                            return;
                        }
                    }
                    0xA => {}    // Pong -- ignore.
                    _ => return, // Unknown opcode.
                }
            }
            FrameResult::NeedMore => {
                // Read more data from the socket.
                let chunk: Vec<u8> = Vec::with_capacity(65536);
                let BufResult(res, returned) = stream.read(chunk).await;
                let n = match res {
                    Ok(n) => n,
                    Err(_) => return,
                };
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&returned[..n]);
            }
            FrameResult::Error => return,
        }
    }
}

enum FrameResult {
    Complete {
        opcode: u8,
        payload: Vec<u8>,
        consumed: usize,
    },
    NeedMore,
    Error,
}

/// Decode a single masked client frame from `buf`.
fn try_decode_client_frame(buf: &[u8]) -> FrameResult {
    if buf.len() < 2 {
        return FrameResult::NeedMore;
    }

    let b0 = buf[0];
    let b1 = buf[1];
    let opcode = b0 & 0x0F;
    let masked = (b1 & 0x80) != 0;

    if !masked {
        return FrameResult::Error; // Client frames MUST be masked.
    }

    let short_len = (b1 & 0x7F) as u64;
    let (payload_len, header_len): (usize, usize) = match short_len {
        0..=125 => (short_len as usize, 2),
        126 => {
            if buf.len() < 4 {
                return FrameResult::NeedMore;
            }
            (u16::from_be_bytes([buf[2], buf[3]]) as usize, 4)
        }
        127 => {
            if buf.len() < 10 {
                return FrameResult::NeedMore;
            }
            let n = u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]);
            if n > 64 * 1024 * 1024 {
                return FrameResult::Error;
            }
            (n as usize, 10)
        }
        _ => unreachable!(),
    };

    let mask_start = header_len;
    let frame_end = mask_start + 4 + payload_len;
    if buf.len() < frame_end {
        return FrameResult::NeedMore;
    }

    let mask = [
        buf[mask_start],
        buf[mask_start + 1],
        buf[mask_start + 2],
        buf[mask_start + 3],
    ];
    let payload_start = mask_start + 4;
    let mut payload = buf[payload_start..frame_end].to_vec();
    for (i, b) in payload.iter_mut().enumerate() {
        *b ^= mask[i & 3];
    }

    FrameResult::Complete {
        opcode,
        payload,
        consumed: frame_end,
    }
}

/// Send an unmasked server frame.
async fn send_server_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    opcode: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let len = payload.len();
    let mut out = Vec::with_capacity(10 + len);

    // FIN=1, RSV=0, opcode.
    out.push(0x80 | opcode);

    // Length (MASK=0 for server frames).
    if len < 126 {
        out.push(len as u8);
    } else if len < 65536 {
        out.push(126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }

    out.extend_from_slice(payload);

    stream.write_all(out).await.0?;
    stream.flush().await
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

async fn handle_sse_stream<S: AsyncWrite + Unpin>(stream: &mut S, path: &str) {
    let query = path.split('?').nth(1).unwrap_or("");
    let chunks: usize = parse_query_param(query, "chunks", 10);
    let delay_ms: u64 = parse_query_param(query, "delay_ms", 100);
    let size: usize = parse_query_param(query, "size", 50);

    // Send response headers (chunked-style: no Content-Length).
    let headers = "HTTP/1.1 200 OK\r\n\
                   Content-Type: text/event-stream\r\n\
                   Cache-Control: no-cache\r\n\
                   Connection: keep-alive\r\n\
                   \r\n";
    if stream
        .write_all(headers.as_bytes().to_vec())
        .await
        .0
        .is_err()
    {
        return;
    }
    if stream.flush().await.is_err() {
        return;
    }

    // Stream chunks.
    let padding = "x".repeat(size);
    for i in 0..chunks {
        if delay_ms > 0 {
            compio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let payload = format!("data: chunk-{i}-{padding}\n\n");
        if stream.write_all(payload.into_bytes()).await.0.is_err() {
            return;
        }
        if stream.flush().await.is_err() {
            return;
        }
    }

    // Done sentinel.
    if stream
        .write_all(b"data: [DONE]\n\n".to_vec())
        .await
        .0
        .is_err()
    {
        return;
    }
    let _ = stream.flush().await;
}

fn parse_query_param<T: std::str::FromStr>(query: &str, key: &str, default: T) -> T {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return v.parse().unwrap_or(default);
            }
        }
    }
    default
}
