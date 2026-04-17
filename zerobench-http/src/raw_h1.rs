//! Raw HTTP/1.1 connection pool — no hyper, no channels.
//!
//! [`RawH1Pool`] opens `opts.max_conns` persistent TCP connections
//! up-front, each with a reusable read and write buffer. Requests are
//! serialised directly into the write buffer, sent via `write_all`, and
//! responses are parsed in-place with `httparse`. No hyper driver task,
//! no mpsc channels, no `BytesMut` / `HeaderMap` cloning — the only
//! allocations on the hot path are the compio I/O submission buffers
//! (kernel-owned, unavoidable on io_uring).
//!
//! # What this SKIPS vs hyper
//!
//! - Internal tokio mpsc channels
//! - Connection driver state machine
//! - Response body `BytesMut` + `Incoming` machinery
//! - `HeaderMap` construction + cloning per response
//! - Flush state machine (poll_flush)
//! - Body channel (Sender/Receiver)
//!
//! # What this LOSES
//!
//! - No HTTP/2 support (H1 only)
//! - No header validation (trusts the server)
//! - No chunked Transfer-Encoding (Content-Length bodies only)
//! - No trailer handling
//! - No automatic redirect following
//! - Simplified keep-alive (Connection: close only)
//! - `response.headers` is empty — extracts won't work on raw responses
//! - No TLS in v1 — errors cleanly on https:// targets

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use futures_util::lock::Mutex;

use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::ExpandCtx;
use zerobench_core::transport::{
    Response, ResponseBody, Target, TransportError, TransportOpts,
};

/// A single slot in the pool — one TCP stream with reusable buffers.
struct RawSlot {
    stream: Option<TcpStream>,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
}

/// Raw HTTP/1.1 connection pool — no hyper, no channels.
///
/// Each slot is a TCP stream with reusable read/write buffers.
/// Requests are serialised directly, responses parsed with httparse.
pub struct RawH1Pool {
    slots: Box<[Mutex<RawSlot>]>,
    target: Target,
    next_idx: AtomicUsize,
}

impl RawH1Pool {
    /// Open `opts.max_conns` connections up-front.
    ///
    /// Returns [`TransportError::Connect`] on the first failure.
    /// Rejects https:// targets — TLS is not supported in the raw
    /// transport's v1.
    pub async fn new(target: &Target, opts: &TransportOpts) -> Result<Self, TransportError> {
        if target.tls {
            return Err(TransportError::Protocol(
                "--raw does not support https:// targets (TLS not wired in raw H1 v1)".into(),
            ));
        }
        if opts.max_conns == 0 {
            return Err(TransportError::Connect("max_conns must be > 0".into()));
        }

        let addr = target.addr();
        let mut pending = Vec::with_capacity(opts.max_conns);
        for _ in 0..opts.max_conns {
            let addr = addr.clone();
            let timeout = opts.connect_timeout;
            let nodelay = opts.tcp_nodelay;
            pending.push(async move {
                let stream = compio::time::timeout(timeout, TcpStream::connect(&addr))
                    .await
                    .map_err(|_| TransportError::Timeout)?
                    .map_err(|e| TransportError::Connect(format!("{addr}: {e}")))?;
                if nodelay {
                    let _ = stream.set_nodelay(true);
                }
                Ok::<_, TransportError>(stream)
            });
        }

        let results = futures_util::future::join_all(pending).await;
        let mut slots = Vec::with_capacity(opts.max_conns);
        for res in results {
            let stream = res?;
            slots.push(Mutex::new(RawSlot {
                stream: Some(stream),
                read_buf: Vec::with_capacity(8192),
                write_buf: Vec::with_capacity(512),
            }));
        }

        Ok(Self {
            slots: slots.into_boxed_slice(),
            target: target.clone(),
            next_idx: AtomicUsize::new(0),
        })
    }

    /// Number of slots in the pool.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// `true` if the pool has zero slots.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Target this pool was opened against.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// Execute one HTTP/1.1 exchange through the pool.
    ///
    /// Acquires a slot round-robin, builds the request directly into
    /// the slot's write buffer, sends it, reads and parses the response
    /// with httparse, and returns a [`Response`] with empty headers
    /// (raw mode doesn't construct a `HeaderMap`).
    pub async fn exchange(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        let n = self.slots.len();
        let start = self.next_idx.fetch_add(1, Ordering::Relaxed);

        // Try-lock scan, fallback to blocking on first slot.
        let mut guard = {
            let mut found = None;
            for offset in 0..n {
                let idx = (start + offset) % n;
                if let Some(g) = self.slots[idx].try_lock() {
                    found = Some(g);
                    break;
                }
            }
            match found {
                Some(g) => g,
                None => self.slots[start % n].lock().await,
            }
        };

        let slot = &mut *guard;

        // Take the stream out of the slot. If any error occurs during
        // the exchange, the stream is dropped (not put back), which
        // invalidates the slot for future exchanges. On success, we
        // put it back (unless the server sent Connection: close).
        let mut stream = match slot.stream.take() {
            Some(s) => s,
            None => return Err(TransportError::Connect("slot dead".into())),
        };

        // === BUILD REQUEST ===
        slot.write_buf.clear();
        build_raw_request(plan, ctx, &self.target, &mut slot.write_buf)?;

        // === SEND ===
        let t0 = Instant::now();
        let req_bytes = std::mem::take(&mut slot.write_buf);
        let req_len = req_bytes.len() as u64;
        let BufResult(write_res, returned_buf) = stream.write_all(req_bytes).await;
        slot.write_buf = returned_buf;

        if let Err(e) = write_res {
            // Stream is already taken out — just return error.
            return Err(TransportError::Io(e));
        }

        // === READ RESPONSE ===
        slot.read_buf.clear();
        let (status, header_len, content_length, keep_alive) =
            match read_response_headers(&mut stream, &mut slot.read_buf).await {
                Ok(result) => result,
                Err(e) => return Err(e),
            };

        let ttfb = t0.elapsed();

        // Read remaining body if needed.
        let body_in_buf = slot.read_buf.len() - header_len;
        let remaining_body = content_length.saturating_sub(body_in_buf);
        if remaining_body > 0 {
            if let Err(e) = read_exact(&mut stream, &mut slot.read_buf, remaining_body).await {
                return Err(e);
            }
        }

        let total = t0.elapsed();
        let resp_len = slot.read_buf.len() as u64;

        // Put the stream back — unless the server sent Connection: close.
        if keep_alive {
            slot.stream = Some(stream);
        }

        Ok(Response {
            status,
            headers: http::HeaderMap::new(),
            body: ResponseBody::Buffered(Bytes::new()),
            bytes_sent: req_len,
            bytes_received: resp_len,
            ttfb,
            total,
        })
    }
}

impl std::fmt::Debug for RawH1Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawH1Pool")
            .field("slots", &self.slots.len())
            .field("target", &self.target)
            .finish()
    }
}

/// A [`Send`]-safe handle around `Arc<RawH1Pool>`.
///
/// compio's `TcpStream` is `!Send` (it uses `Rc` internally for
/// io_uring FD management), which makes `RawH1Pool` — and therefore
/// `Arc<RawH1Pool>` — also `!Send`. This prevents it from satisfying
/// the `Transport::Client: Clone + Send + 'static` bound.
///
/// However, the pool is strictly single-threaded at runtime: each
/// worker thread builds its own pool via `build_client` inside its
/// own compio runtime, and never moves it to another thread. The
/// `Send` bound on `Client` exists only so the dispatcher can
/// *move* a pre-built client into a `std::thread::spawn` closure,
/// but the multi-threaded path builds per-thread anyway.
///
/// # Safety
///
/// `RawH1Handle` is safe to send across threads because:
/// 1. In the multi-threaded path, each thread builds its own pool
///    inside its own compio runtime — no pool instance ever crosses
///    a thread boundary.
/// 2. In the single-threaded path, the pool is built and used on
///    the same thread. `Clone` produces a new `Arc` reference, but
///    the underlying pool is never accessed from multiple threads.
/// 3. The `Mutex<RawSlot>` serialises access within a single thread's
///    concurrent tasks, and `RawH1Pool` has no `&self` methods that
///    mutate shared state without going through a lock.
#[derive(Clone)]
pub struct RawH1Handle(pub std::sync::Arc<RawH1Pool>);

// SAFETY: See the doc comment on `RawH1Handle` above. The pool is
// only ever used on the thread that created it.
// Allow unsafe_code specifically for this send/sync impl — the rest
// of the crate stays `#[deny(unsafe_code)]`.
#[allow(unsafe_code)]
unsafe impl Send for RawH1Handle {}
#[allow(unsafe_code)]
unsafe impl Sync for RawH1Handle {}

impl std::fmt::Debug for RawH1Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl RawH1Handle {
    /// Delegate exchange to the inner pool.
    pub async fn exchange(
        &self,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        self.0.exchange(plan, ctx).await
    }
}

// ---------------------------------------------------------------------------
// Request building — zero-alloc into a reusable Vec<u8>
// ---------------------------------------------------------------------------

/// Build an HTTP/1.1 request directly into `out`.
///
/// Expands URL / header / body templates via `ctx`, strips the scheme
/// and authority from absolute URLs to produce origin-form, and writes
/// the full wire message (request line + headers + body) in a single
/// pass. No `http::Request` construction, no `HeaderMap`, no
/// `Full<Bytes>` — just raw bytes.
fn build_raw_request(
    plan: &RequestPlan,
    ctx: &mut ScenarioContext,
    target: &Target,
    out: &mut Vec<u8>,
) -> Result<(), TransportError> {
    // Build an ExpandCtx from individual fields so we can borrow
    // ctx.body_buf separately when needed.
    let mut ectx = ExpandCtx {
        rng: &mut ctx.rng,
        counter: &ctx.counter,
        scenario_vars: &ctx.vars,
    };

    // Method.
    out.extend_from_slice(plan.method.as_str().as_bytes());
    out.push(b' ');

    // URL — expand template, then extract origin-form (path+query).
    let url_start = out.len();
    plan.url.expand_into(out, &mut ectx);

    // If the expanded URL is absolute (starts with "http"), strip
    // scheme://authority to produce origin-form path+query.
    let url_bytes = &out[url_start..];
    if url_bytes.starts_with(b"http") {
        // Find the "://" then the next '/' after it.
        if let Some(scheme_end) = find_subsequence(url_bytes, b"://") {
            let after_scheme = scheme_end + 3;
            // Find the start of the path after the authority.
            let path_start = url_bytes[after_scheme..]
                .iter()
                .position(|&b| b == b'/' || b == b'?' || b == b'#')
                .map(|p| after_scheme + p);

            match path_start {
                Some(rel) => {
                    // Prepend '/' if authority is followed by '?' directly.
                    let needs_slash = url_bytes[rel] == b'?';
                    let path_portion: Vec<u8> = if needs_slash {
                        let mut v = vec![b'/'];
                        v.extend_from_slice(&url_bytes[rel..]);
                        v
                    } else {
                        url_bytes[rel..].to_vec()
                    };
                    // Strip fragment if present.
                    let without_frag = match path_portion.iter().position(|&b| b == b'#') {
                        Some(i) => &path_portion[..i],
                        None => &path_portion,
                    };
                    out.truncate(url_start);
                    if without_frag.is_empty() {
                        out.push(b'/');
                    } else {
                        out.extend_from_slice(without_frag);
                    }
                }
                None => {
                    // No path at all — just "http://host".
                    out.truncate(url_start);
                    out.push(b'/');
                }
            }
        }
    } else {
        // Relative URL — strip fragment if present.
        let url_slice = &out[url_start..];
        if let Some(frag_pos) = url_slice.iter().position(|&b| b == b'#') {
            let new_end = url_start + frag_pos;
            out.truncate(new_end);
        }
        if out.len() == url_start {
            out.push(b'/');
        }
    }

    // HTTP version.
    out.extend_from_slice(b" HTTP/1.1\r\n");

    // Host header.
    out.extend_from_slice(b"Host: ");
    out.extend_from_slice(target.addr().as_bytes());
    out.extend_from_slice(b"\r\n");

    // Connection: keep-alive.
    out.extend_from_slice(b"Connection: keep-alive\r\n");

    // User headers (expand templates).
    for (name_tpl, val_tpl) in &plan.headers {
        name_tpl.expand_into(out, &mut ectx);
        out.extend_from_slice(b": ");
        val_tpl.expand_into(out, &mut ectx);
        out.extend_from_slice(b"\r\n");
    }

    // Body.
    match &plan.body {
        None => {
            out.extend_from_slice(b"\r\n");
        }
        Some(BodySource::Static(body)) => {
            out.extend_from_slice(b"Content-Length: ");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(body.len()).as_bytes());
            out.extend_from_slice(b"\r\n\r\n");
            out.extend_from_slice(body);
        }
        Some(BodySource::Template(tpl)) => {
            // Expand body into ctx.body_buf, measure, then append.
            ctx.body_buf.clear();
            tpl.expand_into(&mut ctx.body_buf, &mut ectx);
            out.extend_from_slice(b"Content-Length: ");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(ctx.body_buf.len()).as_bytes());
            out.extend_from_slice(b"\r\n\r\n");
            out.extend_from_slice(&ctx.body_buf);
        }
    }

    Ok(())
}

/// Find the start index of `needle` in `haystack`.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// Response reading — minimal httparse parsing
// ---------------------------------------------------------------------------

/// Read until the header terminator `\r\n\r\n` is found, then parse
/// with httparse. Returns `(status, header_len, content_length, keep_alive)`.
///
/// `buf` is the slot's reusable read buffer — data is appended to it
/// across multiple read calls. On return, `buf` may contain body bytes
/// beyond `header_len`.
async fn read_response_headers(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Result<(u16, usize, usize, bool), TransportError> {
    loop {
        // compio's read takes ownership of the buffer and returns it.
        let chunk = Vec::with_capacity(4096);
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = res.map_err(TransportError::Io)?;
        if n == 0 {
            return Err(TransportError::Protocol("connection closed".into()));
        }
        buf.extend_from_slice(&returned[..n]);

        // Check for header end.
        if let Some(end_pos) = find_header_end(buf) {
            // Parse with httparse.
            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut resp = httparse::Response::new(&mut headers);
            match resp.parse(&buf[..end_pos]) {
                Ok(httparse::Status::Complete(header_len)) => {
                    let status = resp.code.unwrap_or(0);
                    let content_length = find_content_length_raw(resp.headers);
                    let keep_alive = !find_connection_close(resp.headers);
                    return Ok((status, header_len, content_length, keep_alive));
                }
                Ok(httparse::Status::Partial) => continue,
                Err(e) => return Err(TransportError::Protocol(format!("parse: {e}"))),
            }
        }

        if buf.len() > 64 * 1024 {
            return Err(TransportError::Protocol("headers too large".into()));
        }
    }
}

/// Scan for `\r\n\r\n` in the buffer. Returns the byte position just
/// past the terminator (i.e. the start of the body).
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
}

/// Extract the `Content-Length` value from raw httparse headers.
fn find_content_length_raw(headers: &[httparse::Header<'_>]) -> usize {
    for h in headers {
        if h.name.eq_ignore_ascii_case("content-length") {
            if let Ok(s) = std::str::from_utf8(h.value) {
                return s.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

/// Check whether the server sent `Connection: close`.
fn find_connection_close(headers: &[httparse::Header<'_>]) -> bool {
    for h in headers {
        if h.name.eq_ignore_ascii_case("connection") {
            if let Ok(s) = std::str::from_utf8(h.value) {
                return s.eq_ignore_ascii_case("close");
            }
        }
    }
    false
}

/// Read exactly `remaining` more bytes into `buf` from `stream`.
async fn read_exact(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    mut remaining: usize,
) -> Result<(), TransportError> {
    while remaining > 0 {
        let chunk = Vec::with_capacity(remaining.min(65536));
        let BufResult(res, returned) = stream.read(chunk).await;
        let n = res.map_err(TransportError::Io)?;
        if n == 0 {
            return Err(TransportError::Protocol("truncated body".into()));
        }
        buf.extend_from_slice(&returned[..n]);
        remaining = remaining.saturating_sub(n);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests — request builder and header parser
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_header_end_finds_crlfcrlf() {
        let buf = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let pos = find_header_end(buf).unwrap();
        assert_eq!(&buf[pos..], b"hello");
    }

    #[test]
    fn find_header_end_returns_none_when_incomplete() {
        let buf = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
        assert!(find_header_end(buf).is_none());
    }

    #[test]
    fn content_length_extraction() {
        let headers = [
            httparse::Header {
                name: "Content-Type",
                value: b"text/plain",
            },
            httparse::Header {
                name: "content-length",
                value: b"42",
            },
        ];
        assert_eq!(find_content_length_raw(&headers), 42);
    }

    #[test]
    fn content_length_missing_returns_zero() {
        let headers = [httparse::Header {
            name: "Content-Type",
            value: b"text/plain",
        }];
        assert_eq!(find_content_length_raw(&headers), 0);
    }

    #[test]
    fn connection_close_detected() {
        let headers = [httparse::Header {
            name: "Connection",
            value: b"close",
        }];
        assert!(find_connection_close(&headers));
    }

    #[test]
    fn connection_keepalive_not_close() {
        let headers = [httparse::Header {
            name: "Connection",
            value: b"keep-alive",
        }];
        assert!(!find_connection_close(&headers));
    }

    #[test]
    fn find_subsequence_works() {
        assert_eq!(find_subsequence(b"hello://world", b"://"), Some(5));
        assert_eq!(find_subsequence(b"nope", b"://"), None);
    }
}
