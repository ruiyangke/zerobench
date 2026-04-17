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

use zerobench_core::plan::RequestPlan;
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::transport::{
    Response, ResponseBody, Target, TransportError, TransportOpts,
};

use crate::raw_h1_common::{
    build_raw_request, find_content_length_raw, find_connection_close,
    find_header_end,
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

