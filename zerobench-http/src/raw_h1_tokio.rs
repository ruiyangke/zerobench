//! Raw HTTP/1.1 connection pool — tokio variant.
//!
//! Same zero-hyper approach as [`super::raw_h1`] but using
//! `tokio::net::TcpStream` with poll-based I/O instead of compio's
//! completion-based model.
//!
//! Key I/O difference: tokio's `AsyncReadExt::read` takes `&mut [u8]`,
//! so we read into a stack-allocated temp buffer and memcpy into the
//! slot's persistent `read_buf` — no per-read heap allocation.
//! The write path borrows `&slot.write_buf` directly (no take/return
//! ownership dance like compio).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

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
struct RawSlotTokio {
    stream: Option<TcpStream>,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
}

/// Raw HTTP/1.1 connection pool — tokio variant.
///
/// Same architecture as `RawH1Pool` (compio): pre-opened slots,
/// round-robin acquire, reusable buffers, httparse response parsing.
/// The only difference is the I/O layer: tokio's poll-based reads
/// with a stack temp buffer instead of compio's completion-based
/// reads with per-read Vec allocation.
pub struct RawH1PoolTokio {
    slots: Box<[Mutex<RawSlotTokio>]>,
    target: Target,
    next_idx: AtomicUsize,
}

impl RawH1PoolTokio {
    /// Open `opts.max_conns` connections up-front.
    ///
    /// Returns [`TransportError::Connect`] on the first failure.
    /// Rejects https:// targets — TLS is not supported in the raw
    /// transport.
    pub async fn new(target: &Target, opts: &TransportOpts) -> Result<Self, TransportError> {
        if target.tls {
            return Err(TransportError::Protocol(
                "--raw does not support https:// targets (TLS not wired in raw H1)".into(),
            ));
        }
        if opts.max_conns == 0 {
            return Err(TransportError::Connect("max_conns must be > 0".into()));
        }

        let addr = target.addr().to_string();
        let mut pending = Vec::with_capacity(opts.max_conns);
        for _ in 0..opts.max_conns {
            let addr = addr.clone();
            let timeout = opts.connect_timeout;
            let nodelay = opts.tcp_nodelay;
            pending.push(async move {
                let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr))
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
            slots.push(Mutex::new(RawSlotTokio {
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
    /// Same logic as `RawH1Pool::exchange` — acquire slot round-robin,
    /// build request, send, read/parse response, drain body. Only the
    /// I/O calls differ (tokio borrow-based vs compio ownership-based).
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
                if let Ok(g) = self.slots[idx].try_lock() {
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

        // Take the stream out. On error it's dropped (slot dies).
        // On success we put it back (unless Connection: close).
        let mut stream = match slot.stream.take() {
            Some(s) => s,
            None => return Err(TransportError::Connect("slot dead".into())),
        };

        // === BUILD REQUEST ===
        slot.write_buf.clear();
        build_raw_request(plan, ctx, &self.target, &mut slot.write_buf)?;

        // === SEND ===
        let t0 = Instant::now();
        let req_len = slot.write_buf.len() as u64;
        // tokio borrows the buffer — no take/return dance.
        stream
            .write_all(&slot.write_buf)
            .await
            .map_err(TransportError::Io)?;

        // === READ RESPONSE ===
        slot.read_buf.clear();
        let (status, header_len, content_length, keep_alive) =
            read_response_headers_tokio(&mut stream, &mut slot.read_buf).await?;

        let ttfb = t0.elapsed();

        // Read remaining body if needed.
        let body_in_buf = slot.read_buf.len() - header_len;
        let remaining_body = content_length.saturating_sub(body_in_buf);
        if remaining_body > 0 {
            read_exact_tokio(&mut stream, &mut slot.read_buf, remaining_body).await?;
        }

        let total = t0.elapsed();
        let resp_len = slot.read_buf.len() as u64;

        // Put the stream back — unless Connection: close.
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

impl std::fmt::Debug for RawH1PoolTokio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawH1PoolTokio")
            .field("slots", &self.slots.len())
            .field("target", &self.target)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Response reading — tokio I/O with stack temp buffer
// ---------------------------------------------------------------------------

/// Read until `\r\n\r\n`, then parse with httparse.
/// Uses a stack-allocated 4 KiB temp buffer — no per-read heap alloc.
async fn read_response_headers_tokio(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Result<(u16, usize, usize, bool), TransportError> {
    loop {
        let mut tmp = [0u8; 4096];
        let n = stream
            .read(&mut tmp)
            .await
            .map_err(TransportError::Io)?;
        if n == 0 {
            return Err(TransportError::Protocol("connection closed".into()));
        }
        buf.extend_from_slice(&tmp[..n]);

        // Check for header end.
        if let Some(end_pos) = find_header_end(buf) {
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
/// Uses a stack-allocated temp buffer, copies via memcpy.
async fn read_exact_tokio(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    mut remaining: usize,
) -> Result<(), TransportError> {
    while remaining > 0 {
        let cap = remaining.min(65536);
        // Extend buf with zeroed space, read into it directly.
        let offset = buf.len();
        buf.resize(offset + cap, 0);
        let n = stream
            .read(&mut buf[offset..])
            .await
            .map_err(TransportError::Io)?;
        if n == 0 {
            return Err(TransportError::Protocol("truncated body".into()));
        }
        buf.truncate(offset + n);
        remaining = remaining.saturating_sub(n);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Transport impl
// ---------------------------------------------------------------------------

/// Zero-sized type that carries the [`Transport`] impl for the raw
/// HTTP/1.1 client using tokio (httparse + tokio, no hyper).
pub struct RawH1TransportTokio;

impl zerobench_core::transport::Transport for RawH1TransportTokio {
    type Client = Arc<RawH1PoolTokio>;

    async fn build_client(
        target: &Target,
        opts: &TransportOpts,
    ) -> Result<Self::Client, TransportError> {
        let pool = RawH1PoolTokio::new(target, opts).await?;
        Ok(Arc::new(pool))
    }

    async fn exchange(
        client: &Self::Client,
        plan: &RequestPlan,
        ctx: &mut ScenarioContext,
    ) -> Result<Response, TransportError> {
        client.exchange(plan, ctx).await
    }
}
