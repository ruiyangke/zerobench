//! Byte-counting IO wrapper.
//!
//! [`CountingStream`] wraps any type that implements compio's
//! [`AsyncRead`](compio::io::AsyncRead) and
//! [`AsyncWrite`](compio::io::AsyncWrite) traits and increments two
//! atomic counters whenever bytes flow through. It lives **below** TLS:
//! a typical stack is
//!
//! ```text
//!     hyper::client::conn::http1
//!         └── HyperStream            (compio ↔ hyper IO bridge)
//!             └── TlsStream          (compio-tls, when the target is HTTPS)
//!                 └── CountingStream ← the counter lives here
//!                     └── TcpStream  (compio-net)
//! ```
//!
//! so we count **on-wire** bytes (encrypted, post-TLS) which is the
//! right number to report in "Transfer/sec" — matches what wrk / `ss -ti`
//! / `tcpdump` would show.
//!
//! # Ordering
//!
//! Counters use [`Ordering::Relaxed`]. The benchmark reporter reads the
//! counters at end-of-run (or between rate windows); there's no
//! happens-before edge to establish with prior IO on a different
//! thread. On x86/ARM, Relaxed compiles to plain loads/stores.
//!
//! # Shared state
//!
//! Counters are `Arc<AtomicU64>` so the pool can read them from a
//! different task than the one holding the stream (e.g. to compute
//! per-request deltas by snapshotting before/after the send_request).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use compio::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut};
use compio::io::{AsyncRead, AsyncWrite};

/// A `Read + Write` stream wrapper that counts bytes in both directions.
///
/// The two counters are exposed as `Arc<AtomicU64>` via [`Self::counts`]
/// so a caller that owns the stream can hand the snapshot handles to
/// another task (e.g. the HTTP/1 pool's per-slot accounting).
pub struct CountingStream<S> {
    inner: S,
    bytes_read: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
}

impl<S> CountingStream<S> {
    /// Wrap `inner`, starting both counters at zero.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            bytes_read: Arc::new(AtomicU64::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Shared handles to the (read, written) counters. Cheap: just two
    /// `Arc` clones. Counters are updated on every successful read/write
    /// with [`Ordering::Relaxed`].
    pub fn counts(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (self.bytes_read.clone(), self.bytes_written.clone())
    }

    /// Borrow the wrapped inner stream, for callers that need a
    /// non-counting operation (e.g. reading socket options).
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Consume the wrapper and recover the inner stream.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead> AsyncRead for CountingStream<S> {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        let BufResult(res, buf) = self.inner.read(buf).await;
        if let Ok(n) = &res {
            self.bytes_read.fetch_add(*n as u64, Ordering::Relaxed);
        }
        BufResult(res, buf)
    }

    async fn read_vectored<V: IoVectoredBufMut>(&mut self, buf: V) -> BufResult<usize, V> {
        let BufResult(res, buf) = self.inner.read_vectored(buf).await;
        if let Ok(n) = &res {
            self.bytes_read.fetch_add(*n as u64, Ordering::Relaxed);
        }
        BufResult(res, buf)
    }
}

impl<S: AsyncWrite> AsyncWrite for CountingStream<S> {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let BufResult(res, buf) = self.inner.write(buf).await;
        if let Ok(n) = &res {
            self.bytes_written.fetch_add(*n as u64, Ordering::Relaxed);
        }
        BufResult(res, buf)
    }

    async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let BufResult(res, buf) = self.inner.write_vectored(buf).await;
        if let Ok(n) = &res {
            self.bytes_written.fetch_add(*n as u64, Ordering::Relaxed);
        }
        BufResult(res, buf)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush().await
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.inner.shutdown().await
    }
}

// ---------------------------------------------------------------------------
// Unit tests — simple in-memory cases. Integration tests using real
// sockets live in `tests/counting_stream.rs`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// `Vec<u8>` implements compio's `AsyncWrite` (appends), and `&[u8]`
    /// implements `AsyncRead`. Handy for unit tests that don't need a
    /// real socket.
    #[compio::test]
    async fn write_increments_counter() {
        let sink: Vec<u8> = Vec::new();
        let mut s = CountingStream::new(sink);
        let (_read, written) = s.counts();
        let BufResult(res, _) = s.write(b"hello".to_vec()).await;
        assert_eq!(res.unwrap(), 5);
        assert_eq!(written.load(Ordering::Relaxed), 5);
        // Inner buffer actually got the bytes.
        assert_eq!(s.into_inner(), b"hello");
    }

    #[compio::test]
    async fn read_increments_counter() {
        let src: &[u8] = b"hello world";
        let mut s = CountingStream::new(src);
        let (read_ctr, _written) = s.counts();
        let buf = Vec::with_capacity(32);
        let BufResult(res, buf) = s.read(buf).await;
        assert_eq!(res.unwrap(), src.len());
        assert_eq!(buf.as_slice(), src);
        assert_eq!(read_ctr.load(Ordering::Relaxed), src.len() as u64);
    }

    #[compio::test]
    async fn multiple_writes_accumulate() {
        let sink: Vec<u8> = Vec::new();
        let mut s = CountingStream::new(sink);
        let (_, written) = s.counts();
        let BufResult(r1, _) = s.write(b"hel".to_vec()).await;
        let BufResult(r2, _) = s.write(b"lo".to_vec()).await;
        assert_eq!(r1.unwrap(), 3);
        assert_eq!(r2.unwrap(), 2);
        assert_eq!(written.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn get_ref_exposes_inner() {
        let sink: Vec<u8> = vec![1, 2, 3];
        let s = CountingStream::new(sink);
        assert_eq!(s.get_ref(), &[1, 2, 3]);
    }

    #[test]
    fn counters_start_at_zero() {
        let s: CountingStream<Vec<u8>> = CountingStream::new(Vec::new());
        let (r, w) = s.counts();
        assert_eq!(r.load(Ordering::Relaxed), 0);
        assert_eq!(w.load(Ordering::Relaxed), 0);
    }
}
