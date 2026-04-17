#![cfg(feature = "runtime-compio")]
//! Integration tests for [`zerobench_http::CountingStream`] over real
//! sockets, plus targeted mock-based tests that exercise code paths
//! real loopback can't reliably reproduce (partial writes, IO errors).
//!
//! The lib-level unit tests verify behaviour over in-memory `Vec<u8>`
//! and `&[u8]` streams. Here we exercise the same counter logic against
//! a real compio `TcpStream` pair so the compio driver code path is
//! covered end-to-end.

use std::io;
use std::sync::atomic::Ordering;

use compio::buf::{BufResult, IoBuf, IoBufMut};
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};

use zerobench_http::CountingStream;

/// Start a listener on 127.0.0.1:0 (OS-assigned port) and accept exactly
/// one connection. Returns the server-side socket paired with the local
/// address to connect to.
async fn loopback_pair() -> (CountingStream<TcpStream>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let accept_fut = listener.accept();
    let connect_fut = TcpStream::connect(addr);
    // Drive both sides of the handshake on the same task. compio's
    // futures are poll-based so we can just `join!`.
    let (accept_res, connect_res) = futures_util::future::join(accept_fut, connect_fut).await;
    let (server, _peer) = accept_res.unwrap();
    let client = connect_res.unwrap();

    // Return server wrapped in CountingStream — that's the side we're
    // exercising — and raw client so the test can drive the other side.
    (CountingStream::new(server), client)
}

#[compio::test]
async fn writes_count_exactly() {
    let (mut server, mut client) = loopback_pair().await;
    let (_read, written) = server.counts();

    let payload: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    let expected_len = payload.len();
    server.write_all(payload).await.unwrap();
    server.flush().await.unwrap();

    // Drain the client side so the TCP layer actually forwards the bytes.
    let mut received = Vec::with_capacity(expected_len);
    while received.len() < expected_len {
        let buf = Vec::with_capacity(expected_len - received.len());
        let BufResult(res, buf) = client.read(buf).await;
        let n = res.unwrap();
        if n == 0 {
            break;
        }
        received.extend_from_slice(&buf[..n]);
    }

    assert_eq!(received.len(), expected_len);
    assert_eq!(written.load(Ordering::Relaxed), expected_len as u64);
}

#[compio::test]
async fn reads_count_until_eof() {
    let (mut server, mut client) = loopback_pair().await;
    let (read_ctr, _written) = server.counts();

    // Client sends a fixed payload and closes the write half.
    let payload: Vec<u8> = b"the quick brown fox jumps over the lazy dog"
        .iter()
        .copied()
        .collect();
    let expected_len = payload.len();
    client.write_all(payload).await.unwrap();
    client.flush().await.unwrap();
    client.shutdown().await.unwrap();
    drop(client);

    // Read until EOF on the server side.
    let mut total = 0usize;
    loop {
        let buf = Vec::with_capacity(64);
        let BufResult(res, buf) = server.read(buf).await;
        let n = res.unwrap();
        if n == 0 {
            break;
        }
        total += buf.len();
    }

    assert_eq!(total, expected_len);
    assert_eq!(read_ctr.load(Ordering::Relaxed), expected_len as u64);
}

#[compio::test]
async fn large_writes_count_exactly() {
    // Loopback will normally accept a 64 KiB write whole — this test
    // doesn't *guarantee* the partial-write branch executes, it just
    // establishes that the counter matches the bytes delivered when
    // write_all drives a multi-call flush path (see
    // `short_writes_accumulate_via_write_all_mock` for the deterministic
    // partial-write case).
    let (mut server, mut client) = loopback_pair().await;
    let (_, written) = server.counts();

    let chunk: Vec<u8> = vec![0xAB; 65_536];
    let expected_len = chunk.len();
    server.write_all(chunk).await.unwrap();
    server.flush().await.unwrap();

    // Drain the client so the kernel's socket buffer doesn't stall.
    let mut received = 0usize;
    while received < expected_len {
        let buf = Vec::with_capacity(4096);
        let BufResult(res, buf) = client.read(buf).await;
        let n = res.unwrap();
        if n == 0 {
            break;
        }
        received += buf.len();
    }

    assert_eq!(received, expected_len);
    assert_eq!(written.load(Ordering::Relaxed), expected_len as u64);
}

// ---------------------------------------------------------------------------
// Mock-based tests.
//
// The loopback-backed tests above exercise the happy path against a real
// socket but don't deterministically hit partial writes or IO errors.
// The mocks here let us nail down both: CountingStream must accumulate
// across every partial write, and must *not* advance its counters when
// the inner stream returns an error.
// ---------------------------------------------------------------------------

/// AsyncWrite mock that returns `Ok(buf_len / 2)` on each call, or
/// `Ok(buf_len)` once the requested slice shrinks to 1 byte.
/// Compio's `write_all` will keep re-issuing until the buffer is fully
/// consumed, so this reliably exercises the counter-on-partial-write path.
struct HalfWriter {
    total_accepted: usize,
}

impl HalfWriter {
    fn new() -> Self {
        Self { total_accepted: 0 }
    }
}

impl AsyncWrite for HalfWriter {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let len = buf.buf_len();
        // Accept half on each call; once we're down to 1 byte take it
        // whole so write_all terminates.
        let n = if len <= 1 { len } else { len / 2 };
        self.total_accepted += n;
        BufResult(Ok(n), buf)
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// AsyncWrite mock whose every `write` call fails.
struct FailingWriter;

impl AsyncWrite for FailingWriter {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        BufResult(
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "mock write failure")),
            buf,
        )
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// AsyncRead mock whose every `read` call fails.
struct FailingReader;

impl AsyncRead for FailingReader {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        BufResult(
            Err(io::Error::new(io::ErrorKind::ConnectionReset, "mock read failure")),
            buf,
        )
    }
}

#[compio::test]
async fn short_writes_accumulate_via_write_all_mock() {
    // Wrap a HalfWriter: every call accepts at most half of what's asked
    // for, so a 16-byte write_all runs through multiple partial writes.
    // The CountingStream's counter must equal the total bytes written.
    let mut s = CountingStream::new(HalfWriter::new());
    let (_, written) = s.counts();

    let payload: Vec<u8> = (0..16u8).collect();
    let expected = payload.len();
    s.write_all(payload).await.unwrap();

    assert_eq!(
        written.load(Ordering::Relaxed),
        expected as u64,
        "counter should equal total bytes written across partial writes"
    );
    // Sanity: the inner writer also saw every byte exactly once.
    assert_eq!(s.into_inner().total_accepted, expected);
}

#[compio::test]
async fn write_error_does_not_advance_counter() {
    let mut s = CountingStream::new(FailingWriter);
    let (_, written) = s.counts();

    let BufResult(res, _) = s.write(b"anything".to_vec()).await;
    assert!(res.is_err(), "mock FailingWriter should always fail");
    assert_eq!(
        written.load(Ordering::Relaxed),
        0,
        "errored writes must not bump the counter"
    );
}

#[compio::test]
async fn read_error_does_not_advance_counter() {
    let mut s = CountingStream::new(FailingReader);
    let (read_ctr, _) = s.counts();

    let buf = Vec::with_capacity(32);
    let BufResult(res, _) = s.read(buf).await;
    assert!(res.is_err(), "mock FailingReader should always fail");
    assert_eq!(
        read_ctr.load(Ordering::Relaxed),
        0,
        "errored reads must not bump the counter"
    );
}

#[compio::test]
async fn bidirectional_counts_are_independent() {
    let (mut server, mut client) = loopback_pair().await;
    let (read_ctr, written_ctr) = server.counts();

    // Client → Server (counts as server "read")
    client.write_all(b"ping".to_vec()).await.unwrap();
    client.flush().await.unwrap();
    let buf = Vec::with_capacity(4);
    let BufResult(res, buf) = server.read(buf).await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(buf, b"ping");

    // Server → Client (counts as server "written")
    server.write_all(b"pong!".to_vec()).await.unwrap();
    server.flush().await.unwrap();
    let buf2 = Vec::with_capacity(5);
    let BufResult(res, buf2) = client.read(buf2).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(buf2, b"pong!");

    assert_eq!(read_ctr.load(Ordering::Relaxed), 4);
    assert_eq!(written_ctr.load(Ordering::Relaxed), 5);
}
