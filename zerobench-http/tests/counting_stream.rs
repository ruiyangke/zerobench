//! Integration tests for [`zerobench_http::CountingStream`] over real
//! sockets.
//!
//! The lib-level unit tests verify behaviour over in-memory `Vec<u8>`
//! and `&[u8]` streams. Here we exercise the same counter logic against
//! a real compio `TcpStream` pair so the compio driver code path is
//! covered end-to-end.

use std::sync::atomic::Ordering;

use compio::buf::BufResult;
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
async fn partial_writes_still_count_correctly() {
    // Exercise the "successful but short" branch — write a large chunk
    // in a loop and verify the counter ends up exactly equal to the
    // bytes we asked for.
    let (mut server, mut client) = loopback_pair().await;
    let (_, written) = server.counts();

    // 64 KiB — large enough that the OS may split the send across
    // multiple `write` calls.
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
