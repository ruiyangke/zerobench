//! Minimal one-shot HTTP POST — used by benchmark harness paths that
//! need to fire an occasional side-channel request (fanout triggers,
//! control-plane probes) without standing up the full mio_h1 pool.
//!
//! Pure mio / non-blocking throughout: the whole point of this module
//! is to avoid regressing the "no blocking std::net on the client
//! side" invariant that `SseFanout`, `WsFanout`, and
//! `SseReconnectStorm` previously broke with `std::net::TcpStream`.
//!
//! # What it does
//!
//! 1. `mio::net::TcpStream::connect(addr)`
//! 2. Register + wait for writable (TCP 3WHS complete)
//! 3. If TLS: wrap as [`MioTlsStream`], drive handshake to completion
//!    via `drive_tls_io`
//! 4. Write request bytes
//! 5. Drain response until EOF or `request_timeout`
//! 6. Close
//!
//! # What it does not do
//!
//! - Redirect following, keep-alive, chunked decoding, extraction,
//!   or header parsing beyond "did something come back?" Callers
//!   only need to know the POST completed.

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::net::TcpStream as MioTcp;
use mio::{Events, Interest, Poll, Token};
use rustls::ClientConfig;

use zerobench_core::transport::{Target, TransportOpts};

use crate::mio_tls::{MioStream, MioTlsStream};

const POLL_TOKEN: Token = Token(0);

/// Send a single HTTP POST with the given path + body, following the
/// target's `tls` flag for scheme. Returns `Ok(())` when the server
/// sent anything back (or closed cleanly), `Err` on connect / write
/// / timeout / handshake failure.
pub fn fire_http_post(
    target: &Target,
    opts: &TransportOpts,
    path: &str,
    body: &[u8],
    tls_config: Option<&Arc<ClientConfig>>,
) -> io::Result<()> {
    let addr = target.resolve(opts).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("resolve: {e}"))
    })?;

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(4);
    let mut tcp = MioTcp::connect(addr)?;
    let _ = tcp.set_nodelay(true);
    poll.registry()
        .register(&mut tcp, POLL_TOKEN, Interest::READABLE | Interest::WRITABLE)?;

    wait_for(&mut poll, &mut events, opts.connect_timeout, |e| {
        e.token() == POLL_TOKEN && e.is_writable()
    })?;
    if tcp.peer_addr().is_err() {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "connect refused",
        ));
    }

    let mut stream = if target.tls {
        let config = tls_config.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "https trigger but no TLS config provided",
            )
        })?;
        let sni = target.sni_name().to_string();
        let tls = MioTlsStream::new(tcp, Arc::clone(config), &sni)?;
        MioStream::Tls(tls)
    } else {
        MioStream::Plain(tcp)
    };

    if stream.is_handshaking() {
        let hs_start = Instant::now();
        while stream.is_handshaking() {
            if hs_start.elapsed() > Duration::from_millis(5_000) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "tls handshake",
                ));
            }
            stream.drive_tls_io()?;
            if !stream.is_handshaking() {
                break;
            }
            let _ = poll.poll(&mut events, Some(Duration::from_millis(200)));
        }
    }

    let host = if (target.tls && target.port == 443) || (!target.tls && target.port == 80) {
        target.host.clone()
    } else {
        format!("{}:{}", target.host, target.port)
    };
    let mut request: Vec<u8> = Vec::with_capacity(256 + body.len());
    request.extend_from_slice(
        format!(
            "POST {path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    );
    request.extend_from_slice(body);

    let mut write_pos = 0;
    while write_pos < request.len() {
        match stream.write(&request[write_pos..]) {
            Ok(0) => {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "zero write"));
            }
            Ok(n) => write_pos += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait_for(&mut poll, &mut events, opts.request_timeout, |e| {
                    e.token() == POLL_TOKEN && e.is_writable()
                })?;
            }
            Err(e) => return Err(e),
        }
    }

    // Drain the response until EOF or request_timeout. We don't parse
    // status — callers consider "got something back, closed cleanly"
    // as success. Servers that return a body before closing will
    // produce Ok(n) with bytes we throw away.
    let read_deadline = Instant::now() + opts.request_timeout;
    let mut scratch = [0u8; 2048];
    let mut got_any = false;
    loop {
        if Instant::now() >= read_deadline {
            // Without at least one byte we treat this as a timeout;
            // otherwise accept the partial read as "trigger delivered"
            // (the server just didn't close before the timeout).
            return if got_any {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::TimedOut, "trigger read"))
            };
        }
        match stream.read(&mut scratch) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                got_any = true;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let left = read_deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    continue;
                }
                let budget = left.min(Duration::from_millis(100));
                let _ = poll.poll(&mut events, Some(budget));
            }
            Err(e) => return Err(e),
        }
    }
}

fn wait_for(
    poll: &mut Poll,
    events: &mut Events,
    timeout: Duration,
    pred: impl Fn(&mio::event::Event) -> bool,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "poll wait"));
        }
        poll.poll(events, Some(deadline - now))?;
        for ev in events.iter() {
            if pred(ev) {
                return Ok(());
            }
        }
    }
}
