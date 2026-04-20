//! S8: integration smoke tests for SSE step variants that previously
//! had no end-to-end coverage: SseFanout and SseReconnectStorm.
//!
//! Both use a minimal chunked HTTP/SSE stub server. The stub keeps
//! the connection alive by default; SseFanout's test additionally
//! dispatches per-request on path so a single port speaks both the
//! `/events` subscriber stream and the `/broadcast` trigger endpoint
//! (matches the same-host assumption in `zerobench_sse::fanout`).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use smallvec::SmallVec;
use zerobench_core::plan::{
    FanoutMode, Mode, Plan, RateProfile, Scenario, SseFanoutPlan, SseHoldPlan,
    SseReconnectStormPlan, Step, TriggerSpec,
};
use zerobench_core::template::Template;
use zerobench_core::transport::{AddrFamily, Target, TransportOpts};
use zerobench_core::var::VarRegistry;

// ---------------------------------------------------------------------------
// Stub SSE server helpers
// ---------------------------------------------------------------------------

/// Read one HTTP/1.1 request off `stream`. Returns `(method, path,
/// headers_blob)` so the dispatcher can route by path.
fn read_http_request(
    stream: &mut TcpStream,
) -> std::io::Result<(String, String, String)> {
    let mut req = Vec::new();
    let mut tmp = [0u8; 2048];
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before sending request",
            ));
        }
        req.extend_from_slice(&tmp[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&req).to_string();
    let first_line = text.lines().next().unwrap_or_default();
    let mut it = first_line.split_whitespace();
    let method = it.next().unwrap_or_default().to_string();
    let path = it.next().unwrap_or_default().to_string();
    Ok((method, path, text))
}

/// Write an SSE-style 200 response headers with chunked body.
fn write_sse_headers(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(
        b"HTTP/1.1 200 OK\r\n\
          Content-Type: text/event-stream\r\n\
          Transfer-Encoding: chunked\r\n\
          Connection: keep-alive\r\n\
          Cache-Control: no-cache\r\n\r\n",
    )
}

/// Write one SSE event as an HTTP chunk.
fn write_sse_chunk(stream: &mut TcpStream, body: &str) -> std::io::Result<()> {
    let chunk = format!("{:x}\r\n{}\r\n", body.len(), body);
    stream.write_all(chunk.as_bytes())?;
    stream.flush()
}

fn http_target(addr: SocketAddr) -> Target {
    Target {
        host: "127.0.0.1".into(),
        port: addr.port(),
        tls: false,
        sni: None,
        addr_family: AddrFamily::V4,
    }
}

// ---------------------------------------------------------------------------
// SseFanout
// ---------------------------------------------------------------------------

/// Single-port stub: `GET /events` → long-lived SSE stream; `POST
/// /broadcast` → counts the hit and emits an event to every active
/// subscriber stream.
///
/// Same port for both sides because `zerobench_sse::fanout` reuses
/// the subscriber Target for the trigger POST.
fn spawn_sse_fanout_stub(triggers_seen: Arc<AtomicU32>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let subs: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));

    std::thread::spawn(move || loop {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let subs = Arc::clone(&subs);
        let triggers_seen = Arc::clone(&triggers_seen);
        std::thread::spawn(move || {
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .ok();
            let (method, path, _raw) = match read_http_request(&mut stream) {
                Ok(v) => v,
                Err(_) => return,
            };
            if method == "GET" && path.starts_with("/events") {
                if write_sse_headers(&mut stream).is_err() {
                    return;
                }
                // Emit a primer event so the subscriber's `ttfb` /
                // handshake recording path exercises the same code
                // the production server would. After that, wait for
                // broadcasts from the trigger side.
                let _ = write_sse_chunk(&mut stream, "data: primer\n\n");
                if let Ok(clone) = stream.try_clone() {
                    subs.lock().unwrap().push(clone);
                }
                // Park the subscriber by reading into the void.
                let mut tmp = [0u8; 1024];
                let _ = stream.read(&mut tmp);
                return;
            }
            if method == "POST" && path.starts_with("/broadcast") {
                triggers_seen.fetch_add(1, Ordering::Relaxed);
                let _ = stream.write_all(
                    b"HTTP/1.1 204 No Content\r\n\
                      Content-Length: 0\r\n\
                      Connection: close\r\n\r\n",
                );
                let mut guard = subs.lock().unwrap();
                guard
                    .retain_mut(|s| write_sse_chunk(s, "data: broadcast\n\n").is_ok());
                return;
            }
            let _ =
                stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
        });
    });
    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn sse_fanout_receives_broadcasts_after_triggers() {
    let triggers = Arc::new(AtomicU32::new(0));
    let addr = spawn_sse_fanout_stub(Arc::clone(&triggers));

    let mut vars = VarRegistry::new();
    let sub_url = Template::compile(&format!("http://{addr}/events"), &mut vars).unwrap();
    let trigger_url =
        Template::compile(&format!("http://{addr}/broadcast"), &mut vars).unwrap();

    let fanout = SseFanoutPlan {
        subscribers: SseHoldPlan {
            url: sub_url,
            headers: SmallVec::new(),
            subscribers: 2,
            hold_for: Duration::from_millis(2_000),
            reconnect: false,
        },
        trigger: TriggerSpec::HttpPost {
            url: trigger_url,
            body: None,
        },
        mode: FanoutMode::TriggerRtt,
    };

    let plan = Plan {
        scenarios: vec![Scenario {
            name: "sse-fanout-smoke".into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![Step::SseFanout(fanout)],
        }],
        vars,
        duration: Duration::from_millis(2_000),
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::Measure,
        name: "sse-fanout-smoke".into(),
    };

    let stats = zerobench_sse::run_sse_fanout_from_plan_threaded(
        &http_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(2_000),
        None,
        None,
        None,
    );
    assert_eq!(stats.len(), 1);

    // The fanout trigger cadence in the backend is fixed at
    // TRIGGER_INTERVAL_MS; over 2s we expect multiple hits.
    let triggered = triggers.load(Ordering::Relaxed);
    assert!(
        triggered >= 1,
        "expected ≥1 HTTP trigger; got {triggered}"
    );
    // Each subscriber gets the primer event plus any broadcasts —
    // at least one event per subscriber should have been recorded.
    let sse = stats[0].per_scenario[0]
        .sse
        .as_ref()
        .expect("sse extras present");
    assert!(
        sse.chunks >= 1,
        "expected ≥1 recorded SSE event across subscribers; got {}",
        sse.chunks
    );
}

// ---------------------------------------------------------------------------
// SseHold — reconnect=true path
// ---------------------------------------------------------------------------

#[test]
fn sse_hold_reconnect_restarts_session_on_server_close() {
    // Server closes after 2 events. With reconnect=true and a
    // 1.5s hold, the SseHold subscriber should cycle through at
    // least two sessions — total events > events_per_session.
    let addr = spawn_sse_reconnect_stub(2, Duration::from_millis(20));

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("http://{addr}/stream"), &mut vars).unwrap();
    let hold = SseHoldPlan {
        url,
        headers: SmallVec::new(),
        subscribers: 1,
        hold_for: Duration::from_millis(1_500),
        reconnect: true,
    };
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "sse-hold-reconnect-smoke".into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![Step::SseHold(hold)],
        }],
        vars,
        duration: Duration::from_millis(1_500),
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::Measure,
        name: "sse-hold-reconnect-smoke".into(),
    };

    let stop = Arc::new(AtomicBool::new(false));
    let stop_timer = Arc::clone(&stop);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1_700));
        stop_timer.store(true, Ordering::Relaxed);
    });

    let stats = zerobench_sse::run_sse_hold_from_plan_threaded(
        &http_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(1_500),
        None,
        None,
        Some(stop),
    );
    let ts = &stats[0];
    let sse = ts.per_scenario[0].sse.as_ref().expect("sse extras");
    assert!(
        sse.chunks >= 4,
        "reconnect=true should yield ≥4 events across sessions; got {}",
        sse.chunks
    );
}

// ---------------------------------------------------------------------------
// SseReconnectStorm
// ---------------------------------------------------------------------------

/// Stream stub that keeps advertising `id: N` tokens and drops the
/// connection after `events_per_session` events. Used to drive the
/// reconnect-storm backend through multiple session cycles inside
/// the test's duration window.
fn spawn_sse_reconnect_stub(
    events_per_session: usize,
    interval: Duration,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || loop {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        std::thread::spawn(move || {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .ok();
            let (_method, _path, _raw) = match read_http_request(&mut stream) {
                Ok(v) => v,
                Err(_) => return,
            };
            if write_sse_headers(&mut stream).is_err() {
                return;
            }
            for i in 0..events_per_session {
                // Two-line event carrying an id + data. The parser
                // emits the Id event immediately and the Data event
                // at the blank line — exercising the SSE Id path the
                // reconnect-storm backend now consumes directly.
                let body = format!("id: {i}\ndata: event-{i}\n\n");
                if write_sse_chunk(&mut stream, &body).is_err() {
                    return;
                }
                if interval > Duration::ZERO {
                    std::thread::sleep(interval);
                }
            }
            // Drop the connection — triggers a reconnect in the client.
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });
    });
    std::thread::sleep(Duration::from_millis(50));
    addr
}

#[test]
fn sse_reconnect_storm_cycles_sessions_and_extracts_ids() {
    let addr = spawn_sse_reconnect_stub(4, Duration::from_millis(40));

    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("http://{addr}/stream"), &mut vars).unwrap();
    let storm = SseReconnectStormPlan {
        subscribers: SseHoldPlan {
            url,
            headers: SmallVec::new(),
            subscribers: 1,
            hold_for: Duration::from_millis(1_200),
            reconnect: true,
        },
        // High kill rate encourages frequent drops + reconnects;
        // combined with the stub's session cap this means the
        // backend sees several cycles inside the test window.
        kill_rate_per_s: 5.0,
        verify_last_event_id: true,
    };

    let plan = Plan {
        scenarios: vec![Scenario {
            name: "sse-reconnect-storm-smoke".into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![Step::SseReconnectStorm(storm)],
        }],
        vars,
        duration: Duration::from_millis(1_200),
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::Measure,
        name: "sse-reconnect-storm-smoke".into(),
    };

    let stop = Arc::new(AtomicBool::new(false));
    let stop_timer = Arc::clone(&stop);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1_500));
        stop_timer.store(true, Ordering::Relaxed);
    });

    let t_start = Instant::now();
    let stats = zerobench_sse::run_sse_reconnect_storm_from_plan_threaded(
        &http_target(addr),
        &TransportOpts::default(),
        &plan,
        Duration::from_millis(1_200),
        None,
        None,
        Some(stop),
    );
    let elapsed = t_start.elapsed();
    // Hard upper bound so a runaway reconnect loop doesn't silently
    // balloon the test into a 30-second hang.
    assert!(
        elapsed < Duration::from_secs(3),
        "reconnect-storm ran too long: {elapsed:?}"
    );

    assert_eq!(stats.len(), 1);
    let ts = &stats[0];
    let sse = ts.per_scenario[0]
        .sse
        .as_ref()
        .expect("sse extras present");
    // Each session emits 4 events; the 1.2s window should see at
    // least 2 sessions worth.
    assert!(
        sse.chunks >= 4,
        "expected ≥4 events across reconnect sessions; got {}",
        sse.chunks
    );
}
