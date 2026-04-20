//! Smoke test for the HttpColdConnect backend.
//!
//! Stands up a minimal `std::net::TcpListener` HTTP server that
//! responds 200 OK `"pong"` with `Content-Length: 4` and
//! `Connection: close`, runs cold-connect against it for a short
//! window, and asserts stats propagate the way the verb expects.
//!
//! The stub is `std::net` on purpose — it's the SERVER under test,
//! not the client. The zerobench client under test is the
//! mio-based cold_connect backend.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use smallvec::SmallVec;
use zerobench_core::plan::{
    ColdConnectPlan, Mode, Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::transport::{AddrFamily, Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_core::Template;

fn spawn_http_stub(stop: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).ok();
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    std::thread::spawn(move || {
                        stream.set_read_timeout(Some(Duration::from_secs(1))).ok();
                        let mut buf = [0u8; 4096];
                        // Read until end-of-headers.
                        let mut total = 0;
                        while total < buf.len() {
                            let n = match stream.read(&mut buf[total..]) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong",
                        );
                        let _ = stream.flush();
                        // Close naturally when the TcpStream drops.
                    });
                }
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    addr
}

fn plan_for(addr: SocketAddr) -> (Plan, Target) {
    let mut vars = VarRegistry::new();
    let url = Template::compile(&format!("http://{addr}/"), &mut vars).unwrap();
    let request = RequestPlan {
        method: http::Method::GET,
        url,
        headers: SmallVec::new(),
        body: None,
        extract: Vec::new(),
        checks: Vec::new(),
        expect_streaming: false,
    };
    let scenario = Scenario {
        name: "cold".into(),
        rate: RateProfile::Saturate {
            max_concurrency: 4,
        },
        steps: vec![Step::HttpColdConnect(ColdConnectPlan { request })],
    };
    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: Duration::from_millis(500),
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::Measure,
        name: "cold-connect-smoke".into(),
    };
    let target = Target {
        host: "127.0.0.1".to_string(),
        port: addr.port(),
        tls: false,
        sni: None,
        addr_family: AddrFamily::V4,
    };
    (plan, target)
}

#[test]
fn cold_connect_records_requests_from_fresh_connections() {
    let stop = Arc::new(AtomicBool::new(false));
    let addr = spawn_http_stub(Arc::clone(&stop));

    let (plan, target) = plan_for(addr);
    let mut opts = TransportOpts::default();
    opts.connect_timeout = Duration::from_millis(500);
    opts.request_timeout = Duration::from_secs(1);

    let stats = zerobench_backends::http::cold_connect::run_cold_connect_from_plan_threaded(
        &target,
        &opts,
        &plan,
        /* connections */ 2,
        Duration::from_millis(300),
        /* target_rps */ None,
        /* tls_config */ None,
        /* live */ None,
        /* stop_flag */ None,
    );
    stop.store(true, Ordering::Relaxed);

    assert_eq!(stats.len(), 1, "one scenario");
    let ts = &stats[0];
    assert!(
        ts.requests >= 1,
        "expected ≥1 cold-connect op, got {}",
        ts.requests
    );
    assert_eq!(
        ts.errors.connect, 0,
        "no connect errors against local stub"
    );
    // Payload is 4 bytes per response ("pong"); a connect+read cycle
    // should drain it.
    assert!(
        ts.bytes_recv as usize >= ts.requests as usize * 4,
        "expected ≥{} response bytes, got {}",
        ts.requests * 4,
        ts.bytes_recv
    );
}
