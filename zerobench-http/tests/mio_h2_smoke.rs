#![cfg(feature = "mio-h2")]
//! Smoke tests for the mio-based HTTP/2 transport.
//!
//! These tests verify error handling and graceful degradation without
//! requiring an external H2 server. End-to-end H2 throughput is
//! validated by the benchmark suite (`bench.sh`) against real servers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zerobench_core::plan::{Plan, RateProfile, RequestPlan, Scenario, Step};
use zerobench_core::template::Template;
use zerobench_core::transport::Target;
use zerobench_core::var::VarRegistry;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Connecting to a non-existent HTTPS server produces connect errors
/// (not a panic). Validates the TLS + H2 error path.
#[test]
fn mio_h2_https_without_server_records_connect_errors() {
    let target = Target::parse("https://127.0.0.1:19443").unwrap();
    let mut vars = VarRegistry::new();
    let url = Template::compile("https://127.0.0.1:19443/bench", &mut vars).unwrap();
    let req = RequestPlan::get(url);
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "tls".into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![Step::Request(req)],
        }],
        vars,
        duration: Duration::from_secs(1),
        warmup: Duration::ZERO,
        cooldown: std::time::Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: zerobench_core::plan::Mode::default(),
        name: String::new(),
    };
    let opts = zerobench_core::transport::TransportOpts {
        insecure_tls: true,
        ..Default::default()
    };
    let tls_config = Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"h2"]));
    let stats = zerobench_http::mio_h2::run_mio_h2_threaded(
        &target, &opts, &plan, 1, 1, Duration::from_secs(1), None, tls_config, None, None,
    );
    let total: u64 = stats.iter().map(|s| s.requests).sum();
    let errors: u64 = stats.iter().map(|s| s.errors.total()).sum();
    assert!(total > 0 || errors > 0, "expected connect attempts");
}

/// Plain HTTP H2 (prior knowledge) against a non-listening port
/// produces connect errors gracefully.
#[test]
fn mio_h2_plain_without_server_records_connect_errors() {
    let target = Target::parse("http://127.0.0.1:19444").unwrap();
    let mut vars = VarRegistry::new();
    let url = Template::compile("http://127.0.0.1:19444/bench", &mut vars).unwrap();
    let req = RequestPlan::get(url);
    let plan = Plan {
        scenarios: vec![Scenario {
            name: "plain".into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![Step::Request(req)],
        }],
        vars,
        duration: Duration::from_secs(1),
        warmup: Duration::ZERO,
        cooldown: std::time::Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: zerobench_core::plan::Mode::default(),
        name: String::new(),
    };

    let stop = Arc::new(AtomicBool::new(false));
    let ws = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        ws.store(true, Ordering::Relaxed);
    });

    let topts = zerobench_core::transport::TransportOpts::default();
    let stats = zerobench_http::mio_h2::run_mio_h2_worker(
        &plan, &target, &topts, 1, &stop, None, None, None,
    );
    assert!(stats.errors.total() > 0, "expected connect errors");
}
