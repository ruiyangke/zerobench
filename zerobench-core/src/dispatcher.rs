//! Scenario dispatch and the worker loop.
//!
//! This module turns a [`Plan`] + a [`Transport::Client`] into a stream of
//! real HTTP exchanges, runs them to completion, and collects per-task
//! statistics for the reporter.
//!
//! Two entry points:
//!
//! - [`run_saturate`] — closed-loop. Spawns `max_tasks` worker coroutines,
//!   each loops "pick scenario → execute steps → record stats" until the
//!   shared [`StopSignal`] trips.
//! - [`run_open_loop`] (Task 10) — token-driven; one scheduler per
//!   scenario emits timestamped tokens into a shared MPMC channel that
//!   the workers drain.
//!
//! Both modes share the same per-iteration step-runner
//! ([`execute_steps`]), so assertions / extracts / template expansion
//! have one well-tested implementation.
//!
//! # Scheduling
//!
//! Workers run as `compio::runtime::spawn` tasks on the current
//! single-threaded runtime. `exchange` futures are `!Send` (they bottom
//! out in compio IO handles) but that's fine — every worker stays on
//! the runtime's thread.
//!
//! # Cancellation
//!
//! A tripped [`StopSignal`] only prevents the worker from picking up a
//! new iteration; any in-flight `exchange` is allowed to finish. That's
//! the correct behaviour for a bench tool — dropping the future on the
//! timeout boundary would invalidate the slot (see `Http1Pool::exchange`
//! timeout handling) and skew stats with spurious errors.

use std::sync::Arc;

use bytes::Bytes;
use rand::Rng;

use crate::plan::{Assertion, Extract, Plan, Step};
use crate::rng::from_entropy;
use crate::scenario_context::ScenarioContext;
use crate::stats::{ErrorKind, TaskStats};
use crate::stop::StopSignal;
use crate::transport::{Response, Transport, TransportError};

// ---------------------------------------------------------------------------
// Public API — saturate-mode dispatcher
// ---------------------------------------------------------------------------

/// Run a closed-loop saturation benchmark.
///
/// Spawns `max_tasks` concurrent worker coroutines. Each worker
/// iterates:
///
/// 1. Pick a scenario (weighted-random; Phase C uses equal weights
///    pending the Task-10 rate-profile work).
/// 2. Walk through its [`Step`]s in order, executing [`Step::Request`]s
///    through `client` and sleeping for [`Step::Pause`] /
///    [`Step::PauseRandom`].
/// 3. Record latency/TTFB/bytes into the worker's [`TaskStats`].
/// 4. Loop until `stop` trips.
///
/// Returns one [`TaskStats`] per worker; the caller merges via
/// [`crate::stats::Summary::merge`].
///
/// Empty plan (no scenarios) returns an empty `Vec` without spawning
/// anything.
pub async fn run_saturate<T: Transport>(
    plan: &Plan,
    client: T::Client,
    max_tasks: usize,
    stop: StopSignal,
) -> Vec<TaskStats> {
    if plan.scenarios.is_empty() || max_tasks == 0 {
        return Vec::new();
    }

    let plan = Arc::new(plan.clone());
    let num_scenarios = plan.scenarios.len();

    let mut handles = Vec::with_capacity(max_tasks);
    for _ in 0..max_tasks {
        let plan = plan.clone();
        let client = client.clone();
        let stop = stop.clone();
        let handle = compio::runtime::spawn(async move {
            worker_saturate::<T>(plan, client, stop, num_scenarios).await
        });
        handles.push(handle);
    }

    let mut out = Vec::with_capacity(max_tasks);
    for h in handles {
        match h.await {
            Ok(stats) => out.push(stats),
            Err(_panic) => {
                // The worker panicked. Contribute an empty stats slot so
                // the count of tasks-started matches tasks-collected,
                // rather than silently dropping the panic or letting it
                // poison the whole run.
                out.push(TaskStats::new(num_scenarios));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Worker — saturate mode
// ---------------------------------------------------------------------------

async fn worker_saturate<T: Transport>(
    plan: Arc<Plan>,
    client: T::Client,
    stop: StopSignal,
    num_scenarios: usize,
) -> TaskStats {
    let mut stats = TaskStats::new(num_scenarios);
    let mut ctx = ScenarioContext::new(plan.vars.len(), from_entropy());

    while !stop.is_stopped() {
        // Pick a scenario. Phase C uses uniform random selection;
        // proper weighted-random lands with the Task-10 rate profile.
        let scenario_id = pick_scenario(num_scenarios, &mut ctx);
        let scenario = &plan.scenarios[scenario_id];

        execute_steps::<T>(
            &client,
            scenario_id as u16,
            &scenario.steps,
            &mut ctx,
            &mut stats,
        )
        .await;

        // Fresh slate between iterations so a prior iteration's extracts
        // don't leak into the next.
        ctx.clear_all();
    }

    stats
}

// ---------------------------------------------------------------------------
// Shared step executor
// ---------------------------------------------------------------------------

/// Execute one iteration of `steps`. On transport error inside a
/// [`Step::Request`], skip the remaining steps of this iteration but
/// still count the error against the task/scenario.
///
/// Latency/bytes for successful responses are folded into `stats`;
/// extractors populate `ctx` so templates in later steps can interpolate
/// the extracted values.
pub(crate) async fn execute_steps<T: Transport>(
    client: &T::Client,
    scenario_id: u16,
    steps: &[Step],
    ctx: &mut ScenarioContext,
    stats: &mut TaskStats,
) {
    for step in steps {
        match step {
            Step::Request(req) => {
                match T::exchange(client, req, ctx).await {
                    Ok(resp) => {
                        process_response(scenario_id, req, &resp, ctx, stats);
                    }
                    Err(e) => {
                        stats.record_error(scenario_id, classify_transport_error(&e));
                        // Don't execute further steps — their templates
                        // may rely on an extract we didn't get to run.
                        break;
                    }
                }
            }
            Step::Pause(d) => compio::time::sleep(*d).await,
            Step::PauseRandom { min, max } => {
                let d = if min == max {
                    *min
                } else {
                    let lo = min.as_nanos() as u64;
                    let hi = max.as_nanos() as u64;
                    let pick = ctx.rng.gen_range(lo..=hi);
                    std::time::Duration::from_nanos(pick)
                };
                compio::time::sleep(d).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Response processing — extracts, assertions, byte accounting
// ---------------------------------------------------------------------------

fn process_response(
    scenario_id: u16,
    req: &crate::plan::RequestPlan,
    resp: &Response,
    ctx: &mut ScenarioContext,
    stats: &mut TaskStats,
) {
    // Status-class error tracking first — 4xx / 5xx are countable
    // errors even though the request completed on the wire.
    let status = resp.status;
    if (400..500).contains(&status) {
        stats.record_error(scenario_id, ErrorKind::Status4xx);
    } else if (500..600).contains(&status) {
        stats.record_error(scenario_id, ErrorKind::Status5xx);
    }

    // Record the successful wire-level exchange (latency/TTFB/bytes).
    stats.record(scenario_id, resp.total, resp.ttfb, resp.bytes_sent, resp.bytes_received);

    // Apply extracts.
    for extract in &req.extract {
        apply_extract(extract, resp, ctx);
    }

    // Apply assertions. Failure increments the counter but doesn't
    // abort the iteration — the request still counted for throughput.
    for check in &req.checks {
        if !check_assertion(check, resp) {
            stats.record_error(scenario_id, ErrorKind::AssertionFailed);
        }
    }
}

fn apply_extract(extract: &Extract, resp: &Response, ctx: &mut ScenarioContext) {
    match extract {
        Extract::Header { name, into } => {
            if let Some(value) = resp.headers.get(name) {
                ctx.set_var(*into, Bytes::copy_from_slice(value.as_bytes()));
            } else {
                ctx.clear_var(*into);
            }
        }
        Extract::StatusCode { into } => {
            // ASCII decimal — same shape as `resp.status.to_string()`.
            let mut buf = [0u8; 5]; // up to 65535
            let mut n = resp.status as u32;
            if n == 0 {
                ctx.set_var(*into, Bytes::from_static(b"0"));
                return;
            }
            let mut i = buf.len();
            while n > 0 {
                i -= 1;
                buf[i] = b'0' + (n % 10) as u8;
                n /= 10;
            }
            ctx.set_var(*into, Bytes::copy_from_slice(&buf[i..]));
        }
    }
}

fn check_assertion(check: &Assertion, resp: &Response) -> bool {
    match check {
        Assertion::StatusEq(code) => resp.status == *code,
        Assertion::StatusIn(codes) => codes.iter().any(|c| *c == resp.status),
        Assertion::LatencyUnder(d) => resp.total < *d,
    }
}

fn classify_transport_error(e: &TransportError) -> ErrorKind {
    match e {
        TransportError::Connect(_) => ErrorKind::Connect,
        TransportError::Timeout => ErrorKind::Timeout,
        TransportError::Protocol(_) => ErrorKind::Read,
        TransportError::Io(_) => ErrorKind::Read,
        TransportError::RequestBuild(_) => ErrorKind::Write,
        TransportError::Tls(_) => ErrorKind::Connect,
    }
}

// ---------------------------------------------------------------------------
// Scenario selection
// ---------------------------------------------------------------------------

/// Pick a scenario index. Phase C strategy: uniform random.
///
/// Task 10 replaces this with weighted-random based on the rate profile
/// (for `Constant(r)` each scenario's weight is `r`; for `Saturate` all
/// scenarios receive equal weight). Keeping the selection logic behind
/// a helper means the open-loop code can specialise without restructuring
/// the worker body.
fn pick_scenario(num_scenarios: usize, ctx: &mut ScenarioContext) -> usize {
    if num_scenarios <= 1 {
        0
    } else {
        ctx.rng.gen_range(0..num_scenarios)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Response;
    use bytes::Bytes;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::time::Duration;

    fn bare_response(status: u16) -> Response {
        Response {
            status,
            headers: HeaderMap::new(),
            body: crate::transport::ResponseBody::Buffered(Bytes::new()),
            bytes_sent: 0,
            bytes_received: 0,
            ttfb: Duration::from_micros(1),
            total: Duration::from_micros(2),
        }
    }

    #[test]
    fn classify_transport_error_covers_all_variants() {
        assert_eq!(
            classify_transport_error(&TransportError::Connect("x".into())),
            ErrorKind::Connect
        );
        assert_eq!(
            classify_transport_error(&TransportError::Timeout),
            ErrorKind::Timeout
        );
        assert_eq!(
            classify_transport_error(&TransportError::Protocol("x".into())),
            ErrorKind::Read
        );
        assert_eq!(
            classify_transport_error(&TransportError::Io(std::io::Error::other("x"))),
            ErrorKind::Read
        );
        assert_eq!(
            classify_transport_error(&TransportError::RequestBuild("x".into())),
            ErrorKind::Write
        );
        assert_eq!(
            classify_transport_error(&TransportError::Tls("x".into())),
            ErrorKind::Connect
        );
    }

    #[test]
    fn check_assertion_status_eq() {
        let resp = bare_response(200);
        assert!(check_assertion(&Assertion::StatusEq(200), &resp));
        assert!(!check_assertion(&Assertion::StatusEq(404), &resp));
    }

    #[test]
    fn check_assertion_status_in() {
        let resp = bare_response(201);
        let codes = smallvec::smallvec![200u16, 201, 204];
        assert!(check_assertion(&Assertion::StatusIn(codes), &resp));
        let none_match = smallvec::smallvec![500u16, 502];
        assert!(!check_assertion(&Assertion::StatusIn(none_match), &resp));
    }

    #[test]
    fn check_assertion_latency_under() {
        let mut resp = bare_response(200);
        resp.total = Duration::from_millis(10);
        assert!(check_assertion(
            &Assertion::LatencyUnder(Duration::from_millis(100)),
            &resp
        ));
        resp.total = Duration::from_millis(200);
        assert!(!check_assertion(
            &Assertion::LatencyUnder(Duration::from_millis(100)),
            &resp
        ));
    }

    #[test]
    fn apply_extract_header_copies_value() {
        use crate::var::{VarRegistry, VarSlot};
        let mut vars = VarRegistry::new();
        let slot = vars.allocate("auth").unwrap();
        let _ = slot;
        let mut ctx = ScenarioContext::new(1, crate::rng::from_seed(1));
        let mut resp = bare_response(200);
        resp.headers.insert(
            HeaderName::from_static("x-token"),
            HeaderValue::from_static("abc"),
        );
        apply_extract(
            &Extract::Header {
                name: HeaderName::from_static("x-token"),
                into: VarSlot(0),
            },
            &resp,
            &mut ctx,
        );
        assert_eq!(
            ctx.get_var(VarSlot(0)).map(|b| b.as_ref()),
            Some(b"abc".as_ref())
        );
    }

    #[test]
    fn apply_extract_status_writes_ascii_decimal() {
        use crate::var::VarSlot;
        let mut ctx = ScenarioContext::new(1, crate::rng::from_seed(1));
        let resp = bare_response(418);
        apply_extract(
            &Extract::StatusCode { into: VarSlot(0) },
            &resp,
            &mut ctx,
        );
        assert_eq!(
            ctx.get_var(VarSlot(0)).map(|b| b.as_ref()),
            Some(b"418".as_ref())
        );
    }

    #[test]
    fn pick_scenario_uniform_in_range() {
        let mut ctx = ScenarioContext::new(0, crate::rng::from_seed(7));
        for _ in 0..100 {
            let s = pick_scenario(5, &mut ctx);
            assert!(s < 5);
        }
        // With one scenario, always 0.
        assert_eq!(pick_scenario(1, &mut ctx), 0);
        assert_eq!(pick_scenario(0, &mut ctx), 0);
    }
}
