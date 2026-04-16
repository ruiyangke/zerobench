//! Shared step-execution helpers used by both dispatchers.
//!
//! The saturate-mode dispatcher (`dispatcher.rs`) and the open-loop
//! dispatcher (`rate.rs`) both need to post-process a completed
//! [`Response`] in the same way: apply extracts, run assertions, update
//! error counters, and classify transport failures into [`ErrorKind`]s.
//!
//! Keeping one implementation prevents drift (e.g. a prior copy in
//! `rate.rs` allocated a `String` for every `Extract::StatusCode`; the
//! shared version uses a 5-byte stack buffer) and means new variants on
//! [`Extract`] / [`Assertion`] / [`TransportError`] only need updating in
//! one place.
//!
//! All items are `pub(crate)`: they're internal implementation detail
//! shared between the two dispatcher modules.

use std::sync::Arc;

use bytes::Bytes;

use crate::live_snapshot::LiveSnapshot;
use crate::plan::{Assertion, Extract, RequestPlan};
use crate::scenario_context::ScenarioContext;
use crate::stats::{ErrorKind, TaskStats};
use crate::transport::{Response, TransportError};

/// Post-process a completed response.
///
/// - Tallies 4xx/5xx into the task's per-kind error counters.
/// - Records latency/TTFB/bytes into the task stats.
/// - If `live` is `Some`, also feeds the sample into the shared
///   [`LiveSnapshot`] so the per-second JSONL ticker sees it.
/// - Applies extracts so later steps' templates can reference them.
/// - Runs assertions; a failed assertion bumps
///   [`ErrorKind::AssertionFailed`] but does not abort the iteration.
pub(crate) fn process_response(
    scenario_id: u16,
    req: &RequestPlan,
    resp: &Response,
    ctx: &mut ScenarioContext,
    stats: &mut TaskStats,
    live: Option<&Arc<LiveSnapshot>>,
) {
    // Status-class error tracking first — 4xx / 5xx are countable
    // errors even though the request completed on the wire.
    let status = resp.status;
    if (400..500).contains(&status) {
        stats.record_error(scenario_id, ErrorKind::Status4xx);
        if let Some(l) = live {
            l.record_error(ErrorKind::Status4xx);
        }
    } else if (500..600).contains(&status) {
        stats.record_error(scenario_id, ErrorKind::Status5xx);
        if let Some(l) = live {
            l.record_error(ErrorKind::Status5xx);
        }
    }

    // Record the successful wire-level exchange (latency/TTFB/bytes).
    stats.record(
        scenario_id,
        resp.total,
        resp.ttfb,
        resp.bytes_sent,
        resp.bytes_received,
    );
    if let Some(l) = live {
        l.record(
            resp.total.as_nanos().min(u128::from(u64::MAX)) as u64,
            resp.bytes_sent,
            resp.bytes_received,
        );
    }

    // Apply extracts.
    for extract in &req.extract {
        apply_extract(extract, resp, ctx);
    }

    // Apply assertions. Failure increments the counter but doesn't
    // abort the iteration — the request still counted for throughput.
    for check in &req.checks {
        if !check_assertion(check, resp) {
            stats.record_error(scenario_id, ErrorKind::AssertionFailed);
            if let Some(l) = live {
                l.record_error(ErrorKind::AssertionFailed);
            }
        }
    }
}

/// Report a transport-level error to the shared [`LiveSnapshot`], in
/// addition to the usual task-stats record. Caller is responsible for
/// having already classified the error via [`classify_transport_error`]
/// and passed the category to `TaskStats::record_error`.
pub(crate) fn record_transport_error_live(live: &Arc<LiveSnapshot>, kind: ErrorKind) {
    live.record_error(kind);
}

/// Write an [`Extract`]'s result into the scenario context.
///
/// `StatusCode` uses a 5-byte stack buffer and hand-written ASCII
/// decimal conversion — no heap allocation on the hot path.
pub(crate) fn apply_extract(extract: &Extract, resp: &Response, ctx: &mut ScenarioContext) {
    match extract {
        Extract::Header { name, into } => {
            if let Some(value) = resp.headers.get(name) {
                ctx.set_var(*into, Bytes::copy_from_slice(value.as_bytes()));
            } else {
                ctx.clear_var(*into);
            }
        }
        Extract::StatusCode { into } => {
            // ASCII decimal — same shape as `resp.status.to_string()`,
            // but zero-alloc (stack buffer + `Bytes::copy_from_slice`).
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

/// Evaluate an assertion against a response. Returns `true` on pass.
pub(crate) fn check_assertion(check: &Assertion, resp: &Response) -> bool {
    match check {
        Assertion::StatusEq(code) => resp.status == *code,
        Assertion::StatusIn(codes) => codes.iter().any(|c| *c == resp.status),
        Assertion::LatencyUnder(d) => resp.total < *d,
    }
}

/// Map a [`TransportError`] into the coarse [`ErrorKind`] the report
/// panel groups by. TLS and connect errors both surface as "connect
/// failed" because from the benchmark's perspective they prevent the
/// request from going on-wire; protocol and IO errors are lumped as
/// "read" because they happen mid-exchange.
pub(crate) fn classify_transport_error(e: &TransportError) -> ErrorKind {
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
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{Response, ResponseBody};
    use bytes::Bytes;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::time::Duration;

    fn bare_response(status: u16) -> Response {
        Response {
            status,
            headers: HeaderMap::new(),
            body: ResponseBody::Buffered(Bytes::new()),
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
        apply_extract(&Extract::StatusCode { into: VarSlot(0) }, &resp, &mut ctx);
        assert_eq!(
            ctx.get_var(VarSlot(0)).map(|b| b.as_ref()),
            Some(b"418".as_ref())
        );
    }

    #[test]
    fn apply_extract_status_zero_is_handled() {
        use crate::var::VarSlot;
        let mut ctx = ScenarioContext::new(1, crate::rng::from_seed(1));
        let resp = bare_response(0);
        apply_extract(&Extract::StatusCode { into: VarSlot(0) }, &resp, &mut ctx);
        assert_eq!(
            ctx.get_var(VarSlot(0)).map(|b| b.as_ref()),
            Some(b"0".as_ref())
        );
    }
}
