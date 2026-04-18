//! The Phase 1 data model.
//!
//! A [`Plan`] is the frozen, thread-shareable description of what the engine
//! should execute. CLI, request-file, and Rhai front-ends all compile down
//! to this type. Phase 2 (rate scheduler → dispatcher → transport) consumes
//! a `Plan` and never inspects the original source.
//!
//! See `docs/design.md` §3 for the full data model. This module implements
//! the v0.0.1 subset: Task 1 lands the skeleton; later tasks extend
//! [`BodySource`], [`Extract`], [`Assertion`] with richer variants, and
//! swap [`RateProfile`] from a placeholder into a real scheduler input.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderName, Method};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::template::Template;
use crate::var::{VarRegistry, VarSlot};

/// A complete benchmark plan.
///
/// Cheap to [`Clone`] — every field is either a small owned value or a
/// reference-counted buffer. Workers receive their own clones and never
/// mutate the plan during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// Scenarios to run, each with its own rate profile and steps. The
    /// engine runs all scenarios concurrently.
    pub scenarios: Vec<Scenario>,
    /// Compile-time variable slot allocation — the registry's order fixes
    /// the meaning of every [`VarSlot`] in this plan.
    pub vars: VarRegistry,
    /// Total measurement duration. Warmup is in addition to this.
    pub duration: Duration,
    /// Optional warmup phase — requests are fired but stats discarded.
    pub warmup: Option<Duration>,
    /// Number of OS worker threads used for this run. Informational —
    /// consumed by the terminal reporter's header line. Default 1.
    #[serde(default = "default_threads")]
    pub threads: usize,
}

fn default_threads() -> usize {
    1
}

impl Plan {
    /// Fresh empty plan with a default 30s duration and no warmup.
    pub fn new() -> Self {
        Self {
            scenarios: Vec::new(),
            vars: VarRegistry::new(),
            duration: Duration::from_secs(30),
            warmup: None,
            threads: 1,
        }
    }
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

/// One named traffic stream — a sequence of steps executed top-to-bottom
/// per iteration, emitted at the scenario's [`RateProfile`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    /// Human-readable name used in reports and error messages.
    pub name: String,
    /// Placeholder until Task 10 lands the real rate-profile type. Filled
    /// in with a single-variant enum so type sites compile unchanged.
    pub rate: RateProfile,
    /// Steps executed in order per iteration; on error, execution of the
    /// remaining steps for that iteration is skipped.
    pub steps: Vec<Step>,
}

impl Scenario {
    /// Construct a scenario with the given name and steps; rate profile
    /// defaults to [`RateProfile::Saturate`] with 50 concurrent tasks —
    /// sensible for plan builders that don't know better (saturate is
    /// the fallback mode; open-loop requires an explicit rate).
    pub fn new(name: impl Into<String>, steps: Vec<Step>) -> Self {
        Self {
            name: name.into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps,
        }
    }
}

/// Per-scenario traffic shape.
///
/// Consumed by the Task-10 rate scheduler for open-loop modes
/// ([`Constant`](Self::Constant) / [`Ramp`](Self::Ramp) /
/// [`Stepped`](Self::Stepped)); the closed-loop
/// [`Saturate`](Self::Saturate) variant is the
/// "fill the pipe with N concurrent tasks" fallback used when the user
/// passes `--saturate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RateProfile {
    /// Fixed rate in requests per second.
    Constant(f64),
    /// Linear ramp from `from` to `to` over `over`.
    Ramp {
        /// Starting rate in req/s.
        from: f64,
        /// Ending rate in req/s.
        to: f64,
        /// Duration over which the ramp completes; the run continues at
        /// `to` thereafter.
        over: Duration,
    },
    /// Stepped rate changes at absolute offsets from the run start.
    /// Each tuple is `(offset_from_start, new_rate)`; the first entry
    /// applies at `0s`.
    Stepped(Vec<(Duration, f64)>),
    /// Closed-loop saturation: N persistent tasks, each looping
    /// request-then-response. `max_concurrency` is the task count.
    Saturate {
        /// Number of concurrent worker tasks.
        max_concurrency: usize,
    },
}

impl RateProfile {
    /// Return a new profile with all rates multiplied by `factor`.
    ///
    /// Used by the multi-threaded open-loop dispatcher to split the
    /// target rate evenly across threads (each thread runs at
    /// `rate / num_threads`).
    pub fn scale(&self, factor: f64) -> Self {
        match self {
            Self::Constant(rps) => Self::Constant(rps * factor),
            Self::Ramp { from, to, over } => Self::Ramp {
                from: from * factor,
                to: to * factor,
                over: *over,
            },
            Self::Stepped(steps) => Self::Stepped(
                steps.iter().map(|(d, r)| (*d, r * factor)).collect(),
            ),
            Self::Saturate { max_concurrency } => Self::Saturate {
                max_concurrency: ((*max_concurrency as f64 * factor).ceil() as usize).max(1),
            },
        }
    }
}

/// One unit of work inside a scenario iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Step {
    /// Send an HTTP request and optionally extract/assert on the response.
    Request(RequestPlan),
    /// Open an SSE stream and read chunks until completion.
    SseStream(SsePlan),
    /// Open a WebSocket, send one message, receive one message (one round).
    WsRound(WsRoundPlan),
    /// Sleep a fixed duration before the next step.
    Pause(Duration),
    /// Sleep a uniformly-random duration in `[min, max]`.
    PauseRandom { min: Duration, max: Duration },
}

/// Compiled SSE stream plan. One iteration opens one stream, reads chunks
/// until the server closes or `expect_chunks` is satisfied, then records
/// the stream as completed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsePlan {
    /// Target URL — a template so `{{env:HOST}}/events/{{var:id}}` works.
    pub url: Template,
    /// Extra HTTP headers beyond the protocol-mandated `Accept:
    /// text/event-stream`. Both sides are templates.
    pub headers: SmallVec<[(Template, Template); 4]>,
    /// Assertion: the stream must emit at least this many data events
    /// before closing. `None` = no minimum, just count whatever arrives.
    pub expect_chunks: Option<usize>,
}

/// Compiled WebSocket round plan. One iteration opens a WS connection,
/// sends `message`, receives one reply, then closes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsRoundPlan {
    /// Target URL — `ws://` or `wss://`. Templates are allowed in the
    /// path/query (see [`crate::template::Template`]).
    pub url: Template,
    /// Extra HTTP headers beyond the protocol-mandated Upgrade/Sec-*
    /// handshake headers. Both sides are templates.
    pub headers: SmallVec<[(Template, Template); 4]>,
    /// Text frame payload sent on every iteration.
    pub message: Template,
}

/// Wire-level protocol a scenario speaks. Inferred from the first
/// non-Pause step; used by the CLI dispatcher to route scenarios to the
/// right backend (HTTP / SSE / WS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// HTTP request/response (via mio_h1 or mio_h2).
    Http,
    /// Server-Sent Events (via zerobench-sse).
    Sse,
    /// WebSocket (via zerobench-ws).
    Ws,
}

impl Scenario {
    /// Infer this scenario's protocol from the first non-Pause step.
    ///
    /// Mixed-protocol scenarios are out of scope for Tier 1 — the first
    /// wire step wins. An empty-or-pauses-only scenario defaults to
    /// [`Protocol::Http`] so it's routed to a backend that silently
    /// skips it (the HTTP backend's `pick_scenario` filters scenarios
    /// that have no Request step).
    pub fn protocol(&self) -> Protocol {
        for step in &self.steps {
            match step {
                Step::Request(_) => return Protocol::Http,
                Step::SseStream(_) => return Protocol::Sse,
                Step::WsRound(_) => return Protocol::Ws,
                Step::Pause(_) | Step::PauseRandom { .. } => continue,
            }
        }
        Protocol::Http
    }
}

/// A single request's compiled description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPlan {
    /// HTTP method (GET/POST/...).
    #[serde(with = "http_serde::method")]
    pub method: Method,
    /// Target URL — a template so `{{env:HOST}}/api/{{var:id}}` works.
    pub url: Template,
    /// Header name/value pairs. Both sides are templates because cookies
    /// and auth tokens frequently interpolate extracted vars.
    pub headers: SmallVec<[(Template, Template); 8]>,
    /// Optional body source. `None` = empty body.
    pub body: Option<BodySource>,
    /// Response extractors applied after the body is received; they write
    /// into [`VarSlot`]s declared in the plan's registry.
    pub extract: Vec<Extract>,
    /// Post-response assertions. Failure increments
    /// `errors.assertion_failed` but does not abort the scenario.
    pub checks: Vec<Assertion>,
    /// The caller wants the response body delivered as a stream rather
    /// than buffered. Used by SSE plans so the runner can time each chunk
    /// as it arrives.
    ///
    /// Default `false` — the buffered path is the v0.0.1 baseline.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub expect_streaming: bool,
}

impl RequestPlan {
    /// Construct a minimal GET request against the given URL template.
    /// Headers/body/extract/checks start empty.
    pub fn get(url: Template) -> Self {
        Self {
            method: Method::GET,
            url,
            headers: SmallVec::new(),
            body: None,
            extract: Vec::new(),
            checks: Vec::new(),
            expect_streaming: false,
        }
    }

    /// `true` if every template in this plan (URL, headers, body) is
    /// fully static — no `{{...}}` parts. A static plan can be
    /// pre-built once into wire bytes and reused without per-request
    /// template expansion.
    pub fn is_static(&self) -> bool {
        if !self.url.is_static() {
            return false;
        }
        for (name, val) in &self.headers {
            if !name.is_static() || !val.is_static() {
                return false;
            }
        }
        match &self.body {
            Some(BodySource::Template(t)) => t.is_static(),
            _ => true,
        }
    }
}

/// How the request body is produced per iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BodySource {
    /// Pre-encoded static bytes — no `{{...}}` in the source, so we skip
    /// the template engine entirely.
    Static(Bytes),
    /// Template expanded per iteration into a scratch buffer.
    Template(Template),
    // File / FilePool variants land with the request-file parser (Task 11).
}

/// Post-response extraction. Written value is stored in the scenario
/// context at the named slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Extract {
    /// Copy a response header value into `into`. Missing header → slot set
    /// to `None`.
    Header {
        #[serde(with = "http_serde::header_name")]
        name: HeaderName,
        into: VarSlot,
    },
    /// Write the numeric status code (as ASCII decimal bytes) into `into`.
    StatusCode { into: VarSlot },
    // JsonPath / RegexBody variants land in later tasks.
}

/// Post-response check. Failure is recorded but non-fatal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Assertion {
    /// Exact status-code match.
    StatusEq(u16),
    /// Status must equal one of these codes.
    StatusIn(SmallVec<[u16; 4]>),
    /// Total request latency must be below this duration.
    LatencyUnder(Duration),
    // BodyContains / JsonEq variants land with richer assertions.
}

// ---------------------------------------------------------------------------
// http_serde — inline, tiny helpers so we don't need a new workspace dep.
// ---------------------------------------------------------------------------
//
// `http::Method` and `http::HeaderName` don't implement serde by default.
// Instead of pulling the whole `http-serde` crate, we inline the narrow
// helpers we need. Phase 1 plans only ever round-trip through JSON for
// the diff tool (Task 13) and debug logging; this is adequate.

mod http_serde {
    pub mod method {
        use http::Method;
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        pub fn serialize<S: Serializer>(m: &Method, s: S) -> Result<S::Ok, S::Error> {
            m.as_str().serialize(s)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Method, D::Error> {
            let s = <&str>::deserialize(d)?;
            s.parse::<Method>().map_err(serde::de::Error::custom)
        }
    }

    pub mod header_name {
        use http::HeaderName;
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        pub fn serialize<S: Serializer>(n: &HeaderName, s: S) -> Result<S::Ok, S::Error> {
            n.as_str().serialize(s)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<HeaderName, D::Error> {
            let s = <String>::deserialize(d)?;
            HeaderName::from_bytes(s.as_bytes()).map_err(serde::de::Error::custom)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_constant() {
        let p = RateProfile::Constant(1000.0);
        match p.scale(0.25) {
            RateProfile::Constant(r) => assert!((r - 250.0).abs() < 1e-9),
            other => panic!("expected Constant, got {other:?}"),
        }
    }

    #[test]
    fn scale_ramp() {
        let p = RateProfile::Ramp {
            from: 100.0,
            to: 1000.0,
            over: Duration::from_secs(10),
        };
        match p.scale(0.5) {
            RateProfile::Ramp { from, to, over } => {
                assert!((from - 50.0).abs() < 1e-9);
                assert!((to - 500.0).abs() < 1e-9);
                assert_eq!(over, Duration::from_secs(10));
            }
            other => panic!("expected Ramp, got {other:?}"),
        }
    }

    #[test]
    fn scale_stepped() {
        let p = RateProfile::Stepped(vec![
            (Duration::from_secs(0), 100.0),
            (Duration::from_secs(5), 500.0),
        ]);
        match p.scale(0.25) {
            RateProfile::Stepped(steps) => {
                assert_eq!(steps.len(), 2);
                assert!((steps[0].1 - 25.0).abs() < 1e-9);
                assert!((steps[1].1 - 125.0).abs() < 1e-9);
                // Durations are preserved.
                assert_eq!(steps[0].0, Duration::from_secs(0));
                assert_eq!(steps[1].0, Duration::from_secs(5));
            }
            other => panic!("expected Stepped, got {other:?}"),
        }
    }

    #[test]
    fn scale_saturate() {
        let p = RateProfile::Saturate { max_concurrency: 100 };
        match p.scale(0.25) {
            RateProfile::Saturate { max_concurrency } => {
                assert_eq!(max_concurrency, 25);
            }
            other => panic!("expected Saturate, got {other:?}"),
        }
        // Minimum 1.
        let p2 = RateProfile::Saturate { max_concurrency: 1 };
        match p2.scale(0.01) {
            RateProfile::Saturate { max_concurrency } => {
                assert_eq!(max_concurrency, 1);
            }
            other => panic!("expected Saturate, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Protocol inference
    // -----------------------------------------------------------------

    fn lit(s: &str) -> Template {
        Template::literal(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn protocol_http_when_first_step_is_request() {
        let sc = Scenario::new(
            "h",
            vec![Step::Request(RequestPlan::get(lit("/")))],
        );
        assert_eq!(sc.protocol(), Protocol::Http);
    }

    #[test]
    fn protocol_sse_when_first_step_is_sse() {
        let sc = Scenario::new(
            "s",
            vec![Step::SseStream(SsePlan {
                url: lit("http://x/events"),
                headers: SmallVec::new(),
                expect_chunks: None,
            })],
        );
        assert_eq!(sc.protocol(), Protocol::Sse);
    }

    #[test]
    fn protocol_ws_when_first_step_is_ws() {
        let sc = Scenario::new(
            "w",
            vec![Step::WsRound(WsRoundPlan {
                url: lit("ws://x/"),
                headers: SmallVec::new(),
                message: lit("ping"),
            })],
        );
        assert_eq!(sc.protocol(), Protocol::Ws);
    }

    #[test]
    fn protocol_skips_pauses_and_picks_first_wire_step() {
        let sc = Scenario::new(
            "mixed",
            vec![
                Step::Pause(Duration::from_millis(5)),
                Step::PauseRandom {
                    min: Duration::from_millis(1),
                    max: Duration::from_millis(2),
                },
                Step::WsRound(WsRoundPlan {
                    url: lit("ws://x/"),
                    headers: SmallVec::new(),
                    message: lit("hi"),
                }),
            ],
        );
        assert_eq!(sc.protocol(), Protocol::Ws);
    }

    #[test]
    fn protocol_defaults_to_http_for_empty_scenario() {
        let sc = Scenario::new("empty", vec![]);
        assert_eq!(sc.protocol(), Protocol::Http);
    }
}
