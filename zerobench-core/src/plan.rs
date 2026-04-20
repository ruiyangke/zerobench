//! The plan data model.
//!
//! A [`Plan`] is the frozen, thread-shareable description of what the engine
//! should execute. CLI, request-file, and Rhai front-ends all compile down
//! to this type. The rate scheduler → dispatcher → transport pipeline consumes
//! a `Plan` and never inspects the original source.
//!
//! See `docs/design-v0.1.0.md` §3 for the full data model.

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
///
/// `warmup == Duration::ZERO` means "no warmup". See
/// `docs/design-v0.1.0.md` §1 for the full field semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// Scenarios to run, each with its own rate profile and steps. The
    /// engine runs scenarios serially by default (see
    /// `docs/PHILOSOPHY.md` P9).
    pub scenarios: Vec<Scenario>,
    /// Compile-time variable slot allocation — the registry's order fixes
    /// the meaning of every [`VarSlot`] in this plan.
    pub vars: VarRegistry,
    /// Total measurement duration (steady-state, per run). Warmup +
    /// cooldown are separate and excluded from the reported histogram.
    pub duration: Duration,
    /// Warmup phase — requests are fired but stats are discarded. Zero
    /// disables warmup entirely.
    #[serde(default)]
    pub warmup: Duration,
    /// Inter-run cooldown, observed between each of `runs` runs. Lets
    /// TIME_WAIT drain, SYN-retransmit timers settle, TCP
    /// congestion-window caches clear between A₁ and A₂. Default
    /// (`Duration::ZERO`) means no cooldown — `measure` sets 10s.
    #[serde(default)]
    pub cooldown: Duration,
    /// Number of times to execute the full scenario set (with cooldown
    /// between each). `measure` defaults to 3 (for bootstrap CI);
    /// `probe` / `soak` default to 1.
    #[serde(default = "default_runs")]
    pub runs: u32,
    /// Number of OS worker threads used for this run. Informational —
    /// consumed by the terminal reporter's header line. Default 1.
    #[serde(default = "default_threads")]
    pub threads: usize,
    /// Verb dispatch — which mode this Plan executes under. The CLI
    /// sets this from the chosen subcommand; archived plans carry it so
    /// `replay` knows what kind of run produced them.
    #[serde(default)]
    pub mode: Mode,
    /// Human-readable plan identity. Required when archiving is on
    /// (contributes to `url_fingerprint` — see `docs/design-v0.1.0.md`
    /// §7.1). Empty for ephemeral probes.
    #[serde(default)]
    pub name: String,
}

fn default_threads() -> usize {
    1
}

fn default_runs() -> u32 {
    1
}

impl Plan {
    /// Fresh empty plan with a default 30s duration and no warmup. Mode
    /// defaults to [`Mode::Measure`] — the v0.1.0 headline verb.
    pub fn new() -> Self {
        Self {
            scenarios: Vec::new(),
            vars: VarRegistry::new(),
            duration: Duration::from_secs(30),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::default(),
            name: String::new(),
        }
    }

    /// Return a borrow of the *identity* fields — the projection
    /// that should drive `plan_hash` / archive bucketing.
    ///
    /// Fields excluded from identity:
    ///
    /// - `duration`, `warmup`, `cooldown` — time budget, not workload
    /// - `runs`, `threads` — concurrency / repetition settings
    /// - `mode` — verb dispatch (`measure` / `probe` / ...), all share
    ///   the same archive family
    /// - `name` — human label, deliberately excluded so rename doesn't
    ///   split the bucket. `url_fingerprint` already mixes `name` in
    ///   for per-profile grouping.
    ///
    /// Included: scenarios (URLs, headers, steps, rate profiles) and
    /// the variable registry (shape matters — slots resolve by index).
    pub fn identity_projection(&self) -> PlanIdentity<'_> {
        PlanIdentity {
            scenarios: &self.scenarios,
            vars: &self.vars,
        }
    }
}

/// Serialisable identity slice of a [`Plan`], produced by
/// [`Plan::identity_projection`]. Used by fingerprinting to keep
/// archive buckets stable when only run-time settings vary.
#[derive(Debug, Serialize)]
pub struct PlanIdentity<'a> {
    /// Workload: URLs, headers, bodies, assertions, extractions.
    pub scenarios: &'a Vec<Scenario>,
    /// Compile-time variable slot layout.
    pub vars: &'a VarRegistry,
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

/// Which verb is driving this Plan's execution. Determines defaults
/// (duration, warmup, runs, cooldown), dispatch path in the CLI, TUI
/// layout selection, and archive semantics.
///
/// See `docs/PHILOSOPHY.md` §5 (the seven verbs) and
/// `docs/design-v0.1.0.md` §2 (the dispatcher).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Mode {
    /// Smoke test. 5s default, 1 run, no archive, no calibration gate.
    Probe,
    /// Client-side ceiling measurement against in-process loopback.
    Calibrate,
    /// Rigorous steady-state measurement. 60s × 3 runs default, with
    /// 15s warmup, 10s cooldown, auto-archive, auto-compare vs
    /// baseline. The v0.1.0 headline verb.
    Measure,
    /// Long-duration `measure` — 5min default, 1 run. Same code path as
    /// `Measure`, different defaults; no in-tool leak/drift analysis.
    Soak,
    /// Ramp offered rate and report the (rate, p99) curve + the knee.
    Curve {
        /// Starting offered rate (req/s).
        from_rate: f64,
        /// Final offered rate (req/s).
        to_rate: f64,
        /// Linear ramp duration.
        ramp_duration: Duration,
        /// Criterion for "the knee".
        knee: KneeCriterion,
    },
    /// Two-target regression comparison with bootstrap CI.
    Compare {
        /// Scheduling between A and B runs.
        schedule: CompareSchedule,
    },
    /// Archive-only: compare two stored `result.json` artifacts.
    Diff,
}

impl Default for Mode {
    fn default() -> Self {
        Self::Measure
    }
}

/// Knee-detection policy for `Mode::Curve`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KneeCriterion {
    /// First rate where p99 > factor × p99 at the lowest sampled rate.
    P99Ratio {
        /// Multiplier — 2.0 means "p99 doubled".
        factor: f64,
    },
    /// First rate where error rate ≥ threshold, sustained for at least
    /// `sustained`. Matches philosophy's "≥3s" default.
    ErrorRate {
        /// 0.0–1.0 fraction.
        threshold: f64,
        /// Minimum consecutive sustain duration.
        sustained: Duration,
    },
}

/// How `compare` interleaves runs across the two sides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareSchedule {
    /// Round-robin with cooldown between every run (default; minimises
    /// per-side system-drift correlation). See `docs/PHILOSOPHY.md`
    /// §5.3.
    Interleaved,
    /// All A runs, then all B runs. For targets that share stateful
    /// resources incompatible with quick context-switching.
    Serial,
}

impl Default for CompareSchedule {
    fn default() -> Self {
        Self::Interleaved
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
///
/// SSE and WebSocket are modelled as protocol-native workloads
/// (`SseHold`, `WsEchoRtt`, etc.) rather than single-shot HTTP
/// requests — see `docs/PHILOSOPHY.md` §4 for the semantic rationale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Step {
    // -----------------------------------------------------------------
    // HTTP
    // -----------------------------------------------------------------
    /// Send an HTTP request and optionally extract/assert on the response.
    Request(RequestPlan),
    /// Open a fresh TCP (+TLS +HTTP) connection per request — no pool
    /// reuse. Measures accept + handshake throughput distinct from
    /// steady-state pool performance. See
    /// `docs/design-v0.1.0.md` §3.1 (`HttpColdConnect`).
    HttpColdConnect(ColdConnectPlan),

    // -----------------------------------------------------------------
    // SSE — server-driven long-lived sessions. Op = one event.
    // -----------------------------------------------------------------
    /// Open N concurrent SSE subscribers and hold them for the
    /// configured duration; measure events/s, event-gap p99, TTFB,
    /// concurrent-count stability. The canonical SSE workload.
    SseHold(SseHoldPlan),
    /// N subscribers + an external broadcast trigger; measure
    /// server → subscriber broadcast latency.
    SseFanout(SseFanoutPlan),
    /// N subscribers with a kill-rate; measures reconnect success and
    /// `Last-Event-ID` propagation per the EventSource spec.
    SseReconnectStorm(SseReconnectStormPlan),

    // -----------------------------------------------------------------
    // WebSocket — bidirectional long-lived sessions.
    // -----------------------------------------------------------------
    /// Idle-capacity test: N connections held open with heartbeat.
    /// Metric: conns-held, conn-drop rate, handshake latency.
    WsHold(WsHoldPlan),
    /// Client-initiated message with explicit echo correlation
    /// (default `ping_pong`); measures RTT over a persistent
    /// connection.
    WsEchoRtt(WsEchoRttPlan),
    /// Server-initiated push RTT — client only reads, measures
    /// inter-message gap and ordering.
    WsServerPushRtt(WsServerPushRttPlan),
    /// Broadcast RTT analogous to `SseFanout`.
    WsFanout(WsFanoutPlan),

    // -----------------------------------------------------------------
    // Control flow
    // -----------------------------------------------------------------
    /// Sleep a fixed duration before the next step.
    Pause(Duration),
    /// Sleep a uniformly-random duration in `[min, max]`.
    PauseRandom {
        /// Minimum sleep.
        min: Duration,
        /// Maximum sleep.
        max: Duration,
    },
}

// ---------------------------------------------------------------------------
// v0.1.0 protocol-native plan structs.
//
// Each struct is the compiled form of one Step variant. URLs and headers
// are `Template`s so run-time variable interpolation still works. All are
// `Send + Sync + Clone` once built.
// ---------------------------------------------------------------------------

/// Cold-connection HTTP benchmark — one request per TCP/TLS/HTTP
/// connection, pool bypassed. Measures handshake throughput.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdConnectPlan {
    /// The request sent on each freshly-opened connection. Its body,
    /// headers, and assertions behave identically to a normal
    /// [`RequestPlan`]; the only difference is connection lifetime.
    pub request: RequestPlan,
}

/// SSE "hold N subscribers for D seconds" — the canonical SSE workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseHoldPlan {
    /// Target URL.
    pub url: Template,
    /// Extra headers beyond `Accept: text/event-stream`.
    pub headers: SmallVec<[(Template, Template); 4]>,
    /// Number of concurrent subscribers to hold open.
    pub subscribers: u32,
    /// How long to keep each subscriber connected before closing.
    pub hold_for: Duration,
    /// Whether to follow the EventSource reconnect protocol (server-
    /// supplied `retry:` field). Default `true`.
    #[serde(default = "default_true")]
    pub reconnect: bool,
}

/// SSE broadcast-latency measurement — N subscribers plus a trigger
/// that provokes a server-side broadcast event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseFanoutPlan {
    /// Subscriber-side workload (N, hold duration, reconnect policy).
    pub subscribers: SseHoldPlan,
    /// How to provoke each broadcast.
    pub trigger: TriggerSpec,
    /// Which accuracy regime to use. See `PHILOSOPHY.md` §4.3.
    pub mode: FanoutMode,
}

/// SSE reconnect-storm test — kill a fraction of subscribers per second,
/// observe reconnect success and `Last-Event-ID` propagation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseReconnectStormPlan {
    /// Subscriber-side workload.
    pub subscribers: SseHoldPlan,
    /// Fraction of subscribers to kill per second (0.0–1.0).
    pub kill_rate_per_s: f64,
    /// Whether to assert `Last-Event-ID` is propagated to the
    /// reconnecting subscriber's next request.
    #[serde(default = "default_true")]
    pub verify_last_event_id: bool,
}

/// WebSocket idle-capacity test: N connections held open with a
/// heartbeat to prevent proxy idle-timeout closure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsHoldPlan {
    /// Target WS URL.
    pub url: Template,
    /// Extra headers beyond the standard Upgrade/Sec-WebSocket-*.
    pub headers: SmallVec<[(Template, Template); 4]>,
    /// Number of concurrent connections.
    pub connections: u32,
    /// Heartbeat interval — default 25s leaves margin against common
    /// 30s/60s proxy idle timeouts.
    pub heartbeat: Duration,
    /// Frame type used for the heartbeat.
    #[serde(default)]
    pub heartbeat_frame: HeartbeatFrame,
    /// How long to hold each connection.
    pub hold_for: Duration,
}

/// WS client-initiated RTT test. Client sends a payload, waits for a
/// correlated echo, measures round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsEchoRttPlan {
    /// Target WS URL.
    pub url: Template,
    /// Extra headers.
    pub headers: SmallVec<[(Template, Template); 4]>,
    /// Number of concurrent connections.
    pub connections: u32,
    /// Messages per second per connection.
    pub msg_rate_per_conn: f64,
    /// How to match server echo to our send.
    pub correlate: CorrelateStrategy,
    /// Application payload. Ignored for `Correlate::PingPong` (the
    /// Ping/Pong frame body is a 16-byte monotonic id).
    pub payload: Template,
}

/// WS server-initiated push test. Client only reads; measures
/// inter-message arrival gap and ordering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsServerPushRttPlan {
    /// Target WS URL.
    pub url: Template,
    /// Extra headers.
    pub headers: SmallVec<[(Template, Template); 4]>,
    /// Number of concurrent connections.
    pub connections: u32,
    /// Expected server push rate (per conn). Used only as an anomaly
    /// threshold — if actual << expected, tool flags a stall.
    pub expected_rate_per_conn: f64,
    /// How long to keep each connection open.
    pub hold_for: Duration,
}

/// WS broadcast-latency test. Analogous to SSE fanout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsFanoutPlan {
    /// Subscriber-side: N connections held open, reading only.
    pub subscribers: WsHoldPlan,
    /// How to provoke each broadcast.
    pub trigger: TriggerSpec,
    /// Which accuracy regime to use.
    pub mode: FanoutMode,
}

/// How a fanout scenario provokes a server-side broadcast event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TriggerSpec {
    /// HTTP POST to a user-supplied URL, optional body.
    HttpPost {
        /// Trigger URL.
        url: Template,
        /// Optional POST body.
        #[serde(default)]
        body: Option<BodySource>,
    },
    /// A designated WS connection (subscriber #0) sends the trigger
    /// frame; server is expected to broadcast to the rest.
    DedicatedWsConnection {
        /// Frame payload sent on each trigger.
        payload: Template,
    },
}

/// Accuracy regime for fanout measurements.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FanoutMode {
    /// Server embeds `emit_ns` in the broadcast payload (requires
    /// server cooperation — see `PHILOSOPHY.md` §4.3 clock-probe).
    /// Most accurate when skew is small relative to RTT.
    Timestamp {
        /// JSON field name in the payload to read the emit nanos from.
        /// Default `"emit_ns"`.
        #[serde(default = "default_emit_field")]
        emit_field: String,
    },
    /// Proxy: measures the time from the trigger HTTP response 2xx to
    /// the first broadcast event. Subject to the approximation caveats
    /// documented in `PHILOSOPHY.md` §4.3 and §15 Q4.
    #[default]
    TriggerRtt,
}

/// Heartbeat frame shape for `WsHoldPlan`. `Ping` is the zero-intrusion
/// default (RFC 6455 §5.5.2 control frame); `TextApp` is the fallback
/// for servers that don't honour Ping frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatFrame {
    /// WebSocket Ping frame (opcode 0x9) — RFC 6455 compliant servers
    /// echo with Pong (0xA) automatically.
    Ping,
    /// Application-level text frame. Used when the server-side code
    /// doesn't respond to Ping (some older Socket.IO / proxies).
    TextApp,
}

impl Default for HeartbeatFrame {
    fn default() -> Self {
        Self::Ping
    }
}

/// Strategy for matching a server echo to the client's send in
/// `WsEchoRttPlan`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CorrelateStrategy {
    /// Zero-intrusion default: client sends a WebSocket Ping frame
    /// (opcode 0x9) carrying a 16-byte monotonic id; RFC 6455 compliant
    /// servers MUST reply with Pong (0xA) echoing the payload verbatim.
    PingPong,
    /// Prepend a 16-byte monotonic id to the application text-frame
    /// payload; match echo by prefix. Use when the app-layer format
    /// tolerates a prefix and the server echoes verbatim.
    #[default]
    MonotonicIdPrepend,
    /// Echo must contain the literal marker. Used when the server
    /// transforms payload but preserves a known substring.
    PayloadSubstring {
        /// The literal substring that must appear in each echo.
        marker: String,
    },
    /// Accept any next server-initiated text-frame. Only valid when
    /// no heartbeat or server-push frames can interleave.
    FirstTextFrame,
}

fn default_true() -> bool {
    true
}

fn default_emit_field() -> String {
    "emit_ns".to_string()
}

/// Wire-level protocol a scenario speaks. Inferred from the first
/// non-Pause step; used by the CLI dispatcher to route scenarios to the
/// right backend (HTTP / SSE / WS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// HTTP request/response (via mio_h1 or mio_h2).
    Http,
    /// Server-Sent Events (via zerobench-backends::sse).
    Sse,
    /// WebSocket (via zerobench-backends::ws).
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
                Step::Request(_) | Step::HttpColdConnect(_) => return Protocol::Http,
                Step::SseHold(_) | Step::SseFanout(_) | Step::SseReconnectStorm(_) => {
                    return Protocol::Sse
                }
                Step::WsHold(_)
                | Step::WsEchoRtt(_)
                | Step::WsServerPushRtt(_)
                | Step::WsFanout(_) => return Protocol::Ws,
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
    /// Default `false` — the buffered path is the baseline.
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
// helpers we need. Plans only ever round-trip through JSON for
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
            vec![Step::SseHold(SseHoldPlan {
                url: lit("http://x/events"),
                headers: SmallVec::new(),
                subscribers: 1,
                hold_for: Duration::from_secs(1),
                reconnect: false,
            })],
        );
        assert_eq!(sc.protocol(), Protocol::Sse);
    }

    #[test]
    fn protocol_ws_when_first_step_is_ws() {
        let sc = Scenario::new(
            "w",
            vec![Step::WsEchoRtt(WsEchoRttPlan {
                url: lit("ws://x/"),
                headers: SmallVec::new(),
                connections: 1,
                msg_rate_per_conn: 1.0,
                correlate: CorrelateStrategy::PingPong,
                payload: lit("ping"),
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
                Step::WsEchoRtt(WsEchoRttPlan {
                    url: lit("ws://x/"),
                    headers: SmallVec::new(),
                    connections: 1,
                    msg_rate_per_conn: 1.0,
                    correlate: CorrelateStrategy::PingPong,
                    payload: lit("hi"),
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
