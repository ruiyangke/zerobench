//! Typed plan construction — the single entry point both the CLI
//! translator (`plan_from_cli.rs`, `verbs/measure.rs`, `verbs/curve.rs`)
//! and the Rhai DSL (`zerobench_dsl::builders`) call through.
//!
//! Adding a field to a `*Plan` struct happens here; both CLI flags
//! and DSL methods feed the same constructors.
//!
//! No Rhai, no clap, no IO. Pure Rust typed setters. Front-ends
//! translate their own input model into method calls.
//!
//! # API shape
//!
//! - [`PlanBuilder`] — top-level aggregator. Caller sets the run-time
//!   knobs (duration, warmup, cooldown, runs, threads, mode) and pushes
//!   scenarios. `finalize()` produces a [`Plan`].
//! - `scenario_*` free functions — each packs a single [`Scenario`]
//!   around one concrete [`Step`] variant. Required per-step fields are
//!   plain arguments (no `Option<T>` for required data). The front-end
//!   is responsible for validating / defaulting its own inputs before
//!   calling.
//!
//! # Why free functions and not `PlanBuilder::add_foo(...)` methods?
//!
//! Two reasons. First, the argument lists are long and protocol-specific;
//! pinning them to `&mut PlanBuilder` adds no real leverage. Second, the
//! free-function shape lets the DSL pre-build a [`Scenario`], stash it in
//! a `Vec<StepSource>`, and push it into the aggregator later —
//! exactly the pattern the Rhai-side builder code already uses.

use std::time::Duration;

use smallvec::SmallVec;

use crate::plan::{
    ColdConnectPlan, CorrelateStrategy, FanoutMode, HeartbeatFrame, Mode, Plan, RateProfile,
    RequestPlan, Scenario, SseFanoutPlan, SseHoldPlan, SseReconnectStormPlan, Step, TriggerSpec,
    WsEchoRttPlan, WsFanoutPlan, WsHoldPlan, WsServerPushRttPlan,
};
use crate::template::Template;
use crate::var::VarRegistry;

// ---------------------------------------------------------------------------
// PlanBuilder
// ---------------------------------------------------------------------------

/// Typed top-level plan builder. Every front-end constructs one, sets the
/// runtime knobs (duration, warmup, cooldown, runs, threads, mode),
/// pushes scenarios (typically built via [`scenario_http_request`] and
/// friends), and finalises.
///
/// Cheap to [`Clone`]: all fields are small owned values plus a
/// [`VarRegistry`] (itself a `Vec<String>`-sized thing).
#[derive(Debug, Clone)]
pub struct PlanBuilder {
    /// Scenarios collected in push order.
    pub scenarios: Vec<Scenario>,
    /// Variable registry. Front-ends typically mutate this directly via
    /// [`Self::vars_mut`] while compiling templates before handing the
    /// finished scenario over with [`Self::push_scenario`].
    pub vars: VarRegistry,
    /// Human-friendly plan name. Participates in `url_fingerprint`
    /// (see `docs/design-v0.1.0.md` §7.1). Empty for ephemeral runs.
    pub name: String,
    /// Steady-state measurement duration per run.
    pub duration: Duration,
    /// Warmup period (stats discarded). `Duration::ZERO` disables.
    pub warmup: Duration,
    /// Inter-run cooldown. `Duration::ZERO` disables.
    pub cooldown: Duration,
    /// Number of consecutive runs (feeds bootstrap CI).
    pub runs: u32,
    /// OS worker thread count (informational; see [`Plan::threads`]).
    pub threads: usize,
    /// Verb dispatch — which mode this plan runs under.
    pub mode: Mode,
}

impl PlanBuilder {
    /// Fresh builder with conservative defaults — mirrors
    /// [`Plan::new`] (`30s` duration, no warmup, 1 run, 1 thread,
    /// [`Mode::Measure`]).
    pub fn new() -> Self {
        Self {
            scenarios: Vec::new(),
            vars: VarRegistry::new(),
            name: String::new(),
            duration: Duration::from_secs(30),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::default(),
        }
    }

    /// Set the plan name (folded into `url_fingerprint`).
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = name.into();
        self
    }

    /// Set the per-run steady-state duration.
    pub fn duration(&mut self, d: Duration) -> &mut Self {
        self.duration = d;
        self
    }

    /// Set the per-run warmup duration (stats discarded).
    pub fn warmup(&mut self, d: Duration) -> &mut Self {
        self.warmup = d;
        self
    }

    /// Set the inter-run cooldown.
    pub fn cooldown(&mut self, d: Duration) -> &mut Self {
        self.cooldown = d;
        self
    }

    /// Set the number of consecutive runs.
    pub fn runs(&mut self, r: u32) -> &mut Self {
        self.runs = r;
        self
    }

    /// Set the OS worker thread count.
    pub fn threads(&mut self, t: usize) -> &mut Self {
        self.threads = t;
        self
    }

    /// Set the dispatch mode.
    pub fn mode(&mut self, m: Mode) -> &mut Self {
        self.mode = m;
        self
    }

    /// Mutable access to the variable registry. Front-ends compile
    /// [`Template`]s through the registry while assembling scenarios;
    /// the finished plan takes ownership of the registry at
    /// [`Self::finalize`] time.
    pub fn vars_mut(&mut self) -> &mut VarRegistry {
        &mut self.vars
    }

    /// Push a pre-built scenario into the plan.
    pub fn push_scenario(&mut self, s: Scenario) -> &mut Self {
        self.scenarios.push(s);
        self
    }

    /// Consume the builder and produce the final [`Plan`].
    pub fn finalize(self) -> Plan {
        Plan {
            scenarios: self.scenarios,
            vars: self.vars,
            duration: self.duration,
            warmup: self.warmup,
            cooldown: self.cooldown,
            runs: self.runs,
            threads: self.threads,
            mode: self.mode,
            name: self.name,
        }
    }
}

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Scenario constructors — one per `Step` variant.
//
// Every constructor is a pure packer: it takes the primitive fields that
// the underlying `*Plan` struct needs and returns a one-step
// `Scenario`. Callers validate / default their own inputs (e.g. the CLI
// clamps `connections` to ≥1 because clap doesn't enforce that).
// ---------------------------------------------------------------------------

/// Request headers in the shape [`RequestPlan::headers`] accepts.
pub type Headers = SmallVec<[(Template, Template); 8]>;

/// Narrow-header shape used by the SSE/WS plans (size 4).
pub type ProtocolHeaders = SmallVec<[(Template, Template); 4]>;

/// Build a one-step [`Step::Request`] scenario.
pub fn scenario_http_request(
    name: impl Into<String>,
    rate: RateProfile,
    request: RequestPlan,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate,
        steps: vec![Step::Request(request)],
    }
}

/// Build a one-step [`Step::HttpColdConnect`] scenario.
pub fn scenario_http_cold_connect(
    name: impl Into<String>,
    rate: RateProfile,
    request: RequestPlan,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate,
        steps: vec![Step::HttpColdConnect(ColdConnectPlan { request })],
    }
}

/// Build a one-step [`Step::SseHold`] scenario. Rate profile is fixed
/// to `Saturate { max_concurrency: subscribers }` per the canonical SSE
/// workload semantics — the subscriber count IS the concurrency.
pub fn scenario_sse_hold(
    name: impl Into<String>,
    subscribers: u32,
    hold_for: Duration,
    url: Template,
    headers: ProtocolHeaders,
    reconnect: bool,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate: RateProfile::Saturate {
            max_concurrency: subscribers.max(1) as usize,
        },
        steps: vec![Step::SseHold(SseHoldPlan {
            url,
            headers,
            subscribers,
            hold_for,
            reconnect,
        })],
    }
}

/// Build a one-step [`Step::SseFanout`] scenario.
#[allow(clippy::too_many_arguments)]
pub fn scenario_sse_fanout(
    name: impl Into<String>,
    subscribers: u32,
    hold_for: Duration,
    subscribe_url: Template,
    headers: ProtocolHeaders,
    reconnect: bool,
    trigger: TriggerSpec,
    mode: FanoutMode,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate: RateProfile::Saturate {
            max_concurrency: subscribers.max(1) as usize,
        },
        steps: vec![Step::SseFanout(SseFanoutPlan {
            subscribers: SseHoldPlan {
                url: subscribe_url,
                headers,
                subscribers,
                hold_for,
                reconnect,
            },
            trigger,
            mode,
        })],
    }
}

/// Build a one-step [`Step::SseReconnectStorm`] scenario.
#[allow(clippy::too_many_arguments)]
pub fn scenario_sse_reconnect_storm(
    name: impl Into<String>,
    subscribers: u32,
    hold_for: Duration,
    url: Template,
    headers: ProtocolHeaders,
    kill_rate_per_s: f64,
    verify_last_event_id: bool,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate: RateProfile::Saturate {
            max_concurrency: subscribers.max(1) as usize,
        },
        steps: vec![Step::SseReconnectStorm(SseReconnectStormPlan {
            subscribers: SseHoldPlan {
                url,
                headers,
                subscribers,
                hold_for,
                reconnect: true,
            },
            kill_rate_per_s,
            verify_last_event_id,
        })],
    }
}

/// Build a one-step [`Step::WsEchoRtt`] scenario. `rate` lets the
/// caller pick between open-loop (`Constant`) and the canonical
/// closed-loop `Saturate { max_concurrency: connections }` — the CLI
/// measure verb uses the latter.
#[allow(clippy::too_many_arguments)]
pub fn scenario_ws_echo_rtt(
    name: impl Into<String>,
    rate: RateProfile,
    url: Template,
    headers: ProtocolHeaders,
    connections: u32,
    msg_rate_per_conn: f64,
    payload: Template,
    correlate: CorrelateStrategy,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate,
        steps: vec![Step::WsEchoRtt(WsEchoRttPlan {
            url,
            headers,
            connections,
            msg_rate_per_conn,
            correlate,
            payload,
        })],
    }
}

/// Build a one-step [`Step::WsHold`] scenario.
#[allow(clippy::too_many_arguments)]
pub fn scenario_ws_hold(
    name: impl Into<String>,
    connections: u32,
    hold_for: Duration,
    url: Template,
    headers: ProtocolHeaders,
    heartbeat: Duration,
    heartbeat_frame: HeartbeatFrame,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate: RateProfile::Saturate {
            max_concurrency: connections.max(1) as usize,
        },
        steps: vec![Step::WsHold(WsHoldPlan {
            url,
            headers,
            connections,
            heartbeat,
            heartbeat_frame,
            hold_for,
        })],
    }
}

/// Build a one-step [`Step::WsServerPushRtt`] scenario.
pub fn scenario_ws_server_push_rtt(
    name: impl Into<String>,
    connections: u32,
    hold_for: Duration,
    url: Template,
    headers: ProtocolHeaders,
    expected_rate_per_conn: f64,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate: RateProfile::Saturate {
            max_concurrency: connections.max(1) as usize,
        },
        steps: vec![Step::WsServerPushRtt(WsServerPushRttPlan {
            url,
            headers,
            connections,
            expected_rate_per_conn,
            hold_for,
        })],
    }
}

/// Build a one-step [`Step::WsFanout`] scenario.
#[allow(clippy::too_many_arguments)]
pub fn scenario_ws_fanout(
    name: impl Into<String>,
    connections: u32,
    hold_for: Duration,
    subscribe_url: Template,
    headers: ProtocolHeaders,
    heartbeat: Duration,
    heartbeat_frame: HeartbeatFrame,
    trigger: TriggerSpec,
    mode: FanoutMode,
) -> Scenario {
    Scenario {
        name: name.into(),
        rate: RateProfile::Saturate {
            max_concurrency: connections.max(1) as usize,
        },
        steps: vec![Step::WsFanout(WsFanoutPlan {
            subscribers: WsHoldPlan {
                url: subscribe_url,
                headers,
                connections,
                heartbeat,
                heartbeat_frame,
                hold_for,
            },
            trigger,
            mode,
        })],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Assertion, Protocol, RequestPlan};

    use bytes::Bytes;
    use http::Method;

    fn lit(s: &str) -> Template {
        Template::literal(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn default_builder_has_sensible_defaults() {
        let b = PlanBuilder::new();
        assert_eq!(b.duration, Duration::from_secs(30));
        assert_eq!(b.warmup, Duration::ZERO);
        assert_eq!(b.cooldown, Duration::ZERO);
        assert_eq!(b.runs, 1);
        assert_eq!(b.threads, 1);
        assert_eq!(b.mode, Mode::Measure);
        assert!(b.name.is_empty());
        assert!(b.scenarios.is_empty());
    }

    #[test]
    fn builder_setters_fluent() {
        let mut b = PlanBuilder::new();
        b.name("t")
            .duration(Duration::from_secs(5))
            .warmup(Duration::from_secs(1))
            .cooldown(Duration::from_secs(2))
            .runs(3)
            .threads(4)
            .mode(Mode::Probe);
        assert_eq!(b.name, "t");
        assert_eq!(b.duration, Duration::from_secs(5));
        assert_eq!(b.warmup, Duration::from_secs(1));
        assert_eq!(b.cooldown, Duration::from_secs(2));
        assert_eq!(b.runs, 3);
        assert_eq!(b.threads, 4);
        assert_eq!(b.mode, Mode::Probe);
    }

    #[test]
    fn finalize_copies_every_knob() {
        let mut b = PlanBuilder::new();
        b.name("plan-x")
            .duration(Duration::from_secs(10))
            .warmup(Duration::from_millis(500))
            .cooldown(Duration::from_secs(3))
            .runs(2)
            .threads(6)
            .mode(Mode::Soak);
        b.push_scenario(scenario_http_request(
            "scn",
            RateProfile::Constant(500.0),
            RequestPlan::get(lit("/")),
        ));
        let plan = b.finalize();
        assert_eq!(plan.name, "plan-x");
        assert_eq!(plan.duration, Duration::from_secs(10));
        assert_eq!(plan.warmup, Duration::from_millis(500));
        assert_eq!(plan.cooldown, Duration::from_secs(3));
        assert_eq!(plan.runs, 2);
        assert_eq!(plan.threads, 6);
        assert_eq!(plan.mode, Mode::Soak);
        assert_eq!(plan.scenarios.len(), 1);
    }

    #[test]
    fn vars_mut_threads_into_finalize() {
        let mut b = PlanBuilder::new();
        let slot = b.vars_mut().allocate("tok").unwrap();
        let plan = b.finalize();
        // Round-trip via the registry — the slot now exists and a
        // subsequent `name` lookup returns what the builder allocated.
        assert_eq!(plan.vars.name(slot), Some("tok"));
    }

    // -----------------------------------------------------------------
    // scenario_http_request
    // -----------------------------------------------------------------

    #[test]
    fn scenario_http_request_builds_request_step() {
        let req = RequestPlan {
            method: Method::POST,
            url: lit("/x"),
            headers: SmallVec::new(),
            body: None,
            extract: Vec::new(),
            checks: vec![Assertion::StatusEq(200)],
            expect_streaming: false,
        };
        let s = scenario_http_request("r", RateProfile::Constant(100.0), req);
        assert_eq!(s.name, "r");
        assert_eq!(s.steps.len(), 1);
        assert_eq!(s.protocol(), Protocol::Http);
        match &s.steps[0] {
            Step::Request(r) => {
                assert_eq!(r.method, Method::POST);
                assert_eq!(r.checks.len(), 1);
            }
            other => panic!("expected Request, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_http_cold_connect
    // -----------------------------------------------------------------

    #[test]
    fn scenario_http_cold_connect_wraps_request() {
        let req = RequestPlan::get(lit("/"));
        let s = scenario_http_cold_connect("cold", RateProfile::Constant(10.0), req);
        assert_eq!(s.protocol(), Protocol::Http);
        assert!(matches!(s.steps[0], Step::HttpColdConnect(_)));
    }

    // -----------------------------------------------------------------
    // scenario_sse_hold
    // -----------------------------------------------------------------

    #[test]
    fn scenario_sse_hold_fills_fields_and_uses_saturate() {
        let s = scenario_sse_hold(
            "sse",
            128,
            Duration::from_secs(30),
            lit("http://h/events"),
            SmallVec::new(),
            true,
        );
        assert_eq!(s.protocol(), Protocol::Sse);
        match &s.rate {
            RateProfile::Saturate { max_concurrency } => assert_eq!(*max_concurrency, 128),
            other => panic!("expected Saturate, got {other:?}"),
        }
        match &s.steps[0] {
            Step::SseHold(p) => {
                assert_eq!(p.subscribers, 128);
                assert_eq!(p.hold_for, Duration::from_secs(30));
                assert!(p.reconnect);
            }
            other => panic!("expected SseHold, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_sse_fanout
    // -----------------------------------------------------------------

    #[test]
    fn scenario_sse_fanout_nests_subscribers_and_trigger() {
        let s = scenario_sse_fanout(
            "f",
            10,
            Duration::from_secs(5),
            lit("http://h/subscribe"),
            SmallVec::new(),
            true,
            TriggerSpec::HttpPost {
                url: lit("http://h/trigger"),
                body: None,
            },
            FanoutMode::TriggerRtt,
        );
        assert_eq!(s.protocol(), Protocol::Sse);
        match &s.steps[0] {
            Step::SseFanout(p) => {
                assert_eq!(p.subscribers.subscribers, 10);
                assert!(matches!(p.trigger, TriggerSpec::HttpPost { .. }));
                assert!(matches!(p.mode, FanoutMode::TriggerRtt));
            }
            other => panic!("expected SseFanout, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_sse_reconnect_storm
    // -----------------------------------------------------------------

    #[test]
    fn scenario_sse_reconnect_storm_sets_kill_rate() {
        let s = scenario_sse_reconnect_storm(
            "storm",
            50,
            Duration::from_secs(10),
            lit("http://h/events"),
            SmallVec::new(),
            0.25,
            true,
        );
        assert_eq!(s.protocol(), Protocol::Sse);
        match &s.steps[0] {
            Step::SseReconnectStorm(p) => {
                assert_eq!(p.subscribers.subscribers, 50);
                assert!((p.kill_rate_per_s - 0.25).abs() < 1e-9);
                assert!(p.verify_last_event_id);
            }
            other => panic!("expected SseReconnectStorm, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_ws_echo_rtt
    // -----------------------------------------------------------------

    #[test]
    fn scenario_ws_echo_rtt_builds_ws_step() {
        let s = scenario_ws_echo_rtt(
            "echo",
            RateProfile::Saturate { max_concurrency: 8 },
            lit("ws://h/"),
            SmallVec::new(),
            8,
            100.0,
            lit("ping"),
            CorrelateStrategy::PingPong,
        );
        assert_eq!(s.protocol(), Protocol::Ws);
        match &s.steps[0] {
            Step::WsEchoRtt(p) => {
                assert_eq!(p.connections, 8);
                assert!((p.msg_rate_per_conn - 100.0).abs() < 1e-9);
                assert!(matches!(p.correlate, CorrelateStrategy::PingPong));
            }
            other => panic!("expected WsEchoRtt, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_ws_hold
    // -----------------------------------------------------------------

    #[test]
    fn scenario_ws_hold_preserves_heartbeat_frame() {
        let s = scenario_ws_hold(
            "hold",
            16,
            Duration::from_secs(60),
            lit("ws://h/"),
            SmallVec::new(),
            Duration::from_secs(25),
            HeartbeatFrame::TextApp,
        );
        assert_eq!(s.protocol(), Protocol::Ws);
        match &s.steps[0] {
            Step::WsHold(p) => {
                assert_eq!(p.connections, 16);
                assert_eq!(p.heartbeat, Duration::from_secs(25));
                assert_eq!(p.heartbeat_frame, HeartbeatFrame::TextApp);
                assert_eq!(p.hold_for, Duration::from_secs(60));
            }
            other => panic!("expected WsHold, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_ws_server_push_rtt
    // -----------------------------------------------------------------

    #[test]
    fn scenario_ws_server_push_rtt_records_expected_rate() {
        let s = scenario_ws_server_push_rtt(
            "push",
            4,
            Duration::from_secs(10),
            lit("ws://h/"),
            SmallVec::new(),
            25.0,
        );
        assert_eq!(s.protocol(), Protocol::Ws);
        match &s.steps[0] {
            Step::WsServerPushRtt(p) => {
                assert_eq!(p.connections, 4);
                assert!((p.expected_rate_per_conn - 25.0).abs() < 1e-9);
                assert_eq!(p.hold_for, Duration::from_secs(10));
            }
            other => panic!("expected WsServerPushRtt, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // scenario_ws_fanout
    // -----------------------------------------------------------------

    #[test]
    fn scenario_ws_fanout_nests_subscribers_trigger_and_mode() {
        let s = scenario_ws_fanout(
            "wsf",
            12,
            Duration::from_secs(20),
            lit("ws://h/sub"),
            SmallVec::new(),
            Duration::from_secs(25),
            HeartbeatFrame::Ping,
            TriggerSpec::DedicatedWsConnection {
                payload: lit("go"),
            },
            FanoutMode::Timestamp {
                emit_field: "emit_ns".into(),
            },
        );
        assert_eq!(s.protocol(), Protocol::Ws);
        match &s.steps[0] {
            Step::WsFanout(p) => {
                assert_eq!(p.subscribers.connections, 12);
                assert_eq!(p.subscribers.heartbeat, Duration::from_secs(25));
                assert!(matches!(
                    p.trigger,
                    TriggerSpec::DedicatedWsConnection { .. }
                ));
                assert!(matches!(p.mode, FanoutMode::Timestamp { .. }));
            }
            other => panic!("expected WsFanout, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Builder + scenario constructor integration
    // -----------------------------------------------------------------

    #[test]
    fn push_scenario_accumulates_in_order() {
        let mut b = PlanBuilder::new();
        b.push_scenario(scenario_http_request(
            "a",
            RateProfile::Constant(1.0),
            RequestPlan::get(lit("/a")),
        ));
        b.push_scenario(scenario_http_request(
            "b",
            RateProfile::Constant(2.0),
            RequestPlan::get(lit("/b")),
        ));
        let plan = b.finalize();
        assert_eq!(plan.scenarios.len(), 2);
        assert_eq!(plan.scenarios[0].name, "a");
        assert_eq!(plan.scenarios[1].name, "b");
    }
}
