//! ARCH STATUS: REWRITE
//!
//! 2,300 LoC of near-duplicated builder boilerplate. Nine builder types
//! (PlanBuilder, ScenarioBuilder, RequestBuilder, SseHoldBuilder,
//! WsEchoRttBuilder, WsHoldBuilder, WsServerPushBuilder, SseFanoutBuilder,
//! WsFanoutBuilder, SseReconnectStormBuilder) each with identical shape:
//!   - `struct FooBuilder { inner: Arc<Mutex<FooBuilderState>> }`
//!   - `struct FooBuilderState { ... }`
//!   - `impl Default for FooBuilderState`
//!   - `impl FooBuilder { new / with_state / take_state }`
//!   - `fn register_foo_builders(engine)` with repeated
//!     `.header` / `.payload` / `.heartbeat_frame` / etc.
//!
//! ARCH(rhai-macro): collapse via a `define_builder!` declarative macro.
//! ARCH(builder-unify): every protocol-specific builder here duplicates
//!                      construction logic already in the CLI's plan_from_cli.rs.
//!                      Post-rewrite: one typed PlanBuilder shared between
//!                      both (see ARCH-REVIEW §4.5, §B3, §B5).
//!
//! Target: ~800 LoC after the macro + shared PlanBuilder consolidation.
//! DSL surface (user-facing Rhai function names and signatures) stays
//! byte-identical.
//!
//! ----------------------------------------------------------------------
//!
//! Rhai-side builder types and engine registrations.
//!
//! These types are owned by the script during evaluation; once the script
//! returns, [`PlanBuilder::finalize`] converts the accumulated state into a
//! real [`zerobench_core::plan::Plan`] and the engine is dropped.
//!
//! # Shared-state pattern
//!
//! Rhai's `Dynamic` needs types to be `Clone`, and with the `sync` feature
//! also `Send + Sync`. Every builder here is an [`Arc`]`<`[`Mutex`]`<State>>`
//! newtype — cloning the builder clones the `Arc`, so method chains like
//! `.header("a", "b").json(...)` all mutate the same underlying state.
//!
//! # Fluent chaining
//!
//! Each builder method takes the receiver **by value** and returns a clone
//! of the same `Arc`. That's the pattern Rhai's method-chain syntax needs
//! (methods must be `Fn(Self, ...) -> Self` where `Self: Clone`).
//!
//! # Hot path discipline
//!
//! None of this code runs on the benchmark hot path. The engine is built,
//! the script is evaluated into a [`PlanBuilder`], the builder is
//! finalized, and then the engine is dropped. The resulting [`Plan`] is
//! a pure Rust data structure — no Rhai traces remain.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderName, Method};
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, ImmutableString, NativeCallContext};
use smallvec::SmallVec;

use zerobench_core::plan::{
    Assertion, BodySource, Extract, Mode, Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::template::Template;
use zerobench_core::transport::HttpVersionPref;
use zerobench_core::var::{VarRegistry, VarSlot};

use crate::error::ScriptError;
use crate::parse;

// ---------------------------------------------------------------------------
// PlanBuilder — the root aggregator
// ---------------------------------------------------------------------------

/// Shared-state wrapper around [`PlanBuilderState`]. The public handle Rhai
/// scripts interact with via the `scenario`, `rate`, `duration`, `warmup`,
/// `transport`, `saturate`, `env`, and `var` top-level functions.
#[derive(Clone)]
pub struct PlanBuilder {
    inner: Arc<Mutex<PlanBuilderState>>,
}

/// The full state accumulated during a script run. Finalized into a
/// [`Plan`] by [`PlanBuilder::finalize`] and then discarded.
///
/// Implements [`Default`] so [`std::mem::take`] can swap it out of the
/// mutex at finalize time without needing to `Arc::try_unwrap` (Rhai
/// leaves reference cycles in its symbol table until the engine itself
/// drops, so unwrapping the Arc would always fail while the engine is
/// alive).
#[derive(Default)]
pub(crate) struct PlanBuilderState {
    /// Scenarios collected in declaration order. `weight` on the inner
    /// builder is optional; absent means "auto-weight evenly".
    pub scenarios: Vec<ScenarioBuilder>,
    /// Variable registry — templates allocate via `{{var:NAME}}` and the
    /// script itself allocates via `var("NAME")`.
    pub vars: VarRegistry,
    /// Global rate, if `rate("...")` was called. Mutually exclusive with
    /// per-scenario `.rate(...)` — checked at finalize.
    pub global_rate: Option<RateProfile>,
    /// Global saturate setting — set by `saturate(n)`. If both
    /// `global_rate` and `saturate_concurrency` are set, `global_rate`
    /// wins (the script explicitly asked for open-loop); saturate is the
    /// default when no rate is given.
    pub saturate_concurrency: Option<usize>,
    /// Measurement duration — required.
    pub duration: Option<Duration>,
    /// Optional warmup period.
    pub warmup: Option<Duration>,
    /// Cooldown between `runs()` iterations. Defaults to zero when the
    /// script does not call `cooldown()`.
    pub cooldown: Option<Duration>,
    /// Total number of measure iterations. Defaults to 1 when unset
    /// (single-run plan); larger values feed the bootstrap CI
    /// aggregator just like `measure --runs N`.
    pub runs: Option<u32>,
    /// Client-side worker threads. Defaults to 1 — the CLI and
    /// top-level runners may override, but the script can pin a
    /// specific fan-out.
    pub threads: Option<usize>,
    /// Human-friendly plan name. Folds into `url_fingerprint` per §7.1.
    pub name: Option<String>,
    /// Preferred HTTP protocol version — `transport("h1"|"h2")` sets this.
    /// Carried out of the Plan and returned separately; the dispatcher
    /// threads it into `TransportOpts`.
    pub transport: HttpVersionPref,
}

impl PlanBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PlanBuilderState::default())),
        }
    }

    /// Peek at the raw URL source of the first wire-step (Request, SSE,
    /// or WS) in the first scenario — used by the CLI to derive a
    /// connection [`Target`] before the plan is finalized. Returns
    /// `None` if no scenarios have wire steps (all pauses, or empty).
    ///
    /// `{{...}}` templates in the URL are returned verbatim: the caller
    /// must either strip them (cheap case: user wrote a literal URL with
    /// templated path) or reject the script (host is templated).
    pub fn first_request_url(&self) -> Option<String> {
        self.with_state(|s| {
            for scn in &s.scenarios {
                let url = scn.with_state(|st| {
                    st.steps
                        .iter()
                        .find_map(|step| match step {
                            StepSource::Request(rb) => {
                                Some(rb.with_state(|r| r.url.clone()))
                            }
                            StepSource::SseHold(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::SseFanout(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::SseReconnectStorm(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::WsEchoRtt(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::WsHold(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::WsServerPush(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::WsFanout(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::Pause(_)
                            | StepSource::PauseRandom { .. } => None,
                        })
                });
                if let Some(u) = url {
                    return Some(u);
                }
            }
            None
        })
    }

    /// Borrow the underlying state mutably. `.unwrap()` is safe here — the
    /// mutex is only poisoned if a `panic!` fired while holding the lock,
    /// which our code never does (all `?` paths release the guard before
    /// returning, and panics would abort the script anyway).
    pub(crate) fn with_state<R>(
        &self,
        f: impl FnOnce(&mut PlanBuilderState) -> R,
    ) -> R {
        let mut guard = self
            .inner
            .lock()
            .expect("plan builder mutex poisoned — a panic escaped an earlier Rhai call");
        f(&mut guard)
    }

    /// Finalize into a real plan + transport preference.
    ///
    /// Validates:
    /// - At least one scenario.
    /// - `duration` was set.
    /// - No conflict between global `rate()` and per-scenario `.rate()`.
    ///
    /// Then normalizes per-scenario rates from `global_rate` + weights if
    /// applicable, compiles every templated string, and assembles a
    /// `(Plan, HttpVersionPref)` pair.
    ///
    /// We swap the state out of the mutex with [`std::mem::take`] rather
    /// than trying to `Arc::try_unwrap` — Rhai's engine holds references
    /// to the registered closures (each of which holds an Arc clone) for
    /// its whole lifetime, so the Arc's strong count is always > 1 while
    /// the engine is alive. Taking the state lets us keep ownership of
    /// the owned data without freeing the shared `Arc<Mutex<...>>`.
    pub fn finalize(self) -> Result<(Plan, HttpVersionPref), ScriptError> {
        let inner = self.with_state(std::mem::take);
        finalize_state(inner)
    }
}

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn finalize_state(
    mut state: PlanBuilderState,
) -> Result<(Plan, HttpVersionPref), ScriptError> {
    if state.scenarios.is_empty() {
        return Err(ScriptError::NoScenarios);
    }
    let duration = state.duration.ok_or(ScriptError::MissingDuration)?;

    // Conflict check: global `rate()` AND any per-scenario `.rate()`.
    let any_scenario_rate = state
        .scenarios
        .iter()
        .any(|s| s.with_state(|st| st.rate.is_some()));
    if state.global_rate.is_some() && any_scenario_rate {
        return Err(ScriptError::ConflictingRate);
    }

    // For multi-host detection we record every observed scheme://host:port
    // as we walk the scenarios — the CLI later needs a single Target, so
    // the script must not mix hosts. We don't fail inside the builder
    // (the loader returns the raw first URL to the CLI), but a future
    // check can live here.

    // Compute total weight sum for auto-normalization when global_rate is
    // set and no scenario has an explicit rate.
    let (total_weight, explicit_weighted_count) = {
        let mut total = 0.0f64;
        let mut explicit = 0usize;
        for s in &state.scenarios {
            s.with_state(|st| {
                if let Some(w) = st.weight {
                    total += w;
                    explicit += 1;
                }
            });
        }
        (total, explicit)
    };

    // If any scenario has an explicit weight, unweighted scenarios get 0
    // weight (user must specify if they mix). If no scenario has weight,
    // we split evenly — each gets 1 / scenario_count.
    let scenario_count = state.scenarios.len();
    let auto_weight = if explicit_weighted_count == 0 {
        Some(1.0 / scenario_count as f64)
    } else {
        None
    };

    let mut scenarios_out: Vec<Scenario> = Vec::with_capacity(scenario_count);
    for scn in state.scenarios.drain(..) {
        let st = scn.take_state();
        let ScenarioBuilderState {
            name,
            weight,
            rate,
            steps,
        } = st;

        // Compile every templated step. We hand over the shared registry
        // so `{{var:NAME}}` uses the same slot assignments the script
        // allocated via `var("NAME")`.
        let mut compiled_steps: Vec<Step> = Vec::with_capacity(steps.len());
        for step_src in steps {
            compiled_steps.push(compile_step(step_src, &mut state.vars, &name)?);
        }

        // Resolve the scenario's rate profile.
        let scenario_rate = resolve_scenario_rate(
            rate,
            &state.global_rate,
            state.saturate_concurrency,
            weight,
            auto_weight,
            total_weight,
        );

        scenarios_out.push(Scenario {
            name,
            rate: scenario_rate,
            steps: compiled_steps,
        });
    }

    let plan = Plan {
        scenarios: scenarios_out,
        vars: state.vars,
        duration,
        warmup: state.warmup.unwrap_or(Duration::ZERO),
        cooldown: state.cooldown.unwrap_or(Duration::ZERO),
        runs: state.runs.unwrap_or(1).max(1),
        threads: state.threads.unwrap_or(1).max(1),
        // Mode is fixed: Rhai-driven runs are always `Measure`. The CLI
        // verb (`probe` / `curve` / etc.) picks a different Mode when
        // needed; scripts don't.
        mode: Mode::default(),
        name: state.name.unwrap_or_default(),
    };
    Ok((plan, state.transport))
}

fn resolve_scenario_rate(
    explicit: Option<RateProfile>,
    global: &Option<RateProfile>,
    saturate: Option<usize>,
    weight: Option<f64>,
    auto_weight: Option<f64>,
    total_explicit_weight: f64,
) -> RateProfile {
    // 1. Explicit per-scenario rate always wins.
    if let Some(r) = explicit {
        return r;
    }
    // 2. Global rate, scaled by weight.
    if let Some(g) = global {
        let share = match (weight, auto_weight) {
            (Some(w), _) => {
                if total_explicit_weight > 0.0 {
                    w / total_explicit_weight.max(f64::EPSILON)
                } else {
                    0.0
                }
            }
            (None, Some(auto)) => auto,
            (None, None) => 0.0,
        };
        return scale_rate(g, share);
    }
    // 3. Saturate fallback.
    let max_concurrency = saturate.unwrap_or(50);
    RateProfile::Saturate { max_concurrency }
}

fn scale_rate(global: &RateProfile, share: f64) -> RateProfile {
    match global {
        RateProfile::Constant(r) => RateProfile::Constant(r * share),
        RateProfile::Ramp { from, to, over } => RateProfile::Ramp {
            from: from * share,
            to: to * share,
            over: *over,
        },
        RateProfile::Stepped(points) => RateProfile::Stepped(
            points.iter().map(|(d, r)| (*d, r * share)).collect(),
        ),
        // Saturate never gets scaled — it's a concurrency count, not a
        // rate. A script that sets `saturate(N)` and then uses weighted
        // scenarios gets `Saturate { N }` on every scenario (matching the
        // CLI's `--saturate` + `--requests DIR` behaviour).
        RateProfile::Saturate { max_concurrency } => RateProfile::Saturate {
            max_concurrency: *max_concurrency,
        },
    }
}

// ---------------------------------------------------------------------------
// ScenarioBuilder
// ---------------------------------------------------------------------------

/// Handed to the scenario body closure as its `s` argument. Scripts call
/// `s.step(...)` to enqueue steps, and (optionally) `s.rate("X")` to set a
/// per-scenario rate.
#[derive(Clone)]
pub(crate) struct ScenarioBuilder {
    inner: Arc<Mutex<ScenarioBuilderState>>,
}

#[derive(Default)]
pub(crate) struct ScenarioBuilderState {
    pub name: String,
    pub weight: Option<f64>,
    pub rate: Option<RateProfile>,
    pub steps: Vec<StepSource>,
}

impl ScenarioBuilder {
    fn new(name: String, weight: Option<f64>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ScenarioBuilderState {
                name,
                weight,
                rate: None,
                steps: Vec::new(),
            })),
        }
    }

    pub(crate) fn with_state<R>(
        &self,
        f: impl FnOnce(&mut ScenarioBuilderState) -> R,
    ) -> R {
        let mut guard = self
            .inner
            .lock()
            .expect("scenario builder mutex poisoned");
        f(&mut guard)
    }

    /// Take the state out of the Arc by swapping `Default`. See
    /// [`PlanBuilder::finalize`] for why we don't try to unwrap the Arc.
    fn take_state(&self) -> ScenarioBuilderState {
        self.with_state(std::mem::take)
    }
}

// ---------------------------------------------------------------------------
// RequestBuilder
// ---------------------------------------------------------------------------

/// Returned by `GET(url)`, `POST(url)`, etc. and passed through the chained
/// `.header`, `.json`, `.body`, `.expect_status`, etc. methods.
#[derive(Clone)]
pub(crate) struct RequestBuilder {
    inner: Arc<Mutex<RequestBuilderState>>,
}

pub(crate) struct RequestBuilderState {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<BodySourceSpec>,
    pub extract: Vec<Extract>,
    pub checks: Vec<Assertion>,
    /// If true, the finalize step emits `Step::HttpColdConnect` — one
    /// fresh TCP+TLS+HTTP connection per request, no pool reuse.
    pub cold: bool,
}

impl Default for RequestBuilderState {
    // `http::Method` has no Default impl, so we provide one. `GET` is the
    // benign "empty" request; this value is only read if someone swaps
    // the state out (at finalize) and we want to reconstruct a blank
    // state behind them — which never happens in practice because
    // finalize drains and drops each RequestBuilder.
    fn default() -> Self {
        Self {
            method: Method::GET,
            url: String::new(),
            headers: Vec::new(),
            body: None,
            extract: Vec::new(),
            checks: Vec::new(),
            cold: false,
        }
    }
}

/// Unresolved body specification. Templates are compiled during
/// [`PlanBuilder::finalize`] so errors carry file/line context.
#[derive(Clone)]
pub(crate) enum BodySourceSpec {
    /// Raw bytes — `.body(...)` / `.body_file(...)`.
    Raw(Bytes),
    /// Template — `.body(s)` when `s` contains `{{...}}`.
    Template(String),
    /// Object built via `.json(#{...})` — always JSON-encoded as a template
    /// so templates inside string values still work.
    JsonTemplate(String),
}

impl RequestBuilder {
    fn new(method: Method, url: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RequestBuilderState {
                method,
                url,
                headers: Vec::new(),
                body: None,
                extract: Vec::new(),
                checks: Vec::new(),
                cold: false,
            })),
        }
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut RequestBuilderState) -> R) -> R {
        let mut guard = self
            .inner
            .lock()
            .expect("request builder mutex poisoned");
        f(&mut guard)
    }

    /// Take the state out of the Arc by swapping `Default`. See
    /// [`PlanBuilder::finalize`] for why we don't try to unwrap the Arc.
    fn take_state(&self) -> RequestBuilderState {
        self.with_state(std::mem::take)
    }
}

// ---------------------------------------------------------------------------
// Enum parsers — turn Rhai strings into core plan enums.
//
// Parsing is strict (unknown variants are errors) because a typo in a
// bench script should fail at plan-build time, not silently fall back
// to a default that changes what's measured.
// ---------------------------------------------------------------------------

fn parse_heartbeat_frame(s: &str) -> Result<zerobench_core::plan::HeartbeatFrame, String> {
    use zerobench_core::plan::HeartbeatFrame;
    match s.to_ascii_lowercase().as_str() {
        "ping" => Ok(HeartbeatFrame::Ping),
        "text" | "textapp" | "text_app" => Ok(HeartbeatFrame::TextApp),
        other => Err(format!(
            "heartbeat_frame: expected \"ping\" or \"text\", got \"{other}\""
        )),
    }
}

fn parse_correlate(s: &str) -> Result<zerobench_core::plan::CorrelateStrategy, String> {
    use zerobench_core::plan::CorrelateStrategy;
    // Accept the bare variant names plus `substring:<marker>` for the
    // one variant that carries data. Case-insensitive; hyphens and
    // underscores are interchangeable.
    let norm = s.trim().to_ascii_lowercase();
    if let Some(marker) = norm.strip_prefix("substring:") {
        if marker.is_empty() {
            return Err("correlate \"substring:\" requires a non-empty marker".into());
        }
        // Take the marker from the original (case preserved, no trim)
        // so the user's literal string survives.
        let marker = s.trim()[10..].to_string();
        return Ok(CorrelateStrategy::PayloadSubstring { marker });
    }
    match norm.as_str() {
        "pingpong" | "ping_pong" | "ping-pong" => Ok(CorrelateStrategy::PingPong),
        "monotonicidprepend" | "monotonic_id_prepend" | "monotonic-id-prepend" | "prepend" => {
            Ok(CorrelateStrategy::MonotonicIdPrepend)
        }
        "firsttextframe" | "first_text_frame" | "first-text-frame" | "first_text" => {
            Ok(CorrelateStrategy::FirstTextFrame)
        }
        other => Err(format!(
            "correlate: expected \"pingpong\", \"prepend\", \"first_text\", or \"substring:<marker>\", got \"{other}\""
        )),
    }
}

fn parse_fanout_mode(s: &str) -> Result<zerobench_core::plan::FanoutMode, String> {
    use zerobench_core::plan::FanoutMode;
    // `timestamp` optionally takes a custom field via `timestamp:<field>`.
    let norm = s.trim().to_ascii_lowercase();
    if let Some(field) = norm.strip_prefix("timestamp:") {
        if field.is_empty() {
            return Err("mode \"timestamp:\" requires a non-empty field name".into());
        }
        // Preserve case from the original.
        let emit_field = s.trim()[10..].to_string();
        return Ok(FanoutMode::Timestamp { emit_field });
    }
    match norm.as_str() {
        "triggerrtt" | "trigger_rtt" | "trigger-rtt" => Ok(FanoutMode::TriggerRtt),
        "timestamp" => Ok(FanoutMode::Timestamp {
            emit_field: "emit_ns".into(),
        }),
        other => Err(format!(
            "mode: expected \"trigger_rtt\" or \"timestamp[:<field>]\", got \"{other}\""
        )),
    }
}

// ---------------------------------------------------------------------------
// Protocol-native builders
//
// SseHoldBuilder / WsEchoRttBuilder wrap the SseHoldPlan /
// WsEchoRttPlan state. Construction is one-shot — users pass the
// essential parameters at call time (sse_hold(url, n, for)) instead
// of chained setters.
// ---------------------------------------------------------------------------

/// Returned by `sse_hold(url, subscribers, hold_for)`. Finalised into
/// [`Step::SseHold`] during plan finalization.
#[derive(Clone)]
pub(crate) struct SseHoldBuilder {
    inner: Arc<Mutex<SseHoldBuilderState>>,
}

pub(crate) struct SseHoldBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subscribers: u32,
    pub hold_for: Duration,
    pub reconnect: bool,
}

impl Default for SseHoldBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            subscribers: 1,
            hold_for: Duration::from_secs(60),
            reconnect: true,
        }
    }
}

impl SseHoldBuilder {
    fn new(url: String, subscribers: u32, hold_for: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SseHoldBuilderState {
                url,
                headers: Vec::new(),
                subscribers: subscribers.max(1),
                hold_for,
                reconnect: true,
            })),
        }
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut SseHoldBuilderState) -> R) -> R {
        let mut guard = self.inner.lock().expect("sse_hold builder mutex poisoned");
        f(&mut guard)
    }

    fn take_state(&self) -> SseHoldBuilderState {
        self.with_state(std::mem::take)
    }
}

/// Returned by `ws_echo_rtt(url, connections, msg_rate_per_conn)`.
/// Finalised into [`Step::WsEchoRtt`] during plan finalization.
#[derive(Clone)]
pub(crate) struct WsEchoRttBuilder {
    inner: Arc<Mutex<WsEchoRttBuilderState>>,
}

pub(crate) struct WsEchoRttBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub connections: u32,
    pub msg_rate_per_conn: f64,
    pub payload: String,
    pub correlate: zerobench_core::plan::CorrelateStrategy,
}

impl Default for WsEchoRttBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            connections: 1,
            msg_rate_per_conn: 100.0,
            payload: "ping".into(),
            correlate: zerobench_core::plan::CorrelateStrategy::MonotonicIdPrepend,
        }
    }
}

impl WsEchoRttBuilder {
    fn new(url: String, connections: u32, msg_rate_per_conn: f64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WsEchoRttBuilderState {
                url,
                headers: Vec::new(),
                connections: connections.max(1),
                msg_rate_per_conn,
                payload: "ping".into(),
                correlate: zerobench_core::plan::CorrelateStrategy::MonotonicIdPrepend,
            })),
        }
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut WsEchoRttBuilderState) -> R) -> R {
        let mut guard = self.inner.lock().expect("ws_echo_rtt builder mutex poisoned");
        f(&mut guard)
    }

    fn take_state(&self) -> WsEchoRttBuilderState {
        self.with_state(std::mem::take)
    }
}

/// Returned by `ws_hold(url, connections, hold_for)`. Finalised into
/// [`Step::WsHold`] during plan finalization.
#[derive(Clone)]
pub(crate) struct WsHoldBuilder {
    inner: Arc<Mutex<WsHoldBuilderState>>,
}

pub(crate) struct WsHoldBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub connections: u32,
    pub heartbeat: Duration,
    pub heartbeat_frame: zerobench_core::plan::HeartbeatFrame,
    pub hold_for: Duration,
}

impl Default for WsHoldBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            connections: 1,
            heartbeat: Duration::from_secs(25),
            heartbeat_frame: zerobench_core::plan::HeartbeatFrame::Ping,
            hold_for: Duration::from_secs(60),
        }
    }
}

impl WsHoldBuilder {
    fn new(url: String, connections: u32, hold_for: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WsHoldBuilderState {
                url,
                headers: Vec::new(),
                connections: connections.max(1),
                heartbeat: Duration::from_secs(25),
                heartbeat_frame: zerobench_core::plan::HeartbeatFrame::Ping,
                hold_for,
            })),
        }
    }
    fn with_state<R>(&self, f: impl FnOnce(&mut WsHoldBuilderState) -> R) -> R {
        let mut g = self.inner.lock().expect("ws_hold builder mutex poisoned");
        f(&mut g)
    }
    fn take_state(&self) -> WsHoldBuilderState {
        self.with_state(std::mem::take)
    }
}

/// Returned by `ws_server_push(url, connections, hold_for)`. Finalised
/// into [`Step::WsServerPushRtt`] during plan finalization.
#[derive(Clone)]
pub(crate) struct WsServerPushBuilder {
    inner: Arc<Mutex<WsServerPushBuilderState>>,
}

pub(crate) struct WsServerPushBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub connections: u32,
    pub expected_rate_per_conn: f64,
    pub hold_for: Duration,
}

impl Default for WsServerPushBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            connections: 1,
            expected_rate_per_conn: 0.0,
            hold_for: Duration::from_secs(60),
        }
    }
}

impl WsServerPushBuilder {
    fn new(url: String, connections: u32, hold_for: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WsServerPushBuilderState {
                url,
                headers: Vec::new(),
                connections: connections.max(1),
                expected_rate_per_conn: 0.0,
                hold_for,
            })),
        }
    }
    fn with_state<R>(&self, f: impl FnOnce(&mut WsServerPushBuilderState) -> R) -> R {
        let mut g = self.inner.lock().expect("ws_server_push builder mutex poisoned");
        f(&mut g)
    }
    fn take_state(&self) -> WsServerPushBuilderState {
        self.with_state(std::mem::take)
    }
}

/// Returned by `sse_fanout(url, subs, hold_for)`. Compiles to
/// [`Step::SseFanout`] at finalize. Requires `.trigger_url(...)`.
#[derive(Clone)]
pub(crate) struct SseFanoutBuilder {
    inner: Arc<Mutex<SseFanoutBuilderState>>,
}
pub(crate) struct SseFanoutBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subscribers: u32,
    pub hold_for: Duration,
    pub reconnect: bool,
    pub trigger_url: String,
    pub mode: zerobench_core::plan::FanoutMode,
}
impl Default for SseFanoutBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            subscribers: 1,
            hold_for: Duration::from_secs(60),
            reconnect: true,
            trigger_url: String::new(),
            mode: zerobench_core::plan::FanoutMode::TriggerRtt,
        }
    }
}
impl SseFanoutBuilder {
    fn new(url: String, subs: u32, hold_for: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SseFanoutBuilderState {
                url,
                headers: Vec::new(),
                subscribers: subs.max(1),
                hold_for,
                reconnect: true,
                trigger_url: String::new(),
                mode: zerobench_core::plan::FanoutMode::TriggerRtt,
            })),
        }
    }
    fn with_state<R>(&self, f: impl FnOnce(&mut SseFanoutBuilderState) -> R) -> R {
        let mut g = self.inner.lock().expect("sse_fanout builder mutex poisoned");
        f(&mut g)
    }
    fn take_state(&self) -> SseFanoutBuilderState {
        self.with_state(std::mem::take)
    }
}

/// Returned by `ws_fanout(url, conns, hold_for)`. Compiles to
/// [`Step::WsFanout`] at finalize.
#[derive(Clone)]
pub(crate) struct WsFanoutBuilder {
    inner: Arc<Mutex<WsFanoutBuilderState>>,
}
pub(crate) struct WsFanoutBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub connections: u32,
    pub hold_for: Duration,
    pub heartbeat: Duration,
    pub heartbeat_frame: zerobench_core::plan::HeartbeatFrame,
    pub trigger_url: String,
    pub mode: zerobench_core::plan::FanoutMode,
}
impl Default for WsFanoutBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            connections: 1,
            hold_for: Duration::from_secs(60),
            heartbeat: Duration::from_secs(25),
            heartbeat_frame: zerobench_core::plan::HeartbeatFrame::Ping,
            trigger_url: String::new(),
            mode: zerobench_core::plan::FanoutMode::TriggerRtt,
        }
    }
}
impl WsFanoutBuilder {
    fn new(url: String, connections: u32, hold_for: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WsFanoutBuilderState {
                url,
                headers: Vec::new(),
                connections: connections.max(1),
                hold_for,
                heartbeat: Duration::from_secs(25),
                heartbeat_frame: zerobench_core::plan::HeartbeatFrame::Ping,
                trigger_url: String::new(),
                mode: zerobench_core::plan::FanoutMode::TriggerRtt,
            })),
        }
    }
    fn with_state<R>(&self, f: impl FnOnce(&mut WsFanoutBuilderState) -> R) -> R {
        let mut g = self.inner.lock().expect("ws_fanout builder mutex poisoned");
        f(&mut g)
    }
    fn take_state(&self) -> WsFanoutBuilderState {
        self.with_state(std::mem::take)
    }
}

/// Returned by `sse_reconnect_storm(url, subs, hold_for)`.
#[derive(Clone)]
pub(crate) struct SseReconnectStormBuilder {
    inner: Arc<Mutex<SseReconnectStormBuilderState>>,
}
pub(crate) struct SseReconnectStormBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subscribers: u32,
    pub hold_for: Duration,
    pub kill_rate_per_s: f64,
    pub verify_last_event_id: bool,
}
impl Default for SseReconnectStormBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            subscribers: 1,
            hold_for: Duration::from_secs(60),
            kill_rate_per_s: 0.1,
            verify_last_event_id: true,
        }
    }
}
impl SseReconnectStormBuilder {
    fn new(url: String, subs: u32, hold_for: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SseReconnectStormBuilderState {
                url,
                headers: Vec::new(),
                subscribers: subs.max(1),
                hold_for,
                kill_rate_per_s: 0.1,
                verify_last_event_id: true,
            })),
        }
    }
    fn with_state<R>(&self, f: impl FnOnce(&mut SseReconnectStormBuilderState) -> R) -> R {
        let mut g = self.inner.lock().expect("sse_reconnect_storm builder mutex poisoned");
        f(&mut g)
    }
    fn take_state(&self) -> SseReconnectStormBuilderState {
        self.with_state(std::mem::take)
    }
}

// ---------------------------------------------------------------------------
// StepSource — intermediate form before template compilation
// ---------------------------------------------------------------------------

/// Un-compiled step; held by ScenarioBuilder.steps and compiled into a
/// real [`Step`] during plan finalization.
///
/// `Clone` is required so Rhai can pass values of this type through
/// `Dynamic` (the engine clones types on the way in/out of the value
/// slot). Cloning a `Request` clones the inner `Arc`, so the two copies
/// share state — but by the time `s.step(req)` runs, the originating
/// `req` temporary has already been consumed, so no sharing matters.
#[derive(Clone)]
pub(crate) enum StepSource {
    Request(RequestBuilder),
    SseHold(SseHoldBuilder),
    SseFanout(SseFanoutBuilder),
    SseReconnectStorm(SseReconnectStormBuilder),
    WsEchoRtt(WsEchoRttBuilder),
    WsHold(WsHoldBuilder),
    WsServerPush(WsServerPushBuilder),
    WsFanout(WsFanoutBuilder),
    Pause(Duration),
    PauseRandom { min: Duration, max: Duration },
}

// ---------------------------------------------------------------------------
// Compilation: StepSource -> Step
// ---------------------------------------------------------------------------

fn compile_step(
    src: StepSource,
    vars: &mut VarRegistry,
    scenario_name: &str,
) -> Result<Step, ScriptError> {
    match src {
        StepSource::Pause(d) => Ok(Step::Pause(d)),
        StepSource::PauseRandom { min, max } => Ok(Step::PauseRandom { min, max }),
        StepSource::SseHold(sb) => {
            let state = sb.take_state();
            let SseHoldBuilderState {
                url,
                headers,
                subscribers,
                hold_for,
                reconnect,
            } = state;
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("sse_hold url {url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let name_tpl =
                    Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("sse_hold header name {name:?}"),
                        error: e,
                    })?;
                let value_tpl =
                    Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("sse_hold header {name:?} value {value:?}"),
                        error: e,
                    })?;
                hdr_out.push((name_tpl, value_tpl));
            }
            Ok(Step::SseHold(zerobench_core::plan::SseHoldPlan {
                url: url_tpl,
                headers: hdr_out,
                subscribers,
                hold_for,
                reconnect,
            }))
        }
        StepSource::WsEchoRtt(wb) => {
            let state = wb.take_state();
            let WsEchoRttBuilderState {
                url,
                headers,
                connections,
                msg_rate_per_conn,
                payload,
                correlate,
            } = state;
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("ws_echo_rtt url {url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let name_tpl =
                    Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("ws_echo_rtt header name {name:?}"),
                        error: e,
                    })?;
                let value_tpl =
                    Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("ws_echo_rtt header {name:?} value {value:?}"),
                        error: e,
                    })?;
                hdr_out.push((name_tpl, value_tpl));
            }
            let payload_tpl =
                Template::compile(&payload, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: "ws_echo_rtt payload".into(),
                    error: e,
                })?;
            Ok(Step::WsEchoRtt(zerobench_core::plan::WsEchoRttPlan {
                url: url_tpl,
                headers: hdr_out,
                connections,
                msg_rate_per_conn,
                correlate,
                payload: payload_tpl,
            }))
        }
        StepSource::WsHold(hb) => {
            let WsHoldBuilderState {
                url,
                headers,
                connections,
                heartbeat,
                heartbeat_frame,
                hold_for,
            } = hb.take_state();
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("ws_hold url {url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let n = Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("ws_hold header name {name:?}"),
                    error: e,
                })?;
                let v = Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("ws_hold header {name:?} value {value:?}"),
                    error: e,
                })?;
                hdr_out.push((n, v));
            }
            Ok(Step::WsHold(zerobench_core::plan::WsHoldPlan {
                url: url_tpl,
                headers: hdr_out,
                connections,
                heartbeat,
                heartbeat_frame,
                hold_for,
            }))
        }
        StepSource::SseFanout(fb) => {
            let SseFanoutBuilderState {
                url,
                headers,
                subscribers,
                hold_for,
                reconnect,
                trigger_url,
                mode,
            } = fb.take_state();
            if trigger_url.is_empty() {
                return Err(ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: "sse_fanout trigger_url".into(),
                    error: zerobench_core::template::TemplateError::NotYetSupported(
                        "sse_fanout requires .trigger_url(...)".into(),
                    ),
                });
            }
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("sse_fanout url {url:?}"),
                error: e,
            })?;
            let trig_tpl = Template::compile(&trigger_url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("sse_fanout trigger_url {trigger_url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let n = Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("sse_fanout header name {name:?}"),
                    error: e,
                })?;
                let v = Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("sse_fanout header {name:?} value {value:?}"),
                    error: e,
                })?;
                hdr_out.push((n, v));
            }
            Ok(Step::SseFanout(zerobench_core::plan::SseFanoutPlan {
                subscribers: zerobench_core::plan::SseHoldPlan {
                    url: url_tpl,
                    headers: hdr_out,
                    subscribers,
                    hold_for,
                    reconnect,
                },
                trigger: zerobench_core::plan::TriggerSpec::HttpPost {
                    url: trig_tpl,
                    body: None,
                },
                mode,
            }))
        }
        StepSource::SseReconnectStorm(sb) => {
            let SseReconnectStormBuilderState {
                url,
                headers,
                subscribers,
                hold_for,
                kill_rate_per_s,
                verify_last_event_id,
            } = sb.take_state();
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("sse_reconnect_storm url {url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let n = Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("sse_reconnect_storm header name {name:?}"),
                    error: e,
                })?;
                let v = Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("sse_reconnect_storm header {name:?} value {value:?}"),
                    error: e,
                })?;
                hdr_out.push((n, v));
            }
            Ok(Step::SseReconnectStorm(
                zerobench_core::plan::SseReconnectStormPlan {
                    subscribers: zerobench_core::plan::SseHoldPlan {
                        url: url_tpl,
                        headers: hdr_out,
                        subscribers,
                        hold_for,
                        reconnect: true,
                    },
                    kill_rate_per_s,
                    verify_last_event_id,
                },
            ))
        }
        StepSource::WsFanout(fb) => {
            let WsFanoutBuilderState {
                url,
                headers,
                connections,
                hold_for,
                heartbeat,
                heartbeat_frame,
                trigger_url,
                mode,
            } = fb.take_state();
            if trigger_url.is_empty() {
                return Err(ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: "ws_fanout trigger_url".into(),
                    error: zerobench_core::template::TemplateError::NotYetSupported(
                        "ws_fanout requires .trigger_url(...)".into(),
                    ),
                });
            }
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("ws_fanout url {url:?}"),
                error: e,
            })?;
            let trig_tpl = Template::compile(&trigger_url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("ws_fanout trigger_url {trigger_url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let n = Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("ws_fanout header name {name:?}"),
                    error: e,
                })?;
                let v = Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("ws_fanout header {name:?} value {value:?}"),
                    error: e,
                })?;
                hdr_out.push((n, v));
            }
            Ok(Step::WsFanout(zerobench_core::plan::WsFanoutPlan {
                subscribers: zerobench_core::plan::WsHoldPlan {
                    url: url_tpl,
                    headers: hdr_out,
                    connections,
                    heartbeat,
                    heartbeat_frame,
                    hold_for,
                },
                trigger: zerobench_core::plan::TriggerSpec::HttpPost {
                    url: trig_tpl,
                    body: None,
                },
                mode,
            }))
        }
        StepSource::WsServerPush(pb) => {
            let WsServerPushBuilderState {
                url,
                headers,
                connections,
                expected_rate_per_conn,
                hold_for,
            } = pb.take_state();
            let url_tpl = Template::compile(&url, vars).map_err(|e| ScriptError::Template {
                scenario: scenario_name.to_string(),
                field: format!("ws_server_push url {url:?}"),
                error: e,
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let n = Template::compile(&name, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("ws_server_push header name {name:?}"),
                    error: e,
                })?;
                let v = Template::compile(&value, vars).map_err(|e| ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("ws_server_push header {name:?} value {value:?}"),
                    error: e,
                })?;
                hdr_out.push((n, v));
            }
            Ok(Step::WsServerPushRtt(
                zerobench_core::plan::WsServerPushRttPlan {
                    url: url_tpl,
                    headers: hdr_out,
                    connections,
                    expected_rate_per_conn,
                    hold_for,
                },
            ))
        }
        StepSource::Request(rb) => {
            let state = rb.take_state();
            let RequestBuilderState {
                method,
                url,
                headers,
                body,
                extract,
                checks,
                cold,
            } = state;

            let url_tpl = Template::compile(&url, vars).map_err(|e| {
                ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("url {url:?}"),
                    error: e,
                }
            })?;

            let mut compiled_headers: SmallVec<[(Template, Template); 8]> =
                SmallVec::new();
            for (name, value) in headers {
                let name_tpl = Template::compile(&name, vars).map_err(|e| {
                    ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("header name {name:?}"),
                        error: e,
                    }
                })?;
                let value_tpl = Template::compile(&value, vars).map_err(|e| {
                    ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("header {name:?} value {value:?}"),
                        error: e,
                    }
                })?;
                compiled_headers.push((name_tpl, value_tpl));
            }

            let body = match body {
                None => None,
                Some(BodySourceSpec::Raw(bytes)) => Some(BodySource::Static(bytes)),
                Some(BodySourceSpec::Template(src))
                | Some(BodySourceSpec::JsonTemplate(src)) => {
                    let tpl = Template::compile(&src, vars).map_err(|e| {
                        ScriptError::Template {
                            scenario: scenario_name.to_string(),
                            field: "body".into(),
                            error: e,
                        }
                    })?;
                    Some(BodySource::Template(tpl))
                }
            };

            let request = RequestPlan {
                method,
                url: url_tpl,
                headers: compiled_headers,
                body,
                extract,
                checks,
                expect_streaming: false,
            };
            Ok(if cold {
                Step::HttpColdConnect(zerobench_core::plan::ColdConnectPlan {
                    request,
                })
            } else {
                Step::Request(request)
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Engine registration
// ---------------------------------------------------------------------------

/// Register every DSL function against `engine`, sharing the top-level
/// [`PlanBuilder`] via moved clones into each registered closure.
pub fn register(engine: &mut Engine, root: PlanBuilder) {
    register_types(engine);
    register_top_level(engine, root.clone());
    register_request_builders(engine);
    register_sse_hold_builders(engine);
    register_ws_echo_rtt_builders(engine);
    register_ws_hold_builders(engine);
    register_ws_server_push_builders(engine);
    register_sse_fanout_builders(engine);
    register_ws_fanout_builders(engine);
    register_sse_reconnect_storm_builders(engine);
    register_scenario_builder(engine);
    register_pause_helpers(engine);
}

fn register_types(engine: &mut Engine) {
    // Opaque types Rhai scripts hold but don't introspect.
    engine.register_type_with_name::<PlanBuilder>("PlanBuilder");
    engine.register_type_with_name::<ScenarioBuilder>("ScenarioBuilder");
    engine.register_type_with_name::<RequestBuilder>("RequestBuilder");
    engine.register_type_with_name::<SseHoldBuilder>("SseHoldBuilder");
    engine.register_type_with_name::<WsEchoRttBuilder>("WsEchoRttBuilder");
    engine.register_type_with_name::<WsHoldBuilder>("WsHoldBuilder");
    engine.register_type_with_name::<WsServerPushBuilder>("WsServerPushBuilder");
    engine.register_type_with_name::<SseFanoutBuilder>("SseFanoutBuilder");
    engine.register_type_with_name::<SseReconnectStormBuilder>("SseReconnectStormBuilder");
    engine.register_type_with_name::<WsFanoutBuilder>("WsFanoutBuilder");
    engine.register_type_with_name::<VarSlotHandle>("VarSlot");
    engine.register_type_with_name::<StepSource>("Step");
}

fn register_top_level(engine: &mut Engine, root: PlanBuilder) {
    // scenario("name", body)  — auto weight.
    //
    // Returns `()` rather than `ScenarioBuilder`. Scripts call this at the
    // top level (`scenario(...);`) as a statement; returning a builder
    // would force every call to be a discarded expression. Rhai's
    // `engine.run()` also expects the overall script to yield `()`, so
    // top-level helpers that return `()` keep that invariant automatic.
    let r = root.clone();
    engine.register_fn(
        "scenario",
        move |ctx: NativeCallContext, name: ImmutableString, body: FnPtr| {
            let scn = ScenarioBuilder::new(name.to_string(), None);
            r.with_state(|s| s.scenarios.push(scn.clone()));
            body.call_within_context::<()>(&ctx, (Dynamic::from(scn),))?;
            Ok::<(), Box<EvalAltResult>>(())
        },
    );

    // scenario("name", weight, body)  — explicit float weight
    let r = root.clone();
    engine.register_fn(
        "scenario",
        move |ctx: NativeCallContext,
              name: ImmutableString,
              weight: f64,
              body: FnPtr| {
            let scn = ScenarioBuilder::new(name.to_string(), Some(weight));
            r.with_state(|s| s.scenarios.push(scn.clone()));
            body.call_within_context::<()>(&ctx, (Dynamic::from(scn),))?;
            Ok::<(), Box<EvalAltResult>>(())
        },
    );
    // Integer-weight overload because Rhai literals `1` come in as i64;
    // without this, `scenario("a", 1, ...)` fails fn resolution (the
    // float variant wants `f64`).
    let r = root.clone();
    engine.register_fn(
        "scenario",
        move |ctx: NativeCallContext,
              name: ImmutableString,
              weight: i64,
              body: FnPtr| {
            let scn =
                ScenarioBuilder::new(name.to_string(), Some(weight as f64));
            r.with_state(|s| s.scenarios.push(scn.clone()));
            body.call_within_context::<()>(&ctx, (Dynamic::from(scn),))?;
            Ok::<(), Box<EvalAltResult>>(())
        },
    );

    // rate("10k/s")  — global open-loop rate
    let r = root.clone();
    engine.register_fn("rate", move |spec: ImmutableString| {
        let rate = parse::parse_rate_with_unit(&spec).map_err(to_rhai_err)?;
        r.with_state(|s| s.global_rate = Some(RateProfile::Constant(rate)));
        Ok::<(), Box<EvalAltResult>>(())
    });

    // saturate(n)  — closed-loop concurrency
    let r = root.clone();
    engine.register_fn("saturate", move |n: i64| {
        let n = if n <= 0 { 1usize } else { n as usize };
        r.with_state(|s| s.saturate_concurrency = Some(n));
    });

    // duration("30s")
    let r = root.clone();
    engine.register_fn("duration", move |spec: ImmutableString| {
        let d = parse::parse_duration(&spec)
            .ok_or_else(|| to_rhai_err(format!("invalid duration {spec:?}")))?;
        r.with_state(|s| s.duration = Some(d));
        Ok::<(), Box<EvalAltResult>>(())
    });

    // warmup("2s")
    let r = root.clone();
    engine.register_fn("warmup", move |spec: ImmutableString| {
        let d = parse::parse_duration(&spec)
            .ok_or_else(|| to_rhai_err(format!("invalid warmup {spec:?}")))?;
        r.with_state(|s| s.warmup = Some(d));
        Ok::<(), Box<EvalAltResult>>(())
    });

    // cooldown("10s") — TIME_WAIT drain between runs.
    let r = root.clone();
    engine.register_fn("cooldown", move |spec: ImmutableString| {
        let d = parse::parse_duration(&spec)
            .ok_or_else(|| to_rhai_err(format!("invalid cooldown {spec:?}")))?;
        r.with_state(|s| s.cooldown = Some(d));
        Ok::<(), Box<EvalAltResult>>(())
    });

    // runs(3) — iterations per plan. Feeds the bootstrap CI aggregator.
    let r = root.clone();
    engine.register_fn("runs", move |n: i64| {
        let n = if n < 1 { 1u32 } else { n as u32 };
        r.with_state(|s| s.runs = Some(n));
    });

    // threads(8) — client-side worker thread count.
    let r = root.clone();
    engine.register_fn("threads", move |n: i64| {
        let n = if n < 1 { 1usize } else { n as usize };
        r.with_state(|s| s.threads = Some(n));
    });

    // plan_name("chat-burst") — overrides the default Target-host name
    // in fingerprints. Useful when a single service is measured under
    // multiple logical profiles.
    let r = root.clone();
    engine.register_fn("plan_name", move |name: ImmutableString| {
        r.with_state(|s| s.name = Some(name.to_string()));
    });

    // transport("h1" | "h2")
    let r = root.clone();
    engine.register_fn("transport", move |which: ImmutableString| {
        let ver = match which.as_str() {
            "auto" => HttpVersionPref::Auto,
            "h1" | "http1" => HttpVersionPref::Http1,
            "h2" | "http2" => HttpVersionPref::Http2,
            other => {
                return Err(to_rhai_err(format!(
                    "transport {other:?} not supported (h1 | h2 | auto)"
                )))
            }
        };
        r.with_state(|s| s.transport = ver);
        Ok::<(), Box<EvalAltResult>>(())
    });

    // env("NAME") — required env var (or script error)
    engine.register_fn("env", move |ctx: NativeCallContext, name: ImmutableString| {
        match std::env::var(name.as_str()) {
            Ok(v) => Ok::<ImmutableString, Box<EvalAltResult>>(v.into()),
            Err(_) => Err(Box::new(EvalAltResult::ErrorRuntime(
                Dynamic::from(format!(
                    "env variable {:?} not set and no default supplied",
                    name.as_str()
                )),
                ctx.call_position(),
            ))),
        }
    });

    // env("NAME", "default") — with fallback
    engine.register_fn(
        "env",
        move |name: ImmutableString, default: ImmutableString| {
            match std::env::var(name.as_str()) {
                Ok(v) => ImmutableString::from(v),
                Err(_) => default,
            }
        },
    );

    // slot("name") — allocate a VarSlot handle.
    //
    // Named `slot` rather than `var` because Rhai reserves `var` as a
    // keyword (future-compat with `static`/`var` declarations). Returns
    // a thin `VarSlotHandle` wrapping the slot index + textual name.
    // Extractors that take the handle reuse the same slot; templates
    // that reference `{{var:name}}` hit the same registry and resolve
    // to the same slot.
    let r = root.clone();
    engine.register_fn("slot", move |name: ImmutableString| {
        let name_str = name.to_string();
        let handle = r.with_state(|s| match s.vars.allocate(&name_str) {
            Ok(slot) => Ok(VarSlotHandle { slot, name: name_str }),
            Err(e) => Err(e),
        });
        handle.map_err(|e| to_rhai_err(format!("slot allocation: {e}")))
    });
}

fn register_request_builders(engine: &mut Engine) {
    for (name, method) in [
        ("GET", Method::GET),
        ("POST", Method::POST),
        ("PUT", Method::PUT),
        ("DELETE", Method::DELETE),
        ("PATCH", Method::PATCH),
        ("HEAD", Method::HEAD),
        ("OPTIONS", Method::OPTIONS),
    ] {
        let m = method.clone();
        engine.register_fn(name, move |url: ImmutableString| {
            RequestBuilder::new(m.clone(), url.to_string())
        });
    }

    // .header(name, value)
    engine.register_fn(
        "header",
        move |b: RequestBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );

    // .body(string)  — template if it contains `{{`, else raw bytes.
    engine.register_fn("body", move |b: RequestBuilder, body: ImmutableString| {
        b.with_state(|s| {
            let body_str = body.to_string();
            if body_str.contains("{{") {
                s.body = Some(BodySourceSpec::Template(body_str));
            } else {
                s.body = Some(BodySourceSpec::Raw(Bytes::from(body_str.into_bytes())));
            }
        });
        b
    });

    // .body_file(path) — read at script-eval time.
    engine.register_fn(
        "body_file",
        move |ctx: NativeCallContext, b: RequestBuilder, path: ImmutableString| {
            let bytes = std::fs::read(path.as_str()).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(
                    Dynamic::from(format!("body_file {:?}: {e}", path.as_str())),
                    ctx.call_position(),
                ))
            })?;
            b.with_state(|s| s.body = Some(BodySourceSpec::Raw(Bytes::from(bytes))));
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .json(#{ ... })  — serializes the map to JSON and sets Content-Type.
    engine.register_fn(
        "json",
        move |ctx: NativeCallContext, b: RequestBuilder, obj: Dynamic| {
            let json = dynamic_to_json(&obj).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(
                    Dynamic::from(format!("json serialization: {e}")),
                    ctx.call_position(),
                ))
            })?;
            b.with_state(|s| {
                // JSON bodies always go through the template engine so
                // values like "{{uuid}}" nested in the map expand per
                // iteration. If there were no templates at all, the
                // template compiler optimizes to a single Literal part.
                s.body = Some(BodySourceSpec::JsonTemplate(json));
                // Set Content-Type unless the user already did.
                let has_ct = s
                    .headers
                    .iter()
                    .any(|(n, _)| n.eq_ignore_ascii_case("content-type"));
                if !has_ct {
                    s.headers.push((
                        "Content-Type".into(),
                        "application/json".into(),
                    ));
                }
            });
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .expect_status(n)
    engine.register_fn("expect_status", move |b: RequestBuilder, code: i64| {
        b.with_state(|s| s.checks.push(Assertion::StatusEq(clamp_u16(code))));
        b
    });

    // .expect_status_in([200, 201, 204])  — Rhai arrays of Dynamic values.
    engine.register_fn(
        "expect_status_in",
        move |ctx: NativeCallContext, b: RequestBuilder, arr: rhai::Array| {
            let mut codes: SmallVec<[u16; 4]> = SmallVec::new();
            for v in arr {
                let code = v.as_int().map_err(|ty| {
                    Box::new(EvalAltResult::ErrorMismatchDataType(
                        "integer".into(),
                        ty.into(),
                        ctx.call_position(),
                    ))
                })?;
                codes.push(clamp_u16(code));
            }
            b.with_state(|s| s.checks.push(Assertion::StatusIn(codes)));
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .expect_latency_under("500ms")
    engine.register_fn(
        "expect_latency_under",
        move |b: RequestBuilder, spec: ImmutableString| {
            let d = parse::parse_duration(&spec).ok_or_else(|| {
                to_rhai_err(format!("invalid latency duration {spec:?}"))
            })?;
            b.with_state(|s| s.checks.push(Assertion::LatencyUnder(d)));
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .extract_header("X-Name", var_slot)
    engine.register_fn(
        "extract_header",
        move |ctx: NativeCallContext,
              b: RequestBuilder,
              header: ImmutableString,
              slot: VarSlotHandle| {
            let name = HeaderName::from_bytes(header.as_bytes()).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(
                    Dynamic::from(format!("invalid header name {:?}: {e}", header.as_str())),
                    ctx.call_position(),
                ))
            })?;
            b.with_state(|s| {
                s.extract.push(Extract::Header {
                    name,
                    into: slot.slot,
                })
            });
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .extract_status(var_slot)
    engine.register_fn(
        "extract_status",
        move |b: RequestBuilder, slot: VarSlotHandle| {
            b.with_state(|s| s.extract.push(Extract::StatusCode { into: slot.slot }));
            b
        },
    );

    // .cold_connect()  — fresh TCP+TLS+HTTP connection per request,
    // no pool reuse. Compiles to Step::HttpColdConnect at finalize.
    engine.register_fn("cold_connect", move |b: RequestBuilder| {
        b.with_state(|s| s.cold = true);
        b
    });
}

fn register_sse_hold_builders(engine: &mut Engine) {
    // sse_hold(url, subscribers, hold_for).
    engine.register_fn(
        "sse_hold",
        move |url: ImmutableString, subs: i64, hold_for: ImmutableString| {
            let secs = parse_duration_str(&hold_for).unwrap_or(Duration::from_secs(60));
            let subs_u: u32 = if subs < 0 { 1 } else { (subs as u32).max(1) };
            SseHoldBuilder::new(url.to_string(), subs_u, secs)
        },
    );

    // Overload that takes a raw seconds int for `hold_for`.
    engine.register_fn(
        "sse_hold",
        move |url: ImmutableString, subs: i64, hold_for_secs: i64| {
            let secs = if hold_for_secs <= 0 { 60 } else { hold_for_secs as u64 };
            let subs_u: u32 = if subs < 0 { 1 } else { (subs as u32).max(1) };
            SseHoldBuilder::new(url.to_string(), subs_u, Duration::from_secs(secs))
        },
    );

    // .header(name, value)
    engine.register_fn(
        "header",
        move |b: SseHoldBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );

    // .reconnect(bool)
    engine.register_fn("reconnect", move |b: SseHoldBuilder, on: bool| {
        b.with_state(|s| s.reconnect = on);
        b
    });
}

fn register_ws_echo_rtt_builders(engine: &mut Engine) {
    // ws_echo_rtt(url, connections, msg_rate_per_conn)
    engine.register_fn(
        "ws_echo_rtt",
        move |url: ImmutableString, conns: i64, rate: f64| {
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsEchoRttBuilder::new(url.to_string(), c, rate)
        },
    );
    // Overload with i64 rate (msg/sec whole number).
    engine.register_fn(
        "ws_echo_rtt",
        move |url: ImmutableString, conns: i64, rate: i64| {
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsEchoRttBuilder::new(url.to_string(), c, rate as f64)
        },
    );

    // .header(name, value)
    engine.register_fn(
        "header",
        move |b: WsEchoRttBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );

    // .payload(text)
    engine.register_fn(
        "payload",
        move |b: WsEchoRttBuilder, text: ImmutableString| {
            b.with_state(|s| s.payload = text.to_string());
            b
        },
    );

    // .correlate(strategy) — how to match server echoes to client sends.
    // Accepts "pingpong", "prepend" (default), "first_text", or
    // "substring:<marker>". Unknown values fail at plan build time.
    engine.register_fn(
        "correlate",
        move |ctx: NativeCallContext,
              b: WsEchoRttBuilder,
              strategy: ImmutableString|
              -> Result<WsEchoRttBuilder, Box<EvalAltResult>> {
            let parsed = parse_correlate(&strategy).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(e.into(), ctx.call_position()))
            })?;
            b.with_state(|s| s.correlate = parsed);
            Ok(b)
        },
    );
}

fn register_ws_hold_builders(engine: &mut Engine) {
    // ws_hold(url, connections, hold_for) — idle-capacity test.
    engine.register_fn(
        "ws_hold",
        move |url: ImmutableString, conns: i64, hold_for: ImmutableString| {
            let secs = parse_duration_str(&hold_for).unwrap_or(Duration::from_secs(60));
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsHoldBuilder::new(url.to_string(), c, secs)
        },
    );
    engine.register_fn(
        "ws_hold",
        move |url: ImmutableString, conns: i64, hold_for_secs: i64| {
            let secs = if hold_for_secs <= 0 { 60 } else { hold_for_secs as u64 };
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsHoldBuilder::new(url.to_string(), c, Duration::from_secs(secs))
        },
    );
    engine.register_fn(
        "heartbeat",
        move |b: WsHoldBuilder, interval: ImmutableString| {
            let d = parse_duration_str(&interval).unwrap_or(Duration::from_secs(25));
            b.with_state(|s| s.heartbeat = d);
            b
        },
    );
    engine.register_fn(
        "header",
        move |b: WsHoldBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );
    // .heartbeat_frame(kind) — "ping" (RFC 6455 Ping, default) or
    // "text" (app-level text frame, for servers that don't reply to
    // Ping).
    engine.register_fn(
        "heartbeat_frame",
        move |ctx: NativeCallContext,
              b: WsHoldBuilder,
              kind: ImmutableString|
              -> Result<WsHoldBuilder, Box<EvalAltResult>> {
            let parsed = parse_heartbeat_frame(&kind).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(e.into(), ctx.call_position()))
            })?;
            b.with_state(|s| s.heartbeat_frame = parsed);
            Ok(b)
        },
    );
}

fn register_sse_fanout_builders(engine: &mut Engine) {
    engine.register_fn(
        "sse_fanout",
        move |url: ImmutableString, subs: i64, hold_for: ImmutableString| {
            let secs = parse_duration_str(&hold_for).unwrap_or(Duration::from_secs(60));
            let s: u32 = if subs < 0 { 1 } else { (subs as u32).max(1) };
            SseFanoutBuilder::new(url.to_string(), s, secs)
        },
    );
    engine.register_fn(
        "trigger_url",
        move |b: SseFanoutBuilder, url: ImmutableString| {
            b.with_state(|s| s.trigger_url = url.to_string());
            b
        },
    );
    engine.register_fn(
        "reconnect",
        move |b: SseFanoutBuilder, on: bool| {
            b.with_state(|s| s.reconnect = on);
            b
        },
    );
    engine.register_fn(
        "header",
        move |b: SseFanoutBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );
    // .mode(kind) — "trigger_rtt" (default, proxy from trigger 2xx)
    // or "timestamp[:<field>]" (read emit ns from the broadcast
    // payload; requires server cooperation).
    engine.register_fn(
        "mode",
        move |ctx: NativeCallContext,
              b: SseFanoutBuilder,
              kind: ImmutableString|
              -> Result<SseFanoutBuilder, Box<EvalAltResult>> {
            let parsed = parse_fanout_mode(&kind).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(e.into(), ctx.call_position()))
            })?;
            b.with_state(|s| s.mode = parsed);
            Ok(b)
        },
    );
}

fn register_ws_fanout_builders(engine: &mut Engine) {
    engine.register_fn(
        "ws_fanout",
        move |url: ImmutableString, conns: i64, hold_for: ImmutableString| {
            let secs = parse_duration_str(&hold_for).unwrap_or(Duration::from_secs(60));
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsFanoutBuilder::new(url.to_string(), c, secs)
        },
    );
    engine.register_fn(
        "trigger_url",
        move |b: WsFanoutBuilder, url: ImmutableString| {
            b.with_state(|s| s.trigger_url = url.to_string());
            b
        },
    );
    engine.register_fn(
        "heartbeat",
        move |b: WsFanoutBuilder, interval: ImmutableString| {
            let d = parse_duration_str(&interval).unwrap_or(Duration::from_secs(25));
            b.with_state(|s| s.heartbeat = d);
            b
        },
    );
    engine.register_fn(
        "header",
        move |b: WsFanoutBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );
    engine.register_fn(
        "heartbeat_frame",
        move |ctx: NativeCallContext,
              b: WsFanoutBuilder,
              kind: ImmutableString|
              -> Result<WsFanoutBuilder, Box<EvalAltResult>> {
            let parsed = parse_heartbeat_frame(&kind).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(e.into(), ctx.call_position()))
            })?;
            b.with_state(|s| s.heartbeat_frame = parsed);
            Ok(b)
        },
    );
    engine.register_fn(
        "mode",
        move |ctx: NativeCallContext,
              b: WsFanoutBuilder,
              kind: ImmutableString|
              -> Result<WsFanoutBuilder, Box<EvalAltResult>> {
            let parsed = parse_fanout_mode(&kind).map_err(|e| {
                Box::new(EvalAltResult::ErrorRuntime(e.into(), ctx.call_position()))
            })?;
            b.with_state(|s| s.mode = parsed);
            Ok(b)
        },
    );
}

fn register_sse_reconnect_storm_builders(engine: &mut Engine) {
    engine.register_fn(
        "sse_reconnect_storm",
        move |url: ImmutableString, subs: i64, hold_for: ImmutableString| {
            let secs = parse_duration_str(&hold_for).unwrap_or(Duration::from_secs(60));
            let s: u32 = if subs < 0 { 1 } else { (subs as u32).max(1) };
            SseReconnectStormBuilder::new(url.to_string(), s, secs)
        },
    );
    engine.register_fn(
        "kill_rate",
        move |b: SseReconnectStormBuilder, rate: f64| {
            b.with_state(|s| s.kill_rate_per_s = rate);
            b
        },
    );
    engine.register_fn(
        "verify_last_event_id",
        move |b: SseReconnectStormBuilder, on: bool| {
            b.with_state(|s| s.verify_last_event_id = on);
            b
        },
    );
    engine.register_fn(
        "header",
        move |b: SseReconnectStormBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );
}

fn register_ws_server_push_builders(engine: &mut Engine) {
    // ws_server_push(url, connections, hold_for) — read-only RTT.
    engine.register_fn(
        "ws_server_push",
        move |url: ImmutableString, conns: i64, hold_for: ImmutableString| {
            let secs = parse_duration_str(&hold_for).unwrap_or(Duration::from_secs(60));
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsServerPushBuilder::new(url.to_string(), c, secs)
        },
    );
    engine.register_fn(
        "ws_server_push",
        move |url: ImmutableString, conns: i64, hold_for_secs: i64| {
            let secs = if hold_for_secs <= 0 { 60 } else { hold_for_secs as u64 };
            let c: u32 = if conns < 0 { 1 } else { (conns as u32).max(1) };
            WsServerPushBuilder::new(url.to_string(), c, Duration::from_secs(secs))
        },
    );
    engine.register_fn(
        "expected_rate",
        move |b: WsServerPushBuilder, rate: f64| {
            b.with_state(|s| s.expected_rate_per_conn = rate);
            b
        },
    );
    engine.register_fn(
        "header",
        move |b: WsServerPushBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );
}

/// Parse a human duration like "60s", "5m", "500ms", or fall back to
/// seconds when bare. `None` if unparseable.
fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix("ms") {
        Some(Duration::from_millis(n.trim().parse().ok()?))
    } else if let Some(n) = s.strip_suffix('s') {
        Some(Duration::from_secs_f64(n.trim().parse().ok()?))
    } else if let Some(n) = s.strip_suffix('m') {
        Some(Duration::from_secs_f64(n.trim().parse::<f64>().ok()? * 60.0))
    } else {
        Some(Duration::from_secs(s.parse().ok()?))
    }
}

fn register_scenario_builder(engine: &mut Engine) {
    // s.step(request_builder)  — Request step.
    //
    // Returns `()` rather than `ScenarioBuilder`. Scripts call step in
    // sequence (`s.step(...); s.step(...);`), and Rhai's block-expression
    // value is the last statement's value. If `.step` returned `s`, the
    // closure body's value would be the last-step `s`, and Rhai would
    // require `scenario` to declare a `ScenarioBuilder` return type.
    // Returning `()` keeps the closure body's return type as `()` and
    // matches how the top-level `scenario(...)` helper is registered.
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, req: RequestBuilder| {
            s.with_state(|st| st.steps.push(StepSource::Request(req)));
        },
    );

    // Protocol-native v0.1.0 step variants.
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: SseHoldBuilder| {
            s.with_state(|st| st.steps.push(StepSource::SseHold(b)));
        },
    );
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: WsEchoRttBuilder| {
            s.with_state(|st| st.steps.push(StepSource::WsEchoRtt(b)));
        },
    );
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: WsHoldBuilder| {
            s.with_state(|st| st.steps.push(StepSource::WsHold(b)));
        },
    );
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: WsServerPushBuilder| {
            s.with_state(|st| st.steps.push(StepSource::WsServerPush(b)));
        },
    );
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: SseFanoutBuilder| {
            s.with_state(|st| st.steps.push(StepSource::SseFanout(b)));
        },
    );
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: SseReconnectStormBuilder| {
            s.with_state(|st| st.steps.push(StepSource::SseReconnectStorm(b)));
        },
    );
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, b: WsFanoutBuilder| {
            s.with_state(|st| st.steps.push(StepSource::WsFanout(b)));
        },
    );

    // s.step(pause(...))  — Pause / PauseRandom step (StepSource directly)
    engine.register_fn("step", move |s: ScenarioBuilder, step: StepSource| {
        s.with_state(|st| st.steps.push(step));
    });

    // s.rate("3k/s")  — per-scenario open-loop rate
    engine.register_fn(
        "rate",
        move |s: ScenarioBuilder, spec: ImmutableString| {
            let rate = parse::parse_rate_with_unit(&spec).map_err(to_rhai_err)?;
            s.with_state(|st| st.rate = Some(RateProfile::Constant(rate)));
            Ok::<(), Box<EvalAltResult>>(())
        },
    );

    // s.saturate(n)  — per-scenario closed-loop concurrency
    engine.register_fn("saturate", move |s: ScenarioBuilder, n: i64| {
        let n = if n <= 0 { 1usize } else { n as usize };
        s.with_state(|st| {
            st.rate = Some(RateProfile::Saturate {
                max_concurrency: n,
            })
        });
    });
}

fn register_pause_helpers(_engine: &mut Engine) {
    // pause() / pause_random() are intentionally NOT registered on
    // the Rhai engine.
    //
    // Rationale: inter-step pauses require per-connection "ready-at"
    // tracking in the mio event loop (a pause blocks only the
    // connection that saw it, not the worker thread's other N-1
    // connections). Wiring that through the existing state machine is
    // a substantial refactor we haven't shipped yet. Rather than keep
    // the DSL registrations and have every backend silently drop
    // `Step::Pause` on the floor — which produced benchmarks where
    // users THOUGHT they were measuring think-time effects but
    // weren't — scripts now fail at script-eval time with
    // `unknown function 'pause'`, which is the loudest possible
    // signal.
    //
    // `Step::Pause` / `Step::PauseRandom` remain in the core Plan
    // enum so a future implementation can wire them without another
    // plan-schema bump.
    let _ = register_pause_helpers_unreachable;
}

#[allow(dead_code)]
fn register_pause_helpers_unreachable(_engine: &mut Engine) {
    // Dead helper kept for reference — the shape of the registrations
    // we'd need to restore to re-enable pause/pause_random once the
    // mio_h1 event loop supports per-connection ready-at tracking.
    let _ = || {
        let _ = (|spec: ImmutableString| {
            let d = parse::parse_duration(&spec)
                .ok_or_else(|| to_rhai_err(format!("invalid pause duration {spec:?}")))?;
            Ok::<StepSource, Box<EvalAltResult>>(StepSource::Pause(d))
        },);
        let _ = (|lo: ImmutableString, hi: ImmutableString| {
            let min = parse::parse_duration(&lo)
                .ok_or_else(|| to_rhai_err(format!("invalid pause min {lo:?}")))?;
            let max = parse::parse_duration(&hi)
                .ok_or_else(|| to_rhai_err(format!("invalid pause max {hi:?}")))?;
            if min > max {
                return Err(to_rhai_err(format!(
                    "pause_random: min {lo:?} > max {hi:?}"
                )));
            }
            Ok::<StepSource, Box<EvalAltResult>>(StepSource::PauseRandom {
                min,
                max,
            })
        },);
    };
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Thin script-visible handle around a [`VarSlot`] + the variable's name.
/// The name is retained on construction for future diagnostic use; only
/// `slot` is consumed by the current Rhai registrations.
#[derive(Debug, Clone)]
pub(crate) struct VarSlotHandle {
    pub slot: VarSlot,
    #[allow(dead_code)]
    pub name: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_rhai_err(msg: impl Into<String>) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::ErrorRuntime(
        Dynamic::from(msg.into()),
        rhai::Position::NONE,
    ))
}

fn clamp_u16(code: i64) -> u16 {
    if code < 0 {
        0
    } else if code > u16::MAX as i64 {
        u16::MAX
    } else {
        code as u16
    }
}

/// Convert a Rhai Dynamic (maps, arrays, scalars, strings) to a JSON
/// string. Keeps `{{...}}` template bytes intact so the template engine
/// sees them during finalize.
fn dynamic_to_json(v: &Dynamic) -> Result<String, String> {
    let json = dynamic_to_serde(v)?;
    serde_json::to_string(&json).map_err(|e| format!("{e}"))
}

fn dynamic_to_serde(v: &Dynamic) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    if v.is_unit() {
        return Ok(Value::Null);
    }
    if let Ok(b) = v.as_bool() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = v.as_int() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(f) = v.as_float() {
        // serde_json can't represent NaN/Inf; coerce to null per the usual
        // JS convention. Finite f64 goes through as a Number.
        return Ok(match serde_json::Number::from_f64(f) {
            Some(n) => Value::Number(n),
            None => Value::Null,
        });
    }
    if let Ok(c) = v.as_char() {
        return Ok(Value::String(c.to_string()));
    }
    if v.is_string() {
        return Ok(Value::String(v.clone().into_string().map_err(|t| {
            format!("expected string, got {t}")
        })?));
    }
    if v.is_array() {
        let arr = v.clone().into_array().map_err(|t| {
            format!("expected array, got {t}")
        })?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(dynamic_to_serde(&item)?);
        }
        return Ok(Value::Array(out));
    }
    if v.is_map() {
        let m = v.clone().try_cast::<rhai::Map>().ok_or_else(|| {
            format!("expected map, got {}", v.type_name())
        })?;
        let mut obj = serde_json::Map::with_capacity(m.len());
        for (k, val) in m {
            obj.insert(k.to_string(), dynamic_to_serde(&val)?);
        }
        return Ok(Value::Object(obj));
    }
    Err(format!(
        "cannot serialize Rhai value of type {} to JSON",
        v.type_name()
    ))
}
