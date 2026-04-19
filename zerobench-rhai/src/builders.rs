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
    Assertion, BodySource, Extract, Mode, Plan, RateProfile, RequestPlan, Scenario, SsePlan,
    Step, WsRoundPlan,
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
                            StepSource::Sse(sb) => {
                                Some(sb.with_state(|s| s.url.clone()))
                            }
                            StepSource::Ws(wb) => {
                                Some(wb.with_state(|w| w.url.clone()))
                            }
                            StepSource::SseHold(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            StepSource::WsEchoRtt(b) => {
                                Some(b.with_state(|s| s.url.clone()))
                            }
                            _ => None,
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
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::default(),
        name: String::new(),
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
// SseBuilder — `SSE(url).header(...).expect_chunks(N)`
// ---------------------------------------------------------------------------

/// Returned by `SSE(url)` and chained through `.header(...)` /
/// `.expect_chunks(...)`. Finalized into a [`Step::SseStream`] during
/// [`PlanBuilder::finalize`].
#[derive(Clone)]
pub(crate) struct SseBuilder {
    inner: Arc<Mutex<SseBuilderState>>,
}

#[derive(Default)]
pub(crate) struct SseBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub expect_chunks: Option<usize>,
}

impl SseBuilder {
    fn new(url: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SseBuilderState {
                url,
                headers: Vec::new(),
                expect_chunks: None,
            })),
        }
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut SseBuilderState) -> R) -> R {
        let mut guard = self
            .inner
            .lock()
            .expect("SSE builder mutex poisoned");
        f(&mut guard)
    }

    /// Take the state out of the Arc. See [`PlanBuilder::finalize`].
    fn take_state(&self) -> SseBuilderState {
        self.with_state(std::mem::take)
    }
}

// ---------------------------------------------------------------------------
// WsBuilder — `WS(url).header(...).message(text)`
// ---------------------------------------------------------------------------

/// Returned by `WS(url)` and chained through `.header(...)` /
/// `.message(...)`. Finalized into a [`Step::WsRound`] during
/// [`PlanBuilder::finalize`].
#[derive(Clone)]
pub(crate) struct WsBuilder {
    inner: Arc<Mutex<WsBuilderState>>,
}

#[derive(Default)]
pub(crate) struct WsBuilderState {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub message: String,
}

impl WsBuilder {
    fn new(url: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WsBuilderState {
                url,
                headers: Vec::new(),
                // Default to empty text frame — scripts that want a
                // payload call `.message("...")`.
                message: String::new(),
            })),
        }
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut WsBuilderState) -> R) -> R {
        let mut guard = self
            .inner
            .lock()
            .expect("WS builder mutex poisoned");
        f(&mut guard)
    }

    /// Take the state out of the Arc. See [`PlanBuilder::finalize`].
    fn take_state(&self) -> WsBuilderState {
        self.with_state(std::mem::take)
    }
}

// ---------------------------------------------------------------------------
// v0.1.0 protocol-native builders
//
// SseHoldBuilder / WsEchoRttBuilder wrap the Phase 1 SseHoldPlan /
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
}

impl Default for WsEchoRttBuilderState {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            connections: 1,
            msg_rate_per_conn: 100.0,
            payload: "ping".into(),
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
    Sse(SseBuilder),
    Ws(WsBuilder),
    SseHold(SseHoldBuilder),
    WsEchoRtt(WsEchoRttBuilder),
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
        StepSource::Sse(sb) => {
            let state = sb.take_state();
            let SseBuilderState {
                url,
                headers,
                expect_chunks,
            } = state;
            let url_tpl = Template::compile(&url, vars).map_err(|e| {
                ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("SSE url {url:?}"),
                    error: e,
                }
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let name_tpl = Template::compile(&name, vars).map_err(|e| {
                    ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("SSE header name {name:?}"),
                        error: e,
                    }
                })?;
                let value_tpl = Template::compile(&value, vars).map_err(|e| {
                    ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("SSE header {name:?} value {value:?}"),
                        error: e,
                    }
                })?;
                hdr_out.push((name_tpl, value_tpl));
            }
            Ok(Step::SseStream(SsePlan {
                url: url_tpl,
                headers: hdr_out,
                expect_chunks,
            }))
        }
        StepSource::Ws(wb) => {
            let state = wb.take_state();
            let WsBuilderState {
                url,
                headers,
                message,
            } = state;
            let url_tpl = Template::compile(&url, vars).map_err(|e| {
                ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: format!("WS url {url:?}"),
                    error: e,
                }
            })?;
            let mut hdr_out: SmallVec<[(Template, Template); 4]> = SmallVec::new();
            for (name, value) in headers {
                let name_tpl = Template::compile(&name, vars).map_err(|e| {
                    ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("WS header name {name:?}"),
                        error: e,
                    }
                })?;
                let value_tpl = Template::compile(&value, vars).map_err(|e| {
                    ScriptError::Template {
                        scenario: scenario_name.to_string(),
                        field: format!("WS header {name:?} value {value:?}"),
                        error: e,
                    }
                })?;
                hdr_out.push((name_tpl, value_tpl));
            }
            let message_tpl = Template::compile(&message, vars).map_err(|e| {
                ScriptError::Template {
                    scenario: scenario_name.to_string(),
                    field: "WS message".into(),
                    error: e,
                }
            })?;
            Ok(Step::WsRound(WsRoundPlan {
                url: url_tpl,
                headers: hdr_out,
                message: message_tpl,
            }))
        }
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
                correlate: zerobench_core::plan::CorrelateStrategy::MonotonicIdPrepend,
                payload: payload_tpl,
            }))
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

            Ok(Step::Request(RequestPlan {
                method,
                url: url_tpl,
                headers: compiled_headers,
                body,
                extract,
                checks,
                expect_streaming: false,
            }))
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
    register_sse_builders(engine);
    register_ws_builders(engine);
    register_sse_hold_builders(engine);
    register_ws_echo_rtt_builders(engine);
    register_scenario_builder(engine);
    register_pause_helpers(engine);
}

fn register_types(engine: &mut Engine) {
    // Opaque types Rhai scripts hold but don't introspect.
    engine.register_type_with_name::<PlanBuilder>("PlanBuilder");
    engine.register_type_with_name::<ScenarioBuilder>("ScenarioBuilder");
    engine.register_type_with_name::<RequestBuilder>("RequestBuilder");
    engine.register_type_with_name::<SseBuilder>("SseBuilder");
    engine.register_type_with_name::<WsBuilder>("WsBuilder");
    engine.register_type_with_name::<SseHoldBuilder>("SseHoldBuilder");
    engine.register_type_with_name::<WsEchoRttBuilder>("WsEchoRttBuilder");
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
}

fn register_sse_builders(engine: &mut Engine) {
    // SSE(url) -> SseBuilder
    engine.register_fn("SSE", move |url: ImmutableString| {
        SseBuilder::new(url.to_string())
    });

    // .header(name, value)
    engine.register_fn(
        "header",
        move |b: SseBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );

    // .expect_chunks(n) — minimum number of data events.
    engine.register_fn("expect_chunks", move |b: SseBuilder, n: i64| {
        let n = if n < 0 { 0usize } else { n as usize };
        b.with_state(|s| s.expect_chunks = Some(n));
        b
    });
}

fn register_sse_hold_builders(engine: &mut Engine) {
    // sse_hold(url, subscribers, hold_for) — Phase 6a hold semantics.
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

fn register_ws_builders(engine: &mut Engine) {
    // WS(url) -> WsBuilder
    engine.register_fn("WS", move |url: ImmutableString| {
        WsBuilder::new(url.to_string())
    });

    // .header(name, value)
    engine.register_fn(
        "header",
        move |b: WsBuilder, name: ImmutableString, value: ImmutableString| {
            b.with_state(|s| s.headers.push((name.to_string(), value.to_string())));
            b
        },
    );

    // .message(text) — text-frame payload sent per iteration.
    engine.register_fn(
        "message",
        move |b: WsBuilder, text: ImmutableString| {
            b.with_state(|s| s.message = text.to_string());
            b
        },
    );
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

    // s.step(sse_builder)  — SSE step.
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, sse: SseBuilder| {
            s.with_state(|st| st.steps.push(StepSource::Sse(sse)));
        },
    );

    // s.step(ws_builder)  — WS round step.
    engine.register_fn(
        "step",
        move |s: ScenarioBuilder, ws: WsBuilder| {
            s.with_state(|st| st.steps.push(StepSource::Ws(ws)));
        },
    );

    // v0.1.0 protocol-native variants.
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

fn register_pause_helpers(engine: &mut Engine) {
    // pause("50ms")
    engine.register_fn("pause", move |spec: ImmutableString| {
        let d = parse::parse_duration(&spec)
            .ok_or_else(|| to_rhai_err(format!("invalid pause duration {spec:?}")))?;
        Ok::<StepSource, Box<EvalAltResult>>(StepSource::Pause(d))
    });

    // pause_random("10ms", "20ms")
    engine.register_fn(
        "pause_random",
        move |lo: ImmutableString, hi: ImmutableString| {
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
        },
    );
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
