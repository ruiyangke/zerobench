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
//! The [`define_builder!`] macro collapses this pattern: each invocation
//! produces the wrapper struct, an `Arc<Mutex<..>>`-backed `Default` impl,
//! and the `modify`/`with_state`/`take_state` helper surface.
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
//!
//! ARCH(builder-unify): the per-protocol construction in [`compile_step`]
//! duplicates the CLI's `plan_from_cli.rs`. Resolved in Phase 4b
//! (CLI/DSL unify) — see ARCH-REVIEW §4.5, §B3, §B5.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderName, Method};
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, ImmutableString, NativeCallContext};
use smallvec::SmallVec;

use zerobench_core::plan::{
    Assertion, BodySource, Extract, Mode, Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::template::{Template, TemplateError};
use zerobench_core::transport::HttpVersionPref;
use zerobench_core::var::{VarRegistry, VarSlot};

use crate::error::ScriptError;
use crate::parse;

// ---------------------------------------------------------------------------
// define_builder! — collapses the Arc<Mutex<State>> newtype pattern.
//
// Every Rhai-facing builder has the same shape:
//   - `Clone` wrapper around `Arc<Mutex<State>>`
//   - a `Default` state (auto-derived; the Default is only consumed by
//     `std::mem::take` at finalize, so field-level defaults are not
//     semantically meaningful — `new(args)` overrides them)
//   - `with_state(state) -> Self` constructor (handy when we want to
//     consume a state we built piece-wise)
//   - `modify(|s| ...) -> R` — a scoped mutex guard that centralises the
//     `.lock().expect(...)` boilerplate
//   - `take_state() -> State` — `mem::take`-based extraction used by
//     `finalize` / `compile_step`
//
// The macro is crate-private (no `#[macro_export]`); it's only used by
// this module.
// ---------------------------------------------------------------------------

macro_rules! define_builder {
    (
        $(#[$outer:meta])*
        $vis:vis struct $Name:ident [ $State:ident ] {
            $(
                $(#[$field_meta:meta])*
                pub $field:ident : $ty:ty
            ),* $(,)?
        }
    ) => {
        $(#[$outer])*
        #[derive(Clone)]
        $vis struct $Name {
            inner: ::std::sync::Arc<::std::sync::Mutex<$State>>,
        }

        #[derive(Default)]
        pub(crate) struct $State {
            $(
                $(#[$field_meta])*
                pub $field: $ty,
            )*
        }

        impl Default for $Name {
            fn default() -> Self {
                Self {
                    inner: ::std::sync::Arc::new(::std::sync::Mutex::new($State::default())),
                }
            }
        }

        impl $Name {
            /// Wrap an already-constructed state in the shared-mutex
            /// newtype. Used by the per-builder `new(args)` shims to
            /// install script-supplied required parameters (url,
            /// connections, duration).
            ///
            /// Kept `pub(crate)` because `$State` is `pub(crate)` — the
            /// builder types are `pub` but their state is an
            /// implementation detail.
            #[inline]
            #[allow(dead_code)]
            pub(crate) fn with_state(state: $State) -> Self {
                Self {
                    inner: ::std::sync::Arc::new(::std::sync::Mutex::new(state)),
                }
            }

            /// Acquire the mutex and apply `f` to the state. Used by
            /// every fluent method; centralises the `.lock().unwrap()`
            /// boilerplate.
            ///
            /// Panics only if a previous call panicked while holding
            /// the lock — none of our call sites panic past the `?`
            /// return paths, and a panic would abort the script anyway.
            #[inline]
            pub(crate) fn modify<R>(&self, f: impl FnOnce(&mut $State) -> R) -> R {
                let mut guard = self
                    .inner
                    .lock()
                    .expect(concat!(stringify!($Name), " mutex poisoned"));
                f(&mut guard)
            }

            /// Swap the state out of the mutex with `std::mem::take` and
            /// hand it back. See [`PlanBuilder::finalize`] for why we
            /// can't just `Arc::try_unwrap` (Rhai's engine keeps
            /// `Arc` clones alive via registered closures until the
            /// engine itself drops).
            #[inline]
            #[allow(dead_code)]
            pub(crate) fn take_state(&self) -> $State {
                ::std::mem::take(
                    &mut *self
                        .inner
                        .lock()
                        .expect(concat!(stringify!($Name), " mutex poisoned")),
                )
            }
        }
    };
}

/// Register the common `.header(name, value)` method on a builder type
/// whose state has a `pub headers: Vec<(String, String)>` field. Every
/// protocol builder reuses the exact same body, so the boilerplate goes
/// into this helper macro.
macro_rules! register_header_fn {
    ($engine:expr, $Builder:ty) => {
        $engine.register_fn(
            "header",
            move |b: $Builder, name: ImmutableString, value: ImmutableString| {
                b.modify(|s| s.headers.push((name.to_string(), value.to_string())));
                b
            },
        );
    };
}

/// Register a string-parsed setter — `parser` is a fn that turns
/// `ImmutableString` into `Result<T, String>`, and the resulting `T` is
/// assigned to `state.$field`. Used by `correlate`, `mode`, and
/// `heartbeat_frame`, which all share this shape.
macro_rules! register_parsed_setter_fn {
    ($engine:expr, $method:literal, $Builder:ty, $field:ident, $parser:expr) => {
        $engine.register_fn(
            $method,
            move |ctx: NativeCallContext, b: $Builder, kind: ImmutableString|
                -> Result<$Builder, Box<EvalAltResult>> {
                let parsed = $parser(&kind).map_err(|e| {
                    Box::new(EvalAltResult::ErrorRuntime(e.into(), ctx.call_position()))
                })?;
                b.modify(|s| s.$field = parsed);
                Ok(b)
            },
        );
    };
}

/// Register a plain `(b, x) -> b` setter with a pre-transform. Used for
/// the fluent setters that assign a single value to `state.$field`
/// (e.g. `.reconnect(bool)`, `.expected_rate(f64)`,
/// `.trigger_url(str)`).
macro_rules! register_setter_fn {
    // Setter with an identity transform: `state.field = value`.
    ($engine:expr, $method:literal, $Builder:ty, $field:ident, $ArgTy:ty) => {
        $engine.register_fn($method, move |b: $Builder, value: $ArgTy| {
            b.modify(|s| s.$field = value);
            b
        });
    };
    // Setter with a transform closure: `state.field = transform(value)`.
    (
        $engine:expr, $method:literal, $Builder:ty, $field:ident,
        $ArgTy:ty, $transform:expr
    ) => {
        $engine.register_fn($method, move |b: $Builder, value: $ArgTy| {
            let v = $transform(value);
            b.modify(|s| s.$field = v);
            b
        });
    };
}

// ---------------------------------------------------------------------------
// PlanBuilder — the root aggregator
// ---------------------------------------------------------------------------

define_builder! {
    /// Shared-state wrapper around [`PlanBuilderState`]. The public handle
    /// Rhai scripts interact with via the `scenario`, `rate`, `duration`,
    /// `warmup`, `transport`, `saturate`, `env`, and `var` top-level
    /// functions.
    pub struct PlanBuilder[PlanBuilderState] {
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
}

// PlanBuilder's public Rust API (in addition to the macro-generated
// surface): `new()` for symmetry with the existing callers in `lib.rs`,
// plus two bespoke operations — `first_request_url` (URL peek before
// finalize) and `finalize` (state → `(Plan, HttpVersionPref)`).
impl PlanBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self::default()
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
        self.modify(|s| {
            for scn in &s.scenarios {
                let url = scn.modify(|st| {
                    st.steps.iter().find_map(|step| match step {
                        StepSource::Request(rb) => Some(rb.modify(|r| r.url.clone())),
                        StepSource::SseHold(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::SseFanout(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::SseReconnectStorm(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::WsEchoRtt(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::WsHold(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::WsServerPush(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::WsFanout(b) => Some(b.modify(|s| s.url.clone())),
                        StepSource::Pause(_) | StepSource::PauseRandom { .. } => None,
                    })
                });
                if let Some(u) = url {
                    return Some(u);
                }
            }
            None
        })
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
        let inner = self.take_state();
        finalize_state(inner)
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
        .any(|s| s.modify(|st| st.rate.is_some()));
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
            s.modify(|st| {
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

define_builder! {
    /// Handed to the scenario body closure as its `s` argument. Scripts call
    /// `s.step(...)` to enqueue steps, and (optionally) `s.rate("X")` to set
    /// a per-scenario rate.
    pub(crate) struct ScenarioBuilder[ScenarioBuilderState] {
        pub name: String,
        pub weight: Option<f64>,
        pub rate: Option<RateProfile>,
        pub steps: Vec<StepSource>,
    }
}

impl ScenarioBuilder {
    fn new(name: String, weight: Option<f64>) -> Self {
        Self::with_state(ScenarioBuilderState {
            name,
            weight,
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// RequestBuilder
// ---------------------------------------------------------------------------

define_builder! {
    /// Returned by `GET(url)`, `POST(url)`, etc. and passed through the
    /// chained `.header`, `.json`, `.body`, `.expect_status`, etc. methods.
    pub(crate) struct RequestBuilder[RequestBuilderState] {
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
}

impl RequestBuilder {
    fn new(method: Method, url: String) -> Self {
        Self::with_state(RequestBuilderState {
            method,
            url,
            ..Default::default()
        })
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

// ---------------------------------------------------------------------------
// Protocol-native builders (SSE + WebSocket)
//
// Construction is one-shot — users pass the essential parameters at call
// time (e.g. `sse_hold(url, n, for)`) instead of chained setters. The
// macro supplies the Arc<Mutex> plumbing; each `new(...)` just pipes the
// required fields into `with_state`.
// ---------------------------------------------------------------------------

define_builder! {
    /// Returned by `sse_hold(url, subscribers, hold_for)`. Finalised into
    /// [`Step::SseHold`] during plan finalization.
    pub(crate) struct SseHoldBuilder[SseHoldBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub subscribers: u32,
        pub hold_for: Duration,
        pub reconnect: bool,
    }
}

impl SseHoldBuilder {
    fn new(url: String, subscribers: u32, hold_for: Duration) -> Self {
        Self::with_state(SseHoldBuilderState {
            url,
            subscribers: subscribers.max(1),
            hold_for,
            // The default-false bool flips to true here because the
            // SSE `reconnect: on` behaviour is the zero-intrusion
            // norm; a script opts OUT via `.reconnect(false)`.
            reconnect: true,
            ..Default::default()
        })
    }
}

define_builder! {
    /// Returned by `ws_echo_rtt(url, connections, msg_rate_per_conn)`.
    /// Finalised into [`Step::WsEchoRtt`] during plan finalization.
    pub(crate) struct WsEchoRttBuilder[WsEchoRttBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub connections: u32,
        pub msg_rate_per_conn: f64,
        pub payload: String,
        pub correlate: zerobench_core::plan::CorrelateStrategy,
    }
}

impl WsEchoRttBuilder {
    fn new(url: String, connections: u32, msg_rate_per_conn: f64) -> Self {
        Self::with_state(WsEchoRttBuilderState {
            url,
            connections: connections.max(1),
            msg_rate_per_conn,
            payload: "ping".into(),
            ..Default::default()
        })
    }
}

define_builder! {
    /// Returned by `ws_hold(url, connections, hold_for)`. Finalised into
    /// [`Step::WsHold`] during plan finalization.
    pub(crate) struct WsHoldBuilder[WsHoldBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub connections: u32,
        pub heartbeat: Duration,
        pub heartbeat_frame: zerobench_core::plan::HeartbeatFrame,
        pub hold_for: Duration,
    }
}

impl WsHoldBuilder {
    fn new(url: String, connections: u32, hold_for: Duration) -> Self {
        Self::with_state(WsHoldBuilderState {
            url,
            connections: connections.max(1),
            heartbeat: Duration::from_secs(25),
            hold_for,
            ..Default::default()
        })
    }
}

define_builder! {
    /// Returned by `ws_server_push(url, connections, hold_for)`. Finalised
    /// into [`Step::WsServerPushRtt`] during plan finalization.
    pub(crate) struct WsServerPushBuilder[WsServerPushBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub connections: u32,
        pub expected_rate_per_conn: f64,
        pub hold_for: Duration,
    }
}

impl WsServerPushBuilder {
    fn new(url: String, connections: u32, hold_for: Duration) -> Self {
        Self::with_state(WsServerPushBuilderState {
            url,
            connections: connections.max(1),
            hold_for,
            ..Default::default()
        })
    }
}

define_builder! {
    /// Returned by `sse_fanout(url, subs, hold_for)`. Compiles to
    /// [`Step::SseFanout`] at finalize. Requires `.trigger_url(...)`.
    pub(crate) struct SseFanoutBuilder[SseFanoutBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub subscribers: u32,
        pub hold_for: Duration,
        pub reconnect: bool,
        pub trigger_url: String,
        pub mode: zerobench_core::plan::FanoutMode,
    }
}

impl SseFanoutBuilder {
    fn new(url: String, subs: u32, hold_for: Duration) -> Self {
        Self::with_state(SseFanoutBuilderState {
            url,
            subscribers: subs.max(1),
            hold_for,
            reconnect: true,
            ..Default::default()
        })
    }
}

define_builder! {
    /// Returned by `ws_fanout(url, conns, hold_for)`. Compiles to
    /// [`Step::WsFanout`] at finalize.
    pub(crate) struct WsFanoutBuilder[WsFanoutBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub connections: u32,
        pub hold_for: Duration,
        pub heartbeat: Duration,
        pub heartbeat_frame: zerobench_core::plan::HeartbeatFrame,
        pub trigger_url: String,
        pub mode: zerobench_core::plan::FanoutMode,
    }
}

impl WsFanoutBuilder {
    fn new(url: String, connections: u32, hold_for: Duration) -> Self {
        Self::with_state(WsFanoutBuilderState {
            url,
            connections: connections.max(1),
            hold_for,
            heartbeat: Duration::from_secs(25),
            ..Default::default()
        })
    }
}

define_builder! {
    /// Returned by `sse_reconnect_storm(url, subs, hold_for)`.
    pub(crate) struct SseReconnectStormBuilder[SseReconnectStormBuilderState] {
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub subscribers: u32,
        pub hold_for: Duration,
        pub kill_rate_per_s: f64,
        pub verify_last_event_id: bool,
    }
}

impl SseReconnectStormBuilder {
    fn new(url: String, subs: u32, hold_for: Duration) -> Self {
        Self::with_state(SseReconnectStormBuilderState {
            url,
            subscribers: subs.max(1),
            hold_for,
            kill_rate_per_s: 0.1,
            verify_last_event_id: true,
            ..Default::default()
        })
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
// Compilation: StepSource -> Step
//
// The per-variant compilation used to hand-roll the same template-compile
// + header-compile boilerplate nine times. `compile_tpl` /
// `compile_headers` centralise the error-wrapping so each arm of the
// match is a one-shot destructure + compile + construct.
// ---------------------------------------------------------------------------

fn compile_tpl(
    src: &str,
    vars: &mut VarRegistry,
    scenario: &str,
    field: impl Into<String>,
) -> Result<Template, ScriptError> {
    Template::compile(src, vars).map_err(|e| ScriptError::Template {
        scenario: scenario.to_string(),
        field: field.into(),
        error: e,
    })
}

fn compile_headers<const N: usize>(
    raw: Vec<(String, String)>,
    vars: &mut VarRegistry,
    scenario: &str,
    kind: &str,
) -> Result<SmallVec<[(Template, Template); N]>, ScriptError> {
    let mut out: SmallVec<[(Template, Template); N]> = SmallVec::new();
    for (name, value) in raw {
        let n = compile_tpl(&name, vars, scenario, format!("{kind} header name {name:?}"))?;
        let v = compile_tpl(
            &value,
            vars,
            scenario,
            format!("{kind} header {name:?} value {value:?}"),
        )?;
        out.push((n, v));
    }
    Ok(out)
}

fn compile_step(
    src: StepSource,
    vars: &mut VarRegistry,
    scenario: &str,
) -> Result<Step, ScriptError> {
    match src {
        StepSource::Pause(d) => Ok(Step::Pause(d)),
        StepSource::PauseRandom { min, max } => Ok(Step::PauseRandom { min, max }),
        StepSource::SseHold(sb) => {
            let SseHoldBuilderState {
                url,
                headers,
                subscribers,
                hold_for,
                reconnect,
            } = sb.take_state();
            Ok(Step::SseHold(zerobench_core::plan::SseHoldPlan {
                url: compile_tpl(&url, vars, scenario, format!("sse_hold url {url:?}"))?,
                headers: compile_headers(headers, vars, scenario, "sse_hold")?,
                subscribers,
                hold_for,
                reconnect,
            }))
        }
        StepSource::WsEchoRtt(wb) => {
            let WsEchoRttBuilderState {
                url,
                headers,
                connections,
                msg_rate_per_conn,
                payload,
                correlate,
            } = wb.take_state();
            Ok(Step::WsEchoRtt(zerobench_core::plan::WsEchoRttPlan {
                url: compile_tpl(&url, vars, scenario, format!("ws_echo_rtt url {url:?}"))?,
                headers: compile_headers(headers, vars, scenario, "ws_echo_rtt")?,
                connections,
                msg_rate_per_conn,
                correlate,
                payload: compile_tpl(&payload, vars, scenario, "ws_echo_rtt payload")?,
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
            Ok(Step::WsHold(zerobench_core::plan::WsHoldPlan {
                url: compile_tpl(&url, vars, scenario, format!("ws_hold url {url:?}"))?,
                headers: compile_headers(headers, vars, scenario, "ws_hold")?,
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
                    scenario: scenario.to_string(),
                    field: "sse_fanout trigger_url".into(),
                    error: TemplateError::NotYetSupported(
                        "sse_fanout requires .trigger_url(...)".into(),
                    ),
                });
            }
            Ok(Step::SseFanout(zerobench_core::plan::SseFanoutPlan {
                subscribers: zerobench_core::plan::SseHoldPlan {
                    url: compile_tpl(&url, vars, scenario, format!("sse_fanout url {url:?}"))?,
                    headers: compile_headers(headers, vars, scenario, "sse_fanout")?,
                    subscribers,
                    hold_for,
                    reconnect,
                },
                trigger: zerobench_core::plan::TriggerSpec::HttpPost {
                    url: compile_tpl(
                        &trigger_url,
                        vars,
                        scenario,
                        format!("sse_fanout trigger_url {trigger_url:?}"),
                    )?,
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
            Ok(Step::SseReconnectStorm(
                zerobench_core::plan::SseReconnectStormPlan {
                    subscribers: zerobench_core::plan::SseHoldPlan {
                        url: compile_tpl(
                            &url,
                            vars,
                            scenario,
                            format!("sse_reconnect_storm url {url:?}"),
                        )?,
                        headers: compile_headers(headers, vars, scenario, "sse_reconnect_storm")?,
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
                    scenario: scenario.to_string(),
                    field: "ws_fanout trigger_url".into(),
                    error: TemplateError::NotYetSupported(
                        "ws_fanout requires .trigger_url(...)".into(),
                    ),
                });
            }
            Ok(Step::WsFanout(zerobench_core::plan::WsFanoutPlan {
                subscribers: zerobench_core::plan::WsHoldPlan {
                    url: compile_tpl(&url, vars, scenario, format!("ws_fanout url {url:?}"))?,
                    headers: compile_headers(headers, vars, scenario, "ws_fanout")?,
                    connections,
                    heartbeat,
                    heartbeat_frame,
                    hold_for,
                },
                trigger: zerobench_core::plan::TriggerSpec::HttpPost {
                    url: compile_tpl(
                        &trigger_url,
                        vars,
                        scenario,
                        format!("ws_fanout trigger_url {trigger_url:?}"),
                    )?,
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
            Ok(Step::WsServerPushRtt(
                zerobench_core::plan::WsServerPushRttPlan {
                    url: compile_tpl(&url, vars, scenario, format!("ws_server_push url {url:?}"))?,
                    headers: compile_headers(headers, vars, scenario, "ws_server_push")?,
                    connections,
                    expected_rate_per_conn,
                    hold_for,
                },
            ))
        }
        StepSource::Request(rb) => {
            let RequestBuilderState {
                method,
                url,
                headers,
                body,
                extract,
                checks,
                cold,
            } = rb.take_state();

            let url_tpl = compile_tpl(&url, vars, scenario, format!("url {url:?}"))?;
            let compiled_headers: SmallVec<[(Template, Template); 8]> =
                compile_headers::<8>(headers, vars, scenario, "request")?;

            let body = match body {
                None => None,
                Some(BodySourceSpec::Raw(bytes)) => Some(BodySource::Static(bytes)),
                Some(BodySourceSpec::Template(src))
                | Some(BodySourceSpec::JsonTemplate(src)) => {
                    let tpl = compile_tpl(&src, vars, scenario, "body")?;
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
                Step::HttpColdConnect(zerobench_core::plan::ColdConnectPlan { request })
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
    // scenario("name", [weight,] body) — three overloads:
    // - no weight (auto — split evenly)
    // - f64 weight
    // - i64 weight (Rhai int-literal fallback; `scenario("a", 1, ...)`
    //   otherwise fails fn resolution against the f64 variant).
    //
    // All three return `()` rather than `ScenarioBuilder`. Scripts call
    // `scenario(...);` as a statement; `engine.run()` also expects the
    // overall script to yield `()`, so top-level helpers that return
    // `()` keep that invariant automatic.
    macro_rules! scenario_fn {
        ($weight_ty:ty, $weight_expr:expr) => {{
            let r = root.clone();
            engine.register_fn(
                "scenario",
                move |ctx: NativeCallContext,
                      name: ImmutableString,
                      weight: $weight_ty,
                      body: FnPtr| {
                    let scn = ScenarioBuilder::new(name.to_string(), Some($weight_expr(weight)));
                    r.modify(|s| s.scenarios.push(scn.clone()));
                    body.call_within_context::<()>(&ctx, (Dynamic::from(scn),))?;
                    Ok::<(), Box<EvalAltResult>>(())
                },
            );
        }};
    }
    // No-weight overload — cannot use the macro above because it has a
    // different arity.
    let r = root.clone();
    engine.register_fn(
        "scenario",
        move |ctx: NativeCallContext, name: ImmutableString, body: FnPtr| {
            let scn = ScenarioBuilder::new(name.to_string(), None);
            r.modify(|s| s.scenarios.push(scn.clone()));
            body.call_within_context::<()>(&ctx, (Dynamic::from(scn),))?;
            Ok::<(), Box<EvalAltResult>>(())
        },
    );
    scenario_fn!(f64, |w: f64| w);
    scenario_fn!(i64, |w: i64| w as f64);

    // rate("10k/s")  — global open-loop rate
    let r = root.clone();
    engine.register_fn("rate", move |spec: ImmutableString| {
        let rate = parse::parse_rate_with_unit(&spec).map_err(to_rhai_err)?;
        r.modify(|s| s.global_rate = Some(RateProfile::Constant(rate)));
        Ok::<(), Box<EvalAltResult>>(())
    });

    // saturate(n)  — closed-loop concurrency
    let r = root.clone();
    engine.register_fn("saturate", move |n: i64| {
        r.modify(|s| s.saturate_concurrency = Some(n.max(1) as usize));
    });

    // duration("30s") / warmup("2s") / cooldown("10s") — every plan-level
    // duration knob shares the same parse-then-assign shape. `$field` is
    // the `Option<Duration>` field on `PlanBuilderState` to set.
    macro_rules! plan_duration_fn {
        ($method:literal, $field:ident) => {{
            let r = root.clone();
            engine.register_fn($method, move |spec: ImmutableString| {
                let d = parse::parse_duration(&spec)
                    .ok_or_else(|| to_rhai_err(format!(concat!("invalid ", $method, " {:?}"), spec)))?;
                r.modify(|s| s.$field = Some(d));
                Ok::<(), Box<EvalAltResult>>(())
            });
        }};
    }
    plan_duration_fn!("duration", duration);
    plan_duration_fn!("warmup", warmup);
    plan_duration_fn!("cooldown", cooldown);

    // runs(3) — iterations per plan. Feeds the bootstrap CI aggregator.
    let r = root.clone();
    engine.register_fn("runs", move |n: i64| {
        r.modify(|s| s.runs = Some(n.max(1) as u32));
    });

    // threads(8) — client-side worker thread count.
    let r = root.clone();
    engine.register_fn("threads", move |n: i64| {
        r.modify(|s| s.threads = Some(n.max(1) as usize));
    });

    // plan_name("chat-burst") — overrides the default Target-host name
    // in fingerprints. Useful when a single service is measured under
    // multiple logical profiles.
    let r = root.clone();
    engine.register_fn("plan_name", move |name: ImmutableString| {
        r.modify(|s| s.name = Some(name.to_string()));
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
        r.modify(|s| s.transport = ver);
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
        move |name: ImmutableString, default: ImmutableString| match std::env::var(name.as_str()) {
            Ok(v) => ImmutableString::from(v),
            Err(_) => default,
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
        let handle = r.modify(|s| match s.vars.allocate(&name_str) {
            Ok(slot) => Ok(VarSlotHandle {
                slot,
                name: name_str,
            }),
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
    register_header_fn!(engine, RequestBuilder);

    // .body(string)  — template if it contains `{{`, else raw bytes.
    engine.register_fn("body", move |b: RequestBuilder, body: ImmutableString| {
        b.modify(|s| {
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
            b.modify(|s| s.body = Some(BodySourceSpec::Raw(Bytes::from(bytes))));
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
            b.modify(|s| {
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
                    s.headers
                        .push(("Content-Type".into(), "application/json".into()));
                }
            });
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .expect_status(n)
    engine.register_fn("expect_status", move |b: RequestBuilder, code: i64| {
        b.modify(|s| s.checks.push(Assertion::StatusEq(clamp_u16(code))));
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
            b.modify(|s| s.checks.push(Assertion::StatusIn(codes)));
            Ok::<RequestBuilder, Box<EvalAltResult>>(b)
        },
    );

    // .expect_latency_under("500ms")
    engine.register_fn(
        "expect_latency_under",
        move |b: RequestBuilder, spec: ImmutableString| {
            let d = parse::parse_duration(&spec)
                .ok_or_else(|| to_rhai_err(format!("invalid latency duration {spec:?}")))?;
            b.modify(|s| s.checks.push(Assertion::LatencyUnder(d)));
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
                    Dynamic::from(format!(
                        "invalid header name {:?}: {e}",
                        header.as_str()
                    )),
                    ctx.call_position(),
                ))
            })?;
            b.modify(|s| {
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
            b.modify(|s| s.extract.push(Extract::StatusCode { into: slot.slot }));
            b
        },
    );

    // .cold_connect()  — fresh TCP+TLS+HTTP connection per request,
    // no pool reuse. Compiles to Step::HttpColdConnect at finalize.
    engine.register_fn("cold_connect", move |b: RequestBuilder| {
        b.modify(|s| s.cold = true);
        b
    });
}

fn register_sse_hold_builders(engine: &mut Engine) {
    // sse_hold(url, subscribers, hold_for).
    engine.register_fn(
        "sse_hold",
        move |url: ImmutableString, subs: i64, hold_for: ImmutableString| {
            let secs = hold_for_from_str(&hold_for);
            let subs_u = clamp_u32_min1(subs);
            SseHoldBuilder::new(url.to_string(), subs_u, secs)
        },
    );
    // Overload that takes a raw seconds int for `hold_for`.
    engine.register_fn(
        "sse_hold",
        move |url: ImmutableString, subs: i64, hold_for_secs: i64| {
            SseHoldBuilder::new(
                url.to_string(),
                clamp_u32_min1(subs),
                hold_for_from_secs(hold_for_secs),
            )
        },
    );

    register_header_fn!(engine, SseHoldBuilder);
    register_setter_fn!(engine, "reconnect", SseHoldBuilder, reconnect, bool);
}

fn register_ws_echo_rtt_builders(engine: &mut Engine) {
    engine.register_fn(
        "ws_echo_rtt",
        move |url: ImmutableString, conns: i64, rate: f64| {
            let c = clamp_u32_min1(conns);
            WsEchoRttBuilder::new(url.to_string(), c, rate)
        },
    );
    engine.register_fn(
        "ws_echo_rtt",
        move |url: ImmutableString, conns: i64, rate: i64| {
            let c = clamp_u32_min1(conns);
            WsEchoRttBuilder::new(url.to_string(), c, rate as f64)
        },
    );

    register_header_fn!(engine, WsEchoRttBuilder);
    register_setter_fn!(
        engine, "payload", WsEchoRttBuilder, payload, ImmutableString,
        |t: ImmutableString| t.to_string()
    );
    // .correlate(strategy) — how to match server echoes to client sends.
    // Accepts "pingpong", "prepend" (default), "first_text", or
    // "substring:<marker>". Unknown values fail at plan build time.
    register_parsed_setter_fn!(
        engine, "correlate", WsEchoRttBuilder, correlate, parse_correlate
    );
}

fn register_ws_hold_builders(engine: &mut Engine) {
    engine.register_fn(
        "ws_hold",
        move |url: ImmutableString, conns: i64, hold_for: ImmutableString| {
            let secs = hold_for_from_str(&hold_for);
            let c = clamp_u32_min1(conns);
            WsHoldBuilder::new(url.to_string(), c, secs)
        },
    );
    engine.register_fn(
        "ws_hold",
        move |url: ImmutableString, conns: i64, hold_for_secs: i64| {
            WsHoldBuilder::new(
                url.to_string(),
                clamp_u32_min1(conns),
                hold_for_from_secs(hold_for_secs),
            )
        },
    );
    register_setter_fn!(
        engine, "heartbeat", WsHoldBuilder, heartbeat, ImmutableString,
        |i: ImmutableString| parse_duration_str(&i).unwrap_or(Duration::from_secs(25))
    );
    register_header_fn!(engine, WsHoldBuilder);
    // .heartbeat_frame(kind) — "ping" (RFC 6455 Ping, default) or
    // "text" (app-level text frame, for servers that don't reply to
    // Ping).
    register_parsed_setter_fn!(
        engine, "heartbeat_frame", WsHoldBuilder, heartbeat_frame, parse_heartbeat_frame
    );
}

fn register_sse_fanout_builders(engine: &mut Engine) {
    engine.register_fn(
        "sse_fanout",
        move |url: ImmutableString, subs: i64, hold_for: ImmutableString| {
            let secs = hold_for_from_str(&hold_for);
            let s = clamp_u32_min1(subs);
            SseFanoutBuilder::new(url.to_string(), s, secs)
        },
    );
    register_setter_fn!(
        engine, "trigger_url", SseFanoutBuilder, trigger_url, ImmutableString,
        |u: ImmutableString| u.to_string()
    );
    register_setter_fn!(engine, "reconnect", SseFanoutBuilder, reconnect, bool);
    register_header_fn!(engine, SseFanoutBuilder);
    // .mode(kind) — "trigger_rtt" (default, proxy from trigger 2xx)
    // or "timestamp[:<field>]" (read emit ns from the broadcast
    // payload; requires server cooperation).
    register_parsed_setter_fn!(
        engine, "mode", SseFanoutBuilder, mode, parse_fanout_mode
    );
}

fn register_ws_fanout_builders(engine: &mut Engine) {
    engine.register_fn(
        "ws_fanout",
        move |url: ImmutableString, conns: i64, hold_for: ImmutableString| {
            let secs = hold_for_from_str(&hold_for);
            let c = clamp_u32_min1(conns);
            WsFanoutBuilder::new(url.to_string(), c, secs)
        },
    );
    register_setter_fn!(
        engine, "trigger_url", WsFanoutBuilder, trigger_url, ImmutableString,
        |u: ImmutableString| u.to_string()
    );
    register_setter_fn!(
        engine, "heartbeat", WsFanoutBuilder, heartbeat, ImmutableString,
        |i: ImmutableString| parse_duration_str(&i).unwrap_or(Duration::from_secs(25))
    );
    register_header_fn!(engine, WsFanoutBuilder);
    register_parsed_setter_fn!(
        engine, "heartbeat_frame", WsFanoutBuilder, heartbeat_frame, parse_heartbeat_frame
    );
    register_parsed_setter_fn!(
        engine, "mode", WsFanoutBuilder, mode, parse_fanout_mode
    );
}

fn register_sse_reconnect_storm_builders(engine: &mut Engine) {
    engine.register_fn(
        "sse_reconnect_storm",
        move |url: ImmutableString, subs: i64, hold_for: ImmutableString| {
            let secs = hold_for_from_str(&hold_for);
            let s = clamp_u32_min1(subs);
            SseReconnectStormBuilder::new(url.to_string(), s, secs)
        },
    );
    register_setter_fn!(
        engine, "kill_rate", SseReconnectStormBuilder, kill_rate_per_s, f64
    );
    register_setter_fn!(
        engine, "verify_last_event_id", SseReconnectStormBuilder,
        verify_last_event_id, bool
    );
    register_header_fn!(engine, SseReconnectStormBuilder);
}

fn register_ws_server_push_builders(engine: &mut Engine) {
    engine.register_fn(
        "ws_server_push",
        move |url: ImmutableString, conns: i64, hold_for: ImmutableString| {
            let secs = hold_for_from_str(&hold_for);
            let c = clamp_u32_min1(conns);
            WsServerPushBuilder::new(url.to_string(), c, secs)
        },
    );
    engine.register_fn(
        "ws_server_push",
        move |url: ImmutableString, conns: i64, hold_for_secs: i64| {
            WsServerPushBuilder::new(
                url.to_string(),
                clamp_u32_min1(conns),
                hold_for_from_secs(hold_for_secs),
            )
        },
    );
    register_setter_fn!(
        engine, "expected_rate", WsServerPushBuilder, expected_rate_per_conn, f64
    );
    register_header_fn!(engine, WsServerPushBuilder);
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
    engine.register_fn("step", move |s: ScenarioBuilder, req: RequestBuilder| {
        s.modify(|st| st.steps.push(StepSource::Request(req)));
    });

    // Protocol-native v0.1.0 step variants. Every `s.step(b)` closure
    // has the same shape — enqueue the builder wrapped in the matching
    // StepSource variant — so collapse the eight registrations into a
    // macro loop.
    macro_rules! step_fn {
        ($Builder:ty, $Variant:ident) => {
            engine.register_fn("step", move |s: ScenarioBuilder, b: $Builder| {
                s.modify(|st| st.steps.push(StepSource::$Variant(b)));
            });
        };
    }
    step_fn!(SseHoldBuilder, SseHold);
    step_fn!(WsEchoRttBuilder, WsEchoRtt);
    step_fn!(WsHoldBuilder, WsHold);
    step_fn!(WsServerPushBuilder, WsServerPush);
    step_fn!(SseFanoutBuilder, SseFanout);
    step_fn!(SseReconnectStormBuilder, SseReconnectStorm);
    step_fn!(WsFanoutBuilder, WsFanout);

    // s.step(pause(...))  — Pause / PauseRandom step (StepSource directly)
    engine.register_fn("step", move |s: ScenarioBuilder, step: StepSource| {
        s.modify(|st| st.steps.push(step));
    });

    // s.rate("3k/s")  — per-scenario open-loop rate
    engine.register_fn(
        "rate",
        move |s: ScenarioBuilder, spec: ImmutableString| {
            let rate = parse::parse_rate_with_unit(&spec).map_err(to_rhai_err)?;
            s.modify(|st| st.rate = Some(RateProfile::Constant(rate)));
            Ok::<(), Box<EvalAltResult>>(())
        },
    );

    // s.saturate(n)  — per-scenario closed-loop concurrency
    engine.register_fn("saturate", move |s: ScenarioBuilder, n: i64| {
        let max_concurrency = n.max(1) as usize;
        s.modify(|st| st.rate = Some(RateProfile::Saturate { max_concurrency }));
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
            Ok::<StepSource, Box<EvalAltResult>>(StepSource::PauseRandom { min, max })
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

/// Clamp a user-supplied i64 count to at least 1 u32. Applied to the
/// `connections` / `subscribers` arguments at every protocol builder
/// entry point.
#[inline]
fn clamp_u32_min1(n: i64) -> u32 {
    if n < 1 { 1 } else { (n as u32).max(1) }
}

/// Parse a `hold_for` spec. The DSL accepts either a duration string
/// ("30s") or a bare integer seconds count; this folds both shapes into
/// a single `Duration`, defaulting to 60 s on parse failure or non-
/// positive integers.
#[inline]
fn hold_for_from_str(spec: &str) -> Duration {
    parse_duration_str(spec).unwrap_or(Duration::from_secs(60))
}

#[inline]
fn hold_for_from_secs(secs: i64) -> Duration {
    Duration::from_secs(if secs <= 0 { 60 } else { secs as u64 })
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
        return Ok(Value::String(
            v.clone().into_string().map_err(|t| format!("expected string, got {t}"))?,
        ));
    }
    if v.is_array() {
        let arr = v.clone().into_array().map_err(|t| format!("expected array, got {t}"))?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(dynamic_to_serde(&item)?);
        }
        return Ok(Value::Array(out));
    }
    if v.is_map() {
        let m = v
            .clone()
            .try_cast::<rhai::Map>()
            .ok_or_else(|| format!("expected map, got {}", v.type_name()))?;
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

