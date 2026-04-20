//! zerobench-core — Plan + type vocabulary for the benchmark runner.
//!
//! This crate is intentionally the *narrow waist* of the architecture. It
//! contains ONLY the types every other crate needs to speak in common —
//! no I/O, no hot-path infrastructure, no presentation.
//!
//! Modules:
//!   - [`plan`]             — `Plan`, `Scenario`, `Step` (closed enum), all
//!                            per-protocol `*Plan` structs, `RateProfile`,
//!                            `Protocol`, `Mode`, assertions/extracts
//!   - [`plan_builder`]     — typed `PlanBuilder` + `scenario_*` free
//!                            functions. The one place CLI + DSL agree on
//!                            how to pack `*Plan` structs.
//!   - [`stats`]            — `TaskStats`, `ScenarioStats` with typed
//!                            `SseExtras`/`WsExtras`, `Summary`, error counters
//!   - [`template`]         — `Template` + `ExpandCtx` (`{{…}}` substitution)
//!   - [`var`]              — `VarRegistry` + `VarSlot` (response extracts)
//!   - [`scenario_context`] — per-iteration execution context
//!   - [`request_file`]     — `.http` request-file parser
//!   - [`transport`]        — `Target`, `TransportOpts`, `HttpVersionPref`,
//!                            `TargetError` (runtime `TransportError` lives in
//!                            `zerobench-runtime::transport`)
//!   - [`histogram`]        — HDR histogram constants + helpers
//!   - [`rng`]              — per-worker `BenchRng` (entropy-seeded Xoshiro)
//!
//! Runtime infrastructure (LiveSnapshot, archive, calibrate, fingerprint,
//! tls, stop, json_scan, machine) lives in `zerobench-runtime`.
//!
//! Report / statistical comparison (compare, report renderers) lives in
//! `zerobench-report`.
//!
//! See `docs/ARCH-REVIEW-2026-04-20.md` §4.1, §7.

pub mod histogram;
pub mod plan;
pub mod plan_builder;
pub mod request_file;
pub mod rng;
pub mod scenario_context;
pub mod stats;
pub mod template;
pub mod transport;
pub mod var;

pub use histogram::{duration_to_hist_ns, new_hist, HIST_HI_NS, HIST_LO_NS, HIST_SIG};
pub use plan::{
    Assertion, BodySource, Extract, Plan, Protocol, RateProfile, RequestPlan, Scenario, Step,
};
pub use plan_builder::PlanBuilder;
pub use request_file::{
    parse_request_bytes, parse_request_file, parse_scenario_dir, ParsedRequest, RequestFileError,
    ScenarioEntry,
};
pub use rng::BenchRng;
pub use scenario_context::ScenarioContext;
pub use stats::{
    ErrorCounters, ErrorCountersExport, ErrorKind, LatencyExport, PerRunMetrics, ScenarioExport,
    ScenarioStats, SseExtras, SseExtrasExport, Summary, SummaryExport, TaskStats, WsExtras,
    WsExtrasExport,
};
pub use template::{ExpandCtx, Template, TemplateError};
pub use transport::{HttpVersionPref, Target, TargetError, TransportOpts};
pub use var::{VarError, VarRegistry, VarSlot};
