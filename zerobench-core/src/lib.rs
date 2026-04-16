//! zerobench-core — plan model, transport trait, dispatcher, recorder.
//!
//! See `docs/design.md` in the repo root for the architectural overview.

pub mod dispatcher;
pub mod plan;
pub mod report;
pub mod rng;
pub mod scenario_context;
pub mod stats;
pub mod stop;
pub mod template;
pub mod transport;
pub mod var;

pub use dispatcher::run_saturate;
pub use plan::{
    Assertion, BodySource, Extract, Plan, RateProfile, RequestPlan, Scenario, Step,
};
pub use report::{print_json, print_terminal, ColorChoice};
pub use rng::BenchRng;
pub use scenario_context::ScenarioContext;
pub use stats::{ErrorCounters, ErrorKind, ScenarioStats, Summary, TaskStats};
pub use stop::StopSignal;
pub use template::{ExpandCtx, Template, TemplateError};
pub use transport::{
    Response, ResponseBody, Target, TargetError, Transport, TransportError, TransportOpts,
};
pub use var::{VarError, VarRegistry, VarSlot};
