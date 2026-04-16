//! zerobench-core — plan model, transport trait, dispatcher, recorder.
//!
//! See `docs/design.md` in the repo root for the architectural overview.

pub mod plan;
pub mod rng;
pub mod scenario_context;
pub mod stats;
pub mod template;
pub mod transport;
pub mod var;

pub use plan::{
    Assertion, BodySource, Extract, Plan, RateProfile, RequestPlan, Scenario, Step,
};
pub use rng::BenchRng;
pub use scenario_context::ScenarioContext;
pub use stats::{ErrorCounters, ErrorKind, ScenarioStats, Summary, TaskStats};
pub use template::{ExpandCtx, Template, TemplateError};
pub use transport::{
    Response, ResponseBody, Target, TargetError, Transport, TransportError, TransportOpts,
};
pub use var::{VarError, VarRegistry, VarSlot};
