//! zerobench-core — plan model, transport trait, dispatcher, recorder.
//!
//! See `docs/design.md` in the repo root for the architectural overview.

pub mod plan;
pub mod rng;
pub mod stats;
pub mod template;
pub mod var;

pub use plan::{
    Assertion, BodySource, Extract, Plan, RateProfile, RequestPlan, Scenario, Step,
};
pub use rng::BenchRng;
pub use stats::{ErrorCounters, ErrorKind, ScenarioStats, Summary, TaskStats};
pub use template::{ExpandCtx, Part, Template, TemplateError};
pub use var::{VarError, VarRegistry, VarSlot};
