//! zerobench-core — plan model, transport trait, dispatcher, recorder.
//!
//! See `docs/design.md` in the repo root for the architectural overview.

pub mod dispatcher;
pub mod live_snapshot;
pub mod plan;
pub mod rate;
pub mod report;
pub mod request_file;
pub mod rng;
pub mod scenario_context;
pub mod stats;
mod step_exec;
pub mod stop;
pub mod template;
pub mod tls;
pub mod transport;
pub mod var;

pub use dispatcher::run_saturate;
pub use live_snapshot::{LiveSnapshot, LiveTick};
pub use plan::{
    Assertion, BodySource, Extract, Plan, RateProfile, RequestPlan, Scenario, Step,
};
pub use rate::{run_open_loop, run_scheduler, KeepupCounter, Token};
pub use report::{
    print_json, print_jsonl_tick, print_prometheus, print_terminal, ColorChoice,
};
pub use request_file::{
    parse_request_bytes, parse_request_file, parse_scenario_dir, ParsedRequest,
    RequestFileError, ScenarioEntry,
};
pub use rng::BenchRng;
pub use scenario_context::ScenarioContext;
pub use stats::{ErrorCounters, ErrorKind, ScenarioStats, Summary, TaskStats};
pub use stop::StopSignal;
pub use template::{ExpandCtx, Template, TemplateError};
pub use tls::tls_client_config;
pub use transport::{
    HttpVersionPref, Response, ResponseBody, Target, TargetError, Transport, TransportError,
    TransportOpts,
};
pub use var::{VarError, VarRegistry, VarSlot};
