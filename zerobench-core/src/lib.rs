//! zerobench-core — plan model, transport abstraction, recorder.
//!
//! See `docs/design.md` in the repo root for the architectural overview.

pub mod archive;
pub mod calibrate;
pub mod fingerprint;
pub mod histogram;
pub mod live_snapshot;
pub mod machine;
pub mod plan;
pub mod report;
pub mod request_file;
pub mod rng;
pub mod scenario_context;
pub mod stats;
pub mod stop;
pub mod template;
pub mod tls;
pub mod transport;
pub mod var;

pub use archive::{Archive, ArchiveWriter, EnvRecord, Index, SchemaVersions};
pub use calibrate::{ClientSelfCheck, LoopbackEcho, SelfCheckResult, Verdict};
pub use fingerprint::{
    canonical_sha256, plan_hash, run_id, target_fingerprint, url_fingerprint,
    url_fingerprint_anonymous, IpFamilyTag,
};
pub use histogram::{duration_to_hist_ns, new_hist, HIST_HI_NS, HIST_LO_NS, HIST_SIG};
pub use live_snapshot::{LiveSnapshot, LiveTick, ScenarioTick};
pub use machine::MachineFingerprint;
pub use plan::{
    Assertion, BodySource, Extract, Plan, Protocol, RateProfile, RequestPlan, Scenario, SsePlan,
    Step, WsRoundPlan,
};
pub use report::{
    print_json, print_jsonl_tick, print_prometheus, print_terminal, ColorChoice,
};
pub use request_file::{
    parse_request_bytes, parse_request_file, parse_scenario_dir, ParsedRequest,
    RequestFileError, ScenarioEntry,
};
pub use rng::BenchRng;
pub use scenario_context::ScenarioContext;
pub use stats::{
    ErrorCounters, ErrorCountersExport, ErrorKind, LatencyExport, ScenarioExport, ScenarioStats,
    SseExtras, SseExtrasExport, Summary, SummaryExport, TaskStats, WsExtras, WsExtrasExport,
};
pub use stop::StopSignal;
pub use template::{ExpandCtx, Template, TemplateError};
pub use tls::tls_client_config;
pub use transport::{
    HttpVersionPref, Target, TargetError, TransportError, TransportOpts,
};
pub use var::{VarError, VarRegistry, VarSlot};
