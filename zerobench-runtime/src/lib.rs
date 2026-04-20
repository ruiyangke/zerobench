//! zerobench-runtime — cross-cutting infrastructure for the benchmark runner.
//!
//! Nothing here is protocol-specific. Everything in this crate works the
//! same way whether the workload is HTTP / SSE / WS — it's the machinery
//! around benchmarks, not the benchmarks themselves.
//!
//! Modules:
//!   - [`archive`]       — run-archive sidecar I/O (plan.json / env.json / result.json / histlog)
//!   - [`calibrate`]     — client-side loopback self-check (§9.6.2)
//!   - [`fingerprint`]   — plan_hash / url_fingerprint / target_fingerprint / run_id
//!   - [`json_scan`]     — byte-level JSON field lookup (used by fanout Timestamp mode)
//!   - [`live_snapshot`] — sharded HDR histogram + counters, read per-tick by TUI
//!   - [`machine`]       — host fingerprint (CPU, RAM, OS) captured once per run
//!   - [`stop`]          — shared stop-flag primitive
//!   - [`tls`]           — shared rustls `ClientConfig` builder
//!   - [`transport`]     — runtime error taxonomy (`TransportError`)
//!
//! `BenchRng` lives in `zerobench_core::rng` because `template` and
//! `scenario_context` (in core) need it. Everything here is downstream of
//! core; nothing here is reachable from core.
//!
//! Part of the architecture-v2 rewrite. See `docs/ARCH-REVIEW-2026-04-20.md`
//! §4.1, §7, and `docs/ARCH-TAGS.md`.

pub mod archive;
pub mod calibrate;
pub mod fingerprint;
pub mod json_scan;
pub mod live_snapshot;
pub mod machine;
pub mod stop;
pub mod tls;
pub mod transport;

pub use archive::{
    load_histogram_from_histlog, Archive, ArchiveWriter, EnvRecord, Index, SchemaVersions,
};
pub use calibrate::{ClientSelfCheck, LoopbackEcho, SelfCheckResult, Verdict};
pub use fingerprint::{
    canonical_sha256, plan_hash, run_id, target_fingerprint, url_fingerprint,
    url_fingerprint_anonymous, IpFamilyTag,
};
pub use json_scan::find_json_u64_field;
pub use live_snapshot::{LiveSnapshot, LiveTick, ScenarioTick};
pub use machine::MachineFingerprint;
pub use stop::StopSignal;
pub use tls::tls_client_config;
pub use transport::TransportError;
