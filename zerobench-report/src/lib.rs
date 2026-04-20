//! zerobench-report — presentation + statistical comparison.
//!
//! This crate owns everything "after the run":
//!   - [`compare`] — bootstrap CI / AD / KS / Holm-Bonferroni (CROWN JEWEL).
//!   - [`report`]  — terminal / JSON / Prometheus renderers and live JSONL ticks.
//!
//! Nothing in this crate is touched by the hot path. It's read-only
//! consumer-of-types: it depends on `zerobench-core` for the plan/stat
//! types and on `zerobench-runtime` for `LiveTick` (live-progress rows).
//!
//! See `docs/ARCH-REVIEW-2026-04-20.md` §4.1, §7.

pub mod compare;
pub mod report;

pub use compare::{
    ad_test, compare_all, compare_metric, holm_bonferroni, ks_test, AdResult, CompareOptions,
    ComparisonResult, KsResult, Metric, Significance, StrategyUsed,
};
pub use report::{print_json, print_jsonl_tick, print_prometheus, print_terminal, ColorChoice};
