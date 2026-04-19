//! Per-verb dispatch modules for v0.1.0.
//!
//! Each verb module handles one of the seven user-facing verbs
//! described in `docs/PHILOSOPHY.md` §5 and
//! `docs/design-v0.1.0.md` §2. A verb module owns its own CLI args
//! struct, argument validation, pre-run work (calibration, machine
//! fingerprint, archive setup), dispatch to the backend, and
//! post-run archive finalisation.
//!
//! Shared machinery (connection pool, scheduler, HDR aggregators) lives
//! in `zerobench-core` and is reused across verbs. Verb-specific UX —
//! exit-code conventions, progress banners, default durations — lives
//! in the respective module.
//!
//! As of Phase 7a only `measure` ships; `probe`, `calibrate`, `curve`,
//! `compare`, `watch`, `diff` follow in subsequent commits. The
//! existing `Diff` subcommand in `zerobench-cli/src/diff.rs` remains
//! the v0.0.1 fallback until `verbs::diff` supersedes it.

pub mod measure;
