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
//! Verbs: `measure`, `probe`, `calibrate`, `curve`, `diff`.

pub mod calibrate;
pub mod curve;
pub mod diff;
pub mod measure;
pub mod probe;
