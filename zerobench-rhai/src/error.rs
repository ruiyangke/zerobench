//! ARCH STATUS: MOVE → zerobench-dsl::error (crate rename)
//!
//! ScriptError variants for the DSL loader. Moves with rename; no rewrite.
//! See ARCH-REVIEW §4, Q-A.
//!
//! ----------------------------------------------------------------------
//!
//! Errors returned by the Rhai loader.

use std::io;
use std::path::PathBuf;

use zerobench_core::template::TemplateError;

/// Errors produced while loading, compiling, or finalizing a Rhai script
/// into a [`zerobench_core::plan::Plan`].
///
/// Variants are organized by phase: IO → parse → eval → finalize. The
/// loader surfaces the first error encountered; scripts are typically
/// small enough that composite errors aren't worth the complexity.
#[derive(Debug, thiserror::Error)]
pub enum ScriptError {
    /// Could not read the script file from disk.
    #[error("failed to read script {path:?}: {source}")]
    Io {
        /// Path the loader tried to open.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: io::Error,
    },

    /// Rhai failed to parse the script text. The string contains the
    /// flattened error message with file/line info from Rhai.
    #[error("script parse error: {0}")]
    Parse(String),

    /// Rhai evaluated the script but hit a runtime error — e.g. a
    /// missing function, a type mismatch in a builder call, or a bubble
    /// from [`ScriptError::MissingEnv`] inside `env()`.
    #[error("script evaluation error: {0}")]
    Eval(String),

    /// A templated string (URL / header / body) failed to compile against
    /// the shared [`VarRegistry`]. Surfaces which scenario + which field
    /// the string came from so the user doesn't have to hunt.
    #[error("template error in scenario {scenario:?}, {field}: {error}")]
    Template {
        /// Name of the scenario whose RequestBuilder held the bad string.
        scenario: String,
        /// Human label for which field was bad (`"url ..."`, `"body"`, ...).
        field: String,
        /// Underlying compile error from the template engine.
        #[source]
        error: TemplateError,
    },

    /// `duration(...)` was never called. Required — the Plan carries no
    /// default duration because the CLI always overrides it, but scripts
    /// must be explicit.
    #[error("script must call duration(...) to set the benchmark duration")]
    MissingDuration,

    /// Zero `scenario(...)` calls were made. A plan with no scenarios
    /// wouldn't do anything.
    #[error("script must declare at least one scenario(...)")]
    NoScenarios,

    /// Both a global `rate(...)` and a per-scenario `.rate(...)` were
    /// used. Pick one — they mean incompatible things (global distributes
    /// by weight; per-scenario is absolute).
    #[error(
        "conflicting rate: global `rate()` and per-scenario `.rate()` are mutually exclusive"
    )]
    ConflictingRate,

    /// `env("NAME")` without a default was called for a variable that
    /// isn't in the environment. Surfaced as its own variant so callers
    /// (CI, tests) can match on it explicitly.
    #[error("env variable not set: {0} (use env(\"{0}\", \"default\") for a fallback)")]
    MissingEnv(String),

    /// The script produced zero Request steps — all scenarios were pure
    /// pause-loops. The CLI needs at least one URL to pick a connection
    /// target, so this is rejected up-front.
    #[error("script has no Request steps — at least one GET/POST/etc is required")]
    NoRequestSteps,

    /// The first request URL contains `{{...}}` templates in the host
    /// portion. The target host is resolved once at startup to open the
    /// connection pool, so it cannot be templated per iteration.
    #[error(
        "first request URL {0:?} has a templated host — the connection target must be static"
    )]
    TemplatedHost(String),

    /// Couldn't parse the first request URL as a URL at all.
    #[error("couldn't parse first request URL {url:?}: {reason}")]
    InvalidUrl {
        /// The raw URL string.
        url: String,
        /// Human reason from the underlying parser.
        reason: String,
    },
}
