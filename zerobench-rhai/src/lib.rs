//! zerobench-rhai — Rhai scripting front-end.
//!
//! Rhai is used **only at compile time** to construct a [`Plan`]. The
//! loader stands up a fresh [`Engine`], registers the DSL builder types
//! and top-level functions, evaluates the script to populate a shared
//! [`PlanBuilder`], finalizes the builder into a real [`Plan`], and drops
//! the engine. Nothing Rhai-related survives into the hot path.
//!
//! # Entry point
//!
//! [`load_script`] is the single public function. It returns a
//! [`LoadedScript`] with a compiled [`Plan`], a connection [`Target`]
//! derived from the first request URL, and the transport preference.
//! The CLI combines these with transport options and dispatches through
//! the normal runner.
//!
//! # What's in the DSL (v0.0.1)
//!
//! See the module docs on [`builders`] for the full registered surface.
//! Highlights:
//!
//! - `scenario("name", |s| { ... })` with optional weight.
//! - `GET`/`POST`/`PUT`/`DELETE`/`PATCH`/`HEAD`/`OPTIONS`.
//! - Fluent chain: `.header`, `.body`, `.body_file`, `.json`,
//!   `.expect_status`, `.expect_status_in`, `.expect_latency_under`,
//!   `.extract_header`, `.extract_status`.
//! - `s.step(req)` / `s.step(pause("..."))` / `s.step(pause_random(...))`.
//! - `rate("10k/s")`, `saturate(N)`, `duration("30s")`, `warmup("2s")`,
//!   `transport("h1"|"h2")`.
//! - `env("NAME")`, `env("NAME", "default")`, `slot("slot_name")`
//!   (named `slot` not `var` because `var` is a reserved Rhai keyword).
//!
//! # Not in v0.0.1 (explicitly deferred)
//!
//! - `extract_json` — needs JsonPath; defer.
//! - `on_response` hooks — hot-path hook; explicitly out of scope.
//! - `body_multipart` — out of scope.
//!
//! [`Plan`]: zerobench_core::plan::Plan
//! [`HttpVersionPref`]: zerobench_core::transport::HttpVersionPref
//! [`Target`]: zerobench_core::transport::Target

pub mod builders;
pub mod error;
pub(crate) mod parse;

use std::path::Path;

use rhai::Engine;

pub use builders::PlanBuilder;
pub use error::ScriptError;
use zerobench_core::plan::{Plan, Step};
use zerobench_core::transport::{HttpVersionPref, Target};

/// The outcome of loading a Rhai script — a fully-built [`Plan`], the
/// connection [`Target`] to open, and the preferred HTTP version.
///
/// Returned as a struct rather than a tuple because it has three fields
/// and all of them carry distinct semantics; a tuple would invite misuse.
#[derive(Debug)]
pub struct LoadedScript {
    /// The fully-built plan. Ready to be executed by `run_open_loop` or
    /// `run_saturate`.
    pub plan: Plan,
    /// The connection target — derived from the host+port of the first
    /// request URL in the first scenario's first Request step.
    pub target: Target,
    /// Preferred HTTP version, set by `transport("h1"|"h2")`. Defaults to
    /// `HttpVersionPref::Auto`.
    pub http_version: HttpVersionPref,
}

/// Load a Rhai script from `path`, evaluate it once to populate a
/// [`PlanBuilder`], finalize into a [`Plan`], and return a [`LoadedScript`].
///
/// The Rhai engine is constructed fresh and dropped before this function
/// returns — no interpreter state survives into the benchmark hot path.
///
/// # Error surface
///
/// - [`ScriptError::Io`] — can't read the file.
/// - [`ScriptError::Parse`] — Rhai couldn't parse the script.
/// - [`ScriptError::Eval`] — the script ran but raised an error.
/// - [`ScriptError::MissingEnv`] — the script called `env("X")` without a
///   default and `X` wasn't set.
/// - [`ScriptError::Template`] — a `{{...}}` string failed to compile.
/// - [`ScriptError::MissingDuration`] / [`ScriptError::NoScenarios`] /
///   [`ScriptError::ConflictingRate`] — plan validation at finalize.
/// - [`ScriptError::NoRequestSteps`] / [`ScriptError::TemplatedHost`] /
///   [`ScriptError::InvalidUrl`] — target derivation.
pub fn load_script(path: &Path) -> Result<LoadedScript, ScriptError> {
    let src = std::fs::read_to_string(path).map_err(|e| ScriptError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    load_script_str(&src)
}

/// Variant of [`load_script`] that takes an already-loaded script string.
/// Separated so tests can drive the loader without a tempfile; CLI callers
/// should stick with [`load_script`] so file paths show up in error
/// messages.
pub fn load_script_str(src: &str) -> Result<LoadedScript, ScriptError> {
    // `Engine::new_raw` gives us a minimal engine: arithmetic + flow
    // control, no `eval`/`system`/filesystem built-ins. We register our
    // own handful of functions on top. This keeps the scripting surface
    // tight — scripts can't, e.g., open network sockets or exec
    // commands.
    //
    // We DO still want the standard library's utilities for maps, arrays,
    // strings, math; `new_raw()` strips those, so re-register the core
    // package explicitly.
    let mut engine = Engine::new_raw();
    // Safety: CorePackage is the subset without `eval` / OS access. See
    // https://rhai.rs/book/lib/index.html — StandardPackage adds a larger
    // set we don't need.
    use rhai::packages::{CorePackage, Package};
    CorePackage::new().register_into_engine(&mut engine);

    // Cap a runaway script at ~10M operations. A well-formed plan builds
    // in well under a million ops; this guard rail catches infinite loops
    // and pathological recursion without affecting any real use case. The
    // cap triggers `EvalAltResult::ErrorTooManyOperations`, which we map
    // to `ScriptError::Eval` below.
    engine.set_max_operations(10_000_000);

    let root = PlanBuilder::new();
    builders::register(&mut engine, root.clone());

    // Evaluate the script. Rhai's `run` returns `()`; we discard the
    // result because everything we need went into `root`.
    match engine.run(src) {
        Ok(()) => {}
        Err(e) => {
            // Surface MissingEnv specifically so callers can match on it.
            // Rhai wraps runtime errors in `ErrorRuntime(Dynamic, pos)` —
            // we check the message for the "env variable ... not set"
            // prefix we emit from the `env()` shim.
            let msg = format!("{e}");
            if msg.contains("env variable") && msg.contains("not set and no default supplied") {
                // Extract the var name between the first pair of double
                // quotes for a cleaner `MissingEnv(var)`.
                let name = msg
                    .find('"')
                    .and_then(|s| {
                        let rest = &msg[s + 1..];
                        rest.find('"').map(|e| &rest[..e])
                    })
                    .unwrap_or("")
                    .to_string();
                return Err(ScriptError::MissingEnv(name));
            }
            // Structural classification: parse errors are their own
            // `EvalAltResult` variant; everything else is a runtime /
            // eval error. Substring-on-message matching is brittle
            // across Rhai version bumps.
            if matches!(*e, rhai::EvalAltResult::ErrorParsing(..)) {
                return Err(ScriptError::Parse(msg));
            }
            return Err(ScriptError::Eval(msg));
        }
    }

    // Peek at the first wire-step URL BEFORE finalize — finalize takes
    // the state out of the builder. We parse it into a Target first so
    // TemplatedHost errors surface even when the URL contains templates
    // that would otherwise error during Template::compile (e.g.
    // `{{env:UNSET}}`).
    //
    // `first_request_url` returns the URL of the first Request, SSE, or
    // WS step — all three backends need a connection target derived from
    // somewhere in the plan. The CLI's HTTP dispatch path still reaches
    // for this when the plan has an HTTP scenario; pure-SSE / pure-WS
    // plans still produce a well-formed Target (Target::parse accepts
    // ws:// and wss:// as scheme prefixes).
    let first_url_opt = root.first_request_url();
    let target_opt = match &first_url_opt {
        Some(u) => Some(parse_target_strict(u)?),
        None => None,
    };
    let (plan, http_version) = root.finalize()?;
    let first_url = first_url_opt.ok_or(ScriptError::NoRequestSteps)?;
    // target_opt is Some iff first_url_opt was Some, which we just checked.
    let target = target_opt.expect("target parsed when first_url_opt was Some");
    // `_first_url` is discarded — it was only needed for target parsing.
    let _ = first_url;
    // Sanity-check: at least one wire step (Request/SSE/WS) survived
    // compilation. Protocol-agnostic because multi-protocol plans are
    // now first-class citizens.
    debug_assert!(plan.scenarios.iter().any(|s| s.steps.iter().any(|st| {
        matches!(
            st,
            Step::Request(_) | Step::SseStream(_) | Step::WsRound(_)
        )
    })));

    Ok(LoadedScript {
        plan,
        target,
        http_version,
    })
}

/// Extract a [`Target`] from a URL string, rejecting URLs with templated
/// hosts. Path / query / fragment templates are fine — only the authority
/// portion (scheme://host:port) has to be literal, because that's what
/// `Target::parse` consumes.
fn parse_target_strict(url: &str) -> Result<Target, ScriptError> {
    // Find the authority end — first `/`, `?`, or `#` after `://`.
    let after_scheme = url.find("://").map(|i| i + 3).unwrap_or(0);
    let authority_end = url[after_scheme..]
        .find(|c: char| c == '/' || c == '?' || c == '#')
        .map(|i| after_scheme + i)
        .unwrap_or(url.len());
    let authority = &url[..authority_end];

    if authority.contains("{{") {
        return Err(ScriptError::TemplatedHost(url.to_string()));
    }
    Target::parse(authority).map_err(|e| ScriptError::InvalidUrl {
        url: url.to_string(),
        reason: format!("{e}"),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_script_produces_single_scenario_plan() {
        let src = r#"
            scenario("ping", |s| {
                s.step(GET("http://h:1234/path"));
            });
            rate("1k/s");
            duration("5s");
        "#;
        let loaded = load_script_str(src).unwrap();
        assert_eq!(loaded.plan.scenarios.len(), 1);
        assert_eq!(loaded.plan.scenarios[0].name, "ping");
        assert_eq!(loaded.plan.scenarios[0].steps.len(), 1);
        assert_eq!(loaded.target.host, "h");
        assert_eq!(loaded.target.port, 1234);
    }

    #[test]
    fn missing_duration_returns_specific_error() {
        let src = r#"
            scenario("p", |s| { s.step(GET("http://h:1/")); });
            rate("1k/s");
        "#;
        let err = load_script_str(src).unwrap_err();
        assert!(matches!(err, ScriptError::MissingDuration));
    }

    #[test]
    fn no_scenarios_returns_specific_error() {
        let src = r#"
            duration("5s");
        "#;
        let err = load_script_str(src).unwrap_err();
        assert!(matches!(err, ScriptError::NoScenarios));
    }

    #[test]
    fn templated_host_returns_specific_error() {
        // `{{uuid}}` in the host is nonsensical but syntactically valid;
        // we want to reject it up-front because the connection target is
        // opened once at startup. Using `{{uuid}}` rather than `{{env:X}}`
        // because env templates resolve at compile time and would
        // surface as a Template/MissingEnv error first.
        let src = r#"
            scenario("p", |s| { s.step(GET("http://{{uuid}}/path")); });
            duration("5s");
        "#;
        let err = load_script_str(src).unwrap_err();
        assert!(matches!(err, ScriptError::TemplatedHost(_)), "got {err:?}");
    }

    #[test]
    fn templated_path_is_fine() {
        let src = r#"
            scenario("p", |s| { s.step(GET("http://h:1/path/{{uuid}}")); });
            duration("5s");
        "#;
        let loaded = load_script_str(src).unwrap();
        assert_eq!(loaded.target.host, "h");
    }
}
