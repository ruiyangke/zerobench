//! Convert a parsed [`CliArgs`] into a runnable
//! ([`Plan`], [`Target`], [`TransportOpts`]) triple.
//!
//! The conversion is pure — no IO beyond optional `--body-file` /
//! `--request-file` / `--requests` reads — which makes it easy to
//! unit-test without standing up a runtime.

use std::fs;

use http::Method;
use smallvec::SmallVec;
use zerobench_core::plan::{
    Assertion, BodySource, Plan, RateProfile, RequestPlan, Scenario, Step,
};
use zerobench_core::request_file::{
    parse_request_bytes, parse_scenario_dir, RequestFileError,
};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TargetError, TransportOpts};
use zerobench_core::var::VarRegistry;

use crate::cli_args::CliArgs;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("target url: {0}")]
    Target(#[from] TargetError),
    #[error("template: {0}")]
    Template(#[from] zerobench_core::template::TemplateError),
    #[error("method {0:?} is not valid")]
    InvalidMethod(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("request file: {0}")]
    RequestFile(#[from] RequestFileError),
    #[error("either a URL, --request-file, or --requests is required")]
    MissingInput,
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

/// Build a runnable plan + connection target + transport opts.
///
/// Three input modes:
/// 1. Positional URL — the Phase C path; one scenario, one request.
/// 2. `--request-file PATH` — parse a single `.http` file; one scenario.
/// 3. `--requests DIR` — parse every `.http` file in the directory;
///    one scenario per file, weighted per `scenarios.toml`.
pub fn build(args: &CliArgs) -> Result<(Plan, Target, TransportOpts), BuildError> {
    // Transport opts are shared across all input modes.
    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
    };

    if let Some(path) = &args.request_file {
        return build_from_request_file(args, &opts, path);
    }
    if let Some(dir) = &args.requests {
        return build_from_request_dir(args, &opts, dir);
    }

    let url = args.url.as_deref().ok_or(BuildError::MissingInput)?;
    build_from_url(args, &opts, url)
}

// ---------------------------------------------------------------------------
// Single-URL mode (Phase C classic behaviour)
// ---------------------------------------------------------------------------

fn build_from_url(
    args: &CliArgs,
    opts: &TransportOpts,
    url: &str,
) -> Result<(Plan, Target, TransportOpts), BuildError> {
    let target = Target::parse(url)?;

    // HTTP method — infer POST if the user passed a body but no -X.
    let method_raw = if args.body.is_some() || args.body_file.is_some() {
        if args.method.eq_ignore_ascii_case("GET") {
            // The default was never overridden; imply POST.
            "POST".to_string()
        } else {
            args.method.clone()
        }
    } else {
        args.method.clone()
    };
    let method = method_raw
        .parse::<Method>()
        .map_err(|_| BuildError::InvalidMethod(method_raw.clone()))?;

    // Build the registry + URL template.
    let mut vars = VarRegistry::new();
    let url_tpl = Template::compile(url, &mut vars)?;

    // Headers — both sides through the template engine.
    let mut headers: SmallVec<[(Template, Template); 8]> = SmallVec::new();
    for (name, value) in &args.headers {
        let name_tpl = Template::compile(name, &mut vars)?;
        let value_tpl = Template::compile(value, &mut vars)?;
        headers.push((name_tpl, value_tpl));
    }

    // Body — inline string wins over file.
    let body = if let Some(inline) = &args.body {
        Some(BodySource::Template(Template::compile(inline, &mut vars)?))
    } else if let Some(path) = &args.body_file {
        let bytes = fs::read(path)?;
        Some(BodySource::Static(bytes::Bytes::from(bytes)))
    } else {
        None
    };

    let checks = build_checks(args);

    let request = RequestPlan {
        method,
        url: url_tpl,
        headers,
        body,
        extract: Vec::new(),
        checks,
    };

    let rate = pick_rate_profile(args, 1.0);
    let scenario = Scenario {
        name: "cli".into(),
        rate,
        steps: vec![Step::Request(request)],
    };

    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: args.duration,
        warmup: None,
    };

    Ok((plan, target, opts.clone()))
}

// ---------------------------------------------------------------------------
// Single .http file mode
// ---------------------------------------------------------------------------

fn build_from_request_file(
    args: &CliArgs,
    opts: &TransportOpts,
    path: &std::path::Path,
) -> Result<(Plan, Target, TransportOpts), BuildError> {
    let bytes = fs::read(path)?;
    let source_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("request-file");

    let mut vars = VarRegistry::new();
    let parsed = parse_request_bytes(&bytes, source_name, &mut vars)?;
    let target = parsed.target.clone();

    let checks = build_checks(args);

    let request = RequestPlan {
        method: parsed.method,
        url: parsed.url,
        headers: parsed.headers,
        body: parsed.body,
        extract: Vec::new(),
        checks,
    };

    let rate = pick_rate_profile(args, 1.0);
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("request")
        .to_string();
    let scenario = Scenario {
        name,
        rate,
        steps: vec![Step::Request(request)],
    };
    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: args.duration,
        warmup: None,
    };

    Ok((plan, target, opts.clone()))
}

// ---------------------------------------------------------------------------
// Directory-of-.http mode
// ---------------------------------------------------------------------------
//
// Design choice for `--saturate` + `--requests`:
//   - Each scenario shares the same connection pool (i.e. every
//     scenario gets `RateProfile::Saturate { max_concurrency: -c }`).
//     Workers pick a scenario at random (Phase C uniform selection);
//     this means the effective per-scenario share is ~equal across
//     scenarios, independent of weights.
//   - The weights matter for open-loop (`-r TOTAL`) mode where each
//     scenario's rate = `TOTAL * weight`.
//
// The alternative — split the saturate pool by weight — was rejected
// because it makes the user's `-c N` flag mean something different
// from "N concurrent connections total"; that invariant is more
// valuable than proportional scenario mixing under saturate.
fn build_from_request_dir(
    args: &CliArgs,
    opts: &TransportOpts,
    dir: &std::path::Path,
) -> Result<(Plan, Target, TransportOpts), BuildError> {
    let entries = parse_scenario_dir(dir)?;
    if entries.is_empty() {
        return Err(BuildError::MissingInput);
    }

    let checks = build_checks(args);
    let mut vars = VarRegistry::new();
    let mut scenarios: Vec<Scenario> = Vec::with_capacity(entries.len());
    let mut target_from_first: Option<Target> = None;

    for entry in &entries {
        let bytes = fs::read(&entry.file)?;
        let source_name = entry
            .file
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("request-file");
        let parsed = parse_request_bytes(&bytes, source_name, &mut vars)?;

        if target_from_first.is_none() {
            target_from_first = Some(parsed.target.clone());
        }

        let request = RequestPlan {
            method: parsed.method,
            url: parsed.url,
            headers: parsed.headers,
            body: parsed.body,
            extract: Vec::new(),
            checks: checks.clone(),
        };
        let rate = pick_rate_profile(args, entry.weight as f64);
        scenarios.push(Scenario {
            name: entry.name.clone(),
            rate,
            steps: vec![Step::Request(request)],
        });
    }

    let target = target_from_first.expect("at least one scenario");
    let plan = Plan {
        scenarios,
        vars,
        duration: args.duration,
        warmup: None,
    };
    Ok((plan, target, opts.clone()))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn build_checks(args: &CliArgs) -> Vec<Assertion> {
    let mut checks: Vec<Assertion> = Vec::new();
    if let Some(code) = args.expect_status {
        checks.push(Assertion::StatusEq(code));
    }
    if let Some(list) = &args.expect_status_in {
        let mut codes: SmallVec<[u16; 4]> = SmallVec::new();
        for c in &list.0 {
            codes.push(*c);
        }
        checks.push(Assertion::StatusIn(codes));
    }
    checks
}

/// Pick a rate profile for a scenario whose weight is `weight` in
/// `[0, 1]` (for single-scenario invocations, `weight = 1.0`).
fn pick_rate_profile(args: &CliArgs, weight: f64) -> RateProfile {
    if args.saturate {
        // Every scenario shares the same concurrency pool; see the
        // module-level comment.
        RateProfile::Saturate {
            max_concurrency: args.connections,
        }
    } else if let Some(total_rps) = args.rate {
        RateProfile::Constant(total_rps * weight)
    } else {
        // No mode given — default to saturate.
        RateProfile::Saturate {
            max_concurrency: args.connections,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(argv: &[&str]) -> CliArgs {
        CliArgs::try_parse_from(argv).unwrap()
    }

    #[test]
    fn build_minimal_saturate_plan() {
        let args = parse(&["zerobench", "--saturate", "http://127.0.0.1:1234/"]);
        let (plan, target, opts) = build(&args).unwrap();
        assert_eq!(target.host, "127.0.0.1");
        assert_eq!(target.port, 1234);
        assert_eq!(opts.max_conns, 50);
        assert_eq!(plan.scenarios.len(), 1);
        match &plan.scenarios[0].rate {
            RateProfile::Saturate { max_concurrency } => {
                assert_eq!(*max_concurrency, 50);
            }
            other => panic!("expected Saturate, got {other:?}"),
        }
    }

    #[test]
    fn build_with_explicit_rate_gives_constant_profile() {
        let args = parse(&["zerobench", "-r", "500", "http://h:1/"]);
        let (plan, _, _) = build(&args).unwrap();
        match &plan.scenarios[0].rate {
            RateProfile::Constant(r) => assert_eq!(*r, 500.0),
            other => panic!("expected Constant, got {other:?}"),
        }
    }

    #[test]
    fn build_infers_post_from_body() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--body",
            "{\"x\":1}",
            "http://h:1/",
        ]);
        let (plan, _, _) = build(&args).unwrap();
        if let Step::Request(r) = &plan.scenarios[0].steps[0] {
            assert_eq!(r.method, Method::POST);
            assert!(r.body.is_some());
        } else {
            panic!("expected Request");
        }
    }

    #[test]
    fn build_honours_explicit_method() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "-X",
            "PUT",
            "--body",
            "data",
            "http://h:1/",
        ]);
        let (plan, _, _) = build(&args).unwrap();
        if let Step::Request(r) = &plan.scenarios[0].steps[0] {
            assert_eq!(r.method, Method::PUT);
        } else {
            panic!("expected Request");
        }
    }

    #[test]
    fn build_expect_status_adds_assertion() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--expect-status",
            "418",
            "http://h:1/",
        ]);
        let (plan, _, _) = build(&args).unwrap();
        if let Step::Request(r) = &plan.scenarios[0].steps[0] {
            assert_eq!(r.checks.len(), 1);
            match &r.checks[0] {
                Assertion::StatusEq(c) => assert_eq!(*c, 418),
                other => panic!("expected StatusEq, got {other:?}"),
            }
        }
    }

    #[test]
    fn build_expect_status_in_adds_list_assertion() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--expect-status-in",
            "200,201,204",
            "http://h:1/",
        ]);
        let (plan, _, _) = build(&args).unwrap();
        if let Step::Request(r) = &plan.scenarios[0].steps[0] {
            match &r.checks[0] {
                Assertion::StatusIn(codes) => {
                    assert_eq!(codes.as_slice(), &[200, 201, 204]);
                }
                other => panic!("expected StatusIn, got {other:?}"),
            }
        }
    }

    #[test]
    fn build_rejects_invalid_url() {
        let args = parse(&["zerobench", "--saturate", "ftp://h:1/"]);
        assert!(matches!(build(&args), Err(BuildError::Target(_))));
    }

    #[test]
    fn build_header_through_template_engine() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "-H",
            "X-Run: {{uuid}}",
            "http://h:1/",
        ]);
        let (plan, _, _) = build(&args).unwrap();
        if let Step::Request(r) = &plan.scenarios[0].steps[0] {
            assert_eq!(r.headers.len(), 1);
            // Both name and value went through the engine.
            assert!(r.headers[0].1.part_count() > 0);
        }
    }
}
