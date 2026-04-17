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
use zerobench_core::transport::{HttpVersionPref, Target, TargetError, TransportOpts};
use zerobench_core::var::VarRegistry;

use crate::cli_args::{CliArgs, CliHttpVersion};

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
    /// Multiple `.http` files in a `--requests DIR` point at different
    /// hosts/ports. The gateway/worker fan-out assumes all scenarios
    /// share one target; silently using the first would give wrong
    /// results.
    #[error("--requests DIR contains scenarios pointing at different hosts:\n{0}")]
    MultipleHosts(String),
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
    // Transport opts are shared across all input modes. When --sse is on
    // we force HTTP/1: v0.0.1's streaming path is only wired through
    // `Http1Pool::exchange_streaming`, and HTTP/2 multiplexing would
    // conflate chunk timing across concurrent streams anyway.
    let http_version = match args.http_version {
        CliHttpVersion::Auto => HttpVersionPref::Auto,
        CliHttpVersion::H1 => HttpVersionPref::Http1,
        CliHttpVersion::H2 => HttpVersionPref::Http2,
    };
    #[cfg(feature = "sse")]
    let http_version = if args.sse {
        HttpVersionPref::Http1
    } else {
        http_version
    };

    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        http_version,
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
        expect_streaming: is_sse(args),
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
        threads: 1,
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
        expect_streaming: is_sse(args),
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
        threads: 1,
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
    let mut first_target: Option<(String, Target)> = None;
    // Collect every scenario's (filename, host:port) so we can report
    // the whole mismatch set in one shot rather than erroring on the
    // second file.
    let mut host_summary: Vec<(String, String)> = Vec::with_capacity(entries.len());

    for entry in &entries {
        let bytes = fs::read(&entry.file)?;
        let source_name = entry
            .file
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("request-file");
        let parsed = parse_request_bytes(&bytes, source_name, &mut vars)?;

        let authority = format_authority(&parsed.target);
        host_summary.push((source_name.to_string(), authority));

        if first_target.is_none() {
            first_target = Some((source_name.to_string(), parsed.target.clone()));
        }

        let request = RequestPlan {
            method: parsed.method,
            url: parsed.url,
            headers: parsed.headers,
            body: parsed.body,
            extract: Vec::new(),
            checks: checks.clone(),
            expect_streaming: is_sse(args),
        };
        let rate = pick_rate_profile(args, entry.weight as f64);
        scenarios.push(Scenario {
            name: entry.name.clone(),
            rate,
            steps: vec![Step::Request(request)],
        });
    }

    // Detect and surface multi-host inconsistency. We compare by
    // `(host, port, tls)` because those determine which TCP endpoint
    // the transport opens; SNI overrides are per-connection metadata,
    // not a routing key.
    let (first_file, first) = first_target.expect("at least one scenario");
    let first_authority = format_authority(&first);
    let all_match = host_summary
        .iter()
        .all(|(_, a)| a == &first_authority);
    if !all_match {
        let mut lines = Vec::with_capacity(host_summary.len());
        lines.push(format!("  {first_file}: {first_authority}"));
        for (name, authority) in &host_summary {
            if name == &first_file {
                continue;
            }
            lines.push(format!("  {name}: {authority}"));
        }
        return Err(BuildError::MultipleHosts(lines.join("\n")));
    }

    let target = first;
    let plan = Plan {
        scenarios,
        vars,
        duration: args.duration,
        warmup: None,
        threads: 1,
    };
    Ok((plan, target, opts.clone()))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Stable string form of a target's routing key (`host:port + scheme`).
/// Used for multi-host mismatch error rendering.
fn format_authority(t: &Target) -> String {
    let scheme = if t.tls { "https" } else { "http" };
    format!("{scheme}://{}:{}", t.host, t.port)
}

/// `true` when the user asked for SSE mode. Centralised behind a helper
/// so the #[cfg(feature = "sse")] gate lives in one spot instead of
/// smearing across every RequestPlan construction site.
#[inline]
fn is_sse(_args: &CliArgs) -> bool {
    #[cfg(feature = "sse")]
    {
        _args.sse
    }
    #[cfg(not(feature = "sse"))]
    {
        false
    }
}

/// Extract the path-and-query portion of a URL (e.g. `/echo?x=1`) for
/// use as the HTTP Upgrade target. Falls back to `/` when absent.
///
/// Used by the `--ws` dispatch path, which needs the path as a separate
/// field on [`zerobench_ws::WsPlan`] (the rest of the URL goes into
/// [`Target`]).
#[cfg(feature = "ws")]
pub fn extract_path_from_url(url: &str) -> String {
    let rest = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => return "/".to_string(),
    };
    match rest.find(|c: char| c == '/' || c == '?' || c == '#') {
        Some(i) => {
            let p = &rest[i..];
            // Strip fragment — it's client-side only.
            match p.find('#') {
                Some(j) => p[..j].to_string(),
                None => p.to_string(),
            }
        }
        None => "/".to_string(),
    }
}

/// Build a [`WsPlan`] from the parsed CLI args + the positional URL.
///
/// The `--ws` path doesn't go through the regular [`Plan`] construction
/// because the WebSocket benchmark doesn't need templates, scenarios,
/// rate profiles, or any of the other Phase-C machinery. Everything the
/// runner needs lives directly on [`WsPlan`].
#[cfg(feature = "ws")]
pub fn build_ws_plan(
    args: &CliArgs,
) -> Result<(zerobench_ws::WsPlan, zerobench_core::transport::TransportOpts), BuildError> {
    let url = args.url.as_deref().ok_or(BuildError::MissingInput)?;
    let target = Target::parse(url)?;
    let path = extract_path_from_url(url);

    let opts = zerobench_core::transport::TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        http_version: HttpVersionPref::Http1,
    };

    let plan = zerobench_ws::WsPlan {
        target,
        path,
        headers: args.headers.clone(),
        message: bytes::Bytes::copy_from_slice(args.ws_message.as_bytes()),
        opts: opts.clone(),
    };

    Ok((plan, opts))
}

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

    #[test]
    fn build_from_requests_dir_rejects_multiple_hosts() {
        use std::sync::atomic::{AtomicU32, Ordering};
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let pid = std::process::id();
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "zerobench-multihost-{pid}-{seq}-{nanos}"
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // Two scenarios pointing at *different* hosts.
        std::fs::write(
            dir.join("a.http"),
            "GET /a HTTP/1.1\nHost: api-one.example.com\n\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("b.http"),
            "GET /b HTTP/1.1\nHost: api-two.example.com\n\n",
        )
        .unwrap();

        let args = parse(&[
            "zerobench",
            "--saturate",
            "--requests",
            dir.to_str().unwrap(),
        ]);
        let err = build(&args).unwrap_err();
        let _ = std::fs::remove_dir_all(&dir);
        match err {
            BuildError::MultipleHosts(details) => {
                assert!(
                    details.contains("api-one.example.com"),
                    "details should list first host, got: {details}"
                );
                assert!(
                    details.contains("api-two.example.com"),
                    "details should list conflicting host, got: {details}"
                );
                assert!(details.contains("a.http"));
                assert!(details.contains("b.http"));
            }
            other => panic!("expected MultipleHosts, got {other:?}"),
        }
    }

    #[test]
    fn build_default_http_version_is_auto() {
        let args = parse(&["zerobench", "--saturate", "http://h:1/"]);
        let (_plan, _target, opts) = build(&args).unwrap();
        assert_eq!(opts.http_version, HttpVersionPref::Auto);
    }

    #[test]
    fn build_honours_http_version_h1() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--http-version",
            "h1",
            "http://h:1/",
        ]);
        let (_plan, _target, opts) = build(&args).unwrap();
        assert_eq!(opts.http_version, HttpVersionPref::Http1);
    }

    #[test]
    fn build_honours_http_version_h2() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--http-version",
            "h2",
            "http://h:1/",
        ]);
        let (_plan, _target, opts) = build(&args).unwrap();
        assert_eq!(opts.http_version, HttpVersionPref::Http2);
    }

    #[test]
    fn build_from_requests_dir_accepts_matching_hosts() {
        use std::sync::atomic::{AtomicU32, Ordering};
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let pid = std::process::id();
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "zerobench-samehost-{pid}-{seq}-{nanos}"
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("login.http"),
            "POST /login HTTP/1.1\nHost: api.example.com\n\n{}\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("browse.http"),
            "GET /browse HTTP/1.1\nHost: api.example.com\n\n",
        )
        .unwrap();

        let args = parse(&[
            "zerobench",
            "--saturate",
            "--requests",
            dir.to_str().unwrap(),
        ]);
        let result = build(&args);
        let _ = std::fs::remove_dir_all(&dir);
        let (plan, target, _) = result.unwrap();
        assert_eq!(plan.scenarios.len(), 2);
        assert_eq!(target.host, "api.example.com");
    }
}
