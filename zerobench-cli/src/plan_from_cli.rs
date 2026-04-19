//! Convert a parsed [`CliArgs`] into a runnable
//! ([`Plan`], [`Target`], [`TransportOpts`]) triple.
//!
//! The conversion is pure — no IO beyond optional `--body-file` /
//! `--request-file` / `--requests` reads — which makes it easy to
//! unit-test without standing up a runtime.

use std::fs;
use std::time::Duration;

use http::Method;
use smallvec::SmallVec;
use zerobench_core::plan::{
    Assertion, BodySource, Mode, Plan, RateProfile, RequestPlan, Scenario, Step,
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
    // Transport opts are shared across all input modes.
    //
    // `--http2-prior-knowledge` is an alias for `--http-version h2` —
    // discoverable via a curl-familiar flag name. Our mio_h2 always uses
    // prior knowledge on plain HTTP, so the two are indistinguishable
    // at the wire level.
    let http_version = if args.http2_prior_knowledge {
        HttpVersionPref::Http2
    } else {
        match args.http_version {
            CliHttpVersion::Auto => HttpVersionPref::Auto,
            CliHttpVersion::H1 => HttpVersionPref::Http1,
            CliHttpVersion::H2 => HttpVersionPref::Http2,
        }
    };
    // The legacy `--sse` forced HTTP/1 fallback. v0.1.0 SSE goes
    // through `measure --sse-hold` which uses HTTP/1 by construction;
    // no special-case here.

    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        http_version,
        resolve_overrides: args.resolve.clone(),
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

    // Resolve --method. Explicit `-X` wins. When `-X` was omitted and a
    // body source was provided, auto-promote GET → POST so the common
    // `zerobench --json '{}' URL` shortcut does the right thing.
    let has_body_hint = args.body.is_some()
        || args.body_file.is_some()
        || args.json.is_some()
        || args.form.is_some();
    let method_raw = match &args.method {
        Some(m) => m.clone(),
        None if has_body_hint => "POST".to_string(),
        None => "GET".to_string(),
    };
    let method = method_raw
        .parse::<Method>()
        .map_err(|_| BuildError::InvalidMethod(method_raw.clone()))?;

    // S2.22 — warn on explicit `-X GET` combined with a body.
    if has_body_hint
        && method == Method::GET
        && args.method.as_deref().is_some_and(|m| m.eq_ignore_ascii_case("GET"))
    {
        eprintln!(
            "warning: GET request with body — technically valid but most servers ignore it",
        );
    }

    // Build the registry + URL template.
    let mut vars = VarRegistry::new();
    let url_tpl = Template::compile(url, &mut vars)?;

    // Headers — both sides through the template engine. Auth headers
    // derived from --basic-auth / --bearer are appended after the
    // user's -H list, but only if the user didn't already pass
    // `-H Authorization: ...`. When they conflict, explicit -H wins
    // with a stderr warning (S1.7).
    let user_has_auth_header = args
        .headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("Authorization"));

    let mut headers: SmallVec<[(Template, Template); 8]> = SmallVec::new();
    for (name, value) in &args.headers {
        let name_tpl = Template::compile(name, &mut vars)?;
        let value_tpl = Template::compile(value, &mut vars)?;
        headers.push((name_tpl, value_tpl));
    }
    push_auth_header(args, user_has_auth_header, &mut vars, &mut headers)?;

    // Body — precedence: inline --body > --body-file > --json > --form.
    // clap's `conflicts_with` ensures at most one of these is set, so
    // the order here only matters for defensive code review.
    let body = if let Some(inline) = &args.body {
        Some(BodySource::Template(Template::compile(inline, &mut vars)?))
    } else if let Some(path) = &args.body_file {
        let bytes = fs::read(path)?;
        Some(BodySource::Static(bytes::Bytes::from(bytes)))
    } else if let Some(json) = &args.json {
        push_content_type(
            "application/json",
            &args.headers,
            &mut vars,
            &mut headers,
        )?;
        Some(BodySource::Template(Template::compile(json, &mut vars)?))
    } else if let Some(form) = &args.form {
        push_content_type(
            "application/x-www-form-urlencoded",
            &args.headers,
            &mut vars,
            &mut headers,
        )?;
        Some(BodySource::Template(Template::compile(form, &mut vars)?))
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
        expect_streaming: false,
    };

    let rate = pick_rate_profile(args, 1.0);
    let scenario = Scenario {
        name: "cli".into(),
        rate,
        steps: vec![Step::Request(request)],
    };

    // NOTE (S1.6): `plan.warmup` is plumbed from the CLI but the mio
    // dispatch layer in zerobench-http doesn't honour it yet — it
    // starts measuring on first request. TODO: wire through.
    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: args.duration,
        warmup: args.warmup.unwrap_or(Duration::ZERO),
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::default(),
        name: String::new(),
    };

    Ok((plan, target, opts.clone()))
}

/// Append an Authorization header for --basic-auth / --bearer, or warn
/// and skip when the user already supplied one via -H.
fn push_auth_header(
    args: &CliArgs,
    user_has_auth_header: bool,
    vars: &mut VarRegistry,
    headers: &mut SmallVec<[(Template, Template); 8]>,
) -> Result<(), BuildError> {
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;

    let auth_value = if let Some(user_pass) = &args.basic_auth {
        Some(format!("Basic {}", B64.encode(user_pass.as_bytes())))
    } else if let Some(token) = &args.bearer {
        Some(format!("Bearer {token}"))
    } else {
        None
    };

    let Some(value) = auth_value else { return Ok(()) };
    if user_has_auth_header {
        eprintln!(
            "warning: -H 'Authorization: ...' overrides --basic-auth / --bearer",
        );
        return Ok(());
    }
    headers.push((
        Template::compile("Authorization", vars)?,
        Template::compile(&value, vars)?,
    ));
    Ok(())
}

/// Append `Content-Type: <ct>` unless the user already set one via -H.
/// Used by --json and --form body paths.
fn push_content_type(
    ct: &str,
    user_headers: &[(String, String)],
    vars: &mut VarRegistry,
    headers: &mut SmallVec<[(Template, Template); 8]>,
) -> Result<(), BuildError> {
    let already = user_headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("Content-Type"));
    if already {
        return Ok(());
    }
    headers.push((
        Template::compile("Content-Type", vars)?,
        Template::compile(ct, vars)?,
    ));
    Ok(())
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
        expect_streaming: false,
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
        warmup: args.warmup.unwrap_or(Duration::ZERO),
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::default(),
        name: String::new(),
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
            expect_streaming: false,
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
        warmup: args.warmup.unwrap_or(Duration::ZERO),
        cooldown: Duration::ZERO,
        runs: 1,
        threads: 1,
        mode: Mode::default(),
        name: String::new(),
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
    fn build_http2_prior_knowledge_flag_implies_h2() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--http2-prior-knowledge",
            "http://h:1/",
        ]);
        let (_plan, _target, opts) = build(&args).unwrap();
        assert_eq!(opts.http_version, HttpVersionPref::Http2);
    }

    #[test]
    fn build_warmup_is_plumbed_through_to_plan() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--warmup",
            "7s",
            "http://h:1/",
        ]);
        let (plan, _target, _opts) = build(&args).unwrap();
        assert_eq!(plan.warmup, std::time::Duration::from_secs(7));
    }

    #[test]
    fn build_warmup_absent_yields_zero() {
        let args = parse(&["zerobench", "--saturate", "http://h:1/"]);
        let (plan, _target, _opts) = build(&args).unwrap();
        assert_eq!(plan.warmup, std::time::Duration::ZERO);
    }

    #[test]
    fn build_resolve_overrides_forwarded_to_transport_opts() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--resolve",
            "example.com:443:10.0.0.5",
            "http://example.com/",
        ]);
        let (_plan, _target, opts) = build(&args).unwrap();
        assert_eq!(opts.resolve_overrides.len(), 1);
        assert_eq!(opts.resolve_overrides[0].0, "example.com");
        assert_eq!(opts.resolve_overrides[0].1, 443);
        assert_eq!(opts.resolve_overrides[0].2, "10.0.0.5");
    }

    #[test]
    fn build_basic_auth_adds_header() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--basic-auth",
            "alice:pw",
            "http://h:1/",
        ]);
        let (plan, _target, _opts) = build(&args).unwrap();
        let step = &plan.scenarios[0].steps[0];
        let Step::Request(req) = step else { panic!("expected Request") };
        // Expect one header named `Authorization`.
        assert_eq!(req.headers.len(), 1);
    }

    #[test]
    fn build_bearer_adds_header() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--bearer",
            "token123",
            "http://h:1/",
        ]);
        let (plan, _target, _opts) = build(&args).unwrap();
        let Step::Request(req) = &plan.scenarios[0].steps[0] else {
            panic!("expected Request")
        };
        assert_eq!(req.headers.len(), 1);
    }

    #[test]
    fn build_explicit_auth_header_wins_over_basic_auth() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--basic-auth",
            "alice:pw",
            "-H",
            "Authorization: CustomScheme 42",
            "http://h:1/",
        ]);
        let (plan, _target, _opts) = build(&args).unwrap();
        let Step::Request(req) = &plan.scenarios[0].steps[0] else {
            panic!("expected Request")
        };
        // Only one Authorization header — the explicit -H, not two.
        assert_eq!(req.headers.len(), 1);
    }

    #[test]
    fn build_json_body_adds_content_type_and_implies_post() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--json",
            "{\"k\":1}",
            "http://h:1/",
        ]);
        let (plan, _target, _opts) = build(&args).unwrap();
        let Step::Request(req) = &plan.scenarios[0].steps[0] else {
            panic!("expected Request")
        };
        assert_eq!(req.method, http::Method::POST);
        // Content-Type header added.
        assert_eq!(req.headers.len(), 1);
        assert!(req.body.is_some());
    }

    #[test]
    fn build_form_body_adds_content_type_and_implies_post() {
        let args = parse(&[
            "zerobench",
            "--saturate",
            "--form",
            "a=1&b=2",
            "http://h:1/",
        ]);
        let (plan, _target, _opts) = build(&args).unwrap();
        let Step::Request(req) = &plan.scenarios[0].steps[0] else {
            panic!("expected Request")
        };
        assert_eq!(req.method, http::Method::POST);
        assert_eq!(req.headers.len(), 1);
        assert!(req.body.is_some());
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
