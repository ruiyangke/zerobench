//! Integration tests for the `.http` request-file parser.

use std::cell::Cell;

use bytes::Bytes;
use zerobench_core::{
    parse_request_bytes, parse_request_file, parse_scenario_dir, rng, BodySource, ExpandCtx,
    RequestFileError, TemplateError, VarRegistry,
};

// ---------------------------------------------------------------------------
// Expansion harness — same shape as tests/template.rs; used to assert on the
// runtime expansion of the compiled templates the parser returns.
// ---------------------------------------------------------------------------

struct Harness {
    rng: zerobench_core::BenchRng,
    counter: Cell<u64>,
    vars: Vec<Option<Bytes>>,
}

impl Harness {
    fn new() -> Self {
        Self {
            rng: rng::from_seed(0xABC0FFEE),
            counter: Cell::new(0),
            vars: Vec::new(),
        }
    }

    fn ctx(&mut self) -> ExpandCtx<'_> {
        ExpandCtx {
            rng: &mut self.rng,
            counter: &self.counter,
            scenario_vars: &self.vars,
        }
    }
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[test]
fn parses_post_with_headers_body_and_templates() {
    let src = "\
POST /api/foo HTTP/1.1
Host: example.com
Content-Type: application/json
X-Run: {{uuid}}

{\"data\":\"{{rand_hex:8}}\"}";

    let mut vars = VarRegistry::new();
    let parsed = parse_request_file(src, "test.http", &mut vars).unwrap();

    assert_eq!(parsed.method.as_str(), "POST");
    assert_eq!(parsed.target.host, "example.com");
    assert_eq!(parsed.target.port, 80);
    assert!(!parsed.target.tls);

    // URL should expand to the full URL.
    let mut h = Harness::new();
    let mut out = Vec::new();
    parsed.url.expand_into(&mut out, &mut h.ctx());
    assert_eq!(
        std::str::from_utf8(&out).unwrap(),
        "http://example.com/api/foo"
    );

    // Three headers: Host, Content-Type, X-Run.
    assert_eq!(parsed.headers.len(), 3);

    // X-Run header value should expand to a 36-char UUID.
    let (xrun_name, xrun_val) = parsed
        .headers
        .iter()
        .find(|(n, _)| {
            let mut b = Vec::new();
            n.expand_into(&mut b, &mut h.ctx());
            b == b"X-Run"
        })
        .expect("X-Run header");
    let mut name_buf = Vec::new();
    xrun_name.expand_into(&mut name_buf, &mut h.ctx());
    assert_eq!(name_buf, b"X-Run");
    let mut val_buf = Vec::new();
    xrun_val.expand_into(&mut val_buf, &mut h.ctx());
    assert_eq!(val_buf.len(), 36);

    // Body present and expands with rand_hex.
    match parsed.body {
        Some(BodySource::Template(t)) => {
            let mut b = Vec::new();
            t.expand_into(&mut b, &mut h.ctx());
            let s = std::str::from_utf8(&b).unwrap();
            assert!(s.starts_with("{\"data\":\""));
            assert!(s.ends_with("\"}"));
            // 8 bytes = 16 hex chars. Outer literals: len("{\"data\":\"") + len("\"}").
            let expected_len = "{\"data\":\"".len() + 16 + "\"}".len();
            assert_eq!(s.len(), expected_len);
        }
        other => panic!("expected Template body, got {other:?}"),
    }
}

#[test]
fn crlf_and_lf_produce_identical_results() {
    let lf = "GET /foo HTTP/1.1\nHost: example.com\n\n";
    let crlf = "GET /foo HTTP/1.1\r\nHost: example.com\r\n\r\n";

    let mut v1 = VarRegistry::new();
    let a = parse_request_file(lf, "a.http", &mut v1).unwrap();
    let mut v2 = VarRegistry::new();
    let b = parse_request_file(crlf, "b.http", &mut v2).unwrap();

    assert_eq!(a.method, b.method);
    assert_eq!(a.target, b.target);
    assert!(a.body.is_none());
    assert!(b.body.is_none());
    assert_eq!(a.headers.len(), b.headers.len());
}

#[test]
fn comment_lines_in_header_area_are_skipped() {
    let src = "\
# this is a comment
# another one
GET /ping HTTP/1.1
# yep, comments between headers too
Host: example.com

";
    let mut vars = VarRegistry::new();
    let parsed = parse_request_file(src, "c.http", &mut vars).unwrap();
    assert_eq!(parsed.method.as_str(), "GET");
    assert_eq!(parsed.target.host, "example.com");
    // Host header is the only non-comment header line.
    assert_eq!(parsed.headers.len(), 1);
}

#[test]
fn absolute_url_in_request_line_is_used_verbatim() {
    let src = "\
POST http://api.example.com:8080/foo HTTP/1.1
Host: shouldnt-matter.example
Content-Type: text/plain

hello";
    let mut vars = VarRegistry::new();
    let parsed = parse_request_file(src, "abs.http", &mut vars).unwrap();
    assert_eq!(parsed.target.host, "api.example.com");
    assert_eq!(parsed.target.port, 8080);

    let mut h = Harness::new();
    let mut out = Vec::new();
    parsed.url.expand_into(&mut out, &mut h.ctx());
    assert_eq!(
        std::str::from_utf8(&out).unwrap(),
        "http://api.example.com:8080/foo"
    );
}

#[test]
fn empty_body_is_ok() {
    let src = "\
GET /health HTTP/1.1
Host: example.com

";
    let mut vars = VarRegistry::new();
    let parsed = parse_request_file(src, "h.http", &mut vars).unwrap();
    assert!(parsed.body.is_none());
}

#[test]
fn body_without_explicit_blank_is_ok_when_request_line_only_has_no_body_needed() {
    // No blank line, but also no body — everything is headers.
    let src = "GET /health HTTP/1.1\nHost: example.com\n";
    let mut vars = VarRegistry::new();
    let parsed = parse_request_file(src, "h.http", &mut vars).unwrap();
    assert!(parsed.body.is_none());
}

#[test]
fn binary_body_is_preserved_via_parse_request_bytes() {
    // A body with a non-UTF-8 byte (0xFF alone is invalid UTF-8).
    let mut src: Vec<u8> = b"POST /upload HTTP/1.1\nHost: h\n\n".to_vec();
    src.push(0xFF);
    src.extend_from_slice(b"rest");
    let mut vars = VarRegistry::new();
    let parsed = parse_request_bytes(&src, "bin.http", &mut vars).unwrap();
    match parsed.body {
        Some(BodySource::Static(b)) => {
            assert_eq!(b.len(), 5);
            assert_eq!(b[0], 0xFF);
            assert_eq!(&b[1..], b"rest");
        }
        other => panic!("expected Static body, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

#[test]
fn empty_input_errors() {
    let mut vars = VarRegistry::new();
    let err = parse_request_file("", "e.http", &mut vars).unwrap_err();
    assert!(matches!(err, RequestFileError::Empty));
}

#[test]
fn missing_request_line_errors() {
    let src = "# only a comment\n\n";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "c.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::MissingRequestLine { file } => {
            assert_eq!(file, "c.http");
        }
        other => panic!("expected MissingRequestLine, got {other:?}"),
    }
}

#[test]
fn malformed_request_line_errors() {
    let src = "NOT A REQUEST LINE\nHost: h\n\n";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "m.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::InvalidRequestLine(s) => {
            assert!(s.contains("m.http"));
            assert!(s.contains(":1"), "expected source:line prefix, got {s}");
        }
        other => panic!("expected InvalidRequestLine, got {other:?}"),
    }
}

#[test]
fn http_2_rejected() {
    let src = "GET / HTTP/2\nHost: h\n\n";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "h2.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::UnsupportedVersion(s) => {
            assert!(s.contains("h2.http"));
            assert!(s.contains("HTTP/2"));
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn missing_host_errors_for_relative_path() {
    let src = "GET /foo HTTP/1.1\nX-Other: 1\n\n";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "no-host.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::MissingHost { file } => {
            assert_eq!(file, "no-host.http");
        }
        other => panic!("expected MissingHost, got {other:?}"),
    }
}

#[test]
fn malformed_header_without_colon_errors() {
    let src = "GET /foo HTTP/1.1\nHost: example.com\nNotAHeader\n\n";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "bad.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::MalformedHeader(detail) => {
            assert!(detail.contains("NotAHeader"));
            assert!(detail.contains("bad.http"));
            // Header is on line 3 (line 1 = request line).
            assert!(detail.contains(":3"), "expected line 3 prefix, got {detail}");
        }
        other => panic!("expected MalformedHeader, got {other:?}"),
    }
}

#[test]
fn missing_env_in_header_template_propagates() {
    let src = "\
GET /foo HTTP/1.1
Host: example.com
Authorization: {{env:ZEROBENCH_NEVER_SET_IN_TESTS_X7}}

";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "env.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::Template {
            field,
            error: TemplateError::MissingEnv(name),
        } => {
            assert!(field.contains("Authorization"));
            assert_eq!(name, "ZEROBENCH_NEVER_SET_IN_TESTS_X7");
        }
        other => panic!("expected Template MissingEnv, got {other:?}"),
    }
}

#[test]
fn missing_blank_line_with_trailing_body_errors_with_source() {
    // Headers terminated by a single newline, then a body — no blank
    // line. This is the ambiguous shape: we can't tell whether the
    // trailing line is another header or the body. Expect MissingBlankLine.
    let src = "POST /foo HTTP/1.1\nHost: h\n{\"x\":1}";
    let mut vars = VarRegistry::new();
    let err = parse_request_file(src, "nobody.http", &mut vars).unwrap_err();
    match err {
        RequestFileError::MissingBlankLine { file } => {
            assert_eq!(file, "nobody.http");
        }
        other => panic!("expected MissingBlankLine, got {other:?}"),
    }
}

#[test]
fn header_names_and_values_are_passed_through_template_engine() {
    // Headers may reference vars from the registry.
    let src = "\
GET /foo HTTP/1.1
Host: example.com
X-Token: {{var:token}}

";
    let mut vars = VarRegistry::new();
    let parsed = parse_request_file(src, "tok.http", &mut vars).unwrap();
    // One `{{var:token}}` allocated.
    assert_eq!(vars.len(), 1);
    assert_eq!(parsed.headers.len(), 2);
}

// ---------------------------------------------------------------------------
// Directory / scenarios.toml
// ---------------------------------------------------------------------------

/// Minimal temp-dir helper — avoids pulling in the `tempfile` /
/// `tempdir` dev-deps just for these four directory tests. Creates a
/// uniquely-named directory under the system tempdir and deletes it on
/// drop.
struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let pid = std::process::id();
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "zerobench-{label}-{pid}-{seq}-{nanos}"
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn write_scenario_dir() -> TempDir {
    let dir = TempDir::new("scenariodir");
    std::fs::write(
        dir.path().join("login.http"),
        "POST /login HTTP/1.1\nHost: api.example.com\n\n{}\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("browse.http"),
        "GET /browse HTTP/1.1\nHost: api.example.com\n\n",
    )
    .unwrap();
    dir
}

#[test]
fn directory_without_weights_gives_equal_share() {
    let dir = write_scenario_dir();
    let entries = parse_scenario_dir(dir.path()).unwrap();
    assert_eq!(entries.len(), 2);
    // Equal weights, sum to 1.
    for e in &entries {
        assert!((e.weight - 0.5).abs() < 1e-5, "weight {}", e.weight);
    }
    let sum: f32 = entries.iter().map(|e| e.weight).sum();
    assert!((sum - 1.0).abs() < 1e-5);
}

#[test]
fn directory_with_scenarios_toml_normalizes_weights() {
    let dir = write_scenario_dir();
    std::fs::write(
        dir.path().join("scenarios.toml"),
        r#"
[[scenario]]
file = "login.http"
weight = 1.0

[[scenario]]
file = "browse.http"
weight = 9.0
"#,
    )
    .unwrap();

    let entries = parse_scenario_dir(dir.path()).unwrap();
    assert_eq!(entries.len(), 2);
    let login = entries.iter().find(|e| e.name == "login").unwrap();
    let browse = entries.iter().find(|e| e.name == "browse").unwrap();
    assert!((login.weight - 0.1).abs() < 1e-5, "got {}", login.weight);
    assert!((browse.weight - 0.9).abs() < 1e-5, "got {}", browse.weight);
}

#[test]
fn scenarios_toml_with_nonexistent_file_errors() {
    let dir = write_scenario_dir();
    std::fs::write(
        dir.path().join("scenarios.toml"),
        r#"
[[scenario]]
file = "does-not-exist.http"
weight = 0.5
"#,
    )
    .unwrap();
    let err = parse_scenario_dir(dir.path()).unwrap_err();
    assert!(matches!(err, RequestFileError::Scenario(_)));
}

#[test]
fn empty_directory_errors() {
    let dir = TempDir::new("empty");
    let err = parse_scenario_dir(dir.path()).unwrap_err();
    assert!(matches!(err, RequestFileError::Scenario(_)));
}
