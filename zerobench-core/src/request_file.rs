//! `.http` request-file parser (curl `--trace-ascii` compatible).
//!
//! Accepts the raw HTTP/1.1 wire format produced by `curl --trace-ascii`,
//! or that you'd hand-type in a scratch file while iterating on an API.
//!
//! # Format
//!
//! ```text
//! # optional comment lines in the header area
//! POST /api/foo HTTP/1.1                  <-- request line required
//! Host: example.com                       <-- Host header required
//! Content-Type: application/json
//! X-My-Header: {{uuid}}
//!                                          <-- BLANK LINE = end of headers
//! {"data":"{{rand_hex:16}}"}               <-- body (rest of file)
//! ```
//!
//! Both `\r\n` and `\n` line endings are accepted. `#` comments are
//! allowed in the header area only — inside the body, `#` is a literal
//! byte. The blank line between headers and body is required even when
//! the body is empty (omitting it falls back to "no body" with a clear
//! error if the request line is malformed).
//!
//! Values go through [`crate::template::Template`], so `{{uuid}}` /
//! `{{env:HOST}}` / `{{var:token}}` all work in URLs, header values, and
//! the body.
//!
//! # Directory mode
//!
//! [`parse_scenario_dir`] reads every `*.http` file in a directory and
//! returns one [`ScenarioEntry`] per file. An optional `scenarios.toml`
//! in the same directory may override the per-scenario weights:
//!
//! ```toml
//! [[scenario]]
//! file = "login.http"
//! weight = 0.1
//!
//! [[scenario]]
//! file = "browse.http"
//! weight = 0.9
//! ```
//!
//! If `scenarios.toml` is absent, all `.http` files receive equal
//! weight. Weights are normalized so they sum to `1.0`.

use std::path::{Path, PathBuf};

use http::Method;
use smallvec::SmallVec;

use crate::plan::BodySource;
use crate::template::{Template, TemplateError};
use crate::transport::{Target, TargetError};
use crate::var::VarRegistry;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by [`parse_request_file`] / [`parse_request_bytes`] /
/// [`parse_scenario_dir`].
#[derive(Debug, thiserror::Error)]
pub enum RequestFileError {
    /// File read or directory enumeration failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The input was empty (zero bytes, or only whitespace/comments).
    #[error("request file is empty")]
    Empty,
    /// No request line was found. In practice this means either the file
    /// only contained comments, or the user accidentally included only
    /// headers without the leading `METHOD PATH HTTP/1.x` line.
    #[error("{file}: missing request line (expected `METHOD PATH HTTP/1.x`)")]
    MissingRequestLine {
        /// Source identifier (file name or `"request-file"`).
        file: String,
    },
    /// The request line was malformed: not three space-separated tokens,
    /// or the method/path couldn't be parsed. The inner string carries
    /// the source name, line number, and offending text.
    #[error("invalid request line: {0}")]
    InvalidRequestLine(String),
    /// `HTTP/2` or later in the request line. Phase D only speaks HTTP/1.x.
    #[error("unsupported HTTP version: {0}")]
    UnsupportedVersion(String),
    /// A header line did not contain a `:` separator. The inner string
    /// carries the source name, line number, and offending text.
    #[error("malformed header: {0}")]
    MalformedHeader(String),
    /// The parser found headers but never reached the blank line that
    /// separates headers from the body, and there are trailing bytes
    /// that look like a body. A file that ends cleanly after its last
    /// header (with no body) parses fine.
    #[error("{file}: missing blank line between headers and body")]
    MissingBlankLine {
        /// Source identifier (file name or `"request-file"`).
        file: String,
    },
    /// No `Host:` header and the request line didn't carry an absolute
    /// URL. Without one of those we don't know where to connect.
    #[error("{file}: missing required Host header")]
    MissingHost {
        /// Source identifier (file name or `"request-file"`).
        file: String,
    },
    /// Template compilation failed inside one of the request's fields.
    #[error("template compile failed in {field}: {error}")]
    Template {
        /// Which field — `"url"`, `"header name"`, `"header value"`,
        /// `"body"` — produced the error.
        field: String,
        /// Underlying template error.
        #[source]
        error: TemplateError,
    },
    /// Target-URL parsing failed after we had assembled the authority.
    #[error("target url: {0}")]
    Target(#[from] TargetError),
    /// Something went wrong while resolving a scenario directory —
    /// nonexistent file referenced by `scenarios.toml`, malformed TOML,
    /// directory with no `.http` files, etc.
    #[error("scenario dir error: {0}")]
    Scenario(String),
}

// ---------------------------------------------------------------------------
// Output type
// ---------------------------------------------------------------------------

/// A single parsed request, with every string field already compiled into
/// a [`Template`]. The caller drops this into a [`crate::plan::RequestPlan`]
/// along with any extractors / assertions it wants to attach.
#[derive(Debug)]
pub struct ParsedRequest {
    /// Request method (`GET`, `POST`, ...).
    pub method: Method,
    /// Connection target derived from the `Host:` header (or from an
    /// absolute URL in the request line, if one was supplied).
    pub target: Target,
    /// Full URL template (scheme + authority + path/query). Expanded per
    /// iteration.
    pub url: Template,
    /// Request headers — both name and value are templates. `Host` is
    /// included in this list because some transports (HTTP/2 with SNI
    /// override, for example) want to control it explicitly.
    pub headers: SmallVec<[(Template, Template); 8]>,
    /// Optional body. `None` means empty body (many GETs).
    pub body: Option<BodySource>,
}

// ---------------------------------------------------------------------------
// String parser
// ---------------------------------------------------------------------------

/// Parse a UTF-8 `.http` file into a [`ParsedRequest`].
///
/// `source_name` is included in the template errors (e.g. `"login.http"`)
/// so the user can identify the offending file in multi-file layouts.
pub fn parse_request_file(
    source: &str,
    source_name: &str,
    vars: &mut VarRegistry,
) -> Result<ParsedRequest, RequestFileError> {
    parse_request_bytes(source.as_bytes(), source_name, vars)
}

/// Byte-level parser — preserves binary bodies that aren't valid UTF-8
/// (a hand-copied gRPC-over-HTTP/1 payload, a PNG upload, etc.). The
/// header region still must be UTF-8 because HTTP/1.1 headers are
/// ISO-8859-1 by spec but `httparse`-compatible tools, `curl`, and every
/// real-world API uses ASCII; we reject non-UTF-8 in headers with a
/// clear error rather than silently mangling them.
pub fn parse_request_bytes(
    bytes: &[u8],
    source_name: &str,
    vars: &mut VarRegistry,
) -> Result<ParsedRequest, RequestFileError> {
    if bytes.is_empty() {
        return Err(RequestFileError::Empty);
    }

    // Walk the header section byte-by-byte to find the blank line. The
    // body (if any) starts at the first byte past the blank line.
    //
    // Header region has to be valid UTF-8 (we need to trim / split on
    // colons / compare prefixes). Body is kept as raw bytes.
    let (header_end, body_start) = find_header_body_split(bytes);
    let has_blank_line_terminator = body_start > header_end;

    // Attempt UTF-8 decode of the header region.
    let header_bytes = &bytes[..header_end];
    let header_text = std::str::from_utf8(header_bytes).map_err(|_| {
        RequestFileError::InvalidRequestLine(format!(
            "{source_name}: header region is not valid UTF-8"
        ))
    })?;

    // Split into logical lines, accepting both CRLF and LF. Track the
    // 1-based line number of each so we can surface it in errors.
    // `str::lines()` is happy with either line ending; enumerate() gives
    // us the index.
    //
    // Locate the first non-empty, non-comment line — that's the request
    // line. Lines before it may be comments or blanks.
    let mut request_line: Option<(usize, &str)> = None;
    let mut header_lines: Vec<(usize, &str)> = Vec::new();
    for (i, line) in header_text.lines().enumerate() {
        let line_no = i + 1;
        let trimmed = line.trim();
        if request_line.is_none() {
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            request_line = Some((line_no, trimmed));
        } else {
            if trimmed.is_empty() {
                // Blank line inside the header region before EOF —
                // `lines()` already strips the line terminator, so an
                // "empty" logical line here is a separator *inside* the
                // header block (e.g. a stray blank between headers).
                // Per our grammar the real separator is already handled
                // in find_header_body_split, so just skip.
                continue;
            }
            if trimmed.starts_with('#') {
                continue;
            }
            header_lines.push((line_no, line));
        }
    }

    let (_req_line_no, request_line) = request_line.ok_or_else(|| {
        RequestFileError::MissingRequestLine {
            file: source_name.to_string(),
        }
    })?;

    // ---- Parse the request line -----------------------------------------
    let (method, raw_target, version) =
        parse_request_line(request_line, source_name, _req_line_no)?;
    validate_version(version, source_name, _req_line_no)?;

    // ---- Parse headers --------------------------------------------------
    // We preserve header order. Values are *not* trimmed (HTTP leaves
    // intra-value whitespace to the sender), but the single space after
    // the colon is conventionally dropped — we peel at most one leading
    // space to match curl's behaviour and no more.
    let mut raw_headers: Vec<(&str, &str)> = Vec::with_capacity(header_lines.len());
    let mut host_value: Option<&str> = None;
    for (line_no, line) in header_lines {
        let (name, value) = line.split_once(':').ok_or_else(|| {
            RequestFileError::MalformedHeader(format!(
                "{source_name}:{line_no}: {line}"
            ))
        })?;
        let name_trimmed = name.trim();
        if name_trimmed.is_empty() {
            return Err(RequestFileError::MalformedHeader(format!(
                "{source_name}:{line_no}: {line}"
            )));
        }
        // Strip the conventional single space after the colon, if any,
        // but preserve every other byte.
        let value = value.strip_prefix(' ').unwrap_or(value);
        if name_trimmed.eq_ignore_ascii_case("host") {
            host_value = Some(value);
        }
        raw_headers.push((name_trimmed, value));
    }

    // If the parser saw headers but never reached a blank-line
    // separator AND there are non-whitespace bytes remaining that look
    // like a body, that's a missing blank line. We only surface this
    // when it actually matters: header lines present + no terminator +
    // trailing content. A bare `GET /health HTTP/1.1\nHost: h\n`
    // continues to parse cleanly.
    if !has_blank_line_terminator && !raw_headers.is_empty() {
        // Find the last CRLF/LF in the input; if there are bytes after
        // the final line ending, those bytes were clearly intended as a
        // body — but without the blank-line separator we can't tell
        // them apart from a continuation of the header block. Report a
        // clear error.
        let last_newline = bytes.iter().rposition(|b| *b == b'\n');
        let trailing_start = match last_newline {
            Some(i) => i + 1,
            None => bytes.len(),
        };
        let trailing = &bytes[trailing_start..];
        if !trailing.is_empty() && !trailing.iter().all(|b| b.is_ascii_whitespace()) {
            return Err(RequestFileError::MissingBlankLine {
                file: source_name.to_string(),
            });
        }
    }

    // ---- Determine the connection target --------------------------------
    //
    // Three shapes in the wild:
    //   1. Relative path in request line, Host header present.
    //      → scheme defaults to http, authority from Host.
    //   2. Absolute URL in request line.
    //      → everything comes from the URL; Host header is advisory.
    //   3. Neither — error.
    let (target, full_url) = if looks_like_absolute_url(raw_target) {
        // Absolute form: request_line carries the full URL.
        let target = Target::parse(raw_target)?;
        (target, raw_target.to_string())
    } else {
        // Relative form: Host header must be present.
        let host = host_value
            .ok_or_else(|| RequestFileError::MissingHost {
                file: source_name.to_string(),
            })?
            .trim();
        if host.is_empty() {
            return Err(RequestFileError::MissingHost {
                file: source_name.to_string(),
            });
        }
        // We assume plain HTTP for relative paths. TLS users should either
        // use an absolute URL in the request line or wrap with `--url`
        // override (outside the scope of the v0.0.1 parser).
        let full = format!("http://{host}{raw_target}");
        let target = Target::parse(&full)?;
        (target, full)
    };

    // ---- Compile templates ----------------------------------------------
    let url_tpl =
        Template::compile(&full_url, vars).map_err(|e| RequestFileError::Template {
            field: format!("{source_name}: url"),
            error: e,
        })?;

    let mut headers: SmallVec<[(Template, Template); 8]> = SmallVec::new();
    for (name, value) in raw_headers {
        let name_tpl =
            Template::compile(name, vars).map_err(|e| RequestFileError::Template {
                field: format!("{source_name}: header name {name:?}"),
                error: e,
            })?;
        let value_tpl =
            Template::compile(value, vars).map_err(|e| RequestFileError::Template {
                field: format!("{source_name}: header value for {name:?}"),
                error: e,
            })?;
        headers.push((name_tpl, value_tpl));
    }

    // ---- Body -----------------------------------------------------------
    let body_bytes = &bytes[body_start..];
    let body = if body_bytes.is_empty() {
        None
    } else if std::str::from_utf8(body_bytes).is_ok() {
        // UTF-8 body — go through the template engine so `{{...}}`
        // substitutions fire. SAFETY: we just validated the UTF-8ness.
        let body_str = std::str::from_utf8(body_bytes).expect("validated above");
        let body_tpl =
            Template::compile(body_str, vars).map_err(|e| RequestFileError::Template {
                field: format!("{source_name}: body"),
                error: e,
            })?;
        Some(BodySource::Template(body_tpl))
    } else {
        // Non-UTF-8 body — binary payload. Pass through verbatim; no
        // template expansion possible.
        Some(BodySource::Static(bytes::Bytes::copy_from_slice(body_bytes)))
    };

    Ok(ParsedRequest {
        method,
        target,
        url: url_tpl,
        headers,
        body,
    })
}

// ---------------------------------------------------------------------------
// Helpers — request-line parsing
// ---------------------------------------------------------------------------

fn parse_request_line<'a>(
    line: &'a str,
    source: &str,
    line_no: usize,
) -> Result<(Method, &'a str, &'a str), RequestFileError> {
    let mk_err = || {
        RequestFileError::InvalidRequestLine(format!("{source}:{line_no}: {line}"))
    };

    // Expect exactly three whitespace-separated tokens.
    let mut parts = line.split_whitespace();
    let method_str = parts.next().ok_or_else(mk_err)?;
    let target = parts.next().ok_or_else(mk_err)?;
    let version = parts.next().ok_or_else(mk_err)?;
    if parts.next().is_some() {
        return Err(mk_err());
    }

    let method = method_str.parse::<Method>().map_err(|_| mk_err())?;

    Ok((method, target, version))
}

fn validate_version(
    version: &str,
    source: &str,
    line_no: usize,
) -> Result<(), RequestFileError> {
    match version {
        "HTTP/1.1" | "HTTP/1.0" => Ok(()),
        // curl sometimes emits lowercase `http/1.1` in its trace output.
        // Accept both.
        v if v.eq_ignore_ascii_case("HTTP/1.1") || v.eq_ignore_ascii_case("HTTP/1.0") => Ok(()),
        other => Err(RequestFileError::UnsupportedVersion(format!(
            "{source}:{line_no}: {other}"
        ))),
    }
}

fn looks_like_absolute_url(target: &str) -> bool {
    target.starts_with("http://")
        || target.starts_with("https://")
        || target.starts_with("ws://")
        || target.starts_with("wss://")
}

// ---------------------------------------------------------------------------
// Helpers — find the blank line that terminates the header block.
// ---------------------------------------------------------------------------
//
// Returns (header_end, body_start). `header_end` is the byte index one past
// the last header byte (exclusive); `body_start` is the first byte of the
// body (or `bytes.len()` if there is no body).
//
// The "blank line" we look for is either `\r\n\r\n` or `\n\n` (the two
// canonical forms). If we reach EOF without finding one, the whole input
// counts as headers and the body is empty.
fn find_header_body_split(bytes: &[u8]) -> (usize, usize) {
    // Look for \r\n\r\n first (curl / browsers), then \n\n (editor
    // normalization).
    if let Some(idx) = find_subseq(bytes, b"\r\n\r\n") {
        return (idx, idx + 4);
    }
    if let Some(idx) = find_subseq(bytes, b"\n\n") {
        return (idx, idx + 2);
    }
    // No blank line: the entire input is header lines; body is empty.
    // Trim a trailing newline from the header section so `lines()` does
    // not produce a spurious empty tail.
    let end = bytes.len();
    (end, end)
}

fn find_subseq(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// Directory / scenarios.toml
// ---------------------------------------------------------------------------

/// One entry in a parsed scenario directory.
#[derive(Debug, Clone)]
pub struct ScenarioEntry {
    /// Scenario name — the `.http` file stem (e.g. `login` for
    /// `login.http`).
    pub name: String,
    /// Absolute or directory-relative path to the `.http` file.
    pub file: PathBuf,
    /// Weight in `[0, 1]`. All entries in a returned vector sum to 1.0
    /// (within f32 precision).
    pub weight: f32,
}

/// Enumerate scenarios for a directory.
///
/// Behaviour:
///
/// - Collect every `*.http` file in `dir` (non-recursive).
/// - If `scenarios.toml` exists, read per-scenario weights from it. Any
///   file referenced by `scenarios.toml` must exist on disk; an entry for
///   a missing file is a [`RequestFileError::Scenario`]. `.http` files
///   not mentioned in `scenarios.toml` are included with zero weight
///   (and rebalanced to equal shares if *everything* ends up at zero).
/// - If `scenarios.toml` is absent, every `.http` file gets equal weight.
/// - Weights are normalized so the returned vector sums to 1.0.
/// - At least one `.http` file must exist or we return
///   [`RequestFileError::Scenario`].
pub fn parse_scenario_dir(dir: &Path) -> Result<Vec<ScenarioEntry>, RequestFileError> {
    if !dir.is_dir() {
        return Err(RequestFileError::Scenario(format!(
            "{} is not a directory",
            dir.display()
        )));
    }

    // Collect *.http files (non-recursive). Sort for deterministic order.
    let mut http_files: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .map(|e| e.eq_ignore_ascii_case("http"))
                .unwrap_or(false)
        {
            http_files.push(path);
        }
    }
    http_files.sort();

    if http_files.is_empty() {
        return Err(RequestFileError::Scenario(format!(
            "no .http files found in {}",
            dir.display()
        )));
    }

    // Read optional scenarios.toml.
    let toml_path = dir.join("scenarios.toml");
    let weight_map = if toml_path.exists() {
        let text = std::fs::read_to_string(&toml_path)?;
        Some(parse_scenarios_toml(&text, dir)?)
    } else {
        None
    };

    // Build entries.
    let mut entries: Vec<ScenarioEntry> = Vec::with_capacity(http_files.len());
    for path in http_files {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("scenario")
            .to_string();
        let weight = match &weight_map {
            Some(map) => map.get(&name).copied().unwrap_or(0.0),
            None => 1.0,
        };
        entries.push(ScenarioEntry {
            name,
            file: path,
            weight,
        });
    }

    // If a scenarios.toml was present, verify every listed name
    // corresponds to an existing file.
    if let Some(map) = &weight_map {
        for name in map.keys() {
            if !entries.iter().any(|e| &e.name == name) {
                return Err(RequestFileError::Scenario(format!(
                    "scenarios.toml references `{name}.http` but no such file was found in {}",
                    dir.display()
                )));
            }
        }
    }

    // Normalize weights. If every weight ended up zero (e.g. the user
    // wrote `weight = 0` everywhere, or scenarios.toml had no entries)
    // treat it as equal-split.
    let total: f32 = entries.iter().map(|e| e.weight).sum();
    if total <= 0.0 || !total.is_finite() {
        let n = entries.len() as f32;
        let share = 1.0 / n;
        for e in &mut entries {
            e.weight = share;
        }
    } else {
        for e in &mut entries {
            e.weight /= total;
        }
    }

    Ok(entries)
}

/// Parse `scenarios.toml` into a name→weight map. Missing `weight` keys
/// default to `0.0` (so the caller can decide whether to equal-split).
fn parse_scenarios_toml(
    text: &str,
    dir: &Path,
) -> Result<std::collections::HashMap<String, f32>, RequestFileError> {
    #[derive(serde::Deserialize)]
    struct TomlRoot {
        #[serde(default)]
        scenario: Vec<TomlScenario>,
    }
    #[derive(serde::Deserialize)]
    struct TomlScenario {
        file: String,
        #[serde(default)]
        weight: Option<f32>,
    }

    let root: TomlRoot = toml::from_str(text)
        .map_err(|e| RequestFileError::Scenario(format!("scenarios.toml: {e}")))?;

    let mut out = std::collections::HashMap::new();
    for sc in root.scenario {
        // Support both `file = "login.http"` and `file = "login"`.
        let path = dir.join(&sc.file);
        let name = if sc.file.ends_with(".http") {
            let stem = Path::new(&sc.file)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(&sc.file)
                .to_string();
            stem
        } else {
            sc.file.clone()
        };
        let weight = sc.weight.unwrap_or(0.0).max(0.0);
        // Advisory existence check — this catches typos early.
        let exists = path.exists()
            || dir
                .join(format!("{}.http", name))
                .exists();
        if !exists {
            return Err(RequestFileError::Scenario(format!(
                "scenarios.toml: referenced file `{}` not found in {}",
                sc.file,
                dir.display()
            )));
        }
        out.insert(name, weight);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Unit tests (integration tests in tests/request_file.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_absolute_url_recognizes_common_schemes() {
        assert!(looks_like_absolute_url("http://example.com/"));
        assert!(looks_like_absolute_url("https://api.example.com/x"));
        assert!(looks_like_absolute_url("ws://host/sock"));
        assert!(looks_like_absolute_url("wss://host/sock"));
        assert!(!looks_like_absolute_url("/api/foo"));
        assert!(!looks_like_absolute_url("api/foo"));
        assert!(!looks_like_absolute_url(""));
    }

    #[test]
    fn find_header_body_split_handles_crlf() {
        let s = b"GET / HTTP/1.1\r\nHost: h\r\n\r\nbody";
        let (end, start) = find_header_body_split(s);
        assert_eq!(&s[..end], b"GET / HTTP/1.1\r\nHost: h");
        assert_eq!(&s[start..], b"body");
    }

    #[test]
    fn find_header_body_split_handles_lf() {
        let s = b"GET / HTTP/1.1\nHost: h\n\nbody";
        let (end, start) = find_header_body_split(s);
        assert_eq!(&s[..end], b"GET / HTTP/1.1\nHost: h");
        assert_eq!(&s[start..], b"body");
    }

    #[test]
    fn find_header_body_split_handles_no_body() {
        let s = b"GET / HTTP/1.1\nHost: h\n";
        let (end, start) = find_header_body_split(s);
        assert_eq!(end, s.len());
        assert_eq!(start, s.len());
    }

    #[test]
    fn validate_version_accepts_http_1_x() {
        assert!(validate_version("HTTP/1.1", "t.http", 1).is_ok());
        assert!(validate_version("HTTP/1.0", "t.http", 1).is_ok());
    }

    #[test]
    fn validate_version_rejects_http_2() {
        let err = validate_version("HTTP/2", "t.http", 3).unwrap_err();
        match err {
            RequestFileError::UnsupportedVersion(s) => {
                assert!(s.contains("t.http"));
                assert!(s.contains(":3"));
                assert!(s.contains("HTTP/2"));
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
        assert!(matches!(
            validate_version("HTTP/3", "t.http", 1),
            Err(RequestFileError::UnsupportedVersion(_))
        ));
    }
}
