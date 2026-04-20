//! Shared helpers for raw HTTP/1.1 request building and response parsing.
//!
//! Shared by all three HTTP backends (mio_h1, cold_connect, mio_h2):
//! check_assertions, apply_extractions, capture_headers, build_raw_request,
//! ConnectionMode, ContentLength.
//!
//! Contains request building and response header parsing that is
//! runtime-agnostic — only synchronous byte manipulation, no I/O.

use std::time::Duration;

use bytes::Bytes;

use zerobench_core::plan::{Assertion, BodySource, Extract, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::ExpandCtx;
use zerobench_core::transport::Target;
use zerobench_runtime::transport::TransportError;

/// Whether the generated request carries `Connection: keep-alive` or
/// `Connection: close`. Callers pick based on their session model:
/// `mio_h1` reuses pooled connections (keep-alive); `cold_connect`
/// tears the socket down after every op (close).
///
/// If the user supplied a literal `Connection:` header via
/// `plan.headers`, their value wins and this default is skipped —
/// important for WebSocket-like upgrade scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    KeepAlive,
    Close,
}

// ---------------------------------------------------------------------------
// Request building — zero-alloc into a reusable Vec<u8>
// ---------------------------------------------------------------------------

/// Build an HTTP/1.1 request directly into `out`.
///
/// Expands URL / header / body templates via `ctx`, strips the scheme
/// and authority from absolute URLs to produce origin-form, and writes
/// the full wire message (request line + headers + body) in a single
/// pass. No `http::Request` construction, no `HeaderMap`, no
/// `Full<Bytes>` — just raw bytes.
pub fn build_raw_request(
    plan: &RequestPlan,
    ctx: &mut ScenarioContext,
    target: &Target,
    connection: ConnectionMode,
    out: &mut Vec<u8>,
) -> Result<(), TransportError> {
    // Build an ExpandCtx from individual fields so we can borrow
    // ctx.body_buf separately when needed.
    let mut ectx = ExpandCtx {
        rng: &mut ctx.rng,
        counter: &ctx.counter,
        scenario_vars: &ctx.vars,
    };

    // Method.
    out.extend_from_slice(plan.method.as_str().as_bytes());
    out.push(b' ');

    // URL — expand template, then extract origin-form (path+query).
    let url_start = out.len();
    plan.url.expand_into(out, &mut ectx);

    // If the expanded URL is absolute (starts with "http"), strip
    // scheme://authority to produce origin-form path+query.
    let url_bytes = &out[url_start..];
    if url_bytes.starts_with(b"http") {
        // Find the "://" then the next '/' after it.
        if let Some(scheme_end) = find_subsequence(url_bytes, b"://") {
            let after_scheme = scheme_end + 3;
            // Find the start of the path after the authority.
            let path_start = url_bytes[after_scheme..]
                .iter()
                .position(|&b| b == b'/' || b == b'?' || b == b'#')
                .map(|p| after_scheme + p);

            match path_start {
                Some(rel) => {
                    // Prepend '/' if authority is followed by '?' directly.
                    let needs_slash = url_bytes[rel] == b'?';
                    let path_portion: Vec<u8> = if needs_slash {
                        let mut v = vec![b'/'];
                        v.extend_from_slice(&url_bytes[rel..]);
                        v
                    } else {
                        url_bytes[rel..].to_vec()
                    };
                    // Strip fragment if present.
                    let without_frag = match path_portion.iter().position(|&b| b == b'#') {
                        Some(i) => &path_portion[..i],
                        None => &path_portion,
                    };
                    out.truncate(url_start);
                    if without_frag.is_empty() {
                        out.push(b'/');
                    } else {
                        out.extend_from_slice(without_frag);
                    }
                }
                None => {
                    // No path at all — just "http://host".
                    out.truncate(url_start);
                    out.push(b'/');
                }
            }
        }
    } else {
        // Relative URL — strip fragment if present.
        let url_slice = &out[url_start..];
        if let Some(frag_pos) = url_slice.iter().position(|&b| b == b'#') {
            let new_end = url_start + frag_pos;
            out.truncate(new_end);
        }
        if out.len() == url_start {
            out.push(b'/');
        }
    }

    // HTTP version.
    out.extend_from_slice(b" HTTP/1.1\r\n");

    // Host header.
    out.extend_from_slice(b"Host: ");
    out.extend_from_slice(target.addr().as_bytes());
    out.extend_from_slice(b"\r\n");

    // Default Connection header — skipped when the user supplied one
    // via plan.headers (static literal match, case-insensitive). A
    // dynamic header-name template isn't detectable without expansion,
    // so a `{{var:...}}: close` override will race with the default;
    // that edge case is out of scope.
    let user_has_connection = plan.headers.iter().any(|(name_tpl, _)| {
        name_tpl
            .static_literal()
            .map(|b| b.eq_ignore_ascii_case(b"connection"))
            .unwrap_or(false)
    });
    if !user_has_connection {
        match connection {
            ConnectionMode::KeepAlive => {
                out.extend_from_slice(b"Connection: keep-alive\r\n")
            }
            ConnectionMode::Close => out.extend_from_slice(b"Connection: close\r\n"),
        }
    }

    // User headers (expand templates).
    for (name_tpl, val_tpl) in &plan.headers {
        name_tpl.expand_into(out, &mut ectx);
        out.extend_from_slice(b": ");
        val_tpl.expand_into(out, &mut ectx);
        out.extend_from_slice(b"\r\n");
    }

    // Body.
    match &plan.body {
        None => {
            out.extend_from_slice(b"\r\n");
        }
        Some(BodySource::Static(body)) => {
            out.extend_from_slice(b"Content-Length: ");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(body.len()).as_bytes());
            out.extend_from_slice(b"\r\n\r\n");
            out.extend_from_slice(body);
        }
        Some(BodySource::Template(tpl)) => {
            // Expand body into ctx.body_buf, measure, then append.
            ctx.body_buf.clear();
            tpl.expand_into(&mut ctx.body_buf, &mut ectx);
            out.extend_from_slice(b"Content-Length: ");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(ctx.body_buf.len()).as_bytes());
            out.extend_from_slice(b"\r\n\r\n");
            out.extend_from_slice(&ctx.body_buf);
        }
    }

    Ok(())
}

/// Find the start index of `needle` in `haystack`.
pub fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// Response header parsing — runtime-agnostic
// ---------------------------------------------------------------------------

/// Outcome of scanning response headers for `Content-Length`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentLength {
    /// No `Content-Length` header in the response.
    Missing,
    /// A valid non-negative integer value.
    Present(usize),
    /// The header was present but its value didn't parse as a
    /// non-negative integer. RFC 9110 §8.6 requires the client to
    /// treat the message as "malformed" — the connection is
    /// unrecoverable because we can't tell where the body ends.
    Malformed,
}

/// Extract the `Content-Length` value from raw httparse headers.
///
/// Returns `Missing` when the header is absent, `Malformed` when it
/// is present but not a valid non-negative integer, and
/// `Present(n)` otherwise. Callers that previously treated "missing
/// or malformed" as `0` must now distinguish them — treating a
/// malformed CL as 0 would let the parser slide past the body and
/// mis-frame the next response on the same keep-alive connection.
pub(crate) fn find_content_length_raw(headers: &[httparse::Header<'_>]) -> ContentLength {
    for h in headers {
        if h.name.eq_ignore_ascii_case("content-length") {
            let Ok(s) = std::str::from_utf8(h.value) else {
                return ContentLength::Malformed;
            };
            return match s.trim().parse::<usize>() {
                Ok(n) => ContentLength::Present(n),
                Err(_) => ContentLength::Malformed,
            };
        }
    }
    ContentLength::Missing
}

// ---------------------------------------------------------------------------
// Post-response assertions / extractions (shared across HTTP backends)
// ---------------------------------------------------------------------------

/// Apply response assertions from the `RequestPlan`. Returns the
/// number of failed assertions. A zero result means every assertion
/// passed; non-zero is added to the scenario's `errors.assertion_failed`
/// counter by the caller.
///
/// Shared between `mio_h1`, `cold_connect`, and `mio_h2` so a DSL
/// `.expect_status(200)` is enforced regardless of which backend the
/// CLI routes to.
pub fn check_assertions(
    plan: &RequestPlan,
    status: u16,
    total_latency: Duration,
) -> u32 {
    let mut failures = 0u32;
    for check in &plan.checks {
        let pass = match check {
            Assertion::StatusEq(code) => status == *code,
            Assertion::StatusIn(codes) => codes.iter().any(|c| *c == status),
            Assertion::LatencyUnder(d) => total_latency < *d,
        };
        if !pass {
            failures += 1;
        }
    }
    failures
}

/// Apply response extractions into the `ScenarioContext`.
///
/// `extracted_headers` is `(lowercased-name, value)` tuples. The
/// caller is responsible for lowercasing names when it captures them
/// from the parsed response — this function does byte-exact matches.
///
/// Shared across HTTP backends so `.extract_header(...)` /
/// `.extract_status(...)` from the DSL works against every backend
/// that finalises a response.
pub fn apply_extractions(
    plan: &RequestPlan,
    status: u16,
    extracted_headers: &[(Vec<u8>, Vec<u8>)],
    ctx: &mut ScenarioContext,
) {
    for extract in &plan.extract {
        match extract {
            Extract::Header { name, into } => {
                let target_name = name.as_str().as_bytes();
                let found = extracted_headers
                    .iter()
                    .find(|(k, _)| k.as_slice() == target_name);
                if let Some((_, value)) = found {
                    ctx.set_var(*into, Bytes::copy_from_slice(value));
                } else {
                    ctx.clear_var(*into);
                }
            }
            Extract::StatusCode { into } => {
                // ASCII decimal — zero-alloc (5-byte stack buffer).
                let mut buf = [0u8; 5];
                let mut n = status as u32;
                if n == 0 {
                    ctx.set_var(*into, Bytes::from_static(b"0"));
                    continue;
                }
                let mut i = buf.len();
                while n > 0 {
                    i -= 1;
                    buf[i] = b'0' + (n % 10) as u8;
                    n /= 10;
                }
                ctx.set_var(*into, Bytes::copy_from_slice(&buf[i..]));
            }
        }
    }
}

/// Capture headers from an `httparse::Response` into a form suitable
/// for [`apply_extractions`]. Names are lowercased so matches stay
/// case-insensitive.
pub fn capture_headers(resp: &httparse::Response<'_, '_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(resp.headers.len());
    for h in resp.headers.iter() {
        if h.name.is_empty() {
            break;
        }
        let name_lower: Vec<u8> =
            h.name.as_bytes().iter().map(|b| b.to_ascii_lowercase()).collect();
        out.push((name_lower, h.value.to_vec()));
    }
    out
}

/// Check whether the server sent `Connection: close`.
pub(crate) fn find_connection_close(headers: &[httparse::Header<'_>]) -> bool {
    for h in headers {
        if h.name.eq_ignore_ascii_case("connection") {
            if let Ok(s) = std::str::from_utf8(h.value) {
                return s.eq_ignore_ascii_case("close");
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Unit tests — request builder and header parser
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_length_extraction() {
        let headers = [
            httparse::Header {
                name: "Content-Type",
                value: b"text/plain",
            },
            httparse::Header {
                name: "content-length",
                value: b"42",
            },
        ];
        assert_eq!(find_content_length_raw(&headers), ContentLength::Present(42));
    }

    #[test]
    fn content_length_missing_returns_missing() {
        let headers = [httparse::Header {
            name: "Content-Type",
            value: b"text/plain",
        }];
        assert_eq!(find_content_length_raw(&headers), ContentLength::Missing);
    }

    #[test]
    fn content_length_malformed_returns_malformed() {
        let headers = [httparse::Header {
            name: "Content-Length",
            value: b"not-a-number",
        }];
        assert_eq!(
            find_content_length_raw(&headers),
            ContentLength::Malformed
        );
    }

    #[test]
    fn content_length_negative_treated_as_malformed() {
        let headers = [httparse::Header {
            name: "Content-Length",
            value: b"-1",
        }];
        assert_eq!(
            find_content_length_raw(&headers),
            ContentLength::Malformed
        );
    }

    #[test]
    fn content_length_non_utf8_returns_malformed() {
        let headers = [httparse::Header {
            name: "Content-Length",
            value: &[0xff, 0xfe, 0xfd],
        }];
        assert_eq!(
            find_content_length_raw(&headers),
            ContentLength::Malformed
        );
    }

    #[test]
    fn connection_close_detected() {
        let headers = [httparse::Header {
            name: "Connection",
            value: b"close",
        }];
        assert!(find_connection_close(&headers));
    }

    #[test]
    fn connection_keepalive_not_close() {
        let headers = [httparse::Header {
            name: "Connection",
            value: b"keep-alive",
        }];
        assert!(!find_connection_close(&headers));
    }

    #[test]
    fn find_subsequence_works() {
        assert_eq!(find_subsequence(b"hello://world", b"://"), Some(5));
        assert_eq!(find_subsequence(b"nope", b"://"), None);
    }

    // ----- Request builder: Connection header semantics -----

    use smallvec::smallvec;
    use zerobench_core::plan::RequestPlan;
    use zerobench_core::rng::from_entropy;
    use zerobench_core::template::Template;
    use zerobench_core::transport::Target;
    use zerobench_core::var::VarRegistry;

    fn mk_plan(url: &str) -> RequestPlan {
        let mut vars = VarRegistry::new();
        let url_tpl = Template::compile(url, &mut vars).unwrap();
        RequestPlan::get(url_tpl)
    }

    fn mk_ctx() -> ScenarioContext {
        ScenarioContext::new(0, from_entropy())
    }

    fn mk_target() -> Target {
        Target::parse("http://127.0.0.1:8080").unwrap()
    }

    #[test]
    fn keepalive_emits_connection_keep_alive() {
        let plan = mk_plan("http://127.0.0.1:8080/x");
        let mut ctx = mk_ctx();
        let mut out = Vec::new();
        build_raw_request(&plan, &mut ctx, &mk_target(), ConnectionMode::KeepAlive, &mut out).unwrap();
        let wire = std::str::from_utf8(&out).unwrap();
        assert!(wire.contains("Connection: keep-alive\r\n"), "wire = {wire}");
        assert!(!wire.contains("Connection: close"), "wire = {wire}");
    }

    #[test]
    fn close_emits_connection_close() {
        let plan = mk_plan("http://127.0.0.1:8080/x");
        let mut ctx = mk_ctx();
        let mut out = Vec::new();
        build_raw_request(&plan, &mut ctx, &mk_target(), ConnectionMode::Close, &mut out).unwrap();
        let wire = std::str::from_utf8(&out).unwrap();
        assert!(wire.contains("Connection: close\r\n"), "wire = {wire}");
        assert!(!wire.contains("Connection: keep-alive"), "wire = {wire}");
    }

    #[test]
    fn user_connection_header_wins() {
        // User overrides with `Connection: upgrade` (e.g. WS handshake).
        // The default must NOT be emitted, regardless of mode.
        let mut vars = VarRegistry::new();
        let url_tpl = Template::compile("http://127.0.0.1:8080/ws", &mut vars).unwrap();
        let mut plan = RequestPlan::get(url_tpl);
        let name = Template::literal(bytes::Bytes::from_static(b"Connection"));
        let val = Template::literal(bytes::Bytes::from_static(b"upgrade"));
        plan.headers = smallvec![(name, val)];

        let mut ctx = mk_ctx();
        let mut out = Vec::new();
        build_raw_request(&plan, &mut ctx, &mk_target(), ConnectionMode::Close, &mut out).unwrap();
        let wire = std::str::from_utf8(&out).unwrap();
        // Exactly one Connection line, and it's the user's.
        assert_eq!(wire.matches("Connection:").count(), 1, "wire = {wire}");
        assert!(wire.contains("Connection: upgrade\r\n"), "wire = {wire}");
    }

    #[test]
    fn user_connection_header_case_insensitive() {
        let mut vars = VarRegistry::new();
        let url_tpl = Template::compile("http://127.0.0.1:8080/x", &mut vars).unwrap();
        let mut plan = RequestPlan::get(url_tpl);
        let name = Template::literal(bytes::Bytes::from_static(b"connection"));
        let val = Template::literal(bytes::Bytes::from_static(b"close"));
        plan.headers = smallvec![(name, val)];

        let mut ctx = mk_ctx();
        let mut out = Vec::new();
        build_raw_request(&plan, &mut ctx, &mk_target(), ConnectionMode::KeepAlive, &mut out).unwrap();
        let wire = std::str::from_utf8(&out).unwrap();
        assert_eq!(wire.matches("Connection:").count(), 0);
        assert_eq!(wire.matches("connection:").count(), 1, "wire = {wire}");
    }
}
