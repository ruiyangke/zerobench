//! Shared helpers for raw HTTP/1.1 request building and response parsing.
//!
//! Contains request building and response header parsing that is
//! runtime-agnostic — only synchronous byte manipulation, no I/O.

use zerobench_core::plan::{BodySource, RequestPlan};
use zerobench_core::scenario_context::ScenarioContext;
use zerobench_core::template::ExpandCtx;
use zerobench_core::transport::{Target, TransportError};

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

    // Connection: keep-alive.
    out.extend_from_slice(b"Connection: keep-alive\r\n");

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

/// Extract the `Content-Length` value from raw httparse headers.
pub(crate) fn find_content_length_raw(headers: &[httparse::Header<'_>]) -> usize {
    for h in headers {
        if h.name.eq_ignore_ascii_case("content-length") {
            if let Ok(s) = std::str::from_utf8(h.value) {
                return s.trim().parse().unwrap_or(0);
            }
        }
    }
    0
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
        assert_eq!(find_content_length_raw(&headers), 42);
    }

    #[test]
    fn content_length_missing_returns_zero() {
        let headers = [httparse::Header {
            name: "Content-Type",
            value: b"text/plain",
        }];
        assert_eq!(find_content_length_raw(&headers), 0);
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
}
