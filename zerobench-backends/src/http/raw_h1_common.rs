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
            ConnectionMode::KeepAlive => out.extend_from_slice(b"Connection: keep-alive\r\n"),
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
    haystack.windows(needle.len()).position(|w| w == needle)
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

/// Return `true` iff the response carries `Transfer-Encoding: chunked`.
///
/// RFC 9112 §6.1 allows comma-separated TE values; we scan each
/// comma-split token case-insensitively. `chunked` must be the
/// final coding in the list (§7.1), and we treat any presence of
/// the token as "chunked" because a non-final `chunked` is a
/// server bug we can't recover from either way.
pub(crate) fn find_transfer_encoding_chunked(headers: &[httparse::Header<'_>]) -> bool {
    for h in headers {
        if h.name.eq_ignore_ascii_case("transfer-encoding") {
            let Ok(s) = std::str::from_utf8(h.value) else {
                continue;
            };
            for token in s.split(',') {
                if token.trim().eq_ignore_ascii_case("chunked") {
                    return true;
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Chunked transfer-encoding decoder
// ---------------------------------------------------------------------------

/// Incremental parser for `Transfer-Encoding: chunked` response
/// bodies. Fed a `&[u8]` that grows as new reads arrive; tracks
/// its own position so each `advance` call resumes where the
/// previous one stopped.
///
/// Wire format (RFC 9112 §7.1):
/// ```text
///   <hex-size>[;ext]\r\n
///   <size bytes>\r\n
///   ...
///   0\r\n
///   [<trailer-field>\r\n]*
///   \r\n
/// ```
///
/// The decoder does not retain body bytes — it only frames the
/// response so callers know when the body is fully received and
/// the connection can be reused for the next keep-alive request.
#[derive(Debug, Clone)]
pub(crate) struct ChunkedDecoder {
    state: ChunkState,
    pos: usize,
}

#[derive(Debug, Clone)]
enum ChunkState {
    /// Looking for the next chunk's size line.
    Size,
    /// Reading `remaining` bytes of chunk data, then a trailing `\r\n`.
    Data { remaining: usize },
    /// Last chunk (size=0) was consumed; now scanning the trailer
    /// block for the terminating blank line.
    Trailers,
}

/// Outcome of a single `ChunkedDecoder::advance` call.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChunkProgress {
    /// More body bytes are needed before any further framing
    /// progress is possible.
    NeedMore,
    /// The full chunked body has been consumed. `consumed` is
    /// the total byte count within the body slice — add this
    /// to the caller's body start offset to skip past the
    /// chunked payload.
    Done { consumed: usize },
    /// The stream is malformed — chunk size not hex, data
    /// chunk missing trailing CRLF, etc. The caller must drop
    /// the connection; resynchronising is impossible.
    Err(&'static str),
}

impl Default for ChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedDecoder {
    /// Fresh decoder, positioned at the start of a body.
    pub(crate) fn new() -> Self {
        Self {
            state: ChunkState::Size,
            pos: 0,
        }
    }

    /// Attempt to consume as much of `body` as possible. `body`
    /// must be the post-headers portion of the response buffer
    /// (not the full wire buffer).
    ///
    /// On `NeedMore` the caller should read more bytes into the
    /// buffer and call `advance` again with the (grown) slice;
    /// internal state is preserved across calls. On `Done` the
    /// caller can mark the connection keep-alive.
    pub(crate) fn advance(&mut self, body: &[u8]) -> ChunkProgress {
        loop {
            match self.state {
                ChunkState::Size => {
                    let Some(rem) = body.get(self.pos..) else {
                        return ChunkProgress::NeedMore;
                    };
                    let Some(line_end) = memchr::memmem::find(rem, b"\r\n") else {
                        return ChunkProgress::NeedMore;
                    };
                    let line = &rem[..line_end];
                    // Drop optional chunk extensions after ';'.
                    let size_bytes = match line.iter().position(|&b| b == b';') {
                        Some(i) => &line[..i],
                        None => line,
                    };
                    let size = match parse_hex_size(size_bytes) {
                        Some(n) => n,
                        None => return ChunkProgress::Err("invalid chunk size"),
                    };
                    self.pos += line_end + 2;
                    if size == 0 {
                        self.state = ChunkState::Trailers;
                    } else {
                        self.state = ChunkState::Data { remaining: size };
                    }
                }
                ChunkState::Data { remaining } => {
                    let need_end = self.pos.saturating_add(remaining).saturating_add(2);
                    if body.len() < need_end {
                        return ChunkProgress::NeedMore;
                    }
                    if &body[self.pos + remaining..self.pos + remaining + 2] != b"\r\n" {
                        return ChunkProgress::Err("chunk missing trailing CRLF");
                    }
                    self.pos = need_end;
                    self.state = ChunkState::Size;
                }
                ChunkState::Trailers => {
                    let Some(rem) = body.get(self.pos..) else {
                        return ChunkProgress::NeedMore;
                    };
                    if rem.len() < 2 {
                        return ChunkProgress::NeedMore;
                    }
                    // Empty trailer block — fast path.
                    if rem.starts_with(b"\r\n") {
                        self.pos += 2;
                        return ChunkProgress::Done { consumed: self.pos };
                    }
                    // Non-empty trailers: scan for \r\n\r\n terminator.
                    match memchr::memmem::find(rem, b"\r\n\r\n") {
                        Some(p) => {
                            self.pos += p + 4;
                            return ChunkProgress::Done { consumed: self.pos };
                        }
                        None => return ChunkProgress::NeedMore,
                    }
                }
            }
        }
    }
}

/// Parse `bytes` as an ASCII hex integer, ignoring leading and
/// trailing whitespace. Returns `None` on empty input, non-hex
/// digits, or overflow beyond `usize::MAX`.
fn parse_hex_size(bytes: &[u8]) -> Option<usize> {
    // Trim ASCII whitespace in-place (no alloc).
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace())?;
    let end = bytes.iter().rposition(|b| !b.is_ascii_whitespace())? + 1;
    let slice = &bytes[start..end];
    if slice.is_empty() {
        return None;
    }
    let s = std::str::from_utf8(slice).ok()?;
    usize::from_str_radix(s, 16).ok()
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
pub fn check_assertions(plan: &RequestPlan, status: u16, total_latency: Duration) -> u32 {
    let mut failures = 0u32;
    for check in &plan.checks {
        let pass = match check {
            Assertion::StatusEq(code) => status == *code,
            Assertion::StatusIn(codes) => codes.contains(&status),
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
        let name_lower: Vec<u8> = h
            .name
            .as_bytes()
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .collect();
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
        assert_eq!(
            find_content_length_raw(&headers),
            ContentLength::Present(42)
        );
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
        assert_eq!(find_content_length_raw(&headers), ContentLength::Malformed);
    }

    #[test]
    fn content_length_negative_treated_as_malformed() {
        let headers = [httparse::Header {
            name: "Content-Length",
            value: b"-1",
        }];
        assert_eq!(find_content_length_raw(&headers), ContentLength::Malformed);
    }

    #[test]
    fn content_length_non_utf8_returns_malformed() {
        let headers = [httparse::Header {
            name: "Content-Length",
            value: &[0xff, 0xfe, 0xfd],
        }];
        assert_eq!(find_content_length_raw(&headers), ContentLength::Malformed);
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
        build_raw_request(
            &plan,
            &mut ctx,
            &mk_target(),
            ConnectionMode::KeepAlive,
            &mut out,
        )
        .unwrap();
        let wire = std::str::from_utf8(&out).unwrap();
        assert!(wire.contains("Connection: keep-alive\r\n"), "wire = {wire}");
        assert!(!wire.contains("Connection: close"), "wire = {wire}");
    }

    #[test]
    fn close_emits_connection_close() {
        let plan = mk_plan("http://127.0.0.1:8080/x");
        let mut ctx = mk_ctx();
        let mut out = Vec::new();
        build_raw_request(
            &plan,
            &mut ctx,
            &mk_target(),
            ConnectionMode::Close,
            &mut out,
        )
        .unwrap();
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
        build_raw_request(
            &plan,
            &mut ctx,
            &mk_target(),
            ConnectionMode::Close,
            &mut out,
        )
        .unwrap();
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
        build_raw_request(
            &plan,
            &mut ctx,
            &mk_target(),
            ConnectionMode::KeepAlive,
            &mut out,
        )
        .unwrap();
        let wire = std::str::from_utf8(&out).unwrap();
        assert_eq!(wire.matches("Connection:").count(), 0);
        assert_eq!(wire.matches("connection:").count(), 1, "wire = {wire}");
    }

    // ---------- Transfer-Encoding: chunked detection ----------

    #[test]
    fn te_chunked_detected() {
        let headers = [httparse::Header {
            name: "Transfer-Encoding",
            value: b"chunked",
        }];
        assert!(find_transfer_encoding_chunked(&headers));
    }

    #[test]
    fn te_chunked_case_insensitive() {
        let headers = [httparse::Header {
            name: "transfer-encoding",
            value: b"Chunked",
        }];
        assert!(find_transfer_encoding_chunked(&headers));
    }

    #[test]
    fn te_chunked_in_list_detected() {
        let headers = [httparse::Header {
            name: "Transfer-Encoding",
            value: b"gzip, chunked",
        }];
        assert!(find_transfer_encoding_chunked(&headers));
    }

    #[test]
    fn te_missing_returns_false() {
        let headers = [httparse::Header {
            name: "Content-Length",
            value: b"5",
        }];
        assert!(!find_transfer_encoding_chunked(&headers));
    }

    #[test]
    fn te_not_chunked_returns_false() {
        let headers = [httparse::Header {
            name: "Transfer-Encoding",
            value: b"gzip",
        }];
        assert!(!find_transfer_encoding_chunked(&headers));
    }

    // ---------- ChunkedDecoder ----------

    fn advance_all(dec: &mut ChunkedDecoder, body: &[u8]) -> ChunkProgress {
        dec.advance(body)
    }

    #[test]
    fn chunked_single_chunk_empty_trailer() {
        // "hello" (5 bytes) then terminating 0-chunk + empty trailer.
        let body = b"5\r\nhello\r\n0\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(
            advance_all(&mut dec, body),
            ChunkProgress::Done {
                consumed: body.len(),
            }
        );
    }

    #[test]
    fn chunked_multi_chunk() {
        let body = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(
            dec.advance(body),
            ChunkProgress::Done {
                consumed: body.len()
            }
        );
    }

    #[test]
    fn chunked_with_extensions_ignored() {
        let body = b"5;ext=foo\r\nhello\r\n0\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(
            dec.advance(body),
            ChunkProgress::Done {
                consumed: body.len()
            }
        );
    }

    #[test]
    fn chunked_with_trailers() {
        let body = b"3\r\nfoo\r\n0\r\nX-Trailer: yes\r\nX-Other: 42\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(
            dec.advance(body),
            ChunkProgress::Done {
                consumed: body.len()
            }
        );
    }

    #[test]
    fn chunked_large_hex_size() {
        // 0x100 = 256 bytes.
        let mut body: Vec<u8> = b"100\r\n".to_vec();
        body.extend(std::iter::repeat(b'A').take(256));
        body.extend_from_slice(b"\r\n0\r\n\r\n");
        let mut dec = ChunkedDecoder::new();
        assert_eq!(
            dec.advance(&body),
            ChunkProgress::Done {
                consumed: body.len()
            }
        );
    }

    #[test]
    fn chunked_incremental_delivery_resumes() {
        let full = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        // Feed up to just the size of the first chunk.
        assert_eq!(dec.advance(&full[..3]), ChunkProgress::NeedMore);
        // Feed up to mid-first-chunk.
        assert_eq!(dec.advance(&full[..7]), ChunkProgress::NeedMore);
        // Feed up to after-first-chunk terminator, start of second size.
        assert_eq!(dec.advance(&full[..13]), ChunkProgress::NeedMore);
        // Feed to end.
        assert_eq!(
            dec.advance(full),
            ChunkProgress::Done {
                consumed: full.len()
            }
        );
    }

    #[test]
    fn chunked_invalid_hex_size_errors() {
        let body = b"xyz\r\ndata\r\n0\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(dec.advance(body), ChunkProgress::Err("invalid chunk size"));
    }

    #[test]
    fn chunked_missing_trailing_crlf_errors() {
        // 5-byte data chunk followed by wrong terminator.
        let body = b"5\r\nhelloXX0\r\n\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(
            dec.advance(body),
            ChunkProgress::Err("chunk missing trailing CRLF")
        );
    }

    #[test]
    fn chunked_empty_size_line_need_more() {
        // Only got a partial size line — decoder must wait.
        let body = b"5";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(dec.advance(body), ChunkProgress::NeedMore);
    }

    #[test]
    fn chunked_zero_only_need_more_trailer() {
        // `0\r\n` arrived but trailer `\r\n` not yet.
        let body = b"0\r\n";
        let mut dec = ChunkedDecoder::new();
        assert_eq!(dec.advance(body), ChunkProgress::NeedMore);
    }
}
