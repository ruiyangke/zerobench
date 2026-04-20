//! Tiny fixed-purpose JSON scanner — pulls a single numeric field
//! out of a payload without pulling in `serde_json`.
//!
//! Used by the fanout backends to implement
//! [`FanoutMode::Timestamp`](zerobench_core::plan::FanoutMode::Timestamp): the
//! server embeds `"emit_ns":<integer>` in each broadcast payload, and
//! the subscriber scans for it at reception time so the post-run RTT
//! pass has server-local timing to correlate against.
//!
//! This is **not** a JSON parser — it's a byte-level lookup for one
//! specific shape (`"<field>":<digits>`). A full JSON dep would be
//! ~300 KB of compiled code for this single call, which isn't worth
//! it. The caveats documented on [`find_json_u64_field`] (first match
//! wins, no nested-object disambiguation) are acceptable for the
//! benchmark scenarios this powers.

/// Locate `"<field>":N` in `payload` and parse `N` as a non-negative
/// integer. Returns `None` if the field is absent, its value isn't an
/// integer, the integer overflows `u64`, or the shape doesn't match
/// `"field":digits` after optional whitespace.
///
/// Caveats by construction:
/// - If the same quoted literal appears as a *value* for a different
///   field, or in a nested object with the same field name, the
///   first match wins. Server-side broadcast formats typically put
///   `emit_ns` once at the top level.
/// - Negative numbers and floats return `None`. `FanoutMode::Timestamp`
///   specifies nanoseconds since an epoch, which is always u64.
pub fn find_json_u64_field(payload: &[u8], field: &[u8]) -> Option<u64> {
    let mut needle: Vec<u8> = Vec::with_capacity(field.len() + 2);
    needle.push(b'"');
    needle.extend_from_slice(field);
    needle.push(b'"');
    let start = memchr::memmem::find(payload, &needle)?;
    let after = start + needle.len();
    let rest = payload.get(after..)?;
    let mut i = 0;
    while i < rest.len() && matches!(rest[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    if i >= rest.len() || rest[i] != b':' {
        return None;
    }
    i += 1;
    while i < rest.len() && matches!(rest[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    let mut n: u64 = 0;
    let mut any = false;
    while i < rest.len() && rest[i].is_ascii_digit() {
        n = n.checked_mul(10)?.checked_add((rest[i] - b'0') as u64)?;
        any = true;
        i += 1;
    }
    if any {
        Some(n)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_field() {
        let body = br#"{"emit_ns":1234567890,"payload":"hi"}"#;
        assert_eq!(find_json_u64_field(body, b"emit_ns"), Some(1234567890));
    }

    #[test]
    fn whitespace_between_colon_and_value() {
        let body = br#"{"ts": 42}"#;
        assert_eq!(find_json_u64_field(body, b"ts"), Some(42));
    }

    #[test]
    fn missing_field_returns_none() {
        let body = br#"{"other":5}"#;
        assert_eq!(find_json_u64_field(body, b"ts"), None);
    }

    #[test]
    fn non_numeric_value_returns_none() {
        let body = br#"{"ts":"hello"}"#;
        assert_eq!(find_json_u64_field(body, b"ts"), None);
    }

    #[test]
    fn negative_value_returns_none() {
        let body = br#"{"ts":-5}"#;
        assert_eq!(find_json_u64_field(body, b"ts"), None);
    }

    #[test]
    fn first_match_wins_for_nested_same_name() {
        let body = br#"{"ts":1,"inner":{"ts":2}}"#;
        assert_eq!(find_json_u64_field(body, b"ts"), Some(1));
    }

    #[test]
    fn empty_payload() {
        assert_eq!(find_json_u64_field(b"", b"ts"), None);
    }
}
