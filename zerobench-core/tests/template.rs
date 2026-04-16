//! Integration tests for Template compile + expand.

use std::cell::Cell;

use bytes::Bytes;
use zerobench_core::{rng, ExpandCtx, Template, TemplateError, VarRegistry};

// Helper: build a fresh ExpandCtx backed by local state. Returns the
// harness by value so callers can destructure it.
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

    fn with_var(mut self, slot: usize, value: Bytes) -> Self {
        if self.vars.len() <= slot {
            self.vars.resize(slot + 1, None);
        }
        self.vars[slot] = Some(value);
        self
    }

    fn ctx(&mut self) -> ExpandCtx<'_> {
        ExpandCtx {
            rng: &mut self.rng,
            counter: &self.counter,
            scenario_vars: &self.vars,
        }
    }
}

fn expand(t: &Template, h: &mut Harness) -> Vec<u8> {
    let mut out = Vec::new();
    let mut ctx = h.ctx();
    t.expand_into(&mut out, &mut ctx);
    out
}

#[test]
fn literal_only_copies_bytes_identically() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("plain text, no vars", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b"plain text, no vars");
    assert_eq!(vars.len(), 0);
}

#[test]
fn env_is_baked_at_compile_time() {
    // CARGO is set while running `cargo test`.
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{env:CARGO}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    let s = std::str::from_utf8(&out).unwrap();
    assert!(!s.is_empty(), "CARGO env var should be set by test harness");
    // The resolution at compile time should make this template a
    // single Literal part.
    assert_eq!(t.part_count(), 1);
}

#[test]
fn env_default_used_when_unset() {
    // An extremely unlikely env name.
    let mut vars = VarRegistry::new();
    let t = Template::compile(
        "{{env:ZEROBENCH_DEFINITELY_UNSET_42:fallback}}",
        &mut vars,
    )
    .unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b"fallback");
}

#[test]
fn env_missing_without_default_errors() {
    let mut vars = VarRegistry::new();
    let err =
        Template::compile("{{env:ZEROBENCH_MISSING_NO_DEFAULT_42}}", &mut vars).unwrap_err();
    match err {
        TemplateError::UnknownVariable(s) => assert!(s.starts_with("env:")),
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn uuid_produces_36_char_hyphenated() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{uuid}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    let s = std::str::from_utf8(&out).unwrap();
    assert_eq!(s.len(), 36);
    // Hyphen layout: 8-4-4-4-12.
    assert_eq!(s.as_bytes()[8], b'-');
    assert_eq!(s.as_bytes()[13], b'-');
    assert_eq!(s.as_bytes()[18], b'-');
    assert_eq!(s.as_bytes()[23], b'-');
}

#[test]
fn uuid4_produces_36_char_hyphenated() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{uuid4}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    let s = std::str::from_utf8(&out).unwrap();
    assert_eq!(s.len(), 36);
    // Version nibble should be 4 for uuid4.
    assert_eq!(s.as_bytes()[14], b'4');
}

#[test]
fn rand_int_in_bounds_across_many_expansions() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{rand_int:1:10}}", &mut vars).unwrap();
    let mut h = Harness::new();
    for _ in 0..1000 {
        let mut out = Vec::new();
        let mut ctx = h.ctx();
        t.expand_into(&mut out, &mut ctx);
        let s = std::str::from_utf8(&out).unwrap();
        let n: i64 = s.parse().unwrap();
        assert!(
            (1..=10).contains(&n),
            "rand_int out of bounds: {n} (from {s:?})"
        );
    }
}

#[test]
fn rand_int_inclusive_endpoints() {
    // Over many iterations, we should see both 1 and 2 (inclusive).
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{rand_int:1:2}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let mut saw_one = false;
    let mut saw_two = false;
    for _ in 0..200 {
        let mut out = Vec::new();
        let mut ctx = h.ctx();
        t.expand_into(&mut out, &mut ctx);
        match &out[..] {
            b"1" => saw_one = true,
            b"2" => saw_two = true,
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert!(saw_one && saw_two);
}

#[test]
fn rand_int_min_equals_max() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{rand_int:7:7}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b"7");
}

#[test]
fn rand_int_rejects_min_greater_than_max() {
    let mut vars = VarRegistry::new();
    let err = Template::compile("{{rand_int:5:3}}", &mut vars).unwrap_err();
    match err {
        TemplateError::InvalidRandInt(_) => {}
        other => panic!("expected InvalidRandInt, got {other:?}"),
    }
}

#[test]
fn rand_int_rejects_malformed_args() {
    let mut vars = VarRegistry::new();
    let err = Template::compile("{{rand_int:abc}}", &mut vars).unwrap_err();
    assert!(matches!(err, TemplateError::InvalidRandInt(_)));

    let err = Template::compile("{{rand_int:1}}", &mut vars).unwrap_err();
    assert!(matches!(err, TemplateError::InvalidRandInt(_)));
}

#[test]
fn rand_hex_produces_expected_length() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{rand_hex:16}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out.len(), 32);
    assert!(out.iter().all(|b| b.is_ascii_hexdigit()));
}

#[test]
fn rand_str_produces_alphanumeric() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{rand_str:24}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out.len(), 24);
    assert!(out.iter().all(|b| b.is_ascii_alphanumeric()));
}

#[test]
fn now_ms_and_ns_monotonic_non_decreasing() {
    let mut vars = VarRegistry::new();
    let t_ms = Template::compile("{{now_ms}}", &mut vars).unwrap();
    let t_ns = Template::compile("{{now_ns}}", &mut vars).unwrap();
    let mut h = Harness::new();

    let mut prev_ms: u128 = 0;
    let mut prev_ns: u128 = 0;
    for _ in 0..50 {
        let a = expand(&t_ms, &mut h);
        let b = expand(&t_ns, &mut h);
        let ms: u128 = std::str::from_utf8(&a).unwrap().parse().unwrap();
        let ns: u128 = std::str::from_utf8(&b).unwrap().parse().unwrap();
        assert!(ms >= prev_ms, "now_ms went backwards: {prev_ms} -> {ms}");
        assert!(ns >= prev_ns, "now_ns went backwards: {prev_ns} -> {ns}");
        prev_ms = ms;
        prev_ns = ns;
    }
}

#[test]
fn now_iso_formats_correctly() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{now_iso}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    let s = std::str::from_utf8(&out).unwrap();
    assert_eq!(s.len(), 24);
    assert_eq!(s.as_bytes()[4], b'-');
    assert_eq!(s.as_bytes()[7], b'-');
    assert_eq!(s.as_bytes()[10], b'T');
    assert_eq!(s.as_bytes()[13], b':');
    assert_eq!(s.as_bytes()[16], b':');
    assert_eq!(s.as_bytes()[19], b'.');
    assert_eq!(s.as_bytes()[23], b'Z');
    // Year should start with 20.
    assert!(s.starts_with("20"));
}

#[test]
fn counter_increments_per_expansion() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{counter}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let a = expand(&t, &mut h);
    let b = expand(&t, &mut h);
    let c = expand(&t, &mut h);
    assert_eq!(a, b"0");
    assert_eq!(b, b"1");
    assert_eq!(c, b"2");
}

#[test]
fn counter_global_increments_across_templates() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{counter_global}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let a = expand(&t, &mut h);
    let b = expand(&t, &mut h);
    let an: u64 = std::str::from_utf8(&a).unwrap().parse().unwrap();
    let bn: u64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
    assert_eq!(bn, an + 1);
}

#[test]
fn var_ref_unset_expands_to_empty() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("[{{var:token}}]", &mut vars).unwrap();
    assert_eq!(vars.len(), 1);
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b"[]");
}

#[test]
fn var_ref_populated_expands_to_value() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("[{{var:token}}]", &mut vars).unwrap();
    let slot = vars.allocate("token").unwrap(); // same name → same slot
    let mut h = Harness::new().with_var(slot.0 as usize, Bytes::from_static(b"abc"));
    let out = expand(&t, &mut h);
    assert_eq!(out, b"[abc]");
}

#[test]
fn unclosed_variable_errors() {
    let mut vars = VarRegistry::new();
    let err = Template::compile("start {{unclosed rest", &mut vars).unwrap_err();
    match err {
        TemplateError::Unclosed(pos) => assert_eq!(pos, 6),
        other => panic!("expected Unclosed, got {other:?}"),
    }
}

#[test]
fn unknown_variable_errors() {
    let mut vars = VarRegistry::new();
    let err = Template::compile("{{bogus}}", &mut vars).unwrap_err();
    match err {
        TemplateError::UnknownVariable(ref s) if s == "bogus" => {}
        other => panic!("expected UnknownVariable(\"bogus\"), got {other:?}"),
    }
}

#[test]
fn line_file_is_not_yet_supported() {
    let mut vars = VarRegistry::new();
    let err = Template::compile("{{line:./fixtures/ids.txt}}", &mut vars).unwrap_err();
    match err {
        TemplateError::NotYetSupported(_) => {}
        other => panic!("expected NotYetSupported, got {other:?}"),
    }
}

#[test]
fn escape_doubles_braces() {
    let mut vars = VarRegistry::new();
    // `{{{{uuid}}}}` → literal `{{uuid}}`; no UUID expansion.
    let t = Template::compile("{{{{uuid}}}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b"{{uuid}}");
}

#[test]
fn mixed_template_interleaves_literal_and_vars() {
    let mut vars = VarRegistry::new();
    let t =
        Template::compile("a={{rand_int:5:5}},b={{counter}},c={{counter}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b"a=5,b=0,c=1");
}

#[test]
fn expand_reuses_output_buffer_across_calls() {
    // Verifies the "no per-call alloc beyond the output buffer" property:
    // after the first expansion grows the buffer to its target size, the
    // same buffer's capacity should be reused on subsequent calls.
    let mut vars = VarRegistry::new();
    let t = Template::compile("value={{rand_int:100:999}}", &mut vars).unwrap();
    let mut h = Harness::new();
    let mut out = Vec::with_capacity(64);
    let mut ctx = h.ctx();
    t.expand_into(&mut out, &mut ctx);
    let cap_after_first = out.capacity();
    out.clear();
    t.expand_into(&mut out, &mut ctx);
    assert_eq!(out.capacity(), cap_after_first);
}

#[test]
fn compile_is_idempotent_for_var_registry() {
    // Two references to the same var name share a slot.
    let mut vars = VarRegistry::new();
    let _ = Template::compile("{{var:x}}", &mut vars).unwrap();
    let _ = Template::compile("{{var:x}}", &mut vars).unwrap();
    let _ = Template::compile("{{var:y}}", &mut vars).unwrap();
    assert_eq!(vars.len(), 2);
}

#[test]
fn whitespace_around_expression_is_trimmed() {
    // `{{ uuid }}` should behave like `{{uuid}}` — 36-char hyphenated UUID.
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{ uuid }}", &mut vars).unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    let s = std::str::from_utf8(&out).unwrap();
    assert_eq!(s.len(), 36);
}

#[test]
fn whitespace_around_var_name_is_trimmed() {
    // `{{var: token}}` must register the var as `token`, not ` token`.
    let mut vars = VarRegistry::new();
    let _ = Template::compile("{{var: token}}", &mut vars).unwrap();
    assert_eq!(vars.len(), 1);
    // Allocating the plain name should share the slot, proving the
    // whitespace was stripped at compile time.
    let slot = vars.allocate("token").unwrap();
    assert_eq!(slot.0, 0);
    assert_eq!(vars.len(), 1);
}

#[test]
fn whitespace_around_rand_int_bounds_is_trimmed() {
    let mut vars = VarRegistry::new();
    let t = Template::compile("{{rand_int: 1 : 10}}", &mut vars).unwrap();
    let mut h = Harness::new();
    for _ in 0..100 {
        let mut out = Vec::new();
        let mut ctx = h.ctx();
        t.expand_into(&mut out, &mut ctx);
        let n: i64 = std::str::from_utf8(&out).unwrap().parse().unwrap();
        assert!((1..=10).contains(&n), "out of bounds: {n}");
    }
}

#[test]
fn env_default_preserves_internal_and_trailing_whitespace() {
    // The DEFAULT of `{{env:NAME:DEFAULT}}` is preserved verbatim — a
    // user legitimately may want spaces inside a default value.
    let mut vars = VarRegistry::new();
    let t = Template::compile(
        "{{env:ZEROBENCH_SPACE_DEFAULT_42: with spaces }}",
        &mut vars,
    )
    .unwrap();
    let mut h = Harness::new();
    let out = expand(&t, &mut h);
    assert_eq!(out, b" with spaces ");
}
