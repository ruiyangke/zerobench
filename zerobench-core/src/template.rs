//! Compiled string templates with `{{...}}` substitution.
//!
//! # Compilation
//!
//! [`Template::compile`] scans the source once, producing a [`Vec<Part>`].
//! Each `{{...}}` expression is looked up in the v0.0.1 vocabulary (see
//! module docs in [`crate`]); literal bytes between expressions collapse
//! into [`Part::Literal`].
//!
//! # Escape rules
//!
//! - `{{{{` emits a literal `{{` (the inner `{{` is not an expression
//!   opener).
//! - `}}}}` emits a literal `}}`.
//! - An unclosed `{{` returns [`TemplateError::Unclosed`].
//!
//! # Expansion
//!
//! [`Template::expand_into`] writes directly into a caller-supplied
//! `Vec<u8>`, performing zero allocations beyond the output buffer's own
//! growth. Parts that produce small strings (counters, numbers, UUIDs)
//! format on-stack.
//!
//! # Thread safety
//!
//! [`Template`] is [`Send`] + [`Sync`]. Counters come in two flavors:
//!
//! - `{{counter}}` is per-worker: each worker holds its own
//!   `Cell<u64>`, passed via [`ExpandCtx::counter`].
//! - `{{counter_global}}` is process-wide: parts hold a `&'static
//!   AtomicU64`, shared across all workers.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::rng::BenchRng;
use crate::var::{VarError, VarRegistry, VarSlot};

// ---------------------------------------------------------------------------
// Global counter — one per process, pointed at by every CounterGlobal part.
// ---------------------------------------------------------------------------

static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by [`Template::compile`].
#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    /// A `{{` was not followed by a matching `}}`.
    #[error("unclosed template variable at position {0}")]
    Unclosed(usize),

    /// The expression inside `{{...}}` is not a known variable.
    #[error("unknown variable: {0}")]
    UnknownVariable(String),

    /// `rand_int:MIN:MAX` failed to parse or had invalid bounds.
    #[error("invalid rand_int args (need MIN:MAX with MIN <= MAX): {0}")]
    InvalidRandInt(String),

    /// `rand_hex:BYTES` or `rand_str:LEN` failed to parse.
    #[error("invalid rand args: {0}")]
    InvalidRandArgs(String),

    /// A v0.0.1-deferred feature (e.g. `{{line:FILE}}`) was used.
    #[error("not yet supported: {0}")]
    NotYetSupported(String),

    /// More than 256 distinct named variables were declared in this plan.
    #[error(transparent)]
    TooManyVars(#[from] VarError),

    /// A parameterized expression was given with an empty operand — e.g.
    /// `{{var:}}` (no name) or `{{env::default}}` (empty name). The
    /// contained `&'static str` is the prefix ("var", "env", ...).
    #[error("empty operand for `{{{{{0}:...}}}}` — name is required")]
    EmptyOperand(&'static str),

    /// `{{env:NAME}}` was used with a NAME that isn't in the environment
    /// and no default was supplied. Previously this surfaced as a
    /// misleading `UnknownVariable("env:NAME")` — the prefix was spelled
    /// correctly; only the environment lookup failed.
    #[error("env variable not set and no default supplied: {0}")]
    MissingEnv(String),
}

// ---------------------------------------------------------------------------
// Template + Part
// ---------------------------------------------------------------------------

/// A compiled template — a sequence of literals and substitution parts.
///
/// Produced by [`Template::compile`]; consumed on the hot path by
/// [`Template::expand_into`]. Cheap to clone (all owned bytes are
/// reference-counted via [`bytes::Bytes`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Template {
    parts: Vec<Part>,
    estimated_size: usize,
}

impl Template {
    /// Returns an empty template (expands to no bytes).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct a template consisting of a single literal.
    pub fn literal(bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        let size = bytes.len();
        Self {
            parts: vec![Part::Literal(bytes)],
            estimated_size: size,
        }
    }

    /// Compile `src` into a template. Variable names referenced by
    /// `{{var:NAME}}` are allocated in `vars`.
    pub fn compile(src: &str, vars: &mut VarRegistry) -> Result<Self, TemplateError> {
        compile_template(src, vars)
    }

    /// Expand the template into `out`.
    ///
    /// The output buffer is appended to; callers reuse the same `Vec<u8>`
    /// across requests to avoid per-call allocation.
    pub fn expand_into(&self, out: &mut Vec<u8>, ctx: &mut ExpandCtx) {
        out.reserve(self.estimated_size);
        for p in &self.parts {
            expand_part(p, out, ctx);
        }
    }

    /// Size hint in bytes. Dynamic parts contribute a reasonable upper
    /// bound so callers can pre-reserve scratch buffers.
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    /// Number of parts (literals + substitutions) the template compiled to.
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }
}

/// A single substitution unit.
///
/// Parts with configuration (ranges, lengths, slot indices) carry it
/// inline so expansion needs no lookups beyond the part itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Part {
    /// Raw bytes emitted verbatim.
    Literal(Bytes),

    /// `{{uuid}}` — UUIDv7 (sortable by time).
    Uuid,

    /// `{{uuid4}}` — UUIDv4 (fully random).
    Uuid4,

    /// `{{now_ms}}` — milliseconds since the Unix epoch.
    NowMs,

    /// `{{now_ns}}` — nanoseconds since the Unix epoch.
    NowNs,

    /// `{{now_iso}}` — RFC 3339 / ISO 8601 UTC timestamp.
    NowIso,

    /// `{{counter}}` — per-worker monotonic counter. The counter storage
    /// lives in [`ExpandCtx::counter`].
    Counter,

    /// `{{counter_global}}` — process-wide monotonic counter. Points at
    /// [`GLOBAL_COUNTER`]. Serialized as a tagged unit variant; on
    /// deserialization we rebind to the same static.
    CounterGlobal,

    /// `{{rand_int:MIN:MAX}}` — uniform integer in `[min, max]`.
    RandInt { min: i64, max: i64 },

    /// `{{rand_hex:BYTES}}` — BYTES random bytes rendered as 2·BYTES hex
    /// chars (lowercase).
    RandHex { bytes: usize },

    /// `{{rand_str:LEN}}` — LEN alphanumeric ASCII chars.
    RandStr { len: usize },

    /// `{{var:NAME}}` — response-extracted variable. Missing slots expand
    /// to empty bytes.
    VarRef(VarSlot),
}

// ---------------------------------------------------------------------------
// ExpandCtx
// ---------------------------------------------------------------------------

/// Per-expansion context — RNG, per-worker counter, and extracted vars.
///
/// Borrowed from the worker task for the duration of a single request's
/// template expansions (URL, headers, body).
pub struct ExpandCtx<'a> {
    /// Mutable RNG reference (each worker owns one).
    pub rng: &'a mut BenchRng,
    /// Per-worker counter — `{{counter}}` reads and increments.
    pub counter: &'a Cell<u64>,
    /// Extracted-variable backing store, indexed by [`VarSlot`].
    /// Slots that have not been populated (either because the iteration
    /// hasn't reached the extract, or because the extract found nothing)
    /// hold `None` and expand to empty bytes.
    pub scenario_vars: &'a [Option<Bytes>],
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

fn compile_template(src: &str, vars: &mut VarRegistry) -> Result<Template, TemplateError> {
    let bytes = src.as_bytes();
    let mut parts: Vec<Part> = Vec::new();
    let mut literal: Vec<u8> = Vec::new();
    let mut estimated = 0usize;

    // Flush the current literal buffer into a Literal part, if non-empty.
    // (fn-as-closure rather than a helper because it mutates local state.)
    let flush = |literal: &mut Vec<u8>, parts: &mut Vec<Part>, estimated: &mut usize| {
        if !literal.is_empty() {
            *estimated += literal.len();
            parts.push(Part::Literal(Bytes::from(std::mem::take(literal))));
        }
    };

    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];

        // Opening-brace handling: check for `{{` (opener) and `{{{{` (escape).
        if b == b'{' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            // Escape? `{{{{` → literal `{{`.
            if i + 3 < bytes.len() && bytes[i + 2] == b'{' && bytes[i + 3] == b'{' {
                literal.push(b'{');
                literal.push(b'{');
                i += 4;
                continue;
            }

            // Expression — find matching `}}`.
            let expr_start = i + 2;
            let Some(end_rel) = find_double_close(&bytes[expr_start..]) else {
                return Err(TemplateError::Unclosed(i));
            };
            let expr_end = expr_start + end_rel;
            let expr = std::str::from_utf8(&bytes[expr_start..expr_end]).map_err(|_| {
                TemplateError::UnknownVariable(
                    String::from_utf8_lossy(&bytes[expr_start..expr_end]).into_owned(),
                )
            })?;

            flush(&mut literal, &mut parts, &mut estimated);
            // Note: do NOT pre-trim `expr` here. `parse_expression` trims
            // each argument component selectively — the DEFAULT in
            // `{{env:NAME:DEFAULT}}` must preserve trailing whitespace.
            let (part, size_hint) = parse_expression(expr, vars)?;
            estimated += size_hint;
            parts.push(part);
            i = expr_end + 2; // past `}}`
            continue;
        }

        // Closing-brace escape `}}}}` → literal `}}`. Lone `}}` is treated
        // as literal text.
        if b == b'}'
            && i + 3 < bytes.len()
            && bytes[i + 1] == b'}'
            && bytes[i + 2] == b'}'
            && bytes[i + 3] == b'}'
        {
            literal.push(b'}');
            literal.push(b'}');
            i += 4;
            continue;
        }

        literal.push(b);
        i += 1;
    }

    flush(&mut literal, &mut parts, &mut estimated);
    // After flush, if there's residual template with `{{` but no closer, we
    // would have already errored above; nothing to check here.

    // Fold adjacent literals (can happen after escape + literal sequences).
    let parts = coalesce_literals(parts);
    let estimated_size = parts
        .iter()
        .map(|p| match p {
            Part::Literal(b) => b.len(),
            _ => estimate_dynamic(p),
        })
        .sum();
    Ok(Template {
        parts,
        estimated_size,
    })
}

/// Scan for the first `}}` in `haystack`. Returns the byte index of the
/// first `}` of the pair, or `None` if no closer exists.
fn find_double_close(haystack: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + 1 < haystack.len() {
        if haystack[i] == b'}' && haystack[i + 1] == b'}' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Combine adjacent `Part::Literal`s into one. Emitted because escape
/// sequences (`{{{{`, `}}}}`) each push two `Literal`s naïvely.
fn coalesce_literals(parts: Vec<Part>) -> Vec<Part> {
    let mut out: Vec<Part> = Vec::with_capacity(parts.len());
    for p in parts {
        match (out.last_mut(), p) {
            (Some(Part::Literal(prev)), Part::Literal(next)) => {
                let mut v = Vec::with_capacity(prev.len() + next.len());
                v.extend_from_slice(prev);
                v.extend_from_slice(&next);
                *prev = Bytes::from(v);
            }
            (_, p) => out.push(p),
        }
    }
    out
}

/// Best-effort upper-bound size for a dynamic part. Used only to
/// pre-reserve output buffers; correctness does not depend on accuracy.
fn estimate_dynamic(p: &Part) -> usize {
    match p {
        Part::Literal(_) => 0, // handled separately
        Part::Uuid | Part::Uuid4 => 36,
        Part::NowMs => 13,
        Part::NowNs => 19,
        Part::NowIso => 24,
        Part::Counter | Part::CounterGlobal => 20,
        Part::RandInt { .. } => 20,
        Part::RandHex { bytes } => bytes * 2,
        Part::RandStr { len } => *len,
        Part::VarRef(_) => 32,
    }
}

/// Parse a single `expr` (contents between `{{` and `}}`) into a `Part`.
///
/// Returns the part and a size hint to feed the estimator.
///
/// Whitespace rule: we trim space around every name-like component
/// (prefix, NAME, MIN/MAX, etc.) so `{{var: token}}` and
/// `{{rand_int: 1 : 2}}` behave like their no-space forms. The sole
/// exception is the DEFAULT for `{{env:NAME:DEFAULT}}`, which is
/// preserved verbatim — a user might legitimately want spaces in a
/// default value.
fn parse_expression(
    expr: &str,
    vars: &mut VarRegistry,
) -> Result<(Part, usize), TemplateError> {
    // Simple (no-arg) variables — match against the trimmed expression so
    // `{{ uuid }}` is equivalent to `{{uuid}}`.
    match expr.trim() {
        "uuid" => return Ok((Part::Uuid, 36)),
        "uuid4" => return Ok((Part::Uuid4, 36)),
        "now_ms" => return Ok((Part::NowMs, 13)),
        "now_ns" => return Ok((Part::NowNs, 19)),
        "now_iso" => return Ok((Part::NowIso, 24)),
        "counter" => return Ok((Part::Counter, 20)),
        "counter_global" => return Ok((Part::CounterGlobal, 20)),
        _ => {}
    }

    // Parameterized variables: prefix:args.
    if let Some((head, tail)) = expr.split_once(':') {
        let head = head.trim();
        match head {
            "env" => {
                // `env:NAME` or `env:NAME:DEFAULT`. NAME is the segment
                // before the first ':'; DEFAULT is the rest (may contain
                // more colons).
                let (name, default) = match tail.split_once(':') {
                    Some((n, d)) => (n.trim(), Some(d)),
                    None => (tail.trim(), None),
                };
                if name.is_empty() {
                    return Err(TemplateError::EmptyOperand("env"));
                }
                let value = match std::env::var(name) {
                    Ok(v) => v,
                    Err(_) => default
                        .map(|d| d.to_string())
                        .ok_or_else(|| TemplateError::MissingEnv(name.to_string()))?,
                };
                let size = value.len();
                return Ok((Part::Literal(Bytes::from(value)), size));
            }
            "rand_int" => {
                let (a, b) = tail.split_once(':').ok_or_else(|| {
                    TemplateError::InvalidRandInt(tail.to_string())
                })?;
                let min: i64 = a
                    .trim()
                    .parse()
                    .map_err(|_| TemplateError::InvalidRandInt(tail.to_string()))?;
                let max: i64 = b
                    .trim()
                    .parse()
                    .map_err(|_| TemplateError::InvalidRandInt(tail.to_string()))?;
                if min > max {
                    return Err(TemplateError::InvalidRandInt(tail.to_string()));
                }
                return Ok((Part::RandInt { min, max }, 20));
            }
            "rand_hex" => {
                let bytes: usize = tail
                    .trim()
                    .parse()
                    .map_err(|_| TemplateError::InvalidRandArgs(format!("rand_hex:{tail}")))?;
                return Ok((Part::RandHex { bytes }, bytes * 2));
            }
            "rand_str" => {
                let len: usize = tail
                    .trim()
                    .parse()
                    .map_err(|_| TemplateError::InvalidRandArgs(format!("rand_str:{tail}")))?;
                return Ok((Part::RandStr { len }, len));
            }
            "var" => {
                let name = tail.trim();
                if name.is_empty() {
                    return Err(TemplateError::EmptyOperand("var"));
                }
                let slot = vars.allocate(name)?;
                return Ok((Part::VarRef(slot), 32));
            }
            "line" => {
                return Err(TemplateError::NotYetSupported(format!("line:{tail}")));
            }
            _ => {}
        }
    }

    Err(TemplateError::UnknownVariable(expr.trim().to_string()))
}

// ---------------------------------------------------------------------------
// Expander
// ---------------------------------------------------------------------------

fn expand_part(part: &Part, out: &mut Vec<u8>, ctx: &mut ExpandCtx<'_>) {
    match part {
        Part::Literal(b) => out.extend_from_slice(b),

        Part::Uuid => {
            let mut buf = [0u8; uuid::fmt::Hyphenated::LENGTH];
            let u = Uuid::now_v7();
            let s = u.hyphenated().encode_lower(&mut buf);
            out.extend_from_slice(s.as_bytes());
        }
        Part::Uuid4 => {
            let mut buf = [0u8; uuid::fmt::Hyphenated::LENGTH];
            let u = Uuid::new_v4();
            let s = u.hyphenated().encode_lower(&mut buf);
            out.extend_from_slice(s.as_bytes());
        }

        Part::NowMs => {
            let ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            write_u128(out, ms);
        }
        Part::NowNs => {
            let ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            write_u128(out, ns);
        }
        Part::NowIso => {
            write_iso8601_utc(out);
        }

        Part::Counter => {
            let n = ctx.counter.get();
            ctx.counter.set(n.wrapping_add(1));
            write_u64(out, n);
        }
        Part::CounterGlobal => {
            let n = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
            write_u64(out, n);
        }

        Part::RandInt { min, max } => {
            // `*max` is inclusive. We validated min <= max at compile.
            let n: i64 = if min == max {
                *min
            } else {
                ctx.rng.gen_range(*min..=*max)
            };
            write_i64(out, n);
        }
        Part::RandHex { bytes } => {
            write_rand_hex(out, *bytes, ctx.rng);
        }
        Part::RandStr { len } => {
            write_rand_str(out, *len, ctx.rng);
        }

        Part::VarRef(slot) => {
            if let Some(Some(val)) = ctx.scenario_vars.get(slot.0 as usize) {
                out.extend_from_slice(val);
            }
            // Unset or out-of-range slot → empty expansion.
        }
    }
}

// ---------------------------------------------------------------------------
// Small helpers — format into the output buffer with no heap allocation.
// ---------------------------------------------------------------------------

fn write_u128(out: &mut Vec<u8>, n: u128) {
    // u128 has at most 39 decimal digits.
    let mut buf = [0u8; 40];
    let mut i = buf.len();
    if n == 0 {
        out.push(b'0');
        return;
    }
    let mut n = n;
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    out.extend_from_slice(&buf[i..]);
}

fn write_u64(out: &mut Vec<u8>, n: u64) {
    write_u128(out, n as u128);
}

fn write_i64(out: &mut Vec<u8>, n: i64) {
    if n < 0 {
        out.push(b'-');
        // i64::MIN's abs doesn't fit in i64; cast via u64 first.
        write_u128(out, (n as i128).unsigned_abs());
    } else {
        write_u128(out, n as u128);
    }
}

/// ISO 8601 / RFC 3339 in UTC, `YYYY-MM-DDTHH:MM:SS.sssZ` — 24 chars.
///
/// We implement manual calendar conversion rather than pulling in `chrono`
/// or `time`: the math is trivial and the binary savings are real.
fn write_iso8601_utc(out: &mut Vec<u8>) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = now.as_secs() as i64;
    let millis = now.subsec_millis();

    // Split into date + time-of-day.
    let days_since_epoch = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400) as u32;

    let (year, month, day) = civil_from_days(days_since_epoch);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    write_u_padded(out, year as u64, 4);
    out.push(b'-');
    write_u_padded(out, month as u64, 2);
    out.push(b'-');
    write_u_padded(out, day as u64, 2);
    out.push(b'T');
    write_u_padded(out, hour as u64, 2);
    out.push(b':');
    write_u_padded(out, minute as u64, 2);
    out.push(b':');
    write_u_padded(out, second as u64, 2);
    out.push(b'.');
    write_u_padded(out, millis as u64, 3);
    out.push(b'Z');
}

fn write_u_padded(out: &mut Vec<u8>, mut n: u64, width: usize) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    if n == 0 {
        for _ in 0..width {
            out.push(b'0');
        }
        return;
    }
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    let digits = buf.len() - i;
    if digits < width {
        for _ in 0..(width - digits) {
            out.push(b'0');
        }
    }
    out.extend_from_slice(&buf[i..]);
}

/// Convert "days since 1970-01-01" to (year, month, day) using Howard
/// Hinnant's civil-from-days algorithm. Month is 1..=12, day 1..=31.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = (y + i64::from(m <= 2)) as i32;
    (year, m as u32, d as u32)
}

fn write_rand_hex(out: &mut Vec<u8>, bytes: usize, rng: &mut BenchRng) {
    // Generate 8 bytes at a time via rng.next_u64 to keep calls cheap.
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut remaining = bytes;
    while remaining > 0 {
        let chunk = remaining.min(8);
        let mut n: u64 = rng.gen();
        // Emit `chunk` bytes worth of hex (2 chars per byte).
        for _ in 0..chunk {
            let b = (n & 0xFF) as u8;
            n >>= 8;
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 0x0F) as usize]);
        }
        remaining -= chunk;
    }
}

fn write_rand_str(out: &mut Vec<u8>, len: usize, rng: &mut BenchRng) {
    const ALPHABET: &[u8; 62] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for _ in 0..len {
        let idx = rng.gen_range(0..ALPHABET.len());
        out.push(ALPHABET[idx]);
    }
}

// ---------------------------------------------------------------------------
// Tests (unit; integration tests live in `tests/template.rs`)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_double_close_works() {
        assert_eq!(find_double_close(b"abc}}xyz"), Some(3));
        assert_eq!(find_double_close(b"no closer"), None);
        assert_eq!(find_double_close(b"}}start"), Some(0));
    }

    #[test]
    fn write_i64_handles_min_value() {
        let mut buf = Vec::new();
        write_i64(&mut buf, i64::MIN);
        assert_eq!(buf, b"-9223372036854775808");
    }

    #[test]
    fn civil_date_for_epoch() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(31), (1970, 2, 1));
        assert_eq!(civil_from_days(365), (1971, 1, 1));
    }
}
