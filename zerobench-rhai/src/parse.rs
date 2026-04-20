//! ARCH STATUS: MOVE → zerobench-dsl::parse (crate rename)
//!
//! Self-contained string-parser helpers. Moves with the crate rename;
//! no rewrite.
//! See ARCH-REVIEW §4, Q-A.
//!
//! ----------------------------------------------------------------------
//!
//! Duration / rate spec parsers used by the Rhai DSL.
//!
//! Kept separate from [`crate::builders`] so unit tests don't need to touch
//! Rhai types. These are intentionally copies of the CLI's parsers — the
//! alternative is to depend on `zerobench` itself, which creates a circular
//! workspace graph. They're ~40 lines total and the duplication is easier
//! to maintain than a new internal crate.

use std::time::Duration;

/// Parse a duration spec (`10s`, `1m`, `2m30s`, `500ms`). Returns `None`
/// on malformed input.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total = Duration::ZERO;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if start == i {
            return None;
        }
        let n: u64 = std::str::from_utf8(&bytes[start..i]).ok()?.parse().ok()?;

        let u_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() {
            i += 1;
        }
        let unit = std::str::from_utf8(&bytes[u_start..i]).ok()?.trim();
        let d = match unit {
            "ns" => Duration::from_nanos(n),
            "us" | "µs" => Duration::from_micros(n),
            "ms" => Duration::from_millis(n),
            "s" | "" => Duration::from_secs(n),
            "m" => Duration::from_secs(n * 60),
            "h" => Duration::from_secs(n * 3600),
            _ => return None,
        };
        total += d;
    }
    Some(total)
}

/// Parse a rate spec (`100`, `10k`, `1.5k`, `2M`, `10k/s`). Returns the
/// rate in requests per second.
///
/// `/s` suffix is treated as optional — the CLI accepts both `10k` and
/// `10k/s` for the same thing. Anything else after `/` is an error.
pub fn parse_rate_with_unit(s: &str) -> Result<f64, String> {
    let s = s.trim();
    let (n_part, unit_part) = match s.rsplit_once('/') {
        Some((l, r)) => (l.trim(), Some(r.trim())),
        None => (s, None),
    };
    if let Some(u) = unit_part {
        // "10k/s" — accept "s", reject anything else.
        if u != "s" && !u.is_empty() {
            return Err(format!(
                "unsupported rate unit {u:?} (only /s is supported)"
            ));
        }
    }
    if n_part.is_empty() {
        return Err("empty rate".into());
    }
    let (num, mult) = match n_part.chars().last() {
        Some('k') | Some('K') => (&n_part[..n_part.len() - 1], 1_000.0f64),
        Some('m') | Some('M') => (&n_part[..n_part.len() - 1], 1_000_000.0f64),
        _ => (n_part, 1.0),
    };
    let n: f64 = num
        .parse()
        .map_err(|e| format!("invalid rate {s:?}: {e}"))?;
    if !n.is_finite() || n <= 0.0 {
        return Err(format!("rate must be a positive finite number, got {n}"));
    }
    Ok(n * mult)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_forms() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration("2m30s"), Some(Duration::from_secs(150)));
        assert!(parse_duration("abc").is_none());
        assert!(parse_duration("").is_none());
    }

    #[test]
    fn rate_forms() {
        assert_eq!(parse_rate_with_unit("100"), Ok(100.0));
        assert_eq!(parse_rate_with_unit("10k"), Ok(10_000.0));
        assert_eq!(parse_rate_with_unit("10k/s"), Ok(10_000.0));
        assert_eq!(parse_rate_with_unit("1.5k/s"), Ok(1_500.0));
        assert_eq!(parse_rate_with_unit("2M"), Ok(2_000_000.0));
        assert!(parse_rate_with_unit("10k/min").is_err());
        assert!(parse_rate_with_unit("-5").is_err());
        assert!(parse_rate_with_unit("0").is_err());
    }
}
