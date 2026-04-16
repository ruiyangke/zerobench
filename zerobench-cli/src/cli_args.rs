//! Command-line argument parsing via `clap` derive.
//!
//! Parsing produces a [`CliArgs`]; the conversion to a runnable
//! [`Plan`] / [`Target`] / [`TransportOpts`] triple lives in
//! [`crate::plan_from_cli`] so this file stays a thin, declarative
//! schema.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Parser, ValueEnum};

/// `zerobench` — fast, correct HTTP benchmarking tool.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "zerobench",
    version,
    about = "Fast, correct HTTP benchmarking — open-loop, HDR-precise, io_uring-native"
)]
pub struct CliArgs {
    /// Target URL.
    pub url: String,

    /// Max concurrent connections / worker tasks (closed-loop) or
    /// pool ceiling (open-loop).
    #[arg(short = 'c', long = "connections", default_value_t = 50)]
    pub connections: usize,

    /// Measurement duration (e.g. `10s`, `1m`, `2m30s`).
    #[arg(short = 'd', long = "duration", default_value = "30s",
          value_parser = parse_duration_flag)]
    pub duration: Duration,

    /// Run in closed-loop saturate mode — N workers, each looping
    /// request-then-response. Mutually exclusive with `--rate`.
    #[arg(long = "saturate", action = ArgAction::SetTrue, conflicts_with = "rate")]
    pub saturate: bool,

    /// Open-loop target rate in req/s (e.g. `100`, `10k`, `1.5k`).
    /// Mutually exclusive with `--saturate`. Task 10 wires this flag.
    #[arg(short = 'r', long = "rate", value_parser = parse_rate_flag)]
    pub rate: Option<f64>,

    /// HTTP method.
    #[arg(short = 'X', long = "method", default_value = "GET")]
    pub method: String,

    /// Add a header. Repeat to add multiple. Form: `Name: Value`.
    /// Value may contain `{{...}}` templates.
    #[arg(short = 'H', long = "header", value_parser = parse_header_flag)]
    pub headers: Vec<(String, String)>,

    /// Inline request body. May contain `{{...}}`. Implies `--method POST`
    /// unless `-X` was given.
    #[arg(long = "body", conflicts_with = "body_file")]
    pub body: Option<String>,

    /// Request body from a file path. Loaded once at startup.
    #[arg(long = "body-file")]
    pub body_file: Option<PathBuf>,

    /// Assertion: exact status code.
    #[arg(long = "expect-status")]
    pub expect_status: Option<u16>,

    /// Assertion: status code is in this comma-separated list (e.g.
    /// `200,201,204`).
    #[arg(long = "expect-status-in", value_parser = parse_status_list,
          num_args = 1)]
    pub expect_status_in: Option<StatusList>,

    /// Color output preference.
    #[arg(long = "color", value_enum, default_value_t = CliColor::Auto)]
    pub color: CliColor,

    /// Output format.
    #[arg(long = "format", value_enum, default_value_t = CliFormat::Terminal)]
    pub format: CliFormat,

    /// TCP+TLS connect timeout.
    #[arg(long = "connect-timeout", default_value = "5s",
          value_parser = parse_duration_flag)]
    pub connect_timeout: Duration,

    /// Per-request deadline.
    #[arg(long = "timeout", default_value = "30s",
          value_parser = parse_duration_flag)]
    pub request_timeout: Duration,

    /// Accept invalid TLS certificates (self-signed, expired, hostname
    /// mismatch). No-op for http:// targets.
    #[arg(short = 'k', long = "insecure", action = ArgAction::SetTrue)]
    pub insecure: bool,
}

// ---------------------------------------------------------------------------
// Value enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliColor {
    Always,
    Auto,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliFormat {
    Terminal,
    Json,
}

// ---------------------------------------------------------------------------
// Value parsers
// ---------------------------------------------------------------------------

/// Parse a duration spec (`10s`, `1m`, `2m30s`, `500ms`).
pub fn parse_duration_flag(s: &str) -> Result<Duration, String> {
    parse_duration(s).ok_or_else(|| {
        format!(
            "invalid duration {s:?}; expected forms like `10s`, `1m`, `2m30s`, `500ms`"
        )
    })
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total = Duration::ZERO;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Numeric run (integer only; we don't support fractional seconds).
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if start == i {
            return None;
        }
        let n: u64 = std::str::from_utf8(&bytes[start..i]).ok()?.parse().ok()?;

        // Unit run.
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

/// Parse a rate spec (`100`, `10k`, `1.5k`, `2M`).
pub fn parse_rate_flag(s: &str) -> Result<f64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty rate".into());
    }
    let (num, mult) = match s.chars().last().unwrap() {
        'k' | 'K' => (&s[..s.len() - 1], 1_000.0f64),
        'm' | 'M' => (&s[..s.len() - 1], 1_000_000.0f64),
        _ => (s, 1.0),
    };
    let n: f64 = num
        .parse()
        .map_err(|e| format!("invalid rate {s:?}: {e}"))?;
    if !n.is_finite() || n <= 0.0 {
        return Err(format!("rate must be a positive finite number, got {n}"));
    }
    Ok(n * mult)
}

/// Parse `Name: Value` into a tuple.
pub fn parse_header_flag(s: &str) -> Result<(String, String), String> {
    let (name, value) = s
        .split_once(':')
        .ok_or_else(|| format!("expected 'Name: Value', got {s:?}"))?;
    Ok((name.trim().to_string(), value.trim().to_string()))
}

/// Newtype around `Vec<u16>` so clap treats the parsed output as a
/// single value (rather than a list of repeated occurrences).
#[derive(Debug, Clone)]
pub struct StatusList(pub Vec<u16>);

/// Parse a comma-separated list of status codes.
pub fn parse_status_list(s: &str) -> Result<StatusList, String> {
    let out: Result<Vec<u16>, _> = s
        .split(',')
        .map(|t| t.trim().parse::<u16>())
        .collect();
    out.map(StatusList)
        .map_err(|e| format!("invalid status list {s:?}: {e}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_duration_forms() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(
            parse_duration("2m30s"),
            Some(Duration::from_secs(150))
        );
        assert_eq!(
            parse_duration("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        // Bare number defaults to seconds.
        assert_eq!(parse_duration("30"), Some(Duration::from_secs(30)));
        // Invalid.
        assert!(parse_duration("").is_none());
        assert!(parse_duration("10x").is_none());
        assert!(parse_duration("abc").is_none());
    }

    #[test]
    fn parse_rate_suffixes() {
        assert_eq!(parse_rate_flag("100"), Ok(100.0));
        assert_eq!(parse_rate_flag("10k"), Ok(10_000.0));
        assert_eq!(parse_rate_flag("1.5k"), Ok(1_500.0));
        assert_eq!(parse_rate_flag("2M"), Ok(2_000_000.0));
        assert!(parse_rate_flag("").is_err());
        assert!(parse_rate_flag("-5").is_err());
        assert!(parse_rate_flag("0").is_err());
    }

    #[test]
    fn parse_header_form() {
        assert_eq!(
            parse_header_flag("Authorization: Bearer x"),
            Ok(("Authorization".into(), "Bearer x".into()))
        );
        assert!(parse_header_flag("no colon").is_err());
    }

    #[test]
    fn parse_status_list_form() {
        let ok = parse_status_list("200,201,204").unwrap();
        assert_eq!(ok.0, vec![200, 201, 204]);
        assert!(parse_status_list("200,xyz").is_err());
    }

    #[test]
    fn args_minimal_url_and_saturate() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-c",
            "10",
            "-d",
            "5s",
            "http://127.0.0.1:1234/",
        ])
        .unwrap();
        assert!(args.saturate);
        assert_eq!(args.connections, 10);
        assert_eq!(args.duration, Duration::from_secs(5));
        assert_eq!(args.url, "http://127.0.0.1:1234/");
    }

    #[test]
    fn args_rate_and_saturate_are_mutually_exclusive() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-r",
            "100",
            "http://127.0.0.1:1/",
        ])
        .unwrap_err();
        // clap's "conflicts_with" error kind.
        assert!(format!("{err}").contains("cannot be used"));
    }

    #[test]
    fn args_repeatable_header() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-H",
            "X-A: 1",
            "-H",
            "X-B: 2",
            "http://127.0.0.1:1/",
        ])
        .unwrap();
        assert_eq!(args.headers.len(), 2);
    }

    #[test]
    fn args_default_duration_is_30s() {
        let args = CliArgs::try_parse_from(["zerobench", "http://127.0.0.1:1/"])
            .unwrap();
        assert_eq!(args.duration, Duration::from_secs(30));
        assert_eq!(args.connections, 50);
    }
}
