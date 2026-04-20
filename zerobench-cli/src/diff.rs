//! `zerobench diff BASELINE CURRENT` — compare two bench JSON outputs.
//!
//! TODO: consolidate with src/verbs/diff.rs (pre-existing duplication,
//! pre-rewrite) — both subcommands read archive JSON and render deltas,
//! but this one is the `diff` verb (regression-gate presets) and the
//! other is the `compare` verb (statistical comparison).
//!
//! Reads both JSON blobs (the `schema_version: 1` shape emitted by
//! `print_json`), computes a per-metric delta, and writes a human or
//! machine-readable report. Exit code 1 when any metric breaches its
//! configured regression threshold.
//!
//! # Regression rules (defaults, configurable)
//!
//! - p99 increase above 5%          → regression
//! - p99.9 increase above 5%        → regression
//! - RPS decrease above 2%          → regression
//! - Any error-category count up    → regression
//!
//! `max` latency is tracked but deliberately *not* a regression signal —
//! single-point max is dominated by outliers. It's displayed with a
//! neutral marker so the user can eyeball it.

use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;

use yansi::{Condition, Paint};

use crate::cli_args::{CliColor, DiffArgs, DiffFormat};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Execute the diff subcommand. Called from `main::run` when the user
/// passes `zerobench diff ...`.
pub fn run(args: &DiffArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let baseline_text = std::fs::read_to_string(&args.baseline)
        .map_err(|e| format!("reading baseline {}: {e}", args.baseline.display()))?;
    let current_text = std::fs::read_to_string(&args.current)
        .map_err(|e| format!("reading current {}: {e}", args.current.display()))?;

    let baseline: serde_json::Value = serde_json::from_str(&baseline_text)
        .map_err(|e| format!("parsing baseline JSON: {e}"))?;
    let current: serde_json::Value = serde_json::from_str(&current_text)
        .map_err(|e| format!("parsing current JSON: {e}"))?;

    // Schema version sanity check. We only emit version 1; anything else
    // is a user (or upgrade) error.
    let base_ver = baseline.get("schema_version").and_then(|v| v.as_u64());
    let curr_ver = current.get("schema_version").and_then(|v| v.as_u64());
    if base_ver != Some(1) || curr_ver != Some(1) {
        return Err(format!(
            "unsupported schema_version (baseline={base_ver:?}, current={curr_ver:?})"
        )
        .into());
    }

    let deltas = compute_deltas(&baseline, &current, args);
    let regression = deltas.iter().any(|d| d.status == Status::Regression);

    match args.format {
        DiffFormat::Terminal => {
            let stdout = io::stdout();
            let is_tty = stdout.is_terminal();
            let mut out = stdout.lock();
            write_terminal(&mut out, args, &deltas, regression, is_tty)?;
        }
        DiffFormat::Json => {
            let stdout = io::stdout();
            let mut out = stdout.lock();
            write_json(&mut out, args, &deltas, regression)?;
        }
    }

    if regression {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

// ---------------------------------------------------------------------------
// Delta computation
// ---------------------------------------------------------------------------

/// One row of the diff report.
#[derive(Debug, Clone)]
struct Delta {
    /// Human-friendly metric name (e.g. `"p99"`, `"errors (5xx)"`).
    metric: String,
    /// The baseline value, stringified for the table.
    baseline_display: String,
    /// The current value, stringified.
    current_display: String,
    /// Percent-delta (`(current - baseline) / baseline * 100`). `None`
    /// when the baseline is 0 — a transition from 0-to-N is expressed
    /// as an absolute delta in `display_delta` instead.
    delta_pct: Option<f64>,
    /// Rendered delta for the table: `"+3%"`, `"-0.5%"`, `"+3"`.
    display_delta: String,
    /// Regression / improvement / neutral.
    status: Status,
    /// Structured field name for the JSON output.
    json_key: String,
    /// Raw numeric values for the JSON output.
    json_baseline: f64,
    json_current: f64,
}

/// Three-way classification for per-metric status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    /// Metric moved in the bad direction beyond the configured
    /// threshold. Exit code 1.
    Regression,
    /// Metric unchanged or moved in the good direction.
    Ok,
    /// Metric moved but we don't treat it as a regression signal (e.g.
    /// `max` latency). Displayed with a neutral marker.
    Neutral,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Regression => "regression",
            Status::Ok => "ok",
            Status::Neutral => "neutral",
        }
    }
}

fn compute_deltas(
    baseline: &serde_json::Value,
    current: &serde_json::Value,
    args: &DiffArgs,
) -> Vec<Delta> {
    let mut out = Vec::new();

    // --- RPS ---
    let b_rps = get_f64(baseline, "requests_per_sec");
    let c_rps = get_f64(current, "requests_per_sec");
    out.push(latency_or_rps_delta(
        "rps",
        "rps",
        b_rps,
        c_rps,
        Direction::HigherIsBetter,
        args.threshold_rps,
    ));

    // --- Latency percentiles ---
    let b_p50 = get_f64_path(baseline, &["latency_ns", "p50"]);
    let c_p50 = get_f64_path(current, &["latency_ns", "p50"]);
    out.push(latency_or_rps_delta(
        "p50",
        "p50_ns",
        b_p50,
        c_p50,
        Direction::LowerIsBetter,
        args.threshold_p99, // reuse p99 threshold for info; not primary regression signal
    ));
    // p50/p90 don't directly count as regressions by default (we only
    // flag p99/p99.9). Override to Neutral-or-Ok here.
    demote_latency_regression(out.last_mut().unwrap());

    let b_p90 = get_f64_path(baseline, &["latency_ns", "p90"]);
    let c_p90 = get_f64_path(current, &["latency_ns", "p90"]);
    out.push(latency_or_rps_delta(
        "p90",
        "p90_ns",
        b_p90,
        c_p90,
        Direction::LowerIsBetter,
        args.threshold_p99,
    ));
    demote_latency_regression(out.last_mut().unwrap());

    let b_p99 = get_f64_path(baseline, &["latency_ns", "p99"]);
    let c_p99 = get_f64_path(current, &["latency_ns", "p99"]);
    out.push(latency_or_rps_delta(
        "p99",
        "p99_ns",
        b_p99,
        c_p99,
        Direction::LowerIsBetter,
        args.threshold_p99,
    ));

    let b_p999 = get_f64_path(baseline, &["latency_ns", "p99_9"]);
    let c_p999 = get_f64_path(current, &["latency_ns", "p99_9"]);
    out.push(latency_or_rps_delta(
        "p99.9",
        "p99_9_ns",
        b_p999,
        c_p999,
        Direction::LowerIsBetter,
        args.threshold_p99,
    ));

    let b_max = get_f64_path(baseline, &["latency_ns", "max"]);
    let c_max = get_f64_path(current, &["latency_ns", "max"]);
    out.push(latency_or_rps_delta(
        "max",
        "max_ns",
        b_max,
        c_max,
        Direction::LowerIsBetter,
        args.threshold_p99,
    ));
    // max is too noisy to treat as a hard regression — stamp as Neutral.
    if let Some(last) = out.last_mut() {
        last.status = match last.status {
            Status::Regression => Status::Neutral,
            other => other,
        };
    }

    // --- Error counters ---
    let categories = [
        "connect",
        "read",
        "write",
        "timeout",
        "keepup",
        "status_4xx",
        "status_5xx",
        "assertion_failed",
    ];
    for cat in categories {
        let b = get_u64_path(baseline, &["errors", cat]);
        let c = get_u64_path(current, &["errors", cat]);
        out.push(error_delta(cat, b, c));
    }

    out
}

#[derive(Clone, Copy)]
enum Direction {
    /// e.g. rps — higher numbers mean better.
    HigherIsBetter,
    /// e.g. p99 — lower numbers mean better.
    LowerIsBetter,
}

fn latency_or_rps_delta(
    display_name: &str,
    json_key: &str,
    baseline: f64,
    current: f64,
    direction: Direction,
    threshold_pct: f64,
) -> Delta {
    let delta_pct = if baseline > 0.0 {
        Some((current - baseline) / baseline * 100.0)
    } else {
        None
    };

    let status = match (direction, delta_pct) {
        (_, None) => {
            if current == baseline {
                Status::Ok
            } else {
                // 0-to-N change. For rps that's an improvement (assuming
                // baseline was 0 — unlikely but possible); for latency
                // it's a regression.
                match direction {
                    Direction::HigherIsBetter => {
                        if current > 0.0 {
                            Status::Ok
                        } else {
                            Status::Regression
                        }
                    }
                    Direction::LowerIsBetter => {
                        if current > 0.0 {
                            Status::Regression
                        } else {
                            Status::Ok
                        }
                    }
                }
            }
        }
        (Direction::HigherIsBetter, Some(d)) => {
            if d < -threshold_pct {
                Status::Regression
            } else {
                Status::Ok
            }
        }
        (Direction::LowerIsBetter, Some(d)) => {
            if d > threshold_pct {
                Status::Regression
            } else {
                Status::Ok
            }
        }
    };

    let baseline_display = format_value(display_name, baseline);
    let current_display = format_value(display_name, current);
    let display_delta = match delta_pct {
        Some(d) => format!("{:+.2}%", d),
        None => "—".into(),
    };

    Delta {
        metric: display_name.into(),
        baseline_display,
        current_display,
        delta_pct,
        display_delta,
        status,
        json_key: json_key.into(),
        json_baseline: baseline,
        json_current: current,
    }
}

fn error_delta(category: &str, baseline: u64, current: u64) -> Delta {
    let diff = current as i64 - baseline as i64;
    let status = if current > baseline {
        Status::Regression
    } else {
        Status::Ok
    };
    let display_delta = if diff == 0 {
        "±0".into()
    } else if diff > 0 {
        format!("+{diff}")
    } else {
        format!("{diff}")
    };
    Delta {
        metric: format!("errors ({category})"),
        baseline_display: baseline.to_string(),
        current_display: current.to_string(),
        delta_pct: None,
        display_delta,
        status,
        json_key: format!("errors.{category}"),
        json_baseline: baseline as f64,
        json_current: current as f64,
    }
}

/// p50/p90 moves don't count as regressions by default (we only flag
/// p99/p99.9). Convert any p50/p90 "regression" status to Neutral.
fn demote_latency_regression(d: &mut Delta) {
    if matches!(d.status, Status::Regression) {
        d.status = Status::Neutral;
    }
}

// ---------------------------------------------------------------------------
// JSON <-> f64/u64 helpers. Missing fields default to 0 (interpreted as
// "not present in this run"); this is more useful than hard-erroring in
// the face of schema evolution where a new field landed after baseline.
// ---------------------------------------------------------------------------

fn get_f64(v: &serde_json::Value, k: &str) -> f64 {
    // `Value::as_f64` already handles integer variants (both u64 and
    // i64), converting to f64 losslessly within the float's precision
    // range — no need for a manual `or_else(as_u64)` fallback.
    v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0)
}

fn get_f64_path(v: &serde_json::Value, path: &[&str]) -> f64 {
    let mut cur = v;
    for k in path {
        match cur.get(*k) {
            Some(next) => cur = next,
            None => return 0.0,
        }
    }
    cur.as_f64().unwrap_or(0.0)
}

fn get_u64_path(v: &serde_json::Value, path: &[&str]) -> u64 {
    let mut cur = v;
    for k in path {
        match cur.get(*k) {
            Some(next) => cur = next,
            None => return 0,
        }
    }
    cur.as_u64().unwrap_or(0)
}

/// Format a raw metric value for the report.
///
/// - `rps` is rendered as a decimal with one fractional digit.
/// - Latencies (`p50`, `p99`, ...) live in nanoseconds in the JSON and
///   come back through a shared formatter that picks µs/ms/s.
/// - Everything else is shown as the raw number.
fn format_value(metric: &str, v: f64) -> String {
    match metric {
        "rps" => format!("{v:.1}"),
        "p50" | "p90" | "p99" | "p99.9" | "max" => format_ns(v as u64),
        _ => format!("{v:.0}"),
    }
}

fn format_ns(ns: u64) -> String {
    if ns == 0 {
        "0".to_string()
    } else if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        let us = ns as f64 / 1_000.0;
        if us < 10.0 {
            format!("{us:.1}µs")
        } else {
            format!("{us:.0}µs")
        }
    } else if ns < 1_000_000_000 {
        let ms = ns as f64 / 1_000_000.0;
        if ms < 10.0 {
            format!("{ms:.1}ms")
        } else {
            format!("{ms:.0}ms")
        }
    } else {
        format!("{:.2}s", ns as f64 / 1e9)
    }
}

// ---------------------------------------------------------------------------
// Writers
// ---------------------------------------------------------------------------

fn write_terminal(
    out: &mut impl Write,
    args: &DiffArgs,
    deltas: &[Delta],
    regression: bool,
    is_tty: bool,
) -> io::Result<()> {
    let color = color_effective(args.color, is_tty);
    let on = if color { Condition::ALWAYS } else { Condition::NEVER };

    writeln!(
        out,
        "{} {} → {}",
        "baseline".bold().whenever(on),
        args.baseline.display(),
        args.current.display(),
    )?;
    writeln!(out)?;

    // Column widths sized to fit the longest metric name ("errors
    // (assertion_failed)") plus a small padding margin.
    const METRIC_W: usize = 28;
    writeln!(
        out,
        "{:<METRIC_W$}{:>14}{:>14}{:>12}  {}",
        "metric", "baseline", "current", "delta", "status"
    )?;
    writeln!(out, "{}", "─".repeat(METRIC_W + 14 + 14 + 12 + 10))?;
    for d in deltas {
        let status_str = match d.status {
            Status::Ok => "✓".green().whenever(on).to_string(),
            Status::Regression => {
                format!("{} {}", "⚠".red().whenever(on), "REGRESSION".red().whenever(on))
            }
            Status::Neutral => "—".dim().whenever(on).to_string(),
        };
        writeln!(
            out,
            "{:<METRIC_W$}{:>14}{:>14}{:>12}  {}",
            d.metric, d.baseline_display, d.current_display, d.display_delta, status_str
        )?;
    }
    writeln!(out)?;

    let summary = if regression {
        format!("{}", "REGRESSION — one or more metrics breached threshold.".red().whenever(on))
    } else {
        format!("{}", "OK — no regressions detected.".green().whenever(on))
    };
    writeln!(out, "{summary}")?;
    out.flush()
}

fn write_json(
    out: &mut impl Write,
    args: &DiffArgs,
    deltas: &[Delta],
    regression: bool,
) -> io::Result<()> {
    let mut deltas_obj = serde_json::Map::new();
    for d in deltas {
        deltas_obj.insert(
            d.json_key.clone(),
            serde_json::json!({
                "baseline": d.json_baseline,
                "current": d.json_current,
                "delta_pct": d.delta_pct,
                "status": d.status.as_str(),
            }),
        );
    }
    let blob = serde_json::json!({
        "baseline_path": args.baseline.display().to_string(),
        "current_path": args.current.display().to_string(),
        "regression": regression,
        "threshold_p99": args.threshold_p99,
        "threshold_rps": args.threshold_rps,
        "deltas": deltas_obj,
    });
    serde_json::to_writer_pretty(&mut *out, &blob)?;
    writeln!(out)?;
    out.flush()
}

fn color_effective(choice: CliColor, is_tty: bool) -> bool {
    match choice {
        CliColor::Always => true,
        CliColor::Never => false,
        CliColor::Auto => {
            if !is_tty {
                return false;
            }
            std::env::var_os("NO_COLOR")
                .map(|v| v.is_empty())
                .unwrap_or(true)
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn base_json(rps: f64, p99_ns: u64, err_5xx: u64) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "duration_ms": 30_000,
            "requests": 299_827,
            "requests_per_sec": rps,
            "bytes_sent": 2_039_873,
            "bytes_received": 7_562_456,
            "latency_ns": {
                "p50": 120_000u64,
                "p90": 450_000u64,
                "p99": p99_ns,
                "p99_9": 8_400_000u64,
                "max": 22_000_000u64
            },
            "errors": {
                "connect": 0,
                "read": 0,
                "write": 0,
                "timeout": 0,
                "keepup": 0,
                "status_4xx": 0,
                "status_5xx": err_5xx,
                "assertion_failed": 0
            }
        })
    }

    fn default_args() -> DiffArgs {
        DiffArgs {
            baseline: "b.json".into(),
            current: "c.json".into(),
            threshold_p99: 5.0,
            threshold_rps: 2.0,
            format: DiffFormat::Terminal,
            color: CliColor::Never,
        }
    }

    #[test]
    fn identical_inputs_produce_no_regression() {
        let b = base_json(10_000.0, 2_100_000, 0);
        let c = base_json(10_000.0, 2_100_000, 0);
        let deltas = compute_deltas(&b, &c, &default_args());
        for d in &deltas {
            assert_ne!(d.status, Status::Regression, "{d:?}");
        }
    }

    #[test]
    fn p99_jump_above_threshold_is_regression() {
        let baseline = base_json(10_000.0, 2_100_000, 0);
        let current = base_json(10_000.0, 2_500_000, 0); // ~19% worse
        let deltas = compute_deltas(&baseline, &current, &default_args());
        let p99 = deltas.iter().find(|d| d.metric == "p99").unwrap();
        assert_eq!(p99.status, Status::Regression);
    }

    #[test]
    fn rps_drop_above_threshold_is_regression() {
        let baseline = base_json(10_000.0, 2_100_000, 0);
        let current = base_json(9_500.0, 2_100_000, 0); // 5% drop
        let deltas = compute_deltas(&baseline, &current, &default_args());
        let rps = deltas.iter().find(|d| d.metric == "rps").unwrap();
        assert_eq!(rps.status, Status::Regression);
    }

    #[test]
    fn small_p99_jump_within_threshold_is_ok() {
        let baseline = base_json(10_000.0, 2_100_000, 0);
        let current = base_json(10_000.0, 2_150_000, 0); // ~2.4% up
        let deltas = compute_deltas(&baseline, &current, &default_args());
        let p99 = deltas.iter().find(|d| d.metric == "p99").unwrap();
        assert_eq!(p99.status, Status::Ok);
    }

    #[test]
    fn error_count_up_is_regression() {
        let baseline = base_json(10_000.0, 2_100_000, 0);
        let current = base_json(10_000.0, 2_100_000, 3);
        let deltas = compute_deltas(&baseline, &current, &default_args());
        let e5xx = deltas
            .iter()
            .find(|d| d.metric == "errors (status_5xx)")
            .unwrap();
        assert_eq!(e5xx.status, Status::Regression);
    }

    #[test]
    fn max_latency_regression_is_demoted_to_neutral() {
        let baseline = base_json(10_000.0, 2_100_000, 0);
        let mut current = base_json(10_000.0, 2_100_000, 0);
        // Double the max — not a regression signal.
        current["latency_ns"]["max"] = serde_json::Value::from(44_000_000u64);
        let deltas = compute_deltas(&baseline, &current, &default_args());
        let max = deltas.iter().find(|d| d.metric == "max").unwrap();
        assert_eq!(max.status, Status::Neutral);
    }

    #[test]
    fn threshold_override_suppresses_regression() {
        let baseline = base_json(10_000.0, 2_100_000, 0);
        let current = base_json(10_000.0, 2_500_000, 0); // 19% p99 jump
        let mut args = default_args();
        args.threshold_p99 = 25.0; // tolerate up to 25%
        let deltas = compute_deltas(&baseline, &current, &args);
        let p99 = deltas.iter().find(|d| d.metric == "p99").unwrap();
        assert_eq!(p99.status, Status::Ok);
    }

    #[test]
    fn format_ns_picks_readable_units() {
        assert_eq!(format_ns(0), "0");
        assert_eq!(format_ns(500), "500ns");
        assert_eq!(format_ns(120_000), "120µs");
        assert_eq!(format_ns(2_100_000), "2.1ms");
        assert_eq!(format_ns(22_000_000), "22ms");
        assert_eq!(format_ns(1_500_000_000), "1.50s");
    }
}
