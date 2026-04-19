//! `zerobench compare A B` — diff two v0.1.0 `result.json` artefacts.
//!
//! Per PHILOSOPHY §P2 / §9.3: the comparison-first workflow. This
//! verb reads two `SummaryExport` files, computes raw percentile
//! deltas, and prints a side-by-side table. Regression gating is
//! opt-in via `--regress-on METRIC:+PCT,...`; absent, output is
//! informational only (exit 0 regardless of delta).
//!
//! The full statistical comparison engine (bootstrap CI for N≥3,
//! Anderson-Darling for N=1, Holm-Bonferroni multi-metric correction)
//! lands with Phase 8. For now, diff is the simple-delta form —
//! already useful as a regression gate when thresholds are set.
//!
//! # Naming
//!
//! `compare` here (as a v0.1.0 subcommand) is distinct from the
//! top-level `Diff` subcommand, which reads v0.0.1 JSON. When
//! Phase 7c finalises the migration, `Diff` is deprecated and
//! `compare` becomes the default.

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args;
use zerobench_core::{LatencyExport, SummaryExport};

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Flags accepted by `zerobench compare A.json B.json`.
#[derive(Debug, Clone, Args)]
pub struct CompareArgs {
    /// Baseline artefact (the "before" — A in the diff).
    #[arg(value_name = "BASELINE")]
    pub baseline: PathBuf,
    /// Current artefact (the "after" — B in the diff).
    #[arg(value_name = "CURRENT")]
    pub current: PathBuf,

    /// Regression thresholds — `METRIC:+PCT[,...]`. Any threshold
    /// crossed flips exit code 1. Absent → informational only.
    ///
    /// Supported metrics: `rate`, `p50`, `p90`, `p99`, `p99_9`,
    /// `p99_99`, `max`, `error_rate`.
    ///
    /// Example: `--regress-on p99:+5%,p99_9:+10%,error_rate:+0.01%`
    #[arg(long = "regress-on")]
    pub regress_on: Option<String>,
}

/// One parsed `METRIC:+PCT%` threshold.
#[derive(Debug, Clone, PartialEq)]
pub struct RegressThreshold {
    /// The metric name (must match a `MetricId` variant).
    pub metric: MetricId,
    /// Fractional increase — `0.05` for `+5%`. Signed so negative
    /// thresholds express "flag if the metric dropped by more than N%".
    pub delta: f64,
}

/// Metrics reported in the diff table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricId {
    /// Throughput (req/s). Regression = *decrease*.
    Rate,
    /// Median latency. Regression = *increase*.
    P50,
    /// 90th-percentile latency.
    P90,
    /// 99th-percentile latency.
    P99,
    /// 99.9th-percentile latency.
    P99_9,
    /// 99.99th-percentile latency.
    P99_99,
    /// Max latency observed.
    Max,
    /// Error rate = total errors / requests. Regression = *increase*.
    ErrorRate,
}

impl MetricId {
    /// Human-readable label for the report table.
    pub fn label(&self) -> &'static str {
        match self {
            MetricId::Rate => "rate",
            MetricId::P50 => "p50",
            MetricId::P90 => "p90",
            MetricId::P99 => "p99",
            MetricId::P99_9 => "p99.9",
            MetricId::P99_99 => "p99.99",
            MetricId::Max => "max",
            MetricId::ErrorRate => "error_rate",
        }
    }

    /// `true` when an *increase* is the regression direction (latency,
    /// error rate). `false` for rate (where a decrease is the
    /// regression).
    pub fn increase_is_bad(&self) -> bool {
        !matches!(self, MetricId::Rate)
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "rate" => Some(MetricId::Rate),
            "p50" => Some(MetricId::P50),
            "p90" => Some(MetricId::P90),
            "p99" => Some(MetricId::P99),
            "p99_9" | "p99.9" => Some(MetricId::P99_9),
            "p99_99" | "p99.99" => Some(MetricId::P99_99),
            "max" => Some(MetricId::Max),
            "error_rate" => Some(MetricId::ErrorRate),
            _ => None,
        }
    }
}

/// Parse `"METRIC:+PCT%,..."` into a list of [`RegressThreshold`]s.
pub fn parse_regress_spec(s: &str) -> Result<Vec<RegressThreshold>, String> {
    s.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(parse_single_threshold)
        .collect()
}

fn parse_single_threshold(raw: &str) -> Result<RegressThreshold, String> {
    let (metric_str, delta_str) = raw
        .split_once(':')
        .ok_or_else(|| format!("--regress-on entry `{raw}` expects METRIC:+PCT%"))?;
    let metric = MetricId::from_str(metric_str.trim())
        .ok_or_else(|| format!("unknown metric `{metric_str}` in --regress-on"))?;

    let mut d = delta_str.trim();
    let sign = if let Some(rest) = d.strip_prefix('+') {
        d = rest;
        1.0
    } else if let Some(rest) = d.strip_prefix('-') {
        d = rest;
        -1.0
    } else {
        1.0
    };
    let d = d.strip_suffix('%').unwrap_or(d);
    let pct: f64 = d
        .parse()
        .map_err(|e| format!("--regress-on `{raw}`: bad percentage: {e}"))?;
    Ok(RegressThreshold {
        metric,
        delta: sign * pct / 100.0,
    })
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

/// Run the diff.
pub fn run(args: CompareArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let baseline = load_export(&args.baseline)?;
    let current = load_export(&args.current)?;

    // Schema compatibility check. Major-version mismatch refuses —
    // minor-version mismatch is additive-only per PHILOSOPHY §9.3.2.
    if baseline.schema_version != current.schema_version {
        eprintln!(
            "warning: schema_version mismatch — baseline v{}, current v{} (proceeding)",
            baseline.schema_version, current.schema_version,
        );
    }

    let rows = compute_rows(&baseline, &current);
    render_table(&baseline, &current, &rows, &args);

    // Apply regression gating if asked.
    let regressed = match &args.regress_on {
        None => {
            eprintln!(
                "note: no --regress-on thresholds configured; comparison is informational only."
            );
            false
        }
        Some(raw) => {
            let thresholds = parse_regress_spec(raw)
                .map_err(|e| format!("--regress-on: {e}"))?;
            let mut any = false;
            for t in &thresholds {
                if let Some(row) = rows.iter().find(|r| r.metric == t.metric) {
                    let crossed = check_threshold(row.delta_pct, t);
                    if crossed {
                        eprintln!(
                            "regressed: {} delta {:+.2}% exceeds threshold {:+.2}%",
                            t.metric.label(),
                            row.delta_pct * 100.0,
                            t.delta * 100.0,
                        );
                        any = true;
                    }
                }
            }
            any
        }
    };

    Ok(if regressed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}

fn load_export(path: &PathBuf) -> Result<SummaryExport, Box<dyn std::error::Error>> {
    let bytes = fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let export: SummaryExport = serde_json::from_slice(&bytes)
        .map_err(|e| format!("cannot parse {} as result.json: {e}", path.display()))?;
    Ok(export)
}

// ---------------------------------------------------------------------------
// Row computation
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Row {
    metric: MetricId,
    a: f64,
    b: f64,
    delta_pct: f64, // (b - a) / a
}

fn compute_rows(a: &SummaryExport, b: &SummaryExport) -> Vec<Row> {
    fn pct_delta(a: f64, b: f64) -> f64 {
        if a.abs() < f64::EPSILON {
            if b.abs() < f64::EPSILON {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            (b - a) / a
        }
    }

    let err_rate = |s: &SummaryExport| -> f64 {
        if s.requests == 0 {
            0.0
        } else {
            let total = s.errors.connect
                + s.errors.read
                + s.errors.write
                + s.errors.timeout
                + s.errors.keepup
                + s.errors.status_4xx
                + s.errors.status_5xx
                + s.errors.assertion_failed;
            total as f64 / s.requests as f64
        }
    };

    let lat_field = |e: &LatencyExport, m: MetricId| -> f64 {
        match m {
            MetricId::P50 => e.p50_ns as f64,
            MetricId::P90 => e.p90_ns as f64,
            MetricId::P99 => e.p99_ns as f64,
            MetricId::P99_9 => e.p99_9_ns as f64,
            MetricId::P99_99 => e.p99_99_ns as f64,
            MetricId::Max => e.max_ns as f64,
            _ => 0.0,
        }
    };

    let mut rows: Vec<Row> = Vec::with_capacity(8);

    // Rate — b/a swapped semantically because *decrease* is the
    // regression direction. We still report raw `(b-a)/a`; gate logic
    // inverts via increase_is_bad().
    rows.push(Row {
        metric: MetricId::Rate,
        a: a.rate_per_s,
        b: b.rate_per_s,
        delta_pct: pct_delta(a.rate_per_s, b.rate_per_s),
    });

    for m in [
        MetricId::P50,
        MetricId::P90,
        MetricId::P99,
        MetricId::P99_9,
        MetricId::P99_99,
        MetricId::Max,
    ] {
        let av = lat_field(&a.latency, m);
        let bv = lat_field(&b.latency, m);
        rows.push(Row {
            metric: m,
            a: av,
            b: bv,
            delta_pct: pct_delta(av, bv),
        });
    }

    rows.push(Row {
        metric: MetricId::ErrorRate,
        a: err_rate(a),
        b: err_rate(b),
        delta_pct: pct_delta(err_rate(a), err_rate(b)),
    });

    rows
}

fn check_threshold(delta_pct: f64, t: &RegressThreshold) -> bool {
    // Regression direction depends on the metric. For latency/errors,
    // increase > threshold is bad. For rate, decrease > |threshold| is
    // bad (the user specifies `rate:-5%` or `rate:+5%`, both interpreted
    // as "flag if rate dropped by >5%").
    if t.metric.increase_is_bad() {
        delta_pct > t.delta
    } else {
        // Rate — regression is negative delta; threshold magnitude
        // is compared against |delta| when delta is negative.
        let t_mag = t.delta.abs();
        delta_pct < -t_mag
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn render_table(
    a: &SummaryExport,
    b: &SummaryExport,
    rows: &[Row],
    _args: &CompareArgs,
) {
    println!("compare");
    println!(
        "  baseline  {:>12}  rate   p50   p99   p99.9   errors",
        "reqs"
    );
    println!(
        "  A         {:>12}  {:>6.0}/s  {}  {}  {}  {}",
        fmt_int(a.requests),
        a.rate_per_s,
        fmt_ns(a.latency.p50_ns),
        fmt_ns(a.latency.p99_ns),
        fmt_ns(a.latency.p99_9_ns),
        fmt_int(error_count(a)),
    );
    println!(
        "  B         {:>12}  {:>6.0}/s  {}  {}  {}  {}",
        fmt_int(b.requests),
        b.rate_per_s,
        fmt_ns(b.latency.p50_ns),
        fmt_ns(b.latency.p99_ns),
        fmt_ns(b.latency.p99_9_ns),
        fmt_int(error_count(b)),
    );
    println!();
    println!("{:>10}  {:>14}  {:>14}  {:>10}", "metric", "A", "B", "Δ");
    for row in rows {
        let (a_fmt, b_fmt) = match row.metric {
            MetricId::Rate => (format!("{:.1}/s", row.a), format!("{:.1}/s", row.b)),
            MetricId::ErrorRate => (
                format!("{:.4}%", row.a * 100.0),
                format!("{:.4}%", row.b * 100.0),
            ),
            _ => (fmt_ns(row.a as u64), fmt_ns(row.b as u64)),
        };
        let delta = if row.delta_pct.is_infinite() {
            "∞".to_string()
        } else {
            format!("{:+.2}%", row.delta_pct * 100.0)
        };
        println!(
            "{:>10}  {:>14}  {:>14}  {:>10}",
            row.metric.label(),
            a_fmt,
            b_fmt,
            delta
        );
    }
}

fn error_count(s: &SummaryExport) -> u64 {
    s.errors.connect
        + s.errors.read
        + s.errors.write
        + s.errors.timeout
        + s.errors.keepup
        + s.errors.status_4xx
        + s.errors.status_5xx
        + s.errors.assertion_failed
}

fn fmt_int(n: u64) -> String {
    // Simple group-of-3 separator.
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.2}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use zerobench_core::stats::ErrorCountersExport;

    fn mk_export(rate: f64, p99: u64, errors: u64) -> SummaryExport {
        SummaryExport {
            schema_version: 1,
            duration_ns: 1_000_000_000,
            requests: 1000,
            rate_per_s: rate,
            bytes_sent: 0,
            bytes_recv: 0,
            latency: LatencyExport {
                count: 1000,
                min_ns: 100,
                p50_ns: p99 / 10,
                p90_ns: p99 / 2,
                p99_ns: p99,
                p99_9_ns: p99 * 2,
                p99_99_ns: p99 * 3,
                max_ns: p99 * 4,
                mean_ns: (p99 as f64) / 8.0,
                stddev_ns: 100.0,
            },
            ttfb: LatencyExport {
                count: 0,
                min_ns: 0,
                p50_ns: 0,
                p90_ns: 0,
                p99_ns: 0,
                p99_9_ns: 0,
                p99_99_ns: 0,
                max_ns: 0,
                mean_ns: 0.0,
                stddev_ns: 0.0,
            },
            errors: ErrorCountersExport {
                connect: errors,
                read: 0,
                write: 0,
                timeout: 0,
                keepup: 0,
                status_4xx: 0,
                status_5xx: 0,
                assertion_failed: 0,
            },
            scenarios: Vec::new(),
        }
    }

    #[test]
    fn parse_regress_spec_basic() {
        let out = parse_regress_spec("p99:+5%,p99_9:+10%").unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].metric, MetricId::P99);
        assert!((out[0].delta - 0.05).abs() < 1e-9);
        assert_eq!(out[1].metric, MetricId::P99_9);
        assert!((out[1].delta - 0.10).abs() < 1e-9);
    }

    #[test]
    fn parse_regress_spec_tolerates_alias() {
        let out = parse_regress_spec("p99.9:+3%").unwrap();
        assert_eq!(out[0].metric, MetricId::P99_9);
    }

    #[test]
    fn parse_regress_spec_rejects_unknown_metric() {
        assert!(parse_regress_spec("bogus:+5%").is_err());
    }

    #[test]
    fn parse_regress_spec_rejects_missing_colon() {
        assert!(parse_regress_spec("p99+5%").is_err());
    }

    #[test]
    fn rows_computed_correctly() {
        let a = mk_export(1000.0, 1_000_000, 0);
        let b = mk_export(950.0, 1_100_000, 5);
        let rows = compute_rows(&a, &b);

        let rate = rows.iter().find(|r| r.metric == MetricId::Rate).unwrap();
        assert!((rate.delta_pct - (-0.05)).abs() < 1e-6, "{rate:?}");

        let p99 = rows.iter().find(|r| r.metric == MetricId::P99).unwrap();
        assert!((p99.delta_pct - 0.10).abs() < 1e-6, "{p99:?}");

        let err = rows
            .iter()
            .find(|r| r.metric == MetricId::ErrorRate)
            .unwrap();
        // 0 / 1000 → 5 / 1000 = 0.005, delta from 0 is ∞.
        assert!(err.delta_pct.is_infinite());
    }

    #[test]
    fn threshold_fires_on_latency_increase() {
        let t = RegressThreshold {
            metric: MetricId::P99,
            delta: 0.05,
        };
        assert!(check_threshold(0.10, &t), "10% > 5% should fire");
        assert!(!check_threshold(0.04, &t), "4% < 5% should not fire");
        assert!(!check_threshold(-0.50, &t), "p99 down is an improvement");
    }

    #[test]
    fn threshold_fires_on_rate_decrease() {
        let t = RegressThreshold {
            metric: MetricId::Rate,
            delta: 0.05,
        };
        assert!(check_threshold(-0.10, &t), "rate drop 10% > threshold 5%");
        assert!(!check_threshold(-0.04, &t), "rate drop 4% below threshold");
        assert!(!check_threshold(0.20, &t), "rate increase is good");
    }
}
