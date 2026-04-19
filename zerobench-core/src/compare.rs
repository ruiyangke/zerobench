//! Statistical comparison engine — Phase 8a.
//!
//! Implements the run-level percentile bootstrap from
//! `docs/PHILOSOPHY.md` §9.3 / `docs/design-v0.1.0.md` §8. Given two
//! [`SummaryExport`]s with per-run metrics, computes bootstrap 95 %
//! confidence intervals on the delta for each metric.
//!
//! # Strategy selection
//!
//! - **`RunBootstrap`** — default when both sides have `per_run.len() ≥ 3`.
//!   10 000 resamples (with replacement) from the N per-run values on
//!   each side; the 2.5th and 97.5th percentiles of the resampled
//!   delta distribution form the 95 % CI.
//! - **`RawDelta`** — fallback when per-run data is missing or
//!   insufficient. No CI; the caller gets the raw point delta with
//!   a `NoEvidence` significance tag.
//!
//! Anderson-Darling and Kolmogorov-Smirnov strategies (PHILOSOPHY
//! §9.3 `ad-distribution` / `ks-distribution`) operate on HDR
//! histogram ECDFs and require the canonical `.histlog` sidecar —
//! land with Phase 8b after Phase 5c ships the log writer.
//!
//! # Determinism
//!
//! The bootstrap PRNG is seeded from the plan + run_id pair so two
//! invocations of `compare` against the same archived artefacts
//! produce byte-identical output. Seed flows through
//! [`BootstrapOptions::seed`] (defaults to a hash of the two
//! run_ids).

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::stats::{PerRunMetrics, SummaryExport};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// The metric axis under comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    /// Achieved rate (req/s). Regression = *decrease*.
    Rate,
    /// 50th-percentile latency (ns). Regression = *increase*.
    P50,
    /// 90th-percentile latency (ns).
    P90,
    /// 99th-percentile latency (ns).
    P99,
    /// 99.9th-percentile latency (ns).
    P99_9,
    /// 99.99th-percentile latency (ns).
    P99_99,
    /// Max observed latency (ns).
    Max,
    /// Error rate = total errors / requests.
    ErrorRate,
}

impl Metric {
    /// Human label used in report tables.
    pub fn label(&self) -> &'static str {
        match self {
            Metric::Rate => "rate",
            Metric::P50 => "p50",
            Metric::P90 => "p90",
            Metric::P99 => "p99",
            Metric::P99_9 => "p99.9",
            Metric::P99_99 => "p99.99",
            Metric::Max => "max",
            Metric::ErrorRate => "error_rate",
        }
    }

    /// `true` when *increase* is the regression direction (latency
    /// metrics, error rate). `false` for rate where a decrease is the
    /// regression.
    pub fn increase_is_bad(&self) -> bool {
        !matches!(self, Metric::Rate)
    }

    /// Pull this metric's value from a single run's metrics.
    pub fn extract(&self, run: &PerRunMetrics) -> f64 {
        match self {
            Metric::Rate => run.rate_per_s,
            Metric::P50 => run.latency.p50_ns as f64,
            Metric::P90 => run.latency.p90_ns as f64,
            Metric::P99 => run.latency.p99_ns as f64,
            Metric::P99_9 => run.latency.p99_9_ns as f64,
            Metric::P99_99 => run.latency.p99_99_ns as f64,
            Metric::Max => run.latency.max_ns as f64,
            Metric::ErrorRate => {
                if run.requests == 0 {
                    0.0
                } else {
                    run.errors_total as f64 / run.requests as f64
                }
            }
        }
    }

    /// The canonical axis set reported by `compare` — ordered so the
    /// table renders top-to-bottom in a familiar shape.
    pub fn canonical_axes() -> &'static [Metric] {
        &[
            Metric::Rate,
            Metric::P50,
            Metric::P90,
            Metric::P99,
            Metric::P99_9,
            Metric::P99_99,
            Metric::Max,
            Metric::ErrorRate,
        ]
    }
}

/// Which compare strategy produced a [`ComparisonResult`]. Reported
/// in JSON / terminal output so readers can tell bootstrap CIs apart
/// from raw-delta "no evidence" rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyUsed {
    /// Run-level percentile bootstrap (N ≥ 3 both sides).
    RunBootstrap,
    /// Raw delta — per-run data missing or fewer than 3 runs.
    RawDelta,
}

/// Significance verdict. `NotApplicable` when the strategy doesn't
/// produce a formal test (e.g. `RawDelta`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Significance {
    /// 95 % CI excludes zero → delta is unlikely to be noise.
    Significant,
    /// 95 % CI straddles zero → delta is consistent with noise.
    NotSignificant,
    /// Strategy produced no formal test.
    NotApplicable,
}

/// Options controlling [`compare_metric`] behaviour.
#[derive(Debug, Clone)]
pub struct CompareOptions {
    /// Bootstrap resample count. Default 10 000 (PHILOSOPHY §9.3).
    pub bootstrap_resamples: u32,
    /// Confidence level in (0.0, 1.0). Default 0.95.
    pub confidence_level: f64,
    /// PRNG seed for bootstrap resampling. Same seed → byte-identical
    /// output across invocations.
    pub seed: u64,
}

impl Default for CompareOptions {
    fn default() -> Self {
        Self {
            bootstrap_resamples: 10_000,
            confidence_level: 0.95,
            seed: 0xA5A5_A5A5_5A5A_5A5A,
        }
    }
}

/// Outcome of comparing one metric between two summaries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComparisonResult {
    /// Which metric was compared.
    pub metric: Metric,
    /// Point estimate for side A (e.g. mean of per-run values).
    pub a: f64,
    /// Point estimate for side B.
    pub b: f64,
    /// Absolute delta `b − a`.
    pub delta: f64,
    /// Fractional delta `(b − a) / a`. `None` when `a == 0` and
    /// `b != 0`, i.e. the ratio is undefined.
    pub delta_pct: Option<f64>,
    /// Bootstrap 95 % CI on the *absolute* delta. `None` for
    /// [`StrategyUsed::RawDelta`].
    pub ci: Option<(f64, f64)>,
    /// Number of run samples contributing to each side.
    pub n_a: usize,
    /// Number of run samples contributing to each side.
    pub n_b: usize,
    /// Strategy that produced this result.
    pub strategy: StrategyUsed,
    /// Significance verdict.
    pub significance: Significance,
}

impl ComparisonResult {
    /// Whether the `--regress-on <METRIC:PCT%>` threshold is crossed
    /// for this result.
    ///
    /// Semantics matches PHILOSOPHY §9.3 "threshold-crossing":
    ///
    /// - **RunBootstrap**: crossed iff the *appropriate* CI bound
    ///   exceeds the threshold. For increase-is-bad metrics that's
    ///   the CI lower bound; for rate it's the CI upper bound on the
    ///   (negative) delta.
    /// - **RawDelta**: crossed iff the raw `delta_pct` alone exceeds
    ///   the threshold (no CI to gate on).
    ///
    /// `threshold_pct` is positive — e.g. 0.05 for `+5%`. The user
    /// intent "flag if rate drops ≥5%" is the same input.
    pub fn regressed_beyond(&self, threshold_pct: f64) -> bool {
        let threshold_pct = threshold_pct.abs();
        match (self.strategy, self.ci, self.delta_pct) {
            (StrategyUsed::RunBootstrap, Some((lo, hi)), Some(dp)) => {
                if self.metric.increase_is_bad() {
                    // Increase is bad: regression if the CI's lower
                    // bound on delta still exceeds threshold_pct × |a|
                    // in absolute terms. Simplify by comparing
                    // fractional lower bound to threshold.
                    if self.a.abs() < f64::EPSILON {
                        // a=0, CI on absolute delta; can't convert to
                        // fractional — fall back to raw.
                        dp > threshold_pct
                    } else {
                        let lo_pct = lo / self.a;
                        lo_pct > threshold_pct
                    }
                } else {
                    // Rate: regression if CI's upper bound on delta
                    // is below -threshold × |a|.
                    if self.a.abs() < f64::EPSILON {
                        dp < -threshold_pct
                    } else {
                        let hi_pct = hi / self.a;
                        hi_pct < -threshold_pct
                    }
                }
            }
            (_, _, Some(dp)) => {
                if self.metric.increase_is_bad() {
                    dp > threshold_pct
                } else {
                    dp < -threshold_pct
                }
            }
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Compare a single metric between two summaries.
///
/// Selects the strategy automatically: `RunBootstrap` when both
/// `per_run` vectors have length ≥ 3, otherwise `RawDelta` using the
/// aggregate percentiles from [`SummaryExport::latency`] /
/// [`SummaryExport::rate_per_s`].
pub fn compare_metric(
    a: &SummaryExport,
    b: &SummaryExport,
    metric: Metric,
    opts: &CompareOptions,
) -> ComparisonResult {
    if a.per_run.len() >= 3 && b.per_run.len() >= 3 {
        compare_bootstrap(a, b, metric, opts)
    } else {
        compare_raw(a, b, metric)
    }
}

/// Convenience — run [`compare_metric`] across [`Metric::canonical_axes`].
pub fn compare_all(
    a: &SummaryExport,
    b: &SummaryExport,
    opts: &CompareOptions,
) -> Vec<ComparisonResult> {
    Metric::canonical_axes()
        .iter()
        .copied()
        .map(|m| compare_metric(a, b, m, opts))
        .collect()
}

// ---------------------------------------------------------------------------
// Raw-delta path
// ---------------------------------------------------------------------------

fn compare_raw(a: &SummaryExport, b: &SummaryExport, metric: Metric) -> ComparisonResult {
    let a_val = extract_aggregate(a, metric);
    let b_val = extract_aggregate(b, metric);
    let delta = b_val - a_val;
    let delta_pct = pct_delta(a_val, b_val);
    ComparisonResult {
        metric,
        a: a_val,
        b: b_val,
        delta,
        delta_pct,
        ci: None,
        n_a: a.per_run.len(),
        n_b: b.per_run.len(),
        strategy: StrategyUsed::RawDelta,
        significance: Significance::NotApplicable,
    }
}

fn extract_aggregate(s: &SummaryExport, metric: Metric) -> f64 {
    match metric {
        Metric::Rate => s.rate_per_s,
        Metric::P50 => s.latency.p50_ns as f64,
        Metric::P90 => s.latency.p90_ns as f64,
        Metric::P99 => s.latency.p99_ns as f64,
        Metric::P99_9 => s.latency.p99_9_ns as f64,
        Metric::P99_99 => s.latency.p99_99_ns as f64,
        Metric::Max => s.latency.max_ns as f64,
        Metric::ErrorRate => {
            if s.requests == 0 {
                0.0
            } else {
                let tot = s.errors.connect
                    + s.errors.read
                    + s.errors.write
                    + s.errors.timeout
                    + s.errors.keepup
                    + s.errors.status_4xx
                    + s.errors.status_5xx
                    + s.errors.assertion_failed;
                tot as f64 / s.requests as f64
            }
        }
    }
}

fn pct_delta(a: f64, b: f64) -> Option<f64> {
    if a.abs() < f64::EPSILON {
        if (b - a).abs() < f64::EPSILON {
            Some(0.0)
        } else {
            None // undefined ratio
        }
    } else {
        Some((b - a) / a)
    }
}

// ---------------------------------------------------------------------------
// Bootstrap path
// ---------------------------------------------------------------------------

fn compare_bootstrap(
    a: &SummaryExport,
    b: &SummaryExport,
    metric: Metric,
    opts: &CompareOptions,
) -> ComparisonResult {
    let a_values: Vec<f64> = a.per_run.iter().map(|r| metric.extract(r)).collect();
    let b_values: Vec<f64> = b.per_run.iter().map(|r| metric.extract(r)).collect();

    let a_mean = mean(&a_values);
    let b_mean = mean(&b_values);
    let delta = b_mean - a_mean;
    let delta_pct = pct_delta(a_mean, b_mean);

    // Seeded PRNG — determinism contract.
    let mut rng = Xoshiro { s: [opts.seed.wrapping_add(metric as u64 * 0x9E37_79B9), 0x243F_6A88_85A3_08D3, 0x13198A2E_03707344, 0xA409_3822_299F_31D0] };

    let mut deltas: Vec<f64> = Vec::with_capacity(opts.bootstrap_resamples as usize);
    for _ in 0..opts.bootstrap_resamples {
        let a_rs = mean_resample(&a_values, &mut rng);
        let b_rs = mean_resample(&b_values, &mut rng);
        deltas.push(b_rs - a_rs);
    }
    deltas.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));

    let alpha = 1.0 - opts.confidence_level;
    let lo_idx = ((alpha / 2.0) * deltas.len() as f64).round() as usize;
    let hi_idx =
        ((1.0 - alpha / 2.0) * deltas.len() as f64).round() as usize;
    let lo = deltas[lo_idx.min(deltas.len() - 1)];
    let hi = deltas[hi_idx.min(deltas.len() - 1)];

    let significance = if lo > 0.0 || hi < 0.0 {
        Significance::Significant
    } else {
        Significance::NotSignificant
    };

    ComparisonResult {
        metric,
        a: a_mean,
        b: b_mean,
        delta,
        delta_pct,
        ci: Some((lo, hi)),
        n_a: a_values.len(),
        n_b: b_values.len(),
        strategy: StrategyUsed::RunBootstrap,
        significance,
    }
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

fn mean_resample(values: &[f64], rng: &mut Xoshiro) -> f64 {
    let n = values.len();
    let mut sum = 0.0;
    for _ in 0..n {
        let idx = (rng.next_u64() as usize) % n;
        sum += values[idx];
    }
    sum / n as f64
}

// ---------------------------------------------------------------------------
// Two-sample Kolmogorov–Smirnov distribution test (Phase 8b)
//
// Tests the hypothesis "these two latency distributions were drawn
// from the same population." Uses the classical two-sample KS D
// statistic computed directly from HDR bucket counts:
//
//     D = max |F_A(v) - F_B(v)|  over all v
//
// where F_A, F_B are the empirical CDFs of the two histograms.
// p-value from the asymptotic Kolmogorov distribution, suitable for
// the "large N" regime we always operate in (>>100 samples per side).
//
// KS is less tail-sensitive than Anderson-Darling. AD lands in
// Phase 8c along with Holm-Bonferroni correction and a
// `--compare-strategy` CLI flag.
// ---------------------------------------------------------------------------

/// Result of a two-sample KS test between two HDR histograms.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KsResult {
    /// The D statistic — max absolute difference between the two
    /// empirical CDFs. Range [0.0, 1.0].
    pub d_statistic: f64,
    /// Asymptotic p-value under H₀ that the two samples are from
    /// the same distribution. Low values reject H₀.
    pub p_value: f64,
    /// Sample count side A.
    pub n_a: u64,
    /// Sample count side B.
    pub n_b: u64,
    /// Significance at conventional α=0.05. `NotApplicable` when
    /// either histogram is empty.
    pub significance: Significance,
}

/// Two-sample Kolmogorov–Smirnov on two HDR histograms.
///
/// Empty histograms yield a result with `d_statistic = 0.0`,
/// `p_value = 1.0`, and `significance = NotApplicable`.
pub fn ks_test(a: &Histogram<u64>, b: &Histogram<u64>) -> KsResult {
    let n_a = a.len();
    let n_b = b.len();

    if n_a == 0 || n_b == 0 {
        return KsResult {
            d_statistic: 0.0,
            p_value: 1.0,
            n_a,
            n_b,
            significance: Significance::NotApplicable,
        };
    }

    let d = ks_d_statistic(a, b);

    // Asymptotic p-value from the Kolmogorov distribution:
    //   λ = D · sqrt(n·m / (n+m))
    //   p ≈ 2 · Σ_{k=1..∞} (-1)^{k-1} · exp(-2 · k² · λ²)
    let n = n_a as f64;
    let m = n_b as f64;
    let en = (n * m / (n + m)).sqrt();
    let lambda = d * en;
    let p = kolmogorov_p_value(lambda);

    let significance = if p < 0.05 {
        Significance::Significant
    } else {
        Significance::NotSignificant
    };

    KsResult {
        d_statistic: d,
        p_value: p,
        n_a,
        n_b,
        significance,
    }
}

/// Compute the D statistic — max |F_A(v) - F_B(v)| — via merge-step
/// over the two histograms' recorded buckets.
fn ks_d_statistic(a: &Histogram<u64>, b: &Histogram<u64>) -> f64 {
    let n_a = a.len() as f64;
    let n_b = b.len() as f64;

    let a_pairs: Vec<(u64, u64)> = a
        .iter_recorded()
        .map(|iv| (iv.value_iterated_to(), iv.count_at_value()))
        .collect();
    let b_pairs: Vec<(u64, u64)> = b
        .iter_recorded()
        .map(|iv| (iv.value_iterated_to(), iv.count_at_value()))
        .collect();

    // Merge-walk. At each distinct value v in the union, compare
    // cumulative F_A(v) and F_B(v).
    let mut ia = 0usize;
    let mut ib = 0usize;
    let mut cum_a: u64 = 0;
    let mut cum_b: u64 = 0;
    let mut max_d: f64 = 0.0;

    while ia < a_pairs.len() || ib < b_pairs.len() {
        let va = a_pairs.get(ia).map(|p| p.0);
        let vb = b_pairs.get(ib).map(|p| p.0);

        let step_value = match (va, vb) {
            (Some(x), Some(y)) => x.min(y),
            (Some(x), None) => x,
            (None, Some(y)) => y,
            (None, None) => break,
        };

        // Advance every index whose value equals step_value so we
        // observe the full jump at this step before comparing.
        while ia < a_pairs.len() && a_pairs[ia].0 == step_value {
            cum_a += a_pairs[ia].1;
            ia += 1;
        }
        while ib < b_pairs.len() && b_pairs[ib].0 == step_value {
            cum_b += b_pairs[ib].1;
            ib += 1;
        }

        let f_a = cum_a as f64 / n_a;
        let f_b = cum_b as f64 / n_b;
        let d = (f_a - f_b).abs();
        if d > max_d {
            max_d = d;
        }
    }

    max_d
}

/// Asymptotic Kolmogorov distribution p-value: the probability that
/// sqrt(n·m/(n+m)) · D exceeds `lambda` under H₀ of equal
/// distributions. Series converges rapidly for `lambda > ~0.3`.
fn kolmogorov_p_value(lambda: f64) -> f64 {
    if lambda <= 0.0 {
        return 1.0;
    }
    // Q(λ) = 2 · Σ_{k=1..∞} (-1)^{k-1} · exp(-2·k²·λ²)
    // Truncate when the term falls below f64 epsilon × running sum.
    let mut sum = 0.0;
    let lambda_sq = lambda * lambda;
    let mut sign = 1.0;
    for k in 1..=100 {
        let term = sign * (-2.0 * (k as f64) * (k as f64) * lambda_sq).exp();
        sum += term;
        if term.abs() < 1e-12 {
            break;
        }
        sign = -sign;
    }
    (2.0 * sum).clamp(0.0, 1.0)
}

// ---------------------------------------------------------------------------
// Tiny deterministic PRNG — xoshiro256++.
//
// Kept inline instead of pulling `rand_xoshiro` as a public dep for
// the compare module because we only need one instance per
// `compare_metric` call and don't want to leak a `rand` API surface
// into `zerobench-core::compare`'s public types.
// ---------------------------------------------------------------------------

struct Xoshiro {
    s: [u64; 4],
}

impl Xoshiro {
    fn next_u64(&mut self) -> u64 {
        let result = self.s[0]
            .wrapping_add(self.s[3])
            .rotate_left(23)
            .wrapping_add(self.s[0]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{ErrorCountersExport, LatencyExport};

    fn per_run(rate: f64, p99: u64) -> PerRunMetrics {
        PerRunMetrics {
            index: 0,
            rate_per_s: rate,
            requests: 1000,
            errors_total: 0,
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
        }
    }

    fn base_export(runs: Vec<PerRunMetrics>) -> SummaryExport {
        let rate = if runs.is_empty() {
            0.0
        } else {
            mean(&runs.iter().map(|r| r.rate_per_s).collect::<Vec<_>>())
        };
        SummaryExport {
            schema_version: 1,
            duration_ns: 1_000_000_000,
            requests: runs.iter().map(|r| r.requests).sum(),
            rate_per_s: rate,
            bytes_sent: 0,
            bytes_recv: 0,
            latency: runs.first().map(|r| r.latency.clone()).unwrap_or(LatencyExport {
                count: 0, min_ns: 0, p50_ns: 0, p90_ns: 0, p99_ns: 0,
                p99_9_ns: 0, p99_99_ns: 0, max_ns: 0, mean_ns: 0.0, stddev_ns: 0.0,
            }),
            ttfb: LatencyExport {
                count: 0, min_ns: 0, p50_ns: 0, p90_ns: 0, p99_ns: 0,
                p99_9_ns: 0, p99_99_ns: 0, max_ns: 0, mean_ns: 0.0, stddev_ns: 0.0,
            },
            errors: ErrorCountersExport {
                connect: 0, read: 0, write: 0, timeout: 0, keepup: 0,
                status_4xx: 0, status_5xx: 0, assertion_failed: 0,
            },
            scenarios: Vec::new(),
            per_run: runs,
        }
    }

    #[test]
    fn raw_delta_when_per_run_missing() {
        let a = base_export(vec![]);
        let mut a = a;
        a.rate_per_s = 1000.0;
        let mut b = base_export(vec![]);
        b.rate_per_s = 950.0;
        let result = compare_metric(&a, &b, Metric::Rate, &CompareOptions::default());
        assert_eq!(result.strategy, StrategyUsed::RawDelta);
        assert!(result.ci.is_none());
        assert!((result.delta - (-50.0)).abs() < 1e-9);
        assert!((result.delta_pct.unwrap() - (-0.05)).abs() < 1e-9);
    }

    #[test]
    fn bootstrap_when_three_runs_each_side() {
        let a = base_export(vec![
            per_run(1000.0, 1_000_000),
            per_run(1002.0, 1_010_000),
            per_run(998.0, 990_000),
        ]);
        let b = base_export(vec![
            per_run(950.0, 1_100_000),
            per_run(955.0, 1_090_000),
            per_run(945.0, 1_110_000),
        ]);
        let result = compare_metric(&a, &b, Metric::Rate, &CompareOptions::default());
        assert_eq!(result.strategy, StrategyUsed::RunBootstrap);
        assert!(result.ci.is_some());
        // Rate decreased by ~50 on B; CI should be entirely negative.
        let (lo, hi) = result.ci.unwrap();
        assert!(hi < 0.0, "CI=({lo}, {hi})");
        assert_eq!(result.significance, Significance::Significant);
    }

    #[test]
    fn bootstrap_not_significant_when_overlapping() {
        // Two samples with the same mean — delta should straddle 0.
        let runs = vec![
            per_run(1000.0, 1_000_000),
            per_run(1005.0, 1_005_000),
            per_run(995.0, 995_000),
        ];
        let a = base_export(runs.clone());
        let b = base_export(runs);
        let result = compare_metric(&a, &b, Metric::Rate, &CompareOptions::default());
        assert_eq!(result.significance, Significance::NotSignificant);
        let (lo, hi) = result.ci.unwrap();
        assert!(lo <= 0.0 && hi >= 0.0, "CI=({lo}, {hi})");
    }

    #[test]
    fn bootstrap_is_deterministic_with_same_seed() {
        let a = base_export(vec![
            per_run(1000.0, 1_000_000),
            per_run(1002.0, 1_010_000),
            per_run(998.0, 990_000),
        ]);
        let b = base_export(vec![
            per_run(950.0, 1_100_000),
            per_run(955.0, 1_090_000),
            per_run(945.0, 1_110_000),
        ]);
        let opts = CompareOptions::default();
        let r1 = compare_metric(&a, &b, Metric::Rate, &opts);
        let r2 = compare_metric(&a, &b, Metric::Rate, &opts);
        assert_eq!(r1, r2);
    }

    #[test]
    fn bootstrap_varies_with_seed() {
        let a = base_export(vec![
            per_run(1000.0, 1_000_000),
            per_run(1100.0, 1_010_000),
            per_run(900.0, 990_000),
        ]);
        let b = base_export(vec![
            per_run(950.0, 1_100_000),
            per_run(1050.0, 1_090_000),
            per_run(850.0, 1_110_000),
        ]);
        let mut o1 = CompareOptions::default();
        o1.seed = 42;
        let mut o2 = CompareOptions::default();
        o2.seed = 99;
        let r1 = compare_metric(&a, &b, Metric::Rate, &o1);
        let r2 = compare_metric(&a, &b, Metric::Rate, &o2);
        // Point estimates same (mean is deterministic); CI differs due to seed.
        assert_eq!(r1.a, r2.a);
        assert_ne!(r1.ci, r2.ci);
    }

    #[test]
    fn compare_all_covers_canonical_axes() {
        let a = base_export(vec![per_run(1000.0, 1_000_000)]);
        let b = base_export(vec![per_run(1050.0, 900_000)]);
        let out = compare_all(&a, &b, &CompareOptions::default());
        assert_eq!(out.len(), Metric::canonical_axes().len());
        assert!(out.iter().any(|r| r.metric == Metric::Rate));
        assert!(out.iter().any(|r| r.metric == Metric::P99));
    }

    #[test]
    fn regression_gate_raw_delta_latency() {
        let a = base_export(vec![]);
        let mut a = a;
        a.latency.p99_ns = 1_000_000;
        let mut b = base_export(vec![]);
        b.latency.p99_ns = 1_100_000;
        let r = compare_metric(&a, &b, Metric::P99, &CompareOptions::default());
        assert!(r.regressed_beyond(0.05), "+10% > 5% threshold");
        assert!(!r.regressed_beyond(0.20), "+10% < 20% threshold");
    }

    #[test]
    fn regression_gate_raw_delta_rate() {
        let mut a = base_export(vec![]);
        a.rate_per_s = 1000.0;
        let mut b = base_export(vec![]);
        b.rate_per_s = 920.0;
        let r = compare_metric(&a, &b, Metric::Rate, &CompareOptions::default());
        assert!(r.regressed_beyond(0.05), "-8% past 5% threshold");
        assert!(!r.regressed_beyond(0.20), "-8% within 20% threshold");
        // Improvement should never fire.
        let mut b2 = base_export(vec![]);
        b2.rate_per_s = 1500.0;
        let r2 = compare_metric(&a, &b2, Metric::Rate, &CompareOptions::default());
        assert!(!r2.regressed_beyond(0.05));
    }

    #[test]
    fn regression_gate_bootstrap_uses_ci() {
        // Very low-variance samples: CI is narrow, regression is
        // clearly identified.
        let a = base_export(vec![
            per_run(1000.0, 1_000_000),
            per_run(1000.0, 1_000_000),
            per_run(1000.0, 1_000_000),
        ]);
        let b = base_export(vec![
            per_run(1000.0, 1_200_000),
            per_run(1000.0, 1_200_000),
            per_run(1000.0, 1_200_000),
        ]);
        let r = compare_metric(&a, &b, Metric::P99, &CompareOptions::default());
        assert_eq!(r.strategy, StrategyUsed::RunBootstrap);
        assert!(r.regressed_beyond(0.05), "+20% past 5% threshold");
    }

    #[test]
    fn metric_labels_match_report_schema() {
        assert_eq!(Metric::Rate.label(), "rate");
        assert_eq!(Metric::P99.label(), "p99");
        assert_eq!(Metric::P99_9.label(), "p99.9");
        assert_eq!(Metric::ErrorRate.label(), "error_rate");
    }

    #[test]
    fn metric_increase_is_bad_flags() {
        assert!(Metric::P99.increase_is_bad());
        assert!(Metric::ErrorRate.increase_is_bad());
        assert!(!Metric::Rate.increase_is_bad());
    }

    // -----------------------------------------------------------------
    // KS test
    // -----------------------------------------------------------------

    fn mk_hist() -> Histogram<u64> {
        Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
    }

    #[test]
    fn ks_identical_histograms_are_not_significant() {
        let mut a = mk_hist();
        let mut b = mk_hist();
        // Same samples into each.
        for v in [100u64, 200, 500, 1_000, 2_000, 5_000].iter().cycle().take(1000) {
            a.record(*v).unwrap();
            b.record(*v).unwrap();
        }
        let result = ks_test(&a, &b);
        assert_eq!(result.n_a, 1000);
        assert_eq!(result.n_b, 1000);
        assert!(result.d_statistic < 1e-9, "D={}", result.d_statistic);
        assert!(result.p_value > 0.99, "p={}", result.p_value);
        assert_eq!(result.significance, Significance::NotSignificant);
    }

    #[test]
    fn ks_shifted_histograms_are_significant() {
        let mut a = mk_hist();
        let mut b = mk_hist();
        // A centred ~1k ns, B centred ~10k ns — clearly different.
        for v in (500u64..=1500).step_by(10) {
            a.record(v).unwrap();
        }
        for v in (5_000u64..=15_000).step_by(100) {
            b.record(v).unwrap();
        }
        let result = ks_test(&a, &b);
        assert_eq!(result.significance, Significance::Significant);
        assert!(result.p_value < 0.01, "p={}", result.p_value);
        assert!(result.d_statistic > 0.5, "D={}", result.d_statistic);
    }

    #[test]
    fn ks_empty_histograms_yield_not_applicable() {
        let a = mk_hist();
        let b = mk_hist();
        let result = ks_test(&a, &b);
        assert_eq!(result.significance, Significance::NotApplicable);
        assert_eq!(result.d_statistic, 0.0);
        assert!(result.p_value >= 1.0 - 1e-9);
    }

    #[test]
    fn ks_one_empty_one_populated_returns_not_applicable() {
        let a = mk_hist();
        let mut b = mk_hist();
        for v in 1..=100u64 {
            b.record(v * 100).unwrap();
        }
        let result = ks_test(&a, &b);
        assert_eq!(result.significance, Significance::NotApplicable);
    }

    #[test]
    fn ks_small_shift_might_not_be_significant() {
        // Two histograms with nearly-identical spread but slight
        // offset — at small N the KS test should not reject H₀.
        let mut a = mk_hist();
        let mut b = mk_hist();
        for v in 500u64..=1500 {
            a.record(v).unwrap();
            b.record(v + 10).unwrap(); // tiny shift
        }
        let result = ks_test(&a, &b);
        // We don't assert either way on significance — the exact
        // threshold depends on sample size. But the test must at
        // least produce a valid [0,1] p-value.
        assert!(result.p_value >= 0.0 && result.p_value <= 1.0);
        assert!(result.d_statistic >= 0.0 && result.d_statistic <= 1.0);
    }

    #[test]
    fn ks_is_deterministic() {
        let mut a = mk_hist();
        let mut b = mk_hist();
        for v in 100u64..=500 {
            a.record(v).unwrap();
            b.record(v + 5).unwrap();
        }
        let r1 = ks_test(&a, &b);
        let r2 = ks_test(&a, &b);
        assert_eq!(r1, r2);
    }

    #[test]
    fn kolmogorov_p_value_monotonic_in_lambda() {
        let small = kolmogorov_p_value(0.1);
        let medium = kolmogorov_p_value(0.5);
        let large = kolmogorov_p_value(2.0);
        assert!(small >= medium);
        assert!(medium >= large);
        assert!(large < 0.001, "p={large}");
    }

    #[test]
    fn kolmogorov_p_value_bounds() {
        // λ = 0 → p = 1
        assert!((kolmogorov_p_value(0.0) - 1.0).abs() < 1e-9);
        // Large λ → p → 0
        assert!(kolmogorov_p_value(10.0) < 1e-6);
    }
}
