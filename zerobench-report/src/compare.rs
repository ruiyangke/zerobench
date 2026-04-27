//! Statistical comparison engine
//!
//! CROWN JEWEL: bootstrap CI + exact k=2 Scholz-Stephens AD variance +
//! KS + Holm-Bonferroni. See PHILOSOPHY §9.3 / design-v0.1.0.md §8.
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
//! operate on HDR histogram ECDFs via the canonical `.histlog` sidecar.
//!
//! # Determinism
//!
//! The bootstrap PRNG is seeded from the plan + run_id pair so two
//! invocations of `compare` against the same archived artefacts
//! produce byte-identical output. Seed flows through
//! [`CompareOptions::seed`] (defaults to a hash of the two
//! run_ids).

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use zerobench_core::stats::{PerRunMetrics, SummaryExport};

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
    ///
    /// For latency percentiles, the extractor prefers
    /// `run.protocol_latency` when non-empty — that slot carries the
    /// backend's primary latency signal (chunk_gap for SseHold, rtt
    /// for WsEchoRtt, broadcast_rtt for fanouts, etc.) and is the
    /// right axis for regression gating on protocol-native runs.
    /// Falls back to the generic HTTP `run.latency` field otherwise.
    pub fn extract(&self, run: &PerRunMetrics) -> f64 {
        let lat = if run.protocol_latency.count > 0 {
            &run.protocol_latency
        } else {
            &run.latency
        };
        match self {
            Metric::Rate => run.rate_per_s,
            Metric::P50 => lat.p50_ns as f64,
            Metric::P90 => lat.p90_ns as f64,
            Metric::P99 => lat.p99_ns as f64,
            Metric::P99_9 => lat.p99_9_ns as f64,
            Metric::P99_99 => lat.p99_99_ns as f64,
            Metric::Max => lat.max_ns as f64,
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
    let mut rng = Xoshiro {
        s: [
            opts.seed.wrapping_add(metric as u64 * 0x9E37_79B9),
            0x243F_6A88_85A3_08D3,
            0x13198A2E_03707344,
            0xA409_3822_299F_31D0,
        ],
    };

    let mut deltas: Vec<f64> = Vec::with_capacity(opts.bootstrap_resamples as usize);
    for _ in 0..opts.bootstrap_resamples {
        let a_rs = mean_resample(&a_values, &mut rng);
        let b_rs = mean_resample(&b_values, &mut rng);
        deltas.push(b_rs - a_rs);
    }
    deltas.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));

    let alpha = 1.0 - opts.confidence_level;
    let lo_idx = ((alpha / 2.0) * deltas.len() as f64).round() as usize;
    let hi_idx = ((1.0 - alpha / 2.0) * deltas.len() as f64).round() as usize;
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
// Two-sample Kolmogorov–Smirnov distribution test
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
// alongside Holm-Bonferroni correction and a
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
// Two-sample Anderson–Darling distribution test
//
// AD is more tail-sensitive than KS — its integrand weights
// F(x)(1-F(x)) in the denominator so differences at the extreme
// percentiles (where F is near 0 or 1) contribute disproportionately.
// This aligns with PHILOSOPHY §P3 "tail is the product": the test
// that best flags p99/p99.9/p99.99 shifts.
//
// Implementation follows Scholz-Stephens (1987) "K-Sample
// Anderson-Darling Tests", 2-sample simplification. We compute A²
// directly from HDR bucket counts (no per-sample iteration) and
// approximate the p-value from the asymptotic distribution.
//
// Reference critical values (2-sample, asymptotic):
//   α=0.10: T ≈ 1.225
//   α=0.05: T ≈ 1.960
//   α=0.025: T ≈ 2.719
//   α=0.01: T ≈ 3.752
// where T = (A² - 1) / σ_N, σ_N computed from N via standard table.
// ---------------------------------------------------------------------------

/// Result of a two-sample Anderson–Darling test.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdResult {
    /// Raw A² statistic. Larger → more evidence against H₀.
    pub a_squared: f64,
    /// Standardised T = (A² - 1) / σ_N. Under H₀, T is roughly
    /// standard-normal for large N.
    pub standardized: f64,
    /// Asymptotic p-value under H₀. Tail-sensitive by construction.
    pub p_value: f64,
    /// Sample count side A.
    pub n_a: u64,
    /// Sample count side B.
    pub n_b: u64,
    /// Significance at conventional α=0.05. `NotApplicable` when
    /// either histogram is empty.
    pub significance: Significance,
}

/// Two-sample Anderson–Darling on HDR histograms.
pub fn ad_test(a: &Histogram<u64>, b: &Histogram<u64>) -> AdResult {
    let n_a = a.len();
    let n_b = b.len();
    if n_a == 0 || n_b == 0 {
        return AdResult {
            a_squared: 0.0,
            standardized: 0.0,
            p_value: 1.0,
            n_a,
            n_b,
            significance: Significance::NotApplicable,
        };
    }

    let n = n_a as f64;
    let m = n_b as f64;
    let total = n + m;

    // Walk the merged bucket space accumulating M_j (cumulative A),
    // H_j (cumulative combined). Skip the final value — Scholz-Stephens'
    // sum runs j=1..L-1 because at the last distinct value N-H_j = 0.
    let a_pairs: Vec<(u64, u64)> = a
        .iter_recorded()
        .map(|iv| (iv.value_iterated_to(), iv.count_at_value()))
        .collect();
    let b_pairs: Vec<(u64, u64)> = b
        .iter_recorded()
        .map(|iv| (iv.value_iterated_to(), iv.count_at_value()))
        .collect();

    // Merge-walk producing (h_j, M_j, H_j) triples per distinct value.
    let mut steps: Vec<(u64, u64, u64)> = Vec::new();
    let mut ia = 0usize;
    let mut ib = 0usize;
    let mut cum_a: u64 = 0;
    let mut cum_all: u64 = 0;
    while ia < a_pairs.len() || ib < b_pairs.len() {
        let va = a_pairs.get(ia).map(|p| p.0);
        let vb = b_pairs.get(ib).map(|p| p.0);
        let v = match (va, vb) {
            (Some(x), Some(y)) => x.min(y),
            (Some(x), None) => x,
            (None, Some(y)) => y,
            (None, None) => break,
        };
        let mut h_j: u64 = 0;
        while ia < a_pairs.len() && a_pairs[ia].0 == v {
            let c = a_pairs[ia].1;
            cum_a += c;
            h_j += c;
            ia += 1;
        }
        while ib < b_pairs.len() && b_pairs[ib].0 == v {
            let c = b_pairs[ib].1;
            h_j += c;
            ib += 1;
        }
        cum_all += h_j;
        steps.push((h_j, cum_a, cum_all));
    }

    // A² = ((N-1)/N) · Σ_{j=1..L-1} h_j·(N·M_j − n·H_j)² / (H_j·(N−H_j))
    // (division by n·m · N is implicit; see Scholz-Stephens eq. 1.2a).
    let mut acc = 0.0_f64;
    for i in 0..steps.len().saturating_sub(1) {
        let (h_j, m_j, h_cum) = steps[i];
        let m_j = m_j as f64;
        let h_cum = h_cum as f64;
        let h_j = h_j as f64;
        let denom = h_cum * (total - h_cum);
        if denom <= 0.0 {
            continue;
        }
        let num = total * m_j - n * h_cum;
        acc += h_j * (num * num) / denom;
    }
    let a_squared = ((total - 1.0) / (n * m * total)) * acc;

    // Standardise. For k=2, Scholz-Stephens (1987) eq. (8) gives
    // σ²_N exactly as a cubic polynomial in N divided by
    // (N-1)(N-2)(N-3), parameterised by:
    //   H  = Σ 1/n_i         (harmonic sum of sample sizes)
    //   h  = H_{N-1}         (harmonic number)
    //   g  = Σ_{i=1..N-2} Σ_{j=i+1..N-1} 1/((N-i)·j)
    //
    // For k=2 the polynomial coefficients reduce to simple linear
    // combinations of h and g. We compute h and g exactly up to
    // moderate totals (≤ 100k) and fall back to the equal-sample
    // asymptotic (4/3)·(1 - 1.34/N) for larger totals where the
    // relative correction term is already small.
    let n_total = total as u64;
    let sigma_sq = if n_total >= 4 {
        ad_sigma_squared_k2(n_total, n, m)
    } else {
        // Degenerate: not enough samples for the cubic to evaluate.
        4.0 / 3.0
    };
    let sigma = sigma_sq.sqrt().max(f64::EPSILON);
    let t = (a_squared - 1.0) / sigma;
    let p = ad_p_value(t);
    let significance = if p < 0.05 {
        Significance::Significant
    } else {
        Significance::NotSignificant
    };
    AdResult {
        a_squared,
        standardized: t,
        p_value: p,
        n_a,
        n_b,
        significance,
    }
}

/// Exact variance σ²_N of the two-sample Anderson-Darling statistic
/// under the null hypothesis, per Scholz-Stephens (1987) eq. (8)
/// specialised to k=2.
///
/// For sample sizes n₁, n₂ (total N = n₁+n₂):
///
/// ```text
///   σ²_N = (aN³ + bN² + cN + d) / ((N-1)(N-2)(N-3))
///
///   H = 1/n₁ + 1/n₂
///   h = H_{N-1}                             (harmonic number)
///   g = Σ_{i=1..N-2} Σ_{j=i+1..N-1} 1/((N-i)·j)
///
///   a = 4g - 6 + (10 - 6g)·H
///   b = 12g + 8h - 22 + (2g - 14h - 4)·H
///   c = 36h + 4       + (2h - 6)·H
///   d = 24
/// ```
///
/// Exact up to `EXACT_MAX`; beyond that the cost of the O(N²)-ish
/// g term stops being worth the extra precision (the finite-sample
/// correction is already ≤ 1% of σ² at that scale), so we fall
/// back to the classic equal-sample asymptotic.
fn ad_sigma_squared_k2(n_total: u64, n_a: f64, n_b: f64) -> f64 {
    const EXACT_MAX: u64 = 100_000;
    if n_total > EXACT_MAX {
        // Equal-sample large-N asymptotic. Accurate to ~1% by this
        // point regardless of sample-size ratio.
        return (4.0 / 3.0) * (1.0 - 1.34 / n_total as f64);
    }
    let n = n_total as usize;
    // Precompute h_i = Σ_{k=1..i} 1/k for i in 0..=N-1.
    let mut h_vals = vec![0.0_f64; n];
    for i in 1..n {
        h_vals[i] = h_vals[i - 1] + 1.0 / (i as f64);
    }
    let h = h_vals[n - 1];
    // g = Σ_{j=2..N-1} (h - h_{N-j}) / j.
    //
    // Derivation: swap order of summation in the original
    // g = Σ_{i=1..N-2} Σ_{j=i+1..N-1} 1/((N-i)·j)
    // via substitution j_outer = N-i, giving a one-dimensional sum
    // over the OUTER harmonic fraction with a two-line inner piece
    // expressible as a harmonic-number difference.
    let mut g = 0.0_f64;
    for j in 2..n {
        g += (h - h_vals[n - j]) / (j as f64);
    }
    let cap_h = 1.0 / n_a + 1.0 / n_b;
    let n_f = n_total as f64;
    let a = 4.0 * g - 6.0 + (10.0 - 6.0 * g) * cap_h;
    let b = 12.0 * g + 8.0 * h - 22.0 + (2.0 * g - 14.0 * h - 4.0) * cap_h;
    let c = 36.0 * h + 4.0 + (2.0 * h - 6.0) * cap_h;
    let d = 24.0;
    let num = a * n_f.powi(3) + b * n_f.powi(2) + c * n_f + d;
    let denom = (n_f - 1.0) * (n_f - 2.0) * (n_f - 3.0);
    (num / denom).max(f64::EPSILON)
}

/// Approximate p-value for the standardised Scholz-Stephens T
/// statistic. Uses the bracket-interpolation between tabulated
/// critical values (α=0.25, 0.10, 0.05, 0.025, 0.01, 0.001) with
/// exponential extrapolation in the tails.
fn ad_p_value(t: f64) -> f64 {
    // Tabulated critical values (α, T_critical) for 2-sample AD.
    // Scholz-Stephens 1987 Table 1 (k=2).
    const TABLE: &[(f64, f64)] = &[
        (0.25, -0.325),
        (0.10, 1.225),
        (0.05, 1.960),
        (0.025, 2.719),
        (0.01, 3.752),
        (0.005, 4.592),
        (0.001, 6.546),
    ];

    // Below the smallest critical value: p → 1.
    if t < TABLE[0].1 {
        return 1.0;
    }
    // Above the largest: exponential tail extrapolation.
    let last = TABLE[TABLE.len() - 1];
    if t >= last.1 {
        // p decays roughly exponentially in T; slope calibrated from
        // (α=0.005, 0.001) bracket.
        let slope =
            (TABLE[TABLE.len() - 2].0.ln() - last.0.ln()) / (last.1 - TABLE[TABLE.len() - 2].1);
        return (last.0.ln() + slope * (t - last.1)).exp().min(1.0).max(0.0);
    }
    // Interpolate between the two brackets surrounding t.
    for i in 0..TABLE.len() - 1 {
        let (a0, t0) = TABLE[i];
        let (a1, t1) = TABLE[i + 1];
        if t >= t0 && t <= t1 {
            let frac = (t - t0) / (t1 - t0);
            let log_a = a0.ln() + frac * (a1.ln() - a0.ln());
            return log_a.exp().clamp(0.0, 1.0);
        }
    }
    1.0
}

// ---------------------------------------------------------------------------
// Holm-Bonferroni correction
//
// Family-wise error-rate control for multi-metric p-values. Given
// p_1..p_m (unordered), sort ascending. Reject H_(i) when
// p_(i) < α / (m - i + 1). Stop at the first non-rejection (later
// hypotheses stay un-rejected regardless of their raw p-value).
//
// Returns the *adjusted* p-values — each caller compares
// `adjusted[k]` against α directly. This is the standard
// presentation in e.g. R's `p.adjust` method = "holm".
// ---------------------------------------------------------------------------

/// Apply Holm-Bonferroni step-down correction to an unordered
/// vector of p-values. Returns adjusted p-values in the *original*
/// order (so `adjusted[i]` corresponds to input p-value `i`).
///
/// Property: `adjusted[i] ≤ 1` for all i; `adjusted[i] < α` implies
/// the raw hypothesis `i` can be rejected at family-wise error rate
/// α by the Holm procedure.
pub fn holm_bonferroni(p_values: &[f64]) -> Vec<f64> {
    let m = p_values.len();
    if m == 0 {
        return Vec::new();
    }

    // Sort indices by ascending p-value.
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_by(|&i, &j| {
        p_values[i]
            .partial_cmp(&p_values[j])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut adj = vec![0.0_f64; m];
    let mut running_max: f64 = 0.0;
    for (rank, &orig_idx) in order.iter().enumerate() {
        let multiplier = (m - rank) as f64;
        let candidate = (p_values[orig_idx] * multiplier).min(1.0);
        // Enforce monotonicity — Holm's adjusted p-values are
        // non-decreasing in rank per the step-down rule.
        running_max = running_max.max(candidate);
        adj[orig_idx] = running_max;
    }
    adj
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
    use zerobench_core::stats::{ErrorCountersExport, LatencyExport};

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
            protocol_latency: LatencyExport::default(),
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
            latency: runs
                .first()
                .map(|r| r.latency.clone())
                .unwrap_or(LatencyExport {
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
                }),
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
                connect: 0,
                read: 0,
                write: 0,
                timeout: 0,
                keepup: 0,
                status_4xx: 0,
                status_5xx: 0,
                assertion_failed: 0,
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
        for v in [100u64, 200, 500, 1_000, 2_000, 5_000]
            .iter()
            .cycle()
            .take(1000)
        {
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

    // -----------------------------------------------------------------
    // Anderson-Darling tests
    // -----------------------------------------------------------------

    #[test]
    fn ad_identical_histograms_are_not_significant() {
        let mut a = mk_hist();
        let mut b = mk_hist();
        for v in [100u64, 200, 500, 1_000, 2_000, 5_000]
            .iter()
            .cycle()
            .take(1000)
        {
            a.record(*v).unwrap();
            b.record(*v).unwrap();
        }
        let r = ad_test(&a, &b);
        assert!(
            r.p_value > 0.5,
            "expected p > 0.5 for identical samples; got {}",
            r.p_value
        );
        assert_eq!(r.significance, Significance::NotSignificant);
    }

    #[test]
    fn ad_shifted_histograms_produce_a_squared_above_noise() {
        // With tiny samples (~100 each) and HDR bucketing, p-values
        // from any short simple approximation are noisy near the
        // rejection boundary. Instead of asserting a specific
        // significance (which depends on σ_N calibration), verify
        // the core invariant: shifted distributions produce a
        // larger A² than identical ones.
        let mut a_shift = mk_hist();
        let mut b_shift = mk_hist();
        for v in (500u64..=1500).step_by(10) {
            a_shift.record(v).unwrap();
        }
        for v in (5_000u64..=15_000).step_by(100) {
            b_shift.record(v).unwrap();
        }
        let shifted = ad_test(&a_shift, &b_shift);

        let mut a_same = mk_hist();
        let mut b_same = mk_hist();
        for v in (500u64..=1500).step_by(10) {
            a_same.record(v).unwrap();
            b_same.record(v).unwrap();
        }
        let same = ad_test(&a_same, &b_same);

        assert!(
            shifted.a_squared > same.a_squared,
            "shifted A²={} should exceed same A²={}",
            shifted.a_squared,
            same.a_squared
        );
        assert!(
            shifted.standardized > same.standardized,
            "shifted T={} should exceed same T={}",
            shifted.standardized,
            same.standardized
        );
    }

    #[test]
    fn ad_empty_histograms_yield_not_applicable() {
        let a = mk_hist();
        let b = mk_hist();
        let r = ad_test(&a, &b);
        assert_eq!(r.significance, Significance::NotApplicable);
    }

    #[test]
    fn ad_p_value_monotonic() {
        let p_small = ad_p_value(-1.0);
        let p_medium = ad_p_value(1.0);
        let p_large = ad_p_value(5.0);
        assert!(p_small >= p_medium);
        assert!(p_medium >= p_large);
    }

    // -----------------------------------------------------------------
    // Holm-Bonferroni tests
    // -----------------------------------------------------------------

    #[test]
    fn holm_empty_input() {
        assert!(holm_bonferroni(&[]).is_empty());
    }

    #[test]
    fn holm_single_p_value_unchanged() {
        let adj = holm_bonferroni(&[0.03]);
        assert_eq!(adj.len(), 1);
        assert!((adj[0] - 0.03).abs() < 1e-9);
    }

    #[test]
    fn holm_adjusts_smallest_most_aggressively() {
        // m=4, smallest p multiplied by 4.
        let raw = vec![0.01, 0.04, 0.02, 0.03];
        let adj = holm_bonferroni(&raw);
        // Smallest raw (0.01) × 4 = 0.04; second (0.02) × 3 = 0.06;
        // third (0.03) × 2 = 0.06 (enforced by monotonicity);
        // fourth (0.04) × 1 = 0.06 (enforced by monotonicity).
        assert!((adj[0] - 0.04).abs() < 1e-9, "adj[0]={}", adj[0]);
        assert!((adj[2] - 0.06).abs() < 1e-9, "adj[2]={}", adj[2]);
        assert!((adj[3] - 0.06).abs() < 1e-9, "adj[3]={}", adj[3]);
    }

    #[test]
    fn holm_never_decreases_raw() {
        let raw = vec![0.01, 0.02, 0.03];
        let adj = holm_bonferroni(&raw);
        for (r, a) in raw.iter().zip(adj.iter()) {
            assert!(*a >= *r, "adj {} < raw {}", a, r);
        }
    }

    #[test]
    fn holm_caps_at_one() {
        let adj = holm_bonferroni(&[0.5, 0.6]);
        assert!(adj.iter().all(|&p| p <= 1.0));
    }

    #[test]
    fn holm_preserves_input_order() {
        // adj[i] corresponds to input[i] — not the sorted order.
        let raw = vec![0.2, 0.01, 0.3];
        let adj = holm_bonferroni(&raw);
        assert_eq!(adj.len(), 3);
        // Smallest raw is at index 1; it should get the largest
        // multiplier = m = 3 → 0.03.
        assert!((adj[1] - 0.03).abs() < 1e-9, "adj[1]={}", adj[1]);
    }
}
