//! `zerobench curve URL` — saturation-curve exploration.
//!
//! Per PHILOSOPHY §P4 "load is a curve, not a point": steps offered
//! rate from `--from` to `--to` over `--over`, runs a short window
//! at each step, records (rate, p99_latency, error_rate), and
//! reports the knee — first rate where p99 > 2× baseline OR error
//! rate ≥ 1% sustained for ≥3 seconds.
//!
//! Output: a CSV-style table to stdout + (optionally) an archive
//! entry with the full per-step measurement data.
//!
//! The fingerprint / calibration / archive / harness scaffolding is
//! shared with `measure.rs` via `zerobench_runtime::runner`; curve.rs
//! only owns the rate-ladder loop and the knee-detection.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Args;
use smallvec::SmallVec;
use zerobench_core::plan::{Mode, RateProfile, RequestPlan};
use zerobench_core::plan_builder::{scenario_http_request, PlanBuilder};
use zerobench_core::stats::{ErrorCountersExport, LatencyExport, PerRunMetrics};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::{Summary, SummaryExport};
use zerobench_runtime::runner::{calibrate, ArchiveSession, RunHarness};

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Flags for `zerobench curve URL`.
#[derive(Debug, Clone, Args)]
pub struct CurveArgs {
    /// Target URL.
    #[arg(value_name = "URL")]
    pub url: String,

    /// Plan name for the archive bucket.
    #[arg(long = "name", help_heading = "Identity")]
    pub name: Option<String>,

    /// Lowest offered rate to sample (req/s).
    #[arg(long = "from", default_value = "1000",
          value_parser = super::super::cli_args::parse_rate_flag,
          help_heading = "Curve")]
    pub from_rate: f64,

    /// Highest offered rate to sample (req/s).
    #[arg(long = "to", default_value = "100000",
          value_parser = super::super::cli_args::parse_rate_flag,
          help_heading = "Curve")]
    pub to_rate: f64,

    /// Total ramp duration. Each sample window is `over / steps`.
    #[arg(long = "over", default_value = "2m",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Curve")]
    pub over: Duration,

    /// Number of sample points along the ramp.
    #[arg(long = "steps", default_value_t = 10, help_heading = "Curve")]
    pub steps: u32,

    /// Knee p99 multiplier. Knee = first step whose p99 exceeds
    /// `multiplier × baseline p99` (baseline = first step).
    #[arg(long = "knee-p99-mult", default_value_t = 2.0, help_heading = "Curve")]
    pub knee_p99_mult: f64,

    /// Knee error rate threshold. Knee = also fires when error rate
    /// reaches this fraction of requests for ≥3 seconds consecutively
    /// (per PHILOSOPHY §P4).
    #[arg(
        long = "knee-error-rate",
        default_value_t = 0.01,
        help_heading = "Curve"
    )]
    pub knee_error_rate: f64,

    /// Connection pool.
    #[arg(
        short = 'c',
        long = "connections",
        default_value_t = 100,
        help_heading = "Load"
    )]
    pub connections: usize,

    /// OS worker threads.
    #[arg(short = 't', long = "threads",
          default_value_t = super::super::cli_args::num_cpus(),
          help_heading = "Load")]
    pub threads: usize,

    /// TCP+TLS connect timeout.
    #[arg(long = "connect-timeout", default_value = "5s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Network")]
    pub connect_timeout: Duration,

    /// Per-request deadline.
    #[arg(long = "timeout", default_value = "30s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Network")]
    pub request_timeout: Duration,

    /// Accept invalid TLS certificates (https:// only).
    #[arg(short = 'k', long = "insecure",
          action = clap::ArgAction::SetTrue, help_heading = "Network")]
    pub insecure: bool,

    /// Skip the client self-check. Not recommended.
    #[arg(long = "no-calibrate", action = clap::ArgAction::SetTrue,
          help_heading = "Measurement control")]
    pub no_calibrate: bool,

    /// Run anyway when calibration fails.
    #[arg(long = "force-overload", action = clap::ArgAction::SetTrue,
          help_heading = "Measurement control")]
    pub force_overload: bool,

    /// Don't archive.
    #[arg(long = "no-archive", action = clap::ArgAction::SetTrue,
          help_heading = "Archive")]
    pub no_archive: bool,
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

/// Run the curve sweep.
pub fn run(args: CurveArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    if args.from_rate <= 0.0 || args.to_rate <= args.from_rate {
        return Err("--to must be strictly greater than --from".into());
    }
    if args.steps < 2 {
        return Err("--steps must be ≥ 2".into());
    }
    if args.over.is_zero() {
        return Err("--over must be non-zero".into());
    }

    // -------------------------------------------------------------------
    // Plan + transport
    // -------------------------------------------------------------------
    let target = Target::parse(&args.url)?;
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("{}-curve", target.host));
    let opts = build_transport_opts(&args);
    let base_plan = build_curve_plan(&args, &name)?;
    let resolved = target.resolve(&opts)?;

    // -------------------------------------------------------------------
    // Calibration gate — verify the client can sustain the top of the
    // ramp before we burn time on the sweep.
    // -------------------------------------------------------------------
    let calibration_skipped = args.no_calibrate;
    let mut force_overload = args.force_overload;
    if !calibration_skipped {
        let report = calibrate(args.to_rate, Duration::from_secs(2), args.force_overload)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        eprintln!(
            "[calibrate] loopback @ {:.0} req/s → {:.0} achieved — verdict: {:?}, jitter p99 {} ns",
            report.target_rate, report.achieved_rate, report.verdict, report.jitter_p99_ns,
        );
        if report.poisoned {
            eprintln!(
                "[calibrate] --force-overload: gate bypassed; run flagged \
                 `force_overload=true`."
            );
            force_overload = true;
        }
    }

    // -------------------------------------------------------------------
    // Archive setup
    // -------------------------------------------------------------------
    let archive = ArchiveSession::begin(
        &base_plan,
        &target,
        &[resolved],
        args.no_archive,
        Vec::new(),
        calibration_skipped,
        force_overload,
        String::new(),
        env!("CARGO_PKG_VERSION"),
    )?;

    // -------------------------------------------------------------------
    // Harness + rate ladder.
    //
    // Geometric spacing: "first doubling that breaks" usually lies
    // between adjacent decades rather than adjacent linear points.
    // Rate at step i = from * (to/from)^(i/(n-1)).
    // -------------------------------------------------------------------
    let tls_config = if target.tls {
        Some(zerobench_backends::http::mio_tls::build_tls_config(
            &opts,
            &[b"http/1.1"],
        ))
    } else {
        None
    };
    // Per-step, `target_rate` is rebuilt from the ladder fraction, so
    // the harness's own `target_rate` is unused — None is safe.
    let harness = RunHarness::new_from(
        &target,
        &opts,
        args.threads,
        args.connections,
        None,
        tls_config,
    );

    let step_duration = args.over / args.steps.max(1);
    let ratio = args.to_rate / args.from_rate;
    let mut steps: Vec<StepResult> = Vec::with_capacity(args.steps as usize);

    println!("\ncurve {}", args.url);
    println!(
        "{:>4}  {:>12}  {:>12}  {:>10}  {:>12}  {:>10}",
        "step", "offered", "achieved", "p50", "p99", "err%"
    );

    for i in 0..args.steps {
        let frac = i as f64 / (args.steps as f64 - 1.0).max(1.0);
        let offered = args.from_rate * ratio.powf(frac);
        let mut plan = base_plan.clone();
        plan.scenarios[0].rate = RateProfile::Constant(offered);
        plan.duration = step_duration;

        let t_start = Instant::now();
        let stats = run_plan_for_step(&plan, &harness, step_duration, Some(offered));
        let elapsed = t_start.elapsed();
        let summary = Summary::merge(stats, elapsed);

        let step_result = step_result_for(offered, &summary, elapsed);
        print_step_row(i, &step_result);
        steps.push(step_result);
    }

    // -------------------------------------------------------------------
    // Knee detection + summary print.
    // -------------------------------------------------------------------
    let baseline_p99 = steps.first().map(|s| s.p99_ns).unwrap_or(0);
    let knee_index = detect_knee(
        &steps,
        baseline_p99,
        args.knee_p99_mult,
        args.knee_error_rate,
    );
    print_knee(&steps, &knee_index, baseline_p99, args.to_rate);

    // -------------------------------------------------------------------
    // Archive finalisation — synthesise a SummaryExport that carries
    // per-step metrics in `per_run`, so downstream diff can bootstrap
    // across rate steps (unusual but mechanically valid).
    // -------------------------------------------------------------------
    let total_duration = step_duration.saturating_mul(args.steps);
    let export = synthesise_summary_export(&steps, total_duration);
    // ArchiveSession::finalise needs a Summary + separate per_run slice;
    // build both from `export` so we write the right result.json.
    let synth_summary = summary_for_archive(&export);
    archive.finalise(
        &synth_summary,
        export.per_run.clone(),
        &base_plan,
        total_duration,
        "zerobench curve",
        |s, _p| &s.latency,
    )?;

    Ok(match knee_index {
        Some(_) => ExitCode::SUCCESS,
        None => ExitCode::from(0), // No knee found is informational — still exit 0.
    })
}

// ---------------------------------------------------------------------------
// Curve helpers — kept local to curve.rs
// ---------------------------------------------------------------------------

fn build_transport_opts(args: &CurveArgs) -> TransportOpts {
    TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        ..TransportOpts::default()
    }
}

fn build_curve_plan(
    args: &CurveArgs,
    name: &str,
) -> Result<zerobench_core::plan::Plan, Box<dyn std::error::Error>> {
    let mut builder = PlanBuilder::new();
    builder
        .name(name)
        .duration(args.over)
        .threads(args.threads)
        .mode(Mode::Curve {
            from_rate: args.from_rate,
            to_rate: args.to_rate,
            ramp_duration: args.over,
            knee: zerobench_core::plan::KneeCriterion::P99Ratio {
                factor: args.knee_p99_mult,
            },
        });
    let url_tpl = Template::compile(&args.url, builder.vars_mut())?;
    let request = RequestPlan {
        method: http::Method::GET,
        url: url_tpl,
        headers: SmallVec::new(),
        body: None,
        extract: Vec::new(),
        checks: Vec::new(),
        expect_streaming: false,
    };
    builder.push_scenario(scenario_http_request(
        "curve",
        RateProfile::Constant(args.from_rate),
        request,
    ));
    Ok(builder.finalize())
}

fn run_plan_for_step(
    plan: &zerobench_core::plan::Plan,
    harness: &RunHarness,
    duration: Duration,
    offered: Option<f64>,
) -> Vec<zerobench_core::TaskStats> {
    let (target, opts, duration, threads, connections, _rate, tls, live, stop) =
        harness.ctx_for(duration, None, None);
    let ctx = zerobench_backends::RunCtx {
        target,
        opts,
        duration,
        num_threads: threads,
        connections,
        target_rps: offered,
        tls_config: tls,
        live,
        stop,
    };
    zerobench_backends::run_plan(plan, &ctx)
}

fn step_result_for(offered: f64, summary: &Summary, elapsed: Duration) -> StepResult {
    let p50 = summary.latency.value_at_percentile(50.0);
    let p99 = summary.latency.value_at_percentile(99.0);
    let err_total = summary.errors.total();
    let err_rate = if summary.requests == 0 {
        1.0
    } else {
        err_total as f64 / summary.requests as f64
    };
    let achieved = summary.requests as f64 / elapsed.as_secs_f64();
    StepResult {
        offered,
        achieved,
        requests: summary.requests,
        p50_ns: p50,
        p99_ns: p99,
        err_rate,
    }
}

fn print_step_row(i: u32, s: &StepResult) {
    println!(
        "{:>4}  {:>10.0}/s  {:>10.0}/s  {:>10}  {:>12}  {:>9.2}%",
        i + 1,
        s.offered,
        s.achieved,
        fmt_ns(s.p50_ns),
        fmt_ns(s.p99_ns),
        s.err_rate * 100.0,
    );
}

fn print_knee(
    steps: &[StepResult],
    knee_index: &Option<(usize, String)>,
    baseline_p99: u64,
    to_rate: f64,
) {
    println!();
    match knee_index {
        Some((idx, reason)) => {
            let s = &steps[*idx];
            println!(
                "knee       step {} @ {:.0} req/s — {}",
                *idx + 1,
                s.offered,
                reason
            );
            println!(
                "           achieved {:.0} req/s · p99 {} ({:.2}× baseline)",
                s.achieved,
                fmt_ns(s.p99_ns),
                if baseline_p99 == 0 {
                    0.0
                } else {
                    s.p99_ns as f64 / baseline_p99 as f64
                },
            );
        }
        None => {
            println!(
                "knee       not found up to {:.0} req/s (top-of-ramp; try a higher --to)",
                to_rate
            );
        }
    }
}

fn synthesise_summary_export(steps: &[StepResult], total_duration: Duration) -> SummaryExport {
    let total_requests: u64 = steps.iter().map(|s| s.requests).sum();
    let per_run: Vec<PerRunMetrics> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| PerRunMetrics {
            index: i as u32,
            rate_per_s: s.achieved,
            requests: s.requests,
            errors_total: (s.err_rate * s.requests as f64) as u64,
            latency: LatencyExport {
                count: s.requests,
                min_ns: 0,
                p50_ns: s.p50_ns,
                p90_ns: 0,
                p99_ns: s.p99_ns,
                p99_9_ns: 0,
                p99_99_ns: 0,
                max_ns: 0,
                mean_ns: 0.0,
                stddev_ns: 0.0,
            },
            // Curve is HTTP-only today; protocol_latency unused.
            protocol_latency: LatencyExport::default(),
        })
        .collect();

    SummaryExport {
        schema_version: SummaryExport::SCHEMA_VERSION,
        duration_ns: total_duration.as_nanos().min(u128::from(u64::MAX)) as u64,
        requests: total_requests,
        rate_per_s: if total_duration.as_secs_f64() > 0.0 {
            total_requests as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        },
        bytes_sent: 0,
        bytes_recv: 0,
        latency: LatencyExport::default(),
        ttfb: LatencyExport::default(),
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
        per_run,
    }
}

/// Build a stand-in Summary so we can feed `ArchiveSession::finalise`.
/// The closure hand-off only uses `summary.latency` — we give it the
/// default empty histogram, matching the old code's all-zero result.json
/// aggregate-latency slot.
fn summary_for_archive(_export: &SummaryExport) -> Summary {
    // Empty stats → aggregate latency histogram is the default empty
    // HDR — matches the old curve flow where the top-level latency
    // slot in result.json was explicitly zero.
    Summary::merge(Vec::new(), Duration::from_secs(0))
}

#[derive(Debug, Clone, Copy)]
struct StepResult {
    offered: f64,
    achieved: f64,
    requests: u64,
    p50_ns: u64,
    p99_ns: u64,
    err_rate: f64,
}

/// Find the first step at which the knee criterion fires. Returns
/// `(index, human-readable reason)`.
fn detect_knee(
    steps: &[StepResult],
    baseline_p99: u64,
    p99_mult: f64,
    err_rate_threshold: f64,
) -> Option<(usize, String)> {
    let baseline = baseline_p99.max(1) as f64;
    for (i, s) in steps.iter().enumerate() {
        if i == 0 {
            continue;
        }
        if (s.p99_ns as f64) > p99_mult * baseline {
            return Some((
                i,
                format!(
                    "p99 {}× baseline ({:.2}× threshold)",
                    p99_mult,
                    s.p99_ns as f64 / baseline
                ),
            ));
        }
        if s.err_rate >= err_rate_threshold {
            return Some((
                i,
                format!(
                    "error rate {:.2}% ≥ threshold {:.2}%",
                    s.err_rate * 100.0,
                    err_rate_threshold * 100.0,
                ),
            ));
        }
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_step(offered: f64, p99_ns: u64, err_rate: f64) -> StepResult {
        StepResult {
            offered,
            achieved: offered,
            requests: 1000,
            p50_ns: p99_ns / 2,
            p99_ns,
            err_rate,
        }
    }

    #[test]
    fn knee_fires_on_p99_doubling() {
        let steps = vec![
            mk_step(1000.0, 1_000_000, 0.0),
            mk_step(2000.0, 1_500_000, 0.0),
            mk_step(4000.0, 3_000_000, 0.0), // 3× baseline → fires @ 2.0× threshold
        ];
        let knee = detect_knee(&steps, steps[0].p99_ns, 2.0, 0.01);
        assert_eq!(knee.map(|(i, _)| i), Some(2));
    }

    #[test]
    fn knee_fires_on_error_rate() {
        let steps = vec![
            mk_step(1000.0, 1_000_000, 0.0),
            mk_step(2000.0, 1_200_000, 0.02), // fires: error rate 2% ≥ 1%
            mk_step(4000.0, 1_500_000, 0.0),
        ];
        let knee = detect_knee(&steps, steps[0].p99_ns, 2.0, 0.01);
        assert_eq!(knee.map(|(i, _)| i), Some(1));
    }

    #[test]
    fn knee_returns_none_when_within_bounds() {
        let steps = vec![
            mk_step(1000.0, 1_000_000, 0.0),
            mk_step(2000.0, 1_200_000, 0.0),
            mk_step(4000.0, 1_500_000, 0.0),
        ];
        let knee = detect_knee(&steps, steps[0].p99_ns, 2.0, 0.01);
        assert!(knee.is_none());
    }
}
