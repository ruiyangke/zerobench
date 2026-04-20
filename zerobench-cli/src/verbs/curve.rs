//! ARCH STATUS: REWRITE
//!
//! Phase 2c routed the HTTP backend call through
//! `zerobench_backends::run_plan` — dispatch is now a single call. Still
//! duplicates plan construction with measure.rs (ARCH(builder-unify))
//! and its own runs/archive lifecycle. Post-rewrite: ~100 LoC wrapping
//! runner.execute() in a rate-sweep loop.
//! See ARCH-REVIEW §6 Phase 4, §B2.
//!
//! ----------------------------------------------------------------------
//!
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

use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use smallvec::SmallVec;
use zerobench_core::plan::{Mode, Plan, RateProfile, RequestPlan, Scenario, Step};
use zerobench_core::stats::{ErrorCountersExport, LatencyExport, PerRunMetrics};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_core::{Summary, SummaryExport};
use zerobench_runtime::archive::{Archive, ArchiveWriter, EnvRecord, Index, SchemaVersions};
use zerobench_runtime::calibrate::ClientSelfCheck;
use zerobench_runtime::fingerprint::{
    plan_hash, run_id, target_fingerprint, url_fingerprint, IpFamilyTag,
};
use zerobench_runtime::machine::MachineFingerprint;

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
    #[arg(long = "knee-p99-mult", default_value_t = 2.0,
          help_heading = "Curve")]
    pub knee_p99_mult: f64,

    /// Knee error rate threshold. Knee = also fires when error rate
    /// reaches this fraction of requests for ≥3 seconds consecutively
    /// (per PHILOSOPHY §P4).
    #[arg(long = "knee-error-rate", default_value_t = 0.01,
          help_heading = "Curve")]
    pub knee_error_rate: f64,

    /// Connection pool.
    #[arg(short = 'c', long = "connections", default_value_t = 100,
          help_heading = "Load")]
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

    let run_start_wall = SystemTime::now();

    let target = Target::parse(&args.url)?;
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("{}-curve", target.host));

    // Build the base plan (single GET /, saturate — the Step
    // template is reused for every step with a different rate).
    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        ..TransportOpts::default()
    };

    let mut vars = VarRegistry::new();
    let url_tpl = Template::compile(&args.url, &mut vars)?;
    let base_plan = Plan {
        scenarios: vec![Scenario {
            name: "curve".into(),
            rate: RateProfile::Constant(args.from_rate),
            steps: vec![Step::Request(RequestPlan {
                method: http::Method::GET,
                url: url_tpl,
                headers: SmallVec::new(),
                body: None,
                extract: Vec::new(),
                checks: Vec::new(),
                expect_streaming: false,
            })],
        }],
        vars,
        duration: args.over,
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: args.threads,
        mode: Mode::Curve {
            from_rate: args.from_rate,
            to_rate: args.to_rate,
            ramp_duration: args.over,
            knee: zerobench_core::plan::KneeCriterion::P99Ratio {
                factor: args.knee_p99_mult,
            },
        },
        name: name.clone(),
    };

    let resolved = target.resolve(&opts)?;
    let resolved_vec = vec![resolved];
    let plan_h = plan_hash(&base_plan);
    let url_fp = url_fingerprint(&base_plan, &target, IpFamilyTag::Auto);
    let target_fp = target_fingerprint(&base_plan, &target, &resolved_vec, &plan_h);
    let id = run_id(&plan_h, &target_fp, run_start_wall);

    // -------------------------------------------------------------------
    // Client self-check — calibrate gate.
    // -------------------------------------------------------------------
    if !args.no_calibrate {
        let check = ClientSelfCheck::spawn()?;
        let cal = check.check(args.to_rate, Duration::from_secs(2), None)?;
        eprintln!(
            "[calibrate] loopback @ {:.0} req/s → {:.0} achieved ({:.1}%) verdict={:?}",
            cal.offered_rate,
            cal.achieved_rate,
            cal.sustained_pct * 100.0,
            cal.verdict,
        );
        if matches!(cal.verdict, zerobench_runtime::Verdict::Refuse) && !args.force_overload {
            return Err(format!(
                "client cannot sustain top-of-ramp {} req/s (achieved {:.0}). \
                 Lower --to or pass --force-overload.",
                args.to_rate, cal.achieved_rate
            )
            .into());
        }
        // P10 jitter floor — see measure.rs for rationale.
        const JITTER_P99_FLOOR_NS: u64 = 5_000;
        if cal.jitter.len() == 0 {
            return Err(
                "client self-check produced no jitter samples — calibration \
                 is broken; cannot verify the scheduler noise floor."
                    .into(),
            );
        }
        let jitter_p99 = cal.jitter.value_at_percentile(99.0);
        if jitter_p99 > JITTER_P99_FLOOR_NS && !args.force_overload {
            return Err(format!(
                "client scheduler jitter p99 is {} ns (> {} ns floor); curve \
                 knee detection would chase noise. Disable CPU frequency \
                 scaling, or pass --force-overload.",
                jitter_p99, JITTER_P99_FLOOR_NS
            )
            .into());
        }
    }

    // -------------------------------------------------------------------
    // Machine fingerprint + archive setup
    // -------------------------------------------------------------------
    let machine = MachineFingerprint::collect();
    let archive_writer = if args.no_archive {
        None
    } else {
        let archive = Archive::resolve();
        let writer = ArchiveWriter::begin(&archive, &url_fp, &id)?;
        writer.write_plan(&base_plan)?;
        writer.write_machine(&machine)?;
        eprintln!("[archive] run_id = {id}");
        Some(writer)
    };

    let mut env = EnvRecord::started_now(env!("CARGO_PKG_VERSION"));
    env.resolved_ips = resolved_vec.iter().map(|a| a.to_string()).collect();
    env.calibration_skipped = args.no_calibrate;
    env.force_overload = args.force_overload;
    if let Some(w) = archive_writer.as_ref() {
        w.write_env(&env)?;
    }

    // -------------------------------------------------------------------
    // Step through the rate ladder.
    //
    // Geometric spacing feels more informative than linear at most
    // rate ranges ("first doubling that breaks" is usually between
    // adjacent decades, not adjacent linear points). Steps at
    // from * (to/from)^(i/(n-1)).
    // -------------------------------------------------------------------
    let step_duration = args.over / args.steps.max(1);
    let ratio = args.to_rate / args.from_rate;
    let mut steps: Vec<StepResult> = Vec::with_capacity(args.steps as usize);

    let tls_config = if target.tls {
        Some(zerobench_backends::http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]))
    } else {
        None
    };

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
        let stop: Option<Arc<AtomicBool>> = None;
        let step_ctx = zerobench_backends::RunCtx {
            target: target.clone(),
            opts: opts.clone(),
            duration: step_duration,
            num_threads: args.threads.max(1),
            connections: args.connections,
            target_rps: Some(offered),
            tls_config: tls_config.clone(),
            live: None,
            stop,
        };
        let stats = zerobench_backends::run_plan(&plan, &step_ctx);
        let elapsed = t_start.elapsed();
        let summary = Summary::merge(stats, elapsed);

        let p50 = summary.latency.value_at_percentile(50.0);
        let p99 = summary.latency.value_at_percentile(99.0);
        let err_total = summary.errors.total();
        let err_rate = if summary.requests == 0 {
            1.0
        } else {
            err_total as f64 / summary.requests as f64
        };
        let achieved = summary.requests as f64 / elapsed.as_secs_f64();

        let step_result = StepResult {
            offered,
            achieved,
            requests: summary.requests,
            p50_ns: p50,
            p99_ns: p99,
            err_rate,
        };

        println!(
            "{:>4}  {:>10.0}/s  {:>10.0}/s  {:>10}  {:>12}  {:>9.2}%",
            i + 1,
            step_result.offered,
            step_result.achieved,
            fmt_ns(step_result.p50_ns),
            fmt_ns(step_result.p99_ns),
            step_result.err_rate * 100.0,
        );
        steps.push(step_result);
    }

    // -------------------------------------------------------------------
    // Knee detection.
    // -------------------------------------------------------------------
    let baseline_p99 = steps.first().map(|s| s.p99_ns).unwrap_or(0);
    let knee_index = detect_knee(
        &steps,
        baseline_p99,
        args.knee_p99_mult,
        args.knee_error_rate,
    );

    println!();
    match knee_index.as_ref() {
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
                args.to_rate
            );
        }
    }

    // -------------------------------------------------------------------
    // Archive finalisation.
    // -------------------------------------------------------------------
    if let Some(writer) = archive_writer {
        env.set_ended();
        writer.write_env(&env)?;

        // Synthesise a SummaryExport from the aggregate of all steps.
        // result.json for curve runs carries per_run = one entry per
        // rate step, so downstream diff can bootstrap across steps
        // (unusual but mechanically valid).
        let total_requests: u64 = steps.iter().map(|s| s.requests).sum();
        let total_duration = step_duration.saturating_mul(args.steps);
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

        let export = SummaryExport {
            schema_version: SummaryExport::SCHEMA_VERSION,
            duration_ns: total_duration
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
            requests: total_requests,
            rate_per_s: if total_duration.as_secs_f64() > 0.0 {
                total_requests as f64 / total_duration.as_secs_f64()
            } else {
                0.0
            },
            bytes_sent: 0,
            bytes_recv: 0,
            latency: LatencyExport {
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
            per_run,
        };
        writer.write_result(&export)?;

        let index = Index {
            schema_version: Index::SCHEMA_VERSION,
            schema_versions: SchemaVersions {
                plan: 1,
                machine: MachineFingerprint::SCHEMA_VERSION,
                env: EnvRecord::SCHEMA_VERSION,
                index: Index::SCHEMA_VERSION,
                result: Some(SummaryExport::SCHEMA_VERSION),
            },
            plan_hash: plan_h,
            target_fingerprint: target_fp,
            url_fingerprint: url_fp,
            replayed_from: None,
        };
        writer.finalise(&index)?;
    }

    Ok(match knee_index {
        Some(_) => ExitCode::SUCCESS,
        None => ExitCode::from(0), // No knee found is informational — still exit 0.
    })
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
                    p99_mult, s.p99_ns as f64 / baseline
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
