//! `zerobench measure URL` — the v0.1.0 headline verb.
//!
//! Implements `docs/design-v0.1.0.md` §2.3 and PHILOSOPHY §5. Ties
//! together Phases 1-5 of the v0.1.0 rollout:
//!
//! 1. Build a [`Plan`] with `mode = Mode::Measure`, the caller's
//!    duration / warmup / cooldown / runs.
//! 2. Compute `plan_hash`, `url_fingerprint`, `target_fingerprint`,
//!    `run_id` (Phase 3).
//! 3. Run the [`ClientSelfCheck`] (Phase 2) against loopback — refuse
//!    the run when the client can't sustain the offered rate, unless
//!    `--force-overload` was passed.
//! 4. Collect [`MachineFingerprint`] (Phase 4). Refuse when the
//!    monotonic clock is coarser than 10 µs unless
//!    `--allow-coarse-clock` was passed.
//! 5. Open [`ArchiveWriter`] (Phase 5a) and write `plan.json`,
//!    `machine.json`, `env.json` before the real run starts.
//! 6. Dispatch `runs` consecutive benchmark runs to the existing
//!    v0.0.1 HTTP backend, with `cooldown` between runs.
//! 7. Merge per-run stats into a [`Summary`]; print a compact report.
//! 8. Stamp `env.ended_at_unix`, rewrite `env.json`, finalise
//!    `INDEX.json`.
//!
//! Phase 6 replaces the backend dispatch with protocol-native SSE/WS
//! paths. Phase 5b adds `result.json` + `.histlog` emission. For now,
//! the verb proves the Phase 1-5 machinery against HTTP targets.

use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::{ArgAction, Args};
use smallvec::SmallVec;

use zerobench_core::archive::{Archive, ArchiveWriter, EnvRecord, Index, SchemaVersions};
use zerobench_core::calibrate::{ClientSelfCheck, Verdict};
use zerobench_core::fingerprint::{
    plan_hash, run_id, target_fingerprint, url_fingerprint, IpFamilyTag,
};
use zerobench_core::machine::MachineFingerprint;
use zerobench_core::plan::{Mode, Plan, RateProfile, RequestPlan, Scenario, Step};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_core::stats::{ErrorCountersExport, LatencyExport, PerRunMetrics};
use zerobench_core::{ColorChoice, Summary, SummaryExport, TaskStats};

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Flags accepted by `zerobench measure URL [flags]`.
///
/// Intentionally a narrower surface than the top-level `CliArgs` — the
/// measure verb is opinionated about defaults (60s × 3 runs + warmup +
/// cooldown). Users who want one-shot smoke tests use `probe`; users
/// who want the knee of the saturation curve use `curve`.
#[derive(Debug, Clone, Args)]
pub struct MeasureArgs {
    /// Target URL (HTTP/HTTPS). SSE/WS targets land in Phase 6.
    #[arg(value_name = "URL")]
    pub url: String,

    /// Plan name — contributes to `url_fingerprint` per §7.1, so all
    /// runs against the same service-as-named land in the same
    /// archive bucket. Defaults to the target host.
    #[arg(long = "name", help_heading = "Identity")]
    pub name: Option<String>,

    /// Steady-state duration per run (`60s`, `2m`, `30s`).
    #[arg(short = 'd', long = "duration", default_value = "60s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Load")]
    pub duration: Duration,

    /// Warmup duration before each run (stats discarded).
    #[arg(long = "warmup", default_value = "15s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Load")]
    pub warmup: Duration,

    /// Cooldown between consecutive runs (TIME_WAIT drain).
    #[arg(long = "cooldown", default_value = "10s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Load")]
    pub cooldown: Duration,

    /// Number of consecutive runs (for bootstrap CI aggregation).
    #[arg(long = "runs", default_value_t = 3, help_heading = "Load")]
    pub runs: u32,

    /// Open-loop target rate in req/s. Omitted → saturate mode
    /// (closed-loop, N workers).
    #[arg(short = 'r', long = "rate",
          value_parser = super::super::cli_args::parse_rate_flag,
          help_heading = "Load")]
    pub rate: Option<f64>,

    /// Max concurrent connections (H1) or streams (H2).
    #[arg(short = 'c', long = "connections", default_value_t = 50,
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
    #[arg(short = 'k', long = "insecure", action = ArgAction::SetTrue,
          help_heading = "Network")]
    pub insecure: bool,

    /// Skip the loopback self-check (P5). Not recommended — stamps
    /// `calibration_skipped: true` in env.json, poisoning comparisons.
    #[arg(long = "no-calibrate", action = ArgAction::SetTrue,
          help_heading = "Measurement control")]
    pub no_calibrate: bool,

    /// Run anyway when self-check shows the client cannot sustain the
    /// requested rate. Stamps `force_overload: true` in env.json.
    #[arg(long = "force-overload", action = ArgAction::SetTrue,
          help_heading = "Measurement control")]
    pub force_overload: bool,

    /// Run anyway when the monotonic clock resolution is > 10 µs.
    /// Stamps `clock_coarse: true` in env.json.
    #[arg(long = "allow-coarse-clock", action = ArgAction::SetTrue,
          help_heading = "Measurement control")]
    pub allow_coarse_clock: bool,

    /// Disable archive writing for this run (no `$ZEROBENCH_HOME`
    /// write). Result still renders to stdout.
    #[arg(long = "no-archive", action = ArgAction::SetTrue,
          help_heading = "Archive")]
    pub no_archive: bool,

    /// User-defined `KEY=VAL` pair recorded in env.json. Repeatable.
    #[arg(long = "context", value_parser = parse_kv_pair,
          help_heading = "Archive")]
    pub context: Vec<(String, String)>,
}

fn parse_kv_pair(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("--context expects KEY=VAL, got `{s}`"))
}

// ---------------------------------------------------------------------------
// Verb entry
// ---------------------------------------------------------------------------

/// Execute a `measure` invocation end-to-end.
pub fn run(args: MeasureArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    // -------------------------------------------------------------------
    // Plan construction
    // -------------------------------------------------------------------

    // Capture the wall-clock start for .histlog interval timestamps.
    let run_start_wall = SystemTime::now();

    let target = Target::parse(&args.url)?;
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| target.host.clone());

    let plan = build_measure_plan(&args, &target, &name)?;

    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        ..TransportOpts::default()
    };

    // -------------------------------------------------------------------
    // Fingerprints + run_id
    // -------------------------------------------------------------------

    let resolved = target.resolve(&opts)?;
    let resolved_vec = vec![resolved];

    let plan_h = plan_hash(&plan);
    let url_fp = url_fingerprint(&plan, &target, IpFamilyTag::Auto);
    let target_fp = target_fingerprint(&plan, &target, &resolved_vec, &plan_h);
    let id = run_id(&plan_h, &target_fp, SystemTime::now());

    // -------------------------------------------------------------------
    // Phase 2: client self-check
    // -------------------------------------------------------------------

    let calibration_skipped = args.no_calibrate;
    if !calibration_skipped {
        let target_rate = args.rate.unwrap_or_else(|| {
            // Saturate mode has no nominal rate; calibrate against
            // a conservative 10k/s baseline to prove the scheduler
            // isn't the bottleneck on this machine.
            10_000.0
        });
        eprintln!(
            "[calibrate] self-check at {:.0} req/s against loopback (~2s)...",
            target_rate
        );
        let check = ClientSelfCheck::spawn()?;
        let result = check.check(target_rate, Duration::from_secs(2), None)?;
        eprintln!(
            "[calibrate] achieved {:.0}/{:.0} req/s ({:.1}%) — verdict: {:?}, jitter p99 {:?} ns",
            result.achieved_rate,
            target_rate,
            result.sustained_pct * 100.0,
            result.verdict,
            result
                .jitter
                .value_at_percentile(99.0),
        );
        if matches!(result.verdict, Verdict::Refuse) && !args.force_overload {
            return Err(format!(
                "client cannot sustain {:.0} req/s on this machine (achieved {:.0}). \
                 Lower --rate, pass --no-calibrate to skip this gate, \
                 or pass --force-overload to run anyway.",
                target_rate, result.achieved_rate
            )
            .into());
        }
        if matches!(result.verdict, Verdict::Refuse) {
            eprintln!(
                "[calibrate] --force-overload: proceeding despite insufficient client ceiling. \
                 Run is flagged `force_overload=true` and will poison comparisons."
            );
        }
    }

    // -------------------------------------------------------------------
    // Phase 4: machine fingerprint
    // -------------------------------------------------------------------

    let machine = MachineFingerprint::collect();
    if machine.clock_is_coarse() && !args.allow_coarse_clock {
        return Err(format!(
            "monotonic clock resolution is {} ns (> 10 µs). Sub-µs percentiles \
             will be quantisation artefacts. Pass --allow-coarse-clock to run anyway.",
            machine
                .clock_monotonic_ns_resolution
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into())
        )
        .into());
    }

    // -------------------------------------------------------------------
    // Phase 5a: archive setup — writes plan/machine/env before the run.
    // -------------------------------------------------------------------

    let archive_writer = if args.no_archive {
        None
    } else {
        let archive = Archive::resolve();
        let writer = ArchiveWriter::begin(&archive, &url_fp, &id)?;
        writer.write_plan(&plan)?;
        writer.write_machine(&machine)?;
        eprintln!("[archive] run_id = {id}");
        eprintln!("[archive] dir = {}", writer.dir().display());
        Some(writer)
    };

    let mut env = EnvRecord::started_now(env!("CARGO_PKG_VERSION"));
    env.tool_features = enabled_features();
    env.resolved_ips = resolved_vec.iter().map(|a| a.to_string()).collect();
    env.context = args.context.clone();
    env.force_overload = args.force_overload;
    env.calibration_skipped = calibration_skipped;
    if let Some(writer) = archive_writer.as_ref() {
        writer.write_env(&env)?;
    }

    // -------------------------------------------------------------------
    // Runs loop
    // -------------------------------------------------------------------

    let target_rate = args.rate;
    let tls_config = if target.tls {
        Some(zerobench_http::mio_tls::build_tls_config(&opts, &[b"http/1.1"]))
    } else {
        None
    };

    eprintln!(
        "[measure] {} runs × {} (warmup {}, cooldown {}) against {}",
        args.runs,
        format_duration(args.duration),
        format_duration(args.warmup),
        format_duration(args.cooldown),
        args.url,
    );

    let mut all_stats: Vec<TaskStats> = Vec::new();
    let mut per_run: Vec<PerRunMetrics> = Vec::new();
    for run_idx in 0..args.runs {
        if run_idx > 0 && !args.cooldown.is_zero() {
            eprintln!(
                "[cooldown] {} (TIME_WAIT drain)...",
                format_duration(args.cooldown)
            );
            std::thread::sleep(args.cooldown);
        }

        // Warmup: fire traffic, discard. Mio dispatch doesn't natively
        // separate warmup from measure yet (TODO in v0.1.0 Phase 6);
        // for now we run (warmup + duration) and discard the first
        // `warmup` worth of stats by re-running without warmup on
        // iteration >0. On iteration 0 we honour it as a prefix.
        if !args.warmup.is_zero() && run_idx == 0 {
            eprintln!("[warmup] {} (discarded)...", format_duration(args.warmup));
            let _ = zerobench_http::mio_h1::run_mio_threaded(
                &target,
                &opts,
                &plan,
                args.threads.max(1),
                args.connections,
                args.warmup,
                target_rate,
                tls_config.clone(),
                None,
                None,
            );
        }

        eprintln!("[run {}/{}] starting ({})...",
            run_idx + 1, args.runs, format_duration(args.duration));
        let stop: Option<Arc<AtomicBool>> = None;
        let stats = zerobench_http::mio_h1::run_mio_threaded(
            &target,
            &opts,
            &plan,
            args.threads.max(1),
            args.connections,
            args.duration,
            target_rate,
            tls_config.clone(),
            None,
            stop,
        );
        let run_requests: u64 = stats.iter().map(|s| s.requests).sum();
        eprintln!("[run {}/{}] {} requests", run_idx + 1, args.runs, run_requests);

        // Phase 8a: capture per-run metrics before merging into the
        // aggregate. These are the elementary samples the bootstrap
        // CI resamples over. Cloning TaskStats here is cheap (HDR
        // histograms are contiguous u64 arrays; for typical bounds
        // that's ~30 KiB per stat).
        let run_summary = Summary::merge(stats.clone(), args.duration);
        per_run.push(PerRunMetrics {
            index: run_idx,
            rate_per_s: run_summary.requests_per_sec(),
            requests: run_summary.requests,
            errors_total: run_summary.errors.total(),
            latency: LatencyExport::from_hist(&run_summary.latency),
        });
        // Silence unused-import warning when LatencyExport /
        // ErrorCountersExport / PerRunMetrics happen not to be used
        // in some build configurations.
        let _ = std::marker::PhantomData::<(LatencyExport, ErrorCountersExport)>::default();

        all_stats.extend(stats);
    }

    // -------------------------------------------------------------------
    // Merge + render
    // -------------------------------------------------------------------

    let total_measured = args.duration.saturating_mul(args.runs);
    let summary = Summary::merge(all_stats, total_measured);
    {
        use std::io::{IsTerminal, Write};
        let is_tty = std::io::stdout().is_terminal();
        let mut out = std::io::stdout().lock();
        let _ = zerobench_core::print_terminal(
            &summary,
            &plan,
            ColorChoice::Auto,
            is_tty,
            &mut out,
        );
        let _ = out.flush();
    }

    // -------------------------------------------------------------------
    // Archive finalisation
    // -------------------------------------------------------------------

    if let Some(writer) = archive_writer {
        env.set_ended();
        writer.write_env(&env)?;

        // Phase 5b: emit result.json with the Summary projection.
        let mut export = summary.to_export();
        // Phase 8a: attach per-run metric vectors so the compare
        // engine can bootstrap CIs from the elementary samples.
        export.per_run = per_run;
        writer.write_result(&export)?;

        // Phase 5c: canonical HDR-V2-compressed-log sidecar. Readable
        // by HdrHistogram Plotter, jHiccup, wrk2 pipeline, any JVM
        // HDR consumer. Carries the raw bucket counts — where
        // result.json carries percentile snapshots.
        let comment = format!(
            "zerobench {} · plan={} · target={} · run_id={}",
            env!("CARGO_PKG_VERSION"),
            plan.name,
            args.url,
            id,
        );
        writer.write_histlog(
            "result",
            &summary.latency,
            run_start_wall,
            total_measured,
            Some(&comment),
        )?;

        let index = Index {
            schema_version: Index::SCHEMA_VERSION,
            schema_versions: SchemaVersions {
                plan: 1,
                machine: MachineFingerprint::SCHEMA_VERSION,
                env: EnvRecord::SCHEMA_VERSION,
                index: Index::SCHEMA_VERSION,
                result: Some(SummaryExport::SCHEMA_VERSION),
            },
            plan_hash: plan_h.clone(),
            target_fingerprint: target_fp.clone(),
            url_fingerprint: url_fp.clone(),
            replayed_from: None,
        };
        writer.finalise(&index)?;
    }

    let total_errors = summary.errors.total();
    if summary.requests == 0 || total_errors > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------

fn build_measure_plan(
    args: &MeasureArgs,
    target: &Target,
    name: &str,
) -> Result<Plan, Box<dyn std::error::Error>> {
    let mut vars = VarRegistry::new();
    // For the Phase 7a MVP we benchmark `GET /`. Rhai-scripted multi-
    // step scenarios go through `zerobench run` / Phase 7 follow-ups.
    let url = Template::compile(&args.url, &mut vars)?;

    let request = RequestPlan {
        method: http::Method::GET,
        url,
        headers: SmallVec::new(),
        body: None,
        extract: Vec::new(),
        checks: Vec::new(),
        expect_streaming: false,
    };

    let rate = match args.rate {
        Some(r) => RateProfile::Constant(r),
        None => RateProfile::Saturate {
            max_concurrency: args.connections,
        },
    };

    let scenario = Scenario {
        name: "measure".into(),
        rate,
        steps: vec![Step::Request(request)],
    };

    // `target` participates in url_fp via host/port/scheme/sni; the
    // plan itself doesn't embed the Target (RequestPlan's URL is the
    // authority). The argument is accepted for API symmetry with
    // sibling verb builders and future multi-scenario construction.
    let _ = target;

    Ok(Plan {
        scenarios: vec![scenario],
        vars,
        duration: args.duration,
        warmup: args.warmup,
        cooldown: args.cooldown,
        runs: args.runs,
        threads: args.threads,
        mode: Mode::Measure,
        name: name.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_duration(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 60.0 {
        let mins = (s / 60.0) as u64;
        let rem = s - mins as f64 * 60.0;
        if rem < 0.1 {
            format!("{mins}m")
        } else {
            format!("{mins}m{rem:.0}s")
        }
    } else if s >= 1.0 {
        format!("{s:.0}s")
    } else {
        format!("{}ms", d.as_millis())
    }
}

fn enabled_features() -> String {
    // Informational — surfaced in env.json so archived runs record
    // which backends were compiled in. `#[cfg]` can't appear inside a
    // `concat!` call (see Rust issue #15701), so we assemble the list
    // at runtime instead.
    #[allow(unused_mut)]
    let mut parts: Vec<&'static str> = vec!["h1"];
    #[cfg(feature = "h2")]
    parts.push("h2");
    #[cfg(feature = "sse")]
    parts.push("sse");
    #[cfg(feature = "ws")]
    parts.push("ws");
    #[cfg(feature = "script")]
    parts.push("script");
    #[cfg(feature = "tui")]
    parts.push("tui");
    parts.join(", ")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_args(url: &str) -> MeasureArgs {
        MeasureArgs {
            url: url.to_string(),
            name: Some("unit-test".into()),
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            rate: None,
            connections: 4,
            threads: 1,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            insecure: false,
            no_calibrate: true,
            force_overload: false,
            allow_coarse_clock: false,
            no_archive: true,
            context: Vec::new(),
        }
    }

    #[test]
    fn plan_built_with_measure_mode_and_name() {
        let args = sample_args("http://127.0.0.1:1/");
        let target = Target::parse(&args.url).unwrap();
        let plan = build_measure_plan(&args, &target, "unit-test").unwrap();
        assert_eq!(plan.mode, Mode::Measure);
        assert_eq!(plan.name, "unit-test");
        assert_eq!(plan.scenarios.len(), 1);
        assert_eq!(plan.duration, Duration::from_secs(1));
    }

    #[test]
    fn plan_uses_saturate_when_no_rate() {
        let args = sample_args("http://x:1/");
        let target = Target::parse(&args.url).unwrap();
        let plan = build_measure_plan(&args, &target, "x").unwrap();
        assert!(matches!(
            plan.scenarios[0].rate,
            RateProfile::Saturate { .. }
        ));
    }

    #[test]
    fn plan_uses_constant_rate_when_given() {
        let mut args = sample_args("http://x:1/");
        args.rate = Some(1234.5);
        let target = Target::parse(&args.url).unwrap();
        let plan = build_measure_plan(&args, &target, "x").unwrap();
        match plan.scenarios[0].rate {
            RateProfile::Constant(r) => assert!((r - 1234.5).abs() < 1e-9),
            ref other => panic!("expected Constant, got {other:?}"),
        }
    }

    #[test]
    fn context_pair_parsing() {
        assert_eq!(
            parse_kv_pair("commit=abc123").unwrap(),
            ("commit".into(), "abc123".into())
        );
        assert_eq!(
            parse_kv_pair("label=production-api-v2").unwrap(),
            ("label".into(), "production-api-v2".into())
        );
        assert!(parse_kv_pair("no-equals").is_err());
    }

    #[test]
    fn format_duration_ranges() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_secs(45)), "45s");
        assert_eq!(format_duration(Duration::from_secs(60)), "1m");
        assert_eq!(format_duration(Duration::from_secs(125)), "2m5s");
    }
}
