//! `zerobench probe URL` — 5-second smoke test.
//!
//! Per PHILOSOPHY §5 / Q1 RESOLVED: the smoke-test verb. Short,
//! opinionated, no archive, no calibration gate, no statistical
//! rigour — "does the target respond, and roughly how fast?" A
//! developer who types `zerobench http://…` gets this behaviour.
//!
//! Defaults: 5s duration, 1 run, no warmup, no cooldown, saturate
//! (closed-loop) with `-c 20`, no archive, no calibration. Override
//! via flags if you know what you want.
//!
//! For the rigorous path, use `zerobench measure`.

use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use clap::{ArgAction, Args};
use smallvec::SmallVec;

use zerobench_core::plan::{Mode, Plan, RateProfile, RequestPlan, Scenario, Step};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_core::{Summary, TaskStats};
use zerobench_report::ColorChoice;

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Flags accepted by `zerobench probe URL [flags]`.
#[derive(Debug, Clone, Args)]
pub struct ProbeArgs {
    /// Target URL (HTTP/HTTPS). SSE/WS land with
    #[arg(value_name = "URL")]
    pub url: String,

    /// Duration — default deliberately short (5s). `probe` is a smoke
    /// test, not a measurement.
    #[arg(short = 'd', long = "duration", default_value = "5s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Load")]
    pub duration: Duration,

    /// Open-loop target rate. Omitted → saturate with `-c` workers.
    #[arg(short = 'r', long = "rate",
          value_parser = super::super::cli_args::parse_rate_flag,
          help_heading = "Load")]
    pub rate: Option<f64>,

    /// Concurrent connections (saturate mode) / pool cap (rate mode).
    #[arg(
        short = 'c',
        long = "connections",
        default_value_t = 20,
        help_heading = "Load"
    )]
    pub connections: usize,

    /// OS worker threads. Probe defaults to 2 to keep start-up cheap;
    /// use `measure` for a proper core-wide run.
    #[arg(
        short = 't',
        long = "threads",
        default_value_t = 2,
        help_heading = "Load"
    )]
    pub threads: usize,

    /// TCP+TLS connect timeout.
    #[arg(long = "connect-timeout", default_value = "5s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Network")]
    pub connect_timeout: Duration,

    /// Per-request deadline.
    #[arg(long = "timeout", default_value = "5s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Network")]
    pub request_timeout: Duration,

    /// Accept invalid TLS certificates (https:// only).
    #[arg(short = 'k', long = "insecure", action = ArgAction::SetTrue,
          help_heading = "Network")]
    pub insecure: bool,
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

// TODO: consolidate probe's custom runner glue with zerobench_runtime::runner
// (used by measure/curve). Probe uses a different backend path — the unified
// path is achievable but was deferred in Phase 4c.

/// Execute a probe invocation.
pub fn run(args: ProbeArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let target = Target::parse(&args.url)?;

    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        ..TransportOpts::default()
    };

    // Minimal plan — one scenario, GET /.
    let mut vars = VarRegistry::new();
    let url_tpl = Template::compile(&args.url, &mut vars)?;
    let request = RequestPlan {
        method: http::Method::GET,
        url: url_tpl,
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

    let plan = Plan {
        scenarios: vec![Scenario {
            name: "probe".into(),
            rate,
            steps: vec![Step::Request(request)],
        }],
        vars,
        duration: args.duration,
        warmup: Duration::ZERO,
        cooldown: Duration::ZERO,
        runs: 1,
        threads: args.threads,
        mode: Mode::Probe,
        name: String::new(), // probe is anonymous — no archive
    };

    // TLS config for https://.
    let tls_config = if target.tls {
        Some(zerobench_backends::http::mio_tls::build_tls_config(
            &opts,
            &[b"http/1.1"],
        ))
    } else {
        None
    };

    eprintln!(
        "[probe] {} for {} (no archive, no calibration gate)",
        args.url,
        format_duration(args.duration)
    );

    let stop: Option<Arc<AtomicBool>> = None;
    let stats: Vec<TaskStats> = zerobench_backends::http::mio_h1::run_mio_threaded(
        &target,
        &opts,
        &plan,
        args.threads.max(1),
        args.connections,
        args.duration,
        args.rate,
        tls_config,
        None,
        stop,
    );

    let summary = Summary::merge(stats, args.duration);
    {
        use std::io::{IsTerminal, Write};
        let is_tty = std::io::stdout().is_terminal();
        let mut out = std::io::stdout().lock();
        let _ =
            zerobench_report::print_terminal(&summary, &plan, ColorChoice::Auto, is_tty, &mut out);
        let _ = out.flush();
    }

    let total_errors = summary.errors.total();
    if summary.requests == 0 || total_errors > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

fn format_duration(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 60.0 {
        format!("{:.0}m", s / 60.0)
    } else if s >= 1.0 {
        format!("{s:.0}s")
    } else {
        format!("{}ms", d.as_millis())
    }
}
