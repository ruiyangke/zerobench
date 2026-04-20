//! `zerobench calibrate` — find the client's in-process loopback
//! ceiling.
//!
//! Per PHILOSOPHY §5 / §9.6.2: this is the user-invoked, explicit
//! version of the self-check that `measure`/`compare`/`curve`/`soak`
//! run implicitly. Useful for answering "how fast can I push from
//! this machine, ever?" without pointing at a target.
//!
//! Output: achieved rate, sustained %, jitter p50/p99, verdict.

use std::process::ExitCode;
use std::time::Duration;

use clap::Args;
use zerobench_runtime::calibrate::{ClientSelfCheck, Verdict};

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Flags for `zerobench calibrate`.
#[derive(Debug, Clone, Args)]
pub struct CalibrateArgs {
    /// Target request rate (req/s). The self-check tries to sustain
    /// this against an in-process loopback echo for `--duration` and
    /// reports the actual rate achieved.
    #[arg(short = 'r', long = "rate", default_value = "10000",
          value_parser = super::super::cli_args::parse_rate_flag,
          help_heading = "Load")]
    pub rate: f64,

    /// How long to run the calibration.
    #[arg(short = 'd', long = "duration", default_value = "5s",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "Load")]
    pub duration: Duration,

    /// Connection pool size against the in-process echo. Defaults to 8.
    #[arg(short = 'c', long = "concurrency", default_value_t = 8,
          help_heading = "Load")]
    pub concurrency: usize,
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

/// Run a calibration, print the verdict, exit non-zero on Refuse.
pub fn run(args: CalibrateArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    eprintln!(
        "[calibrate] target {:.0} req/s × {:?} (pool {})",
        args.rate, args.duration, args.concurrency,
    );

    let check = ClientSelfCheck::spawn()?;
    let result = check.check(args.rate, args.duration, Some(args.concurrency))?;

    println!("rate         offered {:>10.0} req/s", result.offered_rate);
    println!("             achieved {:>10.0} req/s", result.achieved_rate);
    println!(
        "             sustained {:>9.2}%",
        result.sustained_pct * 100.0
    );
    println!("completed    {}", result.completed);
    println!("elapsed      {:?}", result.elapsed);
    println!();
    let jitter_ns = |pct: f64| -> String {
        let ns = result.jitter.value_at_percentile(pct);
        fmt_ns(ns)
    };
    println!(
        "jitter (abs) p50={}  p90={}  p99={}  p99.9={}  max={}",
        jitter_ns(50.0),
        jitter_ns(90.0),
        jitter_ns(99.0),
        jitter_ns(99.9),
        fmt_ns(result.jitter.max()),
    );
    let lat_ns = |pct: f64| -> String { fmt_ns(result.latency.value_at_percentile(pct)) };
    println!(
        "latency      p50={}  p90={}  p99={}  p99.9={}  max={}",
        lat_ns(50.0),
        lat_ns(90.0),
        lat_ns(99.0),
        lat_ns(99.9),
        fmt_ns(result.latency.max()),
    );
    println!();
    let verdict_line = match result.verdict {
        Verdict::Pass => format!(
            "verdict      PASS — client sustains {:.0} req/s on this machine",
            args.rate
        ),
        Verdict::Marginal => format!(
            "verdict      MARGINAL — sustained {:.1}% (≥95%, <99%). Tool runs at this rate but tool-overhead will show up in percentiles.",
            result.sustained_pct * 100.0
        ),
        Verdict::Refuse => format!(
            "verdict      REFUSE — only {:.1}% of requested rate sustained. Lower --rate or reduce --concurrency.",
            result.sustained_pct * 100.0
        ),
    };
    println!("{verdict_line}");

    Ok(match result.verdict {
        Verdict::Pass | Verdict::Marginal => ExitCode::SUCCESS,
        Verdict::Refuse => ExitCode::from(1),
    })
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
