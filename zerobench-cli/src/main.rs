//! zerobench — CLI entry point.
//!
//! Flow:
//! 1. Parse args via `clap`.
//! 2. Build the `Plan` / `Target` / `TransportOpts` triple from args.
//! 3. Open the HTTP transport client against the target.
//! 4. Dispatch the bench — `run_saturate` (Task 7) or `run_open_loop`
//!    (Task 10).
//! 5. Merge per-task stats into a `Summary`.
//! 6. Render via the chosen reporter.
//! 7. Exit 0 on clean runs, 1 when errors/assertion failures occurred,
//!    2 for usage errors.

use std::io::IsTerminal;
use std::process::ExitCode;

use clap::Parser;
use zerobench_core::{
    print_json, print_terminal, run_saturate, ColorChoice, StopSignal, Summary, Transport,
};
use zerobench_http::HttpTransport;

mod cli_args;
mod plan_from_cli;

use cli_args::{CliArgs, CliColor, CliFormat};

#[compio::main]
async fn main() -> ExitCode {
    let args = CliArgs::parse();
    match run(args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run(args: CliArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    // Reject open-loop until Task 10 lands that path.
    if args.rate.is_some() && !args.saturate {
        // Task 10 updates this branch to call run_open_loop instead.
        // For Task 9 we surface a clear message rather than silently
        // misbehaving.
        return Err("--rate mode lands in Task 10 of the v0.0.1 plan; pass --saturate for now".into());
    }

    let (plan, target, opts) = plan_from_cli::build(&args)?;

    // Stand up the transport client.
    let client = <HttpTransport as Transport>::build_client(&target, &opts).await?;

    // Run.
    let stop = StopSignal::after(plan.duration);
    let stats = run_saturate::<HttpTransport>(&plan, client, args.connections, stop).await;
    let summary = Summary::merge(stats, plan.duration);

    // Render.
    let color = match args.color {
        CliColor::Always => ColorChoice::Always,
        CliColor::Auto => ColorChoice::Auto,
        CliColor::Never => ColorChoice::Never,
    };
    let stdout = std::io::stdout();
    let is_tty = stdout.is_terminal();
    let mut out = stdout.lock();
    match args.format {
        CliFormat::Terminal => {
            print_terminal(&summary, &plan, color, is_tty, &mut out)?;
        }
        CliFormat::Json => {
            print_json(&summary, &plan, &mut out)?;
        }
    }

    // Exit code policy: 0 clean, 1 errors/assertion failures, 2 usage
    // errors (handled in `main`).
    let total_errors = summary.errors.total();
    if total_errors > 0 {
        Ok(ExitCode::from(1))
    } else if summary.requests == 0 {
        // Plan ran to completion with zero requests — usually means the
        // server was unreachable or the duration was set absurdly low.
        // Signal it as a non-zero exit so CI pipelines notice.
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}
