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
    print_json, print_terminal, run_open_loop, run_saturate, ColorChoice, StopSignal,
    Summary, Transport,
};
use zerobench_http::HttpTransport;

mod cli_args;
mod diff;
mod plan_from_cli;

use cli_args::{CliArgs, CliColor, CliFormat, Subcommand};

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
    // Route sub-commands first — they don't touch the transport layer.
    if let Some(cmd) = args.command.clone() {
        return match cmd {
            Subcommand::Diff(da) => diff::run(&da),
        };
    }

    let open_loop = args.rate.is_some();

    let (plan, target, opts) = plan_from_cli::build(&args)?;

    // Stand up the transport client.
    let client = <HttpTransport as Transport>::build_client(&target, &opts).await?;

    // Set up live streaming for `--format jsonl`.
    // `LiveSnapshot::new` already returns an `Arc`, so we keep it as
    // is rather than double-wrapping.
    let live = match args.format {
        CliFormat::Jsonl => Some(zerobench_core::LiveSnapshot::new()),
        _ => None,
    };

    // Spawn the per-second ticker task if we're streaming JSONL. The
    // ticker writes one line per second to stdout; the final summary
    // goes to stderr so pipelines capturing stdout get clean JSONL.
    let ticker_stop = StopSignal::new();
    let ticker_handle = if let Some(live) = &live {
        let live = live.clone();
        let stop = ticker_stop.clone();
        Some(compio::runtime::spawn(
            async move { jsonl_ticker(live, stop).await },
        ))
    } else {
        None
    };

    // Run.
    let stop = StopSignal::after(plan.duration);
    let stats = if open_loop {
        run_open_loop::<HttpTransport>(
            &plan,
            client,
            args.connections,
            stop,
            live.clone(),
        )
        .await
    } else {
        run_saturate::<HttpTransport>(
            &plan,
            client,
            args.connections,
            stop,
            live.clone(),
        )
        .await
    };

    // Stop the ticker and let it flush its final tick.
    ticker_stop.stop();
    if let Some(h) = ticker_handle {
        let _ = h.await;
    }

    let summary = Summary::merge(stats, plan.duration);

    // Render.
    let color = match args.color {
        CliColor::Always => ColorChoice::Always,
        CliColor::Auto => ColorChoice::Auto,
        CliColor::Never => ColorChoice::Never,
    };
    match args.format {
        CliFormat::Terminal => {
            let stdout = std::io::stdout();
            let is_tty = stdout.is_terminal();
            let mut out = stdout.lock();
            print_terminal(&summary, &plan, color, is_tty, &mut out)?;
        }
        CliFormat::Json => {
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            print_json(&summary, &plan, &mut out)?;
        }
        CliFormat::Jsonl => {
            // JSONL lines were already streamed to stdout; emit the
            // final terminal summary to stderr so stdout stays pure
            // JSONL for downstream pipelines.
            let stderr = std::io::stderr();
            let is_tty = stderr.is_terminal();
            let mut out = stderr.lock();
            print_terminal(&summary, &plan, color, is_tty, &mut out)?;
        }
        CliFormat::Prom => {
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            zerobench_core::print_prometheus(&summary, &plan, &mut out)?;
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

/// Per-second ticker: wakes on integer-second boundaries relative to
/// the `LiveSnapshot` start, calls `swap_and_snapshot`, emits one JSONL
/// line to stdout. Exits when `stop` trips; emits one last tick on
/// exit so any residual samples aren't lost.
async fn jsonl_ticker(
    live: std::sync::Arc<zerobench_core::LiveSnapshot>,
    stop: StopSignal,
) {
    use std::io::Write;
    let start = live.start();
    let interval = std::time::Duration::from_secs(1);
    let mut next = start + interval;
    while !stop.is_stopped() {
        let now = std::time::Instant::now();
        let wait = if next > now {
            next - now
        } else {
            std::time::Duration::ZERO
        };
        // Cap individual sleep at ~100ms so we wake promptly when
        // `stop` trips between ticks.
        let poll_wait = wait.min(std::time::Duration::from_millis(100));
        compio::time::sleep(poll_wait).await;
        if stop.is_stopped() {
            break;
        }
        if std::time::Instant::now() < next {
            // Hasn't been a full second yet — loop and poll again.
            continue;
        }
        let tick = live.swap_and_snapshot();
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let _ = zerobench_core::print_jsonl_tick(&tick, &mut out);
        let _ = out.flush();
        next += interval;
    }

    // Flush one final tick so stragglers aren't lost. We emit even
    // when `tick.requests == 0` — a trailing partial window may still
    // carry non-zero error counters (especially keepup under
    // backpressure) or a short burst of samples that wouldn't reach a
    // full integer-second boundary. The only case we suppress is the
    // "completely empty" tick (no requests, no errors, no bytes) —
    // emitting that would just be noise for downstream pipelines.
    let tick = live.swap_and_snapshot();
    let has_anything = tick.requests > 0
        || tick.bytes_sent > 0
        || tick.bytes_recv > 0
        || tick.errors.total() > 0;
    if has_anything {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let _ = zerobench_core::print_jsonl_tick(&tick, &mut out);
        let _ = out.flush();
    }
}
