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

    // --- TUI / JSONL mutual-exclusion guard -----------------------------
    //
    // For v0.0.1 the TUI and JSONL streamer both write to stdout; running
    // both interleaves ANSI cursor moves with JSONL lines, corrupting
    // anything downstream trying to parse either. Fail fast with a clean
    // error rather than producing garbage.
    //
    // Checked before `build_client` so a mis-invocation surfaces as a
    // clean usage error rather than a network failure.
    #[cfg(feature = "tui")]
    let tui_enabled = args.tui;
    #[cfg(not(feature = "tui"))]
    let tui_enabled = false;

    if tui_enabled && matches!(args.format, CliFormat::Jsonl) {
        return Err(
            "--tui cannot be combined with --format jsonl (both write to stdout)".into(),
        );
    }
    #[cfg(feature = "tui")]
    if tui_enabled {
        // The dashboard is unusable without a TTY (no place to render
        // the alt-screen buffer) — surface that up-front instead of
        // failing mid-run with a confusing crossterm error.
        if !std::io::stdout().is_terminal() {
            return Err("--tui requires stdout to be a TTY".into());
        }
    }

    let (plan, target, opts) = plan_from_cli::build(&args)?;

    // Stand up the transport client.
    let client = <HttpTransport as Transport>::build_client(&target, &opts).await?;

    // Set up live streaming for JSONL streaming OR the TUI dashboard —
    // both consume the same `LiveSnapshot`. `LiveSnapshot::new`
    // already returns an `Arc`, so we keep it as is rather than
    // double-wrapping.
    let live = if matches!(args.format, CliFormat::Jsonl) || tui_enabled {
        Some(zerobench_core::LiveSnapshot::new())
    } else {
        None
    };

    // Spawn the per-second ticker task if we're streaming JSONL. The
    // ticker writes one line per second to stdout; the final summary
    // goes to stderr so pipelines capturing stdout get clean JSONL.
    let ticker_stop = StopSignal::new();
    let ticker_handle = if let (Some(live), CliFormat::Jsonl) = (&live, &args.format) {
        let live = live.clone();
        let stop = ticker_stop.clone();
        Some(compio::runtime::spawn(
            async move { jsonl_ticker(live, stop).await },
        ))
    } else {
        None
    };

    // Run. The stop signal ticks after `plan.duration` elapses; the
    // TUI also flips it when the user hits `q` so the dispatcher
    // exits early and records the actual (shorter) duration.
    let stop = StopSignal::after(plan.duration);

    // Spawn the TUI task if `--tui` is on. The TUI owns the terminal
    // for the duration of the run; its own loop calls
    // `swap_and_snapshot` at 1 Hz to drain the shared LiveSnapshot.
    //
    // We share `stop` with the dispatcher: when the user hits `q`,
    // the TUI stops it, which breaks the workers out of their loops.
    // This matches the design spec — `q` terminates the whole run,
    // not just the dashboard.
    #[cfg(feature = "tui")]
    let tui_handle = if tui_enabled {
        let live = live.clone().expect("live snapshot set above when tui_enabled");
        let target_rate_opt = args.rate;
        let total_duration = plan.duration;
        let url_label = format_url_label(&target);
        let stop_for_tui = stop.clone();
        let handle = compio::runtime::spawn(async move {
            zerobench_tui::run_tui(
                live,
                stop_for_tui,
                target_rate_opt,
                total_duration,
                url_label,
            )
            .await
        });
        Some(handle)
    } else {
        None
    };

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

    // Wait for the TUI task to restore the terminal before we print
    // the final report. The shared `stop` has already tripped (either
    // the timer fired or the user hit `q`), so the TUI loop is on
    // its way out; we just wait for it to drop the alt-screen.
    #[cfg(feature = "tui")]
    if let Some(handle) = tui_handle {
        let _ = handle.await;
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

/// Compact human-friendly URL for the TUI header bar — `http://host`
/// or `https://host:port`, omitting the default port so the header
/// stays readable.
#[cfg(feature = "tui")]
fn format_url_label(target: &zerobench_core::transport::Target) -> String {
    let scheme = if target.tls { "https" } else { "http" };
    let default_port = if target.tls { 443 } else { 80 };
    if target.port == default_port {
        format!("{scheme}://{}", target.host)
    } else {
        format!("{scheme}://{}:{}", target.host, target.port)
    }
}
