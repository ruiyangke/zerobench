//! zerobench — CLI entry point.
//!
//! Synchronous, mio/epoll-based. Zero async runtime.
//!
//! `run_mio_sync` is the bare-bench path used by `zerobench <url>`;
//! `run_script_sync` is the Rhai-script path. Both route protocol
//! dispatch through `zerobench_backends::run_plan` (Phase 2c). The
//! rigorous verbs (`measure`, `curve`) consume the shared runner in
//! `zerobench_runtime::runner` (Phase 4c).

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::fs::File;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::process::ExitCode;

use clap::Parser;
use zerobench_core::Summary;
use zerobench_report::{print_json, print_terminal, ColorChoice};

mod cli_args;
mod diff;
mod plan_from_cli;
mod verbs;

use cli_args::{CliArgs, CliColor, CliFormat, Subcommand};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    let args = CliArgs::parse();

    if let Some(cmd) = args.command.clone() {
        return match cmd {
            Subcommand::Diff(da) => match diff::run(&da) {
                Ok(code) => code,
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::from(2)
                }
            },
            Subcommand::Run(ra) => match run_script_sync(ra) {
                Ok(code) => code,
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::from(2)
                }
            },
            Subcommand::Measure(ma) => match verbs::measure::run(ma) {
                Ok(code) => code,
                Err(e) => {
                    print_error_with_hint(&*e);
                    ExitCode::from(2)
                }
            },
            Subcommand::Probe(pa) => match verbs::probe::run(pa) {
                Ok(code) => code,
                Err(e) => {
                    print_error_with_hint(&*e);
                    ExitCode::from(2)
                }
            },
            Subcommand::Compare(ca) => match verbs::diff::run(ca) {
                Ok(code) => code,
                Err(e) => {
                    print_error_with_hint(&*e);
                    ExitCode::from(2)
                }
            },
            Subcommand::Calibrate(ca) => match verbs::calibrate::run(ca) {
                Ok(code) => code,
                Err(e) => {
                    print_error_with_hint(&*e);
                    ExitCode::from(2)
                }
            },
            Subcommand::Curve(cv) => match verbs::curve::run(cv) {
                Ok(code) => code,
                Err(e) => {
                    print_error_with_hint(&*e);
                    ExitCode::from(2)
                }
            },
        };
    }

    // S0.4 — no URL, no request file, no subcommand: print a friendly
    // quickstart instead of clap's "error: ..." so CI health checks
    // (e.g. `zerobench && echo ok`) pass.
    if args.url.is_none() && args.request_file.is_none() && args.requests.is_none() {
        print_quickstart();
        return ExitCode::SUCCESS;
    }

    // S3.27 — dry run: resolve DNS, print a config block, exit.
    if args.dry_run {
        return match run_dry(&args) {
            Ok(code) => code,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::from(2)
            }
        };
    }

    match run_mio_sync(&args) {
        Ok(code) => code,
        Err(e) => {
            print_error_with_hint(&*e);
            ExitCode::from(2)
        }
    }
}

/// S0.4 — friendlier no-args landing page.
fn print_quickstart() {
    println!(
        "zerobench — HTTP benchmarking (mio/epoll)\n\
         \n\
         Quick start:\n    \
             zerobench http://localhost:8080             # 30s saturate\n    \
             zerobench http://localhost:8080 -d 1m -c 200\n    \
             zerobench --sse http://localhost:8080/events -c 100\n    \
             zerobench run my.rhai                        # scripted scenarios\n    \
             zerobench --help                             # full reference\n"
    );
}

/// S2.21 — rewrite bare error strings with a practical hint. The
/// underlying `BuildError` and `TargetError` types are owned by
/// zerobench-core and immutable to the CLI, so we pattern-match on the
/// formatted message substring.
fn print_error_with_hint(e: &(dyn std::error::Error + 'static)) {
    let msg = format!("{e}");
    eprintln!("error: {msg}");
    let lower = msg.to_lowercase();
    if lower.contains("invalid port") {
        eprintln!("hint: port must be 1-65535.");
    } else if lower.contains("invalid url") || lower.contains("missing host") {
        eprintln!("hint: URL must include scheme, e.g. http:// or https://");
    } else if lower.contains("connection refused") {
        eprintln!("hint: is the target server running on that host:port?");
    }
}

/// S3.27 — parse args, build plan, resolve DNS, print config, exit 0.
fn run_dry(args: &CliArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let (plan, target, opts) = plan_from_cli::build(args)?;

    // Resolve DNS — `Target::resolve` already consults
    // `opts.resolve_overrides` (populated from `--resolve` on the CLI)
    // before falling back to the system resolver.
    let scheme = if target.tls { "https" } else { "http" };
    let url_label = if (target.tls && target.port == 443) || (!target.tls && target.port == 80) {
        format!("{scheme}://{}", target.host)
    } else {
        format!("{scheme}://{}:{}", target.host, target.port)
    };
    let resolved = match target.resolve(&opts) {
        Ok(sa) => sa.to_string(),
        Err(e) => format!("(unresolved: {e})"),
    };

    let mode = if let Some(r) = args.rate {
        format!("open-loop rate {r:.0} req/s")
    } else {
        format!("saturate ({} tasks)", args.connections)
    };

    println!("dry run — no traffic sent");
    println!("target:     {url_label} \u{2192} {resolved}");
    println!(
        "plan:       {} scenario{}, {}, {}",
        plan.scenarios.len(),
        if plan.scenarios.len() == 1 { "" } else { "s" },
        mode,
        format_duration(plan.duration),
    );
    // The method & headers live on the first scenario's first step.
    if let Some(zerobench_core::plan::Step::Request(req)) =
        plan.scenarios.first().and_then(|s| s.steps.first())
    {
        println!("method:     {}", req.method);
        if req.headers.is_empty() {
            println!("headers:    (none)");
        } else {
            println!("headers:    {} header(s)", req.headers.len());
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Format `Duration` for the dry-run block (`30s`, `1m`, `2m30s`).
fn format_duration(d: std::time::Duration) -> String {
    let s = d.as_secs();
    if s == 0 {
        format!("{}ms", d.as_millis())
    } else if s.is_multiple_of(3600) {
        format!("{}h", s / 3600)
    } else if s.is_multiple_of(60) {
        format!("{}m", s / 60)
    } else if s > 60 {
        let m = s / 60;
        let sec = s % 60;
        format!("{m}m{sec}s")
    } else {
        format!("{s}s")
    }
}

/// Open `path` for writing the final report, returning the owned file
/// so stdout-locking code can use a single `Write` trait object.
fn open_output_file(path: &Path) -> Result<File, Box<dyn std::error::Error>> {
    File::create(path)
        .map_err(|e| format!("cannot open output file {}: {e}", path.display()).into())
}

// ---------------------------------------------------------------------------
// TUI helpers
// ---------------------------------------------------------------------------

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

#[cfg(feature = "tui")]
fn build_transport_info(
    args: &CliArgs,
    target: &zerobench_core::transport::Target,
    opts: &zerobench_core::transport::TransportOpts,
) -> zerobench_tui::TransportInfo {
    use zerobench_core::transport::HttpVersionPref;
    use zerobench_tui::{RunMode, TransportInfo};

    let mode = match args.rate {
        Some(r) => RunMode::Rate(r),
        None => RunMode::Saturate(args.connections),
    };
    let (protocol, alpn) = match opts.http_version {
        HttpVersionPref::Http1 => ("H1".to_string(), Some("http/1.1".to_string())),
        HttpVersionPref::Http2 => ("H2".to_string(), Some("h2".to_string())),
        HttpVersionPref::Auto => {
            if target.tls {
                ("H2/H1".to_string(), Some("h2,http/1.1".to_string()))
            } else {
                ("H1".to_string(), None)
            }
        }
    };
    let alpn = if target.tls { alpn } else { None };

    TransportInfo {
        mode,
        connections: args.connections,
        protocol,
        tls: target.tls,
        alpn,
    }
}

// ---------------------------------------------------------------------------
// Mio dispatch — synchronous (called from main() without an async runtime)
// ---------------------------------------------------------------------------

/// Drive the mio-based benchmark — pure synchronous epoll, no async runtime.
/// Builds the plan, spawns N OS threads with their own `mio::Poll`, waits
/// for `plan.duration`, merges stats, renders.
fn run_mio_sync(args: &CliArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    // SSE / WS bench paths go through `zerobench measure --sse-hold N`
    // / `--ws-echo N` (v0.1.0). The old top-level `--sse` / `--ws`
    // flags are rejected at the clap level (see CliArgs).
    #[cfg(feature = "tui")]
    let tui_enabled = args.tui;
    #[cfg(not(feature = "tui"))]
    let tui_enabled = false;

    if tui_enabled && matches!(args.format, CliFormat::Jsonl) {
        return Err("--tui cannot be combined with --format jsonl (both write to stdout)".into());
    }
    #[cfg(feature = "tui")]
    if tui_enabled && !std::io::stdout().is_terminal() {
        return Err("--tui requires stdout to be a TTY".into());
    }

    let (mut plan, target, opts) = plan_from_cli::build(args)?;
    plan.threads = args.threads;

    let num_threads = args.threads.max(1);

    // `--http2-prior-knowledge` is a discoverability alias for
    // `--http-version h2` — honour both when picking the wire version.
    let use_h2 =
        args.http2_prior_knowledge || matches!(args.http_version, cli_args::CliHttpVersion::H2);

    // Build TLS config when targeting https://.
    let tls_config = if target.tls {
        let alpn: &[&[u8]] = if use_h2 { &[b"h2"] } else { &[b"http/1.1"] };
        Some(zerobench_backends::http::mio_tls::build_tls_config(
            &opts, alpn,
        ))
    } else {
        None
    };

    // Set up LiveSnapshot + StopSignal for TUI (or pass None for headless).
    let live = if tui_enabled {
        Some(zerobench_runtime::LiveSnapshot::new(plan.scenarios.len()))
    } else {
        None
    };

    // When TUI is active, create a shared stop signal that both the TUI
    // (user presses q) and the timer can trip. The mio workers use the
    // same underlying AtomicBool — no separate timer inside the backend.
    let (stop, stop_flag) = if tui_enabled {
        let s = zerobench_runtime::StopSignal::after_wall(plan.duration);
        let flag = std::sync::Arc::clone(s.flag());
        (Some(s), Some(flag))
    } else {
        (None, None)
    };

    // Spawn TUI on a dedicated OS thread when enabled.
    #[cfg(feature = "tui")]
    let tui_handle = if tui_enabled {
        let live_for_tui = live.clone().unwrap();
        let stop_for_tui = stop.clone().unwrap();
        let url_label = format_url_label(&target);
        let transport = build_transport_info(args, &target, &opts);
        let scenario_names: Vec<String> = plan.scenarios.iter().map(|s| s.name.clone()).collect();
        let total_duration = plan.duration;
        let target_rate = args.rate;

        Some(std::thread::spawn(move || {
            zerobench_tui::run_tui(
                live_for_tui,
                stop_for_tui,
                target_rate,
                total_duration,
                url_label,
                transport,
                scenario_names,
            )
        }))
    } else {
        None
    };
    #[cfg(not(feature = "tui"))]
    let tui_handle: Option<std::thread::JoinHandle<std::io::Result<()>>> = None;

    let stats = if use_h2 {
        zerobench_backends::http::mio_h2::run_mio_h2_threaded(
            &target,
            &opts,
            &plan,
            num_threads,
            args.connections,
            plan.duration,
            args.rate,
            tls_config,
            live,
            stop_flag,
        )
    } else {
        zerobench_backends::http::mio_h1::run_mio_threaded(
            &target,
            &opts,
            &plan,
            num_threads,
            args.connections,
            plan.duration,
            args.rate,
            tls_config,
            live,
            stop_flag,
        )
    };

    // Trip the stop signal so the TUI sees the run as completed.
    if let Some(ref s) = stop {
        s.stop();
    }

    // Wait for TUI to exit (user presses q after inspecting charts).
    if let Some(handle) = tui_handle {
        let _ = handle.join();
    }

    let summary = Summary::merge(stats, plan.duration);

    let color = match args.color {
        CliColor::Always => ColorChoice::Always,
        CliColor::Auto => ColorChoice::Auto,
        CliColor::Never => ColorChoice::Never,
    };
    render_report(&summary, &plan, args.format, args.output.as_deref(), color)?;

    let total_errors = summary.errors.total();
    if total_errors > 0 || summary.requests == 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

/// Render the final report to stdout, stderr (for JSONL), or a
/// user-supplied file (`-o FILE`). Centralised so the bench path and
/// `zerobench run` path share one implementation.
///
/// When `output` is `Some`, every format writes to that file — the
/// file isn't a TTY so `is_tty` is forced to false (users can still
/// force colors with `--color always`).
fn render_report(
    summary: &Summary,
    plan: &zerobench_core::plan::Plan,
    format: CliFormat,
    output: Option<&Path>,
    color: ColorChoice,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = output {
        let mut file = open_output_file(path)?;
        write_report(summary, plan, format, color, false, &mut file)?;
        return Ok(());
    }

    match format {
        CliFormat::Jsonl => {
            let stderr = std::io::stderr();
            let is_tty = stderr.is_terminal();
            let mut out = stderr.lock();
            write_report(summary, plan, format, color, is_tty, &mut out)?;
        }
        _ => {
            let stdout = std::io::stdout();
            let is_tty = stdout.is_terminal();
            let mut out = stdout.lock();
            write_report(summary, plan, format, color, is_tty, &mut out)?;
        }
    }
    Ok(())
}

/// Dispatch a single `Summary` → `Write` render for the given format.
///
/// Generic over `W: Write` because `print_terminal` / `print_json` /
/// `print_prometheus` in `zerobench-report` take `impl Write` bounds
/// (`Sized`) — a `&mut dyn Write` trait object wouldn't satisfy them.
fn write_report<W: Write>(
    summary: &Summary,
    plan: &zerobench_core::plan::Plan,
    format: CliFormat,
    color: ColorChoice,
    is_tty: bool,
    out: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    match format {
        CliFormat::Terminal | CliFormat::Jsonl => {
            print_terminal(summary, plan, color, is_tty, out)?;
        }
        CliFormat::Json => {
            print_json(summary, plan, out)?;
        }
        CliFormat::Prom => {
            zerobench_report::print_prometheus(summary, plan, out)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SSE dispatch
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Rhai script dispatch
// ---------------------------------------------------------------------------

/// Load a `.rhai` scenario script, apply any CLI overrides, stand up the
/// transport(s), dispatch the run, and render the report.
///
/// The Rhai engine lives only inside `load_script` — this function sees
/// a pure Rust [`zerobench_core::plan::Plan`] and never pulls in the
/// interpreter again. CLI
/// overrides (`--duration`, `--rate`) mutate the Plan in-place after
/// load; `--connections` / TLS / timeouts shape the transport opts.
///
/// # Tier-1 dispatch
///
/// The plan may contain a mix of HTTP, SSE, and WS scenarios. Each
/// backend runs in its own thread group — the three event loops stay
/// separate under the hood. After all backends finish, per-worker
/// `TaskStats` are merged into a single unified `Summary` with
/// protocol-specific extras (SSE/WS histograms, byte counters)
/// preserved per scenario.
/// Dispatch a plan across the HTTP / SSE / WS backends in parallel and
/// merge their per-worker stats into one summary. Shared by the serial
/// (one scenario per call) and `--parallel` (all scenarios in one call)
/// execution modes of `run_script_sync`.
fn dispatch_multi_protocol_plan(
    plan: &zerobench_core::plan::Plan,
    target: &zerobench_core::transport::Target,
    opts: &zerobench_core::transport::TransportOpts,
    connections: usize,
    num_threads: usize,
    target_rps: Option<f64>,
) -> Result<Summary, Box<dyn std::error::Error>> {
    // Shared TLS config across all three protocol groups — the Rhai
    // path pins ALPN to http/1.1 for every backend today.
    let tls_config = if target.tls {
        Some(zerobench_backends::http::mio_tls::build_tls_config(
            opts,
            &[b"http/1.1"],
        ))
    } else {
        None
    };

    let ctx = zerobench_backends::RunCtx {
        target: target.clone(),
        opts: opts.clone(),
        duration: plan.duration,
        num_threads,
        connections,
        target_rps,
        tls_config,
        live: None,
        stop: None,
    };
    let all_stats = zerobench_backends::run_plan(plan, &ctx);
    Ok(Summary::merge(all_stats, plan.duration))
}

fn run_script_sync(args: cli_args::RunArgs) -> Result<ExitCode, Box<dyn std::error::Error>> {
    use zerobench_core::plan::{RateProfile, Step};
    use zerobench_core::template::Template;
    use zerobench_core::transport::TransportOpts;

    let zerobench_dsl::LoadedScript {
        mut plan,
        target,
        http_version: _,
    } = zerobench_dsl::load_script(&args.script)?;

    if let Some(d) = args.duration {
        plan.duration = d;
    }
    if let Some(r) = args.rate {
        let n = plan.scenarios.len() as f64;
        for s in &mut plan.scenarios {
            s.rate = RateProfile::Constant(r / n);
        }
    } else if args.saturate {
        // Override every scenario to saturate mode so the same Rhai
        // file can be re-run for tail-latency measurements without
        // editing the script.
        for s in &mut plan.scenarios {
            s.rate = RateProfile::Saturate {
                max_concurrency: args.connections,
            };
        }
    }

    // S1.7 — apply --basic-auth / --bearer to every request in the
    // plan. Explicit `Authorization:` headers set by the script win
    // (with a warning), matching the bench-path behaviour.
    if args.basic_auth.is_some() || args.bearer.is_some() {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        let value = if let Some(up) = &args.basic_auth {
            format!("Basic {}", B64.encode(up.as_bytes()))
        } else {
            format!("Bearer {}", args.bearer.as_deref().unwrap_or(""))
        };
        let name_tpl = Template::compile("Authorization", &mut plan.vars)?;
        let value_tpl = Template::compile(&value, &mut plan.vars)?;
        for scenario in &mut plan.scenarios {
            for step in &mut scenario.steps {
                if let Step::Request(req) = step {
                    let already = req.headers.iter().any(|(n, _)| {
                        // Template doesn't expose the literal — we
                        // compare part count/raw via Debug as a best
                        // effort. Scripts that set Authorization end
                        // up with at least one header named as such.
                        let dbg = format!("{n:?}").to_lowercase();
                        dbg.contains("authorization")
                    });
                    if already {
                        eprintln!(
                            "warning: script already sets Authorization header — --basic-auth / --bearer ignored for one or more scenarios",
                        );
                        continue;
                    }
                    req.headers.push((name_tpl.clone(), value_tpl.clone()));
                }
            }
        }
    }

    let opts = TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        ..TransportOpts::default()
    };

    plan.threads = args.threads;
    let num_threads = args.threads.max(1);

    let color = match args.color {
        CliColor::Always => ColorChoice::Always,
        CliColor::Auto => ColorChoice::Auto,
        CliColor::Never => ColorChoice::Never,
    };

    // Serial mode (default): run each scenario in isolation with the
    // full `-c N` connection pool against its own endpoint. Gives you
    // each scenario's ceiling without cross-scenario contention —
    // matches the wrk/Gatling convention.
    //
    // Parallel mode (`--parallel`): interleave all scenarios through a
    // shared pool. Models a realistic mixed-traffic client fleet.
    let target_rps_val = if args.saturate { None } else { args.rate };

    if !args.parallel && plan.scenarios.len() > 1 {
        let scenarios = plan.scenarios.clone();
        let mut any_errors = false;
        let mut any_requests = false;
        for (i, sc) in scenarios.iter().enumerate() {
            let mut sub_plan = plan.clone();
            sub_plan.scenarios = vec![sc.clone()];
            println!(
                "\n─── scenario {}/{}: {} ({:?}) ───",
                i + 1,
                scenarios.len(),
                sc.name,
                sc.protocol(),
            );
            let summary = dispatch_multi_protocol_plan(
                &sub_plan,
                &target,
                &opts,
                args.connections,
                num_threads,
                target_rps_val,
            )?;
            render_report(
                &summary,
                &sub_plan,
                args.format,
                args.output.as_deref(),
                color,
            )?;
            if summary.errors.hard_total() > 0 {
                any_errors = true;
            }
            if summary.requests > 0 {
                any_requests = true;
            }
        }
        return Ok(if any_errors || !any_requests {
            ExitCode::from(1)
        } else {
            ExitCode::SUCCESS
        });
    }

    // Single scenario OR --parallel → single dispatch covering all.
    // `--saturate` trumps any rate in the script or CLI. Otherwise, if
    // the user passed --rate, use it. Otherwise, leave None and let the
    // dispatcher pick up the per-scenario RateProfile.
    let target_rps = target_rps_val;

    // All three backend groups use HTTP/1.1 ALPN on the Rhai path —
    // build one shared TLS config to hand to `run_plan`.
    let tls_config = if target.tls {
        Some(zerobench_backends::http::mio_tls::build_tls_config(
            &opts,
            &[b"http/1.1"],
        ))
    } else {
        None
    };

    let ctx = zerobench_backends::RunCtx {
        target: target.clone(),
        opts: opts.clone(),
        duration: plan.duration,
        num_threads,
        connections: args.connections,
        target_rps,
        tls_config,
        live: None,
        stop: None,
    };
    let all_stats = zerobench_backends::run_plan(&plan, &ctx);

    let summary = Summary::merge(all_stats, plan.duration);

    render_report(&summary, &plan, args.format, args.output.as_deref(), color)?;

    // Exit-code policy: hard transport errors (connect/read/write/
    // timeout/keepup) or zero operations completed → exit 1. 4xx/5xx
    // and assertion failures are signal, not infrastructure problems,
    // so they do not gate exit status. Matches the measure-verb path.
    if summary.errors.hard_total() > 0 || summary.requests == 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}
