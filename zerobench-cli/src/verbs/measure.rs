//! `zerobench measure URL` — the headline verb.
//!
//! Implements `docs/design-v0.1.0.md` §2.3 and PHILOSOPHY §5. The flow
//! for every invocation is the standard verb-level choreography:
//!
//! 1. Build a [`Plan`] with `mode = Mode::Measure` from CLI args.
//! 2. Run the client self-check gate (via
//!    [`zerobench_runtime::runner::calibrate`]).
//! 3. Collect [`MachineFingerprint`] and refuse if the monotonic clock
//!    is coarse.
//! 4. Open an [`ArchiveSession`] (fingerprints + pre-run sidecars).
//! 5. For each run: warmup (first run only) → steady-state →
//!    cooldown (before subsequent runs).
//! 6. Merge per-run stats into a [`Summary`] and render.
//! 7. Finalise the archive (env end-time + result.json + .histlog +
//!    INDEX.json).
//!
//! Steps 2, 4, and 7 live in `zerobench-runtime::runner`; measure.rs
//! owns steps 1, 5, and 6.

use std::process::ExitCode;
use std::time::Duration;

use clap::{ArgAction, Args};
use smallvec::SmallVec;

use zerobench_core::plan::{
    CorrelateStrategy, FanoutMode, HeartbeatFrame, Mode, Plan, RateProfile, RequestPlan,
    TriggerSpec,
};
use zerobench_core::plan_builder::{
    scenario_http_cold_connect, scenario_http_request, scenario_sse_fanout, scenario_sse_hold,
    scenario_sse_reconnect_storm, scenario_ws_echo_rtt, scenario_ws_fanout, scenario_ws_hold,
    scenario_ws_server_push_rtt, PlanBuilder,
};
use zerobench_core::stats::{LatencyExport, PerRunMetrics};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::Summary;
use zerobench_report::report::pick_primary_histogram;
use zerobench_report::ColorChoice;
use zerobench_runtime::machine::MachineFingerprint;
use zerobench_runtime::runner::{calibrate, op_label_for, ArchiveSession, RunHarness};

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
    /// Target URL. Scheme selects the protocol: http/https for HTTP,
    /// and combined with one of the protocol flags for SSE/WS
    /// (`--sse-hold`, `--ws-echo`, etc.).
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
    #[arg(
        short = 'c',
        long = "connections",
        default_value_t = 50,
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

    /// Enable the live TUI. Streams per-second req/s, p50/p99, error
    /// rate, and per-scenario breakdowns to the terminal. Incompatible
    /// with `--format jsonl` (both write to stdout). Requires the
    /// `tui` feature to have useful output; without it the flag is
    /// accepted but produces headless behaviour.
    #[arg(long = "tui", action = ArgAction::SetTrue, help_heading = "Display")]
    pub tui: bool,

    // -------------------------------------------------------------------
    // SSE Hold
    //
    // When `--sse-hold N` is passed, the verb builds a
    // `Step::SseHold` scenario instead of the default HTTP GET.
    // Measures production-relevant SSE semantics per PHILOSOPHY §4.3:
    // N persistent subscribers, "op = event received", chunk-gap p99
    // as the primary latency metric.
    // -------------------------------------------------------------------
    /// Open N concurrent SSE subscribers and hold them for `--for`.
    /// URL must be an SSE endpoint (`text/event-stream`); measures
    /// events/s and inter-event gap rather than req/s.
    #[arg(
        long = "sse-hold",
        value_name = "SUBSCRIBERS",
        help_heading = "SSE",
        conflicts_with_all = [
            "ws_echo", "cold_connect", "ws_hold", "ws_push",
            "sse_fanout", "ws_fanout", "sse_reconnect_storm",
        ],
    )]
    pub sse_hold: Option<u32>,

    /// Subscriber hold duration for `--sse-hold`. Defaults to
    /// `--duration` when omitted.
    #[arg(long = "for", value_name = "DURATION",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "SSE")]
    pub hold_for: Option<Duration>,

    /// Whether subscribers follow the WHATWG EventSource reconnect
    /// protocol when the server closes. Default `true`.
    #[arg(long = "sse-reconnect", action = ArgAction::SetTrue,
          help_heading = "SSE")]
    pub sse_reconnect: bool,

    // -------------------------------------------------------------------
    // WebSocket EchoRtt
    //
    // When `--ws-echo N` is passed, the verb builds a
    // `Step::WsEchoRtt` scenario. N persistent connections each send
    // text frames at `--msg-rate` req/s per conn; every send expects
    // a correlated echo; RTT is the primary latency axis.
    // -------------------------------------------------------------------
    /// Open N persistent WS connections and measure echo RTT per
    /// message. URL must be `ws://` or `wss://`; the server must
    /// echo text frames verbatim (or preserve the 16-char monotonic
    /// id prefix for correlation).
    #[arg(
        long = "ws-echo",
        value_name = "CONNECTIONS",
        help_heading = "WebSocket",
        conflicts_with_all = [
            "sse_hold", "cold_connect", "ws_hold", "ws_push",
            "sse_fanout", "ws_fanout", "sse_reconnect_storm",
        ],
    )]
    pub ws_echo: Option<u32>,

    /// Per-connection send rate for `--ws-echo`. Defaults to 100 msg/s
    /// per connection.
    #[arg(long = "msg-rate", value_name = "RATE",
          value_parser = super::super::cli_args::parse_rate_flag,
          default_value = "100", help_heading = "WebSocket")]
    pub ws_msg_rate: f64,

    /// Text payload body for each send (the 16-char monotonic id
    /// prefix is prepended automatically for correlation).
    #[arg(
        long = "ws-payload",
        default_value = "ping",
        help_heading = "WebSocket"
    )]
    pub ws_payload: String,

    // -------------------------------------------------------------------
    // HTTP cold-connect
    //
    // Fresh TCP + TLS + HTTP connection per op — measures accept +
    // handshake throughput separately from steady-state pool
    // performance (PHILOSOPHY §4.2, design §3.1).
    // -------------------------------------------------------------------
    /// Open a fresh connection for every request — no pool reuse.
    /// Records handshake + TTFB together as the primary latency;
    /// meaningful only for HTTP targets.
    #[arg(
        long = "cold-connect",
        action = ArgAction::SetTrue,
        help_heading = "HTTP",
        conflicts_with_all = [
            "sse_hold", "ws_echo", "ws_hold", "ws_push",
            "sse_fanout", "ws_fanout", "sse_reconnect_storm",
        ],
    )]
    pub cold_connect: bool,

    // -------------------------------------------------------------------
    // WsHold — idle-capacity test
    // -------------------------------------------------------------------
    /// Open N persistent WS connections and hold them open with
    /// periodic heartbeats — measures idle-capacity / conn-drop rate.
    #[arg(
        long = "ws-hold",
        value_name = "CONNECTIONS",
        help_heading = "WebSocket",
        conflicts_with_all = [
            "sse_hold", "ws_echo", "cold_connect", "ws_push",
            "sse_fanout", "ws_fanout", "sse_reconnect_storm",
        ],
    )]
    pub ws_hold: Option<u32>,

    /// Heartbeat interval for `--ws-hold`. Default 25s (margin
    /// against common 30/60s proxy idle timeouts).
    #[arg(long = "ws-heartbeat", value_name = "DURATION",
          value_parser = super::super::cli_args::parse_duration_flag,
          help_heading = "WebSocket")]
    pub ws_heartbeat: Option<Duration>,

    // -------------------------------------------------------------------
    // WsServerPushRtt — read-only inter-message gap
    // -------------------------------------------------------------------
    /// Open N persistent WS connections and only read inbound frames.
    /// Records the inter-message gap distribution; flags a stall if
    /// observed rate < 50% of `--ws-expected-rate`.
    #[arg(
        long = "ws-push",
        value_name = "CONNECTIONS",
        help_heading = "WebSocket",
        conflicts_with_all = [
            "sse_hold", "ws_echo", "cold_connect", "ws_hold",
            "sse_fanout", "ws_fanout", "sse_reconnect_storm",
        ],
    )]
    pub ws_push: Option<u32>,

    /// Expected server-push rate per connection (msg/s). When set,
    /// drives the stall-detection gate for `--ws-push`.
    #[arg(long = "ws-expected-rate", value_name = "RATE",
          value_parser = super::super::cli_args::parse_rate_flag,
          default_value = "0",
          help_heading = "WebSocket")]
    pub ws_expected_rate: f64,

    // -------------------------------------------------------------------
    // Fanout / reconnect-storm (CLI surface — Rhai is richer)
    //
    // These need a trigger URL (for fanouts) or a kill rate (for
    // storm), so the CLI accepts the minimum viable knobs. Multi-
    // header / advanced scheduling still goes through `zerobench run`
    // with a Rhai script.
    // -------------------------------------------------------------------
    /// Open N SSE subscribers and fire `--trigger-url` periodically;
    /// report broadcast RTT.
    #[arg(
        long = "sse-fanout",
        value_name = "SUBSCRIBERS",
        help_heading = "SSE",
        conflicts_with_all = [
            "sse_hold", "ws_echo", "cold_connect", "ws_hold", "ws_push",
            "ws_fanout", "sse_reconnect_storm",
        ],
    )]
    pub sse_fanout: Option<u32>,

    /// Open N WS subscribers and fire `--trigger-url` periodically;
    /// report broadcast RTT.
    #[arg(
        long = "ws-fanout",
        value_name = "CONNECTIONS",
        help_heading = "WebSocket",
        conflicts_with_all = [
            "sse_hold", "ws_echo", "cold_connect", "ws_hold", "ws_push",
            "sse_fanout", "sse_reconnect_storm",
        ],
    )]
    pub ws_fanout: Option<u32>,

    /// Trigger URL for `--sse-fanout` / `--ws-fanout`. HTTP POST.
    /// Templates (`{{counter}}`, `{{uuid}}`) re-render per firing.
    #[arg(long = "trigger-url", value_name = "URL", help_heading = "Fanout")]
    pub trigger_url: Option<String>,

    /// Open N SSE subscribers and kill them at `--kill-rate` per second
    /// (exponential interval), reconnecting with Last-Event-ID.
    #[arg(
        long = "sse-reconnect-storm",
        value_name = "SUBSCRIBERS",
        help_heading = "SSE",
        conflicts_with_all = [
            "sse_hold", "ws_echo", "cold_connect", "ws_hold", "ws_push",
            "sse_fanout", "ws_fanout",
        ],
    )]
    pub sse_reconnect_storm: Option<u32>,

    /// Aggregate kill rate (per subscriber per second) for
    /// `--sse-reconnect-storm`.
    #[arg(
        long = "kill-rate",
        value_name = "RATE",
        default_value = "0.1",
        help_heading = "SSE"
    )]
    pub kill_rate: f64,
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
    // TUI is wired for the top-level `zerobench <url>` dispatch path
    // but not for the `measure` verb's multi-run + archive lifecycle.
    // Reject the combination explicitly so users get a clear, actionable
    // message rather than silent no-op rendering.
    if args.tui {
        return Err(
            "--tui is not supported by the `measure` verb; use `zerobench <url> --tui` for live metrics".into(),
        );
    }

    // -------------------------------------------------------------------
    // Plan + transport
    // -------------------------------------------------------------------
    let target = Target::parse(&args.url)?;
    let name = args.name.clone().unwrap_or_else(|| target.host.clone());
    let plan = build_measure_plan(&args, &target, &name)?;
    let opts = build_transport_opts(&args);
    let resolved = target.resolve(&opts)?;

    // -------------------------------------------------------------------
    // Calibration gate
    // -------------------------------------------------------------------
    let calibration_skipped = args.no_calibrate;
    let mut force_overload = args.force_overload;
    if !calibration_skipped {
        // Saturate mode has no nominal rate; calibrate against a
        // conservative 10k/s baseline to prove the scheduler isn't the
        // bottleneck on this machine.
        let target_rate = args.rate.unwrap_or(10_000.0);
        eprintln!(
            "[calibrate] self-check at {:.0} req/s against loopback (~2s)...",
            target_rate
        );
        let report = calibrate(target_rate, Duration::from_secs(2), args.force_overload)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        eprintln!(
            "[calibrate] achieved {:.0}/{:.0} req/s — verdict: {:?}, jitter p99 {} ns",
            report.achieved_rate, report.target_rate, report.verdict, report.jitter_p99_ns,
        );
        if report.poisoned {
            eprintln!(
                "[calibrate] --force-overload: gate bypassed; run will be flagged \
                 `force_overload=true` and poisons comparisons."
            );
            force_overload = true;
        }
    }

    // -------------------------------------------------------------------
    // Machine-clock gate
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
    // Archive setup
    // -------------------------------------------------------------------
    let archive = ArchiveSession::begin(
        &plan,
        &target,
        &[resolved],
        args.no_archive,
        args.context.clone(),
        calibration_skipped,
        force_overload,
        enabled_features(),
        env!("CARGO_PKG_VERSION"),
    )?;

    // -------------------------------------------------------------------
    // Harness
    // -------------------------------------------------------------------
    let tls_config = if target.tls {
        Some(zerobench_backends::http::mio_tls::build_tls_config(
            &opts,
            &[b"http/1.1"],
        ))
    } else {
        None
    };
    let harness = RunHarness::new_from(
        &target,
        &opts,
        args.threads,
        args.connections,
        args.rate,
        tls_config,
    );

    eprintln!(
        "[measure] {} runs × {} (warmup {}, cooldown {}) against {}",
        args.runs,
        format_duration(args.duration),
        format_duration(args.warmup),
        format_duration(args.cooldown),
        args.url,
    );

    // -------------------------------------------------------------------
    // Runs loop — warmup → (steady → cooldown) × runs
    // -------------------------------------------------------------------
    let mut all_stats = Vec::new();
    let mut per_run: Vec<PerRunMetrics> = Vec::new();
    for run_idx in 0..args.runs {
        if run_idx > 0 && !args.cooldown.is_zero() {
            eprintln!(
                "[cooldown] {} (TIME_WAIT drain)...",
                format_duration(args.cooldown)
            );
            std::thread::sleep(args.cooldown);
        }

        // Warmup is amortised across the N runs per PHILOSOPHY §P8 —
        // only fire on iteration 0.
        if !args.warmup.is_zero() && run_idx == 0 {
            eprintln!("[warmup] {} (discarded)...", format_duration(args.warmup));
            let _ = run_plan_with_harness(&plan, &harness, args.warmup);
        }

        eprintln!(
            "[run {}/{}] starting ({})...",
            run_idx + 1,
            args.runs,
            format_duration(args.duration)
        );
        let stats = run_plan_with_harness(&plan, &harness, args.duration);
        let run_ops: u64 = stats.iter().map(|s| s.requests).sum();
        eprintln!(
            "[run {}/{}] {} {}",
            run_idx + 1,
            args.runs,
            run_ops,
            op_label_for(&plan)
        );

        let run_summary = Summary::merge(stats.clone(), args.duration);
        per_run.push(build_per_run_metrics(run_idx, &run_summary, &plan));
        all_stats.extend(stats);
    }

    // -------------------------------------------------------------------
    // Merge + render + archive finalise
    // -------------------------------------------------------------------
    let total_measured = args.duration.saturating_mul(args.runs);
    let summary = Summary::merge(all_stats, total_measured);
    render_stdout(&summary, &plan);

    let comment = format!(
        "zerobench {} · plan={} · target={} · run_id={}",
        env!("CARGO_PKG_VERSION"),
        plan.name,
        args.url,
        archive.run_id(),
    );
    archive.finalise(
        &summary,
        per_run,
        &plan,
        total_measured,
        &comment,
        pick_primary_histogram,
    )?;

    // Exit code gates on *transport* errors only. 4xx/5xx and assertion
    // failures are benchmark signal — a load test against a 404 route
    // should still exit 0.
    let hard_errors = summary.errors.hard_total();
    if summary.requests == 0 || hard_errors > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

// ---------------------------------------------------------------------------
// Helpers — kept local to measure.rs
// ---------------------------------------------------------------------------

/// Build the `TransportOpts` a `measure` invocation hands to the backend.
fn build_transport_opts(args: &MeasureArgs) -> TransportOpts {
    TransportOpts {
        connect_timeout: args.connect_timeout,
        request_timeout: args.request_timeout,
        max_conns: args.connections,
        tcp_nodelay: true,
        insecure_tls: args.insecure,
        ..TransportOpts::default()
    }
}

/// Thin wrapper that calls `zerobench_backends::run_plan` with a context
/// materialised from `harness.ctx_for(...)`. Keeps the verb's loop
/// readable while respecting the runtime → backends dep direction.
fn run_plan_with_harness(
    plan: &Plan,
    harness: &RunHarness,
    duration: Duration,
) -> Vec<zerobench_core::TaskStats> {
    let (target, opts, duration, threads, connections, rate, tls, live, stop) =
        harness.ctx_for(duration, None, None);
    let ctx = zerobench_backends::RunCtx {
        target,
        opts,
        duration,
        num_threads: threads,
        connections,
        target_rps: rate,
        tls_config: tls,
        live,
        stop,
    };
    zerobench_backends::run_plan(plan, &ctx)
}

/// Render the final report to stdout with terminal formatting.
fn render_stdout(summary: &Summary, plan: &Plan) {
    use std::io::{IsTerminal, Write};
    let is_tty = std::io::stdout().is_terminal();
    let mut out = std::io::stdout().lock();
    let _ = zerobench_report::print_terminal(summary, plan, ColorChoice::Auto, is_tty, &mut out);
    let _ = out.flush();
}

/// Build a `PerRunMetrics` entry from a single-run summary. These are
/// the elementary samples the diff tool's bootstrap CI resamples over.
fn build_per_run_metrics(idx: u32, summary: &Summary, plan: &Plan) -> PerRunMetrics {
    // Protocol-native primary histogram per §P7 / §6c. Compare engine's
    // `--regress-on` reads this slot when set; pure-HTTP runs leave it
    // empty and the engine falls through to the generic `latency` field.
    let proto_hist = pick_primary_histogram(summary, plan);
    let proto_latency = if std::ptr::eq(proto_hist as *const _, &summary.latency as *const _) {
        LatencyExport::default()
    } else {
        LatencyExport::from_hist(proto_hist)
    };
    PerRunMetrics {
        index: idx,
        rate_per_s: summary.requests_per_sec(),
        requests: summary.requests,
        errors_total: summary.errors.total(),
        latency: LatencyExport::from_hist(&summary.latency),
        protocol_latency: proto_latency,
    }
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------
//
// Per-protocol plan construction delegates to
// `zerobench_core::plan_builder::scenario_*`. Adding a field to a
// `*Plan` struct happens in one place now: the scenario constructor in
// core. The CLI translator (here) and the Rhai DSL
// (`zerobench_dsl::builders::compile_step`) both feed it.

fn build_measure_plan(
    args: &MeasureArgs,
    target: &Target,
    name: &str,
) -> Result<Plan, Box<dyn std::error::Error>> {
    let mut builder = PlanBuilder::new();
    builder
        .name(name)
        .duration(args.duration)
        .warmup(args.warmup)
        .cooldown(args.cooldown)
        .runs(args.runs)
        .threads(args.threads)
        .mode(Mode::Measure);

    let url = Template::compile(&args.url, builder.vars_mut())?;
    let hold_for_default = || args.hold_for.unwrap_or(args.duration);

    let scenario = if let Some(subscribers) = args.sse_fanout {
        let Some(trig) = args.trigger_url.as_ref() else {
            return Err("--sse-fanout requires --trigger-url".into());
        };
        let trigger = Template::compile(trig, builder.vars_mut())?;
        scenario_sse_fanout(
            "sse-fanout",
            subscribers,
            hold_for_default(),
            url,
            SmallVec::new(),
            true,
            TriggerSpec::HttpPost {
                url: trigger,
                body: None,
            },
            FanoutMode::TriggerRtt,
        )
    } else if let Some(connections) = args.ws_fanout {
        let Some(trig) = args.trigger_url.as_ref() else {
            return Err("--ws-fanout requires --trigger-url".into());
        };
        let trigger = Template::compile(trig, builder.vars_mut())?;
        scenario_ws_fanout(
            "ws-fanout",
            connections,
            hold_for_default(),
            url,
            SmallVec::new(),
            args.ws_heartbeat.unwrap_or(Duration::from_secs(25)),
            HeartbeatFrame::Ping,
            TriggerSpec::HttpPost {
                url: trigger,
                body: None,
            },
            FanoutMode::TriggerRtt,
        )
    } else if let Some(subscribers) = args.sse_reconnect_storm {
        scenario_sse_reconnect_storm(
            "sse-reconnect-storm",
            subscribers,
            hold_for_default(),
            url,
            SmallVec::new(),
            args.kill_rate,
            true,
        )
    } else if let Some(connections) = args.ws_hold {
        // WsHold — N persistent connections + heartbeat.
        scenario_ws_hold(
            "ws-hold",
            connections,
            hold_for_default(),
            url,
            SmallVec::new(),
            args.ws_heartbeat.unwrap_or(Duration::from_secs(25)),
            HeartbeatFrame::Ping,
        )
    } else if let Some(connections) = args.ws_push {
        // WsServerPushRtt — read-only, measure inter-message gap.
        scenario_ws_server_push_rtt(
            "ws-push",
            connections,
            hold_for_default(),
            url,
            SmallVec::new(),
            args.ws_expected_rate,
        )
    } else if let Some(connections) = args.ws_echo {
        // WS Echo RTT mode — N persistent connections at msg_rate/conn.
        let payload = Template::compile(&args.ws_payload, builder.vars_mut())?;
        scenario_ws_echo_rtt(
            "ws-echo-rtt",
            RateProfile::Saturate {
                max_concurrency: connections as usize,
            },
            url,
            SmallVec::new(),
            connections,
            args.ws_msg_rate,
            payload,
            CorrelateStrategy::MonotonicIdPrepend,
        )
    } else if let Some(subscribers) = args.sse_hold {
        // SSE Hold mode — N subscribers held for `hold_for`
        // (defaults to --duration).
        scenario_sse_hold(
            "sse-hold",
            subscribers,
            hold_for_default(),
            url,
            SmallVec::new(),
            args.sse_reconnect,
        )
    } else {
        // HTTP — single-step GET. Multi-step scenarios
        // go through `zerobench run` (Rhai).
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

        if args.cold_connect {
            scenario_http_cold_connect("measure", rate, request)
        } else {
            scenario_http_request("measure", rate, request)
        }
    };

    // `target` participates in url_fp via host/port/scheme/sni; the
    // plan itself doesn't embed the Target (RequestPlan's URL is the
    // authority). The argument is accepted for API symmetry with
    // sibling verb builders and future multi-scenario construction.
    let _ = target;

    builder.push_scenario(scenario);
    Ok(builder.finalize())
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
    let mut parts: Vec<&'static str> = vec!["h1", "h2", "sse", "ws", "script"];
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
    use zerobench_core::plan::{Protocol, Step};

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
            sse_hold: None,
            hold_for: None,
            sse_reconnect: false,
            ws_echo: None,
            ws_msg_rate: 100.0,
            ws_payload: "ping".to_string(),
            cold_connect: false,
            ws_hold: None,
            ws_heartbeat: None,
            ws_push: None,
            ws_expected_rate: 0.0,
            sse_fanout: None,
            ws_fanout: None,
            trigger_url: None,
            sse_reconnect_storm: None,
            kill_rate: 0.1,
            tui: false,
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
    fn sse_hold_flag_produces_sse_step() {
        let mut args = sample_args("http://sse.example.com/stream");
        args.sse_hold = Some(100);
        args.hold_for = Some(Duration::from_secs(30));
        args.sse_reconnect = true;

        let target = Target::parse(&args.url).unwrap();
        let plan = build_measure_plan(&args, &target, "x").unwrap();

        assert_eq!(plan.scenarios.len(), 1);
        assert_eq!(plan.scenarios[0].protocol(), Protocol::Sse);
        match &plan.scenarios[0].steps[0] {
            Step::SseHold(p) => {
                assert_eq!(p.subscribers, 100);
                assert_eq!(p.hold_for, Duration::from_secs(30));
                assert!(p.reconnect);
            }
            other => panic!("expected SseHold, got {other:?}"),
        }
    }

    #[test]
    fn sse_hold_defaults_hold_for_to_duration_when_omitted() {
        let mut args = sample_args("http://sse.example.com/stream");
        args.sse_hold = Some(10);
        args.duration = Duration::from_secs(5);
        args.hold_for = None;

        let target = Target::parse(&args.url).unwrap();
        let plan = build_measure_plan(&args, &target, "x").unwrap();

        match &plan.scenarios[0].steps[0] {
            Step::SseHold(p) => assert_eq!(p.hold_for, Duration::from_secs(5)),
            _ => panic!("expected SseHold"),
        }
    }

    #[test]
    fn http_plan_built_when_sse_hold_absent() {
        let args = sample_args("http://x:1/");
        let target = Target::parse(&args.url).unwrap();
        let plan = build_measure_plan(&args, &target, "x").unwrap();
        assert_eq!(plan.scenarios[0].protocol(), Protocol::Http);
        assert!(matches!(plan.scenarios[0].steps[0], Step::Request(_)));
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
