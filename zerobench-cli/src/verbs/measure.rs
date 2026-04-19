//! `zerobench measure URL` — the headline verb.
//!
//! Implements `docs/design-v0.1.0.md` §2.3 and PHILOSOPHY §5. The
//! flow for every invocation:
//!
//! 1. Build a [`Plan`] with `mode = Mode::Measure`, the caller's
//!    duration / warmup / cooldown / runs.
//! 2. Compute `plan_hash`, `url_fingerprint`, `target_fingerprint`,
//!    `run_id`.
//! 3. Run the [`ClientSelfCheck`] against loopback — refuse the
//!    run when the client can't sustain the offered rate, unless
//!    `--force-overload` was passed.
//! 4. Collect [`MachineFingerprint`]. Refuse when the monotonic
//!    clock is coarser than 10 µs unless `--allow-coarse-clock`
//!    was passed.
//! 5. Open [`ArchiveWriter`] and write `plan.json`, `machine.json`,
//!    `env.json` before the real run starts.
//! 6. Dispatch `runs` consecutive benchmark runs to the protocol-
//!    appropriate backend (HTTP / SseHold / WsEchoRtt / fanout /
//!    reconnect-storm / ...), with `cooldown` between runs.
//! 7. Merge per-run stats into a [`Summary`]; print a compact report.
//! 8. Emit `result.json` + `.histlog`, stamp `env.ended_at_unix`,
//!    rewrite `env.json`, finalise `INDEX.json`.

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
use zerobench_core::plan::{
    CorrelateStrategy, Mode, Plan, Protocol, RateProfile, RequestPlan, Scenario, SseHoldPlan,
    Step, WsEchoRttPlan,
};
use zerobench_core::template::Template;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::var::VarRegistry;
use zerobench_core::stats::{ErrorCountersExport, LatencyExport, PerRunMetrics};
use zerobench_core::report::pick_primary_histogram;
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
    #[arg(long = "ws-payload", default_value = "ping", help_heading = "WebSocket")]
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
    #[arg(long = "trigger-url", value_name = "URL",
          help_heading = "Fanout")]
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
    #[arg(long = "kill-rate", value_name = "RATE",
          default_value = "0.1",
          help_heading = "SSE")]
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
    // -------------------------------------------------------------------
    // Plan construction
    // -------------------------------------------------------------------

    // TUI is wired for the top-level `zerobench <url>` dispatch path
    // but not for the `measure` verb's multi-run + archive lifecycle:
    // the per-run reset / cooldown interaction with a single
    // long-lived live snapshot is different enough that we reject
    // the combination explicitly rather than silently rendering
    // nothing. Users who need live metrics for a one-off should
    // use the top-level form, which carries the TUI wiring end to
    // end. The flag is accepted on `measure` so that `zerobench
    // measure ... --tui` produces a clear, actionable error rather
    // than a Clap parse failure.
    if args.tui {
        return Err(
            "--tui is not supported by the `measure` verb; use `zerobench <url> --tui` for live metrics".into(),
        );
    }

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
    // client self-check
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

        // P10 / PHILOSOPHY §9.6.2: hard floor on the client's own
        // scheduler jitter. If the loopback self-check's p99 is above
        // 5µs, the client's noise floor dominates any real
        // measurement — percentile comparisons become meaningless.
        // Refuse unless --force-overload is passed.
        //
        // Guard against an empty jitter histogram: HDR returns 0/1
        // for value_at_percentile on no samples, which would silently
        // pass the gate. Treat "no samples" as a broken self-check.
        const JITTER_P99_FLOOR_NS: u64 = 5_000;
        if result.jitter.len() == 0 {
            return Err(
                "client self-check produced no jitter samples — calibration \
                 is broken; cannot verify the scheduler noise floor. Pass \
                 --no-calibrate to skip the gate."
                    .into(),
            );
        }
        let jitter_p99 = result.jitter.value_at_percentile(99.0);
        if jitter_p99 > JITTER_P99_FLOOR_NS && !args.force_overload {
            return Err(format!(
                "client scheduler jitter p99 is {} ns (> {} ns floor). Real \
                 percentile deltas this run would be indistinguishable from \
                 client noise. Disable CPU frequency scaling / pin the \
                 process, pass --no-calibrate to skip, or --force-overload \
                 to run anyway.",
                jitter_p99, JITTER_P99_FLOOR_NS
            )
            .into());
        }
        if jitter_p99 > JITTER_P99_FLOOR_NS {
            eprintln!(
                "[calibrate] --force-overload: jitter p99 {} ns > {} ns floor; \
                 comparison deltas under ~2× that figure are noise.",
                jitter_p99, JITTER_P99_FLOOR_NS
            );
        }
    }

    // -------------------------------------------------------------------
    // machine fingerprint
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
    // archive setup — writes plan/machine/env before the run.
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

        // Protocol-dispatched: the first scenario's protocol drives
        // the backend choice. Single-scenario plans in the measure
        // verb keep this simple; multi-scenario plans (Rhai) go
        // through a different code path.
        let scenario_protocol = plan
            .scenarios
            .first()
            .map(|s| s.protocol())
            .unwrap_or(Protocol::Http);

        // Warmup: fire traffic, discard. Only on iteration 0 — we
        // amortise the warmup cost across the N runs per PHILOSOPHY
        // §P8. Mio dispatch doesn't natively separate warmup from
        // measure yet — we model it as a prefixed short run whose
        // stats we drop on the floor.
        if !args.warmup.is_zero() && run_idx == 0 {
            eprintln!("[warmup] {} (discarded)...", format_duration(args.warmup));
            // Dispatch on the FIRST non-Pause step type, not just
            // Protocol. A SseFanout plan warmed up with run_sse_hold
            // would measure the wrong backend — and possibly
            // exhaust the trigger rate budget / poison server
            // state — before the real run starts.
            let first_step = plan.scenarios.first().and_then(|s| {
                s.steps.iter().find(|st| {
                    !matches!(
                        st,
                        zerobench_core::plan::Step::Pause(_)
                            | zerobench_core::plan::Step::PauseRandom { .. }
                    )
                })
            });
            match first_step {
                Some(zerobench_core::plan::Step::HttpColdConnect(_)) => {
                    let _ = zerobench_http::cold_connect::run_cold_connect_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.connections as u32,
                        args.warmup,
                        target_rate,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "sse")]
                Some(zerobench_core::plan::Step::SseHold(_)) => {
                    let _ = zerobench_sse::run_sse_hold_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "sse")]
                Some(zerobench_core::plan::Step::SseFanout(_)) => {
                    let _ = zerobench_sse::run_sse_fanout_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "sse")]
                Some(zerobench_core::plan::Step::SseReconnectStorm(_)) => {
                    let _ = zerobench_sse::run_sse_reconnect_storm_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "ws")]
                Some(zerobench_core::plan::Step::WsEchoRtt(_)) => {
                    let _ = zerobench_ws::run_ws_echo_rtt_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "ws")]
                Some(zerobench_core::plan::Step::WsHold(_)) => {
                    let _ = zerobench_ws::run_ws_hold_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "ws")]
                Some(zerobench_core::plan::Step::WsServerPushRtt(_)) => {
                    let _ = zerobench_ws::run_ws_server_push_rtt_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                #[cfg(feature = "ws")]
                Some(zerobench_core::plan::Step::WsFanout(_)) => {
                    let _ = zerobench_ws::run_ws_fanout_from_plan_threaded(
                        &target,
                        &opts,
                        &plan,
                        args.warmup,
                        tls_config.clone(),
                        None,
                        None,
                    );
                }
                // Default (Request / None / other) — HTTP backend.
                _ => {
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
            }
        }

        eprintln!(
            "[run {}/{}] starting ({}, {:?})...",
            run_idx + 1,
            args.runs,
            format_duration(args.duration),
            scenario_protocol,
        );
        let stop: Option<Arc<AtomicBool>> = None;
        // Route to cold-connect when ANY HTTP scenario contains a
        // HttpColdConnect step. Strictly correct per-scenario
        // dispatch would require splitting the plan and driving each
        // HTTP scenario through its own backend; the single-scenario
        // measure verb never hits that path (the CLI-built plan has
        // exactly one scenario), and mixed-cold/hot Rhai plans are
        // a known limitation documented in the gap audit.
        let any_http_is_cold = plan.scenarios.iter().any(|s| {
            s.protocol() == Protocol::Http
                && s.steps.iter().any(|st| {
                    matches!(st, zerobench_core::plan::Step::HttpColdConnect(_))
                })
        });
        let stats = match scenario_protocol {
            Protocol::Http if any_http_is_cold => {
                zerobench_http::cold_connect::run_cold_connect_from_plan_threaded(
                    &target,
                    &opts,
                    &plan,
                    args.connections as u32,
                    args.duration,
                    target_rate,
                    tls_config.clone(),
                    None,
                    stop,
                )
            }
            Protocol::Http => zerobench_http::mio_h1::run_mio_threaded(
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
            ),
            Protocol::Sse => {
                #[cfg(feature = "sse")]
                {
                    let first_sse_step = plan.scenarios.first().and_then(|s| {
                        s.steps.iter().find(|st| {
                            !matches!(
                                st,
                                zerobench_core::plan::Step::Pause(_)
                                    | zerobench_core::plan::Step::PauseRandom { .. }
                            )
                        })
                    });
                    match first_sse_step {
                        Some(zerobench_core::plan::Step::SseFanout(_)) => {
                            zerobench_sse::run_sse_fanout_from_plan_threaded(
                                &target,
                                &opts,
                                &plan,
                                args.duration,
                                tls_config.clone(),
                                None,
                                stop,
                            )
                        }
                        Some(zerobench_core::plan::Step::SseReconnectStorm(_)) => {
                            zerobench_sse::run_sse_reconnect_storm_from_plan_threaded(
                                &target,
                                &opts,
                                &plan,
                                args.duration,
                                tls_config.clone(),
                                None,
                                stop,
                            )
                        }
                        _ => zerobench_sse::run_sse_hold_from_plan_threaded(
                            &target,
                            &opts,
                            &plan,
                            args.duration,
                            tls_config.clone(),
                            None,
                            stop,
                        ),
                    }
                }
                #[cfg(not(feature = "sse"))]
                {
                    return Err("SSE scenario requires `--features sse`".into());
                }
            }
            Protocol::Ws => {
                #[cfg(feature = "ws")]
                {
                    // Sub-dispatch within WS.
                    let first_ws_step = plan
                        .scenarios
                        .first()
                        .and_then(|s| s.steps.iter().find(|st| {
                            !matches!(
                                st,
                                zerobench_core::plan::Step::Pause(_)
                                    | zerobench_core::plan::Step::PauseRandom { .. }
                            )
                        }));
                    match first_ws_step {
                        Some(zerobench_core::plan::Step::WsHold(_)) => {
                            zerobench_ws::run_ws_hold_from_plan_threaded(
                                &target,
                                &opts,
                                &plan,
                                args.duration,
                                tls_config.clone(),
                                None,
                                stop,
                            )
                        }
                        Some(zerobench_core::plan::Step::WsServerPushRtt(_)) => {
                            zerobench_ws::run_ws_server_push_rtt_from_plan_threaded(
                                &target,
                                &opts,
                                &plan,
                                args.duration,
                                tls_config.clone(),
                                None,
                                stop,
                            )
                        }
                        Some(zerobench_core::plan::Step::WsFanout(_)) => {
                            zerobench_ws::run_ws_fanout_from_plan_threaded(
                                &target,
                                &opts,
                                &plan,
                                args.duration,
                                tls_config.clone(),
                                None,
                                stop,
                            )
                        }
                        _ => zerobench_ws::run_ws_echo_rtt_from_plan_threaded(
                            &target,
                            &opts,
                            &plan,
                            args.duration,
                            tls_config.clone(),
                            None,
                            stop,
                        ),
                    }
                }
                #[cfg(not(feature = "ws"))]
                {
                    return Err("WS scenario requires `--features ws`".into());
                }
            }
        };

        // Op count semantics per protocol:
        //   HTTP → stats.requests is req count
        //   SSE  → stats.requests is event count (populated by
        //          run_sse_hold_from_plan_threaded which treats each
        //          SSE event as an op per PHILOSOPHY §4.3)
        let run_ops: u64 = stats.iter().map(|s| s.requests).sum();
        // Op-label picks on the FIRST non-Pause step type, not just
        // protocol, because the protocol-native variants have
        // distinct unit semantics (e.g. SseHold's op is "event"
        // whereas SseFanout's op is "broadcast").
        let first_step_for_label = plan.scenarios.first().and_then(|s| {
            s.steps.iter().find(|st| {
                !matches!(
                    st,
                    zerobench_core::plan::Step::Pause(_)
                        | zerobench_core::plan::Step::PauseRandom { .. }
                )
            })
        });
        let op_label = match first_step_for_label {
            Some(zerobench_core::plan::Step::HttpColdConnect(_)) => "cold-connects",
            Some(zerobench_core::plan::Step::SseFanout(_)) => "broadcasts",
            Some(zerobench_core::plan::Step::SseReconnectStorm(_)) => "events",
            Some(zerobench_core::plan::Step::SseHold(_)) => "events",
            Some(zerobench_core::plan::Step::WsHold(_)) => "frames",
            Some(zerobench_core::plan::Step::WsFanout(_)) => "broadcasts",
            Some(zerobench_core::plan::Step::WsServerPushRtt(_)) => "messages",
            Some(zerobench_core::plan::Step::WsEchoRtt(_)) => "messages",
            _ => match scenario_protocol {
                Protocol::Http => "requests",
                Protocol::Sse => "events",
                Protocol::Ws => "messages",
            },
        };
        eprintln!(
            "[run {}/{}] {} {}",
            run_idx + 1,
            args.runs,
            run_ops,
            op_label
        );

        // capture per-run metrics before merging into the
        // aggregate. These are the elementary samples the bootstrap
        // CI resamples over. Cloning TaskStats here is cheap (HDR
        // histograms are contiguous u64 arrays; for typical bounds
        // that's ~30 KiB per stat).
        let run_summary = Summary::merge(stats.clone(), args.duration);
        // The protocol-native primary histogram per §P7 / §6c of
        // the design. Compare-engine's `--regress-on` reads this
        // slot when set; pure-HTTP runs leave it empty and the
        // engine falls through to the generic `latency` field.
        let proto_hist = pick_primary_histogram(&run_summary, &plan);
        let proto_latency = if std::ptr::eq(
            proto_hist as *const _,
            &run_summary.latency as *const _,
        ) {
            LatencyExport::default()
        } else {
            LatencyExport::from_hist(proto_hist)
        };
        per_run.push(PerRunMetrics {
            index: run_idx,
            rate_per_s: run_summary.requests_per_sec(),
            requests: run_summary.requests,
            errors_total: run_summary.errors.total(),
            latency: LatencyExport::from_hist(&run_summary.latency),
            protocol_latency: proto_latency,
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

        // emit result.json with the Summary projection.
        let mut export = summary.to_export();
        // attach per-run metric vectors so the compare
        // engine can bootstrap CIs from the elementary samples.
        export.per_run = per_run;
        writer.write_result(&export)?;

        // canonical HDR-V2-compressed-log sidecar. Readable
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
        // PHILOSOPHY §P7 / design §5c: downstream tooling (HDR
        // plotter, Grafana) consumes the histlog. For HTTP the main
        // `summary.latency` is the right histogram; for
        // protocol-native backends it's empty — the real signal
        // lives in the per-scenario SseExtras / WsExtras slot.
        // Pick the primary histogram per the same per-protocol
        // rules as the terminal report.
        let primary_hist = pick_primary_histogram(&summary, &plan);
        writer.write_histlog(
            "result",
            primary_hist,
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

    // Exit code gates on *transport* errors (connect/read/write/
    // timeout/keepup) only. 4xx/5xx and assertion failures are part
    // of the benchmark signal — a load test against a route that
    // legitimately 404s should still exit 0.
    let hard_errors = summary.errors.hard_total();
    if summary.requests == 0 || hard_errors > 0 {
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
    let url = Template::compile(&args.url, &mut vars)?;

    let scenario = if let Some(subscribers) = args.sse_fanout {
        let Some(trig) = args.trigger_url.as_ref() else {
            return Err("--sse-fanout requires --trigger-url".into());
        };
        let trigger = Template::compile(trig, &mut vars)?;
        let hold_for = args.hold_for.unwrap_or(args.duration);
        Scenario {
            name: "sse-fanout".into(),
            rate: RateProfile::Saturate {
                max_concurrency: subscribers as usize,
            },
            steps: vec![Step::SseFanout(zerobench_core::plan::SseFanoutPlan {
                subscribers: SseHoldPlan {
                    url,
                    headers: SmallVec::new(),
                    subscribers,
                    hold_for,
                    reconnect: true,
                },
                trigger: zerobench_core::plan::TriggerSpec::HttpPost {
                    url: trigger,
                    body: None,
                },
                mode: zerobench_core::plan::FanoutMode::TriggerRtt,
            })],
        }
    } else if let Some(connections) = args.ws_fanout {
        let Some(trig) = args.trigger_url.as_ref() else {
            return Err("--ws-fanout requires --trigger-url".into());
        };
        let trigger = Template::compile(trig, &mut vars)?;
        let hold_for = args.hold_for.unwrap_or(args.duration);
        Scenario {
            name: "ws-fanout".into(),
            rate: RateProfile::Saturate {
                max_concurrency: connections as usize,
            },
            steps: vec![Step::WsFanout(zerobench_core::plan::WsFanoutPlan {
                subscribers: zerobench_core::plan::WsHoldPlan {
                    url,
                    headers: SmallVec::new(),
                    connections,
                    heartbeat: args.ws_heartbeat.unwrap_or(Duration::from_secs(25)),
                    heartbeat_frame: zerobench_core::plan::HeartbeatFrame::Ping,
                    hold_for,
                },
                trigger: zerobench_core::plan::TriggerSpec::HttpPost {
                    url: trigger,
                    body: None,
                },
                mode: zerobench_core::plan::FanoutMode::TriggerRtt,
            })],
        }
    } else if let Some(subscribers) = args.sse_reconnect_storm {
        let hold_for = args.hold_for.unwrap_or(args.duration);
        Scenario {
            name: "sse-reconnect-storm".into(),
            rate: RateProfile::Saturate {
                max_concurrency: subscribers as usize,
            },
            steps: vec![Step::SseReconnectStorm(
                zerobench_core::plan::SseReconnectStormPlan {
                    subscribers: SseHoldPlan {
                        url,
                        headers: SmallVec::new(),
                        subscribers,
                        hold_for,
                        reconnect: true,
                    },
                    kill_rate_per_s: args.kill_rate,
                    verify_last_event_id: true,
                },
            )],
        }
    } else if let Some(connections) = args.ws_hold {
        // WsHold — N persistent connections + heartbeat.
        let hold_for = args.hold_for.unwrap_or(args.duration);
        let heartbeat = args.ws_heartbeat.unwrap_or(Duration::from_secs(25));
        Scenario {
            name: "ws-hold".into(),
            rate: RateProfile::Saturate {
                max_concurrency: connections as usize,
            },
            steps: vec![Step::WsHold(zerobench_core::plan::WsHoldPlan {
                url,
                headers: SmallVec::new(),
                connections,
                heartbeat,
                heartbeat_frame: zerobench_core::plan::HeartbeatFrame::Ping,
                hold_for,
            })],
        }
    } else if let Some(connections) = args.ws_push {
        // WsServerPushRtt — read-only, measure inter-message gap.
        let hold_for = args.hold_for.unwrap_or(args.duration);
        Scenario {
            name: "ws-push".into(),
            rate: RateProfile::Saturate {
                max_concurrency: connections as usize,
            },
            steps: vec![Step::WsServerPushRtt(
                zerobench_core::plan::WsServerPushRttPlan {
                    url,
                    headers: SmallVec::new(),
                    connections,
                    expected_rate_per_conn: args.ws_expected_rate,
                    hold_for,
                },
            )],
        }
    } else if let Some(connections) = args.ws_echo {
        // WS Echo RTT mode — build Step::WsEchoRtt with
        // N persistent connections at msg_rate/conn.
        let payload = Template::compile(&args.ws_payload, &mut vars)?;
        Scenario {
            name: "ws-echo-rtt".into(),
            rate: RateProfile::Saturate {
                max_concurrency: connections as usize,
            },
            steps: vec![Step::WsEchoRtt(WsEchoRttPlan {
                url,
                headers: SmallVec::new(),
                connections,
                msg_rate_per_conn: args.ws_msg_rate,
                correlate: CorrelateStrategy::MonotonicIdPrepend,
                payload,
            })],
        }
    } else if let Some(subscribers) = args.sse_hold {
        // SSE Hold mode — build Step::SseHold with
        // N subscribers held for `hold_for` (defaults to --duration).
        let hold_for = args.hold_for.unwrap_or(args.duration);
        Scenario {
            name: "sse-hold".into(),
            rate: RateProfile::Saturate {
                max_concurrency: subscribers as usize,
            },
            steps: vec![Step::SseHold(SseHoldPlan {
                url,
                headers: SmallVec::new(),
                subscribers,
                hold_for,
                reconnect: args.sse_reconnect,
            })],
        }
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

        let step = if args.cold_connect {
            Step::HttpColdConnect(zerobench_core::plan::ColdConnectPlan {
                request,
            })
        } else {
            Step::Request(request)
        };
        Scenario {
            name: "measure".into(),
            rate,
            steps: vec![step],
        }
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
