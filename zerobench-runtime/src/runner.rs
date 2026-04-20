//! Shared run-loop infrastructure for the verb-layer.
//!
//! Every rigorous verb (`measure`, `curve`, future `soak` / `watch`) walks
//! through the same pre-run / per-run / post-run choreography:
//!
//! 1. Build a `Plan`.
//! 2. Run a short self-check against loopback (refuse impossible rates).
//! 3. Collect the machine fingerprint (refuse coarse monotonic clocks).
//! 4. Open an archive dir and stamp plan/machine/env before the run.
//! 5. Step through one or more measurement windows, feeding a
//!    `Vec<PerRunMetrics>` per window.
//! 6. Merge everything into a `Summary`, render, and finalise the archive.
//!
//! Steps 2-4 and 6 are verb-agnostic. They live here so `measure.rs` and
//! `curve.rs` don't reinvent them. Each verb is left with the minimum
//! irreducible work: plan construction, picking the shape of the per-run
//! loop, and rendering.
//!
//! # Dependency discipline
//!
//! `zerobench-runtime` is strictly upstream of `zerobench-backends` and
//! `zerobench-report`. This module must not import either:
//!
//!   - The backend dispatcher is called by the verb itself, using
//!     [`RunHarness::ctx_for`] to materialise the `RunCtx` on demand.
//!   - Archive finalisation takes `pick_primary` as a closure so the
//!     verb can pass `zerobench_report::report::pick_primary_histogram`
//!     without runtime needing to see the report crate.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hdrhistogram::Histogram;
use rustls::ClientConfig;

use zerobench_core::plan::{Plan, Protocol, Step};
use zerobench_core::stats::{PerRunMetrics, SummaryExport};
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::Summary;

use crate::archive::{Archive, ArchiveWriter, EnvRecord, Index, SchemaVersions};
use crate::calibrate::{ClientSelfCheck, Verdict};
use crate::fingerprint::{plan_hash, run_id, target_fingerprint, url_fingerprint, IpFamilyTag};
use crate::live_snapshot::LiveSnapshot;
use crate::machine::MachineFingerprint;

// ---------------------------------------------------------------------------
// Calibration
// ---------------------------------------------------------------------------

/// P10 / PHILOSOPHY §9.6.2 — hard floor on the client's own scheduler
/// jitter. If the loopback self-check's p99 is above this many ns, the
/// client's noise floor dominates any real measurement; percentile
/// comparisons become meaningless. Refuse the run unless
/// `--force-overload` was passed.
pub const JITTER_P99_FLOOR_NS: u64 = 5_000;

/// Summary of a completed calibration probe.
///
/// The verb stays in charge of printing — this type just carries the
/// data a verb needs to decide whether to proceed. `poisoned` is `true`
/// when `--force-overload` was passed AND the underlying check actually
/// tripped a gate (insufficient rate ceiling or jitter floor). A run
/// with `poisoned == true` must flip `env.force_overload = true` when
/// writing the archive.
#[derive(Debug, Clone)]
pub struct CalibrationReport {
    /// What the self-check actually sustained (req/s).
    pub achieved_rate: f64,
    /// What the caller asked for (req/s).
    pub target_rate: f64,
    /// Pass/Marginal/Refuse from the self-check.
    pub verdict: Verdict,
    /// p99 of the scheduler-drift histogram in nanoseconds.
    pub jitter_p99_ns: u64,
    /// `true` when `--force-overload` masked a gate failure.
    pub poisoned: bool,
}

/// Run the client self-check and return a report, or a human-readable
/// error describing why the run should refuse.
///
/// This is the gate both `measure` and `curve` call before touching the
/// network. The behaviour mirrors what measure.rs had inline before the
/// extraction:
///
///   - Refuse if the achieved rate is insufficient (`Verdict::Refuse`)
///     and `--force-overload` wasn't passed.
///   - Refuse if the jitter histogram is empty (broken self-check).
///   - Refuse if jitter p99 > `JITTER_P99_FLOOR_NS` and
///     `--force-overload` wasn't passed.
///
/// When `force_overload` is set, a failing gate is logged via the
/// verb (caller pattern-matches on `report.poisoned`) and the run
/// proceeds — `report.poisoned` reflects that gate-masking happened.
///
/// The self-check spawns and then drops `ClientSelfCheck`, so each
/// call gets a fresh loopback echo server.
pub fn calibrate(
    target_rate: f64,
    duration: Duration,
    force_overload: bool,
) -> Result<CalibrationReport, String> {
    let check = ClientSelfCheck::spawn()
        .map_err(|e| format!("calibration: cannot spawn loopback echo: {e}"))?;
    let result = check
        .check(target_rate, duration, None)
        .map_err(|e| format!("calibration: self-check failed: {e}"))?;

    // Guard: HDR's `value_at_percentile` on an empty histogram returns
    // 0/1 silently, so we'd pass the jitter gate even with no samples.
    // Treat "no samples" as a broken self-check.
    if result.jitter.len() == 0 {
        return Err(
            "client self-check produced no jitter samples — calibration \
             is broken; cannot verify the scheduler noise floor. Pass \
             --no-calibrate to skip the gate."
                .into(),
        );
    }

    let jitter_p99 = result.jitter.value_at_percentile(99.0);

    // Rate-ceiling gate.
    let mut poisoned = false;
    if matches!(result.verdict, Verdict::Refuse) {
        if !force_overload {
            return Err(format!(
                "client cannot sustain {:.0} req/s on this machine (achieved {:.0}). \
                 Lower --rate, pass --no-calibrate to skip this gate, \
                 or pass --force-overload to run anyway.",
                target_rate, result.achieved_rate
            ));
        }
        poisoned = true;
    }

    // Jitter-floor gate.
    if jitter_p99 > JITTER_P99_FLOOR_NS {
        if !force_overload {
            return Err(format!(
                "client scheduler jitter p99 is {} ns (> {} ns floor). Real \
                 percentile deltas this run would be indistinguishable from \
                 client noise. Disable CPU frequency scaling / pin the \
                 process, pass --no-calibrate to skip, or --force-overload \
                 to run anyway.",
                jitter_p99, JITTER_P99_FLOOR_NS
            ));
        }
        poisoned = true;
    }

    Ok(CalibrationReport {
        achieved_rate: result.achieved_rate,
        target_rate,
        verdict: result.verdict,
        jitter_p99_ns: jitter_p99,
        poisoned,
    })
}

// ---------------------------------------------------------------------------
// RunHarness
// ---------------------------------------------------------------------------

/// Bundles every per-scenario runtime knob a verb passes through its
/// measurement loop. Replaces the inline `RunCtx` struct literal that
/// measure.rs and curve.rs kept re-building per iteration.
///
/// The harness holds `Target` / `TransportOpts` / TLS config / thread
/// and connection counts / open-loop rate. Per-window knobs (duration,
/// live snapshot, external stop flag) are supplied to [`Self::ctx_for`]
/// at the call site because they change per iteration (warmup vs
/// steady-state, rate-step vs rate-step).
///
/// Does **not** call the backend dispatcher. The verb writes
/// `zerobench_backends::run_plan(&plan, &harness.ctx_for(...))` —
/// `zerobench-backends` is downstream of `zerobench-runtime`, so the
/// harness can only produce a `RunCtx`, not invoke it.
#[derive(Clone)]
pub struct RunHarness {
    /// Target host/port/scheme/SNI.
    pub target: Target,
    /// Connect/read/write timeouts, `insecure_tls`, `max_conns`, etc.
    pub opts: TransportOpts,
    /// Pre-built rustls client config, or `None` for plain-TCP targets.
    pub tls_config: Option<Arc<ClientConfig>>,
    /// OS worker thread count for sharded backends.
    pub threads: usize,
    /// Closed-loop pool size / per-scenario connection budget.
    pub connections: usize,
    /// Open-loop target rate. `None` means closed-loop saturate.
    pub target_rate: Option<f64>,
}

impl RunHarness {
    /// Build a harness from the verb's already-parsed args.
    ///
    /// `threads` is the verb's `args.threads.max(1)` — the backends
    /// assume non-zero. Callers that let their CLI parser clamp to 1
    /// can forward `args.threads` directly.
    pub fn new_from(
        target: &Target,
        opts: &TransportOpts,
        threads: usize,
        connections: usize,
        target_rate: Option<f64>,
        tls_config: Option<Arc<ClientConfig>>,
    ) -> Self {
        Self {
            target: target.clone(),
            opts: opts.clone(),
            tls_config,
            threads: threads.max(1),
            connections,
            target_rate,
        }
    }

    /// Materialise a per-window [`RunCtx`]-shaped value as a tuple of
    /// fields the verb can hand to `zerobench_backends::RunCtx { ... }`.
    ///
    /// Returning a tuple instead of a typed struct keeps the runtime
    /// crate from importing `zerobench-backends` (upstream → downstream
    /// cycle would otherwise result). The verb pattern is:
    ///
    /// ```ignore
    /// let (target, opts, duration, threads, connections, rate, tls, live, stop) =
    ///     harness.ctx_for(duration, live, stop);
    /// let ctx = zerobench_backends::RunCtx {
    ///     target, opts, duration,
    ///     num_threads: threads, connections,
    ///     target_rps: rate, tls_config: tls, live, stop,
    /// };
    /// ```
    ///
    /// Two extra tokens per call site, no dep cycle.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn ctx_for(
        &self,
        duration: Duration,
        live: Option<Arc<LiveSnapshot>>,
        stop: Option<Arc<AtomicBool>>,
    ) -> (
        Target,
        TransportOpts,
        Duration,
        usize,
        usize,
        Option<f64>,
        Option<Arc<ClientConfig>>,
        Option<Arc<LiveSnapshot>>,
        Option<Arc<AtomicBool>>,
    ) {
        (
            self.target.clone(),
            self.opts.clone(),
            duration,
            self.threads,
            self.connections,
            self.target_rate,
            self.tls_config.clone(),
            live,
            stop,
        )
    }
}

// ---------------------------------------------------------------------------
// op_label_for
// ---------------------------------------------------------------------------

/// Pick the `[run N/M] X ops` status-line label for a plan's first
/// non-Pause step.
///
/// The scenario protocol tells you the general kind of op (Http /
/// Sse / Ws), but the specific variant overrides that with a finer
/// label — e.g. `SseFanout` measures "broadcasts" not "events",
/// `WsEchoRtt` measures "messages" not "frames".
pub fn op_label_for(plan: &Plan) -> &'static str {
    let first_step = plan.scenarios.first().and_then(|s| {
        s.steps.iter().find(|st| {
            !matches!(st, Step::Pause(_) | Step::PauseRandom { .. })
        })
    });
    match first_step {
        Some(Step::HttpColdConnect(_)) => "cold-connects",
        Some(Step::SseFanout(_)) => "broadcasts",
        Some(Step::SseReconnectStorm(_)) => "events",
        Some(Step::SseHold(_)) => "events",
        Some(Step::WsHold(_)) => "frames",
        Some(Step::WsFanout(_)) => "broadcasts",
        Some(Step::WsServerPushRtt(_)) => "messages",
        Some(Step::WsEchoRtt(_)) => "messages",
        _ => {
            let protocol = plan
                .scenarios
                .first()
                .map(|s| s.protocol())
                .unwrap_or(Protocol::Http);
            match protocol {
                Protocol::Http => "requests",
                Protocol::Sse => "events",
                Protocol::Ws => "messages",
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ArchiveSession
// ---------------------------------------------------------------------------

/// Pre-run + post-run archive lifecycle in one place.
///
/// `ArchiveSession::begin` computes fingerprints, opens the archive
/// directory (when not `no_archive`), and stamps `plan.json`,
/// `machine.json`, and a first `env.json` record. `ArchiveSession::finalise`
/// closes the run: it updates `env.json` with the end timestamp, writes
/// `result.json` + the histlog sidecar, and drops `INDEX.json` as the
/// completion marker.
///
/// Works in both modes:
///   - `no_archive = true` — still computes fingerprints (so downstream
///     callers can use `run_id()` / `url_fingerprint()` / etc.) but
///     does not touch disk. Finalise is a no-op.
///   - `no_archive = false` — full disk roundtrip.
pub struct ArchiveSession {
    writer: Option<ArchiveWriter>,
    env: EnvRecord,
    plan_hash: String,
    url_fingerprint: String,
    target_fingerprint: String,
    run_id: String,
    start_wall: SystemTime,
}

impl ArchiveSession {
    /// Resolve fingerprints, optionally open the archive directory, and
    /// stamp the pre-run sidecars (plan.json / machine.json / env.json).
    ///
    /// `resolved` is the DNS-resolved address set — passed to the
    /// target-fingerprint computation and recorded in `env.resolved_ips`.
    ///
    /// `context` is the verb's user-supplied `--context KEY=VAL` list
    /// (empty for verbs that don't accept it — `curve`, `probe`).
    ///
    /// `calibration_skipped` and `force_overload` flag the run as
    /// poisoned for comparison purposes; they land in `env.json`
    /// verbatim.
    ///
    /// `tool_features` is the feature-flag string the verb wants
    /// recorded (`"h1, h2, sse, ws, script"`, etc.).
    #[allow(clippy::too_many_arguments)]
    pub fn begin(
        plan: &Plan,
        target: &Target,
        resolved: &[SocketAddr],
        no_archive: bool,
        context: Vec<(String, String)>,
        calibration_skipped: bool,
        force_overload: bool,
        tool_features: String,
        tool_version: &str,
    ) -> io::Result<Self> {
        let start_wall = SystemTime::now();

        let plan_h = plan_hash(plan);
        let url_fp = url_fingerprint(plan, target, IpFamilyTag::Auto);
        let target_fp = target_fingerprint(plan, target, resolved, &plan_h);
        let id = run_id(&plan_h, &target_fp, start_wall);

        let machine = MachineFingerprint::collect();

        let writer = if no_archive {
            None
        } else {
            let archive = Archive::resolve();
            let w = ArchiveWriter::begin(&archive, &url_fp, &id)?;
            w.write_plan(plan)?;
            w.write_machine(&machine)?;
            eprintln!("[archive] run_id = {id}");
            eprintln!("[archive] dir = {}", w.dir().display());
            Some(w)
        };

        let mut env = EnvRecord::started_now(tool_version);
        env.tool_features = tool_features;
        env.resolved_ips = resolved.iter().map(|a| a.to_string()).collect();
        env.context = context;
        env.force_overload = force_overload;
        env.calibration_skipped = calibration_skipped;
        if let Some(w) = writer.as_ref() {
            w.write_env(&env)?;
        }

        Ok(Self {
            writer,
            env,
            plan_hash: plan_h,
            url_fingerprint: url_fp,
            target_fingerprint: target_fp,
            run_id: id,
            start_wall,
        })
    }

    /// The run ID string — useful for `[run N/M]` banners and for
    /// archive-directory introspection during the run.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// SHA-256 hex of the URL grouping inputs.
    pub fn url_fingerprint(&self) -> &str {
        &self.url_fingerprint
    }

    /// SHA-256 hex of the url+resolved-IPs+plan_hash bundle.
    pub fn target_fingerprint(&self) -> &str {
        &self.target_fingerprint
    }

    /// SHA-256 hex of the canonical plan JSON.
    pub fn plan_hash(&self) -> &str {
        &self.plan_hash
    }

    /// Wall-clock timestamp the archive session was opened at. Needed
    /// by the histlog writer for the interval-record start field.
    pub fn start_wall(&self) -> SystemTime {
        self.start_wall
    }

    /// Flip the `--force-overload` flag on the in-memory env record.
    /// Used when the calibration step decides the run is poisoned but
    /// the verb already set up the session before calibrating.
    pub fn set_force_overload(&mut self, force_overload: bool) {
        self.env.force_overload = force_overload;
    }

    /// Close the archive: update `env.json` with the end timestamp,
    /// write `result.json` + `<name>.histlog`, and stamp `INDEX.json`.
    ///
    /// `pick_primary` is the per-protocol primary-histogram picker.
    /// Runtime doesn't depend on `zerobench-report`, so the verb passes
    /// `zerobench_report::report::pick_primary_histogram` explicitly.
    ///
    /// `histlog_comment` lands as a `#`-prefixed leading line in the
    /// histlog file — conventionally `"zerobench X.Y.Z · plan=NAME ·
    /// target=URL · run_id=ID"`.
    ///
    /// `per_run` carries the elementary samples the diff tool's
    /// bootstrap CI resamples over; they're attached to the exported
    /// `SummaryExport::per_run` slot before write.
    pub fn finalise<F>(
        mut self,
        summary: &Summary,
        per_run: Vec<PerRunMetrics>,
        plan: &Plan,
        total_duration: Duration,
        histlog_comment: &str,
        pick_primary: F,
    ) -> io::Result<()>
    where
        F: for<'a> FnOnce(&'a Summary, &'a Plan) -> &'a Histogram<u64>,
    {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        self.env.set_ended();
        writer.write_env(&self.env)?;

        let mut export = summary.to_export();
        export.per_run = per_run;
        writer.write_result(&export)?;

        let primary_hist = pick_primary(summary, plan);
        writer.write_histlog(
            "result",
            primary_hist,
            self.start_wall,
            total_duration,
            Some(histlog_comment),
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
            plan_hash: self.plan_hash,
            target_fingerprint: self.target_fingerprint,
            url_fingerprint: self.url_fingerprint,
            replayed_from: None,
        };
        writer.finalise(&index)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;
    use std::time::Duration;
    use zerobench_core::plan::{
        CorrelateStrategy, FanoutMode, HeartbeatFrame, RateProfile, RequestPlan, TriggerSpec,
    };
    use zerobench_core::plan_builder::{
        scenario_http_cold_connect, scenario_http_request, scenario_sse_fanout, scenario_sse_hold,
        scenario_sse_reconnect_storm, scenario_ws_echo_rtt, scenario_ws_fanout, scenario_ws_hold,
        scenario_ws_server_push_rtt, PlanBuilder,
    };
    use zerobench_core::template::Template;
    use zerobench_core::transport::TransportOpts;
    use zerobench_core::var::VarRegistry;

    fn url_tpl(vars: &mut VarRegistry) -> Template {
        Template::compile("http://x/", vars).unwrap()
    }

    fn request_plan(vars: &mut VarRegistry) -> RequestPlan {
        RequestPlan {
            method: http::Method::GET,
            url: url_tpl(vars),
            headers: SmallVec::new(),
            body: None,
            extract: Vec::new(),
            checks: Vec::new(),
            expect_streaming: false,
        }
    }

    /// Build a plan with a single scenario from the given `scenario_*`
    /// builder return. Prepends a Pause step to the scenario so the
    /// `op_label_for` Pause-skipping path is always exercised.
    fn plan_with(
        mut scenario: zerobench_core::plan::Scenario,
    ) -> Plan {
        // Insert Pause at the front to assert `op_label_for` skips it.
        scenario.steps.insert(0, Step::Pause(Duration::from_millis(1)));
        let mut b = PlanBuilder::new();
        b.name("t").duration(Duration::from_secs(1));
        b.push_scenario(scenario);
        b.finalize()
    }

    #[test]
    fn op_label_http_request() {
        let mut vars = VarRegistry::new();
        let rp = request_plan(&mut vars);
        let p = plan_with(scenario_http_request("s", RateProfile::Constant(1.0), rp));
        assert_eq!(op_label_for(&p), "requests");
    }

    #[test]
    fn op_label_http_cold_connect() {
        let mut vars = VarRegistry::new();
        let rp = request_plan(&mut vars);
        let p = plan_with(scenario_http_cold_connect(
            "s",
            RateProfile::Saturate { max_concurrency: 1 },
            rp,
        ));
        assert_eq!(op_label_for(&p), "cold-connects");
    }

    #[test]
    fn op_label_sse_hold_is_events() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let p = plan_with(scenario_sse_hold(
            "s",
            1,
            Duration::from_secs(1),
            u,
            SmallVec::new(),
            true,
        ));
        assert_eq!(op_label_for(&p), "events");
    }

    #[test]
    fn op_label_sse_fanout_is_broadcasts() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let u2 = url_tpl(&mut vars);
        let p = plan_with(scenario_sse_fanout(
            "s",
            1,
            Duration::from_secs(1),
            u,
            SmallVec::new(),
            true,
            TriggerSpec::HttpPost {
                url: u2,
                body: None,
            },
            FanoutMode::TriggerRtt,
        ));
        assert_eq!(op_label_for(&p), "broadcasts");
    }

    #[test]
    fn op_label_sse_reconnect_storm_is_events() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let p = plan_with(scenario_sse_reconnect_storm(
            "s",
            1,
            Duration::from_secs(1),
            u,
            SmallVec::new(),
            0.1,
            true,
        ));
        assert_eq!(op_label_for(&p), "events");
    }

    #[test]
    fn op_label_ws_hold_is_frames() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let p = plan_with(scenario_ws_hold(
            "s",
            1,
            Duration::from_secs(1),
            u,
            SmallVec::new(),
            Duration::from_secs(25),
            HeartbeatFrame::Ping,
        ));
        assert_eq!(op_label_for(&p), "frames");
    }

    #[test]
    fn op_label_ws_fanout_is_broadcasts() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let u2 = url_tpl(&mut vars);
        let p = plan_with(scenario_ws_fanout(
            "s",
            1,
            Duration::from_secs(1),
            u,
            SmallVec::new(),
            Duration::from_secs(25),
            HeartbeatFrame::Ping,
            TriggerSpec::HttpPost {
                url: u2,
                body: None,
            },
            FanoutMode::TriggerRtt,
        ));
        assert_eq!(op_label_for(&p), "broadcasts");
    }

    #[test]
    fn op_label_ws_server_push_rtt_is_messages() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let p = plan_with(scenario_ws_server_push_rtt(
            "s",
            1,
            Duration::from_secs(1),
            u,
            SmallVec::new(),
            10.0,
        ));
        assert_eq!(op_label_for(&p), "messages");
    }

    #[test]
    fn op_label_ws_echo_rtt_is_messages() {
        let mut vars = VarRegistry::new();
        let u = url_tpl(&mut vars);
        let payload = url_tpl(&mut vars);
        let p = plan_with(scenario_ws_echo_rtt(
            "s",
            RateProfile::Saturate { max_concurrency: 1 },
            u,
            SmallVec::new(),
            1,
            10.0,
            payload,
            CorrelateStrategy::MonotonicIdPrepend,
        ));
        assert_eq!(op_label_for(&p), "messages");
    }

    #[test]
    fn op_label_falls_back_to_protocol_when_only_pauses() {
        // Scenario with only a pause step; no matching first_step.
        // HTTP protocol by default → "requests".
        let mut b = PlanBuilder::new();
        b.name("t").duration(Duration::from_secs(1));
        b.push_scenario(zerobench_core::plan::Scenario {
            name: "t".into(),
            rate: RateProfile::Saturate { max_concurrency: 1 },
            steps: vec![Step::Pause(Duration::from_millis(1))],
        });
        let p = b.finalize();
        assert_eq!(op_label_for(&p), "requests");
    }

    fn simple_http_plan() -> Plan {
        let mut vars = VarRegistry::new();
        let rp = request_plan(&mut vars);
        let mut b = PlanBuilder::new();
        b.name("t").duration(Duration::from_secs(1));
        b.push_scenario(scenario_http_request(
            "s",
            RateProfile::Saturate { max_concurrency: 1 },
            rp,
        ));
        b.finalize()
    }

    #[test]
    fn harness_ctx_for_packs_fields_correctly() {
        let target = zerobench_core::transport::Target::parse("http://127.0.0.1:1/").unwrap();
        let opts = TransportOpts::default();
        let harness = RunHarness::new_from(&target, &opts, 4, 16, Some(1234.5), None);
        let (t, o, dur, threads, conns, rate, tls, live, stop) =
            harness.ctx_for(Duration::from_secs(2), None, None);
        assert_eq!(t.host, target.host);
        assert_eq!(o.max_conns, opts.max_conns);
        assert_eq!(dur, Duration::from_secs(2));
        assert_eq!(threads, 4);
        assert_eq!(conns, 16);
        assert_eq!(rate, Some(1234.5));
        assert!(tls.is_none());
        assert!(live.is_none());
        assert!(stop.is_none());
    }

    #[test]
    fn harness_threads_clamped_to_one() {
        let target = zerobench_core::transport::Target::parse("http://127.0.0.1:1/").unwrap();
        let opts = TransportOpts::default();
        let h = RunHarness::new_from(&target, &opts, 0, 1, None, None);
        assert_eq!(h.threads, 1);
    }

    #[test]
    fn archive_session_begin_and_finalise_no_archive_mode() {
        // no_archive=true: session computes fingerprints without
        // writing anything to disk.
        let target = zerobench_core::transport::Target::parse("http://127.0.0.1:1/").unwrap();
        let plan = simple_http_plan();
        let session = ArchiveSession::begin(
            &plan,
            &target,
            &[],
            true,
            Vec::new(),
            false,
            false,
            "h1".into(),
            "0.0.0",
        )
        .expect("begin");

        assert!(!session.run_id().is_empty());
        assert_eq!(session.plan_hash().len(), 64); // SHA-256 hex
        assert_eq!(session.url_fingerprint().len(), 64);
        assert_eq!(session.target_fingerprint().len(), 64);

        // finalise is a no-op in no_archive mode; nothing to assert
        // beyond "doesn't crash".
        let summary = Summary::merge(Vec::new(), Duration::from_secs(1));
        session
            .finalise(
                &summary,
                Vec::new(),
                &plan,
                Duration::from_secs(1),
                "test",
                |s, _p| &s.latency,
            )
            .expect("finalise");
    }

    #[test]
    fn archive_session_writes_all_sidecars_when_enabled() {
        // Redirect ZEROBENCH_HOME to a temp dir so writes happen there.
        let dir = std::env::temp_dir().join(format!(
            "zb-runner-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let prior = std::env::var_os("ZEROBENCH_HOME");
        std::env::set_var("ZEROBENCH_HOME", &dir);

        let target = zerobench_core::transport::Target::parse("http://127.0.0.1:1/").unwrap();
        let plan = simple_http_plan();
        let session = ArchiveSession::begin(
            &plan,
            &target,
            &[],
            false,
            vec![("k".into(), "v".into())],
            false,
            false,
            "h1".into(),
            "0.0.0",
        )
        .expect("begin");

        let url_fp = session.url_fingerprint().to_string();
        let run_id = session.run_id().to_string();
        let run_dir = dir.join("runs").join(&url_fp).join(&run_id);

        // Pre-run: plan.json, machine.json, env.json all exist.
        assert!(run_dir.join("plan.json").exists());
        assert!(run_dir.join("machine.json").exists());
        assert!(run_dir.join("env.json").exists());
        // INDEX.json and result.json only appear after finalise.
        assert!(!run_dir.join("INDEX.json").exists());

        let summary = Summary::merge(Vec::new(), Duration::from_secs(1));
        session
            .finalise(
                &summary,
                Vec::new(),
                &plan,
                Duration::from_secs(1),
                "unit-test",
                |s, _p| &s.latency,
            )
            .expect("finalise");

        assert!(run_dir.join("result.json").exists());
        assert!(run_dir.join("result.histlog").exists());
        assert!(run_dir.join("INDEX.json").exists());

        // Restore env.
        match prior {
            Some(v) => std::env::set_var("ZEROBENCH_HOME", v),
            None => std::env::remove_var("ZEROBENCH_HOME"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn jitter_floor_constant_is_5_microseconds() {
        // Sentinel: changing this value changes the client-side
        // measurement contract. The number comes from
        // PHILOSOPHY §9.6.2.
        assert_eq!(JITTER_P99_FLOOR_NS, 5_000);
    }
}
