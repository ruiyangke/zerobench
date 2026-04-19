---
status: draft
version: v0.1.0-design
upstream: docs/PHILOSOPHY.md
date: 2026-04-19
---

# zerobench v0.1.0 — Design

> **Reading order**: `PHILOSOPHY.md` first (the *why*), then this doc
> (the *how*). This file translates each principle from the
> philosophy doc into concrete types, APIs, CLI surfaces, and module
> boundaries. Where the philosophy says "we measure X," this says
> "here is the struct and where it's recorded."
>
> Scope: source of truth for v0.1.0 implementation. Non-breaking
> changes land as follow-up patches; breaking changes require an
> ADR that either cites PHILOSOPHY.md or proposes to amend it.

## 0. Delta from v0.0.1

### 0.1. Kept from v0.0.1

- Two-phase Plan/execute architecture (compile → run; Rhai drops
  before Phase 2).
- mio/epoll sync I/O across all transports. No tokio, no compio.
- HDR histograms as the canonical latency record.
- Per-thread state, zero contended locks on hot path.
- Workspace crate layout: `zerobench-core`, `zerobench-http`,
  `zerobench-sse`, `zerobench-ws`, `zerobench-rhai`,
  `zerobench-tui`, `zerobench-cli`.

### 0.2. Broken from v0.0.1

- `Step::SseStream` (stream-completion SSE) → removed. Replaced
  with `SseHold`, `SseFanout`, `SseReconnectStorm`.
- `Step::WsRound` (handshake-per-op WS) → removed. Replaced with
  `WsHold`, `WsEchoRtt`, `WsServerPushRtt`, `WsFanout`.
- `ops_per_s` top-line across mixed-protocol runs → removed.
  Protocol-native headlines only.
- Default mode of bare `zerobench URL` → now `probe` (5s smoke
  test), was `saturate`.
- `result.json` schema → v1 → v2; shim via `--read-schema=1`.

### 0.3. New in v0.1.0

- Seven user-facing verbs (§2) with distinct code paths.
- Protocol-native Step variants (§3).
- Client-calibration self-check, always fresh, piggybacked on
  warmup (§5).
- Two-level archive fingerprinting (`url_fp` / `target_fp`) (§7).
- Statistical comparison engine: run-bootstrap CI + AD
  distribution test (§8).
- TUI with mode-aware layouts, baseline-delta overlay (§10).
- SLO-gate assertions: `expect_p99`, `expect_error_rate`,
  `expect_steady_state`, `expect_rate`, `expect_keepup` (§9).
- Performance CI gates: allocation counter, scheduler-jitter
  micro, vs-wrk regression guard (§12).

---

## 1. Type-level summary

All zerobench-core types are `Send + Sync + Clone` once built.
Workers receive clones; no mutation crosses threads.

```rust
// zerobench-core/src/plan.rs
pub struct Plan {
    pub scenarios: Vec<Scenario>,
    pub vars:      VarRegistry,
    pub duration:  Duration,
    pub warmup:    Duration,        // was Option in v0.0.1; now always-present
    pub cooldown:  Duration,        // NEW — inter-run gap
    pub runs:      u32,             // NEW — measure default 3
    pub threads:   usize,
    pub mode:      Mode,            // NEW — verb dispatch
    pub name:      String,          // NEW — required when archiving
}

pub enum Mode {
    Probe,
    Calibrate,
    Measure,
    Curve { from_rate: f64, to_rate: f64, knee_criterion: KneeCriterion },
    Compare { other: Arc<Plan>, schedule: CompareSchedule },
    Soak,                           // long-duration measure
    Diff,                           // archive-only, no execution
    // Note: `Watch` variant dropped from v0.1.0 (2026-04-19).
    // See PHILOSOPHY.md §5 — continuous monitoring is
    // Prometheus/Grafana territory, not a verb in the measurement
    // apparatus.
}

pub struct Scenario {
    pub name:       String,
    pub rate:       RateProfile,
    pub steps:      Vec<Step>,
    pub protocol:   Protocol,       // inferred from first non-Pause step
    pub assertions: Vec<Assertion>, // §9 — SLO gates
}

pub enum Step {
    // HTTP
    Request(RequestPlan),
    HttpColdConnect(ColdConnectPlan),
    // SSE
    SseHold(SseHoldPlan),
    SseFanout(SseFanoutPlan),
    SseReconnectStorm(SseReconnectPlan),
    // WS
    WsHold(WsHoldPlan),
    WsEchoRtt(WsEchoRttPlan),
    WsServerPushRtt(WsServerPushPlan),
    WsFanout(WsFanoutPlan),
    // Control
    Pause(Duration),
    PauseRandom { min: Duration, max: Duration },
}
```

Every `*Plan` struct is frozen-once-built and Arc-cloneable.
Templates (`{{...}}`) live inside URLs, headers, and bodies —
unchanged from v0.0.1 semantics.

---

## 2. Mode dispatcher

```rust
// zerobench-cli/src/main.rs
fn main() -> ExitCode {
    let args = CliArgs::parse();
    let plan = plan_from_args::build(&args)?;
    match plan.mode {
        Mode::Probe     => probe::run(&plan),
        Mode::Calibrate => calibrate::run(&plan),
        Mode::Measure | Mode::Soak => measure::run(&plan),
        Mode::Curve {..} => curve::run(&plan),
        Mode::Compare {..} => compare::run(&plan),
        // Mode::Watch removed 2026-04-19; see PHILOSOPHY.md §5.
        Mode::Diff      => diff::run(&args),
    }
}
```

Each verb has its own module under `zerobench-cli/src/verbs/`.
Shared machinery (connection pools, histograms, LiveSnapshot)
lives in `zerobench-core`.

### 2.1. `probe`

- Duration fixed at 5s (overridable with `-d`).
- Runs fixed at 1.
- `--no-archive` is the default (smoke test ≠ baseline).
- No calibration self-check (P5 gate does not run for probes).
- Output: terminal summary + exit 0 on success.

### 2.2. `calibrate`

- Loopback-echo target (in-process thread, pinned to the last
  core per P5).
- Ramps offered rate until sustained rate falls below 99% of
  offered for ≥3 consecutive seconds, or hits `--max-rate`.
- Reports: `client_ceiling_rps`, `client_ceiling_concurrency`,
  scheduler-jitter distribution.
- Writes `$ZEROBENCH_HOME/runs/<machine_fp>/calibrate-<ts>/` for
  reference but is **never cached** (§5).

### 2.3. `measure` / `soak`

- `measure`: 60s + 15s warmup + 10s cooldown × (runs-1), runs=3.
- `soak`: 5min + 30s warmup × 1 run. Same code path; different
  defaults.
- Each run fires a self-check (§5) in the first 2–3s of warmup.
  Failure → refuse to start.
- Runs are serialised per scenario (P9); `--parallel` opts in
  to shared-pool interleave.
- After all runs: statistical comparison (§8) vs baseline if
  one exists for `(url_fp, plan_hash)`.

### 2.4. `curve`

- Input: `--from-rate` / `--to-rate` / `--ramp-duration`.
- Emits rate tokens at linearly-increasing rate across the ramp.
- Per-second HDR slices kept; at ramp end, reconstruct (rate,
  p99_latency) pairs and find the knee.
- Knee criterion: first rate where p99 > 2 × p99 at lowest rate,
  OR first rate where error rate ≥ 1% for ≥3s consecutive.
- Output: knee rate, curve CSV, knee reasoning.

### 2.5. `compare`

- Two positional URLs, or `--script file.rhai --side-a ... --side-b ...`.
- Interleaved schedule (default): `A₁ → cooldown → B₁ → cooldown
  → A₂ → cooldown → B₂ → ...`.
- `--compare-schedule=serial` for sequential A-then-B.
- After completion: statistical diff per-metric (§8) between
  A and B histograms at the same run index, then aggregated.

### 2.6. `watch` — **dropped (2026-04-19)**

The original design had a long-running verb for continuous
measurement with stop conditions. Dropped from v0.1.0 —
continuous monitoring is Prometheus/Grafana territory
(`zerobench-prom-adapter`); "rerun on schedule" is cron +
`measure` + `compare`. See PHILOSOPHY §5 for the rationale.
No `verbs/watch.rs`; `Mode::Watch` removed from the Plan enum;
`WatchUntil` / `ComparisonOp` types deleted.

### 2.7. `diff`

- No execution; reads two archived `result.json` / `result.histlog`
  pairs and runs the §8 statistical engine.
- `diff <run_id_a> <run_id_b>` or `diff a.json b.json`.
- Exit 0 if within CI; 1 if any `--regress-on` threshold crossed.

---

## 3. Protocol backend APIs

All backends expose the same trait:

```rust
// zerobench-core/src/backend.rs
pub trait Backend: Send + 'static {
    type Plan;
    type Stats: MergeableStats;

    fn run(
        plan:  &Self::Plan,
        target: &Target,
        opts:  &TransportOpts,
        scheduler: &dyn Scheduler,
        snapshot: Arc<LiveSnapshot>,
        stop: StopSignal,
    ) -> Vec<Self::Stats>;
}
```

### 3.1. HTTP backend (`zerobench-http`)

```rust
pub struct RequestPlan {
    pub method:  http::Method,
    pub url:     Template,
    pub headers: SmallVec<[(Template, Template); 8]>,
    pub body:    Option<BodySource>,
    pub extract: Vec<Extract>,
    pub checks:  Vec<Assertion>,
    pub http_version: HttpVersion,       // Auto | H1 | H2 | H3 | H2C
    pub conn_concurrency: u32,           // -c
    pub stream_concurrency: Option<u32>, // -m, None for H1
}

pub struct ColdConnectPlan {
    pub request: RequestPlan,
    pub rate: RateProfile,   // handshakes/s
    // Each request opens a new TCP/TLS/H2 conn, sends one request,
    // closes. Pool is bypassed.
}
```

- H1: one `hyper::client::conn::http1::SendRequest` per pool slot.
- H2: one `hyper::client::conn::http2::SendRequest` per conn;
  streams multiplexed up to `stream_concurrency` bounded by
  `SETTINGS_MAX_CONCURRENT_STREAMS`.
- H3: `h3` over `mio`-driven `quinn-proto` (no quinn runtime;
  we drive the state machine manually over mio events).

### 3.2. SSE backend (`zerobench-sse`)

```rust
pub struct SseHoldPlan {
    pub url: Template,
    pub headers: SmallVec<[(Template, Template); 4]>,
    pub subscribers: u32,      // n
    pub hold_for: Duration,    // for="60s"
    pub reconnect: bool,       // follow RFC reconnect? default true
}

pub struct SseFanoutPlan {
    pub subscriber_plan: SseHoldPlan,
    pub trigger: TriggerSpec,       // POST endpoint + payload
    pub mode: FanoutMode,           // Timestamp | TriggerRtt
    pub clock_probe: Option<ClockProbeSpec>,  // §5.3 in philosophy
}

pub struct SseReconnectPlan {
    pub subscriber_plan: SseHoldPlan,
    pub kill_rate: Rate,       // 10%/s
    pub verify_last_event_id: bool,
}
```

Event parsing per WHATWG EventSource spec (`event:`, `id:`, `retry:`,
`data:` fields, blank-line dispatch). Each event is one histogram
observation of `inter_event_gap_ns` plus `ttfb_ns` for the first.

### 3.3. WS backend (`zerobench-ws`)

```rust
pub struct WsHoldPlan {
    pub url: Template,
    pub connections: u32,
    pub heartbeat: Duration,
    pub heartbeat_frame: HeartbeatFrame,   // Ping | TextApp
    pub hold_for: Duration,
}

pub struct WsEchoRttPlan {
    pub url: Template,
    pub connections: u32,
    pub msg_rate_per_conn: f64,
    pub correlate: CorrelateStrategy,
    // ping_pong (default) | monotonic_id_prepend |
    // payload_substring | first_text_frame
    pub payload: Template,      // ignored for ping_pong
}

pub struct WsServerPushPlan {
    pub url: Template,
    pub connections: u32,
    pub expected_rate_per_conn: f64,   // for detection of stalls
    pub hold_for: Duration,
}

pub struct WsFanoutPlan { /* analogous to SseFanoutPlan */ }
```

RFC 6455 frame codec, per-connection state machine driven by
mio events. Ping/pong correlation uses the 16-byte
`application_data` field (opcode 0x9 → 0xA payload echo).

---

## 4. Rate scheduler

```rust
// zerobench-core/src/scheduler.rs
pub enum RateProfile {
    Constant(f64),
    Ramp { from: f64, to: f64, over: Duration },
    Stepped(Vec<(Duration, f64)>),
    Saturate { max_concurrency: u32 },
}

pub struct Token {
    pub scenario_id:    u16,
    pub intended_start: Instant,
    pub seq:            u64,
}

pub trait Scheduler: Send + Sync {
    fn next(&self) -> Option<Token>;          // blocking until time
    fn keepup_state(&self) -> KeepupState;    // warn/fail/ok
}
```

Open-loop implementation:
- Single thread emits tokens into an MPMC channel at the
  intended rate.
- Workers pull tokens; latency = `now - intended_start`.
- Keep-up thresholds: 100ms drift for 1s → warn;
  1s drift for 3s → fail (configurable, Linux-CFS-calibrated
  per PHILOSOPHY §P6).

Saturate implementation: no tokens. N workers loop
request-then-response. `keepup_state` always returns `NotApplicable`.

---

## 5. Calibration

```rust
// zerobench-core/src/calibrate.rs
pub struct ClientSelfCheck {
    loopback_addr: SocketAddr,   // in-process echo
    echo_thread:   JoinHandle<()>,
    echo_core:     CoreId,        // pinned to last physical core
}

impl ClientSelfCheck {
    pub fn spawn() -> Self;
    pub fn check(&self, rate: f64, duration: Duration) -> SelfCheckResult;
    pub fn shutdown(self);
}

pub struct SelfCheckResult {
    pub offered_rate:    f64,
    pub achieved_rate:   f64,
    pub sustained_pct:   f64,     // achieved / offered
    pub jitter_p99_ns:   u64,
    pub verdict:         Verdict, // Pass | Refuse(reason)
}
```

Always fresh. No cache. Piggybacks on warmup's first 2–3s
(§PHILOSOPHY 5.1). `--no-calibrate` skips; the skip is stamped
`calibration: skipped` in the archive, poisoning comparisons.

---

## 6. Stats & histograms

### 6.1. TaskStats (per-thread, per-scenario)

```rust
// zerobench-core/src/stats.rs
pub struct TaskStats {
    pub latency_ns:   Histogram<u64>,     // main
    pub ttfb_ns:      Histogram<u64>,     // HTTP only
    pub requests:     u64,
    pub bytes_in:     u64,
    pub bytes_out:    u64,
    pub errors:       ErrorCounters,
    pub per_scenario: Vec<ScenarioStats>,
}

pub struct ScenarioStats {
    pub scenario_id:   u16,
    pub protocol:      Protocol,
    pub latency:       Histogram<u64>,
    pub sse:           Option<SseExtras>,
    pub ws:            Option<WsExtras>,
    pub http:          Option<HttpExtras>,
    pub conn_metrics:  ConnMetrics,        // §9.5.5 in philosophy
    pub errors:        ErrorCounters,
    pub partial:       bool,               // true if Rhai panic
    pub force_overload: bool,              // poisons comparisons
    pub calibration_skipped: bool,         // poisons comparisons
}

pub struct SseExtras {
    pub ttfb_ns:           Histogram<u64>,
    pub event_gap_ns:      Histogram<u64>,
    pub events:            u64,
    pub streams_held_peak: u64,
    pub bytes_received:    u64,
    pub mode:              SseMode,
}

pub struct WsExtras {
    pub handshake_ns:      Histogram<u64>,
    pub rtt_ns:            Histogram<u64>,
    pub messages_sent:     u64,
    pub messages_recv:     u64,
    pub bytes_sent:        u64,
    pub bytes_recv:        u64,
    pub conns_held_peak:   u64,
    pub mode:              WsMode,
    pub correlate:         Option<CorrelateStrategy>,
}

pub struct HttpExtras {
    pub negotiated_version: http::Version,
    pub h2_server_max_streams: Option<u32>,
    pub h2_capped:          bool,
}
```

### 6.2. Rolling HDR for LiveSnapshot

Per-scenario ring of `W` per-second HDR sub-histograms (W =
`--live-window`, default 5s). Advanced at each second boundary.
Each sub-histogram is the same HDR config as the final merged
histogram (ns, [1, 60_000_000_000], 3 sig figs → ~100KB per
sub-histogram).

```rust
pub struct RollingHdr {
    window:    VecDeque<Histogram<u64>>,  // W entries
    final_:    Histogram<u64>,
    window_s:  u32,
    cursor_s:  AtomicU64,
}

impl RollingHdr {
    pub fn record(&self, t: Instant, ns: u64);
    pub fn advance(&self);          // called at second boundaries
    pub fn snapshot(&self) -> HistogramSnapshot;  // for TUI/JSONL
    pub fn merged(&self) -> Histogram<u64>;       // final report
}
```

### 6.3. LiveSnapshot (TUI + JSONL consumer)

```rust
pub struct LiveSnapshot {
    per_scenario: DashMap<u16, Arc<ScenarioLive>>,
    phase:        AtomicU8,   // warmup | measure | cooldown
    elapsed_ns:   AtomicU64,
}

pub struct ScenarioLive {
    rolling:      RollingHdr,
    rate_counter: AtomicU64,
    errors:       ErrorCountersAtomic,
    conn_metrics: ConnMetricsAtomic,
    keepup_state: AtomicU8,
}
```

Single aggregator. Two consumers: TUI (read snapshots at 10 Hz)
and JSONL emitter (read at `--progress-every` cadence).

---

## 7. Archive layout

```
$ZEROBENCH_HOME/
  runs/
    <url_fingerprint>/
      <run_id>/
        plan.json
        result.json           # schema v2
        result.histlog        # HDR V2 compressed log
        warmup.histlog
        machine.json
        env.json
        INDEX.json
        stdout.txt, stderr.txt
  baselines/
    <url_fingerprint>/        # symlinks to pinned run_ids
  cache/                       # REMOVED — no calibration cache (§Q3)
```

### 7.1. Fingerprint computation

```rust
// zerobench-core/src/fingerprint.rs
pub fn url_fingerprint(plan: &Plan, target: &Target) -> String;
  // sha256(JCS({scheme, host, port, sni, plan.name, ip_family}))

pub fn target_fingerprint(plan: &Plan, resolved: &[SocketAddr]) -> String;
  // sha256(JCS({scheme, host, resolved_ips_sorted, port, sni, plan_hash}))

pub fn plan_hash(plan: &Plan) -> String;
  // sha256(JCS(plan))  — tool_version NOT included

pub fn run_id(plan_hash: &str, target_fp: &str, when: DateTime<Utc>) -> String;
  // format!("{}-{}-{}", when.to_rfc3339(), &plan_hash[..8], &target_fp[..8])
```

JCS = RFC 8785 JSON Canonicalization Scheme.

### 7.2. Machine fingerprint

Collected at run start; persisted as `machine.json`. Fields
enumerated in PHILOSOPHY §8.3. Platform-branched collectors:

```rust
// zerobench-core/src/machine.rs
pub struct MachineFingerprint { /* all fields from PHILOSOPHY §8.3 */ }

#[cfg(target_os = "linux")]
fn collect_linux() -> MachineFingerprint;  // /proc, /sys, sysctl

#[cfg(target_os = "macos")]
fn collect_macos() -> MachineFingerprint;  // sysctl, SPNetworkDataType
```

---

## 8. Statistical comparison engine

```rust
// zerobench-core/src/compare.rs
pub enum CompareStrategy {
    RunBootstrap { resamples: u32 },   // default N≥3
    AdDistribution,                    // default N=1
    KsDistribution,                    // opt-in
    MinOfN,                            // opt-in, Netflix pattern
}

pub struct ComparisonResult {
    pub metric:     MetricName,        // p50/p90/p99/p99.9/p99.99/rate/error_rate
    pub delta:      f64,
    pub delta_pct:  f64,
    pub ci:         Option<(f64, f64)>,   // bootstrap only
    pub test_stat:  Option<TestStat>,     // AD/KS
    pub significance: Significance,
}

pub fn compare(
    a: &Summary,
    b: &Summary,
    strategy: CompareStrategy,
    correction: MultipleComparisonCorrection,  // HolmBonferroni default
) -> Vec<ComparisonResult>;
```

Bootstrap implementation: 10_000 resamples at the run level
(not observation level). For N ≥ 3 runs per side, sample with
replacement from the N run-level p99 values. Report the 2.5th
and 97.5th percentiles of the resampled delta distribution.

AD implementation: use the Scholz-Stephens k-sample statistic
over ECDFs derived from HDR bucket counts. Asymptotic p-values
from tabulated critical values.

`--regress-on p99:+5%` threshold logic:
- RunBootstrap: crossed if CI-lower > threshold.
- AD/KS: crossed if delta > threshold AND p < 0.05.
- MinOfN: crossed if conservative bound > threshold.

---

## 9. SLO assertions

```rust
// zerobench-core/src/assertion.rs
pub enum Assertion {
    // pre-v0.1.0
    StatusEq(u16),
    StatusIn(SmallVec<[u16; 4]>),
    LatencyUnder(Duration),
    // NEW — SLO-gate class
    ExpectP99  { under: Duration },
    ExpectPN   { n: f64, under: Duration },
    ExpectErrorRate { below: f64, category: Option<ErrorCategory> },
    ExpectSteadyState {
        metric: MetricName,
        window: Duration,
        cv_below: f64,
        after:  Duration,
    },
    ExpectRate { at_least: f64, window: Option<Duration> },
    ExpectKeepup { max_level: KeepupLevel },
}

impl Assertion {
    pub fn evaluate(&self, summary: &ScenarioSummary) -> AssertionResult;
}
```

Rhai builders (see `zerobench-rhai::builders::assertions`):

```rhai
scenario("ping", |s| {
    s.step(GET("/ping"));
    s.expect_p99(under: "5ms");
    s.expect_error_rate(below: "0.01%");
    s.expect_steady_state(metric: "throughput_rate",
                          window: "10s", cv_below: 0.10, after: "15s");
});
```

Any assertion failing → exit 1, result archive records failures.

---

## 10. TUI architecture

```rust
// zerobench-tui/src/lib.rs
pub struct TuiApp {
    snapshot:   Arc<LiveSnapshot>,
    baseline:   Option<Arc<Summary>>,   // loaded from archive if exists
    mode:       Mode,                    // drives layout selection
    layout:     Box<dyn Layout>,
}

pub trait Layout {
    fn render(&mut self, frame: &mut Frame, state: &LiveSnapshot, baseline: Option<&Summary>);
    fn handle_key(&mut self, key: KeyEvent) -> TuiAction;
}

pub struct MeasureLayout { /* throughput + latency + errors + baseline-delta */ }
pub struct CurveLayout   { /* 2D rate-vs-p99 scatter (ratatui::Chart) */ }
pub struct CompareLayout { /* split-screen A | B, live side-by-side */ }
// WatchLayout removed 2026-04-19 with the Watch verb.
```

Mode-aware dispatch in `TuiApp::new`:

```rust
let layout: Box<dyn Layout> = match plan.mode {
    Mode::Measure | Mode::Soak => Box::new(MeasureLayout::new(baseline)),
    Mode::Curve {..}           => Box::new(CurveLayout::new()),
    Mode::Compare {..}         => Box::new(CompareLayout::new()),
    // Mode::Watch removed 2026-04-19.
    Mode::Probe | Mode::Calibrate => Box::new(MeasureLayout::new(None)),
    Mode::Diff                 => unreachable!(),  // no TUI for diff
};
```

LiveSnapshot → TUI: poll at 10 Hz (100 ms). ratatui diffs frames;
terminal doesn't flicker. Baseline delta overlay: `p99 810µs  +3.2%`
rendered alongside every headline metric when baseline is loaded.

---

## 11. CLI surface

### 11.1. Verb routing

```
zerobench [URL]                 → probe (bare URL = probe)
zerobench probe URL             → probe
zerobench calibrate             → calibrate (no URL; loopback)
zerobench measure URL           → measure
zerobench curve URL             → curve
zerobench compare URL1 URL2     → compare
                                     # zerobench watch removed 2026-04-19
zerobench soak URL              → soak (long-duration measure)
zerobench diff A B              → diff
zerobench run script.rhai       → (existing) Rhai entry point
zerobench replay <run_id>       → replay
zerobench lint script.rhai      → lint
zerobench bench                 → self-benchmark (§12)
zerobench archive ...           → pin / unpin / unlock / prune
```

### 11.2. Global flag groups (clap `help_heading`)

- **Load**: `-c/--conn-concurrency`, `-m/--stream-concurrency`,
  `-t/--threads`, `-d/--duration`, `-r/--rate`,
  `--warmup`, `--cooldown`, `--runs`.
- **Request**: `-X/--method`, `-H/--header`, `--body`, `--body-file`,
  `--json`, `--form`, `--basic-auth`, `--bearer`, `--from-curl`.
- **Protocol**: `--http-version`, `--insecure`,
  `--respect-retry-after`, `--treat-429-as-error`,
  `--h2-goaway-drain-ms`.
- **Network**: `--connect-timeout`, `--timeout`, `--resolve`,
  `--ip-family`.
- **Assertions**: `--expect-status`, `--expect-status-in`,
  `--expect-latency-under`, `--regress-on`, `--regress-on-all`.
- **Output**: `--format` (terminal/json/jsonl/prom),
  `--color`, `--tui`, `-o/--output`, `--dry-run`, `--explain`.
- **Archive**: `--no-archive`, `--archive-retain`, `--name`,
  `--context`.
- **Measurement control**: `--no-calibrate`, `--force-overload`,
  `--allow-coarse-clock`, `--jitter-ns`, `--seed`.
- **Statistical**: `--compare-strategy`, `--bootstrap-method`,
  `--compare-schedule`, `--compare-cooldown`, `--joint-ci`,
  `--compare-view`.

Full flag-to-module wiring lives in `zerobench-cli/src/cli_args.rs`.
The current v0.0.1 groups are extended, not replaced.

---

## 12. Performance CI

### 12.1. Floors (gate on every PR)

Implemented in `zerobench-bench/` (new crate):

```rust
// zerobench-bench/src/lib.rs
pub fn bench_tool_overhead() -> FloorReport;
pub fn bench_scheduler_jitter() -> FloorReport;
pub fn bench_template_expansion() -> FloorReport;
pub fn bench_pool_acquisition() -> FloorReport;
pub fn bench_hot_path_no_alloc() -> FloorReport;
pub fn bench_vs_wrk() -> FloorReport;
```

Each returns a `FloorReport` compared against thresholds from
PHILOSOPHY §9.6.1:

| Bench | Threshold | Target file |
|-------|-----------|-------------|
| tool_overhead_p99_ns | < 2000 | `zerobench-bench/src/overhead.rs` |
| scheduler_jitter_p99_ns | < 5000 | `.../jitter.rs` |
| template_ns_typical_5var | < 200 | `.../template.rs` |
| pool_acquisition_p99_ns | < 100 | `.../pool.rs` |
| hot_path_allocs | == 0 | `.../alloc.rs` |
| vs_wrk_throughput_ratio | ≥ 1.0 (mandatory), ≥ 1.2 (expected) | `.../vs_wrk.sh` |

CI job `perf-floors`: runs all six, fails if any cross threshold.
Regression >5% on any non-wrk floor, or <1.0× on vs-wrk → PR block.

### 12.2. Allocation counter

```rust
// zerobench-bench/src/alloc.rs
#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

#[test]
fn hot_path_no_alloc() {
    ALLOC.reset();
    let plan = canonical_plan();
    let mut worker = MeasureWorker::new(&plan);
    for _ in 0..100_000 {
        worker.tick(&test_token());
    }
    assert_eq!(ALLOC.total(), 0, "hot path allocated {} bytes", ALLOC.total());
}
```

### 12.3. Loom tests

```rust
// zerobench-bench/tests/loom.rs
#[test]
fn live_snapshot_consistency() {
    loom::model(|| {
        let snap = Arc::new(LiveSnapshot::new(1));
        let w = snap.clone();
        let r = snap.clone();
        thread::spawn(move || { w.record(0, 100); });
        thread::spawn(move || { let _ = r.snapshot(); });
    });
}
```

Replaces `perf lock contention` as the primary concurrency check
(PHILOSOPHY AP-10 resolution).

---

## 13. Module boundaries

```
zerobench-core/                # Types, scheduler, stats, compare, fingerprint
  plan.rs, scenario.rs, template.rs, var.rs
  scheduler.rs, stop.rs
  stats.rs, histogram.rs, rolling_hdr.rs, live_snapshot.rs
  report.rs, compare.rs, assertion.rs
  fingerprint.rs, machine.rs, tls.rs
  calibrate.rs
  archive.rs                   # NEW — $ZEROBENCH_HOME layout, fingerprint-to-path
  backend.rs                   # NEW — Backend trait

zerobench-http/                # HTTP/1/2/3 backends
  mio_h1.rs, mio_h2.rs, mio_h3.rs (NEW)
  cold_connect.rs (NEW)
  mio_tls.rs, raw_h1_common.rs

zerobench-sse/                 # SSE backend
  hold.rs (replaces lib.rs single-mode)
  fanout.rs (NEW)
  reconnect_storm.rs (NEW)
  line_parser.rs, clock_probe.rs (NEW)

zerobench-ws/                  # WS backend
  hold.rs (NEW)
  echo_rtt.rs (NEW — correlation strategies)
  server_push_rtt.rs (NEW)
  fanout.rs (NEW)
  frame.rs, handshake.rs, conn.rs

zerobench-rhai/                # DSL
  builders.rs, parse.rs, error.rs
  assertions.rs (NEW — SLO expectation builders)
  lint.rs (NEW)

zerobench-tui/                 # Live view
  state.rs
  layouts/ (NEW — one module per Mode)
    measure.rs, curve.rs, compare.rs
  widgets/ (existing — latency, throughput, errors)

zerobench-cli/                 # Binary entry
  main.rs, cli_args.rs
  plan_from_cli.rs, plan_from_rhai.rs (NEW), plan_from_http_file.rs
  verbs/ (NEW)
    probe.rs, calibrate.rs, measure.rs, curve.rs
    compare.rs, diff.rs, replay.rs

zerobench-bench/               # NEW — self-benchmark crate
  src/
    overhead.rs, jitter.rs, template.rs, pool.rs, alloc.rs
    vs_wrk.sh
  tests/
    loom.rs
```

New crates: `zerobench-bench`. New top-level modules across
existing crates noted with `(NEW)`.

---

## 14. Feature flag matrix

```toml
[features]
default = ["h1", "h2", "sse", "ws", "script", "tui"]
h1      = []
h2      = ["dep:hpack"]
h3      = ["dep:quinn-proto", "dep:rustls/quic"]
sse     = []
ws      = []
script  = ["dep:rhai"]
tui     = ["dep:ratatui", "dep:crossterm"]
full    = ["h1", "h2", "h3", "sse", "ws", "script", "tui", "bench"]
bench   = []   # gates zerobench-bench inclusion
```

`cargo install zerobench` → default features → ~8 MB binary
(PHILOSOPHY §9.6.5 budget). `cargo install zerobench --features full`
→ ~14 MB.

---

## 15. Schema registry

Every JSON artifact carries a `schema_version`. JSON schema files
live in `zerobench-core/schemas/`:

```
schemas/
  result-v2.json
  plan-v1.json
  machine-v1.json
  env-v1.json
  index-v1.json
  jsonl-stream-v2.json
```

`cargo test -p zerobench-core` validates every generated artifact
against its schema (via `jsonschema-rs`). A CI step rejects PRs
that remove or retype schema fields without a major-version bump.

---

## 16. Migration helpers

```rust
// zerobench-core/src/migrate.rs
pub fn read_v1_result(json: &str) -> Result<Summary, MigrationError>;
pub fn rewrite_v0_rhai_hint(script: &str) -> Vec<RewriteHint>;
```

`zerobench lint` surfaces rewrite hints for removed v0.0.1 forms
(`SSE(...).expect_chunks()` → `sse_hold(...)`, etc.). No automatic
rewriter — hints are copy-paste guidance; mechanical rewrites miss
context.

---

## 17. Open technical questions

These are design-level (not philosophy-level) decisions that need
implementation-phase resolution:

- **Q1**: H3 backend — drive `quinn-proto` directly over mio, or
  accept a thin async bridge (tokio-free via `futures-io`)?
  Impact on P10 overhead budget.
- **Q2**: Allocation counter scope — does `#[test] fn
  hot_path_no_alloc` run on every thread or aggregate? Per-thread
  is stricter but costlier to wire.
- **Q3**: Loom coverage — which state machines need loom tests
  vs which are provably lockless-by-construction (Arc<Atomic*>)?
- **Q4**: Rhai function-table cost — per-worker-thread cached
  Rhai engine for `on_response` hooks: how big is the footprint,
  and does it survive the "zero hot-path alloc" rule?
- **Q5**: Schema bump cadence — when a new metric lands (e.g.
  H3 1-RTT vs 0-RTT), is that v2.x additive or v3 breaking?
- **Q6**: Archive rotation on NFS — test coverage; known fragile.
  Acceptance criteria for "NFS-safe enough to ship"?

---

## 18. Change log

- **2026-04-19**: initial draft. Tracks PHILOSOPHY.md v1 with
  seven Qs resolved (five RESOLVED, two tagged "calibrate from
  telemetry"). Ready for critic/reviser loop.
