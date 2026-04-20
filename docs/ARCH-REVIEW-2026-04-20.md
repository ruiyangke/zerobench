# zerobench Architecture Review — 2026-04-20

**Scope:** end-to-end architectural critique of the `main` branch at
commit `e57d09d` (35K LoC, 8 crates, 624 tests), and a concrete
improvement plan given the stated budget constraints (*time and money
to rewrite where warranted*).

**Author:** architectural audit pass, post-merge of `v0.1.0-impl`.

---

## 0. Executive summary

**Verdict:** structurally sloppy, functionally fine. Score **5.5/10**.

The project ships, passes 624 tests, and has seven protocol-native
backends (HTTP/1, HTTP/2, HTTP cold-connect, SSE hold / fanout /
reconnect-storm, WS echo-RTT / hold / server-push / fanout). But it's a
10K-LoC tool wearing a 35K-LoC cloak — every protocol-specific
addition has required modifications in 4+ crates plus a hand-coded
dispatch switch somewhere in the CLI. The seams were drawn once and
never revisited as the system grew.

The fix is **not a ground-up rewrite** (we'd throw away the crown
jewels: HDR+bootstrap analysis, WS frame codec, SSE parser, Rhai DSL,
statistical rigour). The fix is a **~30% targeted rewrite** of the
dispatch / runner / backend-abstraction layer, preserving the 70%
that's actually good. Estimated effort: **5 weeks of focused work,
net −3,000 LoC, ~0 regressions**.

This document:
1. Lists every architectural smell found (§2).
2. Defines the principles we want the target to hold to (§3).
3. Sketches the target architecture (§4).
4. Compares three improvement paths: incremental / targeted / full
   rewrite (§5).
5. Recommends and phases the targeted approach (§6).
6. Gives a crate-by-crate disposition table (§7).

---

## 1. What's worth keeping

Calibration for the critique that follows: the project isn't a
disaster. These are the crown jewels — don't rewrite them.

| Crown jewel | Where | Why keep |
|---|---|---|
| Statistical analysis | `compare.rs` | Bootstrap CI, Scholz-Stephens k=2 AD exact formula, KS, Holm-Bonferroni — real domain expertise |
| HDR histogram + CO-free latency | throughout | `intended_start` from token-bucket slot, not post-sleep — correct per PHILOSOPHY §1/P6 |
| WebSocket codec | `zerobench-ws/src/frame.rs`, `handshake.rs` | RFC 6455 masking, fragmentation, close codes, ping/pong cap |
| SSE line parser | `zerobench-sse/src/line_parser.rs` | WHATWG-correct; handles `Id` events across chunk boundaries |
| LiveSnapshot sharding | `zerobench-core/src/live_snapshot.rs` | 16-shard mutex with thread-local hash; real 1M+ req/s design |
| Rhai DSL *surface* | `zerobench-rhai/*` (user-facing) | 51 callables, tested, good API |
| Archive format | `fingerprint.rs`, `archive.rs` | plan_hash identity projection, `$ZEROBENCH_HOME/<url_fp>/<run_id>/` sidecars |
| Template engine | `template.rs`, `var.rs` | `{{uuid}}`, `{{var:NAME}}`, `{{line:FILE}}`, static-literal detection |
| Per-protocol stubs + tests | `tests/backends_smoke.rs` × 2, inline tests | 620+ tests, real wire-level coverage |

These represent roughly 40% of the codebase by LoC, and close to 100%
of the *delivered value*. Any migration plan must preserve them
byte-compatibly.

---

## 2. Architectural smells (prioritised)

### 2.1 BLOCKING — these tax every future change

#### A1. Missing `trait Backend` — the central omission

11 backend entry-point functions, each with a bespoke signature. No
trait. The CLI hand-codes 9-way match statements to dispatch.

```
run_mio_threaded               (target, opts, plan, threads, conns, dur, rps, tls, live, stop)
run_cold_connect_from_plan…    (target, opts, plan, conns, dur, rps, tls, live, stop)
run_mio_h2_threaded            (target, opts, plan, threads, conns, dur, rps, tls, live, stop)
run_sse_hold_from_plan…        (target, opts, plan, dur, tls, live, stop)
run_sse_fanout_from_plan…      (target, opts, plan, dur, tls, live, stop)
run_sse_reconnect_storm…       (target, opts, plan, dur, tls, live, stop)
run_ws_echo_rtt_from_plan…     (target, opts, plan, dur, tls, live, stop)
run_ws_hold_from_plan…         (target, opts, plan, dur, tls, live, stop)
run_ws_server_push_rtt…        (target, opts, plan, dur, tls, live, stop)
run_ws_fanout_from_plan…       (target, opts, plan, dur, tls, live, stop)
```

Count the switch statements implementing "which backend handles which Step":

| Location | Arms | What it dispatches on |
|---|---|---|
| `measure::run` warmup block | 9 | `Step` variant (finer) |
| `measure::run` steady-state block | 9 | `Step` variant (finer, **duplicate of above**) |
| `main::run_mio_sync` | 3 | `Protocol` enum (coarser) |
| `main::dispatch_multi_protocol_plan` | 3 | `Protocol` enum |
| `main::run_script_sync` | 3 | `Protocol` enum |
| `report::pick_latency_source` | 3 | `Protocol` enum |

**Six places, two different dispatch granularities, no single source
of truth for routing.** Adding an 8th backend is a 6-site edit.

#### A2. `zerobench-core` is a kitchen sink, not a core

20 modules, **28 external deps** including `rustls`, `webpki-roots`,
`mio`, `blake3`, `serde_json`, `hdrhistogram`, `uuid`, `num_cpus`,
`libc`, `time`, `parking_lot`, `toml`, `yansi`.

A crate named "core" should be the *vocabulary* of the system. This one is:

- **Presentation:** `report.rs` (1,480 LoC — terminal / JSON / Prometheus)
- **Analysis:** `compare.rs` (1,397 LoC — bootstrap CI, KS, AD, Holm)
- **Persistence:** `archive.rs`, `machine.rs`
- **Transport:** `tls.rs`, `transport.rs`
- **Runtime:** `calibrate.rs` (spawns an in-process echo server!),
  `stop.rs`, `live_snapshot.rs`
- **Types:** `plan.rs`, `stats.rs`, `template.rs`, `var.rs`,
  `scenario_context.rs`
- **Helpers:** `json_scan.rs`, `rng.rs`, `histogram.rs`, `fingerprint.rs`

Every transport crate pulls in all of it. Compile-time cost is real;
semantic cost is worse — there's no place to put a "core type"
without also dragging in rustls.

#### A3. `plan.rs` leaks protocol details into core

915 LoC, 25 public types. Includes `SseHoldPlan`, `SseFanoutPlan`,
`SseReconnectStormPlan`, `WsEchoRttPlan`, `WsHoldPlan`,
`WsServerPushRttPlan`, `WsFanoutPlan`, `TriggerSpec`, `FanoutMode`,
`HeartbeatFrame`, `CorrelateStrategy`. These are *backend-internal
vocabulary* living in the shared vocabulary crate.

Adding `Step::Http3QuicStream(Http3QuicPlan)` touches core. Core then
has a `Http3QuicPlan` type it doesn't use. Same story in reverse: if
we want to split out an experimental backend crate, its plan struct
still has to live in core.

The `Step` enum has **11 variants** today. Every backend's `match` has
to exhaust them. Adding variant 12 is a 20+ file edit.

#### A4. Triple-record antipattern at every hot path

Every backend's "I completed one op" site does this trio:
```rust
stats.record(scenario_id, latency, ttfb, bytes_sent, bytes_recv);
if let Some(live) = live {
    let ns = latency.as_nanos() as u64;
    live.record(ns, bytes_sent, bytes_recv);
    live.record_scenario(scenario_id, ns, bytes_sent, bytes_recv);
}
```

Grep shows **15 identical sites** across mio_h1, mio_h2, cold_connect,
sse/hold, sse/reconnect_storm, ws/echo_rtt, ws/hold,
ws/server_push_rtt, ws/fanout, sse/fanout. Error-recording has its
own triplicate (`stats.record_error` + `live.record_error` +
`live.record_scenario_error`).

Inconsistencies already exist: `TaskStats::record` takes `Duration`;
`LiveSnapshot::record` takes `u64`. Every call site does `.as_nanos()
as u64` manually. One will get it wrong.

---

### 2.2 SIGNIFICANT — fix within weeks

#### B1. SSE and WS fanout are ~70% the same code

`zerobench-sse/src/fanout.rs` (635 LoC) and `zerobench-ws/src/fanout.rs`
(409 LoC) each contain:

- `run_trigger_loop` — structurally identical
- `fire_trigger` / `fire_http_trigger` — same function, different name
- `render_template` — identical
- Subscriber-spawn-N pattern — structurally identical
- Post-run correlation loop — **duplicated twice** in the recent
  `FanoutMode::Timestamp` commit (once per file, with identical
  `if emit_field.is_some() { … } else { trigger-peekable }` blocks)

#### B2. `measure::run()` — 693-line god function

Single function at `cli/verbs/measure.rs:380`, end-to-end responsible
for: plan construction, fingerprint, calibration, machine probe,
archive setup, the runs loop, warmup dispatch, steady-state dispatch,
per-run stat recording, summary merge, archive finalise, report
render, exit-code gating.

Of those, items 3–12 are not CLI concerns — they're **the runner**.
They belong in a library where `probe`, `curve`, `soak`, `replay` can
share them. Right now `curve.rs` (603 LoC) and `probe.rs` (196 LoC)
each re-implement their own dispatch, plan building, and exit gating.

#### B3. Rhai `builders.rs` — 2,300 LoC of near-duplicates

9 builders (Plan, Scenario, Request, SseHold, WsEchoRtt, WsHold,
WsServerPush, SseFanout, WsFanout, SseReconnectStorm) each with
identical shape:

- `pub(crate) struct FooBuilder { inner: Arc<Mutex<FooBuilderState>> }`
- `pub(crate) struct FooBuilderState { ... }`
- `impl Default for FooBuilderState`
- `impl FooBuilder { new, with_state, take_state }`
- `fn register_foo_builders(engine)` with `.header` / `.heartbeat` /
  `.payload` / etc. registrations

When `.heartbeat_frame(…)` landed it needed to be copy-pasted into two
builders that share no code. Any future "all WS builders need method
X" change is seven identical edits.

#### B4. `report.rs` hard-codes protocol-aware rendering

`pick_latency_source(summary, plan) -> (&str, u64, u64, u64, u64, u64)`
inspects `plan.scenarios[0].protocol()` and branches to
`sse_latency_from_scenarios`, `ws_latency_from_scenarios`, or default.
Returns a 5-tuple of percentiles plus a label.

The `(u64, u64, u64, u64, u64)` unnamed tuple is the worst kind of
"domain type" — you have to comment which slot is which. Every new
protocol-native metric requires a new branch here.

#### B5. CLI-args vs Rhai — two sources of truth for plan construction

Plans can be built two ways:

- CLI args (`--ws-echo N`, `--ws-payload "ping"`, `--kill-rate 0.1`) →
  `build_measure_plan()` at `measure.rs:1073` (~200 LoC)
- Rhai DSL (`ws_echo_rtt(url, N, rate).payload("ping")`) →
  `builders.rs::finalize_state` (~500 LoC)

Both produce `Plan`. Neither shares code with the other. When
`.heartbeat_frame(…)` landed in the Rhai `WsHoldBuilder`, the CLI's
`build_measure_plan` for `--ws-hold` had no corresponding
`--ws-heartbeat-frame` flag. **Each source silently supports a
different feature subset.**

#### B6. Error model is Balkanised

11 typed error enums across 10 crates:
`VarError`, `TargetError`, `TransportError`, `TemplateError`,
`RequestFileError`, `ScriptError`, `WsError`, `FrameError`,
`HandshakeError`, `BuildError`, `ErrorKind`.

Plus 3 backend-private ad-hoc enums: `ColdErr`, `SessionErr`,
`RecvErr`. Plus 15 `Box<dyn Error>` sites in the CLI. Plus one
untyped categorical `ErrorKind` that all transport errors collapse
into before crossing the boundary to `TaskStats`.

Every backend independently decides "how does my specific io::Error
map to an ErrorKind::Connect vs ErrorKind::Read". If `HostUnreachable`
vs `ConnectionRefused` ever need different treatment, it's 10+ sites
to update.

---

### 2.3 MINOR — fix opportunistically

- **`stats.rs` is 959 LoC** holding ErrorCounters + ErrorKind +
  ScenarioStats + TaskStats + Summary + SseExtras + WsExtras +
  SummaryExport. Split by concern.
- **`Target` has all fields `pub`** (`transport.rs:46`) — anyone can
  stamp a `Target` without `Target::parse`. Invariants not enforced.
- **`LiveSnapshot::record` takes `u64` latency; `TaskStats::record`
  takes `Duration`.** Callers paper over the gap manually.
- **`cargo build` default is `["h1"]`** — produces a binary that
  can't drive SSE/WS/TUI/H2/script. Headline features gated behind
  `--features full`.
- **`zerobench-stub` pulls `compio` + `compio-tls` + `compio-net`**,
  directly contradicting the README's "mio-only runtime". Comment
  this explicitly.
- **`register_pause_helpers_unreachable`** — dead reference code I
  left in `builders.rs:2178`. Belongs in a commit message, not source.
- **Closed-world enum bit me before**: `first_request_url` was
  missing 5 `Step` variants for months. The compiler couldn't catch
  it because the consumer used `find_map` + targeted match.
- **Feature-gating is inconsistent**: `#[cfg(feature = "sse")]` in the
  `measure` verb's warmup but NOT in `main::dispatch_multi_protocol_plan`.
- **`delay_ms` vs `delay` SSE query param** still inconsistent between
  `zerobench-stub/src/main.rs` docs and the real stubs.

---

## 3. Principles for the target architecture

A short list. Everything proposed in §4–§6 derives from these.

### P1. Closed-over-open for protocols

Adding a new backend should touch **exactly one new crate**. No edits
to core, no edits to the CLI dispatch, no edits to the reporter.

**Mechanism:** trait-object polymorphism (`Box<dyn Backend>`), plus a
backend registry. New backend = new crate + one registration line.

### P2. Crystalline core

`zerobench-core` (or its successor) should be the *vocabulary* of the
system. Target: **≤5 external deps, <2K LoC, zero protocol-specific
types, zero I/O**. It's what every other crate imports, and its
compile should take <2s.

### P3. Composable observations

Recording a sample is *one* operation. Observability sinks are
*orthogonal*: persistent stats, live TUI, JSONL stream, Prometheus
push, archive. Adding a sink shouldn't require touching every backend.

**Mechanism:** `trait Sink`, fan-out recorder, single-call API from
backend hot paths.

### P4. Type-once dispatch

No string-keyed dispatch. No enum-branched dispatch where trait
objects fit. The compiler should enforce completeness, and adding a
protocol shouldn't open an N-way match anywhere.

### P5. One plan-builder

CLI flags and Rhai DSL both lower to the same builder API. Any
feature added to one is automatically reflected in the other (or
surfaced as a compile error if forgotten).

### P6. Single runner

One dispatcher library that knows "here's a plan, here's a context,
produce results". `measure`, `probe`, `curve`, `soak`, `replay` all
call it. They differ only in *policy* (reporting format, archive
behaviour, retry rules) — never in dispatch mechanics.

### P7. Typed, narrow errors; categorical summaries

Every backend returns `Result<Sample, TransportError>` at the op
level. One `classify(&TransportError) -> ErrorKind` function owns the
mapping to the report's categorical taxonomy. Backend-private error
enums disappear.

### P8. No silent feature gates

The CLI knows at parse time which features the current binary has.
`--ws-echo` against an `h1`-only build produces a clear
"`ws` feature not compiled in this build, rebuild with
`--features ws`" at flag-parse, not at dispatch.

### P9. Tests as fossils

The 620 passing tests are a safety net for the rewrite. Every test
that exists today must pass at every phase boundary. If a test
becomes structurally invalid (e.g. asserts on `Step::Pause` which
we removed), it gets reshaped, not deleted.

---

## 4. Target architecture

### 4.1 Crate graph

**Before** (effective dependency weight shown):

```
zerobench-cli ── deps: ALL
   ├── zerobench-rhai ── core
   ├── zerobench-tui ── core
   ├── zerobench-http ── core (h1/h2)
   ├── zerobench-sse ── core, http
   └── zerobench-ws ── core, http
zerobench-core ── 28 external deps, 20 modules, 11K LoC
```

**After** (target):

```
zerobench-cli ── runtime, dsl, dispatcher, backends*
   ├── zerobench-dsl (Rhai bindings)        ── types, builder
   ├── zerobench-tui                        ── runtime
   ├── zerobench-dispatcher                 ── types, runtime, backend-registry
   ├── zerobench-http  (Backend impl)       ── types, runtime
   ├── zerobench-sse   (Backend impls × 3)  ── types, runtime, fanout-core
   ├── zerobench-ws    (Backend impls × 4)  ── types, runtime, fanout-core
   └── zerobench-backend-registry           ── all backends, trait object glue
zerobench-fanout-core ── types, runtime            (shared trigger+correlate logic)
zerobench-runtime      ── types                    (LiveSnapshot, Sink, StopSignal, Calibrate)
zerobench-archive      ── types                    (plan_hash, sidecars, histlog)
zerobench-analysis     ── types                    (bootstrap, KS, AD, compare)
zerobench-report       ── types, analysis          (terminal, JSON, Prometheus)
zerobench-builder      ── types                    (the single plan builder — §4.5)
zerobench-types        ── 3-4 external deps only, <2K LoC
   Plan, Scenario, Step (opaque), RateProfile, Sample, Stats,
   Target, TransportOpts, HttpVersionPref, Mode,
   Template, Var, ExpandCtx
```

Eleven narrow crates replace two fat ones. Compile-time cost
amortises because each crate imports only what it needs. Adding
`zerobench-http3` is *literally* `cargo new` + `impl Backend` + one
line in the registry.

### 4.2 `Backend` trait

```rust
/// The one-and-only polymorphism boundary for protocol backends.
/// Each crate defines one `impl Backend for …` per supported Step.
///
/// Concrete implementations typically look like:
///   pub struct MioH1; impl Backend for MioH1 { … }
///   pub struct ColdConnect; impl Backend for ColdConnect { … }
///   pub struct WsEchoRtt; impl Backend for WsEchoRtt { … }
pub trait Backend: Send + Sync {
    /// The human-readable name ("mio-h1", "ws-echo-rtt"). Used in
    /// reports and in `--trace` diagnostic output.
    fn name(&self) -> &'static str;

    /// True iff this backend handles the given step shape. The
    /// dispatcher calls this on every registered backend per
    /// scenario to find the first match.
    fn handles(&self, step: &Step) -> bool;

    /// Execute the scenario; record samples into `ctx.sink`.
    /// Returns a rollup of per-scenario counters.
    ///
    /// Errors at the TRANSPORT level surface through
    /// `ctx.sink.record_error`; errors in the setup (e.g. plan
    /// unsupported by this backend) return `Err` and abort.
    fn run(&self, ctx: &RunCtx<'_>, plan: &Plan) -> Result<Vec<TaskStats>, RunError>;
}
```

Context is a single struct bundling what every backend needs:

```rust
pub struct RunCtx<'a> {
    pub target: &'a Target,
    pub opts: &'a TransportOpts,
    pub duration: Duration,
    pub target_rps: Option<f64>,
    pub threads: usize,
    pub connections: usize,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub sink: &'a dyn Sink,          // <-- no more Option<&LiveSnapshot>
    pub stop: &'a dyn StopSignal,    // <-- no more Option<Arc<AtomicBool>>
    pub vars: &'a ScenarioContext,
}
```

No more eight-arg function signatures. No more `Option<Arc<T>>` dance.

### 4.3 `Sink` trait — kills the triple-record antipattern

```rust
/// Where a backend sends completed-op measurements. Implementations:
///
///  - `TaskStatsSink`      — the persistent `TaskStats` + per-scenario.
///  - `LiveSink`           — the TUI's sharded `LiveSnapshot`.
///  - `JsonlSink`          — per-sample JSONL stream (future).
///  - `PrometheusPushSink` — cumulative gauges (future).
///  - `MultiSink<A, B>`    — fan-out to two sinks.
///
/// The dispatcher composes whichever sinks the verb requested:
///   let sink = MultiSink::new(task_stats, live_snapshot);
///   backend.run(&ctx.with_sink(&sink), &plan);
pub trait Sink: Send + Sync {
    fn record(&self, sid: u16, sample: Sample);
    fn record_error(&self, sid: u16, kind: ErrorKind);
}

pub struct Sample {
    pub latency: Duration,
    pub ttfb: Duration,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
}
```

Backend call site goes from:
```rust
stats.record(sid, lat, ttfb, bs, br);
if let Some(live) = live {
    let ns = lat.as_nanos() as u64;
    live.record(ns, bs, br);
    live.record_scenario(sid, ns, bs, br);
}
```
to:
```rust
ctx.sink.record(sid, Sample { latency: lat, ttfb, bytes_sent: bs, bytes_recv: br });
```

**15 identical call sites become 15 × 1-line calls.** The
`Option<&LiveSnapshot>` is gone. `Duration` ↔ `u64` confusion is gone.
Adding a fourth sink (JSONL stream, say) doesn't touch any backend.

### 4.4 Opaque `Step` + backend-owned payloads

Today `Step` is a closed-world 11-variant enum known to core.
Target:

```rust
// In zerobench-types:
pub struct Step {
    pub kind: StepKind,  // small stable enum: Http, Sse, Ws, Custom(&'static str)
    pub payload: StepPayload,
}

pub enum StepPayload {
    // Core-known payloads (for compat + tooling):
    Request(RequestPlan),
    // Opaque: backend deserialises from json_value at plan-load.
    Opaque(serde_json::Value),
}
```

Or, if we want typed ergonomics inside each crate, use `Arc<dyn
Any>`:

```rust
pub struct Step {
    pub kind: &'static str,       // "sse_hold", "ws_echo_rtt", etc.
    pub payload: Arc<dyn Any + Send + Sync>,
}
```

Backends downcast:
```rust
impl Backend for SseHold {
    fn handles(&self, step: &Step) -> bool { step.kind == "sse_hold" }
    fn run(&self, ctx: &RunCtx, plan: &Plan) -> Result<…> {
        let sse_hold_plan: &SseHoldPlan = plan.scenarios[0].steps[0]
            .payload.downcast_ref().ok_or(RunError::PlanMismatch)?;
        …
    }
}
```

Tradeoff: loses compile-time exhaustiveness on `Step` matching. But we
already don't get it (match arms all use `find_map`, not exhaustive
match). And **this is exactly the knob that unlocks out-of-tree
backends** — a user could ship `zerobench-http3` without our cooperation.

### 4.5 One `PlanBuilder` — kills CLI/Rhai divergence

```rust
// zerobench-builder crate:
pub struct PlanBuilder { … }
impl PlanBuilder {
    pub fn scenario(&mut self, name: &str) -> &mut ScenarioBuilder { … }
    pub fn duration(&mut self, d: Duration) -> &mut Self { … }
    pub fn rate(&mut self, r: RateProfile) -> &mut Self { … }
    …
}
pub struct ScenarioBuilder<'a> { … }
impl ScenarioBuilder<'_> {
    // One method per knob — no builder-per-protocol.
    pub fn step_http(&mut self, req: RequestPlan) -> &mut Self { … }
    pub fn step_sse_hold(&mut self, p: SseHoldPlan) -> &mut Self { … }
    pub fn step_ws_echo(&mut self, p: WsEchoRttPlan) -> &mut Self { … }
    …
}
```

- **CLI** (`plan_from_cli.rs`): translates flags into `PlanBuilder` calls.
- **Rhai DSL** (`zerobench-dsl`): Rhai functions are thin wrappers over
  `PlanBuilder` calls.

One place to add a knob. Forgetting to wire a flag surfaces as
"PlanBuilder has no method for this"; forgetting to wire a Rhai
function surfaces as "unknown function". Both are compile-time /
eval-time checkable. Today, silent divergence.

### 4.6 Fanout-core

Extract the common skeleton of sse-fanout + ws-fanout:

```rust
// zerobench-fanout-core:
pub trait FanoutSubscriber: Send {
    /// Subscribe, hold, record inbound events with `received_at`
    /// and (when `emit_field.is_some()`) parsed `emit_ns`.
    fn run(self, ctx: FanoutCtx<'_>) -> FanoutSubStats;
}

pub fn run_fanout<S: FanoutSubscriber>(
    factory: impl Fn() -> S + Send + Sync,
    trigger: &TriggerSpec,
    mode: FanoutMode,
    ctx: &RunCtx<'_>,
) -> FanoutRollup {
    // Shared: spawn N subscribers, run trigger loop, collect,
    // correlate with triggers OR with emit_ns (per `mode`).
}
```

Protocol crates:
```rust
// zerobench-sse:
struct SseFanoutSubscriber { … }
impl FanoutSubscriber for SseFanoutSubscriber { … }
impl Backend for SseFanout {
    fn run(&self, ctx: …, plan: …) -> … {
        run_fanout(|| SseFanoutSubscriber::new(…), trigger, mode, ctx)
    }
}
```

~80 LoC protocol-specific each, instead of 400–600.

### 4.7 Error flow

```rust
// zerobench-types:
pub enum TransportError {
    Connect(io::Error),
    Tls(String),
    Read(io::Error),
    Write(io::Error),
    Timeout,
    BadResponse(&'static str),
    ProtocolMismatch,
}

pub fn classify(e: &TransportError) -> ErrorKind { … }
```

Every backend returns `Result<Sample, TransportError>` at op level.
Delete `ColdErr`, `SessionErr`, `RecvErr`. One classifier.

---

## 5. Three improvement paths

### Path A — Incremental refactor in place

**Approach:** fix one smell per week, keep the existing crate
structure, cargo check green after every commit.

**Effort:** ~6 weeks spread over ~3 months of calendar time.

**Pros:**
- Zero risk of regressions (green tests every commit).
- Continuous user value (no long dark period).
- Can pause / resume freely.

**Cons:**
- Coupling goes deep; cleaning `plan.rs` while keeping it in `core`
  means touching 30+ files per change.
- Most intermediate states are *worse* than both start and end —
  awkward duplication while migrating.
- Some fixes are mutually blocking (e.g. can't fix report.rs's
  protocol branches without a backend trait; can't split core
  cleanly without opaque Step).

**Estimated LoC delta:** −1,500 net.

**Estimated residual debt:** moderate. Runs aground on A3 (closed-
world Step) — full solution requires semi-big-bang.

### Path B — Targeted rewrite of the runtime layer (RECOMMENDED)

**Approach:** rewrite the dispatch / runner / backend-trait /
observability layer from scratch in a new set of crates. Port
existing protocol implementations to the new trait. Keep the
reporting + analysis + archive + parsers + Rhai surface as-is.

**Effort:** 5 weeks focused.

**Pros:**
- Fixes the root architectural problems (A1, A2, A3, A4, B1, B2, B5,
  B6) in one coherent design.
- Preserves crown jewels (analysis, parsers, DSL surface, tests).
- Reduces net LoC substantially (~3K) while adding capability.
- Sets up for Out-Of-Tree backends (HTTP/3, custom wire protocols).
- Each phase is a real shippable improvement — not a months-long
  limbo.

**Cons:**
- 1–2 weeks of "the tree compiles but some tests are red during
  phase transitions". Manageable with phased PRs.
- Touch ~60% of files (but most trivially — import-rename pass).
- Requires discipline to not expand scope into also-rewriting the
  crown jewels.

**Estimated LoC delta:** −3,000 net.

**Estimated residual debt:** low. This hits nearly every architectural
smell except the minor ones.

### Path C — Full rewrite

**Approach:** new repo, new design, port feature by feature.

**Effort:** 3–4 months.

**Pros:**
- Clean slate.
- Chance to reconsider choices that calcified (e.g. mio vs
  io_uring-direct, HDR vs t-digest).

**Cons:**
- Throws away 620 tests that cover edge cases nobody's going to
  remember to re-test.
- Throws away edge-case fixes accumulated across 131 commits (e.g.
  B4 Last-Event-ID chunk-boundary fix, S2 AD sigma² exact formula).
- Long dark period where the tool can't be used.
- History shows rewrite projects ship 50% of features 2× slower than
  estimated. Budget of $X → actual $2.5X.
- The jewels are **exactly the parts a rewrite would most likely get
  wrong** — they encode hard-won domain knowledge.

**Estimated LoC delta:** −5,000 net (but after a 4-month hole).

**Estimated residual debt:** very low *if executed well*. Medium-
high otherwise — there's a real chance of re-introducing old bugs.

---

## 6. Recommended path — phased 5-week rewrite (B)

### Phase 1 — Architecture skeleton (Week 1)

**Goal:** narrow-waist core is in place; old code still works against it.

- Create `zerobench-types` crate. Move `plan.rs` (types only, minus
  protocol-specific structs — those move to owning crates), `stats.rs`
  (the shared parts), `template.rs`, `var.rs`, `scenario_context.rs`,
  `histogram.rs`, `transport.rs` (Target + TransportOpts).
- Create `zerobench-analysis` crate. Move `compare.rs` +
  `fingerprint.rs`.
- Create `zerobench-archive` crate. Move `archive.rs` + `machine.rs`.
- Create `zerobench-report` crate. Move `report.rs` (stripped of
  protocol-specific formatters — those get wired to sink metadata in
  phase 3).
- Create `zerobench-runtime` crate. Move `live_snapshot.rs`, `stop.rs`,
  `calibrate.rs`, `json_scan.rs`, `tls.rs`, `rng.rs`.
- Old `zerobench-core` becomes a **re-export facade** (`pub use
  zerobench_types::*; pub use zerobench_analysis::*; …`) so no call
  site changes in this phase. Consumers migrate gradually.

**Exit criteria:** all 624 tests pass; `cargo build --features full`
is green; no crate has >10 external deps.

**Risk:** low. Move-and-rename with re-exports.

### Phase 2 — Backend trait + dispatcher + sinks (Week 2)

**Goal:** the new polymorphism boundary exists; one backend uses it.

- Define `Backend` trait in `zerobench-dispatcher`.
- Define `Sink` trait + `Sample` + `TaskStatsSink` + `LiveSink` +
  `MultiSink` in `zerobench-runtime`.
- Port `mio_h1` as the reference implementation. Keep the old
  `run_mio_threaded` function as a shim that constructs the context
  and calls `MioH1.run(&ctx, plan)` under the hood.
- Define `Dispatcher::execute(plan, ctx) -> Summary`. Currently
  dispatches only `Step::Request` to `MioH1`.

**Exit criteria:** `measure` verb produces identical output for HTTP
plans via the new dispatch. All tests green.

**Risk:** low. Old entry points still work; the new code path is
opt-in via one flag.

### Phase 3 — Port remaining backends + extract fanout-core (Week 3)

**Goal:** every protocol backend goes through `Backend`. Old entry
points become thin shims.

- Port `cold_connect`, `mio_h2` to trait. Share `capture_headers`,
  `check_assertions`, `apply_extractions` via the `zerobench-http`
  crate (already done in §recent commits).
- Port `sse_hold`, `sse_reconnect_storm`. Extract `fanout-core`
  crate; port `sse_fanout` + `ws_fanout` against it.
- Port `ws_echo_rtt`, `ws_hold`, `ws_server_push_rtt`.
- Wire protocol-specific metadata into `Sample` so `report.rs` can
  format via metadata, not `match protocol`.
- Delete `ColdErr`, `SessionErr`, `RecvErr`. All backends now return
  `Result<Sample, TransportError>` at op level.

**Exit criteria:** old `run_*_from_plan_threaded` functions gone or
reduced to one-line shims. Every protocol goes through `Dispatcher`.
All tests green.

**Risk:** medium. Touch ~15 files in the protocol crates, but each
is an isolated port.

### Phase 4 — CLI + DSL consolidation (Week 4)

**Goal:** one `PlanBuilder`. `measure.rs` is ~150 LoC.

- Create `zerobench-builder` crate with `PlanBuilder` +
  `ScenarioBuilder` — fluent, thread-safe, deterministic.
- Rewrite `plan_from_cli.rs` as flag → builder translation
  (~200 LoC).
- Rewrite `zerobench-rhai/builders.rs`: the 9 protocol builders
  collapse into a single macro. Rhai functions wrap `PlanBuilder`
  calls directly.
- Rewrite `measure.rs::run` as: `let plan = build(args)?; let summary
  = dispatcher.execute(plan, ctx)?; report::render(&summary);`.
  ~150 LoC.
- Rewrite `probe.rs`, `curve.rs` against the same dispatcher.

**Exit criteria:** no protocol-specific code in the CLI. No dual
plan-construction paths. `cli_args.rs` + `measure.rs` shrink by
>50%.

**Risk:** medium-high. Most user-visible file rewrites. Keep the
existing integration tests strictly; they're the safety net.

### Phase 5 — Polish + remove old core (Week 5)

**Goal:** tree is idiomatic; old core facade is gone.

- Delete `zerobench-core` facade. Every consumer imports from the
  narrow crates directly.
- Enforce error discipline: `#![deny(clippy::large_enum_variant)]`,
  typed errors on all public APIs.
- Rewrite remaining `Box<dyn Error>` sites in CLI to typed errors.
- Feature-flag audit: binary announces its compiled feature set on
  `--version`; flag parse refuses `--ws-echo` on `h1`-only build.
- Delete dead code (`register_pause_helpers_unreachable`).
- Update docs (`PHILOSOPHY.md`, `design-v0.1.0.md`, `REVIEW-*.md`).

**Exit criteria:** `cargo clippy --workspace -- -D warnings` clean.
`cargo-udeps` reports no unused deps. All 620+ tests pass.

**Risk:** low. Mostly cleanup.

### Total effort & deliverables

| Phase | Weeks | Net LoC | Risk | Test count |
|---|---|---|---|---|
| 1 | 1 | +300 | low | 624 → 624 |
| 2 | 1 | +400 | low | 624 → 630 |
| 3 | 1 | −1,500 | medium | 630 → 660 |
| 4 | 1 | −1,800 | medium-high | 660 → 680 |
| 5 | 1 | −400 | low | 680 → 700 |
| **Total** | **5** | **−3,000** | — | **+75** |

**Net result:** same feature surface, ~30K LoC instead of 35K,
3 more crates but each much narrower, all the core abstractions
landed, ready for HTTP/3 / QUIC / gRPC backends without CLI edits.

---

## 7. Crate-by-crate disposition

| Current crate / module | Disposition | New home | Notes |
|---|---|---|---|
| `zerobench-core::plan` | SPLIT | `zerobench-types` (core types) + protocol crates (SseHoldPlan etc.) | Payload moves to `Arc<dyn Any>` or `serde_json::Value`-opaque |
| `zerobench-core::stats` | SPLIT | `zerobench-types::stats` (shared) + protocol crates (SseExtras, WsExtras) | 959 LoC → ~400 + 100 + 100 |
| `zerobench-core::report` | MOVE | `zerobench-report` crate | Strip protocol branches; read metadata from `Sample` |
| `zerobench-core::compare` | MOVE | `zerobench-analysis` crate | No change internally |
| `zerobench-core::archive` | MOVE | `zerobench-archive` crate | No change internally |
| `zerobench-core::fingerprint` | MOVE | `zerobench-analysis` | Pairs naturally with compare |
| `zerobench-core::live_snapshot` | MOVE | `zerobench-runtime` | Becomes a `Sink` impl |
| `zerobench-core::calibrate` | MOVE | `zerobench-runtime` | Needs split from core anyway |
| `zerobench-core::template` | KEEP AS-IS | `zerobench-types::template` | Good abstraction |
| `zerobench-core::var` | KEEP AS-IS | `zerobench-types::var` | Tiny, correct |
| `zerobench-core::tls` | MOVE | `zerobench-runtime::tls` | One helper; not a domain type |
| `zerobench-core::transport` | SPLIT | `Target`, `TransportOpts` → types; `TransportError` → runtime | |
| `zerobench-core::json_scan` | KEEP | `zerobench-runtime::json_scan` | Fine; shared by fanout |
| `zerobench-core::rng` | KEEP | `zerobench-runtime::rng` | Tiny |
| `zerobench-core::stop` | KEEP | `zerobench-runtime::stop` | Becomes `StopSignal` trait |
| `zerobench-core::histogram` | KEEP | `zerobench-types::histogram` | Constants + helpers |
| `zerobench-core::machine` | MOVE | `zerobench-archive::machine` | Only used by archive |
| `zerobench-core::scenario_context` | KEEP | `zerobench-types::scenario_context` | |
| `zerobench-core::request_file` | KEEP | `zerobench-types::request_file` | |
| `zerobench-http` | REWORK | same | Introduce `Backend` impls; keep parsers |
| `zerobench-sse` | REWORK | same | Same |
| `zerobench-ws` | REWORK | same | Same |
| `zerobench-rhai` | REWRITE `builders.rs` | same | Builders collapse into macro; ~2,300 → ~800 LoC |
| `zerobench-tui` | KEEP AS-IS | same | Works; no architectural issues |
| `zerobench-cli` | REWRITE verbs | same | measure/probe/curve become ~150 LoC each via dispatcher |
| `zerobench-stub` | KEEP AS-IS | same | Self-contained test server, can live on its own runtime |
| **NEW: `zerobench-types`** | CREATE | — | Plan, Step, Scenario, Target, Template, Var, Stats, Sample, ErrorKind |
| **NEW: `zerobench-runtime`** | CREATE | — | LiveSnapshot, Sink, StopSignal, calibrate, tls, json_scan |
| **NEW: `zerobench-analysis`** | CREATE | — | bootstrap, KS, AD, compare, fingerprint |
| **NEW: `zerobench-archive`** | CREATE | — | plan_hash, sidecars, histlog |
| **NEW: `zerobench-report`** | CREATE | — | terminal, JSON, Prometheus rendering |
| **NEW: `zerobench-dispatcher`** | CREATE | — | Backend trait, Dispatcher, RunCtx |
| **NEW: `zerobench-builder`** | CREATE | — | PlanBuilder + ScenarioBuilder |
| **NEW: `zerobench-fanout-core`** | CREATE | — | Trigger loop + correlation logic shared by sse + ws fanout |

8 existing crates + 7 new = 15 total. Each narrower and more focused
than the current 2 fat ones.

---

## 8. Risk matrix & mitigation

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Breaking a statistical invariant during refactor (e.g. AD sigma² regression) | Low | High | `compare.rs` stays in `zerobench-analysis` untouched. No behavioural rewrite, only relocation |
| Behaviour drift in LiveSnapshot sharding | Very low | Medium | Port tests verbatim; run them at every phase boundary |
| Archive format incompatibility | Low | High | `plan_hash` identity projection untouched; sidecars untouched |
| Rhai DSL regression | Medium | High | Keep `builders.rs` tests byte-identical (per §4.5 the DSL *surface* doesn't change; only the internal factoring does) |
| Performance regression under 1M+ req/s | Medium | Medium | Re-run `zerobench vs node` benchmarks at every phase boundary. Specifically: `benchmarks/v8-compio saturate` numbers must stay within 3% of current |
| Feature flag matrix explosion | Low | Low | Single `full` feature gates all optional crates. Minimal `default` = `types` only |
| Scope creep into also-rewriting the crown jewels | Medium | High | Phase discipline; crown jewels are in §1 with "do not touch" marks |

---

## 9. Non-goals

Things we are **not** addressing in this rewrite:

- **Runtime choice.** mio/epoll stays. No io_uring, no compio, no
  tokio. `zerobench-stub`'s compio usage remains flagged as a
  documented test-only exception.
- **Histogram family.** HDR histograms stay. No t-digest / DDSketch
  side-grade.
- **Statistical method set.** Bootstrap + KS + AD + Holm stays. No
  new tests added in the rewrite.
- **CLI verb set.** `measure`, `probe`, `curve`, `compare`, `replay`,
  `run` stay. No `watch` revival, no new verbs.
- **Rhai as the DSL.** Keep.
- **Archive format.** `$ZEROBENCH_HOME/<url_fp>/<run_id>/{plan,machine,env,result,histlog,INDEX}`
  stays byte-compatible. Old archives remain readable.
- **Wire protocols.** Every protocol currently spoken stays supported
  at the same RFC compliance level.

If any of the above tempts us mid-rewrite, that's scope creep — reject
and log as follow-up.

---

## 10. Go / no-go decision inputs

Before starting:

- [ ] Confirm 5-week calendar window is actually available (no feature
  freezes or release commitments in conflict).
- [ ] Confirm no pending downstream consumers depend on the current
  `zerobench-core` re-export surface (if so, we extend the facade
  deprecation period to one minor version).
- [ ] Set up a `arch-v2` branch on the main repo for the work; no
  merges to `main` until Phase 3 complete.
- [ ] Baseline bench numbers captured from the current `main` on the
  reference machine (32-core, 2-socket NUMA) for regression
  detection at each phase boundary.

If all four boxes check: recommend **Path B, start immediately**.

---

## Appendix A — 15 minutes of before/after diffs

### measure.rs — dispatch

Before (9-arm match, duplicated for warmup):
```rust
match first_step {
    Some(Step::HttpColdConnect(_)) => { run_cold_connect_from_plan_threaded(…) }
    #[cfg(feature = "sse")]
    Some(Step::SseHold(_))         => { run_sse_hold_from_plan_threaded(…) }
    #[cfg(feature = "sse")]
    Some(Step::SseFanout(_))       => { run_sse_fanout_from_plan_threaded(…) }
    …7 more arms…
}
```

After:
```rust
dispatcher.execute(&plan, &ctx)?;
```

### backend hot path

Before (15 copies of this trio):
```rust
stats.record(sid, latency, ttfb, bytes_sent, bytes_recv);
if let Some(live) = live {
    let ns = latency.as_nanos() as u64;
    live.record(ns, bytes_sent, bytes_recv);
    live.record_scenario(sid, ns, bytes_sent, bytes_recv);
}
```

After:
```rust
ctx.sink.record(sid, Sample { latency, ttfb, bytes_sent, bytes_recv });
```

### Adding a new protocol (HTTP/3, say)

Before: touch `plan.rs` (new Step variant), `report.rs` (new branch),
`measure.rs` (new match arm × 2), `main.rs` (new match arm × 3),
`Cargo.toml` × 3, 6-file PR, review blocks on core changes.

After: `cargo new zerobench-http3`, write `impl Backend for
Http3Stream`, register it in `backend_registry::all_builtin()`. One
crate, one line in the registry. No CLI changes.

---

## Appendix B — Principled no-code reviewer answers

**Q: Why not just a `Box<dyn Fn>` registry instead of a trait?**
A: We want `fn handles(&self, &Step) -> bool` for capability-matching,
`fn name(&self) -> &'static str` for diagnostics, and per-backend
state if any. Closures can't carry state cleanly across these.

**Q: Why keep Rhai? Why not a data-only DSL (TOML/JSON)?**
A: `{{uuid}}`, `{{var:NAME}}`, loops, conditionals on env vars,
`parse_int(env("X", "10"))` — real scripts need a real language. The
Rhai *surface* is fine; only the internal builder scaffolding bloated.

**Q: Why `Arc<dyn Any>` for Step payload — dynamic typing?**
A: Alternative is `serde_json::Value` (slower, allocates). Alternative
is `Box<dyn StepPayload>` with a trait (requires serialize/deserialize
through trait objects — tricky with serde). `Arc<dyn Any>` is the
simplest thing that enables out-of-tree backends. For serde round-trip
we use `serde_json::Value` as the wire form and each backend `From`s
it into its typed plan struct at plan-load.

**Q: Is 5 weeks realistic?**
A: Yes, assuming full focus. Every phase is a self-contained PR
shape. Phase 2 and 3 are the riskiest (actual architecture
change); Phase 1 and 5 are mechanical. If we budget 6 weeks for
slack, we almost certainly ship. Three months would indicate we're
secretly doing Path C.

**Q: What about keeping an "escape hatch" entry point like
`run_mio_threaded` for direct callers?**
A: No external callers — `grep` confirms all calls come from
`zerobench-cli`. Deprecate and delete.

---

*End of review.*
