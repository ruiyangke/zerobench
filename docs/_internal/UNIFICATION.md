# Tier 1 Protocol Unification — Design

Unify HTTP / SSE / WebSocket into a single `Plan` so one Rhai file can
express all three, one `zerobench run script.rhai` call runs everything,
one report displays all results.

The three event loops stay separate under the hood (one per backend,
running in parallel threads). Unification is at the **surface** —
`Plan`, DSL, and report.

## Design decisions

1. **Each scenario is one protocol.** Mixed-protocol scenarios are out
   of scope for Tier 1. A scenario's protocol is inferred from its first
   non-Pause step.

2. **`rate("80k/s")` applies to HTTP scenarios only.** SSE and WS
   scenarios saturate by default (N concurrent connections/streams).
   Rate control for stream-opens can be added in Tier 2.

3. **Top-line throughput = sum of "operations"** across all scenarios,
   where an operation is:
   - HTTP: one request/response
   - SSE: one completed stream (not each chunk)
   - WS: one message round-trip

4. **`-c N` for HTTP** = total connection pool shared across HTTP scenarios.
   For SSE/WS scenarios, each scenario gets `-c N` concurrent
   streams/connections (existing behaviour).

## Type changes (zerobench-core)

### `Step` variants

```rust
pub enum Step {
    Request(RequestPlan),    // HTTP (existing)
    SseStream(SsePlan),      // NEW
    WsRound(WsRoundPlan),    // NEW
    Pause(Duration),         // existing
    PauseRandom { min: Duration, max: Duration },  // existing
}

pub struct SsePlan {
    pub url: Template,
    pub headers: SmallVec<[(Template, Template); 4]>,
    pub expect_chunks: Option<usize>,   // assertion: stream must emit at least N data events
}

pub struct WsRoundPlan {
    pub url: Template,
    pub headers: SmallVec<[(Template, Template); 4]>,
    pub message: Template,     // text frame payload sent per iteration
}
```

### `Protocol` inference

```rust
pub enum Protocol { Http, Sse, Ws }

impl Scenario {
    pub fn protocol(&self) -> Protocol {
        for step in &self.steps {
            match step {
                Step::Request(_) => return Protocol::Http,
                Step::SseStream(_) => return Protocol::Sse,
                Step::WsRound(_) => return Protocol::Ws,
                _ => continue,  // Pause variants
            }
        }
        Protocol::Http  // empty scenario — shouldn't happen, default safely
    }
}
```

### `ScenarioStats` extras

```rust
pub struct ScenarioStats {
    pub scenario_id: u16,
    pub requests: u64,
    pub latency: Histogram<u64>,
    pub errors: ErrorCounters,
    pub sse: Option<SseExtras>,       // NEW
    pub ws: Option<WsExtras>,         // NEW
}

pub struct SseExtras {
    pub ttfb: Histogram<u64>,
    pub chunk_gap: Histogram<u64>,
    pub chunks: u64,
    pub streams_completed: u64,
    pub bytes_received: u64,
}

pub struct WsExtras {
    pub handshake: Histogram<u64>,
    pub rtt: Histogram<u64>,
    pub messages_sent: u64,
    pub messages_recv: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
}
```

## Rhai DSL additions (zerobench-rhai)

```rhai
// Existing HTTP
scenario("http-ping", |s| {
    s.step(GET("http://x/ping").expect_status(200));
});

// NEW SSE
scenario("sse-events", |s| {
    s.step(SSE("http://x/events").expect_chunks(100));
});

// NEW WebSocket
scenario("ws-echo", |s| {
    s.step(WS("ws://x/chat").message("ping"));
});
```

Builder functions to register:
- `SSE(url: String) -> SseBuilder`
- `SseBuilder::header(name, value) -> SseBuilder`
- `SseBuilder::expect_chunks(n: i64) -> SseBuilder`
- `WS(url: String) -> WsBuilder`
- `WsBuilder::header(name, value) -> WsBuilder`
- `WsBuilder::message(text: String) -> WsBuilder`
- Plus `.step()` acceptance on `ScenarioBuilder` for `StepSource::Sse(SseBuilder)` and `::Ws(WsBuilder)`.

## CLI dispatcher split (zerobench-cli)

In `run_script_sync` (and the main bench path if we extend it to accept
request files with mixed protocols — future work):

```rust
// Partition scenarios by protocol.
let mut http_scenarios = Vec::new();
let mut sse_scenarios = Vec::new();
let mut ws_scenarios = Vec::new();
for (idx, sc) in plan.scenarios.iter().enumerate() {
    match sc.protocol() {
        Protocol::Http => http_scenarios.push((idx, sc.clone())),
        Protocol::Sse  => sse_scenarios.push((idx, sc.clone())),
        Protocol::Ws   => ws_scenarios.push((idx, sc.clone())),
    }
}

// Run each backend (threads launched in parallel).
let http_handle = spawn_http(plan, http_scenarios, ...);
let sse_handle = spawn_sse(plan, sse_scenarios, ...);
let ws_handle = spawn_ws(plan, ws_scenarios, ...);

// Join, merge into unified Summary.
let http_stats = http_handle.join()?;
let sse_stats = sse_handle.join()?;
let ws_stats = ws_handle.join()?;

let summary = Summary::merge_multi(http_stats, sse_stats, ws_stats, ...);
```

### Backend adaptations

- **HTTP**: `run_mio_threaded` already takes `&Plan`. Modify its scenario
  filter so it only executes scenarios whose first step is `Request(_)`.
  Scenarios with other protocols are silently skipped (they're handled
  by other backends).
- **SSE**: `run_sse_threaded` currently takes `&Plan` but only reads
  `scenarios[0].steps[0]`. Extend to iterate all SSE scenarios, distribute
  `-c` across them, attribute stats back to the right scenario_id.
- **WS**: Currently takes `WsPlan` directly. Add a new
  `run_ws_scenarios_threaded(target, opts, scenarios, conns, ...)` that
  handles multiple WS scenarios from one Plan. Keep the old single-plan
  API for the `--ws` CLI shortcut path.

## Report unification

`print_terminal` learns to render per-scenario rows based on protocol:

```
target         saturate (200 tasks) (16 threads)
duration       30.00s  |  total 500,000 operations
throughput     16,667 ops/s · ↑ 2.1 MB/s  ↓ 3.5 MB/s
transfer       ↑ 63 MB sent  ↓ 105 MB received
errors         connect 0  read 0  write 0  timeout 0

scenarios
  http-rpc    HTTP  (50%)  250,000 req   p50=200µs  p99=500µs  p99.9=1.2ms
  sse-events  SSE   (25%)  100 streams  2,500,000 chunks  TTFB p99=10ms  chunk p99=50µs
  ws-chat     WS    (25%)  100 conns    125,000 msgs  RTT p99=300µs  hs p99=5ms
```

## Out of scope for Tier 1

- Mixed-protocol scenarios (one scenario with HTTP + SSE in sequence)
- Open-loop rate control for SSE stream-opens / WS connection-opens
- Unified event loop (Tier 2)
- Transaction-trait abstraction (Tier 3)

## Implementation phases

**Phase 1 — Core types (~150 LOC)**
- Add `Step::SseStream`, `Step::WsRound`, `SsePlan`, `WsRoundPlan`
- Add `Protocol` enum + `Scenario::protocol()`
- Extend `ScenarioStats` with `sse`, `ws` optional extras (Default impls)
- Extend `Summary::merge` to roll up extras
- Update tests for new variants (Default paths must not break)

**Phase 2 — Rhai DSL (~120 LOC)**
- `StepSource::Sse(SseBuilder)` and `::Ws(WsBuilder)` variants
- `engine.register_fn("SSE", ...)`, `engine.register_fn("WS", ...)`
- Methods: `.header`, `.expect_chunks` (SSE), `.message` (WS)
- `ScenarioBuilder::step(StepSource)` accepts both
- Plan finalization translates `SseBuilder`/`WsBuilder` into `Step` variants

**Phase 3 — Backend adaptations (~200 LOC)**
- HTTP backend: skip non-HTTP scenarios silently
- SSE backend: iterate all SSE scenarios, distribute `-c`, tag stats with scenario_id
- WS backend: same treatment
- Stats emission: each backend fills its own `SseExtras`/`WsExtras`

**Phase 4 — CLI dispatch (~80 LOC)**
- Partition plan by protocol
- Spawn three threads (one per backend), join, merge
- Single unified `Summary`

**Phase 5 — Report (~100 LOC)**
- Top-line uses "operations" (protocol-agnostic term) when mixed
- Per-scenario rendering branches on extras
- Protocol badge column (HTTP / SSE / WS)

**Phase 6 — Tests + benchmark script**
- Unit tests for new step variants + protocol inference
- Rhai compile tests for new builders
- Integration: Rhai file exercising all three protocols
- Update `zeroship-bench.rhai` in appbase to include SSE + WS scenarios
- Simplify `run_zerobench.sh` to a single invocation
