---
status: stable
version: v0.1.0
supersedes: docs/_internal/design.md (v0.0.1 architecture, archived)
date: 2026-04-18
---

# zerobench — Philosophy (v0.1.0 direction)

> **tl;dr** — zerobench is not a load generator. It is a *measurement
> apparatus*. Numbers are cheap; trustworthy, comparable claims about
> system behaviour are rare. v0.0.1 chased correctness primitives
> (HDR-ns, CO-free, mio). v0.1.0 turns those primitives into claims:
> calibration before measurement, baselines before rates, protocol-native
> metrics instead of a forced "ops/s" denominator, reproducibility
> metadata on every run, statistical significance on every comparison.

## 0. Status and scope

This document is the philosophical spec for v0.1.0. It supersedes the
goals/non-goals of the pre-0.1.0 design (archived at
`_internal/design.md`). The pre-0.1.0 architecture (two-phase
Plan/execute, mio engine, Rhai DSL, HDR histograms) is retained; what
changes is what we *ask the tool to do* and how we *judge success*.

This is the upstream doc. The companion type-level spec is
`design-v0.1.0.md`; for historical context (pre-0.1.0) see
`_internal/design.md`.

Reference prior art influencing this design: wrk2 (constant-throughput
CO-free), HdrHistogram (HDR logs + percentile distributions), Gatling
(bootstrap CIs, percentile reports), k6/xk6-sse (SSE primitives),
Artillery (websocket engine, mixed-mode scenarios), Netflix NDBench
(long-run stability, min-of-N), Coburn Watson's load-testing notes
(canary comparison scoring). Where a revision is informed by one of
these, it is cited inline.

### Performance baseline (v0.0.1, current)

Against a canonical loopback-echo target at matched concurrency,
**zerobench sustains 1.2–1.5× wrk's throughput**, with zero hot-path
allocations and zero contended locks. This is not an aspiration —
it is the measured starting point v0.1.0 inherits. The §9.6
Performance contract's hard floors are calibrated to *defend this
lead*, not to reach it. Regressions below parity with wrk block
release.

---

## 1. Thesis

> **A benchmark is a claim about a system. zerobench's job is to make
> claims that are trustworthy and comparable.**

A datapoint is a number with a measurement procedure. A claim is a
datapoint plus enough context (machine, commit, tool version, flags,
duration, warmup, error rate, concurrency) that someone else can
reproduce it and judge whether it applies to their situation.

v0.0.1 produces datapoints. v0.1.0 produces claims.

The practical consequence: every number zerobench emits — on stdout, in
JSON, in the TUI — must ship with enough context that it survives
being forwarded in a screenshot, a PR comment, or a bug report. Bare
numbers are banned.

---

## 2. What zerobench is / is not

### Is

- A **measurement apparatus** for HTTP, HTTP/2, HTTP/3, SSE, and WebSocket services.
- A **comparison tool**: commit vs commit, runtime vs runtime,
  config vs config.
- A **regression gate** suitable for CI (SLO-fails the build).
- A **saturation explorer**: given a target, find where it breaks.

### Is not

- A **load simulator** (Locust/Gatling territory — browser workflows,
  realistic user journeys).
- A **correctness checker** (we assert status and latency, not
  schema/semantic conformance).
- A **browser emulator** (k6/artillery territory).
- **wrk-compatible** in CLI or output. A convenience shim may appear
  later; the core does not compromise for it.
- **Distributed** (multi-client coordination) — deferred to v0.2.x.

The "is" list is short on purpose. Every capability beyond it
dilutes the claim-quality we're trying to guarantee.

---

## 3. Principles

Twelve principles, ordered by how often they fire in design decisions.

### P1. Every number ships with context

Tool refuses to emit a bare number. Every result — terminal, JSON,
TUI — carries: tool version + feature flags, machine fingerprint (see
§8.2), duration + warmup, target (resolved IP + scheme + port + SNI),
plan (scenarios, rates, concurrency, canonical hash), timestamp,
user-supplied `--context KEY=VAL` pairs.

Without this, two numbers from two runs cannot be compared.

**Mandatory fields that refuse-to-run if missing**: tool version
+ feature flags, machine fingerprint, duration + warmup, target
(scheme + host + port, resolved IP set), plan hash, timestamp.
These are derivable in every environment; their absence indicates
a bug or a corrupted install.

**Best-effort fields that warn-if-missing**: git commit (only if
target is local and detectable), NIC model/speed (some containers
hide), NUMA topology (bare-metal vs cloud), cpufreq governor
(some distros mask). Warnings list the absent fields once at
startup; the run proceeds and records `best_effort_missing:
["git_commit", ...]` in env.json.

User-supplied `--context KEY=VAL` is always optional.

<!-- Added in round 1: addressing critic CRITICAL on §4.3 server cooperation requirement, and the missing context on SNI -->

### P2. Compare, don't measure in isolation

The default user workflow is *comparison*, not *measurement*. A
single run is rarely the question; "is this better than before /
than node / than yesterday?" is.

First-run behaviour: record result, save as baseline for that
target+plan hash. Subsequent runs: auto-compare to baseline, show
delta **with bootstrap 95% confidence intervals** (§9.3), exit
non-zero on statistically-significant regression beyond configured
thresholds.

**Default threshold behaviour** (when `--regress-on` is not
specified): comparison produces informational output only; exit
code stays 0 regardless of delta. A warning line reminds the
user: "no regression thresholds configured; comparison is
informational". Adding `--regress-on` enables gate semantics.
Rationale: an unconfigured regression gate is a surprise failure
mode; users must opt in explicitly.

The `diff` subcommand stays but becomes the fallback, not the
primary interface.

### P3. Tail latency is the product

Mean latency lies. Every output shows p50, p90, p99, p99.9, p99.99,
max. Histograms exported in **HdrHistogram V2 compressed-log format**
as `result.histlog` — the same Base64+ZLIB format jHiccup, wrk2, and
the JVM ecosystem emit, so external tools (HdrHistogram Plotter,
jHiccup analyzer) read zerobench output directly. A plotter-friendly
percentile tabular dump (`.hgrm`, the HdrHistogram convention) is
also produced for quick-look.

**Live percentiles use a rolling HDR histogram**, not t-digest.
A sliding window of HDR buckets is maintained for the TUI.

**Ring-buffer structure**: the window is implemented as a ring
of `W` per-second HDR sub-histograms (W = window seconds); at
each second boundary, the oldest sub-histogram is rotated out
and reset, and its contents are folded into the final `result.
histlog` via HDR addition. Memory is `W × ~100KB = ~500KB for
5s, ~3MB for 30s` per scenario — bounded regardless of run
length. The final histogram merges all sub-histograms at
end-of-run.

Window size is 5 seconds by default; configurable via
`--live-window 30s` for noisier workloads where a short window
shows too much jitter. Minimum window is 1s (below that, there
is insufficient data for meaningful percentile estimates;
`--live-window 0s` is rejected at plan-compile time). Maximum
is 3600s (1h) — beyond that the ring-buffer memory cost
exceeds reasonable limits. End-of-run merged HDR is the source
of truth. Live percentiles are labelled "live HDR (Ns window)";
final percentiles are labelled "HDR (full run)".

**Long-run memory bound**: soak runs up to 24h use the same
ring — memory is constant-time in window size, not run length.
HDR count fields are u64; at 1M req/s × 86400s = 86.4B
observations, any single bucket is nowhere near u64 overflow.

**On-disk incremental writes**: every 60s boundary, zerobench
appends an **interval record** to `result.histlog` (the HDR V2
log format natively supports interval-labelled records —
one entry per interval with `interval_start_s` /
`interval_length_s` header per the compressed-log spec). Each
interval record is ~1-10 KB compressed. At 24h × 60s intervals
= 1440 records × ~5KB = ~7MB total on disk. I/O is bursty once
per minute, not per-second, and never on the hot path.

Merge at end-of-run is O(intervals × buckets) — reading the
interval records back and merging into one final histogram —
typically <200ms for a 24h soak. External tools that consume
`.histlog` can either consume the intervals (time-series
reconstruction) or the final merged histogram (aggregate), both
from the same file.

<!-- Added in round 1: addressing MINOR #19 — pick one HDR format as source of truth; emit both .hlog (compressed log, canonical) and .hgrm (plotting) -->
<!-- Added in round 2: addressing MAJOR #7 — drop t-digest in favour of rolling HDR; addressing MINOR #15 — rename .hlog to .histlog to avoid Heroku collision -->

### P4. Load is a curve, not a point

"How fast is X?" has no single-number answer. It has a curve: offered
rate on the x-axis, p99 latency (or error rate) on the y-axis, with
a knee. The knee is the answer.

A first-class `curve` mode ramps offered rate from low to high,
identifies the knee (first p99 doubling over the low-rate baseline,
or first non-zero error rate sustained for ≥3s), and reports both the
curve and the knee. `rate` and `saturate` remain for when you know
what you want.

`curve` differs from `calibrate` (§5): `calibrate` finds the
**client's** ceiling against `/dev/null`; `curve` finds the
**server's** knee against the real target. Non-overlapping questions.

<!-- Added in round 1: addressing CRITICAL #4 — disambiguate calibrate vs curve -->

### P5. The client must not bottleneck

Every run begins with a **client self-check**: emit the target rate
against an **in-process echo endpoint** (TCP loopback, same kernel
path, no user-space bypass) and verify the scheduler holds the rate
under the configured concurrency on this machine. Report the client's
ceiling before the real run starts.

**Why loopback not localhost bypass**: we want to exercise the real
kernel network stack (socket, buffer, poll), because any per-request
kernel cost that bottlenecks the client will also bottleneck the real
run.

**Topology**: the echo endpoint runs as a dedicated thread in the
zerobench process, **pinned to a core that is not used by load-
generator workers**. On a machine with ≥4 cores, workers are
pinned to cores `[0..N-2]` and the echo thread to core `N-1`
(last). On 2-core machines, pinning is skipped and a warning is
issued that the self-check may underestimate tool capacity. In-
process rather than separate-process, because a separate process
adds scheduler cross-context overhead that doesn't exist in the
real run. Loopback is *cheaper* per request than any real NIC
(no PCIe, no wire, no driver IRQ coalescing), so the rate the
client sustains against loopback is an **upper bound on the
tool's achievable rate everywhere else** — real targets run at
≤ this ceiling because the network-stack cost only grows. If the
self-check caps at X req/s, no real target will see more than X
from this client on this machine.

If the user requests 500k req/s and the client can only sustain 200k
against loopback, the tool says so up front — not after 30 seconds of
misleading data.

<!-- Added in round 1: addressing MAJOR #7 — how is the stub invoked -->

### P6. Coordinated omission is a measurement defect, always

Open-loop with **scheduled intended-start times** (wrk2's model) is
the default. Latency = `now - intended_start`. The scheduler tracks
a token bucket with infinite capacity for backlog; when the bucket
exceeds a warn threshold, we mark *keep-up failure* in the report
and continue, logging the drift.

**Deviation from wrk2**: wrk2 continues indefinitely regardless of
backlog (reports it but does not abort). zerobench additionally
supports an **abort-on-fail** mode, but **it is not the default**,
to preserve comparability with wrk2-era numbers. The behaviour is
selectable:

- **Warn threshold**: scheduler is ≥100ms behind intended rate for
  ≥1s. Emits `keepup=warn` into JSONL; visible in TUI as amber.
  Always active.
- **Fail threshold** (default: report-only, wrk2-compatible):
  scheduler is ≥1s behind intended rate for ≥3s. Emits
  `keepup=fail` and flags the run as degraded, but continues.
  Comparable with wrk2.
- **`--strict-keepup`** (opt-in): fail threshold aborts the run
  with non-zero exit and invalidates the result for comparison.
  For CI where CO-compromised data is worse than no data.

The thresholds (100ms/1s/3s) are **Linux-CFS-calibrated**:
under CFS with default `sched_latency_ns=24ms`, scheduler drift
below 100ms is indistinguishable from normal jitter; above 1s is
pathological. On macOS (XNU's Mach scheduler with different tuning),
in cgroup-constrained containers with CPU quotas, or on Linux
kernels configured for real-time (SCHED_DEADLINE / PREEMPT_RT),
the sensible thresholds differ — we do not pretend otherwise.
These defaults are adjustable via `--keepup-warn-ms` /
`--keepup-fail-ms`; the default ships a `platform_default=linux-cfs`
tag in the result so downstream analysis can tell whether a run
used the canonical numbers or a platform-specific override.
Users on non-Linux platforms should calibrate their own
thresholds from a no-op benchmark against loopback on idle
hardware.

Closed-loop (`saturate`) is a deliberate mode for *tail-under-overload*
studies, clearly labelled as such in the report (header says
"mode: saturate (closed-loop) — CO-suppression not applicable").
`rate`-based runs that degrade into saturation mark the transition
(token bucket overflowed at t=N) and report it as above.

<!-- Added in round 1: addressing MAJOR #8 — CO thresholds specified; credit wrk2 for scheduled-start model -->

### P7. Protocols are different. Treat them differently.

See §4. HTTP is request-driven with short sessions. SSE is
server-driven with long sessions. WS is bidirectional with long
sessions. The current v0.0.1 unification of "operation per second"
across all three is fiction — it makes SSE/WS metrics meaningless.

Unification is at the **report shape** (identical schema fields,
compare-able structure) — not at the **op numerator** (shoehorning
everything into a req/s number).

### P8. Long enough to trust, short enough to iterate

Defaults matter because they shape user expectations. Three default
durations:

- `probe`: 5s — smoke test, "does it work". Short by design:
  probe is the zero-argument default and any new user's first
  impression; 5s makes `zerobench URL` feel snappy. Smoke tests
  don't need 10s to conclude "the server is up"; if you want
  rigour, use `measure`.
- `measure`: 60s + 15s warmup, 3 runs, 10s cooldown between runs —
  **new default**. Cooldown lets TIME_WAIT drain (per wrk2 notes on
  port-exhaustion artefacts). Warmup is configurable via
  `--warmup DURATION`; for JIT-heavy targets (JVM HotSpot, V8
  TurboFan) users should pass a larger value (60–120s is
  commonly cited in JMH and JEP-430 guidance, though the exact
  number is workload-specific — the tool cannot verify from the
  client side that a given warmup was sufficient). No target-
  by-name presets ship: zerobench does not encode per-runtime
  timings because (a) the client cannot observe server JIT state
  and therefore cannot orchestrate or verify reoptimization
  cycles, and (b) baking target names into the CLI bleeds
  zeroship-specific context into a general-purpose tool.
- `soak`: 5 min + warmup, 1 run — long-run stability (find leaks,
  GC cliffs, FD exhaustion). Based on Netflix NDBench patterns for
  durable-state regression detection. The 5-minute default
  surfaces most GC-class issues and FD exhaustion on typical
  services; **slow memory leaks may need hours** — use
  `--duration 24h` for deep leak-hunt runs. zerobench does not
  detect leaks directly (out of scope, §15 Q6) but its stable-
  rate emission over long periods is the precondition for
  external leak detection tools (pprof, eBPF) to do their work.

30-second runs silently missing GC pauses is a measurement defect.
v0.1.0 default moves to 60s + warmup + 3 runs.

**Warmup semantics**: warmup's histogram is *discarded entirely*
from the steady-state `result.json` but preserved in a separate
`warmup.histlog` for inspection. This is standard in JVM
benchmarking (JMH) and wrk2 (`-w warmup`).

**Warmup failure handling**: warmup aborts the steady-state phase
only on **connectivity errors**, not application errors. The
default threshold is `--warmup-connect-error-threshold=5%`
counting only `errors.connect_refused | connect_timeout |
connect_reset | tls_handshake | read_timeout | write_timeout`.
Application-level 4xx/5xx during warmup do **not** abort —
authentication endpoints, validation APIs, and rate-limited
services legitimately return 4xx during steady state, and a
warmup mostly returning 401 is valid data for a "how fast does
auth reject?" benchmark. Users who want strict application-
level guards specify `--warmup-app-error-threshold=X%`
explicitly.

Abort exit code is 2 (not 1 — measure phase never ran, so it's
neither pass nor fail; it's "did not measure"). `warmup.histlog`
and `warmup_result.json` are still archived; users can diagnose
from them. This prevents the silently-bad-data-in-a-broken-
environment footgun.

<!-- Added in round 1: addressing Missing Concepts — warmup discard semantics; addressing MAJOR #10 — inter-run cooldown default 10s -->

### P9. Scenarios are independent experiments

Serial-per-scenario stays (one scenario at a time, dedicated pool,
isolated histogram). The `--parallel` opt-out exists for explicitly
modelling mixed production traffic. This is current v0.0.1 behaviour
— kept and documented.

Consequence: `duration("60s")` in a 5-scenario Rhai script means
each scenario runs 60s. For `S` scenarios with `R` runs each:

    total_wall_time = S × (warmup + R × duration + (R-1) × cooldown)

e.g., 5 scenarios × 3 runs × 60s each with 15s warmup and 10s
cooldown = 5 × (15 + 3×60 + 2×10) = 5 × 215 = 1075s (~18min).

Pre-run summary displays this formula with the concrete numbers
so users see the expected wall time before hitting Enter.

### P10. The tool is invisible

**Hard constraint, not aspiration.** The tool is the measurement
instrument; its performance floor must be strictly lower than the
signals it claims to measure. Violations are bugs, not trade-offs.

Floors (verified in CI — see §9.6 Performance contract):

- **Per-request tool overhead <2 µs p99** against loopback echo on
  a dedicated core.
- **Scheduler jitter <5 µs p99** for open-loop token emission.
- **Zero heap allocation on the hot path.** Template buffers,
  connection slots, histogram entries — all pre-allocated and
  reused. Enforced by an allocation-counter test.
- **Zero contended locks on the hot path.** Per-thread state +
  sharded counters; never a global mutex in the request flow.
- **Tool overhead <1% of the measured number.** The boot-time
  self-check (P5) verifies this: if loopback echo at the requested
  rate runs below 99.0% sustained, **the tool refuses to run**.
  `--force-overload` opts in with an explicit acknowledgement
  recorded in the result archive and stamped on every emitted
  number.

**Operational definition** (so the 1% claim is falsifiable).

Per-metric conditions — each reported number gets its own ratio:

- **Latency p99** (the default signal):
  for all offered r ≤ 0.9 · r_max_loopback:
    `L_loopback_p99(r) ≤ 0.01 · L_target_p99(r)`
- **Throughput (req/s)**: tool overhead is the *gap* between
  offered and achieved rate under CO-free scheduling:
    `(r_offered − r_achieved_loopback) / r_offered ≤ 0.01`
  i.e., at loopback, the tool achieves ≥99% of offered rate.
- **Per-chunk SSE / per-message WS**: analogous per-event
  latency ratio using the protocol's natural op timestamp.

The emitted metric is a **map**:

```json
"tool_overhead": {
  "latency_p99_ratio": 0.004,
  "rate_gap_ratio": 0.008,
  "ws_rtt_p99_ratio": 0.006,
  "sse_event_gap_p99_ratio": 0.003
}
```

If any entry exceeds 0.01 (1%), `tool_influenced=true` is stamped
on the run and a terminal warning cites the offending metric.
Tool does not refuse — some legitimate targets (in-memory KV,
sub-µs) will always violate at the latency metric — but it
**labels every number** from such a run so downstream consumers
cannot silently compare them against properly-separated runs.

Corollary: `mio` + sync IO stays. Not because async is bad; because
a predictable, low-variance tool is easier to reason about than one
whose runtime scheduler contributes jitter. Users don't care about
mio vs tokio; they care that the tool doesn't move the number.

P10 is the *promise*. §9.6 is the *contract that enforces it* —
hard floors, CI gates, cross-tool validation, dependency-weight
budget.

### P11. Reproducibility before speed

A result that cannot be replayed is a story. Every run archives:

- `plan.json` — the compiled Plan (deterministic from source).
- `result.histlog` — HdrHistogram V2 compressed log (canonical raw data).
- `result.json` — full Summary with JSON-exported percentiles, error
  counters, metadata. `schema_version` field is mandatory.
- `machine.json` — CPU, kernel, NUMA, clock source, FD limit,
  transparent-hugepage state, cpufreq governor (see §8.2).
- `env.json` — relevant env vars, tool version, git commit (if
  target is local and detectable), start/end timestamps, resolved
  target IP set, TLS negotiated version + cipher (if applicable).
- `warmup.histlog` — warmup-phase histogram (kept for inspection,
  not in comparisons).

Archived to `$ZEROBENCH_HOME/runs/<plan_hash>/<UTC-ISO-timestamp>/`
by default. `--no-archive` opts out for truly ephemeral runs.

"Exactly" reproducibility is physically impossible (kernel, cpufreq,
NIC queue state vary). The success contract (§14.3) is weakened to:
"same plan + same target + comparable machine fingerprint → replay
reports within the configured CI of the archived run, or reports a
machine-fingerprint mismatch."

<!-- Added in round 1: addressing CRITICAL #11 — weakened reproducibility claim; added TLS negotiated info; added transparent-hugepage/cpufreq to machine fingerprint -->

### P12. Observability is first-class

Default output is never silent. Non-TTY default: one progress line
per second to stderr. TTY default: same line, updated in place.
JSONL (`--format jsonl`) is the programmatic interface with a
stable schema. The TUI (P13) is the rich view; both TUI and JSONL
are views on the single `LiveSnapshot` aggregator.

Progress percentage reflects **steady-state progress**: warmup shows
`warmup 8s/15s` then flips to `measure 5s/60s (8%)`. Progress never
counts warmup in the `(X% done)` figure.

If the target dies at t=0, the user sees "0 req/s · N connect errors"
within a second — not 30 seconds of silence followed by a confused
report.

### P13. The TUI is a signature feature

The TUI (`--tui`) is not "opt-in polish" — it is the interactive
debugging and demo interface, and earns sustained engineering
investment.

P10 (invisible tool) constrains *measurement overhead*, not UX. The
tool's CPU impact on the number must be <1%. The tool's visual
impact on the user can be substantial — deliberately so. A
benchmarker watching a 5-minute run needs to see *why* a spike
happened, not just that one did.

Scope commitments for v0.1.0:

- **Mode-aware layouts.** `measure` gets throughput + latency +
  errors with baseline-delta overlay. `curve` gets a 2D
  rate-vs-p99 scatter (ratatui Chart widget). `compare` gets
  split-screen, both targets live.
- **Causality visible, not hidden.** Drop the current tab system —
  error, latency, and throughput render on one screen so spike
  correlation is visible without tab-switching. Tabs become
  drill-downs, not primary navigation.
- **Baseline delta is a first-class panel.** If a baseline exists
  for `target + plan_hash`, every headline metric shows live delta
  (`p99 810µs  +3.2% vs baseline 2026-04-15`).
- **Persistent-session metrics render idiomatically.** SSE/WS show
  "subs held · chunks/s · chunk-gap p99" headlines, not "ops/s"
  shoehorning.

Budget: ~3K LOC in `zerobench-tui`, ongoing. Every new mode or
protocol ships with TUI support as a definition-of-done gate.

**Invariant: LiveSnapshot is the one source.** Every metric visible
in the TUI is present in JSONL under the same name. CI enforces
this (a schema-parity test). The TUI never drives schema evolution
— JSONL does.

<!-- Added 2026-04-18: user chose Path A (invest more) on TUI direction.
     TUI upgraded from opt-in view to signature feature. -->

---

## 4. Protocols as differently-shaped measurements

The central v0.1.0 design change.

### 4.1. Shape table

| | HTTP | SSE | WS |
|---|---|---|---|
| Load direction | client-driven | **server-driven** | bidirectional |
| Session shape | many short | few, long-lived | few, long-lived |
| Natural "op" | req/resp | chunk received | message exchange |
| Natural concurrency knob | pool size | subscribers held | connections held |
| Real user question | req/s + p99 | subs × chunk rate + chunk-gap p99 | conns × msg rate + RTT p99 |
| Equivalent tool | wrk2, bombardier | k6/xk6-sse, sse-bench | Artillery WS, Gatling WS |

### 4.2. HTTP — client-driven, short sessions

Op = one req/resp round-trip. Throughput metric = req/s.

Modes (Step variants):
- `rate("10k/s")` — open-loop constant rate. *Existing.*
- `saturate(c=300)` — closed-loop, N persistent workers. *Existing.*
- `ramp("1k..100k over 30s")` — linear ramp. *New; promotes
  `RateProfile::Ramp` into first-class CLI flag.*
- `handshake_rate(rate="1k/s")` — sustained new-connection rate
  with no keep-alive; one connection per request. Intent: measure
  the server's **handshake throughput** (accept + TCP + TLS + HTTP
  headers parse). Reported as connections/s achieved, handshake
  latency p50/p99, TLS resume rate (only meaningful when server
  issues session tickets — if
  `negotiated_stack.tls_session_tickets=false`, resume rate is
  omitted rather than reported as 0, since 0 would imply "no
  resumption happening" when really "resumption not available"),
  and the breakdown of time spent in each phase (SYN-ACK, TLS
  ClientHello→Finished, HTTP request→first byte). This is a
  **steady-state** handshake benchmark — the question "how fast
  can the server accept and handshake new connections?", which
  matters for connection-heavy workloads (serverless, short-lived
  clients). Renamed from `conn_churn` / `cold_connect` (earlier
  draft names) — those names implied either pool-cycling or
  one-shot cold-start, both misleading. `handshake_rate` is a
  sustained rate measurement of the handshake phase.
  *New Step: `HttpHandshakeRate`.*

<!-- Added in round 1: addressing MAJOR #12 — rename conn_churn → cold_connect, clarify this is a cold-start measurement, not steady-state -->

#### 4.2.1. H2/H3 concurrency — connections vs streams

HTTP/2 and HTTP/3 multiplex streams over a single connection. `-c N`
alone is ambiguous: 100 streams over 1 conn is a *very* different
server workload than 1 stream over 100 conns (different HPACK
state, different TCP/TLS handshake cost, different HOL-blocking
risk). We mirror **nghttp2's `h2load` flag shape**, which has been
the validated pattern for a decade:

- `--conn-concurrency N` — number of TCP/QUIC connections. Short
  alias `-c` (H1/H2/H3).
- `--stream-concurrency M` — max concurrent streams per connection
  (H2/H3 only; errors with a clear message for H1). Short alias `-m`.
- Total in-flight = `c × m`, bounded by the server's advertised
  `SETTINGS_MAX_CONCURRENT_STREAMS`.

Example shapes the split makes expressible:

```
# H2-stress — 1 conn, 100 streams. Exercises multiplexing, HPACK reuse.
zerobench measure -c 1 -m 100 --http-version h2 https://x

# H1-like — 100 conns, 1 stream each. Flat handshake cost, no HOL.
zerobench measure -c 100 -m 1 --http-version h2 https://x

# Browser profile — 6 conns, 30 streams. Real-world client shape.
zerobench measure -c 6 -m 30 --http-version h2 https://x
```

**Default for H2/H3**: `-m 100` when not specified. This is a
deliberate departure from h2load's `-m 1` default — h2load's default
makes H2 behave like H1 unless explicitly told otherwise, which
means most h2load users never exercise multiplexing and produce
misleading H2 numbers. A `-m 100` default matches browser-ish
behaviour and exercises the feature H2 is chosen for. `-m 1` remains
the explicit "simulate H1 semantics over H2 wire" opt-in.

**Interaction with server's `SETTINGS_MAX_CONCURRENT_STREAMS`**:
respect it. If requested `-m 200` but server advertises 50, we cap
at 50 per conn — we do **not** silently open more conns to
compensate (that would lie about server capacity). The cap and the
negotiated ceiling are recorded in the run metadata
(`negotiated_stack.h2_max_concurrent_streams_server = 50`) and
surfaced in the report header (`⚠ server capped m: 200 → 50`).

Per-conn and per-stream latency/error statistics are reported
separately in the JSON artifact; the terminal report headlines the
aggregate and shows per-conn/per-stream only in verbose mode.

<!-- Added 2026-04-19: resolves Q5 (H2 stream-concurrency as distinct mode). Mirrors h2load's -c/-m split, departs on default (-m 100 vs h2load's -m 1) to avoid misleading H2 benchmarks. -->

### 4.3. SSE — server-driven, long-lived sessions

Op = one chunk received (not one completed stream). Throughput
metric = chunks/s aggregated across all subscribers. Concurrency
metric = subscribers held.

**Chunk semantics**: zerobench parses the SSE wire format per
WHATWG HTML Living Standard §9.2 EventSource — tracks `event:`,
`id:`, `retry:`, and `data:` fields. An "op" is one **event**
(dispatched when a blank line terminates the event), not a raw
network chunk, because TCP may deliver one event across multiple
reads or pack multiple events into one read. `id:` propagation is
tracked for reconnect storm mode. This matches xk6-sse and
sse-bench behaviour.

Modes (Step variants):
- `sse_hold(n=1000, for="60s")` — open 1000 subscribers, keep them
  open 60s. Report: events/s, event-gap p99, concurrent-count
  stability, bytes/s, TTFB p99. **This is the real SSE question.**
  Works against any SSE server; no cooperation required.
  *Replaces current `SseStream`.*
- `sse_fanout(subs=1000, trigger="POST /emit", mode=...)` — 1000
  subs, server emits events on an external trigger, measure
  broadcast latency. Two submodes:
  - `mode="timestamp"` (preferred, requires server cooperation):
    event data contains `{"emit_ns": <monotonic_ns>, "seq": <n>,
    ...}`. zerobench ships a reference `zerobench-emit-server`
    implementing this one-line spec.
    **Clock-sync verification and protocol**: when SUT is local
    (resolved IP is loopback or matches a local NIC), zerobench
    uses its own monotonic clock — skew is zero by construction.
    When SUT is remote, zerobench runs a brief handshake at
    session start:
    - Calls a **clock-probe endpoint** (user-configurable via
      `--clock-probe-url PATH`, default
      `/__zerobench/echo_ns`).
    - The endpoint must accept POST and respond with a JSON body
      `{"emit_ns": <server_monotonic_ns>, "recv_ns":
      <server_monotonic_ns_at_request_receipt>}`.
    - zerobench records its own monotonic clock at send
      (`c_send_ns`) and receive (`c_recv_ns`); skew is estimated
      as the midpoint-offset (`server_emit_ns - (c_send_ns +
      c_recv_ns)/2`), assuming symmetric round-trip latency.
    - The reference `zerobench-emit-server` implements this
      endpoint. Users writing their own SUT-side cooperation
      implement the two-field response — a 3-line protocol,
      documented in `docs/specs/clock-probe.md`.

    **Thresholds scale with the measured round-trip latency**,
    not absolute values — a 10ms absolute threshold would refuse
    every transatlantic run (where one-way latency is naturally
    50–100ms). Policy: if `|skew| > 0.1 × RTT_p50`, zerobench
    warns and tags the result with `clock_skew_ns` and
    `clock_skew_to_rtt_ratio`; if `|skew| > RTT_p50`,
    timestamp-mode refuses without `--force-skew`. Both values
    always land in the artifact regardless. Silent trust is
    unacceptable for a measurement apparatus (P1); proportional
    thresholds keep the gate meaningful on WANs without being
    wrong on LANs.
  - `mode="trigger-rtt"` (fallback, no server cooperation):
    measures time from the HTTP POST returning 2xx to the first
    event arriving at subscriber, as a proxy for broadcast latency.
    The POST's own RTT is measured alongside and reported
    separately. **Subtraction caveat**: POST-RTT and subscriber-
    receive-time are on different sockets with different kernel
    paths; subtracting is *approximately* valid only when both
    paths are similar (same host, same NIC). Report labels the
    two quantities `trigger_post_rtt_p99` and `broadcast_proxy_
    p99` and does **not** emit a pre-computed subtraction —
    users who need it can compute themselves, with awareness of
    the approximation. §15 Q4 documents the bound.
- `sse_reconnect_storm(n=1000, kill_rate="10%/s")` — kill a
  fraction of subs per second, measure reconnect success and
  Last-Event-ID propagation. Requires WHATWG EventSource reconnect
  semantics; works against any compliant server.

The current `Step::SseStream` with `expect_chunks(100)` is removed.
It answered the wrong question.

<!-- Added in round 1: addressing CRITICAL #1 — fanout fallback mode for uncooperative servers -->
<!-- Added in round 2: addressing Missing Concepts — SSE event/chunk parsing spec; addressing MAJOR #9 — drop unevidenced "100µs" claim -->

### 4.4. WS — bidirectional, long-lived sessions

Op depends on the mode — we do not force a single "op" on WS.

Modes (Step variants):

- `ws_hold(n=10000, heartbeat="25s")` — idle capacity. Can the
  server hold 10K conns? Metric: conns-held, conn-drop rate,
  handshake-success rate, heartbeat-pong latency. Server-side
  RSS / FD usage is deliberately **not** reported here — the
  target may be remote, and zerobench does not pretend to read
  server process state. Heartbeat default is 25s, not
  30s: most WS proxies (nginx, envoy, ALB, Cloudflare) default
  idle timeouts to 60s, and many apps set 30s; 25s gives margin
  under every common default. Heartbeat is a native Ping frame
  (RFC 6455 §5.5.2) by default; `heartbeat_frame="text"` falls
  back to an application text-frame ping for servers that don't
  respond to Ping frames (uncommon but known: older Socket.IO
  versions, some reverse proxies). If configured heartbeat is ≥
  the empirically-observed conn-drop cliff, zerobench warns.
- `ws_echo_rtt(n=1000, msg_rate="100/s/conn",
  correlate="ping_pong" | "monotonic_id_prepend" | "payload_substring:XYZ" | "first_text_frame")` —
  client sends, waits for a specific echo back on the same
  connection, measures that round-trip. Correlation strategy is
  explicit:
  - `ping_pong` (**default, zero-intrusion**): client sends a
    WebSocket Ping frame (opcode 0x9) with a 16-byte monotonic
    id in the payload; server MUST reply with a Pong (opcode
    0xA) echoing the payload per RFC 6455 §5.5.3. Does not
    touch the application text-frame channel; works against any
    RFC-compliant server with no cooperation and no app-payload
    modification.
  - `monotonic_id_prepend`: client prepends a 16-byte monotonic
    id to the user-supplied text-frame payload; echo matched by
    prefix. Use when server echoes app-layer payloads verbatim
    AND the app-layer format tolerates a 16-byte prefix. Lint
    flags this mode as "payload-modifying".
  - `payload_substring:XYZ`: echo must contain the literal XYZ;
    used when server transforms payload but preserves a marker.
  - `first_text_frame`: any next text-frame from server counts,
    **but only if** no server-initiated frames can interleave
    (no heartbeats, no server-push). Enforced with a warning if
    heartbeats are configured.
  Default changed (round 3) from `monotonic_id` (payload-
  overwriting) to `ping_pong` because ping/pong at the frame
  level is zero-intrusion — works on any RFC-compliant server
  without risking breakage of structured app-layer payloads.
  Mirrors Artillery `match` and Gatling `reconciliate` prior
  art; JSON schema records `correlate_strategy`.
- `ws_server_push_rtt(n=1000, expected_rate="100/s/conn")` —
  server pushes messages unsolicited; client records chunk-gap
  and ordering. No client→server send. Measures server's ability
  to push to persistent subscribers. Analogous to SSE but with
  possible server-initiated backpressure.
- `ws_fanout(n=1000, trigger=..., mode=...)` — broadcast RTT
  analogous to SSE fanout. `trigger` is a callable that provokes
  the server broadcast: either an HTTP POST to a trigger URL, a
  Rhai-supplied function, or a dedicated "trigger connection"
  (one WS conn designated as the publisher; others as
  subscribers). Submodes mirror SSE fanout:
  - `mode="timestamp"`: trigger payload encodes `emit_ns`; each
    subscriber's first matching message measures
    `recv_ns - emit_ns`. Requires server cooperation (echo the
    `emit_ns`) or the dedicated-trigger-connection path (which
    zerobench drives itself and needs no cooperation).
  - `mode="trigger-rtt"`: time from trigger-POST 2xx to first
    message received at any subscriber. Fallback for
    uncooperative servers; exhibits the same accuracy caveats
    as SSE trigger-rtt.

**"RTT" disambiguation**: v0.0.1 used `WsRound` which conflated all
three. v0.1.0 forces the caller to pick. The JSON schema reports
`rtt_mode: "echo" | "server_push_gap" | "fanout_timestamp" |
"fanout_trigger_rtt"` so downstream consumers cannot confuse them.

The current `Step::WsRound` (handshake + 1 send + 1 recv + close)
is removed. Handshake-per-op dominates the signal and is not a real
workload.

<!-- Added in round 1: addressing CRITICAL #2 — split WS "RTT" into three precisely-defined modes; each names its prior-art equivalent -->
<!-- Added in round 2: addressing CRITICAL #4 — specify WS echo-match correlation strategies explicitly -->

### 4.5. Report unification

Unify the *shape* (comparable, diff-able) not the *numerator*:

Default (compact) form shows the minimum needed to identify the
run:

```
zerobench 0.1.0 · AMD EPYC 7763/64c/L6.12 · run_id 2026-04-18T14:23:05Z-4a9ce1b2-ef019a22
target         http://zeroship:5101 → 127.0.0.1:5101 (TLS1.3/AES128-GCM)
steady-state   180s (3×60s)   warmup 45s, cooldown 20s (excluded)

scenarios
  http-ping    HTTP  client-driven   1,397,617 req    p99=817µs    errors 0    keepup=ok
  sse-events   SSE   server-driven   1,000 subs · 250K events/s    event-gap p99=4ms    errors 0    keepup=ok
  ws-chat      WS echo-rtt           10,000 conns · 950K msgs/s    RTT p99=300µs        errors 0    keepup=ok
```

`--report-verbose` (or `-v`) expands identity fields and full
machine fingerprint:

```
zerobench 0.1.0 (features: h1, h2, h3, sse, ws, script)
machine        AMD EPYC 7763 · 64c · Linux 6.12 · NUMA 2 nodes · thp=madvise · gov=performance
plan_hash      sha256:4a9c…e1b2
url_fp         sha256:baba…f00d   (scheme+host+port+SNI+name+family)
target_fp      sha256:ef01…9a22   (plan × resolved IPs)
seed           0x4a9c (from plan_hash)
… etc.
```

The JSON/JSONL artifacts always contain the full set regardless
of display verbosity — terminal compactness does not affect the
machine-readable record.

The `steady-state` duration is the number users should compare —
warmup/cooldown are excluded from all histograms per §P8.
Previous drafts showed total-with-warmup at the top, misleadingly
implying warmup counted.

**Cross-protocol comparison** (§5 `compare`) does *not* produce a
scalar. It emits a table with one row per matched scenario pair,
diffing like-vs-like protocols. Scenario matching is:

1. By default, match by identical scenario name (plus protocol).
2. `--match "a:b,c:d"` overrides to map name-pairs explicitly
   (for intentional renames like `login-v2` → `login-v3`).
3. Mismatched protocols within a pair always fail hard:
   "cannot compare `http-ping@A` with `ws-chat@B` — different
   protocols."

This is Gatling's approach (report diffs are per-assertion, not
aggregated).

<!-- Added in round 1: addressing CRITICAL #3 — spec how compare works across protocols, refuse to produce a single scalar -->
<!-- Added in round 2: addressing MAJOR #14 — --match name-remap override; addressing MAJOR #6 — run_id introduction -->

---

## 5. Modes (user-facing verbs)

Seven verbs. Each answers a specific question. They do not overlap.

| Verb | Question answered | Who's the bottleneck-under-test? | Default duration | Default runs |
|------|-------------------|----------------------------------|------------------|--------------|
| `probe URL` | Does it work? (smoke test) | N/A | 5s | 1 |
| `calibrate` | What's **my client's** ceiling here? | client | ~30s ramp vs loopback | 1 |
| `measure URL` | Steady-state numbers at a given rate? | server (checked against calibrate) | 60s + 15s warmup + 10s cooldown between runs | 3 (CI) |
| `curve URL` | Where's **the server's** saturation knee? | server | ramp 2min | 1 |
| `compare URL1 URL2` | Is A faster than B? | server (both) | measure, **interleaved** round-robin with 10s cooldown every run (§5.3) | 3 each |
| `diff a.json b.json` | Regression vs saved baseline? | N/A (archival) | n/a | n/a |

`measure` is the new default. `probe` is what today's `zerobench URL`
does. `saturate` and `rate` become flags on `measure` / `probe`, not
top-level verbs.

**Dropped from v0.1.0**: `watch`. The original design had a
long-running verb that emitted JSONL per window and archived each
window as a separate run, with stop conditions (`--until
regression`, `--until p99<Xms sustained`). Rejected on scope:
continuous monitoring is what Prometheus/Grafana are for (§13.2
names `zerobench-prom-adapter` as the documented path), and
"rerun on schedule" is cron + `measure` + `compare` — three
standard Unix tools doing what they do. Reintroducing `watch`
later is cheap if users ask; it wasn't earning its complexity
(~300 LOC of verb + stop-condition DSL + per-window archive
rotation + compare-engine coupling) against the alternative.

### 5.1. `calibrate` vs `curve` — explicit disambiguation

Both ramp offered rate. They measure different things:

- **`calibrate`** targets **loopback** (the in-process echo from P5).
  Its answer is "how fast can this client, on this machine, push?"
  Output: the rate at which the client's scheduler falls behind.
  It has nothing to do with the server under test.
- **`curve`** targets the **real server**. Its answer is "where does
  this server's p99 double?" Output: a (rate, p99) curve and the
  knee.

**Manual vs implicit**: `calibrate` is both a user-invoked
subcommand *and* an implicit check at the start of every
`measure` / `curve` / `compare` / `soak` run. There is **no
cache** (see §15 Q3): fresh calibration every time, piggybacked
on warmup at near-zero net cost.

Flow:

1. `measure` (or sibling) starts. First 2–3s of warmup doubles
   as a loopback-echo self-check at the requested rate.
2. Sustained ≥99.0% of requested rate against loopback → proceed
   into the remainder of warmup + the measurement window.
3. Sustained <99.0% → refuse to run (§9.6.2); suggest `-r
   <achieved_ceiling>` or `--force-overload`.
4. User-invoked `zerobench calibrate` runs a longer, explicit
   ramp (~30s) to produce a full client-ceiling curve for
   reporting/archival purposes. This does **not** write a cache;
   the next `measure` still runs its own self-check.
5. `--no-calibrate` skips the implicit self-check entirely;
   result archive stamps `calibration: skipped`, which poisons
   any diff against a properly-calibrated baseline (analogous to
   `force_overload`).

Rationale for no cache: calibration is cheap (2–5s on warmup),
client-local, and every invalidation axis we'd add (tool
version, machine fingerprint, concurrency flags, CPU governor,
cgroup limits, thermal state) is a way for stale calibration to
lie silently. Since we always re-measure the remote (expensive)
target, caching the cheap client-side check is backward. Full
rationale in §15 Q3.

<!-- 2026-04-19: §5.1 rewritten per Q3 RESOLVED (no cache). Any review-loop
     pass that reintroduces a calibration cache here contradicts §15 Q3. -->
<!-- Added in round 1: addressing CRITICAL #4 — disambiguate calibrate vs curve. -->
<!-- Added in round 3: resolve manual-vs-implicit; superseded 2026-04-19 by Q3 resolution. -->

### 5.3. `compare` scheduling: interleaved, not serial

When comparing A vs B with `--runs 3`, zerobench executes the
sequence `A₁ → cooldown → B₁ → cooldown → A₂ → cooldown → B₂ →
…` (round-robin with 10s default cooldown between every run)
rather than all three A runs then all three B runs. Cooldown
between every run (not just between sides) is important because
TIME_WAIT sockets, SYN-retransmit timers, TCP congestion-window
caches, and DNS resolver caches all leak state between the
preceding run and the next run on the same kernel. Interleaving
without a cooldown would let A₁'s tail conditions influence B₁.

Rationale: round-robin interleaving minimises the chance that
system drift (other load, thermal throttling, kernel cache
warmth) correlates with the A-vs-B assignment. This matches best
practice from Netflix NDBench's A/B cycling and from SPEC CPU's
randomised-order recommendation.

The cost: `(2N-1) × cooldown` additional wall time. For N=3 runs
at 10s cooldown, that's 50s. The benefit: A and B see
statistically similar system conditions, reducing between-side
confounds.

Use `--compare-schedule=serial` to revert to sequential A-then-B
— useful when the two targets share stateful resources that
make quick context-switching infeasible (e.g., A and B are two
versions of the same DB and only one can be running at a time;
or when doing a blue/green pre-warmed vs cold comparison where
warm-state between calls matters). The cost: N consecutive A
runs exposed to the same local drift, same for B.

`--compare-cooldown 0` skips cooldowns (not recommended;
measurable TIME_WAIT contamination).

<!-- §5.2 / §5.4: `watch` verb dropped from v0.1.0 (2026-04-19).
     Continuous monitoring belongs in Prometheus + zerobench-prom-
     adapter (§13.2); "rerun on schedule" is cron + measure + compare.
     Reintroduction left as an open question if users ask. -->

<!-- Added in round 1: addressing MINOR #16 — obsolete after watch drop -->
<!-- Added in round 2: addressing CRITICAL #5 — obsolete after watch drop -->

---

## 6. Input formats

**Two front-ends, no middle tier.**

- **CLI flags** for simple cases (one URL, one method, one rate).
- **Rhai DSL** for everything else: multi-scenario, env-configured,
  chained extraction, conditional, looped.

No YAML/TOML scenario format. The industry shows "config + expression
language" always evolves into a DSL with worse ergonomics than a
real scripting language (Artillery YAML → JS flow, Gatling DSL). Rhai
is the right embedding (sandboxed, Rust-native, drops before
Phase 2); the problem isn't the DSL, it's our docs and feature
parity.

### 6.1. `.http` files — retained, not deprecated

Reviewer correction: dropping the `.http` parser was the wrong call.
`.http` / IntelliJ HTTP Client / VSCode REST Client syntax is a
widely-used **version-controlled artifact format**, and users replay
them as benchmarks. We keep the parser.

What we **do** deprecate:

- `weights.toml` sidecar for request-file directories. Weights move
  into Rhai (`scenario("x").rate("…")`). If you only have `.http`
  files, they default to equal-weight.
- the original `.http`-only "run-a-folder" mode that lacked
  per-request configurability beyond what `.http` syntax provides.

What we **add**:

- `--from-curl 'curl ...'` parses one-shot curl invocations into
  a single-scenario Plan, for copy-paste-from-browser-devtools
  ergonomics.

### 6.2. Precedence when a `.http` directive conflicts with CLI/Rhai

Order (later wins):

1. `.http` request-body and inline directives (`@rate = 10k/s`,
   `@timeout = 5s`).
2. Rhai script overrides for a named scenario (`scenario("foo")
   .rate("20k/s")`).
3. CLI flags (`--rate 30k/s`).

Conflicts are reported at plan-compile time with the resolved
value: "rate(10k/s → 20k/s → 30k/s) from .http, rhai, cli". Users
can always see what won.

**Negation flags** (`--no-archive`, `--no-keepalive`, etc.):
negation at level N *clears* the value from levels <N and is
propagated through to the final plan; a positive value at level
N+1 can re-enable. Example: `.http` sets `@archive = true`, Rhai
passes `--no-archive` equivalent, CLI passes `--archive` → the
final value is true. Order-of-operations is the same as positive
flags (later wins).

<!-- Added in round 1: addressing CRITICAL #5 — restore .http parser, clarify what's actually deprecated -->
<!-- Added in round 2: addressing MAJOR #10 — specify .http directive precedence -->

---

## 7. Rhai gap closure

Rhai stays. v0.1.0 closes five gaps:

1. **CLI ↔ Rhai feature parity.** Every CLI flag that makes sense
   per-request has a Rhai builder method. Audit produces a
   checklist (see `design-v0.1.0.md`).
2. **Pattern library.** `examples/` covers 10 canonical patterns:
   login-then-action, weighted mix, env-configured, rate ramp,
   response-chained extraction, multi-endpoint, conditional
   scenario, SSE hold, WS echo-rtt, fanout.
3. **`--explain`.** Dumps the compiled Plan as readable text.
   Debugging "why doesn't my scenario fire" becomes trivial.
4. **SLO-gate assertions.** Precisely specified (§7.1).
5. **`lint` subcommand.** `zerobench lint foo.rhai` compiles
   without running, surfaces unknown methods, missing `rate`, unused
   variables, unreachable code, **and flags v0.0.1 forms that have
   been renamed** (e.g. `.expect_chunks()` → rewrite hint for
   `sse_hold(...)`). `lint --format=json` emits structured
   diagnostics (one object per finding with `severity`, `rule`,
   `file`, `line`, `col`, `message`, `suggestion`) — pipeable
   into migration scripts, editor integrations (LSP bridge
   planned), and CI linters. `lint --format=rewrite-hints` emits
   a diff-style patch file usable as input to `patch(1)`.

### 7.1. SLO assertion grammar

Assertions target the post-warmup steady-state histogram, not the
whole run. All durations/rates are human-parsed ("5ms", "0.01%").

- `expect_p99(under: "5ms")` — steady-state p99 latency.
- `expect_p_n(n: 99.9, under: "20ms")` — arbitrary percentile.
  `n` is clamped to the HDR histogram's native precision (3
  significant digits → p99.9 is native; p99.95 rounds to p99.9
  with a lint warning "n=99.95 clamped to 99.9 per HDR
  resolution"). Unsupported extreme precisions (beyond p99.99
  for typical HDR configuration) are rejected at plan-compile
  time.
- `expect_error_rate(below: "0.01%")` — (errors / total) across
  all categories. `expect_error_rate(category: "http_status_5xx",
  below: "0.001%")` for per-category. **Available categories are
  exactly those listed in §9.2's `errors` object**; `zerobench
  lint` flags typos.
- `expect_steady_state(metric: "throughput_rate", window: "10s",
  cv_below: 0.10, after: "15s")` — rolling window CV (stddev /
  mean) of the given metric must be below the threshold starting
  `after` seconds into the steady-state phase. Default metric is
  `throughput_rate` (with a CV default of 0.10, i.e. 10%). For
  `p99_latency_ms` the default CV is 0.25 (25%) — tail latency is
  intrinsically noisier than throughput under GC/runtime jitter.
  **No cross-metric default**; users must pick the metric they
  care about. Named metrics available: `throughput_rate`,
  `p50_latency_ms`, `p99_latency_ms`, `p999_latency_ms`,
  `error_rate`. We use **coefficient of variation (CV)**, not raw
  variance, because CV is scale-invariant (5% CV reads the same on
  1k/s and 1M/s workloads).
- `expect_rate(at_least: "40k/s", window: "10s")` — steady-state
  throughput **sustained over a rolling window**. Default window
  is the full steady-state duration. `window` shorter than the
  full duration asserts the rate never dipped below the threshold
  for any `window`-sized slice — useful for "stays above X even
  during GC pauses" gates.
- `expect_keepup(max_level: "warn" | "ok")` — scheduler never
  reached the given severity. Default `max_level="ok"` means no
  `keepup=warn` and no `keepup=fail` — strictest. `max_level=
  "warn"` tolerates warns but fails on `keepup=fail`. Defaults
  to on for rate-based modes; disable via `expect_keepup(skip:
  true)` if you know your client is undersized.

Each assertion failing flips exit code non-zero and is listed in
stdout and `result.json.assertions[]`. Pass/fail is deterministic
from the archived `result.json`.

<!-- Added in round 1: addressing MAJOR #13 — specify expect_steady_state precisely; introduce CV; cross-reference P6 -->
<!-- Added in round 2: addressing CRITICAL #3 — more realistic default CV per-metric; addressing MAJOR #13 — list available categories; addressing MAJOR #13 — remove implicit cross-metric default -->

---

## 8. Reproducibility contract

### 8.1. Archive layout

Every non-`--no-archive` run writes to
`$ZEROBENCH_HOME/runs/<url_fingerprint>/<run_id>/`:

```
$ZEROBENCH_HOME/runs/
  <url_fingerprint>/           # scheme+host+port+SNI+name+family (stable across DNS)
    <run_id>/                  # UTC-ISO-ts + plan_hash[:8] + target_fp[:8]
      plan.json                # deterministic compile of source
      result.json              # full Summary + assertions + metadata
      result.histlog           # HdrHistogram V2 compressed log (canonical)
      warmup.histlog           # warmup-phase histogram
      machine.json             # full machine fingerprint (§8.3)
      env.json                 # tool version + flags + context + timestamps + TLS + resolved IPs
      stdout.txt, stderr.txt
      INDEX.json               # schema_versions + grouping metadata + replayed_from
```

**Every JSON artifact carries `schema_version`** (independent per
file). `INDEX.json` has a top-level `schema_versions` block
enumerating the version of each sibling artifact:

```json
{
  "schema_versions": {
    "result": 2, "plan": 1, "machine": 1, "env": 1, "index": 1
  },
  "plan_hash": "sha256:4a9c…",
  "target_fingerprint": "sha256:ef01…",
  "url_fingerprint": "sha256:baba…",
  "replayed_from": null
}
```

Readers must reject unknown major versions and accept unknown
minor versions (additive fields only). Files evolve independently.

`url_fingerprint` is the **stable grouping key** (scheme+host+
port+SNI, no resolved IPs): all runs against the same service-as-
URL land together, enabling "show me the last 30 runs of my staging
endpoint" even when DNS rotates between runs.

`target_fingerprint` (in `INDEX.json`) separately records the
resolved IP set at run time; `diff` and `replay` warn but do not
refuse when two runs' IPs differ — the warning surfaces
load-balancer rotations so operators can interpret noise.

`run_id` is `<UTC-ISO-timestamp>-<plan_hash[:8]>-<target_fp[:8]>`,
globally unique and copy-pasteable. `replay <run_id>` is
unambiguous.

### 8.2. Canonicalisation — `plan_hash` and `target_fingerprint`

**`plan_hash`** is sha256 of the plan serialised via **RFC 8785 JSON
Canonicalization Scheme (JCS)**. No ambiguity about key order, number
format, or whitespace. Tool version is **not** in the hash — two
versions of zerobench producing the same Plan from the same source
must hash identically. This enables tool-version upgrades without
breaking archive groupings.

**`url_fingerprint`** = sha256 of `{scheme, host, port, SNI,
plan_name_tag, ip_family}`. `plan_name_tag` is `plan.name` — a
required field if archiving is enabled — **not** derived from the
filename. Relying on the filename was rejected in round 4 because
renaming a script silently splits the archive, a subtle trap.

- If a Rhai script omits `plan.name` and archiving is on,
  compilation fails: "archived runs require `plan.name`; set it
  or pass `--no-archive`". For CLI-only runs, `--name NAME` is
  required when archive is on; a heuristic like
  `{scheme}-{host}-{method}-{path-slug}` is suggested in the
  error but never defaulted silently.
- `ip_family` is included so that v4 and v6 runs against the same
  URL do not share an archive bucket (previously an omission).

This matches NDBench's "workload identity" approach rather than raw
URL grouping, and is explicit rather than magic.

**`target_fingerprint`** = sha256 of `{scheme, host, resolved_IPs
(sorted), port, SNI, plan_hash}`. Includes the full plan because
the *exact* benchmark against an *exact* backend is the finest
grain of identity.

Two-level fingerprinting separates "same workload against same
service" (url_fp) from "same workload against same backend" (target_fp).

### 8.3. Machine fingerprint (`machine.json`)

Full set, stable format:

```json
{
  "cpu_model": "AMD EPYC 7763",
  "cpu_cores_logical": 128,
  "cpu_cores_physical": 64,
  "cpu_base_ghz": 2.45,
  "cpu_flags": ["avx2", "sse4_2", ...],
  "numa_nodes": 2,
  "numa_worker_binding": "round_robin",
  "kernel": "Linux 6.12.0-rc3",
  "libc": "glibc 2.39",
  "total_ram_gib": 512,
  "clock_source": "tsc",
  "clock_monotonic_ns_resolution": 1,
  "cpufreq_governor": "performance",
  "transparent_hugepage": "madvise",
  "fd_limit_soft": 1048576,
  "fd_limit_hard": 1048576,
  "fd_limit_raised_by_tool": false,
  "containerized": true,
  "cgroup_version": "v2",
  "cgroup_cpu_quota_cores": 8.0,
  "cgroup_mem_limit_gib": 32,
  "_note": "quota/mem fields use null for 'unlimited'; omit if cgroup_version is null",
  "so_rcvbuf_default": 212992,
  "so_sndbuf_default": 212992,
  "ulimits": {...},
  "nic_driver": "ena",
  "nic_link_speed_gbps": 100,
  "hostname_blake3": "b3:0fe8..."
}
```

Hostname hashed via Blake3 (not sha256 — Blake3 has no practical
rainbow-table attack surface given the length of realistic hostnames,
and we use it only for equality-matching, not for any security
purpose). `--expose-hostname` can emit plaintext if the user wants.

`clock_monotonic_ns_resolution` matters because if the kernel's
monotonic clock is 1µs-resolution (some hypervisors), every latency
measurement is already bucketed at 1µs — any "sub-µs" percentile
reported would be a quantisation artefact. **If resolution is
>10µs** (bad hypervisor, nested virt), zerobench refuses to run by
default with a clear error; `--allow-coarse-clock` forces a run,
tagging every output with `clock_coarse=true`.

`fd_limit_raised_by_tool` records whether zerobench itself called
`setrlimit(RLIMIT_NOFILE)` to raise the soft limit at startup. The
tool attempts to raise soft up to hard when the plan's concurrency
needs more FDs than the current soft limit; if hard is also too
low, it fails fast with a clear error ("ulimit -n is H; plan needs
C; raise hard limit or reduce concurrency"). Users can audit the
pre-raise value via `env.json.fd_limit_soft_initial`.

macOS note: `nic_driver` is Linux-specific; on macOS we report
`nic_driver: "darwin-bsd"`, `nic_model` from SPNetworkDataType, and
`nic_link_speed_gbps` where available. Fields never-applicable on a
platform are omitted entirely rather than filled with nulls — the
JSON schema marks them as "present iff platform supports".

<!-- Added in round 1: addressing CRITICAL #14 — target IP in fingerprint; addressing MINOR #17 — Blake3 over sha256 for hostname; addressing MAJOR #9 — JCS canonicalisation; addressing Missing Concepts — clock skew/resolution -->
<!-- Added in round 2: addressing MAJOR #11 — refuse/warn on coarse clock; addressing MINOR #16 — macOS NIC fallback -->

### 8.4. Replay

`zerobench replay <run_id>` re-executes the plan against the
original target (if reachable), produces a fresh result, and auto-
diffs against the archived one. `run_id` is globally unique (§8.1).
Exit code: 0 if within the saved run's 95% CI; 1 if outside; 2 if
the replay could not run (target unreachable, machine fingerprint
mismatch without `--force`).

**Replay provenance**: the new run's `INDEX.json` sets
`replayed_from: <original_run_id>` and carries the delta-vs-
original in `replay_diff.json`. CI and downstream tooling
identify "this is a replay, not a fresh run" by presence of
`replayed_from`; fresh runs omit this field (or set it to null).
This makes the criterion 3 test mechanically checkable: a replay
artifact is unambiguous.

**External-file dependencies**: plans that reference external
files (`body.from_file(...)`, `body.from_file_streamed(...)`)
are recorded in `plan.json.external_files[]` with their
**content hashes** at plan-compile time. On replay:
- If the file exists and its hash matches: proceed normally.
- If the file exists and the hash mismatches: refuse by
  default; `--accept-external-drift` proceeds with a warning
  tagged in the result.
- If the file is missing: refuse; `--skip-missing-external`
  substitutes empty bodies and tags the result.

This keeps "reproducibility" honest — the plan alone isn't the
whole specification when external data is referenced; the data
is part of the experiment.

Machine fingerprint mismatch default: warn and refuse. `--force`
runs anyway but prefixes the new archive with `XFINGERPRINT-` for
clarity. DNS-rotation-induced `target_fingerprint` mismatch is
warn-only (not refuse), since it is a real operational signal —
the user can still choose to abort.

<!-- Added in round 1: addressing CRITICAL #11 — replay does not claim "exact", uses CI overlap -->
<!-- Added in round 2: addressing MAJOR #6 — replay takes run_id, not plan_hash -->

---

## 9. Observability contract

### 9.1. Default output (non-TUI)

Non-TTY: one line per second to stderr. Time-duration fields on
stdout/stderr are human-rendered (ms/µs/s) for readability; raw
ns appears only in JSONL/JSON artifacts.
```
[ 5s/60s]  rate 48,221/s  p99 810µs  errors 0  keepup=ok   (8% done)
```

TTY: same line, updated in place (CR + clear-to-EOL, no ANSI
needed).

Progress pct is `steady_state_elapsed / steady_state_total`. During
warmup, replace `(X% done)` with `(warmup 8s/15s)`.

**Update cadence** scales with duration: runs ≤ 30s emit at 500ms
cadence (so a 10s probe gets 20 updates); runs 30s-5min emit at
1s; runs >5min emit at 5s. This keeps the progress stream
informative without flooding the terminal on short runs or
pointless chatter on long runs. Override via `--progress-every
DURATION`.

### 9.2. JSONL schema

Stable, versioned. Breaking changes bump `schema_version` and an
old-schema reader becomes available via `--read-schema=N`.

All time units are **nanoseconds** consistently — no mixing of
`_ms` and `_ns` across fields (a v0.0.1 schema wart). Rates are
per-second, integer.

```json
{
  "schema_version": 2,
  "run_id": "2026-04-18T14:23:05Z-4a9ce1b2-ef01baba",
  "t_ns": 5000000000,
  "scenario": "ping",
  "protocol": "http",
  "phase": "measure",
  "rate": 48221,
  "p50_ns": 112000,
  "p90_ns": 240000,
  "p99_ns": 810000,
  "p999_ns": 4200000,
  "p9999_ns": 12000000,
  "max_ns": 18000000,
  "errors": {
    "connect_refused": 0,
    "connect_timeout": 0,
    "connect_reset": 0,
    "read_timeout": 0,
    "read_reset": 0,
    "write_timeout": 0,
    "tls_handshake": 0,
    "http_status_4xx": 0,
    "http_status_5xx": 0,
    "protocol": 0
  },
  "concurrent_sessions": 1,
  "bytes_in": 482213,
  "bytes_out": 240018,
  "keepup": "ok"
}
```

`errors` keys are **exhaustive and stable** — every category is
emitted even if zero. Readers can unambiguously distinguish
`connect_refused` (server not listening) from `connect_timeout`
(network drop) from `tls_handshake` (cert mismatch, protocol
downgrade).

**Per-scenario vs aggregate**: each JSONL record is **per-
scenario** — the `scenario` field distinguishes them. There is
no aggregate "ops/s across protocols" record; §P7 rules that
out. The rolled-up final report (§4.5) displays per-scenario rows
but never sums across them. Parsers that want a total within a
protocol (e.g. sum of HTTP req/s across HTTP scenarios) must
group-by `protocol` and sum themselves — zerobench deliberately
does not emit such sums, because their meaning is workload-
specific.

<!-- Added in round 1: addressing Missing Concepts — failure taxonomy; addressing MAJOR #18 — per-category error counters -->

### 9.3. Statistical comparison (the `diff` / `compare` engine)

When comparing two runs (or a run to its baseline), zerobench
reports deltas with statistical uncertainty. **Resampling is
run-level, not observation-level** — HDR histograms aggregate
counts into buckets, so resampling "observations" from a histogram
is not equivalent to resampling real observations, and would be
statistically unsound.

Three strategies, pick via `--compare-strategy` (default chosen by N):

- `run-bootstrap` (**default when both sides have N ≥ 3 runs**):
  **percentile bootstrap** with 10,000 resamples of the N run-
  level p99 values (and other percentiles) on each side. Report
  the [2.5%, 97.5%] empirical percentiles of the delta
  distribution as the 95% CI. Used in RCT-style benchmarking
  literature (bootstrap over experimental units). **We prefer
  percentile over BCa** at small N (3-10) because BCa's
  jackknife-based acceleration correction has poor finite-sample
  properties at N<10 — the complexity cost is not justified. A
  followup ADR will revisit if users routinely run N≥30. Users
  who want BCa explicitly: `--bootstrap-method=bca`.
- `ad-distribution` (**default when N=1 on either side**): two-
  sample **Anderson–Darling** test on the empirical CDFs implied
  by the HDR histograms (bucket mid-points weighted by counts).
  AD weights the distribution tails more heavily than KS (via the
  `1/[F(x)(1-F(x))]` weighting function) and is demonstrably
  better at detecting tail shifts on heavy-tailed data — exactly
  the regime latency distributions live in. The AD statistic is
  valid on histogram-encoded data because the ECDF is fully
  determined by the bucket counts. Reports AD A² statistic,
  p-value from the Scholz-Stephens asymptotic table, and raw
  per-percentile deltas (p50, p90, p99, p99.9, p99.99) with no CI,
  labelled "N=1 per side — no CI available". KS is offered as
  `--compare-strategy=ks-distribution` for users comparing to
  KS-based prior-art baselines; the AD default is zerobench's
  opinion because tail sensitivity is the product (P3).
- `min-of-n` (opt-in, Netflix NDBench pattern): report
  `min(p99_B_runs) − min(p99_A_runs)` as a **conservative
  regression bound**, not an estimator. Interpretation: if
  negative, B is at least this much better than A (ignoring
  noise); if positive, B is at worst this much worse. No CI.
  Output is labelled `bound=true; not a statistical estimate`.

Output (run-bootstrap, N≥3): "Δp99 = +120µs (+14.8%, 95% CI
[+95µs, +147µs], N=3×3 runs, method=run-bootstrap-percentile)". If CI
crosses zero, reported as "no significant change at 95%". No
"p<0.001" headline — we show the CI, not a p-value, to avoid
pseudo-precision.

**Multiple comparisons**: when diffing a set of percentiles
{p50, p90, p99, p99.9, p99.99} simultaneously, p-values (for
distribution-test strategies) apply a **Holm-Bonferroni step-
down correction** rather than naive Bonferroni. Holm is
uniformly more powerful than Bonferroni and still controls
family-wise error rate. For CIs (run-bootstrap), the 95% CI
per-percentile is **joint** only when `--joint-ci` is passed
(uses Bonferroni-adjusted individual levels so the union has
95% coverage). The default is per-percentile marginal CIs,
which is what readers expect, with a legend note: "CIs are
marginal per percentile; use --joint-ci for family-wise 95%
coverage across all reported percentiles."

**`--regress-on` with multiple thresholds**: when multiple
thresholds are specified (e.g.,
`p99:+5%,p999:+10%,error_rate:+0.01%`), each is **evaluated
independently** — family-wise correction does **not** apply.
Rationale: `--regress-on` is a *configured gate*, not a
hypothesis test; the user has decided what matters and the
false-positive rate is intentional (they want any of those
listed to trigger). Family-wise correction would delay real
regressions. Users who want conjunctive gating (all thresholds
must cross) use `--regress-on-all` (opposite polarity).

Output (ad-distribution, N=1): "Δp99 = +120µs raw (no CI, N=1);
AD A²=3.41 (p=0.002) — distributions differ significantly in the
tail. Use --runs N for per-percentile CIs."

`probe → diff` interaction: `probe` runs once. `diff probe.json
baseline.json` auto-selects `ad-distribution` and warns that N=1
is weaker than `measure --runs 3`.

Regression thresholds: `--regress-on` takes a comma list like
`p99:+5%,p999:+10%,error_rate:+0.01%`. Crossing semantics vary by
strategy:
- `run-bootstrap`: crossed iff the **lower bound of the 95% CI on
  the delta** exceeds the threshold. Prevents false positives
  from noisy runs.
- `ad-distribution` / `ks-distribution`: crossed iff the raw
  delta exceeds the threshold **AND** the distribution test
  rejects equal distributions at p<0.05. Conjunction reduces
  false positives with N=1; users are warned these have less
  power than run-bootstrap.
- `min-of-n`: crossed iff the conservative regression bound
  exceeds the threshold (i.e., even the best-case-for-A, worst-
  case-for-B estimate is worse than the threshold).

Exit 0 if no threshold is crossed, exit 1 if any is.

<!-- Added in round 1: addressing Missing Concepts — statistical significance; addressing MAJOR #10 — bootstrap CI method; credit Gatling + Netflix -->
<!-- Added in round 2: addressing CRITICAL #2 — fix statistically-wrong observation-level bootstrap; use run-level resampling as default and permutation test for single-run; explicit about min-of-n being a bound not an estimator -->
<!-- Added in round 3: addressing CRITICAL #1 — bucket permutation is mathematically unsound; replace with KS test on ECDFs (valid on histogram data); BCa correction; explicit threshold-crossing per strategy; probe→diff explicit transition -->

### 9.3.1. JSONL backpressure policy

A slow JSONL consumer must not affect measurement. Policy:

- JSONL is written to a **bounded ring buffer** (default 1024
  records, ~1MB). If the buffer fills because the consumer is
  slow, **oldest records are dropped**, with a
  `jsonl_dropped_count` counter incremented in subsequent
  records and in the final `result.json.meta.jsonl_dropped`.
- A terminal warning is printed when the first record is
  dropped: "JSONL consumer slow; dropped N records — use
  `--jsonl-buffer-size` to increase or fix the consumer."
- The **stdout progress line** is synchronous (no buffer); a
  slow terminal blocks no longer than one second (the cadence)
  because the line is rewritten not appended.
- **HDR histograms in-memory are never dropped** — the
  canonical final report is unaffected by JSONL back-pressure.

Rationale: preserving measurement fidelity is higher priority
than streaming completeness. Downstream consumers who cannot
drop records can set `--jsonl-file PATH` and read from the file
after the run; file writes don't bound-drop.

### 9.3.2. Schema-stability commitment (public contract for adapters)

The JSONL schema (§9.2) is the public contract with adapter
authors and external tools. Commitments:

- **Minor version bumps** (2.x → 2.(x+1)) are **additive-only**:
  new fields may appear, existing fields never change meaning
  or type. Readers built for 2.x keep working on 2.(x+1) data.
- **Major version bumps** (2.x → 3.0) may break compatibility;
  tool announces in release notes one minor version ahead.
- `--read-schema=N` shim is retained for **two subsequent major
  versions** after a break (so v3 reads v1/v2; v4 reads v2/v3;
  v5 refuses v1). Deprecation warnings appear one major
  version before removal.

Adapter authors (Prometheus, OTel, custom) can pin to a major
version and get automatic additive improvements. Commitments
are CI-enforced: the schema is frozen in
`schemas/jsonl-result-v2.json` (JSON Schema draft 2020-12) and
`cargo test -p zerobench-schema` uses `jsonschema-rs` to
validate generated records + runs a `git diff`-based check that
rejects PRs which remove or retype fields without a major
version bump. The schema file is versioned in-tree; adapter
authors can `git clone` it as their contract document.

### 9.4. TUI

Unchanged feature-gated rich view. Calls the same
`LiveSnapshot` that feeds JSONL — one aggregator, two consumers.

---

## 9.5. Cross-cutting workload concerns

These concerns apply to every mode; specified once here rather than
repeated per-protocol.

### 9.5.1. Request body generation

Rhai provides a `body` helper for data variation:

- `body.template("...{{seq}}...")` — sequential substitution.
- `body.faker("email" | "uuid" | "name" | ...)` — standard fakers
  (mirrors k6 `faker.js`, Gatling `Feeder`).
- `body.from_file("data.ndjson")` — one line per request;
  round-robin or random via `body.from_file(..., strategy="random")`.
- `body.json({...})` — literal JSON with variable interpolation.

Bodies are **generated per-request in the hot path** (not
pre-generated at plan compile) to avoid memory blowup on long runs,
but template compilation is cached.

**Cost honesty**: the P10 <1% overhead envelope applies to the
scheduler + I/O + histogram path. Body-generation cost is
**separate** and reported as
`machine.json.body_generation_cost_ns_p50` / `p99`. If
body-generation cost exceeds 10% of measured latency at the p50,
the tool warns: "body generation is a non-trivial fraction of
measured latency; numbers reflect tool+server, not just server."
This is an honest disclosure rather than the unsubstantiated
earlier claim that body generation always folds into <1%.

**Determinism**: `body.faker`, random selection, and any other
stochastic source derive from a seeded PRNG. Seed defaults to
`plan_hash` alone — **every run of the same plan uses the same
seed by default**, making runs bit-identical in their stochastic
choices. For users who want independent randomisation across
runs (e.g., to avoid cache-line coincidences, or to avoid
reusing the same generated UUIDs that may conflict with unique
constraints in the SUT), `--seed random` draws a fresh seed per
run and records it. Setting `--seed <u64>` explicitly fixes the
seed. Seeds are recorded in `env.json.seed` and
`result.json.meta.seed`; replay uses the archived seed unless
`--new-seed` is passed.

**Insert-heavy workload footgun**: seed=plan_hash means two runs
against the same database will attempt the same unique inserts.
For insert-heavy benchmarks, use `--seed random` (fresh per run)
and record it — you lose bit-identical reproducibility but avoid
primary-key collisions. This trade-off is explicit rather than
hidden. Alternatively, a `body.faker("uuid", unique=true)`
helper opts into a two-source mix: deterministic up to a per-run
**nonce derived from `hash(plan_hash || run_start_unix_ns ||
hostname_blake3)`** by default. The hostname component is
excludable via `body.faker("uuid", unique="cross_machine")`
which omits hostname from the nonce — enabling two runs on
different machines to produce identical unique UUIDs (useful for
coordinated distributed tests). The nonce is recorded in
`env.json.body_nonce` (and its formula in
`env.json.body_nonce_formula`) so replay can pin it.

Without seed control, "reproducible" claims in P11 would be
vacuous for scenarios using random bodies.

**Large bodies**: bodies up to 100 MiB are supported in-memory.
For larger, use `body.from_file_streamed("huge.bin")` which
mmap-reads chunks; `chunk_size` configurable (default 64 KiB).
Before the measurement phase, the tool issues
`posix_madvise(MADV_WILLNEED)` over the file range to prefetch
pages, and (on Linux) `readahead(2)` to force the kernel to
page-in. On warmup end, a `madvise(MADV_SEQUENTIAL)` hint
reduces cache pollution. These hints ensure page faults do not
randomly appear on the hot path and contaminate latency.
Streaming bodies use **HTTP/1.1 chunked transfer encoding**
(RFC 7230 §4.1) — which is explicitly designed for keep-alive
pipelining. Keep-alive remains enabled. Earlier drafts that
disabled keep-alive for streamed bodies contradicted the RFC and
are corrected here. HTTP/2 uses `DATA` frames which are
inherently streamable; same applies.

### 9.5.2. Cookie jar

Real web workloads need session cookies. zerobench supports:

- **No-jar** (default): cookies are ignored; each request is
  isolated. Fastest; matches wrk/bombardier.
- **Per-connection jar**: cookies set on a connection persist for
  its lifetime; RFC 6265-compliant. Enabled via `--cookie-jar
  connection`.
- **Per-scenario jar**: cookies shared across all virtual users
  in a scenario (simulates one user session). `--cookie-jar
  scenario`.
- **Explicit via Rhai**: `jar.set("name", "value")`,
  `jar.clear()` for precise control.

Jars respect `Secure`, `HttpOnly`, domain/path scoping, and
Max-Age/Expires. `SameSite` is honored but cross-site requests
are rare in a single-scenario benchmark.

### 9.5.3. Authentication lifecycle

Real benchmarks need auth tokens that may expire mid-run. zerobench
supports three patterns:

- **Static token**: `--bearer TOKEN`, `--basic-auth U:P`,
  `-H "Authorization: ..."`. No refresh.
- **Scenario-prelude**: Rhai scenario starts with a `login` step
  that captures a token and stores it in a scenario-local var.
  Refreshed once per scenario invocation.
- **Refresh hook**: `on_401(fn() { ... })` — on receiving a 401,
  invoke the hook to obtain a new token and retry the request
  once. **If the retry also returns 401, the second 401 is
  treated as a permanent auth failure** (counted in
  `errors.auth_permanent_failure`) — no further refresh attempts
  for that request-chain, avoiding infinite refresh loops
  against a misconfigured SUT. The hook's own return rate is
  tracked in `auth_refresh_failure_rate` so deployments where
  refresh itself flakes are visible.

**Retry latency accounting** (the real question): a 401 → refresh →
retry sequence produces real wall-clock latency that the user
should see.

**Canonical data model** (single source of truth, multiple views):
every attempt is recorded as an observation with three fields:
`seq_id` (monotonic per-chain), `phase` (`first | refresh |
retry`), `latency_ns`. Histograms are **views** over this log.
Reports emit three named views from the same canonical data:

- `latency_main` — first attempts only (what most benchmarks
  report; comparable to k6/Artillery defaults).
- `latency_user_visible` — sum of all phases within a chain (what
  a real user experiences end-to-end). For chains with no
  refresh, this reduces identically to the first-attempt latency
  — the view degrades gracefully so chains without auth churn
  are not artificially separated from chains with.
- `latency_auth_overhead` — just the `refresh` phases (auth churn
  cost isolated). Empty for chains with no refresh; these chains
  contribute nothing to the overhead histogram.

The JSON schema emits all three; users choose which to compare on
via `--compare-view latency_main|user_visible|auth_overhead`.
Default comparison view is `user_visible` because that is what the
name "latency" in a benchmark report normally connotes to an
end-user audience. `--compare-view latency_main` selects the
"comparable to k6" view.

Rationale: a single canonical log with multiple named views
prevents the documented-two-policies trap where different runs
might have been measured with different policies and
produce non-comparable numbers. The canonical log is identical
across runs; only the *view used to compare* varies.

`errors.auth_refresh` counts the number of chains that
experienced a refresh (a churn signal, not an error).

### 9.5.4. Rate-limit handling (429 / Retry-After)

429 with `Retry-After` is a first-class signal, not an error:

- By default, 429 responses are counted in `errors.http_status_4xx`
  AND also in `rate_limited` (a separate counter). Latency still
  recorded.
- `--respect-retry-after` honours `Retry-After` by delaying the
  next request on that connection by the indicated duration,
  trimming offered rate accordingly. Rate-budget bookkeeping tracks
  how much was trimmed.
- `--treat-429-as-error` (opt-out) reverts to strict failure
  counting for traditional SLA-gate use.

The default is "count 429 and keep sending" because a rate-
limiting SUT is itself information — you want to see how the limit
behaves under load, not avoid it.

### 9.5.5. Connection reuse metrics

Per-scenario connection metrics in `result.json` and JSONL:

- `conns_opened` — total TCP handshakes initiated (new sockets).
- `conns_evicted` — pool evictions (idle-too-long, max-age).
- `conns_closed_by_peer` — peer-initiated close (FIN).
- `conns_peak` — concurrent-open peak during the run.
- `tls_resumed_count` — TLS sessions resumed via tickets or PSK.
  Field is present only when the negotiated stack advertised
  session-resumption support (tickets enabled or PSK exchanged);
  omitted otherwise to avoid the "0/0 = undefined" trap.
- `tls_resumed_rate` — `tls_resumed_count / tls_capable_opens`,
  where `tls_capable_opens` is conns opened on a TLS-capable
  transport where resumption was advertised. Range 0.0–1.0,
  decimal 4-digit precision. Omitted when tls_resumed_count is
  omitted.
- `reused_requests` — requests served over an existing kept-alive
  connection (no new handshake needed).
- `keepalive_hit_rate` — `reused_requests / total_requests`.
  Ranges 0 (every request opens a new connection) to ~1 (pool
  serves nearly everything). Replaces the earlier-draft
  `reuse_ratio` which mixed per-request and per-connection
  denominators.

All ratios are reported as decimal (0.0–1.0) with 4-digit
precision to avoid aliasing at low-rate workloads.

Without these, you cannot distinguish "server is fast" from "our
pool is masking backend latency". Critical for understanding pool
behaviour. Inspired by JMeter's connection-reuse stats and
Gatling's HTTP connection graph.

### 9.5.6. Protocol version negotiation

For HTTP, zerobench uses ALPN on TLS and Upgrade on cleartext (per
RFC 7540 §3.2). For WS, the HTTP Upgrade dance per RFC 6455 §4.

Behaviour when target negotiates differently than requested:

- `--http-version auto` (default): accepts whatever the target
  negotiates; reports the actually-negotiated version in
  `env.json.negotiated_http_version` and in the report header.
- `--http-version h2` (strict): if target refuses H2, fails the
  run with a clear error: "requested h2, target negotiated h1.1
  via ALPN". Comparable policy for `h3`, `h1`.
- `--http-version h2c` (cleartext H2): uses RFC 7540 §3.2 HTTP
  Upgrade dance (`Upgrade: h2c`, `HTTP2-Settings`); no ALPN
  involved. If server responds with 101 Switching Protocols, H2c
  is active; otherwise the run fails with "requested h2c, target
  did not Upgrade".
- WebSocket: `Sec-WebSocket-Version: 13` is sent per RFC 6455; any
  non-101 response is a `ws_handshake` error.
- **HTTP/2 GOAWAY mid-run**: server sends `GOAWAY(last_stream_id,
  error_code)` → zerobench stops new stream opens on that
  connection, drains in-flight streams within the
  `--h2-goaway-drain-ms=5000` deadline, then closes the
  connection. A fresh connection is opened if the pool policy
  allows. GOAWAY is recorded in `errors.h2_goaway` (separate
  counter), not as a failure; a clean GOAWAY is a graceful
  signal. Error-code is preserved in the JSONL for debugging.

**WebSocket close codes** (RFC 6455 §7.4):
- 1000 normal closure → `conns_closed_normal` (not an error).
- 1001 going away → `conns_closed_normal`.
- 1002 protocol error, 1007 invalid payload, 1008 policy, 1009
  too big → `errors.ws_protocol`.
- 1011 server internal error → `errors.ws_server_internal`.
- 1012 service restart, 1013 try again later → `errors.
  ws_server_unavailable`.
- 1006 abnormal closure (no close frame) → `errors.ws_abnormal`.
- 3000-3999 library-defined, 4000-4999 app-defined → captured
  verbatim in `errors.ws_app_close[code]` (per-code bucket).

Every result records the negotiated stack **per scenario** in
`result.json.scenarios[].negotiated_stack` (TLS version + cipher,
HTTP version, WS extensions offered/accepted). `env.json` carries
only run-wide context; per-scenario version details live in the
scenario record, because a multi-protocol run legitimately has
different values per scenario.

**Version surprises**: if `--http-version auto` negotiates
something other than the server's advertised preference (e.g., a
target ALPN-advertises h2 but serves h1.1 after negotiation —
which would be a bug in the target), zerobench records the
mismatch in `env.json.negotiation_warnings[]` and emits a
terminal warning.

Version mismatches between compared runs are surfaced in `diff`
output: "A=h2, B=h3; comparison crosses protocol versions".

### 9.5.7. IP family selection

`--ip-family auto|v4|v6` — default `auto` resolves both A and
AAAA and picks per the OS's `getaddrinfo` preference (RFC 6724).
Force-family overrides are recorded in `env.json.ip_family_forced`
and are part of the target_fingerprint. Mixed v4/v6 runs across
scenarios are allowed; each scenario records its family.

### 9.5.8. CPU affinity and socket tuning

**CPU affinity**: by default, worker threads are pinned to
distinct physical cores; the self-check echo thread pins to a
separate core (§P5). Hyperthread siblings are **excluded from
the worker set** on SMT-enabled Linux — a worker on core 0 and
an echo thread on core 32 (the SMT sibling) would share cache
and ALUs, corrupting the measurement. The chosen affinity mask
is recorded in `machine.json.worker_affinity` for audit.

**Sibling detection — platform-branched**:

- **Linux**: read `/sys/devices/system/cpu/cpuN/topology/
  thread_siblings_list` per logical CPU; reconstruct the
  physical↔logical mapping. Restrictive containers / cgroup
  cpuset masks that hide the path fall through to the
  detection-failed path below.
- **macOS**: read `hw.physicalcpu`, `hw.logicalcpu`, and
  `hw.packages` via `sysctl(3)`. macOS does not expose per-core
  sibling mapping the way sysfs does, but the
  logical-per-physical ratio is sufficient to exclude
  hyperthread siblings from the worker set (pick one logical per
  physical, round-robin).
- **Detection failed** (genuinely unknown topology — obscure
  platform, unreadable sysfs *and* unreadable sysctl, user
  cpuset that hides siblings):
  - zerobench **logs an ERROR** to stderr: "SMT siblings
    unavailable; pinning disabled; measurements may be
    SMT-contaminated." No silent fallback.
  - `--no-affinity` is assumed automatically; a flag records this
    in `machine.json.worker_affinity_reason="detection_failed"`.
  - A user can override with `--siblings 0,32|1,33|...` to
    supply the mapping manually when they know it.

`--no-affinity` lets the OS schedule workers freely; only use
when the benchmark is explicitly about OS-scheduler behaviour.

**Socket tuning flags recorded in `machine.json`**:
- `so_reuseport_enabled` — true if kernel supports + tool uses.
- `so_incoming_cpu` — if used (per-socket CPU steering hint).
- `tcp_nodelay` — Nagle disabled on benchmark sockets (default
  true, because benchmarks measure per-request latency).
- `tcp_quickack` — Linux-specific ACK-coalescing disable, on by
  default for benchmarking.
- `tcp_congestion` — the congestion control algorithm (cubic,
  bbr, reno) — affects throughput-over-RTT measurements.

These are informational; the tool does not rewrite them but
records them so differences between machines become visible.

**Rate-scheduler jitter**: **disabled by default.** Earlier
drafts proposed ±1µs jitter to avoid lockstep with target timers,
but at sub-100µs request latency, 1µs is a 1% signal-to-jitter
contamination — directly violating P10. Lockstep concerns are
real but rare; zerobench's CO-free scheduler with intended-start
times already breaks trivial lockstep. Users who suspect lockstep
pathology can enable `--jitter-ns N`; the value is recorded in
the JSON artifact. Seed (if jitter is enabled) derives from the
same seed as body generation (§9.5.1) for deterministic replay,
recorded in `env.json.scheduler_seed`.

### 9.5.9. Archive size and rotation

`.histlog` files compress well (~100KB per minute of 1M req/s run)
but archives accumulate. Size policy:

- `--archive-retain=30d` (default): prune runs older than 30 days
  at the next `$ZEROBENCH_HOME` write. Per-(url_fingerprint).
- `--archive-retain=N` (integer): keep last N runs per
  url_fingerprint.
- `--archive-retain=unlimited`: no pruning.
- Pruning deletes entire run directories; never partial data.
- **Baselines** (marked via `zerobench archive pin <run_id>`) are
  never pruned — users can anchor specific runs as permanent
  reference points.

**Concurrency**: pruning is guarded by an advisory `flock(2)` on
`$ZEROBENCH_HOME/.prune.lock`. Runs in progress maintain a per-run
lockfile in their own directory; the pruner refuses to delete any
run whose lockfile is held. Pruning runs at most once per
invocation (atomic), not opportunistically, so concurrent
`zerobench` processes cannot race into "who prunes first".

**Stale lockfiles** (from crashed zerobench processes): on both
local and NFS filesystems, the pruner treats a lockfile as stale
if (a) its PID is not a running process on the local host AND
(b) its age exceeds `--stale-lock-age` (default 1h). Stale
lockfiles are reaped before pruning continues. PID checks are
reliable on local FS; on NFS the PID may belong to a remote
host, so the age check dominates. Users can force reap with
`zerobench archive unlock <run_id>` for manual recovery.

**NFS caveat**: `flock(2)` semantics on NFS are fragile (Linux
clients translate to `fcntl(2)` byte-range locks, which many NFS
servers handle inconsistently). If `$ZEROBENCH_HOME` is on NFS:
- zerobench detects NFS at startup via `statfs(2)` (`f_type ==
  NFS_SUPER_MAGIC`) and falls back to **O_EXCL-based
  lockfiles** with a stable PID + nonce, plus a stale-lock
  detector (if the lockfile is older than `--stale-lock-age=1h`
  and its PID is dead on the local machine, it's reaped).
- Users are warned once per process: "archive on NFS; using
  O_EXCL fallback locking, which is safe but can be slower".
- `--archive-retain=unlimited` is one option on NFS; let
  something else (cron, backup tooling) do retention. For users
  who want bounded archive on NFS without relying on cron,
  `--archive-retain=N --lock-mode=oexcl` is explicit and the
  O_EXCL fallback is used unconditionally; it works but may be
  slower due to lock-acquisition retries. Example cron snippet
  for external retention is given in the README
  (`find $ZEROBENCH_HOME/runs -mindepth 2 -maxdepth 2 -mtime
  +30 -type d ! -newer <pin-file> -exec rm -rf {} +`).

Pruning logs to `$ZEROBENCH_HOME/archive.log` so users can audit
what was removed.

### 9.5.10. Multi-target runs

`measure URL1,URL2,URL3` is explicitly **not** supported as a
shorthand. `compare` accepts exactly two positional URLs because
that is the specific "A vs B regression" case with CI-comparison
built in. `compare URL1 URL2 URL3` is an error; the suggested
fix is to write a Rhai script with named scenarios. For ≥3
targets, use Rhai with named scenarios per target:

```rhai
scenario("node").url("http://node:3000/").rate("10k/s");
scenario("zeroship").url("http://zeroship:5101/").rate("10k/s");
```

**Rhai + `compare` interaction**: `compare --script bench.rhai
--side-a "scenario=node" --side-b "scenario=zeroship"` selects
two scenarios from a single script as the two sides. This is the
documented pattern for "run the same scenario in two environments
from one script". If `compare` is passed a script **and** two
URLs, URLs win (explicit precedence) and the script provides only
the scenario template to apply to each URL; scenario selection
ambiguity errors.

The reason multi-target on the CLI is disallowed: it hides the
question of whether scenarios run serial (P9 default) or parallel,
and the flags to express this would duplicate the DSL. `compare
URL1 URL2` is the
specific two-target case with CI-comparison built in; beyond two,
use Rhai.

<!-- Added in round 2: addressing Missing Concepts — body generation, auth lifecycle, 429 handling, multi-target policy -->

---

## 9.6. Performance contract

The tool is the instrument. If the instrument's performance is
comparable to what it measures, every number is contaminated. The
floors below are **non-negotiable**: violations block release and
block PR merges.

### 9.6.1. Hard floors (CI-enforced)

Measured against a dedicated-core loopback echo, on the canonical
CI reference machine (documented in `docs/reference-machine.md`):

| Floor | Target | Measurement |
|-------|--------|-------------|
| **Throughput vs wrk baseline** | **≥ 1.0× mandatory, ≥ 1.2× expected** (current: 1.2–1.5×) | `bench/vs-wrk.sh` — same echo, same `-c`, same `-d`; compare req/s. Regression below parity with wrk blocks release. |
| Per-request tool overhead | **<2 µs p99** | `bench/tool-overhead.rs` — subtract loopback echo p99 from zerobench-measured p99 against same echo |
| Scheduler jitter | **<5 µs p99** | `bench/scheduler-jitter.rs` — actual vs intended token-emission times, open-loop at 500k/s |
| Template expansion | **<200 ns** typical 5-var | `bench/template.rs` micro |
| Connection-slot acquisition | **<100 ns p99** | `bench/pool.rs` micro |
| Hot-path allocations | **zero** | `#[test] fn hot_path_no_alloc` — counter-allocator wrapping global allocator |
| Hot-path contended locks | **zero** | lockless-by-construction design review + `loom` concurrency tests on the hot-path types; optional Linux-only `perf lock contention` inspection during pre-release audits (not required on every PR, since perf-lock needs CONFIG_LOCK_STAT and root and is macOS-unavailable) |

Regression >5% on any floor → PR blocked by CI. The wrk-baseline
row is the only one with a *lead metric* (1.2× expected above
parity): it protects the observed v0.0.1 headroom, rather than
merely enforcing non-regression.

### 9.6.2. Self-refusal gate

P5's client self-check is upgraded from warning to hard gate:

- Loopback-echo run at requested rate sustained ≥99.0% → proceed.
- Sustained <99.0% → **refuse to run**; print achieved ceiling and
  suggest `-r <ceiling>` or `--force-overload`.
- `--force-overload` runs anyway; result archive carries a
  `force_overload: true` flag that poisons comparison (any diff
  against a non-overloaded baseline fails loudly).

### 9.6.3. Meta-benchmark

`zerobench bench` — a subcommand that benchmarks zerobench itself.
Runs a canonical suite against the bundled loopback echo:

- Single-request RTT distribution (HDR, 1M samples)
- Open-loop rate sustainability at 100k / 500k / 1M / 2M req/s
- Template expansion micro
- Connection-pool acquisition micro
- Scheduler jitter distribution

CI runs this on every PR and comparison-diffs against `main`. Any
floor regression > 5% blocks merge. Results archived to
`docs/bench-history/<commit>.json` for trend tracking.

### 9.6.4. Cross-tool validation

Quarterly, we run zerobench, wrk, wrk2, bombardier, and oha against
the same canonical echo server, same `-c`, same `-d`, same
assertion surface. Expectations differ per peer tool because their
measurement models differ:

- **vs wrk (closed-loop service-time)**: zerobench must sustain
  **≥ 1.0× throughput (mandatory) and ≥ 1.2× (expected; current
  baseline is 1.2–1.5×)**. Regression below parity blocks release.
  Zerobench being faster than wrk is *the* canonical validation
  signal; a world in which wrk becomes faster than zerobench is a
  world in which we have a regression to hunt.
- **vs wrk2 (constant-throughput CO-free, same measurement model)**:
  throughput within **±5%** at matched offered rate. Tail
  percentiles within **±5%** at the same rate (both tools are
  CO-free, HDR-ns). Larger divergences are investigated.
- **vs bombardier / oha (closed-loop simple clients)**: throughput
  within **±2%** at saturation. They measure the same thing we do
  in saturate mode; divergence points at a tool bug somewhere.

Archived as `docs/validation/<YYYY-QN>.md` with setup, numbers, and
any action items. A quarter where we fail to run validation is a
failed quarter; it blocks the next minor release.

### 9.6.5. Binary and dependency budget

- **Default install ≤ 8 MB** stripped.
- **Every new dependency** requires a `deps.md` entry:
  - what it's for
  - binary-size delta (measured, not estimated)
  - per-request overhead delta (measured against §9.6.1 floors)
  - alternatives considered
- Dependencies pulled transitively that add >500 KB get the same
  scrutiny.

### 9.6.6. No background work during measurement

During a measurement window (between warmup end and cooldown start):

- No lazy initialization. Everything is pre-warmed.
- No periodic tasks (GC-adjacent cleanup, log rotation, metrics
  flush). If state needs flushing, it happens at window boundaries.
- No logging I/O beyond the configured `--format` output. Rhai
  `print` statements on the hot path are a compile error in v0.1.0.
- No `getrandom` on the hot path; per-thread RNG pre-seeded at
  plan-construction time.

---

## 10. What we're dropping

| Drop | Reason |
|------|--------|
| `Step::SseStream` (stream-completion SSE) | Measures the wrong question. Replaced by `SseHold` and friends. |
| `Step::WsRound` (handshake-per-op WS) | Measures the wrong question. Replaced by `WsEchoRtt` / `WsServerPushRtt` / `WsHold` / `WsFanout`. |
| "ops/s" top-line across mixed-protocol runs | Fiction. Separate protocol headlines instead. |
| Default-is-saturate | `measure` is the new default, open-loop with CO-free rate. `saturate` is a conscious opt-in. |
| `weights.toml` (request-file dir mode) | Rhai does weighted scenarios with `.rate()` per scenario. `.http` files default to equal-weight. |

**Not dropped** (reviewer correction):

- `.http` request-file parser — retained. It's a real version-
  controlled artifact format, not an idiosyncratic convenience.
- **HTTP/3** — retained. Zeroship is shipping HTTP/3 in the
  runtime; a benchmarker that can't measure its own platform's
  headline protocol is self-defeating. We accept the maintenance
  cost.

<!-- Added in round 1: addressing CRITICAL #5 — .http retained; addressing CRITICAL #6 — HTTP/3 retained -->

---

## 11. What we're adding

| Add | Why |
|-----|-----|
| `calibrate`, `measure`, `curve`, `compare` subcommands | Each answers a specific question today done by awkward flag combos or shell loops. |
| `SseHold`, `SseFanout` (two submodes), `SseReconnectStorm` Step variants | Real SSE questions. |
| `WsHold`, `WsEchoRtt`, `WsServerPushRtt`, `WsFanout` Step variants | Real WS questions, precisely named. |
| `HttpColdConnect` Step variant | Measures accept + handshake separately from pool reuse. |
| Machine fingerprint in every result (§8.3) | Reproducibility (P11). |
| Client-calibration self-check on boot | P5 + P10. |
| `--runs N` with bootstrap-CI reporting | P2 + P8. |
| `expect_p99`, `expect_p_n`, `expect_error_rate`, `expect_steady_state`, `expect_rate`, `expect_keepup` | SLO-gate CI use case (§7.1). |
| `--explain`, `lint`, `replay` subcommands | Rhai usability (§7), reproducibility (§8.4). |
| `$ZEROBENCH_HOME` auto-archiving | Reproducibility (P11). |
| Default progress line on stderr | Observability (P12). |
| HdrHistogram `.histlog` (compressed V2 log) output | Interop with JVM ecosystem tooling. |
| Exhaustive per-category error taxonomy (§9.2) | Distinguish connect-refused from timeout from TLS, etc. |
| `--from-curl` parser | Copy-paste from browser devtools. |

---

## 12. Migration from v0.0.1

This is a major version bump to v0.1.0. Breaking changes are acceptable.

- **Report schema**: v1 → v2. `schema_version` per file.
  `--read-schema=1` enables a compatibility shim for v1 inputs
  into `diff`/`replay`. Field-level diff:
  - **v1 removed** (mapping in parens): `ops_per_s` (→ removed;
    use per-protocol metrics), `rtt_ns` on WS (→ `rtt_ns` +
    `rtt_mode`), `chunk_count` on SSE (→ `events` +
    `events_per_s`).
  - **v1 renamed**: `error_connect` → `errors.connect_refused |
    errors.connect_timeout | errors.connect_reset` (split);
    `error_other` → `errors.protocol`; `reuse_ratio` →
    `keepalive_hit_rate`.
  - **v1 → v2 additions** (no v1 equivalent): `run_id`, `phase`,
    `negotiated_stack`, `auth_refresh`, `conns_*`,
    `tool_overhead_p99_ratio`, `tool_influenced`,
    `clock_skew_ms`, `keepup`, `h2_goaway`,
    `ws_app_close[code]`.
  - The shim maps v1 `ops_per_s` into v2 `rate` for HTTP
    scenarios only; non-HTTP v1 ops/s cannot be faithfully
    remapped and yield a shim warning.
- **Rhai builders**: `SSE(...).expect_chunks(n)` removed. Replaced
  with `sse_hold(url, n=..., for=...)`. `WS(...).message(m)`
  removed. Replaced with `ws_echo_rtt(url, n=..., msg_rate=...)`.
  `zerobench lint` flags v0.0.1 forms **and emits a one-line
  rewrite hint**: `-- old: SSE("/stream").expect_chunks(100); new:
  sse_hold("/stream", n=1, for="...").expect_chunks(100)`. No
  auto-migration tool in v0.1.0; hints are intentionally copy-
  paste guidance, because idiomatic migration often needs context
  that a mechanical rewrite lacks.
- **Default duration**: 30s → 60s + 15s warmup + 3 runs. Users
  relying on `-d 30s` add the flag explicitly.
- **Default mode**: `probe`-equivalent (5s) for bare
  `zerobench URL`; `measure` for explicit `zerobench measure URL`.
- **HTTP/3**: retained; no migration needed.
- **`.http` parser**: retained; no migration needed. `weights.toml`
  removed (ignored with a lint warning for one release cycle).

### 12.1. Deprecation timeline

Concrete cadence for v0.1.0 and onwards:

| Item | v0.1.0 | v0.2.0 | v0.3.0 |
|------|--------|--------|--------|
| Old `weights.toml` | warn-only | hard error | removed |
| `SSE(...).expect_chunks()` Rhai form | lint error | removed | — |
| `Step::WsRound` | removed | — | — |
| `--read-schema=1` shim | enabled | enabled | warn-on-use |
| `--read-schema=2` shim | n/a (current) | enabled | enabled |
| v1 schema shim (next major) | n/a | — | removed |
| `--ws-correlate=monotonic_id` (old name) | warn (use `monotonic_id_prepend`) | removed | — |

Pattern: deprecations get one full minor-version cycle of
warning before removal; major-version shim carry for two
subsequent majors (§9.3.2).

<!-- Added in round 1: addressing MINOR #18 — explicit no-auto-migration policy with rewrite hints -->

---

## 13. Non-goals (explicit)

- **wrk compat** (flags, output, Lua API).
- **Browser emulation** (DOM, multipage navigation, full cookie
  jar with JS-visible manipulation). Minimal RFC-6265 cookie jar
  is supported (§9.5.1a); full browser-class semantics are not.
- **Correctness / schema / contract testing.**
- **Distributed client coordination** (multi-machine load gen)
  — deferred. v0.2.x is a placeholder for "after v0.1.0 is
  shipped and users are asking"; no commitment on timing. If
  demand stays low, this becomes a permanent non-goal. The
  `docs/ROADMAP.md` (separate file) carries the actual plan.
- **Chaos / fault injection** (latency injection, packet loss
  simulation, intentional partitions). Users can compose with
  `tc qdisc netem` at the OS level or with service-mesh sidecars
  (Istio, Linkerd). zerobench does not own this concern because
  the tool-vs-chaos-harness split is industry-standard
  (Gatling + Chaos Monkey, k6 + Pumba) and muddying it reduces
  claim clarity. This is a considered non-goal, not an oversight.
- **GUI.**
- **Windows** — Linux + macOS only for v0.1.0. "User asks" is
  defined concretely: one filed GitHub issue with a reproducible
  use-case and a stated willingness to be the alpha tester.
  Below that bar, Windows work is deferred.

### 13.1. Telemetry — none

**zerobench does not phone home.** No usage stats, no crash
reports sent automatically, no version-check pings, no update
checks, no `--version` network calls. Tool is suitable for
air-gapped environments out of the box. Users who want to share
crash logs can opt in via `zerobench report-crash <run_id>`
which generates a portable tarball for manual sharing — never
an automatic network call. This is an explicit commitment, not
a default-to-be-changed-later. Network access is initiated only
for the benchmark target(s) the user specified.

### 13.2. Metrics export for live monitoring

In-scope for v0.1.0: the JSONL stream on stdout/file. Downstream
consumers (Prometheus, InfluxDB, any time-series DB) can ingest
JSONL easily.

Out of scope for v0.1.0 **core**: native Prometheus `/metrics`
endpoint, OpenTelemetry OTLP export, StatsD pushing.

**Shipped as a companion**: `zerobench-prom-adapter` (separate
binary, same repo) that tails the JSONL stream and exposes
`/metrics` over HTTP for Prometheus scraping. This is a thin
adapter (~200 LOC) rather than core functionality because:
- Prometheus integration has decisions (histogram buckets vs
  summaries, label cardinality, scrape interval) that belong to
  the deployer, not the tool.
- Keeping it separate lets core iterate without breaking scrape
  contracts.
- Users can write their own adapter in 50 lines if ours doesn't
  fit.

OpenTelemetry, InfluxDB, etc. adapters are community-invitation
targets; the JSONL schema (§9.2) is the public contract.

### 13.3. Service discovery

DNS-A/AAAA and `--resolve HOST:IP` are supported (v0.0.1
carryovers). Out of scope for v0.1.0: DNS-SRV lookup, Consul/etcd
lookup, Kubernetes-aware endpoint discovery. Users running
against K8s services either target the cluster-DNS name or
pre-resolve via `--resolve`. The cluster-DNS path suffices for
>95% of reported use cases; dynamic discovery is a v0.2.x item.

### 13.4. Multipart form-data and file uploads

`--form k=v` sends `application/x-www-form-urlencoded`.
`--form k=@file` sends `multipart/form-data` with a file part,
matching curl semantics. Large file uploads use the streaming
body path (§9.5.1). `--form-file name=@path` is the
disambiguating Rhai equivalent.

### 13.5. Worker parallelism

`-t N` (worker threads) is an implementation knob, not a
philosophy concern. zerobench picks a sensible default (physical
cores, capped at 32) and lets users override. The tool guarantees
that `-t N` changes do not change measurement semantics — latency
and rate are thread-agnostic — only resource utilisation.
Addressed here explicitly because reviewers noted `-t`'s absence
from this doc was ambiguous; it is *design*-level (in
`design-v0.1.0.md`), not *philosophy*-level.

---

## 14. Success criteria for v0.1.0

Concrete, falsifiable. Shipped means:

1. **Time to first comparison**: a new user, starting from no
   baseline, can go from cold to "zeroship vs node, side-by-side,
   with CIs and regression deltas" in a command count ≤ 3.
   **Mechanical test**: a CI step runs `grep -cE '^\$ zerobench'`
   over the README's "5-minute tour" markdown; count must be ≤ 3
   unique commands. Running the extracted commands on a stock
   Ubuntu 24.04 container produces a non-empty `diff` output
   with a CI block.
2. **SLO gate**: `zerobench measure … && echo ok || echo REGRESSED`
   works in CI. Exit code reflects assertion results. Test: a
   run with **any** failing assertion produces exit 1 and a
   summary line listing **every** failing assertion (one per
   line, with its observed value and threshold); a passing
   run produces exit 0. Multiple failures do not short-circuit
   — users need the full diagnostic to fix in one pass, not
   iteratively.
3. **Replay fidelity**: a `result.json` from a prior run, replayed
   on the same machine fingerprint against the same target, reports
   delta within the **maximum of** (the saved run's 95% CI, a
   tool-wide noise floor of ±5% p99, cold-cache allowance of +10%
   p99 on first attempt). Tolerances are stacked because: (a)
   some runs have implausibly narrow CIs due to low variance in
   a short window that does not persist across time; (b) cold OS
   page cache and TLS session-cache miss are well-known first-
   replay artefacts (Netflix NDBench explicitly excludes first
   run); (c) SPEC CPU's run-to-run tolerance guideline is ±3% for
   median metrics, which translates to roughly ±5% at p99 for
   throughput-oriented benchmarks. Numbers are **initial
   defaults**, expected to be recalibrated from telemetry in the
   first release cycle; a followup ADR will adjust based on
   observed bimodality. Test: replay a CI artifact 30 days old on
   the same runner with cache flushed, expect exit 0.
4. **Measurement fidelity**: every scenario result declares its
   mode:
   - HTTP: `http_mode ∈ {rate, saturate, ramp, handshake_rate}`.
   - SSE: `sse_mode ∈ {hold, fanout_timestamp, fanout_trigger_rtt,
     reconnect_storm}` plus the concrete subscriber count and
     events/s + event-gap p99 fields.
   - WS: `ws_mode ∈ {hold, echo_rtt, server_push_rtt, fanout_
     timestamp, fanout_trigger_rtt}` plus the concrete
     correlate_strategy for echo_rtt.
   No "ops/s" aggregate across protocols anywhere. **Test**:
   grep every result.json for a scenario record lacking its
   mode field; count must be zero. Also: grep for the literal
   token `ops_per_s` at top level; count must be zero.
5. **Self-check honesty**: if the client cannot sustain the
   requested rate against loopback, the tool says so before the real
   run. Test: ask for 10M req/s, tool warns and halves displayed
   ceiling.
6. **Context discipline**: no **structured artifact** (JSON,
   JSONL, `result.histlog`, archive index) emits without tool
   version + machine fingerprint + plan hash + duration in the
   same file or a sibling file readable by the same consumer.
   Test: every `result.json`, `result.histlog`, and JSONL record
   either carries these fields directly or references
   `INDEX.json` / `machine.json` / `env.json` in its containing
   directory — CI enforces this via a fixture-scan. **Terminal
   prose output is exempt**: the compact report header (§4.5)
   contains the identifiers but stdout progress-lines and TUI
   cells do not repeat them per-line (context is the enclosing
   report). The criterion applies to data artifacts, not
   human-readable output.
7. **Statistical honesty**: `diff` output that reports "+X%" must
   include a 95% CI on that delta (run-bootstrap) OR a
   distribution-test verdict (AD/KS) with raw per-percentile
   deltas, per §9.3 — not observation-level bootstrap of HDR
   buckets. Test: `diff A B` output matches one of these
   regexes:
   - `CI \[[-+0-9.µmsn %]+, [-+0-9.µmsn %]+\]` (bootstrap path).
   - `A²=[0-9.]+ \(p=[0-9.]+\)` or `D=[0-9.]+ \(p=[0-9.]+\)`
     (distribution-test path).
   - `no significant change` (no-change verdict).
   Single-run comparisons carry a visible N=1 warning in all
   cases.
8. **Protocol clarity**: every WS/SSE report must declare its mode
   — `ws_echo_rtt.correlate=ping_pong`, `sse_fanout.mode=
   timestamp`, etc. Test: grep any WS/SSE result for absence of
   mode declaration; count must be zero.
<!-- 9. No-infinite-CI-loops test obsolete after watch drop (2026-04-19). -->
11. **Rhai script crash handling**: a Rhai panic during a run
    is caught at the scenario boundary. The scenario's
    histogram and error counters up to the point of panic are
    preserved and written to
    `result.json.scenarios[].partial=true`. Exit code is 3
    ("scenario panic"), distinct from assertion failure (1) and
    did-not-measure (2). The partial artefact is still diffable
    but flagged `partial=true`; `diff` refuses to compare
    partial-vs-complete without `--allow-partial`. **Concrete
    test**: run the bundled `tests/fixtures/panic-in-step.rhai`
    (which calls `throw "boom"` after ~100 requests); expect
    exit 3 and non-empty `result.json` with `partial=true`.

10. **Coarse-clock refusal**: with a clock source reporting
    ≥10µs resolution, `zerobench measure` refuses without
    `--allow-coarse-clock`. Portable test: CI uses an
    `LD_PRELOAD` shim returning `{tv_nsec &= ~((1<<14)-1)}`
    (forces ~16µs resolution); expect exit 2 with a clear
    error. Avoids CI-environment-dependent qemu setups.

<!-- Added in round 1: addressing CRITICAL #11 — #3 weakened to CI overlap; #6 and #7 added for context discipline / statistical honesty -->
<!-- Added in round 2: #7 updated for run-bootstrap; #8, #9, #10 added as new falsifiable tests -->

---

## 15. Open questions

These are tensions v0.1.0 must resolve; listed here for the
critic/reviser loop.

### Q1. `probe` vs bare `zerobench URL` — one or two? **RESOLVED (2026-04-19)**

**Decision: bare `zerobench URL` runs `probe`** (5s, one run, no
auto-archive). `zerobench measure URL` is the rigorous default.
`probe` also exists as an explicit verb for scripts.

Rationale: a bare URL is a curious-user first impression — a
5-second smoke test matches expectations of modern CLI tooling
(hey, bombardier, oha) and is the safe landing for "does it work."
A 60-second rigorous measurement as the no-flag default would
confuse first-time users ("why is this tool taking so long?") and
spam `$ZEROBENCH_HOME` with baselines they didn't ask for. The
README leads with `probe` → `measure` → `compare` progression.

### Q2. Auto-comparison default — does first run auto-save a baseline? **RESOLVED (2026-04-19)**

**Decision: yes**, `measure` / `compare` / `curve` / `soak` runs
auto-archive. `probe` does not (it's a smoke test; archival would
spam baselines from casual invocations — see Q1). Disk pressure is
bounded by `--archive-retain=30d|100runs` (default; configurable)
and by the `$ZEROBENCH_HOME` env var opt-out.

Consequence chain:
- Second `measure` against same `target_fp + plan_fp` auto-diffs:
  *"p99 810µs (+2.3% vs baseline 2026-04-15)"*.
- CI use case is first-class: the archive path is predictable,
  glob-able, and stable across runs.
- The "I just want one number" user runs `probe` (Q1), gets no
  archive, no baseline, no diff. Clean.

### Q3. `calibrate` caching — do we need it? **RESOLVED (2026-04-19)**

**Decision: no cache.** Calibrate runs fresh on every `measure` /
`compare` / `curve` / `soak`, integrated into the warmup phase
(first 2–3s runs a loopback-echo self-check in parallel with real
warmup traffic). `probe` skips calibration (smoke test — no rigor
claim).

Rationale:

1. **Calibrate is cheap** (2–5s against in-process loopback).
   Piggybacking on warmup makes net cost ≈0.
2. **Client-state asymmetry**: we already always re-measure the
   remote target fresh per run. If we accept that cost for the
   expensive measurement, caching the cheap one is backward.
3. **Cache complexity multiplies bugs**. Every invalidation axis
   (tool version, CPU, kernel, governor, cgroup, concurrency flags,
   TTL) is a way for a stale calibration to lie silently — a
   strictly worse failure mode than "ran calibrate fresh, took 2s."
4. **CI is not an argument for caching**. CI machines have noisy
   neighbors, variable thermals, and container-quota drift; fresh
   calibration is *more* valuable there, not less.

Drops: `$ZEROBENCH_HOME/cache/`, `--recalibrate`,
`--calibration-trust-until`, cache-hit transparency banner.

Keeps: `--no-calibrate` as an explicit opt-out (advanced users
running on machines they trust). Skipped calibration stamps the
result archive as `calibration: skipped`, which poisons comparison
against properly-calibrated baselines (analogous to the
`force_overload` poison from §9.6.2).

### Q4. SSE `mode="trigger-rtt"` accuracy bounds?

The trigger-rtt proxy measures time from the HTTP POST returning
2xx to the first event arriving at subscriber — which adds:
- One HTTP round trip (the POST → 2xx ACK).
- Server's own internal trigger-to-broadcast latency (usually
  negligible but implementation-dependent).
- Any lag between POST-ACK and the server actually emitting the
  broadcast.

Subtracting the POST's own RTT (also measured and reported as a
distinct field) gives **an approximate** broadcast latency. The
subtraction is valid only when (a) both paths use the same
socket-stack layer (they do not — they're different sockets)
and (b) server internal trigger→broadcast is ~0. §4.3 updated
in round 6 to not emit a pre-computed subtraction; the tool
reports both quantities and defers the math to the analyst.

Bounds on accuracy depend on the SUT; for a well-behaved local
server this is within single-digit-µs of truth; for remote or
complex servers, it is **an upper bound** on broadcast latency
(real latency ≤ reported trigger-rtt − POST-RTT).

### Q5. HTTP/2 stream concurrency as a distinct mode? **RESOLVED (2026-04-19)**

**Decision: yes — mirror h2load's flag shape, fix its default.** See
§4.2.1 for the full spec.

- `--conn-concurrency N` / `-c` — connection count (H1/H2/H3).
- `--stream-concurrency M` / `-m` — streams/conn (H2/H3 only).
- Total in-flight = `c × m`, capped by server's
  `SETTINGS_MAX_CONCURRENT_STREAMS` (reported, not silently worked
  around).
- **Default `-m 100` for H2/H3** (departs from h2load's `-m 1`,
  which makes H2 benchmark as H1 by default and misleads).

Rationale: h2load has been the de facto H2 benchmark for a decade
and the `-c`/`-m` split is industry-literate. Zero innovation cost
on our side; the only judgment call is the default, where we choose
"exercise the feature H2 is chosen for" over "behave like H1 until
told otherwise."

### Q6. Soak-mode leak detection **RESOLVED (2026-04-19)**

**Decision: no in-house leak or drift analysis.** `soak` is a
long-duration `measure`; the aggregation, output, and statistics
are identical. The only differences are: longer default window
(5 min + warmup), higher confidence in extreme percentiles
(p99.99, max), and more capture of periodic behaviour (GC cycles,
cache rotations, server-side heartbeats).

Rationale:

1. **Target may be remote.** We cannot portably sample target-side
   resources (RSS, FDs, GC events). `/proc/<pid>` only works for
   local targets. Inventing a half-working local-only diagnostic
   lies about the tool's scope.
2. **Measurement ≠ diagnosis.** Computing regression slopes on our
   own metrics (p99 drift, error-rate trend) crosses from
   *measurement* into *interpretation*. "p99 drifted +10%" is a
   claim about *why* performance changed — JIT deopt? GC? State
   bloat? Cache warmup ending? We cannot tell, and pretending to
   would mislead. The user owns diagnosis, paired with their own
   observability stack (htop, perf, Prometheus, Grafana, pprof).
3. **The data is already there.** `soak` emits per-second JSONL
   just like `measure`. A user who wants drift analysis can pipe
   it through their own tooling — and will get a better answer
   than we could offer without context on the system under test.

What soak explicitly does **not** do in v0.1.0:
- No linear-regression / trend detection on any metric.
- No "leak detected" / "drift suspected" warnings.
- No target-side resource sampling (RSS, FDs, GC).
- No cooperation assumed with the target beyond the protocol under
  measurement.

This is philosophy-aligned: the tool measures honestly, reports
tail and distribution faithfully (P3), and refuses to invent
numbers it cannot justify (P1). Diagnosis is the user's job with
their own context.

<!-- Added in round 1: surfacing HTTP/2 stream-concurrency and soak-leak-detection as genuine open questions raised by critic's concerns -->

---

## 16. Change log

(Newest first. Each entry is one round of critic→reviser loop.)

- **2026-04-18 r20**: change-log reordered newest-first for
  readability; final coherence pass complete; loop converged.
- **2026-04-18 r19**: stale-lockfile reaping applies to local
  FS too (not just NFS); `zerobench archive unlock` manual
  recovery command.
- **2026-04-18 r18**: criterion 6 scoped to structured
  artifacts (not terminal prose); sibling-file reference pattern
  acceptable when artifact is in a known archive layout.
- **2026-04-18 r17**: `--regress-on` multiple-threshold
  evaluation explicitly not family-wise-corrected (configured
  gate, not hypothesis test); `--regress-on-all` opposite-
  polarity flag; §5 verbs table includes cooldown defaults.
- **2026-04-18 r16**: tls_resumed_* fields omitted when
  resumption unsupported (vs misleading 0); consistency pass on
  auth_refresh references in schema diff.
- **2026-04-18 r15**: P9 wall-time formula corrected (warmup is
  per-run, not per-scenario); handshake_rate TLS-resume reporting
  guards against "no tickets issued" case.
- **2026-04-18 r14**: P2 default threshold behaviour (opt-in
  gate) clarified; cgroup fields use `null` for "unlimited".
- **2026-04-18 r13**: live HDR window min/max bounds; watch
  regression baseline precedence; SLO-gate assertion failure
  reports all failures (not short-circuit).
- **2026-04-18 r12**: Q4 clarified with explicit "upper-bound"
  semantics; auth retry capped at one refresh (no infinite
  loops); criterion 11 concrete test fixture; status section
  clarifies design-v0.1.0.md is future-dated.
- **2026-04-18 r11**: report header compact default + `-v`
  verbose; mmap prefetch hints (MADV_WILLNEED + readahead) for
  large streamed bodies; external-file content-hash pinning in
  plan.json for replay correctness; probe default 10s → 5s;
  telemetry commitment extended to include no-update-check.
- **2026-04-18 r10**: HDR interval records (incremental
  persistence to `.histlog` once per minute, not per second);
  criterion 4 made grep-testable with explicit mode enum;
  distributed coordination shifted from "v0.2.x" to "deferred,
  no commitment" + roadmap reference; `expect_rate(window)`
  for rolling-window assertions; §12.1 concrete deprecation
  timeline table.
- **2026-04-18 r9**: body faker cross-machine nonce option;
  schema-contract tool named (`jsonschema-rs` + CI git-diff
  test); warmup-for-runtime presets for JVM/Node/zeroship;
  SSE clock-probe protocol spec (3-line JSON response);
  Holm-Bonferroni multiple-comparison correction; --joint-ci
  for family-wise percentile coverage; `--compare-schedule=
  serial` rationale clarified; container/cgroup fields in
  machine fingerprint; progress cadence scales with duration.
- **2026-04-18 r8**: SMT-sibling-detection failure loud (no
  silent fallback); rolling HDR as ring buffer with per-second
  sub-histograms (bounded memory over long runs); rate-scheduler
  jitter disabled by default (1µs jitter contaminates sub-100µs
  signal); `watch` × (diff|compare) interactions defined
  (per-window run_ids, compare-with-watch refused); P1 mandatory
  vs best-effort field split; body faker nonce formula specified;
  JSONL backpressure policy (bounded ring + drop with counter);
  schema-stability commitment (additive minor, shim 2 majors);
  success criterion 7 regex-based matches multiple strategies;
  Rhai-panic handling with partial histogram + exit code 3.
- **2026-04-18 r7**: P10 per-metric overhead ratios (not just
  p99); cooldown between every run in `compare` interleave;
  warmup abort distinguishes connectivity-errors from app-level
  errors (4xx/5xx are valid steady-state); bootstrap default
  changed from BCa to percentile (justified at small N); streaming
  bodies keep-alive restored per RFC 7230; Prometheus adapter
  shipped as companion binary (not core); JSONL units unified to
  `_ns` with human rendering only on stdout; CPU-affinity + SMT-
  sibling exclusion; `so_reuseport`, `so_incoming_cpu`,
  `tcp_congestion` recorded; rate-scheduler jitter seed tied to
  body seed for deterministic replay; success criterion 1 made
  mechanically testable.
- **2026-04-18 r6**: P10 operational definition (loopback p99 ≤
  1% of target p99, with tool_influenced labelling not refusal);
  seed-determinism footgun acknowledged with `--seed random` and
  `unique=true` faker option; SSE trigger-rtt subtraction caveat
  (no pre-computed subtraction emitted); NFS archive retention
  cron example; `compare` interleaved by default (A₁B₁A₂B₂A₃B₃
  round-robin); negotiated stack moved to per-scenario; v1→v2
  schema migration field-diff table; explicit no-telemetry
  commitment (§13.1); metrics export as non-goal (§13.2);
  service discovery as non-goal (§13.3); multipart form-data
  spec (§13.4); replay provenance via `replayed_from`; soak
  duration guidance with long-leak caveat; NUMA worker binding
  recorded; coarse-clock portable test via LD_PRELOAD.
- **2026-04-18 r5**: auth view `user_visible` degrades for no-
  refresh chains; NFS lock fallback with O_EXCL for archive
  rotation; lint structured-diagnostics and rewrite-hints
  output; HDR-precision clamp for `expect_p_n`; seed defaults to
  `plan_hash` alone for "same plan, same seed" determinism; SSE
  timestamp-mode clock-skew verification via NTP-style exchange;
  P5 echo pinned to separate core; `fd_limit` auto-raise with
  record of initial value; JSONL includes `run_id`; report header
  shows url_fp/target_fp/run_id; H2c cleartext Upgrade path;
  GOAWAY drain semantics; WS close-code taxonomy (RFC 6455
  §7.4); warmup-error-threshold guard with exit=2 distinct from
  measurement failure; per-scenario vs aggregate JSONL
  clarification; expect_keepup accepts max_level.
- **2026-04-18 r4**: Anderson–Darling replaces KS as default for
  N=1 tail-sensitive comparison (KS retained as opt-in); auth
  retry unified as canonical observation-log with three named
  views (latency_main, user_visible, auth_overhead); plan.name
  required for archived runs instead of silent filename-stem
  default; keepalive_hit_rate replaces ambiguous reuse_ratio; WS
  fanout trigger modes spec'd; IP family in url_fingerprint;
  deterministic seed control with replay-preservation;
  100 MiB body + streaming support; RFC-6265 cookie jar spec;
  archive pruning concurrency lock; heartbeat default 25s (proxy
  margin); noise-floor numbers tagged as initial defaults with
  re-calibration plan; chaos/fault-injection and worker
  parallelism explicitly declared non-philosophy; schema_version
  is per-file, aggregated in INDEX.json.
- **2026-04-18 r3**: replaced bucket-permutation (still wrong)
  with KS-test on ECDFs (valid on histogram data); BCa-corrected
  bootstrap; `ping_pong` default for WS echo RTT (zero-intrusion);
  `handshake_rate` replaces `cold_connect`/`conn_churn`; auth-
  retry latency now recorded per-phase (deliberate deviation from
  k6); CO thresholds justified and wrk2-compatible default +
  strict opt-in; `calibrate` implicit-vs-manual flow resolved;
  connection-reuse metrics, protocol negotiation, IP family,
  archive rotation spec added; url_fingerprint includes
  plan-name-tag to separate workloads on same host; replay
  tolerance stacked with noise floor and cold-cache allowance.
- **2026-04-18 r2**: corrected statistically-wrong bootstrap to
  run-level resampling + permutation-test for N=1; added WS echo
  correlation strategies; SSE EventSource parsing per WHATWG;
  two-level URL/target fingerprinting with stable archive grouping;
  `run_id` for replay; body-generation, auth-lifecycle, and 429
  handling specs; `--unbounded` guard on `watch`; coarse-clock
  refusal; live HDR instead of t-digest; rolling-window CV defaults
  by metric.
- **2026-04-18 r1**: initial draft → round 1 revision. Added
  statistical-significance framework (bootstrap CI), machine
  fingerprint spec, RFC 8785 canonicalisation, WS three-mode RTT
  split, SSE fanout fallback mode, `cold_connect` (renamed from
  `conn_churn`), HdrHistogram `.histlog` as source of truth,
  exhaustive error taxonomy, warmup/progress semantics, `.http`
  parser and HTTP/3 retained (reversing earlier drop).

  Post-loop, spawns `design-v0.1.0.md`.
