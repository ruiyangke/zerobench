# zerobench

Fast, correct, modern benchmarking for HTTP, SSE, and WebSocket —
mio/epoll-native, HDR-precise (nanosecond), protocol-native metrics,
baseline-compare-first workflow.

## Why another benchmark tool?

zerobench is a **measurement apparatus**, not a load generator.
Every number it emits carries enough context (machine, plan,
commit, duration, flags, resolved IPs, TLS, …) that it survives a
screenshot, PR comment, or regression investigation. Bare numbers
are banned.

- **Open-loop by default** — constant-rate scheduler, coordinated-omission-free latency.
- **Tail latency is the product** — HDR histograms end-to-end; p50/p90/p99/p99.9/p99.99/max on every report.
- **Protocols are different, treated differently** — HTTP is client-driven short sessions, SSE is server-driven long sessions, WS is bidirectional long sessions. Each gets protocol-native metrics.
- **Comparison is first-class** — every run auto-archives; `compare` runs bootstrap CI + Anderson-Darling + Kolmogorov-Smirnov against a baseline.
- **Fresh calibration per run** — in-process loopback echo; refuses to run when the client can't sustain the requested rate.

Performance floor: zerobench ≥ 1.20× wrk on loopback (enforced by `perf-gate.sh`).

## Install

```bash
# default build — all protocols + Rhai DSL, no TUI
cargo install zerobench

# + live terminal dashboard
cargo install zerobench --features tui

# full (equivalent to --features tui for now)
cargo install zerobench --features full
```

HTTP/1, HTTP/2, SSE, and WebSocket are always-on — they're not
feature-gated. Only `tui` (the ratatui dashboard) is optional.

## Six verbs

Each verb answers one question. No overlap.

| Verb | Question | Archives? |
|------|----------|-----------|
| `probe URL` | Does it respond, roughly how fast? (5s smoke) | no |
| `measure URL` | Steady-state throughput + tail at a given rate? | yes |
| `calibrate` | What's my machine's client-side ceiling? | no |
| `curve URL` | Where's the saturation knee? | yes |
| `compare A.json B.json` | Is B a regression against A? (bootstrap CI + AD + KS) | — |
| `diff A.json B.json` | Raw percentile delta table (simple gate) | — |
| `run script.rhai` | Multi-scenario / multi-protocol plan from a Rhai script | yes |

### Quick tour

```bash
# Smoke test — 5s, 1 run, no archive
zerobench probe http://api/events

# Rigorous measurement — 60s × 3 runs, calibrate-gated, archived
zerobench measure http://api/events --rate 10k --runs 3 --name api-events

# Client ceiling — how fast can my laptop push?
zerobench calibrate --rate 100k --duration 5s

# Saturation curve — ramp 1k..100k/s over 2 minutes; find the knee
zerobench curve http://api/events --from 1k --to 100k --over 2m --steps 10

# Compare against a saved baseline with bootstrap CI + AD/KS
zerobench compare baseline/result.json current/result.json --regress-on p99:+5%
```

### SSE — N persistent subscribers, "event is the op"

```bash
zerobench measure http://api/stream --sse-hold 1000 --hold-for 60s --runs 3 --name api-stream
```

Reports events/s (not streams/s) and inter-event gap p50/p99/p99.9. Matches
the production SSE workload question: "how many concurrent subscribers at
what chunk-gap tail?"

### WebSocket — persistent connections, echo-RTT

```bash
zerobench measure ws://api/chat --ws-echo 100 --msg-rate 50 --runs 3 --name chat-rtt
```

Each connection sends at `--msg-rate` msg/s, measures RTT to the correlated
echo. Uses a 16-char monotonic-id prefix for correlation (works with any
verbatim-echo server). RTT histogram, not handshake latency.

### Rhai DSL — multi-scenario, multi-protocol

```rhai
// bench.rhai — mix HTTP, SSE, and WS in one run
scenario("http-ping", |s| {
    s.step(GET("http://api/ping"));
});
scenario("sse-events", |s| {
    s.step(sse_hold("http://api/events", 1000, "60s"));
});
scenario("ws-chat", |s| {
    s.step(ws_echo_rtt("ws://api/chat", 100, 50));
});
rate("10k/s");
duration("60s");
```

```bash
zerobench run bench.rhai -c 200 -t 32
```

See `examples/chained.rhai` for header extraction, variable slots, and
auth flows.

## Archive layout

Every `measure` / `compare` / `curve` run writes to
`$ZEROBENCH_HOME/runs/<url_fingerprint>/<run_id>/` (falls back to
`$HOME/.zerobench` when `$ZEROBENCH_HOME` is unset):

```
plan.json       — compiled Plan (deterministic, sha256-hashed)
machine.json    — CPU / kernel / NUMA / cgroup / TLS / clock-resolution
env.json        — tool version, flags, --context pairs, timestamps
result.json     — full percentile ladder + per-run metrics
result.histlog  — HDR V2 compressed log (reads in HdrHistogram Plotter,
                  jHiccup, wrk2 pipeline)
INDEX.json      — schema versions + plan_hash + fingerprints
```

`run_id = <ISO8601>-<plan_hash[:8]>-<target_fp[:8]>` — copy-paste-able,
filesystem-safe, globally unique. `INDEX.json` is written last and acts as
a completion marker: its absence signals a partial / crashed run.

## Statistical compare

```bash
$ zerobench compare baseline/result.json current/result.json --compare-strategy auto

compare
  baseline          reqs  rate    p50    p99   p99.9   errors
  A                8,996  2999/s  528µs  1.10ms 1.16ms  0
  B                8,997  2999/s  552µs  1.11ms 1.14ms  0

    metric               A               B           Δ
      rate         2999.0/s        2999.0/s      +0.01%
       p99          1.10ms          1.11ms      +0.75%
       ...

AD two-sample: A²=6.252  T=4.549  p=0.0052  N=2999/3000  → differ (p < 0.05)
KS two-sample: D=0.0358  p=0.0426  N=2999/3000  → differ (p < 0.05)

bootstrap: 10000 resamples, seed 0xa5a5..., per-run N = A:3 / B:3
    metric           95% CI on Δ
      rate  [-1.00/s, +1.67/s]
       p99  [-4.1µs, +21.2µs]
```

- **Bootstrap CI** (N ≥ 3 runs per side) — 10k resamples at the run level,
  95% percentile CI on the absolute delta. Seed is derived from the plan
  + target so runs are byte-reproducible.
- **Anderson-Darling** — tail-sensitive distribution test (Scholz-Stephens 1987).
- **Kolmogorov-Smirnov** — classic ECDF max-gap test on HDR histograms.
- **Holm-Bonferroni** — family-wise p-value correction when gating on
  multiple metrics (opt-in: `--holm-bonferroni`).

Pick a strategy with `--compare-strategy {auto,ad,ks,none}`.

## Self-refusal gate

The tool refuses to run when it can't sustain the offered rate against a
loopback echo — "the tool is the instrument" and its ceiling must be above
the signals it measures.

```bash
$ zerobench measure http://api --rate 1M
[calibrate] self-check at 1_000_000 req/s against loopback (~2s)...
[calibrate] achieved 237_000/1_000_000 req/s (23.7%) — verdict: Refuse
error: client cannot sustain 1_000_000 req/s on this machine (achieved 237000).
       Lower --rate, pass --no-calibrate, or --force-overload.
```

`--force-overload` stamps the result archive with `force_overload: true` —
any `compare` against a non-overloaded baseline fails loudly.

## Crate layout

```
zerobench          (binary)  CLI entry, verbs, argument parsing
zerobench-core               Plan, Scenario, Step, stats, templates,
                             variables, histogram — the narrow waist
zerobench-runtime            LiveSnapshot, archive, calibrate,
                             fingerprint, machine, recorder, runner,
                             stop, tls, transport errors
zerobench-report             terminal / JSON / Prometheus renderers,
                             statistical compare (bootstrap / AD / KS)
zerobench-backends           every protocol backend: HTTP/1, HTTP/2,
                             cold-connect, SSE (hold/fanout/storm),
                             WebSocket (echo/hold/fanout/push) + dispatch
zerobench-dsl                Rhai scenario DSL — `scenario(...)`,
                             `sse_hold(...)`, `ws_echo_rtt(...)`, etc.
zerobench-tui                ratatui live dashboard (optional via `tui` feature)
zerobench-stub               test-only echo server for perf-gate (not published)
```

Adding a new backend: one module under `zerobench-backends/src/`,
one arm in `dispatch::run_one_protocol`, one `Step::X(XPlan)` variant
in `zerobench-core::plan`. No feature flags, no dyn dispatch.

## Design docs

- [`docs/PHILOSOPHY.md`](docs/PHILOSOPHY.md) — design principles (measurement apparatus vs load generator)
- [`docs/design-v0.1.0.md`](docs/design-v0.1.0.md) — type-level spec

## License

MIT
