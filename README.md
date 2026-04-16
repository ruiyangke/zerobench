# zerobench

Fast, correct, modern HTTP benchmarking — open-loop by default, HDR-precise
(nanosecond), compio/io_uring-native, HTTP/1/2/3 + WebSocket + SSE, with
compile-time Rhai scenario DSL.

> **Status: v0.0.1 — pre-alpha.** Design is locked (see [`docs/design.md`](docs/design.md)); implementation is in progress.

## Why another benchmark tool?

`wrk` is great but measurement-model dated (closed-loop service-time latency,
no open-loop / constant-rate mode → coordinated omission under load). `wrk2`
fixed the measurement model but never really caught on. `k6` is full-featured
but heavy and JS-per-request bleeds throughput. `oha` is friendly but limited.

zerobench aims at a specific point in the design space:

- **Open-loop by default.** Constant-rate scheduler, coordinated-omission-free latency. Closed-loop (`--saturate`) opt-in.
- **Pure-Rust hot path.** Scenarios described in Rhai (or CLI) get compiled to a `Plan` data structure; the engine executes in 100% Rust with zero interpreter on the critical path.
- **Multi-protocol, one engine.** H1/H2/H3/WS/SSE all ride the same dispatcher, rate controller, and recorder.
- **Low-level hyper, not reqwest.** We own the connection pool, measure TTFB, count wire bytes exactly.
- **HDR histograms in nanoseconds.** No information loss at sub-millisecond tail.

## Quick start

*(when v0.0.1 lands)*

```bash
# Closed-loop (wrk-like): saturate with 300 conns for 30s
zerobench --saturate -c 300 -d 30s http://api/events

# Open-loop: constant 10k req/s for 30s
zerobench -r 10k -d 30s http://api/events

# Scripted multi-scenario with independent rates
zerobench run ./bench.rhai
```

## Architecture

```
      Phase 1 — compile (once)        Phase 2 — execute (hot path)
      ┌─────────────────────┐         ┌──────────────────────────┐
      │  CLI / Rhai / .http │         │  RateScheduler per        │
      │           ↓          │         │    scenario → tokens →   │
      │  Plan { scenarios,   │   ─→    │  Worker pool → Transport │
      │   templates, rates,  │         │    (H1/H2/H3/WS/SSE) →   │
      │   vars, checks }     │         │  Extract + Check →       │
      │                     │         │  Recorder → Report       │
      └─────────────────────┘         └──────────────────────────┘
                                         100% Rust, no interpreter
```

See [`docs/design.md`](docs/design.md) for the full spec.

## Features

- `h1` (default) — HTTP/1.1
- `h2` — HTTP/2
- `h3` — HTTP/3 / QUIC
- `ws` (default) — WebSocket
- `sse` (default) — Server-Sent Events
- `tui` — live dashboard via ratatui
- `script` — Rhai scenario DSL
- `all` — enable everything

```bash
cargo install zerobench                                   # H1 + WS + SSE
cargo install zerobench --features "h2 h3 tui script"     # everything
```

## Crate layout

```
zerobench-core/    Plan, Template, Transport, Dispatcher, Recorder
zerobench-http/    H1/H2/H3 via hyper + cyper-core
zerobench-ws/      RFC 6455 client on compio
zerobench-sse/     SSE line framing on zerobench-http
zerobench-rhai/    Compile-time scenario DSL
zerobench-tui/     ratatui dashboard
zerobench-cli/     binary (installs as `zerobench`)
```

## License

MIT
