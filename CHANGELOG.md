# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-04-21

Initial public release.

### Verbs

- `probe URL` — 5-second smoke test, no archive
- `measure URL` — rigorous steady-state measurement, calibrate-gated, auto-archived
- `calibrate` — client-side ceiling probe against in-process loopback echo
- `curve URL` — saturation-curve sweep; reports the knee
- `compare A.json B.json` — bootstrap CI + Anderson-Darling + Kolmogorov-Smirnov
- `diff A.json B.json` — raw percentile delta table (simple gate)
- `run script.rhai` — multi-scenario / multi-protocol plan from a Rhai script

### Protocols

- **HTTP/1.1** — mio/epoll, static fast-path, keep-alive pipelining, `Transfer-Encoding: chunked` response support
- **HTTP/2** — h2 crate manually polled from mio
- **Server-Sent Events** — hold mode (N subscribers × events/s), fanout (broadcast RTT), reconnect-storm
- **WebSocket** — echo-RTT (monotonic-id prefix / ping-pong / substring / first-text correlation), hold, server-push-RTT, fanout
- **HTTP cold-connect** — fresh TCP per op, for connection-setup latency measurement

### Measurement

- HDR histograms end-to-end (nanosecond resolution, 3-sig-fig)
- Open-loop, coordinated-omission-free latency
- Per-protocol primary histograms (chunk-gap for SseHold, RTT for WsEchoRtt, broadcast-rtt for fanouts)
- Client self-check with 5 µs p99 scheduler-jitter floor
- Refuses to run when the client can't sustain the offered rate; `--force-overload` escape stamps the archive

### Statistical compare

- 10,000-resample run-level bootstrap (95% CI on deltas)
- Anderson-Darling two-sample (Scholz-Stephens 1987)
- Kolmogorov-Smirnov two-sample
- Holm-Bonferroni family-wise correction (opt-in)
- Deterministic: seed derived from plan + target

### Archive

- `$ZEROBENCH_HOME/runs/<url_fp>/<run_id>/` (fallback `$HOME/.zerobench`)
- `plan.json`, `machine.json`, `env.json`, `result.json`, `result.histlog`, `INDEX.json`
- HDR V2 compressed log for HdrHistogram Plotter / jHiccup / wrk2 pipeline interop
- `run_id = <ISO8601>-<plan_hash[:8]>-<target_fp[:8]>`

### Rhai DSL

- Top-level: `scenario(name, body)`, `duration("10s")`, `rate("100k/s")`, `saturate(N)`, `runs(3)`, `threads(16)`, `plan_name(...)`, `env("VAR", "default")`
- Request builders: `GET/POST/PUT/DELETE/PATCH/HEAD/OPTIONS(url)` with `.header`, `.body`, `.json`, `.body_file`, `.expect_status`, `.extract_header`, `.extract_status`, `.cold_connect`, ...
- SSE/WS builders: `sse_hold`, `sse_fanout`, `sse_reconnect_storm`, `ws_echo_rtt`, `ws_hold`, `ws_server_push_rtt`, `ws_fanout`
- Variable slots for auth flows: `slot("token")` + `.extract_header("Authorization", token)`

### Architecture

- 8-crate workspace: `zerobench` (CLI), `zerobench-core` (types), `zerobench-runtime`, `zerobench-report`, `zerobench-backends`, `zerobench-dsl`, `zerobench-tui`, `zerobench-stub` (internal)
- No dynamic dispatch; closed-world `Step` enum; every backend dispatched by one `run_plan` match
- No feature flags except `tui` (dashboard is optional)
- `#[deny(unsafe_code)]` at workspace level; five `unsafe` blocks scoped to `zerobench_runtime::machine` for libc sysctl

### Performance floor

zerobench/wrk ≥ 1.20× against the bundled stub server on loopback (enforced
by `perf-gate.sh`; historical median ~1.20).
