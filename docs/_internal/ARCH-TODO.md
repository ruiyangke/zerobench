# ARCH-TODO — rewrite phase log

**STATUS (2026-04-20):** all phases complete. This file is retained as
a historical record of the rewrite; for the current architecture see
`docs/ARCH-REVIEW-2026-04-20.md` §4 and the module-level docs in each
crate.

Companions: `ARCH-REVIEW-2026-04-20.md` (the rewrite plan, historical),
`ARCH-TAGS.md` (marker scheme, historical).

---

## Completion log

| Phase | Commit | Description |
|---|---|---|
| Phase 0 — annotate | `af7f3ed`, `4689726`, `4d68add`, `b9a11ad`, `d1d08bf` | ARCH-TAGS scheme + file-level dispositions across all crates |
| Phase 1 — split core | `2e0d358` | zerobench-core split into core + runtime + report |
| Phase 1c — transport split | `5164722` | TransportError extracted into zerobench-runtime |
| Phase 2a — backends crate | `d909c9b` | http + sse + ws consolidated into zerobench-backends |
| Phase 2b — Recorder | `9264423` | Recorder struct collapses the triple-record antipattern |
| Phase 2b.fix — cold_connect | `a937dff` | route errors through Recorder for per-scenario live writes |
| Phase 2c — dispatch | `3fd583b` | `run_plan` becomes the single protocol-dispatch entry point |
| Phase 3a — error-unify | `763f922` | ad-hoc backend error enums → core's `TransportError` |
| Phase 3b — fanout-core | `056c259` | SSE/WS fanout share `backends::fanout_core` |
| Phase 4a.1 — rhai → dsl | `67e8cbe` | crate rename: zerobench-rhai → zerobench-dsl |
| Phase 4a.2 — define_builder! | `8a5c24b` | builders.rs collapsed via macro (2,300 → ~800 LoC) |
| Phase 4b — PlanBuilder | `446146a` | CLI + DSL unified on shared `PlanBuilder` |
| Phase 4c — runner | `2c5e6aa` | measure/curve/main extract into `zerobench_runtime::runner` |
| Phase 5 — polish | (this commit) | strip stale ARCH markers; dead-code sweep |

Every inline `ARCH(recorder|error-unify|dispatch|builder-unify|fanout-core|rhai-macro|feature-delete|keep)` marker is now gone; the `ARCH STATUS` module headers have been cleaned. `grep -r 'ARCH(' zerobench-*/src/` returns nothing.

---

## Final state

- Crates: `zerobench-core` (vocabulary), `zerobench-runtime` (runtime infra, crown jewels), `zerobench-backends` (http/sse/ws), `zerobench-report` (rendering), `zerobench-dsl` (Rhai DSL), `zerobench-tui` (terminal dashboard), `zerobench-cli` (user-facing binary).
- The only remaining Cargo feature flag is `tui` on `zerobench-cli` (optional dashboard front-end).
- Crown jewels marked in the module docs (not via a tag): `runtime::live_snapshot` (16-shard sharded mutex), `backends::sse::line_parser` (WHATWG line framer), `backends::ws::frame` (RFC 6455 §5.2 codec).

---

*Original roadmap regenerated on Phase 0 completion; supersede by this completion log.*
