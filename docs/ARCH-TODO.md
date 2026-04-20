# ARCH-TODO — live index of rewrite markers

**Generated from `grep ARCH(` after the Phase 0 annotation pass.**
Not hand-authored — regenerate with the grep recipes at the bottom
of `docs/ARCH-TAGS.md` or the commands at the end of this file.

Companions: `ARCH-REVIEW-2026-04-20.md` (the rewrite plan), `ARCH-TAGS.md` (marker scheme).

---

## Counts

### File-level dispositions

| Disposition | Files | Meaning |
|---|---|---|
| `KEEP` | **15** | No architectural change (crown jewels + infra already in the right place) |
| `MOVE` | **30** | Moves wholesale to a new crate |
| `SPLIT` | **1** | Type splits across crates |
| `REWRITE` | **8** | Significant rewrite in its phase |
| `DELETE` | **3** | Removed entirely (the three lib.rs files whose crates dissolve into backends) |
| `RENAME` | **1** | `zerobench-rhai` → `zerobench-dsl` |
| **Total** | **58** | Every source file touched by the rewrite |

### Inline action markers

| Tag | Sites | Concern |
|---|---|---|
| `ARCH(recorder)` | **11** | Triple-record antipattern — collapses to `recorder.record(sid, sample)` |
| `ARCH(error-unify)` | **5** | Ad-hoc `ColdErr`/`SessionErr`/`RecvErr` → core's `TransportError` |
| `ARCH(dispatch)` | **4** | N-way protocol match → one call to `backends::run_scenario` |
| `ARCH(builder-unify)` | **5** | CLI + Rhai duplicate plan construction → shared typed `PlanBuilder` |
| `ARCH(fanout-core)` | **4** | SSE / WS fanout duplicated helpers → `backends::fanout_core` |
| `ARCH(feature-delete)` | **2** | `#[cfg(feature = "…")]` guards that disappear |
| `ARCH(rhai-macro)` | **1** | 2,300-LoC builders.rs → macro-collapse to ~800 |
| `ARCH(keep)` | **8** | Crown-jewel markers — do not rewrite on move |
| **Total markers** | **40** | |

---

## Dispositions by crate

### `zerobench-core` (20 files — vocabulary crate, stays lean)

| File | Disposition |
|---|---|
| `archive.rs` | MOVE → zerobench-runtime::archive |
| `calibrate.rs` | MOVE → zerobench-runtime::calibrate |
| `compare.rs` | MOVE → zerobench-report::compare (crown jewel — no rewrite) |
| `fingerprint.rs` | MOVE → zerobench-runtime::fingerprint |
| `histogram.rs` | KEEP |
| `json_scan.rs` | MOVE → zerobench-runtime::json_scan |
| `lib.rs` | REWRITE — public surface shrinks to vocabulary only |
| `live_snapshot.rs` | MOVE → zerobench-runtime::live_snapshot (crown jewel — no rewrite of the sharding) |
| `machine.rs` | MOVE → zerobench-runtime::machine |
| `plan.rs` | KEEP — closed-enum Step + all *Plan structs stay here |
| `report.rs` | MOVE → zerobench-report (split into terminal/json/prometheus) |
| `request_file.rs` | KEEP — Plan-adjacent |
| `rng.rs` | KEEP — stays in core; `template` + `scenario_context` depend on `BenchRng` |
| `scenario_context.rs` | KEEP — Plan-adjacent |
| `stats.rs` | KEEP — TaskStats + typed SseExtras/WsExtras |
| `stop.rs` | MOVE → zerobench-runtime::stop |
| `template.rs` | KEEP |
| `tls.rs` | MOVE → zerobench-runtime::tls |
| `transport.rs` | SPLIT — Target/TransportOpts stay; TransportError → runtime |
| `var.rs` | KEEP — Plan-adjacent |

### `zerobench-http` (7 files, all → zerobench-backends::http)

| File | Disposition | Notes |
|---|---|---|
| `cold_connect.rs` | MOVE → backends::http::cold_connect | `ARCH(error-unify)` on `ColdErr`; `ARCH(recorder)` on hot path |
| `lib.rs` | DELETE | Crate dissolves; `ARCH(feature-delete)` removes mio-h1/mio-h2 cfg gates |
| `mio_h1.rs` | MOVE → backends::http::mio_h1 | `ARCH(recorder)` on response-complete site |
| `mio_h2.rs` | MOVE → backends::http::mio_h2 | `ARCH(recorder)` on body-done site |
| `mio_tls.rs` | MOVE → backends::http::mio_tls | |
| `raw_h1_common.rs` | MOVE → backends::http::raw_h1_common | Crown jewel helpers (assertions, extractions, CL parsing) |
| `simple_post.rs` | MOVE → backends::http::simple_post | `ARCH(fanout-core)` — primary caller is fanout triggers |

### `zerobench-sse` (5 files, all → zerobench-backends::sse)

| File | Disposition | Notes |
|---|---|---|
| `fanout.rs` | MOVE → backends::sse::fanout | `ARCH(fanout-core)` — extract run_trigger_loop / fire_trigger / render_template |
| `hold.rs` | MOVE → backends::sse::hold | `ARCH(recorder)` on event path |
| `lib.rs` | DELETE | Crate dissolves |
| `line_parser.rs` | MOVE → backends::sse::line_parser | CROWN JEWEL — WHATWG line framer |
| `reconnect_storm.rs` | MOVE → backends::sse::reconnect_storm | `ARCH(error-unify)` on SessionErr |

### `zerobench-ws` (7 files, all → zerobench-backends::ws)

| File | Disposition | Notes |
|---|---|---|
| `conn.rs` | MOVE → backends::ws::conn | |
| `echo_rtt.rs` | MOVE → backends::ws::echo_rtt | `ARCH(error-unify)` on RecvErr; `ARCH(recorder)` on RTT path |
| `fanout.rs` | MOVE → backends::ws::fanout | `ARCH(fanout-core)` — duplicates sse/fanout.rs |
| `frame.rs` | MOVE → backends::ws::frame | CROWN JEWEL — RFC 6455 §5.2 codec |
| `handshake.rs` | MOVE → backends::ws::handshake | CROWN JEWEL — RFC 6455 §4 Upgrade |
| `hold.rs` | MOVE → backends::ws::hold | `ARCH(recorder)` |
| `lib.rs` | DELETE | Crate dissolves |
| `server_push_rtt.rs` | MOVE → backends::ws::server_push_rtt | `ARCH(recorder)` |

### `zerobench-rhai` → `zerobench-dsl` (crate rename, 4 files)

| File | Disposition | Notes |
|---|---|---|
| `lib.rs` | RENAME → zerobench-dsl | |
| `builders.rs` | REWRITE | `ARCH(rhai-macro)` — 2,300 → ~800 LoC via `define_builder!`. `ARCH(builder-unify)` — consume shared PlanBuilder |
| `parse.rs` | MOVE → zerobench-dsl::parse | |
| `error.rs` | MOVE → zerobench-dsl::error | |

### `zerobench-cli` (10 files)

| File | Disposition | Notes |
|---|---|---|
| `cli_args.rs` | KEEP (prune `#[cfg(feature=…)]`) | |
| `diff.rs` | KEEP (trim) | Possibly dead code vs verbs/diff.rs |
| `main.rs` | REWRITE | `ARCH(dispatch)` — THREE 3-way matches → one `backends::run_scenario` |
| `plan_from_cli.rs` | REWRITE | `ARCH(builder-unify)` |
| `verbs/calibrate.rs` | KEEP | Already minimal |
| `verbs/curve.rs` | REWRITE | 603 LoC → ~100 via shared runner |
| `verbs/diff.rs` | REWRITE (trim) | |
| `verbs/measure.rs` | REWRITE | 1,468 LoC → ~150. `ARCH(dispatch)` ×2, `ARCH(builder-unify)`, `ARCH(feature-delete)` |
| `verbs/mod.rs` | KEEP | |
| `verbs/probe.rs` | REWRITE (trim) | |

### `zerobench-tui` (12 files — all KEEP)

| File | Disposition |
|---|---|
| `lib.rs` | KEEP |
| `state.rs` | KEEP (consumes runtime::LiveTick) |
| `export.rs` | KEEP |
| `ui/*.rs` (8 files) | KEEP (single marker on `ui/mod.rs` covers the subdir) |

---

## Phase roadmap → marker clearance

Every phase removes the markers it completes. `grep ARCH(` at the end of the rewrite must return zero.

| Phase | Work | Markers cleared |
|---|---|---|
| **Phase 1** (week 1) | Split core. Create zerobench-types, zerobench-runtime, zerobench-report. | All `MOVE → zerobench-runtime::*` (9 files). All `MOVE → zerobench-report::*` (2 files). 1 `SPLIT` (transport). 1 `REWRITE` (lib.rs). |
| **Phase 2** (week 2) | Create zerobench-backends crate. Dispatch function. Recorder struct. Port mio_h1 as reference. | 0 markers cleared; infrastructure for phase 3. |
| **Phase 3** (week 3) | Port all backends. Extract fanout-core. Unify errors. | All `MOVE → zerobench-backends::*` (17 files). 3 `DELETE` (http/sse/ws lib.rs). All `ARCH(recorder)` (11). All `ARCH(error-unify)` (5). All `ARCH(fanout-core)` (4). All `ARCH(dispatch)` inside backends. |
| **Phase 4** (week 4) | Rewrite measure/curve/probe/diff verbs. Unify PlanBuilder. Rename rhai → dsl. Collapse rhai-macro. | All `ARCH(dispatch)` in CLI. All `ARCH(builder-unify)` (5). `ARCH(rhai-macro)` (1). RENAME (1). 4 `REWRITE` (main.rs + 3 verbs). |
| **Phase 5** (week 5) | Polish. Kill feature flags. Error strictness. Delete core facade. | All `ARCH(feature-delete)` (2). All `ARCH(keep)` markers get promoted to normal module docs (markers themselves removed). Remaining `REWRITE` / `KEEP (prune)` items. |
| **Phase 6+** (optional) | StatsD exporter. JSONL sink. OTel (maybe). | New code, no markers to clear. |

---

## Verification recipes

Run from the workspace root.

```bash
# Which files are affected, and how
grep -rn "^//! ARCH STATUS:" zerobench-*/src/

# Phase-by-phase clearance check (should approach zero as rewrite progresses)
grep -rc "ARCH(" zerobench-*/src/ | awk -F: '{s+=$2} END {print "inline markers:", s}'

# By concern
for tag in recorder dispatch error-unify fanout-core builder-unify rhai-macro feature-delete keep; do
  n=$(grep -rh "ARCH($tag)" zerobench-*/src/ | wc -l)
  printf "%-18s %d\n" "$tag" "$n"
done

# Zero-check at end of rewrite
test -z "$(grep -r 'ARCH(' zerobench-*/src/)" && echo "Phase 0 markers all cleared" \
  || echo "Still outstanding."
```

---

*Index regenerated on Phase 0 completion. Keep updated at every phase boundary so the "done" line converges toward zero.*
