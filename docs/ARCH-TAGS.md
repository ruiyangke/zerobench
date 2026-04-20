# ARCH annotation scheme

Markers for the architecture-v2 rewrite (see `ARCH-REVIEW-2026-04-20.md`).
Every file touched by the rewrite carries a file-level **disposition**;
specific smell sites carry inline **action markers**.

Both are greppable. Use the grep recipes at the bottom of this doc to
enumerate outstanding work per concern.

## Locked target (recap)

- **6 crates:** `zerobench-{core, runtime, report, backends, dsl, cli}`
- **`zerobench-stub` deleted** — inline mio/httparse stubs in tests instead
- **`zerobench-rhai` renamed to `zerobench-dsl`**
- **All protocol backends (`http`, `sse`, `ws`) merge into `zerobench-backends`**
- **Static dispatch everywhere** — `Step` is a closed enum in core; no traits for polymorphism
- **No feature flags** — everything always built
- **No backcompat** — rename freely

## File-level disposition

Every non-trivial source file gets one of these as the first module doc.

```rust
//! ARCH STATUS: <DISPOSITION>
//!
//! Part of the arch-v2 rewrite. See docs/ARCH-REVIEW-2026-04-20.md
//! §<section> / docs/ARCH-TAGS.md.
```

Dispositions:

| Tag | Meaning |
|---|---|
| `KEEP` | Stays in current crate, unchanged architecture. Implementation may be refined but module boundary is correct. |
| `MOVE → <crate>::<mod>` | Moves wholesale to a new crate. Implementation unchanged. |
| `SPLIT` | Splits across multiple crates. Follow-up inline markers indicate the split boundary. |
| `REWRITE` | Significant internal rewrite. Public API shape likely to change. |
| `DELETE` | Removed entirely. |
| `RENAME → <new>` | File/module renamed in place. |

## Inline action markers

At specific smell sites within a file. Format:

```rust
// ARCH(<tag>): <what to do> — see ARCH-REVIEW §<section>
```

Tag vocabulary:

| Tag | Marks |
|---|---|
| `move` | Specific type / block that moves (when file SPLITs) |
| `split` | Type definition that splits across crates |
| `keep` | Crown jewel — do not rewrite (see ARCH-REVIEW §1) |
| `rewrite` | Block that needs rewriting in a later phase |
| `delete` | Block / type / field that goes away |
| `recorder` | Triple-record site (stats + live + live_scenario); collapses to `Recorder::record` — §4.3 |
| `dispatch` | N-way protocol switch; becomes a single `match step { … }` in `zerobench-backends` — §4.1 |
| `builder-unify` | Duplicate plan-construction between CLI flags and Rhai DSL; consolidate — §4.5 |
| `fanout-core` | Logic to extract into `zerobench-backends::fanout_core` — §4.6 |
| `error-unify` | Ad-hoc error enum (ColdErr/SessionErr/RecvErr); replace with `TransportError` — §4.7 |
| `rhai-macro` | Rhai builder boilerplate; collapse via `define_builder!` macro — §B3 |
| `feature-delete` | `#[cfg(feature = "…")]` guard that goes away (no feature flags in target) |

## Examples

### File-level disposition

```rust
//! ARCH STATUS: MOVE → zerobench-runtime::archive
//!
//! Part of the arch-v2 rewrite. Archive I/O is runtime infrastructure;
//! moves alongside LiveSnapshot, StopSignal, calibrate, tls. See
//! docs/ARCH-REVIEW-2026-04-20.md §7.
```

```rust
//! ARCH STATUS: KEEP — crown jewel, do not rewrite
//!
//! HDR+bootstrap+AD+KS analysis. Moves to zerobench-report at most;
//! code stays byte-for-byte. See ARCH-REVIEW §1.
```

```rust
//! ARCH STATUS: SPLIT
//!
//! Types stay in core (TaskStats, SseExtras, WsExtras, Sample);
//! live-recording logic moves to zerobench-runtime::live_snapshot.
```

### Inline markers

At a triple-record site:

```rust
// ARCH(recorder): collapses to `recorder.record(sid, sample)` — §4.3
stats.record(scenario_id, latency, ttfb, bytes_sent, bytes_recv);
if let Some(live) = live {
    let ns = latency.as_nanos() as u64;
    live.record(ns, bytes_sent, bytes_recv);
    live.record_scenario(scenario_id, ns, bytes_sent, bytes_recv);
}
```

At an N-way protocol dispatch:

```rust
// ARCH(dispatch): replaced by `zerobench_backends::run_scenario(step, ctx)` — §4.1
match first_step {
    Some(Step::HttpColdConnect(_)) => run_cold_connect_from_plan_threaded(…),
    #[cfg(feature = "sse")]
    Some(Step::SseHold(_))         => run_sse_hold_from_plan_threaded(…),
    …
}
```

At an ad-hoc error enum:

```rust
// ARCH(error-unify): replace with core's TransportError — §4.7
#[derive(Debug)]
enum ColdErr {
    ConnectIo(io::Error),
    Tls(String),
    Write(io::Error),
    Read(io::Error),
    Timeout,
    BadResponse(&'static str),
}
```

At a feature gate:

```rust
// ARCH(feature-delete): no feature flags in target; always built
#[cfg(feature = "sse")]
Some(Step::SseHold(_)) => { … }
```

## Grep recipes

Progress metrics during the rewrite:

```bash
# All markers
grep -rn "ARCH(" zerobench-*/src/ zerobench-*/tests/

# File-level dispositions
grep -rn "ARCH STATUS:" zerobench-*/src/

# By concern
grep -rn "ARCH(dispatch)"       # N-way switches that collapse
grep -rn "ARCH(recorder)"       # Triple-record sites
grep -rn "ARCH(error-unify)"    # Ad-hoc error enums
grep -rn "ARCH(rhai-macro)"     # Rhai boilerplate
grep -rn "ARCH(fanout-core)"    # Shared fanout sites
grep -rn "ARCH(builder-unify)"  # CLI/Rhai duplication
grep -rn "ARCH(feature-delete)" # Feature-flag guards

# By destination crate
grep -rn "ARCH STATUS: MOVE → zerobench-runtime"
grep -rn "ARCH STATUS: MOVE → zerobench-report"
grep -rn "ARCH STATUS: MOVE → zerobench-backends"
grep -rn "ARCH STATUS: KEEP"
grep -rn "ARCH STATUS: SPLIT"
grep -rn "ARCH STATUS: DELETE"

# How much work per file
grep -rc "ARCH(" zerobench-*/src/ | sort -t: -k2 -rn | head

# Total outstanding
grep -rc "ARCH(" zerobench-*/src/ | awk -F: '{s+=$2} END {print s}'
```

## Lifecycle

1. Annotation pass (this doc + 5 commits) — see ARCH-REVIEW §10 "Phase 0".
2. Each rewrite phase removes the markers it completes. Annotations are
   ephemeral; when the work is done, the marker goes too.
3. Final rewrite commit does a cleanup pass:
   `grep -rn "ARCH(" zerobench-*/src/` should return zero.
4. This `ARCH-TAGS.md` doc + `ARCH-REVIEW-2026-04-20.md` get archived
   under `docs/history/` once the rewrite completes.

## Canonical references

- **`ARCH-REVIEW-2026-04-20.md`** — the architectural critique and rewrite
  proposal. Section numbers in inline markers refer to this doc.
- **This file** — the marker scheme itself.
- **`ARCH-TODO.md`** (generated) — live index of outstanding markers,
  regenerated from `grep ARCH(`. See `scripts/arch_todo.sh` (to be added).
