# Contributing to zerobench

Thanks for considering a contribution! This tool aims to be a
*measurement apparatus* — small, predictable, and faithful to the
numbers it reports. Changes are easier to accept when they stay on
that side of the line.

## Ground rules

- **Read [`docs/PHILOSOPHY.md`](docs/PHILOSOPHY.md) first.** It says
  what the tool is and isn't. Features that contradict it tend to be
  rejected regardless of code quality.
- **Reproduce with a test.** Every bug fix lands with a regression
  test; every feature lands with a behavioural test. Unit tests live
  next to the code; integration tests live in each crate's `tests/`.
- **Benchmark perf-sensitive changes.** `perf-gate.sh` enforces
  `zerobench ≥ 1.20× wrk` against the bundled stub. CI doesn't run it
  (needs real network), but patches that touch the hot path (mio
  backends, histogram record, recorder) should include a local
  before/after.
- **No async.** The tool is mio/epoll-based by design. Do not pull in
  tokio/async-std as a dependency unless you've discussed it first.
  The `tokio` entry in `zerobench-backends/Cargo.toml` is `io-util`
  only — traits, no runtime.
- **No new feature flags.** The only cargo feature is `tui`. If
  you find yourself adding `#[cfg(feature = "…")]`, push back on
  your own design.

## Workflow

```bash
cargo build --workspace --all-features
cargo test  --workspace --all-features
cargo fmt --all
cargo clippy --workspace --all-features -- -D warnings
cargo doc --workspace --no-deps --all-features   # must be warning-free
```

CI runs the first four (`.github/workflows/ci.yml`). The doc check is
enforced by convention; run it locally.

## Commits

- Present-tense summary, no "will" / "should" in the subject line.
- Reference the crate in the prefix when the change is crate-scoped
  (`fix(http): …`, `feat(dsl): …`, `refactor(arch): …`,
  `docs(readme): …`). Overall-architecture touches use `refactor(arch)`.
- One logical change per commit. The `ARCH` refactor commits in the
  history are a reasonable template for large changes.

## Pull requests

Include:
1. What changed and why (not just what — CLAUDE.md agents are good at
   what, humans need why).
2. Test coverage (new tests or the fixture that caught it).
3. Any perf delta, if you touched the hot path.
4. A note if the change touches the archive schema
   (`INDEX.json::schema_versions.*`) — those need a bump + CHANGELOG
   entry.

## Code of conduct

Be civil. Personal attacks or harassment in issues / PRs / commits
result in a block.
