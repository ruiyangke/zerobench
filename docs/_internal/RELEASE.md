# Release playbook — zerobench 0.1.0

All prep work is committed (commits `6fbdbce` and `d378236` on `main`).
What remains is mechanical: push to GitHub, then publish the seven
crates to crates.io in dependency order.

## Pre-flight (done)

- [x] Version pinned at `0.1.0` (workspace + all path deps)
- [x] `zerobench-stub` marked `publish = false`
- [x] Every publishable crate has `readme`, `keywords`, `categories`,
      `homepage` metadata
- [x] README + CHANGELOG.md match the shipped feature surface
- [x] `--version` banner prints truthful protocol + feature list
- [x] bench.sh + perf-gate.sh use current feature names
- [x] `cargo build --workspace --all-features` clean
- [x] `cargo test --workspace --all-features`: 702 passed / 0 failed
- [x] `cargo doc --workspace --no-deps --all-features`: 0 warnings
- [x] `cargo publish --dry-run -p zerobench-core` succeeds
      (downstream crates can only be dry-run'd after their deps land)

## 1. Push to GitHub

The repository URL in every crate's metadata is
`https://github.com/zeroship-dev/zerobench`. Create that repo (or a
fork of this local tree), then:

```bash
cd ~/Projects/zerobench
git remote add origin git@github.com:zeroship-dev/zerobench.git
git push -u origin main
```

If you push to a different org/name, **update the URL** in
`Cargo.toml` (`workspace.package.repository` and `homepage`) before
publishing — crates.io reads the metadata verbatim and the link is
hard to change after the fact without yanking.

## 2. Publish to crates.io in dependency order

Each crate depends on the previous ones. After each `cargo publish`
succeeds, wait ~30–60 seconds for the index to propagate, or cargo
will fail to resolve the freshly-published dep.

```bash
cd ~/Projects/zerobench

cargo publish -p zerobench-core       && sleep 60
cargo publish -p zerobench-runtime    && sleep 60
cargo publish -p zerobench-report     && sleep 60
cargo publish -p zerobench-backends   && sleep 60
cargo publish -p zerobench-dsl        && sleep 60
cargo publish -p zerobench-tui        && sleep 60
cargo publish -p zerobench
```

`zerobench-stub` is `publish = false` and won't be uploaded.

## 3. Tag the release

```bash
git tag -a v0.1.0 -m "zerobench 0.1.0 — initial release"
git push origin v0.1.0
```

Consider creating a GitHub Release pointing at the tag with the
CHANGELOG.md entry as the body.

## 4. Verify

```bash
# Binary install works from the wire
cargo install zerobench
zerobench --version

# Package page looks sane
open https://crates.io/crates/zerobench
open https://docs.rs/zerobench

# Library usage works
cargo new --bin zerobench-smoke && cd zerobench-smoke
cargo add zerobench-core
cat > src/main.rs << 'EOF'
use zerobench_core::plan::Plan;
fn main() {
    let p = Plan::new();
    println!("duration = {:?}", p.duration);
}
EOF
cargo run
```

## Rollback

Once a version is published to crates.io it **cannot be deleted**,
only yanked. To yank:

```bash
cargo yank --version 0.1.0 zerobench
cargo yank --version 0.1.0 zerobench-core
# ... and the others
```

Yanking prevents new projects from depending on that version but
doesn't break existing lockfile-pinned builds. Then publish 0.1.1
with the fix.

## Semver going forward

- **0.1.x** — bugfix and doc-only releases.
- **0.2.0** — any breaking change to `Plan`/`Scenario`/`Step`/the
  archive format (`result.json` / `plan.json` schema versions).
- **1.0.0** — signals stability commitment. Not before the archive
  format has seen real downstream users.

Schema versions in `INDEX.json` are the ground truth for archive
compatibility; bump them independently when needed and the tool
will refuse to read incompatible runs.
