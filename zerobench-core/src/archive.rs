//! Run archive — `$ZEROBENCH_HOME/runs/<url_fp>/<run_id>/` layout.
//!
//! Implements `docs/design-v0.1.0.md` §7.1 and `docs/PHILOSOPHY.md`
//! §8.1. Every `measure` / `compare` / `curve` / `soak` / `watch` run
//! (and `calibrate` for its own archival path) writes a set of
//! sidecars into a deterministic directory whose name is derived from
//! the plan + target fingerprints (§Phase 3) and the run timestamp.
//!
//! # Layout
//!
//! ```text
//! $ZEROBENCH_HOME/
//!   runs/
//!     <url_fingerprint>/
//!       <run_id>/
//!         plan.json         — deterministic compile of source
//!         result.json       — full Summary + metadata (Phase 5b)
//!         result.histlog    — HDR V2 compressed log (Phase 5b)
//!         warmup.histlog    — warmup-phase histogram (Phase 5b)
//!         machine.json      — full machine fingerprint
//!         env.json          — tool version + flags + context + timestamps
//!         INDEX.json        — schema_versions + grouping metadata
//!         stdout.txt, stderr.txt  — captured outputs (caller's responsibility)
//! ```
//!
//! # Environment
//!
//! `$ZEROBENCH_HOME` — user override.
//! `$HOME/.zerobench` — fallback when `$ZEROBENCH_HOME` is unset.
//! In-test overrides pass a `PathBuf` directly.
//!
//! # Schema versions
//!
//! Every JSON artifact carries its own `schema_version`. The
//! top-level `INDEX.json` collects them into a `schema_versions`
//! block so a reader can version-check each sibling without opening
//! it. See `docs/PHILOSOPHY.md` §8.1.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::machine::MachineFingerprint;
use crate::plan::Plan;

// ---------------------------------------------------------------------------
// Archive root
// ---------------------------------------------------------------------------

/// Resolved archive root — `$ZEROBENCH_HOME` or `$HOME/.zerobench`.
///
/// Construct via [`Archive::resolve`] (reads env) or [`Archive::at`]
/// (pass an explicit path, useful in tests).
#[derive(Debug, Clone)]
pub struct Archive {
    root: PathBuf,
}

impl Archive {
    /// Resolve the archive root from the environment.
    ///
    /// Order of precedence:
    ///
    /// 1. `$ZEROBENCH_HOME` — explicit user override.
    /// 2. `$HOME/.zerobench` — per-user default on Unix.
    /// 3. `./.zerobench` — fallback when neither env var is set
    ///    (mostly hits in CI on sandboxes with no $HOME).
    ///
    /// The directory is **not** created here — [`ArchiveWriter::begin`]
    /// creates the specific run directory on demand.
    pub fn resolve() -> Self {
        let root = std::env::var_os("ZEROBENCH_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|h| {
                    let mut p = PathBuf::from(h);
                    p.push(".zerobench");
                    p
                })
            })
            .unwrap_or_else(|| PathBuf::from(".zerobench"));
        Self { root }
    }

    /// Use an explicit directory as the archive root. Intended for
    /// tests.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { root: path.into() }
    }

    /// The root path; `join("runs/...")` to derive sub-paths.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The `runs/` subdirectory — top-level grouping.
    pub fn runs_dir(&self) -> PathBuf {
        self.root.join("runs")
    }

    /// Directory for a given `url_fingerprint` — stable across runs.
    pub fn url_dir(&self, url_fp: &str) -> PathBuf {
        self.runs_dir().join(url_fp)
    }

    /// Directory for a specific run — `runs/<url_fp>/<run_id>/`.
    pub fn run_dir(&self, url_fp: &str, run_id: &str) -> PathBuf {
        self.url_dir(url_fp).join(run_id)
    }
}

// ---------------------------------------------------------------------------
// INDEX.json
// ---------------------------------------------------------------------------

/// Top-of-run index — contains per-artifact schema versions and the
/// grouping metadata a reader needs before opening siblings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    /// Schema version of `INDEX.json` itself.
    pub schema_version: u32,
    /// Schema versions of sibling artifacts. Keys are artifact names
    /// without the `.json` suffix (`"plan"`, `"machine"`, `"env"`,
    /// `"result"` when present). A reader rejects unknown major
    /// versions per §8.1.
    pub schema_versions: SchemaVersions,
    /// SHA-256 hex of the canonical plan JSON (§Phase 3 fingerprint).
    pub plan_hash: String,
    /// SHA-256 hex of the url+resolved-IPs+plan_hash bundle.
    pub target_fingerprint: String,
    /// SHA-256 hex of the URL grouping inputs.
    pub url_fingerprint: String,
    /// Set when this archive directory is a replay of an earlier
    /// archived run; points at that original `run_id`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed_from: Option<String>,
}

/// Per-artifact schema-version map — extend with `result` when Phase
/// 5b lands the Summary serialisation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersions {
    /// `plan.json` schema version.
    pub plan: u32,
    /// `machine.json` schema version.
    pub machine: u32,
    /// `env.json` schema version.
    pub env: u32,
    /// `INDEX.json` schema version.
    pub index: u32,
    /// `result.json` schema version — present when the run completed
    /// and the summary was serialised (Phase 5b).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<u32>,
}

impl Index {
    /// Schema version of `INDEX.json`. Bumps on any field
    /// rename/removal.
    pub const SCHEMA_VERSION: u32 = 1;
}

// ---------------------------------------------------------------------------
// env.json
// ---------------------------------------------------------------------------

/// `env.json` payload — tool version, user context, timestamps, and
/// network identity recorded at run time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvRecord {
    /// Schema version.
    pub schema_version: u32,
    /// `CARGO_PKG_VERSION` at build time.
    pub tool_version: String,
    /// Feature-flag string (`"h1, h2, sse, ws, script, tui"`).
    pub tool_features: String,
    /// Build profile — `"release"` or `"debug"`.
    pub build_profile: String,
    /// Git commit of the zerobench binary, when the build was
    /// reproducible-from-source. Empty when unknown.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub git_commit: String,
    /// Unix-seconds timestamps.
    pub started_at_unix: i64,
    /// Set when the run completes successfully; absent for in-flight
    /// or aborted runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_unix: Option<i64>,
    /// Resolved target IP set captured at connect time. Empty when
    /// the run short-circuits before resolution.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_ips: Vec<String>,
    /// Negotiated TLS version (`"TLS1.3"` etc.) when TLS was used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_version: Option<String>,
    /// Negotiated TLS cipher suite when TLS was used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cipher: Option<String>,
    /// `--context KEY=VAL` user-supplied entries. Key order is
    /// preserved via a `Vec` of tuples (JSON `{}` would reorder).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<(String, String)>,
    /// `true` when `--force-overload` was passed — marks every number
    /// in the run as comparison-poisoned per §9.6.2.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub force_overload: bool,
    /// `true` when `--no-calibrate` was passed. Also poisons
    /// comparisons against properly-calibrated baselines.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub calibration_skipped: bool,
}

impl EnvRecord {
    /// Schema version for `env.json`.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Fresh record at run start. `ended_at_unix` is `None` until the
    /// caller flips it via [`EnvRecord::set_ended`].
    pub fn started_now(tool_version: impl Into<String>) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            tool_version: tool_version.into(),
            tool_features: String::new(),
            build_profile: if cfg!(debug_assertions) {
                "debug".into()
            } else {
                "release".into()
            },
            git_commit: String::new(),
            started_at_unix: unix_seconds_now(),
            ended_at_unix: None,
            resolved_ips: Vec::new(),
            tls_version: None,
            tls_cipher: None,
            context: Vec::new(),
            force_overload: false,
            calibration_skipped: false,
        }
    }

    /// Stamp the completion timestamp — call once the run wraps up
    /// (successful or otherwise) before writing to disk.
    pub fn set_ended(&mut self) {
        self.ended_at_unix = Some(unix_seconds_now());
    }
}

fn unix_seconds_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// ArchiveWriter — atomic-enough dir creation + sidecar emission
// ---------------------------------------------------------------------------

/// One-shot writer that materialises `runs/<url_fp>/<run_id>/` and
/// emits the archived sidecars.
///
/// Use pattern:
///
/// ```ignore
/// let archive = Archive::resolve();
/// let writer = ArchiveWriter::begin(&archive, &url_fp, &run_id)?;
/// writer.write_plan(&plan)?;
/// writer.write_machine(&fp)?;
/// writer.write_env(&env)?;
/// writer.finalise(&index)?;      // writes INDEX.json last
/// ```
#[derive(Debug)]
pub struct ArchiveWriter {
    dir: PathBuf,
}

impl ArchiveWriter {
    /// Create `runs/<url_fp>/<run_id>/` (and all intermediate dirs).
    /// Refuses if the target directory already exists and is
    /// non-empty — duplicate run_ids indicate a caller bug
    /// (run_id is expected to be globally unique per §7.1).
    pub fn begin(archive: &Archive, url_fp: &str, run_id: &str) -> io::Result<Self> {
        let dir = archive.run_dir(url_fp, run_id);
        fs::create_dir_all(&dir)?;
        // Non-empty check: cheap scan that ignores the directory
        // itself. A fresh run directory should contain nothing.
        let mut iter = fs::read_dir(&dir)?;
        if iter.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("archive directory is not empty: {}", dir.display()),
            ));
        }
        Ok(Self { dir })
    }

    /// The directory path — useful for callers that need to drop
    /// `stdout.txt` / `stderr.txt` alongside the JSON artifacts.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write `plan.json`. Pretty-printed for human review.
    pub fn write_plan(&self, plan: &Plan) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(plan)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.write_file("plan.json", &bytes)
    }

    /// Write `machine.json`.
    pub fn write_machine(&self, fp: &MachineFingerprint) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(fp)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.write_file("machine.json", &bytes)
    }

    /// Write `env.json`.
    pub fn write_env(&self, env: &EnvRecord) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(env)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.write_file("env.json", &bytes)
    }

    /// Write `INDEX.json` last — acts as a completion marker. A reader
    /// that finds `INDEX.json` can assume the archive is complete and
    /// consistent; its absence signals a partial / crashed run.
    pub fn finalise(&self, index: &Index) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(index)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.write_file("INDEX.json", &bytes)
    }

    fn write_file(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        // Atomic-within-directory: write to `<name>.tmp`, fsync, rename.
        // The fsync guarantees bytes hit disk before rename, which is
        // strictly ordered by most filesystems (ext4, xfs, btrfs,
        // apfs). Crash recovery sees either the old file (possibly
        // absent) or the new complete file — never partial content.
        let tmp = self.dir.join(format!("{name}.tmp"));
        let final_ = self.dir.join(name);
        {
            use std::io::Write;
            let mut f = fs::File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &final_)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn tempdir() -> PathBuf {
        let mut d = std::env::temp_dir();
        let nonce = format!(
            "zerobench-archive-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        d.push(nonce);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn dummy_plan() -> Plan {
        let mut p = Plan::new();
        p.name = "test-bench".into();
        p
    }

    fn dummy_fp() -> MachineFingerprint {
        MachineFingerprint {
            schema_version: MachineFingerprint::SCHEMA_VERSION,
            cpu_model: Some("test-cpu".into()),
            cpu_cores_logical: 4,
            cpu_cores_physical: Some(4),
            cpu_flags: vec!["sse2".into()],
            total_ram_gib: Some(16),
            kernel: "Test 0.0".into(),
            hostname_blake3: "b3:deadbeefdeadbeefdeadbeefdeadbeef".into(),
            clock_monotonic_ns_resolution: Some(1),
            cpufreq_governor: None,
            transparent_hugepage: None,
            fd_limit_soft: Some(1024),
            fd_limit_hard: Some(65536),
            containerized: Some(false),
            cgroup_version: None,
        }
    }

    fn dummy_index() -> Index {
        Index {
            schema_version: Index::SCHEMA_VERSION,
            schema_versions: SchemaVersions {
                plan: 1,
                machine: 1,
                env: 1,
                index: 1,
                result: None,
            },
            plan_hash: "a".repeat(64),
            target_fingerprint: "b".repeat(64),
            url_fingerprint: "c".repeat(64),
            replayed_from: None,
        }
    }

    #[test]
    fn resolve_honours_zerobench_home() {
        // This test sets + unsets an env var — avoid races by using
        // a unique value and restoring prior state.
        let prior = std::env::var_os("ZEROBENCH_HOME");
        std::env::set_var("ZEROBENCH_HOME", "/tmp/override-for-test");
        let a = Archive::resolve();
        assert_eq!(a.root(), Path::new("/tmp/override-for-test"));
        // Restore env.
        match prior {
            Some(v) => std::env::set_var("ZEROBENCH_HOME", v),
            None => std::env::remove_var("ZEROBENCH_HOME"),
        }
    }

    #[test]
    fn writer_creates_run_directory() {
        let root = tempdir();
        let archive = Archive::at(&root);
        let writer = ArchiveWriter::begin(&archive, "urlfp", "run-1").expect("begin");
        assert!(writer.dir().exists());
        assert!(writer.dir().ends_with("runs/urlfp/run-1"));
    }

    #[test]
    fn writer_refuses_non_empty_dir() {
        let root = tempdir();
        let archive = Archive::at(&root);
        // Pre-populate the target directory.
        let target = archive.run_dir("urlfp", "run-1");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("unrelated"), "hi").unwrap();
        let err = ArchiveWriter::begin(&archive, "urlfp", "run-1").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn writes_all_four_sidecars() {
        let root = tempdir();
        let archive = Archive::at(&root);
        let writer = ArchiveWriter::begin(&archive, "urlfp", "run-2").unwrap();
        writer.write_plan(&dummy_plan()).expect("plan");
        writer.write_machine(&dummy_fp()).expect("machine");
        writer
            .write_env(&EnvRecord::started_now("0.1.0"))
            .expect("env");
        writer.finalise(&dummy_index()).expect("index");

        for expected in ["plan.json", "machine.json", "env.json", "INDEX.json"] {
            let p = writer.dir().join(expected);
            assert!(p.exists(), "missing: {}", p.display());
            let bytes = fs::read(&p).unwrap();
            assert!(!bytes.is_empty(), "empty: {}", p.display());
        }
    }

    #[test]
    fn artifacts_round_trip_through_json() {
        let root = tempdir();
        let archive = Archive::at(&root);
        let writer = ArchiveWriter::begin(&archive, "fp", "run-3").unwrap();

        let plan = dummy_plan();
        let fp = dummy_fp();
        let env = EnvRecord::started_now("0.1.0");
        let idx = dummy_index();

        writer.write_plan(&plan).unwrap();
        writer.write_machine(&fp).unwrap();
        writer.write_env(&env).unwrap();
        writer.finalise(&idx).unwrap();

        let plan_back: Plan = serde_json::from_slice(
            &fs::read(writer.dir().join("plan.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(plan.name, plan_back.name);

        let fp_back: MachineFingerprint = serde_json::from_slice(
            &fs::read(writer.dir().join("machine.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(fp, fp_back);

        let env_back: EnvRecord = serde_json::from_slice(
            &fs::read(writer.dir().join("env.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(env.tool_version, env_back.tool_version);

        let idx_back: Index = serde_json::from_slice(
            &fs::read(writer.dir().join("INDEX.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(idx, idx_back);
    }

    #[test]
    fn index_omits_replayed_from_when_none() {
        let idx = dummy_index();
        let bytes = serde_json::to_string(&idx).unwrap();
        assert!(
            !bytes.contains("replayed_from"),
            "expected replayed_from omitted when None; got {bytes}"
        );
    }

    #[test]
    fn index_includes_replayed_from_when_set() {
        let mut idx = dummy_index();
        idx.replayed_from = Some("earlier-run-id".into());
        let bytes = serde_json::to_string(&idx).unwrap();
        assert!(
            bytes.contains("replayed_from"),
            "expected replayed_from present; got {bytes}"
        );
    }

    #[test]
    fn env_record_omits_empty_optional_fields() {
        let env = EnvRecord::started_now("1.0.0");
        let bytes = serde_json::to_string(&env).unwrap();
        assert!(!bytes.contains("git_commit"), "got {bytes}");
        assert!(!bytes.contains("ended_at_unix"), "got {bytes}");
        assert!(!bytes.contains("resolved_ips"), "got {bytes}");
        assert!(!bytes.contains("tls_version"), "got {bytes}");
        assert!(!bytes.contains("context"), "got {bytes}");
        assert!(!bytes.contains("force_overload"), "got {bytes}");
        assert!(!bytes.contains("calibration_skipped"), "got {bytes}");
    }

    #[test]
    fn env_record_set_ended_adds_timestamp() {
        let mut env = EnvRecord::started_now("1.0.0");
        assert!(env.ended_at_unix.is_none());
        env.set_ended();
        assert!(env.ended_at_unix.is_some());
        assert!(env.ended_at_unix.unwrap() >= env.started_at_unix);
    }
}
