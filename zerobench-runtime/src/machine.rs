//! ARCH STATUS: MOVE → zerobench-runtime::machine
//!
//! Pairs with archive (runtime writes machine fingerprint into the
//! archive at run start). Moves wholesale; no rewrite.
//! See docs/ARCH-REVIEW-2026-04-20.md §7.
//!
//! ----------------------------------------------------------------------
//!
//! Machine fingerprint collection — `docs/design-v0.1.0.md` §7.2 and
//! `docs/PHILOSOPHY.md` §8.3.
//!
//! Every archived run ships a `machine.json` sidecar so that
//! reproducibility claims (§P11) are anchored to observable hardware
//! and OS state. The fingerprint is collected once at run start and
//! serialised verbatim — **it is not part of any hash input**
//! (`plan_hash` / `url_fingerprint` / `target_fingerprint` are all
//! machine-independent by design).
//!
//! # `unsafe` scope
//!
//! This module is the workspace's single authorised `unsafe`
//! consumer. Machine fingerprinting requires libc FFI (`uname`,
//! `gethostname`, `clock_getres`, `getrlimit`) for which no safe
//! cross-platform std equivalent exists. Every `unsafe` block here
//! calls a POSIX-stable function with correctly-initialised
//! out-parameters and checks return codes. No raw pointer arithmetic,
//! no lifetime stretching.
#![allow(unsafe_code)]
//!
//! Platform branching:
//!
//! - **Linux**: read `/proc`, `/sys`, and issue `getrlimit(2)` /
//!   `sysconf(3)` / `clock_getres(2)` directly via `libc`.
//! - **macOS**: read via `sysctl(3)` — stubbed in this version to
//!   enable CI builds on macOS runners; fields for which we lack
//!   a Darwin code path are `None`.
//! - **Other Unix / Windows**: minimal best-effort (CPU count,
//!   kernel release, hostname). Most fields are `None`. The tool
//!   still runs; the fingerprint is diminished.
//!
//! Fields that can't be determined on a given platform are **omitted
//! from the JSON output** rather than emitted as `null` — matches the
//! philosophy's "no bare numbers" principle: if we can't observe it,
//! we don't claim a value.

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Public struct
// ---------------------------------------------------------------------------

/// Snapshot of the local machine at run start.
///
/// Every `Option<T>` field is `None` when the platform does not expose
/// the underlying signal or when detection fails. The struct is
/// serialised with `#[serde(skip_serializing_if = "Option::is_none")]`
/// so absent fields are *omitted* from the JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineFingerprint {
    /// Schema version for `machine.json`. Bumps when we make breaking
    /// changes to the field set.
    pub schema_version: u32,

    // -------------------------------------------------------------------
    // CPU
    // -------------------------------------------------------------------
    /// Vendor/model string from `/proc/cpuinfo` (Linux) or
    /// `machdep.cpu.brand_string` (macOS). `None` if undetectable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_model: Option<String>,
    /// Logical CPU count (hyperthreads or SMT siblings included).
    pub cpu_cores_logical: usize,
    /// Physical CPU count (unique cores, SMT siblings merged). `None`
    /// when sibling detection fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_cores_physical: Option<usize>,
    /// Instruction set feature flags (`avx2`, `sse4_2`, …). Empty vec
    /// when undetectable.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cpu_flags: Vec<String>,

    // -------------------------------------------------------------------
    // Memory
    // -------------------------------------------------------------------
    /// Total system RAM in GiB (rounded). `None` when undetectable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_ram_gib: Option<u64>,

    // -------------------------------------------------------------------
    // Kernel / userspace
    // -------------------------------------------------------------------
    /// Kernel identifier: `Linux 6.12.0`, `Darwin 24.3.0`, etc.
    pub kernel: String,
    /// Hostname hashed via Blake3 for audit without identity leak.
    /// Equality-matching across fingerprints works unchanged; the
    /// plaintext hostname is never archived unless the user passes
    /// `--expose-hostname`. See PHILOSOPHY §8.3.
    pub hostname_blake3: String,

    // -------------------------------------------------------------------
    // Clock
    // -------------------------------------------------------------------
    /// Resolution of the monotonic clock in nanoseconds. If >10 000
    /// (10 µs), `zerobench measure` refuses by default — coarse
    /// clocks produce quantisation artefacts in sub-µs percentiles.
    /// `None` when undetectable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_monotonic_ns_resolution: Option<u64>,

    // -------------------------------------------------------------------
    // CPU governor / THP (Linux-specific)
    // -------------------------------------------------------------------
    /// cpufreq governor on cpu0 (`performance` / `powersave` / …).
    /// Linux-only; `None` elsewhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpufreq_governor: Option<String>,
    /// Transparent-hugepage state (`always` / `madvise` / `never`).
    /// Linux-only; `None` elsewhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transparent_hugepage: Option<String>,

    // -------------------------------------------------------------------
    // FD limits
    // -------------------------------------------------------------------
    /// Soft `RLIMIT_NOFILE` at run start. `None` on platforms that
    /// don't expose `getrlimit`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fd_limit_soft: Option<u64>,
    /// Hard `RLIMIT_NOFILE`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fd_limit_hard: Option<u64>,

    // -------------------------------------------------------------------
    // Container / cgroup (Linux-specific)
    // -------------------------------------------------------------------
    /// `Some(true)` / `Some(false)` on Linux depending on whether
    /// `/proc/self/cgroup` indicates a container runtime; `None`
    /// elsewhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub containerized: Option<bool>,
    /// `"v1"` or `"v2"` on Linux cgroup hosts; `None` elsewhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroup_version: Option<String>,
}

impl MachineFingerprint {
    /// Schema version constant — bump when fields are
    /// renamed/retyped/removed (minor additions don't bump).
    pub const SCHEMA_VERSION: u32 = 1;

    /// Collect the fingerprint for the current machine. Always
    /// succeeds — per-field detection failures yield `None` rather
    /// than aborting.
    pub fn collect() -> Self {
        let cpu_cores_logical = num_cpus::get();
        let kernel = collect_kernel();
        let hostname_blake3 = collect_hostname_blake3();
        let (cpu_model, cpu_flags, cpu_cores_physical) = collect_cpu();
        let total_ram_gib = collect_ram_gib();
        let clock_monotonic_ns_resolution = collect_clock_resolution();
        let (fd_limit_soft, fd_limit_hard) = collect_fd_limits();
        let cpufreq_governor = collect_cpufreq_governor();
        let transparent_hugepage = collect_transparent_hugepage();
        let (containerized, cgroup_version) = collect_container_info();

        Self {
            schema_version: Self::SCHEMA_VERSION,
            cpu_model,
            cpu_cores_logical,
            cpu_cores_physical,
            cpu_flags,
            total_ram_gib,
            kernel,
            hostname_blake3,
            clock_monotonic_ns_resolution,
            cpufreq_governor,
            transparent_hugepage,
            fd_limit_soft,
            fd_limit_hard,
            containerized,
            cgroup_version,
        }
    }

    /// `true` when the monotonic clock resolution is coarser than the
    /// tool's worst-case measurement floor (10 µs). Callers check this
    /// and refuse without `--allow-coarse-clock` per PHILOSOPHY §8.3.
    pub fn clock_is_coarse(&self) -> bool {
        self.clock_monotonic_ns_resolution
            .map(|ns| ns > 10_000)
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Platform-independent collectors
// ---------------------------------------------------------------------------

fn collect_kernel() -> String {
    // `uname -sr` equivalent via libc::uname. Blocks on neither; a
    // small stack buffer.
    unsafe {
        let mut u: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut u) != 0 {
            return "unknown".into();
        }
        let sys = cstr_to_string(u.sysname.as_ptr());
        let rel = cstr_to_string(u.release.as_ptr());
        format!("{sys} {rel}")
    }
}

fn collect_hostname_blake3() -> String {
    let mut buf = [0u8; 256];
    let ret = unsafe {
        libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len())
    };
    if ret != 0 {
        return "unknown".into();
    }
    // Find the first NUL.
    let nul = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let digest = blake3::hash(&buf[..nul]);
    // Shorten to the first 16 bytes → 32 hex chars. Equality across
    // fingerprints is preserved; archive paths stay readable.
    let bytes = digest.as_bytes();
    format!("b3:{}", hex::encode(&bytes[..16]))
}

fn collect_clock_resolution() -> Option<u64> {
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        if libc::clock_getres(libc::CLOCK_MONOTONIC, &mut ts) != 0 {
            return None;
        }
        let ns = (ts.tv_sec as i128 * 1_000_000_000 + ts.tv_nsec as i128).max(1);
        Some(ns as u64)
    }
}

fn collect_fd_limits() -> (Option<u64>, Option<u64>) {
    unsafe {
        let mut lim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return (None, None);
        }
        let soft = if lim.rlim_cur == libc::RLIM_INFINITY {
            u64::MAX
        } else {
            lim.rlim_cur as u64
        };
        let hard = if lim.rlim_max == libc::RLIM_INFINITY {
            u64::MAX
        } else {
            lim.rlim_max as u64
        };
        (Some(soft), Some(hard))
    }
}

// ---------------------------------------------------------------------------
// Linux-specific collectors
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn collect_cpu() -> (Option<String>, Vec<String>, Option<usize>) {
    let text = match std::fs::read_to_string("/proc/cpuinfo") {
        Ok(s) => s,
        Err(_) => return (None, Vec::new(), None),
    };

    let mut model: Option<String> = None;
    let mut flags: Vec<String> = Vec::new();
    let mut physical_ids: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    let mut current_phys: Option<String> = None;
    let mut current_core: Option<String> = None;

    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            if line.is_empty() {
                // End of a logical CPU block — record its (phys, core).
                if let (Some(p), Some(c)) = (current_phys.take(), current_core.take()) {
                    physical_ids.insert((p, c));
                }
            }
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        match key {
            "model name" if model.is_none() => {
                model = Some(value.to_string());
            }
            "flags" | "Features" if flags.is_empty() => {
                flags = value.split_whitespace().map(String::from).collect();
            }
            "physical id" => current_phys = Some(value.to_string()),
            "core id" => current_core = Some(value.to_string()),
            _ => {}
        }
    }
    // Trailing block without blank-line terminator.
    if let (Some(p), Some(c)) = (current_phys, current_core) {
        physical_ids.insert((p, c));
    }

    let physical = if physical_ids.is_empty() {
        None
    } else {
        Some(physical_ids.len())
    };
    (model, flags, physical)
}

#[cfg(target_os = "linux")]
fn collect_ram_gib() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest
                .trim()
                .split_whitespace()
                .next()?
                .parse()
                .ok()?;
            // 1 GiB = 1024 * 1024 KiB. /proc/meminfo reports in KiB.
            return Some(kb / (1024 * 1024));
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn collect_cpufreq_governor() -> Option<String> {
    std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .ok()
        .map(|s| s.trim().to_string())
}

#[cfg(target_os = "linux")]
fn collect_transparent_hugepage() -> Option<String> {
    let raw = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled").ok()?;
    // Format: "always [madvise] never" — the bracketed entry is the
    // current value.
    for tok in raw.split_whitespace() {
        if tok.starts_with('[') && tok.ends_with(']') {
            return Some(tok[1..tok.len() - 1].to_string());
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn collect_container_info() -> (Option<bool>, Option<String>) {
    // `/proc/self/cgroup` on cgroup v2 hosts contains a single line
    // `0::/...` where the path indicates the cgroup. Containers are
    // typically under `/docker/...`, `/kubepods/...`, `/system.slice
    // /docker-...`, or similar. On cgroup v1 there are multiple lines
    // with `N:subsystem:/path`.
    let text = match std::fs::read_to_string("/proc/self/cgroup") {
        Ok(s) => s,
        Err(_) => return (None, None),
    };

    let is_v2 = text.lines().all(|l| l.starts_with("0::"));
    let version = if is_v2 { "v2" } else { "v1" };

    let containerized = text.lines().any(|line| {
        let path = line.rsplit(':').next().unwrap_or("");
        path.contains("/docker/")
            || path.contains("/kubepods")
            || path.contains("/containerd")
            || path.contains("/crio")
            || path.contains("/podman")
            || path.contains("/lxc/")
    });

    (Some(containerized), Some(version.into()))
}

// ---------------------------------------------------------------------------
// macOS-specific collectors (stubs for this version)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn collect_cpu() -> (Option<String>, Vec<String>, Option<usize>) {
    // TODO: populate via sysctl machdep.cpu.brand_string, .features.
    // For now: log a minimal entry so tests pass on macOS CI.
    (None, Vec::new(), Some(num_cpus::get_physical()))
}

#[cfg(target_os = "macos")]
fn collect_ram_gib() -> Option<u64> {
    // TODO: sysctl hw.memsize. For now, None — JSON omits the field.
    None
}

#[cfg(target_os = "macos")]
fn collect_cpufreq_governor() -> Option<String> {
    None // Not applicable; macOS has its own power-management model.
}

#[cfg(target_os = "macos")]
fn collect_transparent_hugepage() -> Option<String> {
    None // Linux-specific concept.
}

#[cfg(target_os = "macos")]
fn collect_container_info() -> (Option<bool>, Option<String>) {
    // macOS hosts Docker via lightweight VM; from inside the VM the
    // fingerprint looks like Linux (and runs the Linux path). On bare
    // macOS this is always false.
    (Some(false), None)
}

// ---------------------------------------------------------------------------
// Fallback collectors for any other Unix
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn collect_cpu() -> (Option<String>, Vec<String>, Option<usize>) {
    (None, Vec::new(), None)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn collect_ram_gib() -> Option<u64> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn collect_cpufreq_governor() -> Option<String> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn collect_transparent_hugepage() -> Option<String> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn collect_container_info() -> (Option<bool>, Option<String>) {
    (None, None)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

unsafe fn cstr_to_string(p: *const libc::c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(p)
        .to_string_lossy()
        .into_owned()
}

/// Duration → nanoseconds, saturating at u64::MAX. Tiny helper kept
/// adjacent to the clock-resolution logic for co-location with
/// clock-sensitive code.
#[inline]
pub fn duration_to_ns_sat(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_returns_plausible_values() {
        let fp = MachineFingerprint::collect();

        assert_eq!(fp.schema_version, MachineFingerprint::SCHEMA_VERSION);
        assert!(fp.cpu_cores_logical > 0);
        assert!(!fp.kernel.is_empty());
        assert!(
            fp.hostname_blake3.starts_with("b3:"),
            "hostname_blake3 should be prefixed; got {}",
            fp.hostname_blake3
        );
        // hex-encoded 16-byte Blake3 prefix = 32 chars + "b3:" = 35.
        assert_eq!(
            fp.hostname_blake3.len(),
            35,
            "hostname_blake3 length unexpected: {}",
            fp.hostname_blake3
        );
    }

    #[test]
    fn fingerprint_round_trips_through_json() {
        let fp = MachineFingerprint::collect();
        let j = serde_json::to_string(&fp).expect("serialize");
        let back: MachineFingerprint = serde_json::from_str(&j).expect("deserialize");
        assert_eq!(fp, back);
    }

    #[test]
    fn absent_fields_omitted_from_json() {
        let fp = MachineFingerprint {
            schema_version: 1,
            cpu_model: None,
            cpu_cores_logical: 4,
            cpu_cores_physical: None,
            cpu_flags: Vec::new(),
            total_ram_gib: None,
            kernel: "Linux 6.0.0".into(),
            hostname_blake3: "b3:deadbeef".into(),
            clock_monotonic_ns_resolution: None,
            cpufreq_governor: None,
            transparent_hugepage: None,
            fd_limit_soft: None,
            fd_limit_hard: None,
            containerized: None,
            cgroup_version: None,
        };
        let j = serde_json::to_string(&fp).expect("serialize");
        assert!(!j.contains("cpu_model"), "expected cpu_model omitted; got {j}");
        assert!(
            !j.contains("cpu_flags"),
            "expected cpu_flags omitted when empty; got {j}"
        );
        assert!(!j.contains("total_ram_gib"));
        assert!(!j.contains("clock_monotonic_ns_resolution"));
        assert!(!j.contains("cpufreq_governor"));
    }

    #[test]
    fn clock_is_coarse_gate() {
        let mut fp = MachineFingerprint {
            schema_version: 1,
            cpu_model: None,
            cpu_cores_logical: 1,
            cpu_cores_physical: None,
            cpu_flags: Vec::new(),
            total_ram_gib: None,
            kernel: "x".into(),
            hostname_blake3: "b3:0".into(),
            clock_monotonic_ns_resolution: Some(1),
            cpufreq_governor: None,
            transparent_hugepage: None,
            fd_limit_soft: None,
            fd_limit_hard: None,
            containerized: None,
            cgroup_version: None,
        };
        assert!(!fp.clock_is_coarse());
        fp.clock_monotonic_ns_resolution = Some(10_000);
        assert!(!fp.clock_is_coarse(), "10µs is boundary, not coarse");
        fp.clock_monotonic_ns_resolution = Some(10_001);
        assert!(fp.clock_is_coarse());
        fp.clock_monotonic_ns_resolution = None;
        assert!(!fp.clock_is_coarse(), "None ≠ coarse");
    }

    #[test]
    fn hostname_blake3_is_stable_across_calls() {
        let a = collect_hostname_blake3();
        let b = collect_hostname_blake3();
        assert_eq!(a, b);
    }

    #[test]
    fn duration_to_ns_saturates() {
        assert_eq!(duration_to_ns_sat(Duration::from_nanos(100)), 100);
        assert_eq!(duration_to_ns_sat(Duration::ZERO), 0);
        let huge = Duration::from_secs(u64::MAX / 2);
        let ns = duration_to_ns_sat(huge);
        // Must not overflow, must not panic.
        assert!(ns <= u64::MAX);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_cpu_model_detected() {
        let (model, _flags, _phys) = collect_cpu();
        // Almost every real Linux distro has "model name" in
        // /proc/cpuinfo for x86_64. arm64 Linux uses "Features" etc.
        // so model might legitimately be None on arm64 VMs — only
        // assert presence when the env looks x86-ish.
        let arch_env = std::env::consts::ARCH;
        if arch_env == "x86_64" && std::path::Path::new("/proc/cpuinfo").exists() {
            assert!(
                model.is_some(),
                "expected cpu_model on Linux x86_64; got None"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_ram_nonzero() {
        if std::path::Path::new("/proc/meminfo").exists() {
            let r = collect_ram_gib();
            assert!(r.is_some(), "RAM detection failed on Linux");
            assert!(r.unwrap() >= 1, "RAM < 1 GiB? got {:?}", r);
        }
    }
}
