//! Two-level fingerprinting + run_id formatter for the v0.1.0 archive.
//!
//! Implements `docs/design-v0.1.0.md` §7.1:
//!
//! - [`plan_hash`]      — sha256 of the JCS-canonical JSON of the Plan
//!                         (tool version excluded so upgrades don't
//!                         split archive groups).
//! - [`url_fingerprint`] — sha256 over `{scheme, host, port, sni, plan_name, ip_family}`.
//!                         The **stable grouping key**: all runs
//!                         against the same URL land together even when
//!                         DNS rotates.
//! - [`target_fingerprint`] — sha256 over `{scheme, host, resolved_IPs (sorted), port, sni, plan_hash}`.
//!                             Identifies "same plan against same backend".
//! - [`run_id`]         — `<UTC-ISO-timestamp>-<plan_hash[:8]>-<target_fp[:8]>`.
//!
//! # Canonicalisation
//!
//! We implement a simplified variant of **RFC 8785 JCS** sufficient for
//! zerobench-internal use: keys sorted lexicographically (byte order,
//! equivalent to UTF-16 codepoint order for ASCII field names, which
//! is what we have), no whitespace, default `serde_json` number
//! representation. Full JCS compliance is not a goal — we need
//! deterministic hashing, not interop with external JCS tooling.
//!
//! If a future requirement demands true JCS, the canonicalizer is
//! a drop-in replacement; callers use [`plan_hash`] and never see
//! the JSON.

use std::net::SocketAddr;
use std::time::SystemTime;

use serde::Serialize;
use sha2::{Digest, Sha256};
use time::format_description::well_known::Iso8601;
use time::OffsetDateTime;

use zerobench_core::plan::Plan;
use zerobench_core::transport::Target;

// ---------------------------------------------------------------------------
// Canonical JSON
// ---------------------------------------------------------------------------

/// Serialise `value` to a canonical JSON byte string and return its
/// lowercase-hex SHA-256 digest.
///
/// Canonicalisation: object keys sorted lexicographically; no
/// whitespace; default `serde_json` numeric / string formatting.
///
/// This is a simplified JCS (RFC 8785) — adequate for internal
/// fingerprinting; not for interop with external JCS consumers.
pub fn canonical_sha256<T: Serialize>(value: &T) -> String {
    let v = serde_json::to_value(value).expect("value must be serialisable");
    let canonical = canonicalise(v);
    // `serde_json::to_vec` uses compact (no-whitespace) formatting.
    let bytes = serde_json::to_vec(&canonical).expect("canonical value must be re-serialisable");
    let digest = Sha256::digest(&bytes);
    hex::encode(digest)
}

fn canonicalise(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            // Collect into a BTreeMap (sorted by key), then back into
            // serde_json::Map which preserves insertion order in its
            // serialisation.
            let mut sorted: std::collections::BTreeMap<String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for (k, v) in m {
                sorted.insert(k, canonicalise(v));
            }
            let mut out = serde_json::Map::with_capacity(sorted.len());
            for (k, v) in sorted {
                out.insert(k, v);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(a) => {
            serde_json::Value::Array(a.into_iter().map(canonicalise).collect())
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Plan hash
// ---------------------------------------------------------------------------

/// SHA-256 of the canonical JSON representation of `plan`'s
/// *identity projection*.
///
/// **Tool version is intentionally not part of the hash.** Two versions
/// of zerobench producing the same `Plan` from the same source must
/// hash identically, so archived runs stay grouped when the tool is
/// upgraded.
///
/// **Run-time settings (`duration`, `runs`, `threads`, `cooldown`,
/// `warmup`, `mode`, `name`) are also excluded** so changing those
/// knobs doesn't fragment the archive — the same benchmark run for
/// 30s vs 60s, 1 run vs 5, or 4 threads vs 16, still groups together
/// under the same `plan_hash`. See [`Plan::identity_projection`].
pub fn plan_hash(plan: &Plan) -> String {
    canonical_sha256(&plan.identity_projection())
}

// ---------------------------------------------------------------------------
// URL fingerprint
// ---------------------------------------------------------------------------

/// Address family preference — part of the URL fingerprint so `v4`
/// and `v6` runs against the same URL do not share an archive bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IpFamilyTag {
    /// Either family as returned by the OS resolver (RFC 6724).
    Auto,
    /// IPv4 only.
    V4,
    /// IPv6 only.
    V6,
}

/// Inputs to [`url_fingerprint`] — captured as a struct so the
/// canonical JSON is stable across refactors.
#[derive(Debug, Clone, Serialize)]
struct UrlFpInput<'a> {
    scheme: &'a str,
    host: &'a str,
    port: u16,
    sni: Option<&'a str>,
    plan_name: &'a str,
    ip_family: IpFamilyTag,
}

/// Stable grouping key for archived runs.
///
/// Returns the SHA-256 hex digest of `{scheme, host, port, sni,
/// plan_name, ip_family}` in canonical JSON form. All runs against
/// the same service URL (regardless of DNS resolution at run time)
/// share this fingerprint.
///
/// # Panics
///
/// Panics if `plan.name` is empty — archiving requires a non-empty
/// plan name (§7.1 of the design doc). Callers that want an
/// ephemeral, un-archivable fingerprint should use
/// [`url_fingerprint_anonymous`] instead.
pub fn url_fingerprint(plan: &Plan, target: &Target, ip_family: IpFamilyTag) -> String {
    assert!(
        !plan.name.is_empty(),
        "plan.name must be non-empty for url_fingerprint; set plan.name or use url_fingerprint_anonymous",
    );
    url_fingerprint_impl(plan.name.as_str(), target, ip_family)
}

/// Like [`url_fingerprint`] but uses `"<anonymous>"` as the plan name.
///
/// Suitable for `probe` runs that explicitly opt out of archiving.
pub fn url_fingerprint_anonymous(target: &Target, ip_family: IpFamilyTag) -> String {
    url_fingerprint_impl("<anonymous>", target, ip_family)
}

fn url_fingerprint_impl(plan_name: &str, target: &Target, ip_family: IpFamilyTag) -> String {
    let scheme = if target.tls { "https" } else { "http" };
    let input = UrlFpInput {
        scheme,
        host: target.host.as_str(),
        port: target.port,
        sni: target.sni.as_deref(),
        plan_name,
        ip_family,
    };
    canonical_sha256(&input)
}

// ---------------------------------------------------------------------------
// Target fingerprint
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct TargetFpInput<'a> {
    scheme: &'a str,
    host: &'a str,
    port: u16,
    sni: Option<&'a str>,
    resolved_ips: Vec<String>, // sorted, canonical string form
    plan_hash: &'a str,
}

/// SHA-256 of `{scheme, host, port, sni, resolved_IPs (sorted),
/// plan_hash}`. Identifies "same plan against same backend".
///
/// `resolved` may be empty, in which case the hash captures only the
/// pre-resolution parameters. Callers routinely invoke this after the
/// DNS resolve step in the verb dispatcher.
pub fn target_fingerprint(
    plan: &Plan,
    target: &Target,
    resolved: &[SocketAddr],
    plan_hash: &str,
) -> String {
    let scheme = if target.tls { "https" } else { "http" };
    // Canonicalise by sorting the resolved IPs lexicographically.
    // Using `ToString` + sort is sufficient: v4 addresses sort by
    // dotted-quad order; v6 by colon-hex. Matching addresses produce
    // matching hashes.
    let mut resolved_ips: Vec<String> = resolved.iter().map(|a| a.to_string()).collect();
    resolved_ips.sort();
    let input = TargetFpInput {
        scheme,
        host: target.host.as_str(),
        port: target.port,
        sni: target.sni.as_deref(),
        resolved_ips,
        plan_hash,
    };
    // `plan` is carried for API consistency (caller passes it); the
    // plan-level identity enters via `plan_hash` which the caller
    // computed once.
    let _ = plan;
    canonical_sha256(&input)
}

// ---------------------------------------------------------------------------
// run_id
// ---------------------------------------------------------------------------

/// Format a globally-unique, copy-pasteable run identifier.
///
/// Format: `<UTC-ISO-timestamp>-<plan_hash[:8]>-<target_fp[:8]>`
///
/// Example: `2026-04-19T14:23:05Z-4a9ce1b2-ef01baba`
///
/// The ISO-8601 timestamp is truncated to second precision and uses
/// the `Z` suffix (UTC). `plan_hash` and `target_fp` must be
/// lowercase-hex SHA-256 strings (any length ≥ 8).
pub fn run_id(plan_hash: &str, target_fp: &str, when: SystemTime) -> String {
    assert!(
        plan_hash.len() >= 8 && target_fp.len() >= 8,
        "plan_hash/target_fp must be ≥8 hex characters each",
    );
    let odt = OffsetDateTime::from(when);
    // Truncate to seconds for filesystem friendliness.
    let odt = odt.replace_nanosecond(0).unwrap_or(odt);
    let ts = odt
        .format(&Iso8601::DEFAULT)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());
    // `time::Iso8601::DEFAULT` produces offsets like `+00:00:00`; we
    // want the RFC-3339 `Z` form for readability and filesystem
    // friendliness.
    let ts = normalise_iso_utc(&ts);
    format!("{}-{}-{}", ts, &plan_hash[..8], &target_fp[..8])
}

fn normalise_iso_utc(s: &str) -> String {
    // Replace a trailing `+00:00:00` or `+00:00` with `Z`, and trim
    // any sub-second fractional part. The `time` crate's DEFAULT
    // config produces strings like `2026-04-19T14:23:05.000000000Z`
    // or `...+00:00:00` depending on version; normalise aggressively.
    let mut s = s.to_string();
    if let Some(plus_idx) = s.find('+') {
        // Replace `+HH:MM[:SS]` suffix with `Z`.
        s.truncate(plus_idx);
        s.push('Z');
    } else if s.ends_with('Z') {
        // Already ends in Z — fine.
    }
    // Strip fractional seconds: look for `.` after the `T`.
    if let Some(t_idx) = s.find('T') {
        if let Some(dot_idx) = s[t_idx..].find('.') {
            let abs = t_idx + dot_idx;
            // Find the next non-digit after the dot.
            let end = s[abs + 1..]
                .find(|c: char| !c.is_ascii_digit())
                .map(|i| abs + 1 + i)
                .unwrap_or(s.len());
            s.replace_range(abs..end, "");
        }
    }
    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn test_target(scheme_tls: bool, host: &str, port: u16, sni: Option<&str>) -> Target {
        use zerobench_core::transport::AddrFamily;
        Target {
            host: host.to_string(),
            port,
            tls: scheme_tls,
            sni: sni.map(String::from),
            addr_family: AddrFamily::Any,
        }
    }

    #[test]
    fn canonical_hash_is_stable_across_key_order() {
        let a = serde_json::json!({ "b": 1, "a": 2 });
        let b = serde_json::json!({ "a": 2, "b": 1 });
        assert_eq!(canonical_sha256(&a), canonical_sha256(&b));
    }

    #[test]
    fn canonical_hash_differs_on_value() {
        let a = serde_json::json!({ "a": 1 });
        let b = serde_json::json!({ "a": 2 });
        assert_ne!(canonical_sha256(&a), canonical_sha256(&b));
    }

    #[test]
    fn canonical_hash_nested_objects_sorted() {
        let a = serde_json::json!({
            "outer": { "z": 1, "a": 2 },
            "list": [ { "y": 1, "x": 2 } ],
        });
        let b = serde_json::json!({
            "list": [ { "x": 2, "y": 1 } ],
            "outer": { "a": 2, "z": 1 },
        });
        assert_eq!(canonical_sha256(&a), canonical_sha256(&b));
    }

    #[test]
    fn plan_hash_is_deterministic() {
        let mut plan = Plan::new();
        plan.name = "bench-a".into();
        let h1 = plan_hash(&plan);
        let h2 = plan_hash(&plan);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64, "sha256 hex = 64 chars");
    }

    #[test]
    fn plan_hash_does_not_depend_on_name() {
        // Name is a human label — it is deliberately excluded from
        // identity so renaming a plan does not fragment the archive.
        // Per-profile grouping happens through `url_fingerprint`,
        // which still mixes `name` in.
        let mut p1 = Plan::new();
        p1.name = "a".into();
        let mut p2 = Plan::new();
        p2.name = "b".into();
        assert_eq!(plan_hash(&p1), plan_hash(&p2));
    }

    #[test]
    fn plan_hash_stable_across_runtime_settings() {
        // Varying duration / warmup / cooldown / runs / threads /
        // mode must NOT produce a new archive bucket — it's the
        // same workload measured under a different time budget.
        use zerobench_core::plan::Mode;
        let p1 = Plan::new();
        let mut p2 = Plan::new();
        p2.duration = std::time::Duration::from_secs(900);
        p2.warmup = std::time::Duration::from_secs(5);
        p2.cooldown = std::time::Duration::from_secs(30);
        p2.runs = 7;
        p2.threads = 32;
        p2.mode = Mode::default();
        assert_eq!(plan_hash(&p1), plan_hash(&p2));
    }

    #[test]
    fn plan_hash_changes_when_workload_changes() {
        use zerobench_core::plan::{RateProfile, Scenario};
        // A scenario addition is a workload change, so the hash
        // must differ — identity is driven by scenarios + vars.
        let p1 = Plan::new();
        let mut p2 = Plan::new();
        p2.scenarios.push(Scenario {
            name: "added".into(),
            rate: RateProfile::Saturate {
                max_concurrency: 10,
            },
            steps: Vec::new(),
        });
        assert_ne!(plan_hash(&p1), plan_hash(&p2));
    }

    #[test]
    fn url_fingerprint_stable_across_dns_rotations() {
        let t1 = test_target(false, "api.example.com", 80, None);
        let t2 = test_target(false, "api.example.com", 80, None);
        let mut p = Plan::new();
        p.name = "my-bench".into();
        assert_eq!(
            url_fingerprint(&p, &t1, IpFamilyTag::Auto),
            url_fingerprint(&p, &t2, IpFamilyTag::Auto),
        );
    }

    #[test]
    fn url_fingerprint_splits_on_plan_name() {
        let t = test_target(false, "api.example.com", 80, None);
        let mut p1 = Plan::new();
        p1.name = "bench-a".into();
        let mut p2 = Plan::new();
        p2.name = "bench-b".into();
        assert_ne!(
            url_fingerprint(&p1, &t, IpFamilyTag::Auto),
            url_fingerprint(&p2, &t, IpFamilyTag::Auto),
        );
    }

    #[test]
    fn url_fingerprint_splits_on_ip_family() {
        let t = test_target(false, "api.example.com", 80, None);
        let mut p = Plan::new();
        p.name = "b".into();
        assert_ne!(
            url_fingerprint(&p, &t, IpFamilyTag::V4),
            url_fingerprint(&p, &t, IpFamilyTag::V6),
        );
    }

    #[test]
    #[should_panic(expected = "plan.name must be non-empty")]
    fn url_fingerprint_panics_on_empty_name() {
        let t = test_target(false, "h", 80, None);
        let p = Plan::new(); // name = ""
        let _ = url_fingerprint(&p, &t, IpFamilyTag::Auto);
    }

    #[test]
    fn url_fingerprint_anonymous_does_not_require_name() {
        let t = test_target(false, "h", 80, None);
        let fp = url_fingerprint_anonymous(&t, IpFamilyTag::Auto);
        assert_eq!(fp.len(), 64);
    }

    #[test]
    fn target_fingerprint_splits_on_resolved_ip() {
        let t = test_target(false, "api.example.com", 80, None);
        let p = Plan::new();
        let ph = plan_hash(&p);
        let a: Vec<SocketAddr> = vec!["10.0.0.1:80".parse().unwrap()];
        let b: Vec<SocketAddr> = vec!["10.0.0.2:80".parse().unwrap()];
        assert_ne!(
            target_fingerprint(&p, &t, &a, &ph),
            target_fingerprint(&p, &t, &b, &ph),
        );
    }

    #[test]
    fn target_fingerprint_is_insensitive_to_ip_order() {
        let t = test_target(false, "api.example.com", 80, None);
        let p = Plan::new();
        let ph = plan_hash(&p);
        let ordered: Vec<SocketAddr> = vec![
            "10.0.0.1:80".parse().unwrap(),
            "10.0.0.2:80".parse().unwrap(),
        ];
        let reversed: Vec<SocketAddr> = vec![
            "10.0.0.2:80".parse().unwrap(),
            "10.0.0.1:80".parse().unwrap(),
        ];
        assert_eq!(
            target_fingerprint(&p, &t, &ordered, &ph),
            target_fingerprint(&p, &t, &reversed, &ph),
        );
    }

    #[test]
    fn run_id_format_matches_spec() {
        let when = UNIX_EPOCH + Duration::from_secs(1_744_000_000);
        // 1_744_000_000 seconds = 2025-04-07T05:46:40Z
        let id = run_id(
            "4a9ce1b2abcdef01234567890abcdef0",
            "ef01baba1234567890abcdef0abcdef0",
            when,
        );
        // Shape: <ts>-<h1[:8]>-<h2[:8]>
        assert!(id.ends_with("-4a9ce1b2-ef01baba"), "id={id}");
        assert!(id.starts_with("2025-04-07T"), "id={id}");
        assert!(id.contains("Z"), "id={id}");
        // No sub-second component.
        assert!(!id.contains("."), "id={id}");
    }

    #[test]
    fn run_id_deterministic_at_same_instant() {
        let when = UNIX_EPOCH + Duration::from_secs(1_744_000_000);
        let a = run_id("00000000aaaaaaaa", "11111111bbbbbbbb", when);
        let b = run_id("00000000aaaaaaaa", "11111111bbbbbbbb", when);
        assert_eq!(a, b);
    }
}
