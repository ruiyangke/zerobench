//! Terminal and JSON reporters.
//!
//! Both entry points consume a [`Summary`] + [`Plan`] pair and render to
//! a caller-supplied [`std::io::Write`]. The terminal path mirrors the
//! layout in `docs/design.md` §6; the JSON path emits a stable,
//! versioned blob the Task-13 diff tool will consume.
//!
//! # Color
//!
//! [`ColorChoice::Auto`] consults [`IsTerminal`] on the sink (so output
//! piped to a file stays clean) and honours the `NO_COLOR` env var per
//! <https://no-color.org/>. `Always` / `Never` are self-explanatory.
//!
//! # Duration formatting
//!
//! The built-in `Debug`/`Display` for `Duration` is pretty wordy
//! (`127.5µs` is fine, but `Duration { secs: 0, nanos: 120000 }`
//! shows up in debug). [`format_ns`] picks a reasonable unit and keeps
//! ~3 significant figures.

use std::io::{self, Write};
use std::time::Duration;

use yansi::{Condition, Paint};

use crate::plan::Plan;
use crate::stats::Summary;

// ---------------------------------------------------------------------------
// Public enums
// ---------------------------------------------------------------------------

/// Caller's preference for ANSI color. Mirrors clap's built-in
/// [`clap::ColorChoice`] values so we can map cleanly from a CLI flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorChoice {
    /// Always emit color codes regardless of terminal detection.
    Always,
    /// Enable color only when the sink is a TTY and `NO_COLOR` is unset.
    Auto,
    /// Never emit color codes.
    Never,
}

impl ColorChoice {
    /// Resolve `Auto` by checking `is_terminal` on the sink and the
    /// `NO_COLOR` environment variable.
    fn effective(self, is_tty: bool) -> bool {
        match self {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            ColorChoice::Auto => {
                if !is_tty {
                    return false;
                }
                std::env::var_os("NO_COLOR").map(|v| v.is_empty()).unwrap_or(true)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Terminal reporter
// ---------------------------------------------------------------------------

/// Render the human-readable summary into `out`.
///
/// Layout follows `docs/design.md` §6. The per-scenario block is
/// omitted when `plan.scenarios.len() <= 1`.
pub fn print_terminal(
    summary: &Summary,
    plan: &Plan,
    color: ColorChoice,
    is_tty: bool,
    out: &mut impl Write,
) -> io::Result<()> {
    let use_color = color.effective(is_tty);
    let p = Painter::new(use_color);

    // -- header: target rate, actual rate, duration ----------------------
    writeln!(
        out,
        "{}    {}",
        p.bold("target rate"),
        p.cyan(&describe_target_rate(plan)),
    )?;
    let actual_rps = summary.requests_per_sec();
    writeln!(
        out,
        "{}    {} req/s",
        p.bold("actual rate"),
        p.green(&format!("{actual_rps:.1}")),
    )?;
    writeln!(
        out,
        "{}       {}  |  total {} requests",
        p.bold("duration"),
        p.cyan(&format_duration(summary.duration)),
        p.bold(&format_count(summary.requests)),
    )?;
    writeln!(out)?;

    // -- latency percentiles ---------------------------------------------
    writeln!(
        out,
        "{}        p50={}  p90={}  p99={}  p99.9={}  max={}",
        p.bold("latency"),
        p.green(&format_ns(summary.latency_p(50.0).as_nanos() as u64)),
        p.green(&format_ns(summary.latency_p(90.0).as_nanos() as u64)),
        p.yellow(&format_ns(summary.latency_p(99.0).as_nanos() as u64)),
        p.yellow(&format_ns(summary.latency_p(99.9).as_nanos() as u64)),
        p.red(&format_ns(summary.latency.max())),
    )?;

    // -- throughput (Phase C: just the average; min/max per-second wait
    //    for Task 12's streaming snapshot mechanism)
    writeln!(
        out,
        "{}     {} req/s",
        p.bold("throughput"),
        p.green(&format!("{actual_rps:.0}")),
    )?;

    // -- errors -----------------------------------------------------------
    let e = &summary.errors;
    writeln!(
        out,
        "{}         connect {}  read {}  write {}  timeout {}  keepup {}",
        p.bold("errors"),
        p.count(e.connect),
        p.count(e.read),
        p.count(e.write),
        p.count(e.timeout),
        p.count(e.keepup),
    )?;
    // status classes: 2xx = requests - (4xx+5xx), assuming every request
    // produced a response (requests does NOT include transport failures).
    let status_2xx = summary.requests.saturating_sub(e.status_4xx + e.status_5xx);
    writeln!(
        out,
        "               status 2xx={}  4xx={}  5xx={}",
        p.green(&format_count(status_2xx)),
        p.count(e.status_4xx),
        p.count(e.status_5xx),
    )?;

    // -- assertions -------------------------------------------------------
    // We don't have a structured per-assertion breakdown yet — just the
    // total pass/fail over every check in the plan.
    let total_checks = total_assertion_count(plan);
    if total_checks > 0 {
        let failed = e.assertion_failed;
        let expected = summary.requests.saturating_mul(total_checks as u64);
        let passed = expected.saturating_sub(failed);
        writeln!(
            out,
            "{}     {}: {}/{} {}",
            p.bold("assertions"),
            describe_assertions(plan),
            p.green(&format_count(passed)),
            p.bold(&format_count(expected)),
            if failed == 0 {
                p.green("ok")
            } else {
                p.red("FAIL")
            },
        )?;
    }

    // -- per-scenario breakdown (only when >1 scenario) ------------------
    if plan.scenarios.len() > 1 {
        writeln!(out)?;
        writeln!(out, "{}", p.bold("scenarios"))?;
        let total = summary.requests.max(1);
        for (i, sc) in summary.per_scenario.iter().enumerate() {
            let name = plan
                .scenarios
                .get(i)
                .map(|s| s.name.as_str())
                .unwrap_or("?");
            let share = sc.requests as f64 * 100.0 / total as f64;
            let p99 = if sc.latency.is_empty() {
                "n/a".to_string()
            } else {
                format_ns(sc.latency.value_at_percentile(99.0))
            };
            writeln!(
                out,
                "  {}  ({:.0}%)  {} req  p99={}  errors {}",
                p.bold(name),
                share,
                format_count(sc.requests),
                p.yellow(&p99),
                p.count(sc.errors.total()),
            )?;
        }
    }

    out.flush()
}

// ---------------------------------------------------------------------------
// Painter — color-aware string wrapper.
// ---------------------------------------------------------------------------
//
// We avoid the yansi global enable/disable because test runners execute
// tests in parallel threads and the global state races. The Painter
// captures the color preference at call time; `yansi::Paint` styles
// are attached with `.whenever(Condition::cached(flag))` so the condition
// is evaluated per-instance and never touches global state.

struct Painter {
    on: Condition,
}

impl Painter {
    fn new(use_color: bool) -> Self {
        let on = if use_color {
            Condition::ALWAYS
        } else {
            Condition::NEVER
        };
        Self { on }
    }

    fn bold(&self, s: &str) -> String {
        format!("{}", s.bold().whenever(self.on))
    }
    fn cyan(&self, s: &str) -> String {
        format!("{}", s.cyan().whenever(self.on))
    }
    fn green(&self, s: &str) -> String {
        format!("{}", s.green().whenever(self.on))
    }
    fn yellow(&self, s: &str) -> String {
        format!("{}", s.yellow().whenever(self.on))
    }
    fn red(&self, s: &str) -> String {
        format!("{}", s.red().whenever(self.on))
    }
    /// Colorize a counter — green when zero, red when nonzero.
    fn count(&self, n: u64) -> String {
        let s = format_count(n);
        if n == 0 {
            self.green(&s)
        } else {
            self.red(&s)
        }
    }
}

// ---------------------------------------------------------------------------
// JSON reporter
// ---------------------------------------------------------------------------

/// Render the structured JSON blob into `out`.
///
/// Schema tag `schema_version: 1` fixes the wire format so the diff
/// tool (Task 13) can reject incompatible versions cleanly.
pub fn print_json(summary: &Summary, plan: &Plan, out: &mut impl Write) -> io::Result<()> {
    let blob = serde_json::json!({
        "schema_version": 1,
        "duration_ms": summary.duration.as_millis() as u64,
        "target_rate": target_rate_json(plan),
        "requests": summary.requests,
        "requests_per_sec": summary.requests_per_sec(),
        "bytes_sent": summary.bytes_sent,
        "bytes_received": summary.bytes_recv,
        "latency_ns": {
            "p50":  summary.latency_p(50.0).as_nanos() as u64,
            "p90":  summary.latency_p(90.0).as_nanos() as u64,
            "p99":  summary.latency_p(99.0).as_nanos() as u64,
            "p99_9": summary.latency_p(99.9).as_nanos() as u64,
            "max":  summary.latency.max(),
        },
        "ttfb_ns": {
            "p50": if summary.ttfb.is_empty() { 0 } else { summary.ttfb.value_at_percentile(50.0) },
            "p90": if summary.ttfb.is_empty() { 0 } else { summary.ttfb.value_at_percentile(90.0) },
            "p99": if summary.ttfb.is_empty() { 0 } else { summary.ttfb.value_at_percentile(99.0) },
            "max": summary.ttfb.max(),
        },
        "errors": {
            "connect": summary.errors.connect,
            "read": summary.errors.read,
            "write": summary.errors.write,
            "timeout": summary.errors.timeout,
            "keepup": summary.errors.keepup,
            "status_4xx": summary.errors.status_4xx,
            "status_5xx": summary.errors.status_5xx,
            "assertion_failed": summary.errors.assertion_failed,
        },
        "scenarios": summary
            .per_scenario
            .iter()
            .enumerate()
            .map(|(i, sc)| {
                serde_json::json!({
                    "name": plan.scenarios.get(i).map(|s| s.name.clone()).unwrap_or_default(),
                    "requests": sc.requests,
                    "latency_p50_ns": if sc.latency.is_empty() { 0 } else { sc.latency.value_at_percentile(50.0) },
                    "latency_p99_ns": if sc.latency.is_empty() { 0 } else { sc.latency.value_at_percentile(99.0) },
                    "errors": {
                        "connect": sc.errors.connect,
                        "read": sc.errors.read,
                        "write": sc.errors.write,
                        "timeout": sc.errors.timeout,
                        "keepup": sc.errors.keepup,
                        "status_4xx": sc.errors.status_4xx,
                        "status_5xx": sc.errors.status_5xx,
                        "assertion_failed": sc.errors.assertion_failed,
                    },
                })
            })
            .collect::<Vec<_>>(),
    });

    serde_json::to_writer_pretty(&mut *out, &blob)?;
    writeln!(out)?;
    out.flush()
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Serialize the plan's rate profile(s) into the JSON `target_rate`
/// field. The diff tool (Task 13) reads this to categorise runs.
///
/// Shape:
///
/// ```json
/// { "kind": "constant", "rps": 2000.0 }
/// { "kind": "ramp", "from": 100, "to": 10000, "over_ms": 30000 }
/// { "kind": "saturate", "max_concurrency": 50 }
/// { "kind": "stepped", "steps": [[0, 100.0], [10000, 500.0]] }
/// ```
///
/// Multi-scenario plans where the profiles differ produce an array of
/// per-scenario entries in scenario order; single-scenario (or all
/// identical) plans produce the scalar shape for diff-tool simplicity.
fn target_rate_json(plan: &Plan) -> serde_json::Value {
    use crate::plan::RateProfile;
    use serde_json::json;

    fn one(p: &RateProfile) -> serde_json::Value {
        match p {
            RateProfile::Constant(rps) => json!({ "kind": "constant", "rps": rps }),
            RateProfile::Ramp { from, to, over } => json!({
                "kind": "ramp",
                "from": from,
                "to": to,
                "over_ms": over.as_millis() as u64,
            }),
            RateProfile::Stepped(steps) => {
                let list: Vec<_> = steps
                    .iter()
                    .map(|(d, r)| json!([d.as_millis() as u64, r]))
                    .collect();
                json!({ "kind": "stepped", "steps": list })
            }
            RateProfile::Saturate { max_concurrency } => json!({
                "kind": "saturate",
                "max_concurrency": max_concurrency,
            }),
        }
    }

    match plan.scenarios.as_slice() {
        [] => serde_json::Value::Null,
        [only] => one(&only.rate),
        many => {
            // Emit an array when profiles differ; otherwise collapse to
            // the scalar shape (every scenario has the same rate).
            let first = one(&many[0].rate);
            let all_same = many
                .iter()
                .skip(1)
                .all(|s| one(&s.rate) == first);
            if all_same {
                first
            } else {
                serde_json::Value::Array(many.iter().map(|s| one(&s.rate)).collect())
            }
        }
    }
}

/// Describe the target rate line. Task 10 swaps this for a real
/// `RateProfile::Constant(r)` rendering once the profile enum lands.
fn describe_target_rate(plan: &Plan) -> String {
    use crate::plan::RateProfile;
    // Pick the first scenario's profile as the "plan rate". Most Phase-C
    // plans are single-scenario; multi-scenario open-loop plans with
    // differing rates render per-scenario below the header.
    match plan.scenarios.first().map(|s| &s.rate) {
        Some(RateProfile::Constant(r)) => format!("{r:.0} req/s constant"),
        Some(RateProfile::Ramp { from, to, over }) => {
            format!("{:.0} → {:.0} req/s over {}", from, to, format_duration(*over))
        }
        Some(RateProfile::Stepped(steps)) => {
            format!("stepped, {} step(s)", steps.len())
        }
        Some(RateProfile::Saturate { max_concurrency }) => {
            format!("saturate ({max_concurrency} tasks)")
        }
        None => "saturate".into(),
    }
}

/// One-line human summary of the assertions in the plan (for the
/// "assertions" report line). Phase C: just the assertion count.
fn describe_assertions(plan: &Plan) -> String {
    use crate::plan::Assertion;
    let mut parts = Vec::new();
    for sc in &plan.scenarios {
        for step in &sc.steps {
            if let crate::plan::Step::Request(r) = step {
                for a in &r.checks {
                    parts.push(match a {
                        Assertion::StatusEq(c) => format!("status=={c}"),
                        Assertion::StatusIn(codes) => {
                            let list: Vec<String> = codes.iter().map(|c| c.to_string()).collect();
                            format!("status in [{}]", list.join(","))
                        }
                        Assertion::LatencyUnder(d) => {
                            format!("latency<{}", format_duration(*d))
                        }
                    });
                }
            }
        }
    }
    if parts.is_empty() {
        "none".into()
    } else {
        parts.join(", ")
    }
}

fn total_assertion_count(plan: &Plan) -> usize {
    let mut total = 0;
    for sc in &plan.scenarios {
        for step in &sc.steps {
            if let crate::plan::Step::Request(r) = step {
                total += r.checks.len();
            }
        }
    }
    total
}

/// Format a duration for the report header (`30.00s`, `1.50m`, etc.).
fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else if secs < 3600.0 {
        format!("{:.2}m", secs / 60.0)
    } else {
        format!("{:.2}h", secs / 3600.0)
    }
}

/// Format a nanosecond count as a sub-second unit with ~3 sig figs.
/// `120` → `"120ns"`, `120_000` → `"120µs"`, `2_100_000` → `"2.1ms"`.
pub(crate) fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{}ns", ns)
    } else if ns < 1_000_000 {
        // microseconds
        let us = ns as f64 / 1_000.0;
        if us < 10.0 {
            format!("{:.1}µs", us)
        } else {
            format!("{:.0}µs", us)
        }
    } else if ns < 1_000_000_000 {
        let ms = ns as f64 / 1_000_000.0;
        if ms < 10.0 {
            format!("{:.1}ms", ms)
        } else {
            format!("{:.0}ms", ms)
        }
    } else {
        let s = ns as f64 / 1_000_000_000.0;
        format!("{:.2}s", s)
    }
}

/// Format a large count with thousand separators (`299827` →
/// `"299,827"`). Done by hand to avoid pulling in a locale crate.
fn format_count(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let offset = bytes.len() % 3;
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && i % 3 == offset {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_ns_scales_correctly() {
        assert_eq!(format_ns(0), "0ns");
        assert_eq!(format_ns(999), "999ns");
        assert_eq!(format_ns(1_000), "1.0µs");
        assert_eq!(format_ns(9_500), "9.5µs");
        assert_eq!(format_ns(120_000), "120µs");
        assert_eq!(format_ns(2_100_000), "2.1ms");
        assert_eq!(format_ns(8_400_000), "8.4ms");
        assert_eq!(format_ns(22_100_000), "22ms");
        assert_eq!(format_ns(1_500_000_000), "1.50s");
    }

    #[test]
    fn format_count_inserts_thousands_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(299_827), "299,827");
        assert_eq!(format_count(1_234_567_890), "1,234,567,890");
    }

    #[test]
    fn format_duration_picks_reasonable_unit() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_secs(30)), "30.00s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1.50m");
        assert_eq!(format_duration(Duration::from_secs(3700)), "1.03h");
    }

    #[test]
    fn color_choice_auto_respects_tty() {
        assert!(!ColorChoice::Auto.effective(false));
        // With NO_COLOR unset, tty=true → color enabled.
        if std::env::var_os("NO_COLOR").is_none() {
            assert!(ColorChoice::Auto.effective(true));
        }
    }

    #[test]
    fn color_choice_never_always_unconditional() {
        assert!(!ColorChoice::Never.effective(true));
        assert!(ColorChoice::Always.effective(false));
    }
}
