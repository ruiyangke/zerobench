//! ARCH STATUS: MOVE → zerobench-report (split into submodules)
//!
//! 1,480 LoC god-file. Split on move:
//!   - zerobench-report::terminal  (print_terminal + formatting)
//!   - zerobench-report::json      (print_json)
//!   - zerobench-report::prometheus (print_prometheus)
//!   - (future) zerobench-report::statsd  — see ARCH-REVIEW §Phase 6
//!   - (future) zerobench-report::otlp    — deferred; see §Phase 8
//!
//! ARCH(keep): protocol-specific rendering stays typed — matches on
//! Option<SseExtras>/Option<WsExtras> from core's ScenarioStats; no
//! string-keyed / HashMap metric tables. pick_latency_source +
//! sse_latency_from_scenarios + ws_latency_from_scenarios stay as
//! typed helpers, just live in zerobench-report.
//!
//! See docs/ARCH-REVIEW-2026-04-20.md §7, §B4 (rendering), §Phase 6.
//!
//! ----------------------------------------------------------------------
//!
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
//!
//! # Byte formatting
//!
//! [`format_bytes`] / [`format_byte_rate`] use SI units (kB = 1000,
//! MB = 1_000_000) rather than IEC (kiB = 1024). Rationale: network
//! throughput is universally quoted in SI by the tools and invoices
//! people compare against (curl, iftop, AWS billing, ISP plans).
//! Keeping report numbers on the same scale avoids "why doesn't my
//! 15 MB/s line match my provider's 16 MB/s" confusion.

use std::io::{self, Write};
use std::time::Duration;

use yansi::{Condition, Paint};

use crate::live_snapshot::LiveTick;
use crate::plan::{Plan, Protocol};
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

    // -- header: target, duration ----------------------------------------
    //
    // Layout is aligned on a 10-char label column so the values line up
    // in a mono font:
    //
    //     target     saturate (20 tasks) (32 threads)
    //     duration   2.00s  |  total 443,447 requests
    //     throughput 221,724 req/s · ↑ 15.3 MB/s  ↓ 52.1 MB/s
    //     transfer   ↑ 30.6 MB sent  ↓ 104.2 MB received
    //     latency    p50=140µs  p90=181µs  p99=198µs  p99.9=250µs  max=2.1ms
    //     errors     ...
    //
    // The `actual rate` line is dropped — its value duplicated
    // `throughput` with slightly different rounding.
    writeln!(
        out,
        "{}         {}",
        p.bold("target"),
        p.cyan(&describe_target_rate(plan)),
    )?;

    // Pick "operations" when the plan mixes protocols, "requests" when
    // it's pure HTTP. This keeps the single-protocol report terse
    // while making multi-protocol runs unambiguous.
    let op_label = operation_noun(plan);
    let op_rate_label = rate_noun(plan);
    writeln!(
        out,
        "{}       {}  |  total {} {}",
        p.bold("duration"),
        p.cyan(&format_duration(summary.duration)),
        p.bold(&format_count(summary.requests)),
        op_label,
    )?;

    // -- throughput (req/s + byte rate, combined) ------------------------
    //
    // If the transport doesn't populate bytes (SSE/WS backends historically
    // zeroed them out) we collapse to the plain `<req/s>` form so we
    // never print a misleading `0 B/s`. Otherwise we append the byte
    // rates in both directions.
    let actual_rps = summary.requests_per_sec();
    let has_bytes = summary.bytes_sent != 0 || summary.bytes_recv != 0;
    if has_bytes {
        writeln!(
            out,
            "{}     {} {} · ↑ {}  ↓ {}",
            p.bold("throughput"),
            p.green(&format_rps(actual_rps)),
            op_rate_label,
            p.cyan(&format_byte_rate(summary.bytes_sent, summary.duration)),
            p.cyan(&format_byte_rate(summary.bytes_recv, summary.duration)),
        )?;
        writeln!(
            out,
            "{}       ↑ {} sent  ↓ {} received",
            p.bold("transfer"),
            p.cyan(&format_bytes(summary.bytes_sent)),
            p.cyan(&format_bytes(summary.bytes_recv)),
        )?;
    } else {
        writeln!(
            out,
            "{}     {} {}",
            p.bold("throughput"),
            p.green(&format_rps(actual_rps)),
            op_rate_label,
        )?;
    }

    // -- latency percentiles ---------------------------------------------
    //
    // protocol-aware source. For SSE-only plans we report
    // chunk_gap (inter-event gap) from the scenario extras; for
    // WS-only plans we report rtt. Mixed or HTTP plans use the
    // aggregate `summary.latency` as before.
    let (lat_label, lat_p50, lat_p90, lat_p99, lat_p999, lat_max) =
        pick_latency_source(summary, plan);
    writeln!(
        out,
        "{}      p50={}  p90={}  p99={}  p99.9={}  max={}",
        p.bold(lat_label),
        p.green(&format_ns(lat_p50)),
        p.green(&format_ns(lat_p90)),
        p.yellow(&format_ns(lat_p99)),
        p.yellow(&format_ns(lat_p999)),
        p.red(&format_ns(lat_max)),
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
        // Name column width — pad to the longest name for tabular
        // alignment, but cap at 20 so one malformed name doesn't shove
        // everything rightwards.
        let name_width = plan
            .scenarios
            .iter()
            .map(|s| s.name.len())
            .max()
            .unwrap_or(0)
            .min(20);
        for (i, sc) in summary.per_scenario.iter().enumerate() {
            let scenario = plan.scenarios.get(i);
            let name = scenario.map(|s| s.name.as_str()).unwrap_or("?");
            let protocol = scenario.map(|s| s.protocol()).unwrap_or(Protocol::Http);
            let share = sc.requests as f64 * 100.0 / total as f64;

            // Protocol badge (3 chars, uppercase, padded) — tabular
            // alignment across rows.
            let badge = match protocol {
                Protocol::Http => "HTTP",
                Protocol::Sse => "SSE ",
                Protocol::Ws => "WS  ",
            };

            match protocol {
                Protocol::Http => {
                    let p50 = if sc.latency.is_empty() {
                        "n/a".to_string()
                    } else {
                        format_ns(sc.latency.value_at_percentile(50.0))
                    };
                    let p99 = if sc.latency.is_empty() {
                        "n/a".to_string()
                    } else {
                        format_ns(sc.latency.value_at_percentile(99.0))
                    };
                    let p999 = if sc.latency.is_empty() {
                        "n/a".to_string()
                    } else {
                        format_ns(sc.latency.value_at_percentile(99.9))
                    };
                    writeln!(
                        out,
                        "  {:<name_width$}  {}  ({:>3.0}%)  {} req  p50={}  p99={}  p99.9={}  errors {}",
                        p.bold(name),
                        p.cyan(badge),
                        share,
                        format_count(sc.requests),
                        p.green(&p50),
                        p.yellow(&p99),
                        p.yellow(&p999),
                        p.count(sc.errors.total()),
                        name_width = name_width,
                    )?;
                }
                Protocol::Sse => {
                    let extras = sc.sse.as_ref();
                    let chunks = extras.map(|e| e.chunks).unwrap_or(0);
                    let ttfb_p99 = match extras {
                        Some(e) if !e.ttfb.is_empty() => {
                            format_ns(e.ttfb.value_at_percentile(99.0))
                        }
                        _ => "n/a".to_string(),
                    };
                    let chunk_p99 = match extras {
                        Some(e) if !e.chunk_gap.is_empty() => {
                            format_ns(e.chunk_gap.value_at_percentile(99.0))
                        }
                        _ => "n/a".to_string(),
                    };
                    writeln!(
                        out,
                        "  {:<name_width$}  {}  ({:>3.0}%)  {} streams  {} chunks  TTFB p99={}  chunk p99={}  errors {}",
                        p.bold(name),
                        p.cyan(badge),
                        share,
                        format_count(sc.requests),
                        format_count(chunks),
                        p.yellow(&ttfb_p99),
                        p.yellow(&chunk_p99),
                        p.count(sc.errors.total()),
                        name_width = name_width,
                    )?;
                }
                Protocol::Ws => {
                    let extras = sc.ws.as_ref();
                    let msgs = extras.map(|e| e.messages_recv).unwrap_or(0);
                    let rtt_p99 = match extras {
                        Some(e) if !e.rtt.is_empty() => {
                            format_ns(e.rtt.value_at_percentile(99.0))
                        }
                        _ => "n/a".to_string(),
                    };
                    let hs_p99 = match extras {
                        Some(e) if !e.handshake.is_empty() => {
                            format_ns(e.handshake.value_at_percentile(99.0))
                        }
                        _ => "n/a".to_string(),
                    };
                    writeln!(
                        out,
                        "  {:<name_width$}  {}  ({:>3.0}%)  {} conns  {} msgs  RTT p99={}  handshake p99={}  errors {}",
                        p.bold(name),
                        p.cyan(badge),
                        share,
                        format_count(sc.requests),
                        format_count(msgs),
                        p.yellow(&rtt_p99),
                        p.yellow(&hs_p99),
                        p.count(sc.errors.total()),
                        name_width = name_width,
                    )?;
                }
            }
        }
    }

    out.flush()
}

/// Pick the plural noun that best matches the plan — "requests" when
/// every scenario is HTTP, "operations" when a mix of SSE/WS lives
/// alongside.
///
/// Keeping "requests" for the pure-HTTP case preserves the ergonomics
/// the single-protocol CLI report established over many versions.
fn operation_noun(plan: &Plan) -> &'static str {
    let mut has_non_http = false;
    for s in &plan.scenarios {
        if s.protocol() != Protocol::Http {
            has_non_http = true;
            break;
        }
    }
    if has_non_http {
        "operations"
    } else {
        "requests"
    }
}

/// Singular rate label matching `operation_noun` — `"req/s"` for pure
/// HTTP, `"ops/s"` for mixed plans.
fn rate_noun(plan: &Plan) -> &'static str {
    let mut has_non_http = false;
    for s in &plan.scenarios {
        if s.protocol() != Protocol::Http {
            has_non_http = true;
            break;
        }
    }
    if has_non_http {
        "ops/s"
    } else {
        "req/s"
    }
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
// JSONL tick reporter
// ---------------------------------------------------------------------------

/// Render one per-second tick as a single JSON line. Used by the
/// streaming `--format jsonl` path: the CLI calls this once per second
/// during the run, piping to stdout while the terminal summary goes
/// to stderr at end-of-run.
///
/// Format is stable. Consumers (Grafana, kibana, ad-hoc jq pipelines)
/// must be able to round-trip every field as JSON — no NaN, no Infinity.
pub fn print_jsonl_tick(tick: &LiveTick, out: &mut impl Write) -> io::Result<()> {
    let t_secs = tick.elapsed.as_secs_f64();
    // The aggregator's window is 1s by convention (the ticker wakes on
    // integer-second boundaries, and `LiveSnapshot` swap resets the
    // bucket every call). For the final partial tick the window may be
    // shorter, but we preserve a 1-second denominator so consumers can
    // treat `rps` as a per-second rate regardless of where the run
    // ended. Downstream callers that care about the exact per-window
    // rate can compute `requests_delta / (t - prev_t)` themselves.
    //
    // Schema kept stable (u64): consumers already expect an integer rps
    // field. To switch to f64 we'd bump the JSONL version, which is a
    // breaking change we're not taking in this pass.
    let rps_u64 = tick.requests;

    let blob = serde_json::json!({
        "t": round2(t_secs),
        "rps": rps_u64,
        "requests_delta": tick.requests,
        "bytes_sent": tick.bytes_sent,
        "bytes_recv": tick.bytes_recv,
        "p50_ns":  tick.latency_p_ns(50.0),
        "p90_ns":  tick.latency_p_ns(90.0),
        "p99_ns":  tick.latency_p_ns(99.0),
        "p99_9_ns": tick.latency_p_ns(99.9),
        "errors": {
            "connect": tick.errors.connect,
            "read": tick.errors.read,
            "write": tick.errors.write,
            "timeout": tick.errors.timeout,
            "keepup": tick.errors.keepup,
            "status_4xx": tick.errors.status_4xx,
            "status_5xx": tick.errors.status_5xx,
            "assertion_failed": tick.errors.assertion_failed,
        },
    });
    // Compact, single-line JSON — jq-friendly, one record per line.
    serde_json::to_writer(&mut *out, &blob)?;
    writeln!(out)?;
    out.flush()
}

/// Round a float to 2 decimal places for stable JSON output.
fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

// ---------------------------------------------------------------------------
// Prometheus textfile reporter
// ---------------------------------------------------------------------------

/// Render the final summary as a Prometheus textfile-format block.
///
/// One block, newline-separated, ready to drop into a
/// `textfile_collector` directory or to scrape via `prometheus-file-sd`.
/// Emits the four canonical metric families zerobench tracks:
///
/// - `zerobench_requests_total` (counter)
/// - `zerobench_latency_seconds` (summary with p50/p90/p99/p99.9,
///   plus `_sum` and `_count`)
/// - `zerobench_errors_total{category=...}` (counter, one series per
///   error category)
/// - `zerobench_bytes_sent_total` / `zerobench_bytes_received_total`
///   (counters)
pub fn print_prometheus(
    summary: &Summary,
    _plan: &Plan,
    out: &mut impl Write,
) -> io::Result<()> {
    // requests_total
    writeln!(
        out,
        "# HELP zerobench_requests_total Total HTTP requests executed."
    )?;
    writeln!(out, "# TYPE zerobench_requests_total counter")?;
    writeln!(out, "zerobench_requests_total {}", summary.requests)?;
    writeln!(out)?;

    // latency_seconds — Prometheus convention is seconds-not-ns.
    writeln!(
        out,
        "# HELP zerobench_latency_seconds HTTP request latency."
    )?;
    writeln!(out, "# TYPE zerobench_latency_seconds summary")?;
    let p50 = ns_to_seconds(pct_ns(summary, 50.0));
    let p90 = ns_to_seconds(pct_ns(summary, 90.0));
    let p99 = ns_to_seconds(pct_ns(summary, 99.0));
    let p999 = ns_to_seconds(pct_ns(summary, 99.9));
    writeln!(
        out,
        "zerobench_latency_seconds{{quantile=\"0.5\"}} {}",
        format_f64(p50)
    )?;
    writeln!(
        out,
        "zerobench_latency_seconds{{quantile=\"0.9\"}} {}",
        format_f64(p90)
    )?;
    writeln!(
        out,
        "zerobench_latency_seconds{{quantile=\"0.99\"}} {}",
        format_f64(p99)
    )?;
    writeln!(
        out,
        "zerobench_latency_seconds{{quantile=\"0.999\"}} {}",
        format_f64(p999)
    )?;
    // Compute sum as mean(count) * count. HDR doesn't retain exact
    // samples; we approximate with mean.
    let mean_ns = summary.latency.mean();
    let count = summary.requests;
    let sum_seconds = (mean_ns / 1e9) * count as f64;
    writeln!(
        out,
        "zerobench_latency_seconds_sum {}",
        format_f64(sum_seconds)
    )?;
    writeln!(out, "zerobench_latency_seconds_count {count}")?;
    writeln!(out)?;

    // errors_total by category
    writeln!(out, "# HELP zerobench_errors_total Errors by category.")?;
    writeln!(out, "# TYPE zerobench_errors_total counter")?;
    let e = &summary.errors;
    let pairs = [
        ("connect", e.connect),
        ("read", e.read),
        ("write", e.write),
        ("timeout", e.timeout),
        ("keepup", e.keepup),
        ("status_4xx", e.status_4xx),
        ("status_5xx", e.status_5xx),
        ("assertion_failed", e.assertion_failed),
    ];
    for (cat, n) in pairs {
        writeln!(
            out,
            "zerobench_errors_total{{category=\"{cat}\"}} {n}"
        )?;
    }
    writeln!(out)?;

    // bytes counters
    writeln!(
        out,
        "# HELP zerobench_bytes_sent_total Request bytes sent on-wire."
    )?;
    writeln!(out, "# TYPE zerobench_bytes_sent_total counter")?;
    writeln!(out, "zerobench_bytes_sent_total {}", summary.bytes_sent)?;
    writeln!(out)?;

    writeln!(
        out,
        "# HELP zerobench_bytes_received_total Response bytes received on-wire."
    )?;
    writeln!(out, "# TYPE zerobench_bytes_received_total counter")?;
    writeln!(
        out,
        "zerobench_bytes_received_total {}",
        summary.bytes_recv
    )?;

    out.flush()
}

fn pct_ns(summary: &Summary, pct: f64) -> u64 {
    if summary.latency.is_empty() {
        0
    } else {
        summary.latency.value_at_percentile(pct)
    }
}

/// Pick the right latency source + label for the terminal report's
/// `latency`-line based on plan protocol.
///
/// - All scenarios HTTP → aggregate `summary.latency` (HTTP req/resp).
/// - All scenarios SSE → sum per-scenario `sse.chunk_gap` histograms
///   (inter-event gap — the primary SSE latency axis).
/// - All scenarios WS → sum per-scenario `ws.rtt` histograms.
/// - Mixed → aggregate `summary.latency` (HTTP-centric fallback; the
///   per-scenario breakdown in the scenarios panel shows the rest).
///
/// Returns `(label, p50, p90, p99, p999, max)` all in nanoseconds.
fn pick_latency_source(summary: &Summary, plan: &Plan) -> (&'static str, u64, u64, u64, u64, u64) {
    let protocols: std::collections::HashSet<Protocol> =
        plan.scenarios.iter().map(|s| s.protocol()).collect();

    // Single-protocol fast paths.
    if protocols.len() == 1 {
        match protocols.iter().next().copied() {
            Some(Protocol::Sse) => return sse_latency_from_scenarios(summary),
            Some(Protocol::Ws) => return ws_latency_from_scenarios(summary),
            _ => {}
        }
    }

    // HTTP-or-mixed: aggregate.
    let hist = &summary.latency;
    let label = if protocols.len() > 1 { "latency" } else { "latency" };
    (
        label,
        if hist.is_empty() { 0 } else { hist.value_at_percentile(50.0) },
        if hist.is_empty() { 0 } else { hist.value_at_percentile(90.0) },
        if hist.is_empty() { 0 } else { hist.value_at_percentile(99.0) },
        if hist.is_empty() { 0 } else { hist.value_at_percentile(99.9) },
        hist.max(),
    )
}

fn sse_latency_from_scenarios(summary: &Summary) -> (&'static str, u64, u64, u64, u64, u64) {
    // Per protocol-native semantics: prefer the dedicated
    // broadcast_rtt slot for SseFanout scenarios; fall back to
    // chunk_gap for SseHold / SseReconnectStorm. An SSE scenario
    // populates exactly one of the two.
    let mut broadcast = crate::histogram::new_hist();
    let mut chunk_gap = crate::histogram::new_hist();
    for sc in &summary.per_scenario {
        if let Some(sse) = sc.sse.as_ref() {
            let _ = broadcast.add(&sse.broadcast_rtt);
            let _ = chunk_gap.add(&sse.chunk_gap);
        }
    }
    let (label, agg) = if !broadcast.is_empty() {
        ("broadcast-rtt", broadcast)
    } else {
        ("chunk-gap", chunk_gap)
    };
    if agg.is_empty() {
        (label, 0, 0, 0, 0, 0)
    } else {
        (
            label,
            agg.value_at_percentile(50.0),
            agg.value_at_percentile(90.0),
            agg.value_at_percentile(99.0),
            agg.value_at_percentile(99.9),
            agg.max(),
        )
    }
}

fn ws_latency_from_scenarios(summary: &Summary) -> (&'static str, u64, u64, u64, u64, u64) {
    // As with SSE: WsFanout writes broadcast_rtt and leaves rtt
    // empty; WsEchoRtt / WsServerPushRtt populate rtt. Prefer
    // broadcast_rtt when non-empty.
    let mut broadcast = crate::histogram::new_hist();
    let mut rtt = crate::histogram::new_hist();
    for sc in &summary.per_scenario {
        if let Some(ws) = sc.ws.as_ref() {
            let _ = broadcast.add(&ws.broadcast_rtt);
            let _ = rtt.add(&ws.rtt);
        }
    }
    let (label, agg) = if !broadcast.is_empty() {
        ("broadcast-rtt", broadcast)
    } else {
        ("rtt", rtt)
    };
    if agg.is_empty() {
        (label, 0, 0, 0, 0, 0)
    } else {
        (
            label,
            agg.value_at_percentile(50.0),
            agg.value_at_percentile(90.0),
            agg.value_at_percentile(99.0),
            agg.value_at_percentile(99.9),
            agg.max(),
        )
    }
}

/// Return a borrowed reference to the histogram that carries the
/// PRIMARY latency signal for this plan — the same slot the terminal
/// report renders via [`pick_latency_source`], but as a histogram the
/// caller can feed into `write_histlog`.
///
/// HTTP: `summary.latency`. SSE: `broadcast_rtt` if any scenario
/// populated it, else aggregated `chunk_gap`. WS: `broadcast_rtt` if
/// any, else aggregated `rtt`. On a mixed plan this falls back to
/// `summary.latency` (HTTP-centric aggregate) — the per-scenario
/// sidecars carry the rest.
pub fn pick_primary_histogram<'a>(
    summary: &'a Summary,
    plan: &Plan,
) -> &'a hdrhistogram::Histogram<u64> {
    // Single-protocol SSE or WS → return the per-extras histogram
    // cached on scenario[0]. When the plan has multiple SSE/WS
    // scenarios, we'd have to allocate a fresh aggregate — that
    // allocation doesn't fit the borrowed-return signature, so
    // multi-scenario SSE/WS still writes summary.latency.
    let protocols: std::collections::HashSet<Protocol> =
        plan.scenarios.iter().map(|s| s.protocol()).collect();
    if protocols.len() == 1 && plan.scenarios.len() == 1 {
        if let Some(sc) = summary.per_scenario.first() {
            match protocols.iter().next().copied() {
                Some(Protocol::Sse) => {
                    if let Some(sse) = sc.sse.as_ref() {
                        if !sse.broadcast_rtt.is_empty() {
                            return &sse.broadcast_rtt;
                        }
                        if !sse.chunk_gap.is_empty() {
                            return &sse.chunk_gap;
                        }
                    }
                }
                Some(Protocol::Ws) => {
                    if let Some(ws) = sc.ws.as_ref() {
                        if !ws.broadcast_rtt.is_empty() {
                            return &ws.broadcast_rtt;
                        }
                        if !ws.rtt.is_empty() {
                            return &ws.rtt;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    &summary.latency
}

fn ns_to_seconds(ns: u64) -> f64 {
    ns as f64 / 1e9
}

/// Prometheus-friendly f64 — no NaN / Infinity (both map to 0), fixed
/// decimal with enough precision for a ns-scale value in seconds.
fn format_f64(x: f64) -> String {
    if !x.is_finite() {
        "0".to_string()
    } else if x == 0.0 {
        "0".to_string()
    } else {
        format!("{x:.6}")
    }
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
    let threads = plan.threads;
    let thread_suffix = if threads > 1 {
        format!(" ({threads} threads)")
    } else {
        String::new()
    };
    // Pick the first scenario's profile as the "plan rate". Most Phase-C
    // plans are single-scenario; multi-scenario open-loop plans with
    // differing rates render per-scenario below the header.
    match plan.scenarios.first().map(|s| &s.rate) {
        Some(RateProfile::Constant(r)) => format!("{r:.0} req/s constant{thread_suffix}"),
        Some(RateProfile::Ramp { from, to, over }) => {
            format!(
                "{:.0} → {:.0} req/s over {}{}",
                from,
                to,
                format_duration(*over),
                thread_suffix,
            )
        }
        Some(RateProfile::Stepped(steps)) => {
            format!("stepped, {} step(s){}", steps.len(), thread_suffix)
        }
        Some(RateProfile::Saturate { max_concurrency }) => {
            format!("saturate ({max_concurrency} tasks){thread_suffix}")
        }
        None => format!("saturate{thread_suffix}"),
    }
}

/// One-line human summary of the assertions in the plan (for the
/// "assertions" report line). 
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
pub fn format_duration(d: Duration) -> String {
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
pub fn format_ns(ns: u64) -> String {
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
pub fn format_count(n: u64) -> String {
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

/// Format a byte count using SI units (kB = 1000, MB = 1_000_000, etc.).
///
/// We deliberately pick SI over IEC (kiB=1024): the units that show up in
/// the report mirror what Linux's `curl`/`iftop`/`nethogs`/AWS billing all
/// use for network transfer, and keeping the network and storage numbers
/// on the same scale avoids "why does my 15 MB/s line not match my
/// provider's 16 MB/s" confusion. Below 1 kB we print raw bytes so tiny
/// values render cleanly.
///
/// ```
/// # use zerobench_core::report::format_bytes;
/// assert_eq!(format_bytes(0), "0 B");
/// assert_eq!(format_bytes(512), "512 B");
/// assert_eq!(format_bytes(1_000), "1.0 kB");
/// assert_eq!(format_bytes(15_300_000), "15.3 MB");
/// ```
pub fn format_bytes(n: u64) -> String {
    const KB: u64 = 1_000;
    const MB: u64 = 1_000_000;
    const GB: u64 = 1_000_000_000;
    const TB: u64 = 1_000_000_000_000;

    if n < KB {
        format!("{} B", n)
    } else if n < MB {
        format!("{:.1} kB", n as f64 / KB as f64)
    } else if n < GB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n < TB {
        format!("{:.1} GB", n as f64 / GB as f64)
    } else {
        format!("{:.1} TB", n as f64 / TB as f64)
    }
}

/// Format a byte count over a duration as an SI byte rate (`"15.3 MB/s"`).
///
/// A zero or negative duration produces `"0 B/s"` to avoid NaN /
/// infinity in the report.
pub fn format_byte_rate(bytes: u64, duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs <= 0.0 || bytes == 0 {
        return "0 B/s".to_string();
    }
    let rate = (bytes as f64 / secs).round() as u64;
    format!("{}/s", format_bytes(rate))
}

/// Format a req/s value for the throughput line.
///
/// Uses thousands separators on the integer part (`221,724 req/s`) — no
/// decimals, because the reader doesn't care about 0.5 req/s and the
/// separator carries more info at this scale.
fn format_rps(rps: f64) -> String {
    if !rps.is_finite() || rps < 0.0 {
        return "0".to_string();
    }
    format_count(rps.round() as u64)
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

    #[test]
    fn format_bytes_uses_si_units() {
        // Below 1 kB: raw bytes, no decimals.
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(999), "999 B");

        // 1 kB boundary (SI: 1000, not 1024).
        assert_eq!(format_bytes(1_000), "1.0 kB");
        assert_eq!(format_bytes(1_500), "1.5 kB");
        assert_eq!(format_bytes(999_999), "1000.0 kB");

        // Megabytes.
        assert_eq!(format_bytes(1_000_000), "1.0 MB");
        assert_eq!(format_bytes(15_300_000), "15.3 MB");
        assert_eq!(format_bytes(104_200_000), "104.2 MB");

        // Gigabytes.
        assert_eq!(format_bytes(1_000_000_000), "1.0 GB");
        assert_eq!(format_bytes(2_500_000_000), "2.5 GB");

        // Terabytes.
        assert_eq!(format_bytes(1_000_000_000_000), "1.0 TB");
    }

    #[test]
    fn format_byte_rate_divides_by_duration() {
        // 15.3 MB/s = 15,300,000 B over 1s.
        assert_eq!(
            format_byte_rate(15_300_000, Duration::from_secs(1)),
            "15.3 MB/s"
        );
        // 52.1 MB/s = 104,200,000 B over 2s.
        assert_eq!(
            format_byte_rate(104_200_000, Duration::from_secs(2)),
            "52.1 MB/s"
        );
        // 500 B/s.
        assert_eq!(
            format_byte_rate(500, Duration::from_secs(1)),
            "500 B/s"
        );
    }

    #[test]
    fn format_byte_rate_handles_edge_cases() {
        // Zero duration → 0 B/s (no division-by-zero).
        assert_eq!(
            format_byte_rate(1_000_000, Duration::from_secs(0)),
            "0 B/s"
        );
        // Zero bytes → 0 B/s regardless of duration.
        assert_eq!(format_byte_rate(0, Duration::from_secs(5)), "0 B/s");
    }

    #[test]
    fn print_terminal_omits_transfer_line_when_bytes_zero() {
        use crate::plan::{Mode, Plan, RateProfile, RequestPlan, Scenario, Step};
        use crate::stats::{Summary, TaskStats};
        use crate::template::Template;
        use crate::var::VarRegistry;

        let mut vars = VarRegistry::new();
        let url = Template::compile("/", &mut vars).unwrap();
        let plan = Plan {
            scenarios: vec![Scenario {
                name: "bench".into(),
                rate: RateProfile::Saturate { max_concurrency: 8 },
                steps: vec![Step::Request(RequestPlan::get(url))],
            }],
            vars,
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::default(),
            name: String::new(),
        };

        // Record requests but no bytes (SSE/WS style).
        let mut stats = TaskStats::new(1);
        for _ in 0..100 {
            stats.record(0, Duration::from_micros(100), Duration::from_micros(50), 0, 0);
        }
        let summary = Summary::merge(vec![stats], Duration::from_secs(1));

        let mut out = Vec::new();
        print_terminal(&summary, &plan, ColorChoice::Never, false, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            !s.contains("transfer"),
            "expected no 'transfer' line when bytes are zero:\n{s}"
        );
        // Throughput line should still be present — just without the arrows.
        assert!(s.contains("throughput"), "missing throughput:\n{s}");
        assert!(!s.contains('↑'), "unexpected ↑ arrow in byte-less report:\n{s}");
    }

    #[test]
    fn print_terminal_includes_transfer_line_when_bytes_present() {
        use crate::plan::{Mode, Plan, RateProfile, RequestPlan, Scenario, Step};
        use crate::stats::{Summary, TaskStats};
        use crate::template::Template;
        use crate::var::VarRegistry;

        let mut vars = VarRegistry::new();
        let url = Template::compile("/", &mut vars).unwrap();
        let plan = Plan {
            scenarios: vec![Scenario {
                name: "bench".into(),
                rate: RateProfile::Saturate { max_concurrency: 8 },
                steps: vec![Step::Request(RequestPlan::get(url))],
            }],
            vars,
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::default(),
            name: String::new(),
        };
        let mut stats = TaskStats::new(1);
        for _ in 0..100 {
            stats.record(
                0,
                Duration::from_micros(100),
                Duration::from_micros(50),
                200,
                1_000,
            );
        }
        let summary = Summary::merge(vec![stats], Duration::from_secs(1));

        let mut out = Vec::new();
        print_terminal(&summary, &plan, ColorChoice::Never, false, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("transfer"), "missing 'transfer' line:\n{s}");
        assert!(s.contains('↑'), "missing ↑ arrow:\n{s}");
        assert!(s.contains('↓'), "missing ↓ arrow:\n{s}");
        // 100 * 200 B = 20 kB sent, 100 * 1000 B = 100 kB received.
        assert!(s.contains("20.0 kB"), "expected '20.0 kB' in transfer:\n{s}");
        assert!(s.contains("100.0 kB"), "expected '100.0 kB' in transfer:\n{s}");
    }

    #[test]
    fn format_rps_rounds_and_adds_separators() {
        assert_eq!(format_rps(0.0), "0");
        assert_eq!(format_rps(42.6), "43");
        assert_eq!(format_rps(221_723.5), "221,724");
        assert_eq!(format_rps(1_000_000.0), "1,000,000");
        // Non-finite / negative → "0" (defensive: never print NaN).
        assert_eq!(format_rps(f64::NAN), "0");
        assert_eq!(format_rps(f64::INFINITY), "0");
        assert_eq!(format_rps(-5.0), "0");
    }

    // -----------------------------------------------------------------
    // Tier-1 multi-protocol report smoke tests
    // -----------------------------------------------------------------

    fn plan_with_protocols(entries: &[(&str, Protocol)]) -> Plan {
        use crate::plan::{
            Mode, Plan, RateProfile, RequestPlan, Scenario, SseHoldPlan, Step, WsEchoRttPlan,
        };
        use crate::template::Template;
        use crate::var::VarRegistry;
        use smallvec::SmallVec;

        let mut vars = VarRegistry::new();
        let url = Template::compile("/", &mut vars).unwrap();
        let scenarios: Vec<Scenario> = entries
            .iter()
            .map(|(name, proto)| {
                let step = match proto {
                    Protocol::Http => Step::Request(RequestPlan::get(url.clone())),
                    Protocol::Sse => Step::SseHold(SseHoldPlan {
                        url: url.clone(),
                        headers: SmallVec::new(),
                        subscribers: 1,
                        hold_for: Duration::from_secs(1),
                        reconnect: true,
                    }),
                    Protocol::Ws => Step::WsEchoRtt(WsEchoRttPlan {
                        url: url.clone(),
                        headers: SmallVec::new(),
                        connections: 1,
                        msg_rate_per_conn: 1.0,
                        correlate: crate::plan::CorrelateStrategy::PingPong,
                        payload: url.clone(),
                    }),
                };
                Scenario {
                    name: name.to_string(),
                    rate: RateProfile::Saturate {
                        max_concurrency: 8,
                    },
                    steps: vec![step],
                }
            })
            .collect();
        Plan {
            scenarios,
            vars,
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::default(),
            name: String::new(),
        }
    }

    #[test]
    fn operation_noun_is_requests_for_pure_http() {
        let plan = plan_with_protocols(&[("a", Protocol::Http), ("b", Protocol::Http)]);
        assert_eq!(operation_noun(&plan), "requests");
        assert_eq!(rate_noun(&plan), "req/s");
    }

    #[test]
    fn operation_noun_is_operations_for_mixed_plan() {
        let plan =
            plan_with_protocols(&[("h", Protocol::Http), ("s", Protocol::Sse)]);
        assert_eq!(operation_noun(&plan), "operations");
        assert_eq!(rate_noun(&plan), "ops/s");
    }

    #[test]
    fn print_terminal_uses_operations_label_when_mixed() {
        use crate::stats::{Summary, TaskStats};
        let plan =
            plan_with_protocols(&[("h", Protocol::Http), ("s", Protocol::Sse)]);
        let mut stats = TaskStats::new(plan.scenarios.len());
        stats.record(
            0,
            Duration::from_micros(100),
            Duration::from_micros(50),
            10,
            20,
        );
        stats.record(
            1,
            Duration::from_millis(5),
            Duration::from_millis(1),
            5,
            100,
        );
        // SSE-specific counters for scenario 1.
        stats.per_scenario[1].sse_mut().chunks = 42;
        stats.per_scenario[1].sse_mut().streams_completed = 1;
        let _ = stats.per_scenario[1]
            .sse_mut()
            .ttfb
            .record(1_000_000);

        let summary = Summary::merge(vec![stats], Duration::from_secs(1));
        let mut out = Vec::new();
        print_terminal(&summary, &plan, ColorChoice::Never, false, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("operations"), "expected 'operations' label:\n{s}");
        assert!(s.contains("ops/s"), "expected 'ops/s' label:\n{s}");
        // Per-scenario badges render.
        assert!(s.contains("HTTP"), "expected HTTP badge:\n{s}");
        assert!(s.contains("SSE"), "expected SSE badge:\n{s}");
        // Per-scenario SSE metrics surface.
        assert!(s.contains("chunks"), "expected chunk counter:\n{s}");
    }

    #[test]
    fn print_terminal_ws_row_shows_conns_and_msgs() {
        use crate::stats::{Summary, TaskStats};
        let plan = plan_with_protocols(&[
            ("h", Protocol::Http),
            ("w", Protocol::Ws),
        ]);
        let mut stats = TaskStats::new(plan.scenarios.len());
        stats.record(
            0,
            Duration::from_micros(100),
            Duration::from_micros(50),
            10,
            20,
        );
        stats.record(
            1,
            Duration::from_millis(2),
            Duration::from_millis(1),
            50,
            50,
        );
        let wx = stats.per_scenario[1].ws_mut();
        wx.messages_sent = 1;
        wx.messages_recv = 1;
        let _ = wx.rtt.record(2_000_000);
        let _ = wx.handshake.record(5_000_000);

        let summary = Summary::merge(vec![stats], Duration::from_secs(1));
        let mut out = Vec::new();
        print_terminal(&summary, &plan, ColorChoice::Never, false, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("WS"), "expected WS badge:\n{s}");
        assert!(s.contains("conns"), "expected conns counter:\n{s}");
        assert!(s.contains("msgs"), "expected msgs counter:\n{s}");
        assert!(s.contains("RTT"), "expected RTT label:\n{s}");
    }
}
