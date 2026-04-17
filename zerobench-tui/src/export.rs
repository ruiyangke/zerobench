//! Report export — serializes the TUI's accumulated state to a JSON file.
//!
//! Richer than the CLI's `print_json` because it includes the full
//! time-series tick history, peak/min/avg stats, and transport metadata.
//! Users can load this for offline analysis or feed it to the `diff` tool.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::json;

use crate::state::DashboardState;

/// Generate a timestamped default filename: `zerobench-YYYY-MM-DDThh-mm-ss.json`
fn default_filename() -> String {
    // Use SystemTime for the filename since Instant has no calendar meaning.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // Simple UTC breakdown (no chrono dep).
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    // Hinnant's algorithm for civil date from days since epoch.
    let (y, mo, d) = civil_from_days(days as i64);
    format!("zerobench-{y:04}-{mo:02}-{d:02}T{h:02}-{m:02}-{s:02}.json")
}

/// Export the dashboard state to a JSON file.
///
/// - If `path` is a directory, generates a timestamped filename inside it.
/// - If `path` is `None`, uses the current working directory with a
///   timestamped filename.
/// - Returns the path that was actually written.
pub fn export_report(
    state: &DashboardState,
    path: Option<&Path>,
) -> Result<PathBuf, String> {
    let file_path = match path {
        Some(p) if p.is_dir() => p.join(default_filename()),
        Some(p) => p.to_path_buf(),
        None => PathBuf::from(default_filename()),
    };

    let json = build_json(state);
    let pretty = serde_json::to_string_pretty(&json)
        .map_err(|e| format!("json serialize: {e}"))?;

    let mut f = std::fs::File::create(&file_path)
        .map_err(|e| format!("create {}: {e}", file_path.display()))?;
    f.write_all(pretty.as_bytes())
        .map_err(|e| format!("write {}: {e}", file_path.display()))?;

    Ok(file_path)
}

fn build_json(state: &DashboardState) -> serde_json::Value {
    let elapsed = state.elapsed();
    let avg_rps = if elapsed.as_secs_f64() > 0.0 {
        state.total_requests as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    // Rolling latency from last 5s window.
    let rolling = state.rolling_latency();
    let latency = match &rolling {
        Some(h) => json!({
            "p50_ns": h.value_at_percentile(50.0),
            "p90_ns": h.value_at_percentile(90.0),
            "p99_ns": h.value_at_percentile(99.0),
            "p99_9_ns": h.value_at_percentile(99.9),
            "max_ns": h.max(),
        }),
        None => json!(null),
    };

    let e = &state.total_errors;

    // Per-tick time series for offline charting.
    let ticks: Vec<serde_json::Value> = state.ticks.iter().map(|t| {
        json!({
            "elapsed_s": t.elapsed.as_secs_f64(),
            "requests": t.requests,
            "bytes_sent": t.bytes_sent,
            "bytes_recv": t.bytes_recv,
            "p50_ns": t.p50_ns,
            "p90_ns": t.p90_ns,
            "p99_ns": t.p99_ns,
            "p99_9_ns": t.p99_9_ns,
            "errors": {
                "connect": t.errors.connect,
                "read": t.errors.read,
                "write": t.errors.write,
                "timeout": t.errors.timeout,
                "keepup": t.errors.keepup,
                "status_4xx": t.errors.status_4xx,
                "status_5xx": t.errors.status_5xx,
                "assertion_failed": t.errors.assertion_failed,
            }
        })
    }).collect();

    json!({
        "schema_version": 1,
        "source": "tui_export",
        "url": state.url_label,
        "transport": {
            "protocol": state.transport.protocol,
            "tls": state.transport.tls,
            "alpn": state.transport.alpn,
            "connections": state.transport.connections,
        },
        "target_rate": state.target_rate,
        "duration_planned_ms": state.total_duration.as_millis() as u64,
        "duration_actual_ms": elapsed.as_millis() as u64,
        "run_completed": state.run_completed,

        "summary": {
            "requests": state.total_requests,
            "requests_per_sec": avg_rps,
            "peak_rps": state.peak_rps,
            "min_rps": state.min_rps,
            "bytes_sent": state.cumulative_bytes_sent,
            "bytes_recv": state.cumulative_bytes_recv,
            "latency": latency,
            "errors": {
                "connect": e.connect,
                "read": e.read,
                "write": e.write,
                "timeout": e.timeout,
                "keepup": e.keepup,
                "status_4xx": e.status_4xx,
                "status_5xx": e.status_5xx,
                "assertion_failed": e.assertion_failed,
            }
        },

        "ticks": ticks,
    })
}

// Hinnant civil_from_days — same algorithm used in template.rs.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
