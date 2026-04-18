//! Shared rendering helpers used across every tab.
//!
//! Centralised so that the overall look (rounded borders, palette,
//! inline bar style, numeric formatting) stays consistent as tabs
//! evolve independently.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, BorderType};

// ---------------------------------------------------------------------------
// Colour palette
// ---------------------------------------------------------------------------
//
// Values from the design spec. Truecolor terminals get the exact RGB;
// on older terminals crossterm automatically snaps to the nearest
// 256-colour cube entry, so we don't need a manual fallback path.

/// Primary accent — headers, active tab highlight.
pub const ACCENT: Color = Color::Rgb(88, 170, 255);
/// Success — 2xx, on-target rate.
pub const SUCCESS: Color = Color::Rgb(127, 220, 127);
/// Warning — near-target.
pub const WARNING: Color = Color::Rgb(255, 204, 102);
/// Critical — errors, off-target.
pub const CRITICAL: Color = Color::Rgb(255, 102, 102);

/// Chart series palette — 6 visually distinct colors for multi-line
/// charts. Index with `PALETTE[i % PALETTE.len()]` for consistent
/// coloring across all tabs.
pub const PALETTE: [Color; 6] = [
    Color::Rgb(88, 200, 130),  // green  — p50, rps, 2xx
    Color::Rgb(130, 200, 240), // cyan   — p90, bytes recv
    Color::Rgb(255, 204, 102), // amber  — p99, 4xx
    Color::Rgb(255, 120, 120), // red    — p99.9, errors, 5xx
    Color::Rgb(180, 140, 255), // purple — bytes sent
    Color::Rgb(200, 200, 200), // gray   — reference lines
];

// ---------------------------------------------------------------------------
// Shared block / title helper
// ---------------------------------------------------------------------------

/// Standard panel border — rounded, dim border colour, padded title.
pub fn tile(title: &str) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::DarkGray))
        .title(format!(" {title} "))
        .title_style(Style::new().fg(ACCENT).add_modifier(Modifier::BOLD))
}

// ---------------------------------------------------------------------------
// Status pill
// ---------------------------------------------------------------------------

/// Traffic-light status for the header pill. Mapped to a coloured `⬤`
/// glyph by [`status_pill`]. Computed from the current rps + target +
/// error trend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Green,
    Yellow,
    Red,
    /// Benchmark completed — TUI stays open for inspection.
    Done,
}

/// Decide the header pill colour from live state inputs.
///
/// Error handling is rate-based, not binary:
///
/// - Zero requests + any errors → Red. This is the "connection failed
///   at startup" case — the user needs instant feedback when nothing
///   works. A single 404 on an otherwise-healthy run wouldn't hit this
///   branch because `total_requests > 0`.
/// - error_rate ≥ 5% → Red (critical failure rate).
/// - error_rate ≥ 1% → Yellow (elevated but tolerable), unless the
///   saturate/target logic already says Red.
/// - error_rate < 1% → defer to saturate/target logic. One in a
///   million 404s should not turn the pill red.
///
/// The denominator is `total_requests + total_errors` so a run where
/// *every* attempt errors reports 100% error rate rather than an
/// undefined division.
///
/// `actual_pct` is the actual/target ratio in percent (0..=200, as
/// returned by [`crate::state::DashboardState::actual_vs_target_pct`]).
pub fn compute_status(
    actual_pct: Option<f64>,
    total_requests: u64,
    total_errors: u64,
) -> Status {
    // Connection-failed-at-startup: all errors, zero requests. Users
    // need instant red feedback when literally nothing is working.
    if total_requests == 0 && total_errors > 0 {
        return Status::Red;
    }

    // Rate-based escalation. Denominator includes both buckets so a
    // run where every attempt errors reports 100%.
    let denom = total_requests.saturating_add(total_errors);
    let error_rate = if denom == 0 {
        0.0
    } else {
        total_errors as f64 / denom as f64
    };

    if error_rate >= 0.05 {
        return Status::Red;
    }

    // Compute the rate/target-based status; error_rate < 1% defers
    // entirely to this, ≥ 1% bumps Green → Yellow.
    let base = match actual_pct {
        // Saturate — no target bound: healthy when the error rate
        // stays below the red threshold.
        None => Status::Green,
        Some(pct) => {
            // Spec: green [95, 105], yellow [80, 95) ∪ (105, 120],
            // red otherwise.
            if (95.0..=105.0).contains(&pct) {
                Status::Green
            } else if (80.0..95.0).contains(&pct) || (105.0..=120.0).contains(&pct) {
                Status::Yellow
            } else {
                Status::Red
            }
        }
    };

    if error_rate >= 0.01 && matches!(base, Status::Green) {
        Status::Yellow
    } else {
        base
    }
}

/// Render the status as a coloured `⬤` glyph span.
pub fn status_pill(status: Status) -> Span<'static> {
    let colour = match status {
        Status::Green => SUCCESS,
        Status::Yellow => WARNING,
        Status::Red => CRITICAL,
        Status::Done => ACCENT,
    };
    Span::styled("⬤", Style::new().fg(colour).add_modifier(Modifier::BOLD))
}

// ---------------------------------------------------------------------------
// Horizontal bars — smooth sub-cell resolution via partial-block glyphs.
// ---------------------------------------------------------------------------

/// Inline "bar chart" with 1/8-cell resolution using Unicode
/// partial-block characters (`▏▎▍▌▋▊▉█`). Produces a fixed-width
/// string; callers pad or trim it for their panel.
///
/// `value` and `max` are compared as f64; non-finite or negative
/// inputs produce an empty bar. If `max <= 0` we fall back to zero
/// fill rather than panic.
pub fn hbar_smooth(value: f64, max: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if !value.is_finite() || value <= 0.0 || !max.is_finite() || max <= 0.0 {
        return " ".repeat(width);
    }
    let ratio = (value / max).clamp(0.0, 1.0);
    let cells = ratio * width as f64;
    let full = cells.trunc() as usize;
    let frac = cells.fract();
    let partial = match (frac * 8.0).round() as u8 {
        0 => "",
        1 => "▏",
        2 => "▎",
        3 => "▍",
        4 => "▌",
        5 => "▋",
        6 => "▊",
        7 => "▉",
        _ => "█",
    };
    let used = full + if partial.is_empty() { 0 } else { 1 };
    let rest = width.saturating_sub(used);
    let mut out = String::with_capacity(width * 3);
    for _ in 0..full {
        out.push('█');
    }
    out.push_str(partial);
    for _ in 0..rest {
        out.push(' ');
    }
    out
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Format a requests-per-second value — `"100 req/s"`, `"10.0k req/s"`,
/// `"2.00M req/s"`.
pub fn format_rate(rps: f64) -> String {
    if !rps.is_finite() || rps < 0.0 {
        return "0 req/s".to_string();
    }
    if rps < 1_000.0 {
        format!("{:.0} req/s", rps)
    } else if rps < 1_000_000.0 {
        format!("{:.1}k req/s", rps / 1_000.0)
    } else {
        format!("{:.2}M req/s", rps / 1_000_000.0)
    }
}

/// Format a nanosecond count — `"999ns"`, `"1.0µs"`, `"120µs"`,
/// `"2.1ms"`, `"22ms"`, `"1.50s"`.
pub fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        let us = ns as f64 / 1_000.0;
        if us < 10.0 {
            format!("{us:.1}µs")
        } else {
            format!("{us:.0}µs")
        }
    } else if ns < 1_000_000_000 {
        let ms = ns as f64 / 1_000_000.0;
        if ms < 10.0 {
            format!("{ms:.1}ms")
        } else {
            format!("{ms:.0}ms")
        }
    } else {
        let s = ns as f64 / 1_000_000_000.0;
        format!("{s:.2}s")
    }
}

/// Format a byte count as human-friendly text — `"512 B"`, `"1.2 KB"`,
/// `"3.4 MB"`, `"5.6 GB"`, `"7.8 TB"`. Uses decimal (1000) for easier
/// mental math against req/s numbers.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes < 1_000 {
        return format!("{bytes} B");
    }
    let mut v = bytes as f64;
    let mut idx = 0;
    while v >= 1_000.0 && idx < UNITS.len() - 1 {
        v /= 1_000.0;
        idx += 1;
    }
    format!("{v:.2} {}", UNITS[idx])
}

/// Format a byte count with a trailing `/s` — for bytes-per-second
/// readouts on the Throughput tab.
pub fn format_bytes_rate(bytes_per_sec: f64) -> String {
    if !bytes_per_sec.is_finite() || bytes_per_sec < 0.0 {
        return "0 B/s".to_string();
    }
    let b = bytes_per_sec as u64;
    format!("{}/s", format_bytes(b))
}

// ---------------------------------------------------------------------------
// Terminal-too-small threshold — shared between the outer dispatcher
// and the per-tab renderers so the fallback message matches the
// threshold at which we skip the real layout.
// ---------------------------------------------------------------------------

/// Minimum terminal height for the full layout.
pub const MIN_HEIGHT: u16 = 20;
/// Minimum terminal width for the full layout.
pub const MIN_WIDTH: u16 = 60;

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_rate_scales() {
        assert_eq!(format_rate(100.0), "100 req/s");
        assert_eq!(format_rate(1_500.0), "1.5k req/s");
        assert_eq!(format_rate(9_994.2), "10.0k req/s");
        assert_eq!(format_rate(2_000_000.0), "2.00M req/s");
        assert_eq!(format_rate(f64::NAN), "0 req/s");
        assert_eq!(format_rate(-1.0), "0 req/s");
    }

    #[test]
    fn format_ns_scales() {
        assert_eq!(format_ns(0), "0ns");
        assert_eq!(format_ns(999), "999ns");
        assert_eq!(format_ns(1_000), "1.0µs");
        assert_eq!(format_ns(120_000), "120µs");
        assert_eq!(format_ns(2_100_000), "2.1ms");
        assert_eq!(format_ns(22_000_000), "22ms");
        assert_eq!(format_ns(1_500_000_000), "1.50s");
    }

    #[test]
    fn format_bytes_scales() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(999), "999 B");
        assert_eq!(format_bytes(1_500), "1.50 KB");
        assert_eq!(format_bytes(3_400_000), "3.40 MB");
        assert_eq!(format_bytes(5_600_000_000), "5.60 GB");
    }

    #[test]
    fn hbar_smooth_renders_fractional_cells() {
        // Half-filled 8-cell bar should end in a half-block.
        let s = hbar_smooth(0.5, 1.0, 8);
        assert_eq!(s.chars().count(), 8);
        // Fully filled should be all █.
        let full = hbar_smooth(1.0, 1.0, 8);
        assert!(full.chars().all(|c| c == '█'));
        // Empty should be all spaces.
        let empty = hbar_smooth(0.0, 1.0, 8);
        assert!(empty.chars().all(|c| c == ' '));
    }

    #[test]
    fn hbar_smooth_clamps_overflow() {
        // Overflow should produce a fully-filled bar, not a panic.
        let s = hbar_smooth(5.0, 1.0, 4);
        assert!(s.chars().all(|c| c == '█'));
    }

    #[test]
    fn hbar_smooth_handles_zero_width() {
        assert_eq!(hbar_smooth(0.5, 1.0, 0), "");
    }

    #[test]
    fn compute_status_maps_to_palette() {
        // No traffic yet, no errors — Green (benign warm-up).
        assert_eq!(compute_status(None, 0, 0), Status::Green);
        // Rate-targeted runs still use the actual/target bands when
        // error rate is negligible.
        assert_eq!(compute_status(Some(100.0), 10_000, 0), Status::Green);
        assert_eq!(compute_status(Some(85.0), 10_000, 0), Status::Yellow);
        assert_eq!(compute_status(Some(50.0), 10_000, 0), Status::Red);
        assert_eq!(compute_status(Some(110.0), 10_000, 0), Status::Yellow);
        assert_eq!(compute_status(Some(150.0), 10_000, 0), Status::Red);
    }

    #[test]
    fn compute_status_rate_thresholds() {
        // 0.0005% error rate — well below 1%, stays Green.
        assert_eq!(
            compute_status(None, 1_000_000, 5),
            Status::Green
        );
        // 2% error rate — between 1% and 5%, bumps Green → Yellow.
        assert_eq!(
            compute_status(None, 1_000_000, 20_000),
            Status::Yellow
        );
        // 10% error rate — above 5%, forced Red regardless of target.
        assert_eq!(
            compute_status(None, 1_000_000, 100_000),
            Status::Red
        );
        // Zero requests + any errors = connection failed at startup.
        assert_eq!(compute_status(None, 0, 10), Status::Red);
        // Zero errors, any request count — Green (no error signal).
        assert_eq!(compute_status(None, 100, 0), Status::Green);
    }

    #[test]
    fn compute_status_yellow_band_does_not_downgrade_to_green() {
        // 2% error rate is elevated, but the target-band logic says
        // Yellow already — it should stay Yellow, not get forced back
        // to Green.
        assert_eq!(
            compute_status(Some(85.0), 1_000_000, 20_000),
            Status::Yellow
        );
        // 2% error rate + Red target band stays Red.
        assert_eq!(
            compute_status(Some(50.0), 1_000_000, 20_000),
            Status::Red
        );
    }

    #[test]
    fn compute_status_boundary_values() {
        // Exactly 1% — just at the Yellow threshold.
        assert_eq!(
            compute_status(None, 99_000, 1_000),
            Status::Yellow
        );
        // Exactly 5% — just at the Red threshold.
        assert_eq!(
            compute_status(None, 95_000, 5_000),
            Status::Red
        );
    }
}
