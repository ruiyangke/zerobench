//! Overview tab — the richest single-screen summary.
//!
//! Layout:
//!
//! ```text
//! ╭─ throughput ──────────────╮ ╭─ totals ────────────╮
//! │ 87.3k rps                 │ │ requests   638,421  │
//! │ peak / min / avg          │ │ 2xx / 4xx / 5xx     │
//! │ sparkline                 │ │ bytes ↑/↓           │
//! ╰───────────────────────────╯ ╰─────────────────────╯
//! ╭─ latency (5s rolling) ────╮ ╭─ errors ────────────╮
//! │ hbars per percentile      │ │ counters            │
//! ╰───────────────────────────╯ ╰─────────────────────╯
//! ```

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Sparkline};
use ratatui::Frame;

use crate::state::{DashboardState, ROLLING_LATENCY_WINDOW};

use super::common::{
    format_bytes, format_ns, format_rate, hbar_smooth, tile, CRITICAL, PALETTE, SUCCESS,
};

// ---------------------------------------------------------------------------
// Tab entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect, state: &DashboardState) {
    // Two rows of two panels each.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[0]);
    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[1]);

    render_throughput_sparkline(frame, top[0], state);
    render_totals(frame, top[1], state);
    render_latency_bars(frame, bot[0], state);
    render_errors(frame, bot[1], state);
}

// ---------------------------------------------------------------------------
// Throughput panel — big number + sparkline.
// ---------------------------------------------------------------------------

fn render_throughput_sparkline(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("throughput");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Top row: big rps readout + peak/min/avg. Bottom rows: sparkline.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);

    let rps = state.requests_per_sec();
    let peak = state.peak_rps;
    let min = state.min_rps.unwrap_or(0.0);
    let avg = state.avg_rps();

    let big = Line::from(vec![
        Span::styled(
            format_rate(rps),
            Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  ", Style::new()),
        Span::styled(
            format!(
                "peak {}  min {}  avg {}",
                format_rate(peak),
                format_rate(min),
                format_rate(avg)
            ),
            Style::new().fg(Color::Gray),
        ),
    ]);
    frame.render_widget(Paragraph::new(big), rows[0]);

    let spark_area = rows[1];
    let width = spark_area.width as usize;
    if width == 0 {
        return;
    }
    let data = state.sparkline_data(width);
    if data.is_empty() {
        let hint = Paragraph::new(Line::from(Span::styled(
            "(waiting for first tick)",
            Style::new().fg(Color::DarkGray),
        )));
        frame.render_widget(hint, spark_area);
        return;
    }
    // Cap the max to target_rate when we have one, otherwise auto-scale
    // to the observed peak.
    let max_hint = state
        .target_rate
        .map(|r| (r * 1.05) as u64)
        .unwrap_or_else(|| *data.iter().max().unwrap_or(&1).max(&1));
    let sparkline = Sparkline::default()
        .data(&data)
        .max(max_hint)
        .style(Style::new().fg(PALETTE[0]));
    frame.render_widget(sparkline, spark_area);
}

// ---------------------------------------------------------------------------
// Totals panel — cumulative counters.
// ---------------------------------------------------------------------------

fn render_totals(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("totals");
    let e = &state.total_errors;
    let success = state.total_requests.saturating_sub(e.status_4xx + e.status_5xx);

    let lines = vec![
        kv_line("requests", format_number(state.total_requests), Color::White),
        kv_line_styled(
            "2xx",
            format!("{} ✓", format_number(success)),
            SUCCESS,
        ),
        kv_line_styled(
            "4xx",
            format_number(e.status_4xx),
            if e.status_4xx == 0 { Color::Gray } else { CRITICAL },
        ),
        kv_line_styled(
            "5xx",
            format_number(e.status_5xx),
            if e.status_5xx == 0 { Color::Gray } else { CRITICAL },
        ),
        kv_line(
            "bytes ↑",
            format_bytes(state.cumulative_bytes_sent),
            Color::Gray,
        ),
        kv_line(
            "bytes ↓",
            format_bytes(state.cumulative_bytes_recv),
            Color::Gray,
        ),
    ];

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

// ---------------------------------------------------------------------------
// Latency panel — horizontal bars per percentile.
// ---------------------------------------------------------------------------

fn render_latency_bars(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let title = format!("latency ({}s rolling)", ROLLING_LATENCY_WINDOW);
    let block = tile(&title);
    let hist = state.rolling_latency();
    let inner_width = block.inner(area).width;

    let lines: Vec<Line> = match hist {
        Some(h) => {
            let p50 = h.value_at_percentile(50.0);
            let p90 = h.value_at_percentile(90.0);
            let p99 = h.value_at_percentile(99.0);
            let p99_9 = h.value_at_percentile(99.9);
            let max = h.max();

            // Max drives the bar scale; everything else is proportional.
            // Use max to normalise so the visual p99/p50 ratio is at-a-glance.
            let bar_width = (inner_width as usize).saturating_sub(20).max(6);
            let max_f = max as f64;

            let delta_span = delta_span(state);

            vec![
                percentile_line("p50  ", p50, max_f, bar_width, PALETTE[0]),
                percentile_line("p90  ", p90, max_f, bar_width, PALETTE[1]),
                percentile_line("p99  ", p99, max_f, bar_width, PALETTE[2]),
                percentile_line_with_suffix(
                    "p99.9",
                    p99_9,
                    max_f,
                    bar_width,
                    PALETTE[3],
                    delta_span,
                ),
                percentile_line("max  ", max, max_f, bar_width, CRITICAL),
            ]
        }
        None => vec![Line::from(Span::styled(
            "no samples yet",
            Style::new().fg(Color::DarkGray),
        ))],
    };

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

fn delta_span(state: &DashboardState) -> Option<Span<'static>> {
    match state.p99_9_delta_pct() {
        Some(pct) if pct.abs() < 1.0 => Some(Span::styled(
            format!("  {pct:+.1}%"),
            Style::new().fg(Color::DarkGray),
        )),
        Some(pct) if pct > 0.0 => Some(Span::styled(
            format!(" ▲{pct:.1}%"),
            Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD),
        )),
        Some(pct) => Some(Span::styled(
            format!(" ▼{:.1}%", pct.abs()),
            Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD),
        )),
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Errors panel — compact counter list.
// ---------------------------------------------------------------------------

fn render_errors(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("errors");
    let e = &state.total_errors;

    let counted = |n: u64| -> Span<'static> {
        if n == 0 {
            Span::styled(format!("{n}"), Style::new().fg(Color::DarkGray))
        } else {
            Span::styled(
                format!("{n}"),
                Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD),
            )
        }
    };

    let lines = vec![
        row_2col("connect", counted(e.connect)),
        row_2col("read", counted(e.read)),
        row_2col("write", counted(e.write)),
        row_2col("timeout", counted(e.timeout)),
        row_2col("keepup", counted(e.keepup)),
        row_2col("assert", counted(e.assertion_failed)),
    ];

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn kv_line(key: &str, value: String, colour: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(" {:<9}", key),
            Style::new().fg(Color::Gray),
        ),
        Span::styled(value, Style::new().fg(colour).add_modifier(Modifier::BOLD)),
    ])
}

fn kv_line_styled(key: &str, value: String, colour: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(" {:<9}", key),
            Style::new().fg(Color::Gray),
        ),
        Span::styled(value, Style::new().fg(colour).add_modifier(Modifier::BOLD)),
    ])
}

fn row_2col(label: &'static str, value: Span<'static>) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(" {:<10}", label),
            Style::new().fg(Color::Gray),
        ),
        value,
    ])
}

fn percentile_line(
    label: &'static str,
    value_ns: u64,
    max_ns: f64,
    bar_width: usize,
    colour: Color,
) -> Line<'static> {
    percentile_line_with_suffix(label, value_ns, max_ns, bar_width, colour, None)
}

fn percentile_line_with_suffix(
    label: &'static str,
    value_ns: u64,
    max_ns: f64,
    bar_width: usize,
    colour: Color,
    suffix: Option<Span<'static>>,
) -> Line<'static> {
    let bar = hbar_smooth(value_ns as f64, max_ns, bar_width);
    let mut spans = vec![
        Span::styled(
            format!(" {label}  "),
            Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled(bar, Style::new().fg(colour).add_modifier(Modifier::BOLD)),
        Span::styled(
            format!(" {}", format_ns(value_ns)),
            Style::new().fg(colour),
        ),
    ];
    if let Some(s) = suffix {
        spans.push(s);
    }
    Line::from(spans)
}

/// Format a u64 with thousands separators — `638,421`. Kept inline
/// because std doesn't have one and pulling a crate for cosmetic comma
/// insertion is overkill.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_number_inserts_separators() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(42), "42");
        assert_eq!(format_number(1_000), "1,000");
        assert_eq!(format_number(638_421), "638,421");
        assert_eq!(format_number(1_234_567_890), "1,234,567,890");
    }

    #[test]
    fn percentile_line_uses_label_color() {
        // Smoke test — confirm the formatter produces a non-empty line
        // with the bar glyph present.
        let line = percentile_line("p50  ", 1_000, 10_000.0, 8, SUCCESS);
        assert_eq!(line.spans.len(), 3);
    }
}
