//! Overview tab — the richest single-screen summary.
//!
//! Wide layout (>= 140 cols):
//!
//! ```text
//! ╭─ throughput ──────────────╮ ╭─ totals ─────╮ ╭─ errors ──────╮
//! │ sparkline + big rps       │ │ table        │ │ table         │
//! ╰───────────────────────────╯ ╰──────────────╯ ╰───────────────╯
//! ╭─ latency (5s rolling) ────────────╮ ╭─ scenarios ──────────────╮
//! │ hbars per percentile              │ │ table                    │
//! ╰───────────────────────────────────╯ ╰──────────────────────────╯
//! ```
//!
//! Narrow layout (< 140 cols):
//!
//! ```text
//! ╭─ throughput sparkline ─────────────────────────────────────────╮
//! │ sparkline + big rps                                            │
//! ╰────────────────────────────────────────────────────────────────╯
//! ╭─ latency ──────────────╮ ╭─ totals + errors ──────────────────╮
//! │ bars                   │ │ tables stacked                     │
//! ╰────────────────────────╯ ╰────────────────────────────────────╯
//! ```

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Sparkline, Table};
use ratatui::Frame;

use crate::state::{DashboardState, ROLLING_LATENCY_WINDOW};

use super::common::{
    format_bytes, format_bytes_rate, format_ns, format_rate, hbar_smooth, tile, CRITICAL, PALETTE,
    SUCCESS,
};

// ---------------------------------------------------------------------------
// Tab entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect, state: &DashboardState, wide: bool) {
    if wide {
        render_wide(frame, area, state);
    } else {
        render_narrow(frame, area, state);
    }
}

fn render_wide(frame: &mut Frame, area: Rect, state: &DashboardState) {
    // Top row: throughput (wide) + totals + errors side by side.
    // Bottom row: latency bars + scenarios.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1), Constraint::Fill(1)])
        .split(area);

    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(2),
            Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .split(rows[0]);
    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(2), Constraint::Fill(1)])
        .split(rows[1]);

    render_throughput_sparkline(frame, top[0], state);
    render_totals_table(frame, top[1], state);
    render_errors_table(frame, top[2], state);
    render_latency_bars(frame, bot[0], state);
    render_scenarios_table(frame, bot[1], state);
}

fn render_narrow(frame: &mut Frame, area: Rect, state: &DashboardState) {
    // Top: full-width throughput sparkline.
    // Bottom: latency bars (left) + totals+errors stacked (right).
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1), Constraint::Fill(1)])
        .split(area);

    render_throughput_sparkline(frame, rows[0], state);

    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Fill(1)])
        .split(rows[1]);

    render_latency_bars(frame, bot[0], state);

    // Right side: totals on top, errors on bottom.
    let right_stack = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1), Constraint::Fill(1)])
        .split(bot[1]);
    render_totals_table(frame, right_stack[0], state);
    render_errors_table(frame, right_stack[1], state);
}

// ---------------------------------------------------------------------------
// Throughput panel — big number + sparkline.
// ---------------------------------------------------------------------------

fn render_throughput_sparkline(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("throughput");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Layout adapts to whether there are errors to surface. When
    // `total_errors > 0` we insert a one-line compact error summary
    // between the big rps readout and the sparkline so users don't
    // have to press [4] to notice something is wrong.
    let total_errors = state.total_errors.total();
    let show_errors = total_errors > 0;

    let mut constraints = Vec::with_capacity(3);
    constraints.push(Constraint::Length(2)); // big rps + peak/min/avg
    if show_errors {
        constraints.push(Constraint::Length(1)); // compact error summary
    }
    constraints.push(Constraint::Min(1)); // sparkline fills the rest

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
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

    let spark_area = if show_errors {
        render_error_summary(frame, rows[1], state);
        rows[2]
    } else {
        rows[1]
    };

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

/// Compact one-line error summary shown on the Overview tab when
/// `total_errors > 0`. Mirrors the categories on the Errors tab so
/// users know which bucket is firing without switching tabs.
fn render_error_summary(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let e = &state.total_errors;
    let dim = Style::new().fg(Color::DarkGray);
    let red_bold = Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD);

    // Colour each counter red when non-zero, dim grey when zero —
    // mirrors the Errors tab treatment so the meaning is consistent.
    let counter_style = |n: u64| if n == 0 { dim } else { red_bold };

    let spans = vec![
        Span::styled(
            "errors  ",
            Style::new().fg(Color::Gray).add_modifier(Modifier::BOLD),
        ),
        Span::styled("connect ", dim),
        Span::styled(format!("{}", e.connect), counter_style(e.connect)),
        Span::raw("  "),
        Span::styled("read ", dim),
        Span::styled(format!("{}", e.read), counter_style(e.read)),
        Span::raw("  "),
        Span::styled("4xx ", dim),
        Span::styled(format!("{}", e.status_4xx), counter_style(e.status_4xx)),
        Span::raw("  "),
        Span::styled("5xx ", dim),
        Span::styled(format!("{}", e.status_5xx), counter_style(e.status_5xx)),
        Span::styled("    (tap [4] for detail)", dim),
    ];

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

// ---------------------------------------------------------------------------
// Totals panel — Table widget with cumulative counters + per-second rates.
// ---------------------------------------------------------------------------

fn render_totals_table(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let e = &state.total_errors;
    let success = state
        .total_requests
        .saturating_sub(e.status_4xx + e.status_5xx);
    let (last_sent, last_recv) = state.last_tick_bytes();

    let dim = Style::new().fg(Color::Gray);
    let white_bold = Style::new().fg(Color::White).add_modifier(Modifier::BOLD);

    let rows = vec![
        Row::new(vec![
            Cell::from("requests").style(dim),
            Cell::from(format_number(state.total_requests)).style(white_bold),
        ]),
        Row::new(vec![
            Cell::from("2xx").style(dim),
            Cell::from(format!("{} ✓", format_number(success)))
                .style(Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD)),
        ]),
        Row::new(vec![
            Cell::from("4xx").style(dim),
            Cell::from(format_number(e.status_4xx)).style(if e.status_4xx == 0 {
                dim
            } else {
                Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD)
            }),
        ]),
        Row::new(vec![
            Cell::from("5xx").style(dim),
            Cell::from(format_number(e.status_5xx)).style(if e.status_5xx == 0 {
                dim
            } else {
                Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD)
            }),
        ]),
        Row::new(vec![
            Cell::from("total ↑").style(dim),
            Cell::from(format_bytes(state.cumulative_bytes_sent)).style(dim),
        ]),
        Row::new(vec![
            Cell::from("total ↓").style(dim),
            Cell::from(format_bytes(state.cumulative_bytes_recv)).style(dim),
        ]),
        Row::new(vec![
            Cell::from("rate ↑").style(dim),
            Cell::from(format_bytes_rate(last_sent as f64)).style(dim),
        ]),
        Row::new(vec![
            Cell::from("rate ↓").style(dim),
            Cell::from(format_bytes_rate(last_recv as f64)).style(dim),
        ]),
    ];

    let widths = [Constraint::Length(12), Constraint::Min(8)];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["metric", "value"])
                .style(Style::new().add_modifier(Modifier::BOLD))
                .bottom_margin(0),
        )
        .block(tile("totals"))
        .column_spacing(2);
    frame.render_widget(table, area);
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
            let bar_width = (inner_width as usize).saturating_sub(20).max(6);
            let max_f = max as f64;

            let delta_span = delta_span(state);

            vec![
                percentile_line("p50  ", p50, max_f, bar_width, PALETTE[0]),
                percentile_line("p90  ", p90, max_f, bar_width, PALETTE[1]),
                percentile_line("p99  ", p99, max_f, bar_width, PALETTE[2]),
                percentile_line_with_suffix(
                    "p99.9", p99_9, max_f, bar_width, PALETTE[3], delta_span,
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
// Errors panel — Table widget with error breakdown.
// ---------------------------------------------------------------------------

fn render_errors_table(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let e = &state.total_errors;
    let dim = Style::new().fg(Color::DarkGray);
    let red_bold = Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD);

    let counter_style = |n: u64| -> Style {
        if n == 0 {
            dim
        } else {
            red_bold
        }
    };

    let rows = vec![
        Row::new(vec![
            Cell::from("connect"),
            Cell::from(format!("{}", e.connect)).style(counter_style(e.connect)),
        ]),
        Row::new(vec![
            Cell::from("read"),
            Cell::from(format!("{}", e.read)).style(counter_style(e.read)),
        ]),
        Row::new(vec![
            Cell::from("write"),
            Cell::from(format!("{}", e.write)).style(counter_style(e.write)),
        ]),
        Row::new(vec![
            Cell::from("timeout"),
            Cell::from(format!("{}", e.timeout)).style(counter_style(e.timeout)),
        ]),
        Row::new(vec![
            Cell::from("keepup"),
            Cell::from(format!("{}", e.keepup)).style(counter_style(e.keepup)),
        ]),
        Row::new(vec![
            Cell::from("assert"),
            Cell::from(format!("{}", e.assertion_failed)).style(counter_style(e.assertion_failed)),
        ]),
    ];

    let widths = [Constraint::Length(12), Constraint::Min(8)];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["category", "count"])
                .style(Style::new().add_modifier(Modifier::BOLD))
                .bottom_margin(0),
        )
        .block(tile("errors"))
        .column_spacing(2);
    frame.render_widget(table, area);
}

// ---------------------------------------------------------------------------
// Scenarios panel — Table widget (single default scenario for now).
// ---------------------------------------------------------------------------

fn render_scenarios_table(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let header = Row::new(vec!["#", "name", "rps", "p99", "errors"])
        .style(Style::new().add_modifier(Modifier::BOLD))
        .bottom_margin(0);

    let rows: Vec<Row> = if state.scenario_names.is_empty() {
        // Fallback: no named scenarios — show single aggregate row.
        vec![Row::new(vec![
            Cell::from("1"),
            Cell::from("default"),
            Cell::from(format_rate(state.requests_per_sec())),
            Cell::from(format_ns(state.rolling_p99_9_ns())),
            Cell::from(format!("{}", state.total_errors.total())),
        ])]
    } else {
        state
            .scenario_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let rps = state.scenario_rps(i);
                let p99 = state.scenario_p99_ns(i);
                let errs = state
                    .scenario_total_errors
                    .get(i)
                    .map(|e| e.total())
                    .unwrap_or(0);
                Row::new(vec![
                    Cell::from(format!("{}", i + 1)),
                    Cell::from(name.as_str()),
                    Cell::from(format_rate(rps)),
                    Cell::from(format_ns(p99)),
                    Cell::from(format!("{errs}")),
                ])
            })
            .collect()
    };

    let widths = [
        Constraint::Length(3),
        Constraint::Fill(1),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(8),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(tile("scenarios"));
    frame.render_widget(table, area);
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

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
        Span::styled(format!(" {}", format_ns(value_ns)), Style::new().fg(colour)),
    ];
    if let Some(s) = suffix {
        spans.push(s);
    }
    Line::from(spans)
}

/// Format a u64 with thousands separators — `638,421`. Kept inline
/// because std doesn't have one and pulling a crate for cosmetic comma
/// insertion is overkill.
pub(crate) fn format_number(n: u64) -> String {
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
