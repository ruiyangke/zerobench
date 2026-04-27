//! Errors tab — per-category errors-per-second chart + status-code
//! breakdown + cumulative totals.
//!
//! Layout:
//!
//! ```text
//! ╭─ errors/sec by category over time ───────────────────────╮
//! │ multiple line series (one per error category)             │
//! ╰───────────────────────────────────────────────────────────╯
//! ╭─ status codes over time ───╮ ╭─ cumulative totals ────────╮
//! │ 2xx / 4xx / 5xx percentages │ │ Table: error counters      │
//! ╰─────────────────────────────╯ ╰────────────────────────────╯
//! ```

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Cell, Chart, Paragraph, Row, Table};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{tile, CRITICAL, PALETTE, SUCCESS};
use super::dataset::OwnedDataset;

// ---------------------------------------------------------------------------
// Tab entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect, state: &DashboardState, wide: bool) {
    // Chart dominates; bottom panel is fixed at 8 rows.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    render_category_timeseries(frame, rows[0], state);

    if wide {
        let bot = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Fill(1), Constraint::Fill(1)])
            .split(rows[1]);
        render_status_timeseries(frame, bot[0], state);
        render_cumulative_table(frame, bot[1], state);
    } else {
        // Narrow: stack bottom panels vertically.
        let bot = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Fill(1), Constraint::Fill(1)])
            .split(rows[1]);
        render_status_timeseries(frame, bot[0], state);
        render_cumulative_table(frame, bot[1], state);
    }
}

// ---------------------------------------------------------------------------
// Per-category time-series chart
// ---------------------------------------------------------------------------

fn render_category_timeseries(frame: &mut Frame, area: Rect, state: &DashboardState) {
    if state.ticks.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            "(waiting for first tick)",
            Style::new().fg(Color::DarkGray),
        )))
        .block(tile("errors/sec by category"));
        frame.render_widget(p, area);
        return;
    }

    let marker = state.marker;
    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let mut owned = vec![
        OwnedDataset::line(
            "connect",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.errors.connect as f64))
                .collect(),
            PALETTE[3],
            marker,
        ),
        OwnedDataset::line(
            "read",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.errors.read as f64))
                .collect(),
            Color::Rgb(255, 153, 153),
            marker,
        ),
        OwnedDataset::line(
            "write",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.errors.write as f64))
                .collect(),
            Color::Rgb(255, 180, 120),
            marker,
        ),
        OwnedDataset::line(
            "timeout",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.errors.timeout as f64))
                .collect(),
            PALETTE[2],
            marker,
        ),
        OwnedDataset::line(
            "keepup",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.errors.keepup as f64))
                .collect(),
            PALETTE[4],
            marker,
        ),
        OwnedDataset::line(
            "assert",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.errors.assertion_failed as f64))
                .collect(),
            Color::Rgb(255, 120, 180),
            marker,
        ),
    ];

    let y_raw_max = owned
        .iter()
        .flat_map(|d| d.data.iter())
        .map(|p| p.1)
        .fold(0.0_f64, f64::max)
        .max(1.0);

    // Zero baseline reference line.
    owned.push(OwnedDataset::reference_line(
        vec![(x_min, 0.0), (x_max, 0.0)],
        PALETTE[5],
        marker,
    ));

    let y_max = y_raw_max * 1.1 * state.y_scale;

    let datasets = owned.iter().map(|d| d.to_dataset()).collect();

    let y_labels = vec![
        Line::from(Span::styled("0", Style::new().fg(Color::Gray))),
        Line::from(Span::styled(
            format!("{:.0}", y_max / 2.0),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format!("{:.0}", y_max),
            Style::new().fg(Color::Gray),
        )),
    ];
    let x_labels = vec![
        Line::from(Span::styled(
            format!("{x_min:.0}s"),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format!("{x_max:.0}s"),
            Style::new().fg(Color::Gray),
        )),
    ];

    let chart = Chart::new(datasets)
        .block(tile(
            "errors/sec by category  (connect · read · write · timeout · keepup · assert)",
        ))
        .legend_position(None)
        .x_axis(
            Axis::default()
                .style(Style::new().fg(Color::DarkGray))
                .bounds([x_min, x_max])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .style(Style::new().fg(Color::DarkGray))
                .bounds([0.0, y_max])
                .labels(y_labels),
        );
    frame.render_widget(chart, area);
}

// ---------------------------------------------------------------------------
// Status code percentage time-series.
// ---------------------------------------------------------------------------

fn render_status_timeseries(frame: &mut Frame, area: Rect, state: &DashboardState) {
    if state.ticks.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            "(waiting for first tick)",
            Style::new().fg(Color::DarkGray),
        )))
        .block(tile("status codes"));
        frame.render_widget(p, area);
        return;
    }

    let marker = state.marker;

    let mut s_2xx = Vec::with_capacity(state.ticks.len());
    let mut s_4xx = Vec::with_capacity(state.ticks.len());
    let mut s_5xx = Vec::with_capacity(state.ticks.len());

    for t in &state.ticks {
        let x = t.elapsed.as_secs_f64();
        let total = t.requests.max(1) as f64;
        let pct_4 = t.errors.status_4xx as f64 / total * 100.0;
        let pct_5 = t.errors.status_5xx as f64 / total * 100.0;
        let pct_2 = (100.0 - pct_4 - pct_5).max(0.0);
        s_2xx.push((x, pct_2));
        s_4xx.push((x, pct_4));
        s_5xx.push((x, pct_5));
    }

    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let owned = vec![
        OwnedDataset::line("2xx %", s_2xx, PALETTE[0], marker),
        OwnedDataset::line("4xx %", s_4xx, PALETTE[2], marker),
        OwnedDataset::line("5xx %", s_5xx, PALETTE[3], marker),
    ];

    let datasets = owned.iter().map(|d| d.to_dataset()).collect();

    let y_labels = vec![
        Line::from(Span::styled("0%", Style::new().fg(Color::Gray))),
        Line::from(Span::styled("50%", Style::new().fg(Color::Gray))),
        Line::from(Span::styled("100%", Style::new().fg(Color::Gray))),
    ];
    let x_labels = vec![
        Line::from(Span::styled(
            format!("{x_min:.0}s"),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format!("{x_max:.0}s"),
            Style::new().fg(Color::Gray),
        )),
    ];

    let chart = Chart::new(datasets)
        .block(tile("status codes over time  (2xx · 4xx · 5xx)"))
        .legend_position(None)
        .x_axis(
            Axis::default()
                .style(Style::new().fg(Color::DarkGray))
                .bounds([x_min, x_max])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .style(Style::new().fg(Color::DarkGray))
                .bounds([0.0, 100.0])
                .labels(y_labels),
        );
    frame.render_widget(chart, area);
}

// ---------------------------------------------------------------------------
// Cumulative totals panel — Table widget.
// ---------------------------------------------------------------------------

fn render_cumulative_table(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let e = &state.total_errors;
    let success = state
        .total_requests
        .saturating_sub(e.status_4xx + e.status_5xx);

    let dim = Style::new().fg(Color::DarkGray);
    let red_bold = Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD);
    let green_bold = Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD);

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
            Cell::from("assertion"),
            Cell::from(format!("{}", e.assertion_failed)).style(counter_style(e.assertion_failed)),
        ]),
        Row::new(vec![Cell::from(""), Cell::from("")]),
        Row::new(vec![
            Cell::from("2xx"),
            Cell::from(format!("{success}")).style(green_bold),
        ]),
        Row::new(vec![
            Cell::from("4xx"),
            Cell::from(format!("{}", e.status_4xx)).style(counter_style(e.status_4xx)),
        ]),
        Row::new(vec![
            Cell::from("5xx"),
            Cell::from(format!("{}", e.status_5xx)).style(counter_style(e.status_5xx)),
        ]),
    ];

    let widths = [Constraint::Length(12), Constraint::Min(8)];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["category", "count"])
                .style(Style::new().add_modifier(Modifier::BOLD))
                .bottom_margin(0),
        )
        .block(tile("cumulative totals"))
        .column_spacing(2);
    frame.render_widget(table, area);
}
