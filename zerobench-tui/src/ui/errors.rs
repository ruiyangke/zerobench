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
//! │ 2xx / 4xx / 5xx percentages │ │ connect / read / write /  │
//! ╰─────────────────────────────╯ │ timeout / keepup / assert │
//!                                  ╰────────────────────────────╯
//! ```

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Chart, Paragraph};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{tile, CRITICAL, PALETTE, SUCCESS};
use super::dataset::OwnedDataset;

// ---------------------------------------------------------------------------
// Tab entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect, state: &DashboardState) {
    // Chart dominates; bottom panel is fixed at 8 rows.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    render_category_timeseries(frame, rows[0], state);

    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[1]);

    render_status_timeseries(frame, bot[0], state);
    render_cumulative(frame, bot[1], state);
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
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.errors.connect as f64)).collect(),
            PALETTE[3],
            marker,
        ),
        OwnedDataset::line(
            "read",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.errors.read as f64)).collect(),
            Color::Rgb(255, 153, 153),
            marker,
        ),
        OwnedDataset::line(
            "write",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.errors.write as f64)).collect(),
            Color::Rgb(255, 180, 120),
            marker,
        ),
        OwnedDataset::line(
            "timeout",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.errors.timeout as f64)).collect(),
            PALETTE[2],
            marker,
        ),
        OwnedDataset::line(
            "keepup",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.errors.keepup as f64)).collect(),
            PALETTE[4],
            marker,
        ),
        OwnedDataset::line(
            "assert",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.errors.assertion_failed as f64)).collect(),
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
// Cumulative totals panel.
// ---------------------------------------------------------------------------

fn render_cumulative(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("cumulative totals");
    let e = &state.total_errors;
    let success = state.total_requests.saturating_sub(e.status_4xx + e.status_5xx);

    let counter = |n: u64| -> Span<'static> {
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
        row(" connect   ", counter(e.connect)),
        row(" read      ", counter(e.read)),
        row(" write     ", counter(e.write)),
        row(" timeout   ", counter(e.timeout)),
        row(" keepup    ", counter(e.keepup)),
        row(" assertion ", counter(e.assertion_failed)),
        Line::from(Span::raw("")),
        row(
            " 2xx       ",
            Span::styled(
                format!("{success}"),
                Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD),
            ),
        ),
        row(" 4xx       ", counter(e.status_4xx)),
        row(" 5xx       ", counter(e.status_5xx)),
    ];

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

fn row(label: &'static str, value: Span<'static>) -> Line<'static> {
    Line::from(vec![
        Span::styled(label, Style::new().fg(Color::Gray)),
        value,
    ])
}
