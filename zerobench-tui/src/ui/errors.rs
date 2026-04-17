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
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Chart, Dataset, GraphType, Paragraph};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{tile, CRITICAL, SUCCESS, WARNING};

// ---------------------------------------------------------------------------
// Tab entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
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

    // Build one series per category. Each is `(elapsed_s, count_in_tick)`.
    // For tabs where every category is zero, Chart still renders fine —
    // the flat-zero lines just hug the x-axis.
    let mut s_connect = Vec::with_capacity(state.ticks.len());
    let mut s_read = Vec::with_capacity(state.ticks.len());
    let mut s_write = Vec::with_capacity(state.ticks.len());
    let mut s_timeout = Vec::with_capacity(state.ticks.len());
    let mut s_keepup = Vec::with_capacity(state.ticks.len());
    let mut s_assert = Vec::with_capacity(state.ticks.len());

    for t in &state.ticks {
        let x = t.elapsed.as_secs_f64();
        s_connect.push((x, t.errors.connect as f64));
        s_read.push((x, t.errors.read as f64));
        s_write.push((x, t.errors.write as f64));
        s_timeout.push((x, t.errors.timeout as f64));
        s_keepup.push((x, t.errors.keepup as f64));
        s_assert.push((x, t.errors.assertion_failed as f64));
    }

    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let y_max = [&s_connect, &s_read, &s_write, &s_timeout, &s_keepup, &s_assert]
        .iter()
        .flat_map(|s| s.iter())
        .map(|(_, y)| *y)
        .fold(0.0_f64, f64::max)
        .max(1.0);

    let datasets = vec![
        Dataset::default()
            .name("connect")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(CRITICAL))
            .data(&s_connect),
        Dataset::default()
            .name("read")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(Color::Rgb(255, 153, 153)))
            .data(&s_read),
        Dataset::default()
            .name("write")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(Color::Rgb(255, 180, 120)))
            .data(&s_write),
        Dataset::default()
            .name("timeout")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(WARNING))
            .data(&s_timeout),
        Dataset::default()
            .name("keepup")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(Color::Rgb(200, 180, 255)))
            .data(&s_keepup),
        Dataset::default()
            .name("assert")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(Color::Rgb(255, 120, 180)))
            .data(&s_assert),
    ];

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
                .bounds([0.0, y_max * 1.1])
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

    let mut s_2xx = Vec::with_capacity(state.ticks.len());
    let mut s_4xx = Vec::with_capacity(state.ticks.len());
    let mut s_5xx = Vec::with_capacity(state.ticks.len());

    for t in &state.ticks {
        let x = t.elapsed.as_secs_f64();
        let total = t.requests.max(1) as f64; // avoid /0 on empty ticks
        let pct_4 = t.errors.status_4xx as f64 / total * 100.0;
        let pct_5 = t.errors.status_5xx as f64 / total * 100.0;
        // 2xx "proxy": remaining fraction of successful requests for
        // the tick. We don't record a dedicated 2xx counter, so we
        // derive it as 100% - 4xx% - 5xx%.
        let pct_2 = (100.0 - pct_4 - pct_5).max(0.0);
        s_2xx.push((x, pct_2));
        s_4xx.push((x, pct_4));
        s_5xx.push((x, pct_5));
    }

    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let datasets = vec![
        Dataset::default()
            .name("2xx %")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(SUCCESS))
            .data(&s_2xx),
        Dataset::default()
            .name("4xx %")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(WARNING))
            .data(&s_4xx),
        Dataset::default()
            .name("5xx %")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(CRITICAL))
            .data(&s_5xx),
    ];

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
