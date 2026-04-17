//! Throughput tab — rps time-series, bytes/s chart, summary panel.
//!
//! Layout:
//!
//! ```text
//! ╭─ rps over time ───────────────────────────────────────────╮
//! │ line chart + dashed target reference (open-loop)           │
//! ╰────────────────────────────────────────────────────────────╯
//! ╭─ bytes/s ───────────────╮ ╭─ summary ───────────────────╮
//! │ sent + recv lines       │ │ current / peak / min / avg  │
//! ╰─────────────────────────╯ ╰─────────────────────────────╯
//! ```

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Cell, Chart, Paragraph, Row, Table};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{format_bytes, format_bytes_rate, format_rate, tile, ACCENT, PALETTE, SUCCESS};
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

    render_rps_timeseries(frame, rows[0], state);

    if wide {
        let bot = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Fill(1), Constraint::Fill(1)])
            .split(rows[1]);
        render_bytes_chart(frame, bot[0], state);
        render_summary_table(frame, bot[1], state);
    } else {
        // Narrow: stack bottom panels vertically.
        let bot = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Fill(1), Constraint::Fill(1)])
            .split(rows[1]);
        render_bytes_chart(frame, bot[0], state);
        render_summary_table(frame, bot[1], state);
    }
}

// ---------------------------------------------------------------------------
// RPS over time
// ---------------------------------------------------------------------------

fn render_rps_timeseries(frame: &mut Frame, area: Rect, state: &DashboardState) {
    if state.ticks.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            "(waiting for first tick)",
            Style::new().fg(Color::DarkGray),
        )))
        .block(tile("requests per second"));
        frame.render_widget(p, area);
        return;
    }

    let marker = state.marker;
    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let mut owned = vec![OwnedDataset::line(
        "rps",
        state
            .ticks
            .iter()
            .map(|t| (t.elapsed.as_secs_f64(), t.requests as f64))
            .collect(),
        PALETTE[0],
        marker,
    )];

    // Target reference line — dim gray, spans full x-range.
    if let Some(t) = state.target_rate {
        owned.push(OwnedDataset::reference_line(
            vec![(x_min, t), (x_max, t)],
            PALETTE[5],
            marker,
        ));
    }

    let observed_max = owned[0]
        .data
        .iter()
        .map(|p| p.1)
        .fold(0.0_f64, f64::max);
    let y_max = match state.target_rate {
        Some(t) => (observed_max.max(t) * 1.15).max(1.0),
        None => (observed_max * 1.15).max(1.0),
    } * state.y_scale;

    let datasets = owned.iter().map(|d| d.to_dataset()).collect();

    let y_labels = vec![
        Line::from(Span::styled("0", Style::new().fg(Color::Gray))),
        Line::from(Span::styled(
            format_rate(y_max / 2.0),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format_rate(y_max),
            Style::new().fg(Color::Gray),
        )),
    ];
    let x_labels = vec![
        Line::from(Span::styled(
            format!("{x_min:.0}s"),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format!("{:.0}s", (x_min + x_max) / 2.0),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format!("{x_max:.0}s"),
            Style::new().fg(Color::Gray),
        )),
    ];

    let title = if state.target_rate.is_some() {
        "requests per second  (with target reference)"
    } else {
        "requests per second"
    };
    let chart = Chart::new(datasets)
        .block(tile(title))
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
// Bytes/s chart — sent + recv lines
// ---------------------------------------------------------------------------

fn render_bytes_chart(frame: &mut Frame, area: Rect, state: &DashboardState) {
    if state.ticks.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            "(waiting for first tick)",
            Style::new().fg(Color::DarkGray),
        )))
        .block(tile("bytes/s (sent + recv)"));
        frame.render_widget(p, area);
        return;
    }

    let marker = state.marker;

    let owned = vec![
        OwnedDataset::line(
            "recv",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.bytes_recv as f64))
                .collect(),
            PALETTE[1],
            marker,
        ),
        OwnedDataset::line(
            "sent",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.bytes_sent as f64))
                .collect(),
            PALETTE[4],
            marker,
        ),
    ];

    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let max_y = owned
        .iter()
        .flat_map(|d| d.data.iter())
        .map(|p| p.1)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let y_max = max_y * 1.15 * state.y_scale;

    let datasets = owned.iter().map(|d| d.to_dataset()).collect();

    let y_labels = vec![
        Line::from(Span::styled("0", Style::new().fg(Color::Gray))),
        Line::from(Span::styled(
            format_bytes_rate(y_max / 2.0),
            Style::new().fg(Color::Gray),
        )),
        Line::from(Span::styled(
            format_bytes_rate(y_max),
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
        .block(tile("bytes/s  (recv · sent)"))
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
// Summary — Table widget with clear labeling for rate vs total.
// ---------------------------------------------------------------------------

fn render_summary_table(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let current = state.requests_per_sec();
    let peak = state.peak_rps;
    let min = state.min_rps.unwrap_or(0.0);
    let avg = state.avg_rps();
    let (last_sent, last_recv) = state.last_tick_bytes();

    let dim = Style::new().fg(Color::Gray);
    let green_bold = Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD);
    let accent_bold = Style::new().fg(ACCENT).add_modifier(Modifier::BOLD);

    let rows = vec![
        Row::new(vec![
            Cell::from("current rps").style(dim),
            Cell::from(format_rate(current)).style(green_bold),
        ]),
        Row::new(vec![
            Cell::from("peak rps").style(dim),
            Cell::from(format_rate(peak)).style(accent_bold),
        ]),
        Row::new(vec![
            Cell::from("min rps").style(dim),
            Cell::from(format_rate(min)).style(dim),
        ]),
        Row::new(vec![
            Cell::from("avg rps").style(dim),
            Cell::from(format_rate(avg)).style(dim),
        ]),
        Row::new(vec![
            Cell::from("bytes/s (last)").style(dim),
            Cell::from(format!(
                "↑{} ↓{}",
                format_bytes_rate(last_sent as f64),
                format_bytes_rate(last_recv as f64)
            ))
            .style(dim),
        ]),
        Row::new(vec![
            Cell::from("total bytes").style(dim),
            Cell::from(format!(
                "↑{} ↓{}",
                format_bytes(state.cumulative_bytes_sent),
                format_bytes(state.cumulative_bytes_recv)
            ))
            .style(dim),
        ]),
    ];

    let widths = [Constraint::Length(16), Constraint::Min(10)];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["metric", "value"])
                .style(Style::new().add_modifier(Modifier::BOLD))
                .bottom_margin(0),
        )
        .block(tile("summary"))
        .column_spacing(2);
    frame.render_widget(table, area);
}
