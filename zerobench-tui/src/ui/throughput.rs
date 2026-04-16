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
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Chart, Dataset, GraphType, Paragraph};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{format_bytes, format_bytes_rate, format_rate, tile, ACCENT, SUCCESS};

// ---------------------------------------------------------------------------
// Tab entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect, state: &DashboardState) {
    // Top 60%: rps chart. Bottom 40%: bytes chart + summary.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    render_rps_timeseries(frame, rows[0], state);

    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[1]);

    render_bytes_chart(frame, bot[0], state);
    render_summary(frame, bot[1], state);
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

    let rps: Vec<(f64, f64)> = state
        .ticks
        .iter()
        .map(|t| (t.elapsed.as_secs_f64(), t.requests as f64))
        .collect();

    let x_min = rps.first().map(|p| p.0).unwrap_or(0.0);
    let x_max = rps
        .last()
        .map(|p| p.0)
        .unwrap_or_else(|| state.total_duration.as_secs_f64())
        .max(x_min + 1.0);

    // Y max uses either target + headroom (open-loop) or observed peak
    // + headroom. Ensures the target reference line doesn't clip against
    // the top frame.
    let observed_max = rps.iter().map(|p| p.1).fold(0.0_f64, f64::max);
    let y_max = match state.target_rate {
        Some(t) => (observed_max.max(t) * 1.15).max(1.0),
        None => (observed_max * 1.15).max(1.0),
    };

    // Target reference series — two points at the target rate, drawn as
    // a short "line" that spans the full x-range. Graph type Line keeps
    // it continuous; we colour it dim so it reads as a reference.
    let target_series: Vec<(f64, f64)> = match state.target_rate {
        Some(t) => vec![(x_min, t), (x_max, t)],
        None => Vec::new(),
    };

    let mut datasets = vec![
        Dataset::default()
            .name("rps")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(SUCCESS))
            .data(&rps),
    ];
    if !target_series.is_empty() {
        datasets.push(
            Dataset::default()
                .name("target")
                .marker(Marker::Dot)
                .graph_type(GraphType::Line)
                .style(Style::new().fg(Color::DarkGray))
                .data(&target_series),
        );
    }

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

    let sent: Vec<(f64, f64)> = state
        .ticks
        .iter()
        .map(|t| (t.elapsed.as_secs_f64(), t.bytes_sent as f64))
        .collect();
    let recv: Vec<(f64, f64)> = state
        .ticks
        .iter()
        .map(|t| (t.elapsed.as_secs_f64(), t.bytes_recv as f64))
        .collect();

    let x_min = sent.first().map(|p| p.0).unwrap_or(0.0);
    let x_max = sent
        .last()
        .map(|p| p.0)
        .unwrap_or_else(|| state.total_duration.as_secs_f64())
        .max(x_min + 1.0);

    let max_y = sent
        .iter()
        .chain(recv.iter())
        .map(|p| p.1)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let y_max = max_y * 1.15;

    let datasets = vec![
        Dataset::default()
            .name("recv")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(ACCENT))
            .data(&recv),
        Dataset::default()
            .name("sent")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(Color::Rgb(200, 180, 255)))
            .data(&sent),
    ];

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
// Summary — compact numeric panel.
// ---------------------------------------------------------------------------

fn render_summary(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("summary");

    let current = state.requests_per_sec();
    let peak = state.peak_rps;
    let min = state.min_rps.unwrap_or(0.0);
    let avg = state.avg_rps();

    let lines = vec![
        kv(" current rps", format_rate(current), SUCCESS),
        kv(" peak rps   ", format_rate(peak), ACCENT),
        kv(" min rps    ", format_rate(min), Color::Gray),
        kv(" avg rps    ", format_rate(avg), Color::Gray),
        kv(
            " total sent ",
            format_bytes(state.cumulative_bytes_sent),
            Color::Gray,
        ),
        kv(
            " total recv ",
            format_bytes(state.cumulative_bytes_recv),
            Color::Gray,
        ),
    ];

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

fn kv(key: &'static str, value: String, colour: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(key, Style::new().fg(Color::Gray)),
        Span::raw("  "),
        Span::styled(value, Style::new().fg(colour).add_modifier(Modifier::BOLD)),
    ])
}
