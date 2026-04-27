//! Latency tab — time-series chart + rolling-window bars +
//! distribution histogram.
//!
//! Layout:
//!
//! ```text
//! ╭─ latency over time (p50/p90/p99/p99.9) ─────────────────╮
//! │ multi-line chart                                          │
//! ╰───────────────────────────────────────────────────────────╯
//! ╭─ current 5s window ────────╮ ╭─ distribution (log10) ────╮
//! │ BarChart (horizontal)      │ │ BarChart (vertical)        │
//! ╰────────────────────────────╯ ╰────────────────────────────╯
//! ```

use hdrhistogram::Histogram;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Bar, BarChart, BarGroup, Chart, Paragraph};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{format_ns, tile, CRITICAL, PALETTE};
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

    render_timeseries(frame, rows[0], state);

    if wide {
        let bot = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Fill(1), Constraint::Fill(1)])
            .split(rows[1]);
        render_window_barchart(frame, bot[0], state);
        render_distribution_barchart(frame, bot[1], state);
    } else {
        // Narrow: stack vertically.
        let bot = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Fill(1), Constraint::Fill(1)])
            .split(rows[1]);
        render_window_barchart(frame, bot[0], state);
        render_distribution_barchart(frame, bot[1], state);
    }
}

// ---------------------------------------------------------------------------
// Time-series chart — four lines (p50/p90/p99/p99.9) over run duration.
// ---------------------------------------------------------------------------

fn render_timeseries(frame: &mut Frame, area: Rect, state: &DashboardState) {
    if state.ticks.is_empty() {
        let block = tile("latency over time");
        let p = Paragraph::new(Line::from(Span::styled(
            "(waiting for first tick)",
            Style::new().fg(Color::DarkGray),
        )))
        .block(block);
        frame.render_widget(p, area);
        return;
    }

    let marker = state.marker;

    let owned = [
        OwnedDataset::line(
            "p50",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.p50_ns as f64))
                .collect(),
            PALETTE[0],
            marker,
        ),
        OwnedDataset::line(
            "p90",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.p90_ns as f64))
                .collect(),
            PALETTE[1],
            marker,
        ),
        OwnedDataset::line(
            "p99",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.p99_ns as f64))
                .collect(),
            PALETTE[2],
            marker,
        ),
        OwnedDataset::line(
            "p99.9",
            state
                .ticks
                .iter()
                .map(|t| (t.elapsed.as_secs_f64(), t.p99_9_ns as f64))
                .collect(),
            PALETTE[3],
            marker,
        ),
    ];

    let x_min = 0.0_f64;
    let x_max = state.total_duration.as_secs_f64().max(1.0);

    let y_raw_max = owned
        .iter()
        .flat_map(|d| d.data.iter())
        .map(|p| p.1)
        .fold(0.0_f64, f64::max);
    let y_max = (y_raw_max * 1.10).max(1.0) * state.y_scale;

    let datasets = owned.iter().map(|d| d.to_dataset()).collect();

    let y_labels: Vec<Line> = [0.0, y_max / 2.0, y_max]
        .iter()
        .map(|v| {
            Line::from(Span::styled(
                format_ns(*v as u64),
                Style::new().fg(Color::Gray),
            ))
        })
        .collect();
    let x_labels: Vec<Line> = [x_min, (x_min + x_max) / 2.0, x_max]
        .iter()
        .map(|v| {
            Line::from(Span::styled(
                format!("{v:.0}s"),
                Style::new().fg(Color::Gray),
            ))
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(tile("latency over time  (p50 · p90 · p99 · p99.9)"))
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
// Rolling-window bars — replaced hand-rolled hbar with BarChart.
// ---------------------------------------------------------------------------

fn render_window_barchart(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let hist = state.rolling_latency();

    match hist {
        Some(h) => {
            let p50 = h.value_at_percentile(50.0);
            let p90 = h.value_at_percentile(90.0);
            let p99 = h.value_at_percentile(99.0);
            let p99_9 = h.value_at_percentile(99.9);
            let max = h.max();

            // Convert to microseconds for bar values so bars are
            // proportional and human-readable.
            let to_us = |ns: u64| -> u64 { ns / 1_000 };

            let delta_label = match state.p99_9_delta_pct() {
                Some(pct) if pct.abs() < 1.0 => format!(" ({pct:+.1}%)"),
                Some(pct) if pct > 0.0 => format!(" (▲{pct:.1}%)"),
                Some(pct) => format!(" (▼{:.1}%)", pct.abs()),
                None => String::new(),
            };

            let bars = vec![
                Bar::default()
                    .value(to_us(p50))
                    .label(Line::from("p50"))
                    .style(Style::new().fg(PALETTE[0]))
                    .value_style(Style::new().fg(Color::White))
                    .text_value(format_ns(p50)),
                Bar::default()
                    .value(to_us(p90))
                    .label(Line::from("p90"))
                    .style(Style::new().fg(PALETTE[1]))
                    .value_style(Style::new().fg(Color::White))
                    .text_value(format_ns(p90)),
                Bar::default()
                    .value(to_us(p99))
                    .label(Line::from("p99"))
                    .style(Style::new().fg(PALETTE[2]))
                    .value_style(Style::new().fg(Color::White))
                    .text_value(format_ns(p99)),
                Bar::default()
                    .value(to_us(p99_9))
                    .label(Line::from("p99.9"))
                    .style(Style::new().fg(PALETTE[3]))
                    .value_style(Style::new().fg(Color::White))
                    .text_value(format!("{}{}", format_ns(p99_9), delta_label)),
                Bar::default()
                    .value(to_us(max))
                    .label(Line::from("max"))
                    .style(Style::new().fg(CRITICAL))
                    .value_style(Style::new().fg(Color::White))
                    .text_value(format_ns(max)),
            ];

            let barchart = BarChart::default()
                .data(BarGroup::default().bars(&bars))
                .bar_width(1)
                .bar_gap(0)
                .max(to_us(max).max(1))
                .direction(Direction::Horizontal)
                .block(tile("current 5s window"));
            frame.render_widget(barchart, area);
        }
        None => {
            let p = Paragraph::new(Line::from(Span::styled(
                "no samples yet",
                Style::new().fg(Color::DarkGray),
            )))
            .block(tile("current 5s window"));
            frame.render_widget(p, area);
        }
    }
}

// ---------------------------------------------------------------------------
// Distribution panel — log10-bucketed histogram via BarChart widget.
// ---------------------------------------------------------------------------

fn render_distribution_barchart(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let hist = state.rolling_latency();

    match hist {
        Some(h) => {
            let (bars, max_val) = log_bucketed_bars(&h);

            let barchart = BarChart::default()
                .data(BarGroup::default().bars(&bars))
                .bar_width(3)
                .bar_gap(1)
                .max(max_val.max(1))
                .block(tile("distribution (log10 buckets)"));
            frame.render_widget(barchart, area);
        }
        None => {
            let p = Paragraph::new(Line::from(Span::styled(
                "no samples yet",
                Style::new().fg(Color::DarkGray),
            )))
            .block(tile("distribution (log10 buckets)"));
            frame.render_widget(p, area);
        }
    }
}

/// Bucket the histogram into log10 slots and return Bar widgets +
/// the maximum bucket count (used to set BarChart::max).
fn log_bucketed_bars(hist: &Histogram<u64>) -> (Vec<Bar<'static>>, u64) {
    const BUCKETS: &[(u64, &str)] = &[
        (100_000, "100µs"),
        (500_000, "500µs"),
        (1_000_000, "1ms"),
        (5_000_000, "5ms"),
        (10_000_000, "10ms"),
        (100_000_000, "100ms"),
    ];

    let total = hist.len();
    if total == 0 {
        return (vec![], 0);
    }

    let mut prev_cum: u64 = 0;
    let mut bars = Vec::with_capacity(BUCKETS.len());
    let mut max_val: u64 = 0;
    for (edge, label) in BUCKETS {
        let cum = hist.count_between(0, *edge);
        let in_bucket = cum.saturating_sub(prev_cum);
        prev_cum = cum;
        if in_bucket > max_val {
            max_val = in_bucket;
        }
        bars.push(
            Bar::default()
                .value(in_bucket)
                .label(Line::from(*label))
                .style(Style::new().fg(Color::Rgb(180, 220, 180)))
                .value_style(Style::new().fg(Color::White))
                .text_value(format!("{in_bucket}")),
        );
    }
    (bars, max_val)
}
