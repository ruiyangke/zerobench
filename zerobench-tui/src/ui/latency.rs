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
//! │ p50..max bars              │ │ buckets                    │
//! ╰────────────────────────────╯ ╰────────────────────────────╯
//! ```

use hdrhistogram::Histogram;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Chart, Paragraph};
use ratatui::Frame;

use crate::state::DashboardState;

use super::common::{format_ns, hbar_smooth, tile, CRITICAL, PALETTE, SUCCESS};
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

    render_timeseries(frame, rows[0], state);

    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[1]);

    render_window_bars(frame, bot[0], state);
    render_distribution(frame, bot[1], state);
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

    let owned = vec![
        OwnedDataset::line(
            "p50",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.p50_ns as f64)).collect(),
            PALETTE[0],
            marker,
        ),
        OwnedDataset::line(
            "p90",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.p90_ns as f64)).collect(),
            PALETTE[1],
            marker,
        ),
        OwnedDataset::line(
            "p99",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.p99_ns as f64)).collect(),
            PALETTE[2],
            marker,
        ),
        OwnedDataset::line(
            "p99.9",
            state.ticks.iter().map(|t| (t.elapsed.as_secs_f64(), t.p99_9_ns as f64)).collect(),
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
// Rolling-window bars — same as Overview.
// ---------------------------------------------------------------------------

fn render_window_bars(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("current 5s window");
    let hist = state.rolling_latency();
    let inner_width = block.inner(area).width;

    let lines: Vec<Line> = match hist {
        Some(h) => {
            let p50 = h.value_at_percentile(50.0);
            let p90 = h.value_at_percentile(90.0);
            let p99 = h.value_at_percentile(99.0);
            let p99_9 = h.value_at_percentile(99.9);
            let max = h.max();
            let bar_width = (inner_width as usize).saturating_sub(20).max(6);
            let max_f = max as f64;

            let delta_span = match state.p99_9_delta_pct() {
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
            };

            vec![
                pct_line("p50  ", p50, max_f, bar_width, PALETTE[0], None),
                pct_line("p90  ", p90, max_f, bar_width, PALETTE[1], None),
                pct_line("p99  ", p99, max_f, bar_width, PALETTE[2], None),
                pct_line("p99.9", p99_9, max_f, bar_width, PALETTE[3], delta_span),
                pct_line("max  ", max, max_f, bar_width, CRITICAL, None),
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

fn pct_line(
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

// ---------------------------------------------------------------------------
// Distribution panel — log10-bucketed histogram over the rolling window.
// ---------------------------------------------------------------------------

fn render_distribution(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = tile("distribution (log10 buckets)");

    let hist = state.rolling_latency();
    let lines: Vec<Line> = match hist {
        Some(h) => log_bucketed_lines(&h, area.width.saturating_sub(20).max(10) as usize),
        None => vec![Line::from(Span::styled(
            "no samples yet",
            Style::new().fg(Color::DarkGray),
        ))],
    };

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

/// Bucket the histogram into log10 slots — 100µs / 500µs / 1ms / 5ms /
/// 10ms / 100ms — and render each as a proportional bar.
///
/// Picked these bucket edges because they're the points most developers
/// have intuition for. Anything above 100ms is already a performance
/// problem and we just clamp it into the last bucket.
fn log_bucketed_lines(hist: &Histogram<u64>, bar_width: usize) -> Vec<Line<'static>> {
    const BUCKETS: &[(u64, &str)] = &[
        (100_000, "100µs"),
        (500_000, "500µs"),
        (1_000_000, "1ms  "),
        (5_000_000, "5ms  "),
        (10_000_000, "10ms "),
        (100_000_000, "100ms"),
    ];

    let total = hist.len() as f64;
    if total == 0.0 {
        return vec![Line::from(Span::styled(
            "no samples yet",
            Style::new().fg(Color::DarkGray),
        ))];
    }

    // Count how many samples fall strictly within each (prev, edge]
    // interval. `count_between` returns cumulative-to-edge, so we
    // subtract the previous cumulative.
    let mut prev_cum: u64 = 0;
    let mut lines = Vec::with_capacity(BUCKETS.len());
    for (edge, label) in BUCKETS {
        let cum = hist.count_between(0, *edge);
        let in_bucket = cum.saturating_sub(prev_cum);
        prev_cum = cum;
        let frac = in_bucket as f64 / total;
        let bar = hbar_smooth(frac, 1.0, bar_width);
        lines.push(Line::from(vec![
            Span::styled(
                format!(" {label}  "),
                Style::new().fg(Color::Gray),
            ),
            Span::styled(
                bar,
                Style::new().fg(Color::Rgb(180, 220, 180)),
            ),
            Span::styled(
                format!(" {}", in_bucket),
                Style::new().fg(Color::Gray),
            ),
        ]));
    }
    lines
}
