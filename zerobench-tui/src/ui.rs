//! Rendering — one frame per invocation of [`render`].
//!
//! The UI mirrors the layout spec in `docs/design.md` §6.1:
//!
//! ```text
//! ┌─ zerobench ─── URL ──── elapsed / total ─┐
//! │  target rate            actual rate (%)   │
//! │  <progress gauge>                         │
//! ├─ throughput ─────────────────────────────┤
//! │  <sparkline>                              │
//! ├─ latency (last 5s) ─┬─ errors ─┬─ scenarios ─┤
//! │  p50/p90/p99/p99.9  │  ...     │  ...         │
//! └─────────────────────┴──────────┴──────────────┘
//! ```
//!
//! We don't try to reproduce the exact ASCII-art from the spec (the
//! fonts, symbols, and chart shape depend on ratatui's widgets). The
//! goal is to hit the same information density and layout.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
// `Stylize` is pulled in transitively through ratatui's extension-method
// blanket impl; explicit import isn't needed here.
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Gauge, Paragraph, Sparkline};
use ratatui::Frame;

use crate::state::{DashboardState, ROLLING_LATENCY_WINDOW};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Draw one frame of the dashboard into `frame`. Called from the main
/// TUI loop at 10 Hz — no allocations are strictly necessary, but
/// we prioritise readability over minimal-alloc since the work is
/// negligible compared to the benchmark's network traffic.
pub fn render(frame: &mut Frame, state: &DashboardState) {
    let area = frame.area();
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5), // header: title + target/actual + gauge
            Constraint::Length(8), // throughput sparkline
            Constraint::Min(8),    // latency + errors + scenarios row
            Constraint::Length(1), // keybind footer
        ])
        .split(area);

    render_header(frame, root[0], state);
    render_throughput(frame, root[1], state);
    render_middle_row(frame, root[2], state);
    render_footer(frame, root[3], state);
}

// ---------------------------------------------------------------------------
// Header — title bar + target/actual line + progress gauge.
// ---------------------------------------------------------------------------

fn render_header(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let title = format!(
        " zerobench — {} — {}/{}s ",
        state.url_label,
        state.elapsed().as_secs(),
        state.total_duration.as_secs(),
    );
    let block = Block::bordered().title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Split inner: one row for target/actual, one row for the gauge.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(inner);

    // target vs actual line
    let actual_rps = state.requests_per_sec();
    let target_span = match state.target_rate {
        Some(rate) => format!("target {}", format_rate(rate)),
        None => "target saturate".to_string(),
    };
    let actual_text = match state.actual_vs_target_pct() {
        Some(pct) => {
            format!(
                "actual {} ({:.2}%)",
                format_rate(actual_rps),
                pct
            )
        }
        None => format!("actual {}", format_rate(actual_rps)),
    };
    let line = Line::from(vec![
        Span::styled(target_span, Style::new().bold()),
        Span::raw("    "),
        Span::styled(actual_text, Style::new().fg(Color::Green).bold()),
    ]);
    frame.render_widget(Paragraph::new(line), chunks[0]);

    // progress gauge
    let ratio = state.progress();
    let label = format!("{:.0}% elapsed", ratio * 100.0);
    let gauge = Gauge::default()
        .ratio(ratio)
        .label(label)
        .gauge_style(Style::new().fg(Color::Cyan));
    frame.render_widget(gauge, chunks[1]);
}

// ---------------------------------------------------------------------------
// Throughput — sparkline of requests per tick.
// ---------------------------------------------------------------------------

fn render_throughput(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::bordered().title(" throughput (req/s per tick) ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // We cap the data at the sparkline width so we don't feed more
    // points than pixels; ratatui would otherwise compress them.
    let width = inner.width as usize;
    if width == 0 {
        return;
    }
    let data = state.sparkline_data(width);

    if data.is_empty() {
        // No ticks yet — draw a placeholder hint.
        let hint = Paragraph::new("(waiting for first tick)")
            .style(Style::new().fg(Color::DarkGray))
            .alignment(Alignment::Left);
        frame.render_widget(hint, inner);
        return;
    }

    // Cap the max to target_rate when we have one, so the scale stays
    // stable through the run; otherwise auto-scale to the observed
    // peak.
    let max_hint = state
        .target_rate
        .map(|r| (r * 1.05) as u64) // 5% headroom
        .unwrap_or_else(|| *data.iter().max().unwrap_or(&1).max(&1));

    let sparkline = Sparkline::default()
        .data(&data)
        .max(max_hint)
        .style(Style::new().fg(Color::Green));
    frame.render_widget(sparkline, inner);
}

// ---------------------------------------------------------------------------
// Middle row — latency | errors | scenarios (and optional log pane).
// ---------------------------------------------------------------------------

fn render_middle_row(frame: &mut Frame, area: Rect, state: &DashboardState) {
    // If the log pane is visible, split horizontally with it taking
    // the bottom half; the main triple-column lives above.
    let (main_area, log_area) = if state.log_visible {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(6), Constraint::Length(5)])
            .split(area);
        (chunks[0], Some(chunks[1]))
    } else {
        (area, None)
    };

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(main_area);

    render_latency(frame, cols[0], state);
    render_errors(frame, cols[1], state);
    render_scenarios(frame, cols[2], state);

    if let Some(log) = log_area {
        render_log(frame, log, state);
    }
}

fn render_latency(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let title = format!(" latency (last {}s) ", ROLLING_LATENCY_WINDOW);
    let block = Block::bordered().title(title);

    let hist = state.rolling_latency();
    let lines: Vec<Line> = match hist {
        Some(h) => {
            let p50 = h.value_at_percentile(50.0);
            let p90 = h.value_at_percentile(90.0);
            let p99 = h.value_at_percentile(99.0);
            let p99_9 = h.value_at_percentile(99.9);
            let max = h.max();

            let delta_line = match state.p99_9_delta_pct() {
                Some(pct) if pct.abs() < 1.0 => {
                    // essentially flat
                    Span::styled(
                        format!("  {:+.1}%", pct),
                        Style::new().fg(Color::DarkGray),
                    )
                }
                Some(pct) if pct > 0.0 => Span::styled(
                    format!(" ▲{:.1}%", pct),
                    Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Some(pct) => Span::styled(
                    format!(" ▼{:.1}%", pct.abs()),
                    Style::new().fg(Color::Green).add_modifier(Modifier::BOLD),
                ),
                None => Span::raw(""),
            };

            vec![
                Line::from(vec![
                    Span::styled("p50    ", Style::new().bold()),
                    Span::raw(format_ns(p50)),
                ]),
                Line::from(vec![
                    Span::styled("p90    ", Style::new().bold()),
                    Span::raw(format_ns(p90)),
                ]),
                Line::from(vec![
                    Span::styled("p99    ", Style::new().bold()),
                    Span::raw(format_ns(p99)),
                ]),
                Line::from(vec![
                    Span::styled("p99.9  ", Style::new().bold()),
                    Span::raw(format_ns(p99_9)),
                    delta_line,
                ]),
                Line::from(vec![
                    Span::styled("max    ", Style::new().bold()),
                    Span::styled(format_ns(max), Style::new().fg(Color::Red)),
                ]),
            ]
        }
        None => vec![Line::from(Span::styled(
            "no samples yet",
            Style::new().fg(Color::DarkGray),
        ))],
    };

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn render_errors(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::bordered().title(" errors (cumulative) ");
    let e = &state.total_errors;

    let counted = |n: u64| -> Span<'static> {
        if n == 0 {
            Span::styled(format!("{n}"), Style::new().fg(Color::Green))
        } else {
            Span::styled(
                format!("{n}"),
                Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
            )
        }
    };

    let lines = vec![
        Line::from(vec![Span::raw("connect    "), counted(e.connect)]),
        Line::from(vec![Span::raw("read       "), counted(e.read)]),
        Line::from(vec![Span::raw("write      "), counted(e.write)]),
        Line::from(vec![Span::raw("timeout    "), counted(e.timeout)]),
        Line::from(vec![Span::raw("keepup     "), counted(e.keepup)]),
        Line::from(vec![Span::raw("assert     "), counted(e.assertion_failed)]),
        Line::from(vec![
            Span::raw("4xx/5xx    "),
            counted(e.status_4xx),
            Span::raw("/"),
            counted(e.status_5xx),
        ]),
    ];

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn render_scenarios(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::bordered().title(" scenarios ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // v0.0.1: we don't have per-scenario tick data in LiveTick (only
    // the aggregate). So we show a single pseudo-row summarising the
    // overall request rate. Multi-scenario tick-level breakout is a
    // future enhancement.
    let rps = state.requests_per_sec();
    let last_errs = state.last_tick_errors();
    let lines = vec![
        Line::from(vec![
            Span::styled("total", Style::new().bold()),
            Span::raw("     "),
            Span::styled(format_rate(rps), Style::new().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::raw("  errors/s "),
            Span::styled(
                format!("{}", last_errs.total()),
                if last_errs.total() == 0 {
                    Style::new().fg(Color::Green)
                } else {
                    Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
                },
            ),
        ]),
        Line::from(Span::styled(
            "  (per-scenario breakdown coming)",
            Style::new().fg(Color::DarkGray),
        )),
    ];

    frame.render_widget(Paragraph::new(lines), inner);
}

fn render_log(frame: &mut Frame, area: Rect, _state: &DashboardState) {
    let block = Block::bordered().title(" log (press 'l' to hide) ");
    let paragraph = Paragraph::new(Line::from(Span::styled(
        "(log pane — assertion failures and sample errors will appear here in future)",
        Style::new().fg(Color::DarkGray),
    )))
    .block(block);
    frame.render_widget(paragraph, area);
}

// ---------------------------------------------------------------------------
// Footer — keybind reminder.
// ---------------------------------------------------------------------------

fn render_footer(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let paused = if state.paused_rendering { " [PAUSED]" } else { "" };
    let log_flag = if state.log_visible { " [log]" } else { "" };
    let text = format!(
        " [q] quit   [p] pause render{}   [l] toggle log{}",
        paused, log_flag
    );
    let paragraph = Paragraph::new(Line::from(Span::styled(
        text,
        Style::new().fg(Color::Gray),
    )));
    frame.render_widget(paragraph, area);
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Format a requests-per-second value — `"9,994 req/s"` for small
/// values, `"10.0k req/s"` once we hit thousands.
fn format_rate(rps: f64) -> String {
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

/// Format a nanosecond count — duplicate of `zerobench-core`'s
/// `format_ns` but kept local to avoid exporting an unnecessary
/// public symbol from core just for the TUI.
fn format_ns(ns: u64) -> String {
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
}
