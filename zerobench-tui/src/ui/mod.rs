//! Rendering — one frame per invocation of [`render`].
//!
//! The UI is a tabbed, chart-rich dashboard modeled after btop / k9s /
//! ctop. A persistent header shows the target URL, elapsed time,
//! run-mode pill, and a horizontal tab bar; one of four tabs
//! ([`Tab::Overview`], [`Tab::Latency`], [`Tab::Throughput`],
//! [`Tab::Errors`]) fills the main area; an overlay help pane may
//! appear on top.
//!
//! Each tab lives in its own sub-module under `ui/` with a single
//! `render` function. Shared chrome — the header, the tab bar, the
//! footer, the help overlay — lives in this file and `ui/common.rs`.
//!
//! [`Tab::Overview`]: crate::state::Tab::Overview
//! [`Tab::Latency`]: crate::state::Tab::Latency
//! [`Tab::Throughput`]: crate::state::Tab::Throughput
//! [`Tab::Errors`]: crate::state::Tab::Errors

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    LineGauge, List, ListItem, ListState, Paragraph, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Tabs as TabsWidget,
};
use ratatui::Frame;

use crate::state::{DashboardState, RunMode, Tab};

pub mod common;
pub mod dataset;
pub mod errors;
pub mod help;
pub mod latency;
pub mod overview;
pub mod throughput;

use common::{
    compute_status, format_rate, status_pill, tile, ACCENT, CRITICAL, MIN_HEIGHT, MIN_WIDTH,
    PALETTE,
};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Draw one frame of the dashboard into `frame`.
///
/// Called from the main TUI loop at 10 Hz. The function is
/// non-allocating-lite: every frame allocates a handful of `String`s
/// for formatted numerics, but the work is negligible compared to the
/// benchmark's own traffic.
pub fn render(frame: &mut Frame, state: &DashboardState) {
    let area = frame.area();

    // Degenerate-terminal guard. At very small sizes the chart widgets
    // panic inside ratatui because the axis label layout assumes
    // non-zero chart area; we short-circuit with a hint instead.
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        render_too_small(frame, area);
        return;
    }

    let tall = area.height >= 40;
    let short = area.height < 30;

    // Header height adapts to terminal height.
    let header_h = if short { 4 } else { 5 };

    // Outer layout — fixed header, tab bar (1 row), main area,
    // keybind footer (1 row).
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_h), // header: title + pill + transport info
            Constraint::Length(3),        // tab bar (bordered for visual weight)
            Constraint::Min(8),           // tab body
            Constraint::Length(1),        // footer
        ])
        .split(area);

    render_header(frame, root[0], state, short);
    render_tab_bar(frame, root[1], state);
    render_tab_body(frame, root[2], state, tall, short);
    render_footer(frame, root[3], state);

    if state.help_visible {
        help::render(frame, area);
    }
}

// ---------------------------------------------------------------------------
// Too-small fallback
// ---------------------------------------------------------------------------

fn render_too_small(frame: &mut Frame, area: Rect) {
    let msg = format!(
        "terminal too small — need {}x{}, got {}x{}",
        MIN_WIDTH, MIN_HEIGHT, area.width, area.height
    );
    let p = Paragraph::new(Line::from(Span::styled(
        msg,
        Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD),
    )))
    .alignment(Alignment::Center);
    frame.render_widget(p, area);
}

// ---------------------------------------------------------------------------
// Header — title line, status pill, transport info line, progress gauge
// ---------------------------------------------------------------------------

fn render_header(frame: &mut Frame, area: Rect, state: &DashboardState, short: bool) {
    let block = tile("zerobench");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // In short mode (< 30 rows), compress to 2 content rows.
    let row_constraints = if short {
        vec![Constraint::Length(1), Constraint::Length(1)]
    } else {
        vec![
            Constraint::Length(1), // url + status pill + elapsed
            Constraint::Length(1), // target vs actual rate + LineGauge
            Constraint::Length(1), // transport info
        ]
    };

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(row_constraints)
        .split(inner);

    // Row 1: url · status · elapsed.
    let actual_pct = state.actual_vs_target_pct();
    let errors_present = state.total_errors.total() > 0;
    let status = if state.run_completed {
        common::Status::Done
    } else {
        compute_status(actual_pct, errors_present)
    };

    let elapsed_s = state.elapsed().as_secs_f64();
    let total_s = state.total_duration.as_secs();
    let progress_pct = state.progress() * 100.0;

    let row1 = Line::from(vec![
        Span::styled(" ⬢ ", Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)),
        Span::styled(
            state.url_label.clone(),
            Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled("   ", Style::new()),
        status_pill(status),
        Span::raw(" "),
        if state.run_completed {
            Span::styled("done", Style::new().fg(ACCENT).add_modifier(Modifier::BOLD))
        } else {
            Span::styled(
                format!("{elapsed_s:.1}s / {total_s}s"),
                Style::new().fg(Color::Gray),
            )
        },
        Span::raw(" · "),
        Span::styled(
            if state.run_completed {
                "100%".into()
            } else {
                format!("{progress_pct:.0}%")
            },
            Style::new().fg(Color::Gray),
        ),
    ]);
    frame.render_widget(Paragraph::new(row1), rows[0]);

    if short {
        // In short mode, row 2 is transport info (skip rate row).
        render_transport_line(frame, rows[1], state);
        return;
    }

    // Row 2: target / actual rates + LineGauge.
    let row2_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(2), Constraint::Fill(1)])
        .split(rows[1]);

    let actual_rps = state.requests_per_sec();
    let target_span = match state.target_rate {
        Some(rate) => format!("target {}", format_rate(rate)),
        None => "target saturate".to_string(),
    };
    let actual_text = match actual_pct {
        Some(pct) => format!("actual {} ({:.2}%)", format_rate(actual_rps), pct),
        None => format!("actual {}", format_rate(actual_rps)),
    };
    let row2_left = Line::from(vec![
        Span::styled(" ", Style::new()),
        Span::styled(
            target_span,
            Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled(
            actual_text,
            Style::new()
                .fg(rate_colour(status))
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(Paragraph::new(row2_left), row2_cols[0]);

    // LineGauge — 1 row, replaces the old 3-row Gauge.
    let line_gauge = LineGauge::default()
        .ratio(state.progress())
        .label(format!("{:.0}%", state.progress() * 100.0))
        .filled_symbol("━")
        .unfilled_symbol("━")
        .filled_style(Style::new().fg(ACCENT))
        .unfilled_style(Style::new().fg(Color::DarkGray));
    frame.render_widget(line_gauge, row2_cols[1]);

    // Row 3: transport info.
    render_transport_line(frame, rows[2], state);
}

fn render_transport_line(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let tls_label = if state.transport.tls {
        match &state.transport.alpn {
            Some(alpn) => format!("TLS ({} via ALPN)", alpn),
            None => "TLS".to_string(),
        }
    } else {
        "plaintext".to_string()
    };
    let mode_label = match state.transport.mode {
        RunMode::Saturate(n) => format!("saturate · {n} conns"),
        RunMode::Rate(r) => {
            format!(
                "rate · {} conns · target {}",
                state.transport.connections,
                format_rate(r)
            )
        }
    };
    let row = Line::from(vec![
        Span::styled(" ▸ ", Style::new().fg(ACCENT)),
        Span::styled(
            format!("{} · {} · {}", mode_label, state.transport.protocol, tls_label,),
            Style::new().fg(Color::Gray),
        ),
    ]);
    frame.render_widget(Paragraph::new(row), area);
}

fn rate_colour(status: common::Status) -> Color {
    match status {
        common::Status::Green => common::SUCCESS,
        common::Status::Yellow => common::WARNING,
        common::Status::Red => common::CRITICAL,
        common::Status::Done => common::ACCENT,
    }
}

// ---------------------------------------------------------------------------
// Tab bar
// ---------------------------------------------------------------------------

fn render_tab_bar(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let titles: Vec<Line> = Tab::ALL
        .iter()
        .enumerate()
        .map(|(i, t)| {
            Line::from(vec![
                Span::styled(format!("[{}]", i + 1), Style::new().fg(Color::DarkGray)),
                Span::raw(" "),
                Span::styled(t.label(), Style::new().fg(Color::White)),
            ])
        })
        .collect();

    let tabs = TabsWidget::new(titles)
        .select(state.current_tab.index())
        .block(tile("tabs"))
        .highlight_style(
            Style::new()
                .fg(ACCENT)
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::UNDERLINED),
        )
        .divider(Span::styled("│", Style::new().fg(Color::DarkGray)));
    frame.render_widget(tabs, area);
}

// ---------------------------------------------------------------------------
// Tab body — dispatches to the selected tab's render fn.
// ---------------------------------------------------------------------------

fn render_tab_body(
    frame: &mut Frame,
    area: Rect,
    state: &DashboardState,
    tall: bool,
    short: bool,
) {
    let wide = area.width >= 140;

    // Log pane takes the bottom rows when toggled on, but only if
    // the terminal is tall enough.
    let show_log = state.log_visible && !short;
    let (tab_area, log_area) = if show_log {
        let log_h = if tall { 8 } else { 5 };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(8), Constraint::Length(log_h)])
            .split(area);
        (chunks[0], Some(chunks[1]))
    } else {
        (area, None)
    };

    match state.current_tab {
        Tab::Overview => overview::render(frame, tab_area, state, wide),
        Tab::Latency => latency::render(frame, tab_area, state, wide),
        Tab::Throughput => throughput::render(frame, tab_area, state, wide),
        Tab::Errors => errors::render(frame, tab_area, state, wide),
    }

    if let Some(log) = log_area {
        render_log_pane(frame, log, state);
    }
}

fn render_log_pane(frame: &mut Frame, area: Rect, state: &DashboardState) {
    if state.log_entries.is_empty() {
        let block = tile("log");
        let p = Paragraph::new(Line::from(Span::styled(
            "(no log events — assertion failures and sample errors will appear here; press 'l' to hide)",
            Style::new().fg(Color::DarkGray),
        )))
        .block(block);
        frame.render_widget(p, area);
        return;
    }

    let items: Vec<ListItem> = state
        .log_entries
        .iter()
        .rev() // newest first in the visual list
        .map(|e| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:.1}s ", e.timestamp.as_secs_f64()),
                    Style::new().fg(Color::DarkGray),
                ),
                Span::styled(
                    e.category.clone(),
                    Style::new().fg(PALETTE[3]).add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::raw(e.message.clone()),
            ]))
        })
        .collect();

    let list = List::new(items)
        .block(tile("log"))
        .highlight_style(Style::new().add_modifier(Modifier::REVERSED));

    let mut list_state = ListState::default().with_offset(state.log_scroll);
    frame.render_stateful_widget(list, area, &mut list_state);

    // Scrollbar on the right edge.
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
    let mut sb_state = ScrollbarState::new(state.log_entries.len()).position(state.log_scroll);
    frame.render_stateful_widget(scrollbar, area, &mut sb_state);
}

// ---------------------------------------------------------------------------
// Footer — compact keybind reminder + state flags.
// ---------------------------------------------------------------------------

fn render_footer(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let paused = if state.paused_rendering {
        " [PAUSED]"
    } else {
        ""
    };
    let log_flag = if state.log_visible { " [log]" } else { "" };
    let text = format!(
        " [1-4] tab   [?] help   [+/-] zoom   [m] marker   [0] reset zoom   [r] reset peaks   [p] pause{paused}   [l] log{log_flag}   [j/k] scroll log   [q] quit "
    );
    let paragraph = Paragraph::new(Line::from(Span::styled(
        text,
        Style::new().fg(Color::DarkGray),
    )));
    frame.render_widget(paragraph, area);
}
