//! Help overlay — centred modal pane, rendered on top of the current
//! tab via [`ratatui::widgets::Clear`] + a bordered block. Triggered by
//! `?`, dismissed by the same key (or `Esc`).

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};
use ratatui::Frame;

use super::common::{tile, ACCENT, CRITICAL, SUCCESS, WARNING};

// ---------------------------------------------------------------------------
// Overlay entry point
// ---------------------------------------------------------------------------

pub fn render(frame: &mut Frame, area: Rect) {
    // Centre the panel. Use percentage constraints; the panel shrinks
    // but stays readable on any terminal size above the MIN threshold.
    let v = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(15),
            Constraint::Percentage(70),
            Constraint::Percentage(15),
        ])
        .split(area);
    let h = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(15),
            Constraint::Percentage(70),
            Constraint::Percentage(15),
        ])
        .split(v[1]);
    let panel = h[1];

    // Clear the area behind the panel — otherwise we'd composite over
    // the existing tab's chart.
    frame.render_widget(Clear, panel);

    let block = tile("help");

    let lines = vec![
        Line::from(""),
        section("Navigation"),
        kbd("1 / 2 / 3 / 4", "switch to tab N"),
        kbd("Tab / Shift-Tab", "cycle tabs forward / backward"),
        kbd("?", "show / hide this help"),
        Line::from(""),
        section("Control"),
        kbd("q / Esc", "quit benchmark early (print report)"),
        kbd("s", "save report snapshot to JSON file"),
        kbd("p", "pause rendering (benchmark continues)"),
        kbd("r", "reset peak/min trackers"),
        kbd("l", "toggle inline log pane"),
        kbd("↑ / k", "scroll log up (when visible)"),
        kbd("↓ / j", "scroll log down (when visible)"),
        kbd("Home", "scroll log to top"),
        kbd("End", "scroll log to bottom"),
        Line::from(""),
        section("Charts"),
        kbd("+ / =", "zoom in Y axis (reduce scale 20%)"),
        kbd("-", "zoom out Y axis (increase scale 20%)"),
        kbd("0", "reset Y axis to auto-scale"),
        kbd("m", "toggle marker (braille / dot)"),
        Line::from(""),
        section("Panel legend"),
        legend(
            "▲ pct",
            "metric regressed (worse)",
            Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD),
        ),
        legend(
            "▼ pct",
            "metric improved (better)",
            Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD),
        ),
        legend(
            "⬤ green",
            "rate on target, no errors",
            Style::new().fg(SUCCESS).add_modifier(Modifier::BOLD),
        ),
        legend(
            "⬤ yellow",
            "rate 80-95% of target OR 105-120%",
            Style::new().fg(WARNING).add_modifier(Modifier::BOLD),
        ),
        legend(
            "⬤ red",
            "rate <80% / >120% or errors present",
            Style::new().fg(CRITICAL).add_modifier(Modifier::BOLD),
        ),
        Line::from(""),
        Line::from(Span::styled(
            "  (press ? or Esc to close)",
            Style::new().fg(Color::DarkGray),
        )),
    ];

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, panel);
}

fn section(title: &'static str) -> Line<'static> {
    Line::from(vec![
        Span::raw("  "),
        Span::styled(title, Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)),
    ])
}

fn kbd(key: &'static str, desc: &'static str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("    {key:<18}"),
            Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled(desc, Style::new().fg(Color::Gray)),
    ])
}

fn legend(glyph: &'static str, desc: &'static str, style: Style) -> Line<'static> {
    Line::from(vec![
        Span::raw("    "),
        Span::styled(format!("{glyph:<12}"), style),
        Span::styled(desc, Style::new().fg(Color::Gray)),
    ])
}
