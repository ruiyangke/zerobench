//! Render snapshot tests — render the TUI into a fixed-size
//! `TestBackend` and assert high-signal substrings appear per tab.
//!
//! We use `contains`-based assertions rather than full frame
//! comparisons: ratatui's exact glyph output depends on terminal
//! geometry + symbols set, and a purely-visual tweak (e.g. swapping
//! the bar-character set) shouldn't break the test. The assertions
//! here verify that the *information* the user relies on appears
//! on-screen.

use std::time::Duration;

use hdrhistogram::Histogram;
use ratatui::backend::TestBackend;
use ratatui::Terminal;
use zerobench_core::live_snapshot::LiveTick;
use zerobench_core::stats::ErrorCounters;
use zerobench_tui::state::{DashboardState, RunMode, Tab, TransportInfo};
use zerobench_tui::ui::render;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const TERM_W: u16 = 140;
const TERM_H: u16 = 50;

fn fresh_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

fn tick(elapsed_s: u64, requests: u64, latency_ns: u64) -> LiveTick {
    let mut h = fresh_hist();
    for _ in 0..requests {
        let _ = h.record(latency_ns);
    }
    LiveTick {
        elapsed: Duration::from_secs(elapsed_s),
        requests,
        bytes_sent: 0,
        bytes_recv: 0,
        errors: ErrorCounters::default(),
        latency: h,
    }
}

fn tick_full(
    elapsed_s: u64,
    requests: u64,
    latency_ns: u64,
    bytes_sent: u64,
    bytes_recv: u64,
) -> LiveTick {
    let mut t = tick(elapsed_s, requests, latency_ns);
    t.bytes_sent = bytes_sent;
    t.bytes_recv = bytes_recv;
    t
}

fn fixture_transport() -> TransportInfo {
    TransportInfo {
        mode: RunMode::Saturate(100),
        connections: 100,
        protocol: "H2".into(),
        tls: true,
        alpn: Some("h2".into()),
    }
}

fn fresh_state(target_rate: Option<f64>) -> DashboardState {
    DashboardState::new(
        target_rate,
        Duration::from_secs(30),
        "http://api.example.com".into(),
        fixture_transport(),
    )
}

/// Flatten a ratatui `Buffer` into a single string. Rows are joined
/// with `\n` so `contains` assertions can match multi-line strings if
/// needed, but row-boundaries are preserved for debugging.
fn buffer_to_string(buf: &ratatui::buffer::Buffer) -> String {
    let w = buf.area.width as usize;
    let h = buf.area.height as usize;
    let mut out = String::with_capacity((w + 1) * h);
    for y in 0..h {
        for x in 0..w {
            let cell = &buf[(x as u16, y as u16)];
            out.push_str(cell.symbol());
        }
        out.push('\n');
    }
    out
}

fn render_state(state: &DashboardState) -> (Terminal<TestBackend>, String) {
    let backend = TestBackend::new(TERM_W, TERM_H);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|f| render(f, state)).unwrap();
    let s = buffer_to_string(terminal.backend().buffer());
    (terminal, s)
}

fn render_on(term: &mut Terminal<TestBackend>, state: &DashboardState) -> String {
    term.draw(|f| render(f, state)).unwrap();
    buffer_to_string(term.backend().buffer())
}

// ---------------------------------------------------------------------------
// Header chrome — persistent across all tabs.
// ---------------------------------------------------------------------------

#[test]
fn renders_header_with_url_label() {
    let mut state = fresh_state(Some(10_000.0));
    state.ingest(tick(1, 9_994, 120_000));

    let (_, content) = render_state(&state);
    assert!(
        content.contains("zerobench"),
        "missing title in buffer:\n{content}"
    );
    assert!(
        content.contains("http://api.example.com"),
        "missing url in buffer:\n{content}"
    );
}

#[test]
fn renders_target_and_actual_rates() {
    let mut state = fresh_state(Some(10_000.0));
    state.ingest(tick(1, 9_994, 120_000));

    let (_, content) = render_state(&state);
    assert!(content.contains("target"), "missing target row:\n{content}");
    assert!(
        content.contains("10.0k req/s"),
        "missing target rate value:\n{content}"
    );
    assert!(content.contains("actual"), "missing actual row:\n{content}");
    assert!(
        content.contains("99.94%") || content.contains("99.9%"),
        "missing actual-vs-target percent:\n{content}"
    );
}

#[test]
fn renders_transport_info_line() {
    let mut state = fresh_state(None);
    state.ingest(tick(1, 100, 1_000));
    let (_, content) = render_state(&state);
    assert!(
        content.contains("saturate"),
        "missing saturate label:\n{content}"
    );
    assert!(
        content.contains("100 conns"),
        "missing conn count:\n{content}"
    );
    assert!(content.contains("H2"), "missing protocol:\n{content}");
    assert!(content.contains("TLS"), "missing TLS:\n{content}");
    assert!(content.contains("h2"), "missing ALPN:\n{content}");
}

#[test]
fn renders_tab_bar_with_all_four_tabs() {
    let state = fresh_state(None);
    let (_, content) = render_state(&state);
    assert!(content.contains("Overview"), "missing Overview tab:\n{content}");
    assert!(content.contains("Latency"), "missing Latency tab:\n{content}");
    assert!(
        content.contains("Throughput"),
        "missing Throughput tab:\n{content}"
    );
    assert!(content.contains("Errors"), "missing Errors tab:\n{content}");
}

#[test]
fn renders_keybind_footer() {
    let state = fresh_state(None);
    let (_, content) = render_state(&state);
    assert!(content.contains("[q] quit"), "missing quit keybind:\n{content}");
    assert!(content.contains("[?] help"), "missing help keybind:\n{content}");
    assert!(content.contains("[r] reset"), "missing reset keybind:\n{content}");
    assert!(content.contains("[s] save"), "missing save keybind:\n{content}");
    assert!(content.contains("[1-4]"), "missing tab keybind:\n{content}");
}

// ---------------------------------------------------------------------------
// Overview tab (default)
// ---------------------------------------------------------------------------

#[test]
fn overview_tab_renders_latency_bars() {
    let mut state = fresh_state(Some(10_000.0));
    state.ingest(tick(1, 1_000, 120_000));

    let (_, content) = render_state(&state);
    assert!(content.contains("p50"), "missing p50 label:\n{content}");
    assert!(content.contains("p90"), "missing p90 label:\n{content}");
    assert!(content.contains("p99"), "missing p99 label:\n{content}");
    assert!(content.contains("p99.9"), "missing p99.9 label:\n{content}");
    assert!(content.contains("max"), "missing max label:\n{content}");
    assert!(content.contains("120µs"), "missing 120µs value:\n{content}");
}

#[test]
fn overview_tab_renders_totals_panel() {
    let mut state = fresh_state(None);
    state.ingest(tick_full(1, 100, 1_000, 5_000, 50_000));
    state.ingest(tick_full(2, 100, 1_000, 5_000, 50_000));
    let (_, content) = render_state(&state);
    assert!(
        content.contains("requests"),
        "missing requests label:\n{content}"
    );
    assert!(content.contains("2xx"), "missing 2xx label:\n{content}");
    assert!(content.contains("total"), "missing total label:\n{content}");
    // Cumulative 200 requests should appear in the totals.
    assert!(content.contains("200"), "missing request count:\n{content}");
}

#[test]
fn overview_tab_shows_waiting_hint_before_first_tick() {
    let state = fresh_state(Some(10_000.0));
    let (_, content) = render_state(&state);
    assert!(
        content.contains("waiting for first tick"),
        "missing placeholder hint:\n{content}"
    );
    assert!(
        content.contains("no samples"),
        "missing latency-empty hint:\n{content}"
    );
}

// ---------------------------------------------------------------------------
// Latency tab
// ---------------------------------------------------------------------------

#[test]
fn latency_tab_renders_timeseries_title() {
    let mut state = fresh_state(None);
    state.current_tab = Tab::Latency;
    for i in 1..=5 {
        state.ingest(tick(i, 100, 1_000_000));
    }
    let (_, content) = render_state(&state);
    assert!(
        content.contains("latency over time"),
        "missing time-series title:\n{content}"
    );
    // Rolling window panel should also appear.
    assert!(
        content.contains("current 5s window"),
        "missing rolling window panel:\n{content}"
    );
    assert!(
        content.contains("distribution"),
        "missing distribution panel:\n{content}"
    );
}

#[test]
fn latency_tab_distribution_shows_bucket_labels() {
    let mut state = fresh_state(None);
    state.current_tab = Tab::Latency;
    for i in 1..=3 {
        state.ingest(tick(i, 100, 1_000_000));
    }
    let (_, content) = render_state(&state);
    // At least one log10 bucket label must appear.
    let has_bucket = content.contains("100µs")
        || content.contains("500µs")
        || content.contains("1ms")
        || content.contains("5ms")
        || content.contains("10ms");
    assert!(has_bucket, "missing any bucket label:\n{content}");
}

// ---------------------------------------------------------------------------
// Throughput tab
// ---------------------------------------------------------------------------

#[test]
fn throughput_tab_renders_rps_and_bytes_panels() {
    let mut state = fresh_state(Some(10_000.0));
    state.current_tab = Tab::Throughput;
    for i in 1..=5 {
        state.ingest(tick_full(i, 9_000, 500_000, 5_000, 50_000));
    }
    let (_, content) = render_state(&state);
    assert!(
        content.contains("requests per second"),
        "missing rps panel:\n{content}"
    );
    assert!(
        content.contains("bytes/s"),
        "missing bytes/s panel:\n{content}"
    );
    assert!(
        content.contains("summary"),
        "missing summary panel:\n{content}"
    );
    assert!(
        content.contains("peak rps"),
        "missing peak rps row:\n{content}"
    );
    // Target reference is announced when a target is set.
    assert!(
        content.contains("target reference"),
        "missing target-reference hint:\n{content}"
    );
}

#[test]
fn throughput_tab_no_target_hides_reference_text() {
    let mut state = fresh_state(None);
    state.current_tab = Tab::Throughput;
    state.ingest(tick(1, 100, 1_000));
    let (_, content) = render_state(&state);
    assert!(
        !content.contains("target reference"),
        "saturate run should hide target reference:\n{content}"
    );
}

// ---------------------------------------------------------------------------
// Errors tab
// ---------------------------------------------------------------------------

#[test]
fn errors_tab_renders_all_three_panels() {
    let mut state = fresh_state(None);
    state.current_tab = Tab::Errors;
    let mut err = ErrorCounters::default();
    err.timeout = 2;
    err.connect = 1;
    let mut t = tick(1, 100, 1_000);
    t.errors = err;
    state.ingest(t);
    state.ingest(tick(2, 100, 1_000));

    let (_, content) = render_state(&state);
    assert!(
        content.contains("errors/sec by category"),
        "missing category chart title:\n{content}"
    );
    assert!(
        content.contains("status codes"),
        "missing status-code panel:\n{content}"
    );
    assert!(
        content.contains("cumulative totals"),
        "missing cumulative panel:\n{content}"
    );
    assert!(
        content.contains("connect"),
        "missing connect row:\n{content}"
    );
    assert!(
        content.contains("timeout"),
        "missing timeout row:\n{content}"
    );
    assert!(
        content.contains("assert"),
        "missing assert category:\n{content}"
    );
}

// ---------------------------------------------------------------------------
// Help overlay
// ---------------------------------------------------------------------------

#[test]
fn help_overlay_renders_when_visible() {
    let mut state = fresh_state(None);
    state.ingest(tick(1, 100, 1_000));
    state.help_visible = true;
    let (_, content) = render_state(&state);
    assert!(
        content.contains("Navigation"),
        "missing help nav section:\n{content}"
    );
    assert!(
        content.contains("show / hide"),
        "missing help ? description:\n{content}"
    );
    assert!(
        content.contains("Panel legend"),
        "missing help panel legend:\n{content}"
    );
}

// ---------------------------------------------------------------------------
// Pause / log stubs
// ---------------------------------------------------------------------------

#[test]
fn pause_flag_does_not_break_rendering() {
    let mut state = fresh_state(None);
    state.paused_rendering = true;
    state.ingest(tick(1, 100, 1_000));
    let (_, content) = render_state(&state);
    assert!(content.contains("zerobench"));
    assert!(content.contains("PAUSED"), "missing pause indicator:\n{content}");
}

#[test]
fn log_pane_toggle_renders_stub_message() {
    let mut state = fresh_state(None);
    state.ingest(tick(1, 10, 1_000));
    // Without log: footer shouldn't mention the stub content.
    let (_, plain) = render_state(&state);
    assert!(!plain.contains("no log events"));

    state.log_visible = true;
    let (_, with_log) = render_state(&state);
    assert!(
        with_log.contains("no log events"),
        "log pane missing its stub content:\n{with_log}"
    );
}

// ---------------------------------------------------------------------------
// Small terminal + degenerate sizes
// ---------------------------------------------------------------------------

#[test]
fn renders_small_terminal_without_panicking() {
    // At our threshold the renderer falls back to a "too small" hint.
    let backend = TestBackend::new(40, 10);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = fresh_state(None);
    state.ingest(tick(1, 10, 1_000));

    terminal.draw(|f| render(f, &state)).unwrap();
    let content = buffer_to_string(terminal.backend().buffer());
    assert!(
        content.contains("terminal too small"),
        "expected fallback message:\n{content}"
    );
}

// ---------------------------------------------------------------------------
// Delta indicator behaviour preserved on Overview
// ---------------------------------------------------------------------------

#[test]
fn delta_indicator_renders_up_glyph_on_regression() {
    let mut state = fresh_state(None);
    for i in 0..10 {
        state.ingest(tick(i, 100, 1_000_000));
    }
    for i in 10..15 {
        state.ingest(tick(i, 100, 20_000_000));
    }
    let (_, content) = render_state(&state);
    assert!(
        content.contains("▲"),
        "regression should render ▲ glyph:\n{content}"
    );
    assert!(
        content.contains("%"),
        "delta indicator should include a percent sign:\n{content}"
    );
}

#[test]
fn delta_indicator_renders_down_glyph_on_improvement() {
    let mut state = fresh_state(None);
    for i in 0..10 {
        state.ingest(tick(i, 100, 20_000_000));
    }
    for i in 10..15 {
        state.ingest(tick(i, 100, 1_000_000));
    }
    let (_, content) = render_state(&state);
    assert!(
        content.contains("▼"),
        "improvement should render ▼ glyph:\n{content}"
    );
}

// ---------------------------------------------------------------------------
// Switching tabs changes the rendered body
// ---------------------------------------------------------------------------

#[test]
fn switching_tab_changes_rendered_body() {
    let backend = TestBackend::new(TERM_W, TERM_H);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = fresh_state(None);
    for i in 1..=5 {
        state.ingest(tick(i, 100, 1_000_000));
    }

    // Overview: should show "totals" panel.
    let overview = render_on(&mut terminal, &state);
    assert!(overview.contains("totals"), "overview missing totals:\n{overview}");

    // Switch to Throughput — expect summary/bytes panels.
    state.current_tab = Tab::Throughput;
    let thr = render_on(&mut terminal, &state);
    assert!(thr.contains("summary"), "throughput missing summary:\n{thr}");
    assert!(thr.contains("bytes/s"), "throughput missing bytes/s:\n{thr}");
    // And must *not* still say "totals" (that was Overview-only).
    assert!(
        !thr.contains("── totals"),
        "leftover totals panel after tab switch"
    );
}

#[test]
fn status_pill_renders_green_when_healthy() {
    let mut state = fresh_state(Some(1_000.0));
    state.ingest(tick(1, 1_000, 1_000)); // 100% of target, no errors.
    let (_, content) = render_state(&state);
    // Pill glyph is ⬤.
    assert!(
        content.contains("⬤"),
        "missing status pill glyph:\n{content}"
    );
}
