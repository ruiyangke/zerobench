//! Render snapshot test — renders the TUI into a fixed-size
//! `TestBackend` and asserts that the resulting buffer contains the
//! high-signal substrings (URL, target rate, percentile labels, etc.).
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
use zerobench_tui::state::DashboardState;
use zerobench_tui::ui::render;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn renders_header_with_url_label() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(
        Some(10_000.0),
        Duration::from_secs(30),
        "http://api.example.com".into(),
    );
    state.ingest(tick(1, 9_994, 120_000));

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
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
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(
        Some(10_000.0),
        Duration::from_secs(30),
        "http://api".into(),
    );
    state.ingest(tick(1, 9_994, 120_000));

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
    // target line — either "10.0k req/s" or "10,000 req/s" is fine.
    assert!(
        content.contains("target"),
        "missing target row:\n{content}"
    );
    assert!(
        content.contains("10.0k req/s") || content.contains("10000 req/s"),
        "missing target rate value:\n{content}"
    );
    assert!(
        content.contains("actual"),
        "missing actual row:\n{content}"
    );
    // actual rate — should show ~99.94% of 10k.
    assert!(
        content.contains("99.94%") || content.contains("99.9%"),
        "missing actual-vs-target percent:\n{content}"
    );
}

#[test]
fn renders_latency_panel_with_percentile_labels() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(
        Some(10_000.0),
        Duration::from_secs(30),
        "http://api".into(),
    );
    state.ingest(tick(1, 1_000, 120_000)); // 120µs

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
    // All four percentile labels should be visible.
    assert!(content.contains("p50"), "missing p50 label:\n{content}");
    assert!(content.contains("p90"), "missing p90 label:\n{content}");
    assert!(content.contains("p99"), "missing p99 label:\n{content}");
    assert!(
        content.contains("p99.9"),
        "missing p99.9 label:\n{content}"
    );
    assert!(content.contains("max"), "missing max label:\n{content}");
    assert!(
        content.contains("120µs"),
        "missing 120µs value:\n{content}"
    );
}

#[test]
fn renders_errors_panel() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(None, Duration::from_secs(30), "http://x".into());
    state.ingest(tick(1, 100, 1_000_000));

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
    assert!(content.contains("errors"), "missing errors heading:\n{content}");
    assert!(content.contains("connect"), "missing connect label:\n{content}");
    assert!(content.contains("timeout"), "missing timeout label:\n{content}");
    assert!(content.contains("keepup"), "missing keepup label:\n{content}");
    assert!(content.contains("4xx/5xx"), "missing 4xx/5xx label:\n{content}");
}

#[test]
fn renders_keybind_footer() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let state = DashboardState::new(None, Duration::from_secs(30), "http://x".into());

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
    assert!(content.contains("[q] quit"), "missing quit keybind:\n{content}");
    assert!(content.contains("[p] pause"), "missing pause keybind:\n{content}");
    assert!(content.contains("[l] toggle"), "missing log keybind:\n{content}");
}

#[test]
fn renders_waiting_hint_before_first_tick() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let state = DashboardState::new(
        Some(10_000.0),
        Duration::from_secs(30),
        "http://x".into(),
    );

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
    // No ticks yet — the sparkline block should show its placeholder.
    assert!(
        content.contains("waiting for first tick"),
        "missing placeholder hint:\n{content}"
    );
    assert!(
        content.contains("no samples"),
        "missing latency-empty hint:\n{content}"
    );
}

#[test]
fn renders_small_terminal_without_panicking() {
    // Verify we tolerate a too-small terminal — ratatui will clip,
    // but we must not panic.
    let backend = TestBackend::new(40, 10);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(None, Duration::from_secs(10), "x".into());
    state.ingest(tick(1, 10, 1_000));

    terminal.draw(|f| render(f, &state)).unwrap();
}

#[test]
fn pause_flag_does_not_break_rendering() {
    // Even though the TUI loop skips `terminal.draw` when paused,
    // calling `render` directly with paused state must still work
    // and produce valid output.
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(None, Duration::from_secs(10), "http://x".into());
    state.paused_rendering = true;
    state.ingest(tick(1, 100, 1_000));

    terminal.draw(|f| render(f, &state)).unwrap();

    let content = buffer_to_string(terminal.backend().buffer());
    assert!(content.contains("zerobench"));
    // Footer notes paused state.
    assert!(content.contains("PAUSED"), "missing pause indicator:\n{content}");
}

#[test]
fn log_pane_toggle_renders_extra_block() {
    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut state = DashboardState::new(None, Duration::from_secs(10), "http://x".into());
    state.ingest(tick(1, 10, 1_000));

    // Without log: footer shouldn't contain "[log]" marker.
    terminal.draw(|f| render(f, &state)).unwrap();
    let plain = buffer_to_string(terminal.backend().buffer());
    assert!(!plain.contains("press 'l' to hide"));

    state.log_visible = true;
    terminal.draw(|f| render(f, &state)).unwrap();
    let with_log = buffer_to_string(terminal.backend().buffer());
    assert!(
        with_log.contains("press 'l' to hide"),
        "log pane missing its stub content:\n{with_log}"
    );
}
