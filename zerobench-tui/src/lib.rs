//! zerobench-tui — live terminal dashboard.
//!
//! Feeds from the same [`LiveSnapshot`] aggregator the JSONL streaming
//! output uses (see `zerobench-core::live_snapshot`). The dashboard
//! accumulates per-second ticks into a bounded ring buffer
//! ([`state::DashboardState`]) and redraws at 10 Hz via
//! [`ratatui`]. On exit the terminal is restored and the caller prints
//! the standard end-of-run terminal report — so users always get a
//! pastable summary even after using `--tui`.
//!
//! # Layout
//!
//! Tabbed, chart-rich dashboard modeled after btop / k9s / ctop. Four
//! tabs (Overview / Latency / Throughput / Errors) share a persistent
//! header (URL + status pill + transport info + elapsed) and footer
//! (keybind reminder). A `?` overlay shows the full keymap.
//!
//! # Concurrency
//!
//! The TUI runs inside the same compio runtime as the benchmark
//! workers. A single loop drives three things:
//!
//! - A 10 Hz render tick — redraws the ratatui terminal.
//! - A 1 Hz snapshot tick — swaps the `LiveSnapshot` bucket and
//!   ingests the resulting [`LiveTick`] into state.
//! - Non-blocking keyboard polling — see [`handle_key`].
//!
//! Coalescing everything into one loop (rather than spawning
//! independent tasks) keeps the `DashboardState` owned by a single
//! thread — no locking.
//!
//! # Terminal restoration
//!
//! [`ratatui::try_init`] installs its own panic hook that calls
//! `restore` before re-raising — so the user's terminal isn't left in
//! raw-mode/alt-screen on a panic. On a clean exit we call
//! [`ratatui::restore`] ourselves from the outer wrapper.

pub mod state;
pub mod ui;

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyModifiers};
use ratatui::DefaultTerminal;
use zerobench_core::{LiveSnapshot, StopSignal};

pub use state::{DashboardState, RunMode, Tab, TransportInfo};
pub use ui::render;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/// Render cadence — 10 Hz, matching the design spec.
const RENDER_INTERVAL: Duration = Duration::from_millis(100);

/// Snapshot cadence — once per second the TUI swaps the shared
/// `LiveSnapshot` bucket and folds the tick into `DashboardState`.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the live dashboard against `snapshot` until `stop` trips or the
/// user presses `q`.
///
/// Terminal is restored on every exit path, panic included.
///
/// `target_rate` should be `Some(rate)` for open-loop runs and `None`
/// for saturate. `total_duration` is used for the progress bar;
/// `url_label` is shown in the header. `transport` feeds the transport
/// info line (protocol · mode · TLS).
pub async fn run_tui(
    snapshot: Arc<LiveSnapshot>,
    stop: StopSignal,
    target_rate: Option<f64>,
    total_duration: Duration,
    url_label: String,
    transport: TransportInfo,
) -> io::Result<()> {
    // `ratatui::try_init` installs its own panic hook that invokes
    // `restore()` before the previous hook runs, so a mid-run panic
    // leaves the terminal usable without any extra plumbing here.
    let result = run_tui_inner(
        snapshot,
        stop,
        target_rate,
        total_duration,
        url_label,
        transport,
    )
    .await;

    // Clean-exit restore. Ratatui's own panic hook handles the crash
    // path.
    ratatui::restore();

    result
}

async fn run_tui_inner(
    snapshot: Arc<LiveSnapshot>,
    stop: StopSignal,
    target_rate: Option<f64>,
    total_duration: Duration,
    url_label: String,
    transport: TransportInfo,
) -> io::Result<()> {
    let mut terminal: DefaultTerminal = ratatui::try_init()?;
    let mut state =
        DashboardState::new(target_rate, total_duration, url_label, transport);

    // `next_snapshot_at` tracks the wall-clock instant at which we'll
    // swap the LiveSnapshot bucket. Using a clock-anchored deadline
    // (rather than a rolling "sleep 1s") keeps the ticks aligned with
    // the absolute second boundaries the worker-side live snapshot
    // uses.
    let start = Instant::now();
    let mut next_snapshot_at = start + SNAPSHOT_INTERVAL;
    let mut run_completed = false;

    loop {
        if state.exit_requested {
            // User pressed `q` — trip the shared stop flag so the
            // dispatcher also exits early (if still running).
            if !run_completed {
                stop.stop();
            }
            break;
        }

        // When the benchmark finishes, do a final ingest to capture
        // the last partial tick, mark the run as done, but DON'T
        // exit the TUI. The user can inspect charts at leisure and
        // press `q` when ready.
        if !run_completed && stop.is_stopped() {
            let tick = snapshot.swap_and_snapshot();
            state.ingest(tick);
            run_completed = true;
            state.run_completed = true;
        }

        // --- keyboard (non-blocking) ------------------------------
        if crossterm::event::poll(Duration::ZERO)? {
            if let Event::Key(key) = crossterm::event::read()? {
                handle_key(&mut state, key.code, key.modifiers);
            }
        }

        // --- snapshot ingest (only while running) -----------------
        if !run_completed {
            let now = Instant::now();
            while now >= next_snapshot_at {
                let tick = snapshot.swap_and_snapshot();
                state.ingest(tick);
                next_snapshot_at += SNAPSHOT_INTERVAL;
            }
        }

        // --- render -----------------------------------------------
        if !state.paused_rendering {
            terminal.draw(|f| render(f, &state))?;
        }

        // --- wait for next frame ----------------------------------
        let now = Instant::now();
        let next_frame = now + RENDER_INTERVAL;
        let wake_at = if run_completed {
            next_frame // no snapshot tick needed, just render cadence
        } else {
            next_frame.min(next_snapshot_at)
        };
        let wait = wake_at.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            compio::time::sleep(wait).await;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Keyboard handling — extracted so unit tests can exercise it.
// ---------------------------------------------------------------------------

/// Dispatch a key event against the dashboard state.
///
/// Kept here (not in `ui/`) because the keymap is part of the TUI's
/// public behaviour contract: changing a binding ripples into the help
/// overlay and the footer text and is easier to reason about in one
/// place.
pub(crate) fn handle_key(
    state: &mut DashboardState,
    code: KeyCode,
    mods: KeyModifiers,
) {
    // Help overlay eats most keys while visible — only `?`, `Esc`, or
    // `q` should close/quit.
    if state.help_visible {
        match code {
            KeyCode::Char('?') | KeyCode::Esc => {
                state.help_visible = false;
            }
            KeyCode::Char('q') | KeyCode::Char('Q') => {
                state.exit_requested = true;
            }
            _ => {}
        }
        return;
    }

    match code {
        // --- quit ---------------------------------------------------
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
            state.exit_requested = true;
        }

        // --- tab navigation ----------------------------------------
        KeyCode::Char('1') => state.current_tab = Tab::Overview,
        KeyCode::Char('2') => state.current_tab = Tab::Latency,
        KeyCode::Char('3') => state.current_tab = Tab::Throughput,
        KeyCode::Char('4') => state.current_tab = Tab::Errors,
        KeyCode::Tab => {
            state.current_tab = if mods.contains(KeyModifiers::SHIFT) {
                state.current_tab.prev()
            } else {
                state.current_tab.next()
            };
        }
        KeyCode::BackTab => {
            state.current_tab = state.current_tab.prev();
        }

        // --- overlay / toggles -------------------------------------
        KeyCode::Char('?') => {
            state.help_visible = true;
        }
        KeyCode::Char('p') | KeyCode::Char('P') => {
            state.paused_rendering = !state.paused_rendering;
        }
        KeyCode::Char('l') | KeyCode::Char('L') => {
            state.log_visible = !state.log_visible;
        }
        KeyCode::Char('r') | KeyCode::Char('R') => {
            state.reset_peaks();
        }

        // --- chart zoom / marker toggle ----------------------------
        KeyCode::Char('+') | KeyCode::Char('=') => {
            state.y_scale = (state.y_scale * 0.8).max(0.1);
        }
        KeyCode::Char('-') => {
            state.y_scale = (state.y_scale * 1.25).min(10.0);
        }
        KeyCode::Char('0') => {
            state.y_scale = 1.0;
        }
        KeyCode::Char('m') | KeyCode::Char('M') => {
            state.marker = match state.marker {
                ratatui::symbols::Marker::Braille => ratatui::symbols::Marker::Dot,
                _ => ratatui::symbols::Marker::Braille,
            };
        }

        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn ti() -> TransportInfo {
        TransportInfo::default()
    }

    fn state() -> DashboardState {
        DashboardState::new(None, Duration::from_secs(10), "x".into(), ti())
    }

    #[test]
    fn q_sets_exit_requested() {
        let mut s = state();
        assert!(!s.exit_requested);
        handle_key(&mut s, KeyCode::Char('q'), KeyModifiers::NONE);
        assert!(s.exit_requested);
    }

    #[test]
    fn p_toggles_pause() {
        let mut s = state();
        assert!(!s.paused_rendering);
        handle_key(&mut s, KeyCode::Char('p'), KeyModifiers::NONE);
        assert!(s.paused_rendering);
        handle_key(&mut s, KeyCode::Char('p'), KeyModifiers::NONE);
        assert!(!s.paused_rendering);
    }

    #[test]
    fn l_toggles_log() {
        let mut s = state();
        assert!(!s.log_visible);
        handle_key(&mut s, KeyCode::Char('l'), KeyModifiers::NONE);
        assert!(s.log_visible);
        handle_key(&mut s, KeyCode::Char('l'), KeyModifiers::NONE);
        assert!(!s.log_visible);
    }

    #[test]
    fn unrecognised_key_is_noop() {
        let mut s = state();
        handle_key(&mut s, KeyCode::Enter, KeyModifiers::NONE);
        assert!(!s.exit_requested);
        assert!(!s.paused_rendering);
        assert!(!s.log_visible);
    }

    #[test]
    fn digits_select_tabs() {
        let mut s = state();
        handle_key(&mut s, KeyCode::Char('2'), KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Latency);
        handle_key(&mut s, KeyCode::Char('3'), KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Throughput);
        handle_key(&mut s, KeyCode::Char('4'), KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Errors);
        handle_key(&mut s, KeyCode::Char('1'), KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Overview);
    }

    #[test]
    fn tab_cycles_forward() {
        let mut s = state();
        handle_key(&mut s, KeyCode::Tab, KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Latency);
        handle_key(&mut s, KeyCode::Tab, KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Throughput);
        handle_key(&mut s, KeyCode::Tab, KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Errors);
        handle_key(&mut s, KeyCode::Tab, KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Overview);
    }

    #[test]
    fn shift_tab_cycles_backward() {
        let mut s = state();
        handle_key(&mut s, KeyCode::Tab, KeyModifiers::SHIFT);
        assert_eq!(s.current_tab, Tab::Errors);
        handle_key(&mut s, KeyCode::BackTab, KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Throughput);
    }

    #[test]
    fn question_toggles_help() {
        let mut s = state();
        assert!(!s.help_visible);
        handle_key(&mut s, KeyCode::Char('?'), KeyModifiers::NONE);
        assert!(s.help_visible);
        // While visible, `?` closes it.
        handle_key(&mut s, KeyCode::Char('?'), KeyModifiers::NONE);
        assert!(!s.help_visible);
    }

    #[test]
    fn esc_closes_help_but_quits_when_not_visible() {
        let mut s = state();
        s.help_visible = true;
        handle_key(&mut s, KeyCode::Esc, KeyModifiers::NONE);
        assert!(!s.help_visible);
        assert!(!s.exit_requested);
        // From the main view, Esc quits.
        handle_key(&mut s, KeyCode::Esc, KeyModifiers::NONE);
        assert!(s.exit_requested);
    }

    #[test]
    fn r_resets_peaks() {
        let mut s = state();
        s.peak_rps = 1_000.0;
        s.min_rps = Some(100.0);
        handle_key(&mut s, KeyCode::Char('r'), KeyModifiers::NONE);
        assert_eq!(s.peak_rps, 0.0);
        assert!(s.min_rps.is_none());
    }

    #[test]
    fn help_blocks_tab_navigation() {
        // While help is visible, tab digits should be ignored — the
        // overlay eats the key so the user doesn't accidentally switch
        // panes behind the modal.
        let mut s = state();
        s.help_visible = true;
        handle_key(&mut s, KeyCode::Char('3'), KeyModifiers::NONE);
        assert_eq!(s.current_tab, Tab::Overview);
    }
}
