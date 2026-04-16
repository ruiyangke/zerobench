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
//! # Concurrency
//!
//! The TUI runs inside the same compio runtime as the benchmark
//! workers. A single loop drives three things:
//!
//! - A 10 Hz render tick — redraws the ratatui terminal.
//! - A 1 Hz snapshot tick — swaps the `LiveSnapshot` bucket and
//!   ingests the resulting [`LiveTick`] into state.
//! - Non-blocking keyboard polling — `q` / `p` / `l`.
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

use crossterm::event::{Event, KeyCode};
use ratatui::DefaultTerminal;
use zerobench_core::{LiveSnapshot, StopSignal};

pub use state::DashboardState;
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
/// `url_label` is shown in the header.
pub async fn run_tui(
    snapshot: Arc<LiveSnapshot>,
    stop: StopSignal,
    target_rate: Option<f64>,
    total_duration: Duration,
    url_label: String,
) -> io::Result<()> {
    // `ratatui::try_init` installs its own panic hook that invokes
    // `restore()` before the previous hook runs, so a mid-run panic
    // leaves the terminal usable without any extra plumbing here.
    let result = run_tui_inner(snapshot, stop, target_rate, total_duration, url_label).await;

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
) -> io::Result<()> {
    let mut terminal: DefaultTerminal = ratatui::try_init()?;
    let mut state = DashboardState::new(target_rate, total_duration, url_label);

    // `next_snapshot_at` tracks the wall-clock instant at which we'll
    // swap the LiveSnapshot bucket. Using a clock-anchored deadline
    // (rather than a rolling "sleep 1s") keeps the ticks aligned with
    // the absolute second boundaries the worker-side live snapshot
    // uses.
    let start = Instant::now();
    let mut next_snapshot_at = start + SNAPSHOT_INTERVAL;

    loop {
        if stop.is_stopped() {
            break;
        }
        if state.exit_requested {
            // User pressed `q` — trip the shared stop flag so the
            // dispatcher also exits early; the summary records the
            // actual (shorter) duration rather than the plan's.
            stop.stop();
            break;
        }

        // --- keyboard (non-blocking) ------------------------------
        //
        // `crossterm::event::poll(ZERO)` returns immediately whether
        // or not an event is pending. If one is, `read()` is
        // guaranteed not to block.
        if crossterm::event::poll(Duration::ZERO)? {
            if let Event::Key(key) = crossterm::event::read()? {
                handle_key(&mut state, key.code);
            }
        }

        // --- snapshot ingest --------------------------------------
        //
        // One swap per SNAPSHOT_INTERVAL. Catch up more than one
        // bucket in the pathological case (e.g. the render loop
        // stalled for 3s). This keeps the ring aligned with real
        // time.
        let now = Instant::now();
        while now >= next_snapshot_at {
            let tick = snapshot.swap_and_snapshot();
            state.ingest(tick);
            next_snapshot_at += SNAPSHOT_INTERVAL;
        }

        // --- render -----------------------------------------------
        if !state.paused_rendering {
            terminal.draw(|f| render(f, &state))?;
        }

        // --- wait for next frame or snapshot, whichever comes first -
        let next_frame = now + RENDER_INTERVAL;
        let wake_at = next_frame.min(next_snapshot_at);
        let wait = wake_at.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            compio::time::sleep(wait).await;
        }
    }

    // No final drain: `LiveSnapshot` is only for live display. The
    // authoritative counters live in `TaskStats` via the dispatcher
    // path; the outer caller prints the standard terminal report from
    // those.
    Ok(())
}

// ---------------------------------------------------------------------------
// Keyboard handling — extracted so unit tests can exercise it.
// ---------------------------------------------------------------------------

fn handle_key(state: &mut DashboardState, code: KeyCode) {
    match code {
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
            state.exit_requested = true;
        }
        KeyCode::Char('p') | KeyCode::Char('P') => {
            state.paused_rendering = !state.paused_rendering;
        }
        KeyCode::Char('l') | KeyCode::Char('L') => {
            state.log_visible = !state.log_visible;
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

    #[test]
    fn q_sets_exit_requested() {
        let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
        assert!(!s.exit_requested);
        handle_key(&mut s, KeyCode::Char('q'));
        assert!(s.exit_requested);
    }

    #[test]
    fn p_toggles_pause() {
        let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
        assert!(!s.paused_rendering);
        handle_key(&mut s, KeyCode::Char('p'));
        assert!(s.paused_rendering);
        handle_key(&mut s, KeyCode::Char('p'));
        assert!(!s.paused_rendering);
    }

    #[test]
    fn l_toggles_log() {
        let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
        assert!(!s.log_visible);
        handle_key(&mut s, KeyCode::Char('l'));
        assert!(s.log_visible);
        handle_key(&mut s, KeyCode::Char('l'));
        assert!(!s.log_visible);
    }

    #[test]
    fn unrecognised_key_is_noop() {
        let mut s = DashboardState::new(None, Duration::from_secs(10), "x".into());
        handle_key(&mut s, KeyCode::Enter);
        assert!(!s.exit_requested);
        assert!(!s.paused_rendering);
        assert!(!s.log_visible);
    }
}
