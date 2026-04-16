//! A cheap, cloneable stop-flag.
//!
//! [`StopSignal`] is a shared boolean used by the dispatcher to tell every
//! worker "time's up, return your stats". Workers poll the flag between
//! iterations (or, in open-loop mode, between token pulls).
//!
//! # Why a bare `AtomicBool`?
//!
//! - Workers never *await* the stop signal — they check it in the hot
//!   path, where going through a `Notify` / channel wake-up would add
//!   an unnecessary syscall per iteration.
//! - The signal fires at most once per run, so contention is negligible.
//! - `Arc<AtomicBool>` is cheap to clone into every worker and the
//!   detached timer task.
//!
//! # Scheduling the trip
//!
//! [`StopSignal::after`] spawns a detached `compio::time::sleep` task
//! that flips the flag when the duration elapses. The task holds its
//! own clone of the `Arc`, so it keeps the flag alive even if every
//! worker has already returned.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Shared stop flag.
///
/// Clones share state: calling [`StopSignal::stop`] on any clone trips
/// every other clone.
#[derive(Clone, Debug)]
pub struct StopSignal {
    flag: Arc<AtomicBool>,
}

impl StopSignal {
    /// Construct a fresh signal in the "running" state.
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `true` once [`Self::stop`] has been called on this signal (or any
    /// clone of it).
    pub fn is_stopped(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    /// Trip the signal. Idempotent — safe to call multiple times.
    pub fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    /// Build a signal that automatically trips after `duration` has
    /// elapsed on the current compio runtime.
    ///
    /// The timer runs as a detached task; dropping every user-held clone
    /// of the signal does not cancel the timer, but the timer holds its
    /// own clone of the `Arc` so the flag stays live as long as the
    /// timer task exists. When the timer fires and drops its clone, the
    /// `Arc` deallocates with it — no leak.
    ///
    /// # Panics
    ///
    /// Must be called from inside a compio runtime (the detached sleep
    /// task needs one). Returns a plain signal either way; the timer
    /// future panics only if `compio::runtime::spawn` itself panics.
    pub fn after(duration: Duration) -> Self {
        let sig = Self::new();
        let timer_clone = sig.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(duration).await;
            timer_clone.stop();
        })
        .detach();
        sig
    }
}

impl Default for StopSignal {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_signal_is_not_stopped() {
        let s = StopSignal::new();
        assert!(!s.is_stopped());
    }

    #[test]
    fn stop_trips_flag() {
        let s = StopSignal::new();
        s.stop();
        assert!(s.is_stopped());
    }

    #[test]
    fn stop_on_clone_propagates() {
        let a = StopSignal::new();
        let b = a.clone();
        assert!(!a.is_stopped());
        assert!(!b.is_stopped());
        b.stop();
        assert!(a.is_stopped());
        assert!(b.is_stopped());
    }

    #[test]
    fn stop_is_idempotent() {
        let s = StopSignal::new();
        s.stop();
        s.stop();
        assert!(s.is_stopped());
    }

    #[compio::test]
    async fn after_trips_when_duration_elapses() {
        let s = StopSignal::after(Duration::from_millis(50));
        assert!(!s.is_stopped());
        compio::time::sleep(Duration::from_millis(150)).await;
        assert!(s.is_stopped());
    }

    #[compio::test]
    async fn after_does_not_trip_before_elapsed() {
        let s = StopSignal::after(Duration::from_secs(60));
        // Give the runtime a tick to schedule the detached task, then
        // verify we're still in the "running" state.
        compio::time::sleep(Duration::from_millis(5)).await;
        assert!(!s.is_stopped());
    }
}
