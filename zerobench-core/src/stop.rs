//! A cheap, cloneable stop-flag.
//!
//! [`StopSignal`] is a shared boolean used by the dispatcher to tell every
//! worker "time's up, return your stats". Workers poll the flag between
//! iterations.
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
//! [`StopSignal::after_wall`] spawns a plain OS thread that sleeps
//! for the requested duration, then flips the flag. This works
//! regardless of whether the calling thread is blocked on joins.

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

    /// Borrow the inner `AtomicBool` so callers that need a raw flag
    /// (e.g. mio worker loops) can share the same stop signal without
    /// pulling in the `StopSignal` type.
    pub fn flag(&self) -> &Arc<AtomicBool> {
        &self.flag
    }

    /// Build a signal that trips after `duration` using a plain OS
    /// thread and `std::thread::sleep`. The timer fires even when the
    /// calling thread is blocked on `std::thread::JoinHandle::join`.
    pub fn after_wall(duration: Duration) -> Self {
        let sig = Self::new();
        let timer_clone = sig.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            timer_clone.stop();
        });
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

    #[test]
    fn after_wall_trips_when_duration_elapses() {
        let s = StopSignal::after_wall(Duration::from_millis(50));
        assert!(!s.is_stopped());
        // Block the calling thread.
        std::thread::sleep(Duration::from_millis(150));
        assert!(s.is_stopped());
    }

    #[test]
    fn after_wall_does_not_trip_before_elapsed() {
        let s = StopSignal::after_wall(Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!s.is_stopped());
    }
}
