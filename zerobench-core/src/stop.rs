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
    /// elapsed on the current async runtime.
    ///
    /// On compio: spawns a detached compio timer task.
    /// On tokio: spawns a tokio timer task.
    ///
    /// # Panics
    ///
    /// Must be called from inside the active async runtime.
    #[cfg(feature = "runtime-compio")]
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

    /// Build a signal that automatically trips after `duration` has
    /// elapsed on the current tokio runtime.
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    pub fn after(duration: Duration) -> Self {
        let sig = Self::new();
        let timer_clone = sig.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            timer_clone.stop();
        });
        sig
    }

    /// Build a signal that trips after `duration` using a plain OS
    /// thread and `std::thread::sleep`. Unlike [`Self::after`], this
    /// does **not** require a compio runtime to be running — the timer
    /// fires even when the calling thread is blocked on
    /// `std::thread::JoinHandle::join`.
    ///
    /// Preferred over [`Self::after`] in multi-threaded dispatch where
    /// the main thread blocks on worker thread joins and cannot poll
    /// the compio reactor.
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

    #[cfg(feature = "runtime-compio")]
    #[compio::test]
    async fn after_trips_when_duration_elapses() {
        let s = StopSignal::after(Duration::from_millis(50));
        assert!(!s.is_stopped());
        compio::time::sleep(Duration::from_millis(150)).await;
        assert!(s.is_stopped());
    }

    #[cfg(feature = "runtime-compio")]
    #[compio::test]
    async fn after_does_not_trip_before_elapsed() {
        let s = StopSignal::after(Duration::from_secs(60));
        // Give the runtime a tick to schedule the detached task, then
        // verify we're still in the "running" state.
        compio::time::sleep(Duration::from_millis(5)).await;
        assert!(!s.is_stopped());
    }

    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    #[tokio::test]
    async fn after_trips_when_duration_elapses_tokio() {
        let s = StopSignal::after(Duration::from_millis(50));
        assert!(!s.is_stopped());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(s.is_stopped());
    }

    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    #[tokio::test]
    async fn after_does_not_trip_before_elapsed_tokio() {
        let s = StopSignal::after(Duration::from_secs(60));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!s.is_stopped());
    }

    #[test]
    fn after_wall_trips_when_duration_elapses() {
        let s = StopSignal::after_wall(Duration::from_millis(50));
        assert!(!s.is_stopped());
        // Block the calling thread — no compio runtime needed.
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
