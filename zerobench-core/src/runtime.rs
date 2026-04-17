//! Runtime-agnostic sleep and spawn primitives.
//!
//! This module provides thin wrappers that delegate to the active async
//! runtime (compio or tokio) based on the compile-time feature flag.
//! Internal consumers (`dispatcher.rs`, `rate.rs`, `stop.rs`) call these
//! helpers instead of reaching for the runtime crate directly, so the
//! step-execution and scheduling logic compiles under either backend
//! without `cfg` sprinkled through business code.
//!
//! # Feature-flag precedence
//!
//! When both `runtime-compio` and `runtime-tokio` are enabled (e.g. due
//! to cargo feature unification in a workspace), compio takes precedence.
//! For a clean tokio-only build, no crate in the dependency graph should
//! activate `runtime-compio`.

use std::time::Duration;

/// Sleep for `d`, delegating to the active runtime.
pub async fn runtime_sleep(d: Duration) {
    #[cfg(feature = "runtime-compio")]
    {
        compio::time::sleep(d).await;
    }
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    {
        tokio::time::sleep(d).await;
    }
}

/// Sleep until the instant `deadline`, delegating to the active runtime.
///
/// If `deadline` is in the past, returns immediately.
pub async fn runtime_sleep_until(deadline: std::time::Instant) {
    #[cfg(feature = "runtime-compio")]
    {
        compio::time::sleep_until(deadline).await;
    }
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    }
}

/// Timeout wrapper: resolves to `Ok(val)` if `fut` completes within
/// `duration`, or `Err(())` on timeout.
pub async fn runtime_timeout<F: std::future::Future>(
    duration: Duration,
    fut: F,
) -> Result<F::Output, ()> {
    #[cfg(feature = "runtime-compio")]
    {
        compio::time::timeout(duration, fut).await.map_err(|_| ())
    }
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-compio")))]
    {
        tokio::time::timeout(duration, fut).await.map_err(|_| ())
    }
}
