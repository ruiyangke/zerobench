//! ARCH STATUS: DELETE — contents merge into zerobench-backends::http
//!
//! zerobench-http goes away as a crate. Its modules (cold_connect, mio_h1,
//! mio_h2, mio_tls, raw_h1_common, simple_post) become submodules under
//! zerobench-backends::http. All `#[cfg(feature = "mio-h1")]` /
//! `#[cfg(feature = "mio-h2")]` gates disappear — no feature flags in the
//! target.
//! See docs/ARCH-REVIEW-2026-04-20.md §4.1, §7.
//!
//! ----------------------------------------------------------------------
//!
//! zerobench-http — HTTP/1 and HTTP/2 transports (mio/epoll, zero async).

// ARCH(feature-delete): all `#[cfg(feature = "mio-h1")]` +
// `#[cfg(feature = "mio-h2")]` gates vanish post-move. Every module is
// always-on in zerobench-backends. See ARCH-REVIEW §4, Q4.

// --- Shared H1 request/response helpers ---
#[cfg(feature = "mio-h1")]
pub mod raw_h1_common;

// --- Mio TLS wrapper (shared by mio-h1 and mio-h2) ---
#[cfg(feature = "mio-h1")]
pub mod mio_tls;

// --- Mio H1 backend (synchronous epoll, no async runtime) ---
#[cfg(feature = "mio-h1")]
pub mod mio_h1;

// --- Cold-connect backend (fresh conn per op — HttpColdConnect step) ---
#[cfg(feature = "mio-h1")]
pub mod cold_connect;

// --- One-shot POST (fanout triggers, control-plane probes) ---
#[cfg(feature = "mio-h1")]
pub mod simple_post;

// --- Mio H2 backend (h2 crate manually polled from mio) ---
#[cfg(feature = "mio-h2")]
pub mod mio_h2;
