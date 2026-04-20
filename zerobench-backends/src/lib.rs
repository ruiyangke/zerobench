//! zerobench-backends — every protocol backend in one crate.
//!
//! Submodules:
//!   - [`http`] — raw HTTP/1 + HTTP/2 + cold-connect
//!   - [`sse`]  — Server-Sent Events (hold / fanout / reconnect-storm)
//!   - [`ws`]   — WebSocket (echo / hold / fanout / server-push)
//!
//! Consolidates the former `zerobench-http`, `zerobench-sse`, and
//! `zerobench-ws` crates. Feature flags (`mio-h1`, `mio-h2`) are gone —
//! every module is always-on.
//!
//! See `docs/ARCH-REVIEW-2026-04-20.md` §4.1, §7.

pub mod http;
pub mod sse;
pub mod ws;
