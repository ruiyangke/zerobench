//! zerobench sse — Server-Sent Events benchmarking runner.
//!
//! Protocol-native SSE workload per `docs/PHILOSOPHY.md` §4.3:
//! N persistent subscribers held for a bounded duration, event-is-the-op
//! accounting, inter-event gap as the primary latency axis.
//!
//! # Modules
//!
//! - [`hold`] — `SseHold` backend.
//! - [`line_parser`] — WHATWG EventSource line framer.

pub mod fanout;
pub mod hold;
pub mod line_parser;
pub mod reconnect_storm;

pub use fanout::run_sse_fanout_from_plan_threaded;
pub use hold::run_sse_hold_from_plan_threaded;
pub use line_parser::{SseEvent, SseLineParser};
pub use reconnect_storm::run_sse_reconnect_storm_from_plan_threaded;
