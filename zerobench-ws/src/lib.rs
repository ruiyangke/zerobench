//! ARCH STATUS: DELETE — contents merge into zerobench-backends::ws
//!
//! zerobench-ws goes away as a crate. Modules (conn, frame, handshake,
//! echo_rtt, hold, server_push_rtt, fanout) become submodules of
//! zerobench-backends::ws.
//! See docs/ARCH-REVIEW-2026-04-20.md §4.1, §7.
//!
//! ----------------------------------------------------------------------
//!
//! zerobench-ws — RFC 6455 WebSocket benchmarking runner.
//!
//! Protocol-native WS workload per `docs/PHILOSOPHY.md` §4.4:
//! N persistent connections, echo-RTT with monotonic-id correlation,
//! RTT histogram as the primary latency axis.
//!
//! # Modules
//!
//! - [`echo_rtt`] — `WsEchoRtt` backend.
//! - [`frame`] — RFC 6455 §5.2 wire-format codec.
//! - [`handshake`] — HTTP/1.1 Upgrade exchange + Sec-WebSocket-Accept
//!   validation.
//! - [`conn`] — one established connection (MioStream + recv buffer +
//!   per-connection mask CSPRNG).

pub mod conn;
pub mod echo_rtt;
pub mod fanout;
pub mod frame;
pub mod handshake;
pub mod hold;
pub mod server_push_rtt;

pub use echo_rtt::run_ws_echo_rtt_from_plan_threaded;
pub use fanout::run_ws_fanout_from_plan_threaded;
pub use hold::run_ws_hold_from_plan_threaded;
pub use server_push_rtt::run_ws_server_push_rtt_from_plan_threaded;
