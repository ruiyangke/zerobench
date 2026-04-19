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
pub mod frame;
pub mod handshake;
pub mod hold;
pub mod server_push_rtt;

pub use echo_rtt::run_ws_echo_rtt_from_plan_threaded;
pub use hold::run_ws_hold_from_plan_threaded;
pub use server_push_rtt::run_ws_server_push_rtt_from_plan_threaded;
