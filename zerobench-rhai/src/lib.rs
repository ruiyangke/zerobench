//! zerobench-rhai — Rhai scripting front-end.
//!
//! Rhai is **only** used at compile time to construct a `Plan`. The engine
//! is dropped before execution begins. No per-request interpreter calls
//! except via explicit `on_response` hooks.
