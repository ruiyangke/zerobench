//! zerobench-http — HTTP/1 / HTTP/2 / HTTP/3 transport.
//!
//! Built directly on `hyper` (via `cyper-core`'s compio↔hyper IO bridge),
//! not on the high-level `cyper` client. Owning the connection lifecycle
//! lets us pre-open pools, measure TTFB, and count wire bytes exactly.
//!
//! # Runtime backends
//!
//! - `runtime-compio` (default): compio IO + `cyper-core::HyperStream` bridge.
//! - `runtime-tokio`: native tokio + hyper (no bridge overhead).

// When both runtime features are enabled (e.g. due to cargo feature
// unification), compio takes precedence. The Transport impl dispatches
// based on which runtime is actually active at the call site.

// --- Compio backend modules ---
#[cfg(feature = "runtime-compio")]
pub mod conn;
#[cfg(feature = "runtime-compio")]
pub mod counting_stream;
#[cfg(all(feature = "h1", feature = "runtime-compio"))]
pub mod h1;
#[cfg(all(feature = "h2", feature = "runtime-compio"))]
pub mod h2;

// --- Tokio backend modules ---
#[cfg(feature = "runtime-tokio")]
pub mod conn_tokio;
#[cfg(all(feature = "h1", feature = "runtime-tokio"))]
pub mod h1_tokio;

// --- Raw H1 backend (opt-in, no hyper) ---
#[cfg(all(feature = "raw-h1", feature = "runtime-compio"))]
#[allow(unsafe_code)]
pub mod raw_h1;

// --- Transport dispatch (works on either backend) ---
#[cfg(feature = "h1")]
mod transport_impl;

// --- Re-exports: compio backend ---
#[cfg(feature = "runtime-compio")]
pub use conn::{Connected, open};
#[cfg(feature = "runtime-compio")]
pub use counting_stream::CountingStream;
#[cfg(all(feature = "h1", feature = "runtime-compio"))]
pub use h1::{Http1Pool, StreamingResponse};
#[cfg(all(feature = "h2", feature = "runtime-compio"))]
pub use h2::Http2Client;

// --- Re-exports: tokio backend ---
#[cfg(all(feature = "h1", feature = "runtime-tokio"))]
pub use h1_tokio::Http1PoolTokio;

// --- Re-exports: raw H1 backend ---
#[cfg(all(feature = "raw-h1", feature = "runtime-compio"))]
pub use raw_h1::{RawH1Handle, RawH1Pool};

// --- Re-exports: transport dispatch ---
#[cfg(feature = "h1")]
pub use transport_impl::HttpTransport;
#[cfg(all(feature = "h1", feature = "runtime-compio"))]
pub use transport_impl::HttpClient;
#[cfg(all(feature = "raw-h1", feature = "runtime-compio"))]
pub use transport_impl::RawH1Transport;
