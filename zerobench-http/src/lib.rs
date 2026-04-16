//! zerobench-http — HTTP/1 / HTTP/2 / HTTP/3 transport.
//!
//! Built directly on `hyper` (via `cyper-core`'s compio↔hyper IO bridge),
//! not on the high-level `cyper` client. Owning the connection lifecycle
//! lets us pre-open pools, measure TTFB, and count wire bytes exactly.

pub mod conn;
pub mod counting_stream;
#[cfg(feature = "h1")]
pub mod h1;
#[cfg(feature = "h1")]
mod transport_impl;

pub use conn::{Connected, open};
pub use counting_stream::CountingStream;

#[cfg(feature = "h1")]
pub use h1::Http1Pool;
#[cfg(feature = "h1")]
pub use transport_impl::HttpTransport;
