//! zerobench-http — HTTP/1 / HTTP/2 / HTTP/3 transport.
//!
//! Built directly on `hyper` (via `cyper-core`'s compio↔hyper IO bridge),
//! not on the high-level `cyper` client. Owning the connection lifecycle
//! lets us pre-open pools, measure TTFB, and count wire bytes exactly.
