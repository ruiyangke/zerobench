//! zerobench-sse — Server-Sent Events transport.
//!
//! Rides `zerobench-http` for the underlying HTTP/1 or HTTP/2 connection,
//! adds SSE line framing and chunk-latency recording on top.
