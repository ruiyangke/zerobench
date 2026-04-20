//! HTTP/1 and HTTP/2 transports (mio/epoll, zero async).

// --- Shared H1 request/response helpers ---
pub mod raw_h1_common;

// --- Mio TLS wrapper (shared by mio-h1 and mio-h2) ---
pub mod mio_tls;

// --- Mio H1 backend (synchronous epoll, no async runtime) ---
pub mod mio_h1;

// --- Cold-connect backend (fresh conn per op — HttpColdConnect step) ---
pub mod cold_connect;

// --- One-shot POST (fanout triggers, control-plane probes) ---
pub mod simple_post;

// --- Mio H2 backend (h2 crate manually polled from mio) ---
pub mod mio_h2;
