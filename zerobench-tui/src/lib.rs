//! zerobench-tui — live terminal dashboard.
//!
//! Feeds from the same `LiveSnapshot` aggregator the JSONL streaming output
//! uses. Latency panel uses a streaming t-digest for cheap live queries;
//! the HDR histogram remains the source of truth for the final report.
