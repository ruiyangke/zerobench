//! Shared histogram constants and helpers.
//!
//! Every backend that records latency samples into HDR histograms
//! (`zerobench-backends::sse`, `zerobench-backends::ws`, `mio_h1`,
//! `mio_h2`) uses the same bounds and precision so percentile math is
//! consistent across protocols.

use std::time::Duration;

use hdrhistogram::Histogram;

/// Minimum recordable value (1 nanosecond).
pub const HIST_LO_NS: u64 = 1;
/// Maximum recordable value (60 seconds in nanoseconds).
pub const HIST_HI_NS: u64 = 60_000_000_000;
/// Significant figures of precision.
pub const HIST_SIG: u8 = 3;

/// Construct a new histogram with the standard bounds.
pub fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO_NS, HIST_HI_NS, HIST_SIG)
        .expect("HDR bounds are valid compile-time constants")
}

/// Clamp a [`Duration`] into the histogram's recordable range and
/// return the value in nanoseconds.
pub fn duration_to_hist_ns(d: Duration) -> u64 {
    let ns = d.as_nanos();
    if ns < HIST_LO_NS as u128 {
        HIST_LO_NS
    } else if ns > HIST_HI_NS as u128 {
        HIST_HI_NS
    } else {
        ns as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_hist_creates_valid_histogram() {
        let h = new_hist();
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn duration_to_hist_ns_clamps_low() {
        assert_eq!(duration_to_hist_ns(Duration::from_nanos(0)), HIST_LO_NS);
    }

    #[test]
    fn duration_to_hist_ns_clamps_high() {
        let huge = Duration::from_secs(999);
        assert_eq!(duration_to_hist_ns(huge), HIST_HI_NS);
    }

    #[test]
    fn duration_to_hist_ns_passes_through() {
        assert_eq!(duration_to_hist_ns(Duration::from_micros(500)), 500_000);
    }
}
