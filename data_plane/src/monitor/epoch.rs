//! Tumbling-epoch alignment. The coordinator's monitoring epoch must use the
//! IDENTICAL formula the edge runtime uses for its window start
//! (`(t / window) * window`, see asap-precompute-go window.go `initWindow`) so
//! both sides agree on the epoch id without negotiation.

/// Align a timestamp (ms) down to its tumbling-window start for the given
/// window size (ms). A zero window size yields 0 (treated as "unwindowed").
#[inline]
pub fn align(t_ms: u64, window_ms: u64) -> u64 {
    if window_ms == 0 {
        0
    } else {
        (t_ms / window_ms) * window_ms
    }
}

#[cfg(test)]
mod tests {
    use super::align;

    #[test]
    fn aligns_to_window_start() {
        assert_eq!(align(0, 60_000), 0);
        assert_eq!(align(59_999, 60_000), 0);
        assert_eq!(align(60_000, 60_000), 60_000);
        assert_eq!(align(125_000, 60_000), 120_000);
    }

    #[test]
    fn matches_go_formula() {
        // Mirrors Go (refMs / size) * size for a representative timestamp.
        let t = 1_700_000_123_456u64;
        let w = 60_000u64;
        assert_eq!(align(t, w), (t / w) * w);
    }
}
