//! Exact per-series counter-window state for Prometheus rate/increase readout.
//!
//! Reset corrections use an interval tree, not subtraction of floating global
//! prefixes: unrelated large historical resets must not erase a small reset in
//! the requested window. Timestamps remain exact; callers bind offsets before
//! passing the original (start, end] evaluation interval.
#[derive(Debug)]
pub(crate) struct RangeCounterIndex {
    points: std::sync::Arc<Vec<(i64, f64)>>,
    resets: Vec<Option<ResetSum>>,
    reset_positions: Vec<usize>,
    width: usize,
}

#[derive(Debug, Clone, Copy)]
struct ResetSum {
    sum: f64,
    absolute_sum: f64,
    count: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CounterIndexError;
impl std::fmt::Display for CounterIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counter reset aggregation is numerically unsafe; Prometheus exact subtree execution required")
    }
}
impl std::error::Error for CounterIndexError {}
fn merge(left: Option<ResetSum>, right: Option<ResetSum>) -> Option<ResetSum> {
    match (left, right) {
        (None, value) | (value, None) => value,
        (Some(left), Some(right)) => Some(ResetSum {
            sum: left.sum + right.sum,
            absolute_sum: left.absolute_sum + right.absolute_sum,
            count: left.count + right.count,
        }),
    }
}

impl RangeCounterIndex {
    /// Input comes from one validated retained index series: timestamps must
    /// already be strictly increasing and duplicate conflicts resolved there.
    pub(crate) fn new(points: impl IntoIterator<Item = (i64, Option<f64>)>) -> Self {
        let points: Vec<_> = points
            .into_iter()
            .filter_map(|(timestamp, value)| {
                value
                    .filter(|v| v.to_bits() != 0x7ff0000000000002)
                    .map(|v| (timestamp, v))
            })
            .collect();
        Self::from_shared_points(std::sync::Arc::new(points))
    }
    pub(crate) fn from_shared_points(points: std::sync::Arc<Vec<(i64, f64)>>) -> Self {
        assert!(
            points.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "counter index requires validated increasing series timestamps"
        );
        let reset_positions: Vec<_> = (1..points.len())
            .filter(|i| points[*i].1 < points[*i - 1].1)
            .collect();
        let width = reset_positions.len().max(1).next_power_of_two();
        let mut resets = vec![None; width * 2];
        for (slot, &position) in reset_positions.iter().enumerate() {
            let value = points[position - 1].1;
            resets[width + slot] = Some(ResetSum {
                sum: value,
                absolute_sum: value.abs(),
                count: 1,
            });
        }
        for i in (1..width).rev() {
            resets[i] = merge(resets[i * 2], resets[i * 2 + 1]);
        }
        Self {
            points,
            resets,
            reset_positions,
            width,
        }
    }

    fn correction(&self, start: usize, end: usize) -> Option<ResetSum> {
        let start = self
            .reset_positions
            .partition_point(|position| *position < start);
        let end = self
            .reset_positions
            .partition_point(|position| *position < end);
        let (mut lo, mut hi) = (start + self.width, end + self.width);
        let (mut left, mut right) = (None, None);
        while lo < hi {
            if lo % 2 == 1 {
                left = merge(left, self.resets[lo]);
                lo += 1;
            }
            if hi % 2 == 1 {
                hi -= 1;
                right = merge(self.resets[hi], right);
            }
            lo /= 2;
            hi /= 2;
        }
        merge(left, right)
    }

    /// None means fewer than two usable samples (or an invalid time interval),
    /// while Some(NaN) preserves Prometheus's distinct numeric NaN result.
    pub(crate) fn rate(
        &self,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Option<f64>, CounterIndexError> {
        if start_ms >= end_ms {
            return Ok(None);
        }
        let lo = self.points.partition_point(|point| point.0 <= start_ms);
        let hi = self.points.partition_point(|point| point.0 <= end_ms);
        if hi - lo < 2 {
            return Ok(None);
        }
        let (first_time, first) = self.points[lo];
        let (last_time, last) = self.points[hi - 1];
        let seconds =
            |later: i64, earlier: i64| (i128::from(later) - i128::from(earlier)) as f64 / 1000.0;
        let span = seconds(last_time, first_time);
        let mut delta = last - first;
        if first.is_nan() || last.is_nan() {
            return Ok(Some(f64::NAN));
        }
        if !delta.is_finite() {
            return Err(CounterIndexError);
        }
        // The edge into the first selected sample is outside this range.
        if let Some(correction) = self.correction(lo + 1, hi) {
            let base = delta;
            delta += correction.sum;
            if !delta.is_finite() {
                return Err(CounterIndexError);
            }
            if correction.count > 1 {
                // Ordered float addition is not associative. Bound both the
                // reference sequential additions and the tree reassociation;
                // do not advertise a numerically ill-conditioned value as exact.
                let bound = 4.0
                    * (correction.count as f64 + 2.0)
                    * f64::EPSILON
                    * (base.abs() + correction.absolute_sum);
                if bound > 1e-12 * delta.abs().max(f64::MIN_POSITIVE) {
                    return Err(CounterIndexError);
                }
            }
        }
        let average = span / (hi - lo - 1) as f64;
        let mut to_start = seconds(first_time, start_ms);
        let mut to_end = seconds(end_ms, last_time);
        if to_start >= average * 1.1 {
            to_start = average / 2.0;
        }
        // Sparse-boundary capping precedes the counter's extrapolation-to-zero bound.
        if delta > 0.0 && first >= 0.0 {
            to_start = to_start.min(span * first / delta);
        }
        if to_end >= average * 1.1 {
            to_end = average / 2.0;
        }
        Ok(Some(
            delta * (span + to_start + to_end) / span / seconds(end_ms, start_ms),
        ))
    }

    pub(crate) fn increase(
        &self,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Option<f64>, CounterIndexError> {
        self.rate(start_ms, end_ms).map(|value| {
            value.map(|rate| rate * ((i128::from(end_ms) - i128::from(start_ms)) as f64 / 1000.0))
        })
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.resets.capacity() * std::mem::size_of::<Option<ResetSum>>()
            + self.reset_positions.capacity() * std::mem::size_of::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Reference scans the selected raw samples and applies corrections in their
    // timestamp order, matching logical_dag::rate rather than querying the tree.
    fn raw_rate(input: &[(i64, Option<f64>)], start: i64, end: i64) -> Option<f64> {
        let points: Vec<_> = input
            .iter()
            .filter_map(|(t, v)| {
                v.filter(|v| v.to_bits() != 0x7ff0000000000002)
                    .filter(|_| *t > start && *t <= end)
                    .map(|v| (*t, v))
            })
            .collect();
        if points.len() < 2 || start >= end {
            return None;
        }
        let (first_t, first) = points[0];
        let (last_t, last) = *points.last().unwrap();
        let span = (last_t - first_t) as f64 / 1000.0;
        if span <= 0.0 {
            return None;
        }
        let mut delta = last - first;
        for pair in points.windows(2) {
            if pair[1].1 < pair[0].1 {
                delta += pair[0].1;
            }
        }
        let average = span / (points.len() - 1) as f64;
        let mut to_start = (first_t - start) as f64 / 1000.0;
        let mut to_end = (end - last_t) as f64 / 1000.0;
        if to_start >= average * 1.1 {
            to_start = average / 2.0;
        }
        if delta > 0.0 && first >= 0.0 {
            to_start = to_start.min(span * first / delta);
        }
        if to_end >= average * 1.1 {
            to_end = average / 2.0;
        }
        Some(delta * (span + to_start + to_end) / span / ((end - start) as f64 / 1000.0))
    }
    fn assert_same(actual: Option<f64>, expected: Option<f64>) {
        match (actual, expected) {
            (None, None) => {}
            (Some(a), Some(b)) if a.is_nan() && b.is_nan() => {}
            (Some(a), Some(b)) if a == b => {}
            (Some(a), Some(b)) => assert!((a - b).abs() <= 1e-12 * b.abs().max(1.0), "{a} != {b}"),
            pair => panic!("different absence semantics: {pair:?}"),
        }
    }
    // Events exactly at the left boundary and resets before it cannot contribute.
    #[test]
    fn excludes_left_boundary_and_unrelated_large_historical_reset() {
        let points = [
            (0, Some(1e30)),
            (1000, Some(0.)),
            (2000, Some(8.)),
            (3000, Some(2.)),
            (4000, Some(4.)),
        ];
        let index = RangeCounterIndex::new(points);
        assert_same(
            index.rate(1000, 4000).unwrap(),
            raw_rate(&points, 1000, 4000),
        );
        assert_eq!(index.increase(1999, 4000).unwrap(), Some(4.002));
        assert!(index.estimated_bytes() > 0);
    }
    // Missing/stale samples are omitted, genuine NaN stays a numeric sample.
    #[test]
    fn absence_nan_stale_and_negative_values_match_raw_windows() {
        let values = [
            (0, Some(-4.)),
            (1000, None),
            (2000, Some(-2.)),
            (3000, Some(f64::from_bits(0x7ff0000000000002))),
            (4000, Some(1.)),
            (5000, Some(f64::NAN)),
            (6000, Some(-3.)),
            (7000, Some(2.)),
        ];
        let index = RangeCounterIndex::new(values);
        for start in [-1, 0, 999, 2000, 3999, 4999, 6000, 7000] {
            for end in [1000, 2056, 4056, 6056, 7056, 100_056] {
                assert_same(
                    index.rate(start, end).unwrap(),
                    raw_rate(&values, start, end),
                );
            }
        }
    }
    // Every interval over deterministic reset-heavy, uneven samples agrees with
    // the independent raw scan, including sparse-boundary and zero extrapolation.
    #[test]
    fn reset_heavy_series_matches_raw_over_all_window_edges() {
        let mut state = 17_u64;
        let mut timestamp = 56;
        let mut value = 0.;
        let mut points = Vec::new();
        for i in 0..64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            timestamp += 1 + (state % 2300) as i64;
            value = if i % 7 == 0 {
                (state % 3) as f64
            } else {
                value + (state % 31) as f64 / 7.0
            };
            points.push((timestamp, if i % 13 == 0 { None } else { Some(value) }));
        }
        let index = RangeCounterIndex::new(points.iter().copied());
        for (i, (start, _)) in points.iter().enumerate() {
            for (end, _) in points.iter().skip(i) {
                for shift in [-1, 0, 1, 50_000] {
                    let (start, end) = (start - 1, end + shift);
                    let expected = raw_rate(&points, start, end);
                    assert_same(index.rate(start, end).unwrap(), expected);
                    assert_same(
                        index.increase(start, end).unwrap(),
                        expected.map(|v| v * (end - start) as f64 / 1000.),
                    );
                }
            }
        }
    }
    // This adversarial window cannot be reassociated without changing Prom's
    // sequential result; fail closed instead of returning a wrong warm value.
    #[test]
    fn catastrophic_cancellation_requires_raw_execution() {
        let points = [
            (0, Some(1e30)),
            (1000, Some(0.)),
            (2000, Some(1.)),
            (3000, Some(0.)),
            (4000, Some(1.)),
        ];
        let index = RangeCounterIndex::new(points);
        assert!(raw_rate(&points, -1, 4000).unwrap() > 0.);
        assert_eq!(index.rate(-1, 4000), Err(CounterIndexError));
    }

    // Monotonic counters do not allocate a correction-tree leaf per sample.
    #[test]
    fn monotonic_counter_correction_state_is_constant_size() {
        let small = RangeCounterIndex::new((0..2).map(|i| (i, Some(i as f64))));
        let large = RangeCounterIndex::new((0..10_000).map(|i| (i, Some(i as f64))));
        assert_eq!(small.estimated_bytes(), large.estimated_bytes());
        assert!(large.reset_positions.is_empty());
        assert_same(large.rate(-1, 9_999).unwrap(), Some(999.9));
    }

    // Independent counter resets must never be pooled before the per-series rate.
    #[test]
    fn independent_series_same_timestamp_resets_remain_independent() {
        let a = RangeCounterIndex::new([(1000, Some(100.)), (2000, Some(110.))]);
        let b = RangeCounterIndex::new([(1000, Some(50.)), (2000, Some(5.))]);
        assert_same(a.increase(999, 2000).unwrap(), Some(10.01));
        assert_same(b.increase(999, 2000).unwrap(), Some(5.005));
    }
}
