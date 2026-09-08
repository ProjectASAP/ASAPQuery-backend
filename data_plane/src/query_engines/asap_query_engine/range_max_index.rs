//! Exact range-max state for one complete source-label set.
//!
//! Leaves retain timestamp order. Query bounds are sample offsets computed with
//! (start, end] partition points; no pane rounding or counter semantics apply.
#[derive(Debug)]
pub(crate) struct RangeMaxIndex {
    tree: Vec<Option<f64>>,
    width: usize,
    len: usize,
}
fn merge(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (None, value) | (value, None) => value,
        (Some(a), Some(b)) => Some(if a.is_nan() || b > a { b } else { a }),
    }
}
impl RangeMaxIndex {
    pub(crate) fn new(values: impl ExactSizeIterator<Item = Option<f64>>) -> Self {
        let len = values.len();
        let width = len.max(1).next_power_of_two();
        let mut tree = vec![None; width * 2];
        for (i, value) in values.enumerate() {
            tree[width + i] = value.filter(|v| v.to_bits() != 0x7ff0000000000002);
        }
        for i in (1..width).rev() {
            tree[i] = merge(tree[i * 2], tree[i * 2 + 1]);
        }
        Self { tree, width, len }
    }
    pub(crate) fn query(&self, start: usize, end: usize) -> Option<f64> {
        assert!(start <= end && end <= self.len);
        let (mut lo, mut hi) = (start + self.width, end + self.width);
        let (mut left, mut right) = (None, None);
        while lo < hi {
            if lo % 2 == 1 {
                left = merge(left, self.tree[lo]);
                lo += 1;
            }
            if hi % 2 == 1 {
                hi -= 1;
                right = merge(self.tree[hi], right);
            }
            lo /= 2;
            hi /= 2;
        }
        merge(left, right)
    }
    pub(crate) fn estimated_bytes(&self) -> usize {
        self.tree.capacity() * std::mem::size_of::<Option<f64>>()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_partial_interval_matches_ordered_prometheus_max_semantics() {
        let values = [
            None,
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(-4.0),
            Some(7.0),
            Some(f64::from_bits(0x7ff0000000000002)),
            Some(f64::NEG_INFINITY),
        ];
        let index = RangeMaxIndex::new(values.into_iter());
        for start in 0..=values.len() {
            for end in start..=values.len() {
                let expected = values[start..end]
                    .iter()
                    .copied()
                    .map(|v| v.filter(|v| v.to_bits() != 0x7ff0000000000002))
                    .fold(None, merge);
                assert_eq!(
                    index.query(start, end).map(f64::to_bits),
                    expected.map(f64::to_bits),
                    "{start}..{end}"
                );
            }
        }
        assert!(index.estimated_bytes() > 0);
    }
    #[test]
    fn signed_zero_and_nan_follow_first_non_nan_max_in_timestamp_order() {
        let index = RangeMaxIndex::new(
            [
                Some(f64::NAN),
                Some(-0.0),
                Some(0.0),
                Some(f64::NAN),
                Some(-2.0),
            ]
            .into_iter(),
        );
        assert!(index.query(0, 1).unwrap().is_nan());
        assert_eq!(index.query(0, 3).unwrap().to_bits(), (-0.0_f64).to_bits());
        assert_eq!(index.query(2, 4).unwrap().to_bits(), 0.0_f64.to_bits());
        assert_eq!(index.query(3, 5), Some(-2.0));
    }
    #[test]
    fn empty_and_all_nan_ranges_are_distinct() {
        let empty = RangeMaxIndex::new([].into_iter());
        assert_eq!(empty.query(0, 0), None);
        let nan = RangeMaxIndex::new([Some(f64::NAN), Some(f64::NAN)].into_iter());
        assert!(nan.query(0, 2).unwrap().is_nan());
    }
}
