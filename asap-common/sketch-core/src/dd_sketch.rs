//! DDSketch — log-bucketed quantile sketch, mergeable by store-index alignment.
//!
//! Parallel to `count_sketch::CountSketch`: the minimum viable surface
//! needed for the modified-OTLP `Metric.data = DDSketch{…}` hot path
//! (PR C-CountSketch follow-up). Holds the bucket counts, their
//! absolute-index base offset, and the aggregate `{count, sum, min, max}`.
//!
//! Merge semantics: two sketches with the same relative-accuracy
//! parameter `alpha` are merged by aligning bucket arrays along their
//! `store_offset` and summing counts element-wise, with `min`/`max`
//! combined via min/max and `count`/`sum` added.
//!
//! The wire format is the protobuf-encoded
//! `asap_sketchlib::proto::sketchlib::DDSketchState` emitted by
//! DataCollector's `ddsketchprocessor`. Quantile estimation against
//! stored data is intentionally deferred — queries currently return
//! a placeholder error and fall through to the §5.2 fallback.

use serde::{Deserialize, Serialize};

/// Sparse delta between two consecutive DDSketch snapshots — the
/// input shape for [`DdSketch::apply_delta`]. Mirrors the
/// `DDSketchDelta` proto in `sketchlib-go/proto/ddsketch/ddsketch.proto`
/// (and its Rust bindings vendored in `asap_otel_proto::sketchlib::v1`).
/// Kept as a plain struct in sketch-core so the pure-math crate doesn't
/// need a tonic/prost dependency; proto decode lives in the accumulator.
#[derive(Debug, Clone, Default)]
pub struct DdSketchDelta {
    /// `(absolute_bucket_index, Δcount)` pairs, additive.
    pub buckets: Vec<(i32, u64)>,
    /// Δ total count. May be negative (signed on the wire).
    pub d_count: i64,
    /// Δ sum.
    pub d_sum: f64,
    /// Whether `new_min` carries a meaningful value. Min can only
    /// decrease; a delta that didn't lower min sends `false`.
    pub min_changed: bool,
    pub new_min: f64,
    /// Whether `new_max` carries a meaningful value. Max can only
    /// increase.
    pub max_changed: bool,
    pub new_max: f64,
}

/// Minimal DDSketch state — bucket counts + alpha + aggregates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdSketch {
    /// Relative accuracy parameter; must satisfy `0 < alpha < 1`.
    pub alpha: f64,
    /// Bucket counts in absolute-index order. The absolute index of
    /// `store_counts[i]` is `i + store_offset`.
    pub store_counts: Vec<u64>,
    /// Absolute bucket index corresponding to `store_counts[0]`. May
    /// be negative.
    pub store_offset: i32,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl DdSketch {
    /// Construct an empty sketch.
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha,
            store_counts: Vec::new(),
            store_offset: 0,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Construct from the decoded wire fields.
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw(
        alpha: f64,
        store_counts: Vec<u64>,
        store_offset: i32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    ) -> Self {
        Self {
            alpha,
            store_counts,
            store_offset,
            count,
            sum,
            min,
            max,
        }
    }

    /// Merge one other sketch into self by aligning bucket arrays on
    /// absolute indices. Both operands must share the same `alpha`.
    pub fn merge(
        &mut self,
        other: &DdSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if (self.alpha - other.alpha).abs() > f64::EPSILON {
            return Err(format!(
                "DdSketch alpha mismatch: self={}, other={}",
                self.alpha, other.alpha
            )
            .into());
        }

        if other.store_counts.is_empty() {
            self.count += other.count;
            self.sum += other.sum;
            if other.min < self.min {
                self.min = other.min;
            }
            if other.max > self.max {
                self.max = other.max;
            }
            return Ok(());
        }
        if self.store_counts.is_empty() {
            self.store_counts = other.store_counts.clone();
            self.store_offset = other.store_offset;
        } else {
            let self_start = self.store_offset as i64;
            let self_end = self_start + self.store_counts.len() as i64;
            let other_start = other.store_offset as i64;
            let other_end = other_start + other.store_counts.len() as i64;
            let new_start = self_start.min(other_start);
            let new_end = self_end.max(other_end);
            let new_len = (new_end - new_start) as usize;
            let mut merged = vec![0u64; new_len];
            for (i, c) in self.store_counts.iter().enumerate() {
                let idx = (self_start + i as i64 - new_start) as usize;
                merged[idx] = merged[idx].saturating_add(*c);
            }
            for (i, c) in other.store_counts.iter().enumerate() {
                let idx = (other_start + i as i64 - new_start) as usize;
                merged[idx] = merged[idx].saturating_add(*c);
            }
            self.store_counts = merged;
            self.store_offset = new_start as i32;
        }
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        Ok(())
    }

    /// Apply a sparse delta to this sketch in place. Matches the
    /// `ApplyDelta` logic in `sketchlib-go/sketches/DDSketch/delta.go`:
    /// bucket counts add, total count + sum add, min can only decrease
    /// and max can only increase. Used by the backend ingest path to
    /// reconstitute a full sketch from a base snapshot + subsequent
    /// delta-transmission frames (paper §6.2 B3 / B4 baselines).
    pub fn apply_delta(&mut self, delta: &DdSketchDelta) {
        for (abs_idx, d_count) in &delta.buckets {
            if self.store_counts.is_empty() {
                self.store_counts = vec![0u64; 1];
                self.store_offset = *abs_idx;
            }
            let cur_start = self.store_offset as i64;
            let cur_end = cur_start + self.store_counts.len() as i64;
            let k = *abs_idx as i64;
            if k < cur_start {
                // Prepend zeros.
                let pad = (cur_start - k) as usize;
                let mut buf = vec![0u64; pad];
                buf.append(&mut self.store_counts);
                self.store_counts = buf;
                self.store_offset = *abs_idx;
            } else if k >= cur_end {
                let pad = (k - cur_end + 1) as usize;
                self.store_counts
                    .extend(std::iter::repeat(0u64).take(pad));
            }
            let arr_idx = (k - self.store_offset as i64) as usize;
            self.store_counts[arr_idx] =
                self.store_counts[arr_idx].saturating_add(*d_count);
        }
        if delta.d_count >= 0 {
            self.count = self.count.saturating_add(delta.d_count as u64);
        } else {
            self.count = self.count.saturating_sub((-delta.d_count) as u64);
        }
        self.sum += delta.d_sum;
        if delta.min_changed && delta.new_min < self.min {
            self.min = delta.new_min;
        }
        if delta.max_changed && delta.new_max > self.max {
            self.max = delta.new_max;
        }
    }

    /// Merge a slice of references into a single new sketch. Returns
    /// `Err` on alpha mismatch or an empty input.
    pub fn merge_refs(
        inputs: &[&DdSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("DdSketch::merge_refs called with empty input")?;
        let mut merged = DdSketch::new(first.alpha);
        for d in inputs {
            merged.merge(d)?;
        }
        Ok(merged)
    }

    /// Serialize to MessagePack bytes.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap_or_default()
    }

    /// Deserialize from MessagePack bytes.
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rmp_serde::from_slice(buffer)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_empty() {
        let d = DdSketch::new(0.01);
        assert_eq!(d.count, 0);
        assert!(d.store_counts.is_empty());
        assert_eq!(d.min, f64::INFINITY);
        assert_eq!(d.max, f64::NEG_INFINITY);
    }

    #[test]
    fn test_merge_aligned_same_offset() {
        let mut a = DdSketch::from_raw(0.01, vec![1, 2, 3], -1, 6, 30.0, 1.0, 5.0);
        let b = DdSketch::from_raw(0.01, vec![10, 20, 30], -1, 60, 300.0, 0.5, 6.0);
        a.merge(&b).unwrap();
        assert_eq!(a.store_counts, vec![11, 22, 33]);
        assert_eq!(a.store_offset, -1);
        assert_eq!(a.count, 66);
        assert_eq!(a.sum, 330.0);
        assert_eq!(a.min, 0.5);
        assert_eq!(a.max, 6.0);
    }

    #[test]
    fn test_merge_overlapping_offsets() {
        // a covers indices [-1, 0, 1]; b covers indices [0, 1, 2]
        let mut a = DdSketch::from_raw(0.01, vec![1, 1, 1], -1, 3, 3.0, 1.0, 3.0);
        let b = DdSketch::from_raw(0.01, vec![10, 10, 10], 0, 30, 30.0, 1.0, 3.0);
        a.merge(&b).unwrap();
        // Merged window is [-1, 0, 1, 2] → [1, 11, 11, 10]
        assert_eq!(a.store_counts, vec![1, 11, 11, 10]);
        assert_eq!(a.store_offset, -1);
        assert_eq!(a.count, 33);
    }

    #[test]
    fn test_merge_disjoint_offsets() {
        let mut a = DdSketch::from_raw(0.01, vec![1, 2], 0, 3, 3.0, 1.0, 2.0);
        let b = DdSketch::from_raw(0.01, vec![3, 4], 5, 7, 7.0, 5.0, 6.0);
        a.merge(&b).unwrap();
        // Window [0..7) → [1,2,0,0,0,3,4]
        assert_eq!(a.store_counts, vec![1, 2, 0, 0, 0, 3, 4]);
        assert_eq!(a.store_offset, 0);
    }

    #[test]
    fn test_apply_delta_additive_inside_store() {
        let mut base = DdSketch::from_raw(0.01, vec![1, 2, 3], -1, 6, 30.0, 1.0, 5.0);
        let delta = DdSketchDelta {
            buckets: vec![(-1, 4), (0, 8), (1, 12)],
            d_count: 24,
            d_sum: 120.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: true,
            new_max: 9.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![5, 10, 15]);
        assert_eq!(base.count, 30);
        assert_eq!(base.sum, 150.0);
        assert_eq!(base.min, 1.0);
        assert_eq!(base.max, 9.0);
    }

    #[test]
    fn test_apply_delta_expands_store_on_new_bucket() {
        // Base covers [0..2]; delta adds a bucket at absolute index 4.
        let mut base = DdSketch::from_raw(0.01, vec![1, 2], 0, 3, 3.0, 1.0, 2.0);
        let delta = DdSketchDelta {
            buckets: vec![(4, 7)],
            d_count: 7,
            d_sum: 35.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: true,
            new_max: 6.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![1, 2, 0, 0, 7]);
        assert_eq!(base.store_offset, 0);
        assert_eq!(base.count, 10);
        assert_eq!(base.max, 6.0);
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        // Snapshot the sketch, add more samples via a merge, and confirm
        // the delta+apply path lands at the same state.
        let base = DdSketch::from_raw(0.01, vec![1, 2, 3], 0, 6, 12.0, 1.0, 3.0);
        let addition = DdSketch::from_raw(0.01, vec![10, 0, 20], 0, 30, 70.0, 0.5, 5.0);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = DdSketchDelta {
            buckets: vec![(0, 10), (2, 20)],
            d_count: 30,
            d_sum: 70.0,
            min_changed: true,
            new_min: 0.5,
            max_changed: true,
            new_max: 5.0,
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta);

        assert_eq!(via_delta.store_counts, via_merge.store_counts);
        assert_eq!(via_delta.count, via_merge.count);
        assert_eq!(via_delta.sum, via_merge.sum);
        assert_eq!(via_delta.min, via_merge.min);
        assert_eq!(via_delta.max, via_merge.max);
    }

    #[test]
    fn test_merge_alpha_mismatch() {
        let mut a = DdSketch::new(0.01);
        let b = DdSketch::new(0.02);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original = DdSketch::from_raw(0.01, vec![1, 2, 3], -2, 6, 30.0, 1.0, 5.0);
        let bytes = original.serialize_msgpack();
        let decoded = DdSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(decoded.store_counts, original.store_counts);
        assert_eq!(decoded.store_offset, original.store_offset);
        assert_eq!(decoded.count, original.count);
    }
}
