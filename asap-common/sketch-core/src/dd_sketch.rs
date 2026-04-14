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
