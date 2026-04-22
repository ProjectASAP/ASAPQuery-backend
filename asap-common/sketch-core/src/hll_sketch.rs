//! HyperLogLog sketch — register-wise mergeable cardinality estimator.
//!
//! Parallel to `count_sketch::CountSketch`: the minimum viable surface
//! needed for the modified-OTLP `Metric.data = HLLSketch{…}` hot path
//! (PR C-CountSketch follow-up). Wraps a flat `Vec<u8>` of register
//! values (length = `2^precision`) and merges element-wise by taking
//! the maximum across aligned registers, which is the standard HLL
//! merge semantics.
//!
//! The wire format is the protobuf-encoded
//! `asap_sketchlib::proto::sketchlib::HyperLogLogState` emitted by
//! DataCollector's `hllprocessor`. This type carries the register
//! bytes and the variant/precision metadata losslessly, so the
//! merge + store round-trip works end-to-end. Cardinality estimation
//! against stored HLL data is intentionally deferred to a follow-up
//! — queries currently return a placeholder error and fall through
//! to the §5.2 fallback.

use serde::{Deserialize, Serialize};

/// HLL estimator variant. Mirrors `asap_sketchlib::proto::sketchlib::HllVariant`
/// so the proto round-trip preserves the algorithm identity — the three
/// variants are not mutually compatible on register contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HllVariant {
    Unspecified,
    Regular,
    Datafusion,
    Hip,
}

/// Sparse delta between two consecutive HLL snapshots — the input
/// shape for [`HllSketch::apply_delta`]. Mirrors the `HLLDelta` proto
/// in `sketchlib-go/proto/hll/hll.proto` (and its Rust bindings
/// vendored in `asap_otel_proto::sketchlib::v1`). HLL registers merge
/// with max semantics, so a delta carries only the register indices
/// whose value increased since the last snapshot.
#[derive(Debug, Clone, Default)]
pub struct HllDelta {
    /// `(register_index, new_value)` pairs. `new_value` is the full
    /// post-update register value; `apply_delta` does
    /// `registers[i] = max(registers[i], new_value)`.
    pub updates: Vec<(u32, u8)>,
}

/// Minimal HLL state — registers + variant + precision. Register-wise
/// mergeable (max over aligned cells).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HllSketch {
    pub variant: HllVariant,
    pub precision: u32,
    /// Flat register array, length = `2^precision`.
    pub registers: Vec<u8>,
    /// HIP accumulator components — populated only when `variant == Hip`.
    pub hip_kxq0: f64,
    pub hip_kxq1: f64,
    pub hip_est: f64,
}

impl HllSketch {
    /// Construct an empty sketch at the given precision.
    pub fn new(variant: HllVariant, precision: u32) -> Self {
        let n = 1usize << precision;
        Self {
            variant,
            precision,
            registers: vec![0u8; n],
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
        }
    }

    /// Construct from pre-built register bytes (used by the modified-OTLP
    /// proto-decode path).
    pub fn from_raw(
        variant: HllVariant,
        precision: u32,
        registers: Vec<u8>,
        hip_kxq0: f64,
        hip_kxq1: f64,
        hip_est: f64,
    ) -> Self {
        Self {
            variant,
            precision,
            registers,
            hip_kxq0,
            hip_kxq1,
            hip_est,
        }
    }

    /// Merge one other sketch into self via register-wise max. Both
    /// operands must have identical variant and precision.
    pub fn merge(
        &mut self,
        other: &HllSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.variant != other.variant {
            return Err(format!(
                "HllSketch variant mismatch: self={:?}, other={:?}",
                self.variant, other.variant
            )
            .into());
        }
        if self.precision != other.precision {
            return Err(format!(
                "HllSketch precision mismatch: self={}, other={}",
                self.precision, other.precision
            )
            .into());
        }
        if self.registers.len() != other.registers.len() {
            return Err(format!(
                "HllSketch register-length mismatch: self={}, other={}",
                self.registers.len(),
                other.registers.len()
            )
            .into());
        }
        for (s, o) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *o > *s {
                *s = *o;
            }
        }
        // HIP accumulators add on merge (each source carried its own
        // running estimate; merged state inherits the combined
        // components).
        if self.variant == HllVariant::Hip {
            self.hip_kxq0 += other.hip_kxq0;
            self.hip_kxq1 += other.hip_kxq1;
            self.hip_est += other.hip_est;
        }
        Ok(())
    }

    /// Apply a sparse register delta in place. Matches the
    /// `registers[i] = max(registers[i], new_value)` logic in
    /// `sketchlib-go/sketches/HLL/delta.go::ApplyRegisterDelta`. Used
    /// by the backend ingest path to reconstitute a full sketch from
    /// a base snapshot + subsequent delta-transmission frames (paper
    /// §6.2 B3 / B4 baselines).
    ///
    /// Returns `Err` if any delta index is out of range for the
    /// sketch's precision — indicating a precision mismatch between
    /// the snapshot this sketch was built from and the delta sender.
    pub fn apply_delta(
        &mut self,
        delta: &HllDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let n = self.registers.len();
        for (idx, new_val) in &delta.updates {
            let i = *idx as usize;
            if i >= n {
                return Err(format!(
                    "HllDelta index {i} out of range (precision={} → {n} registers)",
                    self.precision
                )
                .into());
            }
            if *new_val > self.registers[i] {
                self.registers[i] = *new_val;
            }
        }
        Ok(())
    }

    /// Merge a slice of references into a single new sketch. All inputs
    /// must share the same variant and precision; returns `Err` on
    /// mismatch or an empty input.
    pub fn merge_refs(
        inputs: &[&HllSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("HllSketch::merge_refs called with empty input")?;
        let mut merged = HllSketch::new(first.variant, first.precision);
        for hll in inputs {
            merged.merge(hll)?;
        }
        Ok(merged)
    }

    /// Serialize to MessagePack bytes (used by the legacy Arroyo path
    /// and by PR I's `_ENCODING_MSGPACK` variant when that lands).
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
        let h = HllSketch::new(HllVariant::Regular, 4);
        assert_eq!(h.registers.len(), 16);
        assert!(h.registers.iter().all(|&r| r == 0));
    }

    #[test]
    fn test_merge_register_wise_max() {
        let mut a = HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0);
        let b = HllSketch::from_raw(HllVariant::Regular, 2, vec![4, 2, 6, 0], 0.0, 0.0, 0.0);
        a.merge(&b).unwrap();
        assert_eq!(a.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_apply_delta_max_semantics() {
        let mut h =
            HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0);
        let delta = HllDelta {
            updates: vec![(0, 4), (1, 2), (2, 6), (3, 0)],
        };
        h.apply_delta(&delta).unwrap();
        // reg[0]: max(1,4)=4, reg[1]: max(5,2)=5, reg[2]: max(3,6)=6,
        // reg[3]: max(7,0)=7.
        assert_eq!(h.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut h = HllSketch::new(HllVariant::Regular, 2); // 4 registers
        let delta = HllDelta {
            updates: vec![(7, 3)],
        };
        assert!(h.apply_delta(&delta).is_err());
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base =
            HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0);
        let addition =
            HllSketch::from_raw(HllVariant::Regular, 2, vec![4, 0, 6, 0], 0.0, 0.0, 0.0);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = HllDelta {
            updates: vec![(0, 4), (2, 6)],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.registers, via_merge.registers);
    }

    #[test]
    fn test_merge_variant_mismatch() {
        let mut a = HllSketch::new(HllVariant::Regular, 4);
        let b = HllSketch::new(HllVariant::Datafusion, 4);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_precision_mismatch() {
        let mut a = HllSketch::new(HllVariant::Regular, 4);
        let b = HllSketch::new(HllVariant::Regular, 5);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_refs() {
        let a = HllSketch::from_raw(HllVariant::Regular, 1, vec![1, 0], 0.0, 0.0, 0.0);
        let b = HllSketch::from_raw(HllVariant::Regular, 1, vec![0, 3], 0.0, 0.0, 0.0);
        let c = HllSketch::from_raw(HllVariant::Regular, 1, vec![2, 2], 0.0, 0.0, 0.0);
        let merged = HllSketch::merge_refs(&[&a, &b, &c]).unwrap();
        assert_eq!(merged.registers, vec![2, 3]);
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original = HllSketch::from_raw(
            HllVariant::Hip,
            3,
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            1.0,
            2.0,
            3.0,
        );
        let bytes = original.serialize_msgpack();
        let decoded = HllSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(decoded.registers, original.registers);
        assert_eq!(decoded.precision, original.precision);
        assert_eq!(decoded.hip_kxq0, 1.0);
    }
}
