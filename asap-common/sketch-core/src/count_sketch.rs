//! Count Sketch (a.k.a. Count-Min-style signed-counter sketch) —
//! element-wise mergeable frequency estimator.
//!
//! Parallel to `count_min::CountMinSketch` but with **signed** counters,
//! matching the `asap_sketchlib::proto::sketchlib::CountSketchState` wire
//! format that DataCollector's `countsketchprocessor` emits via the
//! modified OTLP `Metric.data = CountSketch{…}` variant.
//!
//! This is the minimal surface needed for PR C-CountSketch in the
//! modified-OTLP hot path: construct from a decoded proto state, merge
//! element-wise with another sketch, emit the matrix for queries and
//! serialization. The richer query semantics of Count Sketch (median-
//! of-estimators heavy-hitter tracking, `TopKState` integration, etc.)
//! are intentionally deferred to a follow-up — the wire format already
//! carries the matrix losslessly, so the merge/store round-trip works
//! with just a matrix today.

use serde::{Deserialize, Serialize};

/// Sparse delta between two consecutive CountSketch snapshots —
/// the input shape for [`CountSketch::apply_delta`]. Mirrors the
/// `CountSketchDelta` proto in
/// `sketchlib-go/proto/countsketch/countsketch.proto` (packed
/// encoding only — the deprecated `cells_legacy` path + the
/// non-delta `topk` / `hh_keys` top-K carrier aren't modeled here).
///
/// Cells apply additively: `matrix[row][col] += d_count` for each
/// `(row, col, d_count)` triple. Top-K on the delta path is a
/// separate follow-up (CS top-K is non-linear; merging deltas
/// would require re-querying the merged matrix).
#[derive(Debug, Clone, Default)]
pub struct CountSketchDelta {
    pub rows: u32,
    pub cols: u32,
    /// `(row, col, d_count)` cell updates, additive on the CS matrix.
    pub cells: Vec<(u32, u32, i64)>,
    /// Per-row L2 norm deltas. Additive, one scalar per row of the
    /// base sketch. Kept on the delta surface for downstream
    /// error-accounting; `apply_delta` itself ignores L2.
    pub l2: Vec<f64>,
}

/// Minimal Count Sketch state — a flat `rows × cols` matrix of signed
/// counts. Element-wise mergeable (sum over aligned cells).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountSketch {
    pub row_num: usize,
    pub col_num: usize,
    /// Row-major matrix of signed counts. `matrix[r][c]` is the value of
    /// hash row `r`, column `c`.
    pub matrix: Vec<Vec<f64>>,
}

impl CountSketch {
    /// Construct an all-zero sketch with the given dimensions.
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            row_num,
            col_num,
            matrix: vec![vec![0.0; col_num]; row_num],
        }
    }

    /// Construct from a pre-built matrix (used by the modified-OTLP
    /// proto-decode path).
    pub fn from_legacy_matrix(matrix: Vec<Vec<f64>>, row_num: usize, col_num: usize) -> Self {
        debug_assert_eq!(matrix.len(), row_num, "row count mismatch");
        debug_assert!(
            matrix.iter().all(|r| r.len() == col_num),
            "column count mismatch in at least one row"
        );
        Self {
            row_num,
            col_num,
            matrix,
        }
    }

    /// Borrow the inner matrix.
    pub fn sketch(&self) -> &Vec<Vec<f64>> {
        &self.matrix
    }

    /// Merge one other sketch into self via element-wise addition. Both
    /// operands must have identical dimensions.
    pub fn merge(
        &mut self,
        other: &CountSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.row_num != other.row_num || self.col_num != other.col_num {
            return Err(format!(
                "CountSketch dimension mismatch: self={}x{}, other={}x{}",
                self.row_num, self.col_num, other.row_num, other.col_num
            )
            .into());
        }
        for r in 0..self.row_num {
            for c in 0..self.col_num {
                self.matrix[r][c] += other.matrix[r][c];
            }
        }
        Ok(())
    }

    /// Apply a sparse delta in place. Matches the `ApplyDelta`
    /// semantics in `sketchlib-go/sketches/CountSketch/delta.go`:
    /// `matrix[row][col] += d_count` for each cell in the delta.
    /// Returns `Err` if any `(row, col)` is out of range — indicating
    /// a dimension mismatch between the snapshot this sketch was
    /// built from and the delta sender.
    pub fn apply_delta(
        &mut self,
        delta: &CountSketchDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (row, col, d_count) in &delta.cells {
            let r = *row as usize;
            let c = *col as usize;
            if r >= self.row_num || c >= self.col_num {
                return Err(format!(
                    "CountSketchDelta cell ({r},{c}) out of range (matrix={}x{})",
                    self.row_num, self.col_num
                )
                .into());
            }
            // `d_count` is signed on the wire; CS counts are signed
            // too (can go negative under adversarial keys).
            self.matrix[r][c] += *d_count as f64;
        }
        Ok(())
    }

    /// Merge a slice of references into a single new sketch. All inputs
    /// must share the same dimensions; returns `Err` on mismatch or an
    /// empty input.
    pub fn merge_refs(
        inputs: &[&CountSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("CountSketch::merge_refs called with empty input")?;
        let mut merged = CountSketch::new(first.row_num, first.col_num);
        for cs in inputs {
            merged.merge(cs)?;
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
        let cs = CountSketch::new(2, 3);
        assert_eq!(cs.row_num, 2);
        assert_eq!(cs.col_num, 3);
        assert_eq!(cs.sketch(), &vec![vec![0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0]]);
    }

    #[test]
    fn test_from_legacy_matrix() {
        let m = vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]];
        let cs = CountSketch::from_legacy_matrix(m.clone(), 2, 3);
        assert_eq!(cs.sketch(), &m);
    }

    #[test]
    fn test_merge_element_wise() {
        let mut a = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let b = CountSketch::from_legacy_matrix(vec![vec![-1.0, -2.0], vec![-3.0, -4.0]], 2, 2);
        a.merge(&b).unwrap();
        assert_eq!(a.sketch(), &vec![vec![0.0, 0.0], vec![0.0, 0.0]]);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let mut a = CountSketch::new(2, 3);
        let b = CountSketch::new(3, 3);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_refs() {
        let a = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0]], 1, 2);
        let b = CountSketch::from_legacy_matrix(vec![vec![3.0, 4.0]], 1, 2);
        let c = CountSketch::from_legacy_matrix(vec![vec![5.0, 6.0]], 1, 2);
        let merged = CountSketch::merge_refs(&[&a, &b, &c]).unwrap();
        assert_eq!(merged.sketch(), &vec![vec![9.0, 12.0]]);
    }

    #[test]
    fn test_apply_delta_additive() {
        let mut cs = CountSketch::from_legacy_matrix(
            vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]],
            2,
            3,
        );
        let delta = CountSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![
                (0, 0, 10),   // 1 + 10 = 11
                (0, 2, -3),   // 3 - 3 = 0
                (1, 1, -15),  // 5 - 15 = -10
            ],
            l2: vec![],
        };
        cs.apply_delta(&delta).unwrap();
        assert_eq!(cs.sketch(), &vec![vec![11.0, -2.0, 0.0], vec![-4.0, -10.0, -6.0]]);
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base = CountSketch::from_legacy_matrix(
            vec![vec![1.0, 2.0], vec![3.0, 4.0]],
            2,
            2,
        );
        let addition = CountSketch::from_legacy_matrix(
            vec![vec![10.0, 0.0], vec![0.0, 20.0]],
            2,
            2,
        );
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = CountSketchDelta {
            rows: 2,
            cols: 2,
            cells: vec![(0, 0, 10), (1, 1, 20)],
            l2: vec![],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.sketch(), via_merge.sketch());
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut cs = CountSketch::new(2, 3);
        let delta = CountSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(2, 0, 1)], // row 2 out of range for 2-row matrix
            l2: vec![],
        };
        assert!(cs.apply_delta(&delta).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original =
            CountSketch::from_legacy_matrix(vec![vec![1.5, -2.5], vec![3.5, -4.5]], 2, 2);
        let bytes = original.serialize_msgpack();
        let decoded = CountSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(decoded.sketch(), original.sketch());
        assert_eq!(decoded.row_num, original.row_num);
        assert_eq!(decoded.col_num, original.col_num);
    }
}
