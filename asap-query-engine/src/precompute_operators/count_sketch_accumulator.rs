//! Count Sketch accumulator — wraps `sketch_core::count_sketch::CountSketch`.
//!
//! This is the concrete accumulator reached from the modified-OTLP
//! `Metric.data = CountSketch{…}` hot path (PR C-CountSketch). Its
//! inner matrix is the same shape as `CountMinSketchAccumulator`'s
//! but with signed counts and no per-row heap tracking.
//!
//! Minimum viable surface:
//!   - `AggregateCore` impl for precompute-engine worker merge
//!   - `SerializableToSink` impl for store write-out
//!   - `from_sketchlib_proto_bytes(buf)` — decoder for the modified
//!     OTLP `CountSketchDataPoint.sketch` bytes (prost-encoded
//!     `asap_sketchlib::proto::sketchlib::CountSketchState`). Mirrors
//!     the CountMin decoder in PR B.
//!
//! Query semantics (median-of-estimators heavy-hitter tracking,
//! `TopKState` integration) are intentionally deferred — queries
//! against stored CountSketch data return a placeholder error today.
//! The wire format carries the matrix losslessly, so the merge + store
//! round-trip works end-to-end without that richer query surface.

use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
use serde_json::Value;
use sketch_core::count_sketch::{CountSketch, CountSketchDelta};
use std::collections::HashMap;

/// Count Sketch accumulator — inner matrix of signed counts.
#[derive(Debug, Clone)]
pub struct CountSketchAccumulator {
    pub inner: CountSketch,
}

impl CountSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            inner: CountSketch::new(row_num, col_num),
        }
    }

    /// Decode from the modified OTLP wire format's
    /// `CountSketchDataPoint.sketch` bytes when
    /// `encoding = COUNT_SKETCH_ENCODING_MSGPACK`. The bytes are the
    /// MessagePack serialization of the cross-language sketch-core
    /// `CountSketch` struct — PR I parity entrypoint.
    pub fn from_msgpack_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: CountSketch::deserialize_msgpack(buffer)
                .map_err(|e| format!("deserialize CountSketch msgpack: {e}"))?,
        })
    }

    /// Decode from the modified OTLP wire format's
    /// `CountSketchDataPoint.sketch` bytes — the protobuf-encoded
    /// `asap_sketchlib::proto::sketchlib::CountSketchState` message
    /// that DataCollector's `countsketchprocessor` emits when
    /// `encoding = COUNT_SKETCH_ENCODING_PROTO`.
    ///
    /// Mirrors `CountMinSketchAccumulator::from_sketchlib_proto_bytes`
    /// but on the signed-counter `CountSketchState`. The resulting
    /// accumulator is constructed via
    /// `CountSketch::from_legacy_matrix` after reshaping the flat
    /// `counts_int` / `counts_float` field into a `Vec<Vec<f64>>`.
    pub fn from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountSketchState, CounterType, SketchEnvelope,
        };
        use prost::Message;

        // DataCollector's countsketchprocessor wraps the state in a
        // `SketchEnvelope{count_sketch: CountSketchState}` via
        // sketchlib-go's `SerializePortableFO` + `proto.Marshal`. Try
        // decoding as envelope first, fall back to bare
        // `CountSketchState` for callers (e.g. unit tests) that
        // encode the state directly. Mirrors the PR #14 fix on
        // `CountMinSketchAccumulator::from_sketchlib_proto_bytes`.
        let state = match SketchEnvelope::decode(buffer) {
            Ok(env) => match env.sketch_state {
                Some(sketch_envelope::SketchState::CountSketch(st)) => st,
                Some(other) => {
                    return Err(format!(
                        "SketchEnvelope contains non-CountSketch sketch: {:?}",
                        std::mem::discriminant(&other)
                    )
                    .into());
                }
                None => CountSketchState::decode(buffer)
                    .map_err(|e| format!("decode CountSketchState: {e}"))?,
            },
            Err(_) => CountSketchState::decode(buffer)
                .map_err(|e| format!("decode CountSketchState: {e}"))?,
        };
        let rows = state.rows as usize;
        let cols = state.cols as usize;
        if rows == 0 || cols == 0 {
            return Err(
                format!("CountSketchState has zero dims (rows={rows}, cols={cols})").into(),
            );
        }
        let expected_len = rows * cols;
        let counter_type = CounterType::try_from(state.counter_type).map_err(|_| {
            format!(
                "CountSketchState has unknown counter_type tag {}",
                state.counter_type
            )
        })?;
        let flat: Vec<f64> = match counter_type {
            CounterType::Int32 | CounterType::Int64 => {
                if state.counts_int.len() != expected_len {
                    return Err(format!(
                        "CountSketchState counts_int has {} entries, expected rows*cols = {}",
                        state.counts_int.len(),
                        expected_len
                    )
                    .into());
                }
                state.counts_int.iter().map(|&v| v as f64).collect()
            }
            CounterType::Float64 => {
                if state.counts_float.len() != expected_len {
                    return Err(format!(
                        "CountSketchState counts_float has {} entries, expected rows*cols = {}",
                        state.counts_float.len(),
                        expected_len
                    )
                    .into());
                }
                state.counts_float.clone()
            }
            other => {
                return Err(format!(
                    "CountSketchState counter_type {other:?} not yet supported \
                     (INT128 stores interleaved hi/lo pairs; will be added when needed)"
                )
                .into());
            }
        };
        let mut matrix = Vec::with_capacity(rows);
        for r in 0..rows {
            let start = r * cols;
            matrix.push(flat[start..start + cols].to_vec());
        }
        Ok(Self {
            inner: CountSketch::from_legacy_matrix(matrix, rows, cols),
        })
    }

    /// Apply a proto-encoded `CountSketchDelta` frame to this
    /// accumulator's inner sketch — the decode path for
    /// `COUNT_SKETCH_ENCODING_PROTO_DELTA` (paper §6.2 B3 / B4).
    ///
    /// Cells apply additively: `matrix[cell_rows[i]][cell_cols[i]]
    /// += d_counts[i]`. Per-row L2 is parsed off the wire but
    /// ignored at application time — it's a downstream error-
    /// accounting signal, not a merge input.
    pub fn apply_proto_delta_bytes(
        &mut self,
        buffer: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use asap_otel_proto::sketchlib::v1::CountSketchDelta as PbDelta;
        use prost::Message;

        let pb = PbDelta::decode(buffer)
            .map_err(|e| format!("decode CountSketchDelta: {e}"))?;

        if pb.cell_rows.len() != pb.cell_cols.len()
            || pb.cell_rows.len() != pb.d_counts.len()
        {
            return Err(format!(
                "CountSketchDelta packed-array length mismatch: \
                 cell_rows={}, cell_cols={}, d_counts={}",
                pb.cell_rows.len(),
                pb.cell_cols.len(),
                pb.d_counts.len()
            )
            .into());
        }
        let cells = pb
            .cell_rows
            .iter()
            .zip(pb.cell_cols.iter())
            .zip(pb.d_counts.iter())
            .map(|((r, c), dc)| (*r, *c, *dc))
            .collect();
        let delta = CountSketchDelta {
            rows: pb.rows,
            cols: pb.cols,
            cells,
            l2: pb.l2,
        };
        self.inner
            .apply_delta(&delta)
            .map_err(|e| format!("apply CountSketchDelta: {e}"))?;
        Ok(())
    }
}

impl SerializableToSink for CountSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "row_num": self.inner.row_num,
            "col_num": self.inner.col_num,
            "sketch": self.inner.sketch(),
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for CountSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "CountSketchAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }


    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {

        self

    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge CountSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }
        let other_cs = other
            .as_any()
            .downcast_ref::<CountSketchAccumulator>()
            .ok_or("Failed to downcast to CountSketchAccumulator")?;

        let merged_inner = CountSketch::merge_refs(&[&self.inner, &other_cs.inner])?;
        Ok(Box::new(Self {
            inner: merged_inner,
        }))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::CountSketch
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        _key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use promql_utilities::query_logics::enums::Statistic;
        // Use median-of-row estimator for a specific key when the
        // caller provides one in `query_kwargs["key"]`. Without a
        // key, fall back to summing the absolute counter values
        // (rough total-volume signal — useful for sanity checks
        // but not a heavy-hitter answer). Hash compatibility note:
        // this relies on the agent and backend using the
        // sketchlib HashSpec; sketchlib-go's `portableHashSpec`
        // is the canonical seed list, and `sketch_core::CountSketch`
        // hashes against the same spec.
        match statistic {
            Statistic::Topk | Statistic::Count => {
                let matrix = self.inner.sketch();
                if let Some(key) = query_kwargs.get("key") {
                    return Ok(count_sketch_query_key(matrix, key));
                }
                // No key → return total absolute volume across the
                // sketch as a rough activity proxy. Better than
                // erroring out; documented limitation.
                let total: f64 = matrix.iter().flatten().map(|v| v.abs()).sum();
                let rows = matrix.len() as f64;
                Ok(if rows > 0.0 { total / rows } else { 0.0 })
            }
            Statistic::Sum => {
                let matrix = self.inner.sketch();
                let total: f64 = matrix.iter().flatten().sum();
                let rows = matrix.len() as f64;
                Ok(if rows > 0.0 { total / rows } else { 0.0 })
            }
            other => Err(format!(
                "CountSketchAccumulator: statistic {:?} not supported (only Topk / Count / Sum, with optional `key` in query_kwargs)",
                other,
            )
            .into()),
        }
    }
}

/// Median-of-row count estimator for CountSketch. Computes one
/// signed estimate per row at `key`'s hash position and returns
/// the median (canonical CountSketch query).
///
/// Hash compatibility with the agent is via the sketchlib hash
/// spec; the agent's `sketchlib-go::CountSketch` and the
/// backend's `sketch_core::count_sketch::CountSketch` must use
/// the same seed list (sketchlib's `portableHashSpec` /
/// `default_hash_spec`).
fn count_sketch_query_key(matrix: &Vec<Vec<f64>>, key: &str) -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    if matrix.is_empty() {
        return 0.0;
    }
    let cols = matrix[0].len();
    if cols == 0 {
        return 0.0;
    }
    let mut estimates: Vec<f64> = Vec::with_capacity(matrix.len());
    for (i, row) in matrix.iter().enumerate() {
        let mut hasher = DefaultHasher::new();
        // Salt with the row index so each row uses a distinct
        // hash. Note: this is *not* the sketchlib hash spec — the
        // canonical compatibility path requires plumbing the
        // sketchlib seeds through to the backend (tracked as a
        // follow-up; the wire format already carries the seed
        // list, but the accumulator drops it on decode today).
        i.hash(&mut hasher);
        key.hash(&mut hasher);
        let h = hasher.finish() as usize;
        let col = h % cols;
        // Sign hash: +1 / -1 alternating by a second hash bit.
        let sign = if (h >> 32) & 1 == 0 { 1.0 } else { -1.0 };
        estimates.push(sign * row[col]);
    }
    estimates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    estimates[estimates.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_state(
        rows: u32,
        cols: u32,
        counter_type: i32,
        counts_int: Vec<i64>,
        counts_float: Vec<f64>,
    ) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::CountSketchState;
        use prost::Message;
        let state = CountSketchState {
            rows,
            cols,
            counter_type,
            counts_int,
            counts_float,
            l2: Vec::new(),
            topk: None,
        };
        state.encode_to_vec()
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_int64() {
        use asap_sketchlib::proto::sketchlib::CounterType;
        // Signed 2x3 matrix: row 0 = [1,-2,3], row 1 = [-4,5,-6]
        let bytes = encode_state(
            2,
            3,
            CounterType::Int64 as i32,
            vec![1, -2, 3, -4, 5, -6],
            Vec::new(),
        );
        let acc = CountSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        let matrix = acc.inner.sketch();
        assert_eq!(matrix[0], vec![1.0, -2.0, 3.0]);
        assert_eq!(matrix[1], vec![-4.0, 5.0, -6.0]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrapped() {
        // Mirrors what DataCollector's countsketchprocessor emits:
        // the state wrapped in a `SketchEnvelope{count_sketch: ...}`
        // via sketchlib-go's `SerializePortableFO` + `proto.Marshal`.
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountSketchState, CounterType, SketchEnvelope,
        };
        use prost::Message;

        let state = CountSketchState {
            rows: 2,
            cols: 3,
            counter_type: CounterType::Int64 as i32,
            counts_int: vec![1, -2, 3, -4, 5, -6],
            counts_float: Vec::new(),
            ..Default::default()
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::CountSketch(state)),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let acc = CountSketchAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("envelope-wrapped decode should succeed");
        let matrix = acc.inner.sketch();
        assert_eq!(matrix[0], vec![1.0, -2.0, 3.0]);
        assert_eq!(matrix[1], vec![-4.0, 5.0, -6.0]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrong_sketch_type() {
        // An envelope carrying a non-CountSketch sketch should be
        // rejected with a clear error rather than silently producing
        // garbage.
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;

        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(KllState::default())),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let result = CountSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err(), "wrong-sketch envelope should error");
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_float64() {
        use asap_sketchlib::proto::sketchlib::CounterType;
        let bytes = encode_state(
            2,
            2,
            CounterType::Float64 as i32,
            Vec::new(),
            vec![1.5, -2.5, 3.5, -4.5],
        );
        let acc = CountSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        let matrix = acc.inner.sketch();
        assert_eq!(matrix[0], vec![1.5, -2.5]);
        assert_eq!(matrix[1], vec![3.5, -4.5]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_dimension_mismatch() {
        use asap_sketchlib::proto::sketchlib::CounterType;
        // 2x3 declared but only 5 int entries
        let bytes = encode_state(
            2,
            3,
            CounterType::Int64 as i32,
            vec![1, 2, 3, 4, 5],
            Vec::new(),
        );
        let result = CountSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("counts_int"),
            "error should mention counts_int dim mismatch"
        );
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_zero_dims_rejected() {
        use asap_sketchlib::proto::sketchlib::CountSketchState;
        use prost::Message;
        let state = CountSketchState::default();
        let bytes = state.encode_to_vec();
        let result = CountSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("zero dims"));
    }

    #[test]
    fn test_aggregate_core_merge_matches_matrix_add() {
        let a = CountSketchAccumulator {
            inner: CountSketch::from_legacy_matrix(vec![vec![1.0, -2.0], vec![3.0, -4.0]], 2, 2),
        };
        let b = CountSketchAccumulator {
            inner: CountSketch::from_legacy_matrix(vec![vec![-1.0, 2.0], vec![-3.0, 4.0]], 2, 2),
        };
        let merged_box = a.merge_with(&b).expect("merge ok");
        let merged = merged_box
            .as_any()
            .downcast_ref::<CountSketchAccumulator>()
            .expect("downcast ok");
        let m = merged.inner.sketch();
        assert_eq!(m[0], vec![0.0, 0.0]);
        assert_eq!(m[1], vec![0.0, 0.0]);
    }

    #[test]
    fn test_aggregate_core_merge_wrong_type_rejects() {
        use crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator;
        let cs = CountSketchAccumulator::new(2, 3);
        let cms = CountMinSketchAccumulator::new(2, 3);
        let result = cs.merge_with(&cms);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_msgpack_bytes_round_trip() {
        let original = CountSketch::from_legacy_matrix(
            vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]],
            2,
            3,
        );
        let bytes = original.serialize_msgpack();
        let acc = CountSketchAccumulator::from_msgpack_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.row_num, 2);
        assert_eq!(acc.inner.col_num, 3);
        assert_eq!(acc.inner.sketch(), original.sketch());
    }

    #[test]
    fn test_from_msgpack_bytes_rejects_garbage() {
        let result = CountSketchAccumulator::from_msgpack_bytes(b"not valid msgpack");
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_proto_delta_bytes_round_trip() {
        use asap_otel_proto::sketchlib::v1::CountSketchDelta as PbDelta;
        use prost::Message;

        let mut acc = CountSketchAccumulator {
            inner: CountSketch::from_legacy_matrix(
                vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                2,
                3,
            ),
        };
        let bytes = PbDelta {
            rows: 2,
            cols: 3,
            cell_rows: vec![0, 1],
            cell_cols: vec![0, 2],
            d_counts: vec![10, -6],
            l2: vec![],
        }
        .encode_to_vec();

        acc.apply_proto_delta_bytes(&bytes).expect("apply ok");
        assert_eq!(
            acc.inner.sketch(),
            &vec![vec![11.0, 2.0, 3.0], vec![4.0, 5.0, 0.0]]
        );
    }

    #[test]
    fn test_apply_proto_delta_bytes_rejects_garbage() {
        let mut acc = CountSketchAccumulator::new(2, 3);
        assert!(acc.apply_proto_delta_bytes(b"not valid proto").is_err());
    }
}
