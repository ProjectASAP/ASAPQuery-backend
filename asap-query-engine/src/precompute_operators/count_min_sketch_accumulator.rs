use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, SerializableToSink,
};
use serde_json::Value;
use sketch_core::count_min::CountMinSketch;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Count-Min Sketch accumulator — wraps sketch_core::CountMinSketch.
/// Core struct, update/merge/serde logic live in sketch-core.
/// This file retains QE-specific trait impls, legacy deserializers, and JSON output.
#[derive(Debug, Clone)]
pub struct CountMinSketchAccumulator {
    pub inner: CountMinSketch,
}

impl CountMinSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            inner: CountMinSketch::new(row_num, col_num),
        }
    }

    // Marked as _update and kept private; only called internally.
    fn _update(&mut self, key: &KeyByLabelValues, value: f64) {
        self.inner.update(&key.to_semicolon_str(), value);
    }

    pub fn query_key(&self, key: &KeyByLabelValues) -> f64 {
        self.inner.query_key(&key.to_semicolon_str())
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let row_num = data["row_num"]
            .as_f64()
            .ok_or("Missing or invalid 'row_num' field")? as usize;
        let col_num = data["col_num"]
            .as_f64()
            .ok_or("Missing or invalid 'col_num' field")? as usize;

        let sketch_data = data["sketch"]
            .as_array()
            .ok_or("Missing or invalid 'sketch' field")?;

        let mut sketch = Vec::new();
        for row in sketch_data {
            let row_array = row.as_array().ok_or("Invalid row in sketch data")?;
            let mut sketch_row = Vec::new();
            for cell in row_array {
                let value = cell.as_f64().ok_or("Invalid cell value in sketch data")?;
                sketch_row.push(value);
            }
            sketch.push(sketch_row);
        }

        Ok(Self {
            inner: CountMinSketch::from_legacy_matrix(sketch, row_num, col_num),
        })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: CountMinSketch::deserialize_msgpack(buffer)?,
        })
    }

    /// Decode from the modified OTLP wire format's
    /// `CountMinSketchDataPoint.sketch` bytes when
    /// `encoding = COUNT_MIN_SKETCH_ENCODING_MSGPACK`. The bytes are the
    /// MessagePack serialization of the cross-language sketch-core
    /// `CountMinSketch` wire struct (same format the legacy Arroyo path
    /// uses — this method is the modified-OTLP entrypoint for PR I).
    pub fn from_msgpack_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: CountMinSketch::deserialize_msgpack(buffer)?,
        })
    }

    /// Decode from the modified OTLP wire format's
    /// `CountMinSketchDataPoint.sketch` bytes — i.e. the protobuf-encoded
    /// `asap_sketchlib::proto::sketchlib::CountMinState` message used by
    /// DataCollector's `countminsketchprocessor` when emitting via
    /// `Metric.data = CountMinSketch{…}` with
    /// `encoding = COUNT_MIN_SKETCH_ENCODING_PROTO`.
    ///
    /// The resulting accumulator is constructed via
    /// `CountMinSketch::from_legacy_matrix` after reshaping the flat
    /// `counts_int` / `counts_float` field into a `Vec<Vec<f64>>`.
    pub fn from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountMinState, CounterType, SketchEnvelope,
        };
        use prost::Message;

        // DataCollector's countminsketchprocessor wraps the state in a
        // `SketchEnvelope{count_min: CountMinState}` via
        // `SerializePortableFO` + `proto.Marshal`. Try decoding as envelope
        // first, fall back to bare `CountMinState` for callers (e.g. unit
        // tests) that encode the state directly.
        let state = match SketchEnvelope::decode(buffer) {
            Ok(env) => match env.sketch_state {
                Some(sketch_envelope::SketchState::CountMin(st)) => st,
                Some(other) => {
                    return Err(format!(
                        "SketchEnvelope contains non-CountMin sketch: {:?}",
                        std::mem::discriminant(&other)
                    )
                    .into());
                }
                // Envelope decoded but was empty (e.g. the buffer is a
                // bare CountMinState that happened to parse as a default
                // envelope). Fall through to bare decode.
                None => CountMinState::decode(buffer)
                    .map_err(|e| format!("decode CountMinState: {e}"))?,
            },
            Err(_) => {
                CountMinState::decode(buffer).map_err(|e| format!("decode CountMinState: {e}"))?
            }
        };
        let rows = state.rows as usize;
        let cols = state.cols as usize;
        if rows == 0 || cols == 0 {
            return Err(format!("CountMinState has zero dims (rows={rows}, cols={cols})").into());
        }
        let expected_len = rows * cols;
        let counter_type = CounterType::try_from(state.counter_type).map_err(|_| {
            format!(
                "CountMinState has unknown counter_type tag {}",
                state.counter_type
            )
        })?;
        let flat: Vec<f64> = match counter_type {
            CounterType::Int32 | CounterType::Int64 => {
                if state.counts_int.len() != expected_len {
                    return Err(format!(
                        "CountMinState counts_int has {} entries, expected rows*cols = {}",
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
                        "CountMinState counts_float has {} entries, expected rows*cols = {}",
                        state.counts_float.len(),
                        expected_len
                    )
                    .into());
                }
                state.counts_float.clone()
            }
            // INT128 stores (hi, lo) pairs and would have 2 * rows * cols
            // entries in counts_int; defer to PR C if a producer ever uses it.
            other => {
                return Err(format!(
                    "CountMinState counter_type {other:?} not yet supported \
                     (PR C will extend coverage)"
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
            inner: CountMinSketch::from_legacy_matrix(matrix, rows, cols),
        })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 8 {
            return Err("Buffer too short for row_num and col_num".into());
        }

        // TODO: this logic will need to be checked for i32 -> f64
        // Github Issue #11

        let row_num = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let col_num = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) as usize;

        let expected_size = 8 + (row_num * col_num * 4);
        if buffer.len() < expected_size {
            return Err("Buffer too short for sketch data".into());
        }

        let mut sketch = Vec::new();
        let mut offset = 8;

        for _ in 0..row_num {
            let mut row = Vec::new();
            for _ in 0..col_num {
                let value = f64::from_le_bytes([
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                    buffer[offset + 5],
                    buffer[offset + 6],
                    buffer[offset + 7],
                ]);
                row.push(value);
                offset += 8;
            }
            sketch.push(row);
        }

        Ok(Self {
            inner: CountMinSketch::from_legacy_matrix(sketch, row_num, col_num),
        })
    }

    /// Merge multiple accumulators efficiently without cloning all of them.
    pub fn merge_multiple(
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut cms_accumulators = Vec::with_capacity(accumulators.len());
        for acc in accumulators {
            if acc.get_accumulator_type() != AggregationType::CountMinSketch {
                return Err(format!(
                    "Cannot merge CountMinSketchAccumulator with {:?}",
                    acc.get_accumulator_type()
                )
                .into());
            }
            let cms_acc = acc
                .as_any()
                .downcast_ref::<CountMinSketchAccumulator>()
                .ok_or("Failed to downcast to CountMinSketchAccumulator")?;
            cms_accumulators.push(cms_acc);
        }

        // Check dimensions are consistent
        let row_num = cms_accumulators[0].inner.row_num;
        let col_num = cms_accumulators[0].inner.col_num;
        for acc in &cms_accumulators {
            if acc.inner.row_num != row_num || acc.inner.col_num != col_num {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let inner_refs: Vec<&CountMinSketch> =
            cms_accumulators.iter().map(|acc| &acc.inner).collect();
        let merged_inner = CountMinSketch::merge_refs(&inner_refs)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

impl SerializableToSink for CountMinSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "row_num": self.inner.row_num,
            "col_num": self.inner.col_num,
            "sketch": self.inner.sketch()
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for CountMinSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "CountMinSketchAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge CountMinSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_cms = other
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .ok_or("Failed to downcast to CountMinSketchAccumulator")?;

        let merged_inner = CountMinSketch::merge_refs(&[&self.inner, &other_cms.inner])?;
        Ok(Box::new(Self {
            inner: merged_inner,
        }))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::CountMinSketch
    }

    fn approx_memory_bytes(&self) -> usize {
        // Conservative constant for the CountMinSketch counter matrix.
        // Real per-instance sizing would require exposing rows/cols on
        // the inner sketch; 16 KiB is a reasonable v1 default.
        16 * 1024
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        key: &Option<crate::KeyByLabelValues>,
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::MultipleSubpopulationAggregate;
        let key_val = key
            .as_ref()
            .ok_or("Key required for CountMinSketchAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
    }
}

impl MultipleSubpopulationAggregate for CountMinSketchAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.query_key(key))
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<CountMinSketchAccumulator> for CountMinSketchAccumulator {
    fn merge_accumulators(
        accumulators: Vec<CountMinSketchAccumulator>,
    ) -> Result<CountMinSketchAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }
        let inners: Vec<CountMinSketch> = accumulators.into_iter().map(|acc| acc.inner).collect();
        let merged_inner = CountMinSketch::merge(inners)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_creation() {
        let cms = CountMinSketchAccumulator::new(4, 1000);
        assert_eq!(cms.inner.row_num, 4);
        assert_eq!(cms.inner.col_num, 1000);
        let sketch = cms.inner.sketch();
        assert_eq!(sketch.len(), 4);
        assert_eq!(sketch[0].len(), 1000);

        for row in &sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_update() {
        let mut cms = CountMinSketchAccumulator::new(2, 10);
        let key = KeyByLabelValues::new();
        cms._update(&key, 1.0);
        let result = cms.query_key(&key);
        assert!(result >= 1.0);
    }

    #[test]
    fn test_count_min_sketch_query() {
        let cms = CountMinSketchAccumulator::new(2, 10);
        let key = KeyByLabelValues::new();
        assert_eq!(cms.query_key(&key), 0.0);

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        assert_eq!(multi_trait.query(Statistic::Sum, &key, None).unwrap(), 0.0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        // Build controlled state via from_legacy_matrix (works for both Legacy and Sketchlib backends).
        let cms1 = CountMinSketchAccumulator {
            inner: CountMinSketch::from_legacy_matrix(
                vec![vec![5.0, 0.0, 0.0], vec![0.0, 0.0, 10.0]],
                2,
                3,
            ),
        };
        let cms2 = CountMinSketchAccumulator {
            inner: CountMinSketch::from_legacy_matrix(
                vec![vec![3.0, 7.0, 0.0], vec![0.0, 0.0, 0.0]],
                2,
                3,
            ),
        };

        let merged = CountMinSketchAccumulator::merge_accumulators(vec![cms1, cms2]).unwrap();

        let merged_sketch = merged.inner.sketch();
        assert_eq!(merged_sketch[0][0], 8.0);
        assert_eq!(merged_sketch[0][1], 7.0);
        assert_eq!(merged_sketch[1][2], 10.0);
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let cms1 = CountMinSketchAccumulator::new(2, 3);
        let cms2 = CountMinSketchAccumulator::new(3, 3);
        let result = CountMinSketchAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_count_min_sketch_serialization() {
        let cms = CountMinSketchAccumulator {
            inner: CountMinSketch::from_legacy_matrix(
                vec![vec![0.0, 42.0, 0.0], vec![0.0, 0.0, 100.0]],
                2,
                3,
            ),
        };

        let bytes = cms.serialize_to_bytes();
        let deserialized =
            CountMinSketchAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized.inner.row_num, 2);
        assert_eq!(deserialized.inner.col_num, 3);
        let deser_sketch = deserialized.inner.sketch();
        assert_eq!(deser_sketch[0][1], 42.0);
        assert_eq!(deser_sketch[1][2], 100.0);
    }

    #[test]
    fn test_count_min_sketch_as_aggregate_core() {
        let cms = CountMinSketchAccumulator::new(2, 3);
        assert_eq!(cms.type_name(), "CountMinSketchAccumulator");
    }

    #[test]
    fn test_trait_object() {
        let cms = CountMinSketchAccumulator::new(2, 3);
        let trait_obj: Box<dyn AggregateCore> = Box::new(cms);
        assert_eq!(trait_obj.type_name(), "CountMinSketchAccumulator");
    }

    #[test]
    fn test_count_min_sketch_key_query() {
        let mut cms = CountMinSketchAccumulator::new(4, 100);
        let key = KeyByLabelValues::new();
        assert_eq!(cms.query_key(&key), 0.0);
        cms._update(&key, 5.0);
        let result = cms.query_key(&key);
        assert!(result >= 5.0);
    }

    #[test]
    fn test_update_and_query_use_same_key_encoding() {
        // Regression test: _update and query_key must hash the same key string.
        // Previously _update went through serialize_to_json (which returns a JSON
        // array, so as_object() is always None) and always stored under key "".
        // query_key correctly used key.labels.join(";"), so they never matched.
        let mut cms = CountMinSketchAccumulator::new(4, 1000);
        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string(), "prod".to_string()]);
        cms._update(&key, 5.0);
        let result = cms.query_key(&key);
        assert!(
            result >= 5.0,
            "_update and query_key used different key encodings: got {result}"
        );

        // Also verify a different key does not interfere.
        let other_key = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);
        // other_key was never updated; its estimate should be lower than key's.
        let other_result = cms.query_key(&other_key);
        // In a sketch this large there should be no collision, so other_result == 0.
        assert_eq!(
            other_result, 0.0,
            "unrelated key returned non-zero: {other_result}"
        );
    }

    #[test]
    fn test_multiple_subpopulation_aggregate() {
        let mut cms = CountMinSketchAccumulator::new(3, 50);
        let key = KeyByLabelValues::new();
        cms._update(&key, 10.0);

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        let result = multi_trait.query(Statistic::Sum, &key, None).unwrap();
        assert!(result >= 10.0);

        let keys = multi_trait.get_keys();
        assert!(keys.is_none());
    }

    #[test]
    fn test_count_min_sketch_merge_multiple() {
        // Build controlled state via from_legacy_matrix (works for both Legacy and Sketchlib backends).
        let cms1 = CountMinSketchAccumulator {
            inner: CountMinSketch::from_legacy_matrix(
                vec![vec![5.0, 0.0, 0.0], vec![0.0, 0.0, 10.0]],
                2,
                3,
            ),
        };
        let cms2 = CountMinSketchAccumulator {
            inner: CountMinSketch::from_legacy_matrix(
                vec![vec![3.0, 7.0, 0.0], vec![0.0, 0.0, 0.0]],
                2,
                3,
            ),
        };
        let cms3 = CountMinSketchAccumulator {
            inner: CountMinSketch::from_legacy_matrix(
                vec![vec![2.0, 0.0, 0.0], vec![0.0, 0.0, 5.0]],
                2,
                3,
            ),
        };

        let boxed_accs: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(cms1), Box::new(cms2), Box::new(cms3)];

        let merged = CountMinSketchAccumulator::merge_multiple(&boxed_accs).unwrap();

        let merged_sketch = merged.inner.sketch();
        assert_eq!(merged_sketch[0][0], 10.0);
        assert_eq!(merged_sketch[0][1], 7.0);
        assert_eq!(merged_sketch[1][2], 15.0);
    }

    #[test]
    fn test_count_min_sketch_merge_multiple_error_cases() {
        let empty: Vec<Box<dyn AggregateCore>> = vec![];
        assert!(CountMinSketchAccumulator::merge_multiple(&empty).is_err());

        let cms1 = CountMinSketchAccumulator::new(2, 3);
        let cms2 = CountMinSketchAccumulator::new(3, 3);
        let boxed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms1), Box::new(cms2)];
        assert!(CountMinSketchAccumulator::merge_multiple(&boxed_accs).is_err());

        use crate::precompute_operators::sum_accumulator::SumAccumulator;
        let cms = CountMinSketchAccumulator::new(2, 3);
        let sum = SumAccumulator::new();
        let mixed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms), Box::new(sum)];
        assert!(CountMinSketchAccumulator::merge_multiple(&mixed_accs).is_err());
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_int64() {
        // Hand-build a CountMinState proto with INT64 counters and verify
        // round-tripping through from_sketchlib_proto_bytes yields the same
        // matrix that the modified-OTLP wire format would carry.
        use asap_sketchlib::proto::sketchlib::{CountMinState, CounterType};
        use prost::Message;

        let rows = 2u32;
        let cols = 3u32;
        // Row-major: row 0 = [1,2,3], row 1 = [4,5,6]
        let counts_int: Vec<i64> = vec![1, 2, 3, 4, 5, 6];
        let state = CountMinState {
            rows,
            cols,
            counter_type: CounterType::Int64 as i32,
            counts_int: counts_int.clone(),
            counts_float: Vec::new(),
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1: Vec::new(),
            l2: Vec::new(),
        };
        let bytes = state.encode_to_vec();

        let acc = CountMinSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        let matrix = acc.inner.sketch();
        assert_eq!(matrix.len(), rows as usize);
        assert_eq!(matrix[0], vec![1.0, 2.0, 3.0]);
        assert_eq!(matrix[1], vec![4.0, 5.0, 6.0]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrapped() {
        // Mirrors what DataCollector's countminsketchprocessor emits:
        // the state is wrapped in a `SketchEnvelope{count_min: ...}`
        // via sketchlib-go's `SerializePortableFO` + `proto.Marshal`.
        // Before the fix, the Rust decoder decoded the envelope bytes as
        // a bare CountMinState, which produced "invalid wire type"
        // errors on field `cols` and silently fell through to §5.2.
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountMinState, CounterType, SketchEnvelope,
        };
        use prost::Message;

        let state = CountMinState {
            rows: 2,
            cols: 3,
            counter_type: CounterType::Int64 as i32,
            counts_int: vec![7, 8, 9, 10, 11, 12],
            counts_float: Vec::new(),
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1: Vec::new(),
            l2: Vec::new(),
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::CountMin(state)),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let acc = CountMinSketchAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("envelope-wrapped decode should succeed");
        let matrix = acc.inner.sketch();
        assert_eq!(matrix[0], vec![7.0, 8.0, 9.0]);
        assert_eq!(matrix[1], vec![10.0, 11.0, 12.0]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrong_sketch_type() {
        // An envelope carrying a non-CountMin sketch should be rejected
        // with a clear error rather than silently producing garbage.
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;

        let kll = KllState::default();
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(kll)),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let result = CountMinSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err(), "wrong-sketch envelope should error");
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_float64() {
        use asap_sketchlib::proto::sketchlib::{CountMinState, CounterType};
        use prost::Message;

        let state = CountMinState {
            rows: 2,
            cols: 2,
            counter_type: CounterType::Float64 as i32,
            counts_int: Vec::new(),
            counts_float: vec![1.5, 2.5, 3.5, 4.5],
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1: Vec::new(),
            l2: Vec::new(),
        };
        let bytes = state.encode_to_vec();

        let acc = CountMinSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        let matrix = acc.inner.sketch();
        assert_eq!(matrix[0], vec![1.5, 2.5]);
        assert_eq!(matrix[1], vec![3.5, 4.5]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_dimension_mismatch() {
        // counts_int has 5 entries but rows*cols = 6 → expect error
        use asap_sketchlib::proto::sketchlib::{CountMinState, CounterType};
        use prost::Message;

        let state = CountMinState {
            rows: 2,
            cols: 3,
            counter_type: CounterType::Int64 as i32,
            counts_int: vec![1, 2, 3, 4, 5],
            counts_float: Vec::new(),
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1: Vec::new(),
            l2: Vec::new(),
        };
        let bytes = state.encode_to_vec();

        let result = CountMinSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("counts_int"),
            "error should mention counts_int dim mismatch"
        );
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_zero_dims_rejected() {
        use asap_sketchlib::proto::sketchlib::CountMinState;
        use prost::Message;

        let state = CountMinState::default();
        let bytes = state.encode_to_vec();

        let result = CountMinSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("zero dims"));
    }
}
