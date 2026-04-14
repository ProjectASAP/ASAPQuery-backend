//! DDSketch accumulator — wraps `sketch_core::dd_sketch::DdSketch`.
//!
//! Concrete accumulator reached from the modified-OTLP
//! `Metric.data = DDSketch{…}` hot path (PR C-CountSketch follow-up).
//! Merge via bucket-index alignment on the inner sketch, serialize as
//! MessagePack for the sink, and decode from the sketchlib
//! `DDSketchState` proto.
//!
//! Query semantics (quantile estimation via log-bucket indices) are
//! intentionally deferred — the wire format carries the bucket counts,
//! offset, and aggregates losslessly, so the merge + store round-trip
//! works end-to-end without that richer query surface.

use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
use serde_json::Value;
use sketch_core::dd_sketch::DdSketch;
use std::collections::HashMap;

/// DDSketch accumulator — inner log-bucketed sketch.
#[derive(Debug, Clone)]
pub struct DDSketchAccumulator {
    pub inner: DdSketch,
}

impl DDSketchAccumulator {
    pub fn new(alpha: f64) -> Self {
        Self {
            inner: DdSketch::new(alpha),
        }
    }

    /// Decode from the modified OTLP wire format's
    /// `DDSketchDataPoint.sketch` bytes when
    /// `encoding = DDSKETCH_ENCODING_MSGPACK`. The bytes are the
    /// MessagePack serialization of the cross-language sketch-core
    /// `DdSketch` struct — PR I parity entrypoint.
    pub fn from_msgpack_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: DdSketch::deserialize_msgpack(buffer)
                .map_err(|e| format!("deserialize DdSketch msgpack: {e}"))?,
        })
    }

    /// Decode from the modified OTLP wire format's
    /// `DDSketchDataPoint.sketch` bytes — the protobuf-encoded
    /// `asap_sketchlib::proto::sketchlib::DDSketchState` message that
    /// DataCollector's `ddsketchprocessor` emits when
    /// `encoding = DD_SKETCH_ENCODING_PROTO`.
    pub fn from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        use asap_sketchlib::proto::sketchlib::DdSketchState;
        use prost::Message;

        let state =
            DdSketchState::decode(buffer).map_err(|e| format!("decode DDSketchState: {e}"))?;
        if !(state.alpha > 0.0 && state.alpha < 1.0) {
            return Err(format!(
                "DDSketchState alpha {} out of range (expected 0 < alpha < 1)",
                state.alpha
            )
            .into());
        }
        let inner = DdSketch::from_raw(
            state.alpha,
            state.store_counts.clone(),
            state.store_offset,
            state.count,
            state.sum,
            state.min,
            state.max,
        );
        Ok(Self { inner })
    }
}

impl SerializableToSink for DDSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "alpha": self.inner.alpha,
            "store_offset": self.inner.store_offset,
            "bucket_count": self.inner.store_counts.len(),
            "count": self.inner.count,
            "sum": self.inner.sum,
            "min": self.inner.min,
            "max": self.inner.max,
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for DDSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "DDSketchAccumulator"
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
                "Cannot merge DDSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }
        let other_dd = other
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .ok_or("Failed to downcast to DDSketchAccumulator")?;
        let merged_inner = DdSketch::merge_refs(&[&self.inner, &other_dd.inner])?;
        Ok(Box::new(Self {
            inner: merged_inner,
        }))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::DDSketch
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        _statistic: promql_utilities::query_logics::enums::Statistic,
        _key: &Option<KeyByLabelValues>,
        _query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("DDSketchAccumulator: query_statistic not yet implemented \
             (bucket round-trip works, but quantile estimation deferred; \
              tracked as a PR C-CountSketch follow-up)"
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_state(
        alpha: f64,
        store_counts: Vec<u64>,
        store_offset: i32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    ) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::DdSketchState;
        use prost::Message;
        let state = DdSketchState {
            alpha,
            store_counts,
            store_offset,
            count,
            sum,
            min,
            max,
        };
        state.encode_to_vec()
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_round_trip() {
        let bytes = encode_state(0.01, vec![1, 2, 3, 4], -2, 10, 50.0, 1.0, 4.0);
        let acc = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.alpha, 0.01);
        assert_eq!(acc.inner.store_counts, vec![1, 2, 3, 4]);
        assert_eq!(acc.inner.store_offset, -2);
        assert_eq!(acc.inner.count, 10);
        assert_eq!(acc.inner.sum, 50.0);
        assert_eq!(acc.inner.min, 1.0);
        assert_eq!(acc.inner.max, 4.0);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_rejects_invalid_alpha() {
        let bytes = encode_state(0.0, vec![1], 0, 1, 1.0, 1.0, 1.0);
        let result = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("alpha"));
    }

    #[test]
    fn test_aggregate_core_merge_aligns_buckets() {
        let a = DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 1, 1], -1, 3, 3.0, 1.0, 3.0),
        };
        let b = DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![10, 10, 10], 0, 30, 30.0, 1.0, 3.0),
        };
        let merged_box = a.merge_with(&b).expect("merge ok");
        let merged = merged_box
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("downcast ok");
        assert_eq!(merged.inner.store_counts, vec![1, 11, 11, 10]);
        assert_eq!(merged.inner.store_offset, -1);
        assert_eq!(merged.inner.count, 33);
    }

    #[test]
    fn test_aggregate_core_merge_wrong_type_rejects() {
        use crate::precompute_operators::count_sketch_accumulator::CountSketchAccumulator;
        let dd = DDSketchAccumulator::new(0.01);
        let cs = CountSketchAccumulator::new(2, 3);
        assert!(dd.merge_with(&cs).is_err());
    }

    #[test]
    fn test_from_msgpack_bytes_round_trip() {
        let original = DdSketch::from_raw(0.01, vec![5, 10, 15, 20], -2, 50, 150.0, 0.25, 8.0);
        let bytes = original.serialize_msgpack();
        let acc = DDSketchAccumulator::from_msgpack_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.alpha, 0.01);
        assert_eq!(acc.inner.store_counts, vec![5, 10, 15, 20]);
        assert_eq!(acc.inner.store_offset, -2);
        assert_eq!(acc.inner.count, 50);
        assert_eq!(acc.inner.sum, 150.0);
    }

    #[test]
    fn test_from_msgpack_bytes_rejects_garbage() {
        let result = DDSketchAccumulator::from_msgpack_bytes(b"not valid msgpack");
        assert!(result.is_err());
    }
}
