//! DDSketch accumulator — wraps `asap_sketchlib::DdSketch`.
//!
//! Concrete accumulator reached from the modified-OTLP
//! `Metric.data = DDSketch{…}` hot path (PR C-CountSketch follow-up).
//! Merge via bucket-index alignment on the inner sketch, serialize as
//! MessagePack for the sink, and decode from the sketchlib
//! `DDSketchState` proto.
//!
//! Query semantics follow the STRICT policy after the DataPoint-level
//! METRIC scalars were dropped from the wire format
//! (ProjectASAP/sketchlib-go#243 / asap_sketchlib#57): the sketch serves
//! Quantile (log-bucket estimation) and Count (sum of bucket counts).
//! Sum/Min/Max are no longer derivable from the wire bytes and are
//! served by controller-provisioned exact aggregations — `query_statistic`
//! returns the unavailable-statistic error for them.

use crate::storage_engines::types::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
use asap_sketchlib::{DdSketch, DdSketchDelta, MessagePackCodec};
use serde_json::Value;
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
            inner: DdSketch::from_msgpack(buffer)
                .map_err(|e| format!("deserialize DdSketch msgpack: {e}"))?,
        })
    }

    /// Decode from the modified OTLP wire format's
    /// `DDSketchDataPoint.sketch` bytes — the protobuf-encoded
    /// `asap_sketchlib::proto::sketchlib::DDSketchState` message that
    /// DataCollector's `ddsketchprocessor` emits when
    /// `encoding = DD_SKETCH_ENCODING_PROTO`.
    pub fn from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
        use prost::Message;

        // DataCollector's ddsketchprocessor wraps the state in a
        // `SketchEnvelope{ddsketch: DdSketchState}` via sketchlib-go's
        // `SerializePortableFO` + `proto.Marshal`. Try envelope first,
        // fall back to bare `DdSketchState` for callers (e.g. unit
        // tests) that encode the state directly. Mirrors the PR #14
        // fix on `CountMinSketchAccumulator::from_sketchlib_proto_bytes`.
        let state = match SketchEnvelope::decode(buffer) {
            Ok(env) => match env.sketch_state {
                Some(sketch_envelope::SketchState::Ddsketch(st)) => st,
                Some(other) => {
                    return Err(format!(
                        "SketchEnvelope contains non-DDSketch sketch: {:?}",
                        std::mem::discriminant(&other)
                    )
                    .into());
                }
                None => DdSketchState::decode(buffer)
                    .map_err(|e| format!("decode DDSketchState: {e}"))?,
            },
            Err(_) => {
                DdSketchState::decode(buffer).map_err(|e| format!("decode DDSketchState: {e}"))?
            }
        };
        if !(state.alpha > 0.0 && state.alpha < 1.0) {
            return Err(format!(
                "DDSketchState alpha {} out of range (expected 0 < alpha < 1)",
                state.alpha
            )
            .into());
        }
        // The DataPoint-level METRIC scalars (count/sum/min/max) were
        // dropped from `DDSketchState` (ProjectASAP/sketchlib-go#243 /
        // asap_sketchlib#57). Reconstruct from the bucket store only:
        // `DdSketch::from_raw` now takes just (alpha, store_counts,
        // store_offset) and recovers `count` by summing the bucket
        // counts via `total_count()`.
        let inner = DdSketch::from_raw(state.alpha, state.store_counts.clone(), state.store_offset);
        Ok(Self { inner })
    }

    /// Apply a proto-encoded `DDSketchDelta` frame to this
    /// accumulator's inner sketch — the decode path for
    /// `DD_SKETCH_ENCODING_PROTO_DELTA` (paper §6.2 B3 / B4).
    ///
    /// Called against an accumulator that already carries the base
    /// sketch state; the caller is the per-series snapshot cache in
    /// the ingest path. Bytes are the
    /// `asap_otel_proto::sketchlib::v1::DdSketchDelta` message.
    pub fn apply_proto_delta_bytes(
        &mut self,
        buffer: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use asap_otel_proto::sketchlib::v1::DdSketchDelta as PbDelta;
        use prost::Message;

        let pb = PbDelta::decode(buffer).map_err(|e| format!("decode DDSketchDelta: {e}"))?;

        // The delta no longer carries d_count/d_sum/min/max
        // (ProjectASAP/sketchlib-go#243 / asap_sketchlib#57). Apply the
        // bucket deltas only; `DdSketch` recomputes its total count from
        // the merged bucket counts (`total_count()`).
        let buckets = pb
            .buckets
            .into_iter()
            .map(|b| (b.index, b.d_count))
            .collect();
        let delta = DdSketchDelta {
            buckets,
            ..Default::default()
        };
        self.inner.apply_delta(&delta);
        Ok(())
    }
}

impl SerializableToSink for DDSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        // The DataPoint-level scalars (sum/min/max) are no longer carried
        // by `DdSketch` (ProjectASAP/sketchlib-go#243 / asap_sketchlib#57).
        // `count` is the bucket-derived total via `total_count()`.
        serde_json::json!({
            "alpha": self.inner.alpha,
            "store_offset": self.inner.store_offset,
            "bucket_count": self.inner.store_counts.len(),
            "count": self.inner.total_count(),
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.to_msgpack().unwrap_or_default()
    }
}

impl AggregateCore for DDSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "DDSketchAccumulator"
    }

    /// Per-window base rotation: drop all bucket counts but keep the
    /// relative-accuracy parameter so the next window's bucket deltas
    /// index into the same log-bucket layout.
    fn reset_to_empty(&mut self) {
        self.inner = DdSketch::new(self.inner.alpha);
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
        statistic: promql_utilities::query_logics::enums::Statistic,
        _key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use promql_utilities::query_logics::enums::Statistic;

        match statistic {
            Statistic::Quantile => {
                // PromQL `histogram_quantile(q, …)` and
                // `quantile_over_time(q, …)` both land here with
                // `q` in `query_kwargs["quantile"]`. Default to
                // 0.99 when the caller didn't provide one
                // (defensive — pattern-matched queries in
                // `inference_config.yaml` always populate it).
                let q: f64 = query_kwargs
                    .get("quantile")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.99);
                if !(0.0..=1.0).contains(&q) {
                    return Err(format!("DDSketchAccumulator: quantile {q} out of [0,1]").into());
                }
                self.inner.quantile(q).ok_or_else(|| {
                    "DDSketchAccumulator: quantile() returned None (sketch empty?)".into()
                })
            }
            // Count is exact, derived by summing the bucket store
            // counts — the only DataPoint-level scalar that survives
            // the wire-format trim (ProjectASAP/sketchlib-go#243 /
            // asap_sketchlib#57).
            Statistic::Count => Ok(self.inner.total_count() as f64),
            // STRICT policy: the Sum/Min/Max scalars were removed from
            // the DDSketch wire format. They are now served by the
            // controller-provisioned exact aggregations (an exact `Sum`
            // and an exact `MinMax`), NOT estimated from the buckets.
            // Surface the unavailable-statistic error so the query path
            // routes to those aggregations instead of returning a wrong
            // (0 / panicked) value.
            Statistic::Sum => Err(
                "DDSketchAccumulator: Sum not available from DDSketch wire format \
                 (ProjectASAP/sketchlib-go#243); use an exact Sum aggregation"
                    .into(),
            ),
            Statistic::Min | Statistic::Max => Err(format!(
                "DDSketchAccumulator: {statistic:?} not available from DDSketch wire format \
                 (ProjectASAP/sketchlib-go#243); use an exact MinMax aggregation",
            )
            .into()),
            other => Err(format!(
                "DDSketchAccumulator: statistic {other:?} not supported (only Quantile / Count)",
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The DataPoint-level METRIC scalars (count/sum/min/max) were dropped
    // from `DdSketchState` (ProjectASAP/sketchlib-go#243 /
    // asap_sketchlib#57); the proto now carries only
    // `alpha`/`store_counts`/`store_offset`.
    fn encode_state(alpha: f64, store_counts: Vec<u64>, store_offset: i32) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::DdSketchState;
        use prost::Message;
        let state = DdSketchState {
            alpha,
            store_counts,
            store_offset,
        };
        state.encode_to_vec()
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_round_trip() {
        let bytes = encode_state(0.01, vec![1, 2, 3, 4], -2);
        let acc = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.alpha, 0.01);
        assert_eq!(acc.inner.store_counts, vec![1, 2, 3, 4]);
        assert_eq!(acc.inner.store_offset, -2);
        // `count` is recovered by summing the bucket store counts.
        assert_eq!(acc.inner.total_count(), 10);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_rejects_invalid_alpha() {
        let bytes = encode_state(0.0, vec![1], 0);
        let result = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("alpha"));
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrapped() {
        // Mirrors what DataCollector's ddsketchprocessor emits: the
        // state wrapped in a `SketchEnvelope{ddsketch: ...}` via
        // sketchlib-go's `SerializePortableFO` + `proto.Marshal`.
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
        use prost::Message;

        let state = DdSketchState {
            alpha: 0.01,
            store_counts: vec![1, 2, 3, 4],
            store_offset: -2,
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let acc = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("envelope-wrapped decode should succeed");
        assert_eq!(acc.inner.alpha, 0.01);
        assert_eq!(acc.inner.total_count(), 10);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrong_sketch_type() {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;

        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(KllState::default())),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let result = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err(), "wrong-sketch envelope should error");
    }

    #[test]
    fn test_aggregate_core_merge_aligns_buckets() {
        let a = DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 1, 1], -1),
        };
        let b = DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![10, 10, 10], 0),
        };
        let merged_box = a.merge_with(&b).expect("merge ok");
        let merged = merged_box
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("downcast ok");
        assert_eq!(merged.inner.store_counts, vec![1, 11, 11, 10]);
        assert_eq!(merged.inner.store_offset, -1);
        assert_eq!(merged.inner.total_count(), 33);
    }

    #[test]
    fn test_aggregate_core_merge_wrong_type_rejects() {
        use crate::precompute_engine::operators::count_sketch_accumulator::CountSketchAccumulator;
        let dd = DDSketchAccumulator::new(0.01);
        let cs = CountSketchAccumulator::new(2, 3);
        assert!(dd.merge_with(&cs).is_err());
    }

    #[test]
    fn test_from_msgpack_bytes_round_trip() {
        let original = DdSketch::from_raw(0.01, vec![5, 10, 15, 20], -2);
        let bytes = original.to_msgpack().unwrap();
        let acc = DDSketchAccumulator::from_msgpack_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.alpha, 0.01);
        assert_eq!(acc.inner.store_counts, vec![5, 10, 15, 20]);
        assert_eq!(acc.inner.store_offset, -2);
        // `count` is recovered by summing the bucket store counts.
        assert_eq!(acc.inner.total_count(), 50);
    }

    #[test]
    fn test_from_msgpack_bytes_rejects_garbage() {
        let result = DDSketchAccumulator::from_msgpack_bytes(b"not valid msgpack");
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_proto_delta_bytes_round_trip() {
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use prost::Message;

        let mut acc = DDSketchAccumulator::new(0.01);
        acc.inner = DdSketch::from_raw(0.01, vec![1, 2, 3], 0);

        // The wire delta now carries only bucket deltas (tags 2-7
        // reserved); `DdSketchBucketDelta` has just `index` + `d_count`.
        let bytes = PbDelta {
            buckets: vec![
                DdSketchBucketDelta {
                    index: 0,
                    d_count: 10,
                },
                DdSketchBucketDelta {
                    index: 2,
                    d_count: 20,
                },
            ],
        }
        .encode_to_vec();

        acc.apply_proto_delta_bytes(&bytes).expect("apply ok");
        assert_eq!(acc.inner.store_counts, vec![11, 2, 23]);
        // `count` recomputed from the merged buckets: 11 + 2 + 23 = 36.
        assert_eq!(acc.inner.total_count(), 36);
    }

    #[test]
    fn test_apply_proto_delta_bytes_rejects_garbage() {
        let mut acc = DDSketchAccumulator::new(0.01);
        assert!(acc.apply_proto_delta_bytes(b"not valid proto").is_err());
    }

    // ----- query_statistic STRICT policy -----
    //
    // After the DataPoint-level METRIC scalars were dropped from the
    // DDSketch wire format (ProjectASAP/sketchlib-go#243 /
    // asap_sketchlib#57), DDSketch serves only quantiles and Count.
    // Sum/Min/Max move to controller-provisioned exact aggregations and
    // MUST surface the unavailable-statistic error (never a panic / 0).

    fn sample_accumulator() -> DDSketchAccumulator {
        // Build the in-memory sketch from bucket counts only — no scalars.
        DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 2, 3, 4], -2),
        }
    }

    #[test]
    fn test_query_statistic_quantile_is_sketch_derived() {
        use promql_utilities::query_logics::enums::Statistic;
        let acc = sample_accumulator();
        let mut kwargs = HashMap::new();
        kwargs.insert("quantile".to_string(), "0.5".to_string());
        let v = acc
            .query_statistic(Statistic::Quantile, &None, &kwargs)
            .expect("quantile should be served from the sketch buckets");
        assert!(
            v.is_finite() && v > 0.0,
            "quantile estimate should be positive finite, got {v}"
        );
    }

    #[test]
    fn test_query_statistic_count_is_bucket_derived() {
        use promql_utilities::query_logics::enums::Statistic;
        let acc = sample_accumulator();
        let v = acc
            .query_statistic(Statistic::Count, &None, &HashMap::new())
            .expect("count should be derivable from the bucket store");
        // 1 + 2 + 3 + 4 = 10.
        assert_eq!(v, 10.0);
    }

    #[test]
    fn test_query_statistic_sum_min_max_return_unavailable_error() {
        use promql_utilities::query_logics::enums::Statistic;
        let acc = sample_accumulator();
        for stat in [Statistic::Sum, Statistic::Min, Statistic::Max] {
            let result = acc.query_statistic(stat, &None, &HashMap::new());
            assert!(
                result.is_err(),
                "{stat:?} must return the unavailable-statistic error (not a panic / 0)"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not available"),
                "{stat:?} error should explain the statistic is unavailable, got: {msg}"
            );
        }
    }
}
