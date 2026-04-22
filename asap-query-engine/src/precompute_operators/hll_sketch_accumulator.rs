//! HLL accumulator — wraps `sketch_core::hll_sketch::HllSketch`.
//!
//! Concrete accumulator reached from the modified-OTLP
//! `Metric.data = HLLSketch{…}` hot path (PR C-CountSketch follow-up).
//! Mirrors the CountSketch accumulator's shape: merge via register-wise
//! max on the inner sketch, serialize as MessagePack for the sink, and
//! decode from the sketchlib `HyperLogLogState` proto.
//!
//! Query semantics (cardinality estimation via the three HLL variants'
//! estimators) are intentionally deferred — the wire format carries the
//! registers + variant + HIP accumulators losslessly, so the merge +
//! store round-trip works end-to-end without that richer query surface.

use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
use serde_json::Value;
use sketch_core::hll_sketch::{HllDelta, HllSketch, HllVariant};
use std::collections::HashMap;

/// HLL accumulator — inner register array + variant metadata.
#[derive(Debug, Clone)]
pub struct HllSketchAccumulator {
    pub inner: HllSketch,
}

impl HllSketchAccumulator {
    pub fn new(variant: HllVariant, precision: u32) -> Self {
        Self {
            inner: HllSketch::new(variant, precision),
        }
    }

    /// Decode from the modified OTLP wire format's
    /// `HLLSketchDataPoint.sketch` bytes when
    /// `encoding = HLL_SKETCH_ENCODING_MSGPACK`. The bytes are the
    /// MessagePack serialization of the cross-language sketch-core
    /// `HllSketch` struct — PR I parity entrypoint.
    pub fn from_msgpack_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: HllSketch::deserialize_msgpack(buffer)
                .map_err(|e| format!("deserialize HllSketch msgpack: {e}"))?,
        })
    }

    /// Decode from the modified OTLP wire format's
    /// `HLLSketchDataPoint.sketch` bytes — the protobuf-encoded
    /// `asap_sketchlib::proto::sketchlib::HyperLogLogState` message
    /// that DataCollector's `hllprocessor` emits when
    /// `encoding = HLL_SKETCH_ENCODING_PROTO`.
    pub fn from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use prost::Message;

        // DataCollector's hllprocessor wraps the state in a
        // `SketchEnvelope{hll: HyperLogLogState}` via sketchlib-go's
        // `SerializePortableFO` + `proto.Marshal`. Try envelope first,
        // fall back to bare `HyperLogLogState` for callers (e.g. unit
        // tests) that encode the state directly. Mirrors the PR #14
        // fix on `CountMinSketchAccumulator::from_sketchlib_proto_bytes`.
        let state = match SketchEnvelope::decode(buffer) {
            Ok(env) => match env.sketch_state {
                Some(sketch_envelope::SketchState::Hll(st)) => st,
                Some(other) => {
                    return Err(format!(
                        "SketchEnvelope contains non-HLL sketch: {:?}",
                        std::mem::discriminant(&other)
                    )
                    .into());
                }
                None => HyperLogLogState::decode(buffer)
                    .map_err(|e| format!("decode HyperLogLogState: {e}"))?,
            },
            Err(_) => HyperLogLogState::decode(buffer)
                .map_err(|e| format!("decode HyperLogLogState: {e}"))?,
        };
        if state.precision == 0 || state.precision > 20 {
            return Err(format!(
                "HyperLogLogState precision {} out of range (expected 1..=20)",
                state.precision
            )
            .into());
        }
        let expected_len = 1usize << state.precision;
        if state.registers.len() != expected_len {
            return Err(format!(
                "HyperLogLogState registers has {} bytes, expected 2^precision = {}",
                state.registers.len(),
                expected_len
            )
            .into());
        }
        let proto_variant = ProtoVariant::try_from(state.variant)
            .map_err(|_| format!("HyperLogLogState has unknown variant tag {}", state.variant))?;
        let variant = match proto_variant {
            ProtoVariant::Unspecified => HllVariant::Unspecified,
            ProtoVariant::Regular => HllVariant::Regular,
            ProtoVariant::Datafusion => HllVariant::Datafusion,
            ProtoVariant::Hip => HllVariant::Hip,
        };
        let inner = HllSketch::from_raw(
            variant,
            state.precision,
            state.registers.clone(),
            state.hip_kxq0,
            state.hip_kxq1,
            state.hip_est,
        );
        Ok(Self { inner })
    }

    /// Apply a proto-encoded `HLLDelta` frame to this accumulator's
    /// inner sketch — the decode path for
    /// `HLL_SKETCH_ENCODING_PROTO_DELTA` (paper §6.2 B3 / B4).
    ///
    /// Called against an accumulator that already carries the base
    /// sketch state; the caller is the per-series snapshot cache in
    /// the ingest path. Bytes are the
    /// `asap_otel_proto::sketchlib::v1::HllDelta` message.
    pub fn apply_proto_delta_bytes(
        &mut self,
        buffer: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use asap_otel_proto::sketchlib::v1::HllDelta as PbDelta;
        use prost::Message;

        let pb = PbDelta::decode(buffer)
            .map_err(|e| format!("decode HLLDelta: {e}"))?;

        let updates = pb
            .updates
            .into_iter()
            .map(|u| (u.index, u.value as u8))
            .collect();
        let delta = HllDelta { updates };
        self.inner
            .apply_delta(&delta)
            .map_err(|e| format!("apply HLLDelta: {e}"))?;
        Ok(())
    }
}

impl SerializableToSink for HllSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "variant": format!("{:?}", self.inner.variant),
            "precision": self.inner.precision,
            "register_bytes": self.inner.registers.len(),
            "hip_kxq0": self.inner.hip_kxq0,
            "hip_kxq1": self.inner.hip_kxq1,
            "hip_est": self.inner.hip_est,
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for HllSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "HllSketchAccumulator"
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
                "Cannot merge HllSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }
        let other_hll = other
            .as_any()
            .downcast_ref::<HllSketchAccumulator>()
            .ok_or("Failed to downcast to HllSketchAccumulator")?;
        let merged_inner = HllSketch::merge_refs(&[&self.inner, &other_hll.inner])?;
        Ok(Box::new(Self {
            inner: merged_inner,
        }))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::HLL
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
        Err("HllSketchAccumulator: query_statistic not yet implemented \
             (register round-trip works, but cardinality estimation deferred; \
              tracked as a PR C-CountSketch follow-up)"
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_state(
        variant: i32,
        precision: u32,
        registers: Vec<u8>,
        hip_kxq0: f64,
        hip_kxq1: f64,
        hip_est: f64,
    ) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::HyperLogLogState;
        use prost::Message;
        let state = HyperLogLogState {
            variant,
            precision,
            registers,
            hip_kxq0,
            hip_kxq1,
            hip_est,
        };
        state.encode_to_vec()
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_regular() {
        use asap_sketchlib::proto::sketchlib::HllVariant as ProtoVariant;
        let bytes = encode_state(
            ProtoVariant::Regular as i32,
            2,
            vec![1, 2, 3, 4],
            0.0,
            0.0,
            0.0,
        );
        let acc = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.variant, HllVariant::Regular);
        assert_eq!(acc.inner.precision, 2);
        assert_eq!(acc.inner.registers, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_hip_preserves_accumulators() {
        use asap_sketchlib::proto::sketchlib::HllVariant as ProtoVariant;
        let bytes = encode_state(
            ProtoVariant::Hip as i32,
            2,
            vec![0, 0, 0, 0],
            1.5,
            2.5,
            42.0,
        );
        let acc = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.variant, HllVariant::Hip);
        assert_eq!(acc.inner.hip_kxq0, 1.5);
        assert_eq!(acc.inner.hip_kxq1, 2.5);
        assert_eq!(acc.inner.hip_est, 42.0);
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_envelope_wrapped() {
        // Mirrors what DataCollector's hllprocessor emits: the state
        // wrapped in a `SketchEnvelope{hll: ...}` via sketchlib-go's
        // `SerializePortableFO` + `proto.Marshal`.
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use prost::Message;

        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision: 2,
            registers: vec![1, 2, 3, 4],
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        };
        let bytes = env.encode_to_vec();

        let acc = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("envelope-wrapped decode should succeed");
        assert_eq!(acc.inner.variant, HllVariant::Regular);
        assert_eq!(acc.inner.registers, vec![1, 2, 3, 4]);
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

        let result = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err(), "wrong-sketch envelope should error");
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_register_length_mismatch() {
        use asap_sketchlib::proto::sketchlib::HllVariant as ProtoVariant;
        // precision=2 → expected 4 registers; supply only 3
        let bytes = encode_state(
            ProtoVariant::Regular as i32,
            2,
            vec![1, 2, 3],
            0.0,
            0.0,
            0.0,
        );
        let result = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("registers"));
    }

    #[test]
    fn test_from_sketchlib_proto_bytes_zero_precision_rejected() {
        use asap_sketchlib::proto::sketchlib::HyperLogLogState;
        use prost::Message;
        let state = HyperLogLogState::default();
        let bytes = state.encode_to_vec();
        let result = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregate_core_merge_matches_register_max() {
        let a = HllSketchAccumulator {
            inner: HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0),
        };
        let b = HllSketchAccumulator {
            inner: HllSketch::from_raw(HllVariant::Regular, 2, vec![4, 2, 6, 0], 0.0, 0.0, 0.0),
        };
        let merged_box = a.merge_with(&b).expect("merge ok");
        let merged = merged_box
            .as_any()
            .downcast_ref::<HllSketchAccumulator>()
            .expect("downcast ok");
        assert_eq!(merged.inner.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_aggregate_core_merge_wrong_type_rejects() {
        use crate::precompute_operators::count_sketch_accumulator::CountSketchAccumulator;
        let hll = HllSketchAccumulator::new(HllVariant::Regular, 2);
        let cs = CountSketchAccumulator::new(2, 3);
        assert!(hll.merge_with(&cs).is_err());
    }

    #[test]
    fn test_from_msgpack_bytes_round_trip() {
        let original = HllSketch::from_raw(
            HllVariant::Hip,
            3,
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            1.5,
            2.5,
            42.0,
        );
        let bytes = original.serialize_msgpack();
        let acc = HllSketchAccumulator::from_msgpack_bytes(&bytes).expect("decode ok");
        assert_eq!(acc.inner.variant, HllVariant::Hip);
        assert_eq!(acc.inner.precision, 3);
        assert_eq!(acc.inner.registers, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(acc.inner.hip_kxq0, 1.5);
    }

    #[test]
    fn test_from_msgpack_bytes_rejects_garbage() {
        let result = HllSketchAccumulator::from_msgpack_bytes(b"not valid msgpack");
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_proto_delta_bytes_round_trip() {
        use asap_otel_proto::sketchlib::v1::{HllDelta as PbDelta, HllRegisterUpdate};
        use prost::Message;

        let mut acc = HllSketchAccumulator::new(HllVariant::Regular, 2);
        acc.inner.registers = vec![1, 5, 3, 7];

        let delta_bytes = PbDelta {
            updates: vec![
                HllRegisterUpdate { index: 0, value: 4 },
                HllRegisterUpdate { index: 2, value: 6 },
            ],
        }
        .encode_to_vec();

        acc.apply_proto_delta_bytes(&delta_bytes).expect("apply ok");
        // Max semantics: reg[0]=max(1,4)=4, reg[2]=max(3,6)=6; others unchanged.
        assert_eq!(acc.inner.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_apply_proto_delta_bytes_rejects_garbage() {
        let mut acc = HllSketchAccumulator::new(HllVariant::Regular, 2);
        assert!(acc.apply_proto_delta_bytes(b"not valid proto").is_err());
    }
}
