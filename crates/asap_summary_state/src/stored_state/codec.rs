//! Stored identity, byte form and statistic readout of Planner kernel states.
//!
//! The store keeps Planner's in-memory kernels directly. This module is the
//! only place that names their persisted type tags and byte encodings; Planner
//! owns their update, merge and estimate.
use super::native::NativeSummaryOutput;
use crate::AggregationType;
use asap_physical_operators::summary_kernels as k;
use asap_physical_operators::summary_kernels::univmon::UnivMonAccumulator;
use asap_physical_operators::summary_kernels::weighted_frequency::{
    FrequencyAlgorithm, WeightedFrequency,
};
pub use asap_physical_operators::AggregateCore;
use asap_sketchlib::MessagePackCodec;
use planner_types::post_asap::{ExactKind, SummaryFamilyType};
use std::collections::HashMap;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Persisted type tag of Planner's exact state (its named msgpack form).
pub const EXACT_V2: &str = "PlannerExactAccumulatorV2";

/// Exact-state tags written before the store kept Planner kernels. Their byte
/// layouts are no longer decoded.
const RETIRED_EXACT_TAGS: &[&str] = &[
    "PlannerExactAccumulatorV1",
    "SumAccumulator",
    "IncreaseAccumulator",
    "MinAccumulator",
    "MaxAccumulator",
    "KeyedSumCountAccumulator",
    "KeyedCounterState",
    "KeyedMinState",
    "KeyedMaxState",
];

/// Whether `type_name` is an exact-state tag whose layout is no longer decoded.
pub fn is_retired_exact(type_name: &str) -> bool {
    RETIRED_EXACT_TAGS.contains(&type_name)
}

/// Every state kind the store can persist, borrowed from a trait object.
enum View<'s> {
    Exact(&'s k::exact::ExactAccumulator),
    Dd(&'s k::DDSketchAccumulator),
    Hll(&'s k::HllSketchAccumulator),
    Kll(&'s k::DatasketchesKLLAccumulator),
    Cms(&'s k::CountMinSketchAccumulator),
    Cs(&'s k::CountSketchAccumulator),
    CmsHeap(&'s k::CountMinSketchWithHeapAccumulator),
    CsHeap(&'s k::CountSketchWithHeapAccumulator),
    Hydra(&'s k::HydraKllSketchAccumulator),
    Frequency(&'s WeightedFrequency),
    UnivMon(&'s UnivMonAccumulator),
    Native(&'s NativeSummaryOutput),
}

fn view(state: &dyn AggregateCore) -> Option<View<'_>> {
    let any = state.as_any();
    macro_rules! try_view {
        ($($variant:ident => $ty:ty),* $(,)?) => {
            $(if let Some(state) = any.downcast_ref::<$ty>() {
                return Some(View::$variant(state));
            })*
        };
    }
    try_view!(
        Exact => k::exact::ExactAccumulator,
        Dd => k::DDSketchAccumulator,
        Hll => k::HllSketchAccumulator,
        Kll => k::DatasketchesKLLAccumulator,
        Cms => k::CountMinSketchAccumulator,
        Cs => k::CountSketchAccumulator,
        CmsHeap => k::CountMinSketchWithHeapAccumulator,
        CsHeap => k::CountSketchWithHeapAccumulator,
        Hydra => k::HydraKllSketchAccumulator,
        Frequency => WeightedFrequency,
        UnivMon => UnivMonAccumulator,
        Native => NativeSummaryOutput,
    );
    None
}

pub(crate) fn frequency_state(bytes: &[u8]) -> Result<WeightedFrequency, Error> {
    Ok(WeightedFrequency::from_bytes(bytes)?)
}

fn exact_aggregation_type(state: &k::exact::ExactAccumulator) -> AggregationType {
    match state.family() {
        SummaryFamilyType::ExactAggregate(kind, _) => match kind {
            ExactKind::Sum => AggregationType::Sum,
            ExactKind::Count => AggregationType::Count,
            ExactKind::Min => AggregationType::Min,
            ExactKind::Max => AggregationType::Max,
            ExactKind::Rate => AggregationType::Rate,
            ExactKind::Increase => AggregationType::Increase,
            other => unreachable!("Planner constructs no exact {other:?} state"),
        },
        _ => unreachable!("Planner validates exact families at construction"),
    }
}

/// Backend storage view of a Planner kernel state held as a trait object.
pub trait StoredState {
    /// Persisted type tag; [`decode`] selects the byte layout by it.
    fn type_name(&self) -> &'static str;
    fn get_accumulator_type(&self) -> AggregationType;
    fn encode(&self) -> Result<Vec<u8>, Error>;
    /// [`StoredState::encode`] for states admitted by [`check_storable`].
    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.encode().unwrap_or_default()
    }
}

impl<'a> StoredState for dyn AggregateCore + 'a {
    fn type_name(&self) -> &'static str {
        match view(self) {
            Some(View::Exact(_)) => EXACT_V2,
            Some(View::Dd(_)) => "DDSketchAccumulatorV2",
            Some(View::Hll(_)) => "HllSketchAccumulatorV2",
            Some(View::Kll(_)) => "DatasketchesKLLAccumulator",
            Some(View::Cms(_)) => "CountMinSketchAccumulatorV2",
            Some(View::Cs(_)) => "CountSketchAccumulatorV2",
            Some(View::CmsHeap(_)) => "CountMinSketchWithHeapAccumulator",
            Some(View::CsHeap(_)) => "CountSketchWithHeapAccumulator",
            Some(View::Hydra(_)) => "HydraKllSketchAccumulator",
            Some(View::Frequency(_)) => "WeightedFrequency",
            Some(View::UnivMon(_)) => "UnivMonAccumulator",
            Some(View::Native(_)) => super::native::NATIVE_OUTPUT_TYPE,
            None => "UnstoredPlannerState",
        }
    }

    fn get_accumulator_type(&self) -> AggregationType {
        use AggregationType as T;
        match view(self) {
            Some(View::Exact(state)) => exact_aggregation_type(state),
            Some(View::Dd(_)) => T::DDSketch,
            Some(View::Hll(_)) => T::HLL,
            Some(View::Kll(_)) => T::DatasketchesKLL,
            Some(View::Cms(_)) => T::CountMinSketch,
            Some(View::Cs(_)) => T::CountSketch,
            Some(View::CmsHeap(_)) => T::CountMinSketchWithHeap,
            Some(View::CsHeap(_)) => T::CountSketchWithHeap,
            Some(View::Hydra(_)) => T::HydraKLL,
            Some(View::Frequency(state)) => match state.algorithm() {
                FrequencyAlgorithm::CountSketch => T::CountSketchWithHeap,
                _ => T::CountMinSketchWithHeap,
            },
            Some(View::UnivMon(_)) => T::UnivMon,
            Some(View::Native(state)) => state.kind(),
            None => T::MultipleSubpopulation,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(
            match view(self).ok_or("Planner state has no stored codec")? {
                View::Exact(state) => rmp_serde::to_vec_named(state)?,
                View::Dd(state) => {
                    rmp_serde::to_vec(&(state.sample_p(), state.inner.to_msgpack()?))?
                }
                View::Hll(state) => {
                    rmp_serde::to_vec(&(state.sample_p(), state.inner.to_msgpack()?))?
                }
                View::Kll(state) => state.inner.to_msgpack()?,
                View::Cms(state) => {
                    rmp_serde::to_vec(&(state.sample_p(), state.inner.to_msgpack()?))?
                }
                View::Cs(state) => {
                    rmp_serde::to_vec(&(state.sample_p(), state.inner.to_msgpack()?))?
                }
                View::CmsHeap(state) => state.inner.to_msgpack()?,
                View::CsHeap(state) => state.inner.to_msgpack()?,
                View::Hydra(state) => state.inner.to_msgpack()?,
                View::Frequency(state) => state.to_bytes(),
                View::UnivMon(state) => state.sketch().serialize_to_bytes()?,
                View::Native(state) => state.bytes().to_vec(),
            },
        )
    }
}

/// Planner's unkeyed exact Sum, Count, Min or Max state after one observation.
pub fn exact_value(kind: ExactKind, value: f64) -> k::exact::ExactAccumulator {
    use planner_types::post_asap::ExactParams as P;
    let params = match kind {
        ExactKind::Sum => P::Sum,
        ExactKind::Count => P::Count,
        ExactKind::Min => P::Min,
        ExactKind::Max => P::Max,
        other => panic!("{other:?} is a counter family, not a scalar value"),
    };
    let mut state =
        k::exact::ExactAccumulator::new(SummaryFamilyType::ExactAggregate(kind, params), false)
            .expect("scalar exact family");
    state.update(None, value, 0);
    state
}

/// PromQL counter evaluation range, when the caller supplies one.
pub fn range_ms(parameters: &HashMap<String, String>) -> Result<Option<(i64, i64)>, Error> {
    match (
        parameters.get("range_start_ms"),
        parameters.get("range_end_ms"),
    ) {
        (Some(start), Some(end)) => Ok(Some((start.parse()?, end.parse()?))),
        (None, None) => Ok(None),
        _ => Err("counter range requires both range_start_ms and range_end_ms".into()),
    }
}

/// Reject a Planner state before it enters the store when it has no stored
/// codec.
pub fn check_storable(state: &dyn AggregateCore) -> Result<(), Error> {
    view(state)
        .map(|_| ())
        .ok_or_else(|| "Planner state has no stored codec".into())
}

/// Decode a persisted `(type_name, bytes)` pair written by
/// [`StoredState::encode`].
pub fn decode(type_name: &str, bytes: &[u8]) -> Result<Box<dyn AggregateCore>, Error> {
    use super::decoders as d;
    Ok(match type_name {
        EXACT_V2 => Box::new(decode_exact(bytes)?),
        "DDSketchAccumulatorV2" => {
            let (sample_p, sketch): (f64, Vec<u8>) = rmp_serde::from_slice(bytes)?;
            Box::new(k::DDSketchAccumulator::from_sketch(
                d::ddsketch_from_msgpack(&sketch)?,
                sample_p,
            )?)
        }
        "HllSketchAccumulatorV2" => {
            let (sample_p, sketch): (f64, Vec<u8>) = rmp_serde::from_slice(bytes)?;
            Box::new(k::HllSketchAccumulator::from_sketch(
                d::hll_from_msgpack(&sketch)?,
                sample_p,
            )?)
        }
        "DatasketchesKLLAccumulator" => Box::new(k::DatasketchesKLLAccumulator {
            inner: d::kll_from_msgpack(bytes)?,
        }),
        "CountMinSketchAccumulatorV2" => {
            let (sample_p, sketch): (f64, Vec<u8>) = rmp_serde::from_slice(bytes)?;
            Box::new(k::CountMinSketchAccumulator::from_sketch(
                d::cms_from_msgpack(&sketch)?,
                sample_p,
            )?)
        }
        "CountSketchAccumulatorV2" => {
            let (sample_p, sketch): (f64, Vec<u8>) = rmp_serde::from_slice(bytes)?;
            Box::new(k::CountSketchAccumulator::from_sketch(
                d::cs_from_msgpack(&sketch)?,
                sample_p,
            )?)
        }
        "CountMinSketchWithHeapAccumulator" => Box::new(k::CountMinSketchWithHeapAccumulator {
            inner: d::cms_with_heap_from_msgpack(bytes)?,
        }),
        "CountSketchWithHeapAccumulator" => Box::new(k::CountSketchWithHeapAccumulator {
            inner: d::cs_with_heap_from_msgpack(bytes)?,
        }),
        "HydraKllSketchAccumulator" => Box::new(k::HydraKllSketchAccumulator {
            inner: asap_sketchlib::HydraKllSketch::from_msgpack(bytes)?,
        }),
        "WeightedFrequency" => Box::new(frequency_state(bytes)?),
        "UnivMonAccumulator" => Box::new(UnivMonAccumulator::from_sketch(
            asap_sketchlib::UnivMon::deserialize_from_bytes(bytes)?,
        )?),
        "DDSketchAccumulator"
        | "HllSketchAccumulator"
        | "CountMinSketchAccumulator"
        | "CountSketchAccumulator" => {
            return Err(format!(
                "stored format {type_name} is retired; sampled sketch storage requires V2"
            )
            .into());
        }
        retired if is_retired_exact(retired) => {
            return Err(format!(
                "stored format {retired} is retired and no longer decoded; \
                 exact state is stored as {EXACT_V2}"
            )
            .into())
        }
        other => return Err(format!("unknown stored state format {other}").into()),
    })
}

/// Decode a bare OTLP `SketchEnvelope` attribute payload into the Planner
/// kernel of its sketch family.
pub fn decode_envelope(bytes: &[u8]) -> Result<Box<dyn AggregateCore>, Error> {
    use super::decoders as d;
    use asap_sketchlib::proto::sketchlib::{sketch_envelope::SketchState, SketchEnvelope};
    use prost::Message;
    let envelope =
        SketchEnvelope::decode(bytes).map_err(|e| format!("decode SketchEnvelope: {e}"))?;
    Ok(match envelope.sketch_state {
        Some(SketchState::Kll(_)) => Box::new(k::DatasketchesKLLAccumulator {
            inner: d::kll_from_proto(bytes)?,
        }),
        Some(SketchState::Ddsketch(_)) => Box::new(
            k::DDSketchAccumulator::from_sketch(
                d::ddsketch_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .expect("validated sampling probability"),
        ),
        Some(SketchState::Hll(_)) => Box::new(
            k::HllSketchAccumulator::from_sketch(
                d::hll_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .expect("validated sampling probability"),
        ),
        Some(SketchState::CountMin(_)) => Box::new(
            k::CountMinSketchAccumulator::from_sketch(
                d::cms_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .expect("validated sampling probability"),
        ),
        Some(SketchState::CountSketch(_)) => Box::new(
            k::CountSketchAccumulator::from_sketch(
                d::cs_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .expect("validated sampling probability"),
        ),
        Some(other) => {
            let family = match other {
                SketchState::Univmon(_) => "UnivMon",
                SketchState::Hydra(_) => "Hydra",
                SketchState::Coco(_) => "CocoSketch",
                SketchState::Elastic(_) => "Elastic",
                _ => "this",
            };
            return Err(format!("SketchEnvelope {family} family has no Planner kernel").into());
        }
        None => return Err("SketchEnvelope carries no sketch state".into()),
    })
}

/// Decode Planner's exact state, rejecting a payload whose population states
/// differ from its declared family.
pub fn decode_exact(bytes: &[u8]) -> Result<k::exact::ExactAccumulator, Error> {
    Ok(rmp_serde::from_slice(bytes)?)
}

/// An empty state of the same family and shape, so a window-reset base keeps
/// its configuration.
pub fn empty_like(state: &dyn AggregateCore) -> Result<Box<dyn AggregateCore>, Error> {
    use asap_sketchlib::{
        CountMinSketch, CountMinSketchWithHeap, CountSketch, CountSketchWithHeap, DdSketch,
        HllSketch,
    };
    Ok(
        match view(state).ok_or("Planner state has no stored codec")? {
            View::Dd(s) => Box::new(
                k::DDSketchAccumulator::from_sketch(DdSketch::new(s.inner.alpha), s.sample_p())
                    .expect("validated sampling probability"),
            ),
            View::Hll(s) => Box::new(
                k::HllSketchAccumulator::from_sketch(
                    HllSketch::new(s.inner.variant, s.inner.precision),
                    s.sample_p(),
                )
                .expect("validated sampling probability"),
            ),
            View::Cms(s) => Box::new(
                k::CountMinSketchAccumulator::from_sketch(
                    CountMinSketch::new(s.inner.rows(), s.inner.cols()),
                    s.sample_p(),
                )
                .expect("validated sampling probability"),
            ),
            View::Cs(s) => Box::new(
                k::CountSketchAccumulator::from_sketch(
                    CountSketch::new(s.inner.rows, s.inner.cols),
                    s.sample_p(),
                )
                .expect("validated sampling probability"),
            ),
            View::CmsHeap(s) => Box::new(k::CountMinSketchWithHeapAccumulator {
                inner: CountMinSketchWithHeap::new(
                    s.inner.rows(),
                    s.inner.cols(),
                    s.inner.heap_size,
                ),
            }),
            View::CsHeap(s) => Box::new(k::CountSketchWithHeapAccumulator {
                inner: CountSketchWithHeap::new(s.inner.rows(), s.inner.cols(), s.inner.heap_size),
            }),
            View::UnivMon(s) => {
                let (heap, rows, cols, layers) = s.dimensions();
                Box::new(UnivMonAccumulator::new(heap, rows, cols, layers)?)
            }
            _ => return Err("only delta-capable sketch families reset to empty".into()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyByLabelValues, Statistic};
    use asap_physical_operators::values::Value;
    use planner_types::{post_asap::SketchQuery, pre_asap::ColumnRef};

    /// Golden payloads for each supported stored family; sampled codecs wrap sketch bytes.
    const GOLDEN: &[(&str, &str)] = &[
        ("DDSketchAccumulatorV2", "93cb3f847ae147ae147bdc01040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001d0c0"),
        ("HllSketchAccumulatorV2", "415341507631010201010000013a0000001388b06d657461646174615f76657273696f6e01af686173685f70726f66696c655f6964bc70726f6a656374617361702e787868332e736565646c6973742e7631ae686173685f616c676f726974686dab787868335f36345f313238af736565645f64657269766174696f6eb4736565645f6c6973745f696e6465785f77726170ae696e7075745f656e636f64696e67b470726f6a656374617361702e696e7075742e7631a9736565645f6c697374dc0014cecafe3553cf000000ade3415118ce8cc70208ce2f024b2bce451a3df5ce6a09e667cebb67ae85ce3c6ef372cea54ff53ace510e527fce9b05688cce1f83d9abce5be0cd19cecbbb9d5dce629a292ace9159015ace152fecd8ce67332667ce8eb44a87cedb0c2e0db463616e6f6e6963616c5f736565645f696e64657805a9707265636973696f6e0491c41000000300010101000000010000000000"),
        ("DatasketchesKLLAccumulator", "92ccc8dc006641534150763101020600000000280000002ccc84ccb06d657461646174615f76657273696f6e01cca16bccccccc8cca16d08cca96974656d5f74797065cca3663634cc93cc920003cc93cccb4008000000000000cccb3fccf0000000000000cccb4000000000000000cc93cccf560f2acc9b7e7e3ccca80000"),
        ("CountMinSketchAccumulatorV2", "939298cb0000000000000000cb4000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000098cb0000000000000000cb4000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb00000000000000000208"),
        ("CountSketchAccumulatorV2", "9403089398cb0000000000000000cb4000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000098cb0000000000000000cbc000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000098cb0000000000000000cb4000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000090"),
        ("CountMinSketchWithHeapAccumulator", "93939298cb0000000000000000cb4008000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000098cb4008000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000002089192a161cb400800000000000002"),
        ("CountSketchWithHeapAccumulator", "93939398cb0000000000000000cb4008000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000098cbc008000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000098cb0000000000000000cb0000000000000000cbc008000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb0000000000000000cb000000000000000003089192a161cb400800000000000002"),
        (EXACT_V2, "83a666616d696c7981ae457861637441676772656761746592a353756da353756da67363616c617281a553756d563282a373756dcb4012000000000000ac636f6d70656e736174696f6ecb0000000000000000a56b65796564c0"),
        (EXACT_V2, "83a666616d696c7981ae457861637441676772656761746592a5436f756e74a5436f756e74a67363616c617281a5436f756e7400a56b657965648181a66c6162656c7391a16181a5436f756e7402"),
        (EXACT_V2, "83a666616d696c7981ae457861637441676772656761746592a452617465a452617465a67363616c617281a7436f756e74657286b47374617274696e675f6d6561737572656d656e7481a576616c7565cb4024000000000000b27374617274696e675f74696d657374616d70cd03e8b56c6173745f7365656e5f6d6561737572656d656e7481a576616c7565cb4039000000000000b36c6173745f7365656e5f74696d657374616d70cd07d0ae746f74616c5f696e637265617365cb402e000000000000ac73616d706c655f636f756e7402a56b65796564c0"),
        ("UnivMonAccumulator", "4153415076310102100000000155000000898bb06d657461646174615f76657273696f6e01af686173685f70726f66696c655f6964bc70726f6a656374617361702e787868332e736565646c6973742e7631ae686173685f616c676f726974686dab787868335f36345f313238af736565645f64657269766174696f6eb4736565645f6c6973745f696e6465785f77726170ae696e7075745f656e636f64696e67b470726f6a656374617361702e696e7075742e7631a9736565645f6c697374dc0014cecafe3553cf000000ade3415118ce8cc70208ce2f024b2bce451a3df5ce6a09e667cebb67ae85ce3c6ef372cea54ff53ace510e527fce9b05688cce1f83d9abce5be0cd19cecbbb9d5dce629a292ace9159015ace152fecd8ce67332667ce8eb44a87cedb0c2e0daa6c617965725f73697a6502aa736b657463685f726f7703aa736b657463685f636f6c10a9686561705f73697a6504a86b65795f74797065a375363498dc0060000000000000ff00000000000000010000000000000001000000000000ff00000000000000000000000000000001ff000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009602020200000092020092cf3ff0000000000000cf400000000000000092010192c3c30201"),
        ("WeightedFrequency", "415341502d57465245512d31000000000008000000000000000200000000000000020000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e03f0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e03f01000000000000000100000000000000040000000100000000000000611500000000000000010000000000000004000000010000000000000061000000000000e03f"),
    ];

    fn golden(tag: &str, hex_bytes: &str) -> (Box<dyn AggregateCore>, Vec<u8>) {
        let mut bytes = hex::decode(hex_bytes).unwrap();
        if tag.ends_with("V2") && tag != EXACT_V2 {
            bytes = rmp_serde::to_vec(&(1.0_f64, bytes)).unwrap();
        }
        (decode(tag, &bytes).unwrap(), bytes)
    }
    fn item() -> Option<KeyByLabelValues> {
        Some(KeyByLabelValues::new_with_labels(vec!["a".into()]))
    }
    /// Read one statistic through the Planner kernel's own readout.
    fn read(state: &dyn AggregateCore, statistic: Statistic, key: Option<KeyByLabelValues>) -> f64 {
        let any = state.as_any();
        if let Some(exact) = any.downcast_ref::<k::exact::ExactAccumulator>() {
            return exact
                .readout(statistic, None, key.as_ref())
                .unwrap()
                .unwrap();
        }
        if let Some(key) = key {
            let keyed = [
                any.downcast_ref::<k::CountMinSketchAccumulator>()
                    .map(|s| s.query_key(&key)),
                any.downcast_ref::<k::CountSketchAccumulator>()
                    .map(|s| s.query_key(&key)),
                any.downcast_ref::<k::CountMinSketchWithHeapAccumulator>()
                    .map(|s| s.query_key(&key)),
                any.downcast_ref::<k::CountSketchWithHeapAccumulator>()
                    .map(|s| s.query_key(&key)),
            ];
            return keyed.into_iter().flatten().next().expect("keyed family");
        }
        let query = match statistic {
            Statistic::Quantile => SketchQuery::Quantile { q: 0.5 },
            Statistic::Cardinality => SketchQuery::Cardinality,
            _ => SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None,
            },
        };
        state.estimate(&query).unwrap()
    }

    // Every family's existing stored bytes decode under their tag and
    // re-encode byte-identically.
    #[test]
    fn golden_bytes_decode_and_reencode_identically() {
        for (tag, bytes) in GOLDEN {
            let (state, bytes) = golden(tag, bytes);
            assert_eq!(state.type_name(), *tag);
            assert_eq!(state.encode().unwrap(), bytes, "{tag}");
        }
    }

    // Decoded golden states answer the readouts they were written with.
    #[test]
    fn golden_states_keep_their_readouts() {
        let state = |i: usize| golden(GOLDEN[i].0, GOLDEN[i].1).0;
        assert_eq!(read(state(0).as_ref(), Statistic::Count, None), 4.0);
        assert!((read(state(1).as_ref(), Statistic::Cardinality, None) - 5.0).abs() < 1.0);
        assert_eq!(read(state(2).as_ref(), Statistic::Quantile, None), 2.0);
        assert_eq!(read(state(3).as_ref(), Statistic::Count, item()), 2.0);
        assert_eq!(read(state(4).as_ref(), Statistic::Count, item()), 2.0);
        assert_eq!(read(state(5).as_ref(), Statistic::Count, item()), 3.0);
        assert_eq!(read(state(6).as_ref(), Statistic::Count, item()), 3.0);
        assert_eq!(read(state(7).as_ref(), Statistic::Sum, None), 4.5);
        assert_eq!(read(state(8).as_ref(), Statistic::Count, item()), 2.0);
        assert_eq!(read(state(9).as_ref(), Statistic::Rate, None), 15.0);
        assert_eq!(read(state(10).as_ref(), Statistic::Count, None), 2.0);
        let frequency = state(11);
        let rows = frequency
            .as_any()
            .downcast_ref::<WeightedFrequency>()
            .unwrap()
            .rows(1);
        assert!(matches!(
            &rows[..],
            [row] if matches!(&row[..], [Value::Utf8(item), Value::Float64(score)]
                if item.as_ref() == "a" && *score == 0.5)
        ));
        assert_eq!(
            frequency.get_accumulator_type(),
            AggregationType::CountMinSketchWithHeap
        );
    }

    // Retired exact layouts and unknown tags fail with a named rejection.
    #[test]
    fn retired_and_unknown_formats_are_rejected_by_name() {
        let sum_v0 = 4.5f64.to_le_bytes();
        for tag in RETIRED_EXACT_TAGS {
            let error = decode(tag, &sum_v0).err().unwrap().to_string();
            assert!(error.contains("retired") && error.contains(tag), "{error}");
        }
        assert!(decode("SketchEnvelopeAccumulator", &[])
            .err()
            .unwrap()
            .to_string()
            .contains("unknown stored state format"));
    }

    // An exact payload whose population differs from its family is rejected.
    #[test]
    fn exact_payload_must_match_its_family() {
        let (sum, mut bytes) = golden(GOLDEN[7].0, GOLDEN[7].1);
        assert!(sum.as_any().is::<k::exact::ExactAccumulator>());
        // Relabel the family as Min while keeping its Sum population.
        let at = bytes.windows(3).position(|w| w == b"Sum").unwrap();
        bytes.splice(at..at + 3, b"Min".iter().copied());
        assert!(decode_exact(&bytes).is_err());
    }

    // Sampled edge counts retain their probability through durable storage.
    #[test]
    fn sampled_envelope_storage_preserves_scaled_count() {
        use asap_sketchlib::proto::sketchlib::SketchEnvelope;
        use prost::Message;
        let mut sketch = asap_sketchlib::DdSketch::new(0.01);
        sketch.update(3.0);
        let mut envelope =
            SketchEnvelope::decode(asap_sketch_codec::encode_ddsketch(&sketch).as_slice()).unwrap();
        for (p, expected) in [(0.0, 1.0), (1.0, 1.0), (0.25, 4.0)] {
            envelope.sample_p = p;
            let state = decode_envelope(&envelope.encode_to_vec()).unwrap();
            let restored = decode(state.type_name(), &state.encode().unwrap()).unwrap();
            assert_eq!(read(restored.as_ref(), Statistic::Count, None), expected);
        }
        for p in [-1.0, 1.1, f64::NAN, f64::INFINITY] {
            envelope.sample_p = p;
            assert!(decode_envelope(&envelope.encode_to_vec()).is_err());
        }
        assert!(decode("DDSketchAccumulator", &sketch.to_msgpack().unwrap())
            .err()
            .unwrap()
            .to_string()
            .contains("retired"));
    }

    // Every sampled frequency/cardinality family retains scaled readouts in storage.
    #[test]
    fn sampled_frequency_and_hll_storage_keep_probability() {
        let key = KeyByLabelValues::new_with_labels(vec!["a".into()]);
        let mut cms = asap_sketchlib::CountMinSketch::new(2, 16);
        cms.update("a", 3.0);
        let mut cs = asap_sketchlib::CountSketch::new(3, 16);
        cs.update("a", 3.0);
        let states: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(k::CountMinSketchAccumulator::from_sketch(cms, 0.25).unwrap()),
            Box::new(k::CountSketchAccumulator::from_sketch(cs, 0.25).unwrap()),
        ];
        for state in states {
            let restored = decode(state.type_name(), &state.encode().unwrap()).unwrap();
            assert_eq!(
                read(restored.as_ref(), Statistic::Count, Some(key.clone())),
                12.0
            );
        }
        let state = k::HllSketchAccumulator::from_sketch(
            asap_sketchlib::HllSketch::new(asap_sketchlib::HllVariant::Regular, 4),
            0.25,
        )
        .unwrap();
        let state = &state as &dyn AggregateCore;
        let restored = decode(state.type_name(), &state.encode().unwrap()).unwrap();
        assert_eq!(
            restored
                .as_any()
                .downcast_ref::<k::HllSketchAccumulator>()
                .unwrap()
                .sample_p(),
            0.25
        );
    }

    // Window resets discard counts while retaining a series' sampling probability.
    #[test]
    fn sampled_reset_keeps_probability() {
        let state =
            k::DDSketchAccumulator::from_sketch(asap_sketchlib::DdSketch::new(0.01), 0.25).unwrap();
        let empty = empty_like(&state).unwrap();
        assert_eq!(
            empty
                .as_any()
                .downcast_ref::<k::DDSketchAccumulator>()
                .unwrap()
                .sample_p(),
            0.25
        );
    }

    // Persisted terminal-mode UnivMon sketches cannot enter the standard-update kernel.
    #[test]
    fn terminal_univmon_storage_is_rejected() {
        let mut sketch = asap_sketchlib::UnivMon::init_univmon(4, 3, 16, 2);
        sketch.fast_insert(&asap_sketchlib::DataInput::U64(1), 1);
        assert!(decode("UnivMonAccumulator", &sketch.serialize_to_bytes().unwrap()).is_err());
    }

    // Planner UnivMon states persist directly without a backend kernel shim.
    #[test]
    fn planner_univmon_round_trips_through_storage() {
        let state = k::univmon::UnivMonAccumulator::new(4, 3, 16, 2).unwrap();
        check_storable(&state).unwrap();
        let bytes = (&state as &dyn AggregateCore).encode().unwrap();
        let restored = decode("UnivMonAccumulator", &bytes).unwrap();
        assert!(restored.as_any().is::<k::univmon::UnivMonAccumulator>());
    }

    // An OTLP envelope attribute decodes into its family's Planner kernel;
    // families without one are rejected.
    #[test]
    fn envelope_payload_decodes_into_its_planner_kernel() {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope::SketchState, SketchEnvelope};
        use prost::Message;
        let mut dd = asap_sketchlib::DdSketch::new(0.01);
        dd.update(3.0);
        let state = decode_envelope(&asap_sketch_codec::encode_ddsketch(&dd)).unwrap();
        assert_eq!(state.get_accumulator_type(), AggregationType::DDSketch);
        assert_eq!(read(state.as_ref(), Statistic::Count, None), 1.0);
        let univmon = SketchEnvelope {
            sketch_state: Some(SketchState::Univmon(Default::default())),
            ..Default::default()
        };
        assert!(decode_envelope(&univmon.encode_to_vec()).is_err());
    }

    // Window reset keeps each delta family's shape and drops its counts.
    #[test]
    fn empty_like_keeps_shape() {
        let (dd, _) = golden(GOLDEN[0].0, GOLDEN[0].1);
        let empty = empty_like(dd.as_ref()).unwrap();
        let empty = empty
            .as_any()
            .downcast_ref::<k::DDSketchAccumulator>()
            .unwrap();
        assert_eq!(empty.inner.total_count(), 0);
        assert_eq!(empty.inner.alpha, 0.01);
        let (cms, _) = golden(GOLDEN[3].0, GOLDEN[3].1);
        let empty = empty_like(cms.as_ref()).unwrap();
        assert_eq!(read(empty.as_ref(), Statistic::Count, item()), 0.0);
        let (kll, _) = golden(GOLDEN[2].0, GOLDEN[2].1);
        assert!(empty_like(kll.as_ref()).is_err());
    }
}
