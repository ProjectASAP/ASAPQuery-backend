//! Versioned physical output batches. Deployment identities and coverage remain
//! outside this payload and must be checked before decoding with the bound schema.
use super::codec::{self, StoredState};
use crate::AggregationType;
use asap_physical_operators::{
    summary_kernels as physical,
    values::{Batch, Schema, Value},
    AggregateCore, Error, KernelError,
};
use planner_types::post_asap::{SummaryFamilyType, SummarySchema};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Stored type tag of a published native output snapshot.
pub const NATIVE_OUTPUT_TYPE: &str = "NativePhysicalOutputV1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredBatch {
    version: u32,
    schema: SummarySchema,
    rows: Vec<Vec<Cell>>,
}

#[derive(Serialize, Deserialize)]
enum Cell {
    Plain(Value),
    Summary {
        family: SummaryFamilyType,
        codec: StateCodec,
        bytes: Vec<u8>,
    },
}

// Codec identity is distinct from algorithm identity: old integer CMS/CS bytes
// must never be interpreted as Float64 weighted state with typed item tuples.
#[derive(Serialize, Deserialize)]
enum StateCodec {
    WeightedFrequencyV1,
    ExactAccumulatorV1,
    /// Retired: kept only so old batches fail with a named rejection.
    SumAccumulatorV1,
    KllMsgpackV1,
    DdMsgpackV1,
    HllMsgpackV1,
    DdSampledV2,
    HllSampledV2,
    ExactAccumulatorV2,
}
fn invalid(message: impl ToString) -> Error {
    Error::Invalid(message.to_string())
}
impl StateCodec {
    fn encode(state: &dyn AggregateCore) -> Result<(Self, Vec<u8>), Error> {
        let any = state.as_any();
        let codec = if any.is::<physical::weighted_frequency::WeightedFrequency>() {
            Self::WeightedFrequencyV1
        } else if any.is::<physical::exact::ExactAccumulator>() {
            Self::ExactAccumulatorV2
        } else if any.is::<physical::DatasketchesKLLAccumulator>() {
            Self::KllMsgpackV1
        } else if any.is::<physical::DDSketchAccumulator>() {
            Self::DdSampledV2
        } else if any.is::<physical::HllSketchAccumulator>() {
            Self::HllSampledV2
        } else {
            return Err(invalid("physical summary has no persisted native codec"));
        };
        Ok((codec, state.encode().map_err(invalid)?))
    }
    fn decode(&self, bytes: &[u8]) -> Result<Arc<dyn AggregateCore>, Error> {
        let tag = match self {
            Self::WeightedFrequencyV1 => "WeightedFrequency",
            Self::ExactAccumulatorV2 => codec::EXACT_V2,
            Self::ExactAccumulatorV1 => {
                return Err(invalid("native ExactAccumulatorV1 is retired; requires V2"))
            }
            Self::SumAccumulatorV1 => {
                return Err(invalid(
                    "native codec SumAccumulatorV1 is retired and no longer decoded",
                ))
            }
            Self::KllMsgpackV1 => "DatasketchesKLLAccumulator",
            Self::DdSampledV2 => "DDSketchAccumulatorV2",
            Self::DdMsgpackV1 | Self::HllMsgpackV1 => {
                return Err(invalid(
                    "native sampled sketch codec V1 is retired; requires V2",
                ))
            }
            Self::HllSampledV2 => "HllSketchAccumulatorV2",
        };
        codec::decode(tag, bytes).map(Arc::from).map_err(invalid)
    }
}

/// A published native result held in the store as one opaque snapshot. It is
/// storage, not a kernel: it never merges and is read only by decoding its
/// batch against the installed physical DAG.
#[derive(Clone)]
pub struct NativeSummaryOutput {
    batch: Batch,
    bytes: Vec<u8>,
    kind: AggregationType,
}

impl NativeSummaryOutput {
    /// Validate that the batch carries one summary family and fits the budget.
    pub fn new(batch: Batch, max_bytes: usize) -> Result<Self, String> {
        let families = batch
            .schema()
            .fields
            .iter()
            .filter_map(|field| {
                (!matches!(field.dtype, SummaryFamilyType::Plain(_))).then_some(&field.dtype)
            })
            .collect::<Vec<_>>();
        let [family] = families.as_slice() else {
            return Err("native stored batch requires one summary column".into());
        };
        use planner_types::post_asap::SketchAlgorithm;
        let mut kind = match family {
            SummaryFamilyType::Sketch(sketch, _) => match sketch.algorithm() {
                SketchAlgorithm::CmsWithHeap => Some(AggregationType::CountMinSketchWithHeap),
                SketchAlgorithm::CountSketchWithHeap => Some(AggregationType::CountSketchWithHeap),
                _ => None,
            },
            SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _) => {
                Some(AggregationType::Sum)
            }
            _ => None,
        };
        for row in batch.rows() {
            let states = row
                .iter()
                .filter_map(|value| match value {
                    Value::Summary { state, .. } => Some(state),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let [state] = states.as_slice() else {
                return Err("native stored row requires one summary state".into());
            };
            codec::check_storable(state.as_ref()).map_err(|error| error.to_string())?;
            let row_kind = state.get_accumulator_type();
            if kind.is_some_and(|kind| kind != row_kind) {
                return Err("native stored rows have different summary families".into());
            }
            kind = Some(row_kind);
        }
        let kind = kind.ok_or("empty native batch has no supported summary family")?;
        let bytes = encode_batch(&batch).map_err(|error| error.to_string())?;
        if bytes.len() > max_bytes || batch.bytes() > max_bytes {
            return Err("native summary exceeds publication/read budget".into());
        }
        Ok(Self { batch, bytes, kind })
    }

    pub fn batch(&self) -> &Batch {
        &self.batch
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn kind(&self) -> AggregationType {
        self.kind
    }

    /// Every stored group label must match the batch's own group column.
    pub fn validate_group(&self, group: &BTreeMap<String, String>) -> Result<(), String> {
        for (key, value) in group {
            let column = self
                .batch
                .schema()
                .fields
                .iter()
                .position(|field| &field.name == key)
                .ok_or("native output is missing its stored group key")?;
            if self
                .batch
                .rows()
                .iter()
                .any(|row| !matches!(&row[column], Value::Utf8(actual) if actual.as_ref() == value))
            {
                return Err("native output group differs from stored address".into());
            }
        }
        Ok(())
    }
}

impl AggregateCore for NativeSummaryOutput {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn merge_with(&self, _: &dyn AggregateCore) -> Result<Box<dyn AggregateCore>, KernelError> {
        Err("native output snapshots require an explicit physical merge operator".into())
    }
    fn approx_memory_bytes(&self) -> usize {
        self.bytes.len() + self.batch.bytes()
    }
}

/// Encode a validated physical output, preserving Float64 and typed identities.
/// This format is independent of the logical and physical plan wire formats.
pub fn encode_batch(batch: &Batch) -> Result<Vec<u8>, Error> {
    let rows = batch
        .rows()
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    Ok(match value {
                        Value::Summary { family, state } => {
                            let (codec, bytes) = StateCodec::encode(state.as_ref())?;
                            Cell::Summary {
                                family: family.clone(),
                                codec,
                                bytes,
                            }
                        }
                        value => Cell::Plain(value.clone()),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()
        })
        .collect::<Result<Vec<_>, Error>>()?;
    rmp_serde::to_vec_named(&StoredBatch {
        version: 1,
        schema: batch.schema().as_ref().clone(),
        rows,
    })
    .map_err(invalid)
}

/// Decode only against the installed output contract. The caller supplies its
/// per-read payload limit; checking state parameters is part of Batch validation.
pub fn decode_batch(bytes: &[u8], expected: Schema, max_bytes: usize) -> Result<Batch, Error> {
    if bytes.len() > max_bytes {
        return Err(invalid("native output payload exceeds read budget"));
    }
    let stored: StoredBatch = rmp_serde::from_slice(bytes).map_err(invalid)?;
    if stored.version != 1 {
        return Err(invalid("unsupported native output format"));
    }
    if stored.schema != *expected {
        return Err(invalid(
            "native output schema differs from installed contract",
        ));
    }
    let rows = stored
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| {
                    Ok(match cell {
                        Cell::Plain(value) => value,
                        Cell::Summary {
                            family,
                            codec,
                            bytes,
                        } => Value::Summary {
                            family,
                            state: codec.decode(&bytes)?,
                        },
                    })
                })
                .collect::<Result<Vec<_>, Error>>()
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Batch::try_new(expected, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use physical::weighted_frequency::{FrequencyAlgorithm, WeightedFrequency};
    use planner_types::{
        post_asap::{SketchAlgorithm, SketchKind, SketchParams, SummaryField},
        pre_asap::DataType,
    };

    fn weighted(algorithm: SketchAlgorithm) -> Batch {
        let (native, params) = match algorithm {
            SketchAlgorithm::CmsWithHeap => (
                FrequencyAlgorithm::Cms,
                SketchParams::CmsWithHeap {
                    width: 64,
                    depth: 5,
                    heap_size: 8,
                },
            ),
            _ => (
                FrequencyAlgorithm::CountSketch,
                SketchParams::CountSketchWithHeap {
                    width: 64,
                    depth: 5,
                    heap_size: 8,
                },
            ),
        };
        let family =
            SummaryFamilyType::Sketch(SketchKind::new(algorithm, params), Default::default());
        let schema = Arc::new(SummarySchema {
            fields: vec![
                SummaryField {
                    name: "group".into(),
                    dtype: SummaryFamilyType::Plain(DataType::Utf8),
                    nullable: false,
                },
                SummaryField {
                    name: "state".into(),
                    dtype: family.clone(),
                    nullable: false,
                },
            ],
            time_index: None,
        });
        let mut state = WeightedFrequency::new(native, 64, 5, 8).unwrap();
        state
            .update(&[Value::Int64(7), Value::Utf8("service-a".into())], 0.125)
            .unwrap();
        state
            .update(
                &[Value::Utf8("7".into()), Value::Utf8("service-b".into())],
                0.25,
            )
            .unwrap();
        Batch::try_new(
            schema,
            vec![vec![
                Value::Utf8("job-a".into()),
                Value::Summary {
                    family,
                    state: Arc::new(state),
                },
            ]],
        )
        .unwrap()
    }

    // Fractional rates and distinct typed item tuples survive both heap codecs.
    #[test]
    fn weighted_outputs_roundtrip_without_integer_conversion() {
        for algorithm in [
            SketchAlgorithm::CmsWithHeap,
            SketchAlgorithm::CountSketchWithHeap,
        ] {
            let batch = weighted(algorithm);
            let bytes = encode_batch(&batch).unwrap();
            let restored = decode_batch(&bytes, batch.schema().clone(), bytes.len()).unwrap();
            let scores = |b: &Batch| {
                let Value::Summary { state, .. } = &b.rows()[0][1] else {
                    panic!()
                };
                state
                    .as_any()
                    .downcast_ref::<WeightedFrequency>()
                    .unwrap()
                    .rows(8)
                    .iter()
                    .map(|row| row.iter().map(|v| v.key().unwrap()).collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            };
            assert_eq!(scores(&batch), scores(&restored));
            assert!(decode_batch(&bytes, batch.schema().clone(), bytes.len() - 1).is_err());
        }
    }

    // A storage tag cannot send weighted physical output through an integer heap decoder.
    #[test]
    fn legacy_sketch_reader_rejects_native_batch_frames() {
        let sample = super::super::SketchSampleState {
            bytes: encode_batch(&weighted(SketchAlgorithm::CmsWithHeap)).unwrap(),
            encoding: super::super::SketchEncoding::NativeBatchV1,
        };
        let result = super::super::delta_apply::per_window_summary_states(
            &[(60_000, &sample)],
            super::super::delta_apply::DeltaSketchKind::CmsWithHeap {
                rows: 5,
                cols: 64,
                heap_size: 8,
            },
        );
        assert!(result.is_err());
    }

    // Every admitted native summary codec survives the same typed boundary.
    #[test]
    fn native_summary_families_and_nonfinite_plain_values_roundtrip() {
        use planner_types::post_asap::{ExactKind, ExactParams};
        let exact = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let sketch = |algorithm, params| {
            SummaryFamilyType::Sketch(SketchKind::new(algorithm, params), Default::default())
        };
        let cases: Vec<(SummaryFamilyType, Arc<dyn AggregateCore>)> = vec![
            (
                exact.clone(),
                Arc::new(physical::exact::ExactAccumulator::new(exact.clone(), false).unwrap()),
            ),
            (
                sketch(SketchAlgorithm::Kll, SketchParams::Kll { k: 200 }),
                Arc::new(physical::DatasketchesKLLAccumulator::new(200)),
            ),
            (
                sketch(
                    SketchAlgorithm::DDSketch,
                    SketchParams::DDSketch { alpha: 0.01 },
                ),
                Arc::new(physical::DDSketchAccumulator::new(0.01)),
            ),
            (
                sketch(SketchAlgorithm::Hll, SketchParams::Hll { precision: 12 }),
                Arc::new(physical::HllSketchAccumulator::new(
                    asap_sketchlib::HllVariant::Regular,
                    12,
                )),
            ),
        ];
        for (family, state) in cases {
            let schema = Arc::new(SummarySchema {
                fields: vec![SummaryField {
                    name: "state".into(),
                    dtype: family.clone(),
                    nullable: false,
                }],
                time_index: None,
            });
            let batch =
                Batch::try_new(schema.clone(), vec![vec![Value::Summary { family, state }]])
                    .unwrap();
            let bytes = encode_batch(&batch).unwrap();
            let restored = decode_batch(&bytes, schema, bytes.len()).unwrap();
            assert_eq!(encode_batch(&restored).unwrap(), bytes);
        }
        let schema = Arc::new(SummarySchema {
            fields: vec![SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            }],
            time_index: None,
        });
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                vec![Value::Float64(f64::NAN)],
                vec![Value::Float64(f64::INFINITY)],
            ],
        )
        .unwrap();
        let restored = decode_batch(&encode_batch(&batch).unwrap(), schema, usize::MAX).unwrap();
        assert!(matches!(restored.rows()[0][0], Value::Float64(v) if v.is_nan()));
        assert!(matches!(restored.rows()[1][0], Value::Float64(v) if v == f64::INFINITY));
    }

    // Recovery validates the format, bound schema and actual sketch parameters.
    #[test]
    fn corrupt_or_relabelled_output_is_rejected() {
        let batch = weighted(SketchAlgorithm::CmsWithHeap);
        let bytes = encode_batch(&batch).unwrap();
        let wrong = weighted(SketchAlgorithm::CountSketchWithHeap);
        assert!(decode_batch(&bytes, wrong.schema().clone(), usize::MAX).is_err());
        let mut stored: StoredBatch = rmp_serde::from_slice(&bytes).unwrap();
        stored.version = 2;
        assert!(decode_batch(
            &rmp_serde::to_vec_named(&stored).unwrap(),
            batch.schema().clone(),
            usize::MAX
        )
        .is_err());
        stored.version = 1;
        let Cell::Summary { bytes: payload, .. } = &mut stored.rows[0][1] else {
            panic!()
        };
        *payload = b"legacy integer heap".to_vec();
        assert!(decode_batch(
            &rmp_serde::to_vec_named(&stored).unwrap(),
            batch.schema().clone(),
            usize::MAX
        )
        .is_err());
    }
}
