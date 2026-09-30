//! Stored-output fixtures: deployment fields paired with the stored state
//! family an installed plan's schema contract would carry.
use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
use planner_types::post_asap::{
    GroupingStrategy, HydraKind, HydraParams, SketchAlgorithm, SketchKind, SketchParams,
    SummaryFamilyType,
};
use serde_json::Value;

/// A tumbling output over `metric` grouped by `grouping`, with its id
/// allocated from `family`.
pub fn output(
    metric: &str,
    family: SummaryFamilyType,
    grouping: Vec<&str>,
    window: u64,
) -> (PrecomputeMaterialization, SummaryFamilyType) {
    let config = PrecomputeMaterialization::new(
        metric,
        KeyByLabelNames::new(grouping.into_iter().map(str::to_owned).collect()),
        window,
        window,
        WindowKind::Tumbling,
    );
    allocated(config, family)
}

/// Allocate `config`'s id from `family` after its deployment fields are final.
pub fn allocated(
    mut config: PrecomputeMaterialization,
    family: SummaryFamilyType,
) -> (PrecomputeMaterialization, SummaryFamilyType) {
    config.allocate_stored_output_id(&family);
    (config, family)
}

/// The Planner family of a kernel fixture spelled as a backend kernel tag and
/// its sketch dimensions, with the kernels' default dimensions.
pub fn family(kind: AggregationType, parameters: &Value) -> SummaryFamilyType {
    let u32_parameter = |names: &[&str], default: u32| {
        names
            .iter()
            .find_map(|name| parameters.get(*name).and_then(Value::as_u64))
            .map_or(default, |value| value as u32)
    };
    let sketch = |algorithm, params| {
        SummaryFamilyType::Sketch(
            SketchKind::new(algorithm, params),
            GroupingStrategy::PerSubpopulationInstance,
        )
    };
    let (width, depth) = (u32_parameter(&["w"], 1000), u32_parameter(&["d"], 4));
    let heap_size = u32_parameter(&["heap_size", "k", "K"], 20);
    match kind {
        AggregationType::DatasketchesKLL => sketch(
            SketchAlgorithm::Kll,
            SketchParams::Kll {
                k: u32_parameter(&["K", "k"], 200),
            },
        ),
        AggregationType::HydraKLL => {
            let k = u32_parameter(&["K", "k"], 200);
            SummaryFamilyType::Sketch(
                SketchKind::new(SketchAlgorithm::Kll, SketchParams::Kll { k }),
                GroupingStrategy::SharedMultiSubpopulation {
                    kind: HydraKind::HydraKll,
                    params: HydraParams::HydraKll {
                        k,
                        shared_buckets: k,
                    },
                },
            )
        }
        AggregationType::CountMinSketch => {
            sketch(SketchAlgorithm::Cms, SketchParams::Cms { width, depth })
        }
        AggregationType::CountMinSketchWithHeap => sketch(
            SketchAlgorithm::CmsWithHeap,
            SketchParams::CmsWithHeap {
                width,
                depth,
                heap_size,
            },
        ),
        AggregationType::CountSketch => sketch(
            SketchAlgorithm::CountSketch,
            SketchParams::CountSketch { width, depth },
        ),
        AggregationType::CountSketchWithHeap => sketch(
            SketchAlgorithm::CountSketchWithHeap,
            SketchParams::CountSketchWithHeap {
                width,
                depth,
                heap_size,
            },
        ),
        AggregationType::DDSketch => sketch(
            SketchAlgorithm::DDSketch,
            SketchParams::DDSketch {
                alpha: ["relativeAccuracy", "relative_accuracy", "alpha"]
                    .iter()
                    .find_map(|name| parameters.get(*name).and_then(Value::as_f64))
                    .unwrap_or(0.01),
            },
        ),
        AggregationType::HLL => sketch(
            SketchAlgorithm::Hll,
            SketchParams::Hll {
                precision: u32_parameter(&["precision", "p"], 14) as u8,
            },
        ),
        AggregationType::UnivMon => sketch(
            SketchAlgorithm::UnivMon,
            SketchParams::UnivMon {
                heap_size: u32_parameter(&["heap_size"], 32),
                sketch_rows: u32_parameter(&["sketch_rows"], 5),
                sketch_cols: u32_parameter(&["sketch_cols"], 1024),
                layers: u32_parameter(&["layers"], 4) as u8,
            },
        ),
        exact => exact
            .planner_exact_family()
            .unwrap_or_else(|| panic!("{exact:?} names no Planner family")),
    }
}
