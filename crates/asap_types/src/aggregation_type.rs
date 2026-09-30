//! Kernel identity of backend-stored summary state.
pub use asap_summary_state::AggregationType;

use planner_types::post_asap::{ExactKind, GroupingStrategy, SketchAlgorithm, SummaryFamilyType};

/// The backend kernel tag that stores state of the Planner `family`. `None`
/// for families the backend has no stored-state kernel for.
pub fn aggregation_type_for_family(family: &SummaryFamilyType) -> Option<AggregationType> {
    Some(match family {
        SummaryFamilyType::ExactAggregate(kind, _) => match kind {
            ExactKind::Sum => AggregationType::Sum,
            ExactKind::Count => AggregationType::Count,
            ExactKind::Min => AggregationType::Min,
            ExactKind::Max => AggregationType::Max,
            ExactKind::Increase => AggregationType::Increase,
            ExactKind::Rate => AggregationType::Rate,
            ExactKind::IRate => return None,
        },
        SummaryFamilyType::Sketch(kind, grouping) => match (kind.algorithm(), grouping) {
            (SketchAlgorithm::Kll, GroupingStrategy::SharedMultiSubpopulation { .. }) => {
                AggregationType::HydraKLL
            }
            (_, GroupingStrategy::SharedMultiSubpopulation { .. }) => return None,
            (SketchAlgorithm::Kll, _) => AggregationType::DatasketchesKLL,
            (SketchAlgorithm::Cms, _) => AggregationType::CountMinSketch,
            (SketchAlgorithm::CmsWithHeap, _) => AggregationType::CountMinSketchWithHeap,
            (SketchAlgorithm::CountSketch, _) => AggregationType::CountSketch,
            (SketchAlgorithm::CountSketchWithHeap, _) => AggregationType::CountSketchWithHeap,
            (SketchAlgorithm::DDSketch, _) => AggregationType::DDSketch,
            (SketchAlgorithm::Hll, _) => AggregationType::HLL,
            (SketchAlgorithm::UnivMon, _) => AggregationType::UnivMon,
            (SketchAlgorithm::Kmv | SketchAlgorithm::Theta, _) => return None,
        },
        _ => return None,
    })
}
