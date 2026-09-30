//! Planner summary-update domains implemented by the raw accumulator kernels.
//! Stored outputs carry no update semantics; these predicates inspect the
//! bound Planner DAG node's `SummaryUpdate`.

/// The implemented raw frequency domain counts occurrences of sample values.
pub fn is_unit_sample_frequency(update: &planner_types::post_asap::SummaryUpdate) -> bool {
    use planner_types::post_asap::{NonNegativeWeightProof, SummaryInputExpr, WeightDomain};
    matches!(
        update.item,
        Some(SummaryInputExpr::Column(
            planner_types::pre_asap::ColumnRef::SampleValue
        ))
    ) && matches!(update.weight, SummaryInputExpr::Constant(1.0))
        && matches!(
            update.weight_domain,
            WeightDomain::NonNegative {
                proof: NonNegativeWeightProof::UnitCount
            }
        )
}

/// Raw HLL hashes the scalar sample; it does not interpret it as a frequency weight.
pub fn is_scalar_sample_value(update: &planner_types::post_asap::SummaryUpdate) -> bool {
    update.item.is_none()
        && matches!(
            update.weight,
            planner_types::post_asap::SummaryInputExpr::Column(
                planner_types::pre_asap::ColumnRef::SampleValue
            )
        )
}
