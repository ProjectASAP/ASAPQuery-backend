//! Deployment-owned selection at the latest ASAPPlanner boundary.
//!
//! ASAPPlanner enumerates a ranked candidate space and deliberately does not
//! commit to one deployment plan.  The backend owns that decision because it
//! also owns placement, runtime capabilities, and the physical wire contract.

use std::rc::Rc;

use crate::types_v2::AccuracyTarget;
use asap_aware_mapping::{
    AccuracyBudgetAllocator, AccuracyEvidenceProvider, AccuracyModel, CostModel, Replacement,
    ReplacementStrategy, SketchAlgorithmStrategy, TargetSubDAG,
};
use planner_types::post_asap::{
    SummaryExpr, SummaryFamilyType, SummaryField, SummaryNode, SummarySchema,
};
use planner_types::pre_asap::{agg_accuracy as planner_agg_accuracy, AggIntent};
use planner_types::pre_asap::{QueryExpr, QueryExprError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SelectionError {
    #[error("failed to derive the pre-ASAP schema: {0}")]
    Schema(#[from] QueryExprError),
    #[error("ASAPPlanner produced no legal summary candidate for the target")]
    NoLegalCandidate,
    #[error("ASAPPlanner sketch strategy produced a logical rewrite instead of a summary")]
    UnexpectedRewrite,
}

/// Deployment extension tag for keyed point-frequency queries. ASAPPlanner
/// intentionally treats extension payloads as opaque; this adapter is the one
/// backend-owned interpretation point.
pub(crate) const FREQUENCY_EXT_KIND: &str = "frequency";

pub fn frequency(accuracy: AccuracyTarget, item: Option<(String, String)>) -> AggIntent {
    let mut payload = serde_json::json!({ "accuracy": accuracy });
    if let Some((label, value)) = item {
        payload["item_label"] = serde_json::Value::String(label);
        payload["item_value"] = serde_json::Value::String(value);
    }
    AggIntent::Extension {
        ext_kind: FREQUENCY_EXT_KIND.to_string(),
        payload,
    }
}

pub fn default_frequency() -> AggIntent {
    frequency(AccuracyTarget::Epsilon(std::f64::consts::E / 2000.0), None)
}

pub fn as_frequency(intent: &AggIntent) -> Option<AccuracyTarget> {
    match intent {
        AggIntent::Extension { ext_kind, payload } if ext_kind == FREQUENCY_EXT_KIND => {
            serde_json::from_value(payload.get("accuracy")?.clone()).ok()
        }
        _ => None,
    }
}

pub fn agg_accuracy(intent: &AggIntent) -> f64 {
    match as_frequency(intent) {
        Some(AccuracyTarget::Exact) => 0.0,
        Some(AccuracyTarget::Epsilon(epsilon))
        | Some(AccuracyTarget::EpsilonDelta { epsilon, .. }) => epsilon,
        None => planner_agg_accuracy(intent),
    }
}

pub fn archive_only(intent: &AggIntent) -> bool {
    if as_frequency(intent).is_some() {
        return false;
    }
    matches!(
        intent,
        AggIntent::Absent
            | AggIntent::AbsentOverTime
            | AggIntent::PresentOverTime
            | AggIntent::Delta
            | AggIntent::Deriv
            | AggIntent::PredictLinear { .. }
            | AggIntent::DoubleExpSmoothing { .. }
            | AggIntent::IDelta
            | AggIntent::Resets
            | AggIntent::Changes
            | AggIntent::HistogramCount
            | AggIntent::HistogramSum
            | AggIntent::HistogramAvg
            | AggIntent::HistogramStdDev
            | AggIntent::HistogramStdVar
            | AggIntent::HistogramFraction { .. }
            | AggIntent::HistogramQuantile { .. }
            | AggIntent::Math(_)
            | AggIntent::TimeFn(_)
            | AggIntent::Group
            | AggIntent::CountValues { .. }
            | AggIntent::LastOverTime
            | AggIntent::FirstOverTime
            | AggIntent::MadOverTime
            | AggIntent::TsOfMinOverTime
            | AggIntent::TsOfMaxOverTime
            | AggIntent::TsOfFirstOverTime
            | AggIntent::TsOfLastOverTime
            | AggIntent::Extension { .. }
    )
}

/// Preserve an unsupported subtree explicitly at the post-ASAP boundary.
pub fn keep_pre_asap(expr: &QueryExpr) -> Result<Rc<SummaryNode>, SelectionError> {
    let schema = expr.output_schema()?;
    Ok(Rc::new(SummaryNode {
        expr: SummaryExpr::KeepPreAsap(Rc::new(expr.clone())),
        schema: SummarySchema {
            fields: schema
                .columns
                .into_iter()
                .map(|column| SummaryField {
                    name: column.name,
                    dtype: SummaryFamilyType::Plain(column.dtype),
                    nullable: column.nullable,
                })
                .collect(),
            time_index: schema.time_index,
        },
        guarantee: None,
    }))
}

/// Select the first legal candidate after the supplied deployment cost model
/// has ranked Planner's exhaustive candidate set.
pub fn select_summary(
    expr: &QueryExpr,
    cost_model: &dyn CostModel,
) -> Result<Rc<SummaryNode>, SelectionError> {
    let root = Rc::new(expr.clone());
    let strategy = SketchAlgorithmStrategy::new(cost_model);
    let candidate = strategy
        .replacements(&TargetSubDAG::new(&root))
        .into_iter()
        .next()
        .ok_or(SelectionError::NoLegalCandidate)?;
    match candidate.replacement {
        Replacement::Summary(node) => Ok(node),
        Replacement::Rewrite(_) => Err(SelectionError::UnexpectedRewrite),
    }
}

pub fn select_summary_default(expr: &QueryExpr) -> Result<Rc<SummaryNode>, SelectionError> {
    select_summary(expr, &asap_aware_mapping::DefaultCostModel)
}

/// Select from Planner's legal candidates with deployment-supplied accuracy
/// models and typed evidence (for example a TopK membership certificate).
pub fn select_summary_with_evidence(
    expr: &QueryExpr,
    cost_model: &dyn CostModel,
    accuracy_model: &dyn AccuracyModel,
    allocator: &dyn AccuracyBudgetAllocator,
    evidence: &dyn AccuracyEvidenceProvider,
) -> Result<Rc<SummaryNode>, SelectionError> {
    let root = Rc::new(expr.clone());
    let strategy = SketchAlgorithmStrategy::with_models_and_evidence(
        cost_model,
        accuracy_model,
        allocator,
        evidence,
    );
    let candidate = strategy
        .replacements(&TargetSubDAG::new(&root))
        .into_iter()
        .next()
        .ok_or(SelectionError::NoLegalCandidate)?;
    match candidate.replacement {
        Replacement::Summary(node) => Ok(node),
        Replacement::Rewrite(_) => Err(SelectionError::UnexpectedRewrite),
    }
}

/// Select a legal summary when Planner offers one, otherwise preserve the
/// subtree explicitly. This mirrors the removed single-tree binder's
/// conservative behavior and is appropriate for serving fallbacks; physical
/// compilation should use [`select_summary`] so an unimplementable target is
/// reported rather than silently committed.
pub fn select_summary_or_keep(
    expr: &QueryExpr,
    cost_model: &dyn CostModel,
) -> Result<Rc<SummaryNode>, SelectionError> {
    match select_summary(expr, cost_model) {
        Err(SelectionError::NoLegalCandidate) => keep_pre_asap(expr),
        result => result,
    }
}
