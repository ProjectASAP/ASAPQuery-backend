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
    #[error("ASAPPlanner workload materialization failed: {0}")]
    Workload(String),
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

/// Search a same-requirement workload cohort through Planner's canonical CSE
/// and replacement inventory. Physical implementation compatibility is checked
/// later, before publication; this function never assigns runtime identities.
pub fn select_workload(
    roots: Vec<(usize, Rc<QueryExpr>)>,
    accuracy: AccuracyTarget,
    cost_model: &dyn CostModel,
) -> Result<Vec<(usize, Rc<SummaryNode>)>, SelectionError> {
    // Canonical CSE still runs inside search_workload_with_targets. Do not
    // offer CSE's per-invocation recompute alternative: this runtime currently
    // provisions continuously maintained, content-addressed state only.
    let strategies: Vec<Box<dyn ReplacementStrategy + '_>> = vec![
        Box::new(SketchAlgorithmStrategy::new(cost_model)),
        Box::new(asap_aware_mapping::SemanticEquivalentRewriteStrategy),
    ];
    let space = asap_aware_mapping::search_workload_with_targets(
        roots
            .into_iter()
            .map(|(id, root)| (id, root, Some(accuracy.clone())))
            .collect(),
        &strategies,
        &asap_aware_mapping::DefaultAccuracyModel,
    );
    let selection = space.global_selection(cost_model);
    let roots = space
        .roots
        .iter()
        .map(|(id, root)| {
            selection
                .materialize(root)
                .map_err(|error| SelectionError::Workload(error.to_string()))?
                .map(|node| (*id, node))
                .ok_or_else(|| SelectionError::Workload(format!("missing query root {id}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(planner_types::post_asap::share_common_summary_subtrees(
        roots,
    ))
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

#[cfg(test)]
mod workload_tests {
    use super::*;
    use crate::physical::post_asap::cost_model::ControlPlaneCostModel;

    fn plan(queries: &[&str], accuracy: AccuracyTarget) -> Vec<(usize, Rc<SummaryNode>)> {
        let roots = queries
            .iter()
            .enumerate()
            .map(|(index, query)| {
                (
                    index,
                    Rc::new(
                        crate::query_parser::parse_query_expr_canonical(query, accuracy.clone())
                            .unwrap(),
                    ),
                )
            })
            .collect();
        select_workload(
            roots,
            accuracy.clone(),
            &ControlPlaneCostModel::new(accuracy),
        )
        .unwrap()
    }

    // Distinct quantile roots retain their readouts while sharing one selected sketch.
    #[test]
    fn quantile_roots_share_selected_producer() {
        let roots = plan(
            &[
                "quantile_over_time(0.90, m[1m])",
                "quantile_over_time(0.99, m[1m])",
            ],
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.01,
            },
        );
        let SummaryExpr::SummaryEstimate {
            summary_input: first,
            query: q1,
        } = &roots[0].1.expr
        else {
            panic!("{:?}", roots[0].1)
        };
        let SummaryExpr::SummaryEstimate {
            summary_input: second,
            query: q2,
        } = &roots[1].1.expr
        else {
            panic!("{:?}", roots[1].1)
        };
        assert!(Rc::ptr_eq(first, second));
        assert_ne!(q1, q2);
    }

    // Sharing must not collapse different source or logical-window requirements.
    #[test]
    fn distinct_windows_and_sources_do_not_share() {
        let roots = plan(
            &[
                "sum_over_time(m[1m])",
                "sum_over_time(m[2m])",
                "sum_over_time(n[1m])",
            ],
            AccuracyTarget::Exact,
        );
        assert!(!Rc::ptr_eq(&roots[0].1, &roots[1].1));
        assert!(!Rc::ptr_eq(&roots[0].1, &roots[2].1));
    }

    // Arithmetic keeps its exact operands visible and reuses the standalone sum.
    #[test]
    fn weighted_mean_retains_shared_sum_operand() {
        let roots = plan(
            &[
                "sum_over_time(m[1m])",
                "sum_over_time(m[1m]) / count_over_time(m[1m])",
            ],
            AccuracyTarget::Exact,
        );
        let SummaryExpr::BinaryOp { lhs, .. } = &roots[1].1.expr else {
            panic!("{:?}", roots[1].1)
        };
        assert!(Rc::ptr_eq(&roots[0].1, lhs));
    }
}
