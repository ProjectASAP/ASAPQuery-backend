//! Deployment-owned selection at the latest ASAPPlanner boundary.
//!
//! ASAPPlanner enumerates a ranked candidate space and deliberately does not
//! commit to one deployment plan.  The backend owns that decision because it
//! also owns placement, runtime capabilities, and the physical wire contract.

use std::rc::Rc;

use asap_aware_mapping::{
    AccuracyBudgetAllocator, AccuracyEvidenceProvider, AccuracyModel, CostModel, Replacement,
    ReplacementStrategy, SketchAlgorithmStrategy, TargetSubDAG,
};
use planner_types::post_asap::{
    SummaryExpr, SummaryFamilyType, SummaryField, SummaryNode, SummarySchema,
};
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
