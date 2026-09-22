//! Deployment-owned selection at the latest ASAPPlanner boundary.
//!
//! ASAPPlanner enumerates a ranked candidate space and deliberately does not
//! commit to one deployment plan.  The backend owns that decision because it
//! also owns placement, runtime capabilities, and the physical wire contract.

use std::rc::Rc;

use crate::types::AccuracyTarget;
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

/// Versioned diagnostic identity over existing canonical IR, never Rc or rank IDs.
/// Evidence/activation generations are reported separately from semantic identity.
pub(crate) fn explain_identity(kind: &str, value: &impl serde::Serialize) -> String {
    use sha2::{Digest, Sha256};
    fn canonical(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(fields) => {
                let sorted: std::collections::BTreeMap<_, _> = fields
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect();
                serde_json::Value::Object(sorted.into_iter().collect())
            }
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(canonical).collect())
            }
            value => value,
        }
    }
    let value = canonical(serde_json::to_value(value).expect("canonical IR serializes"));
    let bytes = serde_json::to_vec(&serde_json::json!({"kind": kind,
        "planner_revision": crate::physical::compiler::PLANNER_REVISION, "value": value}))
    .expect("JSON serializes");
    format!("asap-explain-v1:{kind}:{:x}", Sha256::digest(bytes))
}

// serde_json maps non-finite floats to null. A lossy encoding must never
// become a semantic identity, even when the runtime keeps an exact fallback.
fn lossless_json<T: serde::Serialize + serde::de::DeserializeOwned + PartialEq>(
    value: &T,
) -> Option<serde_json::Value> {
    let encoded = serde_json::to_value(value).ok()?;
    let restored: T = serde_json::from_value(encoded.clone()).ok()?;
    (restored == *value).then_some(encoded)
}

fn target_identity(target: &QueryExpr, accuracy: &AccuracyTarget) -> Option<String> {
    Some(explain_identity(
        "target",
        &(lossless_json(target)?, lossless_json(accuracy)?),
    ))
}

fn summary_identity(node: &SummaryNode) -> Option<String> {
    // Canonical exporter owns operator payloads and edge semantics. Hash its
    // structure, not assigned node IDs or the incidental sharing of Rc values.
    let dag = planner_types::post_asap::compile_executable_dag(&Rc::new(node.clone())).ok()?;
    lossless_json(&dag)?;
    fn visit(
        dag: &planner_types::post_asap::ExecutableDag,
        id: planner_types::post_asap::PostAsapNodeId,
        memo: &mut std::collections::HashMap<planner_types::post_asap::PostAsapNodeId, String>,
    ) -> String {
        if let Some(hash) = memo.get(&id) {
            return hash.clone();
        }
        let node = dag
            .nodes
            .iter()
            .find(|node| node.id == id)
            .expect("exported node exists");
        let mut inputs = dag.edges.iter().filter(|edge| edge.consumer == id).map(|edge| {
            let child = visit(dag, edge.producer, memo);
            serde_json::json!({"child": child, "role": edge.role, "schema": edge.intermediate_schema,
                "state": edge.data_state, "grouping": edge.grouping, "window": edge.window})
        }).collect::<Vec<_>>();
        inputs.sort_by_cached_key(|input| input.to_string());
        let hash = explain_identity(
            "summary_node",
            &serde_json::json!({
            "payload": node.payload, "state": node.output_state,
            "schema": node.output_schema, "guarantee": node.guarantee, "inputs": inputs}),
        );
        memo.insert(id, hash.clone());
        hash
    }
    Some(visit(&dag, dag.root, &mut std::collections::HashMap::new()))
}

pub(crate) fn explained_root_id(node: &SummaryNode, accuracy: &AccuracyTarget) -> Option<String> {
    Some(explain_identity(
        "root",
        &(summary_identity(node)?, lossless_json(accuracy)?),
    ))
}

fn replacement_identity(
    target: &QueryExpr,
    replacement: &Replacement,
    accuracy: &AccuracyTarget,
) -> Option<String> {
    let target = lossless_json(target)?;
    let accuracy = lossless_json(accuracy)?;
    let value = match replacement {
        Replacement::Summary(node) => serde_json::json!({"summary": summary_identity(node)?}),
        Replacement::Rewrite(node) => serde_json::json!({"rewrite": lossless_json(node.as_ref())?}),
        Replacement::ExactComposition(composition) => serde_json::json!({
            "exact_composition": {"placement": format!("{:?}", composition.placement),
                "operation": lossless_json(&composition.op)?, "child": lossless_json(composition.child_target.as_ref())?, "schema": lossless_json(&composition.schema)?}}),
    };
    Some(explain_identity("candidate", &(target, accuracy, value)))
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

/// Legacy single-site witness helper used by residual matching and examples.
/// Deployment selection uses `select_workload_with_accuracy_model_and_trace`
/// followed by physical compilation; this helper does not establish runtime
/// support or authorize publication.
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
        Replacement::Rewrite(_) | Replacement::ExactComposition(_) => {
            Err(SelectionError::UnexpectedRewrite)
        }
    }
}

pub fn select_summary_default(expr: &QueryExpr) -> Result<Rc<SummaryNode>, SelectionError> {
    select_summary(expr, &asap_aware_mapping::DefaultCostModel)
}

#[cfg(test)]
/// Search a same-requirement workload cohort through Planner's canonical CSE
/// and replacement inventory. Physical implementation compatibility is checked
/// later, before publication; this function never assigns runtime identities.
pub fn select_workload(
    roots: Vec<(usize, Rc<QueryExpr>)>,
    accuracy: AccuracyTarget,
    cost_model: &dyn CostModel,
) -> Result<Vec<(usize, Rc<SummaryNode>)>, SelectionError> {
    select_workload_with_evidence(
        roots,
        accuracy,
        cost_model,
        &asap_aware_mapping::NoAccuracyEvidence,
    )
}

#[cfg(test)]
/// The entire cohort uses the same scoped accuracy certificate; callers must
/// not spread one query's evidence to unrelated workload roots.
pub fn select_workload_with_evidence(
    roots: Vec<(usize, Rc<QueryExpr>)>,
    accuracy: AccuracyTarget,
    cost_model: &dyn CostModel,
    evidence: &dyn AccuracyEvidenceProvider,
) -> Result<Vec<(usize, Rc<SummaryNode>)>, SelectionError> {
    select_workload_with_accuracy_model(
        roots,
        accuracy,
        cost_model,
        evidence,
        &asap_aware_mapping::DefaultAccuracyModel,
    )
}

#[cfg(test)]
/// Keep replacement legality and workload-root validation on the same model.
pub fn select_workload_with_accuracy_model(
    roots: Vec<(usize, Rc<QueryExpr>)>,
    accuracy: AccuracyTarget,
    cost_model: &dyn CostModel,
    evidence: &dyn AccuracyEvidenceProvider,
    accuracy_model: &dyn AccuracyModel,
) -> Result<Vec<(usize, Rc<SummaryNode>)>, SelectionError> {
    select_workload_impl(roots, accuracy, cost_model, evidence, accuracy_model, None)
}

/// Return the candidate ranking and committed choices from the same search
/// that produces the installed roots. Missing numeric costs remain explicit.
pub fn select_workload_with_accuracy_model_and_trace(
    roots: Vec<(usize, Rc<QueryExpr>)>,
    accuracy: AccuracyTarget,
    cost_model: &dyn CostModel,
    evidence: &dyn AccuracyEvidenceProvider,
    accuracy_model: &dyn AccuracyModel,
) -> Result<(Vec<(usize, Rc<SummaryNode>)>, serde_json::Value), SelectionError> {
    let mut trace = serde_json::Value::Null;
    let selected = select_workload_impl(
        roots,
        accuracy,
        cost_model,
        evidence,
        accuracy_model,
        Some(&mut trace),
    )?;
    Ok((selected, trace))
}

/// The [`ReplacementStrategy`] set this deployment registers, carrying its own
/// cost and accuracy models. This is deliberately not Planner's
/// `default_strategies_with`: two of the strategies that list would give us are
/// excluded below, for two unrelated reasons.
///
/// The three workload-dependent strategies — `RollupStrategy`,
/// `AccuracyReconciliationStrategy` and `TopKLimitReuseStrategy` — are
/// deliberately absent and must stay absent: they are constructed from the
/// post-CSE sibling set, which only `search_cse_workload_with` owns, so
/// `search_workload_with_targets` registers them itself against every
/// discovered target. Adding them here would ask each target with an empty
/// sibling list and report nothing.
///
/// `SharedSubtreeStrategy` is withheld for a reason the pinned Planner has to
/// fix first, not as a cost policy this deployment could express. Both of its
/// arms are `Replacement::Rewrite`s of the target itself, and `rank_group`
/// ranks that pair (its "Shape 1") ahead of every `Replacement::Summary` in
/// the same group and returns before the sketch-family ranking runs. The
/// winning rewrite then materializes as `KeepPreAsap`, so registering the
/// strategy silently downgrades every shared aggregate from its selected
/// sketch to raw execution — `shared_aggregates_keep_their_summary` pins
/// exactly that. Sharing must decide how many copies of the summary state
/// exist, never whether the state is a summary at all; until Planner can
/// carry the selected summary through the share rewrite, canonical CSE
/// inside `search_workload_with_targets` already shares these subtrees
/// structurally and correctly.
fn replacement_strategies<'a>(
    cost_model: &'a dyn CostModel,
    evidence: &'a dyn AccuracyEvidenceProvider,
    accuracy_model: &'a dyn AccuracyModel,
) -> Vec<Box<dyn ReplacementStrategy + 'a>> {
    vec![
        Box::new(
            SketchAlgorithmStrategy::new_with_planning_inputs_and_evidence(
                cost_model,
                accuracy_model,
                &asap_aware_mapping::EqualSplitAllocator,
                evidence,
            ),
        ),
        Box::new(
            asap_aware_mapping::HydraGroupingStrategy::new_with_planning_inputs_and_evidence(
                cost_model,
                accuracy_model,
                &asap_aware_mapping::EqualSplitAllocator,
                evidence,
            ),
        ),
        Box::new(asap_aware_mapping::ExactCompositionStrategy::new(
            cost_model,
        )),
        Box::new(asap_aware_mapping::SemanticEquivalentRewriteStrategy),
    ]
}

fn select_workload_impl(
    roots: Vec<(usize, Rc<QueryExpr>)>,
    accuracy: AccuracyTarget,
    cost_model: &dyn CostModel,
    evidence: &dyn AccuracyEvidenceProvider,
    accuracy_model: &dyn AccuracyModel,
    mut trace: Option<&mut serde_json::Value>,
) -> Result<Vec<(usize, Rc<SummaryNode>)>, SelectionError> {
    let strategies = replacement_strategies(cost_model, evidence, accuracy_model);
    let space = asap_aware_mapping::search_workload_with_targets(
        roots
            .into_iter()
            .map(|(id, root)| (id, root, Some(accuracy.clone())))
            .collect(),
        &strategies,
        accuracy_model,
    );
    let selection = space.global_selection(cost_model);
    if let Some(trace) = trace.as_deref_mut() {
        let groups = space.cost_sorted(cost_model).iter().enumerate().map(|(index, group)| {
            let chosen = selection.target_selections().find(|selected| Rc::ptr_eq(selected.target, group.target))
                .and_then(|selected| selected.chosen);
            let target = TargetSubDAG::with_consumer_count(group.target, group.consumer_count);
            let candidates = group.candidates.iter().enumerate()
                .map(|(rank, candidate)| {
                    let candidate_cost = cost_model.candidate_cost(candidate, &target);
                    let accuracy_status = if candidate.has_missing_accuracy_evidence() {
                        "unknown"
                    } else {
                        "known"
                    };
                    let runtime_support_status = match candidate.runtime_support_evidence(cost_model) {
                        Some(true) => "supported",
                        Some(false) => "unsupported",
                        None => "unknown_pending_backend_binding",
                    };
                    let decision_reason = if candidate.has_missing_accuracy_evidence() {
                        "missing_accuracy_evidence"
                    } else if candidate.runtime_support_evidence(cost_model) == Some(false) {
                        "unsupported_runtime_operation"
                    } else if chosen.is_some_and(|chosen| std::ptr::eq(chosen, *candidate)) {
                        if candidate_cost.is_some() {
                            "planner_selected_pending_backend_binding"
                        } else {
                            "qualitative_selection_pending_backend_binding"
                        }
                    } else if candidate_cost.is_none() {
                        "missing_comparable_cost"
                    } else {
                        "not_selected_by_planner"
                    };
                    let guarantee = match &candidate.replacement {
                        Replacement::Summary(node) => node.guarantee.as_ref(),
                        _ => None,
                    };
                    serde_json::json!({
                    "rank": rank,
                    "candidate_id": replacement_identity(group.target, &candidate.replacement, &accuracy),
                    "status": if chosen.is_some_and(|chosen| std::ptr::eq(chosen, *candidate)) { "selected" } else { "unselected" },
                    "strategy": candidate.strategy,
                    "provenance": format!("{:?}", candidate.provenance),
                    "rationale": candidate.rationale,
                    "replacement_kind": match &candidate.replacement {
                        Replacement::Summary(_) => "summary",
                        Replacement::Rewrite(_) => "rewrite",
                        Replacement::ExactComposition(_) => "exact_composition",
                    },
                    "accuracy_status": accuracy_status,
                    "guarantee": guarantee,
                    "runtime_support_status": runtime_support_status,
                    "decision_reason": decision_reason,
                    "estimated_cost": candidate_cost.map(|cost| cost.0),
                    "estimated_cost_status": if candidate_cost.is_some() { "available" } else { "unavailable" },
                    "selected": chosen.is_some_and(|chosen| std::ptr::eq(chosen, *candidate)),
                })}).collect::<Vec<_>>();
            let rejected = space.target_subdag_candidates().find(|candidates| Rc::ptr_eq(&candidates.target, group.target))
                .into_iter().flat_map(|candidates| &candidates.rejected).map(|candidate| serde_json::json!({
                    "status": "rejected", "strategy": candidate.strategy,
                    "description": candidate.description, "reason": candidate.error.to_string()
                })).collect::<Vec<_>>();
            serde_json::json!({ "group_id": index,
                "target_id": target_identity(group.target, &accuracy),
                "consumer_count": group.consumer_count, "candidates": candidates, "rejected": rejected })
        }).collect::<Vec<_>>();
        *trace = serde_json::json!({ "schema_version": 1, "group_id_scope": "this_selection", "groups": groups });
    }
    let roots = space
        .roots
        .iter()
        .map(|(id, root)| {
            selection
                .assemble_selected_dag(root)
                .map_err(|error| SelectionError::Workload(error.to_string()))?
                .map(|node| (*id, node))
                .ok_or_else(|| SelectionError::Workload(format!("missing query root {id}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let roots = planner_types::post_asap::share_common_summary_subtrees(roots);
    if let Some(trace) = trace {
        trace["roots"] = serde_json::json!(roots
            .iter()
            .map(|(id, node)| serde_json::json!({
                "query_index": id, "logical_root_id": explained_root_id(node, &accuracy)
            }))
            .collect::<Vec<_>>());
    }
    Ok(roots)
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
    let strategy = SketchAlgorithmStrategy::new_with_planning_inputs_and_evidence(
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
        Replacement::Rewrite(_) | Replacement::ExactComposition(_) => {
            Err(SelectionError::UnexpectedRewrite)
        }
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

    // Allocation identities and ranking ordinals are not semantic candidate identities.
    #[test]
    fn explain_candidate_ids_survive_reparse_and_preserve_accuracy() {
        fn trace(accuracy: AccuracyTarget) -> serde_json::Value {
            let root = crate::query_parser::parse_query_expr_canonical(
                "quantile_over_time(0.9, m[1m])",
                accuracy.clone(),
            )
            .unwrap();
            select_workload_with_accuracy_model_and_trace(
                vec![(0, Rc::new(root))],
                accuracy.clone(),
                &ControlPlaneCostModel::new(accuracy),
                &asap_aware_mapping::NoAccuracyEvidence,
                &asap_aware_mapping::DefaultAccuracyModel,
            )
            .unwrap()
            .1
        }
        let a = trace(AccuracyTarget::Epsilon(0.05));
        let b = trace(AccuracyTarget::Epsilon(0.05));
        assert_eq!(a, b);
        assert!(a["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["candidates"].as_array().unwrap())
            .any(|candidate| candidate["candidate_id"].is_string()));
        assert_ne!(
            a["roots"][0]["logical_root_id"],
            trace(AccuracyTarget::Epsilon(0.1))["roots"][0]["logical_root_id"]
        );
    }

    /// Explain availability from the candidate-cost API, even if a display
    /// estimate exists, and never label an unknown guarantee as selected.
    #[test]
    fn explain_keeps_uncertified_and_uncosted_candidates_explicit() {
        struct Uncosted;
        impl CostModel for Uncosted {
            fn rank_candidates(
                &self,
                _intent: &AggIntent,
                candidates: &[planner_types::post_asap::SketchAlgorithm],
            ) -> Vec<planner_types::post_asap::SketchAlgorithm> {
                candidates.to_vec()
            }

            fn candidate_cost(
                &self,
                _candidate: &asap_aware_mapping::ReplacementSubDAG,
                _target: &TargetSubDAG<'_>,
            ) -> Option<asap_aware_mapping::cost_model::Cost> {
                None
            }
        }

        let accuracy = AccuracyTarget::Epsilon(0.05);
        let root = crate::query_parser::parse_query_expr_canonical(
            "quantile_over_time(0.9,m[1m]) / quantile_over_time(0.5,m[1m])",
            accuracy.clone(),
        )
        .unwrap();
        let (_, trace) = select_workload_with_accuracy_model_and_trace(
            vec![(0, Rc::new(root))],
            accuracy,
            &Uncosted,
            &asap_aware_mapping::NoAccuracyEvidence,
            &asap_aware_mapping::DefaultAccuracyModel,
        )
        .unwrap();
        let candidates = trace["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["candidates"].as_array().unwrap());
        let mut saw_unknown = false;
        for candidate in candidates {
            assert_eq!(candidate["estimated_cost_status"], "unavailable");
            assert!(candidate["estimated_cost"].is_null());
            if candidate["accuracy_status"] == "unknown" {
                saw_unknown = true;
                assert_eq!(candidate["selected"], false);
            }
        }
        assert!(saw_unknown);
    }

    // JSON must not alias NaN and infinity through its null representation.
    #[test]
    fn explain_nonfinite_identity_is_unavailable() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let root = QueryExpr::Literal(planner_types::pre_asap::ScalarValue::Float64(value));
            assert!(replacement_identity(
                &root,
                &Replacement::Rewrite(Rc::new(root.clone())),
                &AccuracyTarget::Exact
            )
            .is_none());
        }
    }

    // Hashing excludes incidental allocation sharing but retains operand roles.
    #[test]
    fn explain_summary_identity_preserves_roles_and_ignores_rc_sharing() {
        let root = plan(
            &["sum_over_time(m[1m]) - sum_over_time(n[1m])"],
            AccuracyTarget::Exact,
        )[0]
        .1
        .clone();
        let id = summary_identity(&root).expect("canonical binary exports");
        let mut reversed = (*root).clone();
        let SummaryExpr::BinaryOp { lhs, rhs, .. } = &mut reversed.expr else {
            panic!("binary expected")
        };
        std::mem::swap(lhs, rhs);
        assert_ne!(Some(id), summary_identity(&reversed));
        let root = plan(
            &["sum_over_time(m[1m]) + sum_over_time(m[1m])"],
            AccuracyTarget::Exact,
        )[0]
        .1
        .clone();
        let id = summary_identity(&root).expect("canonical shared binary exports");
        let mut unshared = (*root).clone();
        let SummaryExpr::BinaryOp { rhs, .. } = &mut unshared.expr else {
            panic!("binary expected")
        };
        *rhs = Rc::new((**rhs).clone());
        assert_eq!(Some(id), summary_identity(&unshared));
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
        let shared = match &lhs.expr {
            SummaryExpr::ValueOperation {
                child,
                operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
                ..
            } => child,
            _ => lhs,
        };
        assert!(Rc::ptr_eq(&roots[0].1, shared));
    }

    // The registered set is exactly the four strategies this deployment
    // supports, in discovery order — which is also the tie-break order
    // `rank_group` falls back to. Both exclusions are load-bearing:
    // `SharedSubtreeStrategy` would downgrade shared aggregates to raw
    // execution (see `shared_aggregates_keep_their_summary`), and the
    // workload-dependent three are already registered by
    // `search_cse_workload_with`, which alone owns their sibling set.
    #[test]
    fn registered_strategies_are_exactly_the_supported_set() {
        let accuracy = AccuracyTarget::Epsilon(0.01);
        let cost_model = ControlPlaneCostModel::new(accuracy);
        let names: Vec<&str> = replacement_strategies(
            &cost_model,
            &asap_aware_mapping::NoAccuracyEvidence,
            &asap_aware_mapping::DefaultAccuracyModel,
        )
        .iter()
        .map(|strategy| strategy.name())
        .collect();
        assert_eq!(
            names,
            [
                "SketchAlgorithmStrategy",
                "HydraGroupingStrategy",
                "ExactCompositionStrategy",
                "SemanticEquivalentRewriteStrategy",
            ]
        );
    }

    // Missing shared-grid evidence keeps Hydra candidates visible but
    // uncertified. Supplying a certificate makes them eligible for selection.
    #[test]
    fn hydra_candidates_remain_visible_without_shared_grid_evidence() {
        use asap_aware_mapping::{AccuracyEvidenceProvider, PropagationStats};
        use planner_types::post_asap::{CompositionOperator, SketchQuery};
        use planner_types::pre_asap::query_expr::Source;
        use planner_types::pre_asap::{Column, DataType, GroupKeys, Reduction, Schema};

        struct MeasuredSharedGrid;
        impl AccuracyEvidenceProvider for MeasuredSharedGrid {
            fn propagation_stats(
                &self,
                _op: &CompositionOperator,
                _family: &SummaryFamilyType,
                _query: Option<&SketchQuery>,
            ) -> PropagationStats {
                PropagationStats {
                    hydra_shared_grid_collision_bound: Some(0.0),
                    hydra_shared_grid_failure_probability: Some(0.0),
                    ..Default::default()
                }
            }
        }

        let accuracy = AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.01,
        };
        let grouped_count = Rc::new(QueryExpr::Aggregate {
            reduction: Reduction::Reduce(GroupKeys::by(vec![2])),
            measures: vec![AggIntent::Count {
                accuracy: accuracy.clone(),
            }],
            output_names: vec![],
            having: None,
            child: Rc::new(QueryExpr::Scan {
                source: Source::TimeSeries { metric: "m".into() },
                predicates: vec![],
                schema: Schema::with_time_index(
                    vec![
                        Column::new("ts", DataType::Timestamp, false),
                        Column::new("value", DataType::Float64, false),
                        Column::new("job", DataType::Utf8, true),
                    ],
                    0,
                    vec![],
                ),
            }),
        });
        let cost_model = ControlPlaneCostModel::new(accuracy);
        let target = TargetSubDAG::new(&grouped_count);
        let hydra = |evidence: &dyn AccuracyEvidenceProvider| {
            asap_aware_mapping::HydraGroupingStrategy::new_with_planning_inputs_and_evidence(
                &cost_model,
                &asap_aware_mapping::DefaultAccuracyModel,
                &asap_aware_mapping::EqualSplitAllocator,
                evidence,
            )
            .replacements(&target)
        };
        let unknown = hydra(&asap_aware_mapping::NoAccuracyEvidence);
        assert_eq!(unknown.len(), 2);
        assert!(unknown
            .iter()
            .all(|candidate| candidate.has_missing_accuracy_evidence()));
        // HydraCms over Cms and HydraCountSketch over CountSketch.
        let certified = hydra(&MeasuredSharedGrid);
        assert_eq!(certified.len(), 2);
        assert!(certified
            .iter()
            .all(|candidate| !candidate.has_missing_accuracy_evidence()));
    }

    // A shared aggregate must keep the sketch plan an unshared one gets:
    // sharing decides how many copies of the state exist, never whether the
    // state is a summary at all.
    #[test]
    fn shared_aggregates_keep_their_summary() {
        use planner_types::pre_asap::query_expr::Source;
        use planner_types::pre_asap::{Column, DataType, GroupKeys, Reduction, Schema};

        let accuracy = AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.01,
        };
        let grouped_count = || {
            Rc::new(QueryExpr::Aggregate {
                reduction: Reduction::Reduce(GroupKeys::by(vec![2])),
                measures: vec![AggIntent::Count {
                    accuracy: accuracy.clone(),
                }],
                output_names: vec![],
                having: None,
                child: Rc::new(QueryExpr::Scan {
                    source: Source::TimeSeries { metric: "m".into() },
                    predicates: vec![],
                    schema: Schema::with_time_index(
                        vec![
                            Column::new("ts", DataType::Timestamp, false),
                            Column::new("value", DataType::Float64, false),
                            Column::new("job", DataType::Utf8, true),
                        ],
                        0,
                        vec![vec![2]],
                    ),
                }),
            })
        };
        let summarized = |roots: Vec<(usize, Rc<QueryExpr>)>| -> Vec<bool> {
            select_workload(
                roots,
                accuracy.clone(),
                &ControlPlaneCostModel::new(accuracy.clone()),
            )
            .unwrap()
            .into_iter()
            .map(|(_, node)| {
                fn is_summary(node: &SummaryNode) -> bool {
                    match &node.expr {
                        SummaryExpr::SummaryAgg { .. } => true,
                        SummaryExpr::SummaryEstimate { summary_input, .. } => {
                            is_summary(summary_input)
                        }
                        _ => false,
                    }
                }
                is_summary(&node)
            })
            .collect()
        };
        assert_eq!(summarized(vec![(0, grouped_count())]), [true]);
        assert_eq!(
            summarized(vec![(0, grouped_count()), (1, grouped_count())]),
            [true, true],
            "sharing must not downgrade a summarized aggregate to raw execution"
        );
    }
}
