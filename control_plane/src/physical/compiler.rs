//! Backend-owned physical compilation over ASAPPlanner's selected post-ASAP IR.
//!
//! Planner owns semantic alternatives and guarantees. This module owns the
//! deployment decision: evidence freshness, target capabilities, windows, the
//! Collector execution projection, and the matching BackendPlan.

use std::collections::HashMap;

use asap_aware_mapping::{
    AccuracyEvidenceProvider, DefaultAccuracyModel, EqualSplitAllocator, PropagationStats,
};
use planner_types::post_asap::{
    CompositionOperator, SketchQuery, SummaryExpr, SummaryFamilyType, SummaryNode,
};
use planner_types::pre_asap::QueryExpr;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use crate::backend_plan::{self, BackendPlan};
use crate::emit::monitor::MonitorIntent;
use crate::intent_algebra::Source;
use crate::physical::colored_dag::emitter::{
    AggregationInput, BackendAggregation, BackendReadout, BackendStageConfig,
};
use crate::sketch_algebra::cost_model::ControlPlaneCostModel;
use crate::types_v2::AccuracyTarget;

pub const PLANNER_REVISION: &str = "3afcba68f4e8397fb81e2be988f47120f63f7a39";

#[derive(Debug, Clone)]
pub struct PlanningQuery {
    pub query_id: String,
    pub expr: QueryExpr,
    pub source: Source,
    pub window_secs: u64,
    /// Label names are deployment metadata because Planner's canonical IR
    /// currently carries positional column IDs at this boundary.
    pub group_by: Vec<String>,
    pub accuracy: AccuracyTarget,
}

#[derive(Debug, Clone, Default)]
pub struct PlanningRequest {
    pub queries: Vec<PlanningQuery>,
    pub evidence: HashMap<String, TopKMembershipEvidence>,
    pub planner_revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopKMembershipEvidence {
    pub selected_lower_bound: f64,
    pub excluded_upper_bound: f64,
    pub interval_failure_probability: f64,
    pub observed_at_unix_ms: u64,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct DeploymentEnvironment {
    pub collector_ids: Vec<String>,
    pub capability_snapshot_id: String,
    pub observed_at_unix_ms: u64,
    pub max_evidence_age_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanEnvelope {
    pub plan_id: u64,
    pub generated_at_unix_ms: u64,
    pub planner_revision: String,
    pub capability_snapshot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorMaterialization {
    pub query_id: String,
    pub metric: String,
    pub algorithm: String,
    pub parameters: Value,
    pub group_by: Vec<String>,
    pub window_secs: u64,
    pub evidence_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorPlan {
    pub collector_id: String,
    pub envelope: PlanEnvelope,
    pub materializations: Vec<CollectorMaterialization>,
}

#[derive(Debug, Clone)]
pub struct CompiledPlanBundle {
    pub envelope: PlanEnvelope,
    pub collector_plans: Vec<CollectorPlan>,
    pub backend_plan: BackendPlan,
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("planner revision mismatch: request={request}, compiler={compiler}")]
    PlannerRevision {
        request: String,
        compiler: &'static str,
    },
    #[error("query {query_id}: {reason}")]
    Query { query_id: String, reason: String },
    #[error("query {query_id}: TopK evidence is stale or invalid: {reason}")]
    InvalidEvidence { query_id: String, reason: String },
    #[error("failed to construct BackendPlan: {0}")]
    BackendPlan(#[from] anyhow::Error),
}

struct QueryEvidence<'a>(Option<&'a TopKMembershipEvidence>);

impl AccuracyEvidenceProvider for QueryEvidence<'_> {
    fn propagation_stats(
        &self,
        op: &CompositionOperator,
        _family: &SummaryFamilyType,
        _query: Option<&SketchQuery>,
    ) -> PropagationStats {
        match (op, self.0) {
            (CompositionOperator::TopKSelection, Some(e)) => PropagationStats {
                topk_selected_lower_bound: Some(e.selected_lower_bound),
                topk_excluded_upper_bound: Some(e.excluded_upper_bound),
                topk_interval_failure_probability: Some(e.interval_failure_probability),
                ..Default::default()
            },
            _ => PropagationStats::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct PhysicalCompiler;

impl PhysicalCompiler {
    pub fn compile(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
    ) -> Result<CompiledPlanBundle, CompileError> {
        if request.planner_revision != PLANNER_REVISION {
            return Err(CompileError::PlannerRevision {
                request: request.planner_revision,
                compiler: PLANNER_REVISION,
            });
        }

        let mut aggregations = Vec::with_capacity(request.queries.len());
        let mut readouts = Vec::with_capacity(request.queries.len());
        let mut collector_materializations = Vec::with_capacity(request.queries.len());

        for query in &request.queries {
            let evidence = request.evidence.get(&query.query_id);
            if let Some(e) = evidence {
                validate_evidence(&query.query_id, e, &environment)?;
            }
            let model = ControlPlaneCostModel::new(query.accuracy.clone());
            let node = crate::planner_selection::select_summary_with_evidence(
                &query.expr,
                &model,
                &DefaultAccuracyModel,
                &EqualSplitAllocator,
                &QueryEvidence(evidence),
            )
            .map_err(|error| CompileError::Query {
                query_id: query.query_id.clone(),
                reason: error.to_string(),
            })?;
            let selected = extract_selected(&node).ok_or_else(|| CompileError::Query {
                query_id: query.query_id.clone(),
                reason: "selected plan has no executable sketch materialization/readout".into(),
            })?;
            let metric = match &query.source {
                Source::TimeSeries { metric } => metric.clone(),
                Source::Table { .. } => {
                    return Err(CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: "MVP physical compiler supports time-series sources only".into(),
                    })
                }
            };
            let aggregation_id = format!("{}:{}", query.query_id, metric);
            let kind = asap_types::SummaryKind::from(selected.kind.clone());
            let params = asap_types::SummaryParams::from(selected.params.clone());
            aggregations.push(BackendAggregation {
                aggregation_id: aggregation_id.clone(),
                metric_name: metric.clone(),
                sketch_kind: kind,
                sketch_params: params,
                window_secs: query.window_secs,
                spatial_filter: String::new(),
                grouping: query.group_by.clone(),
                item_label: None,
                aggregation_input: AggregationInput::SketchEnvelope,
                agg_type_override: None,
            });
            readouts.push(BackendReadout {
                aggregation_id,
                op: selected.readout.clone(),
            });
            collector_materializations.push(CollectorMaterialization {
                query_id: query.query_id.clone(),
                metric,
                algorithm: format!("{:?}", selected.kind.algorithm()).to_ascii_lowercase(),
                parameters: sketch_params_json(&selected.params),
                group_by: query.group_by.clone(),
                window_secs: query.window_secs,
                evidence_source: evidence.map(|e| e.source.clone()),
            });
        }

        let plan_id = stable_plan_id(&collector_materializations);
        let envelope = PlanEnvelope {
            plan_id,
            generated_at_unix_ms: environment.observed_at_unix_ms,
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: environment.capability_snapshot_id,
        };
        let backend_plan = backend_plan::from_stage_config(
            &BackendStageConfig {
                aggregations,
                readouts,
            },
            &Vec::<MonitorIntent>::new(),
            plan_id,
            environment.observed_at_unix_ms,
        )?;
        let collector_plans = environment
            .collector_ids
            .into_iter()
            .map(|collector_id| CollectorPlan {
                collector_id,
                envelope: envelope.clone(),
                materializations: collector_materializations.clone(),
            })
            .collect();
        Ok(CompiledPlanBundle {
            envelope,
            collector_plans,
            backend_plan,
        })
    }
}

fn validate_evidence(
    query_id: &str,
    evidence: &TopKMembershipEvidence,
    env: &DeploymentEnvironment,
) -> Result<(), CompileError> {
    let age = env
        .observed_at_unix_ms
        .saturating_sub(evidence.observed_at_unix_ms);
    let valid = evidence.selected_lower_bound.is_finite()
        && evidence.excluded_upper_bound.is_finite()
        && evidence.selected_lower_bound > evidence.excluded_upper_bound
        && (0.0..=1.0).contains(&evidence.interval_failure_probability)
        && !evidence.source.trim().is_empty()
        && age <= env.max_evidence_age_ms;
    if valid {
        Ok(())
    } else {
        Err(CompileError::InvalidEvidence {
            query_id: query_id.into(),
            reason: format!("margin/failure/source invalid or age {age}ms exceeds policy"),
        })
    }
}

struct SelectedSketch {
    kind: planner_types::post_asap::SketchKind,
    params: planner_types::post_asap::SketchParams,
    readout: SketchQuery,
}

fn extract_selected(node: &SummaryNode) -> Option<SelectedSketch> {
    let SummaryExpr::SummaryEstimate {
        summary_input,
        query,
    } = &node.expr
    else {
        return None;
    };
    let SummaryExpr::SummaryAgg {
        family: SummaryFamilyType::Sketch(kind, _),
        ..
    } = &summary_input.expr
    else {
        return None;
    };
    Some(SelectedSketch {
        kind: kind.clone(),
        params: kind.params().clone(),
        readout: query.clone(),
    })
}

fn sketch_params_json(params: &planner_types::post_asap::SketchParams) -> Value {
    use planner_types::post_asap::SketchParams as P;
    match params {
        P::Kll { k } => json!({"k": k}),
        P::Cms { width, depth } => json!({"width": width, "depth": depth}),
        P::Hll { precision } => json!({"precision": precision}),
        P::DDSketch { alpha } => json!({"alpha": alpha}),
        P::CmsWithHeap {
            width,
            depth,
            heap_size,
        } => json!({"width": width, "depth": depth, "heap_size": heap_size}),
        P::Kmv { k } | P::Theta { k } => json!({"k": k}),
        P::CountSketch { width, depth } => json!({"width": width, "depth": depth}),
        P::CountSketchWithHeap {
            width,
            depth,
            heap_size,
        } => json!({"width": width, "depth": depth, "heap_size": heap_size}),
    }
}

fn stable_plan_id(materializations: &[CollectorMaterialization]) -> u64 {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(materializations).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(now: u64) -> DeploymentEnvironment {
        DeploymentEnvironment {
            collector_ids: vec!["edge-a".into(), "edge-b".into()],
            capability_snapshot_id: "caps-7".into(),
            observed_at_unix_ms: now,
            max_evidence_age_ms: 60_000,
        }
    }

    fn request(query_id: &str, promql: &str) -> PlanningRequest {
        let accuracy = AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.01,
        };
        let parsed = crate::query_parser::parse_query_expr_canonical(promql, accuracy.clone())
            .expect("canonical query");
        let expr = if promql.starts_with("topk(") {
            use crate::optimizer::engine::{DefaultCostModel, RewriteRule, TopKFusion};
            let cost = DefaultCostModel {
                raw_bytes_per_sec: 1.0,
                deployment: None,
            };
            TopKFusion
                .try_rewrite(parsed.clone(), &cost)
                .unwrap_or(parsed)
        } else {
            parsed
        };
        PlanningRequest {
            queries: vec![PlanningQuery {
                query_id: query_id.into(),
                expr,
                source: Source::TimeSeries { metric: "m".into() },
                window_secs: 60,
                group_by: vec![],
                accuracy,
            }],
            evidence: HashMap::new(),
            planner_revision: PLANNER_REVISION.into(),
        }
    }

    #[test]
    fn compiles_one_decision_into_matching_collector_and_backend_views() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile");
        assert_eq!(bundle.collector_plans.len(), 2);
        assert_eq!(bundle.backend_plan.materializations.len(), 1);
        assert_eq!(bundle.backend_plan.routing.len(), 1);
        for plan in &bundle.collector_plans {
            assert_eq!(plan.envelope, bundle.envelope);
            assert_eq!(plan.materializations[0].metric, "m");
            assert_eq!(plan.materializations[0].window_secs, 60);
            assert!(matches!(
                plan.materializations[0].algorithm.as_str(),
                "ddsketch" | "kll"
            ));
        }
    }

    #[test]
    fn topk_fails_closed_without_membership_evidence() {
        let error = PhysicalCompiler
            .compile(request("q-topk", "topk(5, m)"), environment(10_000))
            .expect_err("missing certificate must fail");
        assert!(matches!(error, CompileError::Query { .. }));
    }

    #[test]
    fn stale_topk_evidence_is_rejected_before_planner_selection() {
        let mut request = request("q-topk", "topk(5, m)");
        request.evidence.insert(
            "q-topk".into(),
            TopKMembershipEvidence {
                selected_lower_bound: 101.0,
                excluded_upper_bound: 100.0,
                interval_failure_probability: 0.005,
                observed_at_unix_ms: 1,
                source: "runtime-margin-monitor".into(),
            },
        );
        let error = PhysicalCompiler
            .compile(request, environment(100_000))
            .expect_err("stale certificate must fail");
        assert!(matches!(error, CompileError::InvalidEvidence { .. }));
    }

    #[test]
    fn fresh_topk_evidence_enables_physical_compilation() {
        let mut request = request("q-topk", "topk(5, m)");
        request.evidence.insert(
            "q-topk".into(),
            TopKMembershipEvidence {
                selected_lower_bound: 101.0,
                excluded_upper_bound: 100.0,
                interval_failure_probability: 0.005,
                observed_at_unix_ms: 9_500,
                source: "runtime-margin-monitor".into(),
            },
        );
        let bundle = PhysicalCompiler
            .compile(request, environment(10_000))
            .expect("certified TopK compiles");
        assert_eq!(bundle.backend_plan.materializations.len(), 1);
        assert_eq!(
            bundle.collector_plans[0].materializations[0]
                .evidence_source
                .as_deref(),
            Some("runtime-margin-monitor")
        );
    }

    #[test]
    fn planner_revision_is_part_of_the_compile_contract() {
        let mut request = request("q", "quantile_over_time(0.9, m[1m])");
        request.planner_revision = "different".into();
        assert!(matches!(
            PhysicalCompiler.compile(request, environment(10_000)),
            Err(CompileError::PlannerRevision { .. })
        ));
    }
}
