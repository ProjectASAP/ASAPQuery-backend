//! Backend-owned physical compilation over ASAPPlanner's selected post-ASAP IR.
//!
//! Planner owns semantic alternatives and guarantees. This module owns the
//! deployment decision: evidence freshness, target capabilities, windows, the
//! Collector execution projection, and the matching BackendPlan.

use std::collections::HashMap;
use std::rc::Rc;

use asap_aware_mapping::cost_model::Cost;
use asap_aware_mapping::{
    plan_summary_maintenance_lifecycles, AccuracyEvidenceProvider, CostRate, DefaultAccuracyModel,
    EqualSplitAllocator, Horizon, PropagationStats, SummaryMaintenanceCapabilities,
    SummaryMaintenanceLifecycleCapabilities, SummaryMaintenanceLifecycleCostInputs, WorkloadDemand,
};
use planner_types::post_asap::{
    CompositionOperator, EvaluationSchedule, OutputRepresentation, SketchQuery, SummaryExpr,
    SummaryFamilyType, SummaryMaintenanceLifecycle, SummaryMaintenanceMode, SummaryNode,
};
use planner_types::pre_asap::QueryExpr;
use planner_types::workload::{
    AccuracyRequirement, DataArrival, DataWorkload, DurationMs, Evidence, EvidenceSource,
    Predictability, Query, QueryLanguage, QueryRequirements, QueryTimeScope, QueryWorkload, Rate,
    RepeatedDemand, RepeatingEntry, RepetitionInterval, TimeSelection,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use crate::backend_plan::{self, BackendPlan};
use crate::emit::monitor::MonitorIntent;
use crate::physical::colored_dag::emitter::{
    AggregationInput, BackendAggregation, BackendReadout, BackendStageConfig,
};
use crate::physical::post_asap::cost_model::ControlPlaneCostModel;
use crate::types_v2::AccuracyTarget;
use planner_types::pre_asap::Source;

pub const PLANNER_REVISION: &str = "5d0b6f6edcac65edc89a72051f37977ab0c83031";

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
    pub lifecycle: LifecyclePlanningInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LifecycleCostEvidence {
    pub build: f64,
    pub maintenance_per_update: f64,
    pub read: f64,
    pub retention_per_second: f64,
    pub retirement: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePlanningInput {
    pub evaluation_interval_ms: u32,
    pub ingestion_rate_per_second: f64,
    pub evidence_observed_at_unix_ms: u64,
    pub evidence_valid_for_ms: u64,
    pub horizon_seconds: f64,
    pub costs: LifecycleCostEvidence,
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
    pub lifecycle: CollectorLifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollectorLifecycle {
    pub kind: String,
    pub maintenance_mode: String,
    pub evaluation_schedule: String,
    pub output_representation: String,
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
    #[error("query {query_id}: lifecycle planning failed: {reason}")]
    Lifecycle { query_id: String, reason: String },
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
            validate_lifecycle_input(&query.query_id, &query.lifecycle)?;
            let lifecycle_costs = SummaryMaintenanceLifecycleCostInputs {
                build_cost: Some(Cost(query.lifecycle.costs.build)),
                maintenance_cost_per_update: Some(Cost(
                    query.lifecycle.costs.maintenance_per_update,
                )),
                summary_read_cost: Some(Cost(query.lifecycle.costs.read)),
                retention_cost_rate: Some(CostRate(query.lifecycle.costs.retention_per_second)),
                retirement_cost: Some(Cost(query.lifecycle.costs.retirement)),
            };
            let model = ControlPlaneCostModel::new(query.accuracy.clone())
                .with_summary_maintenance(
                    lifecycle_costs,
                    SummaryMaintenanceCapabilities {
                        incremental_update: true,
                        merge: true,
                        delete: false,
                    },
                );
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
            let lifecycle = select_lifecycle(query, &node, &model, &environment)?;
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
                lifecycle,
            });
        }

        let plan_id = stable_plan_id(&collector_materializations);
        let envelope = PlanEnvelope {
            plan_id,
            generated_at_unix_ms: environment.observed_at_unix_ms,
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: environment.capability_snapshot_id,
        };
        let mut backend_plan = backend_plan::from_stage_config(
            &BackendStageConfig {
                aggregations,
                readouts,
            },
            &Vec::<MonitorIntent>::new(),
            plan_id,
            environment.observed_at_unix_ms,
        )?;
        for materialization in backend_plan.materializations.values_mut() {
            materialization.lifecycle = Some(backend_plan::SummaryMaintenanceLifecycle {
                kind: "continuously_maintained".into(),
                maintenance_mode: "incremental".into(),
                evaluation_schedule: "per_update".into(),
                output_representation: "summary_state".into(),
            });
        }
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

fn validate_lifecycle_input(
    query_id: &str,
    input: &LifecyclePlanningInput,
) -> Result<(), CompileError> {
    let costs = [
        input.costs.build,
        input.costs.maintenance_per_update,
        input.costs.read,
        input.costs.retention_per_second,
        input.costs.retirement,
    ];
    if input.evaluation_interval_ms == 0
        || input.evidence_valid_for_ms == 0
        || !input.ingestion_rate_per_second.is_finite()
        || input.ingestion_rate_per_second < 0.0
        || !input.horizon_seconds.is_finite()
        || input.horizon_seconds <= 0.0
        || costs.iter().any(|cost| !cost.is_finite() || *cost < 0.0)
    {
        return Err(CompileError::Lifecycle {
            query_id: query_id.into(),
            reason: "rates, horizon, validity, intervals, and costs must be finite and non-negative (interval/horizon/validity non-zero)".into(),
        });
    }
    Ok(())
}

fn select_lifecycle(
    query: &PlanningQuery,
    node: &SummaryNode,
    model: &ControlPlaneCostModel,
    environment: &DeploymentEnvironment,
) -> Result<CollectorLifecycle, CompileError> {
    let workload = QueryWorkload {
        language: QueryLanguage::PromQL,
        query_batch: None,
        repeating_queries: Some(vec![RepeatingEntry {
            query: Query(query.query_id.clone()),
            demand: RepeatedDemand::FixedInterval(RepetitionInterval(
                query.lifecycle.evaluation_interval_ms,
            )),
            requirements: QueryRequirements {
                accuracy: AccuracyRequirement::Explicit(query.accuracy.clone()),
                ..QueryRequirements::default()
            },
            predictability: Predictability::Predictable { known_at: None },
            time_selection: TimeSelection {
                // Collector windows are retired as whole states. They do not
                // claim deletion support for moving-window retractions.
                scope: QueryTimeScope::Unknown,
                lookback: Some(DurationMs(query.window_secs.saturating_mul(1_000))),
                as_of: None,
            },
        }]),
        data_workload: Some(DataWorkload {
            arrival: DataArrival::ContinuouslyIngesting,
            ingestion_rate: Evidence {
                value: Some(Rate(query.lifecycle.ingestion_rate_per_second)),
                source: EvidenceSource::Observed,
                observed_at_ms: Some(query.lifecycle.evidence_observed_at_unix_ms),
                valid_for_ms: Some(query.lifecycle.evidence_valid_for_ms),
            },
            ..DataWorkload::default()
        }),
    };
    let plan = plan_summary_maintenance_lifecycles(
        Rc::new(node.clone()),
        WorkloadDemand::new(&workload, &[0]),
        environment.observed_at_unix_ms,
        Some(Horizon(query.lifecycle.horizon_seconds)),
        SummaryMaintenanceLifecycleCapabilities {
            supports_ephemeral: false,
            supports_prepared: false,
            supports_shared: false,
            supports_continuously_maintained: true,
        },
        model,
    )
    .map_err(|error| CompileError::Lifecycle {
        query_id: query.query_id.clone(),
        reason: error.to_string(),
    })?;
    let guarantee = plan
        .deployments
        .first()
        .and_then(|deployment| deployment.summary_maintenance_lifecycle_guarantee.as_ref())
        .ok_or_else(|| CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "latest ASAPPlanner selected no executable Collector lifecycle".into(),
        })?;
    Ok(CollectorLifecycle {
        kind: match guarantee.summary_maintenance_lifecycle {
            SummaryMaintenanceLifecycle::Ephemeral => "ephemeral",
            SummaryMaintenanceLifecycle::Prepared { .. } => "prepared",
            SummaryMaintenanceLifecycle::Shared { .. } => "shared",
            SummaryMaintenanceLifecycle::ContinuouslyMaintained => "continuously_maintained",
        }
        .into(),
        maintenance_mode: match guarantee.summary_maintenance_mode {
            SummaryMaintenanceMode::DirectBuild => "direct_build",
            SummaryMaintenanceMode::Incremental => "incremental",
        }
        .into(),
        evaluation_schedule: match guarantee.evaluation_schedule {
            EvaluationSchedule::OneShot => "one_shot",
            EvaluationSchedule::PerUpdate => "per_update",
            EvaluationSchedule::OnRead => "on_read",
        }
        .into(),
        output_representation: match guarantee.output_representation {
            OutputRepresentation::PlainRows => "plain_rows",
            OutputRepresentation::SummaryState => "summary_state",
            OutputRepresentation::FinalizedValue => "finalized_value",
        }
        .into(),
    })
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
        let expr = parsed;
        PlanningRequest {
            queries: vec![PlanningQuery {
                query_id: query_id.into(),
                expr,
                source: Source::TimeSeries { metric: "m".into() },
                window_secs: 60,
                group_by: vec![],
                accuracy,
                lifecycle: LifecyclePlanningInput {
                    evaluation_interval_ms: 10_000,
                    ingestion_rate_per_second: 100.0,
                    evidence_observed_at_unix_ms: 9_500,
                    evidence_valid_for_ms: 60_000,
                    horizon_seconds: 300.0,
                    costs: LifecycleCostEvidence {
                        build: 10.0,
                        maintenance_per_update: 0.001,
                        read: 0.1,
                        retention_per_second: 0.001,
                        retirement: 1.0,
                    },
                },
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
        assert_eq!(
            bundle
                .backend_plan
                .materializations
                .values()
                .next()
                .unwrap()
                .lifecycle
                .as_ref()
                .unwrap()
                .kind,
            "continuously_maintained"
        );
        assert_eq!(bundle.backend_plan.routing.len(), 1);
        for plan in &bundle.collector_plans {
            assert_eq!(plan.envelope, bundle.envelope);
            assert_eq!(plan.materializations[0].metric, "m");
            assert_eq!(plan.materializations[0].window_secs, 60);
            assert_eq!(
                plan.materializations[0].lifecycle,
                CollectorLifecycle {
                    kind: "continuously_maintained".into(),
                    maintenance_mode: "incremental".into(),
                    evaluation_schedule: "per_update".into(),
                    output_representation: "summary_state".into(),
                }
            );
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
        let mut request = request("q-topk", "topk(5, count_over_time(m[1m]))");
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
        let mut request = request("q-topk", "topk(5, count_over_time(m[1m]))");
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

    #[test]
    fn stale_lifecycle_evidence_fails_closed() {
        let mut request = request("q", "quantile_over_time(0.9, m[1m])");
        request.queries[0].lifecycle.evidence_observed_at_unix_ms = 1;
        request.queries[0].lifecycle.evidence_valid_for_ms = 10;
        assert!(matches!(
            PhysicalCompiler.compile(request, environment(10_000)),
            Err(CompileError::Lifecycle { .. })
        ));
    }
}
