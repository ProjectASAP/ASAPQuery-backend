//! Complete, provider-priced comparisons of already-bound workload alternatives.
//!
//! This is an evidence manifest over the existing physical projection, not a
//! second semantic DAG. Planner supplies legal alternatives; deployment quotes
//! price every reachable operation, and the backend commits one complete plan.

mod materialization_candidates;

#[cfg(test)]
use super::compiler::PhysicalCompiler;

use std::collections::{BTreeMap, BTreeSet};

use asap_aware_mapping::cost_model::Cost;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::compiler::{
    CompileError, DeploymentEnvironment, PhysicalPlan, PlanningQuery, PlanningRequest,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CostDemand {
    /// Exact implementation/configuration being priced, not merely a family.
    pub implementation: Value,
    /// `horizon` includes all work in the manifest's source/time scope;
    /// `query_evaluation` is one execution of this bound query operator.
    pub unit: String,
    pub multiplicity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkloadCostManifest {
    pub plan_id: u64,
    pub plan_version: u64,
    pub planner_revision: String,
    pub capability_snapshot_id: String,
    pub backend_compat: String,
    pub horizon_seconds: f64,
    /// Canonical roots, requirements and demand must match across alternatives.
    pub workload: BTreeMap<String, Value>,
    pub components: BTreeMap<String, CostDemand>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkloadQuote {
    pub manifest: WorkloadCostManifest,
    /// Provider attests feasibility for this capability/data generation,
    /// including an accessible exact backend when the plan has fallback nodes.
    pub executable: bool,
    /// Calibrated costs in ONE model's units. All keys are required, including
    /// explicit zero costs. Horizon quotes include source cardinality, all
    /// maintained groups, retention/spill and the stated partition's work.
    pub unit_costs: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkloadCostEvidence {
    #[serde(default)]
    pub backend_revision: String,
    #[serde(default)]
    pub planner_revision: String,
    pub data_snapshot_id: String,
    pub model_version: String,
    pub observed_at_unix_ms: u64,
    pub valid_for_ms: u64,
    pub quotes: Vec<WorkloadQuote>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AlternativeCost {
    pub alternative_id: Option<String>,
    #[serde(default)]
    pub logical_root_ids: Vec<String>,
    pub physical_alternative_id: Option<String>,
    pub identity_unavailable_reason: Option<String>,
    #[serde(default)]
    pub status: String,
    pub plan_id: Option<u64>,
    pub total_cost: Option<f64>,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaterializationSearchCoverage {
    pub eligible_leaves: usize,
    pub enumerated_local_masks: usize,
    pub exhaustive: bool,
    pub scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkloadCostComparison {
    #[serde(default)]
    pub logical_selection: Vec<Value>,
    #[serde(default, alias = "index_search_coverage")]
    pub materialization_search_coverage: Option<MaterializationSearchCoverage>,
    pub data_snapshot_id: String,
    pub model_version: String,
    pub selected_plan_id: u64,
    pub selected_manifest: WorkloadCostManifest,
    pub component_costs: BTreeMap<String, f64>,
    pub alternatives: Vec<AlternativeCost>,
}

fn invalid(reason: impl Into<String>) -> CompileError {
    CompileError::Snapshot(format!("complete workload cost: {}", reason.into()))
}

pub fn manifest(
    plan: &PhysicalPlan,
    queries: &[PlanningQuery],
) -> Result<WorkloadCostManifest, CompileError> {
    let horizon = queries
        .first()
        .ok_or_else(|| invalid("empty workload"))?
        .lifecycle
        .horizon_seconds;
    if !horizon.is_finite() || horizon <= 0.0 {
        return Err(invalid("invalid horizon"));
    }
    let mut workload = BTreeMap::new();
    let mut reads = BTreeMap::new();
    for query in queries {
        if query.lifecycle.horizon_seconds != horizon || query.lifecycle.evaluation_interval_ms == 0
        {
            return Err(invalid("mixed horizons or unknown recurrence"));
        }
        let canonical = crate::query_plan::canonical_promql(&query.query_string)?;
        if workload
            .insert(
                query.query_id.clone(),
                json!({
                    "query": canonical, "accuracy": query.accuracy,
                    "evaluation_interval_ms": query.lifecycle.evaluation_interval_ms,
                    "source": query.source,
                }),
            )
            .is_some()
        {
            return Err(invalid("duplicate query ID"));
        }
        reads.insert(
            query.query_id.clone(),
            horizon * 1000.0 / f64::from(query.lifecycle.evaluation_interval_ms),
        );
    }
    let mut components = BTreeMap::new();
    let mut add = |id: String, implementation: Value, unit: &str, multiplicity: f64| {
        components.insert(
            id,
            CostDemand {
                implementation,
                unit: unit.into(),
                multiplicity,
            },
        );
    };
    // Backend merge/update, storage, and edge maintenance are separate work.
    // The raw input is read once per source partition, not once per consumer.
    for schema in &plan.precompute_plan.schemas {
        let mut locations = vec!["backend".to_string()];
        locations.extend(
            plan.collector_plans
                .iter()
                .filter(|collector| {
                    collector
                        .materializations
                        .iter()
                        .any(|m| m.materialization == schema.materialization)
                })
                .map(|collector| format!("collector:{}", collector.collector_id)),
        );
        for location in locations {
            let source = json!({"source": schema.source, "location": location, "ingest": plan.precompute_plan.ingest});
            add(format!("source:{}", source), source, "horizon", 1.0);
            let physical = plan
                .precompute_plan
                .materializations
                .iter()
                .find(|m| m.policy_fingerprint() == schema.materialization.fingerprint())
                .ok_or_else(|| invalid("state has no physical implementation"))?;
            let identity = json!({"schema": schema, "location": location, "physical": physical,
                "window_implementation": plan.lifecycle_estimates.iter().find(|e| e.materialization == schema.materialization).map(|e| &e.window_implementation_id)});
            for operation in ["build", "update", "residency", "retire"] {
                add(
                    format!("state:{location}:{}:{operation}", schema.materialization.0),
                    json!({"operation": operation, "binding": identity}),
                    "horizon",
                    1.0,
                );
            }
        }
    }
    for rule in &plan.transmission_plan.rules {
        add(
            format!("transport:{}:{}", rule.producer_id, rule.materialization.0),
            json!(rule),
            "horizon",
            1.0,
        );
    }
    for entry in plan.query_plan.entries.values() {
        let evaluations = *reads
            .get(&entry.query_id)
            .ok_or_else(|| invalid("unmapped query root"))?;
        if entry
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExactFallback { .. }))
        {
            let query = queries
                .iter()
                .find(|query| query.query_id == entry.query_id)
                .ok_or_else(|| invalid("unmapped exact source"))?;
            // Charge the exact service's input upkeep/storage separately from
            // per-evaluation native execution. Zero is valid only if explicitly
            // quoted as already-provisioned/non-incremental for this decision.
            let parsed = crate::query_parser::parse_query_expr_canonical(
                &query.query_string,
                query.accuracy.clone(),
            )
            .map_err(|error| invalid(error.to_string()))?;
            for metric in exact_source_metrics(&parsed)? {
                let source = json!({"source": planner_types::pre_asap::Source::TimeSeries { metric }, "location": "exact_backend"});
                add(format!("source:{}", source), source, "horizon", 1.0);
            }
        }
        // Typed local scans require retained input and ingest/update work even
        // when no precomputed summary is installed. Deduplicate by source.
        for node in entry.nodes.values() {
            if matches!(
                node,
                crate::query_plan::QueryPlanNode::Logical {
                    operator: crate::query_plan::logical::LogicalOperator::Scan { .. },
                    ..
                }
            ) {
                return Err(invalid(
                    "generic backend raw scans are outside the ASAP/Prometheus execution contract",
                ));
            }
            if let crate::query_plan::QueryPlanNode::Logical {
                operator:
                    crate::query_plan::logical::LogicalOperator::ExactSubquery { query }
                    | crate::query_plan::logical::LogicalOperator::CandidateExactSubquery {
                        query, ..
                    },
                ..
            } = node
            {
                let parsed = crate::query_parser::parse_query_expr_canonical(
                    query,
                    crate::types_v2::AccuracyTarget::Exact,
                )
                .map_err(|error| invalid(error.to_string()))?;
                for metric in exact_source_metrics(&parsed)? {
                    let source = json!({"source": planner_types::pre_asap::Source::TimeSeries { metric }, "location": "exact_backend"});
                    add(format!("source:{}", source), source, "horizon", 1.0);
                }
            }
        }
        // Reachability comes from QueryPlan, including materialization reads,
        // arithmetic, reduction and a complete engine-native exact fallback.
        for node_id in entry.topological_order()? {
            add(
                format!("query:{}:{}", entry.query_id, node_id.0),
                json!({"node": entry.nodes[&node_id], "query": entry.canonical_query, "instant": entry.instant}),
                "query_evaluation",
                evaluations,
            );
        }
        add(
            format!("result:{}", entry.query_id),
            json!({"query_id": entry.query_id, "root": entry.root}),
            "query_evaluation",
            evaluations,
        );
    }
    Ok(WorkloadCostManifest {
        plan_id: plan.envelope.plan_id,
        plan_version: plan.envelope.plan_version,
        planner_revision: plan.envelope.planner_revision.clone(),
        capability_snapshot_id: plan.envelope.capability_snapshot_id.clone(),
        backend_compat: plan.envelope.backend_compat.clone(),
        horizon_seconds: horizon,
        workload,
        components,
    })
}

/// Walk the canonical relational tree, preserving every input to binary and
/// fan-in operators. Unsupported source discovery must not produce a partial quote.
pub(crate) fn exact_source_metrics(
    expr: &planner_types::pre_asap::QueryExpr,
) -> Result<BTreeSet<String>, CompileError> {
    use planner_types::pre_asap::{QueryExpr, Source};
    fn visit(expr: &QueryExpr, metrics: &mut BTreeSet<String>) -> Result<(), CompileError> {
        match expr {
            QueryExpr::Scan {
                source: Source::TimeSeries { metric },
                ..
            } if !metric.is_empty() => {
                metrics.insert(metric.clone());
            }
            QueryExpr::PromqlScalarBridge(child)
            | QueryExpr::PromqlVectorFromScalar(child)
            | QueryExpr::PromqlScalarFromVector(child)
            | QueryExpr::PromqlRelabel { child, .. }
            | QueryExpr::PromqlSeriesSample { child, .. }
            | QueryExpr::Filter { child, .. }
            | QueryExpr::Project { child, .. }
            | QueryExpr::Aggregate { child, .. }
            | QueryExpr::Dedup { child, .. }
            | QueryExpr::Sort { child, .. }
            | QueryExpr::Limit { child, .. }
            | QueryExpr::PromqlSubquery { child, .. }
            | QueryExpr::TimeRange { child, .. }
            | QueryExpr::TimeShift { child, .. } => visit(child, metrics)?,
            QueryExpr::BinaryOp {
                lhs: left,
                rhs: right,
                ..
            }
            | QueryExpr::Join { left, right, .. }
            | QueryExpr::SetOp { left, right, .. } => {
                visit(left, metrics)?;
                visit(right, metrics)?;
            }
            QueryExpr::Concat { children, .. } => {
                for child in children {
                    visit(child, metrics)?;
                }
            }
            QueryExpr::Literal(_) | QueryExpr::EvalTimestamp => {}
            // In particular, info() has an implicit metadata source that is
            // not a Scan child, and unnamed selectors require source discovery.
            _ => {
                return Err(invalid(
                    "exact-source pricing cannot enumerate this query's sources",
                ))
            }
        }
        Ok(())
    }
    let mut metrics = BTreeSet::new();
    visit(expr, &mut metrics)?;
    Ok(metrics)
}

pub(super) type PricedComponents = (Cost, BTreeMap<String, f64>);

impl WorkloadCostEvidence {
    fn validate(&self, env: &DeploymentEnvironment) -> Result<(), CompileError> {
        if self.backend_revision != super::compiler::BACKEND_REVISION
            || self.planner_revision != super::compiler::PLANNER_REVISION
        {
            return Err(invalid(format!(
                "cost evidence compiler mismatch: measured backend/planner {}/{}; running {}/{}",
                self.backend_revision,
                self.planner_revision,
                super::compiler::BACKEND_REVISION,
                super::compiler::PLANNER_REVISION
            )));
        }
        if self.data_snapshot_id.trim().is_empty()
            || self.model_version.trim().is_empty()
            || self.valid_for_ms == 0
            || self.observed_at_unix_ms > env.observed_at_unix_ms
            || env.observed_at_unix_ms - self.observed_at_unix_ms
                > self.valid_for_ms.min(env.max_evidence_age_ms)
        {
            return Err(invalid("missing, future or stale evidence generation"));
        }
        Ok(())
    }

    pub(super) fn price(
        &self,
        manifest: &WorkloadCostManifest,
    ) -> Result<PricedComponents, (&'static str, String)> {
        let quotes = self
            .quotes
            .iter()
            .filter(|quote| &quote.manifest == manifest)
            .collect::<Vec<_>>();
        if quotes.len() != 1 {
            return Err((
                "evidence_missing",
                "missing or ambiguous quote for exact manifest".into(),
            ));
        }
        let quote = quotes[0];
        if !quote.executable {
            return Err((
                "rejected",
                "provider reports unavailable implementation".into(),
            ));
        }
        if !quote.unit_costs.keys().eq(manifest.components.keys()) {
            return Err((
                "evidence_invalid",
                "incomplete or extraneous component evidence".into(),
            ));
        }
        let mut total = 0.0;
        let mut components = BTreeMap::new();
        for (id, demand) in &manifest.components {
            let unit = quote.unit_costs[id];
            let cost = unit * demand.multiplicity;
            if !unit.is_finite() || unit < 0.0 || !cost.is_finite() || cost < 0.0 {
                return Err(("evidence_invalid", format!("invalid cost for {id}")));
            }
            total += cost;
            components.insert(id.clone(), cost);
        }
        if !total.is_finite() {
            return Err(("evidence_invalid", "cost overflow".into()));
        }
        Ok((Cost(total), components))
    }
}

fn alternative_description(candidate: &PlanningRequest) -> AlternativeCost {
    let root_ids = candidate
        .queries
        .iter()
        .map(|query| crate::planner_selection::explained_root_id(&query.post_asap, &query.accuracy))
        .collect::<Vec<_>>();
    let complete = root_ids.iter().all(Option::is_some);
    let mut logical_root_ids = root_ids.into_iter().flatten().collect::<Vec<_>>();
    logical_root_ids.sort();
    logical_root_ids.dedup();
    AlternativeCost {
        alternative_id: complete.then(|| {
            crate::planner_selection::explain_identity(
                "alternative",
                &(
                    &logical_root_ids,
                    candidate.hybrid_execution,
                    &candidate.materialization_policy,
                ),
            )
        }),
        logical_root_ids,
        identity_unavailable_reason: (!complete)
            .then(|| "lossless canonical executable export unavailable for a logical root".into()),
        physical_alternative_id: None,
        status: "bind_failed".into(),
        plan_id: None,
        total_cost: None,
        unavailable_reason: None,
    }
}

fn bind_alternative(
    candidate: PlanningRequest,
    env: DeploymentEnvironment,
    metricsql: bool,
) -> Result<(PhysicalPlan, WorkloadCostManifest, AlternativeCost), Box<AlternativeCost>> {
    let mut description = alternative_description(&candidate);
    let queries = candidate.queries.clone();
    let compiled = super::realization::RealizationProvider::compile(
        &super::realization::ExistingRealizations,
        candidate,
        env,
        metricsql,
    );
    let plan = match compiled {
        Ok(plan) => plan,
        Err(error) => {
            description.unavailable_reason = Some(error.to_string());
            return Err(Box::new(description));
        }
    };
    description.plan_id = Some(plan.envelope.plan_id);
    // Reuse concrete catalog identities and selected window implementations.
    // Deployment epochs/plan IDs and evidence prices do not identify semantics.
    let mut bindings = plan
        .lifecycle_estimates
        .iter()
        .map(|item| (item.materialization, item.window_implementation_id.clone()))
        .collect::<Vec<_>>();
    bindings.sort();
    let mut placement = plan
        .collector_plans
        .iter()
        .map(|collector| {
            let mut states = collector
                .materializations
                .iter()
                .map(|state| state.materialization)
                .collect::<Vec<_>>();
            states.sort();
            (&collector.collector_id, states)
        })
        .collect::<Vec<_>>();
    placement.sort();
    description.physical_alternative_id = description.alternative_id.as_ref().map(|alternative| {
        crate::planner_selection::explain_identity(
            "physical",
            &(
                alternative,
                bindings,
                placement,
                &plan.precompute_plan.ingest,
            ),
        )
    });

    match manifest(&plan, &queries) {
        Ok(manifest) => {
            description.status = "bound".into();
            Ok((plan, manifest, description))
        }
        Err(error) => {
            description.unavailable_reason = Some(error.to_string());
            Err(Box::new(description))
        }
    }
}

/// Preserve failed bindings alongside quoteable manifests. This does not select or publish.
pub fn prepare_manifests(
    candidates: Vec<PlanningRequest>,
    env: DeploymentEnvironment,
    metricsql: bool,
) -> (Vec<WorkloadCostManifest>, Vec<AlternativeCost>) {
    let mut manifests = Vec::new();
    let mut alternatives = Vec::new();
    for candidate in candidates {
        match bind_alternative(candidate, env.clone(), metricsql) {
            Ok((_, manifest, description)) => {
                manifests.push(manifest);
                alternatives.push(description);
            }
            Err(description) => alternatives.push(*description),
        }
    }
    (manifests, alternatives)
}

/// Compare complete Planner-authorized forests after binding. Infeasible or
/// uncosted alternatives are retained as unavailable, never assigned zero.
pub fn select(
    candidates: Vec<PlanningRequest>,
    env: DeploymentEnvironment,
    evidence: &WorkloadCostEvidence,
) -> Result<PhysicalPlan, CompileError> {
    select_with_frontend(candidates, env, evidence, false)
}

pub fn select_metricsql(
    candidates: Vec<PlanningRequest>,
    env: DeploymentEnvironment,
    evidence: &WorkloadCostEvidence,
) -> Result<PhysicalPlan, CompileError> {
    select_with_frontend(candidates, env, evidence, true)
}

fn select_with_frontend(
    candidates: Vec<PlanningRequest>,
    env: DeploymentEnvironment,
    evidence: &WorkloadCostEvidence,
    metricsql: bool,
) -> Result<PhysicalPlan, CompileError> {
    evidence.validate(&env)?;
    if candidates.is_empty() || candidates.len() > 64 {
        return Err(invalid(
            "candidate inventory must contain 1..=64 alternatives",
        ));
    }
    let policies: BTreeSet<_> = candidates
        .iter()
        .filter(|c| c.hybrid_execution)
        .filter_map(|c| c.materialization_policy.clone())
        .collect();
    let leaves: BTreeSet<_> = policies.iter().flat_map(|p| p.iter().cloned()).collect();
    let materialization_search_coverage = (!policies.is_empty()).then(|| MaterializationSearchCoverage {
        eligible_leaves: leaves.len(),
        enumerated_local_masks: policies.len(),
        exhaustive: leaves.len() < usize::BITS as usize && policies.len() == (1usize << leaves.len()),
        scope: "Backend materialization versus Prometheus exact-subquery masks over Planner-authorized leaves; native alternative separate; bounded inventory does not claim an unenumerated optimum".into(),
    });
    let logical_selection = candidates[0].logical_selection.clone();
    let mut comparison_workload = None;
    let mut alternatives = Vec::new();
    let mut best_index = 0;
    let mut best: Option<(
        Cost,
        PhysicalPlan,
        WorkloadCostManifest,
        BTreeMap<String, f64>,
    )> = None;
    for candidate in candidates {
        let (plan, manifest, mut description) =
            match bind_alternative(candidate, env.clone(), metricsql) {
                Ok(bound) => bound,
                Err(description) => {
                    alternatives.push(*description);
                    continue;
                }
            };
        let scope = (manifest.workload.clone(), manifest.horizon_seconds);
        if comparison_workload
            .as_ref()
            .is_some_and(|previous| previous != &scope)
        {
            return Err(invalid(
                "alternatives describe different workloads/horizons",
            ));
        }
        comparison_workload = Some(scope);
        match super::realization::RealizationProvider::price(
            &super::realization::ExistingRealizations,
            evidence,
            &manifest,
        ) {
            Ok((cost, components)) => {
                description.status = "unselected".into();
                description.total_cost = Some(cost.0);
                alternatives.push(description);
                if best.as_ref().is_none_or(|(previous, ..)| cost < *previous) {
                    best_index = alternatives.len() - 1;
                    best = Some((cost, plan, manifest, components));
                }
            }
            Err((status, reason)) => {
                description.status = status.into();
                description.unavailable_reason = Some(reason);
                alternatives.push(description);
            }
        }
    }
    let (_, mut plan, selected_manifest, component_costs) = best.ok_or_else(|| {
        CompileError::Alternatives(
            json!({"status": "all_infeasible", "logical_selection": logical_selection,
            "alternatives": alternatives}),
        )
    })?;
    // Exactly the winner retained by the existing strict-less-than selector.
    alternatives[best_index].status = "selected".into();
    plan.cost_comparison = Some(WorkloadCostComparison {
        logical_selection,
        materialization_search_coverage,
        data_snapshot_id: evidence.data_snapshot_id.clone(),
        model_version: evidence.model_version.clone(),
        selected_plan_id: plan.envelope.plan_id,
        selected_manifest,
        component_costs,
        alternatives,
    });
    Ok(plan)
}

/// The current executor exposes continuously maintained state and the native
/// exact backend. Additional Planner-produced forests can use `select` directly.
pub fn with_exact_alternative(
    request: PlanningRequest,
) -> Result<Vec<PlanningRequest>, CompileError> {
    let mut exact = request.clone();
    exact.hybrid_execution = false;
    exact.materialization_policy = None;
    for query in &mut exact.queries {
        let parsed = crate::query_parser::parse_query_expr_canonical(
            &query.query_string,
            query.accuracy.clone(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        query.post_asap = crate::planner_selection::keep_pre_asap(&parsed)
            .map_err(|error| invalid(error.to_string()))?;
    }
    if !request.hybrid_execution
        && request
            .queries
            .iter()
            .zip(&exact.queries)
            .all(|(a, b)| a.post_asap == b.post_asap)
    {
        Ok(vec![request])
    } else {
        if !request.hybrid_execution || request.materialization_policy.is_some() {
            return Ok(vec![request, exact]);
        }
        let mut keys = BTreeSet::new();
        for query in &request.queries {
            match crate::query_plan::logical::materialization_candidate_keys(
                &query.query_string,
                &query.post_asap,
            ) {
                Ok(found) => keys.extend(found),
                // A failed local projection must not make the native alternative
                // disappear. Compile/select retains its concrete unavailability.
                Err(_) => return Ok(vec![request, exact]),
            }
        }
        if keys.is_empty() {
            return Ok(vec![request, exact]);
        }
        let inventory = materialization_candidates::enumerate(keys);
        debug_assert_eq!(inventory.exhaustive, inventory.eligible_leaves <= 4);
        let mut alternatives: Vec<_> = inventory
            .masks
            .into_iter()
            .map(|mask| {
                let mut candidate = request.clone();
                candidate.materialization_policy = Some(mask);
                candidate
            })
            .collect();
        alternatives.push(exact);
        Ok(alternatives)
    }
}

#[cfg(test)]
mod tests {
    use super::super::compiler::BackendLocalPlanningSnapshot;
    use super::*;

    fn fixture() -> BackendLocalPlanningSnapshot {
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        snapshot.query_workload.repeating_queries.as_mut().unwrap()[0].query =
            planner_types::workload::Query("sum(sum_over_time(m[1m]))".into());
        snapshot
    }

    // IDs describe semantics; activation/version changes do not create new alternatives.
    #[test]
    fn explain_identity_is_stable_across_activations_and_distinguishes_native() {
        let (request, mut env) = fixture().planning_request().unwrap();
        let candidates = with_exact_alternative(request).unwrap();
        let (_, first) = prepare_manifests(candidates.clone(), env.clone(), false);
        env.plan_version += 1;
        env.activation_unix_ms += 1;
        let (_, second) = prepare_manifests(candidates, env, false);
        assert_eq!(first.len(), second.len());
        for (a, b) in first.iter().zip(&second) {
            assert!(a.alternative_id.is_some());
            assert!(a.physical_alternative_id.is_some());
            assert_eq!(a.alternative_id, b.alternative_id);
            assert_eq!(a.physical_alternative_id, b.physical_alternative_id);
        }
        assert_ne!(
            first.first().unwrap().alternative_id,
            first.last().unwrap().alternative_id
        );
    }

    // Bind failures remain visible even when the native manifest is usable.
    #[test]
    fn explain_retains_failed_bindings_and_all_missing_quotes() {
        let (mut request, env) = fixture().planning_request().unwrap();
        request.hybrid_execution = false;
        request.queries[0].window_implementations.clear();
        let candidates = with_exact_alternative(request).unwrap();
        let (manifests, explanations) = prepare_manifests(candidates, env, false);
        assert_eq!(manifests.len(), 1);
        assert_eq!(explanations.len(), 2);
        assert_eq!(explanations[0].status, "bind_failed");
        assert!(explanations[0].unavailable_reason.is_some());
        assert_eq!(explanations[1].status, "bound");

        let (candidates, env, mut evidence) = quoted();
        let count = candidates.len();
        evidence.quotes.clear();
        let CompileError::Alternatives(report) = select(candidates, env, &evidence).unwrap_err()
        else {
            panic!("expected structured all-infeasible report")
        };
        assert_eq!(report["status"], "all_infeasible");
        let alternatives = report["alternatives"].as_array().unwrap();
        assert_eq!(alternatives.len(), count);
        assert!(alternatives
            .iter()
            .all(|item| item["status"] == "evidence_missing"));
        assert!(!report["logical_selection"].as_array().unwrap().is_empty());
    }

    // Explanation records the same minimum quote selected by the existing algorithm.
    #[test]
    fn explain_links_selected_roots_without_changing_cost_choice() {
        let (candidates, env, evidence) = quoted();
        let expected = evidence
            .quotes
            .iter()
            .map(|quote| {
                let cost = evidence.price(&quote.manifest).unwrap().0 .0;
                (cost, quote.manifest.plan_id)
            })
            .min_by(|a, b| a.0.total_cmp(&b.0))
            .unwrap();
        let plan = select(candidates, env, &evidence).unwrap();
        assert_eq!(plan.envelope.plan_id, expected.1);
        let comparison = plan.cost_comparison.unwrap();
        let selected = comparison
            .alternatives
            .iter()
            .filter(|item| item.status == "selected")
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].total_cost, Some(expected.0));
        assert!(selected[0].alternative_id.is_some());
        assert!(selected[0].physical_alternative_id.is_some());
        assert_eq!(comparison.logical_selection, plan.logical_selection);
    }

    #[test]
    fn filtered_max_materializations_have_distinct_sds_populations() {
        let mut snapshot = fixture();
        let entries = snapshot.query_workload.repeating_queries.as_mut().unwrap();
        entries[0].query = planner_types::workload::Query(
            "max_over_time(service_retry_queue_depth{job=~\".+\"}[6h])".into(),
        );
        entries[0].requirements.accuracy = planner_types::workload::AccuracyRequirement::Explicit(
            planner_types::types::AccuracyTarget::Exact,
        );
        entries[0].time_selection.lookback = Some(planner_types::workload::DurationMs(21_600_000));
        let mut second = entries[0].clone();
        second.query = planner_types::workload::Query(
            "max_over_time(service_retry_queue_depth{job=\"order-service\"}[6h])".into(),
        );
        entries.push(second);
        let (request, env) = snapshot.planning_request().unwrap();
        let plan = PhysicalCompiler.compile(request.clone(), env).unwrap();
        let costs = manifest(&plan, &request.queries).unwrap();
        assert_eq!(
            costs
                .components
                .keys()
                .filter(|id| id.starts_with("state:"))
                .count(),
            8
        );
        assert_eq!(
            costs
                .components
                .keys()
                .filter(|id| id.starts_with("raw-state:"))
                .count(),
            0
        );
        assert_eq!(
            plan.query_plan
                .entries
                .values()
                .flat_map(|entry| entry.nodes.values())
                .filter(|node| matches!(
                    node,
                    crate::query_plan::QueryPlanNode::ReadMaterialization { .. }
                ))
                .count(),
            2
        );
    }

    #[test]
    fn mixed_materialization_masks_price_state_and_prometheus_subquery_sources() {
        let mut snapshot = fixture();
        let q = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        q.query =
            planner_types::workload::Query("max_over_time(a[1m]) + max_over_time(b[1m])".into());
        q.requirements.accuracy = planner_types::workload::AccuracyRequirement::Explicit(
            crate::types_v2::AccuracyTarget::Exact,
        );
        let (request, environment) = snapshot.planning_request().unwrap();
        let candidates = with_exact_alternative(request).unwrap();
        assert_eq!(candidates.len(), 5, "four legal masks plus native");
        let mut identities = BTreeSet::new();
        for candidate in &candidates[..4] {
            let enabled = candidate.materialization_policy.as_ref().unwrap().len();
            let plan = PhysicalCompiler
                .compile(candidate.clone(), environment.clone())
                .unwrap();
            assert!(identities.insert(plan.envelope.plan_id));
            let cost = manifest(&plan, &candidate.queries).unwrap();
            assert_eq!(
                cost.components
                    .keys()
                    .filter(|k| k.starts_with("state:backend:"))
                    .count(),
                enabled * 4
            );
            assert_eq!(
                cost.components
                    .keys()
                    .filter(|k| k.starts_with("raw-state:"))
                    .count(),
                0
            );
            assert_eq!(
                cost.components
                    .values()
                    .filter(|v| v.unit == "horizon"
                        && v.implementation.get("location").and_then(Value::as_str)
                            == Some("exact_backend"))
                    .count(),
                2 - enabled
            );
            assert!(!plan
                .query_plan
                .entries
                .values()
                .any(|entry| entry.nodes.values().any(|node| matches!(
                    node,
                    crate::query_plan::QueryPlanNode::ExactFallback { .. }
                ))));
        }
        assert!(!candidates.last().unwrap().hybrid_execution);
    }

    fn quoted() -> (
        Vec<PlanningRequest>,
        DeploymentEnvironment,
        WorkloadCostEvidence,
    ) {
        let (request, env) = fixture().planning_request().unwrap();
        let candidates = with_exact_alternative(request).unwrap();
        let quotes = candidates
            .iter()
            .map(|candidate| {
                let plan = PhysicalCompiler
                    .compile(candidate.clone(), env.clone())
                    .unwrap();
                let manifest = manifest(&plan, &candidate.queries).unwrap();
                let unit_costs = manifest
                    .components
                    .keys()
                    .map(|id| (id.clone(), 1.0))
                    .collect();
                WorkloadQuote {
                    manifest,
                    executable: true,
                    unit_costs,
                }
            })
            .collect();
        let evidence = WorkloadCostEvidence {
            backend_revision: crate::physical::compiler::BACKEND_REVISION.into(),
            planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
            data_snapshot_id: "fixture-data-v1".into(),
            model_version: "test-only-unit-costs".into(),
            observed_at_unix_ms: env.observed_at_unix_ms,
            valid_for_ms: env.max_evidence_age_ms,
            quotes,
        };
        (candidates, env, evidence)
    }

    // Retained local input is priced once per metric, separate from the native service.
    #[test]
    fn counter_materialization_manifest_prices_owned_state_and_distinct_native_alternative() {
        use planner_types::workload::{AccuracyRequirement, Query};
        let mut snapshot = fixture();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query = Query("sum(rate(a{job=\"x\"}[1m])) / sum(rate(a{job!=\"x\"}[5m]))".into());
        entry.requirements.accuracy =
            AccuracyRequirement::Explicit(crate::types_v2::AccuracyTarget::Exact);
        let (request, environment) = snapshot.planning_request().unwrap();
        let candidates = with_exact_alternative(request).unwrap();
        assert!(candidates.len() >= 2);
        let local = PhysicalCompiler
            .compile(candidates[0].clone(), environment.clone())
            .unwrap();
        let native = PhysicalCompiler
            .compile(candidates.last().unwrap().clone(), environment)
            .unwrap();
        assert_ne!(local.envelope.plan_id, native.envelope.plan_id);
        let manifest = manifest(&local, &candidates[0].queries).unwrap();
        assert_eq!(
            manifest
                .components
                .keys()
                .filter(|key| key.starts_with("raw-state:a:"))
                .count(),
            0
        );
        assert_eq!(
            manifest
                .components
                .values()
                .filter(|demand| demand.implementation.get("location")
                    == Some(&serde_json::json!("backend")))
                .count(),
            1
        );
        assert!(manifest
            .components
            .values()
            .any(|demand| demand.implementation.get("location")
                == Some(&serde_json::json!("exact_backend"))));
    }

    // All input metrics need upkeep quotes; repeated reads share that upkeep.
    #[test]
    fn exact_manifest_covers_and_deduplicates_query_sources() {
        for (query, expected) in [
            (
                "sum_over_time(m[1m]) + sum_over_time(n[1m])",
                vec!["m", "n"],
            ),
            ("sum_over_time(m[1m]) + count_over_time(m[1m])", vec!["m"]),
        ] {
            let (mut request, env) = fixture().planning_request().unwrap();
            request.queries[0].query_string = query.into();
            request
                .query_workload
                .as_mut()
                .unwrap()
                .repeating_queries
                .as_mut()
                .unwrap()[0]
                .query = planner_types::workload::Query(query.into());
            let exact = with_exact_alternative(request).unwrap().pop().unwrap();
            let plan = PhysicalCompiler
                .compile(exact.clone(), env.clone())
                .unwrap();
            let manifest = manifest(&plan, &exact.queries).unwrap();
            let sources: Vec<_> = manifest
                .components
                .iter()
                .filter(|(id, _)| id.starts_with("source:"))
                .map(|(_, demand)| {
                    demand.implementation["source"]["TimeSeries"]["metric"]
                        .as_str()
                        .unwrap()
                })
                .collect();
            assert_eq!(sources, expected, "{query}");
            let mut evidence = WorkloadCostEvidence {
                backend_revision: crate::physical::compiler::BACKEND_REVISION.into(),
                planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
                data_snapshot_id: "test-data".into(),
                model_version: "test-model".into(),
                observed_at_unix_ms: env.observed_at_unix_ms,
                valid_for_ms: env.max_evidence_age_ms,
                quotes: vec![WorkloadQuote {
                    unit_costs: manifest
                        .components
                        .keys()
                        .map(|id| (id.clone(), 1.0))
                        .collect(),
                    manifest,
                    executable: true,
                }],
            };
            assert!(select(vec![exact.clone()], env.clone(), &evidence).is_ok());
            let source_id = evidence.quotes[0]
                .unit_costs
                .keys()
                .rfind(|id| id.starts_with("source:"))
                .unwrap()
                .clone();
            evidence.quotes[0].unit_costs.remove(&source_id);
            assert!(
                select(vec![exact], env, &evidence).is_err(),
                "missing input upkeep must fail closed"
            );
        }
    }

    // Hidden or unresolved sources must not yield a partially priced manifest.
    #[test]
    fn exact_source_discovery_rejects_unresolved_inputs() {
        let accuracy = fixture().planning_request().unwrap().0.queries[0]
            .accuracy
            .clone();
        for query in ["info(m)", "{job=\"api\"}"] {
            let parsed =
                crate::query_parser::parse_query_expr_canonical(query, accuracy.clone()).unwrap();
            assert!(exact_source_metrics(&parsed).is_err(), "{query}");
        }
    }

    #[test]
    fn complete_cost_changes_selection_and_reports_shared_work_once() {
        let (candidates, env, mut evidence) = quoted();
        assert_ne!(evidence.quotes[0].manifest, evidence.quotes[1].manifest);
        assert!(evidence.quotes[0]
            .manifest
            .components
            .keys()
            .any(|key| key.starts_with("state:")));
        assert!(!evidence.quotes[1]
            .manifest
            .components
            .keys()
            .any(|key| key.starts_with("state:")));
        for cost in evidence.quotes[1].unit_costs.values_mut() {
            *cost = 1000.0;
        }
        let warm = select(candidates.clone(), env.clone(), &evidence).unwrap();
        assert_eq!(warm.envelope.plan_id, evidence.quotes[0].manifest.plan_id);
        let report = warm.cost_comparison.unwrap();
        assert_eq!(
            report.component_costs.len(),
            report.selected_manifest.components.len()
        );
        assert_eq!(report.alternatives.len(), 2);
        assert!(report.alternatives.iter().all(|a| a.total_cost.is_some()));
        for (id, cost) in &mut evidence.quotes[0].unit_costs {
            if id.ends_with(":residency") {
                *cost = 1e9;
            }
        }
        let raw = select(candidates, env, &evidence).unwrap();
        assert_eq!(raw.envelope.plan_id, evidence.quotes[1].manifest.plan_id);
    }

    #[test]
    fn incomplete_unavailable_and_wrong_generation_quotes_are_not_free() {
        let (candidates, env, mut evidence) = quoted();
        evidence.quotes[0].unit_costs.pop_first();
        let plan = select(candidates.clone(), env.clone(), &evidence).unwrap();
        assert!(plan.cost_comparison.unwrap().alternatives[0]
            .unavailable_reason
            .is_some());
        evidence.quotes[1].executable = false;
        assert!(select(candidates.clone(), env.clone(), &evidence).is_err());
        let (_, _, mut evidence) = quoted();
        evidence
            .quotes
            .iter_mut()
            .for_each(|quote| quote.manifest.capability_snapshot_id.push_str("-wrong"));
        assert!(select(candidates.clone(), env.clone(), &evidence).is_err());
        let (_, _, mut evidence) = quoted();
        evidence.observed_at_unix_ms = env.observed_at_unix_ms + 1;
        assert!(select(candidates, env, &evidence).is_err());
    }

    #[test]
    fn evidence_from_a_different_compiler_build_is_rejected_before_matching_quotes() {
        let (candidates, env, mut evidence) = quoted();
        evidence.backend_revision = "stale-backend-build".into();
        let error = select(candidates, env, &evidence).unwrap_err().to_string();
        assert!(error.contains("cost evidence compiler mismatch"), "{error}");
    }

    #[test]
    fn snapshot_requires_quotes_and_roundtrips_selection() {
        let mut snapshot = fixture();
        assert!(snapshot.clone().compile().is_err());
        let (_, _, evidence) = quoted();
        snapshot.workload_cost_evidence = Some(evidence);
        let snapshot: BackendLocalPlanningSnapshot =
            serde_json::from_str(&serde_json::to_string(&snapshot).unwrap()).unwrap();
        assert!(snapshot.compile().unwrap().cost_comparison.is_some());
    }

    #[test]
    fn second_consumer_adds_reads_not_another_shared_state() {
        let (request, env) = fixture().planning_request().unwrap();
        let first = manifest(
            &PhysicalCompiler
                .compile(request.clone(), env.clone())
                .unwrap(),
            &request.queries,
        )
        .unwrap();
        let mut shared = request.clone();
        let mut second = shared.queries[0].clone();
        second.query_id = "second-consumer".into();
        second.query_string = "sum(sum_over_time(m[1m])) * 2".into();
        let entries = shared
            .query_workload
            .as_mut()
            .unwrap()
            .repeating_queries
            .as_mut()
            .unwrap();
        let mut demand = entries[0].clone();
        demand.query = planner_types::workload::Query(second.query_string.clone());
        entries.push(demand);
        shared.queries.push(second);
        let roots = shared
            .queries
            .iter()
            .map(|query| {
                std::rc::Rc::new(
                    crate::query_parser::parse_query_expr_canonical(
                        &query.query_string,
                        query.accuracy.clone(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        super::super::compiler::select_workload_roots(
            &mut shared.queries,
            roots,
            &shared.evidence,
            &shared.exact_composition_costs,
        )
        .unwrap();
        let plan = PhysicalCompiler.compile(shared.clone(), env).unwrap();
        assert_eq!(plan.precompute_plan.materializations.len(), 1);
        let second = manifest(&plan, &shared.queries).unwrap();
        let states = |m: &WorkloadCostManifest| {
            m.components
                .keys()
                .filter(|key| key.starts_with("state:"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(states(&first), states(&second));
        assert!(second.components.len() > first.components.len());
    }

    #[test]
    fn exact_alternative_does_not_require_unused_state_implementation_evidence() {
        let (candidates, env, evidence) = quoted();
        let mut exact = candidates[1].clone();
        assert_eq!(with_exact_alternative(exact.clone()).unwrap().len(), 1);
        exact.queries[0].window_implementations.clear();
        assert!(select(vec![exact], env, &evidence).is_ok());
    }

    #[test]
    fn altered_horizon_duplicate_and_invalid_costs_are_rejected() {
        let (candidates, env, mut evidence) = quoted();
        evidence
            .quotes
            .iter_mut()
            .for_each(|quote| quote.manifest.horizon_seconds += 1.0);
        assert!(select(candidates.clone(), env.clone(), &evidence).is_err());
        let (_, _, mut evidence) = quoted();
        evidence.quotes.extend(evidence.quotes.clone());
        assert!(select(candidates.clone(), env.clone(), &evidence).is_err());
        let (_, _, mut evidence) = quoted();
        for quote in &mut evidence.quotes {
            *quote.unit_costs.values_mut().next().unwrap() = -1.0;
        }
        assert!(select(candidates, env, &evidence).is_err());
    }
}
