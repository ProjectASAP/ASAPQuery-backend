//! Deployment family selection and parameter sizing for ASAPPlanner binding.
//!
//! `rank_candidates` chooses a family; `size_params` sets its parameters.
//! Frequency extensions use `realize_extension` and `readout_extension`.
//! Exact TopK falls back to logical execution upstream because it has no
//! exact accumulator realization; these cost-model hooks cannot bind it.

#![allow(dead_code)]

use asap_aware_mapping::cost_model::{Cost, CostedSummaryDeployment};
use asap_aware_mapping::empirical_comparison::{
    recommend_offline, OfflineComparisonEvidence, OfflineComparisonRequest, OfflineRecommendation,
    SketchConfiguration,
};
use asap_aware_mapping::empirical_cost::EmpiricalEvidenceProvider;
use asap_aware_mapping::{
    CompleteSummaryCandidateEstimate, CostModel, CostProvenance, EvaluationRate,
    ExactCompositionCostInputs, ExactCompositionCostRequest, Horizon, OperationPlacement,
    Realization, Replacement, ReplacementSubDAG, SummaryMaintenanceCapabilities,
    SummaryMaintenanceLifecycleCostInputs, TargetSubDAG, ValueOperationCapabilities,
};
use planner_types::post_asap::{
    ExecutableOperatorPayload, GroupingStrategy, SketchAlgorithm, SketchParams, SketchQuery,
    SummaryFamilyType, SummaryWindowFramework,
};
use planner_types::pre_asap::expr_ir::ColumnRef;

use crate::physical::deployment_cost::wire::WireCostTable;
use crate::physical::erp::{ErpParameterDecision, ErpPlanningInput};
use crate::planner_selection::FREQUENCY_EXT_KIND;
use crate::types::AccuracyTarget;
use planner_types::pre_asap::AggIntent;
use serde::{Deserialize, Serialize};

/// A local state-footprint estimate, never a complete deployment quote.
#[derive(Debug, Serialize)]
pub struct CandidateCostEstimate {
    pub value: f64,
    pub unit: &'static str,
    pub model: &'static str,
    pub source: &'static str,
    pub erp_record_ids: Vec<String>,
}

fn analytical_state_bytes(family: &SummaryFamilyType) -> Option<f64> {
    use asap_types::AggregationType as A;
    use planner_types::post_asap::ExactKind;
    let (aggregation, params) = match family {
        SummaryFamilyType::ExactAggregate(kind, _) => (
            match kind {
                ExactKind::Sum | ExactKind::Count => A::Sum,
                ExactKind::Min => A::Min,
                ExactKind::Max => A::Max,
                ExactKind::Increase | ExactKind::Rate | ExactKind::IRate => A::Increase,
            },
            std::collections::HashMap::new(),
        ),
        SummaryFamilyType::Sketch(kind, GroupingStrategy::PerSubpopulationInstance) => {
            let aggregation = match kind.algorithm() {
                SketchAlgorithm::DDSketch => A::DDSketch,
                SketchAlgorithm::Kll => A::DatasketchesKLL,
                SketchAlgorithm::Hll => A::HLL,
                SketchAlgorithm::Cms => A::CountMinSketch,
                SketchAlgorithm::CountSketch => A::CountSketch,
                SketchAlgorithm::CmsWithHeap => A::CountMinSketchWithHeap,
                SketchAlgorithm::CountSketchWithHeap => A::CountSketchWithHeap,
                SketchAlgorithm::UnivMon => A::UnivMon,
                SketchAlgorithm::Kmv | SketchAlgorithm::Theta => return None,
            };
            let params = super::super::compiler::sketch_params_json(kind.params())
                .as_object()?
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            (aggregation, params)
        }
        // A shared grid needs its own population/layout model; an independent
        // state's measurement is not a measurement of that grid.
        _ => return None,
    };
    Some(super::super::compiler::estimated_state_bytes(&aggregation, &params) as f64)
}

/// One measured execution profile for an exact operator composed with a
/// maintained summary. Values use CPU nanoseconds so every term in Planner's
/// recurring-cost formula has the same physical unit. Peak memory is retained
/// as measured resource evidence and reported separately; it is deliberately
/// not converted into CPU cost by an invented weight.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExactCompositionCostEvidence {
    /// Exact pre-ASAP target serialized with the pinned Planner revision.
    pub target: serde_json::Value,
    /// Exact operation serialized with the pinned Planner revision.
    pub operation: serde_json::Value,
    pub placement: ExactOperationPlacement,
    pub expected_input_rows: f64,
    pub expected_output_rows: f64,
    pub exact_cpu_ns_per_row: f64,
    pub summary_maintenance_cpu_ns_per_update: f64,
    pub summary_read_cpu_ns: f64,
    pub update_rate_per_second: f64,
    pub evaluation_rate_per_second: f64,
    pub raw_recompute_cpu_ns: f64,
    pub observed_peak_memory_bytes: u64,
    pub data_snapshot_id: String,
    pub model_version: String,
    pub observed_at_unix_ms: u64,
    pub valid_for_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExactOperationPlacement {
    Read,
    Maintenance,
}

impl ExactCompositionCostEvidence {
    pub fn validate(&self, now_unix_ms: u64, max_age_ms: u64) -> Result<(), String> {
        let positive = [
            self.exact_cpu_ns_per_row,
            self.summary_maintenance_cpu_ns_per_update,
            self.summary_read_cpu_ns,
            self.update_rate_per_second,
            self.evaluation_rate_per_second,
            self.raw_recompute_cpu_ns,
        ];
        let rows = [self.expected_input_rows, self.expected_output_rows];
        if positive.iter().any(|v| !v.is_finite() || *v <= 0.0)
            || rows.iter().any(|v| !v.is_finite() || *v < 0.0)
            || self.observed_peak_memory_bytes == 0
            || self.data_snapshot_id.trim().is_empty()
            || self.model_version.trim().is_empty()
            || self.valid_for_ms == 0
            || self.observed_at_unix_ms > now_unix_ms
            || now_unix_ms - self.observed_at_unix_ms > self.valid_for_ms.min(max_age_ms)
        {
            return Err(
                "missing, non-physical, future, or stale exact-composition evidence".into(),
            );
        }
        Ok(())
    }

    fn matches(&self, request: &ExactCompositionCostRequest<'_>) -> bool {
        let placement = match request.composition.placement {
            OperationPlacement::Read => ExactOperationPlacement::Read,
            OperationPlacement::Maintenance => ExactOperationPlacement::Maintenance,
        };
        self.placement == placement
            && serde_json::to_value(request.target).ok().as_ref() == Some(&self.target)
            && serde_json::to_value(&request.composition.op).ok().as_ref() == Some(&self.operation)
    }
}

/// See module docs.
pub struct ControlPlaneCostModel {
    /// Workload-level accuracy policy — combined (tighter-of) with each
    /// intent's own accuracy field, matching every `bind_*.rs` rule's old
    /// `accuracy: &AccuracyTarget` parameter.
    pub workload_accuracy: AccuracyTarget,
    lifecycle_costs: SummaryMaintenanceLifecycleCostInputs,
    summary_maintenance: SummaryMaintenanceCapabilities,
    window_framework_costs: Vec<(Option<String>, SummaryWindowFramework, Cost)>,
    offline_evidence: Option<EmpiricalEvidenceProvider>,
    offline_frequency_comparison: Option<(OfflineComparisonEvidence, OfflineComparisonRequest)>,
    exact_composition_costs: Vec<ExactCompositionCostEvidence>,
    erp: Option<ErpPlanningInput>,
    erp_costs: Option<ErpPlanningInput>,
}

impl ControlPlaneCostModel {
    pub fn candidate_cost_estimate(
        &self,
        candidate: &ReplacementSubDAG,
    ) -> Option<CandidateCostEstimate> {
        let Replacement::Summary(root) = &candidate.replacement else {
            // Exact compositions have a separate measured rate model. Raw
            // rewrites have no retained-state estimate in this model.
            return None;
        };
        let dag = planner_types::post_asap::compile_executable_dag(root).ok()?;
        let mut value = 0.0;
        let mut states = 0;
        let mut analytical = false;
        let mut erp_record_ids = Vec::new();
        for node in &dag.nodes {
            let family = match &node.payload {
                ExecutableOperatorPayload::SummaryAgg { family, .. }
                | ExecutableOperatorPayload::SummaryJoin { family, .. } => family,
                _ => continue,
            };
            states += 1;
            let measurement = match family {
                SummaryFamilyType::Sketch(kind, GroupingStrategy::PerSubpopulationInstance) => self
                    .erp_costs
                    .as_ref()
                    .and_then(|erp| erp.candidate_memory_bytes(kind.algorithm(), kind.params())),
                _ => None,
            };
            if let Some((bytes, ids)) = measurement {
                value += bytes;
                erp_record_ids.extend(ids);
            } else {
                value += analytical_state_bytes(family)?;
                analytical = true;
            }
        }
        if states == 0 || !value.is_finite() {
            return None;
        }
        erp_record_ids.sort();
        erp_record_ids.dedup();
        Some(CandidateCostEstimate {
            value,
            unit: "bytes_per_state_partition",
            model: "backend_state_footprint_v1",
            source: if erp_record_ids.is_empty() {
                "analytical"
            } else if analytical {
                "mixed"
            } else {
                "erp"
            },
            erp_record_ids,
        })
    }

    pub fn new(workload_accuracy: AccuracyTarget) -> Self {
        Self {
            workload_accuracy,
            lifecycle_costs: SummaryMaintenanceLifecycleCostInputs::default(),
            summary_maintenance: SummaryMaintenanceCapabilities::default(),
            window_framework_costs: Vec::new(),
            offline_evidence: None,
            offline_frequency_comparison: None,
            exact_composition_costs: Vec::new(),
            erp: None,
            erp_costs: None,
        }
    }

    pub fn with_exact_composition_costs(
        mut self,
        costs: Vec<ExactCompositionCostEvidence>,
    ) -> Self {
        self.exact_composition_costs = costs;
        self
    }

    pub fn with_erp(mut self, erp: ErpPlanningInput) -> Self {
        self.erp_costs = Some(erp.clone());
        self.erp = Some(erp);
        self
    }

    /// Resource evidence can remain usable when ERP's error observations
    /// cannot establish the query's requested confidence guarantee.
    pub fn with_erp_costs(mut self, erp: ErpPlanningInput) -> Self {
        self.erp_costs = Some(erp);
        self
    }

    pub fn erp_parameter_decision(
        &self,
        algorithm: SketchAlgorithm,
        max_error: f64,
        theoretical: SketchParams,
    ) -> Option<ErpParameterDecision> {
        self.erp
            .as_ref()
            .map(|erp| erp.select(algorithm, max_error, theoretical))
    }

    /// Use offline update CPU evidence for algorithm ordering. Physical costs
    /// and accuracy guarantees retain their deployment-specific contracts.
    pub fn with_offline_evidence(mut self, evidence: EmpiricalEvidenceProvider) -> Self {
        self.offline_evidence = Some(evidence);
        self
    }

    pub fn offline_evidence(&self) -> Option<&EmpiricalEvidenceProvider> {
        self.offline_evidence.as_ref()
    }

    /// Opt into a fixed-snapshot frequency comparison. The caller asserts that
    /// the explicit point queries use the supplied integer-key distribution and
    /// offline probe population. The observed mean is an acceptance criterion
    /// over that population, not a per-key or real-time error guarantee.
    pub fn with_offline_frequency_comparison(
        mut self,
        evidence: OfflineComparisonEvidence,
        request: OfflineComparisonRequest,
    ) -> Self {
        self.offline_frequency_comparison = Some((evidence, request));
        self
    }

    /// Explain the same recommendation used by the frequency extension binder.
    /// Exact selection and any unavailable comparison preserve exact execution.
    pub fn offline_frequency_recommendation(
        &self,
        payload: &serde_json::Value,
    ) -> Result<OfflineRecommendation, String> {
        let (evidence, request) = self
            .offline_frequency_comparison
            .as_ref()
            .ok_or("offline frequency comparison is not configured")?;
        if payload
            .get("item_label")
            .and_then(|v| v.as_str())
            .is_none_or(str::is_empty)
            || payload
                .get("item_value")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<i64>().ok())
                .is_none()
        {
            return Err(
                "offline point-frequency comparison requires an explicit integer item readout"
                    .into(),
            );
        }
        let accuracy: AccuracyTarget = serde_json::from_value(
            payload
                .get("accuracy")
                .cloned()
                .ok_or("missing frequency accuracy")?,
        )
        .map_err(|error| error.to_string())?;
        let (eps, delta) = self
            .combined_eps_delta(&accuracy)
            .ok_or("exact accuracy requires exact execution")?;
        if !eps.is_finite()
            || eps <= 0.0
            || eps >= 1.0
            || !delta.is_finite()
            || delta <= 0.0
            || delta >= 1.0
        {
            return Err("invalid frequency accuracy budget".into());
        }
        let (width, depth) = Self::cms_width_depth(eps, delta);
        let width = width
            .checked_next_power_of_two()
            .ok_or("frequency width overflows")?;
        let mut request = request.clone();
        // Only CMS is supported by the backend's frequency capability table.
        // Caller-supplied minima cannot weaken workload/intent requirements.
        request.formal_minimums = Some(vec![SketchConfiguration {
            algorithm: SketchAlgorithm::Cms,
            params: SketchParams::Cms { width, depth },
        }]);
        evidence
            .sketch_evidence
            .validate()
            .map_err(|error| error.to_string())?;
        // Exclude layouts the backend cannot instantiate before selecting the
        // winner, so a cheap unsupported width cannot hide a legal measured one.
        let mut evidence = evidence.clone();
        evidence.sketch_evidence.records.retain(|row| {
            row.algorithm == SketchAlgorithm::Cms
                && matches!(&row.params, SketchParams::Cms { width, .. } if width.is_power_of_two())
        });
        evidence.query_bindings.retain(|binding| {
            evidence
                .sketch_evidence
                .records
                .iter()
                .any(|row| row.id == binding.record_id)
        });
        recommend_offline(&evidence, &request)
    }

    fn rank_with_offline_evidence(
        &self,
        intent: &AggIntent,
        defaults: Vec<SketchAlgorithm>,
    ) -> Vec<SketchAlgorithm> {
        let Some(provider) = &self.offline_evidence else {
            return defaults;
        };
        let accuracy = intent_accuracy(intent);
        if matches!(accuracy, AccuracyTarget::Exact) {
            return defaults;
        }
        let (eps, delta) = asap_aware_mapping::replacement::accuracy_budget(&accuracy);
        // Compare the parameters this deployment will actually bind, not the
        // planner's default sizing or another benchmark configuration.
        let costs: Option<Vec<_>> = defaults
            .iter()
            .map(|algorithm| {
                let params = self.size_params(algorithm.clone(), intent, eps, delta);
                let row = provider.lookup(algorithm, &params).ok()?;
                Some((
                    algorithm.clone(),
                    row.metrics.resources.cpu.update_cpu_ns.as_ref()?.value,
                ))
            })
            .collect();
        let Some(mut costs) = costs else {
            return defaults;
        };
        costs.sort_by(|left, right| left.1.total_cmp(&right.1));
        costs.into_iter().map(|(algorithm, _)| algorithm).collect()
    }

    fn rank_with_erp_costs(
        &self,
        intent: &AggIntent,
        defaults: Vec<SketchAlgorithm>,
    ) -> Vec<SketchAlgorithm> {
        let Some(erp) = &self.erp_costs else {
            return defaults;
        };
        let (eps, delta) =
            asap_aware_mapping::replacement::accuracy_budget(&intent_accuracy(intent));
        let mut has_measurement = false;
        let mut costs = defaults
            .iter()
            .map(|algorithm| {
                let params = self.size_params(algorithm.clone(), intent, eps, delta);
                let measured = erp.candidate_memory_bytes(algorithm, &params);
                has_measurement |= measured.is_some();
                let cost = measured.map(|(bytes, _)| bytes).or_else(|| {
                    analytical_state_bytes(&SummaryFamilyType::Sketch(
                        planner_types::post_asap::SketchKind::new(algorithm.clone(), params),
                        GroupingStrategy::PerSubpopulationInstance,
                    ))
                });
                (algorithm.clone(), cost)
            })
            .collect::<Vec<_>>();
        if !has_measurement {
            return defaults;
        }
        costs.sort_by(|(_, a), (_, b)| match (a, b) {
            (Some(a), Some(b)) => a.total_cmp(b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        costs.into_iter().map(|(algorithm, _)| algorithm).collect()
    }

    /// Keep concrete physical identities in Planner's complete-candidate estimate,
    /// including distinct pane sizes using the same abstract window framework.
    pub fn with_window_implementation_costs(
        mut self,
        costs: Vec<(String, SummaryWindowFramework, Cost)>,
    ) -> Self {
        self.window_framework_costs = costs
            .into_iter()
            .map(|(id, framework, cost)| (Some(id), framework, cost))
            .collect();
        self
    }

    pub fn with_summary_maintenance(
        mut self,
        lifecycle_costs: SummaryMaintenanceLifecycleCostInputs,
        capabilities: SummaryMaintenanceCapabilities,
    ) -> Self {
        self.lifecycle_costs = lifecycle_costs;
        self.summary_maintenance = capabilities;
        self
    }

    /// The tighter (lower) of the workload policy and an intent's own
    /// accuracy target, as `(eps, delta)`. `None` when either side is
    /// `Exact` — mirrors `bind_kll_quantile.rs` / `bind_ddsketch_quantile.rs`
    /// / `bind_hll_cardinality.rs` / `bind_cms_count.rs`'s identical
    /// `match (accuracy, &intent_accuracy) {...}` block. (`TopK` has its
    /// own combination rule — an `Exact` side there picks the *other*
    /// side's budget rather than bailing — see [`Self::topk_eps_delta`].)
    fn combined_eps_delta(&self, intent_accuracy: &AccuracyTarget) -> Option<(f64, f64)> {
        match (&self.workload_accuracy, intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => Some((a.min(*b), 0.01)),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { epsilon, delta })
            | (AccuracyTarget::EpsilonDelta { epsilon, delta }, AccuracyTarget::Epsilon(a)) => {
                Some((a.min(*epsilon), *delta))
            }
            (
                AccuracyTarget::EpsilonDelta {
                    epsilon: a,
                    delta: da,
                },
                AccuracyTarget::EpsilonDelta {
                    epsilon: b,
                    delta: db,
                },
            ) => Some((a.min(*b), da.min(*db))),
        }
    }

    /// `TopK`'s own `(eps, delta)` combination — verbatim port of
    /// `bind_cms_topk.rs`'s `bind` match. Unlike
    /// [`Self::combined_eps_delta`], an `Exact` side does not bail: it
    /// picks the *other* side's budget (falling back to the catalog
    /// default `(0.01, 0.01)` only when both sides are `Exact`). Public
    /// (within the crate) because [`crate::physical::post_asap::lower`]'s
    /// `TopK { accuracy: Exact }` pre-pass needs the same combination.
    pub(crate) fn topk_eps_delta(&self, intent_accuracy: &AccuracyTarget) -> (f64, f64) {
        match (&self.workload_accuracy, intent_accuracy) {
            (AccuracyTarget::Exact, AccuracyTarget::Exact) => (0.01, 0.01),
            (AccuracyTarget::Exact, other) | (other, AccuracyTarget::Exact) => match other {
                AccuracyTarget::Epsilon(a) => (*a, 0.01),
                AccuracyTarget::EpsilonDelta { epsilon, delta } => (*epsilon, *delta),
                AccuracyTarget::Exact => (0.01, 0.01),
            },
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => (a.min(*b), 0.01),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { epsilon, delta })
            | (AccuracyTarget::EpsilonDelta { epsilon, delta }, AccuracyTarget::Epsilon(a)) => {
                (a.min(*epsilon), *delta)
            }
            (
                AccuracyTarget::EpsilonDelta {
                    epsilon: a,
                    delta: da,
                },
                AccuracyTarget::EpsilonDelta {
                    epsilon: b,
                    delta: db,
                },
            ) => (a.min(*b), da.min(*db)),
        }
    }

    /// Recall-tier-filtered, wire-cost-ordered top-k family candidates.
    /// Verbatim port of `bind_cms_topk.rs`'s `TopkRecallTier` +
    /// `candidate_families` + `cheapest_family`. Public (within the
    /// crate) for the same reason as [`Self::topk_eps_delta`].
    pub(crate) fn topk_family_order(
        &self,
        intent_accuracy: &AccuracyTarget,
    ) -> Vec<SketchAlgorithm> {
        let tight = matches!(self.workload_accuracy, AccuracyTarget::Exact)
            || matches!(intent_accuracy, AccuracyTarget::Exact);
        let allowed: &[SketchAlgorithm] = if tight {
            &[SketchAlgorithm::CountSketchWithHeap]
        } else {
            &[
                SketchAlgorithm::CmsWithHeap,
                SketchAlgorithm::CountSketchWithHeap,
            ]
        };
        let table = WireCostTable::default();
        let mut ranked: Vec<SketchAlgorithm> = allowed.to_vec();
        ranked.sort_by_key(|algorithm| table.for_algorithm(algorithm).per_flush());
        ranked
    }

    /// `(width, depth)` for a CMS-family sketch under `(eps, delta)`.
    /// Verbatim: `w = ⌈e/eps⌉` clamped to `≥2`, `d = ⌈ln(1/delta)⌉`
    /// clamped to `≥1`.
    fn cms_width_depth(eps: f64, delta: f64) -> (u32, u32) {
        let w = (std::f64::consts::E / eps).ceil().max(2.0) as u32;
        let d = (1.0 / delta).ln().ceil().max(1.0) as u32;
        (w, d)
    }
}

/// The workload-level accuracy target combined with an intent's own —
/// pulls the intent's `accuracy` field out of whichever variant carries
/// one. `_` covers every non-approximate-capable variant, unreachable in
/// practice (`rank_candidates`/`size_params` are only ever called for
/// `Quantile`/`Cardinality`/`Count`/`TopK` — the shapes
/// `replacement::SketchAlgorithmStrategy::replacements` handles).
fn intent_accuracy(intent: &AggIntent) -> AccuracyTarget {
    match intent {
        AggIntent::Quantile { accuracy, .. }
        | AggIntent::Cardinality { accuracy, .. }
        | AggIntent::TopK { accuracy, .. }
        | AggIntent::FrequencyL2 { accuracy, .. }
        | AggIntent::FrequencyEntropy { accuracy, .. } => accuracy.clone(),
        AggIntent::Count { accuracy } => accuracy.clone(),
        _ => AccuracyTarget::Exact,
    }
}

impl CostModel for ControlPlaneCostModel {
    fn candidate_cost(
        &self,
        candidate: &ReplacementSubDAG,
        _target: &TargetSubDAG<'_>,
    ) -> Option<Cost> {
        self.candidate_cost_estimate(candidate)
            .map(|estimate| Cost(estimate.value))
    }

    fn value_operation_capabilities(&self) -> ValueOperationCapabilities {
        ValueOperationCapabilities {
            read_time: true,
            maintenance_time: false,
        }
    }

    fn exact_composition_cost_inputs(
        &self,
        request: &ExactCompositionCostRequest<'_>,
    ) -> ExactCompositionCostInputs {
        let provenance = || CostProvenance {
            model: "ASAPQuery measured exact composition".into(),
            version: "unavailable".into(),
        };
        let Some(row) = self
            .exact_composition_costs
            .iter()
            .find(|row| row.matches(request))
        else {
            return ExactCompositionCostInputs::unknown(provenance());
        };
        ExactCompositionCostInputs {
            exact_cost_per_row: Some(row.exact_cpu_ns_per_row),
            expected_input_rows: Some(row.expected_input_rows),
            expected_output_rows: Some(row.expected_output_rows),
            summary_maintenance_cost_per_update: Some(row.summary_maintenance_cpu_ns_per_update),
            summary_read_cost: Some(row.summary_read_cpu_ns),
            update_rate: Some(row.update_rate_per_second),
            evaluation_rate: Some(EvaluationRate(
                row.evaluation_rate_per_second * request.effective_consumer_count as f64,
            )),
            raw_recompute_cost: Some(row.raw_recompute_cpu_ns),
            unit: asap_aware_mapping::CostUnit::CostUnitsPerSecond,
            provenance: CostProvenance {
                model: format!(
                    "ASAPQuery measured exact composition ({})",
                    row.data_snapshot_id
                ),
                version: row.model_version.clone(),
            },
        }
    }

    fn summary_maintenance_lifecycle_cost_inputs(
        &self,
        _summary: &planner_types::post_asap::SummaryNode,
    ) -> SummaryMaintenanceLifecycleCostInputs {
        self.lifecycle_costs.clone()
    }

    fn summary_maintenance_capabilities(
        &self,
        _summary: &planner_types::post_asap::SummaryNode,
    ) -> SummaryMaintenanceCapabilities {
        self.summary_maintenance
    }

    fn complete_summary_candidate_estimate(
        &self,
        _root: &planner_types::post_asap::SummaryNode,
        _target: Option<&planner_types::pre_asap::QueryExpr>,
        deployments: &[CostedSummaryDeployment<'_>],
        _horizon: Option<Horizon>,
        _expected_reads: Option<f64>,
        _required_accuracy: &[AccuracyTarget],
    ) -> Option<CompleteSummaryCandidateEstimate> {
        // One compiler candidate describes the complete concrete realization
        // of this query DAG. The current executor exposes one anchored window
        // framework across all reachable summary states; represent that full
        // per-state assignment explicitly rather than relying on traversal
        // order or leaving any state uncosted.
        if deployments.is_empty() || self.window_framework_costs.is_empty() {
            return None;
        }
        let lifecycle_cost: f64 = deployments
            .iter()
            .map(|deployment| deployment.selected_cost.0)
            .sum();
        self.window_framework_costs
            .iter()
            // GOS/error propagation is introduced by the later adaptation
            // slice. Until then, do not claim an approximate exponential
            // histogram window is exact.
            .filter(|(_, framework, _)| {
                !matches!(framework, SummaryWindowFramework::ExponentialHistogram)
            })
            .filter(|(_, _, cost)| cost.0.is_finite() && cost.0 >= 0.0)
            .min_by(|left, right| {
                left.2
                     .0
                    .total_cmp(&right.2 .0)
                    .then_with(|| left.0.cmp(&right.0))
            })
            .map(
                |(id, framework, physical_cost)| CompleteSummaryCandidateEstimate {
                    physical_plan_id: id.clone(),
                    cost: Cost(lifecycle_cost + physical_cost.0),
                    window_frameworks: vec![Some(framework.clone()); deployments.len()],
                    window_accuracy_guarantee: Some(
                        planner_types::post_asap::ResultGuarantee::exact(
                            "backend exact window implementation",
                        ),
                    ),
                },
            )
    }

    fn rank_candidates(
        &self,
        intent: &AggIntent,
        candidates: &[SketchAlgorithm],
    ) -> Vec<SketchAlgorithm> {
        let defaults = match intent {
            // bind_ddsketch_quantile (priority 6) always wins the old
            // dispatcher's tie-break over bind_kll_quantile (priority 5)
            // whenever both can bind (see bind_kll_quantile.rs's
            // `priority()` doc — no eps-dependent split actually exists
            // between the two rules today, both read the same combined
            // eps). Pure static reorder: DDSketch before Kll.
            AggIntent::Quantile { .. } => {
                let mut v = candidates.to_vec();
                if let Some(pos) = v.iter().position(|k| *k == SketchAlgorithm::DDSketch) {
                    let dd = v.remove(pos);
                    v.insert(0, dd);
                }
                v
            }
            AggIntent::TopK { .. } => {
                let preferred = self.topk_family_order(&intent_accuracy(intent));
                // Distinct grouping implementations can use the same algorithm.
                // Ranking must preserve the candidate multiset, not deduplicate it.
                let mut ranked = candidates.to_vec();
                ranked.sort_by_key(|kind| {
                    preferred
                        .iter()
                        .position(|candidate| candidate == kind)
                        .unwrap_or(preferred.len())
                });
                ranked
            }
            // Cardinality → Hll, Count → Cms: control_plane only ever
            // binds one family for each; asap-plan's static order already
            // puts it first (`summary_candidates`), nothing to reorder.
            _ => candidates.to_vec(),
        };
        self.rank_with_erp_costs(intent, self.rank_with_offline_evidence(intent, defaults))
    }

    fn size_params(
        &self,
        kind: SketchAlgorithm,
        intent: &AggIntent,
        eps: f64,
        delta: f64,
    ) -> SketchParams {
        let (max_error, theoretical) = match intent {
            AggIntent::TopK { k, .. } => {
                let (eps, delta) = self.topk_eps_delta(&intent_accuracy(intent));
                let (w, d) = Self::cms_width_depth(eps, delta);
                // CountSketch/CMS columns MUST be a power of two: the
                // agent (asapedgeprocessor config_validate) rejects
                // non-pow2 cols. Round up — this only tightens the
                // additive bound (ε ≤ e/w).
                let w = w.next_power_of_two();
                let heap_size = *k as u32;
                let params = match kind {
                    SketchAlgorithm::CmsWithHeap => SketchParams::CmsWithHeap {
                        width: w,
                        depth: d,
                        heap_size,
                    },
                    _ => SketchParams::CountSketchWithHeap {
                        width: w,
                        depth: d,
                        heap_size,
                    },
                };
                (eps, params)
            }
            _ => {
                let Some((eps, delta)) = self.combined_eps_delta(&intent_accuracy(intent)) else {
                    // Either side Exact: unreachable in practice for
                    // Quantile/Cardinality/Count (an Exact intent never
                    // reaches the replacement strategies upstream — see
                    // `realizations_for_intent`'s `Exact => exact_realization`
                    // arm), kept as a safe fallback rather than a panic.
                    return asap_aware_mapping::DefaultCostModel
                        .size_params(kind, intent, eps, delta);
                };
                let params = match kind.clone() {
                    SketchAlgorithm::Kll => SketchParams::Kll {
                        k: kll_k_for_eps(eps),
                    },
                    SketchAlgorithm::DDSketch if (0.0..1.0).contains(&eps) => {
                        SketchParams::DDSketch { alpha: eps }
                    }
                    SketchAlgorithm::Hll => SketchParams::Hll {
                        precision: hll_precision_for_eps(eps),
                    },
                    SketchAlgorithm::Cms => {
                        let (w, d) = Self::cms_width_depth(eps, delta);
                        SketchParams::Cms { width: w, depth: d }
                    }
                    other => {
                        asap_aware_mapping::DefaultCostModel.size_params(other, intent, eps, delta)
                    }
                };
                (eps, params)
            }
        };
        let decision = match (
            &self.erp,
            super::super::erp::ReadoutEvidence::for_intent(&kind, intent),
        ) {
            (Some(policy), Some(readout)) => {
                Some(policy.select_readout(kind, readout, max_error, theoretical.clone()))
            }
            _ => self.erp_parameter_decision(kind, max_error, theoretical.clone()),
        };
        match decision {
            Some(ErpParameterDecision::Empirical {
                params,
                record_id,
                observed_error,
                estimated_cost,
            }) => {
                tracing::info!(erp_record_id = %record_id, observed_error, estimated_cost, "selected empirical ERP sketch parameters");
                params
            }
            Some(ErpParameterDecision::TheoreticalFallback { params, reason }) => {
                tracing::warn!(reason = %reason, "ERP miss or drift; using theoretical sizing");
                params
            }
            Some(ErpParameterDecision::ExactFallback { reason }) => {
                tracing::warn!(reason = %reason, "ERP and theoretical sizing unavailable; exact fallback required");
                theoretical
            }
            None => theoretical,
        }
    }

    /// Realize control_plane's `Frequency` (`ext_kind: "frequency"`)
    /// point-query intent (ASAPController#150). `SketchAlgorithm::Cms` (heap-
    /// less — a point lookup needs no heap, unlike `TopK`) matches
    /// `capability_matching::pick_family`'s own `Frequency → Cms` mapping
    /// (and its `is_valid_pair` truth table, which declares `(Cms,
    /// Frequency)` valid and has no `(CountSketch, Frequency)` entry) —
    /// the newer, tested, currently-authoritative source of truth for this
    /// choice, not the older `sketch_catalog::sketch_type_for_op`'s
    /// `SketchType::CountSketch` (a genuinely different sketch algorithm
    /// under a same-ish name — `SketchType` has separate `CountSketch`
    /// and `CountMinSketch` variants; that mapping predates
    /// `capability_matching` and disagrees with it). Sized the same
    /// `e/eps` width / `ln(1/delta)` depth way every other CMS-family kind
    /// here is. `PassThrough` for any other `ext_kind` (none exist yet)
    /// or an unparseable/`Exact` accuracy, matching every other
    /// approximate-capable intent's `Exact ⇒ no sketch form` policy.
    fn realize_extension(&self, ext_kind: &str, payload: &serde_json::Value) -> Realization {
        if ext_kind != FREQUENCY_EXT_KIND {
            return Realization::PassThrough;
        }
        if self.offline_frequency_comparison.is_some() {
            return self
                .offline_frequency_recommendation(payload)
                .ok()
                .and_then(|recommendation| recommendation.selected_sketch().cloned())
                .map(|configuration| {
                    Realization::Sketch(planner_types::post_asap::SketchKind::new(
                        configuration.algorithm,
                        configuration.params,
                    ))
                })
                .unwrap_or(Realization::PassThrough);
        }
        let Some(accuracy) = payload
            .get("accuracy")
            .and_then(|v| serde_json::from_value::<AccuracyTarget>(v.clone()).ok())
        else {
            return Realization::PassThrough;
        };
        let Some((eps, delta)) = self.combined_eps_delta(&accuracy) else {
            return Realization::PassThrough;
        };
        let (width, depth) = Self::cms_width_depth(eps, delta);
        Realization::Sketch(planner_types::post_asap::SketchKind::new(
            SketchAlgorithm::Cms,
            SketchParams::Cms {
                width: width.next_power_of_two(),
                depth,
            },
        ))
    }

    /// Build the `SketchQuery` readout for `Frequency`. `item_label`/
    /// `item_value` are populated by `intent_algebra::agg_intent::frequency`
    /// once the filter value is threaded through (ASAPQuery-backend Phase
    /// 3 — not yet); until then `payload` never has them, so this
    /// correctly falls back to the bare bucket total
    /// (`key: SampleValue, value: None`) — the same answer a `Frequency`
    /// intent with no item filter should give either way.
    fn readout_extension(
        &self,
        ext_kind: &str,
        payload: &serde_json::Value,
        _col: &ColumnRef,
    ) -> SketchQuery {
        debug_assert_eq!(
            ext_kind, FREQUENCY_EXT_KIND,
            "readout_extension called for an ext_kind realize_extension never realizes as Sketch"
        );
        let item_label = payload.get("item_label").and_then(|v| v.as_str());
        let item_value = payload
            .get("item_value")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        match item_label {
            Some(label) => SketchQuery::PointCount {
                key: ColumnRef::Named(label.to_string()),
                value: item_value,
            },
            None => SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None,
            },
        }
    }
}

/// A `CostModel` that forces a single family for whichever intent it's
/// asked to rank, delegating parameter sizing to an inner
/// [`ControlPlaneCostModel`]. Used by `physical::workload_planner::bind_workload_typed`,
/// which already has a definitive family pick from the capability matrix
/// (or a `sketch_type_override`) and just needs the matching binding, not
/// a fresh selection decision.
pub struct ForcedFamilyCostModel {
    inner: ControlPlaneCostModel,
    forced: SketchAlgorithm,
}

impl ForcedFamilyCostModel {
    pub fn new(workload_accuracy: AccuracyTarget, forced: SketchAlgorithm) -> Self {
        Self {
            inner: ControlPlaneCostModel::new(workload_accuracy),
            forced,
        }
    }
}

impl CostModel for ForcedFamilyCostModel {
    fn candidate_cost(
        &self,
        candidate: &ReplacementSubDAG,
        target: &TargetSubDAG<'_>,
    ) -> Option<Cost> {
        self.inner.candidate_cost(candidate, target)
    }

    fn rank_candidates(
        &self,
        intent: &AggIntent,
        candidates: &[SketchAlgorithm],
    ) -> Vec<SketchAlgorithm> {
        let mut ranked = self.inner.rank_candidates(intent, candidates);
        if let Some(pos) = ranked.iter().position(|kind| kind == &self.forced) {
            let forced = ranked.remove(pos);
            ranked.insert(0, forced);
        }
        ranked
    }

    fn size_params(
        &self,
        kind: SketchAlgorithm,
        intent: &AggIntent,
        eps: f64,
        delta: f64,
    ) -> SketchParams {
        // Latest Planner validates the selected candidate against its own
        // accuracy algebra.  Reuse its sizing formula for an explicitly
        // forced family so the override changes only algorithm preference,
        // never weakens the requested guarantee.
        asap_aware_mapping::DefaultCostModel.size_params(kind, intent, eps, delta)
    }

    // `realize_extension`/`readout_extension` delegate to `inner` rather
    // than falling back to the trait's default `PassThrough` — otherwise
    // `bind_workload_typed`'s `Frequency` contract row (which binds via
    // `ForcedFamilyCostModel`, already knowing its family pick from the
    // capability matrix) would still decline pending #150 even after
    // `ControlPlaneCostModel` itself learned to realize it.
    fn realize_extension(&self, ext_kind: &str, payload: &serde_json::Value) -> Realization {
        self.inner.realize_extension(ext_kind, payload)
    }

    fn readout_extension(
        &self,
        ext_kind: &str,
        payload: &serde_json::Value,
        col: &ColumnRef,
    ) -> SketchQuery {
        self.inner.readout_extension(ext_kind, payload, col)
    }
}

/// Map an ε rank-error budget to a KLL stream-size `k`. Verbatim port of
/// `bind_kll_quantile.rs::kll_k_for_eps` — power-of-two rungs (200, 400,
/// 800, 2048, 8192) so the in-tree `algebra::directory` continues to
/// recognise the parameter.
fn kll_k_for_eps(eps: f64) -> u32 {
    if eps <= 0.0 {
        return 8192;
    }
    if eps >= 0.01 {
        200
    } else if eps >= 0.005 {
        400
    } else if eps >= 0.0025 {
        800
    } else if eps >= 0.001 {
        2048
    } else {
        8192
    }
}

/// Map an ε standard-error budget to the HLL `precision`. Verbatim port
/// of `bind_hll_cardinality.rs::hll_precision_for_eps`.
fn hll_precision_for_eps(eps: f64) -> u8 {
    if eps <= 0.0 {
        return 16;
    }
    if eps >= 0.03 {
        10
    } else if eps >= 0.015 {
        12
    } else if eps >= 0.008 {
        14
    } else {
        16
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use planner_types::pre_asap::{default_cardinality, default_quantile};

    fn eps(e: f64) -> AccuracyTarget {
        AccuracyTarget::Epsilon(e)
    }

    /// Logical summary selection has an explicit Planner estimate before
    /// deployment pricing, including when a family is forced.
    #[test]
    fn summary_candidates_have_explicit_logical_costs() {
        use asap_aware_mapping::{ReplacementStrategy, SketchAlgorithmStrategy, TargetSubDAG};
        use std::rc::Rc;

        let accuracy = eps(0.1);
        let root = Rc::new(
            crate::query_parser::parse_query_expr_canonical(
                "quantile_over_time(0.9,m[1m])",
                accuracy.clone(),
            )
            .unwrap(),
        );
        let target = TargetSubDAG::new(&root);
        let model = ControlPlaneCostModel::new(accuracy.clone());
        let forced = ForcedFamilyCostModel::new(accuracy, SketchAlgorithm::DDSketch);
        let candidates = SketchAlgorithmStrategy::new(&model).replacements(&target);
        assert!(!candidates.is_empty());
        for candidate in &candidates {
            let estimate = model.candidate_cost_estimate(candidate).unwrap();
            assert!(estimate.value.is_finite() && estimate.value > 0.0);
            assert_eq!(estimate.source, "analytical");
            assert_eq!(estimate.unit, "bytes_per_state_partition");
            assert_eq!(
                model.candidate_cost(candidate, &target),
                Some(Cost(estimate.value))
            );
            assert_eq!(
                forced.candidate_cost(candidate, &target),
                Some(Cost(estimate.value))
            );
        }
    }

    /// Analytical estimates scale with configured state size; unsupported
    /// families remain unavailable instead of receiving a made-up zero.
    #[test]
    fn analytical_footprint_scales_with_parameters() {
        use planner_types::post_asap::SketchKind;
        let kll = |k| {
            SummaryFamilyType::Sketch(
                SketchKind::new(SketchAlgorithm::Kll, SketchParams::Kll { k }),
                GroupingStrategy::PerSubpopulationInstance,
            )
        };
        assert_eq!(analytical_state_bytes(&kll(200)), Some(6400.0));
        assert_eq!(analytical_state_bytes(&kll(400)), Some(12800.0));
        let unsupported = SummaryFamilyType::Sketch(
            SketchKind::new(SketchAlgorithm::Theta, SketchParams::Theta { k: 1024 }),
            GroupingStrategy::PerSubpopulationInstance,
        );
        assert_eq!(analytical_state_bytes(&unsupported), None);
    }

    pub(crate) fn erp_cost_fixture() -> ErpPlanningInput {
        serde_json::from_value(serde_json::json!({
            "artifact": {"schema_version": 1, "producer_version": "synthetic-test-fixture",
                "records": [{"id": "kll-200", "sketch": "kll-percall", "implementation": "lib",
                    "parameters": {"k": 200}, "distribution": {"test": "population-a"}, "trials": 20,
                    "error_metrics": {"max_rank_err": 0.9},
                    "resources": {"memory_bytes": 7777.0, "update_cpu_seconds": 1e-7,
                        "merge_cpu_seconds": 1e-5, "query_cpu_seconds": 1e-6}}]},
            "distribution": {"test": "population-a"}, "implementation": "lib",
            "error_metric": "max_rank_err", "min_trials": 10,
            "expected_updates": 1000.0, "expected_queries": 100.0, "expected_merges": 1.0,
            "retention_seconds": 60.0, "cpu_weight": 1.0, "byte_second_weight": 1e-9,
            "mode": "hybrid"
        })).unwrap()
    }

    /// ERP memory overrides analytical memory for the exact configuration,
    /// even when it is larger. Mismatched profiles fall back to analytical.
    #[test]
    fn erp_cost_precedes_analytical_without_certifying_accuracy() {
        use asap_aware_mapping::{ReplacementStrategy, SketchAlgorithmStrategy};
        use std::rc::Rc;
        let policy = erp_cost_fixture();
        let root = Rc::new(
            crate::query_parser::parse_query_expr_canonical(
                "quantile_over_time(0.9,m[1m])",
                eps(0.1),
            )
            .unwrap(),
        );
        let model = ControlPlaneCostModel::new(eps(0.1));
        let candidates =
            SketchAlgorithmStrategy::new(&model).replacements(&TargetSubDAG::new(&root));
        let candidate = candidates
            .iter()
            .find(|candidate| {
                model
                    .candidate_cost_estimate(candidate)
                    .is_some_and(|cost| cost.value == 6400.0)
            })
            .expect("KLL candidate");
        let estimate = |policy: ErpPlanningInput| {
            ControlPlaneCostModel::new(eps(0.1))
                .with_erp_costs(policy)
                .candidate_cost_estimate(candidate)
                .unwrap()
        };
        let matched = estimate(policy.clone());
        assert_eq!(matched.value, 7777.0);
        assert_eq!(matched.source, "erp");
        assert_eq!(matched.erp_record_ids, ["kll-200"]);
        let mut mismatch = policy.clone();
        mismatch.distribution = serde_json::json!({"test": "population-b"});
        assert_eq!(estimate(mismatch).source, "analytical");
        let mut mismatch = policy.clone();
        mismatch.implementation = Some("different-runtime".into());
        assert_eq!(estimate(mismatch).source, "analytical");
        let mut mismatch = policy.clone();
        mismatch.artifact.records[0].parameters = serde_json::json!({"k": 400});
        assert_eq!(estimate(mismatch).source, "analytical");
        let mut mismatch = policy.clone();
        mismatch.min_trials = 21;
        assert_eq!(estimate(mismatch).source, "analytical");
        let mut mismatch = policy.clone();
        mismatch.artifact.records[0].resources.memory_bytes = f64::NAN;
        assert_eq!(estimate(mismatch).source, "analytical");
        let model = ControlPlaneCostModel::new(eps(0.1)).with_erp_costs(policy);
        assert_eq!(
            model.rank_candidates(
                &default_quantile(0.9),
                &[SketchAlgorithm::DDSketch, SketchAlgorithm::Kll]
            )[0],
            SketchAlgorithm::Kll
        );
        let (_, trace) = crate::planner_selection::select_workload_with_accuracy_model_and_trace(
            vec![(0, root)],
            eps(0.1),
            &model,
            &asap_aware_mapping::NoAccuracyEvidence,
            &asap_aware_mapping::DefaultAccuracyModel,
        )
        .unwrap();
        assert!(trace["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["candidates"].as_array().unwrap())
            .any(|candidate| candidate["cost_estimate"]["source"] == "erp"
                && candidate["cost_estimate"]["unit"] == "bytes_per_state_partition"));
    }

    #[test]
    fn quantile_always_prefers_ddsketch_over_kll() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.1));
        let ranked = model.rank_candidates(
            &default_quantile(0.99),
            &[SketchAlgorithm::Kll, SketchAlgorithm::DDSketch],
        );
        assert_eq!(
            ranked,
            vec![SketchAlgorithm::DDSketch, SketchAlgorithm::Kll]
        );
    }

    #[test]
    fn kll_k_matches_bind_kll_quantile_rungs() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(1.0));
        let params = model.size_params(SketchAlgorithm::Kll, &default_quantile(0.99), 0.01, 0.01);
        assert_eq!(params, SketchParams::Kll { k: 200 });

        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(1.0));
        let tight = planner_types::pre_asap::AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: eps(0.001),
        };
        let params = model.size_params(SketchAlgorithm::Kll, &tight, 0.01, 0.01);
        assert_eq!(params, SketchParams::Kll { k: 2048 });
    }

    #[test]
    fn hll_precision_matches_bind_hll_cardinality_rungs() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(1.0));
        let params = model.size_params(SketchAlgorithm::Hll, &default_cardinality(), 0.01, 0.01);
        assert_eq!(params, SketchParams::Hll { precision: 14 });
    }

    #[test]
    fn topk_tight_tier_forces_countsketch_even_when_ranked_from_cms() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Exact);
        let intent = planner_types::pre_asap::AggIntent::TopK {
            k: 5,
            accuracy: eps(0.01),
        };
        let ranked = model.rank_candidates(
            &intent,
            &[
                SketchAlgorithm::CmsWithHeap,
                SketchAlgorithm::CountSketchWithHeap,
            ],
        );
        assert_eq!(
            ranked,
            vec![
                SketchAlgorithm::CountSketchWithHeap,
                SketchAlgorithm::CmsWithHeap
            ]
        );
    }

    // Workload search can enumerate multiple layouts for the same algorithm.
    #[test]
    fn topk_ranking_preserves_duplicate_candidates() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Exact);
        let intent = AggIntent::TopK {
            k: 1,
            accuracy: eps(0.01),
        };
        let mut candidates = vec![
            SketchAlgorithm::CmsWithHeap,
            SketchAlgorithm::CountSketchWithHeap,
            SketchAlgorithm::CmsWithHeap,
            SketchAlgorithm::CountSketchWithHeap,
        ];
        let mut ranked = model.rank_candidates(&intent, &candidates);
        candidates.sort();
        ranked.sort();
        assert_eq!(ranked, candidates);
    }

    #[test]
    fn topk_loose_tier_prefers_cheaper_cms_heap() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.1));
        let intent = planner_types::pre_asap::AggIntent::TopK {
            k: 5,
            accuracy: eps(0.01),
        };
        let ranked = model.rank_candidates(
            &intent,
            &[
                SketchAlgorithm::CmsWithHeap,
                SketchAlgorithm::CountSketchWithHeap,
            ],
        );
        assert_eq!(ranked[0], SketchAlgorithm::CmsWithHeap);
    }

    #[test]
    fn topk_width_is_rounded_up_to_a_power_of_two() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.1));
        let intent = planner_types::pre_asap::AggIntent::TopK {
            k: 5,
            accuracy: eps(0.01),
        };
        let params = model.size_params(SketchAlgorithm::CmsWithHeap, &intent, 0.0, 0.0);
        let SketchParams::CmsWithHeap {
            width, heap_size, ..
        } = params
        else {
            panic!("expected CmsWithHeap params");
        };
        assert!(width.is_power_of_two());
        assert_eq!(heap_size, 5);
    }
}
