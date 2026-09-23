//! Versioned reference resource model. These coefficients are analytical
//! assumptions, not measurements or currency prices. ERP replaces only the
//! resource dimensions it measures; plan demand always comes from the backend.
use super::*;
use crate::query_plan::{residual::ResidualQueryOperator as Op, QueryPlanNode as Node};
use asap_aware_mapping::erp::ErpResourceProfile;
use asap_types::{
    AggregationType as A, PrecomputeMaterialization, WindowMaterializationLayout as Layout,
};
use planner_types::post_asap::SketchAlgorithm;

pub const MODEL_VERSION: &str = "backend-workload-resources-v1";
const CPU_PER_ITEM: f64 = 1e-7;
const CPU_PER_BYTE: f64 = 1e-9;
const SAMPLE_BYTES: f64 = 24.0;
const SERIES_BYTES: f64 = 256.0;
const MEMORY_WEIGHT: f64 = 1e-9;
const NETWORK_WEIGHT: f64 = 1e-8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComponentResources {
    pub cpu_seconds: f64,
    pub memory_byte_seconds: f64,
    pub network_bytes: f64,
    pub source: String,
    pub erp_record_ids: Vec<String>,
    pub calculation: Value,
}
impl ComponentResources {
    pub(super) fn weighted_cost(&self) -> f64 {
        self.cpu_seconds
            + self.memory_byte_seconds * MEMORY_WEIGHT
            + self.network_bytes * NETWORK_WEIGHT
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AutomaticCostReport {
    pub model_version: String,
    pub weights: Value,
    pub inputs: Value,
    pub assumptions: Vec<String>,
    pub components: BTreeMap<String, ComponentResources>,
}
#[derive(Clone)]
struct State {
    profile: ErpResourceProfile,
    ids: Vec<String>,
    partitions: f64,
    retained: f64,
    allocations_per_second: f64,
    overlap: f64,
    rollup_merges: f64,
}
fn resources(
    cpu: f64,
    memory: f64,
    network: f64,
    ids: Vec<String>,
    calculation: Value,
) -> ComponentResources {
    ComponentResources {
        cpu_seconds: cpu,
        memory_byte_seconds: memory,
        network_bytes: network,
        source: if ids.is_empty() {
            "analytical"
        } else {
            "erp+analytical"
        }
        .into(),
        erp_record_ids: ids,
        calculation,
    }
}
fn state(
    m: &PrecomputeMaterialization,
    request: &PhysicalCompilationRequest,
    cardinality: u64,
) -> Result<State, CompileError> {
    let bytes =
        super::super::compiler::estimated_state_bytes(&m.aggregation_type, &m.parameters) as f64;
    let mut profile = ErpResourceProfile {
        memory_bytes: bytes,
        update_cpu_seconds: CPU_PER_ITEM * (bytes / 16.0).max(2.0).log2(),
        merge_cpu_seconds: CPU_PER_BYTE * bytes,
        query_cpu_seconds: CPU_PER_BYTE * bytes + CPU_PER_ITEM,
    };
    let mut ids = Vec::new();
    let algorithm = match m.aggregation_type {
        A::DatasketchesKLL => Some(SketchAlgorithm::Kll),
        A::HLL => Some(SketchAlgorithm::Hll),
        A::UnivMon => Some(SketchAlgorithm::UnivMon),
        _ => None,
    };
    if let (Some(policy), Some(algorithm)) = (&request.erp, algorithm) {
        let mut policy = policy.clone();
        // Use the same deployed implementation and population checks as the
        // logical cost path. Unknown implementations fall back to this model.
        policy.artifact.records.retain(|row| {
            (row.sketch == "kll-percall" && row.implementation == "lib")
                || (row.sketch == "hll" && row.implementation == "asap-sketchlib-hll-regular-v1")
                || (row.sketch == "univmon"
                    && row.implementation == "asap-sketchlib-univmon-standard-v1")
        });
        let population_matches = policy.observed_populations.is_none()
            || (!request.canonical_roots.is_empty()
                && request.canonical_roots.iter().all(|root| {
                    super::super::compiler::observed_population_matches_root(&policy, root)
                }));
        if population_matches {
            if let Some(params) = super::super::erp::parse_params(&algorithm, &json!(m.parameters))
            {
                if let Some((measured, records)) = policy.candidate_resources(&algorithm, &params) {
                    profile = measured;
                    ids = records;
                }
            }
        }
    }
    let (period, overlap, rollup_merges) = match &m.window_layout {
        Layout::Pane { pane_secs } => (*pane_secs as f64, 1.0, 0.0),
        Layout::FullWindow => (
            m.slide_interval as f64,
            (m.window_size as f64 / m.slide_interval as f64).ceil(),
            0.0,
        ),
        Layout::HierarchicalRollup {
            base_pane_secs,
            levels_secs,
        } => {
            // Every closed child state is folded into its parent once.
            let periods = std::iter::once(base_pane_secs)
                .chain(levels_secs.iter().take(levels_secs.len().saturating_sub(1)));
            (
                *base_pane_secs as f64,
                1.0,
                periods.map(|p| 1.0 / *p as f64).sum(),
            )
        }
    };
    let retained = m
        .num_aggregates_to_retain
        .ok_or_else(|| invalid("state retention count is unknown"))? as f64;
    if period <= 0.0 || retained <= 0.0 {
        return Err(invalid("invalid state layout"));
    }
    Ok(State {
        profile,
        ids,
        partitions: super::super::compiler::retained_partition_count(m, Some(cardinality)) as f64,
        retained,
        allocations_per_second: 1.0 / period
            + match &m.window_layout {
                Layout::HierarchicalRollup { levels_secs, .. } => {
                    levels_secs.iter().map(|p| 1.0 / *p as f64).sum::<f64>()
                }
                _ => 0.0,
            },
        overlap,
        rollup_merges,
    })
}

pub(super) fn estimate(
    request: &PhysicalCompilationRequest,
    env: &PhysicalDeploymentContext,
    plan: &CompiledPhysicalPlan,
    manifest: &WorkloadCostManifest,
) -> Result<AutomaticCostReport, CompileError> {
    let data = request
        .data_workload
        .as_ref()
        .ok_or_else(|| invalid("automatic costing needs data_workload"))?;
    let now = env.observed_at_unix_ms;
    let horizon = manifest.horizon_seconds;
    let rate = data
        .ingestion_rate
        .value_at(now)
        .map(|r| r.0)
        .ok_or_else(|| invalid("automatic costing needs a fresh ingestion rate"))?;
    let cadence = data
        .data_ingestion_interval
        .value_at(now)
        .map(|d| d.0 as f64 / 1000.0)
        .ok_or_else(|| invalid("automatic costing needs fresh source cadence"))?;
    if !rate.is_finite() || rate < 0.0 || !cadence.is_finite() || cadence <= 0.0 {
        return Err(invalid("invalid source rate/cadence"));
    }
    let mut assumptions = vec![
        "Reference analytical coefficients are estimates, not calibrated benchmarks or currency prices.".into(),
        "Absent per-source/selectivity facts, each source and collector uses the whole workload rate and cardinality (conservative replication).".into(),
        "Grouped states use input cardinality as an upper bound on group count; filtered inputs receive no selectivity discount.".into(),
        "Transport uses full state size even for deltas; memory footprint approximates encoded size; result rows include a 256-byte label allowance.".into(),
    ];
    let cardinality = match data.input_cardinality.value_at(now) {
        Some(value) => *value,
        None => {
            if rate == 0.0 {
                return Err(invalid(
                    "zero arrival rate cannot establish active cardinality",
                ));
            }
            assumptions.push("Unknown cardinality estimated as ceil(ingestion_rate * source_cadence), assuming one sample per active series per scrape.".into());
            let n = (rate * cadence).ceil();
            if !n.is_finite() || n >= u64::MAX as f64 {
                return Err(invalid("cardinality estimate overflow"));
            }
            n as u64
        }
    };
    if cardinality == 0 && rate > 0.0 {
        return Err(invalid("positive ingestion rate with zero cardinality"));
    }
    let cardinality = cardinality as f64;
    let mut states = BTreeMap::new();
    for schema in &plan.precompute_plan.schemas {
        let m = plan
            .precompute_plan
            .materializations
            .iter()
            .find(|m| m.policy_fingerprint() == schema.materialization.fingerprint())
            .ok_or_else(|| invalid("missing materialization"))?;
        states.insert(
            schema.materialization.0,
            state(m, request, cardinality as u64)?,
        );
    }
    let mut components = BTreeMap::new();
    let mut insert = |id: String, value: ComponentResources| -> Result<(), CompileError> {
        if !manifest.components.contains_key(&id) {
            return Err(invalid(format!("unmanifested cost {id}")));
        }
        if [
            value.cpu_seconds,
            value.memory_byte_seconds,
            value.network_bytes,
            value.weighted_cost(),
        ]
        .iter()
        .any(|v| !v.is_finite() || *v < 0.0)
        {
            return Err(invalid(format!("non-finite/negative resource cost {id}")));
        }
        if components.insert(id.clone(), value).is_some() {
            return Err(invalid(format!("duplicate cost {id}")));
        }
        Ok(())
    };
    let updates = rate * horizon;
    let max_lookback = plan
        .query_plan
        .entries
        .values()
        .map(|e| e.instant.lookback_ms as f64 / 1000.0)
        .fold(cadence, f64::max);
    let needs_volume = matches!(data.arrival, planner_types::workload::DataArrival::AtRest)
        || plan
            .query_plan
            .entries
            .values()
            .any(|e| e.instant.full_history);
    let source_samples =
        if needs_volume {
            *data.ingestion_volume.value_at(now).ok_or_else(|| {
                invalid("at-rest/full-history costing needs fresh ingestion_volume")
            })? as f64
                + updates
        } else {
            rate * max_lookback
        };
    for (id, demand) in &manifest.components {
        if id.starts_with("source:") {
            let location = demand.implementation["location"].as_str().unwrap_or("");
            let remote_backend = location == "backend" && !plan.collector_plans.is_empty();
            let items = if remote_backend { 0.0 } else { updates };
            let raw_retained = if location == "exact_backend" {
                source_samples * SAMPLE_BYTES + cardinality * SERIES_BYTES
            } else {
                0.0
            };
            insert(
                id.clone(),
                resources(
                    items * CPU_PER_ITEM,
                    raw_retained * horizon,
                    items * SAMPLE_BYTES,
                    vec![],
                    json!({"input_samples": items, "retained_raw_bytes": raw_retained, "cpu_seconds_per_sample": CPU_PER_ITEM,
                    "backend_summary_decode_charged_in_transport": remote_backend}),
                ),
            )?;
        } else if id.starts_with("state:") {
            let binding = &demand.implementation["binding"];
            let materialization: asap_types::PolicyFingerprint =
                serde_json::from_value(binding["schema"]["materialization"].clone())
                    .map_err(|e| invalid(e.to_string()))?;
            let s = states
                .get(&materialization)
                .ok_or_else(|| invalid("unknown state"))?;
            let operation = demand.implementation["operation"].as_str().unwrap_or("");
            let builds = s.partitions * (s.retained + (horizon * s.allocations_per_second).ceil());
            let remote_backend =
                binding["location"] == "backend" && !plan.collector_plans.is_empty();
            let update_count = if remote_backend {
                0.0
            } else {
                updates * s.overlap
            };
            let merges = s.partitions * horizon * s.rollup_merges;
            let (cpu, memory) = match operation {
                "build" => (builds * s.profile.memory_bytes * CPU_PER_BYTE, 0.0),
                "update" => (
                    update_count * s.profile.update_cpu_seconds
                        + merges * s.profile.merge_cpu_seconds,
                    0.0,
                ),
                "residency" => (
                    0.0,
                    s.partitions * s.retained * s.profile.memory_bytes * horizon,
                ),
                "retire" => (builds * CPU_PER_ITEM, 0.0),
                _ => return Err(invalid("unsupported state phase")),
            };
            insert(
                id.clone(),
                resources(
                    cpu,
                    memory,
                    0.0,
                    s.ids.clone(),
                    json!({"operation":operation,
                "partitions":s.partitions, "retained_states_per_partition":s.retained, "builds_and_retirements":builds,
                "updates":update_count,"rollup_merges":merges,"unit_resources":s.profile,
                "allocation_cpu_seconds_per_byte":CPU_PER_BYTE,"retirement_cpu_seconds_per_state":CPU_PER_ITEM}),
                ),
            )?;
        } else if id.starts_with("current-series:") {
            let phase = demand.implementation["phase"].as_str().unwrap_or("");
            let bytes = cardinality * SERIES_BYTES;
            let (cpu, memory) = match phase {
                "build" => (bytes * CPU_PER_BYTE, 0.0),
                "update" => (updates * CPU_PER_ITEM * cardinality.max(2.0).log2(), 0.0),
                "residency" => (0.0, bytes * horizon),
                "retire" => (cardinality * CPU_PER_ITEM, 0.0),
                _ => return Err(invalid("unsupported current-series phase")),
            };
            insert(
                id.clone(),
                resources(
                    cpu,
                    memory,
                    0.0,
                    vec![],
                    json!({"phase":phase,"series":cardinality,"updates":updates,"bytes_per_series":SERIES_BYTES}),
                ),
            )?;
        }
    }
    for rule in &plan.transmission_plan.rules {
        if rule.emit_every_ms == 0 {
            return Err(invalid("unknown transmission cadence"));
        }
        let s = states
            .get(&rule.materialization.0)
            .ok_or_else(|| invalid("unknown transport state"))?;
        let checkpoints = rule
            .full_checkpoint_every_ms
            .map(|ms| {
                if ms == 0 {
                    f64::INFINITY
                } else {
                    horizon * 1000.0 / ms as f64
                }
            })
            .unwrap_or(0.0);
        let frames = (horizon * 1000.0 / rule.emit_every_ms as f64 + checkpoints).ceil()
            * s.partitions
            * s.retained;
        let bytes = frames * s.profile.memory_bytes;
        insert(
            format!("transport:{}:{}", rule.producer_id, rule.materialization.0),
            resources(
                bytes * CPU_PER_BYTE * 2.0 + frames * s.profile.merge_cpu_seconds,
                0.0,
                bytes,
                s.ids.clone(),
                json!({"frames":frames,"bytes_per_frame":s.profile.memory_bytes,"merge_cpu_seconds_per_frame":s.profile.merge_cpu_seconds,
                "encode_decode_cpu_seconds_per_byte":2.0*CPU_PER_BYTE}),
            ),
        )?;
    }
    for entry in plan.query_plan.entries.values() {
        let mut rows = BTreeMap::new();
        let mut profiles: BTreeMap<_, Vec<&State>> = BTreeMap::new();
        for node_id in entry
            .topological_order()
            .map_err(|e| invalid(e.to_string()))?
        {
            let node = &entry.nodes[&node_id];
            let id = format!("query:{}:{}", entry.query_id, node_id.0);
            let evaluations = manifest.components[&id].occurrences_per_horizon;
            let input_rows: f64 = node.inputs().iter().map(|id| rows[id]).sum();
            let mut output_rows = input_rows.max(1.0);
            let mut used: Vec<&State> = node
                .inputs()
                .iter()
                .flat_map(|id| profiles.get(id).into_iter().flatten().copied())
                .collect();
            let mut detail = json!({"input_rows":input_rows});
            let mut network = 0.0;
            let cpu = match node {
                Node::ReadMaterialization { binding } => {
                    let s = states
                        .get(&binding.materialization.0)
                        .ok_or_else(|| invalid("unknown read state"))?;
                    let panes = if binding.full_window_slide_ms.is_some() {
                        1.0
                    } else {
                        (binding
                            .readout_lookback_ms
                            .unwrap_or(entry.instant.lookback_ms) as f64
                            / binding.window_ms as f64)
                            .ceil()
                            .max(1.0)
                    };
                    output_rows = s.partitions;
                    used = vec![s];
                    detail = json!({"partitions":s.partitions,"panes_per_read":panes,"unit_resources":s.profile});
                    s.partitions
                        * (panes * s.profile.memory_bytes * CPU_PER_BYTE
                            + (panes - 1.0) * s.profile.merge_cpu_seconds)
                }
                Node::SummaryEstimate { .. } => used
                    .iter()
                    .map(|s| s.partitions * s.profile.query_cpu_seconds)
                    .sum(),
                Node::SummaryMerge { .. } => used
                    .iter()
                    .map(|s| s.partitions * s.profile.merge_cpu_seconds)
                    .sum(),
                Node::Scalar { .. } => {
                    output_rows = 1.0;
                    CPU_PER_ITEM
                }
                Node::ExactFallback { .. }
                | Node::Logical {
                    operator: Op::ExactSubquery { .. } | Op::CandidateExactSubquery { .. },
                    ..
                } => {
                    let query = match node {
                        Node::Logical {
                            operator:
                                Op::ExactSubquery { query } | Op::CandidateExactSubquery { query, .. },
                            ..
                        } => query.as_str(),
                        _ => entry.canonical_query.as_str(),
                    };
                    let parsed = crate::query_parser::parse_query_expr_canonical(
                        query,
                        crate::types::AccuracyTarget::Exact,
                    )
                    .map_err(|e| invalid(e.to_string()))?;
                    let sources = exact_source_metrics(&parsed)?.len() as f64;
                    let samples = if needs_volume {
                        *data.ingestion_volume.value_at(now).ok_or_else(|| {
                            invalid("full-history exact cost needs fresh ingestion_volume")
                        })? as f64
                    } else {
                        (rate * (entry.instant.lookback_ms as f64 / 1000.0).max(cadence))
                            .max(cardinality)
                    } * sources;
                    let operators = tree_size(
                        &serde_json::to_value(parsed).map_err(|e| invalid(e.to_string()))?,
                    ) as f64;
                    output_rows = cardinality.max(1.0);
                    network = output_rows * SERIES_BYTES + query.len() as f64;
                    detail = json!({"scanned_samples":samples,"syntax_objects":operators,"cpu_seconds_per_item":CPU_PER_ITEM,
                        "formula":"(samples + 1) * log2(max(samples, 2)) * syntax_objects * cpu_seconds_per_item"});
                    (samples + 1.0) * samples.max(2.0).log2() * operators * CPU_PER_ITEM
                }
                Node::Logical {
                    operator: Op::CurrentSeries { .. },
                    ..
                } => {
                    output_rows = cardinality;
                    cardinality * CPU_PER_ITEM
                }
                Node::RelationalJoin { .. } => {
                    output_rows = input_rows.powi(2);
                    output_rows * CPU_PER_ITEM
                }
                Node::ExternalExact { .. }
                | Node::Logical {
                    operator: Op::Scan { .. } | Op::Subquery { .. },
                    ..
                } => {
                    return Err(invalid("no automatic model for external exact/generic scan/nested subquery operator"));
                }
                Node::ReduceSum { grouping, .. } => {
                    if matches!(grouping, crate::query_plan::PhysicalGrouping::Reduce(keys) if keys.is_empty())
                    {
                        output_rows = 1.0;
                    }
                    input_rows * CPU_PER_ITEM
                }
                Node::ExactReadout { .. }
                | Node::Binary { .. }
                | Node::Relational { .. }
                | Node::MembershipFilter { .. }
                | Node::Logical { .. } => {
                    input_rows.max(1.0) * input_rows.max(2.0).log2() * CPU_PER_ITEM
                }
            };
            let ids: BTreeSet<_> = if matches!(
                node,
                Node::ReadMaterialization { .. }
                    | Node::SummaryEstimate { .. }
                    | Node::SummaryMerge { .. }
            ) {
                used.iter().flat_map(|s| s.ids.iter().cloned()).collect()
            } else {
                BTreeSet::new()
            };
            detail["evaluations"] = json!(evaluations);
            detail["output_rows_bound"] = json!(output_rows);
            insert(
                id,
                resources(
                    cpu * evaluations,
                    0.0,
                    network * evaluations,
                    ids.into_iter().collect(),
                    detail,
                ),
            )?;
            rows.insert(node_id, output_rows);
            profiles.insert(node_id, used);
        }
        let id = format!("result:{}", entry.query_id);
        let evaluations = manifest.components[&id].occurrences_per_horizon;
        let bytes = rows[&entry.root] * SERIES_BYTES * evaluations;
        insert(
            id,
            resources(
                bytes * CPU_PER_BYTE,
                0.0,
                bytes,
                vec![],
                json!({"rows_per_evaluation":rows[&entry.root],"evaluations":evaluations,"bytes_per_row":SERIES_BYTES}),
            ),
        )?;
    }
    if components.len() != manifest.components.len() {
        return Err(invalid(
            "automatic cost model did not cover every manifest component",
        ));
    }
    if !components
        .values()
        .map(ComponentResources::weighted_cost)
        .sum::<f64>()
        .is_finite()
    {
        return Err(invalid("total workload cost overflow"));
    }
    Ok(AutomaticCostReport {
        model_version: MODEL_VERSION.into(),
        weights: json!({"unit":"weighted_resource_seconds","cpu_seconds":1.0,"memory_byte_seconds":MEMORY_WEIGHT,"network_bytes":NETWORK_WEIGHT}),
        inputs: json!({"data_workload":data,"horizon_seconds":horizon,"ingestion_rate":rate,"source_cadence_seconds":cadence,
            "input_cardinality":cardinality,"capability_snapshot_id":env.capability_snapshot_id,"observed_at_unix_ms":now}),
        assumptions,
        components,
    })
}
fn tree_size(value: &Value) -> usize {
    match value {
        Value::Object(fields) => 1 + fields.values().map(tree_size).sum::<usize>(),
        Value::Array(items) => items.iter().map(tree_size).sum(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// A concrete implementation uses all ERP resources even when dearer;
    /// a profile for a different implementation cannot replace its estimate.
    #[test]
    fn physical_state_resources_prefer_applicable_erp() {
        let input: super::super::super::compiler::BackendLocalPlanningInput = serde_json::from_str(
            include_str!("../../../../docs/examples/asapquery-planning-snapshot.json"),
        )
        .unwrap();
        let (mut request, env) = input.into_physical_compilation_request().unwrap();
        let plan = super::super::super::compiler::PhysicalPlanCompiler
            .compile_promql(request.clone(), env)
            .unwrap();
        let mut m = plan.precompute_plan.materializations[0].clone();
        m.aggregation_type = A::DatasketchesKLL;
        m.parameters = std::collections::HashMap::from([("k".into(), json!(200))]);
        let baseline = state(&m, &request, 100).unwrap();
        let mut erp = crate::physical::post_asap::cost_model::tests::erp_cost_fixture();
        erp.artifact.records[0].resources = ErpResourceProfile {
            memory_bytes: 1e8,
            update_cpu_seconds: 0.1,
            merge_cpu_seconds: 0.2,
            query_cpu_seconds: 0.3,
        };
        let expected = erp.artifact.records[0].resources.clone();
        request.erp = Some(erp);
        let measured = state(&m, &request, 100).unwrap();
        assert!(!measured.ids.is_empty());
        assert_eq!(measured.profile, expected);
        assert!(measured.profile.memory_bytes > baseline.profile.memory_bytes);
        request.erp.as_mut().unwrap().artifact.records[0].implementation = "other-runtime".into();
        let fallback = state(&m, &request, 100).unwrap();
        assert!(fallback.ids.is_empty());
        assert_eq!(fallback.profile, baseline.profile);
    }
}
