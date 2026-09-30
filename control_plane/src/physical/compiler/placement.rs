//! Precompute-or-query-time placement, decided only as a summary-maintenance
//! lifecycle per unique summary state.
//!
//! Planner enumerates each state's lifecycle alternatives; this backend prices
//! them with its own unit costs and picks the cheapest for the whole workload.
//! A `ContinuouslyMaintained` state is precomputed at ingestion. An `Ephemeral`
//! state is rebuilt for each query from raw data read from Prometheus at query
//! time, so it is offered only when that raw source is bindable.
use super::*;
use asap_aware_mapping::{enumerate_summary_maintenance_lifecycles, CostModel};
use asap_physical_operators::physical_planner::{
    compile, promql_fallback, promql_rows, CompiledPhysicalDag, InputContract, PhysicalCandidate,
};
use planner_types::post_asap::PostAsapOperatorPayload;
use planner_types::pre_asap::{AggIntent, CompareOpKind, ScalarValue};

use crate::query_plan::query_time::{LabelMatch, LabelMatcher, QueryTimeOperator};

/// A query whose every state is `Ephemeral` keeps nothing between queries.
/// Planner's PromQL Fallback lowering compiles it once over raw-series inputs,
/// each read at query time by the matching range-selector `Scan`.
pub(super) struct RawQueryTimeProgram {
    pub(super) program: CompiledPhysicalDag,
    pub(super) scans: Vec<(u64, QueryTimeOperator)>,
}

#[derive(Default)]
pub(super) struct Placement {
    ephemeral: Vec<Vec<Rc<SummaryNode>>>,
    raw: Vec<Option<RawQueryTimeProgram>>,
    pub(super) trace: Vec<Value>,
}

impl Placement {
    pub(super) fn is_ephemeral(&self, query: usize, state: &Rc<SummaryNode>) -> bool {
        self.ephemeral
            .get(query)
            .is_some_and(|states| states.iter().any(|s| Rc::ptr_eq(s, state)))
    }

    pub(super) fn raw_program(&self, query: usize) -> Option<&RawQueryTimeProgram> {
        self.raw.get(query).and_then(Option::as_ref)
    }
}

/// Backend lifecycle prices for any summary state. Retention charges the
/// state's estimated bytes for every retained pane and partition at the
/// summary-store price; an unknown size under a positive price leaves
/// retention unpriced. Panes are estimated from the evaluation interval
/// because the window layout is chosen only for retained state.
struct LifecycleCosts<'a> {
    costs: &'a LifecycleUnitCosts,
    evaluation_interval_ms: u32,
    input_cardinality: Option<u64>,
    delete: bool,
}

impl CostModel for LifecycleCosts<'_> {
    fn rank_candidates(
        &self,
        _intent: &AggIntent,
        candidates: &[SketchAlgorithm],
    ) -> Vec<SketchAlgorithm> {
        candidates.to_vec()
    }

    fn summary_maintenance_lifecycle_cost_inputs(
        &self,
        summary: &SummaryNode,
    ) -> SummaryMaintenanceLifecycleCostInputs {
        let costs = self.costs;
        let panes = selected_input_contract(summary)
            .ok()
            .and_then(|(_, window, _)| window)
            .map_or(1.0, |seconds| {
                (seconds.saturating_mul(1_000) as f64
                    / f64::from(self.evaluation_interval_ms.max(1)))
                .ceil()
                .max(1.0)
            });
        let store = match &summary.expr {
            SummaryExpr::SummaryAgg {
                family, reduction, ..
            } if costs.store_per_byte_second > 0.0 => {
                // Per-series and grouped state keeps one instance per input series at most.
                let partitioned =
                    matches!(reduction, planner_types::pre_asap::Reduction::PerEntity)
                        || reduction
                            .group_keys()
                            .is_some_and(|keys| !keys.keys().is_empty());
                let partitions = if partitioned {
                    self.input_cardinality.unwrap_or(1).max(1) as f64
                } else {
                    1.0
                };
                crate::physical::post_asap::cost_model::analytical_state_bytes(family)
                    .map(|bytes| bytes * panes * partitions * costs.store_per_byte_second)
            }
            _ => Some(0.0),
        };
        SummaryMaintenanceLifecycleCostInputs {
            build_cost: Some(Cost(costs.build)),
            maintenance_cost_per_update: Some(Cost(costs.maintenance_per_update)),
            summary_read_cost: Some(Cost(costs.read)),
            retention_cost_rate: store.map(|store| CostRate(costs.retention_per_second + store)),
            retirement_cost: Some(Cost(costs.retirement)),
        }
    }

    fn summary_maintenance_capabilities(
        &self,
        _summary: &SummaryNode,
    ) -> SummaryMaintenanceCapabilities {
        SummaryMaintenanceCapabilities {
            incremental_update: true,
            merge: true,
            delete: self.delete,
        }
    }
}

/// Selectable total cost of `lifecycle` for `summary`, if Planner listed it.
fn alternative_cost(
    deployment: &asap_aware_mapping::SummaryMaintenanceDeployment,
    lifecycle: &SummaryMaintenanceLifecycle,
) -> Option<Cost> {
    deployment
        .alternatives
        .iter()
        .find(|alternative| {
            alternative.rejection.is_none()
                && &alternative.summary_maintenance_lifecycle == lifecycle
        })
        .and_then(|alternative| alternative.total_cost)
}

/// Rebuilding must be priced cheaper than retaining. An unpriced alternative
/// never displaces retention, the placement used without lifecycle evidence.
fn rebuild_is_cheaper(retained: Option<Cost>, rebuilt: Option<Cost>) -> bool {
    matches!((retained, rebuilt), (Some(retained), Some(rebuilt)) if rebuilt.0 < retained.0)
}

/// Choose one lifecycle per unique state reachable from the (already shared)
/// selected roots. Shared state is priced once with the demand of all its
/// consumers. Without complete workload evidence every state is retained.
pub(super) fn place(
    request: &PhysicalCompilationRequest,
    environment: &PhysicalDeploymentContext,
    frontend: QueryFrontend,
) -> Placement {
    let queries = &request.queries;
    let mut placement = Placement {
        ephemeral: vec![Vec::new(); queries.len()],
        raw: (0..queries.len()).map(|_| None).collect(),
        trace: Vec::new(),
    };
    let (Some(workload), Some(data), Some(first)) = (
        &request.query_workload,
        &request.data_workload,
        queries.first(),
    ) else {
        return placement;
    };
    // Query-time raw reads and exact subtrees both need the Prometheus source.
    let raw_bindable = request.allow_mixed_summary_and_exact_execution
        && !request.require_backend_local_execution
        && frontend == QueryFrontend::PromQl
        && environment.target == PhysicalDeploymentTarget::BackendLocalRemoteWrite;
    let mut states: Vec<(Rc<SummaryNode>, Vec<usize>)> = Vec::new();
    let mut query_states = vec![Vec::new(); queries.len()];
    for (index, query) in queries.iter().enumerate() {
        if super::super::maintained_population::supported_node(&query.selected_plan_root) {
            continue;
        }
        let Ok(selected) = collect_selected_materializations(&query.selected_plan_root, true)
        else {
            continue;
        };
        for state in selected {
            if query_states[index]
                .iter()
                .any(|known: &Rc<SummaryNode>| Rc::ptr_eq(known, &state.node))
            {
                continue;
            }
            query_states[index].push(Rc::clone(&state.node));
            match states
                .iter_mut()
                .find(|(known, _)| Rc::ptr_eq(known, &state.node))
            {
                Some((_, consumers)) => consumers.push(index),
                None => states.push((state.node, vec![index])),
            }
        }
    }
    let raw_programs: Vec<Option<RawQueryTimeProgram>> = (0..queries.len())
        .map(|index| {
            (raw_bindable && !query_states[index].is_empty())
                .then(|| request.canonical_roots.get(index))
                .flatten()
                .and_then(|root| raw_query_time_program(root).ok())
        })
        .collect();
    // A leaf the query-time lowering can externalize reads Prometheus directly.
    let externalizable = |state: &SummaryNode, query: usize| {
        raw_bindable
            && (crate::query_plan::query_time::selected_counter_materialization(
                &queries[query].query_string,
                state,
            )
            .ok()
            .flatten()
            .is_some()
                || crate::query_plan::query_time::selected_range_max_materialization(
                    &queries[query].query_string,
                    state,
                )
                .ok()
                .flatten()
                .is_some())
    };
    let horizon = first.summary_lifecycle_inputs.horizon_seconds;
    let mut ephemeral = vec![false; states.len()];
    let mut decisions = Vec::new();
    for (state_index, (state, consumers)) in states.iter().enumerate() {
        let bindable = consumers
            .iter()
            .all(|&query| raw_programs[query].is_some() || externalizable(state, query));
        let lead = &queries[consumers[0]].summary_lifecycle_inputs;
        let interval = consumers
            .iter()
            .map(|&query| {
                queries[query]
                    .summary_lifecycle_inputs
                    .evaluation_interval_ms
            })
            .min()
            .unwrap_or(lead.evaluation_interval_ms);
        let model = LifecycleCosts {
            costs: &lead.costs,
            evaluation_interval_ms: interval,
            input_cardinality: data
                .input_cardinality
                .value_at(environment.observed_at_unix_ms)
                .copied(),
            delete: environment.target == PhysicalDeploymentTarget::BackendLocalRemoteWrite,
        };
        let Ok(candidates) = enumerate_summary_maintenance_lifecycles(
            Rc::clone(state),
            WorkloadDemand::new_with_data(workload, data, consumers),
            environment.observed_at_unix_ms,
            Some(Horizon(horizon)),
            SummaryMaintenanceLifecycleCapabilities {
                supports_ephemeral: bindable,
                supports_prepared: false,
                supports_shared: false,
                supports_continuously_maintained: true,
            },
            &model,
        ) else {
            continue;
        };
        let Some(deployment) = candidates
            .deployments()
            .iter()
            .find(|deployment| Rc::ptr_eq(&deployment.summary, state))
        else {
            continue;
        };
        let retained = alternative_cost(
            deployment,
            &SummaryMaintenanceLifecycle::ContinuouslyMaintained,
        );
        let rebuilt = alternative_cost(deployment, &SummaryMaintenanceLifecycle::Ephemeral);
        ephemeral[state_index] = rebuild_is_cheaper(retained, rebuilt);
        decisions.push((state_index, bindable, retained, rebuilt));
    }
    // A query rebuilds either all of its states or none: raw query-time inputs
    // and exact subtrees share no snapshot with installed state. Retaining is
    // always realizable, so a query that keeps any state keeps all of them.
    let index_of = |state: &Rc<SummaryNode>| states.iter().position(|(s, _)| Rc::ptr_eq(s, state));
    loop {
        let mut changed = false;
        for (query, owned) in query_states.iter().enumerate() {
            let realizable = owned.iter().all(|state| {
                index_of(state).is_some_and(|i| ephemeral[i])
                    && (raw_programs[query].is_some() || externalizable(state, query))
            });
            if realizable {
                continue;
            }
            for state in owned {
                if let Some(i) = index_of(state).filter(|&i| ephemeral[i]) {
                    ephemeral[i] = false;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    for (state_index, bindable, retained, rebuilt) in decisions {
        let (state, consumers) = &states[state_index];
        placement.trace.push(json!({
            "stage": "deployment.lifecycle_placement",
            "query_ids": consumers.iter().map(|&q| &queries[q].query_id).collect::<Vec<_>>(),
            "logical_root_id": crate::planner_selection::explained_root_id(state, &queries[consumers[0]].accuracy_target),
            "ephemeral_bindable": bindable,
            "continuously_maintained_cost": retained.map(|cost| cost.0),
            "ephemeral_cost": rebuilt.map(|cost| cost.0),
            "selected": if ephemeral[state_index] { "ephemeral" } else { "continuously_maintained" },
        }));
    }
    for (query, owned) in query_states.iter().enumerate() {
        let chosen: Vec<_> = owned
            .iter()
            .filter(|state| index_of(state).is_some_and(|i| ephemeral[i]))
            .cloned()
            .collect();
        if !owned.is_empty() && chosen.len() == owned.len() {
            placement.raw[query] = raw_programs[query].as_ref().map(|raw| RawQueryTimeProgram {
                program: raw.program.clone(),
                scans: raw.scans.clone(),
            });
        }
        placement.ephemeral[query] = chosen;
    }
    placement
}

/// Compile the whole query over raw-series inputs and name the Prometheus
/// range selector that supplies each input.
fn raw_query_time_program(root: &QueryExpr) -> Result<RawQueryTimeProgram, String> {
    let typed = asap_physical_operators::physical_planner::promql_rows::with_series_identity(root)
        .map_err(|error| error.to_string())?;
    let keep = crate::planner_selection::keep_pre_asap(&typed).map_err(|e| e.to_string())?;
    let dag = planner_types::post_asap::compile_post_asap_dag(&keep).map_err(|e| e.to_string())?;
    let mut inputs = BTreeMap::new();
    let mut scans = Vec::new();
    for node in &dag.nodes {
        let PostAsapOperatorPayload::Fallback { expression } = &node.payload else {
            continue;
        };
        let selectors = promql_fallback::raw_series(expression).map_err(|e| e.to_string())?;
        for (ordinal, (selector, schema)) in selectors.into_iter().enumerate() {
            let slot = promql_fallback::raw_series_input(u64::from(node.id.0), ordinal);
            scans.push((slot, range_selector_scan(&selector)?));
            inputs.insert(slot, InputContract::bounded(schema));
        }
    }
    if scans.is_empty() {
        return Err("query reads no raw series".into());
    }
    let program = compile(&dag, inputs, &[u64::from(dag.root.0)]).map_err(|e| e.to_string())?;
    Ok(RawQueryTimeProgram { program, scans })
}

/// `TimeRange { range, [TimeShift { offset }], Scan }` as a Prometheus range
/// selector. `@` modifiers and non-label predicates have no such selector.
fn range_selector_scan(selector: &QueryExpr) -> Result<QueryTimeOperator, String> {
    let QueryExpr::TimeRange { range, child } = selector else {
        return Err("raw input is not a range selector".into());
    };
    let (offset_ms, scan) = match child.as_ref() {
        QueryExpr::TimeShift { shift, child } if shift.at.is_none() => {
            (shift.offset_ms, child.as_ref())
        }
        QueryExpr::TimeShift { .. } => return Err("raw selector uses @".into()),
        scan => (0, scan),
    };
    let QueryExpr::Scan {
        source: Source::TimeSeries { metric },
        predicates,
        schema,
    } = scan
    else {
        return Err("raw input does not read a named time series".into());
    };
    if metric.is_empty() {
        return Err("raw selector has no metric name".into());
    }
    let matchers = predicates
        .iter()
        .map(|predicate| {
            let QueryExpr::Compare { left, op, right } = predicate.0.as_ref() else {
                return Err("raw selector predicate is not a label comparison".to_string());
            };
            let (QueryExpr::Column(column), QueryExpr::Literal(ScalarValue::Utf8(value))) =
                (left.as_ref(), right.as_ref())
            else {
                return Err("raw selector predicate must compare a label with a string".into());
            };
            let name = schema
                .columns
                .get(*column)
                .map(|field| field.name.clone())
                .ok_or("raw selector predicate names an unknown label")?;
            let operation = match op {
                CompareOpKind::Eq => LabelMatch::Equal,
                CompareOpKind::Ne => LabelMatch::NotEqual,
                CompareOpKind::Regex => LabelMatch::Regex,
                CompareOpKind::NotRegex => LabelMatch::NotRegex,
                _ => return Err("raw selector predicate is not a PromQL matcher".into()),
            };
            Ok(LabelMatcher {
                name,
                value: value.clone(),
                operation,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(QueryTimeOperator::Scan {
        metric: Some(metric.clone()),
        matchers,
        range_ms: Some(u64::try_from(range.as_millis()).map_err(|e| e.to_string())?),
        offset_ms,
    })
}

/// Planner's fixed-window realization: a heap or grouped Sum over complete
/// per-series Rate states maintained at ingestion, per the DAG's timing.
pub(super) fn fixed_window_candidate(
    dag: &planner_types::post_asap::PostAsapDag,
) -> Result<PhysicalCandidate, asap_physical_operators::Error> {
    promql_rows::compile_fixed_window_rate_aggregation(dag)
}

/// [`fixed_window_candidate`] under the timing written into `root`.
pub(super) fn root_fixed_window_candidate(
    root: &Rc<SummaryNode>,
) -> Result<PhysicalCandidate, asap_physical_operators::Error> {
    let dag = planner_types::post_asap::compile_post_asap_dag(root)
        .map_err(|e| asap_physical_operators::Error::Invalid(e.to_string()))?;
    fixed_window_candidate(&dag)
}

/// Planner's query-time realization above a maintained population or exact
/// per-series Rate readouts; the input is bound by the backend.
pub(super) fn query_time_candidate(root: &Rc<SummaryNode>) -> Option<PhysicalCandidate> {
    promql_rows::compile_current_series_readout(root)
        .or_else(|_| promql_rows::compile_rate_ranking(root).map(|(_, dag)| dag))
        .ok()
        .map(|query| PhysicalCandidate {
            precompute: None,
            query,
            materialized_outputs: BTreeMap::new(),
        })
}

/// A summary built from another retained state's finalized readouts, such as
/// a heap or grouped Sum over per-series Rate.
fn over_readouts(summary: &SummaryNode) -> bool {
    matches!(&summary.expr, SummaryExpr::SummaryAgg { child, .. }
        if matches!(&child.expr, SummaryExpr::ValueOperation {
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            child,
            ..
        } if matches!(child.expr, SummaryExpr::SummaryAgg { .. })))
}

/// Write `timing` onto the readouts feeding `target`, rebuilding its path.
fn retime(
    node: &Rc<SummaryNode>,
    target: &Rc<SummaryNode>,
    timing: planner_types::post_asap::ExecutionTiming,
) -> Option<Rc<SummaryNode>> {
    let mut next = node.as_ref().clone();
    if Rc::ptr_eq(node, target) {
        let SummaryExpr::SummaryAgg { child, .. } = &mut next.expr else {
            return None;
        };
        let mut readout = child.as_ref().clone();
        let SummaryExpr::ValueOperation { timing: old, .. } = &mut readout.expr else {
            return None;
        };
        *old = timing;
        *child = Rc::new(readout);
        return Some(Rc::new(next));
    }
    match &mut next.expr {
        SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
            *child = retime(child, target, timing)?
        }
        SummaryExpr::SummaryEstimate { summary_input, .. } => {
            *summary_input = retime(summary_input, target, timing)?
        }
        _ => return None,
    }
    Some(Rc::new(next))
}

/// A Planner candidate with a native physical realization after its
/// readout-built states are placed by lifecycle.
pub(super) struct TimedCandidate {
    pub(super) root: Rc<SummaryNode>,
    pub(super) physical: PhysicalCandidate,
    pub(super) trace: Vec<Value>,
}

/// Place each state of `root` built from retained readouts: maintained, it
/// consumes complete per-series states at ingestion; ephemeral, it is rebuilt
/// for each query from the retained state's readouts. Planner's lifecycle plan
/// times the DAG and its native compiler reads that timing. Other states stay
/// retained here; compilation places them. `None` means no native realization.
pub(super) fn time_native_candidate(
    root: &Rc<SummaryNode>,
    query: &QueryCompilationInput,
    workload: &QueryWorkload,
    data: &DataWorkload,
    index: usize,
    environment: &PhysicalDeploymentContext,
) -> Option<TimedCandidate> {
    use planner_types::post_asap::ExecutionTiming;
    let untimed = || {
        query_time_candidate(root).map(|physical| TimedCandidate {
            root: Rc::clone(root),
            physical,
            trace: Vec::new(),
        })
    };
    let lifecycle = &query.summary_lifecycle_inputs;
    let model = LifecycleCosts {
        costs: &lifecycle.costs,
        evaluation_interval_ms: lifecycle.evaluation_interval_ms,
        input_cardinality: data
            .input_cardinality
            .value_at(environment.observed_at_unix_ms)
            .copied(),
        delete: environment.target == PhysicalDeploymentTarget::BackendLocalRemoteWrite,
    };
    let enumerate = || {
        enumerate_summary_maintenance_lifecycles(
            Rc::clone(root),
            WorkloadDemand::new_with_data(workload, data, std::slice::from_ref(&index)),
            environment.observed_at_unix_ms,
            Some(Horizon(lifecycle.horizon_seconds)),
            SummaryMaintenanceLifecycleCapabilities {
                supports_ephemeral: true,
                supports_prepared: false,
                supports_shared: false,
                supports_continuously_maintained: true,
            },
            &model,
        )
        .ok()
    };
    let candidates = enumerate()?;
    let mut choices = Vec::new();
    let mut retimed = Rc::clone(root);
    let mut trace = Vec::new();
    let mut maintained_over_readouts = false;
    for deployment in candidates.deployments() {
        let retained = alternative_cost(
            deployment,
            &SummaryMaintenanceLifecycle::ContinuouslyMaintained,
        );
        let lifecycle = if over_readouts(&deployment.summary) {
            let rebuilt = alternative_cost(deployment, &SummaryMaintenanceLifecycle::Ephemeral);
            let ephemeral = rebuild_is_cheaper(retained, rebuilt);
            maintained_over_readouts |= !ephemeral;
            let timing = if ephemeral {
                ExecutionTiming::QueryTime
            } else {
                ExecutionTiming::IngestionTime
            };
            retimed = retime(&retimed, &find(&retimed, &deployment.summary)?, timing)?;
            // Recorded per candidate forest; compilation records the final
            // placement of the states it installs as `lifecycle_placement`.
            trace.push(json!({
                "stage": "deployment.native_candidate_placement",
                "query_ids": [&query.query_id],
                "candidate_root_id": crate::planner_selection::explained_root_id(root, &query.accuracy_target),
                "logical_root_id": crate::planner_selection::explained_root_id(&deployment.summary, &query.accuracy_target),
                "ephemeral_bindable": true,
                "continuously_maintained_cost": retained.map(|cost| cost.0),
                "ephemeral_cost": rebuilt.map(|cost| cost.0),
                "selected": if ephemeral { "ephemeral" } else { "continuously_maintained" },
            }));
            if ephemeral {
                SummaryMaintenanceLifecycle::Ephemeral
            } else {
                SummaryMaintenanceLifecycle::ContinuouslyMaintained
            }
        } else {
            SummaryMaintenanceLifecycle::ContinuouslyMaintained
        };
        choices.push((deployment.post_asap_node_id, lifecycle));
    }
    if trace.is_empty() {
        return untimed();
    }
    let timed = candidates
        .select(&choices)
        .ok()?
        .execution_timed_dag()
        .ok()?;
    let physical = if maintained_over_readouts {
        fixed_window_candidate(&timed).ok()?
    } else {
        query_time_candidate(&retimed)?
    };
    Some(TimedCandidate {
        root: retimed,
        physical,
        trace,
    })
}

/// The node of `root` structurally equal to `summary`, found after earlier
/// rewrites replaced the original `Rc`s.
fn find(root: &Rc<SummaryNode>, summary: &SummaryNode) -> Option<Rc<SummaryNode>> {
    if root.as_ref() == summary {
        return Some(Rc::clone(root));
    }
    match &root.expr {
        SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
            find(child, summary)
        }
        SummaryExpr::SummaryEstimate { summary_input, .. } => find(summary_input, summary),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Only a priced, strictly cheaper rebuild moves a state to query time.
    #[test]
    fn unpriced_alternatives_never_displace_retention() {
        assert!(rebuild_is_cheaper(Some(Cost(2.0)), Some(Cost(1.0))));
        assert!(!rebuild_is_cheaper(Some(Cost(1.0)), Some(Cost(1.0))));
        assert!(!rebuild_is_cheaper(None, Some(Cost(1.0))));
        assert!(!rebuild_is_cheaper(Some(Cost(1.0)), None));
    }
}
