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
    compile, promql_fallback, CompiledPhysicalDag, InputContract,
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
/// state's estimated bytes for every retained pane at the summary-store price;
/// an unknown size under a positive price leaves retention unpriced.
struct LifecycleCosts<'a> {
    costs: &'a LifecycleUnitCosts,
    evaluation_interval_ms: u32,
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
            SummaryExpr::SummaryAgg { family, .. } if costs.store_per_byte_second > 0.0 => {
                crate::physical::post_asap::cost_model::analytical_state_bytes(family)
                    .map(|bytes| bytes * panes * costs.store_per_byte_second)
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

/// Unknown cost never makes a lifecycle win.
fn rebuild_is_cheaper(retained: Option<Cost>, rebuilt: Option<Cost>) -> bool {
    match (retained, rebuilt) {
        (Some(retained), Some(rebuilt)) => rebuilt.0 < retained.0,
        (None, Some(_)) => true,
        _ => false,
    }
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
        metric: (!metric.is_empty()).then(|| metric.clone()),
        matchers,
        range_ms: Some(u64::try_from(range.as_millis()).map_err(|e| e.to_string())?),
        offset_ms,
    })
}
