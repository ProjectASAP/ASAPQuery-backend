//! Precompute-or-query-time placement, decided only as a summary-maintenance
//! lifecycle per unique summary state.
//!
//! Planner enumerates each state's lifecycle alternatives; this backend prices
//! them with its own unit costs and picks the cheapest for the whole workload.
//! A `ContinuouslyMaintained` state is precomputed at ingestion into the window
//! layout the compiler installs for it. An `Ephemeral` state is never built:
//! its queries run an exact program over raw data read from Prometheus at query
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
/// each read at query time by the matching range-selector `Scan`. A mixed
/// query also reads its retained states, as stored native batches.
pub(super) struct RawQueryTimeProgram {
    pub(super) program: CompiledPhysicalDag,
    pub(super) scans: Vec<(u64, QueryTimeOperator)>,
    /// Retained states read beside the raw inputs. The executor binds each at
    /// its newest complete window within its lag bound (#803).
    pub(super) stored: Vec<(u64, Rc<SummaryNode>)>,
}

#[derive(Default)]
pub(super) struct Placement {
    ephemeral: Vec<Vec<Rc<SummaryNode>>>,
    raw: Vec<Option<RawQueryTimeProgram>>,
    /// Native branches a mixed query maintains and reads as stored batches.
    native: Vec<Vec<Rc<SummaryNode>>>,
    /// The precompute program building each native branch's batch.
    branch_programs: Vec<Vec<(Rc<SummaryNode>, CompiledPhysicalDag)>>,
    /// Retained states of a mixed query, each installed as a complete window.
    complete_window: Vec<Vec<Rc<SummaryNode>>>,
    /// A mixed query's root, its raw selectors typed with the series identity.
    roots: Vec<Option<Rc<SummaryNode>>>,
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

    /// Whether retained `state` serves stored input beside `query`'s raw inputs.
    pub(super) fn beside_raw(&self, query: usize, state: &Rc<SummaryNode>) -> bool {
        self.complete_window
            .get(query)
            .is_some_and(|states| states.iter().any(|s| Rc::ptr_eq(s, state)))
    }

    /// The root a mixed query compiles instead of Planner's selected root.
    pub(super) fn root(&self, query: usize) -> Option<&Rc<SummaryNode>> {
        self.roots.get(query).and_then(Option::as_ref)
    }

    pub(super) fn native_branches(&self, query: usize) -> &[Rc<SummaryNode>] {
        self.native.get(query).map_or(&[], Vec::as_slice)
    }

    /// Each native branch of `query` with the precompute program building it.
    pub(super) fn branch_programs(
        &self,
        query: usize,
    ) -> &[(Rc<SummaryNode>, CompiledPhysicalDag)] {
        self.branch_programs.get(query).map_or(&[], Vec::as_slice)
    }
}

/// Backend lifecycle prices for any summary state. Retention charges the
/// state's estimated bytes for every retained pane and partition at the
/// summary-store price; an unknown size under a positive price leaves
/// retention unpriced.
struct LifecycleCosts {
    costs: LifecycleUnitCosts,
    /// States the installed window layout retains. Without one, panes are
    /// estimated as the state's window over `evaluation_interval_ms`.
    retained_states: Option<u64>,
    evaluation_interval_ms: u32,
    input_cardinality: Option<u64>,
    delete: bool,
}

impl CostModel for LifecycleCosts {
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
        let costs = &self.costs;
        let panes = self.retained_states.map_or_else(
            || {
                selected_input_contract(summary)
                    .ok()
                    .and_then(|(_, window, _)| window)
                    .map_or(1.0, |seconds| {
                        (seconds.saturating_mul(1_000) as f64
                            / f64::from(self.evaluation_interval_ms.max(1)))
                        .ceil()
                        .max(1.0)
                    })
            },
            |states| states as f64,
        );
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

/// The window implementation compilation installs for `state` when `query`
/// retains it, possibly `beside_raw` inputs, with the number of states that
/// layout keeps in the store for the state's own window. A derived state
/// reading a longer window over it retains more, so this is a lower bound there.
fn installed_window(
    query: &QueryCompilationInput,
    state: &SelectedMaterialization,
    query_states: &[SelectedMaterialization],
    environment: &PhysicalDeploymentContext,
    retention_margin_ms: u64,
    beside_raw: bool,
) -> Option<(WindowRealizationCandidate, u64)> {
    let native_cohort = query
        .retained_physical()
        .ok()?
        .is_some_and(|candidate| candidate.precompute.is_some());
    let branch = state_query(
        query,
        state,
        &super::windows::cohort_nodes(query_states),
        native_cohort,
        beside_raw,
    );
    let model = ControlPlaneCostModel::new(branch.accuracy_target.clone())
        .with_window_implementation_costs(
            validate_window_implementations(&branch, environment).ok()?,
        );
    let (id, framework, _) = model.cheapest_window_implementation()?;
    let window = branch
        .window_realization_candidates
        .iter()
        .find(|candidate| {
            id.as_ref() == Some(&candidate.realization_id) && &candidate.framework == framework
        })?
        .clone();
    let retained = retained_state_count(
        branch.query_lookback_ms,
        retention_margin_ms,
        window.slide_secs.saturating_mul(1_000),
        &window.layout,
    );
    Some((window, retained))
}

/// A state's index, raw bindability, retained and rebuilt costs, and
/// retained store states.
type Decision = (usize, bool, Option<Cost>, Option<Cost>, Option<u64>);

/// One installed copy of a retained state and the consumers reading it.
struct Install<'a> {
    /// Window, slide, layout, evaluation cadence, and phase modulo cadence and
    /// window; `None` when no installable window or phase is known.
    layout: Option<(
        u64,
        u64,
        asap_types::WindowMaterializationLayout,
        u64,
        u64,
        u64,
    )>,
    costs: &'a LifecycleUnitCosts,
    retained_states: Option<u64>,
    evaluation_interval_ms: u32,
    consumers: Vec<usize>,
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
        native: vec![Vec::new(); queries.len()],
        branch_programs: (0..queries.len()).map(|_| Vec::new()).collect(),
        complete_window: vec![Vec::new(); queries.len()],
        roots: vec![None; queries.len()],
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
    let mut selected_states: Vec<Vec<SelectedMaterialization>> =
        (0..queries.len()).map(|_| Vec::new()).collect();
    for (index, query) in queries.iter().enumerate() {
        if super::super::maintained_population::supported_node(&query.selected_plan_root) {
            continue;
        }
        let Ok(selected) = collect_selected_materializations(&query.selected_plan_root, true)
        else {
            continue;
        };
        for state in &selected {
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
                None => states.push((Rc::clone(&state.node), vec![index])),
            }
        }
        selected_states[index] = selected;
    }
    let raw_programs: Vec<Option<RawQueryTimeProgram>> = (0..queries.len())
        .map(|index| {
            (raw_bindable && !query_states[index].is_empty())
                .then(|| request.canonical_roots.get(index))
                .flatten()
                .and_then(|root| raw_query_time_program(root).ok())
        })
        .collect();
    let horizon = Some(Horizon(first.summary_lifecycle_inputs.horizon_seconds));
    let now = environment.observed_at_unix_ms;
    let update_rate = data.ingestion_rate.value_at(now).map(|rate| rate.0);
    let model =
        |costs: LifecycleUnitCosts, retained_states, evaluation_interval_ms| LifecycleCosts {
            costs,
            retained_states,
            evaluation_interval_ms,
            input_cardinality: data.input_cardinality.value_at(now).copied(),
            delete: environment.target == PhysicalDeploymentTarget::BackendLocalRemoteWrite,
        };
    // Planner's alternative `lifecycle` for `state`, priced for `consumers`.
    let price = |state: &Rc<SummaryNode>,
                 consumers: &[usize],
                 lifecycle: SummaryMaintenanceLifecycle,
                 model: &LifecycleCosts| {
        let candidates = enumerate_summary_maintenance_lifecycles(
            Rc::clone(state),
            WorkloadDemand::new_with_data(workload, data, consumers),
            now,
            horizon,
            SummaryMaintenanceLifecycleCapabilities {
                supports_ephemeral: lifecycle == SummaryMaintenanceLifecycle::Ephemeral,
                supports_prepared: false,
                supports_shared: false,
                supports_continuously_maintained: lifecycle
                    == SummaryMaintenanceLifecycle::ContinuouslyMaintained,
            },
            model,
        )
        .ok()?;
        let deployment = candidates
            .deployments()
            .iter()
            .find(|deployment| Rc::ptr_eq(&deployment.summary, state))?;
        alternative_cost(deployment, &lifecycle)
    };
    let phases: Vec<Option<u64>> = workload
        .entries()
        .map(|entry| match entry.recurrence {
            QueryRecurrence::Repeated(RepeatedDemand::FixedIntervalAt {
                evaluation_phase, ..
            }) => Some(evaluation_phase.0),
            _ => None,
        })
        .collect();
    let mut decisions: Vec<Decision> = Vec::new();
    for (state_index, (state, consumers)) in states.iter().enumerate() {
        let bindable = consumers.iter().all(|&query| raw_programs[query].is_some());
        // Consumers share one installed state only when compilation groups
        // them: the same window layout and cadence, and the same evaluation
        // phase within both cadence and window. Any other consumer installs its own.
        let mut installs: Vec<Install> = Vec::new();
        for &query in consumers {
            let lifecycle = &queries[query].summary_lifecycle_inputs;
            let installed = selected_states[query]
                .iter()
                .find(|selected| Rc::ptr_eq(&selected.node, state))
                .and_then(|selected| {
                    installed_window(
                        &queries[query],
                        selected,
                        &selected_states[query],
                        environment,
                        request.query_retention_margin_ms,
                        false,
                    )
                });
            let layout = installed
                .as_ref()
                .zip(phases[query])
                .map(|((window, _), phase)| {
                    let cadence_ms = u64::from(lifecycle.evaluation_interval_ms).max(1);
                    let window_ms = window.window_secs.saturating_mul(1_000).max(1);
                    (
                        window.window_secs,
                        window.slide_secs,
                        window.layout.clone(),
                        cadence_ms,
                        phase % cadence_ms,
                        phase % window_ms,
                    )
                });
            match installs
                .iter_mut()
                .find(|install| layout.is_some() && install.layout == layout)
            {
                Some(install) => install.consumers.push(query),
                None => installs.push(Install {
                    layout,
                    costs: &lifecycle.costs,
                    retained_states: installed.map(|(_, retained)| retained),
                    evaluation_interval_ms: lifecycle.evaluation_interval_ms,
                    consumers: vec![query],
                }),
            }
        }
        let retained = installs
            .iter()
            .map(|install| {
                price(
                    state,
                    &install.consumers,
                    SummaryMaintenanceLifecycle::ContinuouslyMaintained,
                    &model(
                        install.costs.clone(),
                        install.retained_states,
                        install.evaluation_interval_ms,
                    ),
                )
                .map(|cost| cost.0)
            })
            .sum::<Option<f64>>()
            .map(Cost);
        // Each consumer runs its raw program once per evaluation. Per state the
        // program builds, finalizes and retires a transient accumulator, as
        // Planner's per-read rebuild charges; it also folds every sample its
        // Scans cover, at the per-update cost maintenance pays for the same
        // source rate. The query's states share that fold.
        let rebuilt = if bindable {
            consumers
                .iter()
                .map(|&query| {
                    let raw = raw_programs[query].as_ref()?;
                    let scanned_seconds = raw
                        .scans
                        .iter()
                        .map(|(_, scan)| match scan {
                            QueryTimeOperator::Scan {
                                range_ms: Some(range_ms),
                                ..
                            } => Some(*range_ms as f64 / 1_000.0),
                            _ => None,
                        })
                        .sum::<Option<f64>>()?;
                    let lifecycle = &queries[query].summary_lifecycle_inputs;
                    let fold =
                        update_rate? * scanned_seconds * lifecycle.costs.maintenance_per_update;
                    let costs = LifecycleUnitCosts {
                        build: lifecycle.costs.build + fold / query_states[query].len() as f64,
                        ..lifecycle.costs.clone()
                    };
                    price(
                        state,
                        std::slice::from_ref(&query),
                        SummaryMaintenanceLifecycle::Ephemeral,
                        &model(costs, None, lifecycle.evaluation_interval_ms),
                    )
                    .map(|cost| cost.0)
                })
                .sum::<Option<f64>>()
                .map(Cost)
        } else {
            None
        };
        let retained_states = installs
            .iter()
            .map(|install| install.retained_states)
            .sum::<Option<u64>>();
        decisions.push((state_index, bindable, retained, rebuilt, retained_states));
    }
    // A query rebuilds either all of its states or none: raw query-time inputs
    // and exact subtrees share no snapshot with installed state. States linked
    // through a query therefore move together, and only when the raw programs
    // of all their queries cost less than retaining all of them.
    let index_of = |state: &Rc<SummaryNode>| states.iter().position(|(s, _)| Rc::ptr_eq(s, state));
    let mut linked: Vec<usize> = (0..states.len()).collect();
    for owned in &query_states {
        let members: Vec<usize> = owned.iter().filter_map(index_of).collect();
        if let Some(&first) = members.first() {
            for member in members {
                let (from, to) = (linked[member], linked[first]);
                linked
                    .iter_mut()
                    .filter(|l| **l == from)
                    .for_each(|l| *l = to);
            }
        }
    }
    let total = |group: usize, cost: fn(&Decision) -> Option<Cost>| {
        decisions
            .iter()
            .filter(|decision| linked[decision.0] == group)
            .map(|decision| cost(decision).map(|cost| cost.0))
            .sum::<Option<f64>>()
            .map(Cost)
    };
    let mut ephemeral: Vec<bool> = (0..states.len())
        .map(|state| {
            rebuild_is_cheaper(
                total(linked[state], |decision| decision.2),
                total(linked[state], |decision| decision.3),
            )
        })
        .collect();
    // A query that alone consumes its states may instead retain some and
    // rebuild the others: raw inputs are read at t_q and each stored native
    // batch at its newest complete window within the lag bound (#803). A
    // retained unit is a native branch, read as its batch, with its sources;
    // every other state is rebuilt. Each option is priced as it would run.
    let mut mixed: Vec<Option<MixedPlacement>> = (0..queries.len()).map(|_| None).collect();
    for (query, owned) in query_states.iter().enumerate() {
        let members: Vec<usize> = owned.iter().filter_map(index_of).collect();
        if !raw_bindable
            || members.len() < 2
            || members.iter().any(|&member| states[member].1 != [query])
        {
            continue;
        }
        let root = &queries[query].selected_plan_root;
        let lifecycle = &queries[query].summary_lifecycle_inputs;
        // Rebuilt beside retained state, a program folds only the samples of
        // the rebuilt states' own selectors.
        let alone = |member: usize| {
            let (
                _,
                QueryTimeOperator::Scan {
                    range_ms: Some(range_ms),
                    ..
                },
            ) = leaf_selector(&states[member].0)?
            else {
                return None;
            };
            let costs = LifecycleUnitCosts {
                build: lifecycle.costs.build
                    + update_rate? * range_ms as f64 / 1_000.0
                        * lifecycle.costs.maintenance_per_update,
                ..lifecycle.costs.clone()
            };
            price(
                &states[member].0,
                &[query],
                SummaryMaintenanceLifecycle::Ephemeral,
                &model(costs, None, lifecycle.evaluation_interval_ms),
            )
        };
        let stored = |branch: &Rc<SummaryNode>, sources: &[usize]| {
            let installed =
                collect_selected_materializations_with(root, true, std::slice::from_ref(branch))
                    .ok()?;
            let unit: Vec<_> = std::iter::once(Rc::clone(branch))
                .chain(sources.iter().map(|&member| Rc::clone(&states[member].0)))
                .collect();
            unit.iter()
                .map(|state| {
                    let selected = installed
                        .iter()
                        .find(|selected| Rc::ptr_eq(&selected.node, state))?;
                    let (window, retained) = installed_window(
                        &queries[query],
                        selected,
                        &installed,
                        environment,
                        request.query_retention_margin_ms,
                        true,
                    )?;
                    // The executor reads the batch as one window of the query lookback.
                    if Rc::ptr_eq(state, branch)
                        && window.window_secs.saturating_mul(1_000)
                            != queries[query].query_lookback_ms
                    {
                        return None;
                    }
                    price(
                        state,
                        &[query],
                        SummaryMaintenanceLifecycle::ContinuouslyMaintained,
                        &model(
                            lifecycle.costs.clone(),
                            Some(retained),
                            lifecycle.evaluation_interval_ms,
                        ),
                    )
                    .map(|cost| cost.0)
                })
                .sum::<Option<f64>>()
                .map(Cost)
        };
        // Each unit: an optional native branch and the member states it
        // retains or rebuilds; a state outside every branch is rebuilt.
        let mut units: Vec<(Option<Rc<SummaryNode>>, Vec<usize>)> = batch_branches(root)
            .into_iter()
            .filter_map(|branch| {
                let sources = immutable_materialization_sources(&branch)?
                    .iter()
                    .map(|source| {
                        members
                            .iter()
                            .copied()
                            .find(|&member| Rc::ptr_eq(&states[member].0, source))
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((Some(branch), sources))
            })
            .collect();
        for &member in &members {
            if !units.iter().any(|(_, unit)| unit.contains(&member)) {
                units.push((None, vec![member]));
            }
        }
        let options: Vec<MixedOption> = units
            .iter()
            .map(|(branch, unit)| MixedOption {
                beside_raw: branch.as_ref().and_then(|branch| stored(branch, unit)),
                alone: unit
                    .iter()
                    .map(|&member| alone(member).map(|cost| cost.0))
                    .sum::<Option<f64>>()
                    .map(Cost),
            })
            .collect();
        let group = if ephemeral[members[0]] {
            total(linked[members[0]], |decision| decision.3)
        } else {
            total(linked[members[0]], |decision| decision.2)
        };
        let target = &queries[query].accuracy_target;
        let mut event = json!({
            "stage": "deployment.mixed_placement",
            "query_ids": [&queries[query].query_id],
            "group_cost": group.map(|cost| cost.0),
            "units": units.iter().zip(&options).map(|((branch, unit), option)| json!({
                "native_branch_id": branch.as_ref().map(|branch| crate::planner_selection::explained_root_id(branch, target)),
                "logical_root_ids": unit.iter().map(|&member| crate::planner_selection::explained_root_id(&states[member].0, target)).collect::<Vec<_>>(),
                "retained_beside_raw_cost": option.beside_raw.map(|cost| cost.0),
                "rebuilt_cost": option.alone.map(|cost| cost.0),
            })).collect::<Vec<_>>(),
            "selected": "group",
        });
        match cheapest_mix(&options) {
            Err(reason) => event["reason"] = reason.into(),
            Ok(Some((rebuild, cost))) if rebuild_is_cheaper(group, Some(cost)) => {
                event["mixed_cost"] = cost.0.into();
                let mut rebuilt = Vec::new();
                let mut branches = Vec::new();
                for ((branch, unit), &rebuild) in units.iter().zip(&rebuild) {
                    match branch {
                        Some(branch) if !rebuild => branches.push(Rc::clone(branch)),
                        _ => {
                            rebuilt.extend(unit.iter().map(|&member| Rc::clone(&states[member].0)))
                        }
                    }
                }
                match mixed_placement(root, &rebuilt, &branches) {
                    Ok(placed) => {
                        event["selected"] = "mixed".into();
                        for ((_, unit), &rebuild) in units.iter().zip(&rebuild) {
                            for &member in unit {
                                ephemeral[member] = rebuild;
                            }
                        }
                        mixed[query] = Some(placed);
                    }
                    Err(reason) => event["reason"] = reason.into(),
                }
            }
            Ok(_) => {}
        }
        placement.trace.push(event);
    }
    for (state_index, bindable, retained, rebuilt, retained_states) in decisions {
        let (state, consumers) = &states[state_index];
        placement.trace.push(json!({
            "stage": "deployment.lifecycle_placement",
            "query_ids": consumers.iter().map(|&q| &queries[q].query_id).collect::<Vec<_>>(),
            "logical_root_id": crate::planner_selection::explained_root_id(state, &queries[consumers[0]].accuracy_target),
            "ephemeral_bindable": bindable,
            "retained_states": retained_states,
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
                stored: Vec::new(),
            });
        }
        placement.ephemeral[query] = chosen;
        if let Some(mixed) = mixed[query].take() {
            placement.roots[query] = Some(mixed.root);
            placement.ephemeral[query] = mixed.rebuilt;
            placement.native[query] = mixed.branches.iter().map(|(b, _)| Rc::clone(b)).collect();
            placement.branch_programs[query] = mixed.branches;
            placement.complete_window[query] = mixed.retained;
            placement.raw[query] = Some(mixed.program);
        }
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
    Ok(RawQueryTimeProgram {
        program,
        scans,
        stored: Vec::new(),
    })
}

/// A mixed query's plan: its identity-typed root, rebuilt states, native
/// branches read as batches with the precompute program building each, every
/// retained state, and the query-time program.
struct MixedPlacement {
    root: Rc<SummaryNode>,
    rebuilt: Vec<Rc<SummaryNode>>,
    branches: Vec<(Rc<SummaryNode>, CompiledPhysicalDag)>,
    retained: Vec<Rc<SummaryNode>>,
    program: RawQueryTimeProgram,
}

/// A unit's prices in a mixed plan: retained, with its batch read beside raw
/// inputs, and rebuilt from its own raw selectors. `None` is inadmissible or
/// unpriced.
struct MixedOption {
    beside_raw: Option<Cost>,
    alone: Option<Cost>,
}

/// Per-state placement enumerates all 2^n assignments of a query's n units.
const MAX_MIXED_UNITS: usize = 8;

/// The cheapest assignment that retains at least one unit and rebuilds at
/// least one, as the units to rebuild, or `None` when no such assignment is
/// admissible and priced.
fn cheapest_mix(options: &[MixedOption]) -> Result<Option<(Vec<bool>, Cost)>, String> {
    if options.len() > MAX_MIXED_UNITS {
        return Err(format!(
            "per-state placement enumerates at most {MAX_MIXED_UNITS} units of a query, not {}; the group decision is kept",
            options.len()
        ));
    }
    let every = (1u32 << options.len()) - 1;
    let mut best: Option<(u32, f64)> = None;
    for rebuilt in 1..every {
        let cost = options
            .iter()
            .enumerate()
            .map(|(index, option)| {
                if rebuilt >> index & 1 == 1 {
                    option.alone
                } else {
                    option.beside_raw
                }
                .map(|cost| cost.0)
            })
            .sum::<Option<f64>>();
        if let Some(cost) = cost.filter(|cost| best.is_none_or(|(_, known)| *cost < known)) {
            best = Some((rebuilt, cost));
        }
    }
    Ok(best.map(|(rebuilt, cost)| {
        (
            (0..options.len())
                .map(|index| rebuilt >> index & 1 == 1)
                .collect(),
            Cost(cost),
        )
    }))
}

/// Branches of `root` the executor can read as stored native batches beside
/// raw inputs (#803): an ungrouped Sum or sketch the precompute program builds
/// from retained readouts. A state over raw samples is stored per population.
fn batch_branches(root: &Rc<SummaryNode>) -> Vec<Rc<SummaryNode>> {
    fn walk(node: &Rc<SummaryNode>, branches: &mut Vec<Rc<SummaryNode>>) {
        let ungrouped = matches!(&node.expr, SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::Sketch(..)
                | SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _),
            reduction: planner_types::pre_asap::Reduction::Reduce(keys),
            ..
        } if keys.keys().is_empty() && !keys.is_without());
        if ungrouped && over_readouts(node) && immutable_materialization_sources(node).is_some() {
            branches.push(Rc::clone(node));
            return;
        }
        match &node.expr {
            SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
                walk(child, branches)
            }
            SummaryExpr::SummaryEstimate { summary_input, .. } => walk(summary_input, branches),
            SummaryExpr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, branches);
                walk(rhs, branches);
            }
            _ => {}
        }
    }
    let mut branches = Vec::new();
    walk(root, &mut branches);
    branches
}

/// The raw range selector a leaf state summarizes, and its query-time `Scan`.
fn leaf_selector(state: &SummaryNode) -> Option<(Rc<QueryExpr>, QueryTimeOperator)> {
    let SummaryExpr::SummaryAgg { child, .. } = &state.expr else {
        return None;
    };
    let SummaryExpr::KeepPreAsap(selector) = &child.expr else {
        return None;
    };
    let scan = range_selector_scan(selector).ok()?;
    Some((Rc::clone(selector), scan))
}

/// A leaf state of an identity-typed root: its original and retyped nodes,
/// its raw leaf, and the `Scan` that reads that leaf at query time.
struct TypedState {
    original: Rc<SummaryNode>,
    state: Rc<SummaryNode>,
    leaf: Rc<SummaryNode>,
    scan: QueryTimeOperator,
}

/// `root` with each of `states`' raw selectors typed with the complete series
/// identity: raw rows read at query time carry it, and Planner's native
/// realization of a branch needs it. A per-series node keeps its input's
/// columns, so it gains the identity too; every other node is unchanged.
fn identity_typed(
    root: &Rc<SummaryNode>,
    states: &[Rc<SummaryNode>],
) -> Result<(Rc<SummaryNode>, Vec<TypedState>), String> {
    use asap_physical_operators::physical_planner::promql_rows::SERIES_IDENTITY_COLUMN;
    fn names(schema: &planner_types::post_asap::SummarySchema) -> Vec<&str> {
        schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect()
    }
    // A node that passed its old input's columns through passes the identity on.
    fn follow(
        next: &mut SummaryNode,
        old: &planner_types::post_asap::SummarySchema,
        new: &planner_types::post_asap::SummarySchema,
    ) {
        if names(&next.schema) == names(old) {
            if let Some(identity) = new
                .fields
                .iter()
                .find(|field| field.name == SERIES_IDENTITY_COLUMN)
            {
                next.schema.fields.push(identity.clone());
            }
        }
    }
    fn retype(
        node: &Rc<SummaryNode>,
        states: &[Rc<SummaryNode>],
        typed: &mut Vec<TypedState>,
    ) -> Result<Rc<SummaryNode>, String> {
        let mut next = node.as_ref().clone();
        if states.iter().any(|state| Rc::ptr_eq(state, node)) {
            let (selector, scan) = leaf_selector(node).ok_or("state has no raw selector")?;
            let selector =
                asap_physical_operators::physical_planner::promql_rows::with_series_identity(
                    &selector,
                )
                .map_err(|e| e.to_string())?;
            let SummaryExpr::SummaryAgg { child, .. } = &mut next.expr else {
                unreachable!("leaf_selector matched a SummaryAgg")
            };
            let mut leaf = crate::planner_selection::keep_pre_asap(&selector)
                .map_err(|e| e.to_string())?
                .as_ref()
                .clone();
            leaf.guarantee = child.guarantee.clone();
            let (old, leaf) = (Rc::clone(child), Rc::new(leaf));
            *child = Rc::clone(&leaf);
            follow(&mut next, &old.schema, &leaf.schema);
            let state = Rc::new(next);
            typed.push(TypedState {
                original: Rc::clone(node),
                state: Rc::clone(&state),
                leaf,
                scan,
            });
            return Ok(state);
        }
        let unchanged = match &mut next.expr {
            SummaryExpr::KeepPreAsap(_) => true,
            SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
                let old = Rc::clone(child);
                *child = retype(child, states, typed)?;
                let unchanged = Rc::ptr_eq(&old, child);
                let new = Rc::clone(child);
                follow(&mut next, &old.schema, &new.schema);
                unchanged
            }
            SummaryExpr::SummaryEstimate { summary_input, .. } => {
                let old = Rc::clone(summary_input);
                *summary_input = retype(summary_input, states, typed)?;
                Rc::ptr_eq(&old, summary_input)
            }
            SummaryExpr::BinaryOp { lhs, rhs, .. } => {
                let (left, right) = (Rc::clone(lhs), Rc::clone(rhs));
                *lhs = retype(lhs, states, typed)?;
                *rhs = retype(rhs, states, typed)?;
                Rc::ptr_eq(&left, lhs) && Rc::ptr_eq(&right, rhs)
            }
            _ => return Err("mixed placement supports no such operator".into()),
        };
        Ok(if unchanged {
            Rc::clone(node)
        } else {
            Rc::new(next)
        })
    }
    let mut typed = Vec::new();
    let root = retype(root, states, &mut typed)?;
    if typed.len() != states.len() {
        return Err("a state is not reachable as a leaf of the root".into());
    }
    Ok((root, typed))
}

/// Type `root` for a mixed plan that rebuilds `rebuilt` from raw rows and
/// reads each of `branches` as its stored native batch. Raw rows and Planner's
/// native precompute of a branch both carry the complete series identity.
fn mixed_placement(
    root: &Rc<SummaryNode>,
    rebuilt: &[Rc<SummaryNode>],
    branches: &[Rc<SummaryNode>],
) -> Result<MixedPlacement, String> {
    let mut leaves = rebuilt.to_vec();
    for branch in branches {
        leaves.extend(immutable_materialization_sources(branch).ok_or("branch has no sources")?);
    }
    let (root, typed) = identity_typed(root, &leaves)?;
    let (rebuilt, sources): (Vec<_>, Vec<_>) = typed
        .into_iter()
        .partition(|state| rebuilt.iter().any(|s| Rc::ptr_eq(s, &state.original)));
    // A branch is the Sum or sketch over readouts of its typed sources.
    let typed_branches: Vec<_> = batch_branches(&root)
        .into_iter()
        .filter(|branch| {
            immutable_materialization_sources(branch).is_some_and(|inputs| {
                !inputs.is_empty()
                    && inputs
                        .iter()
                        .all(|input| sources.iter().any(|s| Rc::ptr_eq(&s.state, input)))
            })
        })
        .collect();
    if typed_branches.len() != branches.len() {
        return Err("a stored branch lost its sources while typing".into());
    }
    let compiled = planner_types::post_asap::compile_post_asap_dag_with_node_ids(&root)
        .map_err(|e| e.to_string())?;
    let id = |node: &Rc<SummaryNode>| {
        compiled
            .node_ids
            .node_id(node)
            .map(|id| u64::from(id.0))
            .ok_or("mixed input is absent from the compiled DAG")
    };
    // As in Planner's fixed-window realization, precompute builds the batch
    // from the complete states of its sources.
    let precompute = typed_branches
        .iter()
        .map(|branch| {
            let inputs = immutable_materialization_sources(branch)
                .expect("filtered above")
                .iter()
                .map(|source| {
                    let schema = std::sync::Arc::new(source.schema.clone());
                    Ok((id(source)?, InputContract::bounded(schema)))
                })
                .collect::<Result<BTreeMap<_, _>, String>>()?;
            let program = compile(&compiled.dag, inputs, &[id(branch)?])
                .map_err(|e| format!("stored branch precompute: {e}"))?;
            Ok((Rc::clone(branch), program))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let program = mixed_program(&compiled, &rebuilt, &typed_branches)?;
    let mut retained = typed_branches;
    retained.extend(sources.into_iter().map(|state| state.state));
    Ok(MixedPlacement {
        root,
        rebuilt: rebuilt.into_iter().map(|state| state.state).collect(),
        branches: precompute,
        retained,
        program,
    })
}

/// Compile the typed DAG with each `stored` batch as a stored input and each
/// `rebuilt` state built from its leaf's raw rows at query time.
fn mixed_program(
    compiled: &planner_types::post_asap::PostAsapDagCompilation,
    rebuilt: &[TypedState],
    stored: &[Rc<SummaryNode>],
) -> Result<RawQueryTimeProgram, String> {
    let id = |node: &Rc<SummaryNode>| {
        compiled
            .node_ids
            .node_id(node)
            .map(|id| u64::from(id.0))
            .ok_or("mixed input is absent from the compiled DAG")
    };
    let mut inputs = BTreeMap::new();
    let mut scans = Vec::new();
    for state in rebuilt {
        let slot = id(&state.leaf)?;
        let schema = std::sync::Arc::new(state.leaf.schema.clone());
        inputs.insert(slot, InputContract::bounded(schema));
        scans.push((slot, state.scan.clone()));
    }
    let stored = stored
        .iter()
        .map(|batch| {
            let slot = id(batch)?;
            let schema = std::sync::Arc::new(batch.schema.clone());
            inputs.insert(slot, InputContract::bounded(schema));
            Ok((slot, Rc::clone(batch)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let program = compile(&compiled.dag, inputs, &[u64::from(compiled.dag.root.0)])
        .map_err(|e| e.to_string())?;
    Ok(RawQueryTimeProgram {
        program,
        scans,
        stored,
    })
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
    // Windows are prepared from the timed root, so the installed layout is not
    // known while its timing is being chosen.
    let model = LifecycleCosts {
        costs: lifecycle.costs.clone(),
        retained_states: None,
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

    fn option(beside_raw: Option<f64>, alone: Option<f64>) -> MixedOption {
        MixedOption {
            beside_raw: beside_raw.map(Cost),
            alone: alone.map(Cost),
        }
    }

    // The cheapest mix retains and rebuilds at least one unit each, and a unit
    // with no stored batch is always rebuilt.
    #[test]
    fn cheapest_mix_is_the_cheapest_admissible_assignment() {
        let (rebuilt, cost) =
            cheapest_mix(&[option(None, Some(5.0)), option(Some(1.0), Some(9.0))])
                .unwrap()
                .unwrap();
        assert_eq!((rebuilt, cost.0), (vec![true, false], 6.0));
        let (rebuilt, _) = cheapest_mix(&[
            option(Some(4.0), Some(1.0)),
            option(Some(1.0), Some(4.0)),
            option(Some(1.0), Some(4.0)),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(rebuilt, [true, false, false]);
        assert!(
            cheapest_mix(&[option(None, Some(1.0)), option(None, Some(1.0))])
                .unwrap()
                .is_none()
        );
        assert!(
            cheapest_mix(&[option(Some(1.0), None), option(Some(1.0), Some(1.0))])
                .unwrap()
                .is_some_and(|(rebuilt, _)| rebuilt == [false, true])
        );
    }

    // Beyond the enumeration cap the search reports why it kept the group decision.
    #[test]
    fn cheapest_mix_rejects_more_units_than_its_cap() {
        let options: Vec<_> = (0..=MAX_MIXED_UNITS)
            .map(|_| option(Some(1.0), Some(1.0)))
            .collect();
        let error = cheapest_mix(&options).unwrap_err();
        assert!(error.contains("at most 8 units"), "{error}");
    }

    // Only a priced, strictly cheaper rebuild moves a state to query time.
    #[test]
    fn unpriced_alternatives_never_displace_retention() {
        assert!(rebuild_is_cheaper(Some(Cost(2.0)), Some(Cost(1.0))));
        assert!(!rebuild_is_cheaper(Some(Cost(1.0)), Some(Cost(1.0))));
        assert!(!rebuild_is_cheaper(None, Some(Cost(1.0))));
        assert!(!rebuild_is_cheaper(Some(Cost(1.0)), None));
    }
}
