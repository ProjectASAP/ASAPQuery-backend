//! Generate feasible layouts from selected state requirements before pricing them.
use super::*;
use asap_types::WindowMaterializationLayout;

/// One input contract for snapshot and HTTP window planning. Quotes apply only
/// to their exact shape and workload; absent quotes use the lifecycle unit costs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WindowCostModel {
    pub implementation_id: String,
    pub cost: ImplementationCostEvidence,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quotes: Vec<WindowImplementationCandidate>,
}

pub(in crate::physical) fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

pub(super) fn is_full_cohort(candidate: &WindowImplementationCandidate) -> bool {
    candidate.slide_secs == candidate.window_secs
        && candidate.layout
            == WindowMaterializationLayout::Pane {
                pane_secs: candidate.window_secs,
            }
}

pub(super) fn cohort_nodes(states: &[SelectedMaterialization]) -> BTreeSet<usize> {
    let mut nodes = BTreeSet::new();
    for state in states {
        if let Some(sources) = immutable_materialization_sources(&state.node) {
            nodes.insert(Rc::as_ptr(&state.node) as usize);
            nodes.extend(sources.iter().map(|source| Rc::as_ptr(source) as usize));
        }
    }
    nodes
}

pub(super) fn supported(
    candidate: &WindowImplementationCandidate,
    target: PhysicalDeploymentTarget,
) -> bool {
    candidate
        .layout
        .validate(candidate.window_secs, candidate.slide_secs)
        .is_ok()
        && match (&candidate.framework, &candidate.layout) {
            (
                SummaryWindowFramework::Tumbling | SummaryWindowFramework::Sliding,
                WindowMaterializationLayout::Pane { pane_secs },
            ) => {
                target == PhysicalDeploymentTarget::BackendLocalRemoteWrite
                    || *pane_secs == candidate.window_secs
            }
            (SummaryWindowFramework::Sliding, WindowMaterializationLayout::FullWindow) => {
                // Collector schedulers currently support overlapping or tumbling windows only.
                target == PhysicalDeploymentTarget::BackendLocalRemoteWrite
                    || candidate.slide_secs <= candidate.window_secs
            }
            _ => false,
        }
}

pub(super) fn derive(
    model: &WindowCostModel,
    lifecycle: &LifecyclePlanningInput,
    window_secs: u64,
    full_cohort: bool,
    target: PhysicalDeploymentTarget,
    staleness_margin_ms: u64,
) -> Vec<WindowImplementationCandidate> {
    let evaluation_ms = u64::from(lifecycle.evaluation_interval_ms);
    // Runtime layouts have second precision. Never truncate a fractional cadence.
    if window_secs == 0 || evaluation_ms == 0 || evaluation_ms % 1_000 != 0 {
        return Vec::new();
    }
    let evaluation_secs = evaluation_ms / 1_000;
    let pane_secs = if full_cohort {
        window_secs
    } else {
        gcd(window_secs, evaluation_secs)
    };
    let slide_secs = if full_cohort {
        window_secs
    } else {
        evaluation_secs
    };
    let mut layouts = vec![WindowMaterializationLayout::Pane { pane_secs }];
    if !full_cohort && evaluation_secs != window_secs {
        layouts.push(WindowMaterializationLayout::FullWindow);
    }
    layouts
        .into_iter()
        .filter_map(|layout| {
            let suffix = match layout {
                WindowMaterializationLayout::Pane { pane_secs } => format!("pane-{pane_secs}s"),
                _ => "full-window".into(),
            };
            let candidate = WindowImplementationCandidate {
                implementation_id: format!(
                    "{}-{window_secs}s-slide-{slide_secs}s-{suffix}",
                    model.implementation_id
                ),
                framework: if slide_secs == window_secs {
                    SummaryWindowFramework::Tumbling
                } else {
                    SummaryWindowFramework::Sliding
                },
                window_secs,
                slide_secs,
                cost: derived_window_cost(
                    &model.cost,
                    lifecycle,
                    window_secs,
                    slide_secs,
                    &layout,
                    staleness_margin_ms,
                ),
                layout,
                derived: true,
                cohort_only: full_cohort && slide_secs != evaluation_secs,
            };
            supported(&candidate, target).then_some(candidate)
        })
        .collect()
}

pub fn prepare_window_implementations(
    query: &mut PlanningQuery,
    model: &WindowCostModel,
    target: PhysicalDeploymentTarget,
    staleness_margin_ms: u64,
) -> Result<(), CompileError> {
    if model.implementation_id.trim().is_empty() {
        return Err(CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "window cost model requires an implementation identity".into(),
        });
    }
    let mut model = model.clone();
    let fingerprint = canonical_promql(&query.query_string).map_err(CompileError::QueryPlan)?;
    model.cost.workload_fingerprint = fingerprint.clone();
    model.cost.horizon_seconds = query.lifecycle.horizon_seconds;
    let states = collect_selected_materializations(
        &query.post_asap,
        target == PhysicalDeploymentTarget::BackendLocalRemoteWrite,
    )
    .map_err(|reason| CompileError::Query {
        query_id: query.query_id.clone(),
        reason,
    })?;
    let cohorts = cohort_nodes(&states);
    let requirements = states
        .iter()
        .map(|state| {
            (
                state.window_secs.unwrap_or(query.window_secs),
                cohorts.contains(&(Rc::as_ptr(&state.node) as usize)),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut candidates = requirements
        .iter()
        .flat_map(|&(window, cohort)| {
            derive(
                &model,
                &query.lifecycle,
                window,
                cohort,
                target,
                staleness_margin_ms,
            )
        })
        .collect::<Vec<_>>();
    let mut quote_shapes = BTreeSet::new();
    for quote in model
        .quotes
        .iter()
        .filter(|q| q.cost.workload_fingerprint == fingerprint)
    {
        let shape = (
            quote.window_secs,
            quote.slide_secs,
            serde_json::to_string(&quote.layout).unwrap(),
        );
        if !quote_shapes.insert(shape) {
            return Err(CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: "duplicate measured window layout".into(),
            });
        }
        let applicable = requirements.iter().any(|&(window, cohort)| {
            quote.window_secs == window
                && if cohort {
                    is_full_cohort(quote)
                } else {
                    quote.slide_secs.saturating_mul(1_000)
                        == u64::from(query.lifecycle.evaluation_interval_ms)
                }
        });
        if !applicable || !supported(quote, target) {
            return Err(CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: format!(
                    "window quote `{}` does not match an executable state layout",
                    quote.implementation_id
                ),
            });
        }
        candidates.retain(|candidate| {
            !(candidate.window_secs == quote.window_secs
                && candidate.slide_secs == quote.slide_secs
                && candidate.layout == quote.layout)
        });
        let mut quote = quote.clone();
        quote.derived = false;
        quote.cohort_only = quote.slide_secs.saturating_mul(1_000)
            != u64::from(query.lifecycle.evaluation_interval_ms);
        candidates.push(quote);
    }
    let mut unique = BTreeMap::new();
    for candidate in &candidates {
        if let Some(previous) = unique.insert(&candidate.implementation_id, candidate) {
            if previous != candidate {
                return Err(CompileError::Lifecycle {
                    query_id: query.query_id.clone(),
                    reason: "window implementation identity describes different offers".into(),
                });
            }
        }
    }
    let mut ids = BTreeSet::new();
    candidates.retain(|candidate| ids.insert(candidate.implementation_id.clone()));
    query.window_implementations = candidates;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> BackendLocalPlanningSnapshot {
        serde_json::from_str(include_str!(
            "../../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap()
    }

    // Target constraints eliminate infeasible offers before lifecycle selection.
    #[test]
    fn collector_generation_excludes_partial_panes_and_sparse_full_windows() {
        let snapshot = snapshot();
        let model = snapshot.implementation.window_cost_model.clone();
        let (request, _) = snapshot.planning_request().unwrap();
        let mut lifecycle = request.queries[0].lifecycle.clone();
        for (interval, expected_count) in [
            (20_000, 1),
            (45_000, 1),
            (60_000, 1),
            (90_000, 0),
            (120_000, 1),
        ] {
            lifecycle.evaluation_interval_ms = interval;
            let candidates = derive(
                &model,
                &lifecycle,
                60,
                false,
                PhysicalDeploymentTarget::DistributedCollectors,
                0,
            );
            assert_eq!(candidates.len(), expected_count, "{interval}");
            assert!(candidates.iter().all(|candidate| supported(
                candidate,
                PhysicalDeploymentTarget::DistributedCollectors
            )));
        }
    }

    // A quote cannot authorize a pane that cuts scheduled ranges or an unimplemented rollup.
    #[test]
    fn quotes_cannot_bypass_layout_or_runtime_constraints() {
        let snapshot = snapshot();
        let mut model = snapshot.implementation.window_cost_model.clone();
        let (request, _) = snapshot.planning_request().unwrap();
        let mut query = request.queries[0].clone();
        query.lifecycle.evaluation_interval_ms = 20_000;
        prepare_window_implementations(
            &mut query,
            &model,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite,
            0,
        )
        .unwrap();
        let mut quote = query.window_implementations[0].clone();
        quote.layout = WindowMaterializationLayout::Pane { pane_secs: 30 };
        model.quotes = vec![quote.clone()];
        assert!(prepare_window_implementations(
            &mut query,
            &model,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite,
            0
        )
        .is_err());
        quote.framework =
            SummaryWindowFramework::Extension("backend.exact-hierarchical-rollup.v1".into());
        quote.layout = WindowMaterializationLayout::HierarchicalRollup {
            base_pane_secs: 10,
            levels_secs: vec![30],
        };
        model.quotes = vec![quote];
        assert!(prepare_window_implementations(
            &mut query,
            &model,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite,
            0
        )
        .is_err());
    }

    // Conflicting identities and repeated shape evidence must fail rather than silently lose an offer.
    #[test]
    fn conflicting_quote_ids_and_duplicate_shapes_are_rejected() {
        let snapshot = snapshot();
        let mut model = snapshot.implementation.window_cost_model.clone();
        let (request, _) = snapshot.planning_request().unwrap();
        let mut query = request.queries[0].clone();
        let mut quote = query.window_implementations[0].clone();
        quote.implementation_id = query.window_implementations[1].implementation_id.clone();
        model.quotes = vec![quote.clone()];
        assert!(prepare_window_implementations(
            &mut query,
            &model,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite,
            0
        )
        .is_err());
        quote.implementation_id = "duplicate-shape".into();
        model.quotes.push(quote);
        assert!(prepare_window_implementations(
            &mut query,
            &model,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite,
            0
        )
        .is_err());
    }

    // External serialization never grants permission to reinterpret measured prices.
    #[test]
    fn serialized_generated_quote_loses_compiler_provenance() {
        let (request, _) = snapshot().planning_request().unwrap();
        let candidate = &request.queries[0].window_implementations[0];
        assert!(candidate.derived);
        let value = serde_json::to_value(candidate).unwrap();
        assert!(value.get("derived").is_none());
        let decoded: WindowImplementationCandidate = serde_json::from_value(value).unwrap();
        assert!(!decoded.derived);
    }
}
