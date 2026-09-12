//! Synthetic complete quotes for deployment fixtures; never used in production.
use control_plane::physical::compiler::{
    BackendLocalPlanningSnapshot, PhysicalCompiler, BACKEND_REVISION, PLANNER_REVISION,
};

pub(crate) fn quoted_snapshot(
    mut snapshot: BackendLocalPlanningSnapshot,
    metricsql: bool,
) -> BackendLocalPlanningSnapshot {
    use control_plane::physical::workload_cost::{
        manifest, with_exact_alternative, WorkloadCostEvidence, WorkloadQuote,
    };
    let (request, environment) = snapshot.clone().planning_request().unwrap();
    let quotes = with_exact_alternative(request)
        .unwrap()
        .into_iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            let plan = if metricsql {
                PhysicalCompiler.compile_metricsql(candidate.clone(), environment.clone())
            } else {
                PhysicalCompiler.compile(candidate.clone(), environment.clone())
            }
            .ok()?;
            let manifest = manifest(&plan, &candidate.queries).unwrap();
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), if index == 0 { 1.0 } else { 1e12 }))
                    .collect(),
                manifest,
                executable: true,
            })
        })
        .collect();
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "data-plane-unit-fixture".into(),
        model_version: "test-only-unit-costs".into(),
        observed_at_unix_ms: environment.observed_at_unix_ms,
        valid_for_ms: environment.max_evidence_age_ms,
        quotes,
    });
    snapshot
}
