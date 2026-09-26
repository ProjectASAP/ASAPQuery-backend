//! Complete a backend-local planning snapshot with deterministic unit-cost
//! quotes for the PromQL compliance harness. Candidate identities and demand
//! still come from the production planner and physical compiler.

use control_plane::physical::{
    compiler::{
        BackendLocalPlanningInput, DeploymentPlanCompiler, BACKEND_REVISION, PLANNER_REVISION,
    },
    workload_cost::{
        enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence, WorkloadQuote,
    },
};
use std::{collections::BTreeMap, path::Path};

fn quote_snapshot(
    mut snapshot: BackendLocalPlanningInput,
) -> Result<BackendLocalPlanningInput, String> {
    if snapshot.workload_cost_evidence.is_some() {
        return Err("planning snapshot already contains workload_cost_evidence".into());
    }
    let observed_at_unix_ms = snapshot.environment.observed_at_unix_ms;
    let valid_for_ms = snapshot.environment.max_evidence_age_ms;
    let (request, environment) = snapshot
        .clone()
        .into_physical_compilation_request()
        .map_err(|error| error.to_string())?;
    let quotes = enumerate_exact_and_materialized_candidates(request)
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter_map(|candidate| {
            let plan = DeploymentPlanCompiler
                .compile_promql(candidate.clone(), environment.clone())
                .ok()?;
            // This execution suite must exercise maintained candidates when legal.
            let unit_cost = if plan.precompute_plan.materializations.is_empty() {
                1e12
            } else {
                1.0
            };
            let manifest = manifest(&plan, &candidate.queries).ok()?;
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), unit_cost))
                    .collect::<BTreeMap<_, _>>(),
                manifest,
                executable: true,
            })
        })
        .collect();
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "promql-compliance".into(),
        model_version: "promql-compliance-unit-costs".into(),
        observed_at_unix_ms,
        valid_for_ms,
        quotes,
    });
    Ok(snapshot)
}

fn main() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
    let input = args
        .next()
        .ok_or("usage: control_plane_quote_snapshot INPUT OUTPUT")?;
    let output = args
        .next()
        .ok_or("usage: control_plane_quote_snapshot INPUT OUTPUT")?;
    if args.next().is_some() {
        return Err("usage: control_plane_quote_snapshot INPUT OUTPUT".into());
    }
    let snapshot = std::fs::read(&input)
        .map_err(|error| format!("read {}: {error}", Path::new(&input).display()))?;
    let snapshot =
        serde_json::from_slice(&snapshot).map_err(|error| format!("decode snapshot: {error}"))?;
    let quoted = quote_snapshot(snapshot)?;
    std::fs::write(
        &output,
        serde_json::to_vec_pretty(&quoted).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("write {}: {error}", Path::new(&output).display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_complete_candidate_quotes_to_a_snapshot() {
        let fixture =
            include_str!("../../../docs/examples/asapquery-compatibility-demo-snapshot.json");
        let mut snapshot: BackendLocalPlanningInput = serde_json::from_str(fixture).unwrap();
        snapshot.workload_cost_evidence = None;
        let quoted = quote_snapshot(snapshot).unwrap();
        assert!(!quoted
            .workload_cost_evidence
            .as_ref()
            .unwrap()
            .quotes
            .is_empty());
        let plan = quoted.compile_promql().unwrap();
        assert!(
            !plan.precompute_plan.materializations.is_empty(),
            "deterministic compliance quotes should select a maintained candidate"
        );
    }
}
