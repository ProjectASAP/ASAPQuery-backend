//! Synthetic prices test Backend ranking, not ERP calibration or runtime speed.
#[path = "support/issue754_workload.rs"]
mod workload;

use control_plane::physical::{
    compiler::{CompileError, QueryFrontend, BACKEND_REVISION, PLANNER_REVISION},
    workload_cost::{
        compile_candidates_for_pricing, enumerate_exact_and_materialized_candidates,
        select_lowest_cost_candidate, CandidateEvaluationStatus, WorkloadCostEvidence,
        WorkloadQuote,
    },
};

/// Every admissible workload candidate can win solely by changing complete quotes.
/// Missing/infeasible cheapest quotes must not become a zero-cost winner.
#[test]
fn workload_candidates_follow_prices_and_feasibility() {
    let mut workloads: Vec<_> = workload::suite()
        .queries
        .into_iter()
        .map(|case| (case.name.clone(), workload::input(&case)))
        .collect();
    workloads.extend(
        workload::ensembles()
            .into_iter()
            .map(|(name, cases)| (name, workload::ensemble_input(&cases))),
    );
    for (name, input) in workloads {
        let (request, env) = input.into_physical_compilation_request().unwrap();
        let candidates = enumerate_exact_and_materialized_candidates(request).unwrap();
        let (mut manifests, admission) =
            compile_candidates_for_pricing(candidates.clone(), env.clone(), QueryFrontend::PromQl);
        // The search is expected to reach one manifest through several candidates;
        // equivalent manifests need exactly one quote.
        let mut seen = std::collections::BTreeSet::new();
        manifests.retain(|m| seen.insert(serde_json::to_string(m).unwrap()));
        assert!(manifests.len() >= 2, "{} needs competing candidates", name);
        assert!(admission.iter().all(|c| c.total_cost.is_none()));
        let mut winners = std::collections::BTreeSet::new();
        for preferred in 0..manifests.len() {
            let evidence = WorkloadCostEvidence {
                backend_revision: BACKEND_REVISION.into(),
                planner_revision: PLANNER_REVISION.into(),
                data_snapshot_id: format!("synthetic-ranking-{}", name),
                model_version: "synthetic-complete-quotes-v1".into(),
                observed_at_unix_ms: env.observed_at_unix_ms,
                valid_for_ms: env.max_evidence_age_ms,
                quotes: manifests
                    .iter()
                    .enumerate()
                    .map(|(index, manifest)| WorkloadQuote {
                        manifest: manifest.clone(),
                        executable: true,
                        unit_costs: manifest
                            .components
                            .keys()
                            .map(|key| (key.clone(), if index == preferred { 1.0 } else { 1e12 }))
                            .collect(),
                    })
                    .collect(),
            };
            // The preference must follow from construction, not fixture multiplicities.
            let totals: Vec<f64> = evidence
                .quotes
                .iter()
                .map(|quote| {
                    quote
                        .manifest
                        .components
                        .iter()
                        .map(|(key, demand)| quote.unit_costs[key] * demand.occurrences_per_horizon)
                        .sum()
                })
                .collect();
            for (index, total) in totals.iter().enumerate() {
                assert!(
                    index == preferred || *total > totals[preferred],
                    "{name}: candidate {index} is not costlier than preferred {preferred}"
                );
            }
            if preferred == 0 {
                // A single foreign quote, even a losing one, invalidates the generation.
                let mut wrong_dataset = evidence.clone();
                let foreign = &mut wrong_dataset.quotes.last_mut().unwrap().manifest;
                foreign.dataset_identity.namespace = "another-tenant".into();
                let error =
                    select_lowest_cost_candidate(candidates.clone(), env.clone(), &wrong_dataset)
                        .unwrap_err();
                assert!(
                    matches!(error, CompileError::CostEvidenceDataset { .. }),
                    "{name}: quotes for another dataset must not price this workload: {error}"
                );
            }
            let plan = select_lowest_cost_candidate(candidates.clone(), env.clone(), &evidence)
                .unwrap_or_else(|e| panic!("{}: {e}", name));
            let report = plan.cost_comparison.unwrap();
            assert_eq!(report.selected_manifest, manifests[preferred], "{}", name);
            assert_eq!(report.model_version, "synthetic-complete-quotes-v1");
            let minimum = report
                .candidate_evaluations
                .iter()
                .filter_map(|c| c.total_cost)
                .fold(f64::INFINITY, f64::min);
            assert_eq!(report.component_costs.values().sum::<f64>(), minimum);
            assert_eq!(
                report
                    .candidate_evaluations
                    .iter()
                    .filter(|c| c.status == CandidateEvaluationStatus::Selected)
                    .count(),
                1
            );
            winners.insert(serde_json::to_string(&report.selected_manifest).unwrap());
            // Inventory order cannot alter a unique cheapest choice.
            let mut reversed = candidates.clone();
            reversed.reverse();
            assert_eq!(
                select_lowest_cost_candidate(reversed, env.clone(), &evidence)
                    .unwrap()
                    .cost_comparison
                    .unwrap()
                    .selected_manifest,
                manifests[preferred]
            );
            for missing in [false, true] {
                let mut unavailable = evidence.clone();
                let expected = if missing {
                    unavailable.quotes.remove(preferred);
                    CandidateEvaluationStatus::EvidenceMissing
                } else {
                    unavailable.quotes[preferred].executable = false;
                    CandidateEvaluationStatus::ProviderRejected
                };
                let report =
                    select_lowest_cost_candidate(candidates.clone(), env.clone(), &unavailable)
                        .unwrap()
                        .cost_comparison
                        .unwrap();
                assert_ne!(report.selected_manifest, manifests[preferred]);
                let preferred_evaluations: Vec<_> = report
                    .candidate_evaluations
                    .iter()
                    .filter(|c| c.plan_id == Some(manifests[preferred].plan_id))
                    .collect();
                assert!(!preferred_evaluations.is_empty(), "{name}");
                for c in preferred_evaluations {
                    assert_eq!(c.status, expected, "{name}");
                    assert!(c.unavailable_reason.is_some() && c.total_cost.is_none());
                }
            }
        }
        assert_eq!(
            winners.len(),
            manifests.len(),
            "{}: selection did not reverse",
            name
        );
    }
}
