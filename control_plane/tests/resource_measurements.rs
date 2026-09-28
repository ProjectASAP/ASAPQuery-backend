//! Work multiplicities and units, using explicit synthetic resource coefficients.
#[path = "support/erp_contract.rs"]
mod fixture;
use control_plane::physical::erp::{ErpAccuracyMode, ErpParameterDecision};
use planner_types::post_asap::{SketchAlgorithm, SketchParams};

/// CPU seconds and byte-seconds scale independently; error statistics are unchanged.
#[test]
fn erp_resource_composition_uses_workload_multiplicities() {
    for (updates, reads, merges, retention) in [(1000., 100., 0., 60.), (2000., 400., 5., 120.)] {
        let mut policy = fixture::policy(ErpAccuracyMode::Empirical);
        policy.expected_updates = updates;
        policy.expected_queries = reads;
        policy.expected_merges = merges;
        policy.retention_seconds = retention;
        let resources = &policy.artifact.records[0].resources;
        let expected = policy.cpu_weight
            * (updates * resources.update_cpu_seconds
                + reads * resources.query_cpu_seconds
                + merges * resources.merge_cpu_seconds)
            + policy.byte_second_weight * resources.memory_bytes * retention;
        let ErpParameterDecision::Empirical {
            estimated_cost,
            record_id,
            ..
        } = policy.select(
            SketchAlgorithm::Cms,
            0.01,
            SketchParams::Cms {
                width: 4096,
                depth: 5,
            },
        )
        else {
            panic!("applicable ERP must be priced")
        };
        assert!((estimated_cost - expected).abs() < 1e-12);
        assert_eq!(record_id, policy.artifact.records[0].id);
    }
}
