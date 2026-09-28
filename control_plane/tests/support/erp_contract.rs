//! Synthetic evidence for admission contracts, not a measured calibration run.
use asap_aware_mapping::erp::{ErpArtifact, ErpRecord, ErpResourceProfile, ERP_SCHEMA_VERSION};
use control_plane::physical::erp::{ErpAccuracyMode, ErpPlanningInput, ErpRuntimeCapabilities};
use std::collections::BTreeMap;
pub fn policy(mode: ErpAccuracyMode) -> ErpPlanningInput {
    ErpPlanningInput {
        artifact: ErpArtifact {
            schema_version: ERP_SCHEMA_VERSION,
            producer_version: "bench-rev".into(),
            records: vec![ErpRecord {
                id: "cms-512".into(),
                sketch: "cms-fastpath-vector2d".into(),
                implementation: "oxide".into(),
                parameters: serde_json::json!({"rows": 3, "cols": 512}),
                distribution: serde_json::json!({"synthetic":{"kind":"zipf","s":1.1}}),
                trials: 20,
                error_metrics: BTreeMap::from([("relative_error".into(), 0.009)]),
                resources: ErpResourceProfile {
                    memory_bytes: 12_288.0,
                    update_cpu_seconds: 1e-7,
                    merge_cpu_seconds: 1e-5,
                    query_cpu_seconds: 1e-6,
                },
            }],
        },
        distribution: serde_json::json!({"synthetic":{"kind":"zipf","s":1.1}}),
        implementation: Some("oxide".into()),
        error_metric: "relative_error".into(),
        min_trials: 10,
        expected_updates: 1_000.0,
        expected_queries: 100.0,
        expected_merges: 0.0,
        retention_seconds: 60.0,
        cpu_weight: 1.0,
        byte_second_weight: 1e-9,
        mode,
        observed_shape: None,
        observed_populations: None,
        resolved_data_descriptor: None,
        observed_shape_source: None,
        shape_match: None,
        runtime: ErpRuntimeCapabilities::default(),
    }
}
