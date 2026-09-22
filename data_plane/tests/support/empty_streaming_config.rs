//! A valid empty DAG installation for process tests that publish a plan later.

pub fn empty() -> data_plane::storage_engines::types::StreamingConfig {
    use control_plane::physical::compiler::{
        PlanEnvelope, PrecomputePlan, BACKEND_COMPAT, PLANNER_REVISION,
    };
    let envelope = PlanEnvelope {
        plan_id: 0,
        plan_version: 0,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "empty-bootstrap".into(),
    };
    let plan = PrecomputePlan::build(envelope, vec![], &[]).unwrap();
    data_plane::storage_engines::types::StreamingConfig::from_precompute_plan(plan).unwrap()
}
