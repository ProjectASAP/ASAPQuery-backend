//! Current typed startup configuration for processes awaiting plan installation.
pub fn empty() -> serde_json::Value {
    use control_plane::physical::compiler::{PlanEnvelope, BACKEND_COMPAT, PLANNER_REVISION};
    let envelope = PlanEnvelope {
        plan_id: 0,
        plan_version: 0,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "process-fixture".into(),
    };
    let plan = asap_types::precompute_plan::PrecomputePlan::build(envelope, vec![], &[]).unwrap();
    serde_json::json!({"precompute_plan": plan})
}
