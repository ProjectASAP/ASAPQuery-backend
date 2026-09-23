//! Empty authoritative installation for tests that publish a workload later.
pub fn empty() -> asap_types::plan_publication::PhysicalPlanInstallRequest {
    use control_plane::physical::compiler::*;
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
    let catalog =
        control_plane::physical::summary_catalog::SummaryCatalog::from_materializations(0, 0, &[])
            .unwrap();
    let mut precompute_plan = PrecomputePlan::build(envelope.clone(), vec![], &[]).unwrap();
    precompute_plan.summary_catalog = Some(catalog.reference().unwrap());
    let mut transmission_plan =
        build_transmission_plan(envelope, &precompute_plan, &Default::default()).unwrap();
    transmission_plan.summary_catalog = precompute_plan.summary_catalog.clone();
    asap_types::plan_publication::PhysicalPlanInstallRequest {
        summary_catalog: catalog,
        precompute_plan,
        transmission_plan,
        query_plan: asap_types::query_plan::QueryPlan::empty(),
        collector_plans: vec![],
        storage_routing: None,
        adaptation_evidence: vec![],
    }
}
