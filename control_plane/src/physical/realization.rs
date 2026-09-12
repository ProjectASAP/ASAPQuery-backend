//! Internal realization boundary over the existing compiler and cost contracts.
//!
//! Providers may validate and price physical implementations, never rewrite
//! selected logical roots or infer a pane width from a query's slide.
use super::compiler::{
    CompileError, DeploymentEnvironment, PhysicalCompiler, PhysicalPlan, PlanningQuery,
    PlanningRequest,
};
use super::workload_cost::{PricedComponents, WorkloadCostEvidence, WorkloadCostManifest};
use asap_aware_mapping::cost_model::Cost;
use planner_types::post_asap::SummaryWindowFramework;

pub(crate) trait RealizationProvider {
    fn stages(
        &self,
        expression: &super::post_asap::PhysicalExpr,
        topology: super::colored_dag::Topology,
    ) -> anyhow::Result<
        std::collections::HashMap<super::colored_dag::StageId, super::colored_dag::StageConfig>,
    >;

    fn windows(
        &self,
        query: &PlanningQuery,
        environment: &DeploymentEnvironment,
    ) -> Result<Vec<(String, SummaryWindowFramework, Cost)>, CompileError>;

    fn compile(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
        metricsql: bool,
    ) -> Result<PhysicalPlan, CompileError>;

    fn price(
        &self,
        evidence: &WorkloadCostEvidence,
        manifest: &WorkloadCostManifest,
    ) -> Result<PricedComponents, (&'static str, String)>;
}

/// Only the currently implemented deployment paths. Capability validation
/// remains in the compiler; a quote cannot authorize an unsupported runtime.
pub(crate) struct ExistingRealizations;

impl RealizationProvider for ExistingRealizations {
    fn stages(
        &self,
        expression: &super::post_asap::PhysicalExpr,
        topology: super::colored_dag::Topology,
    ) -> anyhow::Result<
        std::collections::HashMap<super::colored_dag::StageId, super::colored_dag::StageConfig>,
    > {
        use super::colored_dag::{Emitter, StageAllocator, ThreeStageEmitter};
        let dag = StageAllocator.allocate(expression, topology)?;
        Ok(ThreeStageEmitter.emit_per_stage(&dag)?)
    }

    fn windows(
        &self,
        query: &PlanningQuery,
        environment: &DeploymentEnvironment,
    ) -> Result<Vec<(String, SummaryWindowFramework, Cost)>, CompileError> {
        super::compiler::validate_window_implementations(query, environment)
    }

    fn compile(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
        metricsql: bool,
    ) -> Result<PhysicalPlan, CompileError> {
        if metricsql {
            PhysicalCompiler.compile_metricsql(request, environment)
        } else {
            PhysicalCompiler.compile(request, environment)
        }
    }

    fn price(
        &self,
        evidence: &WorkloadCostEvidence,
        manifest: &WorkloadCostManifest,
    ) -> Result<PricedComponents, (&'static str, String)> {
        evidence.price(manifest)
    }
}
