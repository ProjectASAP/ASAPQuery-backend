//! Internal realization boundary over the existing compiler and cost contracts.
//!
//! Providers may validate and price physical implementations, never rewrite
//! selected logical roots or infer a pane width from a query's slide.
use super::compiler::{
    CompileError, CompiledPhysicalPlan, PhysicalCompilationRequest, PhysicalDeploymentContext,
    PhysicalPlanCompiler, QueryCompilationInput,
};
use super::workload_cost::{PricedComponents, WorkloadCostEvidence, WorkloadCostManifest};
use asap_aware_mapping::cost_model::Cost;
use planner_types::post_asap::SummaryWindowFramework;

pub(crate) trait RealizationProvider {
    fn windows(
        &self,
        query: &QueryCompilationInput,
        environment: &PhysicalDeploymentContext,
    ) -> Result<Vec<(String, SummaryWindowFramework, Cost)>, CompileError>;

    fn compile(
        &self,
        request: PhysicalCompilationRequest,
        environment: PhysicalDeploymentContext,
        frontend: super::compiler::QueryFrontend,
    ) -> Result<CompiledPhysicalPlan, CompileError>;

    fn price(
        &self,
        evidence: &WorkloadCostEvidence,
        manifest: &WorkloadCostManifest,
    ) -> Result<PricedComponents, (super::workload_cost::CandidateEvaluationStatus, String)>;
}

/// Only the currently implemented deployment paths. Capability validation
/// remains in the compiler; a quote cannot authorize an unsupported runtime.
pub(crate) struct ExistingRealizations;

impl RealizationProvider for ExistingRealizations {
    fn windows(
        &self,
        query: &QueryCompilationInput,
        environment: &PhysicalDeploymentContext,
    ) -> Result<Vec<(String, SummaryWindowFramework, Cost)>, CompileError> {
        super::compiler::validate_window_implementations(query, environment)
    }

    fn compile(
        &self,
        request: PhysicalCompilationRequest,
        environment: PhysicalDeploymentContext,
        frontend: super::compiler::QueryFrontend,
    ) -> Result<CompiledPhysicalPlan, CompileError> {
        if frontend == super::compiler::QueryFrontend::MetricsQl {
            PhysicalPlanCompiler.compile_metricsql(request, environment)
        } else {
            PhysicalPlanCompiler.compile_promql(request, environment)
        }
    }

    fn price(
        &self,
        evidence: &WorkloadCostEvidence,
        manifest: &WorkloadCostManifest,
    ) -> Result<PricedComponents, (super::workload_cost::CandidateEvaluationStatus, String)> {
        evidence.price(manifest)
    }
}
