//! Control-plane construction of the shared catalog publication contract.
use super::compiler::PhysicalPlan;
pub use asap_types::plan_publication::{PhysicalPlanInstallRequest, PhysicalPlanPublication};

impl PhysicalPlan {
    pub fn publication(&self) -> Result<PhysicalPlanPublication, String> {
        let artifact = PhysicalPlanPublication {
            summary_catalog: self.summary_catalog.clone(),
            precompute_plan: self.precompute_plan.clone(),
            collector_plans: self.collector_plans.clone(),
            transmission_plan: self.transmission_plan.clone(),
            query_plan: self.query_plan.clone(),
        };
        artifact.validate()?;
        Ok(artifact)
    }
}
