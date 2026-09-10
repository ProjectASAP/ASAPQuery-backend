//! Canonical publication document for one catalog generation.
use super::compiler::{CollectorPlan, PhysicalPlan, PrecomputePlan, TransmissionPlan};
use super::summary_catalog::SummaryCatalog;
use crate::query_plan::QueryPlan;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalPlanPublication {
    pub summary_catalog: SummaryCatalog,
    pub precompute_plan: PrecomputePlan,
    pub collector_plans: Vec<CollectorPlan>,
    pub transmission_plan: TransmissionPlan,
    pub query_plan: QueryPlan,
}
impl PhysicalPlanPublication {
    /// Validate every plan against the shared catalog snapshot.
    pub fn validate(&self) -> Result<(), String> {
        let catalog = &self.summary_catalog;
        self.precompute_plan
            .validate_against_catalog(catalog)
            .map_err(|e| e.to_string())?;
        self.transmission_plan
            .validate(&self.precompute_plan)
            .map_err(|e| e.to_string())?;
        self.transmission_plan
            .validate_against_catalog(catalog)
            .map_err(|e| e.to_string())?;
        self.query_plan
            .validate_against_catalog(catalog)
            .map_err(|e| e.to_string())?;
        let materializations = self
            .precompute_plan
            .materializations
            .iter()
            .map(|config| (config.policy_fingerprint(), config))
            .collect::<std::collections::BTreeMap<_, _>>();
        for entry in self.query_plan.entries.values() {
            for binding in entry.materialization_bindings() {
                let config = materializations
                    .get(&binding.materialization.fingerprint())
                    .copied()
                    .ok_or("query binding has no precompute materialization")?;
                if config.slide_interval.checked_mul(1000) != Some(binding.window_ms) {
                    return Err("query pane differs from precompute emission interval".into());
                }
                if config.pane_origin_ms != binding.pane_origin_ms {
                    return Err("query pane origin differs from precompute definition".into());
                }
            }
        }
        let mut collectors = std::collections::BTreeSet::new();
        for collector in &self.collector_plans {
            if collector.envelope != self.precompute_plan.envelope
                || !collectors.insert(&collector.collector_id)
            {
                return Err("collector envelope mismatch or duplicate collector".into());
            }
            collector
                .validate_against_catalog(catalog)
                .map_err(|e| e.to_string())?;
            for rule in &collector.transmission_rules {
                if !self.transmission_plan.rules.contains(rule) {
                    return Err(
                        "collector transmission rule absent from published transmission plan"
                            .into(),
                    );
                }
            }
        }
        for producer in &self.precompute_plan.producers {
            let collector = self
                .collector_plans
                .iter()
                .find(|c| c.collector_id == producer.collector_id)
                .ok_or("precompute producer has no published CollectorPlan")?;
            if !collector
                .materializations
                .iter()
                .any(|m| m.materialization == producer.materialization)
            {
                return Err(
                    "collector does not produce referenced precompute materialization".into(),
                );
            }
        }
        Ok(())
    }
}
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
