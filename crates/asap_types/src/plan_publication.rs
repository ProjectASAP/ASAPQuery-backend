//! Canonical publication document for one catalog generation.
use crate::precompute_plan::PrecomputePlan;
use crate::producer_plan::{CollectorPlan, TransmissionPlan};
use crate::query_plan::QueryPlan;
use crate::summary_catalog::SummaryCatalog;
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

/// Complete typed envelope accepted by the data-plane install endpoint.
///
/// Runtime-only routing and adaptation evidence decorate the shared
/// publication without changing its catalog generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalPlanInstallRequest {
    pub summary_catalog: SummaryCatalog,
    #[serde(default)]
    pub collector_plans: Vec<CollectorPlan>,
    pub precompute_plan: PrecomputePlan,
    pub transmission_plan: TransmissionPlan,
    pub query_plan: QueryPlan,
    pub storage_routing: Option<serde_json::Value>,
    #[serde(default)]
    pub adaptation_evidence: Vec<crate::producer_plan::RuntimeAdaptationEvidence>,
}

/// A projected writer must refer to the query entry installed in the same
/// generation.
pub fn validate_maintenance_query_bindings(
    precompute: &PrecomputePlan,
    query: &QueryPlan,
) -> Result<(), String> {
    for (query_id, installed) in &precompute.executable_dags {
        installed.validate()?;
        let mut entries = query
            .entries
            .values()
            .filter(|entry| &entry.query_id == query_id);
        let entry = entries
            .next()
            .ok_or("maintenance projection has no query entry")?;
        if entries.next().is_some() {
            return Err("maintenance projection has ambiguous query entries".into());
        }
        if entry.root != installed.binding.query_plan_sink {
            return Err("maintenance projection and query entry have different roots".into());
        }
        let selected = query
            .selected_dags
            .get(query_id)
            .ok_or("maintenance projection has no selected semantic provenance")?;
        if selected.schema_version != crate::executable_plan::OWNED_POST_ASAP_DAG_SCHEMA_VERSION
            || selected.query_id != *query_id
        {
            return Err("selected semantic provenance has invalid identity/version".into());
        }
        selected.decode()?;
        if installed
            .document
            .nodes
            .iter()
            .any(|node| !selected.nodes.contains(node))
            || installed
                .document
                .edges
                .iter()
                .any(|edge| !selected.edges.contains(edge))
        {
            return Err("maintenance projection differs from its selected DAG".into());
        }
    }
    Ok(())
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
        validate_maintenance_query_bindings(&self.precompute_plan, &self.query_plan)?;
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
                let full_slide = matches!(
                    config.window_layout,
                    crate::WindowMaterializationLayout::FullWindow
                )
                .then_some(config.slide_interval.saturating_mul(1_000));
                if binding.full_window_slide_ms != full_slide {
                    return Err(
                        "query full-window cadence differs from precompute definition".into(),
                    );
                }
                if config.stored_window_ms() != binding.window_ms {
                    return Err("query pane differs from precompute stored window".into());
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

    pub fn install_request(
        &self,
        storage_routing: Option<serde_json::Value>,
        adaptation_evidence: Vec<crate::producer_plan::RuntimeAdaptationEvidence>,
    ) -> Result<PhysicalPlanInstallRequest, String> {
        self.validate()?;
        Ok(PhysicalPlanInstallRequest {
            summary_catalog: self.summary_catalog.clone(),
            collector_plans: self.collector_plans.clone(),
            precompute_plan: self.precompute_plan.clone(),
            transmission_plan: self.transmission_plan.clone(),
            query_plan: self.query_plan.clone(),
            storage_routing,
            adaptation_evidence,
        })
    }
}
