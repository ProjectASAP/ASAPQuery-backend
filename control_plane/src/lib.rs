//! Backend control plane: asks ASAPPlanner for the workload's plan and
//! writes it for the data plane to install.
//!
//! The Planner runs every planning stage, including plan selection. This
//! crate only supplies the deployment's capabilities and records which query
//! each root of the selected `PhysicalASAPDAG` answers.

use std::path::Path;

use asap_plan_selection::{plan_stages, PlanningModels};
use planner_types::deployment::DeploymentCapabilities;
use planner_types::ir::physical_export::PhysicalASAPDAGDocument;
use planner_types::ir::schema_support::with_promql_series_identity;
use planner_types::ir::QueryRoot;
use planner_types::workload::{PlanningWorkload, QueryLanguage, RootDemand};
use serde::{Deserialize, Serialize};

/// The plan the data plane installs: the Planner's selected physical DAG and
/// the query each of its roots answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledPlan {
    /// PromQL text of each root of `dag`, in root order.
    pub queries: Vec<String>,
    pub dag: PhysicalASAPDAGDocument,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("only PromQL workloads are supported, got {0:?}")]
    UnsupportedLanguage(QueryLanguage),
    #[error("query {0} lowers to a scalar, which the physical DAG cannot carry yet")]
    ScalarRoot(usize),
    #[error("PromQL lowering: {0}")]
    Lowering(String),
    #[error("planning: {0}")]
    Planning(String),
    #[error("invalid physical DAG: {0}")]
    InvalidDag(String),
    #[error("the DAG has {roots} roots for {queries} queries")]
    RootCount { roots: usize, queries: usize },
    #[error("{path}: {message}")]
    File { path: String, message: String },
}

impl InstalledPlan {
    pub fn validate(&self) -> Result<(), PlanError> {
        self.dag
            .validate()
            .map_err(|e| PlanError::InvalidDag(e.to_string()))?;
        let roots = self.dag.dag.roots.len();
        if roots != self.queries.len() {
            return Err(PlanError::RootCount {
                roots,
                queries: self.queries.len(),
            });
        }
        Ok(())
    }

    /// Reads and validates a plan written by [`InstalledPlan::save`].
    pub fn load(path: &Path) -> Result<Self, PlanError> {
        let file_error = |message: String| PlanError::File {
            path: path.display().to_string(),
            message,
        };
        let text = std::fs::read_to_string(path).map_err(|e| file_error(e.to_string()))?;
        let plan: Self = serde_json::from_str(&text).map_err(|e| file_error(e.to_string()))?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn save(&self, path: &Path) -> Result<(), PlanError> {
        let text = serde_json::to_string_pretty(self).expect("plans serialize");
        std::fs::write(path, text).map_err(|e| PlanError::File {
            path: path.display().to_string(),
            message: e.to_string(),
        })
    }
}

/// Plans `workload` with the Planner's #509 stage pipeline and returns the
/// selected plan. `capabilities` are what this deployment can execute; the
/// Planner rejects every candidate that needs more.
pub fn plan_workload(
    workload: &PlanningWorkload,
    capabilities: &DeploymentCapabilities,
) -> Result<InstalledPlan, PlanError> {
    let language = &workload.query_workload.language;
    if *language != QueryLanguage::PromQL {
        return Err(PlanError::UnsupportedLanguage(language.clone()));
    }
    let roots = asap_frontend_promql::lower_promql_query_workload(workload, 0)
        .map_err(|e| PlanError::Lowering(e.to_string()))?
        .into_iter()
        .enumerate()
        .map(|(i, root)| match root {
            // Rows carry each series' full identity: the representation
            // per-series state needs at runtime.
            QueryRoot::Operator(node) => with_promql_series_identity(&node)
                .map(|node| (i, QueryRoot::Operator(node)))
                .map_err(|e| PlanError::Lowering(e.to_string())),
            QueryRoot::Scalar(_) => Err(PlanError::ScalarRoot(i)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let entries: Vec<_> = workload.query_workload.entries().collect();
    let demand: Vec<RootDemand> = entries.iter().map(RootDemand::from).collect();
    let data = workload.data_workload.clone().unwrap_or_default();
    let models = PlanningModels::builtin().with_capabilities(capabilities);
    let run = plan_stages(roots, &demand, &data, models, 0)
        .map_err(|e| PlanError::Planning(e.to_string()))?;
    let physical = run.plan.physical;
    tracing::info!(
        candidate = %physical.id,
        label = %physical.label,
        materialization = %physical.materialization,
        "selected physical plan"
    );
    let plan = InstalledPlan {
        queries: entries.into_iter().map(|e| e.query.0).collect(),
        dag: PhysicalASAPDAGDocument::new(physical.dag),
    };
    plan.validate()?;
    Ok(plan)
}

/// What this backend's data plane can execute: the Planner executor it
/// runs, without state kept across query evaluations.
pub fn backend_capabilities() -> DeploymentCapabilities {
    DeploymentCapabilities {
        query_time_retention: false,
        ..asap_executor::capabilities()
    }
}
