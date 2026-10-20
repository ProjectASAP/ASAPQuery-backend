//! Backend control plane: asks ASAPPlanner for the workload's plan and
//! writes it for the data plane to install.
//!
//! The Planner runs every planning stage, including plan selection, and
//! returns the selected DAG ready to install ([`InstallablePlan`]): the DAG,
//! the query each root answers, the outputs to store with how long each
//! must stay available, and the raw-sample windows read at query time. This
//! crate only supplies the deployment inputs: what the data plane can
//! execute and keep, and how Stage 3 prices candidates.

use std::path::Path;

use asap_planner::{
    plan_installable, DeploymentCapabilities, FrontendInput, InstallablePlan, InstallablePlanError,
    PlanSelectionSettings, PlanningModels, PlanningWorkload, UserInput,
};

/// What the backend gives the Planner about its deployment. The Planner
/// selects; these only say what can run and what it costs.
#[derive(Debug, Clone, PartialEq)]
pub struct DeploymentInputs {
    /// What the data plane can execute and keep (Stages 1–3).
    pub deployment_capabilities: DeploymentCapabilities,
    /// How Stage 3 prices candidates: cost model, resource speed, and any
    /// measured operation costs.
    pub plan_selection: PlanSelectionSettings,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("planning: {0}")]
    Planning(#[from] asap_planner::PlanError),
    #[error("invalid plan: {0}")]
    InvalidPlan(#[from] InstallablePlanError),
    #[error("{path}: {message}")]
    File { path: String, message: String },
}

/// Plans `workload` with the Planner (frontend and Stages 1–3) and returns
/// the plan Stage 3 selected, ready to install. Only PromQL workloads are
/// planned: the Planner refuses another language for the PromQL frontend.
/// It rejects every candidate that needs more than
/// `inputs.deployment_capabilities`, and refuses a plan a runtime could not
/// run through the executor.
pub fn plan_workload(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<InstallablePlan, PlanError> {
    let models = PlanningModels::builtin()
        .with_deployment_capabilities(&inputs.deployment_capabilities)
        .with_plan_selection(&inputs.plan_selection);
    let input = UserInput::new(workload, FrontendInput::Promql { histograms: None }, models);
    // Only the SQL frontend awaits anything; PromQL planning completes on
    // its first poll.
    let plan = futures::executor::block_on(plan_installable(input))?;
    if let Some(selection) = &plan.selection {
        tracing::info!(candidate = %selection.selected_candidate_id, "selected physical plan");
    }
    Ok(plan)
}

/// Writes `plan` as the plan file the data plane installs.
pub fn save_plan(plan: &InstallablePlan, path: &Path) -> Result<(), PlanError> {
    let text = serde_json::to_string_pretty(plan).expect("plans serialize");
    std::fs::write(path, text).map_err(|e| PlanError::File {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

/// Reads and validates a plan written by [`save_plan`].
pub fn load_plan(path: &Path) -> Result<InstallablePlan, PlanError> {
    let file_error = |message: String| PlanError::File {
        path: path.display().to_string(),
        message,
    };
    let text = std::fs::read_to_string(path).map_err(|e| file_error(e.to_string()))?;
    let plan: InstallablePlan =
        serde_json::from_str(&text).map_err(|e| file_error(e.to_string()))?;
    plan.validate()?;
    Ok(plan)
}

/// What this backend's data plane can execute and keep: every summary the
/// Planner executor builds, raw samples kept in Prometheus, and no
/// stored outputs built in a `QueryRun` (`MatAtQueryKeptAcrossEvaluations`): the SDS describes such
/// outputs, but nothing builds them until the query engine does.
pub fn backend_capabilities() -> DeploymentCapabilities {
    DeploymentCapabilities {
        supported_summaries: None,
        raw_data_retained: true,
        query_time_results_retained: false,
    }
}

impl Default for DeploymentInputs {
    /// [`backend_capabilities`] with the Planner's default plan selection
    /// settings (cost model 1 with illustrative unit costs).
    fn default() -> Self {
        Self {
            deployment_capabilities: backend_capabilities(),
            plan_selection: PlanSelectionSettings::default(),
        }
    }
}
