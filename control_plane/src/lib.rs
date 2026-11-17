//! Backend control plane: asks ASAPPlanner for the workload's plan and
//! writes it for the data plane to install.
//!
//! The Planner runs every planning stage, including plan selection, and
//! returns the selected DAG ready to install ([`InstallablePlan`]): the DAG,
//! with each materialized view's and query-time `Scan`'s `retention_ms`,
//! and the query each root answers. This crate only supplies the
//! deployment inputs: what the data plane can execute and keep, and how
//! Stage 3 costs candidates.

use std::path::Path;

use asap_planner::{
    plan_installable, CostInputs, DeploymentCapabilities, FrontendInput, InstallablePlan,
    InstallablePlanError, PlanningModels, PlanningWorkload, SelectedPlanReport, UserInput,
};

/// What the backend gives the Planner about its deployment. The Planner
/// selects; these only say what can run and what it costs.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeploymentInputs {
    /// What the data plane can execute and keep (Stages 1–3).
    pub deployment_capabilities: DeploymentCapabilities,
    /// How Stage 3 costs candidates: cost model, resource speed, and any
    /// measured operation costs.
    pub cost_inputs: CostInputs,
    /// Accept every query output regardless of its accuracy guarantee:
    /// candidates are compared on cost alone.
    pub ignore_accuracy_guarantees: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("planning: {0}")]
    Planning(#[from] asap_planner::PlanError),
    #[error("invalid plan: {0}")]
    InvalidPlan(#[from] InstallablePlanError),
    #[error("plan the executor cannot run: {0}")]
    NotExecutable(String),
    #[error("{path}: {message}")]
    File { path: String, message: String },
}

/// Plans `workload` with the Planner (frontend and Stages 1–3) and returns
/// the plan Stage 3 selected, ready to install, with Stage 3's report of
/// it. Only PromQL workloads are planned. The Planner rejects every
/// candidate that needs more than `inputs.deployment_capabilities` and
/// refuses a plan its executor cannot compile.
pub fn plan_workload(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<(InstallablePlan, SelectedPlanReport), PlanError> {
    let models = PlanningModels::builtin()
        .with_deployment_capabilities(&inputs.deployment_capabilities)
        .with_cost_inputs(&inputs.cost_inputs)
        .with_ignore_accuracy_guarantees(inputs.ignore_accuracy_guarantees);
    let input = UserInput::new(workload, FrontendInput::Promql, models);
    // Only the SQL frontend awaits anything; PromQL planning completes on
    // its first poll.
    Ok(futures::executor::block_on(plan_installable(input))?)
}

/// Writes `plan` as the plan file the data plane installs.
pub fn save_plan(plan: &InstallablePlan, path: &Path) -> Result<(), PlanError> {
    write_json(plan, path)
}

/// Writes Stage 3's report of the selected plan (cost and predicted usage).
pub fn save_report(report: &SelectedPlanReport, path: &Path) -> Result<(), PlanError> {
    write_json(report, path)
}

fn write_json(value: &impl serde::Serialize, path: &Path) -> Result<(), PlanError> {
    let text = serde_json::to_string_pretty(value).expect("plans serialize");
    std::fs::write(path, text).map_err(|e| PlanError::File {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

/// Reads a plan written by [`save_plan`] and checks that it is valid and
/// that the Planner's executor compiles it.
pub fn load_plan(path: &Path) -> Result<InstallablePlan, PlanError> {
    let file_error = |message: String| PlanError::File {
        path: path.display().to_string(),
        message,
    };
    let text = std::fs::read_to_string(path).map_err(|e| file_error(e.to_string()))?;
    let plan = InstallablePlan::from_json(&text)?;
    plan.validate()?;
    plan.check_compiles().map_err(PlanError::NotExecutable)?;
    Ok(plan)
}

/// What this backend's data plane can execute and keep: every summary the
/// Planner's executor builds (the Planner's default when left `None`) and
/// raw samples, kept in its store for query-time `Scan`s. It refreshes no
/// view on first read yet, so no query-time result is kept across
/// evaluations.
pub fn backend_capabilities() -> DeploymentCapabilities {
    DeploymentCapabilities {
        supported_summaries: None,
        raw_data_retained: true,
        query_time_results_retained: false,
    }
}
