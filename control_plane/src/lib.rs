//! Backend control plane: asks ASAPPlanner for the workload's plan and
//! writes it for the data plane to install.
//!
//! The Planner runs every planning stage, including plan selection, and
//! returns the selected DAG ready to install ([`InstallablePlan`]). This
//! crate only supplies the deployment inputs: what the data plane can
//! execute and keep, and how Stage 3 prices candidates.

use std::path::Path;

use asap_planner::{
    e2e_plan, plan_installable, DeploymentCapabilities, FrontendInput, InstallablePlan,
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
    #[error("{path}: {message}")]
    File { path: String, message: String },
}

/// Plans `workload` with the Planner (frontends and Stages 1–3) and returns
/// the plan Stage 3 selected, ready to install. Only PromQL workloads are
/// planned: the Planner refuses another language for the PromQL frontend.
/// It rejects every candidate that needs more than
/// `inputs.deployment_capabilities`, and refuses a plan the data plane could
/// not run.
pub fn plan_workload(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<InstallablePlan, PlanError> {
    let models = PlanningModels::builtin()
        .with_deployment_capabilities(&inputs.deployment_capabilities)
        .with_plan_selection(&inputs.plan_selection);
    let input = UserInput::new(workload, FrontendInput::Promql { histograms: None }, models);
    // Only the SQL frontend awaits anything; PromQL planning completes on
    // first poll.
    let plan = futures::executor::block_on(plan_installable(input))?;
    if let Some(selection) = &plan.selection {
        tracing::info!(candidate = %selection.selected_candidate_id, "selected physical plan");
    }
    Ok(plan)
}

/// Stage 3's prediction for the plan it selects: its price and its
/// resource usage per second of the workload, and where its nodes' prices
/// came from. For comparing with a measured run.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Prediction {
    pub selected_candidate_id: String,
    pub cost_per_second: Option<f64>,
    /// CPU core-seconds per second: CPU operations and scanned bytes at the
    /// settings' resource speed.
    pub cpu_core_seconds_per_second: f64,
    pub average_memory_bytes: f64,
    pub peak_memory_bytes: u64,
    /// How many of the selected plan's nodes were priced from measured
    /// operation costs, and how many from the Planner's formulas.
    pub nodes_priced_by_measurement: usize,
    pub nodes_priced_by_formula: usize,
}

/// Stage 3's [`Prediction`] for the plan [`plan_workload`] selects for
/// `workload` under `inputs`.
pub fn predict(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<Prediction, PlanError> {
    // `plan_installable` restricts unrestricted summaries to the executor's
    // families; plan over the same candidates.
    let mut capabilities = inputs.deployment_capabilities.clone();
    if capabilities.supported_summaries.is_none() {
        capabilities.supported_summaries = Some(asap_executor::supported_summary_families());
    }
    let models = PlanningModels::builtin()
        .with_deployment_capabilities(&capabilities)
        .with_plan_selection(&inputs.plan_selection);
    let input = UserInput::new(workload, FrontendInput::Promql { histograms: None }, models);
    let output = futures::executor::block_on(e2e_plan(input))?;
    let missing = || PlanError::File {
        path: "Stage 3 selection".into(),
        message: "the planning pass selected no priced candidate".into(),
    };
    let selection = output.selection.ok_or_else(missing)?;
    let cost = selection
        .costs
        .get(&selection.selected)
        .ok_or_else(missing)?;
    let speed = inputs.plan_selection.resource_speed;
    let usage = &cost.usage;
    let measured = cost
        .per_node
        .values()
        .filter(|node| {
            serde_json::to_value(node.price_source).ok() == Some(serde_json::json!("measured"))
        })
        .count();
    Ok(Prediction {
        selected_candidate_id: selection.selected.clone(),
        cost_per_second: cost.cost_per_second,
        cpu_core_seconds_per_second: (usage.cpu_ops_per_second * speed.ms_per_cpu_op
            + usage.scanned_bytes_per_second * speed.ms_per_scanned_byte)
            / 1000.0,
        average_memory_bytes: usage.average_memory_bytes,
        peak_memory_bytes: usage.peak_memory_bytes,
        nodes_priced_by_measurement: measured,
        nodes_priced_by_formula: cost.per_node.len() - measured,
    })
}

/// Writes `plan` as the plan file the data plane installs.
pub fn save_plan(plan: &InstallablePlan, path: &Path) -> Result<(), PlanError> {
    let text = serde_json::to_string_pretty(plan).expect("plans serialize");
    std::fs::write(path, text).map_err(|e| PlanError::File {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

/// What this backend's data plane can execute and keep: every summary the
/// Planner executor builds, raw samples kept in Prometheus, and query-time
/// results kept across evaluations in memory
/// (`MatAtQueryKeptAcrossEvaluations`).
pub fn backend_capabilities() -> DeploymentCapabilities {
    DeploymentCapabilities {
        supported_summaries: None,
        raw_data_retained: true,
        query_time_results_retained: true,
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
