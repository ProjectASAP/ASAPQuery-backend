//! Backend control plane: asks ASAPPlanner for the workload's plan and
//! writes it for the data plane to install.
//!
//! The Planner runs every planning stage, including plan selection. This
//! crate only supplies the deployment inputs (what the data plane can execute
//! and keep, and how Stage 3 prices candidates) and records which query each
//! root of the selected `PhysicalASAPDAG` answers.

use std::path::Path;
use std::rc::Rc;

use asap_logical_asap_optimization::DeploymentCapabilities;
use asap_plan_selection::{
    resize_for_failure_probability, run_stage1_to_stage3, with_confirmed_weights,
    PlanSelectionSettings, PlanningModels, Selection, Stage1ToStage3Run,
};
use planner_types::ir::operator::pane_chain::expand_pane_chains;
use planner_types::ir::physical_export::{compile_physical_asap_workload, PhysicalASAPDAGDocument};
use planner_types::ir::schema::schema_support::with_promql_series_identity;
use planner_types::ir::QueryRoot;
use planner_types::workload::{DataWorkload, PlanningWorkload, QueryLanguage, QueryWorkloadEntry};
use serde::{Deserialize, Serialize};

/// The plan the data plane installs: the Planner's selected physical DAG and
/// the query each of its roots answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledPlan {
    /// PromQL text of each root of `dag`, in root order.
    pub queries: Vec<String>,
    /// The selected candidate's DAG, with every `PaneChain` expanded into
    /// the panes the executor runs.
    pub dag: PhysicalASAPDAGDocument,
}

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
    #[error("only PromQL workloads are supported, got {0:?}")]
    UnsupportedLanguage(QueryLanguage),
    #[error("workload: {0}")]
    Workload(String),
    #[error("query {0} lowers to a scalar, which the data plane cannot answer yet")]
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

    /// The installed plan for `roots`, the timed roots of one candidate in
    /// query order.
    fn from_roots(queries: Vec<String>, roots: &[QueryRoot]) -> Result<Self, PlanError> {
        let roots = roots
            .iter()
            .enumerate()
            .map(|(i, root)| match root {
                // The executor runs a pane chain as its panes.
                QueryRoot::Operator(node) => expand_pane_chains(node)
                    .map(QueryRoot::Operator)
                    .map_err(|e| PlanError::InvalidDag(e.to_string())),
                QueryRoot::Scalar(_) => Err(PlanError::ScalarRoot(i)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let dag = compile_physical_asap_workload(&roots)
            .map_err(|e| PlanError::InvalidDag(e.to_string()))?;
        let plan = Self {
            queries,
            dag: PhysicalASAPDAGDocument::new(dag),
        };
        plan.validate()?;
        Ok(plan)
    }
}

/// One planned workload: what Stages 1–3 returned and the inputs they read.
struct Planned {
    queries: Vec<String>,
    entries: Vec<QueryWorkloadEntry>,
    data: DataWorkload,
    run: Stage1ToStage3Run<usize>,
}

/// Lowers `workload` (Stage 0) and runs Stages 1–3 under `inputs`.
fn plan(workload: &PlanningWorkload, inputs: &DeploymentInputs) -> Result<Planned, PlanError> {
    let language = &workload.query_workload.language;
    if *language != QueryLanguage::PromQL {
        return Err(PlanError::UnsupportedLanguage(language.clone()));
    }
    workload
        .validate()
        .map_err(|e| PlanError::Workload(e.to_string()))?;
    let roots = asap_frontend_promql::lower_promql_query_workload(workload)
        .map_err(|e| PlanError::Lowering(e.to_string()))?
        .into_iter()
        .enumerate()
        .map(|(i, root)| match root {
            // Rows carry each series' full identity, the representation
            // per-series state needs at runtime, as the Planner's own pass
            // prepares PromQL roots.
            QueryRoot::Operator(node) => {
                let node = with_promql_series_identity(&node).unwrap_or_else(|_| Rc::clone(&node));
                Ok((i, QueryRoot::Operator(node)))
            }
            QueryRoot::Scalar(_) => Err(PlanError::ScalarRoot(i)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let entries: Vec<QueryWorkloadEntry> = workload.query_workload.entries().collect();
    let data = workload.data_workload.clone().unwrap_or_default();
    let models = PlanningModels::builtin()
        .with_deployment_capabilities(&inputs.deployment_capabilities)
        .with_plan_selection(&inputs.plan_selection);
    let run = run_stage1_to_stage3(roots, &entries, &data, models)
        .map_err(|e| PlanError::Planning(e.to_string()))?;
    Ok(Planned {
        queries: entries.iter().map(|e| e.query.0.clone()).collect(),
        entries,
        data,
        run,
    })
}

/// Plans `workload` with the Planner's stage pipeline (Stages 0–3) and
/// returns the plan Stage 3 selected. The Planner rejects every candidate
/// that needs more than `inputs.deployment_capabilities`.
pub fn plan_workload(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<InstalledPlan, PlanError> {
    plan_and_select(workload, inputs).map(|(plan, _)| plan)
}

/// [`plan_workload`], with Stage 3's selection record: every valid
/// candidate's predicted resource usage and cost, and why the others lost.
/// Experiments read it to compare predicted with measured usage.
pub fn plan_and_select(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<(InstalledPlan, Selection), PlanError> {
    let planned = plan(workload, inputs)?;
    let selection = planned
        .run
        .stage3_plan_selection
        .map_err(|e| PlanError::Planning(e.to_string()))?;
    tracing::info!(candidate = %selection.selected, "selected physical plan");
    let plan = InstalledPlan::from_roots(planned.queries, &selection.plan)?;
    Ok((plan, selection))
}

/// The selected plan with nothing materialized: the Stage 2 candidate that
/// implements the same logical candidate with every summary built at query
/// time, sized as Stage 3 sizes the selected one. Its answers are what the
/// installed plan's must equal, so tests use it as the reference.
pub fn plan_selected_at_query_time(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
) -> Result<InstalledPlan, PlanError> {
    let planned = plan(workload, inputs)?;
    let run = &planned.run;
    let missing = |what: &str| PlanError::Planning(format!("no {what} in the planner's run"));
    let selected = run
        .selected()
        .ok_or_else(|| missing("selected candidate"))?;
    // Stage 2 numbers `L<n>`'s candidates `P<n>` (nothing materialized),
    // then `P<n>-m<k>`.
    let number = selected
        .from_logical
        .strip_prefix('L')
        .ok_or_else(|| missing("logical candidate number"))?;
    let unmaterialized = run
        .stage2_physical_asap_optimization
        .iter()
        .find(|p| p.id == format!("P{number}"))
        .ok_or_else(|| missing("unmaterialized candidate"))?;
    let targets: Vec<_> = planned
        .entries
        .iter()
        .map(|e| e.requirements.accuracy.clone())
        .collect();
    let sized = resize_for_failure_probability(&unmaterialized.roots, &targets, &planned.data)
        .map_err(|e| PlanError::Planning(e.to_string()))?;
    let roots = with_confirmed_weights(&sized, &planned.data);
    InstalledPlan::from_roots(planned.queries, &roots)
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
