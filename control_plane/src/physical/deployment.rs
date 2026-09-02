//! Physical deployment constraints used by CTSA stage placement.

use crate::optimizer::cost::sketch_capability::{default_capability_table, SketchCapability};

/// Built-in physical resource profile for a sketch implementation.
pub fn sketch_capability(st: &crate::types::SketchType) -> SketchCapability {
    use planner_types::post_asap::SketchAlgorithm;
    let kind: SketchAlgorithm = st.clone().into();
    default_capability_table()
        .remove(&kind)
        .expect("the physical capability table covers every sketch algorithm")
}

/// Resource budget for a single physical pipeline stage.
#[derive(Debug, Clone, Default)]
pub struct StageBudget {
    pub memory_bytes: Option<u64>,
    pub cpu_micros_per_sample: Option<f64>,
    pub disk_bytes: Option<u64>,
    pub bandwidth_bytes_per_sec: Option<f64>,
}

impl StageBudget {
    pub fn fits(&self, capability: &SketchCapability) -> bool {
        self.memory_bytes
            .is_none_or(|limit| capability.memory_bytes_per_series <= limit)
            && self
                .cpu_micros_per_sample
                .is_none_or(|limit| capability.cpu_micros_per_insert <= limit)
            && self
                .bandwidth_bytes_per_sec
                .is_none_or(|limit| capability.transmission_bytes as f64 <= limit)
    }
}

/// Resource constraints for physical CTSA placement targets.
#[derive(Debug, Clone, Default)]
pub struct DeploymentConstraints {
    pub agent: StageBudget,
    pub backend_collector: StageBudget,
    pub backend_db: StageBudget,
    pub original_db: StageBudget,
    pub object_store: StageBudget,
}

impl DeploymentConstraints {
    pub fn from_budgets(budgets: &crate::types::StageResourceBudgets) -> Self {
        Self {
            agent: StageBudget {
                memory_bytes: budgets.agent_memory_bytes,
                cpu_micros_per_sample: budgets.agent_cpu_micros_per_sample,
                ..Default::default()
            },
            backend_collector: StageBudget {
                memory_bytes: budgets.backend_memory_bytes,
                ..Default::default()
            },
            backend_db: StageBudget {
                memory_bytes: budgets.precompute_memory_bytes,
                ..Default::default()
            },
            ..Default::default()
        }
    }
}
