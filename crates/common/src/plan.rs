use asap_planner::InstallablePlan;

/// The installed plan's version for the workload: a counter the installer
/// increments per install.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlanId(pub u64);

/// A validated `InstallablePlan` with the id it was installed under. Nothing
/// else: everything an engine derives from the plan, it computes from `plan`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstalledPlan {
    pub id: PlanId,
    pub plan: InstallablePlan,
}

/// Why an engine refused a plan; the installer keeps the current plan and
/// logs `message`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct InstallError {
    pub message: String,
}

impl InstallError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
