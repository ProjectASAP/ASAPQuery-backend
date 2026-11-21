//! Push precompute's pipelines (docs/design/parallel-precompute.md,
//! "Combine rules"): each ingestion-time breaker's combine rule, chosen at
//! install from the plan node alone. Interface only: the rules are not
//! implemented yet, so every call fails.

use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAGNode, PhysicalASAPNodeId};
use asap_planner::InstallablePlan;

/// How the shards' partial outputs of one breaker become the window's
/// output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineRule {
    /// One state per series, each series on one shard: the partials are
    /// concatenated in shard order.
    Concatenate,
    /// Partials keyed by coarser groups: `Union` of the shards' partials,
    /// then `SummaryMerge` (or the same exact aggregate again) by group.
    Merge,
    /// Not mergeable: shard 0 owns the breaker; the other shards forward
    /// the rows of its non-blocking prefix.
    SinglePartition,
    /// Not a per-arrival fold (a raw window buffer, a subquery): the whole
    /// group runs one ingestion run per window, as in window mode.
    WindowPath,
}

/// One breaker's rule, as the install log and `/api/v1/status/asap`
/// (`ingestion.pipelines`) report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakerRule {
    /// Index of the refresh group in `InstallablePlan::ingestion_refresh_groups`.
    pub group: usize,
    pub node: PhysicalASAPNodeId,
    /// The operator's name, as the plan's JSON names it.
    pub operator: String,
    pub rule: CombineRule,
}

/// The combine rule of a plan node: `Ok(None)` when it runs at query time
/// or is not a breaker (its rows flow through), and an error naming the
/// node when it is an ingestion-time breaker no rule covers.
pub fn combine_rule(_node: &PhysicalASAPDAGNode) -> Result<Option<CombineRule>, String> {
    Err("combine rules are not implemented yet".into())
}

/// Every ingestion-time breaker's rule, by refresh group; an error naming
/// the node when one has no rule (install fails).
pub fn combine_rules(_plan: &InstallablePlan) -> Result<Vec<BreakerRule>, String> {
    Err("combine rules are not implemented yet".into())
}
