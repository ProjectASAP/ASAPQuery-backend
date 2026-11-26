//! Push precompute's combine rules (docs/design/parallel-precompute.md,
//! "Combine rules"): each ingestion-time breaker's rule, from the plan node
//! alone. The tests check the design's table here; the data plane decides
//! which groups are pushed by its install probe (`precompute::push::probe`).

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

/// One breaker's rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakerRule {
    /// Index of the refresh group in `InstallablePlan::ingestion_refresh_groups`.
    pub group: usize,
    pub node: PhysicalASAPNodeId,
    /// The operator's name, as the plan's JSON names it.
    pub operator: String,
    pub rule: CombineRule,
}

/// Exact aggregate functions whose partials re-aggregate (the `Merge`
/// rule); any other is holistic (single partition).
const DECOMPOSABLE: [&str; 6] = ["sum", "min", "max", "count", "topk", "bottomk"];

/// The combine rule of a plan node: `Ok(None)` when it runs at query time
/// or is not a breaker (its rows flow through), and an error naming the
/// node when it is an ingestion-time breaker no rule covers.
pub fn combine_rule(node: &PhysicalASAPDAGNode) -> Result<Option<CombineRule>, String> {
    use asap_executor::planner::ir::operator::arguments::Reduction;
    use asap_executor::planner::ir::properties::ExecutionTiming;
    use asap_executor::planner::ir::{ASAPOp, NonASAPOp, Operator};
    if node.output_state.timing != ExecutionTiming::IngestionTime {
        return Ok(None);
    }
    Ok(match &node.payload {
        Operator::ASAP(
            ASAPOp::SummaryTumblingWindowChain { reduction, .. }
            | ASAPOp::SummaryAgg { reduction, .. },
        ) => Some(match reduction {
            Reduction::PerEntity => CombineRule::Concatenate,
            Reduction::Reduce(_) => CombineRule::Merge,
        }),
        // After the combine: read the combined states.
        Operator::ASAP(
            ASAPOp::SummaryMerge { .. }
            | ASAPOp::SummaryEstimate { .. }
            | ASAPOp::FinalizeExactAccumulator { .. },
        ) => None,
        // A wider level of a window chain: merged from the narrower
        // level's committed windows, another group's views, so its group
        // runs on the window path after them (D7).
        Operator::ASAP(ASAPOp::SummaryMergeWindows { .. }) => Some(CombineRule::WindowPath),
        Operator::ASAP(_) => {
            return Err(format!(
                "node {}: no combine rule for this ingestion-time operator",
                node.id
            ))
        }
        Operator::NonASAP(NonASAPOp::Aggregate {
            reduction,
            measures,
            ..
        }) => Some(match reduction {
            Reduction::PerEntity => CombineRule::Concatenate,
            Reduction::Reduce(_) => {
                let decomposable = measures.iter().all(|measure| {
                    serde_json::to_value(measure)
                        .ok()
                        .and_then(|m| m["kind"].as_str().map(|k| DECOMPOSABLE.contains(&k)))
                        .unwrap_or(false)
                });
                if decomposable {
                    CombineRule::Merge
                } else {
                    CombineRule::SinglePartition
                }
            }
        }),
        Operator::NonASAP(
            NonASAPOp::Sort { .. }
            | NonASAPOp::Join { .. }
            | NonASAPOp::SQLWindowFunc { .. }
            | NonASAPOp::Limit { .. }
            | NonASAPOp::Dedup { .. },
        ) => Some(CombineRule::SinglePartition),
        // Rows held over a window, not folded on arrival.
        Operator::NonASAP(NonASAPOp::TimeRange { .. } | NonASAPOp::PromqlSubquery { .. }) => {
            Some(CombineRule::WindowPath)
        }
        Operator::NonASAP(_) => None,
    })
}

/// Every ingestion-time breaker's rule, by refresh group; an error naming
/// the node when one has no rule (install fails).
pub fn combine_rules(plan: &InstallablePlan) -> Result<Vec<BreakerRule>, String> {
    // Every ingestion-time node has a rule (or is not a breaker), before
    // the groups are read: a breaker no rule covers is named first.
    for node in &plan.dag.nodes {
        combine_rule(node)?;
    }
    let groups = plan.ingestion_refresh_groups().map_err(|e| e.to_string())?;
    let nodes: std::collections::HashMap<PhysicalASAPNodeId, &PhysicalASAPDAGNode> =
        plan.dag.nodes.iter().map(|n| (n.id, n)).collect();
    let mut rules = Vec::new();
    for (group, refresh) in groups.iter().enumerate() {
        let views: std::collections::HashSet<PhysicalASAPNodeId> =
            refresh.views.iter().map(|v| v.node_id).collect();
        let mut stack: Vec<PhysicalASAPNodeId> = views.iter().copied().collect();
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            let node = nodes[&id];
            if let Some(rule) = combine_rule(node)? {
                let operator = serde_json::to_value(&node.payload)
                    .ok()
                    .and_then(|v| {
                        let (_, inner) = v.as_object()?.iter().next()?;
                        Some(inner.as_object()?.keys().next()?.clone())
                    })
                    .unwrap_or_default();
                rules.push(BreakerRule {
                    group,
                    node: id,
                    operator,
                    rule,
                });
            }
            for edge in plan.dag.edges.iter().filter(|e| e.consumer == id) {
                // Another group's view is a slot, not this group's node.
                if !views.contains(&edge.producer)
                    && groups
                        .iter()
                        .any(|g| g.views.iter().any(|v| v.node_id == edge.producer))
                {
                    continue;
                }
                stack.push(edge.producer);
            }
        }
    }
    rules.sort_by_key(|r| (r.group, r.node));
    Ok(rules)
}
