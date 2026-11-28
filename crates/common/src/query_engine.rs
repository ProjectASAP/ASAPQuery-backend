use asap_planner::PhysicalASAPNodeId;

use crate::{PlanId, QueryResult};

/// What answers a query, as the dispatcher resolved it: the engine runs it
/// without matching the query again.
#[derive(Debug, Clone, PartialEq)]
pub enum AnswerSource {
    /// The query is the plan's query `i`, answered by `dag.roots[i]`.
    /// `plan` is the plan it was resolved against.
    Planned {
        plan: PlanId,
        root: PhysicalASAPNodeId,
    },
    // `CapabilityMatched { plan, matched }` goes here once capability
    // matching (answering an unplanned query from the stored views) is
    // designed.
}

/// What the query engine reports for one query.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineQueryResult {
    /// The answer.
    Ready { result: QueryResult },
    /// The stored data does not cover the whole query, or the plan changed
    /// since the query was resolved; the query falls back.
    NotReady,
}

/// A query the engine failed to run. The dispatcher logs and counts it, and
/// the query falls back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct QueryEngineError {
    pub message: String,
}

impl QueryEngineError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
