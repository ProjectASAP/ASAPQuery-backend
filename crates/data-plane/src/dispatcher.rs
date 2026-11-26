//! [`QueryDispatcher`]: resolves each query against the installed plan's
//! query texts, runs it on the query engine, and says how a query the plan
//! does not answer falls back ([`Fallback`]).
//!
//! The Prometheus routes run queries through the query engine's own
//! answers ([`common::Answer`]: labels shared with the
//! interned series and the result cache), not through
//! [`ExecutorQueryEngine::execute`]'s [`common::QueryResult`], which would
//! copy every series' labels per query.

use std::sync::{Arc, OnceLock};

use common::{Answer, AnswerSource, InstalledPlan, PhysicalASAPNodeId, PlanId, QueryEngineError};
use query::{ExecutorQueryEngine, Unanswered};

/// The dispatcher's verdict on one query.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryOutcome<T> {
    /// The plan answered.
    Ready(T),
    NotAnswerable(NotAnswerableReason),
}

/// Why the plan did not answer a query; the query falls back.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NotAnswerableReason {
    /// No plan is installed, or its query texts hold no such query.
    Unmatched,
    /// The stored data does not cover the query, or the plan changed since
    /// it was resolved: why.
    NotReady(String),
    /// The engine failed to run the query.
    EngineError(QueryEngineError),
}

impl NotAnswerableReason {
    /// Why `unanswered` was not answered, as the dispatcher reports it.
    pub(crate) fn new(unanswered: Unanswered) -> Self {
        if unanswered.is_not_ready() {
            Self::NotReady(unanswered.to_string())
        } else {
            Self::EngineError(QueryEngineError::new(unanswered.to_string()))
        }
    }

    /// What a client is told: `query` was not answered because of this.
    pub(crate) fn into_message(self, query: &str) -> String {
        match self {
            Self::Unmatched => format!("the installed plan has no query {:?}", query.trim()),
            Self::NotReady(reason) => reason,
            Self::EngineError(error) => error.message,
        }
    }
}

/// How a query the plan does not answer is answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fallback {
    /// Forwarded to the backend (Prometheus, or the raw store in forward
    /// mode), with why the plan did not answer when it planned the query.
    Forward { reason: Option<String> },
    /// Refused (`--no-forward`), with why.
    Refuse { reason: String },
}

impl Fallback {
    /// For `query`, not answered because of `reason`, when the data plane
    /// may forward or not (`no_forward`).
    pub(crate) fn new(reason: NotAnswerableReason, query: &str, no_forward: bool) -> Self {
        match reason {
            _ if no_forward => Self::Refuse {
                reason: reason.into_message(query),
            },
            NotAnswerableReason::Unmatched => Self::Forward { reason: None },
            NotAnswerableReason::NotReady(_) | NotAnswerableReason::EngineError(_) => {
                Self::Forward {
                    reason: Some(reason.into_message(query)),
                }
            }
        }
    }
}

/// The installed plan as the dispatcher matches it.
struct Planned {
    id: PlanId,
    /// Each query text, trimmed, and its root, in plan order.
    queries: Vec<(String, PhysicalASAPNodeId)>,
}

/// Resolves and runs queries; set up by the plan installer.
pub(crate) struct QueryDispatcher {
    engine: Arc<ExecutorQueryEngine>,
    plan: OnceLock<Planned>,
}

impl QueryDispatcher {
    pub(crate) fn new(engine: Arc<ExecutorQueryEngine>) -> Self {
        Self {
            engine,
            plan: OnceLock::new(),
        }
    }

    /// Matches queries against `plan` from now on (v1: the first plan).
    pub(crate) fn install(&self, plan: &InstalledPlan) {
        let queries = plan
            .plan
            .query_texts
            .iter()
            .zip(&plan.plan.dag.roots)
            .map(|(query, &root)| (query.trim().to_string(), root))
            .collect();
        let _ = self.plan.set(Planned {
            id: plan.id,
            queries,
        });
    }

    /// The plan root answering `query`: the first query text equal to it,
    /// both trimmed.
    pub(crate) fn resolve(&self, query: &str) -> Option<AnswerSource> {
        let plan = self.plan.get()?;
        let query = query.trim();
        let (_, root) = plan.queries.iter().find(|(planned, _)| planned == query)?;
        Some(AnswerSource::Planned {
            plan: plan.id,
            root: *root,
        })
    }

    /// `query` at `time_ms`.
    pub(crate) async fn instant(&self, query: &str, time_ms: i64) -> QueryOutcome<Answer> {
        let (Some(source), Some(engine)) = (self.resolve(query), self.engine.installed()) else {
            return QueryOutcome::NotAnswerable(NotAnswerableReason::Unmatched);
        };
        match engine.instant(&source, query, time_ms).await {
            Ok(result) => QueryOutcome::Ready(result),
            Err(unanswered) => QueryOutcome::NotAnswerable(NotAnswerableReason::new(unanswered)),
        }
    }

    /// `query` at each of `times_ms`: every step's answer, or why some step
    /// has none.
    pub(crate) async fn range(
        &self,
        query: &str,
        times_ms: impl Iterator<Item = i64>,
    ) -> QueryOutcome<Vec<(i64, Answer)>> {
        let (Some(source), Some(engine)) = (self.resolve(query), self.engine.installed()) else {
            return QueryOutcome::NotAnswerable(NotAnswerableReason::Unmatched);
        };
        match engine.range(&source, query, times_ms).await {
            Ok(result) => QueryOutcome::Ready(result),
            Err(unanswered) => QueryOutcome::NotAnswerable(NotAnswerableReason::new(unanswered)),
        }
    }
}

#[cfg(test)]
mod tests {
    use asap_executor::planner::ir::{NonASAPOp, Operator, Predicate, ScalarExpr};

    use super::*;
    use crate::engine::EngineHandle;
    use crate::test_support;

    const QUERY: &str = "sum_over_time(data[10s])";

    /// An engine with `plan` installed and samples through 30 s committed.
    async fn engine(plan: &common::InstallablePlan) -> EngineHandle {
        let plan_json = serde_json::to_string(plan).unwrap();
        let (engine, _) = EngineHandle::spawn(plan_json, test_support::options()).unwrap();
        engine
            .try_ingest(test_support::samples(30_000, 1.0))
            .unwrap();
        engine.advance_watermark(30_000).await.unwrap();
        engine
    }

    /// A planned query is answered from its views, and its text is matched
    /// trimmed; another text is unmatched, and forwarded without a reason.
    #[tokio::test]
    async fn answers_a_planned_query_and_leaves_others_unmatched() {
        let engine = engine(&test_support::plan(QUERY, false)).await;
        let dispatcher = engine.dispatcher();
        let QueryOutcome::Ready(answer) = dispatcher.instant(&format!(" {QUERY} "), 30_000).await
        else {
            panic!("not answered");
        };
        let values: Vec<f64> = answer.iter().map(|sample| sample.value).collect();
        assert_eq!(values, [10.0]);
        let unmatched = dispatcher.instant("up", 30_000).await;
        assert_eq!(
            unmatched,
            QueryOutcome::NotAnswerable(NotAnswerableReason::Unmatched)
        );
        assert_eq!(
            Fallback::new(NotAnswerableReason::Unmatched, "up", false),
            Fallback::Forward { reason: None }
        );
    }

    /// A planned query at a time whose window is not stored yet is not
    /// ready; a range with such a step is not ready either.
    #[tokio::test]
    async fn a_window_not_stored_yet_is_not_ready() {
        let engine = engine(&test_support::plan(QUERY, false)).await;
        let dispatcher = engine.dispatcher();
        let outcome = dispatcher.instant(QUERY, 60_000).await;
        let QueryOutcome::NotAnswerable(reason @ NotAnswerableReason::NotReady(_)) = outcome else {
            panic!("expected not ready, got {outcome:?}");
        };
        assert_eq!(
            Fallback::new(reason.clone(), QUERY, false),
            Fallback::Forward {
                reason: Some(reason.into_message(QUERY))
            }
        );
        let range = dispatcher.range(QUERY, [30_000, 40_000].into_iter()).await;
        assert!(matches!(
            range,
            QueryOutcome::NotAnswerable(NotAnswerableReason::NotReady(_))
        ));
    }

    /// A planned query whose run does not compile is an engine error:
    /// forwarded with the error, or refused with it under `--no-forward`.
    #[tokio::test]
    async fn a_query_the_engine_cannot_run_is_an_engine_error() {
        // Raw data kept, so the query runs read the `Scan`, and a predicate
        // on a column it does not have.
        let mut plan = test_support::plan(QUERY, true);
        let scan = plan
            .dag
            .nodes
            .iter_mut()
            .find(|n| matches!(n.payload, Operator::NonASAP(NonASAPOp::Scan { .. })))
            .unwrap();
        let Operator::NonASAP(NonASAPOp::Scan { predicates, .. }) = &mut scan.payload else {
            unreachable!();
        };
        predicates.push(Predicate(ScalarExpr::Column(99)));
        let engine = engine(&plan).await;
        let outcome = engine.dispatcher().instant(QUERY, 30_000).await;
        let QueryOutcome::NotAnswerable(reason @ NotAnswerableReason::EngineError(_)) = outcome
        else {
            panic!("expected an engine error, got {outcome:?}");
        };
        let message = reason.clone().into_message(QUERY);
        assert!(
            message.starts_with("the data plane cannot run this query"),
            "{message}"
        );
        assert_eq!(
            Fallback::new(reason, QUERY, true),
            Fallback::Refuse { reason: message }
        );
    }

    /// Under `--no-forward` an unmatched query is refused, naming it.
    #[test]
    fn an_unmatched_query_is_refused_without_forwarding() {
        assert_eq!(
            Fallback::new(NotAnswerableReason::Unmatched, " up ", true),
            Fallback::Refuse {
                reason: "the installed plan has no query \"up\"".into()
            }
        );
    }
}
