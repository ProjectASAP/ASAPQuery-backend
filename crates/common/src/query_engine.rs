use asap_planner::PhysicalASAPNodeId;
use async_trait::async_trait;

use crate::{InstallError, InstalledPlan, PlanId, QueryRequest, QueryResult};

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

/// The engine that answers queries from the installed plan's views and raw
/// data. It holds its installed plan and steps through range queries
/// itself.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Replaces the plan it answers from. On an error, the current plan
    /// stays.
    fn install(&self, plan: InstalledPlan) -> Result<(), InstallError>;

    /// One query: a batch of one.
    async fn execute(
        &self,
        source: &AnswerSource,
        request: &QueryRequest,
    ) -> Result<EngineQueryResult, QueryEngineError> {
        let mut results = self
            .execute_batch(&[(source.clone(), request.clone())])
            .await;
        let returned = results.len();
        match results.pop() {
            Some(result) if returned == 1 => result,
            _ => Err(QueryEngineError::new(format!(
                "execute_batch returned {returned} results for one query"
            ))),
        }
    }

    /// Several queries evaluated together, so the work the plan shares
    /// between them runs once. One result per query, in order.
    async fn execute_batch(
        &self,
        queries: &[(AnswerSource, QueryRequest)],
    ) -> Vec<Result<EngineQueryResult, QueryEngineError>>;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryEval, QueryLanguage, Sample};

    /// Returns `results_per_call` copies of its first query's answer and
    /// records the batch it was given.
    struct Batches {
        results_per_call: usize,
        seen: std::sync::Mutex<Vec<Vec<(AnswerSource, QueryRequest)>>>,
    }

    #[async_trait]
    impl QueryEngine for Batches {
        fn install(&self, _plan: InstalledPlan) -> Result<(), InstallError> {
            Ok(())
        }

        async fn execute_batch(
            &self,
            queries: &[(AnswerSource, QueryRequest)],
        ) -> Vec<Result<EngineQueryResult, QueryEngineError>> {
            self.seen.lock().unwrap().push(queries.to_vec());
            let QueryEval::Instant { time_ms } = queries[0].1.eval else {
                unreachable!("the tests send instant queries")
            };
            let answer = EngineQueryResult::Ready {
                result: QueryResult::Scalar(Sample {
                    timestamp_ms: time_ms,
                    value: 1.0,
                }),
            };
            vec![Ok(answer); self.results_per_call]
        }
    }

    fn query() -> (AnswerSource, QueryRequest) {
        let source = AnswerSource::Planned {
            plan: PlanId(1),
            root: 7,
        };
        let request = QueryRequest {
            language: QueryLanguage::PromQL,
            query: "sum(up)".into(),
            eval: QueryEval::Instant { time_ms: 60_000 },
        };
        (source, request)
    }

    fn engine(results_per_call: usize) -> Batches {
        Batches {
            results_per_call,
            seen: Default::default(),
        }
    }

    #[tokio::test]
    async fn execute_is_a_batch_of_one() {
        let engine = engine(1);
        let (source, request) = query();
        let result = engine.execute(&source, &request).await;
        assert_eq!(
            result,
            Ok(EngineQueryResult::Ready {
                result: QueryResult::Scalar(Sample {
                    timestamp_ms: 60_000,
                    value: 1.0
                })
            })
        );
        assert_eq!(*engine.seen.lock().unwrap(), vec![vec![query()]]);
    }

    #[tokio::test]
    async fn execute_reports_a_batch_without_exactly_one_result() {
        let (source, request) = query();
        for returned in [0, 2] {
            let result = engine(returned).execute(&source, &request).await;
            assert_eq!(
                result,
                Err(QueryEngineError::new(format!(
                    "execute_batch returned {returned} results for one query"
                )))
            );
        }
    }
}
