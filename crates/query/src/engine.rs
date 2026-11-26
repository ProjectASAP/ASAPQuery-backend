//! [`ExecutorQueryEngine`]: the query engine over the Planner's executor,
//! behind [`common::QueryEngine`]. It is created before any plan; its
//! install compiles the plan's queries and starts the query workers.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use asap_executor::runtime::Limits;
use async_trait::async_trait;
use common::{
    Answer, AnswerSource, EngineQueryResult, InstallError, InstalledPlan, InstantVector,
    InstantVectorElement, LabelSet, QueryEngineError, QueryEval, QueryRequest, QueryResult,
    RangeVector, RangeVectorElement, Sample,
};
use storage::store::{PublishedStore, QueryTimeResults};

use crate::result_cache::ResultCache;
use crate::workers::InstalledQueries;
use crate::CompiledQueries;

/// How the query engine compiles and runs the plan's queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    /// The most bytes one query run may hold.
    pub run_memory_limit_bytes: usize,
    /// Query worker threads: the most query runs at once.
    pub threads: usize,
    /// Compute each query-time `OneEvaluation` node several queries read
    /// once per evaluation time; `false` computes it in every query's run.
    pub share_one_evaluation_results: bool,
    /// How long a query-time result is kept after its evaluation time.
    pub query_time_results_retention_ms: u64,
    /// The most bytes of query-time results kept.
    pub query_time_results_max_bytes: usize,
    /// Keep each planned query's final answer at each evaluation time.
    pub result_cache: bool,
    /// The most bytes of answers the result cache keeps.
    pub result_cache_max_bytes: usize,
    /// How much longer than the plan's retention views are kept, which
    /// bounds how far back the result cache keeps answers.
    pub view_retention_margin_ms: u64,
    /// Refuse a plan with a query the engine cannot run (`--no-forward`).
    pub refuse_unexecutable: bool,
}

/// The query engine: answers the installed plan's queries on the `query-N`
/// workers, over the snapshots the precompute engine publishes. v1
/// installs one plan.
pub struct ExecutorQueryEngine {
    options: QueryOptions,
    published: Arc<PublishedStore>,
    /// Held while a plan is installed, so two installs do not both start.
    installing: Mutex<()>,
    installed: OnceLock<InstalledQueries>,
}

impl ExecutorQueryEngine {
    /// An engine with no plan, which will read what `published` holds.
    pub fn new(options: QueryOptions, published: Arc<PublishedStore>) -> Self {
        Self {
            options,
            published,
            installing: Mutex::new(()),
            installed: OnceLock::new(),
        }
    }

    /// The installed plan's queries and workers; `None` before the install.
    pub fn installed(&self) -> Option<&InstalledQueries> {
        self.installed.get()
    }

    /// Compiles `plan`'s queries and starts the workers.
    fn start(&self, plan: &InstalledPlan) -> anyhow::Result<InstalledQueries> {
        let options = &self.options;
        let views = plan.plan.materialized_views()?;
        let limits = Limits {
            max_bytes: options.run_memory_limit_bytes,
        };
        let results = QueryTimeResults::new(
            storage::store::millis(options.query_time_results_retention_ms),
            options.query_time_results_max_bytes,
        );
        let compiled = CompiledQueries::new(
            &plan.plan,
            &views,
            limits,
            results,
            options.share_one_evaluation_results,
            options.result_cache.then(|| {
                ResultCache::new(
                    ResultCache::horizon_ms(&plan.plan.dag, options.view_retention_margin_ms),
                    options.result_cache_max_bytes,
                )
            }),
        )?;
        tracing::info!(
            shared = ?compiled.shared_nodes(),
            "query-time OneEvaluation nodes shared across queries"
        );
        let unexecutable = compiled.unexecutable();
        if options.refuse_unexecutable && !unexecutable.is_empty() {
            let reasons: Vec<String> = unexecutable
                .iter()
                .map(|(query, reason)| format!("{query:?}: {reason}"))
                .collect();
            anyhow::bail!(
                "--no-forward refuses a plan with queries the data plane cannot run: {}",
                reasons.join("; ")
            );
        }
        Ok(InstalledQueries::start(
            plan.id,
            compiled,
            self.published.clone(),
            options.threads,
        )?)
    }

    /// One request's answer, as the boundary reports it.
    async fn execute_one(
        &self,
        source: &AnswerSource,
        request: &QueryRequest,
    ) -> Result<EngineQueryResult, QueryEngineError> {
        let Some(workers) = self.installed() else {
            return Ok(EngineQueryResult::NotReady);
        };
        let result = match request.eval {
            QueryEval::Instant { time_ms } => workers
                .instant(source, &request.query, time_ms)
                .await
                .map(|answer| QueryResult::Vector(instant_vector(time_ms, &answer))),
            QueryEval::Range {
                start_ms,
                end_ms,
                step_ms,
            } => {
                if step_ms <= 0 {
                    return Err(QueryEngineError::new(format!(
                        "range step {step_ms} ms is not positive"
                    )));
                }
                let times = (0..)
                    .map_while(|i: i64| i.checked_mul(step_ms)?.checked_add(start_ms))
                    .take_while(|time_ms| *time_ms <= end_ms);
                workers
                    .range(source, &request.query, times)
                    .await
                    .map(|steps| QueryResult::Matrix(range_vector(&steps)))
            }
        };
        match result {
            Ok(result) => Ok(EngineQueryResult::Ready { result }),
            Err(unanswered) if unanswered.is_not_ready() => Ok(EngineQueryResult::NotReady),
            Err(error) => Err(QueryEngineError::new(error.to_string())),
        }
    }
}

#[async_trait]
impl common::QueryEngine for ExecutorQueryEngine {
    fn install(&self, plan: InstalledPlan) -> Result<(), InstallError> {
        let _installing = self.installing.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(installed) = self.installed() {
            return Err(InstallError::new(format!(
                "the query engine has plan {} installed and installs one plan",
                installed.plan().0
            )));
        }
        let workers = self
            .start(&plan)
            .map_err(|error| InstallError::new(format!("{error:#}")))?;
        let _ = self.installed.set(workers);
        Ok(())
    }

    async fn execute_batch(
        &self,
        queries: &[(AnswerSource, QueryRequest)],
    ) -> Vec<Result<EngineQueryResult, QueryEngineError>> {
        let mut results = Vec::with_capacity(queries.len());
        for (source, request) in queries {
            results.push(self.execute_one(source, request).await);
        }
        results
    }
}

/// `answer` as an instant vector at `time_ms`.
fn instant_vector(time_ms: i64, answer: &Answer) -> InstantVector {
    InstantVector {
        timestamp_ms: time_ms,
        series: answer
            .iter()
            .map(|sample| InstantVectorElement {
                labels: (*sample.labels).clone(),
                value: sample.value,
            })
            .collect(),
    }
}

/// Each series' value at each step it has one, series sorted by labels.
fn range_vector(steps: &[(i64, Answer)]) -> RangeVector {
    let mut series: BTreeMap<&LabelSet, Vec<Sample>> = BTreeMap::new();
    for (time_ms, answer) in steps {
        for sample in answer.iter() {
            series.entry(&*sample.labels).or_default().push(Sample {
                timestamp_ms: *time_ms,
                value: sample.value,
            });
        }
    }
    RangeVector {
        series: series
            .into_iter()
            .map(|(labels, samples)| RangeVectorElement {
                labels: labels.clone(),
                samples,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use common::{PlanId, QueryEngine as _, QueryLanguage};

    use super::*;

    fn plan(query: &str) -> common::InstallablePlan {
        let workload = serde_json::from_value(serde_json::json!({
            "query_workload": {"language": "promql", "repeating_queries": [{
                "query": query,
                "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
                "requirements": {},
                "predictability": {"predictable": {"known_at": null}},
            }]},
            "data_workload": {"arrival": {"ingesting": {"rows_per_second": 1.0}},
              "sources": {"data": {"series_count": 1, "sample_interval": 1000,
                "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
        }))
        .unwrap();
        let mut inputs = control_plane::DeploymentInputs::default();
        inputs.deployment_capabilities.raw_data_retained = false;
        control_plane::plan_workload(&workload, &inputs).unwrap().0
    }

    const QUERY: &str = "sum_over_time(data[10s])";

    /// Through `QueryEngine`: nothing is ready before the install, the plan
    /// installs once, a query resolved against another plan is not ready,
    /// and neither is one whose views are not stored yet. Its measurements
    /// are found by its trimmed text.
    #[tokio::test]
    async fn the_query_engine_installs_once_and_reports_not_ready() {
        let options = QueryOptions {
            run_memory_limit_bytes: 1 << 30,
            threads: 1,
            share_one_evaluation_results: true,
            query_time_results_retention_ms: 60_000,
            query_time_results_max_bytes: 1 << 20,
            result_cache: true,
            result_cache_max_bytes: 1 << 20,
            view_retention_margin_ms: 60_000,
            refuse_unexecutable: false,
        };
        let engine =
            ExecutorQueryEngine::new(options, Arc::new(PublishedStore::new(Arc::default())));
        let plan = plan(QUERY);
        let root = plan.dag.roots[0];
        let source = |id| AnswerSource::Planned {
            plan: PlanId(id),
            root,
        };
        let request = |eval| QueryRequest {
            language: QueryLanguage::PromQL,
            query: QUERY.into(),
            eval,
        };
        let instant = request(QueryEval::Instant { time_ms: 30_000 });
        assert_eq!(
            engine.execute(&source(1), &instant).await,
            Ok(EngineQueryResult::NotReady)
        );
        let installed = |id| InstalledPlan {
            id: PlanId(id),
            plan: plan.clone(),
        };
        assert_eq!(engine.install(installed(1)), Ok(()));
        assert!(engine.install(installed(2)).is_err());
        // The query's measurements are found by its text, trimmed.
        let compiled = engine.installed().unwrap().compiled();
        assert_eq!(compiled.metrics_index(&format!(" {QUERY} ")), Some(0));
        assert_eq!(compiled.metrics_index("up"), None);
        let range = request(QueryEval::Range {
            start_ms: 10_000,
            end_ms: 30_000,
            step_ms: 10_000,
        });
        let results = engine
            .execute_batch(&[
                (source(2), instant.clone()),
                (source(1), instant),
                (source(1), range),
            ])
            .await;
        assert_eq!(results, vec![Ok(EngineQueryResult::NotReady); 3]);
    }

    /// Answers become an instant vector, and steps a matrix of each
    /// series' samples, series sorted by labels.
    #[test]
    fn answers_convert_to_boundary_results() {
        let labels = |job: &str| Arc::new(LabelSet::from([("job".to_string(), job.to_string())]));
        let (a, b) = (labels("a"), labels("b"));
        let sample = |labels: &Arc<LabelSet>, value| common::ResultSample {
            labels: labels.clone(),
            value,
        };
        let first: Answer = Arc::new(vec![sample(&b, 1.0), sample(&a, 2.0)]);
        let second: Answer = Arc::new(vec![sample(&a, 3.0)]);
        assert_eq!(
            instant_vector(10, &first),
            InstantVector {
                timestamp_ms: 10,
                series: vec![
                    InstantVectorElement {
                        labels: (*b).clone(),
                        value: 1.0
                    },
                    InstantVectorElement {
                        labels: (*a).clone(),
                        value: 2.0
                    },
                ],
            }
        );
        let point = |timestamp_ms, value| Sample {
            timestamp_ms,
            value,
        };
        assert_eq!(
            range_vector(&[(10, first), (20, second)]),
            RangeVector {
                series: vec![
                    RangeVectorElement {
                        labels: (*a).clone(),
                        samples: vec![point(10, 2.0), point(20, 3.0)],
                    },
                    RangeVectorElement {
                        labels: (*b).clone(),
                        samples: vec![point(10, 1.0)],
                    },
                ],
            }
        );
    }
}
