//! [`PlanInstaller`]: installs a plan into the query engine, the ingest
//! engine and the dispatcher, all or nothing.

use std::sync::{Arc, Mutex};

use common::{IngestEngine, InstallError, InstallablePlan, InstalledPlan, PlanId, QueryEngine};

use crate::dispatcher::QueryDispatcher;

/// Installs plans. v1 installs one: the plan file at startup.
pub struct PlanInstaller {
    ingest: Arc<dyn IngestEngine>,
    query: Arc<dyn QueryEngine>,
    dispatcher: Arc<QueryDispatcher>,
    /// The installed plan's id; `None` before the first install.
    installed: Mutex<Option<PlanId>>,
}

impl PlanInstaller {
    pub fn new(
        ingest: Arc<dyn IngestEngine>,
        query: Arc<dyn QueryEngine>,
        dispatcher: Arc<QueryDispatcher>,
    ) -> Self {
        Self {
            ingest,
            query,
            dispatcher,
            installed: Mutex::new(None),
        }
    }

    /// Validates `plan`, then installs it into the query engine, the ingest
    /// engine and the dispatcher, in that order, stopping at the first
    /// failure. Queries reach a plan only through the dispatcher, which
    /// gets it last: on any failure the current plan stays the one queries
    /// are answered from. The query engine goes first because its install
    /// only compiles and starts idle workers; the ingest engine's starts
    /// ingestion. Refused once a plan is installed.
    pub fn install(&self, plan: InstallablePlan) -> Result<PlanId, InstallError> {
        let mut installed = self.installed.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(id) = *installed {
            return Err(InstallError::new(format!(
                "plan {} is installed, and the data plane installs one plan",
                id.0
            )));
        }
        plan.validate()
            .map_err(|error| InstallError::new(format!("{:#}", anyhow::Error::from(error))))?;
        let plan = InstalledPlan {
            id: PlanId(1),
            plan,
        };
        self.query.install(plan.clone())?;
        self.ingest.install(plan.clone())?;
        self.dispatcher.install(&plan);
        *installed = Some(plan.id);
        Ok(plan.id)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use common::{
        AnswerSource, EngineQueryResult, IngestError, QueryEngineError, QueryRequest, RawDataBatch,
    };
    use query::ExecutorQueryEngine;
    use storage::store::PublishedStore;

    use super::*;
    use crate::test_support;

    /// Records the plans installed into it; refuses every plan when
    /// `refuses`.
    #[derive(Default)]
    struct Fake {
        refuses: bool,
        installed: Mutex<Vec<PlanId>>,
    }

    impl Fake {
        fn refusing() -> Arc<Self> {
            Arc::new(Self {
                refuses: true,
                ..Self::default()
            })
        }

        fn install(&self, plan: InstalledPlan) -> Result<(), InstallError> {
            if self.refuses {
                return Err(InstallError::new("refused"));
            }
            self.installed.lock().unwrap().push(plan.id);
            Ok(())
        }

        fn installed(&self) -> Vec<PlanId> {
            self.installed.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl IngestEngine for Fake {
        fn install(&self, plan: InstalledPlan) -> Result<(), InstallError> {
            Fake::install(self, plan)
        }
        async fn ingest(&self, _batch: RawDataBatch) -> Result<(), IngestError> {
            Ok(())
        }
        fn shutdown(&self) {}
    }

    #[async_trait]
    impl QueryEngine for Fake {
        fn install(&self, plan: InstalledPlan) -> Result<(), InstallError> {
            Fake::install(self, plan)
        }
        async fn execute_batch(
            &self,
            queries: &[(AnswerSource, QueryRequest)],
        ) -> Vec<Result<EngineQueryResult, QueryEngineError>> {
            vec![Ok(EngineQueryResult::NotReady); queries.len()]
        }
    }

    fn dispatcher() -> Arc<QueryDispatcher> {
        let options = query::QueryOptions {
            run_memory_limit_bytes: 1 << 20,
            threads: 1,
            share_one_evaluation_results: true,
            query_time_results_retention_ms: 0,
            query_time_results_max_bytes: 0,
            result_cache: false,
            result_cache_max_bytes: 0,
            view_retention_margin_ms: 0,
            refuse_unexecutable: false,
        };
        let published = Arc::new(PublishedStore::new(Arc::default()));
        let engine = Arc::new(ExecutorQueryEngine::new(options, published));
        Arc::new(QueryDispatcher::new(engine))
    }

    const QUERY: &str = "sum_over_time(data[10s])";

    /// A plan installs into both engines and the dispatcher as plan 1; a
    /// second plan is refused and the first stays.
    #[test]
    fn installs_one_plan_into_both_engines_and_the_dispatcher() {
        let (ingest, query, dispatcher) = (
            Arc::new(Fake::default()),
            Arc::new(Fake::default()),
            dispatcher(),
        );
        let installer = PlanInstaller::new(ingest.clone(), query.clone(), dispatcher.clone());
        let plan = test_support::plan(QUERY, false);
        let root = plan.dag.roots[0];
        assert_eq!(installer.install(plan), Ok(PlanId(1)));
        let other = test_support::plan("count_over_time(data[10s])", false);
        assert!(installer.install(other).is_err());
        assert_eq!(ingest.installed(), [PlanId(1)]);
        assert_eq!(query.installed(), [PlanId(1)]);
        assert_eq!(
            dispatcher.resolve(QUERY),
            Some(AnswerSource::Planned {
                plan: PlanId(1),
                root
            })
        );
        assert_eq!(dispatcher.resolve("count_over_time(data[10s])"), None);
    }

    /// An invalid plan, or one either engine refuses, reaches no later
    /// step: the dispatcher matches no query, and a later plan can still
    /// install.
    #[test]
    fn a_failed_install_leaves_the_plan_as_it_was() {
        let mut invalid = test_support::plan(QUERY, false);
        invalid.query_texts.push("up".into());
        for (ingest, query, plan) in [
            (
                Arc::new(Fake::default()),
                Arc::new(Fake::default()),
                invalid,
            ),
            (
                Arc::new(Fake::default()),
                Fake::refusing(),
                test_support::plan(QUERY, false),
            ),
            (
                Fake::refusing(),
                Arc::new(Fake::default()),
                test_support::plan(QUERY, false),
            ),
        ] {
            let dispatcher = dispatcher();
            let installer = PlanInstaller::new(ingest.clone(), query.clone(), dispatcher.clone());
            assert!(installer.install(plan).is_err());
            assert!(ingest.installed().is_empty());
            assert!(dispatcher.resolve(QUERY).is_none());
            assert!(installer.installed.lock().unwrap().is_none());
        }
    }
}
