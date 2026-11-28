//! [`PlanInstaller`]: installs a plan into the query engine, the ingest
//! engine and the dispatcher; queries see it only after every engine
//! accepted it.

use std::sync::Arc;

use common::{InstallError, InstallablePlan, InstalledPlan, PlanId};
use precompute::engine::PrecomputeEngine;
use query::ExecutorQueryEngine;

use crate::dispatcher::QueryDispatcher;

/// Installs plans. v1 installs one: the plan file at startup.
pub(crate) struct PlanInstaller {
    precompute: Arc<PrecomputeEngine>,
    query: Arc<ExecutorQueryEngine>,
    dispatcher: Arc<QueryDispatcher>,
}

impl PlanInstaller {
    pub(crate) fn new(
        precompute: Arc<PrecomputeEngine>,
        query: Arc<ExecutorQueryEngine>,
        dispatcher: Arc<QueryDispatcher>,
    ) -> Self {
        Self {
            precompute,
            query,
            dispatcher,
        }
    }

    /// Validates `plan`, then installs it into the query engine, the ingest
    /// engine and the dispatcher, in that order, stopping at the first
    /// failure. Queries reach a plan only through the dispatcher, which
    /// gets it last: on any failure the current plan stays the one queries
    /// are answered from (none, in v1). The query engine goes first because
    /// its install only compiles and starts idle workers; the ingest
    /// engine's starts ingestion. v1 installs once: the engines refuse a
    /// second plan (after the ingest engine refused a plan, the query engine
    /// keeps it, unused). A failed install at startup ends the data plane.
    pub(crate) fn install(&self, plan: InstallablePlan) -> Result<PlanId, InstallError> {
        plan.validate()
            .map_err(|error| InstallError::new(format!("{:#}", anyhow::Error::from(error))))?;
        // #935's plan id counts installs; v1's one install is plan 1.
        let plan = InstalledPlan {
            id: PlanId(1),
            plan,
        };
        self.query.install(plan.clone())?;
        self.precompute.install(plan.clone())?;
        self.dispatcher.install(&plan);
        Ok(plan.id)
    }
}

#[cfg(test)]
mod tests {
    use common::AnswerSource;
    use storage::store::PublishedStore;

    use super::*;
    use crate::test_support;
    use control_plane::test_support::one_query_plan;

    const QUERY: &str = "sum_over_time(data[10s])";

    /// The two engines, over one published store, and the dispatcher.
    fn parts() -> (
        Arc<PrecomputeEngine>,
        Arc<ExecutorQueryEngine>,
        Arc<QueryDispatcher>,
    ) {
        let options = test_support::options();
        let published = Arc::new(PublishedStore::new(Arc::default()));
        let precompute = Arc::new(PrecomputeEngine::new(
            options.precompute(),
            None,
            published.clone(),
        ));
        let query = Arc::new(ExecutorQueryEngine::new(options.query(), published));
        let dispatcher = Arc::new(QueryDispatcher::new(query.clone()));
        (precompute, query, dispatcher)
    }

    /// The plan the engine holds, if any.
    fn plan_of(query: &ExecutorQueryEngine) -> Option<PlanId> {
        query.installed().map(|installed| installed.plan())
    }

    /// A plan installs into both engines and the dispatcher as plan 1; a
    /// second plan is refused and the first stays.
    #[test]
    fn installs_one_plan_into_both_engines_and_the_dispatcher() {
        let (precompute, query, dispatcher) = parts();
        let installer = PlanInstaller::new(precompute.clone(), query.clone(), dispatcher.clone());
        let plan = one_query_plan(QUERY, false);
        let root = plan.dag.roots[0];
        assert_eq!(installer.install(plan), Ok(PlanId(1)));
        let other = one_query_plan("count_over_time(data[10s])", false);
        assert!(installer.install(other).is_err());
        assert!(precompute.installed().is_some());
        assert_eq!(plan_of(&query), Some(PlanId(1)));
        assert_eq!(
            dispatcher.resolve(QUERY),
            Some(AnswerSource::Planned {
                plan: PlanId(1),
                root
            })
        );
        assert_eq!(dispatcher.resolve("count_over_time(data[10s])"), None);
    }

    /// An invalid plan, or one either engine refuses (each refuses once it
    /// holds a plan), reaches no later step, and the dispatcher matches no
    /// query.
    #[test]
    fn a_failed_install_leaves_the_plan_as_it_was() {
        let held = || InstalledPlan {
            id: PlanId(7),
            plan: one_query_plan("count_over_time(data[10s])", false),
        };
        let mut invalid = one_query_plan(QUERY, false);
        invalid.query_texts.push("up".into());
        // An invalid plan: neither engine gets it.
        let (precompute, query, dispatcher) = parts();
        let installer = PlanInstaller::new(precompute.clone(), query.clone(), dispatcher.clone());
        assert!(installer.install(invalid).is_err());
        assert!(precompute.installed().is_none());
        assert_eq!(plan_of(&query), None);
        assert!(dispatcher.resolve(QUERY).is_none());
        // The query engine refuses: the ingest engine does not get it.
        let (precompute, query, dispatcher) = parts();
        query.install(held()).unwrap();
        let installer = PlanInstaller::new(precompute.clone(), query.clone(), dispatcher.clone());
        assert!(installer.install(one_query_plan(QUERY, false)).is_err());
        assert!(precompute.installed().is_none());
        assert_eq!(plan_of(&query), Some(PlanId(7)));
        assert!(dispatcher.resolve(QUERY).is_none());
        // The ingest engine refuses: the query engine keeps the plan, unused.
        let (precompute, query, dispatcher) = parts();
        precompute.install(held()).unwrap();
        let installer = PlanInstaller::new(precompute.clone(), query.clone(), dispatcher.clone());
        assert!(installer.install(one_query_plan(QUERY, false)).is_err());
        assert_eq!(plan_of(&query), Some(PlanId(1)));
        assert!(dispatcher.resolve(QUERY).is_none());
    }
}
