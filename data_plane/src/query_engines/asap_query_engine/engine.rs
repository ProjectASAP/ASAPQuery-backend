use std::sync::Arc;
use tracing::debug;

use asap_types::query_requirements::QueryRequirements;
use asap_types::KeyByLabelNames;

#[derive(Clone)]
struct QueryReadinessRequirement {
    materializations: Vec<asap_types::PolicyFingerprint>,
    max_window_ms: u64,
}

fn readiness_requirement(
    entry: &asap_types::query_plan::QueryPlanEntry,
) -> QueryReadinessRequirement {
    let bindings = entry.materialization_bindings();
    let mut materializations = bindings
        .iter()
        .map(|binding| binding.materialization.fingerprint())
        .collect::<Vec<_>>();
    materializations.sort_unstable();
    materializations.dedup();
    QueryReadinessRequirement {
        materializations,
        max_window_ms: bindings
            .iter()
            .map(|binding| binding.window_ms)
            .max()
            .unwrap_or(0),
    }
}

fn complete_window_coverage(
    coverage: Option<(u64, u64)>,
    t0_ms: u64,
    t1_ms: u64,
    window_ms: u64,
) -> bool {
    let Some((coverage_start, coverage_end)) = coverage else {
        return false;
    };
    if window_ms == 0 || coverage_start > coverage_end || t0_ms > t1_ms {
        return false;
    }
    coverage_start.saturating_sub(window_ms) <= t0_ms
        && coverage_end >= t1_ms
        && coverage_end
            .saturating_sub(coverage_start)
            .saturating_add(window_ms)
            >= t1_ms.saturating_sub(t0_ms)
}

#[cfg(test)]
mod readiness_coverage_tests {
    use super::complete_window_coverage;

    #[test]
    fn readiness_requires_complete_and_fresh_window_span() {
        assert!(!complete_window_coverage(None, 100, 400, 100));
        assert!(!complete_window_coverage(Some((200, 300)), 100, 500, 100));
        assert!(!complete_window_coverage(Some((300, 500)), 100, 500, 100));
        assert!(complete_window_coverage(Some((200, 500)), 100, 500, 100));
        assert!(complete_window_coverage(Some((500, 500)), 400, 500, 100));
    }
}

#[cfg(test)]
use crate::storage_engines::types::KeyByLabelValues;
#[cfg(test)]
use crate::AggregateCore;
#[cfg(test)]
use asap_types::Statistic;
#[cfg(test)]
use std::collections::HashMap;

/// Simple query engine for processing PromQL-like queries against precomputed data
pub struct ASAPQueryEngine {
    #[allow(dead_code)]
    prometheus_scrape_interval: u64,
    /// ASAP-tier sketch index. When `Some`, the trait's
    /// `execute` adapter classifies the query's metric/group-by against
    /// the index and short-circuits to `EngineError::CapabilityMiss` when
    /// no ASAP-tier identity covers the request, which the HTTP layer
    /// forwards to the Prometheus fallback. When `None`, the
    /// engine behaves as it did before Phase 5 wire-in (every query
    /// goes through `handle_query`'s legacy path).
    summary_store: Option<Arc<crate::storage_engines::sketch_db::index::SketchStore>>,
    /// Generation-consistent physical snapshot used by the production query
    /// path. The QueryPlan and SummaryCatalog must come from the same snapshot.
    active_physical_plan: Option<crate::storage_engines::types::ActivePhysicalPlanHandle>,
    exact_subquery_endpoint: Option<String>,
    metricsql_exact_subquery_endpoint: Option<String>,
    query_forwarding_policy: crate::query_engines::QueryForwardingPolicy,
    exact_subquery_client: reqwest::Client,
}

impl ASAPQueryEngine {
    pub async fn execute_metricsql_at(
        &self,
        identity: &str,
        now_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        let physical = self.active_physical_plan_snapshot().ok_or_else(|| {
            crate::query_engines::EngineError::capability_miss(
                "query_plan",
                "no active physical plan",
            )
        })?;
        let planned = physical
            .query_plan
            .lookup_canonical(asap_types::query_plan::QueryLanguage::MetricsQl, identity)
            .map_err(|error| {
                crate::query_engines::EngineError::capability_miss("query_plan", error.to_string())
            })?;
        let leaves = self
            .prepare_query_inputs(&physical, planned, &[now_ms])
            .await?;
        let (mut result, mut stats) =
            self.execute_logical_entry(&physical, planned, &leaves, now_ms)?;
        stats.remote_evaluations = leaves.values().map(|leaf| leaf.remote_evaluations).sum();
        stats.remote_rpcs = leaves.values().map(|leaf| leaf.remote_rpcs).sum();
        annotate_logical_execution(&mut result, &stats);
        Ok(result)
    }

    pub async fn execute_metricsql_range(
        &self,
        identity: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        let physical = self.active_physical_plan_snapshot().ok_or_else(|| {
            crate::query_engines::EngineError::capability_miss(
                "query_plan",
                "no active physical plan",
            )
        })?;
        let planned = physical
            .query_plan
            .lookup_canonical(asap_types::query_plan::QueryLanguage::MetricsQl, identity)
            .map_err(|error| {
                crate::query_engines::EngineError::capability_miss("query_plan", error.to_string())
            })?;
        self.execute_logical_range(&physical, planned, start_ms, end_ms, step_ms)
            .await
    }

    /// Construct the query executor. Runtime configuration is read only from
    /// the generation-consistent `RuntimePhysicalPlan` installed separately.
    pub fn new(prometheus_scrape_interval: u64) -> Self {
        Self {
            prometheus_scrape_interval,
            summary_store: None,
            active_physical_plan: None,
            exact_subquery_endpoint: None,
            metricsql_exact_subquery_endpoint: None,
            query_forwarding_policy: crate::query_engines::QueryForwardingPolicy::Enabled,
            exact_subquery_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("HTTP client"),
        }
    }

    pub fn with_exact_subquery_endpoint(mut self, endpoint: String) -> Self {
        self.exact_subquery_endpoint = Some(endpoint);
        self
    }

    pub fn with_metricsql_exact_subquery_endpoint(mut self, endpoint: String) -> Self {
        self.metricsql_exact_subquery_endpoint = Some(endpoint);
        self
    }

    pub fn with_query_forwarding_policy(
        mut self,
        policy: crate::query_engines::QueryForwardingPolicy,
    ) -> Self {
        self.query_forwarding_policy = policy;
        self
    }
    async fn prepare_query_inputs(
        &self,
        physical: &crate::storage_engines::types::RuntimePhysicalPlan,
        entry: &asap_types::query_plan::QueryPlanEntry,
        times: &[u64],
    ) -> Result<super::logical_dag::PreparedLeaves, crate::query_engines::EngineError> {
        super::catalog_resolver::validate_entry(
            physical.summary_catalog.as_deref(),
            entry,
            physical.query_plan.plan_id,
            physical.query_plan.plan_version,
        )?;
        // Candidate-filtered exact cuts have a data dependency: read the
        // installed membership subtree once, then use that vector to build the
        // Prometheus selector. Keeping the result as a prepared leaf also means
        // CandidateTopK reuses the same membership readout during composition.
        let dependencies = super::exact_subqueries::external_dependencies(entry, times)?;
        let mut prepared = super::logical_dag::PreparedLeaves::new();
        let unique_inputs = dependencies
            .into_iter()
            .map(|(_, input, at)| (input, at))
            .collect::<std::collections::BTreeSet<_>>();
        for (input, at) in unique_inputs {
            let evaluation_ms = u64::try_from(at).map_err(|_| {
                crate::query_engines::EngineError::capability_miss(
                    "exact_subquery",
                    "candidate evaluation predates epoch",
                )
            })?;
            let mut subtree = entry.clone();
            subtree.root = input;
            let reachable = subtree.topological_order().map_err(|error| {
                crate::query_engines::EngineError::capability_miss(
                    "installed_logical_dag",
                    error.to_string(),
                )
            })?;
            subtree.nodes.retain(|id, _| reachable.contains(id));
            let (result, _) = self.execute_logical_entry(
                physical,
                &subtree,
                &super::logical_dag::PreparedLeaves::new(),
                evaluation_ms,
            )?;
            prepared.insert(
                (input, at),
                super::logical_dag::PreparedLeaf {
                    value: super::logical_dag::from_result(result)?,
                    remote: false,
                    remote_evaluations: 0,
                    remote_rpcs: 0,
                },
            );
        }
        if !self.query_forwarding_policy.allows_external_queries()
            && entry.nodes.values().any(|node| {
                matches!(
                    node,
                    asap_types::query_plan::QueryPlanNode::ExternalExact { .. }
                )
            })
        {
            debug!(
                language = ?entry.language,
                query_id = %entry.query_id,
                "query forwarding disabled; external exact subquery blocked"
            );
        }
        super::exact_subqueries::prepare_external(
            entry,
            times,
            if self.query_forwarding_policy.allows_external_queries() {
                self.exact_subquery_endpoint.as_deref()
            } else {
                None
            },
            if self.query_forwarding_policy.allows_external_queries() {
                self.metricsql_exact_subquery_endpoint.as_deref()
            } else {
                None
            },
            &self.exact_subquery_client,
            prepared,
        )
        .await
    }

    fn execute_logical_entry(
        &self,
        physical: &crate::storage_engines::types::RuntimePhysicalPlan,
        entry: &asap_types::query_plan::QueryPlanEntry,
        leaves: &super::logical_dag::PreparedLeaves,
        at: u64,
    ) -> Result<
        (
            crate::query_engines::query_result::QueryResult,
            super::logical_dag::ExecutionStats,
        ),
        crate::query_engines::EngineError,
    > {
        use crate::query_engines::EngineError;
        let revision = self
            .summary_store
            .as_ref()
            .map(|index| index.summary_update_revision());
        let result = super::logical_dag::execute_installed(
            entry,
            leaves,
            at,
            |root, evaluation_ms| {
                if let Some(asap_types::query_plan::QueryPlanNode::Logical {
                    operator:
                        asap_types::query_plan::residual::ResidualQueryOperator::CurrentSeries {
                            population,
                            readout,
                        },
                    ..
                }) = entry.nodes.get(&root)
                {
                    let index = self.summary_store.as_ref().ok_or_else(|| {
                        EngineError::capability_miss("current_series", "summary store unavailable")
                    })?;
                    let values = index
                        .current_series
                        .lock()
                        .expect("current-series state poisoned")
                        .read(
                            (
                                physical.query_plan.plan_id,
                                physical.query_plan.plan_version,
                            ),
                            population,
                            readout,
                            evaluation_ms,
                        )
                        .map_err(|error| EngineError::capability_miss("current_series", error))?;
                    use crate::query_engines::query_result::{InstantVectorElement, QueryResult};
                    return Ok(QueryResult::vector(values.into_iter().map(|(labels,value)| {
                        InstantVectorElement::new(crate::storage_engines::types::KeyByLabelValues::new_with_labels(labels.values().cloned().collect()), value)
                            .with_label_keys_override(labels.into_keys().collect())
                    }).collect(), evaluation_ms));
                }
                let mut subtree = entry.clone();
                subtree.root = root;
                let reachable = subtree.topological_order().map_err(|e| {
                    EngineError::capability_miss("installed_logical_dag", e.to_string())
                })?;
                subtree.nodes.retain(|id, _| reachable.contains(id));
                let bindings: Vec<_> = subtree
                    .materialization_bindings()
                    .into_iter()
                    .cloned()
                    .collect();
                let windows: std::collections::BTreeSet<Option<u64>> =
                    bindings.iter().map(|b| b.readout_lookback_ms).collect();
                if windows.len() != 1 || windows.contains(&None) || windows.contains(&Some(0)) {
                    return Err(EngineError::capability_miss(
                        "installed_logical_dag",
                        "bound subtree requires one explicit positive window",
                    ));
                }
                subtree.instant.lookback_ms = windows
                    .first()
                    .copied()
                    .flatten()
                    .expect("explicit semantic lookback checked");
                subtree.instant.full_history = false;
                subtree.instant.cumulative_readout = true;
                let requirement = readiness_requirement(&subtree);
                let index = self.summary_store.as_ref().ok_or_else(|| {
                    EngineError::capability_miss(
                        "installed_logical_dag",
                        "summary store unavailable",
                    )
                })?;
                let (result, t0) = super::live_serve::serve_instant_from_query_plan(
                    index,
                    &subtree,
                    evaluation_ms,
                )
                .map_err(|e| {
                    EngineError::capability_miss(
                        "installed_logical_dag",
                        format!("bound readout failed: {e:?}"),
                    )
                })?;
                let active = self.active_physical_plan.as_ref().ok_or_else(|| {
                    EngineError::capability_miss(
                        "installed_logical_dag",
                        "readiness registry unavailable",
                    )
                })?;
                let plan_id = physical.plan_id();
                let version = physical.plan_version();
                // Exact range accumulators preserve their actual first/last sample
                // timestamps. Sparse counter series may legitimately begin after
                // the range boundary; Prometheus evaluates the samples that exist.
                // The physical plan's retention bound guarantees stored panes were
                // not evicted, so requiring a sample at t0 would reject valid data.
                let exact_accumulator_bindings =
                    physical.summary_catalog.as_deref().is_some_and(|catalog| {
                        bindings.iter().all(|binding| {
                            super::catalog_resolver::resolve(catalog, binding.materialization)
                                .is_ok_and(|resolved| resolved.is_exact())
                        })
                    });
                let sparse_exact_coverage = exact_accumulator_bindings
                    && result
                        .coverage
                        .is_some_and(|(_, coverage_end)| coverage_end >= evaluation_ms);
                if !sparse_exact_coverage
                    && !complete_window_coverage(
                        result.coverage,
                        t0,
                        evaluation_ms,
                        requirement.max_window_ms,
                    )
                {
                    active.mark_materializing(
                        plan_id,
                        version,
                        &requirement.materializations,
                        result.coverage,
                    );
                    return Err(EngineError::capability_miss(
                        "installed_logical_dag",
                        format!("bound readout incomplete at {evaluation_ms}"),
                    ));
                }
                let coverage = result.coverage.expect("coverage checked");
                if !active.mark_ready(plan_id, version, &requirement.materializations, coverage)
                    || !active.mark_serving(
                        plan_id,
                        version,
                        &requirement.materializations,
                        coverage,
                    )
                {
                    return Err(EngineError::capability_miss(
                        "installed_logical_dag",
                        "physical generation changed during bound readout",
                    ));
                }
                Ok(asap_tier_result_to_query_result(
                    result,
                    evaluation_ms,
                    false,
                ))
            },
        );
        let current = self
            .summary_store
            .as_ref()
            .map(|index| index.summary_update_revision());
        if match (revision, current) {
            (Some(before), Some(after)) => !before.matches(after),
            (None, None) => false,
            _ => true,
        } {
            return Err(EngineError::capability_miss(
                "installed_logical_dag",
                "summary input changed during query DAG evaluation",
            ));
        }
        result
    }

    async fn execute_logical_range(
        &self,
        physical: &crate::storage_engines::types::RuntimePhysicalPlan,
        entry: &asap_types::query_plan::QueryPlanEntry,
        start: u64,
        end: u64,
        step: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        use crate::query_engines::{
            query_result::{QueryResult, RangeVectorElement},
            EngineError,
        };
        if step == 0 || start > end || (end - start) / step >= 11_000 {
            return Err(EngineError::capability_miss(
                "installed_logical_dag",
                "invalid range or more than 11000 evaluations",
            ));
        }
        let times: Vec<u64> = (0..=(end - start) / step)
            .map(|n| start + n * step)
            .collect();
        let leaves = self.prepare_query_inputs(physical, entry, &times).await?;
        let mut series =
            std::collections::BTreeMap::<Vec<(String, String)>, RangeVectorElement>::new();
        let mut total = super::logical_dag::ExecutionStats::default();
        let mut at = start;
        loop {
            let (result, stats) = self.execute_logical_entry(physical, entry, &leaves, at)?;
            total.raw_scan_evaluations += stats.raw_scan_evaluations;
            total.summary_readout_evaluations += stats.summary_readout_evaluations;
            total.memo_hits += stats.memo_hits;
            total.remote_evaluations += stats.remote_evaluations;
            total.remote_rpcs += stats.remote_rpcs;
            total.remote_branch_evaluations += stats.remote_branch_evaluations;
            let QueryResult::Vector(result) = result else {
                return Err(EngineError::capability_miss(
                    "installed_logical_dag",
                    "range step requires vector",
                ));
            };
            for point in result.values {
                let keys = point.label_keys_override.ok_or_else(|| {
                    EngineError::capability_miss(
                        "installed_logical_dag",
                        "missing range label identity",
                    )
                })?;
                let identity = keys
                    .iter()
                    .cloned()
                    .zip(point.labels.labels.iter().cloned())
                    .collect();
                series
                    .entry(identity)
                    .or_insert_with(|| {
                        RangeVectorElement::new(point.labels).with_label_keys_override(keys)
                    })
                    .add_sample(at, point.value);
            }
            let Some(next) = at.checked_add(step) else {
                break;
            };
            if next > end {
                break;
            }
            at = next;
        }
        let mut result = QueryResult::matrix(series.into_values().collect());
        total.remote_evaluations = leaves.values().map(|leaf| leaf.remote_evaluations).sum();
        total.remote_rpcs = leaves.values().map(|leaf| leaf.remote_rpcs).sum();
        annotate_logical_execution(&mut result, &total);
        Ok(result)
    }

    pub fn with_active_physical_plan(
        mut self,
        handle: crate::storage_engines::types::ActivePhysicalPlanHandle,
    ) -> Self {
        self.active_physical_plan = Some(handle);
        self
    }

    fn active_physical_plan_snapshot(
        &self,
    ) -> Option<Arc<crate::storage_engines::types::RuntimePhysicalPlan>> {
        self.active_physical_plan
            .as_ref()
            .map(|handle| handle.active_snapshot())
            .filter(|plan| plan.plan_id() != 0)
    }

    /// attach the shared `SketchStore` so the `QueryEngine`
    /// trait adapter's classify+failover logic is active. Without this
    /// call, the engine keeps the pre-Phase-5 behavior (route every
    /// query through `handle_query`).
    pub fn with_sketch_index(
        mut self,
        index: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> Self {
        self.summary_store = Some(index);
        self
    }

    /// Build a minimal `QueryRequirements` from a bare PromQL string —
    /// used by the no-sketch-index miss branch in modern `execute()`,
    /// where we don't have a parsed candidate (analysis was skipped)
    /// but still want to fire the capability-miss notify so the
    /// control-plane feedback loop closes. Lifts (metric_name,
    /// group_by_keys) from the AST via a light walker; returns `None`
    /// for queries that don't reference a concrete metric.
    fn requirements_from_query_str(query: &str) -> Option<QueryRequirements> {
        use promql_parser::parser::Expr;
        let ast = promql_parser::parser::parse(query).ok()?;
        fn walk(expr: &Expr) -> Option<(String, std::collections::BTreeSet<String>)> {
            match expr {
                Expr::VectorSelector(vs) => {
                    let mut keys = std::collections::BTreeSet::new();
                    let mut metric = vs.name.clone().unwrap_or_default();
                    for m in &vs.matchers.matchers {
                        if m.name == "__name__" {
                            if metric.is_empty() {
                                metric = m.value.clone();
                            }
                            continue;
                        }
                        keys.insert(m.name.clone());
                    }
                    if metric.is_empty() {
                        None
                    } else {
                        Some((metric, keys))
                    }
                }
                Expr::MatrixSelector(ms) => walk(&Expr::VectorSelector(ms.vs.clone())),
                Expr::Call(call) => call.args.args.iter().find_map(|a| walk(a)),
                Expr::Aggregate(agg) => walk(&agg.expr),
                Expr::Binary(bin) => walk(&bin.lhs).or_else(|| walk(&bin.rhs)),
                Expr::Subquery(sq) => walk(&sq.expr),
                Expr::Paren(p) => walk(&p.expr),
                Expr::Unary(u) => walk(&u.expr),
                _ => None,
            }
        }
        let (metric, keys) = walk(&ast)?;
        Some(QueryRequirements {
            metric,
            statistics: Vec::new(),
            data_range_ms: None,
            grouping_labels: KeyByLabelNames::new(keys.into_iter().collect()),
            spatial_filter_normalized: String::new(),
        })
    }

    #[cfg(test)]
    fn query_precompute_for_statistic(
        &self,
        precompute: &dyn AggregateCore,
        statistic: &Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Phase 1b of the sketch DB design
        // (docs/design_docs/summary-storage.md):
        // for single-subpopulation queries on additive statistics
        // (Count / Sum / Min / Max), serve from the typed aux
        // columns without deserialising the sketch payload.
        //
        // Keyed queries (`key.is_some()`) still need the full
        // `query_statistic` path — aux is per-accumulator, not
        // per-subpopulation key.
        //
        // `try_answer` returns `None` when the statistic isn't
        // covered by aux (Quantile / Cardinality / TopK / Increase /
        // Rate) or when the accumulator doesn't track the requested
        // aux field; both cases fall through to the existing path
        // so the query result is semantically identical.
        if key.is_none() {
            if let Some(value) = precompute.aux_stats().try_answer(*statistic) {
                return Ok(value);
            }
        }
        precompute.query_statistic(*statistic, key, query_kwargs)
    }

    /// Modern warm-tier path for `/api/v1/query_range` — the range-
    /// query equivalent of the `QueryEngine::execute(&str)` trait
    /// surface. Used by the HTTP server as a fallback when the legacy
    /// `handle_range_query_promql` returns `None`.
    ///
    /// Time semantics follow Prometheus's
    /// `/api/v1/query_range?start&end&step` contract: the immutable
    /// QueryPlan is evaluated independently at `start + n*step`, and
    /// the result contains exactly those timestamps. Native summary
    /// pane boundaries are an internal detail and never become API steps.
    pub async fn execute_range_promql_modern(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        if let Some(physical) = self.active_physical_plan_snapshot() {
            if let Ok(entry) = physical.query_plan.lookup(query) {
                if entry.nodes.values().any(|node| {
                    matches!(node, asap_types::query_plan::QueryPlanNode::Logical { .. })
                }) {
                    return self
                        .execute_logical_range(&physical, entry, start_ms, end_ms, step_ms)
                        .await;
                }
            }
        }
        let Some(idx) = self.summary_store.as_ref() else {
            return Err(crate::query_engines::EngineError::capability_miss(
                crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                format!("ASAPQueryEngine: no sketch index for `{query}` — failing over"),
            ));
        };

        let physical_plan = self.active_physical_plan_snapshot();
        let mut readiness = None;
        let planned = match physical_plan.as_ref() {
            Some(physical_plan) => match physical_plan.query_plan.lookup(query) {
                Ok(query_entry) => {
                    super::catalog_resolver::validate_entry(
                        physical_plan.summary_catalog.as_deref(), query_entry,
                        physical_plan.query_plan.plan_id, physical_plan.query_plan.plan_version,
                    )?;
                    readiness = Some((
                        physical_plan.plan_id(),
                        physical_plan.plan_version(),
                        readiness_requirement(query_entry),
                    ));
                    crate::query_engines::asap_query_engine::live_serve::serve_range_steps_from_query_plan(
                        idx,
                        query_entry,
                        start_ms,
                        end_ms,
                        step_ms,
                    )
                }
                Err(reason) => Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::QueryNotPlanned(reason.to_string())),
            },

            None => Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::QueryNotPlanned(
                "no active physical QueryPlan".into(),
            )),
        };
        let result = planned.and_then(|result| {
            if let Some((plan_id, plan_version, requirement)) = readiness.as_ref() {
                let complete = !requirement.materializations.is_empty()
                    && complete_window_coverage(
                        result.coverage,
                        start_ms,
                        end_ms,
                        requirement.max_window_ms,
                    );
                let Some(active) = self.active_physical_plan.as_ref() else {
                    return Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::MaterializationNotReady(
                        "physical readiness registry is unavailable".into(),
                    ));
                };
                if !complete {
                    active.mark_materializing(
                        *plan_id,
                        *plan_version,
                        &requirement.materializations,
                        result.coverage,
                    );
                    return Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::MaterializationNotReady(
                        format!("coverage {:?} does not completely and freshly cover [{start_ms}, {end_ms}]", result.coverage),
                    ));
                }
                let coverage = result.coverage.expect("complete coverage checked above");
                if !active.mark_ready(
                    *plan_id,
                    *plan_version,
                    &requirement.materializations,
                    coverage,
                ) || !active.mark_serving(
                    *plan_id,
                    *plan_version,
                    &requirement.materializations,
                    coverage,
                ) {
                    return Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::MaterializationNotReady(
                        "physical generation changed while checking readiness".into(),
                    ));
                }
            }
            Ok(result)
        }).map_err(|reason| {
            if let Some(req) = Self::requirements_from_query_str(query) {
            }
            crate::query_engines::EngineError::capability_miss(
                crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                format!(
                    "installed QueryPlan resolver could not serve `{query}` over \
                     [{start_ms}, {end_ms}]: {reason:?} — failing over to archive"
                ),
            )
        })?;

        // Matrix shape — the range_query wire format requires it.
        let warm_qr = asap_tier_result_to_query_result(result.clone(), end_ms, true);

        // Complete coverage is a prerequisite above. Hybrid stitching is
        // retained for legacy/test callers without an active QueryPlan only.
        Ok(warm_qr)
    }
}

// ---------------------------------------------------------------------------
// `QueryEngine` trait impl.
//
// Adapter only — does NOT change `handle_query` or any other existing
// surface. The trait's `execute(&str)` walks the same `handle_query` code
// path the binary's HTTP driver uses today; `None` (capability miss) is
// translated to `EngineError::CapabilityMiss` so the router can fall through
// to the next compatible backend.
// ---------------------------------------------------------------------------

/// Adapt a [`crate::storage_engines::sketch_db::query::ASAPTierResult`] to the engine's
/// existing `QueryResult` shape. The reducer hands back per-series
/// time-stamped scalars; we materialize them as a
/// `QueryResult::Matrix` whose [`crate::query_engines::query_result::RangeVectorElement`]s
/// each map onto one (label-values, samples) entry.
///
/// `now_ms` is unused for the matrix variant (each sample carries its
/// own window-end timestamp); it's plumbed for future extension to
/// the instant-vector case (latest-pane projection).
fn annotate_logical_execution(
    result: &mut crate::query_engines::query_result::QueryResult,
    stats: &super::logical_dag::ExecutionStats,
) {
    use crate::query_engines::query_result::QueryResult;
    let warnings = match result {
        QueryResult::Vector(v) => &mut v.warnings,
        QueryResult::Matrix(m) => &mut m.warnings,
    };
    if stats.remote_evaluations > 0 {
        warnings.push(
            if stats.summary_readout_evaluations > 0 {
                "asap_execution:hybrid"
            } else {
                "asap_execution:exact_dag"
            }
            .into(),
        );
    }
    warnings.push(format!(
        "asap_logical_stats:raw={},summary={},memo_hits={},remote={},remote_rpcs={},remote_branches={}",
        stats.raw_scan_evaluations,
        stats.summary_readout_evaluations,
        stats.memo_hits,
        stats.remote_evaluations,
        stats.remote_rpcs,
        stats.remote_branch_evaluations
    ));
}

fn asap_tier_result_to_query_result(
    result: crate::storage_engines::sketch_db::query::ASAPTierResult,
    now_ms: u64,
    is_range_query: bool,
) -> crate::query_engines::query_result::QueryResult {
    use crate::query_engines::query_result::{
        InstantVectorElement, QueryResult, RangeVectorElement,
    };
    use crate::storage_engines::types::KeyByLabelValues;

    // Instant-query result-shape: the Prometheus adapter's
    // `format_success_response` rejects `Matrix` for queries the
    // request identifies as instant — produces a
    // 500 ”shape mismatch”. Project the per-series last sample into
    // an `InstantVectorElement` and wrap as `Vector` so the wire
    // response carries `resultType: vector` matching the request.
    if !is_range_query {
        let mut elements: Vec<InstantVectorElement> = Vec::with_capacity(result.series.len());
        for (label_values, samples) in result.series {
            // Mirror the range-vector branch: BTreeMap iteration is
            // key-sorted, so `unzip` produces aligned (keys, values).
            // Stash the keys in the per-element `label_keys_override`
            // so the Prometheus adapter renders synthesized keys
            // (notably ASAP-tier `topk`'s `"item"` key) instead of
            // the empty `metric: {}` it would produce when the
            // query-scoped `KeyByLabelNames` is empty.
            let (keys, values): (Vec<String>, Vec<String>) = label_values.into_iter().unzip();
            let labels = KeyByLabelValues::new_with_labels(values);
            // Take the latest sample (the reducer returns one per
            // window_end; for instant readout we want the most recent).
            if let Some((_, value)) = samples.into_iter().last() {
                elements
                    .push(InstantVectorElement::new(labels, value).with_label_keys_override(keys));
            }
        }
        return QueryResult::vector(elements, now_ms);
    }

    let mut elements: Vec<RangeVectorElement> = Vec::with_capacity(result.series.len());
    for (label_values, samples) in result.series {
        // `KeyByLabelValues` is a `Vec<String>` carrying VALUES only;
        // the serializer pairs them with KEYS from a query-scoped
        // `KeyByLabelNames`. For most queries the keys ARE the
        // query's group-by clause, so the default path works. But
        // ASAP-tier `topk` synthesizes an `"item"` key (the top-k
        // entry name) that the original query's group-by doesn't
        // carry — without an override the serializer drops it and
        // the response shows `"metric": {}`. Project the BTreeMap's
        // VALUES in key-sorted order (BTreeMap iteration is
        // key-sorted), and stash the BTreeMap's KEYS in the
        // per-element override so the serializer can pair them
        // correctly.
        let (keys, values): (Vec<String>, Vec<String>) = label_values.into_iter().unzip();
        let labels = KeyByLabelValues::new_with_labels(values);
        let mut element = RangeVectorElement::new(labels).with_label_keys_override(keys);
        for (window_end_ms, value) in samples {
            // `window_end_ms` is i64 from the index; cast to u64
            // for the wire format (window_end is monotonic + post-
            // 1970 in production).
            let ts = if window_end_ms >= 0 {
                window_end_ms as u64
            } else {
                0
            };
            element.add_sample(ts, value);
        }
        elements.push(element);
    }
    QueryResult::matrix(elements)
}

#[async_trait::async_trait]
impl crate::query_engines::routing::query_engine_routing::QueryEngine for ASAPQueryEngine {
    async fn execute(
        &self,
        query: &str,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.execute_at(query, now_ms).await
    }

    async fn execute_at(
        &self,
        query: &str,
        now_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        if let Some(physical) = self.active_physical_plan_snapshot() {
            if let Ok(entry) = physical.query_plan.lookup(query) {
                let leaves = self
                    .prepare_query_inputs(&physical, entry, &[now_ms])
                    .await
                    .map_err(|error| {
                        tracing::warn!(query, error = %error, "installed query DAG preparation failed");
                        error
                    })?;
                let (mut result, mut stats) = self
                    .execute_logical_entry(&physical, entry, &leaves, now_ms)
                    .map_err(|error| {
                        tracing::warn!(query, error = %error, "installed query DAG execution failed");
                        error
                    })?;
                stats.remote_evaluations =
                    leaves.values().map(|leaf| leaf.remote_evaluations).sum();
                stats.remote_rpcs = leaves.values().map(|leaf| leaf.remote_rpcs).sum();
                annotate_logical_execution(&mut result, &stats);
                return Ok(result);
            }
        }
        // One authoritative warm path: ASAPPlanner post-ASAP DAG →
        // SummaryCatalog/materialization resolver → SID lookup → DAG executor.
        // A typed resolver/executor error becomes CapabilityMiss, which lets
        // EngineRouter continue to the archive backend.
        if let Some(idx) = self.summary_store.as_ref() {
            let physical_plan = self.active_physical_plan_snapshot();
            let mut readiness = None;
            let planned = match physical_plan.as_ref() {
                Some(physical_plan) => match physical_plan.query_plan.lookup(query) {
                    Ok(query_entry) => {
                        readiness = Some((
                            physical_plan.plan_id(),
                            physical_plan.plan_version(),
                            readiness_requirement(query_entry),
                        ));
                        crate::query_engines::asap_query_engine::live_serve::serve_instant_from_query_plan(
                            idx, query_entry, now_ms,
                        )
                    },
                    Err(reason) => Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::QueryNotPlanned(reason.to_string())),
                },

                None => Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::QueryNotPlanned(
                    "no active physical QueryPlan".into(),
                )),
            };
            let (result, t0_ms) = planned.and_then(|(result, t0_ms)| {
                if let Some((plan_id, plan_version, requirement)) = readiness.as_ref() {
                    let complete = !requirement.materializations.is_empty()
                        && complete_window_coverage(
                            result.coverage,
                            t0_ms,
                            now_ms,
                            requirement.max_window_ms,
                        );
                    let Some(active) = self.active_physical_plan.as_ref() else {
                        return Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::MaterializationNotReady(
                            "physical readiness registry is unavailable".into(),
                        ));
                    };
                    if !complete {
                        active.mark_materializing(
                            *plan_id,
                            *plan_version,
                            &requirement.materializations,
                            result.coverage,
                        );
                        return Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::MaterializationNotReady(
                            format!("coverage {:?} does not completely and freshly cover [{t0_ms}, {now_ms}]", result.coverage),
                        ));
                    }
                    let coverage = result.coverage.expect("complete coverage checked above");
                    if !active.mark_ready(
                        *plan_id,
                        *plan_version,
                        &requirement.materializations,
                        coverage,
                    ) || !active.mark_serving(
                        *plan_id,
                        *plan_version,
                        &requirement.materializations,
                        coverage,
                    ) {
                        return Err(crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip::MaterializationNotReady(
                            "physical generation changed while checking readiness".into(),
                        ));
                    }
                }
                Ok((result, t0_ms))
            }).map_err(|reason| {
                    if let Some(req) = Self::requirements_from_query_str(query) {
                    }
                    crate::query_engines::EngineError::capability_miss(
                        crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                        format!(
                            "installed QueryPlan resolver could not serve `{query}`: \n                             {reason:?} — failing over to archive"
                        ),
                    )
                })?;

            let warm_qr = asap_tier_result_to_query_result(result.clone(), now_ms, false);
            return Ok(warm_qr);
        }

        // Without a sketch index, notify the control plane directly on a capability
        // miss so the feedback loop also works for this configuration.
        if let Some(req) = Self::requirements_from_query_str(query) {}
        Err(crate::query_engines::EngineError::capability_miss(
            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
            format!("ASAPQueryEngine: no sketch index for `{query}` — failing over to archive"),
        ))
    }

    /// Range-query entry point for the [`EngineRouter`] failover loop.
    ///
    /// Delegates to the inherent `execute_range_promql_modern`, which
    /// runs the ASAP-tier reducer over `[start_ms, end_ms]` and returns
    /// a `matrix` result. Without this override the router would hit the
    /// trait default (`CapabilityMiss`) and never reach the ASAP-tier
    /// range path, so every range query would fall straight through to
    /// the archive even when the warm sketches can answer it.
    async fn execute_range(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        self.execute_range_promql_modern(query, start_ms, end_ms, step_ms)
            .await
    }

    fn capabilities(
        &self,
    ) -> crate::query_engines::routing::query_engine_routing::EngineCapabilities {
        crate::query_engines::routing::query_engine_routing::EngineCapabilities {
            data_source_id: crate::storage_engines::types::StorageBackend::SketchStore
                .data_source_id(),
            storage_backend: crate::storage_engines::types::StorageBackend::SketchStore,
            // Warm-tier sketches are O(sketch-size); call it 16 MiB ceiling
            // for buffered ops (KLL with k=200 is well below this).
            supports_streams_above_bytes: 16 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod sketch_query_tests {
    // use crate::storage_engines::types::{CleanupPolicy, StreamingConfig};
    // use crate::query_engines::asap_query_engine::engine::ASAPQueryEngine;
    // use crate::storage_engines::promsketch_store::PromSketchStore;
    // use crate::storage_engines::TimestampedBucketsMap;
    // use std::collections::HashMap;
    // use std::sync::Arc;

    // /// Minimal no-op store — sketch queries bypass the store entirely
    // struct NoOpStore;

    // impl Store for NoOpStore {
    //     fn query_precomputed_output(
    //         &self,
    //         _: &str,
    //         _: u64,
    //         _: u64,
    //         _: u64,
    //     ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn query_precomputed_output_exact(
    //         &self,
    //         _: &str,
    //         _: u64,
    //         _: u64,
    //         _: u64,
    //     ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output(
    //         &self,
    //         _: crate::storage_engines::types::PrecomputedOutput,
    //         _: Box<dyn crate::storage_engines::types::AggregateCore>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output_batch(
    //         &self,
    //         _: Vec<(
    //             crate::storage_engines::types::PrecomputedOutput,
    //             Box<dyn crate::storage_engines::types::AggregateCore>,
    //         )>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn get_earliest_timestamp_per_aggregation_id(
    //         &self,
    //     ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
    //         Ok(HashMap::new())
    //     }
    //     fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         Ok(())
    //     }
    // }

    // /// Helper: create an engine with a populated PromSketchStore.
    // /// Inserts data points 1..=100 into a series with labels = `series_key`.
    // fn engine_with_sketch_data(series_key: &str) -> ASAPQueryEngine {
    //     let ps = Arc::new(PromSketchStore::with_default_config());
    //     ps.ensure_all_sketches(series_key).unwrap();
    //     for i in 1..=100u64 {
    //         ps.sketch_insert(series_key, i, i as f64).unwrap();
    //     }

    //     let inference_config =
    //         InferenceConfig::new(::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());

    //     ASAPQueryEngine::new(
    //         Arc::new(NoOpStore),
    //         Some(ps),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         ::promql,
    //     )
    // }

    // // ---- Instant query tests ----

    // #[test]
    // fn test_sketch_instant_entropy_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Query at time 0.1s (= 100ms) with a 100ms range
    //     let result = engine.handle_query_promql("entropy_over_time(mymetric[100s])".into(), 0.1);
    //     assert!(result.is_some(), "entropy_over_time should return a result");
    //     let (labels, qr) = result.unwrap();
    //     assert!(!labels.labels.is_empty());
    //     if let crate::query_engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty(), "should have at least one result");
    //         let val = iv.values[0].value;
    //         assert!(val >= 0.0, "entropy should be non-negative, got {}", val);
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_quantile_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     let result =
    //         engine.handle_query_promql("quantile_over_time(0.5, mymetric[100s])".into(), 0.1);
    //     assert!(
    //         result.is_some(),
    //         "quantile_over_time should return a result"
    //     );
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::query_engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty());
    //         let val = iv.values[0].value;
    //         // Median of 1..100 should be roughly 50
    //         assert!(
    //             val > 20.0 && val < 80.0,
    //             "median should be roughly 50, got {}",
    //             val
    //         );
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_avg_over_time() {
    //     let engine = engine_with_sketch_data("cpu");
    //     let result = engine.handle_query_promql("avg_over_time(cpu[100s])".into(), 0.1);
    //     assert!(result.is_some(), "avg_over_time should return a result");
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::query_engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty());
    //         let val = iv.values[0].value;
    //         // avg of 1..100 = 50.5
    //         assert!(val > 30.0 && val < 70.0, "avg should be ~50.5, got {}", val);
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_without_store() {
    //     // Engine with promsketch_store = None
    //     let inference_config =
    //         InferenceConfig::new(::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = ASAPQueryEngine::new(
    //         Arc::new(NoOpStore),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         ::promql,
    //     );
    //     // Sketch function should fall through (return None) without panicking
    //     let result = engine.handle_sketch_query_promql("entropy_over_time(metric[5m])", 100.0);
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_for_non_sketch_function() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // "rate" is not sketch-backed, so should return None from sketch path
    //     let result = engine.handle_sketch_query_promql("rate(mymetric[100s])", 0.1);
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_for_missing_series() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Query a metric that doesn't exist in the sketch store
    //     let result = engine.handle_sketch_query_promql("entropy_over_time(nonexistent[100s])", 0.1);
    //     assert!(result.is_none());
    // }

    // ---- Range query tests ----

    // #[test]
    // fn test_sketch_range_entropy_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Range query: start=0.01, end=0.1 (10ms to 100ms), step=0.01 (10ms)
    //     // with a 50ms window [50s range]
    //     let result = engine.handle_range_query_promql(
    //         "entropy_over_time(mymetric[50s])".into(),
    //         0.01,
    //         0.1,
    //         0.01,
    //     );
    //     assert!(
    //         result.is_some(),
    //         "sketch range query should return a result"
    //     );
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::query_engines::query_result::QueryResult::Matrix(rv) = qr {
    //         assert!(!rv.values.is_empty(), "should have at least one series");
    //         let samples = &rv.values[0].samples;
    //         assert!(
    //             samples.len() > 1,
    //             "range query should produce multiple samples, got {}",
    //             samples.len()
    //         );
    //         for sample in samples {
    //             assert!(
    //                 sample.value >= 0.0,
    //                 "entropy should be non-negative, got {}",
    //                 sample.value
    //             );
    //         }
    //     } else {
    //         panic!("expected Matrix result");
    //     }
    // }

    // #[test]
    // fn test_sketch_range_returns_none_without_store() {
    //     let inference_config =
    //         InferenceConfig::new(::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = ASAPQueryEngine::new(
    //         Arc::new(NoOpStore),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         ::promql,
    //     );
    //     let result = engine.handle_sketch_range_query_promql(
    //         "entropy_over_time(metric[5m])",
    //         0.0,
    //         100.0,
    //         10.0,
    //     );
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_range_returns_none_for_non_sketch_function() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     let result =
    //         engine.handle_sketch_range_query_promql("rate(mymetric[100s])", 0.01, 0.1, 0.01);
    //     assert!(result.is_none());
    // }
}

// Typed auxiliary statistics must answer covered queries without invoking
// the accumulator query method; uncovered statistics use that method.
#[cfg(test)]
mod aux_pushdown_tests {
    use super::*;
    use crate::precompute_engine::operators::{
        max_accumulator::MaxAccumulator, min_accumulator::MinAccumulator,
        sum_accumulator::SumAccumulator,
    };
    use crate::storage_engines::types::AggregationType;
    use asap_types::Statistic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Accumulator that records how many times `query_statistic`
    /// was invoked. Used to verify the aux fast path skips it.
    struct SpyAccumulator {
        inner_sum: f64,
        query_calls: Arc<AtomicUsize>,
    }

    impl crate::storage_engines::types::SerializableToSink for SpyAccumulator {
        fn serialize_to_bytes(&self) -> Vec<u8> {
            Vec::new()
        }
        fn serialize_to_json(&self) -> serde_json::Value {
            serde_json::Value::Null
        }
    }

    impl AggregateCore for SpyAccumulator {
        fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
            Box::new(SpyAccumulator {
                inner_sum: self.inner_sum,
                query_calls: self.query_calls.clone(),
            })
        }
        fn type_name(&self) -> &'static str {
            "SpyAccumulator"
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
        fn merge_with(
            &self,
            _other: &dyn AggregateCore,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            unimplemented!()
        }
        fn get_accumulator_type(&self) -> AggregationType {
            AggregationType::Sum
        }
        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }
        fn query_statistic(
            &self,
            _statistic: Statistic,
            _key: &Option<KeyByLabelValues>,
            _query_kwargs: &HashMap<String, String>,
        ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            self.query_calls.fetch_add(1, Ordering::Relaxed);
            Ok(-1.0) // sentinel: fast path should not return this
        }
        fn aux_stats(&self) -> crate::storage_engines::types::AuxStats {
            crate::storage_engines::types::AuxStats {
                sum: Some(self.inner_sum),
                ..crate::storage_engines::types::AuxStats::empty()
            }
        }
    }

    fn make_engine() -> ASAPQueryEngine {
        use crate::storage_engines::types::{
            CleanupPolicy, StreamingConfig, StreamingConfigHandle,
        };

        let sc = Arc::new(StreamingConfig::new(HashMap::new()));
        let hr = StreamingConfigHandle::from_arc(sc.clone());
        let _ = sc;
        ASAPQueryEngine::new(60)
    }

    #[test]
    fn aux_covered_stat_skips_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone(),
        };
        let result = engine
            .query_precompute_for_statistic(&spy, &Statistic::Sum, &None, &HashMap::new())
            .expect("query ok");
        assert_eq!(result, 42.0, "aux fast path should return aux value");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "query_statistic should NOT be called when aux covers the stat"
        );
    }

    #[test]
    fn aux_uncovered_stat_falls_through_to_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone(),
        };
        // Quantile is not covered by aux → must fall through.
        let result = engine
            .query_precompute_for_statistic(&spy, &Statistic::Quantile, &None, &HashMap::new())
            .expect("query ok");
        assert_eq!(
            result, -1.0,
            "should have returned query_statistic's sentinel"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "query_statistic should be called exactly once when aux misses"
        );
    }

    #[test]
    fn keyed_queries_always_use_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone(),
        };
        let key = Some(KeyByLabelValues::new());
        // Even for Sum (which aux covers), a keyed query must bypass aux
        // — aux is per-accumulator, not per-subpopulation key.
        let result = engine
            .query_precompute_for_statistic(&spy, &Statistic::Sum, &key, &HashMap::new())
            .expect("query ok");
        assert_eq!(result, -1.0);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "keyed queries must skip aux fast path"
        );
    }

    #[test]
    fn real_sum_accumulator_uses_aux_fast_path() {
        // End-to-end: a real SumAccumulator goes through the fast path
        // and returns its sum without ever hitting query_statistic.
        let engine = make_engine();
        let acc = SumAccumulator::with_sum(7.5);
        let result = engine
            .query_precompute_for_statistic(&acc, &Statistic::Sum, &None, &HashMap::new())
            .expect("query ok");
        assert_eq!(result, 7.5);
    }

    #[test]
    fn real_min_max_accumulator_uses_aux_fast_path() {
        let engine = make_engine();
        let min_acc = MinAccumulator::with_value(3.0);
        let max_acc = MaxAccumulator::with_value(99.0);
        assert_eq!(
            engine
                .query_precompute_for_statistic(&min_acc, &Statistic::Min, &None, &HashMap::new())
                .unwrap(),
            3.0
        );
        assert_eq!(
            engine
                .query_precompute_for_statistic(&max_acc, &Statistic::Max, &None, &HashMap::new())
                .unwrap(),
            99.0
        );
    }
}

// ── build_query_execution_context_promql_for_agg_id (forced-agg) tests ──

// ===========================================================================
// HLL count() — capability matching + accumulator query round-trip.
//
// Pins that the warm engine answers `count(metric)` from an HLL-backed
// aggregation: capability matching picks HLL (per
// `compatible_agg_types(Statistic::Count)`), and the HLL accumulator's
// `query_statistic` returns the cardinality estimate. This is the
// runtime contract the wire-side _hll alias resolver above relies on.
// ===========================================================================
// ===========================================================================
// KLL quantile — pin that DatasketchesKLL is in the Quantile capability
// list and the accumulator answers `Statistic::Quantile`. Mirrors the
// HLL-Count contract; closes the wire-side ingest gap diagnosis.
// ===========================================================================
// ===========================================================================
/// Installed plans bind runtime reads and enforce complete coverage.
#[cfg(test)]
mod asap_tier_classify_tests {
    use super::*;
    use crate::query_engines::routing::query_engine_routing::QueryEngine as _;
    use crate::query_engines::EngineError;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchAlgorithm, SketchConfig, SketchSampleState, SketchStore,
        SummarySeriesMetadata,
    };
    use crate::storage_engines::types::{CleanupPolicy, StreamingConfigHandle};
    use std::collections::{BTreeMap, BTreeSet};

    /// `sum by (zone) (http_requests_total)` end-to-end via the
    /// `execute(&str)` adapter. Mirrors the MVP acceptance test's Axis-C
    /// failure: ExactAgg(Sum) sids existed for `http_requests_total`
    /// (one per zone), but the old reducer
    /// returned `UnsupportedFunction("sum")` because
    /// `SketchReducer::evaluate` only knows sketch-backed query
    /// families. This test pins the ExactAgg dispatch branch added
    /// to `execute` so the new path emits per-zone instant-vector
    /// results instead of a CapabilityMiss.
    #[tokio::test]
    async fn execute_sum_by_zone_dispatches_to_exact_agg_reducer() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        // Mirror the acceptance-test setup: four ExactAgg(Sum) sids, one
        // per zone (z0..z3), registered with `group_by_keys=["zone"]`
        // and carrying a `SumAccumulator` per window.
        let zones = ["z0", "z1", "z2", "z3"];
        // Anchor windows so the engine's instant-query default
        // lookback (5 min) reaches them.
        let now_ms = 600_000_u64;
        let window_start = now_ms.saturating_sub(60_000);
        let window_end = now_ms.saturating_sub(30_000);

        for (i, zone) in zones.iter().enumerate() {
            let sid = 9000 + i as u64;
            idx.register(SummarySeriesMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            let value = ((i + 1) * 100) as f64;
            let mut lm = BTreeMap::new();
            lm.insert("zone".to_string(), zone.to_string());
            idx.append_precompute(
                sid,
                lm,
                (window_start, window_end),
                Box::new(SumAccumulator::with_sum(value)),
            );
        }

        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "http_requests_total",
            "Sum",
            serde_json::json!({}),
            &["zone"],
            30000,
        );
        let query = "sum by (zone) (http_requests_total)";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::Reduce(vec!["zone".into()]),
            30000,
            QueryPlanNode::ExactReadout {
                input: QueryNodeId(0),
                readout: ExactReadout::Sum,
            },
        );
        let engine = test_plan::engine(idx, config, vec![9000, 9001, 9002, 9003], entry);
        let result = engine
            .execute_at(query, window_end)
            .await
            .expect("sum by (zone) must dispatch to ExactAgg reducer, not capability-miss");

        // Expect a Vector (instant) result with one entry per zone.
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 4, "one entry per zone");
        // Per-zone values match what each SumAccumulator carries.
        // KeyByLabelValues stores values only; the override carries
        // the corresponding keys.
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for el in &vector.values {
            // The element's label keys override + label values together
            // identify the zone.
            let keys = el
                .label_keys_override
                .as_ref()
                .expect("override populated for ExactAgg path");
            let vals = &el.labels.labels;
            assert_eq!(keys.len(), vals.len());
            let zone_idx = keys
                .iter()
                .position(|k| k == "zone")
                .expect("zone key present");
            by_zone.insert(vals[zone_idx].clone(), el.value);
        }
        assert_eq!(by_zone.get("z0").copied(), Some(100.0));
        assert_eq!(by_zone.get("z1").copied(), Some(200.0));
        assert_eq!(by_zone.get("z2").copied(), Some(300.0));
        assert_eq!(by_zone.get("z3").copied(), Some(400.0));
    }

    // Build a now-anchored KLL `SummarySeriesMetadata` + sample so the
    // engine's instant/range default lookbacks reach it. Mirrors the
    // live MVP workload: the agent emits a bare-named KLL sketch
    // (`http_requests_total_latency_ms`) into the SketchStore.
    fn kll_meta(sid: u64, metric: &str) -> SummarySeriesMetadata {
        // Latest ASAPPlanner sizes an epsilon=0.01 KLL at k=269.
        let cfg = SketchConfig::Kll { k: 269 };
        SummarySeriesMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::Kll))),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::Kll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    fn encode_kll_items_proto(k: u16, items: &[f64]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;
        let state = KllState {
            k: k as u32,
            items: items.to_vec(),
            levels: vec![],
            num_levels: 0,
            ..Default::default()
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
            ..Default::default()
        };
        env.encode_to_vec()
    }

    fn hll_meta(sid: u64, metric: &str) -> SummarySeriesMetadata {
        // Latest ASAPPlanner requires p=14 for a 1% HLL error target.
        let cfg = SketchConfig::Hll { precision: 14 };
        SummarySeriesMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::CardinalityApprox),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::Hll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    fn encode_hll_with_cardinality(precision: u32, distinct: usize) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use asap_sketchlib::{HllSketch, HllVariant};
        use prost::Message;
        let mut sk = HllSketch::new(HllVariant::Regular, precision);
        for i in 0..distinct {
            sk.update(format!("user-{i}").as_bytes());
        }
        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision: sk.precision,
            registers: sk.registers.clone(),
            hip_kxq0: sk.hip_kxq0,
            hip_kxq1: sk.hip_kxq1,
            hip_est: sk.hip_est,
            registers_sparse: None,
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        };
        env.encode_to_vec()
    }

    /// REGRESSION of the HLL `count(metric)` "No result" e2e failure
    /// (`controller_plan_to_query_full_roundtrip_hll`) isolated to the
    /// engine layer. `count(distinct_over_time(unique_users_per_min[w]))`
    /// is the distinct-count idiom (bare `count(v)` is a row count). The Planner DAG represents this as a cardinality readout,
    /// so the executor returns the HLL distinct-count directly. A single
    /// FULL HLL frame (~500 users) is used so
    /// the instant projection reads the real estimate.
    /// Stored sketches cannot authorize a query without an installed QueryPlan.
    #[tokio::test]
    async fn unplanned_queries_fail_closed_even_with_matching_sketches() {
        let idx = Arc::new(SketchStore::new());
        idx.register(hll_meta(7500, "unique_users_per_min"));
        idx.append_sample(
            7500,
            BTreeMap::new(),
            (1_000, 2_000),
            SketchSampleState {
                bytes: encode_hll_with_cardinality(14, 500),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        let engine = ASAPQueryEngine::new(15000).with_sketch_index(idx);
        let query = "count(distinct_over_time(unique_users_per_min[1m]))";
        let instant = engine.execute_at(query, 2_000).await;
        let range = engine
            .execute_range_promql_modern(query, 1_000, 2_000, 1_000)
            .await;
        assert!(
            instant.is_err(),
            "instant execution must require an installed plan"
        );
        assert!(
            range.is_err(),
            "range execution must require an installed plan"
        );
    }

    /// An installed plan authorizes only its exact query identity and still
    /// requires readable materializations before serving.
    #[tokio::test]
    async fn installed_plan_rejects_unknown_queries_and_missing_samples() {
        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let idx = Arc::new(SketchStore::new());
        idx.register(hll_meta(7500, "unique_users_per_min"));
        let config = test_plan::materialization(
            "unique_users_per_min",
            "HLL",
            serde_json::json!({"precision":14}),
            &[],
            1000,
        );
        let query = "count(distinct_over_time(unique_users_per_min[1m]))";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::Reduce(vec![]),
            1000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Cardinality,
            },
        );
        let engine = test_plan::engine(idx, config, vec![7500], entry);
        for query in [query, "count(other_metric)"] {
            assert!(engine.execute_at(query, 2000).await.is_err());
            assert!(engine
                .execute_range_promql_modern(query, 2000, 2000, 1000)
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn execute_count_hll_returns_cardinality_not_rowcount() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7500u64;
        idx.register(hll_meta(sid, "unique_users_per_min"));

        let now_ms = 600_000_u64;
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(3_000), now_ms.saturating_sub(2_000)),
            SketchSampleState {
                bytes: encode_hll_with_cardinality(14, 500),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "unique_users_per_min",
            "HLL",
            serde_json::json!({"precision":14}),
            &[],
            1000,
        );
        let query = "count(distinct_over_time(unique_users_per_min[1m]))";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::Reduce(vec![]),
            1000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Cardinality,
            },
        );
        let engine = test_plan::engine(idx, config, vec![7500], entry);
        let result = engine.execute_at(query, now_ms - 2000).await.expect(
            "count(hll_metric) must dispatch to the Cardinality family \
                 via the candidate capability (empty trace function) and \
                 return the HLL distinct-count, NOT capability-miss",
        );
        assert!(
            result_nonempty(&result),
            "the distinct-count idiom over an HLL sid must return a \
             non-empty cardinality estimate (regression: empty `asap_query` \
             No-result)"
        );
        // The value must be the HLL distinct-count estimate (~500), NOT
        // the outer-Count fold collapsing it to the row-count (1.0).
        let est = match &result {
            crate::query_engines::query_result::QueryResult::Vector(v) => v.values[0].value,
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                m.values[0].samples.last().map(|s| s.value).unwrap_or(0.0)
            }
        };
        assert!(
            est > 100.0,
            "expected the HLL distinct-count estimate (~500), not the \
             row-count fold (1.0); got {est}"
        );
    }

    /// Encode an HLL FULL proto frame over an EXPLICIT set of string items,
    /// so a test can craft overlapping / disjoint distinct sets across
    /// series and compute the TRUE union cardinality.
    fn encode_hll_from_items(precision: u32, items: &[String]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use asap_sketchlib::{HllSketch, HllVariant};
        use prost::Message;
        let mut sk = HllSketch::new(HllVariant::Regular, precision);
        for it in items {
            sk.update(it.as_bytes());
        }
        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision: sk.precision,
            registers: sk.registers.clone(),
            hip_kxq0: sk.hip_kxq0,
            hip_kxq1: sk.hip_kxq1,
            hip_est: sk.hip_est,
            registers_sparse: None,
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// FIX 2 — GLOBAL HLL distinct rollup. `count(hll_metric)` with no `by`
    /// must MERGE the per-series HLL registers (register-wise max) across ALL
    /// matched series and estimate ONCE — the distinct UNION cardinality. Two
    /// series share an overlapping prefix of items and each carry disjoint
    /// items, so summing per-series estimates would over-count the overlap.
    /// The merged global estimate must land within HLL error of the true
    /// union, and be strictly below the naive per-series sum.
    #[tokio::test]
    async fn execute_count_hll_global_merges_registers_across_series() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = 600_000_u64;
        let w_start = now_ms.saturating_sub(3_000);
        let w_end = now_ms.saturating_sub(2_000);

        // Series A: items 0..600. Series B: items 400..1000.
        // Overlap = [400,600) = 200 items; true union = [0,1000) = 1000.
        let precision = 14u32; // ~0.8% standard error; legal for epsilon=0.01
        let a_items: Vec<String> = (0..600).map(|i| format!("u-{i}")).collect();
        let b_items: Vec<String> = (400..1000).map(|i| format!("u-{i}")).collect();
        let true_union = 1000.0_f64;

        for (sid, items) in [(8200u64, &a_items), (8201u64, &b_items)] {
            let mut meta = hll_meta(sid, "unique_users_global");
            meta.agg_kind = crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::Hll,
                config: SketchConfig::Hll { precision },
                spatial_filter_canonical: String::new(),
            };
            idx.register(meta);
            idx.append_sample(
                sid,
                BTreeMap::new(),
                (w_start, w_end),
                SketchSampleState {
                    bytes: encode_hll_from_items(precision, items),
                    encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
                },
            );
        }

        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "unique_users_global",
            "HLL",
            serde_json::json!({"precision":14}),
            &[],
            1000,
        );
        let query = "count(distinct_over_time(unique_users_global[1m]))";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::Reduce(vec![]),
            1000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Cardinality,
            },
        );
        let engine = test_plan::engine(idx, config, vec![8200, 8201], entry);
        let result = engine
            .execute_at(query, w_end)
            .await
            .expect("global distinct count over HLL sids must answer, not capability-miss");

        // GLOBAL distinct is a single scalar — exactly one element.
        let est = match &result {
            crate::query_engines::query_result::QueryResult::Vector(v) => {
                assert_eq!(
                    v.values.len(),
                    1,
                    "a global distinct count must collapse to ONE merged estimate, got {} \
                     (per-series leak): {v:?}",
                    v.values.len()
                );
                v.values[0].value
            }
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                assert_eq!(m.values.len(), 1, "one merged series");
                m.values[0].samples.last().map(|s| s.value).unwrap_or(0.0)
            }
        };

        // Within HLL error of the true union (p=12 → ~1.04/sqrt(2^12) ≈ 1.6%;
        // allow a generous 8% band for the estimator's finite-sample noise).
        let rel_err = (est - true_union).abs() / true_union;
        assert!(
            rel_err < 0.08,
            "global merged estimate {est} must be within HLL error of the \
             true union {true_union} (rel_err {rel_err:.4})"
        );

        // And strictly below the naive per-series sum (600 + 600 = 1200),
        // proving registers were MERGED (max), not the estimates SUMMED.
        assert!(
            est < 1150.0,
            "merged global estimate {est} must be well below the per-series \
             sum (~1200) — proves register-merge, not estimate-sum"
        );
    }

    /// REPRODUCTION (root-cause hunt): `quantile_over_time(0.99,
    /// http_requests_total_latency_ms[30s])` end-to-end via
    /// `execute(&str)` against a now-anchored KLL sid carrying real
    /// sketch state. The window is inside the engine's `[now-30s, now]`
    /// range. This pins the exact end-to-end behaviour the live deploy
    /// shows ("No result" tagged `asap_query`) so we can see whether the
    /// engine produces `Ok(populated)`, `Ok(empty)`, or `CapabilityMiss`.
    #[tokio::test]
    async fn execute_quantile_over_time_kll_now_anchored() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7100u64;
        idx.register(kll_meta(sid, "http_requests_total_latency_ms"));

        let now_ms = 600_000_u64;
        // One complete pane covers the requested 30 second lookback.
        let window_start = now_ms.saturating_sub(30_000);
        let window_end = now_ms;

        let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let bytes = encode_kll_items_proto(269, &items);
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (window_start, window_end),
            SketchSampleState {
                bytes,
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        // The sid must classify as Hit (in-memory unsealed state counts).
        assert_eq!(
            idx.classify(sid),
            crate::storage_engines::sketch_db::index::SeriesLookup::Hit,
            "KLL sid with appended in-memory state must classify Hit"
        );

        let result = engine_quantile_result(idx, now_ms).await;
        let nonempty = match result {
            crate::query_engines::query_result::QueryResult::Vector(v) => !v.values.is_empty(),
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                m.values.iter().any(|s| !s.samples.is_empty())
            }
        };
        assert!(
            nonempty,
            "quantile_over_time over a now-anchored KLL sid must return a \
             non-empty result (got empty → reproduces the live `asap_query` \
             + No-result bug)"
        );
    }

    async fn engine_quantile_result(
        idx: Arc<SketchStore>,
        now_ms: u64,
    ) -> crate::query_engines::query_result::QueryResult {
        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "http_requests_total_latency_ms",
            "DatasketchesKLL",
            serde_json::json!({"k":269}),
            &[],
            30_000,
        );
        let query = "quantile_over_time(0.99, http_requests_total_latency_ms[30s])";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::PerEntity,
            30_000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Quantile { q: 0.99 },
            },
        );
        let sids = idx.snapshot_instances().iter().map(|m| m.sid).collect();
        let engine = test_plan::engine(idx, config, sids, entry);
        engine.execute_at(query, now_ms).await.expect(
            "quantile_over_time over a Hit KLL sid must NOT capability-miss \
                 (if it does, the bug is upstream of the reducer)",
        )
    }

    fn result_nonempty(r: &crate::query_engines::query_result::QueryResult) -> bool {
        match r {
            crate::query_engines::query_result::QueryResult::Vector(v) => !v.values.is_empty(),
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                m.values.iter().any(|s| !s.samples.is_empty())
            }
        }
    }

    /// REGRESSION (delta-stitching carry-in): the live agent emits a
    /// periodic Full snapshot followed by many cheap Delta frames to
    /// save bandwidth, so a short query window (`[30s]`) routinely
    /// contains ONLY deltas — the Full landed earlier, outside the
    /// window. Before the fix, `SketchStore::query_range`'s strict
    /// containment filter (`w.0 >= start`) dropped the out-of-window
    /// Full, the delta-apply reducer couldn't establish a rolling base,
    /// and the engine returned `Ok(empty)` (NOT a capability-miss) — so
    /// the router never failed over and the client saw "No result"
    /// tagged `asap_query`. The fix splices in the most-recent Full
    /// ending before `start` as a carry-in base. This test pins that:
    /// a Full at now-60s + a Delta at now-10s with a `[30s]` window must
    /// produce a NON-EMPTY answer.
    #[tokio::test]
    async fn quantile_over_time_kll_full_before_window_carries_in_base() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7400u64;
        idx.register(kll_meta(sid, "http_requests_total_latency_ms"));

        let now_ms = 600_000_u64;

        let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        // Full at now-60s..now-55s — OUTSIDE the 30s window.
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(60_000), now_ms.saturating_sub(55_000)),
            SketchSampleState {
                bytes: encode_kll_items_proto(269, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        // A delta pane covers the complete requested lookback.
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(30_000), now_ms),
            SketchSampleState {
                bytes: encode_kll_items_proto(269, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoDelta,
            },
        );

        let result = engine_quantile_result(idx, now_ms).await;
        assert!(
            result_nonempty(&result),
            "quantile_over_time with a Full BEFORE the window + a Delta \
             inside it must carry in the Full as a base and return a \
             non-empty result (regression: returned empty `asap_query` \
             No-result)"
        );
    }

    /// A delta-ONLY window with NO Full anywhere is the COMMON case under
    /// the edge's per-window-reset (PWR) delta model: the edge resets its
    /// snapshot base at each window boundary, so a window's first (here:
    /// only) frame is a delta-from-empty that, by construction, encodes
    /// that window's full state. The delta-apply walk bootstraps an empty
    /// rolling state of the sketch kind and applies the delta onto it, so
    /// the window IS queryable (delta-from-empty ⊕ empty = window state).
    ///
    /// Previously this returned empty (the walk skipped any delta with no
    /// carry-in Full), which is the very bug that broke end-to-end
    /// value-validation of delta-transmitted sketches.
    #[tokio::test]
    async fn quantile_over_time_kll_delta_only_no_base_reconstructs_from_empty() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7300u64;
        idx.register(kll_meta(sid, "http_requests_total_latency_ms"));

        let now_ms = 600_000_u64;
        let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(30_000), now_ms),
            SketchSampleState {
                bytes: encode_kll_items_proto(269, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoDelta,
            },
        );

        let result = engine_quantile_result(idx, now_ms).await;
        assert!(
            result_nonempty(&result),
            "delta-from-empty (PWR) window with no carry-in Full must \
             reconstruct that window's state and return a non-empty result"
        );
    }

    /// Issue #301 Layer 3: instant `sum(counter)` must return the
    /// cumulative-since-storage value (Σ of ALL windows' deltas), NOT
    /// the most-recent window's delta. Two windows of 600/900 per zone
    /// → per-zone cumulative = 1200/1800; `sum by (zone)` keeps them
    /// separate; bare `sum` collapses to 3000. This test pins the
    /// `accumulate_windows=true` reducer path the engine selects for
    /// `OuterFn::Plain` instant sums.
    #[tokio::test]
    async fn execute_instant_sum_accumulates_all_windows_not_last() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        let now_ms = 600_000_u64;
        let w1_start = now_ms.saturating_sub(120_000);
        let w1_end = now_ms.saturating_sub(60_000);
        let w2_start = w1_end;
        let w2_end = now_ms;

        for (i, (zone, per_window)) in [("z0", 600.0_f64), ("z1", 900.0)].iter().enumerate() {
            let sid = 14_000 + i as u64;
            idx.register(SummarySeriesMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            for (ws, we) in [(w1_start, w1_end), (w2_start, w2_end)] {
                let mut lm = BTreeMap::new();
                lm.insert("zone".to_string(), zone.to_string());
                idx.append_precompute(
                    sid,
                    lm,
                    (ws, we),
                    Box::new(SumAccumulator::with_sum(*per_window)),
                );
            }
        }

        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "http_requests_total",
            "Sum",
            serde_json::json!({}),
            &["zone"],
            60000,
        );
        let query = "sum by (zone) (http_requests_total)";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::Reduce(vec!["zone".into()]),
            120000,
            QueryPlanNode::ExactReadout {
                input: QueryNodeId(0),
                readout: ExactReadout::Sum,
            },
        );
        let engine = test_plan::engine(idx, config, vec![14000, 14001], entry);
        let result = engine
            .execute_at(query, now_ms)
            .await
            .expect("instant sum by zone must succeed");
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 2, "one entry per zone");
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for el in &vector.values {
            let keys = el.label_keys_override.as_ref().expect("keys present");
            let vals = &el.labels.labels;
            let zi = keys.iter().position(|k| k == "zone").expect("zone key");
            by_zone.insert(vals[zi].clone(), el.value);
        }
        // Cumulative = Σ of ALL windows, NOT the last window's delta
        // (which would be 600 / 900).
        let z0 = by_zone.get("z0").copied().expect("z0");
        let z1 = by_zone.get("z1").copied().expect("z1");
        assert!(
            (z0 - 1200.0).abs() < 1e-9,
            "z0 cumulative expected 1200 (600+600), got {z0} — if 600 the \
             engine took only the LAST window (Layer-3 bug)"
        );
        assert!(
            (z1 - 1800.0).abs() < 1e-9,
            "z1 cumulative expected 1800 (900+900), got {z1}"
        );
    }

    // ── P1-1 / P2-6 — rate over FrequencyEstimate (CMS) + keyed safe-miss ──

    /// A FrequencyEstimate (CountMin) sid with `total` inserts in its
    /// matrix row 0, registered for `metric` / `group_by_keys`, carrying
    /// one PROTO_FULL window anchored just before `now`.
    fn register_cms_freq_sid(
        idx: &SketchStore,
        sid: u64,
        metric: &str,
        group_by: &[&str],
        spatial_filter: &str,
        total_inserts: i64,
        now_ms: u64,
    ) {
        // Matches ControlPlaneCostModel's epsilon=0.01 CMS sizing.
        let cfg = SketchConfig::CountMin { rows: 5, cols: 512 };
        idx.register(SummarySeriesMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms))),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::Cms,
                config: cfg.clone(),
                spatial_filter_canonical: spatial_filter.to_string(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        // Build a CountMinState PROTO_FULL frame whose row 0 sums to
        // `total_inserts` (decode_frequency_total reads row 0's sum).
        let bytes = encode_cms_state_proto(5, 512, total_inserts);
        let window_start = now_ms.saturating_sub(60_000);
        let window_end = now_ms.saturating_sub(30_000);
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (window_start, window_end),
            SketchSampleState {
                bytes,
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
    }

    /// Encode a `CountMinState` with `rows`×`cols` int matrix where row 0
    /// holds `row0_total` in its first cell (rest zero). Mirrors the wire
    /// form `decoders::decode_cms_from_proto` reads.
    fn encode_cms_state_proto(rows: u32, cols: u32, row0_total: i64) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountMinState, CounterType, SketchEnvelope,
        };
        use prost::Message;
        let mut counts_int = vec![0i64; (rows * cols) as usize];
        counts_int[0] = row0_total; // row 0, col 0
        let state = CountMinState {
            rows,
            cols,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            ..Default::default()
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::CountMin(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn bare_cms_frequency_still_answers_after_p2_6() {
        use crate::query_engines::query_result::QueryResult;
        // Regression guard: the working `count_over_time(cms_metric[5m])`
        // demo (NO item key, empty spatial filter) must still be answered
        // by the warm tier after the P2-6 safe-miss was added.
        let now = 600_000_u64;
        let idx = Arc::new(SketchStore::new());
        register_cms_freq_sid(&idx, 7200, "cms_metric", &[], "", 600, now);

        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "cms_metric",
            "CountMinSketch",
            serde_json::json!({"w":512,"d":5}),
            &[],
            30000,
        );
        let query = "count_over_time(cms_metric[30s])";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::PerEntity,
            30000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::PointCount {
                    key: planner_types::pre_asap::ColumnRef::SampleValue,
                    value: None,
                },
            },
        );
        let engine = test_plan::engine(idx, config, vec![7200], entry);
        let result = engine.execute_at(query, now - 30000).await;
        assert!(
            matches!(
                result,
                Ok(QueryResult::Vector(_)) | Ok(QueryResult::Matrix(_))
            ),
            "bare count_over_time over CMS must still be answered warm, got {result:?}"
        );
    }
}

// ===========================================================================
/// A registered series without samples must not hide a readable bound series.
#[cfg(test)]
mod outer_agg_integration_tests {
    use super::*;
    use crate::query_engines::query_result::QueryResult;
    use crate::query_engines::routing::query_engine_routing::QueryEngine as _;
    use crate::query_engines::EngineError;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchAlgorithm, SketchConfig, SketchEncoding,
        SketchSampleState, SketchStore, SummarySeriesMetadata,
    };
    use crate::storage_engines::types::StreamingConfigHandle;
    use asap_sketchlib::DdSketch;
    use asap_sketchlib::MessagePackCodec;
    use std::collections::{BTreeMap, BTreeSet};

    fn dd_sketch_with_values(values: &[f64]) -> Vec<u8> {
        // The msgpack encoding round-trips through
        // `DdSketch::deserialize_msgpack` on the engine side — simpler
        // than the proto envelope and supported by `SketchEncoding::MsgpackFull`.
        let mut sk = DdSketch::new(0.01);
        for v in values {
            sk.update(*v);
        }
        sk.to_msgpack().expect("ddsketch msgpack serialization")
    }

    fn dd_meta_for(sid: u64, metric: &str, group_by: &[&str]) -> SummarySeriesMetadata {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        SummarySeriesMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::DDSketch,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    /// Regression: a candidate's sid set legitimately contains a MIX of
    /// `Ghost` (retired-then-evicted or merged-away, no data) and `Hit`
    /// (Active, carrying live sketch state) sids under the same metric.
    /// This is the exact production shape behind the warm-quantile miss:
    /// the metric's `instances_matching` walk returns the older retired
    /// sketch sids (now dataless ⇒ Ghost) alongside the freshly-minted
    /// Active sketch sids. Iterating ascending-u64, the older Ghost sid
    /// was hit first and aborted the WHOLE query to CapabilityMiss before
    /// the Active sid could answer. After the fix, non-Hit sids are
    /// skipped and the query resolves against the Active sid.
    #[tokio::test]
    async fn ghost_sid_does_not_mask_active_hit_sid_for_quantile() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = 600_000_u64;
        let w_start = now_ms.saturating_sub(60_000);
        let w_end = now_ms.saturating_sub(30_000);

        // Ghost sid (lower number ⇒ iterated first): registered metadata,
        // never appended any sample state. `classify` → Ghost.
        idx.register(dd_meta_for(1, "http_latency_ms", &["zone"]));

        // Active Hit sid (higher number): carries a real DDSketch window.
        idx.register(dd_meta_for(2, "http_latency_ms", &["zone"]));
        idx.append_sample(
            2,
            BTreeMap::from([("zone".to_string(), "z0".to_string())]),
            (w_start, w_end),
            SketchSampleState {
                bytes: dd_sketch_with_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                encoding: SketchEncoding::MsgpackFull,
            },
        );

        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        let config = test_plan::materialization(
            "http_latency_ms",
            "DDSketch",
            serde_json::json!({"alpha":0.01}),
            &["zone"],
            30000,
        );
        let query = "quantile_over_time(0.99, http_latency_ms[30s])";
        let entry = test_plan::entry(
            query,
            &config,
            PhysicalGrouping::PerEntity,
            30000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Quantile { q: 0.99 },
            },
        );
        let engine = test_plan::engine(idx, config, vec![1, 2], entry);
        let result = engine.execute_at(query, w_end).await.expect(
            "a dataless Ghost sid must not abort the query when an \
                 Active Hit sid under the same metric can answer it",
        );
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(
            vector.values.len(),
            1,
            "the single Active sid answers; the Ghost is skipped"
        );
        assert!(
            vector.values[0].value > 0.0,
            "p99 of [1..=10] is a positive quantile, got {}",
            vector.values[0].value
        );
    }
}

// ---------------------------------------------------------------------------
/// Planned range execution requires complete warm coverage at every step.
#[cfg(test)]
mod range_stitch_tests {
    use super::*;
    use crate::query_engines::query_result::{QueryResult, RangeVectorElement, Sample};
    use crate::query_engines::routing::query_engine_routing::{EngineCapabilities, QueryEngine};
    use crate::query_engines::EngineError;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchAlgorithm, SketchConfig, SketchEncoding,
        SketchSampleState, SketchStore, SummarySeriesMetadata,
    };
    use crate::storage_engines::types::{KeyByLabelValues, StreamingConfigHandle};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, BTreeSet};

    /// Encode a CountMin FULL proto frame whose row 0 sums to `total`
    /// (the per-window frequency TOTAL the `count_over_time` reducer reads).
    fn cms_bytes(total: i64) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountMinState, CounterType, SketchEnvelope,
        };
        use prost::Message;
        let (rows, cols) = (5u32, 512u32);
        let mut counts_int = vec![0i64; (rows * cols) as usize];
        counts_int[0] = total;
        let state = CountMinState {
            rows,
            cols,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            ..Default::default()
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::CountMin(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// A CountMin FrequencyEstimate sid — `count_over_time` over it emits one
    /// PER-WINDOW sample (not a single cumulative scalar), which is what the
    /// range stitch needs so warm contributes one value per covered window.
    fn cms_meta(sid: u64, metric: &str) -> SummarySeriesMetadata {
        let cfg = SketchConfig::CountMin { rows: 5, cols: 512 };
        SummarySeriesMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms))),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::Cms,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    /// Incomplete installed materializations must fail closed: a missing pane
    /// surfaces as a CapabilityMiss, which the HTTP layer forwards to the
    /// Prometheus fallback (#746).
    #[tokio::test]
    async fn planned_range_rejects_partial_warm_coverage() {
        use crate::query_engines::asap_query_engine::test_plan;
        use asap_types::query_plan::*;
        {
            let idx = Arc::new(SketchStore::new());
            idx.register(cms_meta(9100, "req_count"));
            idx.append_sample(
                9100,
                BTreeMap::new(),
                (30_000, 60_000),
                SketchSampleState {
                    bytes: cms_bytes(100),
                    encoding: SketchEncoding::ProtoFull,
                },
            );
            let config = test_plan::materialization(
                "req_count",
                "CountMinSketch",
                serde_json::json!({"w":512,"d":5}),
                &[],
                30_000,
            );
            let query = "count_over_time(req_count[30s])";
            let entry = test_plan::entry(
                query,
                &config,
                PhysicalGrouping::PerEntity,
                30_000,
                QueryPlanNode::SummaryEstimate {
                    input: QueryNodeId(0),
                    query: QueryReadout::PointCount {
                        key: planner_types::pre_asap::ColumnRef::SampleValue,
                        value: None,
                    },
                },
            );
            let engine = test_plan::engine(idx, config, vec![9100], entry);
            let result = engine
                .execute_range_promql_modern(query, 30_000, 60_000, 30_000)
                .await;
            assert!(
                matches!(result, Err(EngineError::CapabilityMiss { .. })),
                "a missing first pane must fail closed with a CapabilityMiss: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn active_metricsql_entry_reaches_the_shared_dag_executor() {
        use asap_types::query_plan::{
            FallbackPolicy, InstantExecution, QueryLanguage, QueryNodeId, QueryPlanEntry,
            QueryPlanNode,
        };
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let mut plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let identity = asap_types::query_plan::canonical_promql("1 + 2").unwrap();
        plan.query_plan.entries.insert(
            asap_types::query_plan::QueryPlan::catalog_key(QueryLanguage::MetricsQl, &identity),
            QueryPlanEntry {
                language: QueryLanguage::MetricsQl,
                query_id: "vm-scalar".into(),
                canonical_query: identity.clone(),
                fixed_evaluation: None,
                root: QueryNodeId(2),
                nodes: std::collections::BTreeMap::from([
                    (QueryNodeId(0), QueryPlanNode::Scalar { value: 1.0 }),
                    (QueryNodeId(1), QueryPlanNode::Scalar { value: 2.0 }),
                    (
                        QueryNodeId(2),
                        QueryPlanNode::Binary {
                            inputs: [QueryNodeId(0), QueryNodeId(1)],
                            operator: planner_types::pre_asap::ArithmeticOpKind::Add,
                        },
                    ),
                ]),
                instant: InstantExecution {
                    lookback_ms: 1,
                    full_history: false,
                    cumulative_readout: false,
                },
                fallback: FallbackPolicy::ExactBackend,
            },
        );
        let mut active = crate::drivers::query::servers::http::validate_and_build_runtime_plan(
            crate::drivers::query::servers::http::PhysicalPlanInstallRequest {
                summary_catalog: plan.summary_catalog,
                collector_plans: plan.collector_plans,
                precompute_plan: plan.precompute_plan,
                transmission_plan: plan.transmission_plan,
                query_plan: plan.query_plan,
                storage_routing: None,
                adaptation_evidence: vec![],
            },
            Arc::new(crate::storage_engines::types::BackendStorageRouting::empty()),
        )
        .unwrap();
        active.envelope.expiry_unix_ms = None;
        let active = crate::storage_engines::types::ActivePhysicalPlanHandle::new(active);
        let hot = StreamingConfigHandle::from_active_physical_plan(active.clone());
        let engine = ASAPQueryEngine::new(15).with_active_physical_plan(active);
        let error = engine
            .execute_metricsql_at(&identity, 1_000)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("bound subtree requires one explicit positive window"),
            "{error}"
        );
    }
}
