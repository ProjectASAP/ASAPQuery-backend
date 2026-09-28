//! SQL boundary around the shared, compiler-bound physical DAG executor.
use super::{
    clickhouse_result_adapter::{from_series_rows, ClickHouseQueryResult},
    relational_adapter::ClickHouseRelation,
};
use crate::{
    query_engines::asap_query_engine::{
        catalog_resolver::validate_payload, post_asap_readout::execute_query_plan_from_readout,
    },
    storage_engines::sketch_db::index::SketchStore,
};
use asap_types::query_plan::{QueryNodeId, QueryPlanEntry, QueryPlanNode};
use asap_types::summary_catalog::SummaryCatalog;
use std::collections::{BTreeMap, BTreeSet};

pub type PreparedExternalLeaves = BTreeMap<QueryNodeId, ClickHouseRelation>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseDagFallback {
    NoCandidates,
    UnsupportedPlan(String),
    IncompleteCoverage {
        requested: (u64, u64),
        observed: Option<(u64, u64)>,
    },
    ResultEncoding(String),
}

#[cfg(test)]
thread_local! {
    // Deterministically publish between branches without timing-dependent threads.
    static AFTER_BRANCH: std::cell::Cell<Option<fn(&SketchStore)>> = const { std::cell::Cell::new(None) };
}

struct RelationDagExecutor<'a> {
    index: &'a SketchStore,
    entry: &'a QueryPlanEntry,
    prepared: &'a PreparedExternalLeaves,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
    memo: BTreeMap<QueryNodeId, ClickHouseRelation>,
    schemas: BTreeMap<QueryNodeId, planner_types::post_asap::SummarySchema>,
    #[cfg(test)]
    evaluations: BTreeMap<QueryNodeId, usize>,
}

impl RelationDagExecutor<'_> {
    fn execute(
        &mut self,
        root: QueryNodeId,
        expected_schema: &planner_types::post_asap::SummarySchema,
    ) -> Result<ClickHouseRelation, String> {
        if let Some(schema) = self.schemas.get(&root) {
            if schema != expected_schema {
                return Err(format!(
                    "query `{}` node {} is consumed with inconsistent relation schemas",
                    self.entry.query_id, root.0
                ));
            }
        }
        if let Some(relation) = self.memo.get(&root) {
            return Ok(relation.clone());
        }
        self.schemas.insert(root, expected_schema.clone());
        let relation = self.execute_graph(root, expected_schema)?;
        self.record_evaluation(root);
        self.memo.insert(root, relation.clone());
        Ok(relation)
    }

    #[cfg(test)]
    fn record_evaluation(&mut self, id: QueryNodeId) {
        *self.evaluations.entry(id).or_default() += 1;
    }

    #[cfg(not(test))]
    fn record_evaluation(&mut self, _id: QueryNodeId) {}

    #[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
        fields(stage = "physical.execute", query_id = %self.entry.query_id, root = root.0), err)]
    fn execute_graph(
        &mut self,
        root: QueryNodeId,
        expected: &planner_types::post_asap::SummarySchema,
    ) -> Result<ClickHouseRelation, String> {
        use super::relational_adapter::native;
        use asap_physical_operators::dag::{self, operators::Operator};
        use futures::{FutureExt, StreamExt};
        let compiled = self
            .entry
            .recover_relational_physical_dag()
            .map_err(|e| e.to_string())?;
        if compiled.roots() != [root.0]
            || compiled
                .output_contract(root.0)
                .map_err(|e| e.to_string())?
                .schema
                .as_ref()
                != expected
        {
            return Err("requested relation differs from installed physical root".into());
        }
        let schemas: BTreeMap<_, _> = compiled
            .input_contracts()
            .map(|(id, contract)| (QueryNodeId(id), contract.schema.as_ref().clone()))
            .collect();
        let sources: Vec<_> = schemas.keys().copied().collect();
        let mut resolved_inputs = BTreeMap::new();
        let context = dag::RunContext::new(
            dag::Scope::Query {
                evaluation_time_ms: i64::try_from(self.t1_ms)
                    .map_err(|_| "evaluation time overflow")?,
                revision: self.index.summary_update_revision().mutation_sequence(),
            },
            dag::Limits::default(),
        )
        .map_err(|e| e.to_string())?;
        let mut storage_roots = BTreeSet::new();
        for source in &sources {
            if !matches!(
                self.entry.nodes[source],
                QueryPlanNode::ExternalExact { .. }
            ) {
                storage_roots.insert(*source);
                for id in self
                    .entry
                    .topological_order_from(*source)
                    .map_err(|e| e.to_string())?
                {
                    if matches!(
                        self.entry.nodes[&id],
                        QueryPlanNode::ReadMaterialization { .. }
                    ) {
                        storage_roots.insert(id);
                    }
                }
            }
        }
        let stored = crate::query_engines::asap_query_engine::post_asap_readout::execute_query_plan_readouts(
            self.index,self.entry,&storage_roots.into_iter().collect::<Vec<_>>(),self.t0_ms,self.t1_ms,self.is_cumulative,context.clone(),
        ).map_err(|e|format!("incomplete leaf coverage: {e:?}"))?;
        let mut coverage = None;
        let mut first = true;
        for id in sources {
            let relation = self.execute_source(id, &schemas[&id], &stored)?;
            #[cfg(test)]
            if let Some(publish) = AFTER_BRANCH.with(|hook| hook.take()) {
                publish(self.index);
            }
            coverage = if first {
                first = false;
                relation.coverage
            } else {
                match (coverage, relation.coverage) {
                    (Some((a, b)), Some((c, d))) if a.max(c) <= b.min(d) => {
                        Some((a.max(c), b.min(d)))
                    }
                    _ => None,
                }
            };
            let batch = native::batch(&relation, &schemas[&id]).map_err(|e| e.to_string())?;
            resolved_inputs.insert(
                id.0,
                Box::new(
                    Operator::source(batch.schema().clone(), vec![batch])
                        .map_err(|e| e.to_string())?,
                ) as asap_physical_operators::physical_planner::Source<'_>,
            );
        }
        let graph = {
            let _binding =
                tracing::debug_span!(target: "asap_runtime_debug", "physical_input_binding",
                stage = "physical.bind_inputs", input_count = resolved_inputs.len())
                .entered();
            compiled
                .instantiate(resolved_inputs)
                .map_err(|e| e.to_string())?
        };
        let mut output = graph
            .execute(&[root.0], context)
            .map_err(|e| e.to_string())?
            .remove(0);
        let mut batches = Vec::new();
        loop {
            match output.next().now_or_never() {
                Some(Some(Ok(batch))) => batches.push(batch),
                Some(Some(Err(error))) => return Err(error.to_string()),
                Some(None) => break,
                None => continue,
            }
        }
        native::relation(&batches, expected, coverage).map_err(|e| e.to_string())
    }

    fn execute_source(
        &mut self,
        root: QueryNodeId,
        expected_schema: &planner_types::post_asap::SummarySchema,
        stored: &BTreeMap<
            QueryNodeId,
            crate::query_engines::asap_query_engine::post_asap_readout::PostAsapReadoutOutcome,
        >,
    ) -> Result<ClickHouseRelation, String> {
        match self.entry.nodes.get(&root) {
            Some(QueryPlanNode::ExternalExact { request, .. }) => {
                if request.language != asap_types::QueryLanguage::ClickHouseSql {
                    return Err(
                        "ClickHouse DAG contains an external leaf for another language".into(),
                    );
                }
                let asap_types::query_plan::ExternalExactOutput::Relation { schema } =
                    &request.output
                else {
                    return Err("ClickHouse external leaf must declare relation output".into());
                };
                let declared: planner_types::post_asap::SummarySchema =
                    serde_json::from_value(schema.clone()).map_err(|error| error.to_string())?;
                if &declared != expected_schema {
                    return Err("external exact leaf schema differs from its parent edge".into());
                }
                self.prepared
                    .get(&root)
                    .cloned()
                    .ok_or_else(|| "published external exact leaf was not prepared".into())
            }
            Some(_) => {
                let outcome = stored.get(&root).ok_or("missing bound storage frontier")?;
                let reachable = self
                    .entry
                    .topological_order_from(root)
                    .map_err(|error| format!("invalid leaf DAG: {error}"))?;
                for (leaf_id, binding) in
                    reachable
                        .iter()
                        .filter_map(|id| match self.entry.nodes.get(id) {
                            Some(QueryPlanNode::ReadMaterialization { binding }) => {
                                Some((*id, binding))
                            }
                            _ => None,
                        })
                {
                    let leaf_outcome = stored
                        .get(&leaf_id)
                        .ok_or("missing materialization coverage")?;
                    if !binding.covers_range(self.t0_ms, self.t1_ms)
                        || !complete_pane_coverage(
                            leaf_outcome.coverage,
                            (self.t0_ms, self.t1_ms),
                            binding.window_ms,
                        )
                    {
                        return Err(format!(
                        "incomplete leaf coverage: requested ({}, {}), observed {:?}, pane {} origin {}",
                        self.t0_ms,
                        self.t1_ms,
                        leaf_outcome.coverage, binding.window_ms, binding.pane_origin_ms.unwrap_or(0)
                    ));
                    }
                }
                ClickHouseRelation::from_series_rows(
                    expected_schema,
                    outcome.series.clone(),
                    outcome.coverage,
                )
                .map_err(|error| error.to_string())
            }
            None => Err(format!("published DAG references missing node {}", root.0)),
        }
    }
}
pub enum ClickHouseDagOutcome {
    Accelerated(ClickHouseQueryResult),
    Fallback(ClickHouseDagFallback),
}

fn complete_pane_coverage(
    coverage: Option<(u64, u64)>,
    requested: (u64, u64),
    pane_ms: u64,
) -> bool {
    let Some((first_end, last_end)) = coverage else {
        return false;
    };
    pane_ms > 0 && first_end.saturating_sub(pane_ms) <= requested.0 && last_end >= requested.1
}

pub fn execute_sql_dag_with_external(
    index: &SketchStore,
    entry: &QueryPlanEntry,
    sds: &SummaryCatalog,
    prepared: &PreparedExternalLeaves,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> ClickHouseDagOutcome {
    let revision = index.summary_update_revision();
    let result = execute_sql_dag_with_external_unfenced(
        index,
        entry,
        sds,
        prepared,
        t0_ms,
        t1_ms,
        is_cumulative,
    );
    if !revision.matches(index.summary_update_revision()) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
            "summary input changed during SQL DAG evaluation".into(),
        ));
    }
    result
}

fn execute_sql_dag_with_external_unfenced(
    index: &SketchStore,
    entry: &QueryPlanEntry,
    sds: &SummaryCatalog,
    prepared: &PreparedExternalLeaves,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> ClickHouseDagOutcome {
    if let Err(error) = validate_payload(Some(sds), entry, sds.plan_id, sds.plan_version) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
            error.to_string(),
        ));
    }
    let has_relational_join = entry.topological_order().is_ok_and(|ids| {
        ids.iter().any(|id| {
            matches!(
                entry.nodes.get(id),
                Some(
                    QueryPlanNode::Relational { .. }
                        | QueryPlanNode::RelationalJoin { .. }
                        | QueryPlanNode::ExternalExact { .. }
                )
            )
        })
    });
    if has_relational_join {
        let root_schema = match entry.nodes.get(&entry.root) {
            Some(QueryPlanNode::Relational { output_schema, .. })
            | Some(QueryPlanNode::RelationalJoin { output_schema, .. }) => output_schema.clone(),
            Some(QueryPlanNode::ExternalExact { request, .. }) => {
                let asap_types::query_plan::ExternalExactOutput::Relation { schema } =
                    &request.output
                else {
                    return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                        "ClickHouse external root must declare relation output".into(),
                    ));
                };
                match serde_json::from_value(schema.clone()) {
                    Ok(schema) => schema,
                    Err(error) => {
                        return ClickHouseDagOutcome::Fallback(
                            ClickHouseDagFallback::UnsupportedPlan(error.to_string()),
                        )
                    }
                }
            }
            _ => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    "relational join plan root has no relation schema".into(),
                ))
            }
        };
        let relation = match (RelationDagExecutor {
            index,
            entry,
            prepared,
            t0_ms,
            t1_ms,
            is_cumulative,
            memo: BTreeMap::new(),
            schemas: BTreeMap::new(),
            #[cfg(test)]
            evaluations: BTreeMap::new(),
        })
        .execute(entry.root, &root_schema)
        {
            Ok(relation) => relation,
            Err(error) if error.contains("incomplete leaf coverage") => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
                    requested: (t0_ms, t1_ms),
                    observed: None,
                })
            }
            Err(error) => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    error,
                ));
            }
        };
        let bindings = entry.materialization_bindings();
        let pane_ms = bindings
            .iter()
            .map(|binding| binding.window_ms)
            .max()
            .unwrap_or(0);
        // External-only DAGs have no summary panes to cover. Their source
        // population is defined by the independently bound exact requests.
        if !bindings.is_empty()
            && !complete_pane_coverage(relation.coverage, (t0_ms, t1_ms), pane_ms)
        {
            return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
                requested: (t0_ms, t1_ms),
                observed: relation.coverage,
            });
        }
        return match relation.into_result() {
            Ok(result) => ClickHouseDagOutcome::Accelerated(result),
            Err(error) => ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::ResultEncoding(
                error.to_string(),
            )),
        };
    }
    let base_root = entry.root;
    if let Err(detail) = validate_reachable(entry, base_root) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(detail));
    }
    let outcome =
        match execute_query_plan_from_readout(index, entry, base_root, t0_ms, t1_ms, is_cumulative)
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let detail = format!("{error:?}");
                if detail.contains("missing materialized pane")
                    || detail.contains("incomplete materialized panes")
                {
                    return ClickHouseDagOutcome::Fallback(
                        ClickHouseDagFallback::IncompleteCoverage {
                            requested: (t0_ms, t1_ms),
                            observed: None,
                        },
                    );
                }
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    detail,
                ));
            }
        };
    let pane_ms = entry
        .materialization_bindings()
        .iter()
        .map(|binding| binding.window_ms)
        .max()
        .unwrap_or(0);
    if !complete_pane_coverage(outcome.coverage, (t0_ms, t1_ms), pane_ms) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
            requested: (t0_ms, t1_ms),
            observed: outcome.coverage,
        });
    }
    match from_series_rows(outcome.series) {
        Ok(result) => ClickHouseDagOutcome::Accelerated(result),
        Err(error) => {
            ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::ResultEncoding(error.to_string()))
        }
    }
}

fn validate_reachable(entry: &QueryPlanEntry, root: QueryNodeId) -> Result<(), String> {
    let mut pending = vec![root];
    let mut reachable = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !reachable.insert(id) {
            continue;
        }
        let node = entry
            .nodes
            .get(&id)
            .ok_or_else(|| format!("published DAG references missing node {}", id.0))?;
        pending.extend(node.inputs());
    }
    Ok(())
}

// Validate schemas and graph shape before touching deployment sources.
#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::query_plan::{
        ExternalExactOutput, ExternalExactRequest, FallbackPolicy, InstantExecution, QueryLanguage,
    };
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::DataType,
    };

    fn relation_schema(name: &str) -> SummarySchema {
        SummarySchema {
            fields: vec![SummaryField {
                name: name.into(),
                dtype: SummaryFamilyType::Plain(DataType::Int64),
                nullable: false,
            }],
            time_index: None,
        }
    }

    fn external_entry(schema: &SummarySchema) -> QueryPlanEntry {
        QueryPlanEntry {
            physical_dag: None,
            language: QueryLanguage::ClickHouseSql,
            query_id: "shared-external".into(),
            canonical_query: "SELECT x".into(),
            fixed_evaluation: None,
            root: QueryNodeId(0),
            nodes: BTreeMap::from([(
                QueryNodeId(0),
                QueryPlanNode::ExternalExact {
                    request: ExternalExactRequest {
                        language: QueryLanguage::ClickHouseSql,
                        expression: "SELECT 1 AS x".into(),
                        output: ExternalExactOutput::Relation {
                            schema: serde_json::to_value(schema).unwrap(),
                        },
                        parameters: BTreeMap::new(),
                        start_parameter: None,
                        end_parameter: None,
                        input_contracts: Vec::new(),
                    },
                    inputs: Vec::new(),
                },
            )]),
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        }
    }

    #[test]
    fn relation_dag_memoizes_a_shared_node_and_enforces_one_edge_schema() {
        let schema = relation_schema("x");
        let mut entry = external_entry(&schema);
        entry.compile_relational_physical_dag().unwrap();
        let relation = ClickHouseRelation::from_json_compact(
            &schema,
            br#"{"meta":[{"name":"x","type":"Int64"}],"data":[[1]]}"#,
        )
        .unwrap();
        let prepared = BTreeMap::from([(QueryNodeId(0), relation)]);
        let index = SketchStore::new();
        let mut executor = RelationDagExecutor {
            index: &index,
            entry: &entry,
            prepared: &prepared,
            t0_ms: 0,
            t1_ms: 1,
            is_cumulative: false,
            memo: BTreeMap::new(),
            schemas: BTreeMap::new(),
            evaluations: BTreeMap::new(),
        };

        executor.execute(QueryNodeId(0), &schema).unwrap();
        executor.execute(QueryNodeId(0), &schema).unwrap();
        assert_eq!(executor.evaluations[&QueryNodeId(0)], 1);

        let error = executor
            .execute(QueryNodeId(0), &relation_schema("different"))
            .unwrap_err();
        assert!(error.contains("query `shared-external` node 0"));
        assert!(error.contains("inconsistent relation schemas"));
    }

    // A SQL diamond binds one source to both join inputs in the shared DAG.
    #[test]
    fn relation_dag_executes_a_shared_source_join() {
        use planner_types::pre_asap::{JoinKind, Predicate, QueryExpr, ScalarValue};
        let schema = relation_schema("x");
        let mut entry = external_entry(&schema);
        let mut output = schema.clone();
        output.fields.push(schema.fields[0].clone());
        entry.nodes.insert(
            QueryNodeId(1),
            QueryPlanNode::RelationalJoin {
                inputs: [QueryNodeId(0), QueryNodeId(0)],
                join_kind: JoinKind::Cross,
                pred: serde_json::to_value(Predicate::<usize>(
                    QueryExpr::Literal(ScalarValue::Boolean(true)).into(),
                ))
                .unwrap(),
                left_schema: schema.clone(),
                right_schema: schema.clone(),
                output_schema: output.clone(),
                pruning: None,
            },
        );
        entry.root = QueryNodeId(1);
        entry.compile_relational_physical_dag().unwrap();
        let encoded = serde_json::to_vec(&entry).unwrap();
        let entry: QueryPlanEntry = serde_json::from_slice(&encoded).unwrap();
        let relation = ClickHouseRelation::from_json_compact(
            &schema,
            br#"{"meta":[{"name":"x","type":"Int64"}],"data":[[1],[2]]}"#,
        )
        .unwrap();
        let prepared = BTreeMap::from([(QueryNodeId(0), relation)]);
        let index = SketchStore::new();
        let mut executor = RelationDagExecutor {
            index: &index,
            entry: &entry,
            prepared: &prepared,
            t0_ms: 0,
            t1_ms: 1,
            is_cumulative: false,
            memo: BTreeMap::new(),
            schemas: BTreeMap::new(),
            evaluations: BTreeMap::new(),
        };
        let result = executor
            .execute(entry.root, &output)
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(
            result
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            4
        );
    }

    // Missing installed computation must not trigger serving-time re-lowering.
    #[test]
    fn relation_execution_requires_an_installed_physical_dag() {
        let schema = relation_schema("x");
        let entry = external_entry(&schema);
        let relation = ClickHouseRelation::from_json_compact(
            &schema,
            br#"{"meta":[{"name":"x","type":"Int64"}],"data":[[1]]}"#,
        )
        .unwrap();
        let prepared = BTreeMap::from([(QueryNodeId(0), relation)]);
        let index = SketchStore::new();
        let mut executor = RelationDagExecutor {
            index: &index,
            entry: &entry,
            prepared: &prepared,
            t0_ms: 0,
            t1_ms: 1,
            is_cumulative: false,
            memo: BTreeMap::new(),
            schemas: BTreeMap::new(),
            evaluations: BTreeMap::new(),
        };
        assert!(executor.execute(entry.root, &schema).is_err());
    }

    /// A publication between query branches invalidates the entire result.
    #[test]
    fn query_wide_fence_rejects_publication_between_join_branches() {
        use planner_types::pre_asap::{JoinKind, Predicate, QueryExpr, ScalarValue};
        let schema = relation_schema("x");
        let mut entry = external_entry(&schema);
        entry
            .nodes
            .insert(QueryNodeId(2), entry.nodes[&QueryNodeId(0)].clone());
        let mut output = schema.clone();
        output.fields.push(schema.fields[0].clone());
        entry.nodes.insert(
            QueryNodeId(1),
            QueryPlanNode::RelationalJoin {
                inputs: [QueryNodeId(0), QueryNodeId(2)],
                join_kind: JoinKind::Cross,
                pred: serde_json::to_value(Predicate::<usize>(
                    QueryExpr::Literal(ScalarValue::Boolean(true)).into(),
                ))
                .unwrap(),
                left_schema: schema.clone(),
                right_schema: schema.clone(),
                output_schema: output,
                pruning: None,
            },
        );
        entry.root = QueryNodeId(1);
        entry.compile_relational_physical_dag().unwrap();
        let relation = ClickHouseRelation::from_json_compact(
            &schema,
            br#"{"meta":[{"name":"x","type":"Int64"}],"data":[[1],[2]]}"#,
        )
        .unwrap();
        let prepared = BTreeMap::from([
            (QueryNodeId(0), relation.clone()),
            (QueryNodeId(2), relation),
        ]);
        let index = SketchStore::new();
        let catalog = SummaryCatalog::from_materializations(1, 1, &[]).unwrap();
        let run =
            || execute_sql_dag_with_external(&index, &entry, &catalog, &prepared, 0, 1000, false);
        assert!(matches!(run(), ClickHouseDagOutcome::Accelerated(_)));
        AFTER_BRANCH.with(|hook| {
            hook.set(Some(|index| {
                use crate::storage_engines::sketch_db::index::{SketchEncoding, SketchSampleState};
                index.append_sample(
                    1,
                    BTreeMap::new(),
                    (0, 1000),
                    SketchSampleState {
                        bytes: vec![1],
                        encoding: SketchEncoding::ProtoFull,
                    },
                );
            }))
        });
        assert!(
            matches!(run(), ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(reason))
            if reason.contains("changed during SQL DAG"))
        );
        assert!(AFTER_BRANCH.with(|hook| hook.get().is_none()));
    }

    #[test]
    fn exact_accumulator_window_end_coverage_includes_its_pane_start() {
        assert!(complete_pane_coverage(
            Some((1_000, 2_000)),
            (0, 2_000),
            1_000
        ));
        assert!(!complete_pane_coverage(
            Some((2_000, 2_000)),
            (0, 2_000),
            1_000
        ));
    }
}
