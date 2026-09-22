//! Catalog-backed ClickHouse acceleration boundary.

use async_trait::async_trait;
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use std::{collections::BTreeMap, sync::Arc};

use super::{
    clickhouse_result_adapter::ClickHouseFormat,
    execution::{
        execute_sql_dag_with_external, ClickHouseDagFallback, ClickHouseDagOutcome,
        PreparedExternalLeaves,
    },
    fallback::{ClickHouseExactBackend, ClickHouseRawResponse},
    request::ClickHouseQueryRequest,
    server::{
        ClickHouseAccelerationFallback, ClickHouseAccelerationOutcome, ClickHouseAccelerator,
    },
};
use crate::storage_engines::sketch_db::index::SketchStore;

pub struct CatalogClickHouseAccelerator {
    pub store: Arc<SketchStore>,
    active_physical_plan: Option<crate::storage_engines::types::ActivePhysicalPlanHandle>,
    exact_backend: Option<Arc<dyn ClickHouseExactBackend>>,
}

impl CatalogClickHouseAccelerator {
    pub fn empty(store: Arc<SketchStore>) -> Self {
        Self {
            store,
            active_physical_plan: None,
            exact_backend: None,
        }
    }

    pub fn with_active_physical_plan(
        store: Arc<SketchStore>,
        active: crate::storage_engines::types::ActivePhysicalPlanHandle,
    ) -> Self {
        let mut accelerator = Self::empty(store);
        accelerator.active_physical_plan = Some(active);
        accelerator
    }

    /// Build the production mixed-DAG runtime with its required exact subtree
    /// backend. Keeping both dependencies in one constructor prevents a
    /// listener from enabling acceleration while leaving `ExternalExact`
    /// nodes unexecutable.
    pub fn with_active_physical_plan_and_exact_backend(
        store: Arc<SketchStore>,
        active: crate::storage_engines::types::ActivePhysicalPlanHandle,
        exact_backend: Arc<dyn ClickHouseExactBackend>,
    ) -> Self {
        Self::with_active_physical_plan(store, active).with_exact_backend(exact_backend)
    }

    pub fn with_exact_backend(mut self, exact_backend: Arc<dyn ClickHouseExactBackend>) -> Self {
        self.exact_backend = Some(exact_backend);
        self
    }

    async fn prepare_external_exact(
        &self,
        entry: &asap_types::query_plan::QueryPlanEntry,
        start_ms: u64,
        end_ms: u64,
        request_context: &ClickHouseQueryRequest,
    ) -> Result<PreparedExternalLeaves, String> {
        let mut prepared = PreparedExternalLeaves::new();
        let leaves = entry.nodes.iter().filter_map(|(id, node)| match node {
            asap_types::query_plan::QueryPlanNode::ExternalExact { request, inputs }
                if request.language == asap_types::QueryLanguage::ClickHouseSql
                    && inputs.is_empty() =>
            {
                Some((*id, request))
            }
            _ => None,
        });
        for (id, bound) in leaves {
            let backend = self
                .exact_backend
                .as_ref()
                .ok_or_else(|| "ClickHouse exact subtree endpoint unavailable".to_owned())?;
            let schema = match &bound.output {
                asap_types::query_plan::ExternalExactOutput::Relation { schema } => {
                    serde_json::from_value(schema.clone()).map_err(|error| error.to_string())?
                }
                _ => return Err("ClickHouse exact subtree must produce a relation".into()),
            };
            let mut parameters = bound
                .parameters
                .iter()
                .map(|(name, value)| (format!("param_{name}"), value.clone()))
                .collect::<BTreeMap<_, _>>();
            if let Some(name) = &bound.start_parameter {
                parameters.insert(format!("param_{name}"), start_ms.to_string());
            }
            if let Some(name) = &bound.end_parameter {
                parameters.insert(format!("param_{name}"), end_ms.to_string());
            }
            parameters.insert("default_format".into(), "JSONCompact".into());
            parameters.insert(
                "output_format_json_map_as_array_of_tuples".into(),
                "1".into(),
            );
            parameters.insert(
                "output_format_json_named_tuples_as_objects".into(),
                "0".into(),
            );
            parameters.insert("output_format_json_quote_64bit_integers".into(), "0".into());
            // Preserve the distinction between NULL and unsupported NaN/Inf.
            // The typed decoder rejects quoted non-finite values and falls back.
            parameters.insert("output_format_json_quote_denormals".into(), "1".into());
            if let Some(database) = request_context.database() {
                parameters.insert("database".into(), database.into());
            }
            let request = ClickHouseQueryRequest {
                method: axum::http::Method::POST,
                sql: bound.expression.clone(),
                body: Bytes::from(bound.expression.clone()),
                parameters,
                headers: request_context.headers.clone(),
            };
            let response = backend
                .execute(&request)
                .await
                .map_err(|error| error.to_string())?;
            if !response.status.is_success() {
                return Err(format!(
                    "ClickHouse external subtree returned HTTP {}",
                    response.status
                ));
            }
            let mut relation = super::relational_adapter::ClickHouseRelation::from_json_compact(
                &schema,
                &response.body,
            )
            .map_err(|error| error.to_string())?;
            relation.coverage = Some((start_ms, end_ms));
            prepared.insert(id, relation);
        }
        Ok(prepared)
    }
}

fn requested_format(request: &ClickHouseQueryRequest) -> Result<ClickHouseFormat, String> {
    if let Some(setting) = request.parameters.keys().find(|key| {
        key.starts_with("output_format_") || key.as_str() == "format_tsv_null_representation"
    }) {
        return Err(format!("unsupported output setting {setting}"));
    }
    match request
        .format()
        .unwrap_or("TabSeparated")
        .to_ascii_lowercase()
        .as_str()
    {
        "tabseparated" | "tsv" => Ok(ClickHouseFormat::TabSeparated),
        "jsoneachrow" => Ok(ClickHouseFormat::JsonEachRow),
        "json" => Ok(ClickHouseFormat::Json),
        other => Err(other.to_owned()),
    }
}

#[async_trait]
impl ClickHouseAccelerator for CatalogClickHouseAccelerator {
    async fn execute(&self, request: &ClickHouseQueryRequest) -> ClickHouseAccelerationOutcome {
        let Some(physical) = self
            .active_physical_plan
            .as_ref()
            .map(|h| h.active_snapshot())
        else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
        let Some(context) = physical.query_plan.clickhouse_context.as_ref() else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
        let (fixed_sql, canonical_sql, runtime_range) =
            match control_plane::clickhouse::bind_clickhouse_sql(
                &request.sql,
                &control_plane::clickhouse::ClickHouseSqlCatalog {
                    tables: context.tables.clone(),
                },
                context.accuracy.clone(),
            )
            .await
            {
                Ok(canonical) => canonical,
                Err(error) => {
                    return ClickHouseAccelerationOutcome::Fallback(
                        ClickHouseAccelerationFallback::Planning(error.to_string()),
                    )
                }
            };
        // Concrete entries keep their own physical bindings. Older publications
        // may instead store a template directly as the primary key.
        if let Ok(entry) = physical.query_plan.lookup_clickhouse(&fixed_sql) {
            return self.execute_bound(request, &physical, entry, None).await;
        }
        if let Ok(entry) = physical.query_plan.lookup_clickhouse(&canonical_sql) {
            return self
                .execute_bound(request, &physical, entry, runtime_range)
                .await;
        }
        let mut outcome =
            ClickHouseAccelerationOutcome::Fallback(ClickHouseAccelerationFallback::CatalogMiss);
        if let Some(identities) = context.window_templates.get(&canonical_sql) {
            for identity in identities {
                let Ok(entry) = physical.query_plan.lookup_clickhouse(identity) else {
                    continue;
                };
                outcome = self
                    .execute_bound(request, &physical, entry, runtime_range)
                    .await;
                if matches!(outcome, ClickHouseAccelerationOutcome::Accelerated(_)) {
                    return outcome;
                }
            }
        }
        outcome
    }
}

impl CatalogClickHouseAccelerator {
    async fn execute_bound(
        &self,
        request: &ClickHouseQueryRequest,
        physical: &crate::storage_engines::types::RuntimePhysicalPlan,
        entry: &asap_types::query_plan::QueryPlanEntry,
        runtime_range: Option<(u64, u64)>,
    ) -> ClickHouseAccelerationOutcome {
        let format = match requested_format(request) {
            Ok(format) => format,
            Err(format) => {
                return ClickHouseAccelerationOutcome::Fallback(
                    ClickHouseAccelerationFallback::UnsupportedFormat(format),
                )
            }
        };
        let Some(catalog) = physical.summary_catalog.as_ref() else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
        let Some(mut range) = entry.fixed_evaluation else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::Execution(
                    "ClickHouse plan is missing its fixed evaluation range".into(),
                ),
            );
        };
        if let Some((start_ms, end_ms)) = runtime_range {
            if end_ms - start_ms != range.end_ms - range.start_ms
                || entry.nodes.values().any(|node| {
                    matches!(
                        node,
                        asap_types::query_plan::QueryPlanNode::ExternalExact { .. }
                    )
                })
            {
                return ClickHouseAccelerationOutcome::Fallback(
                    ClickHouseAccelerationFallback::CatalogMiss,
                );
            }
            range.start_ms = start_ms;
            range.end_ms = end_ms;
        }
        let prepared = match self
            .prepare_external_exact(entry, range.start_ms, range.end_ms, request)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                return ClickHouseAccelerationOutcome::Fallback(
                    ClickHouseAccelerationFallback::Execution(error),
                )
            }
        };
        match execute_sql_dag_with_external(
            self.store.as_ref(),
            entry,
            catalog.as_ref(),
            &prepared,
            range.start_ms,
            range.end_ms,
            range.cumulative,
        ) {
            ClickHouseDagOutcome::Accelerated(result) => match result.encode(format) {
                Ok(body) => {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "content-type",
                        HeaderValue::from_static(match format {
                            ClickHouseFormat::Json | ClickHouseFormat::JsonEachRow => {
                                "application/json; charset=UTF-8"
                            }
                            ClickHouseFormat::TabSeparated => {
                                "text/tab-separated-values; charset=UTF-8"
                            }
                        }),
                    );
                    let (execution, detail) = if entry.materialization_bindings().is_empty() {
                        ("exact_fallback", "external_dag")
                    } else if prepared.is_empty() {
                        ("warm", "asap")
                    } else {
                        ("hybrid", "hybrid")
                    };
                    headers.insert("x-asap-execution", HeaderValue::from_static(execution));
                    headers.insert("x-asap-execution-detail", HeaderValue::from_static(detail));
                    ClickHouseAccelerationOutcome::Accelerated(ClickHouseRawResponse {
                        status: StatusCode::OK,
                        headers,
                        body: Bytes::from(body),
                    })
                }
                Err(error) => ClickHouseAccelerationOutcome::Fallback(
                    ClickHouseAccelerationFallback::Execution(error.to_string()),
                ),
            },
            ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
                ..
            }) => ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::IncompleteCoverage,
            ),
            ClickHouseDagOutcome::Fallback(error) => ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::Execution(format!("{error:?}")),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn client_output_settings_cannot_silently_change_warm_format() {
        let mut request = ClickHouseQueryRequest {
            method: axum::http::Method::GET,
            sql: "SELECT labels FROM samples".into(),
            body: Bytes::new(),
            parameters: BTreeMap::from([("default_format".into(), "JSON".into())]),
            headers: HeaderMap::new(),
        };
        assert!(requested_format(&request).is_ok());
        for setting in [
            "output_format_json_map_as_array_of_tuples",
            "output_format_json_quote_64bit_integers",
            "format_tsv_null_representation",
        ] {
            request.parameters.insert(setting.into(), "1".into());
            assert!(requested_format(&request).is_err());
            request.parameters.remove(setting);
        }
    }

    use crate::{
        precompute_engine::operators::SumAccumulator,
        storage_engines::sketch_db::index::{AggKind, Capability, SummarySeriesMetadata},
    };
    use asap_types::query_plan::{
        ClickHousePlanningContext, ExactReadout, ExternalExactOutput, ExternalExactRequest,
        FallbackPolicy, FixedEvaluationRange, InstantExecution, MaterializationBinding,
        PhysicalGrouping, QueryLanguage, QueryNodeId, QueryPlan, QueryPlanEntry, QueryPlanNode,
    };
    use asap_types::summary_catalog::SummaryCatalog;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use axum::http::Method;

    struct FixedExactSubtree;

    #[async_trait]
    impl ClickHouseExactBackend for FixedExactSubtree {
        async fn execute(
            &self,
            request: &ClickHouseQueryRequest,
        ) -> Result<ClickHouseRawResponse, super::super::fallback::ClickHouseFallbackError>
        {
            assert_eq!(
                request.parameters.get("output_format_json_quote_denormals"),
                Some(&"1".into())
            );
            assert_eq!(request.parameters.get("param_from"), Some(&"0".into()));
            assert_eq!(request.parameters.get("param_to"), Some(&"2000".into()));
            Ok(ClickHouseRawResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: Bytes::from_static(br#"{"meta":[{"name":"timestamp","type":"Int64"},{"name":"divisor","type":"Float64"}],"data":[[2000,10.0]],"rows":1}"#),
            })
        }

        async fn ping(
            &self,
        ) -> Result<ClickHouseRawResponse, super::super::fallback::ClickHouseFallbackError>
        {
            unreachable!()
        }
    }
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema, ValueOperation},
        pre_asap::{
            ArithmeticOpKind, Column, CompareOpKind, DataType, GroupKeys, Predicate, ProjectItem,
            QueryExpr, ScalarValue, Schema, SortKey,
        },
    };
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        rc::Rc,
    };

    fn mixed_summary_external_entry(mut entry: QueryPlanEntry) -> QueryPlanEntry {
        let left = QueryNodeId(1);
        let external = QueryNodeId(6);
        let join = QueryNodeId(7);
        let project = QueryNodeId(8);
        let left_schema = relation_schema(&[
            ("timestamp", DataType::Timestamp),
            ("value", DataType::Float64),
        ]);
        let right_schema = relation_schema(&[
            ("timestamp", DataType::Timestamp),
            ("divisor", DataType::Float64),
        ]);
        let joined_schema = relation_schema(&[
            ("timestamp", DataType::Timestamp),
            ("value", DataType::Float64),
            ("timestamp", DataType::Timestamp),
            ("divisor", DataType::Float64),
        ]);
        let output_schema = relation_schema(&[
            ("timestamp", DataType::Timestamp),
            ("ratio", DataType::Float64),
        ]);
        entry
            .nodes
            .retain(|id, _| *id == QueryNodeId(0) || *id == left);
        entry.nodes.insert(external, QueryPlanNode::ExternalExact {
            request: ExternalExactRequest {
                language: QueryLanguage::ClickHouseSql,
                expression: "SELECT toInt64(2000) AS timestamp, toFloat64(10) AS divisor WHERE {from:UInt64} <= {to:UInt64}".into(),
                output: ExternalExactOutput::Relation { schema: serde_json::to_value(&right_schema).unwrap() },
                parameters: BTreeMap::new(),
                start_parameter: Some("from".into()),
                end_parameter: Some("to".into()),
                input_contracts: vec![],
            },
            inputs: vec![],
        });
        entry.nodes.insert(
            join,
            QueryPlanNode::RelationalJoin {
                inputs: [left, external],
                join_kind: planner_types::pre_asap::JoinKind::Inner,
                pred: serde_json::to_value(Predicate(Rc::new(QueryExpr::Compare {
                    left: Rc::new(QueryExpr::Column(0)),
                    op: CompareOpKind::Eq,
                    right: Rc::new(QueryExpr::Column(2)),
                })))
                .unwrap(),
                left_schema,
                right_schema,
                output_schema: joined_schema.clone(),
            },
        );
        entry.nodes.insert(
            project,
            QueryPlanNode::Relational {
                input: join,
                operation: serde_json::to_value(ValueOperation::Project {
                    cols: vec![
                        ProjectItem {
                            alias: Some("timestamp".into()),
                            expr: QueryExpr::Column(0),
                        },
                        ProjectItem {
                            alias: Some("ratio".into()),
                            expr: QueryExpr::Arithmetic {
                                op: ArithmeticOpKind::Div,
                                left: Rc::new(QueryExpr::Column(1)),
                                right: Rc::new(QueryExpr::Column(3)),
                            },
                        },
                    ],
                    qualifier: None,
                })
                .unwrap(),
                input_schema: joined_schema,
                output_schema,
            },
        );
        entry.root = project;
        entry
    }

    fn relation_schema(names: &[(&str, DataType)]) -> SummarySchema {
        SummarySchema {
            fields: names
                .iter()
                .map(|(name, dtype)| SummaryField {
                    name: (*name).into(),
                    dtype: SummaryFamilyType::Plain(dtype.clone()),
                    nullable: false,
                })
                .collect(),
            time_index: names
                .iter()
                .position(|(_, dtype)| *dtype == DataType::Timestamp),
        }
    }

    async fn fixture(end_ms: u64) -> (CatalogClickHouseAccelerator, ClickHouseQueryRequest) {
        fixture_with_store(end_ms, Arc::new(SketchStore::new()), true).await
    }

    async fn fixture_with_store(
        end_ms: u64,
        store: Arc<SketchStore>,
        seed: bool,
    ) -> (CatalogClickHouseAccelerator, ClickHouseQueryRequest) {
        fixture_with_sql(end_ms, store, seed, false).await
    }

    async fn fixture_with_sql(
        end_ms: u64,
        store: Arc<SketchStore>,
        seed: bool,
        moving: bool,
    ) -> (CatalogClickHouseAccelerator, ClickHouseQueryRequest) {
        let sql = if moving {
            format!("SELECT sum(value) FROM requests WHERE timestamp >= 0 AND timestamp < {end_ms}")
        } else {
            "SELECT sum(value) FROM requests".into()
        };
        let mut config = PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            1,
            1,
            WindowKind::Tumbling,
            String::new(),
            "requests".into(),
            None,
            Some("asap_e2e.samples".into()),
            Some("value".into()),
        );
        config.pane_origin_ms = Some(0);
        config.table_timestamp_column = Some("timestamp_ms".into());
        let sds = SummaryCatalog::from_materializations(41, 1, &[config.clone()]).unwrap();
        let materialization = *sds.definitions.keys().next().unwrap();
        let read = QueryNodeId(0);
        let readout = QueryNodeId(1);
        let input_schema = relation_schema(&[
            ("timestamp", DataType::Timestamp),
            ("value", DataType::Float64),
        ]);
        let projected_schema = relation_schema(&[
            ("bucket", DataType::Timestamp),
            ("score", DataType::Float64),
        ]);
        let filter = QueryNodeId(2);
        let project = QueryNodeId(3);
        let sort = QueryNodeId(4);
        let root = QueryNodeId(5);
        let executable = QueryPlanEntry {
            language: QueryLanguage::ClickHouseSql,
            query_id: "SELECT value FROM samples".into(),
            canonical_query: "SELECT value FROM samples".into(),
            fixed_evaluation: Some(FixedEvaluationRange {
                start_ms: 0,
                end_ms: 2_000,
                cumulative: true,
            }),
            root,
            nodes: [
                (
                    read,
                    QueryPlanNode::ReadMaterialization {
                        binding: MaterializationBinding {
            full_window_slide_ms: None,
                            materialization,
                            stored_output_reference: asap_types::sds::StoredOutputReference::for_definition(materialization),
                            output_grouping: PhysicalGrouping::Reduce(Vec::new()),
                            item_labels: Vec::new(),
                            window_ms: 1_000,
                            pane_origin_ms: Some(0),
                            readout_lookback_ms: None,
                        },
                    },
                ),
                (
                    readout,
                    QueryPlanNode::ExactReadout {
                        input: read,
                        readout: ExactReadout::Sum,
                    },
                ),
                (
                    filter,
                    QueryPlanNode::Relational {
                        input: readout,
                    operation: serde_json::json!({"Filter": {"pred": Predicate(Rc::new(QueryExpr::Compare {
                            left: Rc::new(QueryExpr::Column(1)),
                            op: CompareOpKind::Gt,
                            right: Rc::new(QueryExpr::Literal(ScalarValue::Float64(1.0))),
                        }))}}),
                        input_schema: input_schema.clone(),
                        output_schema: input_schema.clone(),
                    },
                ),
                (
                    project,
                    QueryPlanNode::Relational {
                        input: filter,
                        operation: serde_json::to_value(ValueOperation::Project {
                            cols: vec![
                                ProjectItem {
                                    alias: Some("bucket".into()),
                                    expr: QueryExpr::Column(0),
                                },
                                ProjectItem {
                                    alias: Some("score".into()),
                                    expr: QueryExpr::Arithmetic {
                                        op: ArithmeticOpKind::Mul,
                                        left: Rc::new(QueryExpr::Column(1)),
                                        right: Rc::new(QueryExpr::Literal(ScalarValue::Float64(
                                            10.0,
                                        ))),
                                    },
                                },
                            ],
                            qualifier: None,
                        })
                        .unwrap(),
                        input_schema: input_schema,
                        output_schema: projected_schema.clone(),
                    },
                ),
                (
                    sort,
                    QueryPlanNode::Relational {
                        input: project,
                        operation: serde_json::to_value(ValueOperation::Sort {
                            keys: vec![SortKey {
                                expr: QueryExpr::Column(1),
                                ascending: false,
                                nulls_first: false,
                            }],
                            partition_by: GroupKeys::none(),
                        })
                        .unwrap(),
                        input_schema: projected_schema.clone(),
                        output_schema: projected_schema.clone(),
                    },
                ),
                (
                    root,
                    QueryPlanNode::Relational {
                        input: sort,
                        operation: serde_json::to_value(ValueOperation::Limit { n: 1, offset: 0 })
                            .unwrap(),
                        input_schema: projected_schema.clone(),
                        output_schema: projected_schema,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            instant: InstantExecution {
                lookback_ms: 2_000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        let table_schema = Schema::with_time_index(
            vec![
                Column::new("timestamp", DataType::Int64, false),
                Column::new("value", DataType::Float64, false),
            ],
            0,
            vec![],
        );
        let tables = HashMap::from([("requests".into(), table_schema)]);
        let (canonical_sql, template, range) = control_plane::clickhouse::bind_clickhouse_sql(
            &sql,
            &control_plane::clickhouse::ClickHouseSqlCatalog {
                tables: tables.clone(),
            },
            planner_types::types::AccuracyTarget::Exact,
        )
        .await
        .unwrap();
        let window_templates = if range.is_some() {
            BTreeMap::from([(template, vec![canonical_sql.clone()])])
        } else {
            BTreeMap::new()
        };
        let entry = QueryPlanEntry {
            query_id: sql.clone(),
            canonical_query: canonical_sql.clone(),
            language: QueryLanguage::ClickHouseSql,
            fixed_evaluation: Some(FixedEvaluationRange {
                start_ms: 0,
                end_ms,
                cumulative: true,
            }),
            root: executable.root,
            nodes: executable.nodes,
            instant: executable.instant,
            fallback: executable.fallback,
        };
        let query_plan = QueryPlan {
            plan_id: 41,
            plan_version: 1,
            clickhouse_context: Some(ClickHousePlanningContext {
                window_templates,
                tables,
                accuracy: planner_types::types::AccuracyTarget::Exact,
            }),
            selected_dags: Default::default(),
            entries: BTreeMap::from([(
                QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &canonical_sql),
                entry,
            )]),
        };
        store
            .install_summary_catalog(Arc::new(sds.clone()))
            .unwrap();
        if seed {
            store.register(SummarySeriesMetadata {
                storage_handle: 7,
                metric_name: "requests".into(),
                group_by_keys: BTreeSet::new(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: materialization.fingerprint(),
            });
            store.append_precompute(
                7,
                Default::default(),
                (0, 1_000),
                Box::new(SumAccumulator::with_sum(2.0)),
            );
            store.append_precompute(
                7,
                Default::default(),
                (1_000, 2_000),
                Box::new(SumAccumulator::with_sum(3.0)),
            );
        }
        let envelope = asap_types::precompute_plan::PlanEnvelope {
            plan_id: 41,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
            planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
            capability_snapshot_id: "clickhouse-test".into(),
        };
        let mut precompute = asap_types::precompute_plan::PrecomputePlan::build(
            envelope.clone(),
            vec![config],
            &["fixture".into()],
        )
        .unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission = control_plane::physical::compiler::build_transmission_plan(
            envelope.clone(),
            &precompute,
            &BTreeMap::new(),
        )
        .unwrap();
        transmission.summary_catalog = Some(sds.reference().unwrap());
        let active = crate::drivers::query::servers::http::validate_and_build_runtime_plan(
            crate::drivers::query::servers::http::PhysicalPlanInstallRequest {
                summary_catalog: sds,
                collector_plans: vec![],
                precompute_plan: precompute,
                transmission_plan: transmission,
                query_plan,
                storage_routing: None,
                adaptation_evidence: vec![],
            },
            Arc::new(crate::storage_engines::types::BackendStorageRouting::empty()),
        )
        .unwrap();
        let accelerator = CatalogClickHouseAccelerator::with_active_physical_plan(
            store,
            crate::storage_engines::types::ActivePhysicalPlanHandle::new(active),
        );
        let request = ClickHouseQueryRequest {
            method: Method::GET,
            sql,
            body: Bytes::new(),
            parameters: Default::default(),
            headers: HeaderMap::new(),
        };
        (accelerator, request)
    }

    #[tokio::test]
    async fn catalog_hit_executes_bound_summary_store_dag_and_encodes_typed_result() {
        let (accelerator, request) = fixture(2_000).await;
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("expected accelerated response")
        };
        assert_eq!(response.body, "1970-01-01T00:00:02\t50.0\n");
    }

    // A shifted request reads only its bound panes and falls back on a gap.
    #[tokio::test]
    async fn moving_window_binds_request_coverage() {
        let (accelerator, mut request) =
            fixture_with_sql(1_000, Arc::new(SketchStore::new()), true, true).await;
        request.sql =
            "SELECT sum(value) FROM requests WHERE timestamp >= 1000 AND timestamp < 2000".into();
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("shifted covered window must accelerate");
        };
        assert_eq!(response.body, "1970-01-01T00:00:02\t30.0\n");
        // Equivalent inclusive/arithmetic bounds must select the same panes.
        request.sql =
            "SELECT sum(value) FROM requests WHERE timestamp > 1999 - 1000 AND timestamp <= 1999"
                .into();
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("equivalent inclusive window must accelerate");
        };
        assert_eq!(response.body, "1970-01-01T00:00:02\t30.0\n");
        request.sql =
            "SELECT sum(value) FROM requests WHERE timestamp >= 2000 AND timestamp < 3000".into();
        let uncovered = accelerator.execute(&request).await;
        assert!(
            matches!(&uncovered, ClickHouseAccelerationOutcome::Fallback(ClickHouseAccelerationFallback::Execution(detail)) if detail.contains("NoCandidates")),
            "{uncovered:?}"
        );
        request.sql =
            "SELECT sum(value) FROM requests WHERE timestamp >= 0 AND timestamp < 2000".into();
        assert!(matches!(
            accelerator.execute(&request).await,
            ClickHouseAccelerationOutcome::Fallback(ClickHouseAccelerationFallback::CatalogMiss)
        ));
    }

    // A partially covered refresh must not return the available subset.
    #[tokio::test]
    async fn moving_window_partial_coverage_falls_back() {
        let (accelerator, mut request) =
            fixture_with_sql(2_000, Arc::new(SketchStore::new()), true, true).await;
        request.sql =
            "SELECT sum(value) FROM requests WHERE timestamp >= 1000 AND timestamp < 3000".into();
        assert!(matches!(
            accelerator.execute(&request).await,
            ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::IncompleteCoverage
            )
        ));
    }

    // Concrete windows keep their identities; later windows use the shared index.
    #[tokio::test]
    async fn multiple_concrete_windows_share_a_template() {
        let (accelerator, mut request) =
            fixture_with_sql(1_000, Arc::new(SketchStore::new()), true, true).await;
        let active = accelerator.active_physical_plan.as_ref().unwrap();
        let mut snapshot = active.active_snapshot().as_ref().clone();
        let context = snapshot.query_plan.clickhouse_context.as_ref().unwrap();
        let second_sql =
            "SELECT sum(value) FROM requests WHERE timestamp >= 1000 AND timestamp < 2000";
        let (fixed, template, _) = control_plane::clickhouse::bind_clickhouse_sql(
            second_sql,
            &control_plane::clickhouse::ClickHouseSqlCatalog {
                tables: context.tables.clone(),
            },
            context.accuracy.clone(),
        )
        .await
        .unwrap();
        let plan = Arc::make_mut(&mut snapshot.query_plan);
        let mut second = plan.entries.values().next().unwrap().clone();
        second.canonical_query = fixed.clone();
        second.query_id = second_sql.into();
        second.fixed_evaluation = Some(FixedEvaluationRange {
            start_ms: 1000,
            end_ms: 2000,
            cumulative: true,
        });
        plan.entries.insert(
            QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &fixed),
            second,
        );
        plan.clickhouse_context
            .as_mut()
            .unwrap()
            .window_templates
            .get_mut(&template)
            .unwrap()
            .push(fixed);
        plan.validate_against_catalog(snapshot.summary_catalog.as_ref().unwrap())
            .unwrap();
        active.swap(snapshot);
        accelerator.store.append_precompute(
            7,
            Default::default(),
            (2000, 3000),
            Box::new(SumAccumulator::with_sum(5.0)),
        );
        for (sql, expected) in [
            (request.sql.clone(), "1970-01-01T00:00:01\t20.0\n"),
            (second_sql.into(), "1970-01-01T00:00:02\t30.0\n"),
            (
                "SELECT sum(value) FROM requests WHERE timestamp >= 2000 AND timestamp < 3000"
                    .into(),
                "1970-01-01T00:00:03\t50.0\n",
            ),
        ] {
            request.sql = sql;
            let ClickHouseAccelerationOutcome::Accelerated(response) =
                accelerator.execute(&request).await
            else {
                panic!("concrete and refreshed windows must accelerate");
            };
            assert_eq!(response.body, expected);
        }
    }

    // A publication made before time templates keeps its fixed lookup semantics.
    #[tokio::test]
    async fn legacy_bounded_sql_plan_still_executes() {
        let (accelerator, mut request) =
            fixture_with_sql(1_000, Arc::new(SketchStore::new()), true, true).await;
        let active = accelerator.active_physical_plan.as_ref().unwrap();
        let mut snapshot = active.active_snapshot().as_ref().clone();
        let context = snapshot.query_plan.clickhouse_context.as_ref().unwrap();
        let (fixed, _, _) = control_plane::clickhouse::bind_clickhouse_sql(
            &request.sql,
            &control_plane::clickhouse::ClickHouseSqlCatalog {
                tables: context.tables.clone(),
            },
            context.accuracy.clone(),
        )
        .await
        .unwrap();
        let plan = Arc::make_mut(&mut snapshot.query_plan);
        plan.clickhouse_context
            .as_mut()
            .unwrap()
            .window_templates
            .clear();
        let mut entry = plan.entries.pop_first().unwrap().1;
        entry.canonical_query = fixed.clone();
        plan.entries.insert(
            QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &fixed),
            entry,
        );
        active.swap(snapshot);
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("old fixed identity must remain executable");
        };
        assert_eq!(response.body, "1970-01-01T00:00:01\t20.0\n");
        request.sql =
            "SELECT sum(value) FROM requests WHERE timestamp >= 1000 AND timestamp < 2000".into();
        assert!(matches!(
            accelerator.execute(&request).await,
            ClickHouseAccelerationOutcome::Fallback(ClickHouseAccelerationFallback::CatalogMiss)
        ));
    }

    #[tokio::test]
    async fn incomplete_summary_store_coverage_falls_back() {
        let (accelerator, request) = fixture(3_000).await;
        assert!(matches!(
            accelerator.execute(&request).await,
            ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::IncompleteCoverage
            )
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_clickhouse_reader_enters_backfill_service_lifecycle() {
        let Ok(base_url) = std::env::var("CLICKHOUSE_URL") else {
            return;
        };
        let user = std::env::var("CLICKHOUSE_USER").ok();
        let password = std::env::var("CLICKHOUSE_PASSWORD").ok();
        let client = reqwest::Client::new();
        for sql in [
            "CREATE DATABASE IF NOT EXISTS asap_e2e",
            "DROP TABLE IF EXISTS asap_e2e.samples",
            "CREATE TABLE asap_e2e.samples(metric String, labels String, timestamp_ms Int64, value Float64) ENGINE=Memory",
            "INSERT INTO asap_e2e.samples VALUES ('requests','requests',100,2),('errors','errors',1100,3)",
        ] {
            let mut request = client.post(&base_url).body(sql);
            if let Some(user) = &user { request = request.basic_auth(user, password.as_ref()); }
            assert!(request.send().await.unwrap().status().is_success());
        }
        let mut cfg = PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            1,
            1,
            WindowKind::Tumbling,
            String::new(),
            "requests".into(),
            None,
            None,
            None,
        );
        cfg.pane_origin_ms = Some(0);
        cfg.table_name = Some("asap_e2e.samples".into());
        cfg.table_timestamp_column = Some("timestamp_ms".into());
        cfg.value_projection = Some(asap_types::sds::ValueProjectionIdentity::Column {
            name: "value".into(),
        });
        let hot = crate::storage_engines::types::InstalledPrecomputePlanHandle::from_arc(Arc::new(
            crate::storage_engines::types::InstalledPrecomputePlan::new(HashMap::from([(
                cfg.policy_fp_u64(),
                cfg.clone(),
            )])),
        ));
        let registry =
            Arc::new(crate::storage_engines::sketch_db::backfill::BackfillRegistry::new());
        let reader = crate::storage_engines::sketch_db::backfill::ClickHouseReaderConfig {
            base_url,
            database: "asap_e2e".into(),
            table: "samples".into(),
            metric_column: "metric".into(),
            labels_column: "labels".into(),
            timestamp_ms_column: "wrong_deployment_timestamp".into(),
            value_column: "value".into(),
            user,
            password,
        };
        let store = Arc::new(SketchStore::new());
        let service = crate::storage_engines::sketch_db::backfill::BackfillService::new(
            registry.clone(),
            hot,
            crate::storage_engines::sketch_db::backfill::clickhouse_reader_factory(reader),
            crate::storage_engines::sketch_db::backfill::BackfillServiceConfig {
                poll_interval: std::time::Duration::from_millis(10),
            },
        )
        .with_sketch_index(store.clone())
        .with_series_resolver(Arc::new(
            crate::drivers::ingest::series_resolver::SeriesIdResolver::new(),
        ));
        let handle = service.spawn();
        let job = registry.create(
            cfg.policy_fp_u64(),
            (0, 2_000),
            crate::storage_engines::sketch_db::backfill::BackfillSource::ClickHouse {
                database: "asap_e2e".into(),
                table: "samples".into(),
            },
            2,
        );
        for _ in 0..200 {
            if registry
                .get(job)
                .is_some_and(|job| job.status.is_terminal())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        handle.shutdown().await;
        assert_eq!(
            registry.get(job).unwrap().status,
            crate::storage_engines::sketch_db::backfill::BackfillStatus::Complete
        );
        assert!(!store
            .series_ids_for_policy(cfg.policy_fingerprint())
            .is_empty());
        let (accelerator, request) = fixture_with_store(2_000, store, false).await;
        let response = match accelerator.execute(&request).await {
            ClickHouseAccelerationOutcome::Accelerated(response) => response,
            outcome => panic!(
                "published SQL DAG did not read ClickHouse-backfilled SummaryStore state: {outcome:?}"
            ),
        };
        assert_eq!(response.body, "1970-01-01T00:00:02\t50.0\n");
        let mut exact = client
            .post(std::env::var("CLICKHOUSE_URL").unwrap())
            .body("SELECT sum(value) * 10 FROM asap_e2e.samples FORMAT TabSeparated");
        if let Some(user) = std::env::var("CLICKHOUSE_USER").ok() {
            exact = exact.basic_auth(user, std::env::var("CLICKHOUSE_PASSWORD").ok());
        }
        let exact_value: f64 = exact
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let accelerated_value: f64 = std::str::from_utf8(&response.body)
            .unwrap()
            .trim()
            .split('\t')
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(accelerated_value, exact_value);
    }

    #[tokio::test]
    async fn summary_and_external_exact_leaf_compose_in_one_query_dag() {
        let (accelerator, request) = fixture(2_000).await;
        let physical = accelerator
            .active_physical_plan
            .as_ref()
            .unwrap()
            .active_snapshot();
        let entry = mixed_summary_external_entry(
            physical.query_plan.entries.values().next().unwrap().clone(),
        );
        let accelerator = accelerator.with_exact_backend(Arc::new(FixedExactSubtree));
        let prepared = accelerator
            .prepare_external_exact(&entry, 0, 2_000, &request)
            .await
            .unwrap();
        let ClickHouseDagOutcome::Accelerated(result) = execute_sql_dag_with_external(
            accelerator.store.as_ref(),
            &entry,
            physical.summary_catalog.as_ref().unwrap(),
            &prepared,
            0,
            2_000,
            true,
        ) else {
            panic!("mixed summary/external DAG should execute")
        };
        assert_eq!(
            String::from_utf8(result.encode(ClickHouseFormat::TabSeparated).unwrap()).unwrap(),
            "1970-01-01T00:00:02\t0.5\n"
        );
    }

    #[tokio::test]
    async fn real_clickhouse_runtime_differential_uses_production_mixed_constructor() {
        let Ok(base_url) = std::env::var("CLICKHOUSE_URL") else {
            return;
        };
        let (accelerator, mut request) = fixture(2_000).await;
        if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
            request
                .headers
                .insert("x-clickhouse-user", user.parse().unwrap());
        }
        if let Ok(password) = std::env::var("CLICKHOUSE_PASSWORD") {
            request
                .headers
                .insert("x-clickhouse-key", password.parse().unwrap());
        }
        let physical = accelerator
            .active_physical_plan
            .as_ref()
            .unwrap()
            .active_snapshot();
        let mut query_plan = physical.query_plan.as_ref().clone();
        let key = query_plan.entries.keys().next().unwrap().clone();
        let entry = mixed_summary_external_entry(query_plan.entries[&key].clone());
        query_plan.entries.insert(key, entry);
        let mut active = physical.as_ref().clone();
        active.query_plan = Arc::new(query_plan);
        let exact_backend = Arc::new(super::super::fallback::ClickHouseHttpFallback::new(
            base_url,
            "default".into(),
        ));
        let accelerator = CatalogClickHouseAccelerator::with_active_physical_plan_and_exact_backend(
            accelerator.store.clone(),
            crate::storage_engines::types::ActivePhysicalPlanHandle::new(active),
            exact_backend.clone(),
        );
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("real ClickHouse mixed summary/external DAG should accelerate")
        };
        let mut exact_request = request;
        exact_request.method = Method::POST;
        exact_request.sql = "SELECT formatDateTime(toDateTime(2), '%Y-%m-%dT%H:%i:%S') AS timestamp, toFloat64(5) / toFloat64(10) AS ratio FORMAT TabSeparated".into();
        exact_request.body = Bytes::from(exact_request.sql.clone());
        exact_request.parameters.clear();
        let exact = exact_backend.execute(&exact_request).await.unwrap();
        assert_eq!(exact.status, StatusCode::OK);
        assert_eq!(response.body, exact.body);
    }
}
