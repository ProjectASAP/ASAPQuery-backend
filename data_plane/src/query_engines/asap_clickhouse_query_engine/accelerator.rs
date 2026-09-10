//! Catalog-backed ClickHouse acceleration boundary.

use async_trait::async_trait;
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use std::sync::Arc;

use super::{
    clickhouse_result_adapter::ClickHouseFormat,
    execution::{execute_sql_dag, ClickHouseDagFallback, ClickHouseDagOutcome},
    fallback::ClickHouseRawResponse,
    request::ClickHouseQueryRequest,
    server::{
        ClickHouseAccelerationFallback, ClickHouseAccelerationOutcome, ClickHouseAccelerator,
    },
};
use crate::storage_engines::sketch_db::index::SketchStore;

pub struct CatalogClickHouseAccelerator {
    pub store: Arc<SketchStore>,
    active_physical_plan: Option<crate::storage_engines::types::HotReloadActivePhysicalPlan>,
}

impl CatalogClickHouseAccelerator {
    pub fn empty(store: Arc<SketchStore>) -> Self {
        Self {
            store,
            active_physical_plan: None,
        }
    }

    pub fn with_active_physical_plan(
        store: Arc<SketchStore>,
        active: crate::storage_engines::types::HotReloadActivePhysicalPlan,
    ) -> Self {
        let mut accelerator = Self::empty(store);
        accelerator.active_physical_plan = Some(active);
        accelerator
    }
}

fn requested_format(request: &ClickHouseQueryRequest) -> Result<ClickHouseFormat, String> {
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
        let Some(physical) = self.active_physical_plan.as_ref().map(|h| h.snapshot()) else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
        let Some(context) = physical.query_plan.clickhouse_context.as_ref() else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
        let canonical_sql = match control_plane::clickhouse::canonicalize_clickhouse_sql(
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
        let Ok(entry) = physical.query_plan.lookup_clickhouse(&canonical_sql) else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
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
        let Some(range) = entry.fixed_evaluation else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::Execution(
                    "ClickHouse plan is missing its fixed evaluation range".into(),
                ),
            );
        };
        match execute_sql_dag(
            self.store.as_ref(),
            entry,
            catalog.as_ref(),
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
    use crate::{
        precompute_engine::operators::SumAccumulator,
        storage_engines::sketch_db::index::{AggKind, Capability, SketchInstanceMetadata},
    };
    use asap_types::summary_catalog::SummaryCatalog;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use axum::http::Method;
    use control_plane::query_plan::{
        ClickHousePlanningContext, ExactReadout, FallbackPolicy, FixedEvaluationRange,
        InstantExecution, MaterializationBinding, PhysicalGrouping, QueryLanguage, QueryNodeId,
        QueryPlan, QueryPlanEntry, QueryPlanNode,
    };
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
            None,
            None,
        );
        config.pane_origin_ms = Some(0);
        let sds = SummaryCatalog::from_materializations(41, 1, &[config.clone()]).unwrap();
        let materialization = *sds.materializations.keys().next().unwrap();
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
                            materialization,
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
                Column::new("timestamp", DataType::Timestamp, false),
                Column::new("value", DataType::Float64, false),
            ],
            0,
            vec![],
        );
        let tables = HashMap::from([("requests".into(), table_schema)]);
        let canonical_sql = control_plane::clickhouse::canonicalize_clickhouse_sql(
            "SELECT sum(value) FROM requests",
            &control_plane::clickhouse::ClickHouseSqlCatalog {
                tables: tables.clone(),
            },
            planner_types::types::AccuracyTarget::Exact,
        )
        .await
        .unwrap();
        let entry = QueryPlanEntry {
            query_id: "SELECT sum(value) FROM requests".into(),
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
                tables,
                accuracy: planner_types::types::AccuracyTarget::Exact,
            }),
            entries: BTreeMap::from([(
                QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &canonical_sql),
                entry,
            )]),
        };
        store
            .install_summary_catalog(Arc::new(sds.clone()))
            .unwrap();
        store.register(SketchInstanceMetadata {
            sid: 7,
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
        if seed {
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
        let envelope = control_plane::physical::compiler::PlanEnvelope {
            plan_id: 41,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: control_plane::physical::compiler::BACKEND_COMPAT.into(),
            planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
            capability_snapshot_id: "clickhouse-test".into(),
        };
        let mut precompute = control_plane::physical::compiler::PrecomputePlan::build(
            envelope.clone(),
            vec![config],
            &["fixture".into()],
        )
        .unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission = control_plane::physical::compiler::TransmissionPlan::build(
            envelope.clone(),
            &precompute,
            &BTreeMap::new(),
        )
        .unwrap();
        transmission.summary_catalog = Some(sds.reference().unwrap());
        let active = crate::drivers::query::servers::http::build_active_physical_plan(
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
            crate::storage_engines::types::HotReloadActivePhysicalPlan::new(active),
        );
        let request = ClickHouseQueryRequest {
            method: Method::GET,
            sql: "SELECT sum(value) FROM requests".into(),
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
            "INSERT INTO asap_e2e.samples VALUES ('requests','requests',100,2),('requests','requests',1100,3)",
        ] {
            let mut request = client.post(&base_url).body(sql);
            if let Some(user) = &user { request = request.basic_auth(user, password.as_ref()); }
            assert!(request.send().await.unwrap().status().is_success());
        }
        let cfg = PrecomputeMaterialization::new(
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
        let hot = crate::storage_engines::types::HotReloadStreamingConfig::from_arc(Arc::new(
            crate::storage_engines::types::StreamingConfig::new(HashMap::from([(
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
            timestamp_ms_column: "timestamp_ms".into(),
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
            crate::storage_engines::sketch_db::backfill::BackfillSource::Prometheus {
                url: "clickhouse://configured".into(),
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
        assert!(!store.sids_for_policy(cfg.policy_fingerprint()).is_empty());
        let (accelerator, request) = fixture_with_store(2_000, store, false).await;
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("published SQL DAG did not read ClickHouse-backfilled SummaryStore state")
        };
        assert_eq!(response.body, "1970-01-01T00:00:02\t50.0\n");
        let mut exact = client.post(std::env::var("CLICKHOUSE_URL").unwrap()).body(
            "SELECT sum(value) * 10 FROM asap_e2e.samples WHERE metric='requests' FORMAT TabSeparated",
        );
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
}
