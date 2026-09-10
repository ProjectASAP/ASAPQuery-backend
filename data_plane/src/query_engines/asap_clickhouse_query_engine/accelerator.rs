//! Catalog-backed ClickHouse acceleration boundary.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, RwLock},
};

use asap_types::{summary_catalog::SummaryCatalog, PolicyFingerprint};
use async_trait::async_trait;
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use control_plane::query_plan::ExecutableQueryPlan;

use super::{
    clickhouse_result_adapter::ClickHouseFormat,
    execution::{execute_sql_dag, ClickHouseDagFallback, ClickHouseDagOutcome},
    fallback::ClickHouseRawResponse,
    plan_catalog::{
        SdsDescriptorReferences, SqlPlanCatalog, SqlPlanCatalogGeneration, SqlPlanEntry,
    },
    request::ClickHouseQueryRequest,
    server::{
        ClickHouseAccelerationFallback, ClickHouseAccelerationOutcome, ClickHouseAccelerator,
    },
    sql_binder::ClickHouseSqlBinder,
};
use crate::storage_engines::sketch_db::index::SketchStore;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqlRuntimePlan {
    pub start_ms: u64,
    pub end_ms: u64,
    pub cumulative: bool,
    pub materializations: BTreeSet<PolicyFingerprint>,
    /// Control-plane compiled and materialization-bound executable DAG.
    pub executable: ExecutableQueryPlan,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ClickHousePublishedPlan {
    /// Exact ClickHouse request/fallback template used as the lookup key.
    pub sql: String,
    /// ASAPPlanner canonical identity of the explicitly equivalent planning
    /// SQL. Retained on wire for audit; serving never executes it as fallback.
    #[serde(default)]
    pub planning_sql: String,
    pub runtime: SqlRuntimePlan,
    pub descriptors: SdsDescriptorReferences,
}

/// Independently published ClickHouse planning bundle. SDS remains the
/// descriptor authority; SQL DAG metadata lives in this separate catalog.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ClickHousePlanBundle {
    pub sds: SummaryCatalog,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: planner_types::types::AccuracyTarget,
    pub plans: Vec<ClickHousePublishedPlan>,
}

#[derive(Clone, Debug)]
pub struct ClickHouseActiveGeneration {
    pub catalog: Arc<SqlPlanCatalogGeneration<SqlRuntimePlan>>,
    pub binder: ClickHouseSqlBinder,
}

pub fn build_active_generation(
    bundle: ClickHousePlanBundle,
    physical_sds: &SummaryCatalog,
) -> Result<ClickHouseActiveGeneration, super::plan_catalog::SqlPlanCatalogError> {
    if bundle
        .sds
        .reference()
        .map_err(|e| super::plan_catalog::SqlPlanCatalogError::InvalidSds(e.to_string()))?
        != physical_sds
            .reference()
            .map_err(|e| super::plan_catalog::SqlPlanCatalogError::InvalidSds(e.to_string()))?
    {
        return Err(super::plan_catalog::SqlPlanCatalogError::InvalidSds(
            "SQL sidecar SDS differs from physical publication SDS".into(),
        ));
    }
    for plan in &bundle.plans {
        crate::query_engines::asap_query_engine::catalog_resolver::validate_payload(
            Some(physical_sds),
            &plan.runtime.executable,
            physical_sds.plan_id,
            physical_sds.plan_version,
        )
        .map_err(|e| super::plan_catalog::SqlPlanCatalogError::InvalidSds(e.to_string()))?;
    }
    let entries = bundle.plans.into_iter().map(|plan| SqlPlanEntry {
        sql_template: plan.sql,
        plan: plan.runtime,
        descriptors: plan.descriptors,
    });
    let generation = SqlPlanCatalogGeneration::build(physical_sds, entries)?;
    Ok(ClickHouseActiveGeneration {
        catalog: Arc::new(generation),
        binder: ClickHouseSqlBinder::new(
            control_plane::clickhouse::ClickHouseSqlCatalog {
                tables: bundle.tables,
            },
            bundle.accuracy,
        ),
    })
}

pub struct CatalogClickHouseAccelerator {
    pub catalog: Arc<SqlPlanCatalog<SqlRuntimePlan>>,
    binder: RwLock<Option<ClickHouseSqlBinder>>,
    staged_binder: Mutex<Option<(u64, u64, ClickHouseSqlBinder)>>,
    publication: RwLock<()>,
    pub store: Arc<SketchStore>,
    active_physical_plan: Option<crate::storage_engines::types::HotReloadActivePhysicalPlan>,
}

impl CatalogClickHouseAccelerator {
    pub fn empty(store: Arc<SketchStore>) -> Self {
        Self {
            catalog: Arc::new(SqlPlanCatalog::default()),
            binder: RwLock::new(None),
            staged_binder: Mutex::new(None),
            publication: RwLock::new(()),
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

    pub fn from_bundle(
        bundle: ClickHousePlanBundle,
        store: Arc<SketchStore>,
    ) -> Result<Self, super::plan_catalog::SqlPlanCatalogError> {
        let accelerator = Self::empty(store);
        let staged = accelerator.stage_bundle(bundle)?;
        accelerator.activate(staged.plan_id, staged.plan_version)?;
        Ok(accelerator)
    }

    pub fn stage_bundle(
        &self,
        bundle: ClickHousePlanBundle,
    ) -> Result<super::plan_catalog::SqlPlanCatalogAck, super::plan_catalog::SqlPlanCatalogError>
    {
        let _publication = self.publication.write().unwrap();
        for plan in &bundle.plans {
            crate::query_engines::asap_query_engine::catalog_resolver::validate_payload(
                Some(&bundle.sds),
                &plan.runtime.executable,
                bundle.sds.plan_id,
                bundle.sds.plan_version,
            )
            .map_err(|error| {
                super::plan_catalog::SqlPlanCatalogError::InvalidSds(error.to_string())
            })?;
        }
        let entries = bundle.plans.into_iter().map(|plan| SqlPlanEntry {
            sql_template: plan.sql,
            plan: plan.runtime,
            descriptors: plan.descriptors,
        });
        let generation = SqlPlanCatalogGeneration::build(&bundle.sds, entries)?;
        let plan_id = generation.sds.plan_id;
        let plan_version = generation.sds.plan_version;
        let binder = ClickHouseSqlBinder::new(
            control_plane::clickhouse::ClickHouseSqlCatalog {
                tables: bundle.tables,
            },
            bundle.accuracy,
        );
        let ack = self.catalog.stage(generation)?;
        *self.staged_binder.lock().unwrap() = Some((plan_id, plan_version, binder));
        Ok(ack)
    }

    pub fn activate(
        &self,
        plan_id: u64,
        plan_version: u64,
    ) -> Result<super::plan_catalog::SqlPlanCatalogAck, super::plan_catalog::SqlPlanCatalogError>
    {
        let _publication = self.publication.write().unwrap();
        let mut staged = self.staged_binder.lock().unwrap();
        let Some((staged_id, staged_version, _)) = staged.as_ref() else {
            return Err(super::plan_catalog::SqlPlanCatalogError::NotStaged {
                plan_id,
                plan_version,
            });
        };
        if (*staged_id, *staged_version) != (plan_id, plan_version) {
            return Err(super::plan_catalog::SqlPlanCatalogError::NotStaged {
                plan_id,
                plan_version,
            });
        }
        let ack = self.catalog.activate(plan_id, plan_version)?;
        let (_, _, binder) = staged.take().expect("staged binder checked above");
        *self.binder.write().unwrap() = Some(binder);
        Ok(ack)
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
        let physical = self.active_physical_plan.as_ref().map(|h| h.snapshot());
        let sidecar = physical.as_ref().and_then(|p| p.clickhouse_sql.clone());
        let binder = sidecar
            .as_ref()
            .map(|s| s.binder.clone())
            .or_else(|| self.binder.read().unwrap().clone());
        if binder.is_none() {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        }
        // Lookup uses the exact ClickHouse request template published by the
        // control plane. Do not reparse it here: publication may deliberately
        // pair ClickHouse-only exact SQL with equivalent Planner SQL.
        let canonical_sql = control_plane::clickhouse::sql_request_template_identity(&request.sql);
        let (entry, generation) = if let Some(sidecar) = sidecar {
            (
                sidecar.catalog.lookup(&canonical_sql),
                Some(sidecar.catalog.clone()),
            )
        } else {
            let _publication = self.publication.read().unwrap();
            (self.catalog.lookup(&canonical_sql), self.catalog.active())
        };
        let Some(entry) = entry else {
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
        let Some(generation) = generation else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::CatalogMiss,
            );
        };
        match execute_sql_dag(
            self.store.as_ref(),
            &entry.plan.executable,
            generation.catalog.as_ref(),
            entry.plan.start_ms,
            entry.plan.end_ms,
            entry.plan.cumulative,
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
        precompute_engine::operators::{MinMaxAccumulator, SumAccumulator},
        storage_engines::sketch_db::index::{AggKind, Capability, SketchInstanceMetadata},
    };
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use axum::http::Method;
    use control_plane::query_plan::{
        ExactReadout, FallbackPolicy, InstantExecution, MaterializationBinding, PhysicalGrouping,
        QueryNodeId, QueryPlanNode,
    };
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema, ValueOperation},
        pre_asap::{
            ArithmeticOpKind, Column, CompareOpKind, DataType, GroupKeys, Predicate, ProjectItem,
            QueryExpr, ScalarValue, Schema, SortKey,
        },
    };
    use std::collections::BTreeMap;
    use std::rc::Rc;

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
        let config = PrecomputeMaterialization::new(
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
        let sds = SummaryCatalog::from_materializations(41, 1, &[config]).unwrap();
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
        let executable = ExecutableQueryPlan {
            root,
            nodes: [
                (
                    read,
                    QueryPlanNode::ReadMaterialization {
                        binding: MaterializationBinding {
                            item_labels: Default::default(),
                            materialization,
                            output_grouping: PhysicalGrouping::Reduce(Vec::new()),
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
        let descriptors = SdsDescriptorReferences {
            summaries: sds.summary_descriptors.keys().cloned().collect(),
            data: sds.data_descriptors.keys().cloned().collect(),
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
        let canonical_sql = control_plane::clickhouse::sql_request_template_identity(
            "SELECT sum(value) FROM requests",
        );
        let bundle = ClickHousePlanBundle {
            sds: sds.clone(),
            tables,
            accuracy: planner_types::types::AccuracyTarget::Exact,
            plans: vec![ClickHousePublishedPlan {
                sql: canonical_sql,
                planning_sql: String::new(),
                runtime: SqlRuntimePlan {
                    start_ms: 0,
                    end_ms,
                    cumulative: true,
                    materializations: BTreeSet::from([materialization.fingerprint()]),
                    executable,
                },
                descriptors,
            }],
        };
        store.install_summary_catalog(Arc::new(sds)).unwrap();
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
        let accelerator = CatalogClickHouseAccelerator::from_bundle(bundle, store).unwrap();
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
    async fn q05_compiled_sidecar_executes_grouped_max_as_typed_clickhouse_result() {
        let start_ms = 1_788_848_096_000;
        let end_ms = 1_788_891_296_000;
        let mut config = PrecomputeMaterialization::new(
            AggregationType::MinMax,
            "max".into(),
            Default::default(),
            KeyByLabelNames::new(vec!["labels".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            43_200,
            43_200,
            WindowKind::Tumbling,
            String::new(),
            "cache_refresh_lag_seconds".into(),
            None,
            Some("raw_samples".into()),
            Some("value".into()),
        );
        config.pane_origin_ms = Some(start_ms as i64);
        let sds = SummaryCatalog::from_materializations(72, 1, &[config.clone()]).unwrap();
        let envelope = control_plane::physical::compiler::PlanEnvelope {
            plan_id: 72,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: "test".into(),
            planner_revision: "test".into(),
            capability_snapshot_id: "test".into(),
        };
        let mut precompute =
            control_plane::physical::compiler::PrecomputePlan::build_backend_local(
                envelope.clone(),
                vec![config.clone()],
            )
            .unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission = control_plane::physical::compiler::TransmissionPlan::build(
            envelope,
            &precompute,
            &Default::default(),
        )
        .unwrap();
        transmission.summary_catalog = precompute.summary_catalog.clone();
        let schema = Schema::with_time_index(
            vec![
                Column::new("metric", DataType::Utf8, false),
                Column::new("labels", DataType::Utf8, false),
                Column::new("ts_ms", DataType::Timestamp, false),
                Column::new("value", DataType::Float64, false),
            ],
            2,
            vec![],
        );
        let sql = "SELECT labels, max(value) AS value FROM raw_samples WHERE metric='cache_refresh_lag_seconds' AND ts_ms>1788848096000 AND ts_ms<=1788891296000 GROUP BY labels ORDER BY labels";
        let compiled = control_plane::clickhouse::compile_clickhouse_workload(
            &control_plane::clickhouse::ClickHouseSqlWorkload {
                sds: sds.clone(),
                precompute_plan: precompute,
                transmission_plan: transmission,
                tables: HashMap::from([("raw_samples".into(), schema)]),
                accuracy: planner_types::types::AccuracyTarget::Exact,
                queries: vec![control_plane::clickhouse::ClickHouseSqlWorkloadEntry {
                    sql: sql.into(),
                    start_ms,
                    end_ms,
                    cumulative: true,
                }],
            },
        )
        .await
        .unwrap();
        let bundle: ClickHousePlanBundle =
            serde_json::from_value(serde_json::to_value(compiled).unwrap()).unwrap();
        let materialization = config.policy_fingerprint();
        let store = Arc::new(SketchStore::new());
        store.install_summary_catalog(Arc::new(sds)).unwrap();
        store.register(SketchInstanceMetadata {
            sid: 72,
            metric_name: config.metric.clone(),
            group_by_keys: BTreeSet::from(["labels".into()]),
            capability: Some(Capability::ExactAgg(AggregationType::MinMax)),
            agg_kind: AggKind::ExactAgg {
                agg_type: AggregationType::MinMax,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: start_ms as i64,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: materialization,
        });
        for (labels, value) in [("{instance=a}", 7.0), ("{instance=b}", 11.0)] {
            store.append_precompute(
                72,
                BTreeMap::from([("labels".into(), labels.into())]),
                (start_ms, end_ms),
                Box::new(MinMaxAccumulator::with_value(value, "max".into())),
            );
        }
        let accelerator = CatalogClickHouseAccelerator::from_bundle(bundle, store).unwrap();
        let request = ClickHouseQueryRequest {
            method: Method::GET,
            sql: sql.into(),
            body: Bytes::new(),
            parameters: Default::default(),
            headers: HeaderMap::new(),
        };
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("compiled q05 sidecar did not reach the warm executor")
        };
        assert_eq!(
            response.headers["content-type"],
            "text/tab-separated-values; charset=UTF-8"
        );
        assert_eq!(
            std::str::from_utf8(&response.body).unwrap(),
            "{instance=a}\t7.0\n{instance=b}\t11.0\n"
        );
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
