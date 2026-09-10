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
    pub sql: String,
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

pub struct CatalogClickHouseAccelerator {
    pub catalog: Arc<SqlPlanCatalog<SqlRuntimePlan>>,
    binder: RwLock<Option<ClickHouseSqlBinder>>,
    staged_binder: Mutex<Option<(u64, u64, ClickHouseSqlBinder)>>,
    publication: RwLock<()>,
    pub store: Arc<SketchStore>,
}

impl CatalogClickHouseAccelerator {
    pub fn empty(store: Arc<SketchStore>) -> Self {
        Self {
            catalog: Arc::new(SqlPlanCatalog::default()),
            binder: RwLock::new(None),
            staged_binder: Mutex::new(None),
            publication: RwLock::new(()),
            store,
        }
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
        let (entry, generation) = {
            let _publication = self.publication.read().unwrap();
            (self.catalog.lookup(&request.sql), self.catalog.active())
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
        precompute_engine::operators::SumAccumulator,
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
            ArithmeticOpKind, CompareOpKind, DataType, GroupKeys, Predicate, ProjectItem,
            QueryExpr, ScalarValue, SortKey,
        },
    };
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

    fn fixture(end_ms: u64) -> (CatalogClickHouseAccelerator, ClickHouseQueryRequest) {
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
                            materialization,
                            output_grouping: PhysicalGrouping::Reduce(Vec::new()),
                            window_ms: 1_000,
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
                        operation: serde_json::to_value(ValueOperation::Filter {
                            pred: Predicate(Rc::new(QueryExpr::Compare {
                                left: Rc::new(QueryExpr::Column(1)),
                                op: CompareOpKind::Gt,
                                right: Rc::new(QueryExpr::Literal(ScalarValue::Float64(1.0))),
                            })),
                        })
                        .unwrap(),
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
        let bundle = ClickHousePlanBundle {
            sds: sds.clone(),
            tables: HashMap::new(),
            accuracy: planner_types::types::AccuracyTarget::Exact,
            plans: vec![ClickHousePublishedPlan {
                sql: "SELECT sum(value) FROM requests".into(),
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
        let store = Arc::new(SketchStore::new());
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
        let (accelerator, request) = fixture(2_000);
        let ClickHouseAccelerationOutcome::Accelerated(response) =
            accelerator.execute(&request).await
        else {
            panic!("expected accelerated response")
        };
        assert_eq!(response.body, "1970-01-01T00:00:02\t50.0\n");
    }

    #[tokio::test]
    async fn incomplete_summary_store_coverage_falls_back() {
        let (accelerator, request) = fixture(3_000);
        assert!(matches!(
            accelerator.execute(&request).await,
            ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::IncompleteCoverage
            )
        ));
    }
}
