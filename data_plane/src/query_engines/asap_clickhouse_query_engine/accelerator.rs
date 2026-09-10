//! Catalog-backed ClickHouse acceleration boundary.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use asap_types::{summary_catalog::SummaryCatalog, PolicyFingerprint};
use async_trait::async_trait;
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use control_plane::physical::post_asap::{PhysicalExpr, PostAsapPlan};

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
}

#[derive(Debug, serde::Deserialize)]
pub struct ClickHousePublishedPlan {
    pub sql: String,
    pub runtime: SqlRuntimePlan,
    pub descriptors: SdsDescriptorReferences,
}

/// Independently published ClickHouse planning bundle. SDS remains the
/// descriptor authority; SQL DAG metadata lives in this separate catalog.
#[derive(Debug, serde::Deserialize)]
pub struct ClickHousePlanBundle {
    pub sds: SummaryCatalog,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: planner_types::types::AccuracyTarget,
    pub plans: Vec<ClickHousePublishedPlan>,
}

pub struct CatalogClickHouseAccelerator {
    pub catalog: Arc<SqlPlanCatalog<SqlRuntimePlan>>,
    pub binder: ClickHouseSqlBinder,
    pub store: Arc<SketchStore>,
}

impl CatalogClickHouseAccelerator {
    pub fn from_bundle(
        bundle: ClickHousePlanBundle,
        store: Arc<SketchStore>,
    ) -> Result<Self, super::plan_catalog::SqlPlanCatalogError> {
        let entries = bundle.plans.into_iter().map(|plan| SqlPlanEntry {
            sql_template: plan.sql,
            plan: plan.runtime,
            descriptors: plan.descriptors,
        });
        let generation = SqlPlanCatalogGeneration::build(&bundle.sds, entries)?;
        let plan_id = generation.sds.plan_id;
        let plan_version = generation.sds.plan_version;
        let catalog = Arc::new(SqlPlanCatalog::default());
        catalog.stage(generation)?;
        catalog.activate(plan_id, plan_version)?;
        let sql_catalog = control_plane::clickhouse::ClickHouseSqlCatalog {
            tables: bundle.tables,
        };
        Ok(Self {
            catalog,
            binder: ClickHouseSqlBinder::new(sql_catalog, bundle.accuracy),
            store,
        })
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
        let Some(entry) = self.catalog.lookup(&request.sql) else {
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
        let planned = match self.binder.bind(&request.sql).await {
            Ok(planned) => planned,
            Err(error) => {
                return ClickHouseAccelerationOutcome::Fallback(
                    ClickHouseAccelerationFallback::Planning(error.to_string()),
                )
            }
        };
        let PhysicalExpr::Committed(PostAsapPlan::Summary(node)) = planned.physical else {
            return ClickHouseAccelerationOutcome::Fallback(
                ClickHouseAccelerationFallback::Execution("unsupported physical placement".into()),
            );
        };
        match execute_sql_dag(
            self.store.as_ref(),
            node.as_ref(),
            entry.plan.start_ms,
            entry.plan.end_ms,
            entry.plan.cumulative,
            entry.plan.materializations.clone(),
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
