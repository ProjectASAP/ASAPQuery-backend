//! One atomic, bounded-stale snapshot task per SQL population and plan generation.
//! Rows form a multiset: equal rows are distinct members, and a new complete
//! snapshot retracts disappeared rows without inventing a table primary key.
use super::{
    fallback::ClickHouseExactBackend,
    relational_adapter::{fields_from_schema, Cell, ClickHouseRelation},
    request::ClickHouseQueryRequest,
};
use asap_types::query_plan::table_rows::TableRowsPopulation;
use planner_types::post_asap::{maintained_population::PopulationReadout, SummarySchema};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Snapshot {
    members: BTreeMap<String, (Vec<Cell>, usize)>,
    groups: BTreeMap<String, Vec<Vec<Cell>>>,
    epoch: u64,
    observed: Option<Instant>,
}
impl Snapshot {
    fn replace(
        &mut self,
        population: &TableRowsPopulation,
        relation: ClickHouseRelation,
        observed: Instant,
    ) -> Result<(), String> {
        let policy = population
            .maintenance
            .as_ref()
            .ok_or("missing maintenance policy")?;
        if relation.rows.len() > policy.max_rows {
            return Err("table snapshot exceeds row budget".into());
        }
        let mut bytes = 0usize;
        let mut members = BTreeMap::new();
        for row in relation.rows {
            if !matches!(row.get(population.value_column), Some(Cell::Float64(v)) if v.is_finite())
            {
                return Err("table snapshot contains invalid or nonfinite value".into());
            }
            let key = serde_json::to_string(&row).map_err(|e| e.to_string())?;
            bytes = bytes
                .saturating_add(key.len().saturating_mul(4))
                .saturating_add(row.len().saturating_mul(256));
            if bytes > policy.max_bytes / 4 {
                return Err("table snapshot exceeds memory budget".into());
            }
            let member = members.entry(key).or_insert((row, 0usize));
            member.1 += 1;
        }
        if observed.elapsed().as_millis() > u128::from(policy.max_snapshot_age_ms) {
            return Err("complete table snapshot is already stale".into());
        }
        if self.members != members {
            let mut groups: BTreeMap<String, Vec<Vec<Cell>>> = BTreeMap::new();
            for (row, count) in members.values() {
                let group = population
                    .grouping
                    .iter()
                    .map(|k| &row[*k])
                    .collect::<Vec<_>>();
                let key = serde_json::to_string(&group).map_err(|e| e.to_string())?;
                let rows = groups.entry(key).or_default();
                rows.extend(std::iter::repeat_n(row.clone(), *count));
            }
            for rows in groups.values_mut() {
                rows.sort_by(|a, b| {
                    value(a, population.value_column).total_cmp(&value(b, population.value_column))
                });
            }
            self.members = members;
            self.groups = groups;
        }
        // A complete empty snapshot is evidence too; a failed/partial fetch is not.
        self.epoch = self
            .epoch
            .checked_add(1)
            .ok_or("table snapshot epoch exhausted")?;
        self.observed = Some(observed);
        Ok(())
    }

    fn read(
        &self,
        population: &TableRowsPopulation,
        readout: &PopulationReadout,
        output: &SummarySchema,
    ) -> Result<ClickHouseRelation, String> {
        population.validate_output(readout, output)?;
        if self.observed.is_none_or(|t| {
            t.elapsed().as_millis()
                > u128::from(population.maintenance.as_ref().unwrap().max_snapshot_age_ms)
        }) {
            return Err("table population lacks a fresh complete snapshot".into());
        }
        let empty = Vec::new();
        let groups = if self.groups.is_empty() && population.grouping.is_empty() {
            vec![&empty]
        } else {
            self.groups.values().collect()
        };
        let mut result = Vec::new();
        for rows in groups {
            if let PopulationReadout::TopK { k } = readout {
                result.extend(rows.iter().rev().take(*k).cloned());
                continue;
            }
            let mut row = population
                .grouping
                .iter()
                .map(|k| rows[0][*k].clone())
                .collect::<Vec<_>>();
            let cell = match readout {
                PopulationReadout::Count => Cell::UInt64(rows.len() as u64),
                PopulationReadout::Quantile { q } => {
                    let estimate = if rows.is_empty() {
                        f64::NAN
                    } else {
                        let rank = q * (rows.len() - 1) as f64;
                        let lo = rank.floor() as usize;
                        let hi = rank.ceil() as usize;
                        let fraction = rank.fract();
                        let a = value(&rows[lo], population.value_column);
                        let b = value(&rows[hi], population.value_column);
                        a * (1.0 - fraction) + b * fraction
                    };
                    Cell::Float64(estimate)
                }
                PopulationReadout::Sum | PopulationReadout::Average => {
                    let mut sum = 0.0;
                    for row in rows {
                        sum += value(row, population.value_column);
                    }
                    if !sum.is_finite() {
                        return Err("table sum overflow requires native evaluation".into());
                    }
                    Cell::Float64(if matches!(readout, PopulationReadout::Average) {
                        sum / rows.len() as f64
                    } else {
                        sum
                    })
                }
                PopulationReadout::TopK { .. } => unreachable!(),
            };
            row.push(cell);
            result.push(row);
        }
        Ok(ClickHouseRelation {
            rows: result,
            fields: fields_from_schema(output),
            coverage: None,
        })
    }
}
fn value(row: &[Cell], column: usize) -> f64 {
    let Cell::Float64(v) = row[column] else {
        unreachable!("validated table value")
    };
    v
}

struct Job {
    snapshot: Mutex<Snapshot>,
    max_bytes: usize,
    abort: Mutex<Option<tokio::task::AbortHandle>>,
}
impl Drop for Job {
    fn drop(&mut self) {
        if let Some(handle) = self.abort.get_mut().unwrap().take() {
            handle.abort();
        }
    }
}

#[derive(Default)]
pub struct TableRowsRuntime {
    jobs: Mutex<BTreeMap<String, Arc<Job>>>,
}
pub struct TableRowsReadContext {
    pub generation: (u64, u64),
    pub backend: Arc<dyn ClickHouseExactBackend>,
    pub active: crate::storage_engines::types::HotReloadActivePhysicalPlan,
}
impl TableRowsRuntime {
    pub fn read(
        &self,
        context: TableRowsReadContext,
        population: &TableRowsPopulation,
        readout: &PopulationReadout,
        output: &SummarySchema,
    ) -> Result<ClickHouseRelation, String> {
        population.validate_output(readout, output)?;
        let TableRowsReadContext {
            generation,
            backend,
            active,
        } = context;
        let database = Some(population.maintenance.as_ref().unwrap().database.as_str());
        let prefix = format!("{}:{}:", generation.0, generation.1);
        let key = format!(
            "{prefix}{}:{}",
            serde_json::to_string(&database).unwrap(),
            population.key()
        );
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| "table task registry poisoned")?;
        let current = active.snapshot();
        if (current.query_plan.plan_id, current.query_plan.plan_version) != generation {
            return Err("table population plan generation changed".into());
        }
        jobs.retain(|k, _| k.starts_with(&prefix));
        if !jobs.contains_key(&key) {
            if jobs.len() >= 64
                || jobs
                    .values()
                    .map(|j| j.max_bytes)
                    .sum::<usize>()
                    .saturating_add(population.maintenance.as_ref().unwrap().max_bytes)
                    > 1_073_741_824
            {
                return Err("table population task limit exceeded".into());
            }
            let job = Arc::new(Job {
                snapshot: Mutex::new(Snapshot::default()),
                max_bytes: population.maintenance.as_ref().unwrap().max_bytes,
                abort: Mutex::new(None),
            });
            let weak = Arc::downgrade(&job);
            let population = population.clone();
            let database = database.map(str::to_owned);
            let handle = tokio::spawn(async move {
                loop {
                    let current = active.snapshot();
                    if (current.query_plan.plan_id, current.query_plan.plan_version) != generation {
                        break;
                    }
                    let observed = Instant::now();
                    let fetched = tokio::time::timeout(
                        Duration::from_millis(
                            population.maintenance.as_ref().unwrap().max_snapshot_age_ms,
                        ),
                        fetch(&population, backend.as_ref(), database.as_deref()),
                    )
                    .await
                    .map_err(|_| "table snapshot request timed out".to_owned())
                    .and_then(|result| result);
                    {
                        let Some(job) = weak.upgrade() else {
                            break;
                        };
                        let mut snapshot = job.snapshot.lock().expect("table snapshot mutex");
                        if fetched
                            .and_then(|relation| snapshot.replace(&population, relation, observed))
                            .is_err()
                        {
                            snapshot.observed = None;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(
                        population.maintenance.as_ref().unwrap().refresh_interval_ms,
                    ))
                    .await;
                }
            });
            *job.abort.lock().unwrap() = Some(handle.abort_handle());
            jobs.insert(key.clone(), job);
        }
        let job = Arc::clone(&jobs[&key]);
        drop(jobs);
        let result = job
            .snapshot
            .try_lock()
            .map_err(|_| "table snapshot update in progress")?
            .read(population, readout, output);
        result
    }
}

async fn fetch(
    population: &TableRowsPopulation,
    backend: &dyn ClickHouseExactBackend,
    database: Option<&str>,
) -> Result<ClickHouseRelation, String> {
    let policy = population
        .maintenance
        .as_ref()
        .ok_or("missing table policy")?;
    let mut parameters = BTreeMap::from([
        ("default_format".into(), "JSONCompact".into()),
        ("output_format_json_quote_64bit_integers".into(), "0".into()),
        ("output_format_json_quote_denormals".into(), "1".into()),
        (
            "output_format_json_map_as_array_of_tuples".into(),
            "1".into(),
        ),
        (
            "output_format_json_named_tuples_as_objects".into(),
            "0".into(),
        ),
        ("max_result_rows".into(), policy.max_rows.to_string()),
        ("max_result_bytes".into(), policy.max_bytes.to_string()),
        ("result_overflow_mode".into(), "throw".into()),
    ]);
    if let Some(database) = database {
        parameters.insert("database".into(), database.into());
    }
    let request = ClickHouseQueryRequest {
        sql: population.snapshot_sql.clone(),
        parameters,
        method: axum::http::Method::POST,
        body: axum::body::Bytes::from(population.snapshot_sql.clone()),
        headers: axum::http::HeaderMap::new(),
    };
    let response = backend.execute(&request).await.map_err(|e| e.to_string())?;
    if !response.status.is_success() {
        return Err("ClickHouse snapshot request failed".into());
    }
    ClickHouseRelation::from_json_compact(&population.input_schema, &response.body)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::query_plan::table_rows::TableRowsMaintenance;
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField},
        pre_asap::DataType,
    };
    fn schema() -> SummarySchema {
        SummarySchema {
            time_index: None,
            fields: vec![SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            }],
        }
    }
    fn population() -> TableRowsPopulation {
        TableRowsPopulation {
            snapshot_sql: "SELECT value FROM rows".into(),
            input_schema: schema(),
            value_column: 0,
            grouping: vec![],
            max_k: 4,
            quantiles: true,
            maintenance: Some(TableRowsMaintenance {
                database: "default".into(),
                refresh_interval_ms: 10,
                max_snapshot_age_ms: 1000,
                max_rows: 100,
                max_bytes: 100_000,
            }),
        }
    }
    fn relation(values: &[f64]) -> ClickHouseRelation {
        ClickHouseRelation {
            rows: values.iter().map(|v| vec![Cell::Float64(*v)]).collect(),
            fields: fields_from_schema(&schema()),
            coverage: None,
        }
    }
    fn scalar(snapshot: &Snapshot, p: &TableRowsPopulation, q: f64) -> f64 {
        let r = snapshot
            .read(p, &PopulationReadout::Quantile { q }, &schema())
            .unwrap();
        value(&r.rows[0], 0)
    }
    /// Complete replacement retains duplicate multiplicity and retracts removed/updated values.
    #[test]
    fn table_snapshots_retract_updates_deletes_and_keep_duplicates() {
        let p = population();
        let mut s = Snapshot::default();
        s.replace(&p, relation(&[1.0, 1.0, 9.0]), Instant::now())
            .unwrap();
        assert_eq!(scalar(&s, &p, 0.5), 1.0);
        s.replace(&p, relation(&[1.0, 5.0, 9.0]), Instant::now())
            .unwrap();
        assert_eq!(scalar(&s, &p, 0.5), 5.0);
        s.replace(&p, relation(&[5.0, 9.0]), Instant::now())
            .unwrap();
        assert_eq!(scalar(&s, &p, 0.5), 7.0);
        assert_eq!(scalar(&s, &p, 0.9), 8.6);
        assert_eq!(s.epoch, 3);
    }
    /// A complete empty snapshot has SQL aggregate defaults; missing/stale state never does.
    #[test]
    fn empty_snapshot_is_distinct_from_missing_or_stale() {
        let p = population();
        let mut s = Snapshot::default();
        assert!(s.read(&p, &PopulationReadout::Sum, &schema()).is_err());
        s.replace(&p, relation(&[]), Instant::now()).unwrap();
        assert!(scalar(&s, &p, 0.5).is_nan());
        assert_eq!(
            value(
                &s.read(&p, &PopulationReadout::Sum, &schema()).unwrap().rows[0],
                0
            ),
            0.0
        );
        s.observed = Some(Instant::now() - Duration::from_secs(2));
        assert!(s.read(&p, &PopulationReadout::Sum, &schema()).is_err());
    }
    /// Rejecting incomplete/invalid snapshots leaves the committed members and epoch intact.
    #[test]
    fn invalid_snapshot_never_partially_publishes() {
        let p = population();
        let mut s = Snapshot::default();
        s.replace(&p, relation(&[2.0, 4.0]), Instant::now())
            .unwrap();
        assert!(s
            .replace(&p, relation(&[8.0, f64::NAN]), Instant::now())
            .is_err());
        assert_eq!(scalar(&s, &p, 0.5), 3.0);
        assert_eq!(s.epoch, 1);
        assert!(s
            .replace(&p, relation(&[0.0; 101]), Instant::now())
            .is_err());
        assert_eq!(s.epoch, 1);
    }
    /// TopK reads a shared full population and preserves duplicate output rows.
    #[test]
    fn topk_sizes_share_members_and_retract_old_maximum() {
        let p = population();
        let mut s = Snapshot::default();
        s.replace(&p, relation(&[1.0, 8.0, 8.0, 9.0]), Instant::now())
            .unwrap();
        let read = |s: &Snapshot, k| {
            s.read(&p, &PopulationReadout::TopK { k }, &schema())
                .unwrap()
                .rows
                .iter()
                .map(|r| value(r, 0))
                .collect::<Vec<_>>()
        };
        assert_eq!(read(&s, 2), vec![9.0, 8.0]);
        assert_eq!(read(&s, 3), vec![9.0, 8.0, 8.0]);
        s.replace(&p, relation(&[1.0, 8.0, 8.0]), Instant::now())
            .unwrap();
        assert_eq!(read(&s, 2), vec![8.0, 8.0]);
    }
    // Inclusive rank endpoints and an even two-point population retain value-space interpolation.
    #[test]
    fn quantile_endpoints_and_wide_gap_match_clickhouse_inclusive_values() {
        let p = population();
        let mut s = Snapshot::default();
        s.replace(&p, relation(&[1.0, 1_000_000.0]), Instant::now())
            .unwrap();
        assert_eq!(scalar(&s, &p, 0.0), 1.0);
        assert_eq!(scalar(&s, &p, 1.0), 1_000_000.0);
        assert_eq!(scalar(&s, &p, 0.5), 500_000.5);
        assert!((scalar(&s, &p, 0.9) - 900_000.099_999_999_9).abs() < 1e-9);
    }
}

#[cfg(test)]
mod clickhouse_process {
    use super::super::{
        accelerator::CatalogClickHouseAccelerator,
        fallback::{ClickHouseFallbackError, ClickHouseHttpFallback, ClickHouseRawResponse},
        server::{ClickHouseAccelerationOutcome, ClickHouseAccelerator},
    };
    use super::*;
    use control_plane::clickhouse::{
        table_population::{candidates, TablePopulationWorkload},
        ClickHouseSqlAutomaticWorkload, ClickHouseSqlWorkloadEntry,
    };
    use planner_types::{
        pre_asap::{Column, DataType, Schema},
        types::AccuracyTarget,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountSource {
        inner: ClickHouseHttpFallback,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ClickHouseExactBackend for CountSource {
        async fn execute(
            &self,
            r: &ClickHouseQueryRequest,
        ) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.execute(r).await
        }
        async fn execute_bounded(
            &self,
            r: &ClickHouseQueryRequest,
            max_bytes: usize,
        ) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.execute_bounded(r, max_bytes).await
        }
        async fn ping(&self) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
            self.inner.ping().await
        }
    }
    fn query(sql: &str, accept: bool) -> ClickHouseQueryRequest {
        let mut headers = axum::http::HeaderMap::new();
        if accept {
            headers.insert(
                "x-asap-max-snapshot-age-ms",
                axum::http::HeaderValue::from_static("5000"),
            );
        }
        ClickHouseQueryRequest {
            method: axum::http::Method::POST,
            sql: sql.into(),
            body: axum::body::Bytes::from(sql.to_owned()),
            parameters: BTreeMap::from([
                ("default_format".into(), "JSONCompact".into()),
                ("output_format_json_quote_64bit_integers".into(), "1".into()),
                ("database".into(), "default".into()),
            ]),
            headers,
        }
    }
    async fn sql(backend: &dyn ClickHouseExactBackend, statement: &str) {
        let r = backend.execute(&query(statement, false)).await.unwrap();
        assert!(
            r.status.is_success(),
            "{}",
            String::from_utf8_lossy(&r.body)
        );
    }
    fn values(body: &[u8]) -> serde_json::Value {
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        value["data"].clone()
    }
    async fn warm(accel: &CatalogClickHouseAccelerator, sql: &str) -> ClickHouseRawResponse {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match accel.execute(&query(sql, true)).await {
                ClickHouseAccelerationOutcome::Accelerated(r) => return r,
                other => {
                    assert!(
                        Instant::now() < deadline,
                        "population never warmed: {other:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }
    /// Real ClickHouse mutations flow through one shared polling task; admission and source failure fall back.
    #[tokio::test]
    #[ignore = "requires ASAP_SQL_ROWS_CLICKHOUSE_URL pointing to a disposable ClickHouse server"]
    async fn real_clickhouse_population_updates_deletes_and_shared_reads() {
        let url = std::env::var("ASAP_SQL_ROWS_CLICKHOUSE_URL").expect("disposable ClickHouse URL");
        let source = Arc::new(CountSource {
            inner: ClickHouseHttpFallback::new(url, "default".into()),
            calls: AtomicUsize::new(0),
        });
        sql(
            &source.inner,
            "CREATE TABLE sql_row_population (id Int64, value Float64) ENGINE=MergeTree ORDER BY tuple()",
        )
        .await;
        sql(
            &source.inner,
            "INSERT INTO sql_row_population VALUES (1,1),(2,1),(3,5),(4,9),(5,10),(6,20)",
        )
        .await;
        assert!(source
            .inner
            .execute_bounded(&query("SELECT repeat('x', 10000)", false), 128)
            .await
            .is_err());
        let queries = [
            "SELECT quantileExactInclusive(0.5)(value) FROM sql_row_population",
            "SELECT quantileExactInclusive(0.9)(value) FROM sql_row_population",
            "SELECT quantileExactInclusive(0.95)(value) FROM sql_row_population",
            "SELECT quantileExactInclusive(0.99)(value) FROM sql_row_population",
            "SELECT quantileExactInclusive(0)(value) FROM sql_row_population",
            "SELECT quantileExactInclusive(1)(value) FROM sql_row_population",
            "SELECT sum(value) FROM sql_row_population",
            "SELECT avg(value) FROM sql_row_population",
            "SELECT count(*) FROM sql_row_population",
            "SELECT * FROM sql_row_population ORDER BY value DESC LIMIT 2",
            "SELECT * FROM sql_row_population ORDER BY value DESC LIMIT 4",
        ];
        let request = TablePopulationWorkload {
            workload: ClickHouseSqlAutomaticWorkload {
                envelope: asap_types::precompute_plan::PlanEnvelope {
                    plan_id: 9981,
                    plan_version: 1,
                    generated_at_unix_ms: 0,
                    activation_unix_ms: 0,
                    expiry_unix_ms: None,
                    backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
                    planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
                    capability_snapshot_id: "sql-row-test".into(),
                },
                tables: std::collections::HashMap::from([(
                    "sql_row_population".into(),
                    Schema::new(vec![
                        Column::new("id", DataType::Int64, false),
                        Column::new("value", DataType::Float64, false),
                    ]),
                )]),
                accuracy: AccuracyTarget::Exact,
                queries: queries
                    .iter()
                    .map(|q| ClickHouseSqlWorkloadEntry {
                        sql: (*q).into(),
                        start_ms: 1,
                        end_ms: 1000,
                        cumulative: false,
                    })
                    .collect(),
            },
            maintenance: asap_types::query_plan::table_rows::TableRowsMaintenance {
                database: "default".into(),
                refresh_interval_ms: 1000,
                max_snapshot_age_ms: 5000,
                max_rows: 1000,
                max_bytes: 1_000_000,
            },
            horizon_seconds: 60.0,
            query_evaluations: queries.iter().map(|q| ((*q).into(), 100)).collect(),
            capability_snapshot_id: "sql-row-test".into(),
            backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
        };
        let publication = candidates(&request).await.unwrap().remove(0).publication;
        let active = crate::drivers::query::servers::http::build_active_physical_plan(
            crate::drivers::query::servers::http::PhysicalPlanInstallRequest {
                summary_catalog: publication.summary_catalog,
                collector_plans: publication.collector_plans,
                precompute_plan: publication.precompute_plan,
                transmission_plan: publication.transmission_plan,
                query_plan: publication.query_plan,
                storage_routing: None,
                adaptation_evidence: vec![],
            },
            Arc::new(crate::storage_engines::types::BackendStorageRouting::empty()),
        )
        .unwrap();
        let backend: Arc<dyn ClickHouseExactBackend> = source.clone();
        let accel = CatalogClickHouseAccelerator::with_active_physical_plan_and_exact_backend(
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
            crate::storage_engines::types::HotReloadActivePhysicalPlan::new(active),
            backend,
        );
        assert!(matches!(
            accel.execute(&query(queries[0], false)).await,
            ClickHouseAccelerationOutcome::Fallback(_)
        ));
        let mut insufficient = query(queries[0], true);
        insufficient.headers.insert(
            "x-asap-max-snapshot-age-ms",
            axum::http::HeaderValue::from_static("4999"),
        );
        assert!(matches!(
            accel.execute(&insufficient).await,
            ClickHouseAccelerationOutcome::Fallback(_)
        ));
        let mut unknown_quoting = query(queries[0], true);
        unknown_quoting
            .parameters
            .remove("output_format_json_quote_64bit_integers");
        assert!(matches!(
            accel.execute(&unknown_quoting).await,
            ClickHouseAccelerationOutcome::Fallback(_)
        ));
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
        warm(&accel, queries[0]).await;
        for phase in 0..4 {
            if phase == 1 {
                sql(&source.inner,"ALTER TABLE sql_row_population UPDATE value=13 WHERE value=9 SETTINGS mutations_sync=2").await;
            }
            if phase == 2 {
                sql(
                    &source.inner,
                    "ALTER TABLE sql_row_population DELETE WHERE id=2 SETTINGS mutations_sync=2",
                )
                .await;
            }
            if phase == 3 {
                sql(&source.inner, "TRUNCATE TABLE sql_row_population").await;
            }
            if phase > 0 {
                tokio::time::sleep(Duration::from_millis(1200)).await;
            }
            let before = source.calls.load(Ordering::SeqCst);
            for q in queries {
                let native = source.inner.execute(&query(q, false)).await.unwrap();
                let actual = warm(&accel, q).await;
                let native_document: serde_json::Value =
                    serde_json::from_slice(&native.body).unwrap();
                let actual_document: serde_json::Value =
                    serde_json::from_slice(&actual.body).unwrap();
                assert_eq!(
                    native_document["meta"], actual_document["meta"],
                    "native result schema differs for {q}"
                );
                let expected = values(&native.body);
                let got = values(&actual.body);
                let a = expected.as_array().unwrap();
                let b = got.as_array().unwrap();
                assert_eq!(a.len(), b.len(), "{q}");
                for (a, b) in a.iter().zip(b) {
                    for (a, b) in a.as_array().unwrap().iter().zip(b.as_array().unwrap()) {
                        let number = |v: &serde_json::Value| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        };
                        assert!(
                            if a.is_null() || b.is_null() {
                                a.is_null() && b.is_null()
                            } else {
                                (number(a).unwrap() - number(b).unwrap()).abs()
                                    < 1e-12 * number(a).unwrap().abs().max(1.0)
                            },
                            "phase={phase} query={q}: {expected} != {got}"
                        );
                    }
                }
            }
            assert!(
                source.calls.load(Ordering::SeqCst) - before <= 1,
                "readouts spawned separate source scans"
            );
        }
        sql(
            &source.inner,
            "ALTER TABLE sql_row_population ADD COLUMN extra Int64 DEFAULT 0",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(
            matches!(
                accel.execute(&query(queries[0], true)).await,
                ClickHouseAccelerationOutcome::Fallback(_)
            ),
            "a catalog missing native table columns must not serve a partial row schema"
        );
        sql(
            &source.inner,
            "ALTER TABLE sql_row_population DROP COLUMN extra",
        )
        .await;
        warm(&accel, queries[0]).await;
        sql(&source.inner, "DROP TABLE sql_row_population").await;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(matches!(
            accel.execute(&query(queries[0], true)).await,
            ClickHouseAccelerationOutcome::Fallback(_)
        ));
        eprintln!("44 SQL comparisons across 4 epochs passed; quantiles/TopK share background source scans; source scans={}",source.calls.load(Ordering::SeqCst));
    }
}
