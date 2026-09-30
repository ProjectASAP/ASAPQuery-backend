//! Query-time raw PromQL series read from the configured Prometheus-compatible
//! endpoint. This binds a Planner raw-series input contract; it is used only
//! when a selected physical DAG builds its summary from raw samples.
use asap_physical_operators::{
    physical_planner::{
        promql_rows::{series_row, SERIES_IDENTITY_COLUMN},
        InputContract, Source,
    },
    plan::Boundedness,
    runtime::{OutputStream, Reservation, RunContext},
    sources::{DataSources, RawSource},
    values::{Batch, Schema},
    Error,
};
#[cfg(test)]
use asap_types::physical_plan_codec::PhysicalPlanCodec;
use asap_types::query_plan::query_time::{LabelMatch, QueryTimeOperator};
use futures::{stream, StreamExt, TryStreamExt};
use planner_types::{
    post_asap::SummaryFamilyType,
    pre_asap::{self, Column, DataType, QueryExpr},
};
use std::{collections::BTreeMap, sync::Arc};

const ROWS_PER_BATCH: usize = 4096;

fn failed(message: impl std::fmt::Display) -> Error {
    Error::Operator(format!("raw Prometheus read failed: {message}"))
}

/// Series identity, timestamp time index, sample value and label projections.
/// Any other column has no raw PromQL source and is not bound here.
pub(super) fn is_raw_series_contract(contract: &InputContract) -> bool {
    let fields = &contract.schema.fields;
    let plain =
        |index: usize, dtype: DataType| fields[index].dtype == SummaryFamilyType::Plain(dtype);
    let Some(time) = contract.schema.time_index.filter(|&i| i < fields.len()) else {
        return false;
    };
    contract.properties.boundedness == Boundedness::Bounded
        && plain(time, DataType::Timestamp)
        && fields
            .iter()
            .filter(|f| f.name == SERIES_IDENTITY_COLUMN)
            .count()
            == 1
        && fields.iter().filter(|f| f.name == "value").count() == 1
        && fields.iter().enumerate().all(|(index, field)| {
            index == time
                || if field.name == "value" {
                    plain(index, DataType::Float64) && !field.nullable
                } else {
                    plain(index, DataType::Utf8)
                        && (field.name != SERIES_IDENTITY_COLUMN || !field.nullable)
                }
        })
}

struct PrometheusRawSeries {
    client: reqwest::Client,
    endpoint: String,
    selector: String,
    /// Samples are exactly those in `(end_ms - range_ms, end_ms]`.
    end_ms: i64,
    range_ms: i64,
    schema: Schema,
}

/// Bind one installed raw Scan leaf, evaluated at `at`, to its compiled contract.
pub(super) fn bind(
    contract: &InputContract,
    scan: &QueryTimeOperator,
    at: i64,
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<Source<'static>, Error> {
    let QueryTimeOperator::Scan {
        metric: Some(metric),
        matchers,
        range_ms: Some(range_ms),
        offset_ms,
    } = scan
    else {
        return Err(Error::Invalid(
            "query-time raw input requires a named range selector".into(),
        ));
    };
    if !is_raw_series_contract(contract) {
        return Err(Error::Invalid(
            "query-time raw input differs from the raw PromQL series contract".into(),
        ));
    }
    let quote =
        |value: &str| serde_json::to_string(value).map_err(|e| Error::Invalid(e.to_string()));
    let mut terms = vec![format!("__name__={}", quote(metric)?)];
    for matcher in matchers {
        let op = match matcher.operation {
            LabelMatch::Equal => "=",
            LabelMatch::NotEqual => "!=",
            LabelMatch::Regex => "=~",
            LabelMatch::NotRegex => "!~",
        };
        terms.push(format!("{}{op}{}", matcher.name, quote(&matcher.value)?));
    }
    let source = PrometheusRawSeries {
        client: client.clone(),
        endpoint: endpoint.trim_end_matches('/').to_owned(),
        selector: format!("{{{}}}[{range_ms}ms]", terms.join(",")),
        end_ms: at
            .checked_sub(*offset_ms)
            .ok_or_else(|| Error::Invalid("raw selector offset overflow".into()))?,
        range_ms: i64::try_from(*range_ms)
            .map_err(|_| Error::Invalid("raw selector range overflow".into()))?,
        schema: contract.schema.clone(),
    };
    // Reuse Planner's Scan: it opens lazily and rejects connector schema drift.
    let id = pre_asap::Source::TimeSeries {
        metric: metric.clone(),
    };
    let logical = QueryExpr::Scan {
        source: id.clone(),
        predicates: vec![],
        schema: pre_asap::Schema {
            time_index: contract.schema.time_index,
            closed: true,
            ..pre_asap::Schema::new(
                contract
                    .schema
                    .fields
                    .iter()
                    .map(|field| {
                        let SummaryFamilyType::Plain(dtype) = &field.dtype else {
                            unreachable!("raw series contract has only plain columns")
                        };
                        Column::new(field.name.clone(), dtype.clone(), field.nullable)
                    })
                    .collect(),
            )
        },
    };
    let mut sources = DataSources::default();
    sources.register(id, Arc::new(source))?;
    Ok(Box::new(sources.bind(&logical)?))
}

impl PrometheusRawSeries {
    async fn fetch(&self, context: &RunContext) -> Result<(Vec<Batch>, Reservation), Error> {
        use crate::query_engines::request::wait;
        let time = format!("{:.3}", self.end_ms as f64 / 1000.0);
        let request = self
            .client
            .get(format!("{}/api/v1/query", self.endpoint))
            .query(&[("query", self.selector.as_str()), ("time", time.as_str())]);
        let mut response = wait(request.send()).await?.map_err(failed)?;
        if !response.status().is_success() {
            return Err(failed(format!("HTTP {}", response.status())));
        }
        let mut bytes = Vec::new();
        let mut wire = context.reserve(0)?;
        while let Some(chunk) = wait(response.chunk()).await?.map_err(failed)? {
            wire.resize(
                bytes
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(Error::MemoryLimit)?,
            )?;
            bytes.extend_from_slice(&chunk);
        }
        // Same decode workspace bound as the exact-subquery client.
        let _decode = context.reserve(bytes.len().checked_mul(64).ok_or(Error::MemoryLimit)?)?;
        let body: serde_json::Value = serde_json::from_slice(&bytes).map_err(failed)?;
        let batches = self.decode(&body)?;
        // Decoded rows stay charged until the consumer takes them.
        let retained = context.reserve(batches.iter().map(Batch::bytes).sum())?;
        Ok((batches, retained))
    }

    fn decode(&self, body: &serde_json::Value) -> Result<Vec<Batch>, Error> {
        if body["status"] != "success" || body["warnings"].as_array().is_some_and(|w| !w.is_empty())
        {
            return Err(failed("error status or partial-result warnings"));
        }
        if body["data"]["resultType"] != "matrix" {
            return Err(failed("range selector did not return a matrix"));
        }
        let series = body["data"]["result"]
            .as_array()
            .ok_or_else(|| failed("missing matrix result"))?;
        let mut samples = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for entry in series {
            if entry.get("histograms").is_some() {
                return Err(failed("native histogram samples are not float series"));
            }
            let labels = entry["metric"]
                .as_object()
                .ok_or_else(|| failed("missing series labels"))?
                .iter()
                .map(|(name, value)| {
                    Ok((
                        name.clone(),
                        value
                            .as_str()
                            .ok_or_else(|| failed("invalid label"))?
                            .to_owned(),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Error>>()?;
            let mut points = Vec::new();
            for point in entry["values"]
                .as_array()
                .ok_or_else(|| failed("missing samples"))?
            {
                let (Some(time), Some(value)) = (point[0].as_f64(), point[1].as_str()) else {
                    return Err(failed("invalid sample"));
                };
                let time = (time * 1000.0).round() as i64;
                let start = self.end_ms - self.range_ms;
                // PromQL ranges are left-open; Prometheus 2.x also returns the
                // sample at `start`, which the contract excludes.
                if time == start {
                    continue;
                }
                if time < start || time > self.end_ms {
                    return Err(failed("sample lies outside the requested range"));
                }
                points.push((time, value.parse::<f64>().map_err(failed)?));
            }
            if samples.insert(labels, points).is_some() {
                return Err(failed("duplicate series labels"));
            }
        }
        // Deterministic order: series by label set, then samples by time.
        let mut rows = Vec::new();
        for (labels, mut points) in samples {
            points.sort_by_key(|(time, _)| *time);
            for (time, value) in points {
                rows.push(series_row(&self.schema, &labels, time, value)?);
            }
        }
        let mut batches = Vec::new();
        while !rows.is_empty() {
            let rest = rows.split_off(rows.len().min(ROWS_PER_BATCH));
            batches.push(Batch::try_new(
                self.schema.clone(),
                std::mem::replace(&mut rows, rest),
            )?);
        }
        Ok(batches)
    }
}

impl RawSource for PrometheusRawSeries {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }
    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }
    fn scan(&self, context: RunContext) -> Result<OutputStream<'_, Batch>, Error> {
        Ok(stream::once(async move {
            let (batches, mut retained) = self.fetch(&context).await?;
            let mut remaining = batches.iter().map(Batch::bytes).sum::<usize>();
            Ok::<_, Error>(stream::iter(batches.into_iter().map(move |batch| {
                if context.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                remaining -= batch.bytes();
                retained.resize(remaining)?;
                Ok(batch)
            })))
        })
        .try_flatten()
        .boxed_local())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engines::{routing::query_engine_routing::QueryEngine, EngineError};
    use asap_physical_operators::{
        physical_planner::CompiledPhysicalDag,
        plan::PhysicalOperator,
        runtime::{Limits, Scope},
        values::Value,
    };
    use asap_types::query_plan::{query_time::LabelMatcher, *};
    use planner_types::post_asap::{
        compile_post_asap_dag, PostAsapOperatorPayload, SketchAlgorithm, SummaryExpr,
    };
    use std::rc::Rc;

    const QUERY: &str = r#"quantile_over_time(0.5, m{job="api"}[5m])"#;
    const AT: i64 = 1_000_000;

    fn scan() -> QueryTimeOperator {
        QueryTimeOperator::Scan {
            metric: Some("m".into()),
            matchers: vec![LabelMatcher {
                name: "job".into(),
                value: "api".into(),
                operation: LabelMatch::Equal,
            }],
            range_ms: Some(300_000),
            offset_ms: 0,
        }
    }

    /// Planner's Ephemeral-shaped KLL candidate: raw rows -> SummaryAgg -> Quantile.
    /// Returns the compiled DAG and its raw-series input slot.
    fn kll_program() -> (CompiledPhysicalDag, u64) {
        use asap_aware_mapping::{Replacement, ReplacementStrategy, SketchAlgorithmStrategy};
        let logical = control_plane::query_parser::parse_query_expr_canonical(
            QUERY,
            planner_types::types::AccuracyTarget::Epsilon(0.05),
        )
        .unwrap();
        let typed = Rc::new(
            asap_physical_operators::physical_planner::promql_rows::with_series_identity(&logical)
                .unwrap(),
        );
        let kll = SketchAlgorithmStrategy::default_cost_model()
            .replacements(&asap_aware_mapping::TargetSubDAG::new(&typed))
            .into_iter()
            .find_map(|candidate| match candidate.replacement {
                Replacement::Summary(node) => matches!(&node.expr,
                    SummaryExpr::SummaryEstimate { summary_input, .. }
                        if matches!(&summary_input.expr, SummaryExpr::SummaryAgg {
                            family: SummaryFamilyType::Sketch(kind, _), ..
                        } if kind.algorithm() == &SketchAlgorithm::Kll))
                .then_some(node),
                _ => None,
            })
            .expect("Planner offers a KLL candidate");
        let dag = compile_post_asap_dag(&kll).unwrap();
        let raw = dag
            .nodes
            .iter()
            .find(|node| matches!(node.payload, PostAsapOperatorPayload::Fallback { .. }))
            .unwrap();
        let slot = u64::from(raw.id.0);
        let program = asap_physical_operators::physical_planner::compile(
            &dag,
            [(
                slot,
                InputContract::bounded(Arc::new(raw.output_schema.clone())),
            )]
            .into(),
            &[u64::from(dag.root.0)],
        )
        .unwrap();
        (program, slot)
    }

    fn contract() -> InputContract {
        let (program, slot) = kll_program();
        let contract = program
            .input_contracts()
            .find(|(id, _)| *id == slot)
            .unwrap()
            .1
            .clone();
        contract
    }

    /// Serve `/api/v1/query` with a fixed response, recording query parameters.
    async fn prometheus(
        status: u16,
        body: serde_json::Value,
        delay: std::time::Duration,
    ) -> (
        String,
        Arc<std::sync::Mutex<Vec<BTreeMap<String, String>>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let app = axum::Router::new().route(
            "/api/v1/query",
            axum::routing::get(
                move |axum::extract::Query(params): axum::extract::Query<
                    BTreeMap<String, String>,
                >| {
                    let body = body.clone();
                    recorded.lock().unwrap().push(params);
                    async move {
                        tokio::time::sleep(delay).await;
                        (
                            axum::http::StatusCode::from_u16(status).unwrap(),
                            axum::Json(body),
                        )
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (endpoint, seen, server)
    }

    /// Two series, returned out of label and time order, with a left-boundary sample.
    fn matrix() -> serde_json::Value {
        serde_json::json!({"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"__name__": "m", "job": "api", "instance": "b"},
             "values": [[900, "40"], [710, "10"], [800, "50"], [950, "20"], [990, "30"]]},
            {"metric": {"__name__": "m", "job": "api", "instance": "a"},
             "values": [[700, "99"], [960, "3"], [720, "5"], [850, "1"]]}
        ]}})
    }

    fn context() -> RunContext {
        RunContext::new(
            Scope::Query {
                evaluation_time_ms: AT,
                revision: 0,
            },
            Limits::default(),
        )
        .unwrap()
    }

    async fn read(source: &Source<'static>, context: RunContext) -> Result<Vec<Batch>, Error> {
        source.start(vec![], context)?.try_collect().await
    }

    fn cause(error: &Error) -> &Error {
        match error {
            Error::AtNode { source, .. } => cause(source),
            other => other,
        }
    }

    // A matrix response becomes bounded, contract-typed rows ordered by series then time.
    #[tokio::test]
    async fn matrix_binds_as_bounded_ordered_raw_series_rows() {
        let contract = contract();
        assert!(is_raw_series_contract(&contract));
        let (endpoint, seen, server) = prometheus(200, matrix(), std::time::Duration::ZERO).await;
        let source = bind(&contract, &scan(), AT, &reqwest::Client::new(), &endpoint).unwrap();
        assert_eq!(source.output_schema(), contract.schema);
        assert_eq!(source.properties(&[]).boundedness, Boundedness::Bounded);
        assert!(seen.lock().unwrap().is_empty(), "binding performs no I/O");
        let run = context();
        let batches = read(&source, run.clone()).await.unwrap();
        assert_eq!(run.retained_bytes(), 0);
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [BTreeMap::from([
                (
                    "query".into(),
                    r#"{__name__="m",job="api"}[300000ms]"#.into()
                ),
                ("time".into(), "1000.000".into()),
            ])]
        );
        let fields = &contract.schema.fields;
        let column = |name: &str| fields.iter().position(|f| f.name == name).unwrap();
        let rows = batches
            .iter()
            .inspect(|batch| assert_eq!(batch.schema(), &contract.schema))
            .flat_map(|batch| batch.rows().iter().cloned())
            .map(|row| {
                let (Value::Utf8(identity), Value::Timestamp(time), Value::Float64(value)) = (
                    &row[column(SERIES_IDENTITY_COLUMN)],
                    &row[contract.schema.time_index.unwrap()],
                    &row[column("value")],
                ) else {
                    panic!("row violates the raw series contract: {row:?}")
                };
                let labels =
                    asap_physical_operators::physical_planner::promql_rows::decode_series_identity(
                        identity,
                    )
                    .unwrap();
                assert!(matches!(&row[column("job")], Value::Utf8(job) if job.as_ref() == "api"));
                (labels["instance"].clone(), *time, *value)
            })
            .collect::<Vec<_>>();
        let expected = [
            ("a", 720, 5.),
            ("a", 850, 1.),
            ("a", 960, 3.),
            ("b", 710, 10.),
            ("b", 800, 50.),
            ("b", 900, 40.),
            ("b", 950, 20.),
            ("b", 990, 30.),
        ]
        .map(|(instance, seconds, value)| (instance.to_string(), seconds * 1000, value));
        assert_eq!(rows, expected);
        server.abort();
    }

    // HTTP, protocol and timeout failures are execution errors, never empty scans;
    // a cancelled run does not issue the request.
    #[tokio::test]
    async fn endpoint_failures_and_cancellation_are_execution_errors() {
        let contract = contract();
        let client = reqwest::Client::new();
        for (status, body) in [
            (503, serde_json::json!({"status": "error"})),
            (200, serde_json::json!({"status": "error", "error": "bad"})),
            (
                200,
                serde_json::json!({"status": "success", "warnings": ["partial"],
                "data": {"resultType": "matrix", "result": []}}),
            ),
            (
                200,
                serde_json::json!({"status": "success",
                "data": {"resultType": "vector", "result": []}}),
            ),
            (
                200,
                serde_json::json!({"status": "success", "data": {"resultType": "matrix",
                "result": [{"metric": {"job": "api"}, "values": [[1001, "1"]]}]}}),
            ),
        ] {
            let (endpoint, _, server) =
                prometheus(status, body.clone(), std::time::Duration::ZERO).await;
            let source = bind(&contract, &scan(), AT, &client, &endpoint).unwrap();
            let error = read(&source, context()).await.unwrap_err();
            assert!(
                matches!(cause(&error), Error::Operator(_)),
                "{body}: {error}"
            );
            server.abort();
        }
        let (endpoint, seen, server) =
            prometheus(200, matrix(), std::time::Duration::from_secs(30)).await;
        let impatient = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();
        let source = bind(&contract, &scan(), AT, &impatient, &endpoint).unwrap();
        let error = read(&source, context()).await.unwrap_err();
        assert!(matches!(cause(&error), Error::Operator(_)), "{error}");
        let source = bind(&contract, &scan(), AT, &client, &endpoint).unwrap();
        let run = context();
        run.cancel();
        let error = read(&source, run).await.unwrap_err();
        assert!(matches!(cause(&error), Error::Cancelled), "{error}");
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "only the timed-out request was sent"
        );
        server.abort();
    }

    // The raw response is charged to the run budget rather than read unbounded.
    #[tokio::test]
    async fn raw_read_respects_run_memory_budget() {
        let contract = contract();
        let (endpoint, _, server) = prometheus(200, matrix(), std::time::Duration::ZERO).await;
        let source = bind(&contract, &scan(), AT, &reqwest::Client::new(), &endpoint).unwrap();
        let run = RunContext::new(
            Scope::Query {
                evaluation_time_ms: AT,
                revision: 0,
            },
            Limits {
                max_bytes: 256,
                ..Limits::default()
            },
        )
        .unwrap();
        let error = read(&source, run.clone()).await.unwrap_err();
        assert!(matches!(cause(&error), Error::MemoryLimit), "{error}");
        assert_eq!(run.retained_bytes(), 0);
        server.abort();
    }

    fn installed(max_bytes: u64) -> QueryPlanEntry {
        let (program, slot) = kll_program();
        let canonical = canonical_promql(QUERY).unwrap();
        QueryPlanEntry {
            physical_dag: Some(serde_json::from_slice(&program.encode().unwrap()).unwrap()),
            language: QueryLanguage::PromQl,
            query_id: canonical.clone(),
            canonical_query: canonical,
            fixed_evaluation: None,
            root: QueryNodeId(1),
            nodes: BTreeMap::from([
                (
                    QueryNodeId(0),
                    QueryPlanNode::Logical {
                        operator: scan(),
                        inputs: vec![],
                    },
                ),
                (
                    QueryNodeId(1),
                    QueryPlanNode::Physical {
                        inputs: vec![QueryNodeId(0)],
                        source_nodes: vec![slot],
                        max_bytes,
                        drop_metric_name: !control_plane::query_plan::result_keeps_metric_name(
                            &control_plane::query_parser::parse_query_expr_canonical(
                                QUERY,
                                planner_types::types::AccuracyTarget::Exact,
                            )
                            .unwrap(),
                        ),
                    },
                ),
            ]),
            instant: InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        }
    }

    fn engine(endpoint: String) -> super::super::ASAPQueryEngine {
        let index = Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
        let active = super::super::test_plan::install(&index, &[], vec![installed(1 << 20)]);
        super::super::ASAPQueryEngine::new(1000)
            .with_active_physical_plan(active)
            .with_exact_subquery_endpoint(endpoint)
    }

    // An installed physical KLL build + quantile over the query-time raw input
    // matches the exact per-series quantile of the same samples.
    #[tokio::test]
    async fn installed_kll_over_raw_prometheus_matches_exact_quantile() {
        let (endpoint, seen, server) = prometheus(200, matrix(), std::time::Duration::ZERO).await;
        let result = engine(endpoint).execute_at(QUERY, AT as u64).await.unwrap();
        let crate::query_engines::query_result::QueryResult::Vector(result) = result else {
            panic!("expected an instant vector")
        };
        let actual = result
            .values
            .iter()
            .map(|point| {
                let keys = point.label_keys_override.clone().unwrap();
                let labels = keys
                    .into_iter()
                    .zip(point.labels.labels.iter().cloned())
                    .collect::<BTreeMap<_, _>>();
                (labels["instance"].clone(), point.value)
            })
            .collect::<BTreeMap<_, _>>();
        // Exact reference: PromQL quantile_over_time(0.5) of the in-range samples.
        let exact = |mut values: Vec<f64>| {
            values.sort_by(f64::total_cmp);
            let rank = 0.5 * (values.len() - 1) as f64;
            let (low, high) = (rank.floor() as usize, rank.ceil() as usize);
            values[low] + (values[high] - values[low]) * (rank - low as f64)
        };
        assert_eq!(
            actual,
            BTreeMap::from([
                ("a".into(), exact(vec![5., 1., 3.])),
                ("b".into(), exact(vec![10., 50., 40., 20., 30.])),
            ])
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
        server.abort();
    }

    // PromQL drops the metric name from a function result; the raw rows keep
    // it in their series identity, so the adapter removes it.
    #[tokio::test]
    async fn raw_program_results_drop_the_metric_name() {
        let (endpoint, _, server) = prometheus(200, matrix(), std::time::Duration::ZERO).await;
        let result = engine(endpoint).execute_at(QUERY, AT as u64).await.unwrap();
        let crate::query_engines::query_result::QueryResult::Vector(result) = result else {
            panic!("expected an instant vector")
        };
        assert_eq!(result.values.len(), 2);
        for point in &result.values {
            let keys = point.label_keys_override.as_ref().unwrap();
            assert!(!keys.iter().any(|key| key == "__name__"), "{keys:?}");
            assert!(keys.iter().any(|key| key == "instance"), "{keys:?}");
        }
        server.abort();
    }

    // An installed raw input fails the query when its endpoint fails or is not
    // configured; it is never answered as an empty vector.
    #[tokio::test]
    async fn installed_raw_input_errors_propagate() {
        let (endpoint, _, server) = prometheus(
            503,
            serde_json::json!({"status": "error"}),
            std::time::Duration::ZERO,
        )
        .await;
        let error = engine(endpoint)
            .execute_at(QUERY, AT as u64)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, EngineError::Physical(error) if matches!(cause(error), Error::Operator(_))),
            "{error:?}"
        );
        server.abort();
        let index = Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
        let active = super::super::test_plan::install(&index, &[], vec![installed(1 << 20)]);
        let error = super::super::ASAPQueryEngine::new(1000)
            .with_active_physical_plan(active)
            .execute_at(QUERY, AT as u64)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("no Prometheus endpoint"),
            "{error}"
        );
    }
}
