//! Push precompute (docs/design/parallel-precompute.md, "Acceptance and
//! test design", T3–T19): each ingest shard folds its samples into its own
//! instance of each window's pipelines as they arrive; at close the
//! shards' partials are combined by the breaker's rule and committed in
//! window order.
//!
//! Each test first runs its fixture in window mode, the reference, then
//! the same in push mode. Until push mode is implemented, install refuses
//! it, so every push-mode test fails at install; the guards (T8, T9) and
//! the window-mode references pass.
//!
//! T1 and T2 (every parity workload) are in `planner_executor_parity.rs`.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_planner::InstallablePlan;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::{EngineOptions, PrecomputeMode, SlowRefresh};
use data_plane::ingestion::IngestionOptions;
use data_plane::pipelines::{combine_rules, CombineRule};
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const WINDOW: PrecomputeMode = PrecomputeMode::Window;
const PUSH: PrecomputeMode = PrecomputeMode::Push;

// ── Fixtures ────────────────────────────────────────────────────────────

/// One repeating query: its text and refresh interval (ms).
type Query<'a> = (&'a str, u64);

/// A workload of `queries` over `metrics` (`*_total` are counters, others
/// gauges), each with `series` series sampled every `sample_interval_ms`.
fn workload(
    queries: &[Query],
    metrics: &[&str],
    series: usize,
    sample_interval_ms: u64,
) -> asap_planner::PlanningWorkload {
    let sources: serde_json::Map<String, Value> = metrics
        .iter()
        .map(|metric| {
            let kind = if metric.ends_with("_total") {
                "counter"
            } else {
                "gauge"
            };
            let source = json!({"series_count": series, "sample_interval": sample_interval_ms,
                "metric_type": kind, "column_signs": {"value": "non_negative"}});
            (metric.to_string(), source)
        })
        .collect();
    let rows_per_second = (series * metrics.len()) as f64 * 1000.0 / sample_interval_ms as f64;
    let queries: Vec<Value> = queries
        .iter()
        .map(|(query, interval)| {
            json!({
                "query": query,
                "demand": {"fixed_interval_at": {"interval": interval, "evaluation_phase": 0}},
                "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.01,
                    "accuracy_scope": "whole_result"}},
                "predictability": {"predictable": {"known_at": null}},
            })
        })
        .collect();
    serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": queries},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": rows_per_second}},
          "sources": sources},
    }))
    .unwrap()
}

fn plan(
    queries: &[Query],
    metrics: &[&str],
    series: usize,
    sample_interval_ms: u64,
    raw_data_retained: bool,
) -> InstallablePlan {
    let workload = workload(queries, metrics, series, sample_interval_ms);
    plan_workload(&workload, &common::inputs(raw_data_retained))
        .unwrap()
        .0
}

/// A workload file in `tests/workloads/`, its sources resized to `series`
/// series.
fn workload_file(name: &str, series: usize) -> asap_planner::PlanningWorkload {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/workloads/{name}.json"));
    let mut workload: Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for source in workload["data_workload"]["sources"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        source["series_count"] = series.into();
    }
    serde_json::from_value(workload).unwrap()
}

/// Engine options of the tests: no forwarding, `shards` ingest shards,
/// windows from 0, no lateness unless a test sets it.
fn options(mode: PrecomputeMode, shards: usize) -> EngineOptions {
    EngineOptions {
        no_forward: true,
        ingest_threads: shards,
        precompute_mode: mode,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

/// Installs `plan`; panics if install refuses it (push mode, today).
fn install(plan: &InstallablePlan, options: EngineOptions) -> (Router, Arc<DataPlane>) {
    let mode = options.precompute_mode;
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(plan).unwrap(),
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap_or_else(|error| panic!("install in {mode:?} mode: {error:#}")),
    );
    (router(state.clone()), state)
}

/// Labels of series `i` of `metric`: instance `i{i}`, job `job{i % 4}`.
fn labels(metric: &str, i: usize) -> Vec<Label> {
    [
        ("__name__", metric.to_string()),
        ("instance", format!("i{i}")),
        ("job", format!("job{}", i % 4)),
    ]
    .into_iter()
    .map(|(name, value)| Label {
        name: name.into(),
        value,
    })
    .collect()
}

/// Sample value of series `i` at `timestamp_ms`: counters (`*_total`) grow
/// with time, gauges take pseudo-random values.
fn value(metric: &str, i: usize, timestamp_ms: i64) -> f64 {
    let gauge = ((i as i64 * 7919 + timestamp_ms / 100 * 104_729) % 100_003) as f64 / 100.0;
    if metric.ends_with("_total") {
        gauge + (timestamp_ms / 100) as f64 * 1001.0
    } else {
        gauge
    }
}

/// The Remote Write body of one sample at each of `timestamps_ms` for each
/// series in `series` of each of `metrics`.
fn body(metrics: &[&str], series: std::ops::Range<usize>, timestamps_ms: &[i64]) -> Vec<u8> {
    let timeseries = metrics
        .iter()
        .flat_map(|metric| series.clone().map(move |i| (*metric, i)))
        .map(|(metric, i)| TimeSeries {
            labels: labels(metric, i),
            samples: timestamps_ms
                .iter()
                .map(|&timestamp| Sample {
                    timestamp,
                    value: value(metric, i, timestamp),
                })
                .collect(),
        })
        .collect();
    encode(&WriteRequest { timeseries })
}

async fn post(app: &Router, body: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(
            Request::post("/api/v1/write")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

/// Writes one request per second in `(from_s, to_s]`: each series of each
/// metric, one sample at that second.
async fn write_seconds(app: &Router, metrics: &[&str], series: usize, from_s: i64, to_s: i64) {
    for second in from_s + 1..=to_s {
        let status = post(app, body(metrics, 0..series, &[second * 1000])).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "write at {second} s");
    }
}

async fn barrier(app: &Router, time_ms: i64) {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/precompute/watermark?time_ms={time_ms}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// `query`'s status and body at `time_ms`, results sorted by labels.
async fn answer(app: &Router, query: &str, time_ms: i64) -> (StatusCode, Value) {
    let uri = format!(
        "/api/v1/query?query={}&time={}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>(),
        time_ms as f64 / 1000.0
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let mut body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    if let Some(result) = body["data"]["result"].as_array_mut() {
        result.sort_by_key(|s| s["metric"].to_string());
    }
    (status, body)
}

/// Every query of `plan` at each of `times_ms`, by query and time.
async fn answers(
    app: &Router,
    plan: &InstallablePlan,
    times_ms: &[i64],
) -> Vec<(String, i64, Value)> {
    let mut out = Vec::new();
    for query in &plan.query_texts {
        for &time_ms in times_ms {
            let (status, body) = answer(app, query, time_ms).await;
            out.push((
                query.clone(),
                time_ms,
                json!({"status": status.as_u16(), "body": body}),
            ));
        }
    }
    out
}

/// Every stored view's windows: per view, each window's range and its
/// rows printed and sorted.
fn stored(state: &DataPlane) -> BTreeMap<String, Vec<((i64, i64), Vec<String>)>> {
    state
        .engine
        .snapshot()
        .view_rows()
        .into_iter()
        .map(|(view, windows)| {
            let windows = windows
                .into_iter()
                .map(|(range, batches)| {
                    let mut rows: Vec<String> = batches
                        .iter()
                        .flat_map(|b| b.to_rows())
                        .map(|row| format!("{row:?}"))
                        .collect();
                    rows.sort();
                    (range, rows)
                })
                .collect();
            (format!("{view}"), windows)
        })
        .collect()
}

async fn metrics_text(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

/// Sum of every sample of metric `name` whose labels contain each of
/// `matchers` (`key="value"` text); `None` when no sample matches.
fn metric_sum(text: &str, name: &str, matchers: &[&str]) -> Option<f64> {
    let mut found = None;
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let metric = series.split('{').next().unwrap();
        if metric != name || !matchers.iter().all(|m| series.contains(m)) {
            continue;
        }
        *found.get_or_insert(0.0) += value.parse::<f64>().unwrap();
    }
    found
}

/// The refresh groups' window widths (ms), by group index.
fn group_widths(plan: &InstallablePlan) -> Vec<u64> {
    plan.ingestion_refresh_groups()
        .unwrap()
        .iter()
        .map(|g| g.refresh_window.map_or(0, |w| w.width_ms))
        .collect()
}

// ── T3: one morsel reaches every view of its source ─────────────────────

/// An o11y-dashboard-shaped workload: 13 queries over one source, each a
/// different function or matcher, so the Planner selects 13 chains.
fn thirteen_views_on_one_source() -> Vec<(String, u64)> {
    let functions = [
        "sum_over_time",
        "max_over_time",
        "min_over_time",
        "count_over_time",
    ];
    let mut queries: Vec<(String, u64)> = (0..12)
        .map(|i| {
            (
                format!("{}(data{{job=\"job{}\"}}[1m])", functions[i % 4], i / 4),
                15_000,
            )
        })
        .collect();
    queries.push(("quantile_over_time(0.95, data[1m])".into(), 15_000));
    queries
}

/// T3. One morsel reaches every view of its source on the shard thread, read
/// once: seed-862-01 (5–6 views on `data`) and a 13-view source answer as
/// the window path, and `precompute_push` runs once per morsel and group,
/// not once per view.
#[tokio::test(flavor = "multi_thread")]
async fn one_morsel_reaches_every_view_of_its_source() {
    let seed = plan_workload(
        &workload_file("random-mixed-seed-862-01", 64),
        &common::inputs(false),
    )
    .unwrap()
    .0;
    let thirteen: Vec<(String, u64)> = thirteen_views_on_one_source();
    let thirteen: Vec<Query> = thirteen.iter().map(|(q, i)| (q.as_str(), *i)).collect();
    let o11y = plan(&thirteen, &["data"], 64, 1000, false);
    assert!(
        o11y.materialized_views().unwrap().len() >= 13,
        "the 13-query fixture must select 13 views on one source"
    );
    for (label, plan, metrics) in [
        ("seed-862-01", &seed, vec!["data", "data_total"]),
        ("13 views", &o11y, vec!["data"]),
    ] {
        let times: Vec<i64> = (2..=4).map(|m| m * 60_000).collect();
        let (window, _) = install(plan, options(WINDOW, 4));
        write_seconds(&window, &metrics, 64, 0, 240).await;
        barrier(&window, 240_000).await;
        let expected = answers(&window, plan, &times).await;

        let (push, _) = install(plan, options(PUSH, 4));
        write_seconds(&push, &metrics, 64, 0, 240).await;
        barrier(&push, 240_000).await;
        assert_eq!(answers(&push, plan, &times).await, expected, "{label}");

        // 240 requests, each split over 4 shards: at most 4 morsels per
        // request and group, whatever the number of views.
        let text = metrics_text(&push).await;
        for group in 0..group_widths(plan).len() {
            let runs = metric_sum(
                &text,
                "asap_phase_runs_total",
                &["phase=\"precompute_push\"", &format!("group=\"{group}\"")],
            )
            .unwrap_or_else(|| panic!("{label}: no precompute_push runs of group {group}"));
            assert!(
                runs <= (240 * 4 * metrics.len()) as f64,
                "{label}: group {group}: {runs} push runs: more than one per morsel"
            );
        }
    }
}

// ── T4–T7: combine rules ────────────────────────────────────────────────

/// T4. A per-series sketch chain is concatenated: each (window, series)
/// stored once, equal to the window path; its rule is `Concatenate`.
#[tokio::test(flavor = "multi_thread")]
async fn per_series_breakers_are_concatenated() {
    let plan = plan(
        &[("quantile_over_time(0.95, data[1m])", 10_000)],
        &["data"],
        1000,
        1000,
        false,
    );
    let rules = combine_rules(&plan).expect("combine rules");
    assert!(!rules.is_empty());
    assert!(
        rules.iter().all(|r| r.rule == CombineRule::Concatenate),
        "{rules:?}"
    );

    let (window, window_state) = install(&plan, options(WINDOW, 8));
    write_seconds(&window, &["data"], 1000, 0, 30).await;
    barrier(&window, 30_000).await;
    let (push, push_state) = install(&plan, options(PUSH, 8));
    write_seconds(&push, &["data"], 1000, 0, 30).await;
    barrier(&push, 30_000).await;

    let (expected, served) = (stored(&window_state), stored(&push_state));
    for (view, windows) in &served {
        assert_eq!(windows.len(), 3, "view {view}: three windows");
        for (range, rows) in windows {
            assert_eq!(
                rows.len(),
                1000,
                "view {view} {range:?}: one row per series"
            );
        }
    }
    let randomized = serde_json::to_string(&plan.dag)
        .unwrap()
        .contains("\"Kll\"");
    if randomized {
        // KLL compacts at random: same windows and keys, answers within 5%
        // (T1); here only the shape.
        let shape = |v: &BTreeMap<String, Vec<((i64, i64), Vec<String>)>>| {
            v.values()
                .map(|w| {
                    w.iter()
                        .map(|(r, rows)| (*r, rows.len()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(&served), shape(&expected));
    } else {
        assert_eq!(served, expected);
    }
}

/// A plan whose every ingestion-time chain is regrouped from per series to
/// per `job` (`Reduction::Reduce` over the `job` column of its input): the
/// hand-written shape of SQL's grouped chains, which the Planner selects
/// for no PromQL query. `None` when the plan has no per-series chain whose
/// input has a `job` column.
fn regrouped_by_job(plan: &InstallablePlan) -> Option<InstallablePlan> {
    let mut json = serde_json::to_value(plan).unwrap();
    let job_column = json["dag"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|n| n["payload"].pointer("/NonASAP/Scan/schema/fields"))
        .and_then(|fields| {
            fields
                .as_array()
                .unwrap()
                .iter()
                .position(|f| f["name"] == "job")
                // A per-series scan has no `job` column: its first label
                // column (the series identity).
                .or_else(|| {
                    fields
                        .as_array()
                        .unwrap()
                        .iter()
                        .position(|f| f["name"] != "ts" && f["name"] != "value")
                })
        })?;
    let mut regrouped = false;
    for node in json["dag"]["nodes"].as_array_mut().unwrap() {
        for chain in ["SummaryTumblingWindowChain", "SummaryAgg"] {
            if let Some(op) = node["payload"].pointer_mut(&format!("/ASAP/{chain}")) {
                if op["reduction"] == json!("PerEntity") {
                    op["reduction"] = json!({ "Reduce": [job_column] });
                    regrouped = true;
                }
            }
        }
    }
    regrouped
        .then(|| serde_json::from_value(json).ok())
        .flatten()
}

/// T5. `Reduce` breakers merge the shards' partials into one state per
/// group. Combine level: every chain of ExactAggregate sum, count and max,
/// and a sketch, regrouped by `job`, and a selective variant, gets `Merge`.
#[test]
fn reduce_breakers_get_the_merge_rule() {
    let queries: [&str; 5] = [
        "sum_over_time(data[1m])",
        "count_over_time(data[1m])",
        "max_over_time(data[1m])",
        "quantile_over_time(0.9, data[1m])",
        "count_over_time(data{job=~\"job0|job1\"}[1m])",
    ];
    let mut checked = 0;
    for query in queries {
        let per_series = plan(&[(query, 10_000)], &["data"], 64, 1000, false);
        let Some(grouped) = regrouped_by_job(&per_series) else {
            continue;
        };
        let rules = combine_rules(&grouped).expect("combine rules");
        assert!(
            !rules.is_empty() && rules.iter().all(|r| r.rule == CombineRule::Merge),
            "{query}: {rules:?}"
        );
        checked += 1;
    }
    assert!(checked >= 3, "only {checked} regrouped plans checked");
}

/// T5, engine level: grouped by `job` over 4 shards, with one window with
/// no sample, a global `sum()` and a 2-key group-by, push stores one row
/// per (window, job), equal to the window path. The executor's compiler
/// refuses the chains regrouped by editing a planned plan ("temporal output
/// column missing or ambiguous"), and the Planner selects no PromQL plan of
/// this shape, so this needs a schema-complete hand-written plan first.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a compilable hand-written plan with chains grouped by job (the Planner selects none)"]
async fn reduce_breakers_merge_to_one_state_per_group() {
    unimplemented!("hand-written plan: SummaryTumblingWindowChain grouped by job")
}

/// T6. A breaker that does not merge runs on one partition, and a group
/// with a breaker that is not a per-arrival fold runs on the window path.
/// The Planner puts neither at ingestion time (an exact quantile is
/// planned over raw data at query time), so, as the plan allows, this is a
/// combine-level test: the plan's exact quantile, grouped by a label, and its raw
/// `TimeRange`, moved to ingestion time, get `SinglePartition` and
/// `WindowPath`; and that plan, as planned, answers in push mode what it
/// answers in window mode.
#[tokio::test(flavor = "multi_thread")]
async fn non_mergeable_breakers_run_on_one_partition_or_the_window_path() {
    use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPDAGNode;
    use data_plane::pipelines::combine_rule;

    // Exact: no accuracy requirement, raw data kept.
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": "quantile_over_time(0.9, data[1m])",
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 64.0}},
          "sources": {"data": {"series_count": 64, "sample_interval": 1000, "metric_type": "gauge",
              "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    let plan = plan_workload(&workload, &common::inputs(true)).unwrap().0;
    let at_ingestion = |kind: &str| -> PhysicalASAPDAGNode {
        let node = plan
            .dag
            .nodes
            .iter()
            .find(|n| {
                serde_json::to_value(&n.payload)
                    .unwrap()
                    .pointer(&format!("/NonASAP/{kind}"))
                    .is_some()
            })
            .unwrap_or_else(|| panic!("the exact quantile plan has a {kind}"));
        let mut json = serde_json::to_value(node).unwrap();
        json["output_state"]["timing"] = json!("ingestion_time");
        if kind == "Aggregate" {
            // Grouped by a label: a holistic state that no shard owns alone.
            let label = serde_json::to_value(&plan)
                .unwrap()
                .pointer("/dag/nodes")
                .and_then(|nodes| {
                    nodes.as_array().unwrap().iter().find_map(|n| {
                        let fields = n["payload"].pointer("/NonASAP/Scan/schema/fields")?;
                        fields
                            .as_array()
                            .unwrap()
                            .iter()
                            .position(|f| f["name"] != "ts" && f["name"] != "value")
                    })
                })
                .expect("the scan reads a label column");
            json["payload"]["NonASAP"]["Aggregate"]["reduction"] = json!({ "Reduce": [label] });
        }
        serde_json::from_value(json).unwrap()
    };
    assert_eq!(
        combine_rule(&at_ingestion("Aggregate")),
        Ok(Some(CombineRule::SinglePartition))
    );
    assert_eq!(
        combine_rule(&at_ingestion("TimeRange")),
        Ok(Some(CombineRule::WindowPath))
    );

    let times: Vec<i64> = vec![60_000, 70_000, 80_000];
    let (window, _) = install(&plan, options(WINDOW, 4));
    write_seconds(&window, &["data"], 64, 0, 80).await;
    barrier(&window, 80_000).await;
    let expected = answers(&window, &plan, &times).await;
    let (push, _) = install(&plan, options(PUSH, 4));
    write_seconds(&push, &["data"], 64, 0, 80).await;
    barrier(&push, 80_000).await;
    assert_eq!(answers(&push, &plan, &times).await, expected);
}

/// T7. Each breaker gets the rule of the design's table, and install fails,
/// naming the node, for a breaker no rule covers. Over the plan nodes of
/// every parity workload, each regrouped by `job`, and a reserved ASAP
/// operator.
#[test]
fn combine_rules_follow_the_table_and_refuse_unknown_breakers() {
    use asap_executor::planner::ir::{ASAPOp, NonASAPOp, Operator};
    use data_plane::pipelines::combine_rule;

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut seen: Vec<CombineRule> = Vec::new();
    for dir in ["../examples", "tests/workloads"] {
        let mut paths: Vec<_> = std::fs::read_dir(root.join(dir))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "json"))
            .collect();
        paths.sort();
        for path in paths {
            let workload = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            for raw in [true, false] {
                let Ok((plan, _)) = plan_workload(&workload, &common::inputs(raw)) else {
                    continue;
                };
                let regrouped = regrouped_by_job(&plan);
                for plan in std::iter::once(&plan).chain(regrouped.as_ref()) {
                    for node in &plan.dag.nodes {
                        let rule = combine_rule(node).unwrap_or_else(|e| {
                            panic!("{}: node {}: {e}", path.display(), node.id)
                        });
                        let at_ingestion = node.output_state.timing
                            == asap_executor::planner::ir::properties::ExecutionTiming::IngestionTime;
                        let expected = match &node.payload {
                            _ if !at_ingestion => None,
                            Operator::ASAP(
                                ASAPOp::SummaryTumblingWindowChain { reduction, .. }
                                | ASAPOp::SummaryAgg { reduction, .. },
                            ) => Some(match reduction {
                                asap_executor::planner::ir::operator::Reduction::PerEntity => {
                                    CombineRule::Concatenate
                                }
                                _ => CombineRule::Merge,
                            }),
                            Operator::NonASAP(
                                NonASAPOp::Filter { .. }
                                | NonASAPOp::Project { .. }
                                | NonASAPOp::TimeShift { .. }
                                | NonASAPOp::Scan { .. },
                            ) => None,
                            Operator::NonASAP(NonASAPOp::Sort { .. })
                            | Operator::NonASAP(NonASAPOp::Join { .. })
                            | Operator::NonASAP(NonASAPOp::SQLWindowFunc { .. }) => {
                                Some(CombineRule::SinglePartition)
                            }
                            Operator::NonASAP(NonASAPOp::PromqlSubquery { .. }) => {
                                Some(CombineRule::WindowPath)
                            }
                            _ => continue,
                        };
                        assert_eq!(rule, expected, "{}: node {}", path.display(), node.id);
                        if let Some(rule) = rule {
                            seen.push(rule);
                        }
                    }
                }
            }
        }
    }
    for rule in [CombineRule::Concatenate, CombineRule::Merge] {
        assert!(seen.contains(&rule), "no node got {rule:?}");
    }
    // A breaker no rule covers: a reserved ASAP operator.
    let plan = plan(
        &[("sum_over_time(data[1m])", 10_000)],
        &["data"],
        4,
        1000,
        false,
    );
    let mut json = serde_json::to_value(&plan).unwrap();
    let chain = json["dag"]["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|n| {
            n["payload"]
                .pointer("/ASAP/SummaryTumblingWindowChain")
                .is_some()
        })
        .expect("a chain");
    let child = chain["payload"]["ASAP"]["SummaryTumblingWindowChain"]["child"].clone();
    let id = chain["id"].clone();
    chain["payload"] = json!({"ASAP": {"Extension": {"child": child, "name": "unknown"}}});
    // The node keeps its ingestion timing.
    let unknown: InstallablePlan = serde_json::from_value(json).unwrap();
    let error = combine_rules(&unknown).expect_err("a breaker with no rule");
    assert!(
        error.contains(&id.to_string()),
        "the error names the node: {error}"
    );
}

// ── T8, T9: commit order and dependent groups ───────────────────────────

/// T8. Windows commit in window order when their combines finish out of
/// order: window 2's combine waits until window 3's is done; stored and
/// persisted rows are in window order, and a barrier past window 3
/// answers only once window 2 is committed.
#[tokio::test(flavor = "multi_thread")]
async fn windows_commit_in_window_order() {
    let plan = plan(
        &[("sum_over_time(data[30s])", 10_000)],
        &["data"],
        16,
        1000,
        false,
    );
    let dir = std::env::temp_dir().join(format!("push-commit-order-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for mode in [WINDOW, PUSH] {
        let _ = std::fs::remove_dir_all(&dir);
        let options = EngineOptions {
            view_dir: Some(dir.clone()),
            view_sync_interval_ms: 0,
            slow_refreshes: vec![SlowRefresh {
                group: None,
                end_ms: Some(20_000),
                delay_ms: 500,
                ..SlowRefresh::default()
            }],
            ..options(mode, 2)
        };
        let (app, state) = install(&plan, options.clone());
        write_seconds(&app, &["data"], 16, 0, 30).await;
        barrier(&app, 30_000).await;
        let views = stored(&state);
        for (view, windows) in &views {
            let ranges: Vec<(i64, i64)> = windows.iter().map(|(r, _)| *r).collect();
            assert_eq!(
                ranges,
                vec![(0, 10_000), (10_000, 20_000), (20_000, 30_000)],
                "{mode:?}: view {view}: windows in order, window 2 committed before the barrier answered"
            );
        }
        drop((app, state));
        // Persisted in the same order: a restart loads the same rows.
        let (_, reloaded) = install(&plan, EngineOptions { ..options });
        assert_eq!(stored(&reloaded), views, "{mode:?}: persisted order");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// T9. A group that reads another group's views runs once the windows it
/// reads are committed. The Planner selects no such plan (no view reads a
/// view), and whether the executor's compiler accepts a hand-written one
/// is not settled; the design runs it as a post-commit task (D7).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a plan with a group reading another group's views; the Planner selects none (D7)"]
async fn a_group_reading_another_groups_views_runs_after_their_commit() {
    unimplemented!("hand-written plan: group B (60 s) reading group A (15 s)")
}

// ── T10, T11: seal under skew, idle shards ──────────────────────────────

/// T10. Seal is exact under shard skew: with every shard but the first
/// slowed, and samples stamped up to 1 s ahead of the clock, each push
/// instance gets exactly the samples admitted before the flush that seals
/// it: no late drop, and every window equals one shard's window path.
#[tokio::test(flavor = "multi_thread")]
async fn seal_is_exact_under_shard_skew() {
    let plan = plan(
        &[("sum by (job) (sum_over_time(data[30s]))", 10_000)],
        &["data"],
        64,
        1000,
        false,
    );
    let times: Vec<i64> = (3..=12).map(|t| t * 10_000).collect();
    let (one, _) = install(&plan, options(WINDOW, 1));
    write_seconds(&one, &["data"], 64, 0, 120).await;
    barrier(&one, 120_000).await;
    let expected = answers(&one, &plan, &times).await;

    let (skewed, state) = install(
        &plan,
        EngineOptions {
            shard_delay_ms: 20,
            ingestion: IngestionOptions {
                allowed_lateness_ms: 0,
                max_future_ms: 1_000,
                stream_start_ms: Some(0),
                ..IngestionOptions::default()
            },
            ..options(PUSH, 8)
        },
    );
    write_seconds(&skewed, &["data"], 64, 0, 120).await;
    barrier(&skewed, 120_000).await;
    assert_eq!(state.engine.stats().samples_dropped_late, 0);
    assert_eq!(answers(&skewed, &plan, &times).await, expected);
}

/// T11. Idle shards seal empty and hold nothing back: one series over 8
/// shards, and a selective plan whose matched series land on few shards.
/// Every window commits when the barrier passes it, and every shard seals
/// every window (idle ones with an empty partial).
#[tokio::test(flavor = "multi_thread")]
async fn idle_shards_seal_empty_and_hold_nothing_back() {
    for (query, series) in [
        ("sum_over_time(data[30s])", 1),
        ("sum_over_time(data{instance=~\"i0|i1\"}[30s])", 64),
    ] {
        let plan = plan(&[(query, 10_000)], &["data"], series, 1000, false);
        let (app, state) = install(&plan, options(PUSH, 8));
        write_seconds(&app, &["data"], series, 0, 60).await;
        barrier(&app, 60_000).await;
        let windows = state.engine.stats().windows_refreshed;
        assert_eq!(windows, 6, "{query}: every window committed");
        let text = metrics_text(&app).await;
        for shard in 0..8 {
            let seals = metric_sum(
                &text,
                "asap_phase_runs_total",
                &["phase=\"precompute_seal\"", &format!("shard=\"{shard}\"")],
            );
            assert_eq!(
                seals,
                Some(windows as f64),
                "{query}: shard {shard} seals every window"
            );
        }
    }
}

// ── T12: no raw buffer for pushed pipelines ─────────────────────────────

/// T12. Pushed pipelines buffer no raw samples: the dashboard plan, raw
/// data not kept, 10k series at 1 sample/s, W = 15 s, 4 shards. Before each
/// close, raw bytes stay below one request's samples (not rate × W × 20 B,
/// 3 MB per metric) and the open state is reported. With raw data kept,
/// push holds no more raw data than the window path.
#[tokio::test(flavor = "multi_thread")]
async fn pushed_pipelines_buffer_no_raw_samples() {
    const SERIES: usize = 10_000;
    let workload = workload_file("dashboard", SERIES);
    let metrics = ["lat", "reqs"];
    let one_request_bytes = (SERIES * metrics.len() * 20) as f64;
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    let (app, _) = install(&plan, options(PUSH, 4));
    for second in 1..=44i64 {
        assert_eq!(
            post(&app, body(&metrics, 0..SERIES, &[second * 1000])).await,
            StatusCode::NO_CONTENT
        );
        if second % 15 == 14 {
            let text = metrics_text(&app).await;
            let raw = metric_sum(&text, "asap_raw_data_bytes", &[]).unwrap();
            assert!(
                raw < one_request_bytes,
                "at {second} s: {raw} raw bytes buffered"
            );
            assert!(
                metric_sum(&text, "asap_precompute_state_bytes", &[]).is_some(),
                "open state reported"
            );
        }
    }

    let kept = plan_workload(&workload, &common::inputs(true)).unwrap().0;
    let mut raw_bytes = Vec::new();
    for mode in [WINDOW, PUSH] {
        let (app, _) = install(&kept, options(mode, 4));
        write_seconds(&app, &metrics, SERIES, 0, 44).await;
        raw_bytes.push(metric_sum(&metrics_text(&app).await, "asap_raw_data_bytes", &[]).unwrap());
    }
    assert!(
        raw_bytes[1] <= raw_bytes[0],
        "push {} raw bytes, window {}",
        raw_bytes[1],
        raw_bytes[0]
    );
}

// ── T13: late and out-of-order samples ──────────────────────────────────

/// T13. With 5 s allowed lateness, samples 4 s late (their window still
/// open) and 20 s late (sealed) count in the same windows as on the window
/// path, counters included, raw data kept and not; the 20 s-late samples
/// are counted as `late_for_views`.
#[tokio::test(flavor = "multi_thread")]
async fn late_samples_count_in_the_window_paths_windows() {
    let queries: [Query; 2] = [
        ("sum_over_time(data[30s])", 10_000),
        ("rate(data_total[30s])", 10_000),
    ];
    let metrics = ["data", "data_total"];
    for raw in [true, false] {
        let plan = plan(&queries, &metrics, 16, 1000, raw);
        let times: Vec<i64> = (3..=6).map(|t| t * 10_000).collect();
        let mut results = Vec::new();
        for mode in [WINDOW, PUSH] {
            let (app, _) = install(
                &plan,
                EngineOptions {
                    ingestion: IngestionOptions {
                        allowed_lateness_ms: 5_000,
                        stream_start_ms: Some(0),
                        ..IngestionOptions::default()
                    },
                    ..options(mode, 4)
                },
            );
            // Seconds 1–40, every 7th second held back 4 s (still open);
            // then seconds 35–40 of series 0–3 again, 20 s late (sealed).
            for second in 1..=40i64 {
                if second % 7 != 0 {
                    post(&app, body(&metrics, 0..16, &[second * 1000])).await;
                }
                if second >= 4 && (second - 4) % 7 == 0 && second > 4 {
                    post(&app, body(&metrics, 0..16, &[(second - 4) * 1000])).await;
                }
            }
            for second in 60..=61i64 {
                post(&app, body(&metrics, 0..16, &[second * 1000])).await;
            }
            // Behind a barrier, so the window (40, 50] is sealed first.
            barrier(&app, 61_000).await;
            let late = body(&metrics, 0..4, &[40_500]);
            post(&app, late).await;
            barrier(&app, 61_000).await;
            let text = metrics_text(&app).await;
            let late_for_views =
                metric_sum(&text, "asap_samples_total", &["outcome=\"late_for_views\""]);
            results.push((answers(&app, &plan, &times).await, late_for_views));
        }
        assert_eq!(results[1].0, results[0].0, "raw kept {raw}: same windows");
        // 4 late samples per metric an ingestion-time view reads (with raw
        // data kept, the Planner may answer from raw data at query time,
        // and then no view holds them).
        let viewed_metrics = metrics
            .iter()
            .filter(|metric| {
                plan.dag.nodes.iter().any(|node| {
                    node.output_state.timing
                        == asap_executor::planner::ir::properties::ExecutionTiming::IngestionTime
                        && serde_json::to_value(&node.payload)
                            .unwrap()
                            .pointer("/NonASAP/Scan/source/TimeSeries/metric")
                            .is_some_and(|m| m == **metric)
                })
            })
            .count();
        assert_eq!(
            results[1].1,
            Some(4.0 * viewed_metrics as f64),
            "raw kept {raw}: 4 samples late for views per metric a view reads"
        );
        assert!(
            raw || viewed_metrics == 2,
            "without raw data both metrics are viewed"
        );
    }
}

// ── T14, T15: backpressure and memory reservations ──────────────────────

/// T14. Backpressure goes from the commit backlog to the shard queues to
/// Remote Write's 503, and loses nothing: with commits held and a backlog
/// of 2 windows, writes are refused; once released, every window commits
/// and the answers equal an unheld run's.
#[tokio::test(flavor = "multi_thread")]
async fn backpressure_reaches_remote_write_and_loses_nothing() {
    let plan = plan(
        &[("sum_over_time(data[30s])", 10_000)],
        &["data"],
        64,
        1000,
        false,
    );
    let times: Vec<i64> = (3..=12).map(|t| t * 10_000).collect();
    let (free, _) = install(&plan, options(PUSH, 4));
    write_seconds(&free, &["data"], 64, 0, 120).await;
    barrier(&free, 120_000).await;
    let expected = answers(&free, &plan, &times).await;

    let options = EngineOptions {
        max_precompute_backlog: 2,
        // One window computed at a time: the task pool's slots would
        // otherwise hold more windows than this test writes.
        precompute_threads: 1,
        max_queued_samples: 64 * 4,
        ..options(PUSH, 4)
    };
    let (app, state) = install(&plan, options);
    let held = state.engine.hold_precompute();
    let mut refused = false;
    let mut pending = Vec::new();
    for second in 1..=120i64 {
        let body = body(&["data"], 0..64, &[second * 1000]);
        if refused {
            pending.push(body);
            continue;
        }
        match post(&app, body.clone()).await {
            StatusCode::NO_CONTENT => {}
            StatusCode::SERVICE_UNAVAILABLE => {
                refused = true;
                pending.push(body);
            }
            status => panic!("{status}"),
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(refused, "a held commit backlog refuses writes with 503");
    drop(held);
    for body in pending {
        while post(&app, body.clone()).await == StatusCode::SERVICE_UNAVAILABLE {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    barrier(&app, 120_000).await;
    assert_eq!(answers(&app, &plan, &times).await, expected);
}

/// The most open-window state bytes push mode reported while `write` ran,
/// sampled after each request.
async fn peak_state_bytes(app: &Router, requests: Vec<Vec<u8>>) -> f64 {
    let mut peak: f64 = 0.0;
    for body in requests {
        assert_eq!(post(app, body).await, StatusCode::NO_CONTENT);
        let text = metrics_text(app).await;
        peak = peak.max(metric_sum(&text, "asap_precompute_state_bytes", &[]).unwrap_or(0.0));
    }
    peak
}

/// Push folding every request's samples as they come, so open state grows
/// request by request (the default batches them into larger morsels).
fn per_request() -> data_plane::push::MorselBatching {
    data_plane::push::MorselBatching {
        max_samples: 0,
        max_age_ms: 0,
    }
}

/// T15. A memory reservation waits while older windows are open and fails
/// the window only when nothing else is open. With a budget for one open
/// window and two windows open at once (lateness), ingest slows (queue,
/// then 503) but no window fails; a single window over budget fails alone,
/// and later windows commit.
#[tokio::test(flavor = "multi_thread")]
async fn memory_reservations_wait_then_fail_only_a_lone_window() {
    let plan = plan(
        &[("quantile_over_time(0.9, data[30s])", 10_000)],
        &["data"],
        2000,
        1000,
        false,
    );
    let small: Vec<Vec<u8>> = (1..=10)
        .map(|s| body(&["data"], 0..200, &[s * 1000]))
        .collect();
    let large: Vec<Vec<u8>> = (11..=20)
        .map(|s| body(&["data"], 0..2000, &[s * 1000]))
        .collect();
    // Calibration: the open state of one window of 200 and of 2000 series.
    let (app, _) = install(
        &plan,
        EngineOptions {
            push_batching: per_request(),
            ..options(PUSH, 4)
        },
    );
    let one_small = peak_state_bytes(&app, small.clone()).await;
    let one_large = peak_state_bytes(&app, large.clone()).await;
    assert!(one_large > one_small * 2.0, "{one_small} then {one_large}");

    // Two windows of 200 series open at once (5 s lateness), budget 1.5 of one.
    let (app, state) = install(
        &plan,
        EngineOptions {
            precompute_memory_budget_bytes: Some((one_small * 1.5) as u64),
            push_batching: per_request(),
            ingestion: IngestionOptions {
                allowed_lateness_ms: 5_000,
                stream_start_ms: Some(0),
                ..IngestionOptions::default()
            },
            ..options(PUSH, 4)
        },
    );
    for second in 1..=30i64 {
        let body = body(
            &["data"],
            0..200,
            &[second * 1000, (second - 3).max(1) * 1000],
        );
        while post(&app, body.clone()).await == StatusCode::SERVICE_UNAVAILABLE {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    barrier(&app, 30_000).await;
    assert_eq!(state.engine.stats().windows_failed, 0, "waits, never fails");

    // A lone window of 2000 series over a budget between one small and one
    // large window fails; the windows around it commit.
    let (app, state) = install(
        &plan,
        EngineOptions {
            precompute_memory_budget_bytes: Some(((one_small + one_large) / 2.0) as u64),
            push_batching: per_request(),
            ..options(PUSH, 4)
        },
    );
    for (i, body) in small.into_iter().chain(large).enumerate() {
        let _ = i;
        assert_eq!(post(&app, body).await, StatusCode::NO_CONTENT);
    }
    for second in 21..=30i64 {
        assert_eq!(
            post(&app, body(&["data"], 0..200, &[second * 1000])).await,
            StatusCode::NO_CONTENT
        );
    }
    barrier(&app, 30_000).await;
    let stats = state.engine.stats();
    assert_eq!(stats.windows_failed, 1, "{stats:?}");
    assert_eq!(stats.windows_refreshed, 2, "{stats:?}");
}

// ── T16: chunked seal ───────────────────────────────────────────────────

/// Bound on one seal chunk (ms). The design targets about 2 ms and E5 sets
/// it; the test allows a factor for scheduling.
const SEAL_CHUNK_BOUND_MS: u64 = 2;

/// T16. Seal work is chunked, so ingest keeps running during a large seal:
/// 100k series, one 1 s sketch view, 2 shards, requests sent continuously
/// across window ends. The longest stretch with requests queued and no
/// sample ingested stays below the chunk bound times a small factor.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy: 100k series; run in release with --ignored"]
async fn seal_work_is_chunked() {
    const SERIES: usize = 100_000;
    let plan = plan(
        &[("quantile_over_time(0.9, data[1s])", 1000)],
        &["data"],
        SERIES,
        100,
        false,
    );
    let (app, state) = install(&plan, options(PUSH, 2));
    let stop = Arc::new(AtomicBool::new(false));
    let longest_stall_ms = Arc::new(AtomicU64::new(0));
    let watcher = {
        let (state, stop, longest) = (state.clone(), stop.clone(), longest_stall_ms.clone());
        std::thread::spawn(move || {
            let mut last = (state.engine.stats().samples_accepted, Instant::now());
            while !stop.load(Ordering::Relaxed) {
                let accepted = state.engine.stats().samples_accepted;
                if accepted != last.0 || state.engine.queued_samples() == 0 {
                    last = (accepted, Instant::now());
                } else {
                    longest.fetch_max(last.1.elapsed().as_millis() as u64, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_micros(200));
            }
        })
    };
    for step in 1..=50i64 {
        let body = body(&["data"], 0..SERIES, &[step * 100]);
        while post(&app, body.clone()).await == StatusCode::SERVICE_UNAVAILABLE {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    barrier(&app, 5_000).await;
    stop.store(true, Ordering::Relaxed);
    watcher.join().unwrap();
    let stall = longest_stall_ms.load(Ordering::Relaxed);
    assert!(
        stall <= SEAL_CHUNK_BOUND_MS * 10,
        "ingest stalled {stall} ms during a seal"
    );
}

// ── T17: restart ────────────────────────────────────────────────────────

/// T17. A restart loads the view files whatever the shard count or mode:
/// written in push mode over 4 shards, restarted over 2, then in window
/// mode, with no new sample: the same answers, and no window appended
/// twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_loads_views_whatever_the_shard_count_or_mode() {
    let plan = plan(
        &[("sum by (job) (sum_over_time(data[30s]))", 10_000)],
        &["data"],
        64,
        1000,
        false,
    );
    let dir = std::env::temp_dir().join(format!("push-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let at = |mode, shards| EngineOptions {
        view_dir: Some(dir.clone()),
        view_sync_interval_ms: 0,
        ..options(mode, shards)
    };
    let times: Vec<i64> = vec![30_000, 40_000, 50_000, 60_000];
    let (app, state) = install(&plan, at(PUSH, 4));
    write_seconds(&app, &["data"], 64, 0, 60).await;
    barrier(&app, 60_000).await;
    let expected = answers(&app, &plan, &times).await;
    let rows = stored(&state);
    drop((app, state));
    for (mode, shards) in [(PUSH, 2), (WINDOW, 4)] {
        let (app, state) = install(&plan, at(mode, shards));
        barrier(&app, 60_000).await;
        assert_eq!(
            answers(&app, &plan, &times).await,
            expected,
            "{mode:?} over {shards}"
        );
        assert_eq!(
            stored(&state),
            rows,
            "{mode:?} over {shards}: no window appended twice"
        );
        assert_eq!(state.engine.stats().windows_refreshed, 0);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── T18: failures ───────────────────────────────────────────────────────

/// T18. A failing instance or combine fails only its window: in window 2
/// one shard's instance exceeds the run memory limit (many new series);
/// separately, window 2's combine panics. Window 2 is counted failed and
/// its readers are not answered; windows 1 and 3 commit; the engine keeps
/// running.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_instance_or_combine_fails_only_its_window() {
    let plan = plan(
        &[("max_over_time(data[10s])", 10_000)],
        &["data"],
        20_000,
        1000,
        false,
    );
    let write_three_windows = |app: Router| async move {
        for second in 1..=30i64 {
            let series = if (11..=20).contains(&second) {
                20_000
            } else {
                10
            };
            assert_eq!(
                post(&app, body(&["data"], 0..series, &[second * 1000])).await,
                StatusCode::NO_CONTENT
            );
        }
        barrier(&app, 30_000).await;
    };
    let limited = EngineOptions {
        run_memory_limit_bytes: 256 << 10,
        ..options(PUSH, 4)
    };
    let panicking = EngineOptions {
        panic_in_combine_of_window_end_ms: Some(20_000),
        ..options(PUSH, 4)
    };
    for (label, options) in [
        ("instance over its limit", limited),
        ("combine panics", panicking),
    ] {
        let (app, state) = install(&plan, options);
        write_three_windows(app.clone()).await;
        let stats = state.engine.stats();
        assert_eq!(stats.windows_failed, 1, "{label}: {stats:?}");
        assert_eq!(stats.windows_refreshed, 2, "{label}: {stats:?}");
        assert!(state.engine.is_running(), "{label}");
        let query = &plan.query_texts[0];
        assert_ne!(
            answer(&app, query, 20_000).await.0,
            StatusCode::OK,
            "{label}: window 2 unanswered"
        );
        assert_eq!(
            answer(&app, query, 30_000).await.0,
            StatusCode::OK,
            "{label}: window 3"
        );
    }
}

// ── T19: metrics ────────────────────────────────────────────────────────

/// T19. Precompute CPU and state are exported per group and shard: push,
/// seal, combine and commit phases, open state and the newest window's
/// commit lag, with run counts matching windows and morsels.
#[tokio::test(flavor = "multi_thread")]
async fn precompute_phases_are_exported_per_group_and_shard() {
    let plan = plan(
        &[("sum_over_time(data[30s])", 10_000)],
        &["data"],
        64,
        1000,
        false,
    );
    let (app, state) = install(&plan, options(PUSH, 4));
    write_seconds(&app, &["data"], 64, 0, 40).await;
    barrier(&app, 40_000).await;
    let windows = state.engine.stats().windows_refreshed as f64;
    assert_eq!(windows, 4.0);
    let text = metrics_text(&app).await;
    let runs = |phase: &str, extra: &[&str]| {
        let phase = format!("phase=\"{phase}\"");
        let mut matchers = vec![phase.as_str()];
        matchers.extend_from_slice(extra);
        metric_sum(&text, "asap_phase_runs_total", &matchers)
    };
    for shard in 0..4 {
        let shard = format!("shard=\"{shard}\"");
        assert_eq!(
            runs("precompute_seal", &[&shard, "group=\"0\""]),
            Some(windows),
            "{shard}"
        );
        let pushes = runs("precompute_push", &[&shard, "group=\"0\""]).expect("push runs");
        assert!(
            pushes >= 1.0 && pushes <= 40.0,
            "{shard}: {pushes} push runs for 40 requests"
        );
    }
    assert_eq!(runs("precompute_combine", &["group=\"0\""]), Some(windows));
    assert_eq!(runs("precompute_commit", &[]), Some(windows));
    for phase in [
        "precompute_push",
        "precompute_seal",
        "precompute_combine",
        "precompute_commit",
    ] {
        let phase = format!("phase=\"{phase}\"");
        assert!(
            metric_sum(&text, "asap_phase_cpu_seconds_total", &[&phase]).is_some(),
            "{phase} CPU"
        );
        assert!(
            metric_sum(&text, "asap_phase_wall_seconds_total", &[&phase]).is_some(),
            "{phase} wall"
        );
    }
    assert!(metric_sum(&text, "asap_precompute_state_bytes", &["group=\"0\""]).is_some());
    assert!(metric_sum(&text, "asap_precompute_open_instances", &[]).is_some());
    assert!(metric_sum(&text, "asap_window_commit_lag_seconds", &[]).is_some());
}
