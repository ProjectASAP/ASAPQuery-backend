//! What runs once per evaluation is the plan's decision: a query-time
//! `OneEvaluation` node that several planned queries read is computed once
//! per evaluation time and its result shared by their runs, and answers
//! are those of runs that each compute it.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use data_plane::engine::EngineOptions;
use data_plane::remote_write::{Label, Sample, TimeSeries};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

/// #509 Example 1 with raw data kept: `sum by (job) (rate(m[1m]))` and
/// `topk by (job) (10, sum_over_time(m[1m]))` share the planned
/// `OneEvaluation` `TimeRange` over the scan of `m`.
fn plan_json() -> String {
    common::plan_json("example1", true)
}

const QUERIES: [&str; 2] = [
    "sum by (job) (rate(http_requests_total[1m]))",
    "topk by (job) (10, sum_over_time(http_requests_total[1m]))",
];

/// A data plane over six counters (two jobs) sampled every 15 s for five
/// minutes, every window closed.
async fn data_plane(share: bool) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        share_one_evaluation_results: share,
        ..common::no_forward_options()
    };
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    let app = router(state.clone());
    let timeseries = (0..6)
        .map(|series| TimeSeries {
            labels: [
                ("__name__", "http_requests_total".to_string()),
                ("instance", format!("i{series}")),
                ("job", format!("job{}", series % 2)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect(),
            samples: (1..=20)
                .map(|step| Sample {
                    timestamp: step * 15_000,
                    value: (step * (series + 1)) as f64,
                })
                .collect(),
        })
        .collect();
    common::remote_write(&app, timeseries).await;
    common::barrier(&app, 300_000).await;
    (app, state)
}

/// `query`'s answer at `time_s`: each series' labels and value, sorted.
async fn answer(
    app: Router,
    query: &'static str,
    time_s: i64,
) -> Vec<(BTreeMap<String, String>, String)> {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let response = app
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(status, StatusCode::OK, "{query}: {body}");
    let mut rows: Vec<_> = body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| {
            (
                serde_json::from_value(sample["metric"].clone()).unwrap(),
                sample["value"][1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// Both queries run at once at each of two evaluation times: the shared
/// node is computed once per time, the second reader takes the stored
/// result, and both answers equal those of runs that compute it
/// themselves.
#[tokio::test(flavor = "multi_thread")]
async fn a_shared_one_evaluation_node_is_computed_once_per_evaluation_time() {
    let (app, state) = data_plane(true).await;
    let (reference_app, reference) = data_plane(false).await;
    let shared = state.engine.shared_nodes();
    assert!(
        shared > 0,
        "the plan shares no query-time OneEvaluation node"
    );
    assert_eq!(reference.engine.shared_nodes(), 0);
    for (round, time_s) in [240, 300].into_iter().enumerate() {
        let (first, second) = tokio::join!(
            answer(app.clone(), QUERIES[0], time_s),
            answer(app.clone(), QUERIES[1], time_s)
        );
        let results = state.engine.query_time_results();
        assert_eq!(results.computes(), (round as u64 + 1) * shared as u64);
        assert_eq!(results.hits(), (round as u64 + 1) * shared as u64);
        assert!(!first.is_empty() && !second.is_empty());
        assert_eq!(
            first,
            answer(reference_app.clone(), QUERIES[0], time_s).await
        );
        assert_eq!(
            second,
            answer(reference_app.clone(), QUERIES[1], time_s).await
        );
    }
    assert_eq!(reference.engine.query_time_results().computes(), 0);
}

/// A plan whose shared node is computed per run (`None`) contradicts
/// itself and is refused at install.
#[test]
fn refuses_a_plan_that_does_not_share_a_node_two_queries_compute() {
    let mut plan: Value = serde_json::from_str(&plan_json()).unwrap();
    let roots: Vec<u64> = serde_json::from_value(plan["dag"]["roots"].clone()).unwrap();
    // The node both roots reach: a producer with two consumers.
    let edges = plan["dag"]["edges"].as_array().unwrap().clone();
    let shared = edges
        .iter()
        .map(|e| e["producer"].as_u64().unwrap())
        .find(|p| edges.iter().filter(|e| e["producer"] == *p).count() > 1)
        .unwrap();
    assert!(!roots.contains(&shared));
    for node in plan["dag"]["nodes"].as_array_mut().unwrap() {
        if node["id"] == shared {
            node["lifetime"] = "None".into();
        }
    }
    let refused = DataPlane::new(plan.to_string(), "http://127.0.0.1:1".into(), 0);
    let error = refused.err().expect("the plan is installed").to_string();
    assert!(
        error.contains(&format!("node {shared} (QueryTime, None)"))
            && error.contains("shares only query-time OneEvaluation nodes"),
        "{error}"
    );
}
