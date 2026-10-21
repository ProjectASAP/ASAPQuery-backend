//! `--no-forward`: every answer is computed from the plan. A query the plan
//! cannot answer gets an error with the reason, and Prometheus receives no
//! query for it.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use asap_executor::planner::ir::{NonASAPOp, Operator, Predicate, ScalarExpr};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const MINUTE: i64 = 60_000;

/// A stand-in Prometheus that counts every request it receives.
async fn counting_prometheus() -> (String, Arc<AtomicUsize>) {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = requests.clone();
    let app = Router::new().fallback(move || {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            "{}"
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, requests)
}

fn options(no_forward: bool) -> EngineOptions {
    EngineOptions {
        no_forward,
        ..EngineOptions::default()
    }
}

async fn get(app: &Router, uri: &str) -> (StatusCode, Option<String>, Value) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let execution = response
        .headers()
        .get(EXECUTION_HEADER)
        .map(|v| v.to_str().unwrap().to_string());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        execution,
        serde_json::from_slice(&body).unwrap_or(Value::Null),
    )
}

fn instant(query: &str, time_ms: i64) -> String {
    format!(
        "/api/v1/query?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", &(time_ms as f64 / 1000.0).to_string())
            .finish()
    )
}

/// With the flag, an unplanned query, an evaluation whose pane is missing
/// and a range query each get a Prometheus `execution` error with the
/// reason, and the stand-in Prometheus receives no request.
#[tokio::test]
async fn refuses_what_the_plan_cannot_answer_without_asking_prometheus() {
    let (url, requests) = counting_prometheus().await;
    let state =
        DataPlane::with_options(common::plan_json("example4b", false), url, options(true)).unwrap();
    let app = router(Arc::new(state));

    let (status, execution, body) = get(&app, &instant("up", 70 * MINUTE)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(execution.as_deref(), Some("refused"));
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert!(
        body["error"].as_str().unwrap().contains("has no query"),
        "{body}"
    );

    // Nothing was written, so the panes the query reads are not stored.
    let query = "quantile_over_time(0.99, latency_ms[60m])";
    let (status, execution, body) = get(&app, &instant(query, 70 * MINUTE)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(execution.as_deref(), Some("refused"));
    assert!(
        body["error"].as_str().unwrap().contains("not stored"),
        "{body}"
    );

    let range = format!(
        "/api/v1/query_range?query={}&start=0&end=60&step=15",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let (status, execution, _) = get(&app, &range).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(execution.as_deref(), Some("refused"));

    assert_eq!(requests.load(Ordering::SeqCst), 0, "Prometheus got a query");
}

/// Without the flag the same unplanned query is forwarded, as before.
#[tokio::test]
async fn forwards_without_the_flag() {
    let (url, requests) = counting_prometheus().await;
    let state = DataPlane::with_options(common::plan_json("example4b", false), url, options(false))
        .unwrap();
    let app = router(Arc::new(state));
    let (status, execution, _) = get(&app, &instant("up", 70 * MINUTE)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(execution.as_deref(), Some("forwarded"));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

/// With the flag, a plan with a query the data plane cannot run is refused
/// at install; without it, the plan installs and that query is forwarded.
#[test]
fn refuses_to_install_a_plan_with_a_query_it_cannot_run() {
    let mut plan = common::plan("example1", true);
    // A label matcher on a column the `Scan` does not have: the selector of
    // its raw-sample window does not compile, so the queries that read it
    // cannot run.
    let scan = plan.query_time_raw_windows[0].scan_node_id;
    let node = plan
        .dag
        .dag
        .nodes
        .iter_mut()
        .find(|n| n.id == scan)
        .unwrap();
    let Operator::NonASAP(NonASAPOp::Scan { predicates, .. }) = &mut node.payload else {
        panic!("not a Scan");
    };
    predicates.push(Predicate(ScalarExpr::Column(99)));
    plan.validate().unwrap();
    let plan_json = serde_json::to_string(&plan).unwrap();
    let error = DataPlane::with_options(plan_json.clone(), "http://unused".into(), options(true))
        .err()
        .expect("refused");
    assert!(error.to_string().contains("--no-forward"), "{error}");
    assert!(DataPlane::with_options(plan_json, "http://unused".into(), options(false)).is_ok());
}
