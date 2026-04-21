//! End-to-end tests for the §5.2 cold-query fallback.
//!
//! Exercises the full HTTP → engine-miss → [`ColdFallback`] path
//! with raw sample fixtures on a local-FS [`ColdStore`]. The
//! format used here (hour-bucketed JSONL) is byte-identical to
//! what a future S3 cold adapter will read, so these tests
//! double as format-lock tests for the on-disk / on-object layout.
//!
//! Coverage:
//! * bare instant vector → cold path, correct per-series latest
//! * `sum(...)` → cold path, correct scalar
//! * unsupported query shape (e.g. `rate(...)`) falls through to
//!   the inner Prometheus chain
//! * telemetry counters increment on cold hits
//! * purged-time-range semantics: sketch-absent data served from cold

#[cfg(test)]
use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::fallback::cold_store::format::RawSample;
use crate::drivers::query::fallback::metrics::{BYTES_SERVED_COLD_TOTAL, QUERIES_COLD_TOTAL};
use crate::drivers::query::fallback::{
    ColdFallback, FallbackClient, LocalFsColdStore, PrometheusHttpFallback,
};
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::engines::SimpleEngine;
use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
use chrono::{TimeZone, Utc};
use reqwest::Client;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

/// Build a [`RawSample`] with ergonomic literal labels.
fn sample(ts_ms: i64, labels: &[(&str, &str)], value: f64) -> RawSample {
    RawSample {
        ts_ms,
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<BTreeMap<_, _>>(),
        value,
    }
}

/// Write a JSONL part under the hour-bucket prefix (same key
/// layout as S3).
async fn write_part(root: &Path, rel: &str, lines: &[RawSample]) {
    let dir = root.join(rel);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let mut buf = String::new();
    for s in lines {
        buf.push_str(&serde_json::to_string(s).unwrap());
        buf.push('\n');
    }
    tokio::fs::write(dir.join("part-000001.jsonl"), buf)
        .await
        .unwrap();
}

fn make_engine_and_store() -> (Arc<SimpleEngine>, Arc<SimpleMapStore>) {
    let inference_config = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    let streaming_config = Arc::new(StreamingConfig::default());
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    let engine = Arc::new(SimpleEngine::new(
        store.clone(),
        inference_config,
        streaming_config.clone(),
        15000,
        QueryLanguage::promql,
    ));
    (engine, store)
}

/// Start an HTTP server whose fallback chain is
/// `ColdFallback(LocalFsColdStore) → None`. Returns the server
/// port. No sketch data is ingested, so every query
/// capability-misses and is served from cold.
async fn start_cold_only_server(cold_root: &Path) -> u16 {
    let cold_store = Arc::new(LocalFsColdStore::new(cold_root));
    let cold = Arc::new(ColdFallback::new(cold_store)) as Arc<dyn FallbackClient>;

    let adapter_config = AdapterConfig::new(
        crate::data_model::enums::QueryProtocol::PrometheusHttp,
        QueryLanguage::promql,
        Some(cold),
    );
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };
    let (engine, store) = make_engine_and_store();
    let server = HttpServer::new(config, engine, store, None);
    server
        .start_test_server()
        .await
        .expect("Failed to start test server")
}

/// Mock upstream Prometheus that returns a fixed marker body, used
/// to check that unsupported shapes fall through the cold adapter
/// to the inner chain.
async fn start_mock_prometheus(port: u16, marker: &'static str) {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    async fn h(marker: &'static str) -> Json<Value> {
        Json(json!({
            "status": "success",
            "data": {"resultType":"scalar", "result":[0, marker]}
        }))
    }
    let app = Router::new().route("/api/v1/query", get(move || h(marker)));
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;
}

async fn start_chained_server(cold_root: &Path, prom_url: String) -> u16 {
    let cold_store = Arc::new(LocalFsColdStore::new(cold_root));
    let prom: Arc<dyn FallbackClient> = Arc::new(PrometheusHttpFallback::new(prom_url));
    let cold = Arc::new(ColdFallback::new(cold_store).with_inner(prom)) as Arc<dyn FallbackClient>;
    let adapter_config = AdapterConfig::new(
        crate::data_model::enums::QueryProtocol::PrometheusHttp,
        QueryLanguage::promql,
        Some(cold),
    );
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };
    let (engine, store) = make_engine_and_store();
    let server = HttpServer::new(config, engine, store, None);
    server
        .start_test_server()
        .await
        .expect("Failed to start test server")
}

#[tokio::test]
async fn cold_fallback_serves_bare_selector_from_raw_samples() {
    let tmp = TempDir::new().unwrap();
    // 2026-04-21 08:00:00 UTC
    let base = Utc
        .with_ymd_and_hms(2026, 4, 21, 8, 0, 0)
        .unwrap()
        .timestamp_millis();
    write_part(
        tmp.path(),
        "raw/http_requests_total/2026/04/21/08/",
        &[
            sample(base + 10_000, &[("zone", "a")], 1.0),
            sample(base + 20_000, &[("zone", "a")], 2.0), // latest for zone=a
            sample(base + 15_000, &[("zone", "b")], 9.0),
        ],
    )
    .await;

    let server_port = start_cold_only_server(tmp.path()).await;
    let client = Client::new();

    // Query time = base + 30s. Lookback window covers all three samples.
    let query_time = (base + 30_000) as f64 / 1_000.0;
    let resp = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[
            ("query", "http_requests_total".to_string()),
            ("time", query_time.to_string()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();

    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "vector");
    let items = body["data"]["result"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    // Find zone=a entry and verify it got the latest value.
    let za = items
        .iter()
        .find(|v| v["metric"]["zone"] == "a")
        .expect("zone=a series");
    assert_eq!(za["metric"]["__name__"], "http_requests_total");
    assert_eq!(za["value"][1], "2");
}

#[tokio::test]
async fn cold_fallback_serves_sum_aggregation() {
    let tmp = TempDir::new().unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 4, 21, 8, 0, 0)
        .unwrap()
        .timestamp_millis();
    write_part(
        tmp.path(),
        "raw/cpu_seconds_total/2026/04/21/08/",
        &[
            sample(base + 1_000, &[("pod", "a")], 10.0),
            sample(base + 2_000, &[("pod", "b")], 20.0),
            sample(base + 3_000, &[("pod", "c")], 30.0),
        ],
    )
    .await;

    let server_port = start_cold_only_server(tmp.path()).await;
    let client = Client::new();

    let query_time = (base + 10_000) as f64 / 1_000.0;
    let resp = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[
            ("query", "sum(cpu_seconds_total)".to_string()),
            ("time", query_time.to_string()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();

    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "vector");
    let items = body["data"]["result"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["value"][1], "60");
    // Aggregation without grouping → empty metric labels.
    let metric = items[0]["metric"].as_object().unwrap();
    assert!(metric.is_empty());
}

#[tokio::test]
async fn cold_fallback_label_matcher_filters() {
    let tmp = TempDir::new().unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 4, 21, 8, 0, 0)
        .unwrap()
        .timestamp_millis();
    write_part(
        tmp.path(),
        "raw/requests_total/2026/04/21/08/",
        &[
            sample(base + 1_000, &[("zone", "a")], 1.0),
            sample(base + 2_000, &[("zone", "b")], 2.0),
            sample(base + 3_000, &[("zone", "c")], 3.0),
        ],
    )
    .await;

    let server_port = start_cold_only_server(tmp.path()).await;
    let client = Client::new();

    let query_time = (base + 10_000) as f64 / 1_000.0;
    let resp = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[
            ("query", "sum(requests_total{zone=\"b\"})".to_string()),
            ("time", query_time.to_string()),
        ])
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let items = body["data"]["result"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["value"][1], "2");
}

#[tokio::test]
async fn cold_fallback_unsupported_query_delegates_to_inner() {
    let tmp = TempDir::new().unwrap();
    // Pick a deterministic port for the mock Prom — low risk of
    // collision with the rest of the suite since each test picks
    // a different one.
    let prom_port = 19_201;
    start_mock_prometheus(prom_port, "MARKER_INNER").await;

    let server_port =
        start_chained_server(tmp.path(), format!("http://127.0.0.1:{prom_port}")).await;
    let client = Client::new();

    // rate(...) is not a shape we handle in cold — expect the
    // inner Prometheus mock to answer.
    let resp = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[("query", "rate(foo[1m])"), ("time", "1000")])
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success");
    // Mock Prom returns a scalar with "MARKER_INNER" at index [1].
    assert_eq!(body["data"]["result"][1], "MARKER_INNER");
}

#[tokio::test]
async fn cold_fallback_increments_telemetry_counters() {
    let tmp = TempDir::new().unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 4, 21, 8, 0, 0)
        .unwrap()
        .timestamp_millis();
    write_part(
        tmp.path(),
        "raw/telemetry_test_metric/2026/04/21/08/",
        &[sample(base + 1_000, &[("zone", "a")], 1.0)],
    )
    .await;

    let before_q = QUERIES_COLD_TOTAL
        .with_label_values(&["telemetry_test_metric", "sum"])
        .get();
    let before_b = BYTES_SERVED_COLD_TOTAL
        .with_label_values(&["telemetry_test_metric", "sum"])
        .get();

    let server_port = start_cold_only_server(tmp.path()).await;
    let client = Client::new();
    let query_time = (base + 10_000) as f64 / 1_000.0;
    let _ = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[
            ("query", "sum(telemetry_test_metric)".to_string()),
            ("time", query_time.to_string()),
        ])
        .send()
        .await
        .unwrap();

    let after_q = QUERIES_COLD_TOTAL
        .with_label_values(&["telemetry_test_metric", "sum"])
        .get();
    let after_b = BYTES_SERVED_COLD_TOTAL
        .with_label_values(&["telemetry_test_metric", "sum"])
        .get();
    assert!(
        after_q >= before_q + 1.0,
        "expected cold queries counter to advance: before={before_q} after={after_q}"
    );
    assert!(
        after_b > before_b,
        "expected cold bytes-served counter to advance"
    );
}

#[tokio::test]
async fn cold_fallback_purged_time_range_served_from_raw() {
    // Simulates the §5.2 "Purged segment" path: no sketch exists
    // for the queried range (nothing ingested into the engine),
    // but the raw tier has samples, and the cold adapter recovers
    // the exact answer. This is the canonical paper claim.
    let tmp = TempDir::new().unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 4, 21, 8, 0, 0)
        .unwrap()
        .timestamp_millis();
    // Three distinct series so `avg` runs over three latest-per-series
    // values — matches Prometheus instant-vector semantics.
    write_part(
        tmp.path(),
        "raw/purged_metric/2026/04/21/08/",
        &[
            sample(base + 1_000, &[("pod", "a")], 10.0),
            sample(base + 2_000, &[("pod", "b")], 20.0),
            sample(base + 3_000, &[("pod", "c")], 30.0),
        ],
    )
    .await;

    let server_port = start_cold_only_server(tmp.path()).await;
    let client = Client::new();
    let query_time = (base + 10_000) as f64 / 1_000.0;

    // avg over the cold raw samples.
    let resp = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[
            ("query", "avg(purged_metric)".to_string()),
            ("time", query_time.to_string()),
        ])
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let items = body["data"]["result"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    // avg(10, 20, 30) = 20.
    assert_eq!(items[0]["value"][1], "20");
}
