//! Optional real-ClickHouse protocol and Grafana smoke coverage.
//!
//! Set `CLICKHOUSE_URL` (for example `http://127.0.0.1:8123`) to run it.

use std::sync::Arc;

use data_plane::query_engines::asap_clickhouse_query_engine::{
    ClickHouseHttpFallback, ClickHouseHttpServer,
};

#[tokio::test]
async fn exact_proxy_matches_clickhouse_for_sql_and_grafana_smoke_queries() {
    let Ok(clickhouse_url) = std::env::var("CLICKHOUSE_URL") else {
        eprintln!("skipping real ClickHouse E2E because CLICKHOUSE_URL is unset");
        return;
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fallback = Arc::new(ClickHouseHttpFallback::new(
        clickhouse_url.clone(),
        "default".into(),
    ));
    let app = ClickHouseHttpServer::router(fallback);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let client = reqwest::Client::new();
    let proxy_url = format!("http://{address}/");
    let queries = [
        "SELECT number, number * 2 AS doubled FROM numbers(10) ORDER BY number FORMAT JSONEachRow",
        "SELECT version() FORMAT TabSeparated",
        "SELECT name FROM system.databases ORDER BY name FORMAT JSONEachRow",
        "SELECT database, name FROM system.tables ORDER BY database, name FORMAT JSONEachRow",
    ];

    for sql in queries {
        let exact = client
            .get(&clickhouse_url)
            .query(&[("query", sql)])
            .send()
            .await
            .unwrap();
        let proxied = client
            .get(&proxy_url)
            .query(&[("query", sql)])
            .send()
            .await
            .unwrap();
        assert_eq!(proxied.status(), exact.status(), "status for {sql}");
        assert_eq!(
            proxied.bytes().await.unwrap(),
            exact.bytes().await.unwrap(),
            "body for {sql}"
        );
    }

    server.abort();
}
