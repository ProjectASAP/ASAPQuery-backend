use crate::input::{at_ms, Dataset};
use anyhow::{bail, ensure, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

pub fn window_ms(expr: &str) -> Result<i64> {
    let regex = regex::Regex::new(r"\[(\d+)([smhd])\]")?;
    let Some(c) = regex.captures(expr) else {
        return Ok(60_000);
    };
    let n: i64 = c[1].parse()?;
    let unit = match &c[2] {
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => 86_400_000,
    };
    n.checked_mul(unit).context("window overflow")
}
pub fn baseline(name: &str, evaluation_ms: i64, window_ms: i64) -> Result<String> {
    ensure!(window_ms > 0, "positive window required");
    let (counter, query) = match name {
        "spatial-sum" => (
            false,
            r#"SELECT label_0, sum(value) AS value FROM instant_samples GROUP BY label_0"#,
        ),
        "spatial-topk" => (
            false,
            r#"SELECT series_id, label_0, value FROM instant_samples ORDER BY label_0, value DESC, series_id LIMIT 3 BY label_0"#,
        ),
        "spatial-quantile" => (
            false,
            r#"SELECT label_0, quantileExactInclusive(0.9)(value) AS value FROM instant_samples GROUP BY label_0"#,
        ),
        "temporal-sum" => (
            false,
            r#"SELECT series_id, sum(value) AS value FROM window_samples GROUP BY series_id"#,
        ),
        "temporal-quantile" => (
            false,
            r#"SELECT series_id, quantileExactInclusive(0.9)(value) AS value FROM window_samples GROUP BY series_id"#,
        ),
        "temporal-rate" => (
            true,
            r#"SELECT series_id, rate_value AS value FROM per_series_counter"#,
        ),
        "grouped-rate" => (
            true,
            r#"SELECT label_0, sum(rate_value) AS value FROM per_series_counter GROUP BY label_0"#,
        ),
        "grouped-temporal-sum" => (
            false,
            r#"SELECT label_0, sum(series_sum) AS value FROM (SELECT series_id, label_0, sum(value) AS series_sum FROM window_samples GROUP BY series_id, label_0) GROUP BY label_0"#,
        ),
        "topk-rate" => (
            true,
            r#"SELECT series_id, label_0, rate_value AS value FROM per_series_counter ORDER BY label_0, rate_value DESC, series_id LIMIT 3 BY label_0"#,
        ),
        "quantile-ratio" => (
            false,
            r#"SELECT series_id, quantileExactInclusive(0.9)(value) / quantileExactInclusive(0.5)(value) AS value FROM window_samples GROUP BY series_id"#,
        ),
        _ => bail!("no ClickHouse baseline for {name}"),
    };
    let prefix = include_str!("../sql/prefix.sql")
        .replace("{evaluation_ms}", &evaluation_ms.to_string())
        .replace("{window_ms}", &window_ms.to_string());
    Ok(format!(
        "{prefix}{}\n{query} FORMAT JSON",
        if counter {
            include_str!("../sql/counter.sql")
        } else {
            ""
        }
    ))
}
pub async fn post(client: &Client, url: &str, sql: &str, body: Option<String>) -> Result<String> {
    let request = client.post(format!("{}/", url.trim_end_matches('/')));
    let request = if let Some(body) = body {
        request.query(&[("query", sql)]).body(body)
    } else {
        request.body(sql.to_owned())
    };
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(status.is_success(), "ClickHouse {status}: {text}");
    Ok(text)
}
pub async fn rows(client: &Client, url: &str, sql: &str) -> Result<Vec<Value>> {
    let response: Value = serde_json::from_str(&post(client, url, sql, None).await?)?;
    Ok(response["data"]
        .as_array()
        .context("missing ClickHouse rows")?
        .clone())
}
pub async fn seed(client: &Client, url: &str, data: &Dataset, base: i64) -> Result<()> {
    post(client,url,"CREATE TABLE IF NOT EXISTS samples (series_id UInt64, label_0 String, ts_ms Int64, value Float64) ENGINE = MergeTree ORDER BY (series_id, ts_ms)",None).await?;
    post(client, url, "TRUNCATE TABLE samples", None).await?;
    let mut body = String::new();
    for (i, s) in data.series.iter().enumerate() {
        for p in &s.samples {
            body.push_str(&serde_json::to_string(&json!({"series_id":i+1,"label_0":s.labels.get("label_0").context("missing label_0")?,"ts_ms":at_ms(base,p.offset_seconds)?,"value":p.value}))?);
            body.push('\n');
        }
    }
    post(
        client,
        url,
        "INSERT INTO samples FORMAT JSONEachRow",
        Some(body),
    )
    .await?;
    Ok(())
}
