use crate::input::{at_ms, Dataset, Range};
use anyhow::{ensure, Context, Result};
use prost::Message;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
// Remote Write v1's wire messages. These tags are the same ones accepted by
// the backend and Prometheus; keep the minimal schema independently testable.
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}
pub fn encode(data: &Dataset, base: i64) -> Result<Vec<u8>> {
    let timeseries = data
        .series
        .iter()
        .map(|s| {
            let mut labels = s.labels.clone();
            labels.insert("__name__".into(), s.metric.clone());
            let samples = s
                .samples
                .iter()
                .map(|p| {
                    Ok(Sample {
                        value: p.value,
                        timestamp: at_ms(base, p.offset_seconds)?,
                    })
                })
                .collect::<Result<_>>()?;
            Ok(TimeSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| Label { name, value })
                    .collect(),
                samples,
            })
        })
        .collect::<Result<_>>()?;
    Ok(snap::raw::Encoder::new().compress_vec(&WriteRequest { timeseries }.encode_to_vec())?)
}
pub fn client() -> Result<Client> {
    Ok(Client::builder().timeout(Duration::from_secs(60)).build()?)
}
pub async fn push(client: &Client, body: &[u8], targets: &[&str]) -> Result<()> {
    for target in targets {
        let response = client
            .post(format!("{target}/api/v1/write"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(body.to_vec())
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "remote write failed: {} {}",
            response.status(),
            response.text().await?
        );
    }
    Ok(())
}
pub async fn drain(client: &Client, url: &str) -> Result<()> {
    client
        .post(format!("{url}/api/v1/precompute/drain"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
pub async fn wait(client: &Client, url: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if client
                .get(url)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .context(format!("waiting for {url}"))?;
    Ok(())
}
pub async fn query(
    client: &Client,
    url: &str,
    expr: &str,
    base: i64,
    range: Option<&Range>,
    backend: bool,
) -> Result<Value> {
    let mut params = vec![("query", expr.to_string())];
    let path = if let Some(r) = range {
        params.extend([
            (
                "start",
                format!("{:.3}", at_ms(base, r.start_offset_seconds)? as f64 / 1000.),
            ),
            (
                "end",
                format!("{:.3}", at_ms(base, r.end_offset_seconds)? as f64 / 1000.),
            ),
            ("step", r.step_seconds.to_string()),
        ]);
        "query_range"
    } else {
        params.push(("time", format!("{:.3}", base as f64 / 1000.)));
        "query"
    };
    let response = client
        .get(format!("{url}/api/v1/{path}"))
        .query(&params)
        .send()
        .await?;
    let status = response.status();
    let execution = response
        .headers()
        .get("X-ASAP-Execution")
        .and_then(|s| s.to_str().ok());
    let detail = response
        .headers()
        .get("X-ASAP-Execution-Detail")
        .and_then(|s| s.to_str().ok());
    let source = match (execution, detail) {
        (Some("warm"), Some("asap")) => "asap_query",
        (Some("hybrid"), _) => "hybrid",
        _ => "prometheus_fallback",
    };
    let mut body: Value = response.json().await?;
    ensure!(status.is_success(), "query HTTP {status}: {body}");
    if backend {
        body["servedBy"] = Value::String(source.to_owned());
    }
    Ok(body)
}

/// Wait for acknowledged Remote Write data to become query-visible before measuring.
pub async fn wait_for_visible_query(
    client: &Client,
    url: &str,
    expr: &str,
    at: i64,
    expected: &Value,
    policy: &crate::input::Policy,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let actual = query(client, url, expr, at, None, false).await?;
        match crate::compare::compare(expected, &actual, policy) {
            Ok(()) => return Ok(()),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(error).context("acknowledged ingestion did not become query-visible");
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}
