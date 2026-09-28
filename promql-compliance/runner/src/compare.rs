use crate::input::Policy;
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};

pub fn equal_number(a: f64, b: f64, policy: &Policy) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a.is_infinite() || b.is_infinite() {
        return a == b;
    }
    let relative = policy
        .value_tolerance
        .as_ref()
        .and_then(|t| t.relative)
        .unwrap_or(0.);
    let absolute = policy
        .value_tolerance
        .as_ref()
        .and_then(|t| t.absolute)
        .unwrap_or(0.);
    (a - b).abs() <= absolute + relative * a.abs().max(b.abs())
}
#[derive(Debug, PartialEq)]
pub struct Point {
    pub labels: String,
    pub timestamp: i64,
    pub value: f64,
}
pub fn normalize(response: &Value) -> Result<Vec<Point>> {
    ensure!(response["status"] == "success", "query failed: {response}");
    let data = &response["data"];
    let mut points = Vec::new();
    match data["resultType"].as_str() {
        Some("vector" | "matrix") => {
            for s in data["result"].as_array().context("invalid result series")? {
                let labels: std::collections::BTreeMap<String, String> =
                    serde_json::from_value(s["metric"].clone())?;
                let labels = serde_json::to_string(&labels)?;
                let values = if data["resultType"] == "vector" {
                    vec![&s["value"]]
                } else {
                    s["values"]
                        .as_array()
                        .context("invalid matrix")?
                        .iter()
                        .collect()
                };
                for pair in values {
                    let (timestamp, value) = point(pair)?;
                    points.push(Point {
                        labels: labels.clone(),
                        timestamp,
                        value,
                    });
                }
            }
        }
        Some("scalar") => {
            let (timestamp, value) = point(&data["result"])?;
            points.push(Point {
                labels: String::new(),
                timestamp,
                value,
            });
        }
        _ => bail!("unsupported numeric result type"),
    }
    points.sort_by(|a, b| a.labels.cmp(&b.labels).then(a.timestamp.cmp(&b.timestamp)));
    ensure!(
        !points
            .windows(2)
            .any(|p| p[0].labels == p[1].labels && p[0].timestamp == p[1].timestamp),
        "duplicate response sample"
    );
    Ok(points)
}
fn point(value: &Value) -> Result<(i64, f64)> {
    let pair = value.as_array().context("invalid sample")?;
    ensure!(pair.len() == 2, "sample needs timestamp and value");
    Ok((
        crate::input::offset_ms(pair[0].as_f64().context("invalid timestamp")?)?,
        pair[1].as_str().context("invalid sample value")?.parse()?,
    ))
}
pub fn compare(left: &Value, right: &Value, policy: &Policy) -> Result<()> {
    ensure!(
        left["status"] == "success" && right["status"] == "success",
        "failed API response"
    );
    ensure!(
        left["data"]["resultType"] == right["data"]["resultType"],
        "result type mismatch"
    );
    if left["data"]["resultType"] == "string" {
        ensure!(
            left["data"]["result"] == right["data"]["result"],
            "string/timestamp mismatch"
        );
        return Ok(());
    }
    let a = normalize(left)?;
    let b = normalize(right)?;
    ensure!(
        a.len() == b.len(),
        "sample count mismatch {} != {}",
        a.len(),
        b.len()
    );
    for (a, b) in a.iter().zip(b.iter()) {
        ensure!(
            a.labels == b.labels && a.timestamp == b.timestamp,
            "labels/timestamp mismatch: {a:?} != {b:?}"
        );
        ensure!(
            equal_number(a.value, b.value, policy),
            "value mismatch: {} != {}",
            a.value,
            b.value
        );
    }
    Ok(())
}
pub fn parity(range: &Value, instant: &Value, at: i64, policy: &Policy) -> Result<()> {
    ensure!(
        range["data"]["resultType"] == "matrix" && instant["data"]["resultType"] == "vector",
        "invalid range/instant types"
    );
    let a = normalize(range)?
        .into_iter()
        .filter(|p| p.timestamp == at)
        .collect::<Vec<_>>();
    let b = normalize(instant)?;
    ensure!(a.len() == b.len(), "range/instant sample count mismatch");
    for (a, b) in a.iter().zip(b.iter()) {
        ensure!(
            a.labels == b.labels
                && a.timestamp == b.timestamp
                && equal_number(a.value, b.value, policy),
            "range/instant mismatch"
        );
    }
    Ok(())
}
pub fn outcome(result: Result<()>) -> Value {
    match result {
        Ok(()) => json!({"passed":true}),
        Err(e) => json!({"passed":false,"diff":format!("{e:#}")}),
    }
}
