//! Serve the small raw fixture using real canonical binding and native exact execution.
//! This is a local test server; it does not alter the production store or routing profile.
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use control_plane::physical::promql_exact::ExactPromqlPlan;
use data_plane::query_engines::canonical::exact_promql::{execute, Labels, RawSeries};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

struct Snapshot {
    data: Vec<RawSeries>,
    time: f64,
}
type Reply = (StatusCode, Json<Value>);
fn error(message: impl ToString) -> Reply {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"status":"error","errorType":"bad_data","error":message.to_string()})),
    )
}
fn timestamp(
    params: &HashMap<String, String>,
    key: &str,
    default: Option<f64>,
) -> anyhow::Result<f64> {
    let value = match params.get(key) {
        Some(value) => value.parse::<f64>()?,
        None => default.ok_or_else(|| anyhow::anyhow!("missing {key}"))?,
    };
    anyhow::ensure!(value.is_finite(), "nonfinite {key}");
    Ok(value)
}
fn sample_value(value: f64) -> String {
    if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        value.to_string()
    }
}
fn result(data: Value) -> Reply {
    (
        StatusCode::OK,
        Json(
            json!({"status":"success","data":data,"infos":["data_source: asap_exact","accuracy: exact","plan: bound canonical kernels"]}),
        ),
    )
}
async fn instant(
    State(snapshot): State<Arc<Snapshot>>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let run = (|| -> anyhow::Result<Value> {
        let text = params
            .get("query")
            .ok_or_else(|| anyhow::anyhow!("missing query"))?;
        let plan = ExactPromqlPlan::bind(text)?;
        let time = timestamp(&params, "time", Some(snapshot.time))?;
        let rows = execute(&plan, &snapshot.data, time, 300.0)?;
        Ok(
            json!({"resultType":"vector","result":rows.into_iter().map(|s|json!({"metric":s.labels,"value":[time,sample_value(s.value)]})).collect::<Vec<_>>()}),
        )
    })();
    match run {
        Ok(data) => result(data),
        Err(e) => error(e),
    }
}
async fn range(
    State(snapshot): State<Arc<Snapshot>>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let run = (|| -> anyhow::Result<Value> {
        let text = params
            .get("query")
            .ok_or_else(|| anyhow::anyhow!("missing query"))?;
        let plan = ExactPromqlPlan::bind(text)?;
        let start = timestamp(&params, "start", None)?;
        let end = timestamp(&params, "end", None)?;
        let step = timestamp(&params, "step", None)?;
        anyhow::ensure!(end >= start && step > 0.0, "invalid range bounds or step");
        let steps = ((end - start) / step).floor();
        anyhow::ensure!(steps < 11000.0, "too many evaluation steps");
        let count = steps as usize + 1;
        let mut rows: BTreeMap<Labels, Vec<Value>> = BTreeMap::new();
        for i in 0..count {
            let time = start + i as f64 * step;
            for sample in execute(&plan, &snapshot.data, time, 300.0)? {
                rows.entry(sample.labels)
                    .or_default()
                    .push(json!([time, sample_value(sample.value)]));
            }
        }
        Ok(
            json!({"resultType":"matrix","result":rows.into_iter().map(|(labels,values)|json!({"metric":labels,"values":values})).collect::<Vec<_>>()}),
        )
    })();
    match run {
        Ok(data) => result(data),
        Err(e) => error(e),
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let fixture = args
        .next()
        .unwrap_or_else(|| "tools/promql-smoke/cases.json".into());
    let listen = args.next().unwrap_or_else(|| "127.0.0.1:18081".into());
    anyhow::ensure!(
        args.next().is_none(),
        "usage: promql_exact_smoke [cases.json] [listen-address]"
    );
    let cases: Value = serde_json::from_slice(&std::fs::read(fixture)?)?;
    let start = cases["start"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("missing start"))?;
    let interval = cases["interval"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("missing interval"))?;
    let time = start
        + cases["eval_offset"]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("missing eval_offset"))?;
    let mut data = Vec::new();
    for series in cases["series"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing series"))?
    {
        let labels = serde_json::from_value(series["labels"].clone())?;
        let samples = series["values"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing values"))?
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.as_f64().map(|v| (start + i as f64 * interval, v)))
            .collect();
        data.push(RawSeries { labels, samples });
    }
    let app = Router::new()
        .route("/api/v1/query", get(instant))
        .route("/api/v1/query_range", get(range))
        .route("/api/v1/health", get(|| async { "ok" }))
        .with_state(Arc::new(Snapshot { data, time }));
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    eprintln!(
        "Native exact PromQL smoke server at http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
