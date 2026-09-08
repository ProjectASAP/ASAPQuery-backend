//! Verify typed residual execution against an external Prometheus reference.
//! This is operator conformance only, not Planner selection or acceleration evidence.
use control_plane::query_plan::{FallbackPolicy, InstantExecution, QueryPlanEntry};
use data_plane::{
    drivers::ingest::prometheus_remote_write::CanonicalSample,
    query_engines::{
        asap_query_engine::logical_dag::{execute_prepared, PreparedSamples},
        EngineError, QueryResult,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
};
#[derive(Deserialize)]
struct Row {
    metric: String,
    labels: BTreeMap<String, String>,
    timestamp_ms: i64,
    value: f64,
}
fn response(result: QueryResult) -> Value {
    let metric = |keys: Option<Vec<String>>, values: Vec<String>| {
        keys.unwrap_or_default()
            .into_iter()
            .zip(values)
            .collect::<BTreeMap<_, _>>()
    };
    let data = match result {
        QueryResult::Vector(v) => {
            json!({"resultType":"vector","result": v.values.into_iter().map(|x| json!({"metric":metric(x.label_keys_override,x.labels.labels),"value":[v.timestamp as f64/1000.,x.value.to_string()]})).collect::<Vec<_>>()})
        }
        QueryResult::Matrix(v) => {
            json!({"resultType":"matrix","result":v.values.into_iter().map(|x| json!({"metric":metric(x.label_keys_override,x.labels.labels),"values":x.samples.into_iter().map(|s| json!([s.timestamp as f64/1000.,s.value.to_string()])).collect::<Vec<_>>()})).collect::<Vec<_>>()})
        }
    };
    json!({"status":"success","data":data})
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 {
        return Err("usage: logical_dag_replay METRICS.jsonl QUERIES.json [EVAL_MS ...]".into());
    }
    let rows = BufReader::new(std::fs::File::open(&args[1])?)
        .lines()
        .map(|line| -> Result<_, Box<dyn std::error::Error>> {
            let row: Row = serde_json::from_str(&line?)?;
            Ok(CanonicalSample {
                series_key: serde_json::to_string(&(&row.metric, &row.labels))?,
                metric: row.metric,
                labels: row.labels.into_iter().collect(),
                timestamp_ms: row.timestamp_ms,
                value: Some(row.value),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let prepared = PreparedSamples::new(&rows)?;
    let corpus: Value = serde_json::from_reader(std::fs::File::open(&args[2])?)?;
    let times: Vec<u64> = if args.len() > 3 {
        args[3..]
            .iter()
            .map(|s| s.parse())
            .collect::<Result<_, _>>()?
    } else {
        vec![corpus["eval_timestamp_ms"]
            .as_u64()
            .ok_or("missing eval timestamp")?]
    };
    let mut results = Vec::new();
    for at in times {
        for entry in corpus["queries"].as_array().ok_or("missing queries")? {
            let q = entry["query"].as_str().ok_or("missing query")?;
            let start = std::time::Instant::now();
            let plan = QueryPlanEntry::compile_logical(
                entry["id"].to_string(),
                q.to_owned(),
                InstantExecution {
                    lookback_ms: 300_000,
                    full_history: false,
                    cumulative_readout: true,
                },
                FallbackPolicy::ExactBackend,
            )?;
            let result = execute_prepared(&plan, &prepared, at, |_, _| {
                Err(EngineError::capability_miss(
                    "conformance",
                    "unexpected summary callback",
                ))
            });
            results.push(match result {
                Ok(result) => json!({"id":entry["id"],"query":q,"evaluation_ms":at,"route":"typed_residual_conformance","status":"success","response":response(result),"elapsed_ns":start.elapsed().as_nanos()}),
                Err(err) => json!({"id":entry["id"],"query":q,"evaluation_ms":at,"route":"failed","status":"error","error":err.to_string()})
            });
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"sample_count":rows.len(),"results":results}))?
    );
    Ok(())
}
