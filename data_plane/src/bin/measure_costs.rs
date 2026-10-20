//! Measures what the data plane's operations cost on this machine and
//! writes them as ASAPPlanner `MeasuredOperationCost` entries, for Stage 3
//! to price plans from (`PlanSelectionSettings::measured_operation_costs`).
//!
//! Each operation runs the executor's own operator, as the data plane runs
//! it, on one core:
//! - `update`: the `SummaryAgg` build over a batch of samples (the query-time
//!   build), per input row;
//! - `merge`: `SummaryMerge` over panes' states, per input state;
//! - `estimate`: `SummaryEstimate` (p99, or a distinct count) per state.
//!
//! The sketch configurations are those of the given plans' summaries, plus
//! the base table's KLL, DDSketch and HLL entries. Entries of the base table
//! for configurations not measured here (e.g. Count-Min with heap) are kept.
//!
//! Costs the table cannot hold (it prices sketch operations only) are
//! written to `--report`: exact accumulators, the per-sample ingestion
//! update, and a query-time raw window per raw sample end to end.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use asap_executor::arrow::array::{Float64Array, TimestampMillisecondArray};
use asap_executor::operators::{Operator, SummaryEvaluation};
use asap_executor::plan::PhysicalDAG;
use asap_executor::planner::ir::schema::{
    DataType, Field, FieldDataType, Schema, SketchAlgorithm, SketchStatistic,
};
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, BatchColumn, SchemaRef};
use asap_executor::StateColumnUpdater;
use asap_planner::InstallablePlan;
use clap::Parser;
use futures::StreamExt;
use serde_json::{json, Value};

#[derive(Parser)]
struct Args {
    /// Installable plans whose summary families to measure.
    #[arg(long = "plan")]
    plans: Vec<PathBuf>,
    /// A `MeasuredOperationCost` list to extend: its KLL, DDSketch and HLL
    /// configurations are measured too, and its other entries kept.
    #[arg(long)]
    base_table: Option<PathBuf>,
    /// Where to write the `MeasuredOperationCost` list.
    #[arg(long)]
    out: PathBuf,
    /// Where to write the costs the table cannot hold.
    #[arg(long)]
    report: PathBuf,
    /// A plan whose queries read raw samples at query time: each such
    /// query is evaluated end to end, and its cost per raw sample reported.
    #[arg(long)]
    raw_plan: Option<PathBuf>,
    /// Series per measurement.
    #[arg(long, default_value_t = 1000)]
    series: usize,
    /// Samples per series in one pane.
    #[arg(long, default_value_t = 40)]
    samples_per_pane: usize,
    /// Panes merged per group.
    #[arg(long, default_value_t = 6)]
    panes: usize,
    /// Repetitions; the median is reported.
    #[arg(long, default_value_t = 5)]
    repetitions: usize,
}

/// `identity, ts, value`: the input of a per-series summary build.
fn input_schema() -> SchemaRef {
    Arc::new(Schema::with_time_index(
        vec![
            Field::plain("$promql_series_identity", DataType::Utf8, false),
            Field::plain("ts", DataType::Timestamp, false),
            Field::plain("value", DataType::Float64, false),
        ],
        1,
        vec![],
    ))
}

/// `series` series' samples, `per_series` each, in pane `pane`: series by
/// series, in time order; values spread like latencies.
fn input_batch(series: usize, per_series: usize, pane: usize) -> anyhow::Result<Batch> {
    let rows = series * per_series;
    let identities: Vec<String> = (0..series).map(|s| format!("{{\"s\":\"{s}\"}}")).collect();
    let texts = (0..rows).map(|r| identities[r / per_series].as_str());
    let identity = data_plane_text_column(texts)?;
    let base = (pane * per_series) as i64 * 15_000;
    let ts = TimestampMillisecondArray::from_iter_values(
        (0..rows).map(|r| base + (r % per_series) as i64 * 15_000 + 1),
    );
    let value = Float64Array::from_iter_values(
        (0..rows).map(|r| ((r * 7919 + pane * 104_729) % 10_007) as f64 + 1.0),
    );
    Ok(Batch::try_from_columns(
        input_schema(),
        vec![
            BatchColumn::Plain(identity),
            BatchColumn::Plain(Arc::new(ts)),
            BatchColumn::Plain(Arc::new(value)),
        ],
    )?)
}

/// A dictionary text column.
fn data_plane_text_column<'a>(
    texts: impl Iterator<Item = &'a str>,
) -> anyhow::Result<asap_executor::arrow::array::ArrayRef> {
    use asap_executor::arrow::array::{DictionaryArray, StringArray, UInt32Array};
    let mut codes: BTreeMap<&str, u32> = BTreeMap::new();
    let mut values = Vec::new();
    let keys: UInt32Array = texts
        .map(|t| {
            let next = values.len() as u32;
            *codes.entry(t).or_insert_with(|| {
                values.push(t);
                next
            })
        })
        .collect();
    Ok(Arc::new(DictionaryArray::try_new(
        keys,
        Arc::new(StringArray::from(values)),
    )?))
}

/// Runs `operator` over `inputs` and returns its output and the time it
/// took, in nanoseconds.
fn run(operator: Operator, inputs: Vec<Batch>) -> anyhow::Result<(Vec<Batch>, f64)> {
    let schema = inputs
        .first()
        .map(|b| b.schema().clone())
        .context("no input")?;
    let mut dag = PhysicalDAG::default();
    dag.add(0, vec![], Operator::source(schema, inputs)?)?;
    dag.add(1, vec![0], operator)?;
    let limits = Limits {
        max_bytes: 8 << 30,
        ..Limits::default()
    };
    let context = RunContext::new(
        Scope::Query {
            evaluation_time_ms: 0,
            revision: 1,
        },
        limits,
    )?;
    let started = Instant::now();
    let batches = futures::executor::block_on(async {
        let mut output = dag.execute(&[1], context)?.remove(0);
        let mut batches = Vec::new();
        while let Some(batch) = output.next().await {
            batches.push((*batch?).clone());
        }
        Ok::<_, asap_executor::Error>(batches)
    })?;
    Ok((batches, started.elapsed().as_nanos() as f64))
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// Measured nanoseconds per operation of `family`.
struct Measured {
    query_time_update: f64,
    ingestion_update: f64,
    merge: f64,
    estimate: Option<f64>,
}

fn measure(family: &FieldDataType, args: &Args) -> anyhow::Result<Measured> {
    let (series, per_pane) = (args.series, args.samples_per_pane);
    let rows = (series * per_pane) as f64;
    let build = |input: &Batch| {
        Operator::per_group_summary_build(input_schema(), family.clone(), 2, Some(1), vec![0])
            .map_err(anyhow::Error::from)
            .and_then(|op| run(op, vec![input.clone()]))
    };
    let mut query_time_update = Vec::new();
    let mut ingestion_update = Vec::new();
    let mut merge = Vec::new();
    let mut estimate = Vec::new();
    for _ in 0..args.repetitions {
        let panes = (0..args.panes)
            .map(|pane| input_batch(series, per_pane, pane))
            .collect::<anyhow::Result<Vec<_>>>()?;
        // Query-time build: the executor's operator over a batch.
        let mut states = Vec::new();
        let mut build_ns = 0.0;
        for pane in &panes {
            let (out, ns) = build(pane)?;
            build_ns += ns;
            states.extend(out);
        }
        query_time_update.push(build_ns / (rows * args.panes as f64));
        // Ingestion: the state column updater, one sample at a time.
        let values: Vec<(u32, f64, i64)> = (0..series * per_pane)
            .map(|r| {
                let value = ((r * 7919) % 10_007) as f64 + 1.0;
                (
                    (r / per_pane) as u32,
                    value,
                    (r % per_pane) as i64 * 15_000 + 1,
                )
            })
            .collect();
        let mut updater = StateColumnUpdater::new(family)?;
        updater.ensure_groups(series)?;
        let started = Instant::now();
        for &(group, value, time) in &values {
            updater.update(group, value, time)?;
        }
        ingestion_update.push(started.elapsed().as_nanos() as f64 / rows);
        drop(updater.seal());
        // Merge: every pane's state of a series into one.
        let state_schema = states[0].schema().clone();
        let state = state_schema
            .fields
            .iter()
            .position(|f| !matches!(f.dtype, FieldDataType::Plain(_)))
            .context("no state column")?;
        let groups: Vec<usize> = (0..state_schema.fields.len())
            .filter(|&i| i != state)
            .collect();
        let input_states: usize = states.iter().map(Batch::num_rows).sum();
        let (merged, ns) = run(
            Operator::summary_merge(state_schema.clone(), state, groups)?,
            states.clone(),
        )?;
        merge.push(ns / input_states as f64);
        // Estimate: the p99 (or distinct count) of each merged state.
        if let FieldDataType::Sketch(kind, _) = family {
            let statistic = match kind.algorithm() {
                SketchAlgorithm::Hll => SketchStatistic::Cardinality,
                _ => SketchStatistic::Quantile { q: 0.99 },
            };
            let merged_schema = merged[0].schema().clone();
            let state = merged_schema
                .fields
                .iter()
                .position(|f| !matches!(f.dtype, FieldDataType::Plain(_)))
                .context("no state column")?;
            let estimated: usize = merged.iter().map(Batch::num_rows).sum();
            let evaluation = Operator::evaluation(
                merged_schema,
                state,
                SummaryEvaluation::Sketch(statistic.clone()),
            )?;
            let (_, ns) = run(evaluation, merged)?;
            estimate.push(ns / estimated as f64);
        }
    }
    Ok(Measured {
        query_time_update: median(query_time_update),
        ingestion_update: median(ingestion_update),
        merge: median(merge),
        estimate: (!estimate.is_empty()).then(|| median(estimate)),
    })
}

/// Every summary family of `plan`'s `SummaryAgg`s, as JSON.
fn plan_families(plan: &Value) -> Vec<Value> {
    let mut families = Vec::new();
    for node in plan["dag"]["dag"]["nodes"].as_array().into_iter().flatten() {
        if let Some(family) = node["payload"]["ASAP"]["SummaryAgg"].get("family") {
            if !families.contains(family) {
                families.push(family.clone());
            }
        }
    }
    families
}

/// The family of a base-table sketch configuration (KLL, DDSketch, HLL).
fn family_of(sketch: &Value) -> Option<Value> {
    let (algorithm, category) = match sketch.as_object()?.keys().next()?.as_str() {
        "Kll" => ("Kll", "Quantile"),
        "DDSketch" => ("DDSketch", "Quantile"),
        "Hll" => ("Hll", "Cardinality"),
        _ => return None,
    };
    let mut params = sketch.clone();
    if algorithm == "Kll" && params["Kll"].get("failure_probability").is_none() {
        params["Kll"]["failure_probability"] = json!(0.01);
    }
    Some(
        json!({"Sketch": [{"category": category, "algorithm": algorithm, "params": params},
        "PerSubpopulationInstance"]}),
    )
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut families: Vec<Value> = Vec::new();
    for path in &args.plans {
        let text = std::fs::read_to_string(path).with_context(|| path.display().to_string())?;
        let plan: InstallablePlan = serde_json::from_str(&text)?;
        let plan = serde_json::to_value(&plan)?;
        for family in plan_families(&plan) {
            if !families.contains(&family) {
                families.push(family);
            }
        }
    }
    let base: Vec<Value> = match &args.base_table {
        Some(path) => serde_json::from_str(&std::fs::read_to_string(path)?)?,
        None => Vec::new(),
    };
    for entry in &base {
        if let Some(family) = family_of(&entry["sketch"]) {
            if !families.contains(&family) {
                families.push(family);
            }
        }
    }
    let mut table: Vec<Value> = Vec::new();
    let mut report = Vec::new();
    for family_json in &families {
        let measured = match serde_json::from_value::<FieldDataType>(family_json.clone())
            .map_err(anyhow::Error::from)
            .and_then(|family| measure(&family, &args))
        {
            Ok(measured) => measured,
            Err(error) => {
                eprintln!("skipped {family_json}: {error}");
                continue;
            }
        };
        eprintln!(
            "{family_json}: query-time update {:.1} ns/row, ingestion update {:.1} ns/sample, \
             merge {:.1} ns/state, estimate {:?} ns/state",
            measured.query_time_update,
            measured.ingestion_update,
            measured.merge,
            measured.estimate
        );
        report.push(json!({
            "family": family_json,
            "query_time_update_ns_per_row": measured.query_time_update,
            "ingestion_update_ns_per_sample": measured.ingestion_update,
            "merge_ns_per_state": measured.merge,
            "estimate_ns_per_state": measured.estimate,
        }));
        let Some(sketch) = family_json["Sketch"][0].get("params") else {
            continue; // exact accumulators: no entry in the table
        };
        let mut sketch = sketch.clone();
        if let Some(kll) = sketch.get_mut("Kll").and_then(Value::as_object_mut) {
            kll.remove("failure_probability");
        }
        let entries = [
            ("update", Some(measured.query_time_update)),
            ("merge", Some(measured.merge)),
            ("estimate", measured.estimate),
        ];
        for (operation, ns) in entries {
            if let Some(ns) = ns {
                table.retain(|e: &Value| !(e["sketch"] == sketch && e["operation"] == operation));
                table.push(
                    json!({"sketch": sketch, "operation": operation, "cpu_ns_per_operation": ns}),
                );
            }
        }
    }
    let measured_keys: Vec<(Value, Value)> = table
        .iter()
        .map(|e| (e["sketch"].clone(), e["operation"].clone()))
        .collect();
    let kept: Vec<Value> = base
        .into_iter()
        .filter(|e| {
            !measured_keys
                .iter()
                .any(|(s, o)| sketch_matches(s, &e["sketch"]) && o == &e["operation"])
        })
        .collect();
    let kept_count = kept.len();
    table.extend(kept);
    std::fs::write(&args.out, serde_json::to_string_pretty(&table)?)?;
    let raw = match &args.raw_plan {
        Some(path) => raw_windows(path, &args)?,
        None => Vec::new(),
    };
    std::fs::write(
        &args.report,
        serde_json::to_string_pretty(&json!({"families": report, "raw_window_queries": raw}))?,
    )?;
    println!(
        "wrote {} entries ({} measured here, {kept_count} kept from the base table)",
        table.len(),
        table.len() - kept_count
    );
    Ok(())
}

/// Whether two sketch configurations are the same, ignoring a KLL's
/// failure probability as the Planner's matching does.
fn sketch_matches(a: &Value, b: &Value) -> bool {
    let strip = |v: &Value| {
        let mut v = v.clone();
        if let Some(kll) = v.get_mut("Kll").and_then(Value::as_object_mut) {
            kll.remove("failure_probability");
        }
        v
    };
    strip(a) == strip(b)
}

/// Each query of the plan at `path` that reads raw samples, evaluated end to
/// end by the data plane's query engine over generated samples of
/// `args.series` series every 15 s: the cost per raw sample read, with
/// nothing kept from an earlier evaluation.
fn raw_windows(path: &PathBuf, args: &Args) -> anyhow::Result<Vec<Value>> {
    use data_plane::precompute::RawSample;
    use data_plane::query::QueryEngine;
    use data_plane::sds::{Sds, SummaryStore};
    let plan: InstallablePlan = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let sds = Sds::from_plan(&plan)?;
    let time_ms = 120 * 60_000;
    let mut results = Vec::new();
    for query in plan.queries.iter().map(|q| q.query.as_str()) {
        let engine = QueryEngine::new(&plan, &sds);
        let store = SummaryStore::new();
        let Ok((root, ranges)) = engine.raw_ranges(query, time_ms, &store) else {
            continue;
        };
        if ranges.is_empty() {
            continue;
        }
        let mut raw = BTreeMap::new();
        let mut samples = 0usize;
        for range in &ranges {
            let mut points = Vec::new();
            for series in 0..args.series {
                let labels = data_plane::series_ids::intern(&BTreeMap::from([
                    ("__name__".to_string(), range.metric.clone()),
                    ("job".to_string(), format!("job{}", series % 10)),
                    ("instance".to_string(), series.to_string()),
                ]));
                let mut t = range.start_ms - range.start_ms.rem_euclid(15_000) + 15_000;
                let mut counter = 0.0;
                while t <= range.end_ms {
                    counter += ((t / 15_000 + series as i64) % 97) as f64 + 1.0;
                    points.push(RawSample {
                        labels: labels.clone(),
                        timestamp_ms: t,
                        value: counter,
                    });
                    t += 15_000;
                }
            }
            samples += points.len();
            raw.insert(range.node, points);
        }
        let mut times = Vec::new();
        for _ in 0..args.repetitions {
            // A fresh engine each time, so no kept pane is reused.
            let engine = QueryEngine::new(&plan, &sds);
            let started = Instant::now();
            engine
                .evaluate(root, time_ms, &store, None, &raw)
                .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
            times.push(started.elapsed().as_nanos() as f64);
        }
        let ns = median(times);
        eprintln!(
            "{query}: {samples} raw samples, {:.1} ns/sample",
            ns / samples as f64
        );
        results.push(json!({
            "query": query,
            "raw_samples": samples,
            "evaluation_ns": ns,
            "ns_per_raw_sample": ns / samples as f64,
        }));
    }
    Ok(results)
}
