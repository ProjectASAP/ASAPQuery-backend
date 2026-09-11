//! Measure readout-specific ERP evidence from finite JSONL evaluation data.
//! This offline tool retains samples; the production backend does not.
use data_plane::precompute_engine::operators::univmon_accumulator::UnivMonAccumulator;
use data_plane::storage_engines::types::{AggregateCore, SerializableToSink};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: univmon_erp_artifact samples.jsonl")?;
    let mut series = BTreeMap::<String, Vec<(u64, f64)>>::new();
    for line in BufReader::new(std::fs::File::open(&path)?).lines() {
        let row: Value = serde_json::from_str(&line?)?;
        let key = serde_json::to_string(&row["labels"])?;
        series.entry(key).or_default().push((
            row["ts_ms"].as_u64().ok_or("timestamp")?,
            row["value"].as_f64().ok_or("value")?,
        ));
    }
    if series
        .values()
        .any(|samples| samples.windows(2).any(|pair| pair[0].0 >= pair[1].0))
    {
        return Err("each series must have strictly increasing timestamps".into());
    }
    let first = series.values().next().ok_or("empty dataset")?;
    let start = first.first().ok_or("empty series")?.0;
    let end = first.last().ok_or("empty series")?.0;
    let populations: Vec<_> = series
        .iter()
        .take(10)
        .map(|(key, samples)| {
            (
                key,
                samples
                    .iter()
                    .filter(|(ts, _)| *ts > start && *ts <= end)
                    .map(|(_, v)| *v)
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    if populations.len() < 2 {
        return Err("need at least two independent populations".into());
    }
    let mut records = Vec::new();
    for (heap, cols, layers) in [(64, 512, 8), (512, 2048, 8), (4096, 8192, 16)] {
        let mut max_errors = [0.0f64; 3];
        let mut bytes = 0;
        let mut observed = None;
        for (_, raw) in &populations {
            let mut counts = HashMap::<u64, usize>::new();
            for v in raw {
                *counts
                    .entry(if *v == 0.0 { 0 } else { v.to_bits() })
                    .or_default() += 1;
            }
            let truths = [
                counts.len() as f64,
                counts
                    .values()
                    .map(|c| (*c as f64).powi(2))
                    .sum::<f64>()
                    .sqrt(),
                counts
                    .values()
                    .map(|c| {
                        let p = *c as f64 / raw.len() as f64;
                        -p * p.log2()
                    })
                    .sum(),
            ];
            let mut observer = control_plane::physical::erp::ErpShapeObserver::new(raw.len())?;
            for (i, v) in raw.iter().enumerate() {
                observer.observe(&v.to_string(), i / 1000)?;
            }
            let next = observer.snapshot().ok_or("invalid observation")?;
            if observed.as_ref().is_some_and(
                |prior: &control_plane::physical::erp::ErpObservedShape| {
                    prior.observation != next.observation
                },
            ) {
                return Err(
                    "training populations have different shapes; calibrate separate shape strata"
                        .into(),
                );
            }
            observed = Some(next);
            let mut left =
                UnivMonAccumulator::new(heap, 5, cols, layers).map_err(|e| e.to_string())?;
            let mut right = left.clone();
            for (i, v) in raw.iter().enumerate() {
                if i % 2 == 0 { &mut left } else { &mut right }
                    .insert_sample(*v)
                    .map_err(|e| e.to_string())?;
            }
            left.merge_in_place(&right).map_err(|e| e.to_string())?;
            bytes = bytes.max(left.serialize_to_bytes().len());
            for (i, stat) in [
                asap_types::Statistic::Cardinality,
                asap_types::Statistic::FrequencyL2,
                asap_types::Statistic::FrequencyEntropy,
            ]
            .into_iter()
            .enumerate()
            {
                let answer = left
                    .query_statistic(stat, &None, &Default::default())
                    .map_err(|e| e.to_string())?;
                if !answer.is_finite() {
                    return Err("nonfinite estimate".into());
                }
                max_errors[i] = max_errors[i]
                    .max((answer - truths[i]).abs() / if i == 2 { 1.0 } else { truths[i] });
            }
        }
        let observed = observed.unwrap();
        let fit = observed
            .observation
            .fits
            .iter()
            .min_by(|a, b| a.goodness_of_fit.total_cmp(&b.goodness_of_fit))
            .ok_or("no shape")?;
        records.push(json!({"id":format!("univmon-h{heap}-c{cols}-l{layers}"),"sketch":"univmon","implementation":"asap-sketchlib-univmon-standard-v1",
            "parameters":{"heap_size":heap,"sketch_rows":5,"sketch_cols":cols,"layers":layers},"trials":populations.len(),
            "distribution":{"erp_shape":{"family":fit.family,"parameters":fit.parameters,"cardinality":observed.observation.cardinality,"benchmark_events":observed.observation.observed_events}},
            "error_metrics":{"max_cardinality_relative_error":max_errors[0],"max_frequency_l2_relative_error":max_errors[1],"max_frequency_entropy_absolute_bits_error":max_errors[2]},
            "resources":{"memory_bytes":bytes,"update_cpu_seconds":0.0,"merge_cpu_seconds":0.0,"query_cpu_seconds":0.0}}));
        eprintln!("measured heap={heap} cols={cols} layers={layers}: {max_errors:?}");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema_version":1,"producer_version":"backend-standard-unit-frequency-merged-two-pane-data-calibration-cpu-excluded","records":records})
        )?
    );
    Ok(())
}
