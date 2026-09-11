//! Measured window evidence, empirical shape matching, and Planner selection.
use super::*;
use asap_aware_mapping::erp::{
    AccuracyMode, ErpArtifact, ErpRecord, ErpResourceProfile, ErpSelectionRequest,
    ERP_SCHEMA_VERSION,
};
use std::collections::BTreeMap;

// ERP CPU fields must contain scheduled process CPU time, not elapsed time
// distorted by descheduling or competing host work.
#[cfg(target_os = "linux")]
fn process_cpu_seconds() -> std::io::Result<f64> {
    let mut timestamp = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the writable timespec on success.
    let status =
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, timestamp.as_mut_ptr()) };
    if status != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let timestamp = unsafe { timestamp.assume_init() };
    Ok(timestamp.tv_sec as f64 + timestamp.tv_nsec as f64 * 1e-9)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_seconds() -> std::io::Result<f64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "ERP calibration requires a process CPU clock",
    ))
}

pub fn bytes(c: Config) -> usize {
    c.rows * c.cols * 8 + c.heap * 32
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shape {
    cardinality: usize,
    events_per_pane: f64,
    rank_mass: Vec<f64>,
}
impl Shape {
    pub fn observe(data: &[Vec<u32>]) -> Self {
        let mut counts = HashMap::<u32, usize>::new();
        for key in data.iter().flatten() {
            *counts.entry(*key).or_default() += 1;
        }
        let events: usize = counts.values().sum();
        let mut ranks: Vec<_> = counts.values().copied().collect();
        ranks.sort_unstable_by(|a, b| b.cmp(a));
        Self {
            cardinality: ranks.len(),
            events_per_pane: events as f64 / data.len().max(1) as f64,
            rank_mass: [1, 10, 100, 1000]
                .map(|n| ranks.iter().take(n).sum::<usize>() as f64 / events.max(1) as f64)
                .to_vec(),
        }
    }
    fn distance(&self, other: &Self) -> f64 {
        if self.cardinality == 0 || other.cardinality == 0 {
            return f64::INFINITY;
        }
        let cardinality = (self.cardinality as f64 / other.cardinality as f64)
            .log2()
            .abs();
        let volume = ((self.events_per_pane + 1.) / (other.events_per_pane + 1.))
            .log2()
            .abs();
        cardinality
            + volume
            + self
                .rank_mass
                .iter()
                .zip(&other.rank_mass)
                .map(|(a, b)| (a - b).abs())
                .sum::<f64>()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub artifact: ErpArtifact,
    pub shapes: BTreeMap<String, Shape>,
    pub generation_seconds: f64,
    pub provenance: serde_json::Value,
}

// Benchmark all four windows on the same retained stream. Each metric is the
// worst loss over the available calibration endpoints, never held-out queries.
pub fn benchmark(
    data: &[Vec<u32>],
    config: Config,
) -> Result<(Vec<f64>, ErpResourceProfile), Box<dyn std::error::Error>> {
    let t = process_cpu_seconds()?;
    let states: Vec<_> = data.iter().map(|p| build_pane(p, config)).collect();
    let update = (process_cpu_seconds()? - t).max(0.0);
    let mut loss = vec![0.0_f64; 4];
    let mut merges = 0;
    let mut queries = 0;
    let mut merge_seconds = 0.;
    let mut query_seconds = 0.;
    for (q, &window) in WINDOWS.iter().enumerate() {
        // Up to five chronological observations per window, including the last.
        let ends: std::collections::BTreeSet<_> = (0..5)
            .map(|i| window + (data.len() - window) * i / 4)
            .collect();
        for end in ends {
            let t = process_cpu_seconds()?;
            let mut merged = states[end - window].clone();
            for s in &states[end - window + 1..end] {
                merged.merge(s)?;
                merges += 1;
            }
            merge_seconds += (process_cpu_seconds()? - t).max(0.0);
            let t = process_cpu_seconds()?;
            let predicted = merged.topk();
            query_seconds += (process_cpu_seconds()? - t).max(0.0);
            queries += 1;
            loss[q] = loss[q].max(1. - recall(&predicted, &exact_topk(data, end, window)));
        }
    }
    Ok((
        loss,
        ErpResourceProfile {
            memory_bytes: bytes(config) as f64,
            update_cpu_seconds: update / data.iter().map(Vec::len).sum::<usize>().max(1) as f64,
            merge_cpu_seconds: merge_seconds / merges.max(1) as f64,
            query_cpu_seconds: query_seconds / queries as f64,
        },
    ))
}

pub fn build(
    a: &Args,
    replayed: Option<&Vec<Vec<u32>>>,
) -> Result<Catalog, Box<dyn std::error::Error>> {
    let began = Instant::now();
    let mut records = Vec::new();
    let mut shapes = BTreeMap::new();
    // A custom trace uses only its designated calibration prefix. Synthetic
    // catalog seeds must be chosen independently from evaluation seeds.
    let datasets: Vec<_> = (0..a.trials)
        .map(|i| replayed.cloned().unwrap_or_else(|| synthetic(a, i)))
        .collect();
    let data: Vec<_> = datasets.iter().map(|d| &d[..a.calibration_panes]).collect();
    let id = if replayed.is_some() {
        "custom-calibration"
    } else {
        "synthetic"
    };
    shapes.insert(id.into(), Shape::observe(data[0]));
    let grid = candidates(a);
    for (index, config) in grid.iter().copied().enumerate() {
        let mut errors = vec![0.0_f64; 4];
        let mut costs = Vec::new();
        for d in &data {
            let (loss, cost) = benchmark(d, config)?;
            for q in 0..4 {
                errors[q] = errors[q].max(loss[q]);
            }
            costs.push(cost);
        }
        let n = costs.len() as f64;
        records.push(ErpRecord {
            id: format!("{id}-{index}"),
            sketch: format!("{:?}", config.family),
            implementation: "asap_sketchlib/portable/topk".into(),
            parameters: serde_json::to_value(config)?,
            distribution: serde_json::json!({"profile":id}),
            trials: a.trials as u32,
            error_metrics: WINDOWS
                .iter()
                .enumerate()
                .map(|(q, w)| (format!("recall_loss_{w}"), errors[q]))
                .collect(),
            resources: ErpResourceProfile {
                memory_bytes: bytes(config) as f64,
                update_cpu_seconds: costs.iter().map(|c| c.update_cpu_seconds).sum::<f64>() / n,
                merge_cpu_seconds: costs.iter().map(|c| c.merge_cpu_seconds).sum::<f64>() / n,
                query_cpu_seconds: costs.iter().map(|c| c.query_cpu_seconds).sum::<f64>() / n,
            },
        });
        eprintln!("ERP benchmark {}/{}, {:?}", index + 1, grid.len(), config);
    }
    Ok(Catalog {
        artifact: ErpArtifact {
            schema_version: ERP_SCHEMA_VERSION,
            producer_version: format!("backend-topk-window-adapter/{}", a.backend_revision),
            records,
        },
        shapes,
        generation_seconds: began.elapsed().as_secs_f64(),
        provenance: serde_json::json!({"args":a,"resource_time_basis":"process_cpu_seconds","timing":"process CPU seconds per operation; generation_seconds is wall time","scope":"window-conditioned extension of sketch-bench ERP v1; generated by backend adapter","held_out_used":false}),
    })
}

#[derive(Clone, Debug, Serialize)]
pub struct Decision {
    pub configs: Vec<Config>,
    pub profile: String,
    pub distance: f64,
    pub retained_bytes: usize,
    pub estimated_cpu_seconds: f64,
    pub planning_seconds: f64,
    pub record_ids: Vec<String>,
}

pub fn select(
    catalog: &Catalog,
    data: &[Vec<u32>],
    a: &Args,
    shared: bool,
) -> Result<Decision, Box<dyn std::error::Error>> {
    let began = Instant::now();
    catalog.artifact.validate()?;
    if catalog
        .provenance
        .get("resource_time_basis")
        .and_then(serde_json::Value::as_str)
        != Some("process_cpu_seconds")
    {
        return Err(
            "ERP catalog CPU timing is missing or uses wall time; recalibrate before selection"
                .into(),
        );
    }
    let observed = Shape::observe(data);
    let mut matches: Vec<_> = catalog
        .shapes
        .iter()
        .map(|(id, shape)| (id, observed.distance(shape)))
        .collect();
    matches.sort_by(|a, b| a.1.total_cmp(&b.1));
    let (profile, distance) = matches.first().copied().ok_or("empty ERP catalog")?;
    if distance > 0.5
        || matches
            .get(1)
            .is_some_and(|(_, second)| second - distance < 0.05)
    {
        return Err(format!("ERP shape miss/ambiguous: nearest distance={distance}").into());
    }
    let mut configs = Vec::new();
    let mut ids = Vec::new();
    let mut retained_bytes = 0;
    let mut estimated_cpu = 0.;
    let groups: Vec<Vec<usize>> = if shared {
        vec![vec![0, 1, 2, 3]]
    } else {
        (0..4).map(|q| vec![q]).collect()
    };
    for group in groups {
        let retained = group.iter().map(|q| WINDOWS[*q]).max().unwrap();
        let mut artifact = catalog.artifact.clone();
        artifact.records.retain(|r| {
            r.distribution == serde_json::json!({"profile":profile})
                && r.resources.memory_bytes <= a.memory_budget_bytes as f64
        });
        for r in &mut artifact.records {
            let loss = group
                .iter()
                .map(|q| {
                    r.error_metrics
                        .get(&format!("recall_loss_{}", WINDOWS[*q]))
                        .copied()
                        .unwrap_or(f64::INFINITY)
                })
                .fold(0.0_f64, f64::max);
            r.error_metrics.insert("group_loss".into(), loss);
            r.resources.memory_bytes *= retained as f64;
        }
        artifact
            .records
            .retain(|r| r.resources.memory_bytes <= a.total_memory_budget_bytes as f64);
        // Memory minimization matches AutoSketch's objective. Atomic costs are
        // measured and composed separately rather than silently changing goals.
        let chosen = artifact.select(&ErpSelectionRequest {
            distribution: serde_json::json!({"profile":profile}),
            implementation: Some("asap_sketchlib/portable/topk".into()),
            allowed_sketches: vec!["Cms".into(), "CountSketch".into()],
            error_metric: "group_loss".into(),
            max_error: 1. - a.min_recall_at_10,
            min_trials: 3,
            expected_updates: 0.,
            expected_queries: 0.,
            expected_merges: 0.,
            retention_seconds: 1.,
            cpu_weight: 0.,
            byte_second_weight: 1.,
            mode: AccuracyMode::Hybrid,
        })?;
        let r = chosen.record;
        let config: Config = serde_json::from_value(r.parameters.clone())?;
        let events = data.iter().map(Vec::len).sum::<usize>() as f64 / data.len() as f64
            * (a.calibration_panes + a.refreshes) as f64;
        estimated_cpu += events * r.resources.update_cpu_seconds
            + group.iter().map(|q| (WINDOWS[*q] - 1) as f64).sum::<f64>()
                * a.refreshes as f64
                * r.resources.merge_cpu_seconds
            + group.len() as f64 * a.refreshes as f64 * r.resources.query_cpu_seconds;
        retained_bytes += bytes(config) * retained;
        configs.push(config);
        ids.push(r.id.clone());
    }
    if retained_bytes > a.total_memory_budget_bytes {
        return Err("ERP deployment exceeds total memory budget".into());
    }
    Ok(Decision {
        configs,
        profile: profile.clone(),
        distance,
        retained_bytes,
        estimated_cpu_seconds: estimated_cpu,
        planning_seconds: began.elapsed().as_secs_f64(),
        record_ids: ids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Exercise the OS CPU clock through real update, merge, and readout work.
    #[cfg(target_os = "linux")]
    #[test]
    fn calibration_records_process_cpu_for_each_operation() {
        let data = vec![vec![1, 2]; 120];
        let (loss, resources) = benchmark(
            &data,
            Config {
                family: Family::Cms,
                rows: 3,
                cols: 128,
                heap: 16,
            },
        )
        .unwrap();
        assert!(loss.iter().all(|loss| *loss == 0.0));
        for cpu in [
            resources.update_cpu_seconds,
            resources.merge_cpu_seconds,
            resources.query_cpu_seconds,
        ] {
            assert!(cpu.is_finite() && cpu > 0.0);
        }
    }

    // Resource selection must react to measured evidence, not width thresholds.
    #[test]
    fn shape_detects_cardinality_and_rate_mismatch() {
        let a = Shape::observe(&[vec![1; 100]]);
        let b = Shape::observe(&[(0..100).collect()]);
        assert!(a.distance(&b) > 0.5);
        assert_eq!(a.distance(&a), 0.0);
    }
    // Changing measured error must change the chosen config; memory caps fail closed.
    #[test]
    fn selection_uses_error_evidence_and_retained_budget() {
        let mut a =
            Args::try_parse_from(["eval", "--output", "unused", "--backend-revision", "test"])
                .unwrap();
        let data = vec![vec![1; 10]; 120];
        let shape = Shape::observe(&data);
        let small = Config {
            family: Family::Cms,
            rows: 3,
            cols: 128,
            heap: 16,
        };
        let large = Config { cols: 256, ..small };
        let record = |id: &str, c: Config, error: f64| ErpRecord {
            id: id.into(),
            sketch: "Cms".into(),
            implementation: "asap_sketchlib/portable/topk".into(),
            parameters: serde_json::to_value(c).unwrap(),
            distribution: serde_json::json!({"profile":"test"}),
            trials: 3,
            error_metrics: WINDOWS
                .iter()
                .map(|w| (format!("recall_loss_{w}"), error))
                .collect(),
            resources: ErpResourceProfile {
                memory_bytes: bytes(c) as f64,
                update_cpu_seconds: 1e-6,
                merge_cpu_seconds: 1e-5,
                query_cpu_seconds: 1e-5,
            },
        };
        let mut catalog = Catalog {
            artifact: ErpArtifact {
                schema_version: 1,
                producer_version: "test".into(),
                records: vec![record("small", small, 0.1), record("large", large, 0.)],
            },
            shapes: BTreeMap::from([("test".into(), shape)]),
            generation_seconds: 0.,
            provenance: serde_json::json!({"resource_time_basis": "process_cpu_seconds"}),
        };
        let provenance = catalog.provenance.take();
        assert!(select(&catalog, &data, &a, true)
            .unwrap_err()
            .to_string()
            .contains("CPU timing"));
        catalog.provenance = provenance;
        assert_eq!(
            select(&catalog, &data, &a, true).unwrap().configs,
            vec![small]
        );
        catalog.artifact.records[0]
            .error_metrics
            .values_mut()
            .for_each(|e| *e = 0.5);
        assert_eq!(
            select(&catalog, &data, &a, true).unwrap().configs,
            vec![large]
        );
        a.total_memory_budget_bytes = bytes(large) * 120 - 1;
        assert!(select(&catalog, &data, &a, true).is_err());
        a.total_memory_budget_bytes = bytes(large) * 120;
        let out_of_distribution = vec![(0..1000).collect::<Vec<_>>(); 120];
        assert!(select(&catalog, &out_of_distribution, &a, true).is_err());
        catalog
            .shapes
            .insert("duplicate".into(), catalog.shapes["test"].clone());
        let mut duplicate = record("duplicate", large, 0.);
        duplicate.distribution = serde_json::json!({"profile":"duplicate"});
        catalog.artifact.records.push(duplicate);
        assert!(
            select(&catalog, &data, &a, true).is_err(),
            "equally close profiles must be rejected as ambiguous"
        );
    }
}
