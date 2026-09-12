use super::{
    input::{random, Event},
    summaries::*,
};
use asap_aware_mapping::erp::{
    AccuracyMode, ErpArtifact, ErpRecord, ErpResourceProfile, ErpSelectionRequest,
    ERP_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    time::Instant,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub config: Config,
    pub losses: Vec<f64>,
    pub pane_bytes: usize,
    pub pane_bytes_without_enumeration: usize,
    pub update_seconds: f64,
    pub merge_seconds: f64,
    pub query_seconds: f64,
    pub update_cpu_seconds: f64,
    pub merge_cpu_seconds: f64,
    pub query_cpu_seconds: f64,
    pub composition_operations: u64,
    pub query_operations: u64,
}
#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub workload: Workload,
    pub panels: Vec<Panel>,
    pub records: Vec<Evidence>,
    pub construction_seconds: f64,
    pub calibration_events: usize,
    pub trials: usize,
    pub seed: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Deployment {
    pub configs: Vec<Config>,
    pub shared: bool,
    pub planning_seconds: f64,
    pub searches: Vec<serde_json::Value>,
    pub reason: String,
}

pub type Truths = HashMap<(usize, usize), Answer>;
fn endpoints(length: usize, window: usize) -> BTreeSet<usize> {
    (0..5).map(|i| window + (length - window) * i / 4).collect()
}
pub fn truths(data: &[Vec<Event>], w: Workload, panels: &[Panel]) -> anyhow::Result<Truths> {
    let mut out = HashMap::new();
    for window in [1, 10, 60] {
        for end in endpoints(data.len(), window) {
            let mut state = State::new(Config::Exact, w, 0);
            for e in data[end - window..end].iter().flatten() {
                state.update(*e, w, true)?;
            }
            for (i, p) in panels
                .iter()
                .enumerate()
                .filter(|(_, p)| p.window == window)
            {
                out.insert(
                    (i, end),
                    state.answer(if matches!(p.query, Query::Top3) {
                        Query::Count
                    } else {
                        p.query
                    }),
                );
            }
        }
    }
    Ok(out)
}
pub fn benchmark(
    data: &[Vec<Event>],
    w: Workload,
    panels: &[Panel],
    which: &[usize],
    c: Config,
    truth: &Truths,
    seed: u64,
) -> anyhow::Result<Evidence> {
    let enumerate = which
        .iter()
        .any(|i| matches!(panels[*i].query, Query::Count));
    let began = Instant::now();
    let cpu_started = super::input::cpu_seconds()?;
    let mut states = Vec::new();
    let mut pane_bytes = 0;
    let mut pane_bytes_without_enumeration = 0;
    for (i, events) in data.iter().enumerate() {
        let mut s = State::new(c, w, seed ^ i as u64);
        for e in events {
            s.update(*e, w, enumerate)?;
        }
        states.push(s);
    }
    let update_seconds = began.elapsed().as_secs_f64();
    let update_cpu_seconds = super::input::cpu_seconds()? - cpu_started;
    // Payload inspection may allocate temporary wire views; never charge it to updates.
    for s in &states {
        pane_bytes = pane_bytes.max(s.bytes());
        pane_bytes_without_enumeration =
            pane_bytes_without_enumeration.max(s.bytes_without_enumeration());
    }
    let mut losses = vec![0f64; panels.len()];
    let mut merge_seconds = 0.;
    let mut query_seconds = 0.;
    let mut merge_cpu_seconds = 0.;
    let mut query_cpu_seconds = 0.;
    let mut composition_operations = 0;
    let mut query_operations = 0;
    for &i in which {
        let panel = panels[i];
        for end in endpoints(data.len(), panel.window) {
            let t = Instant::now();
            let cpu = super::input::cpu_seconds()?;
            let mut state = states[end - panel.window].clone();
            for s in &states[end - panel.window + 1..end] {
                state.merge(s)?;
            }
            merge_seconds += t.elapsed().as_secs_f64();
            merge_cpu_seconds += super::input::cpu_seconds()? - cpu;
            let t = Instant::now();
            let cpu = super::input::cpu_seconds()?;
            let answer = state.answer(panel.query);
            query_seconds += t.elapsed().as_secs_f64();
            query_cpu_seconds += super::input::cpu_seconds()? - cpu;
            losses[i] = losses[i].max(loss(&answer, &truth[&(i, end)], panel.query));
            composition_operations += panel.window as u64;
            query_operations += 1;
        }
    }
    Ok(Evidence {
        config: c,
        losses,
        pane_bytes,
        pane_bytes_without_enumeration,
        update_seconds,
        merge_seconds,
        query_seconds,
        update_cpu_seconds,
        merge_cpu_seconds,
        query_cpu_seconds,
        composition_operations,
        query_operations,
    })
}
pub fn build(
    data: &[Vec<Event>],
    w: Workload,
    seed: u64,
    trials: usize,
) -> anyhow::Result<Catalog> {
    let began = Instant::now();
    let panels = panels(w);
    let truth = truths(data, w, &panels)?;
    let mut records = Vec::new();
    for (i, c) in grid(w).into_iter().chain([Config::Exact]).enumerate() {
        let mut evidence = benchmark(
            data,
            w,
            &panels,
            &(0..panels.len()).collect::<Vec<_>>(),
            c,
            &truth,
            seed,
        )?;
        for trial in 1..trials {
            let next = benchmark(
                data,
                w,
                &panels,
                &(0..panels.len()).collect::<Vec<_>>(),
                c,
                &truth,
                seed + trial as u64,
            )?;
            for (a, b) in evidence.losses.iter_mut().zip(next.losses) {
                *a = a.max(b);
            }
            evidence.pane_bytes = evidence.pane_bytes.max(next.pane_bytes);
            evidence.pane_bytes_without_enumeration = evidence
                .pane_bytes_without_enumeration
                .max(next.pane_bytes_without_enumeration);
            evidence.update_seconds += next.update_seconds;
            evidence.merge_seconds += next.merge_seconds;
            evidence.query_seconds += next.query_seconds;
            evidence.update_cpu_seconds += next.update_cpu_seconds;
            evidence.merge_cpu_seconds += next.merge_cpu_seconds;
            evidence.query_cpu_seconds += next.query_cpu_seconds;
        }
        evidence.update_seconds /= trials as f64;
        evidence.merge_seconds /= trials as f64;
        evidence.query_seconds /= trials as f64;
        evidence.update_cpu_seconds /= trials as f64;
        evidence.merge_cpu_seconds /= trials as f64;
        evidence.query_cpu_seconds /= trials as f64;
        eprintln!(
            "ERP {w:?} candidate {i}: {c:?}, max loss {}",
            evidence.losses.iter().copied().fold(0f64, f64::max)
        );
        records.push(evidence);
    }
    Ok(Catalog {
        workload: w,
        panels,
        records,
        construction_seconds: began.elapsed().as_secs_f64(),
        calibration_events: data.iter().map(Vec::len).sum(),
        trials,
        seed,
    })
}

/// Discrete per-family Latin hypercube: each dimension is sampled without
/// replacement across the initial points. One-dimensional families use 3 strata.
pub fn lhs(candidates: &[Config], seed: u64) -> Vec<Config> {
    let mut rng = seed;
    let mut out = Vec::new();
    let mut families: Vec<_> = candidates.iter().map(|c| family(*c)).collect();
    families.sort();
    families.dedup();
    for f in families {
        let group: Vec<_> = candidates
            .iter()
            .copied()
            .filter(|c| family(*c) == f)
            .collect();
        let dimensions = coordinates(group[0]).len();
        let mut axes: Vec<Vec<usize>> = (0..dimensions)
            .map(|d| {
                let mut a: Vec<_> = group.iter().map(|c| coordinates(*c)[d]).collect();
                a.sort();
                a.dedup();
                a
            })
            .collect();
        let n = axes.iter().map(Vec::len).min().unwrap_or(0).min(3);
        for a in &mut axes {
            let sampled = (0..n)
                .map(|stratum| {
                    let start = stratum * a.len() / n;
                    let end = (stratum + 1) * a.len() / n;
                    a[start + random(&mut rng) as usize % (end - start)]
                })
                .collect();
            *a = sampled;
            for i in (1..a.len()).rev() {
                let j = random(&mut rng) as usize % (i + 1);
                a.swap(i, j);
            }
        }
        for i in 0..n {
            let point: Vec<_> = axes.iter().map(|a| a[i]).collect();
            if let Some(c) = group.iter().find(|c| coordinates(**c) == point) {
                out.push(*c);
            }
        }
    }
    out
}
pub fn autosketch(
    data: &[Vec<Event>],
    w: Workload,
    seed: u64,
    budget: usize,
) -> anyhow::Result<Deployment> {
    let began = Instant::now();
    let panels = panels(w);
    let truth = truths(data, w, &panels)?;
    let mut configs = Vec::new();
    let mut searches = Vec::new();
    let candidates = grid(w);
    let total_window_panes: usize = panels.iter().map(|p| p.window).sum();
    for (panel_id, panel) in panels.iter().enumerate() {
        let query_budget = budget / total_window_panes * panel.window;
        let start = Instant::now();
        let mut frontier: VecDeque<_> = lhs(&candidates, seed + panel_id as u64)
            .into_iter()
            .map(|c| (c, None))
            .collect();
        let mut seen = HashSet::new();
        let mut measured = HashMap::<Config, Evidence>::new();
        let mut best: Option<Config> = None;
        while let Some((c, direction)) = frontier.pop_front() {
            if !seen.insert((c, direction)) {
                continue;
            }
            if !measured.contains_key(&c) {
                measured.insert(
                    c,
                    benchmark(data, w, &panels, &[panel_id], c, &truth, seed)?,
                );
            }
            let evidence = &measured[&c];
            let feasible = evidence.losses[panel_id] <= 1.
                && evidence.pane_bytes * panel.window <= query_budget;
            if feasible && best.is_none_or(|b| evidence.pane_bytes < measured[&b].pane_bytes) {
                best = Some(c);
            }
            // Continue toward less memory after success and toward more capacity
            // after failure; do not cross the direction's feasibility boundary.
            let coordinates = coordinates(c);
            for axis in 0..coordinates.len() {
                let mut levels: Vec<_> = candidates
                    .iter()
                    .filter(|x| family(**x) == family(c))
                    .map(|x| super::summaries::coordinates(*x)[axis])
                    .collect();
                levels.sort();
                levels.dedup();
                let pos = levels.iter().position(|v| *v == coordinates[axis]).unwrap();
                // DDSketch's alpha direction is inverse to capacity.
                let step: i32 = if feasible { -1 } else { 1 };
                let step = if matches!(c, Config::Dd { .. }) {
                    -step
                } else {
                    step
                };
                if direction.is_some_and(|d| d != (axis, step)) {
                    continue;
                }
                let next = pos as i32 + step;
                if next < 0 || next as usize >= levels.len() {
                    continue;
                }
                let mut target = coordinates.clone();
                target[axis] = levels[next as usize];
                if let Some(n) = candidates.iter().find(|x| {
                    family(**x) == family(c) && super::summaries::coordinates(**x) == target
                }) {
                    frontier.push_back((*n, Some((axis, step))));
                }
            }
        }
        // Explicit exact fallback keeps failed searches out of accuracy-equivalent
        // sketch wins. The search failure is retained in the raw result.
        let chosen = best.unwrap_or(Config::Exact);
        if best.is_none() {
            let fallback = benchmark(data, w, &panels, &[panel_id], Config::Exact, &truth, seed)?;
            anyhow::ensure!(
                fallback.pane_bytes * panel.window <= query_budget,
                "exact fallback exceeds allocated query memory budget"
            );
            measured.insert(Config::Exact, fallback);
        }
        searches.push(serde_json::json!({"panel":panel,"seconds":start.elapsed().as_secs_f64(),"allocated_memory_budget_bytes":query_budget,"candidates":measured.len(),"calibration_feasible":best.is_some(),"selected":chosen,"evidence":measured.values().collect::<Vec<_>>()}));
        configs.push(chosen);
    }
    Ok(Deployment {
        configs,
        shared: false,
        planning_seconds: began.elapsed().as_secs_f64(),
        searches,
        reason:
            "AutoSketch CPU extension: independent query-local searches; explicit exact fallback"
                .into(),
    })
}

pub fn erp(catalog: &Catalog, shared: bool, budget: usize) -> anyhow::Result<Deployment> {
    let began = Instant::now();
    let groups: Vec<Vec<usize>> = if shared {
        vec![(0..catalog.panels.len()).collect()]
    } else {
        (0..catalog.panels.len()).map(|i| vec![i]).collect()
    };
    let mut configs = Vec::new();
    let mut searches = Vec::new();
    let mut total = 0;
    for group in groups {
        let enumerate = group
            .iter()
            .any(|i| matches!(catalog.panels[*i].query, Query::Count));
        let pane_bytes = |e: &Evidence| {
            if enumerate {
                e.pane_bytes
            } else {
                e.pane_bytes_without_enumeration
            }
        };
        let window = group
            .iter()
            .map(|i| catalog.panels[*i].window)
            .max()
            .unwrap();
        let artifact = ErpArtifact {
            schema_version: ERP_SCHEMA_VERSION,
            producer_version: "alibaba-window-adapter-v1".into(),
            records: catalog
                .records
                .iter()
                .filter(|e| e.config != Config::Exact && pane_bytes(e) * window <= budget)
                .map(|e| ErpRecord {
                    id: format!("{:?}", e.config),
                    sketch: family(e.config).into(),
                    implementation: "asap_sketchlib/alibaba".into(),
                    parameters: serde_json::to_value(e.config).unwrap(),
                    distribution: serde_json::json!({"profile":"alibaba-calibration-sample"}),
                    trials: catalog.trials as u32,
                    error_metrics: std::collections::BTreeMap::from([(
                        "normalized_query_loss".into(),
                        group.iter().map(|i| e.losses[*i]).fold(0f64, f64::max),
                    )]),
                    resources: ErpResourceProfile {
                        memory_bytes: (pane_bytes(e) * window) as f64,
                        update_cpu_seconds: e.update_cpu_seconds
                            / catalog.calibration_events.max(1) as f64,
                        merge_cpu_seconds: e.merge_cpu_seconds
                            / e.composition_operations.max(1) as f64,
                        query_cpu_seconds: e.query_cpu_seconds / e.query_operations.max(1) as f64,
                    },
                })
                .collect(),
        };
        let chosen = artifact.select(&ErpSelectionRequest {
            distribution: serde_json::json!({"profile":"alibaba-calibration-sample"}),
            implementation: Some("asap_sketchlib/alibaba".into()),
            allowed_sketches: vec![
                "CmsPoint".into(),
                "Cms".into(),
                "CountSketch".into(),
                "Kll".into(),
                "DdSketch".into(),
            ],
            error_metric: "normalized_query_loss".into(),
            max_error: 1.,
            min_trials: catalog.trials as u32,
            expected_updates: 0.,
            expected_queries: 0.,
            expected_merges: 0.,
            retention_seconds: 1.,
            cpu_weight: 0.,
            byte_second_weight: 1.,
            mode: AccuracyMode::Hybrid,
        });
        let chosen = match chosen {
            Ok(c) => c,
            Err(error) => {
                let bytes = catalog
                    .records
                    .iter()
                    .find(|e| e.config == Config::Exact)
                    .ok_or_else(|| anyhow::anyhow!("missing exact fallback evidence"))?
                    .pane_bytes
                    * window;
                total += bytes;
                configs.push(Config::Exact);
                searches.push(serde_json::json!({"panels":group,"fallback":"exact","reason":error.to_string(),"predicted_retained_bytes":bytes}));
                continue;
            }
        };
        let c: Config = serde_json::from_value(chosen.record.parameters.clone())?;
        total += chosen.record.resources.memory_bytes as usize;
        configs.push(c);
        searches.push(serde_json::json!({"panels":group,"record_id":chosen.record.id,"predicted_retained_bytes":chosen.record.resources.memory_bytes}));
    }
    anyhow::ensure!(
        total <= budget,
        "combined ERP memory exceeds deployment budget"
    );
    Ok(Deployment {
        configs,
        shared,
        planning_seconds: began.elapsed().as_secs_f64(),
        searches,
        reason: "real ASAPPlanner ERP selector; custom calibration profile; memory objective"
            .into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn erp_selection_changes_with_error_evidence_and_rejects_over_budget() {
        let a = Config::Cms {
            depth: 3,
            width: 128,
            heap: 16,
        };
        let b = Config::Cs {
            depth: 5,
            width: 128,
            heap: 16,
        };
        let record = |config, pane_bytes| Evidence {
            config,
            pane_bytes,
            pane_bytes_without_enumeration: pane_bytes,
            losses: vec![0.; 6],
            update_seconds: 1.,
            merge_seconds: 1.,
            query_seconds: 1.,
            update_cpu_seconds: 1.,
            merge_cpu_seconds: 1.,
            query_cpu_seconds: 1.,
            composition_operations: 1,
            query_operations: 1,
        };
        let mut catalog = Catalog {
            workload: Workload::Service,
            panels: panels(Workload::Service),
            records: vec![record(a, 1000), record(b, 2000), record(Config::Exact, 100)],
            construction_seconds: 1.,
            calibration_events: 100,
            trials: 3,
            seed: 42,
        };
        assert_eq!(erp(&catalog, true, 1_000_000).unwrap().configs, vec![a]);
        catalog.records[0].losses[0] = 2.;
        assert_eq!(erp(&catalog, true, 1_000_000).unwrap().configs, vec![b]);
        assert!(erp(&catalog, true, 1).is_err());
    }
    #[test]
    fn lhs_initializes_every_sketch_family() {
        for w in [Workload::Service, Workload::Latency] {
            let g = grid(w);
            let points = lhs(&g, 42);
            for f in [family(g[0]), family(*g.last().unwrap())] {
                let selected: Vec<_> = points.iter().filter(|c| family(**c) == f).collect();
                assert_eq!(selected.len(), 3);
                for d in 0..coordinates(*selected[0]).len() {
                    assert_eq!(
                        selected
                            .iter()
                            .map(|c| coordinates(**c)[d])
                            .collect::<HashSet<_>>()
                            .len(),
                        3
                    );
                }
            }
        }
    }
}
