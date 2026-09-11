use super::{
    input::{self, Event},
    planning::Deployment,
    summaries::*,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    hint::black_box,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Default, Serialize)]
pub struct Timing {
    pub update_seconds: f64,
    pub eviction_seconds: f64,
    pub merge_seconds: f64,
    pub readout_seconds: f64,
    pub update_cpu_seconds: f64,
    pub eviction_cpu_seconds: f64,
    pub merge_cpu_seconds: f64,
    pub readout_cpu_seconds: f64,
    pub oracle_seconds: f64,
    pub input_and_harness_seconds: f64,
}
#[derive(Serialize)]
pub struct ResultRow {
    pub status: String,
    pub workload: Workload,
    pub deployment: Deployment,
    pub timing: Timing,
    pub peak_retained_payload_bytes: usize,
    pub peak_query_payload_bytes: usize,
    pub events: u64,
    pub updates: u64,
    pub samples: Vec<serde_json::Value>,
    pub dashboard_samples: Vec<serde_json::Value>,
    pub violations: usize,
    pub whole_process_peak_rss_kib: Option<u64>,
}
#[derive(Serialize, Deserialize)]
struct OracleSnapshot {
    end_minute: usize,
    workload: Workload,
    answers: HashMap<usize, Answer>,
    cdfs: HashMap<usize, Cdfs>,
}

enum RawPane {
    Keys(Vec<u64>),
    Latencies(Vec<(u32, f64)>),
}
impl RawPane {
    fn new(events: &[Event], w: Workload) -> Self {
        if w == Workload::Latency {
            Self::Latencies(events.iter().map(|e| (e.downstream, e.latency)).collect())
        } else {
            Self::Keys(
                events
                    .iter()
                    .map(|e| {
                        if w == Workload::Edge {
                            ((e.upstream as u64) << 32) | e.downstream as u64
                        } else {
                            e.downstream as u64
                        }
                    })
                    .collect(),
            )
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Keys(v) => v.len() * std::mem::size_of::<u64>(),
            Self::Latencies(v) => v.len() * std::mem::size_of::<(u32, f64)>(),
        }
    }
    fn scan(&self, state: &mut State, w: Workload) -> anyhow::Result<()> {
        match self {
            Self::Keys(keys) => {
                let State::Counts(counts) = state else {
                    anyhow::bail!("raw key scan requires exact counts")
                };
                for key in keys {
                    *counts.entry(*key).or_default() += 1;
                }
            }
            Self::Latencies(values) => {
                for (downstream, latency) in values {
                    state.update(
                        Event {
                            time: 0,
                            upstream: 0,
                            downstream: *downstream,
                            latency: *latency,
                        },
                        w,
                        true,
                    )?;
                }
            }
        }
        Ok(())
    }
}
struct Runtime {
    workload: Workload,
    panels: Vec<Panel>,
    deployment: Deployment,
    stores: Vec<VecDeque<State>>,
    oracle: VecDeque<State>,
    raw: VecDeque<RawPane>,
    timing: Timing,
    samples: Vec<serde_json::Value>,
    dashboards: Vec<serde_json::Value>,
    peak: usize,
    query_peak: usize,
    events: u64,
    updates: u64,
    violations: usize,
    budget: usize,
    scan: bool,
    start: usize,
    oracle_directory: Option<PathBuf>,
    write_oracle: bool,
}
impl Runtime {
    fn pane(&mut self, index: usize, events: Vec<Event>) -> anyhow::Result<()> {
        self.events += events.len() as u64;
        if self.scan {
            let t = Instant::now();
            let cpu = input::cpu_seconds()?;
            self.raw.push_back(RawPane::new(&events, self.workload));
            self.timing.update_seconds += t.elapsed().as_secs_f64();
            self.timing.update_cpu_seconds += input::cpu_seconds()? - cpu;
            self.updates += events.len() as u64;
            let t = Instant::now();
            let cpu = input::cpu_seconds()?;
            while self.raw.len() > 60 {
                self.raw.pop_front();
            }
            self.timing.eviction_seconds += t.elapsed().as_secs_f64();
            self.timing.eviction_cpu_seconds += input::cpu_seconds()? - cpu;
            self.peak = self.peak.max(self.raw.iter().map(RawPane::bytes).sum());
        } else {
            for (store_index, store) in self.stores.iter_mut().enumerate() {
                let config = self.deployment.configs[store_index];
                let enumerate = self.deployment.shared
                    || matches!(self.panels[store_index].query, Query::Count);
                let t = Instant::now();
                let cpu = input::cpu_seconds()?;
                let mut state = State::new(config, self.workload, index as u64);
                for event in &events {
                    state.update(*event, self.workload, enumerate)?;
                }
                store.push_back(state);
                self.timing.update_seconds += t.elapsed().as_secs_f64();
                self.timing.update_cpu_seconds += input::cpu_seconds()? - cpu;
                self.updates += events.len() as u64;
                let t = Instant::now();
                let cpu = input::cpu_seconds()?;
                let retain = if self.deployment.shared {
                    60
                } else {
                    self.panels[store_index].window
                };
                while store.len() > retain {
                    store.pop_front();
                }
                self.timing.eviction_seconds += t.elapsed().as_secs_f64();
                self.timing.eviction_cpu_seconds += input::cpu_seconds()? - cpu;
            }
            self.peak = self
                .peak
                .max(self.stores.iter().flatten().map(State::bytes).sum());
            anyhow::ensure!(
                self.peak <= self.budget,
                "retained logical memory budget exceeded"
            );
        }
        let t = Instant::now();
        if self.oracle_directory.is_none() {
            let mut truth = State::new(Config::Exact, self.workload, 0);
            for e in &events {
                truth.update(*e, self.workload, true)?;
            }
            self.oracle.push_back(truth);
            while self.oracle.len() > 60 {
                self.oracle.pop_front();
            }
        }
        self.timing.oracle_seconds += t.elapsed().as_secs_f64();
        if index < self.start {
            return Ok(());
        }
        let mut answers = HashMap::<usize, Answer>::new();
        let mut emitted_truths = HashMap::<usize, Answer>::new();
        let mut emitted_cdfs = HashMap::<usize, Cdfs>::new();
        let mut merge_groups = Vec::new();
        let mut query_cost = 0.;
        let groups: Vec<Vec<usize>> = if self.deployment.shared || self.scan {
            [1, 10, 60]
                .into_iter()
                .map(|w| {
                    self.panels
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.window == w)
                        .map(|(i, _)| i)
                        .collect()
                })
                .collect()
        } else {
            (0..self.panels.len()).map(|i| vec![i]).collect()
        };
        for (group_id, group) in groups.iter().enumerate() {
            let window = self.panels[group[0]].window;
            let t = Instant::now();
            let cpu = input::cpu_seconds()?;
            let mut merged = if self.scan {
                let mut s = State::new(Config::Exact, self.workload, 0);
                for pane in self.raw.iter().skip(self.raw.len() - window) {
                    pane.scan(&mut s, self.workload)?;
                }
                s
            } else {
                let store = &self.stores[if self.deployment.shared { 0 } else { group[0] }];
                let mut selected = store.iter().skip(store.len() - window);
                let mut s = selected.next().unwrap().clone();
                for other in selected {
                    s.merge(other)?;
                }
                s
            };
            let elapsed = t.elapsed().as_secs_f64();
            let elapsed_cpu = input::cpu_seconds()? - cpu;
            if self.scan {
                self.timing.readout_seconds += elapsed;
                self.timing.readout_cpu_seconds += elapsed_cpu;
            } else {
                self.timing.merge_seconds += elapsed;
                self.timing.merge_cpu_seconds += elapsed_cpu;
            }
            query_cost += elapsed;
            merge_groups.push(serde_json::json!({"id":group_id,"panels":group,"seconds":elapsed,"cpu_seconds":elapsed_cpu,"operation":if self.scan{"raw_scan_groupby"}else{"merge"}}));
            self.query_peak = self.query_peak.max(merged.bytes());
            for i in group {
                let t = Instant::now();
                let cpu = input::cpu_seconds()?;
                let answer = black_box(merged.answer(self.panels[*i].query));
                let readout = t.elapsed().as_secs_f64();
                let readout_cpu = input::cpu_seconds()? - cpu;
                self.timing.readout_seconds += readout;
                self.timing.readout_cpu_seconds += readout_cpu;
                query_cost += readout;
                self.samples.push(serde_json::json!({"end_minute":index+1,"panel_id":i,"panel":self.panels[*i],"merge_group":group_id,"readout_seconds":readout,"readout_cpu_seconds":readout_cpu,"output_groups":answer.len()}));
                answers.insert(*i, answer);
            }
            if self.write_oracle {
                let t = Instant::now();
                if self.workload == Workload::Latency {
                    emitted_cdfs.insert(window, merged.exact_cdfs()?);
                }
                for i in group {
                    emitted_truths.insert(
                        *i,
                        if matches!(self.panels[*i].query, Query::Top3) {
                            merged.answer(Query::Count)
                        } else {
                            answers[i].clone()
                        },
                    );
                }
                self.timing.oracle_seconds += t.elapsed().as_secs_f64();
            }
            self.query_peak = self
                .query_peak
                .max(merged.bytes() + answers.values().map(|a| a.len() * 16).sum::<usize>());
        }
        let t = Instant::now();
        let reference = if let Some(directory) = &self.oracle_directory {
            let path = directory.join(format!("minute-{}.bin.gz", index + 1));
            if self.write_oracle {
                let file = std::fs::File::options()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                let mut out = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
                let snapshot = OracleSnapshot {
                    end_minute: index + 1,
                    workload: self.workload,
                    answers: emitted_truths,
                    cdfs: emitted_cdfs,
                };
                bincode::serialize_into(&mut out, &snapshot)?;
                out.finish()?;
                snapshot
            } else {
                bincode::deserialize_from(flate2::read::GzDecoder::new(std::fs::File::open(path)?))?
            }
        } else {
            let mut reference = HashMap::new();
            let mut cdfs = HashMap::new();
            for window in [1, 10, 60] {
                let mut selected = self.oracle.iter().skip(self.oracle.len() - window);
                let mut truth = selected.next().unwrap().clone();
                for s in selected {
                    truth.merge(s)?;
                }
                for (i, panel) in self
                    .panels
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| p.window == window)
                {
                    let answer = truth.answer(if matches!(panel.query, Query::Top3) {
                        Query::Count
                    } else {
                        panel.query
                    });
                    reference.insert(i, answer);
                }
                if self.workload == Workload::Latency {
                    cdfs.insert(window, truth.exact_cdfs()?);
                }
            }
            OracleSnapshot {
                end_minute: index + 1,
                workload: self.workload,
                answers: reference,
                cdfs,
            }
        };
        anyhow::ensure!(
            reference.end_minute == index + 1 && reference.workload == self.workload,
            "oracle metadata mismatch"
        );
        for (i, panel) in self.panels.iter().enumerate() {
            let answer = reference
                .answers
                .get(&i)
                .ok_or_else(|| anyhow::anyhow!("missing oracle panel"))?;
            let predicted = &answers[&i];
            let error = loss(predicted, answer, panel.query);
            self.violations += usize::from(error > 1.);
            let sample = self
                .samples
                .iter_mut()
                .rev()
                .find(|s| s["panel_id"].as_u64() == Some(i as u64))
                .unwrap();
            sample["normalized_loss"] = serde_json::json!(error);
            let (window_events, window_keys) = if self.workload == Workload::Latency {
                let cdf = &reference.cdfs[&panel.window];
                (
                    cdf.values()
                        .map(|v| v.last().map_or(0, |x| x.1))
                        .sum::<u64>(),
                    cdf.len(),
                )
            } else {
                (
                    answer.values().map(|v| *v as u64).sum::<u64>(),
                    answer.len(),
                )
            };
            sample["window_events"] = serde_json::json!(window_events);
            sample["window_keys"] = serde_json::json!(window_keys);
            sample["nonfinite_exact_groups"] =
                serde_json::json!(answer.values().filter(|x| !x.is_finite()).count());
            if let Query::Quantile(q) = panel.query {
                let cdf = reference
                    .cdfs
                    .get(&panel.window)
                    .ok_or_else(|| anyhow::anyhow!("missing exact CDF"))?;
                let (max, mean) = rank_error(predicted, cdf, q);
                sample["max_rank_distance_with_interpolation_tolerance"] = serde_json::json!(max);
                sample["mean_rank_distance_with_interpolation_tolerance"] = serde_json::json!(mean);
            }
        }
        self.timing.oracle_seconds += t.elapsed().as_secs_f64();
        self.dashboards.push(serde_json::json!({"end_minute":index+1,"timed_query_operations_seconds":query_cost,"merge_groups":merge_groups}));
        if (index + 1) % 30 == 0 {
            eprintln!(
                "{:?}: evaluated through minute {}, events {}, violations {}",
                self.workload,
                index + 1,
                self.events,
                self.violations
            );
        }
        Ok(())
    }
}

pub fn execute(
    directory: &Path,
    workload: Workload,
    deployment: Deployment,
    calibration_files: usize,
    total_files: usize,
    budget: usize,
    scan: bool,
    oracle_directory: Option<&Path>,
    write_oracle: bool,
) -> anyhow::Result<ResultRow> {
    anyhow::ensure!(
        calibration_files >= 20 && total_files > calibration_files,
        "invalid replay geometry"
    );
    let began = Instant::now();
    let panels = panels(workload);
    let count = if deployment.shared { 1 } else { panels.len() };
    anyhow::ensure!(
        deployment.configs.len() == count,
        "configuration count mismatch"
    );
    anyhow::ensure!(
        !write_oracle
            || (oracle_directory.is_some()
                && deployment.shared
                && deployment.configs == vec![Config::Exact]),
        "only a shared exact deployment may produce the oracle"
    );
    if write_oracle {
        std::fs::create_dir_all(oracle_directory.unwrap())?;
    }
    let mut runtime = Runtime {
        workload,
        panels,
        deployment,
        stores: (0..count).map(|_| VecDeque::new()).collect(),
        oracle: VecDeque::new(),
        raw: VecDeque::new(),
        timing: Timing::default(),
        samples: vec![],
        dashboards: vec![],
        peak: 0,
        query_peak: 0,
        events: 0,
        updates: 0,
        violations: 0,
        budget,
        scan,
        start: calibration_files * 3,
        oracle_directory: oracle_directory.map(Path::to_path_buf),
        write_oracle,
    };
    let mut current = (calibration_files - 20) * 3;
    let mut pending = Vec::new();
    for index in calibration_files - 20..total_files {
        input::visit(
            &directory.join(format!("observations_{index}.bin.gz")),
            |event| {
                let minute = event.time as usize / 60000;
                anyhow::ensure!(
                    minute >= current && minute < total_files * 3,
                    "replay timestamp out of range"
                );
                if (workload == Workload::Edge && event.upstream == u32::MAX)
                    || (workload == Workload::Latency
                        && (!event.latency.is_finite() || event.latency < 0.))
                {
                    return Ok(());
                }
                while current < minute {
                    runtime.pane(current, std::mem::take(&mut pending))?;
                    current += 1;
                }
                pending.push(event);
                Ok(())
            },
        )?;
    }
    while current < total_files * 3 {
        runtime.pane(current, std::mem::take(&mut pending))?;
        current += 1;
    }
    runtime.timing.input_and_harness_seconds = (began.elapsed().as_secs_f64()
        - runtime.timing.update_seconds
        - runtime.timing.eviction_seconds
        - runtime.timing.merge_seconds
        - runtime.timing.readout_seconds
        - runtime.timing.oracle_seconds)
        .max(0.);
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("VmHWM:")
                    .and_then(|x| x.split_whitespace().next())
                    .and_then(|n| n.parse().ok())
            })
        });
    Ok(ResultRow {
        status: "complete".into(),
        workload,
        deployment: runtime.deployment,
        timing: runtime.timing,
        peak_retained_payload_bytes: runtime.peak,
        peak_query_payload_bytes: runtime.query_peak,
        events: runtime.events,
        updates: runtime.updates,
        samples: runtime.samples,
        dashboard_samples: runtime.dashboards,
        violations: runtime.violations,
        whole_process_peak_rss_kib: rss,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, File},
        io::Write,
    };
    #[test]
    fn all_workloads_share_endpoints_and_exact_panes_match_raw_scans() {
        let root =
            std::env::temp_dir().join(format!("alibaba-dashboard-e2e-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        for file in 0..22u32 {
            let mut out = flate2::write::GzEncoder::new(
                File::create(root.join(format!("observations_{file}.bin.gz"))).unwrap(),
                flate2::Compression::fast(),
            );
            for minute in file * 3..file * 3 + 3 {
                for id in 1..=4u32 {
                    for repeat in 0..id {
                        out.write_all(&(minute * 60000 + repeat).to_le_bytes())
                            .unwrap();
                        out.write_all(&id.to_le_bytes()).unwrap();
                        out.write_all(&id.to_le_bytes()).unwrap();
                        out.write_all(&(if id == 1 { 0. } else { id as f64 }).to_le_bytes())
                            .unwrap();
                    }
                }
            }
            out.finish().unwrap();
        }
        // Sort fixture events by timestamp within each minute, matching the
        // production reader contract (the construction above is key-major).
        for file in 0..22u32 {
            let path = root.join(format!("observations_{file}.bin.gz"));
            let mut raw = Vec::new();
            use std::io::Read;
            flate2::read::GzDecoder::new(File::open(&path).unwrap())
                .read_to_end(&mut raw)
                .unwrap();
            let mut rows: Vec<_> = raw.chunks_exact(20).map(|x| x.to_vec()).collect();
            rows.sort_by_key(|x| u32::from_le_bytes(x[..4].try_into().unwrap()));
            let mut out = flate2::write::GzEncoder::new(
                File::create(&path).unwrap(),
                flate2::Compression::fast(),
            );
            for row in rows {
                out.write_all(&row).unwrap();
            }
            out.finish().unwrap();
        }
        for workload in [Workload::Service, Workload::Edge, Workload::Latency] {
            let plan = Deployment {
                configs: vec![Config::Exact],
                shared: true,
                planning_seconds: 0.,
                searches: vec![],
                reason: "test".into(),
            };
            let scan = execute(
                &root,
                workload,
                plan.clone(),
                20,
                22,
                1 << 28,
                true,
                None,
                false,
            )
            .unwrap();
            let oracle = root.join(format!("oracle-{workload:?}"));
            let pane = execute(
                &root,
                workload,
                plan.clone(),
                20,
                22,
                1 << 28,
                false,
                Some(&oracle),
                true,
            )
            .unwrap();
            let cached = execute(
                &root,
                workload,
                plan,
                20,
                22,
                1 << 28,
                true,
                Some(&oracle),
                false,
            )
            .unwrap();
            assert_eq!(cached.violations, 0);
            assert_eq!(cached.samples.len(), scan.samples.len());
            assert_eq!(scan.violations, 0);
            assert_eq!(pane.violations, 0);
            assert_eq!(scan.events, pane.events);
            assert!(cached.samples.iter().all(|s| s["window_keys"] == 4));
            assert!(cached
                .samples
                .iter()
                .all(|s| s["window_events"].as_u64()
                    == s["panel"]["window"].as_u64().map(|w| w * 10)));
            assert_eq!(scan.samples.len(), 6 * panels(workload).len());
            assert_eq!(
                scan.samples
                    .iter()
                    .map(|s| (&s["end_minute"], &s["panel_id"]))
                    .collect::<Vec<_>>(),
                pane.samples
                    .iter()
                    .map(|s| (&s["end_minute"], &s["panel_id"]))
                    .collect::<Vec<_>>()
            );
            // Exercise measured profiles, real ERP selection, query-local
            // searches and held-out execution together, not just mocked costs.
            let (calibration, _) = input::calibration(&root, 20, 1000, 42).unwrap();
            let catalog = super::super::planning::build(&calibration, workload, 42, 1).unwrap();
            let local = super::super::planning::erp(&catalog, false, 1 << 28).unwrap();
            let shared = super::super::planning::erp(&catalog, true, 1 << 28).unwrap();
            let auto =
                super::super::planning::autosketch(&calibration, workload, 42, 1 << 28).unwrap();
            assert_eq!(auto.configs.len(), panels(workload).len());
            for deployment in [local, shared, auto, super::super::analytical(workload)] {
                let result = execute(
                    &root,
                    workload,
                    deployment,
                    20,
                    22,
                    1 << 28,
                    false,
                    Some(&oracle),
                    false,
                )
                .unwrap();
                assert_eq!(result.samples.len(), cached.samples.len());
                assert_eq!(result.events, cached.events);
                assert_eq!(result.violations, 0);
            }
        }
        fs::remove_dir_all(root).unwrap();
    }
}
