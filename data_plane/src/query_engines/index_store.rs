//! Bounded state for installed exact indexes; not a generic raw-query store.
use crate::drivers::ingest::prometheus_remote_write::CanonicalSample;
use crate::query_engines::asap_query_engine::{
    range_counter_index::RangeCounterIndex, range_max_index::RangeMaxIndex,
};
use crate::query_engines::{
    query_result::{InstantVectorElement, QueryResult},
    EngineError,
};
use crate::storage_engines::types::KeyByLabelValues;
use control_plane::query_plan::logical::{LabelMatch, LabelMatcher, TemporalOperation};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
type Labels = BTreeMap<String, String>;
#[derive(Clone, Debug, Default)]
pub struct IndexSpec {
    pub retention_ms: u64,
    pub populations: Vec<Vec<LabelMatcher>>,
    pub counter: bool,
    pub max: bool,
}
struct Series {
    labels: Labels,
    points: Arc<Vec<(i64, f64)>>,
    counter: OnceLock<RangeCounterIndex>,
    max: OnceLock<RangeMaxIndex>,
}
impl Clone for Series {
    fn clone(&self) -> Self {
        Self {
            labels: self.labels.clone(),
            points: self.points.clone(),
            counter: OnceLock::new(),
            max: OnceLock::new(),
        }
    }
}
#[derive(Default)]
struct Generation {
    plan: Option<(u64, u64)>,
    specs: BTreeMap<String, IndexSpec>,
    series: BTreeMap<Labels, Arc<Series>>,
    watermark: Option<i64>,
    evicted_before: BTreeMap<String, i64>,
    snapshot: Option<Arc<IndexedSamples>>,
}
#[derive(Default)]
pub struct IndexStore {
    state: Mutex<Generation>,
}
pub struct IndexedSamples {
    series: Vec<Arc<Series>>,
    specs: BTreeMap<String, IndexSpec>,
    evicted_before: BTreeMap<String, i64>,
}
fn miss(text: impl Into<String>) -> EngineError {
    EngineError::capability_miss("installed_index", text)
}
// A union of installed selectors authorizes admission. Invalid regex selectors
// admit no state; their readout fails closed through the existing regex check.
fn selected_population(
    spec: &IndexSpec,
    labels: &Labels,
    regexes: &BTreeMap<String, Option<regex::Regex>>,
) -> bool {
    spec.populations.iter().any(|population| {
        population.iter().all(|matcher| {
            let value = labels.get(&matcher.name).map(String::as_str).unwrap_or("");
            match matcher.operation {
                LabelMatch::Equal => value == matcher.value,
                LabelMatch::NotEqual => value != matcher.value,
                LabelMatch::Regex | LabelMatch::NotRegex => regexes
                    .get(&matcher.value)
                    .and_then(Option::as_ref)
                    .is_some_and(|regex| {
                        regex.is_match(value) == matches!(matcher.operation, LabelMatch::Regex)
                    }),
            }
        })
    })
}
impl IndexStore {
    pub fn append_admitted(
        &self,
        plan_id: u64,
        version: u64,
        samples: &[CanonicalSample],
        specs: &BTreeMap<String, IndexSpec>,
    ) {
        let mut regexes = BTreeMap::new();
        for matcher in specs
            .values()
            .flat_map(|spec| spec.populations.iter().flatten())
        {
            if matches!(matcher.operation, LabelMatch::Regex | LabelMatch::NotRegex) {
                regexes.entry(matcher.value.clone()).or_insert_with(|| {
                    regex::Regex::new(&format!("(?s)^(?:{})$", matcher.value)).ok()
                });
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.plan != Some((plan_id, version)) {
            *state = Generation {
                plan: Some((plan_id, version)),
                ..Default::default()
            };
        }
        state.snapshot = None;
        state.specs = specs.clone();
        state.series.retain(|labels, _| {
            labels
                .get("__name__")
                .and_then(|metric| specs.get(metric))
                .is_some_and(|spec| selected_population(spec, labels, &regexes))
        });
        for sample in samples {
            if !specs.contains_key(&sample.metric) {
                continue;
            }
            state.watermark = Some(
                state
                    .watermark
                    .map_or(sample.timestamp_ms, |time| time.max(sample.timestamp_ms)),
            );
            let Some(value) = sample.value.filter(|v| v.to_bits() != 0x7ff0000000000002) else {
                continue;
            };
            if state
                .evicted_before
                .get(&sample.metric)
                .is_some_and(|cutoff| sample.timestamp_ms <= *cutoff)
            {
                continue;
            }
            let mut labels: Labels = sample.labels.clone().into_iter().collect();
            labels.insert("__name__".into(), sample.metric.clone());
            if !selected_population(&specs[&sample.metric], &labels, &regexes) {
                continue;
            }
            let series = state.series.entry(labels.clone()).or_insert_with(|| {
                Arc::new(Series {
                    labels,
                    points: Arc::new(Vec::new()),
                    counter: OnceLock::new(),
                    max: OnceLock::new(),
                })
            });
            let series = Arc::make_mut(series);
            series.counter = OnceLock::new();
            series.max = OnceLock::new();
            let points = Arc::make_mut(&mut series.points);
            if points
                .last()
                .map_or(true, |point| point.0 < sample.timestamp_ms)
            {
                points.push((sample.timestamp_ms, value));
            } else {
                match points.binary_search_by_key(&sample.timestamp_ms, |p| p.0) {
                    Ok(_) => {} // Receiver already rejected conflicts and duplicates.
                    Err(index) => points.insert(index, (sample.timestamp_ms, value)),
                }
            }
        }
        let cutoffs: BTreeMap<_, _> = specs
            .iter()
            .filter_map(|(metric, spec)| {
                state.watermark.map(|watermark| {
                    (
                        metric.clone(),
                        watermark
                            .saturating_sub(i64::try_from(spec.retention_ms).unwrap_or(i64::MAX)),
                    )
                })
            })
            .collect();
        let mut evicted = Vec::new();
        for (labels, series) in &mut state.series {
            let metric = &labels["__name__"];
            let Some(cutoff) = cutoffs.get(metric) else {
                continue;
            };
            let count = series.points.partition_point(|p| p.0 <= *cutoff);
            if count > 0 {
                let series = Arc::make_mut(series);
                series.counter = OnceLock::new();
                series.max = OnceLock::new();
                let points = Arc::make_mut(&mut series.points);
                points.drain(..count);
                points.shrink_to_fit();
                evicted.push((metric.clone(), *cutoff));
            }
        }
        for (metric, cutoff) in evicted {
            state
                .evicted_before
                .entry(metric)
                .and_modify(|v| *v = (*v).max(cutoff))
                .or_insert(cutoff);
        }
        state.series.retain(|_, series| !series.points.is_empty());
    }
    pub fn snapshot(&self, plan_id: u64, version: u64) -> Result<Arc<IndexedSamples>, EngineError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.plan.is_some_and(|p| p != (plan_id, version)) {
            return Err(miss("index state belongs to another plan generation"));
        }
        if let Some(snapshot) = &state.snapshot {
            return Ok(snapshot.clone());
        }
        let snapshot = Arc::new(IndexedSamples {
            series: state.series.values().cloned().collect(),
            specs: state.specs.clone(),
            evicted_before: state.evicted_before.clone(),
        });
        state.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }
    pub fn sample_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .series
            .values()
            .map(|s| s.points.len())
            .sum()
    }
    pub fn estimated_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .series
            .values()
            .map(|s| s.bytes())
            .sum()
    }
    pub fn range_max_index_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .series
            .values()
            .filter_map(|s| s.max.get())
            .map(|i| i.estimated_bytes())
            .sum()
    }
    pub fn range_counter_index_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .series
            .values()
            .filter_map(|s| s.counter.get())
            .map(|i| i.estimated_bytes())
            .sum()
    }
}
impl Series {
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.points.capacity() * std::mem::size_of::<(i64, f64)>()
            + self
                .labels
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>()
            + self.counter.get().map_or(0, |i| i.estimated_bytes())
            + self.max.get().map_or(0, |i| i.estimated_bytes())
    }
}
impl IndexedSamples {
    pub fn prepare(&self) {
        for series in &self.series {
            if let Some(spec) = self.specs.get(&series.labels["__name__"]) {
                if spec.counter {
                    series.counter.get_or_init(|| {
                        RangeCounterIndex::from_shared_points(series.points.clone())
                    });
                }
                if spec.max {
                    series.max.get_or_init(|| {
                        RangeMaxIndex::new(series.points.iter().map(|p| Some(p.1)))
                    });
                }
            }
        }
    }
    pub fn read_counter(
        &self,
        metric: &str,
        matchers: &[LabelMatcher],
        range_ms: u64,
        offset_ms: i64,
        at: u64,
        operation: TemporalOperation,
    ) -> Result<(QueryResult, usize), EngineError> {
        if !matches!(
            operation,
            TemporalOperation::Rate | TemporalOperation::Increase
        ) {
            return Err(miss("unsupported counter readout"));
        }
        self.read(metric, matchers, range_ms, offset_ms, at, Some(operation))
    }
    pub fn read_max(
        &self,
        metric: &str,
        matchers: &[LabelMatcher],
        range_ms: u64,
        at: u64,
    ) -> Result<(QueryResult, usize), EngineError> {
        self.read(metric, matchers, range_ms, 0, at, None)
    }
    fn read(
        &self,
        metric: &str,
        matchers: &[LabelMatcher],
        range_ms: u64,
        offset_ms: i64,
        at: u64,
        operation: Option<TemporalOperation>,
    ) -> Result<(QueryResult, usize), EngineError> {
        let end = i64::try_from(at)
            .ok()
            .and_then(|t| t.checked_sub(offset_ms))
            .ok_or_else(|| miss("index timestamp overflow"))?;
        let start = end
            .checked_sub(i64::try_from(range_ms).map_err(|_| miss("index range overflow"))?)
            .ok_or_else(|| miss("index range overflow"))?;
        if self
            .evicted_before
            .get(metric)
            .is_some_and(|cutoff| start < *cutoff)
        {
            return Err(miss("requested interval predates retained index state"));
        }
        let spec = self
            .specs
            .get(metric)
            .ok_or_else(|| miss("index metric is not installed"))?;
        if range_ms == 0 || operation.is_some() && !spec.counter || operation.is_none() && !spec.max
        {
            return Err(miss("index readout is not installed"));
        }
        let regexes: Result<BTreeMap<_, _>, _> = matchers
            .iter()
            .filter(|m| matches!(m.operation, LabelMatch::Regex | LabelMatch::NotRegex))
            .map(|m| {
                regex::Regex::new(&format!("(?s)^(?:{})$", m.value)).map(|r| (m.value.clone(), r))
            })
            .collect();
        let regexes = regexes.map_err(|e| miss(e.to_string()))?;
        let mut values = Vec::new();
        let mut reads = 0;
        for series in &self.series {
            if series.labels.get("__name__").map(String::as_str) != Some(metric)
                || !matchers.iter().all(|m| {
                    let value = series.labels.get(&m.name).map(String::as_str).unwrap_or("");
                    match m.operation {
                        LabelMatch::Equal => value == m.value,
                        LabelMatch::NotEqual => value != m.value,
                        LabelMatch::Regex => regexes[&m.value].is_match(value),
                        LabelMatch::NotRegex => !regexes[&m.value].is_match(value),
                    }
                })
            {
                continue;
            }
            let value = if let Some(operation) = operation {
                let index = series
                    .counter
                    .get_or_init(|| RangeCounterIndex::from_shared_points(series.points.clone()));
                match operation {
                    TemporalOperation::Rate => index.rate(start, end),
                    TemporalOperation::Increase => index.increase(start, end),
                    _ => unreachable!(),
                }
                .map_err(|e| miss(e.to_string()))?
            } else {
                let index = series
                    .max
                    .get_or_init(|| RangeMaxIndex::new(series.points.iter().map(|p| Some(p.1))));
                index.query(
                    series.points.partition_point(|p| p.0 <= start),
                    series.points.partition_point(|p| p.0 <= end),
                )
            };
            reads += 1;
            if let Some(value) = value {
                let mut labels = series.labels.clone();
                labels.remove("__name__");
                values.push(
                    InstantVectorElement::new(
                        KeyByLabelValues::new_with_labels(labels.values().cloned().collect()),
                        value,
                    )
                    .with_label_keys_override(labels.into_keys().collect()),
                );
            }
        }
        Ok((QueryResult::vector(values, at), reads))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(metric: &str, time: i64, value: f64) -> CanonicalSample {
        CanonicalSample {
            metric: metric.into(),
            labels: HashMap::from([("job".into(), "api".into())]),
            series_key: format!("{metric}:api"),
            timestamp_ms: time,
            value: Some(value),
        }
    }
    use std::collections::HashMap;
    fn specs() -> BTreeMap<String, IndexSpec> {
        BTreeMap::from([(
            "m".into(),
            IndexSpec {
                retention_ms: 2000,
                populations: vec![vec![]],
                counter: true,
                max: true,
            },
        )])
    }
    #[test]
    fn no_installed_indexes_retain_no_rows() {
        let store = IndexStore::default();
        store.append_admitted(1, 1, &[sample("m", 1000, 1.)], &BTreeMap::new());
        assert_eq!(store.sample_count(), 0);
        assert_eq!(store.estimated_bytes(), 0);
    }
    #[test]
    fn eviction_is_bounded_and_older_queries_require_exact_leaf_fallback() {
        let store = IndexStore::default();
        store.append_admitted(
            1,
            1,
            &[
                sample("m", 1000, 3.),
                sample("m", 2000, 7.),
                sample("unused", 2000, 900.),
            ],
            &specs(),
        );
        let initial = store.snapshot(1, 1).unwrap();
        assert!(
            initial.read_max("m", &[], 5000, 2000).is_ok(),
            "natural beginning is not eviction"
        );
        store.append_admitted(1, 1, &[sample("m", 4000, 9.)], &specs());
        assert_eq!(store.sample_count(), 1);
        let latest = store.snapshot(1, 1).unwrap();
        assert!(latest.read_max("m", &[], 3000, 4000).is_err());
        let (QueryResult::Vector(value), reads) = latest.read_max("m", &[], 2000, 4000).unwrap()
        else {
            panic!()
        };
        assert_eq!(reads, 1);
        assert_eq!(value.values[0].value, 9.);
        let (QueryResult::Vector(old), _) = initial.read_max("m", &[], 2000, 2000).unwrap() else {
            panic!()
        };
        assert_eq!(
            old.values[0].value, 7.,
            "old in-flight generation stays coherent"
        );
        assert!(store.snapshot(2, 1).is_err());
    }
    #[test]
    fn other_indexed_metrics_advance_idle_metric_eviction() {
        let store = IndexStore::default();
        let mut config = specs();
        config.insert(
            "active".into(),
            IndexSpec {
                retention_ms: 2000,
                populations: vec![vec![]],
                counter: true,
                max: false,
            },
        );
        store.append_admitted(1, 1, &[sample("m", 1000, 3.)], &config);
        store.append_admitted(1, 1, &[sample("active", 5000, 4.)], &config);
        assert_eq!(store.sample_count(), 1);
        assert!(store
            .snapshot(1, 1)
            .unwrap()
            .read_max("m", &[], 5000, 5000)
            .is_err());
    }
    #[test]
    fn only_installed_population_union_is_retained_and_unmatched_input_advances_eviction() {
        let store = IndexStore::default();
        let mut config = specs();
        config.get_mut("m").unwrap().populations = vec![vec![LabelMatcher {
            name: "job".into(),
            value: "user-service".into(),
            operation: LabelMatch::Equal,
        }]];
        let mut user = sample("m", 1000, 3.);
        user.labels.insert("job".into(), "user-service".into());
        let mut order = sample("m", 1000, 4.);
        order.labels.insert("job".into(), "order-service".into());
        let mut missing = sample("m", 1000, 5.);
        missing.labels.clear();
        store.append_admitted(
            1,
            1,
            &[user.clone(), order.clone(), missing.clone()],
            &config,
        );
        assert_eq!(store.sample_count(), 1);
        order.timestamp_ms = 5000;
        store.append_admitted(1, 1, &[order.clone()], &config);
        assert_eq!(
            store.sample_count(),
            0,
            "unselected population still advances event time"
        );
        config
            .get_mut("m")
            .unwrap()
            .populations
            .push(vec![LabelMatcher {
                name: "job".into(),
                value: "order-.+".into(),
                operation: LabelMatch::Regex,
            }]);
        order.timestamp_ms = 1000;
        store.append_admitted(
            1,
            2,
            &[user.clone(), order.clone(), missing.clone()],
            &config,
        );
        assert_eq!(
            store.sample_count(),
            2,
            "union keeps both selected jobs but no missing label"
        );
        config.get_mut("m").unwrap().populations.push(vec![]);
        store.append_admitted(1, 3, &[user, order, missing], &config);
        assert_eq!(
            store.sample_count(),
            3,
            "one empty selector authorizes all source series"
        );
    }
    #[test]
    fn snapshots_and_both_indexes_share_one_canonical_buffer() {
        let store = IndexStore::default();
        store.append_admitted(
            1,
            1,
            &[sample("m", 1000, 3.), sample("m", 2000, 7.)],
            &specs(),
        );
        let snapshot = store.snapshot(1, 1).unwrap();
        let points = snapshot.series[0].points.as_ptr();
        snapshot.prepare();
        assert_eq!(snapshot.series[0].points.as_ptr(), points);
        assert!(Arc::ptr_eq(&snapshot, &store.snapshot(1, 1).unwrap()));
        assert!(store.range_counter_index_bytes() > 0 && store.range_max_index_bytes() > 0);
        let retained = store.state.lock().unwrap();
        assert!(Arc::ptr_eq(
            retained.series.values().next().unwrap(),
            &snapshot.series[0]
        ));
    }
}
