//! Bounded current-value state. Never pools a series' old samples into a quantile.
use crate::drivers::ingest::prometheus_remote_write::CanonicalSample;
use asap_types::query_plan::{
    current_series::SeriesPopulation,
    query_time::{LabelMatch, QueryTimeOperator},
    QueryPlan, QueryPlanNode,
};
use std::collections::{BTreeMap, BTreeSet};

type Labels = BTreeMap<String, String>;
pub type Vector = Vec<(Labels, f64)>;
#[derive(Debug, Clone)]
struct Ranked {
    value: f64,
    labels: Labels,
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value
            .total_cmp(&other.value)
            .then(self.labels.cmp(&other.labels))
    }
}
#[derive(Clone)]
struct Member {
    timestamp: i64,
    value: Option<f64>,
    group: Labels,
    bytes: u64,
}
#[derive(Default, Clone)]
struct Group {
    ordered: BTreeSet<Ranked>,
}
#[derive(Clone)]
struct Population {
    definition: SeriesPopulation,
    members: BTreeMap<Labels, Member>,
    expiry: BTreeSet<(i64, Labels)>,
    groups: BTreeMap<Labels, Group>,
    bytes: u64,
    /// Input timestamp at which the budget blew, cleared once the lookback
    /// window has moved entirely past it. `None` means the population serves.
    unavailable: Option<i64>,
    last_read: i64,
    matchers: Vec<promql_parser::label::Matcher>,
}
impl Population {
    fn new(definition: SeriesPopulation) -> Result<Self, String> {
        definition.validate().map_err(|e| e.to_string())?;
        let mut matchers = vec![];
        for matcher in &definition.matchers {
            use promql_parser::parser::token::{T_EQL, T_EQL_REGEX, T_NEQ, T_NEQ_REGEX};
            let token = match matcher.operation {
                LabelMatch::Equal => T_EQL,
                LabelMatch::NotEqual => T_NEQ,
                LabelMatch::Regex => T_EQL_REGEX,
                LabelMatch::NotRegex => T_NEQ_REGEX,
            };
            matchers.push(promql_parser::label::Matcher::new_matcher(
                token,
                matcher.name.clone(),
                matcher.value.clone(),
            )?);
        }
        Ok(Self {
            definition,
            members: BTreeMap::new(),
            expiry: BTreeSet::new(),
            groups: BTreeMap::new(),
            bytes: 0,
            unavailable: None,
            last_read: i64::MIN,
            matchers,
        })
    }
    fn remove(&mut self, labels: &Labels) {
        if let Some(old) = self.members.remove(labels) {
            self.expiry.remove(&(old.timestamp, labels.clone()));
            self.bytes -= old.bytes;
            if let Some(value) = old.value {
                if let Some(group) = self.groups.get_mut(&old.group) {
                    group.ordered.remove(&Ranked {
                        value,
                        labels: labels.clone(),
                    });
                    if group.ordered.is_empty() {
                        self.groups.remove(&old.group);
                    }
                }
            }
        }
    }
    /// A budget overflow clears the population, so holding the latch past the
    /// offending window would strand the plan on Prometheus until an operator
    /// forces a replan. Re-arm once the burst has aged out of the lookback.
    fn rearm(&mut self, cutoff: i64) {
        if self.unavailable.is_some_and(|blew_at| cutoff >= blew_at) {
            self.unavailable = None;
        }
    }
    fn expire(&mut self, cutoff: i64) {
        while let Some((timestamp, labels)) = self.expiry.first().cloned() {
            if timestamp > cutoff {
                break;
            }
            self.remove(&labels);
        }
    }
    fn update(&mut self, sample: &CanonicalSample, cutoff: i64) {
        self.rearm(cutoff);
        if self.unavailable.is_some()
            || sample.metric.as_ref() != self.definition.metric
            || sample.timestamp_ms <= cutoff
        {
            return;
        }
        let mut labels: Labels = sample
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        labels.insert("__name__".into(), sample.metric.to_string());
        if !self.matchers.iter().all(|matcher| {
            matcher.is_match(labels.get(&matcher.name).map(String::as_str).unwrap_or(""))
        }) {
            return;
        }
        if self
            .members
            .get(&labels)
            .is_some_and(|old| old.timestamp >= sample.timestamp_ms)
        {
            return;
        }
        let group: Labels = labels
            .iter()
            .filter(|(key, _)| {
                if self.definition.grouping.without {
                    key.as_str() != "__name__" && !self.definition.grouping.labels.contains(key)
                } else {
                    self.definition.grouping.labels.contains(key)
                }
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.remove(&labels);
        // Bound the retained keys, tree nodes, and shared readout caches together.
        let bytes = 1024
            + labels
                .iter()
                .map(|(k, v)| (k.len() + v.len()) as u64 * 16)
                .sum::<u64>();
        if self.members.len() >= self.definition.max_series
            || self.bytes.saturating_add(bytes) > self.definition.max_bytes
        {
            self.unavailable = Some(sample.timestamp_ms);
            self.members.clear();
            self.groups.clear();
            self.expiry.clear();
            self.bytes = 0;
            return;
        }
        self.bytes += bytes;
        self.expiry.insert((sample.timestamp_ms, labels.clone()));
        self.members.insert(
            labels.clone(),
            Member {
                timestamp: sample.timestamp_ms,
                value: sample.value,
                group: group.clone(),
                bytes,
            },
        );
        if let Some(value) = sample.value {
            let state = self.groups.entry(group).or_default();
            state.ordered.insert(Ranked { value, labels });
        }
    }
    /// Every current member with a value, group by group.
    fn snapshot(&self) -> Vector {
        self.groups
            .values()
            .flat_map(|group| group.ordered.iter())
            .map(|member| (member.labels.clone(), member.value))
            .collect()
    }
}

#[derive(Default)]
pub struct CurrentSeriesStore {
    generation: Option<(u64, u64)>,
    populations: BTreeMap<String, Population>,
    first: Option<i64>,
    watermark: Option<i64>,
    // Completed population versions, bounded by the same declared state budget.
    history: BTreeMap<String, BTreeMap<i64, (i64, Population)>>,
    history_bytes: BTreeMap<String, u64>,
}
impl CurrentSeriesStore {
    pub(crate) fn approx_resident_bytes(&self) -> usize {
        self.populations
            .values()
            .map(|population| population.bytes as usize + 1024)
            .sum::<usize>()
            + self
                .history_bytes
                .values()
                .map(|bytes| *bytes as usize)
                .sum::<usize>()
    }

    /// Called only after the complete Remote Write batch was admitted successfully.
    pub fn ingest(&mut self, plan: &QueryPlan, samples: &[CanonicalSample]) {
        let generation = (plan.plan_id, plan.plan_version);
        if self.generation != Some(generation) {
            *self = Self::default();
            self.generation = Some(generation);
            for entry in plan.entries.values() {
                for node in entry.nodes.values() {
                    if let QueryPlanNode::Logical {
                        operator: QueryTimeOperator::CurrentSeries { population, .. },
                        ..
                    } = node
                    {
                        if let Ok(state) = Population::new(population.clone()) {
                            self.populations.entry(population.key()).or_insert(state);
                        }
                    }
                }
            }
        }
        if self.populations.is_empty() || samples.is_empty() {
            return;
        }
        let mut batches = BTreeMap::<i64, Vec<&CanonicalSample>>::new();
        for sample in samples {
            batches.entry(sample.timestamp_ms).or_default().push(sample);
        }
        if self
            .watermark
            .is_some_and(|w| batches.keys().any(|t| *t <= w))
        {
            // Late writes invalidate completed versions; never serve a stale revision.
            self.history.clear();
            self.history_bytes.clear();
        }
        let max_lag = self
            .populations
            .values()
            .map(|p| p.definition.max_input_lag_ms)
            .min()
            .unwrap() as i64;
        for (timestamp, batch) in batches {
            if self
                .watermark
                .is_some_and(|w| timestamp > w.saturating_add(max_lag))
            {
                self.first = Some(timestamp);
            }
            self.first.get_or_insert(timestamp);
            self.watermark = Some(self.watermark.unwrap_or(timestamp).max(timestamp));
            let watermark = self.watermark.unwrap();
            for (key, population) in &mut self.populations {
                let cutoff = watermark.saturating_sub(population.definition.lookback_ms as i64);
                population.expire(cutoff);
                for sample in &batch {
                    population.update(sample, cutoff);
                }
                let retention = population.definition.history_retention_ms;
                if retention == 0 || timestamp < watermark {
                    continue;
                }
                let history = self.history.entry(key.clone()).or_default();
                let bytes = self.history_bytes.entry(key.clone()).or_default();
                let oldest = watermark.saturating_sub(retention as i64);
                let checkpoint_bytes = population.bytes.saturating_add(1024);
                // Include fixed per-version metadata even for an empty population.
                while history.first_key_value().is_some_and(|(t, _)| *t < oldest)
                    || (!history.is_empty()
                        && bytes
                            .saturating_add(checkpoint_bytes)
                            .saturating_add(population.bytes)
                            > population.definition.max_bytes)
                {
                    let (_, (_, expired)) = history.pop_first().unwrap();
                    *bytes -= expired.bytes.saturating_add(1024);
                }
                if population.unavailable.is_none()
                    && bytes
                        .saturating_add(checkpoint_bytes)
                        .saturating_add(population.bytes)
                        <= population.definition.max_bytes
                {
                    if let Some((_, previous)) =
                        history.insert(timestamp, (self.first.unwrap(), population.clone()))
                    {
                        *bytes -= previous.bytes.saturating_add(1024);
                    }
                    *bytes += checkpoint_bytes;
                } else {
                    // A missing version must not let a historical read reuse an older value.
                    history.clear();
                    *bytes = 0;
                }
            }
        }
    }

    pub fn read(
        &mut self,
        generation: (u64, u64),
        definition: &SeriesPopulation,
        at: u64,
    ) -> Result<Vector, String> {
        if self.generation != Some(generation) {
            return Err("current-series generation is not ingested".into());
        }
        let at = i64::try_from(at).map_err(|_| "invalid evaluation timestamp")?;
        let watermark = self.watermark.ok_or("current-series state is cold")?;
        if at < watermark
            || (at == watermark
                && self
                    .history
                    .get(&definition.key())
                    .is_some_and(|h| h.contains_key(&at)))
        {
            if definition.history_retention_ms == 0
                || at < watermark.saturating_sub(definition.history_retention_ms as i64)
            {
                return Err("current-series state cannot answer historical evaluations outside declared retention".into());
            }
            let (timestamp, (first, saved)) = self
                .history
                .get(&definition.key())
                .and_then(|h| h.range(..=at).next_back())
                .ok_or("current-series historical coverage is unavailable")?;
            if at > timestamp.saturating_add(definition.max_input_lag_ms as i64)
                || at.saturating_sub(definition.lookback_ms as i64) < *first
            {
                return Err("current-series historical lookback is not covered".into());
            }
            let mut population = saved.clone();
            population.expire(at.saturating_sub(definition.lookback_ms as i64));
            return Ok(population.snapshot());
        }
        if at > watermark.saturating_add(definition.max_input_lag_ms as i64) {
            return Err("current-series input is behind evaluation time".into());
        }
        if at.saturating_sub(definition.lookback_ms as i64)
            < self.first.ok_or("current-series state is cold")?
        {
            return Err("current-series lookback is not covered yet".into());
        }
        let population = self
            .populations
            .get_mut(&definition.key())
            .ok_or("current-series population is not installed")?;
        population.rearm(at.saturating_sub(definition.lookback_ms as i64));
        if population.unavailable.is_some() {
            return Err("current-series population exceeded its resource budget".into());
        }
        if at < population.last_read {
            return Err("current-series evaluation precedes already expired state".into());
        }
        population.last_read = at;
        population.expire(at.saturating_sub(definition.lookback_ms as i64));
        Ok(population.snapshot())
    }
    pub fn population_count(&self) -> usize {
        self.populations.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::ingest::prometheus_remote_write::{
        canonicalize_request, Label, PrometheusRemoteWriteConfig, Sample, TimeSeries, WriteRequest,
        STALE_NAN_BITS,
    };
    use asap_types::query_plan::{
        FallbackPolicy, InstantExecution, QueryLanguage, QueryNodeId, QueryPlanEntry,
    };
    fn definition() -> SeriesPopulation {
        SeriesPopulation {
            metric: "a".into(),
            matchers: vec![],
            grouping: asap_types::query_plan::query_time::Grouping {
                labels: vec!["job".into()],
                without: false,
            },
            lookback_ms: 300_000,
            max_input_lag_ms: 60_000,
            history_retention_ms: 0,
            max_series: 100,
            max_bytes: 1_000_000,
            max_k: 3,
            quantiles: true,
        }
    }

    // A declared one-second horizon expires the left-boundary sample, not five minutes later.
    #[test]
    fn declared_one_second_horizon_expires_members() {
        let mut population = definition();
        population.lookback_ms = 1_000;
        population.max_input_lag_ms = 1_000;
        population.validate().unwrap();
        let plan = plan(&population);
        let mut store = CurrentSeriesStore::default();
        store.ingest(&plan, &[sample("old", "api", 0, Some(10.))]);
        store.ingest(&plan, &[sample("new", "api", 500, Some(3.))]);
        let values = store.read((7, 1), &population, 1_000).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].1, 3.);
        let values = store.read((7, 1), &population, 1_500).unwrap();
        assert!(values.is_empty());
    }
    // Historical reads use the state at that timestamp, never the latest values.
    #[test]
    fn declared_history_retains_as_of_population_within_budget() {
        let mut population = definition();
        population.lookback_ms = 1_000;
        population.max_input_lag_ms = 1_000;
        population.history_retention_ms = 2_000;
        let plan = plan(&population);
        let mut store = CurrentSeriesStore::default();
        store.ingest(
            &plan,
            &[
                sample("one", "api", 0, Some(1.)),
                sample("one", "api", 1_000, Some(2.)),
                sample("one", "api", 2_000, Some(3.)),
                sample("one", "api", 3_000, Some(4.)),
            ],
        );
        assert_eq!(store.read((7, 1), &population, 1_000).unwrap()[0].1, 2.);
        assert_eq!(store.read((7, 1), &population, 3_000).unwrap()[0].1, 4.);
        assert!(store.read((7, 1), &population, 999).is_err());
        assert!(
            store.history_bytes[&population.key()] + store.populations[&population.key()].bytes
                <= population.max_bytes
        );
    }

    // Resource exhaustion and late revisions cannot masquerade as historical coverage.
    #[test]
    fn retained_history_rejects_evicted_and_late_revisions() {
        let mut population = definition();
        population.lookback_ms = 1_000;
        population.max_input_lag_ms = 1_000;
        population.history_retention_ms = 2_000;
        let installed = plan(&population);
        let mut store = CurrentSeriesStore::default();
        store.ingest(
            &installed,
            &[
                sample("one", "api", 0, Some(1.)),
                sample("one", "api", 1_000, Some(2.)),
                sample("one", "api", 2_000, Some(3.)),
            ],
        );
        store.ingest(&installed, &[sample("two", "api", 1_500, Some(20.))]);
        assert!(store.read((7, 1), &population, 1_000).is_err());
        let mut bounded = population.clone();
        bounded.max_bytes = 2_500;
        let mut store = CurrentSeriesStore::default();
        store.ingest(
            &plan(&bounded),
            &[
                sample("one", "api", 0, Some(1.)),
                sample("one", "api", 1_000, Some(2.)),
                sample("one", "api", 2_000, Some(3.)),
            ],
        );
        assert!(store.read((7, 1), &bounded, 1_000).is_err());
        assert!(
            store.history_bytes[&bounded.key()] + store.populations[&bounded.key()].bytes
                <= bounded.max_bytes
        );
    }

    fn plan(p: &SeriesPopulation) -> QueryPlan {
        let mut plan = QueryPlan::empty();
        plan.plan_id = 7;
        plan.plan_version = 1;
        plan.entries.insert(
            "test".into(),
            QueryPlanEntry {
                physical_dag: None,
                language: QueryLanguage::PromQl,
                query_id: "test".into(),
                canonical_query: "quantile by (job) (0.5, a)".into(),
                fixed_evaluation: None,
                root: QueryNodeId(0),
                nodes: BTreeMap::from([(
                    QueryNodeId(0),
                    QueryPlanNode::Logical {
                        operator: QueryTimeOperator::CurrentSeries {
                            population: p.clone(),
                        },
                        inputs: vec![],
                    },
                )]),
                instant: InstantExecution {
                    lookback_ms: 300_000,
                    full_history: false,
                    cumulative_readout: true,
                },
                fallback: FallbackPolicy::ExactBackend,
            },
        );
        plan
    }
    fn sample(pod: &str, job: &str, timestamp: i64, value: Option<f64>) -> CanonicalSample {
        canonicalize_request(
            &WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: [("__name__", "a"), ("pod", pod), ("job", job)]
                        .into_iter()
                        .map(|(name, value)| Label {
                            name: name.into(),
                            value: value.into(),
                        })
                        .collect(),
                    samples: vec![Sample {
                        timestamp,
                        value: value.unwrap_or(f64::from_bits(STALE_NAN_BITS)),
                    }],
                    exemplars: vec![],
                    histograms: vec![],
                }],
            },
            &PrometheusRemoteWriteConfig::default(),
        )
        .unwrap()
        .remove(0)
    }
    fn warm(store: &mut CurrentSeriesStore, plan: &QueryPlan) {
        for t in (0..=300_000).step_by(60_000) {
            store.ingest(
                plan,
                &[
                    sample("x", "api", t, Some(1.)),
                    sample("y", "api", t, Some(9.)),
                    sample("z", "api", t, Some(5.)),
                    sample("w", "db", t, Some(50.)),
                ],
            );
        }
    }
    fn members(values: Vector) -> Vec<(String, f64)> {
        let mut members = values
            .into_iter()
            .map(|(labels, value)| (labels["pod"].clone(), value))
            .collect::<Vec<_>>();
        members.sort_by(|a, b| a.0.cmp(&b.0));
        members
    }

    // Equal sample values still represent two series; replacements and stale
    // markers retract them, and out-of-order values cannot resurrect a series.
    #[test]
    fn snapshot_follows_current_series_membership() {
        let p = definition();
        let plan = plan(&p);
        let mut store = CurrentSeriesStore::default();
        warm(&mut store, &plan);
        store.ingest(&plan, &[sample("y", "api", 301_000, Some(1.))]);
        assert_eq!(
            members(store.read((7, 1), &p, 301_000).unwrap()),
            [("w", 50.), ("x", 1.), ("y", 1.), ("z", 5.)].map(|(pod, value)| (pod.into(), value))
        );
        store.ingest(&plan, &[sample("z", "api", 302_000, None)]);
        store.ingest(&plan, &[sample("z", "api", 301_000, Some(100.))]);
        assert_eq!(
            members(store.read((7, 1), &p, 302_000).unwrap()),
            [("w", 50.), ("x", 1.), ("y", 1.)].map(|(pod, value)| (pod.into(), value))
        );
        let rows = store.read((7, 1), &p, 302_000).unwrap();
        assert!(rows
            .iter()
            .all(|(labels, _)| labels["__name__"] == "a" && labels.contains_key("job")));
    }

    /// Cold state, gaps, old generations and historical timestamps cannot masquerade as complete populations.
    #[test]
    fn coverage_expiration_generation_and_capacity_fail_closed() {
        let p = definition();
        let plan = plan(&p);
        let mut store = CurrentSeriesStore::default();
        store.ingest(&plan, &[sample("x", "api", 0, Some(1.))]);
        assert!(store.read((7, 1), &p, 0).is_err());
        warm(&mut store, &plan);
        assert!(store.read((7, 2), &p, 300_000).is_err());
        assert!(store.read((7, 1), &p, 299_000).is_err());
        for t in (360_000..=600_000).step_by(60_000) {
            store.ingest(&plan, &[sample("y", "api", t, Some(9.))]);
        }
        assert_eq!(store.read((7, 1), &p, 600_000).unwrap().len(), 1); // Prometheus 3.5 lookback is left-open
        assert_eq!(store.read((7, 1), &p, 600_001).unwrap().len(), 1);
        assert!(store.read((7, 1), &p, 600_000).is_err());
        store.ingest(&plan, &[sample("y", "api", 900_000, Some(9.))]);
        assert!(store.read((7, 1), &p, 900_000).is_err());
        let mut bounded = p.clone();
        bounded.max_series = 3;
        let plan = super::tests::plan(&bounded);
        let mut store = CurrentSeriesStore::default();
        warm(&mut store, &plan);
        assert!(store
            .read((7, 1), &bounded, 300_000)
            .unwrap_err()
            .contains("budget"));
    }

    // Prometheus 3.5 selectors exclude samples exactly at evaluation - lookback.
    #[test]
    fn lookback_left_boundary_expires_members() {
        let p = definition();
        let plan = plan(&p);
        let mut store = CurrentSeriesStore::default();
        warm(&mut store, &plan);
        for t in (360_000..=600_000).step_by(60_000) {
            store.ingest(&plan, &[sample("y", "api", t, Some(9.))]);
        }
        assert_eq!(
            members(store.read((7, 1), &p, 600_000).unwrap()),
            vec![("y".to_string(), 9.)]
        );
    }
}
