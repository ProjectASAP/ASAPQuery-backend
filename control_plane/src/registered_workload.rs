//! Registration state: canonical Planner workload plus collector deployment options.
use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, ensure};
use planner_types::workload::*;

use crate::{
    query_parser::{self, ParsedQuery},
    types::{AggType, SketchType, WorkloadCharacteristics},
    types_v2::AccuracyTarget,
};

/// Facts about the collector deployment, not query semantics or data arrival.
#[derive(Debug, Clone, Default)]
pub struct DeploymentOptions {
    pub sketch_type_override: Option<SketchType>,
    pub query_id: Option<crate::types_v2::QueryId>,
    pub deployment_model: Option<String>,
    pub retained_labels: Vec<String>,
    pub bytes_per_raw_sample: u32,
    pub distinct_keys_per_window: Option<u64>,
    pub memory_budget_bytes: Option<u64>,
}

/// The registry owns one canonical query; stage-specific metadata is derived on demand.
#[derive(Debug, Clone)]
pub struct RegisteredWorkload {
    workload: QueryWorkload,
    pub deployment: DeploymentOptions,
}

impl RegisteredWorkload {
    pub fn new(workload: QueryWorkload, deployment: DeploymentOptions) -> anyhow::Result<Self> {
        workload
            .validate()
            .map_err(|e| anyhow!("invalid workload: {e:?}"))?;
        ensure!(
            workload.language == QueryLanguage::PromQL,
            "metric registration requires PromQL"
        );
        ensure!(
            workload.entries().count() == 1,
            "metric registration requires exactly one query entry"
        );
        let registered = Self {
            workload,
            deployment,
        };
        let entry = registered.entry();
        query_parser::parse_query_expr_canonical(&entry.query.0, registered.accuracy())?;
        let range = metric_query_range(&entry.query.0)?;
        ensure!(matches!(entry.recurrence, QueryRecurrence::OneTime { invocations: 1, execute_at: None } | QueryRecurrence::Repeated(RepeatedDemand::FixedInterval(_))), "metric registration supports one invocation or a fixed interval without an evaluation phase");
        ensure!(
            entry.time_selection.as_of.is_none(),
            "metric registration does not support as_of"
        );
        ensure!(
            matches!(
                entry.time_selection.scope,
                QueryTimeScope::RealTime | QueryTimeScope::Unknown
            ),
            "metric registration requires real-time selection"
        );
        ensure!(
            entry
                .time_selection
                .lookback
                .is_none_or(|d| range.is_some_and(|r| u128::from(d.0) == r.as_millis())),
            "lookback conflicts with the query range"
        );
        if let LatencyRequirement::ExplicitMaxMs(ms) = entry.requirements.response_latency {
            ensure!(
                Duration::try_from_secs_f64(ms / 1000.0).is_ok(),
                "latency exceeds supported duration"
            );
        }
        Ok(registered)
    }

    pub fn workload(&self) -> &QueryWorkload {
        &self.workload
    }
    pub fn entry(&self) -> QueryWorkloadEntry {
        self.workload
            .entries()
            .next()
            .expect("validated single query")
    }
    pub fn parsed(&self) -> ParsedQuery {
        use planner_types::pre_asap::{AggIntent, QueryExpr};
        let query = self.entry().query.0;
        let expr = query_parser::parse_query_expr_canonical(&query, self.accuracy())
            .expect("validated canonical query");
        let mut parsed = query_parser::qe_to_parsed_query(&expr);
        // Temporal count is the collector's per-item frequency operation.
        // Classify the canonical tree, so formatting cannot change this choice.
        if matches!(&expr, QueryExpr::Aggregate { measures, child, .. }
            if matches!(measures.as_slice(), [AggIntent::Count { .. }])
                && matches!(child.as_ref(), QueryExpr::TimeRange { .. }))
        {
            parsed.aggregations = vec![AggType::Frequency];
            parsed.exact_required = false;
        }
        parsed
    }
    pub fn metric_name(&self) -> String {
        self.parsed().metric_name
    }
    pub fn label_filters(&self) -> HashMap<String, String> {
        self.parsed().label_filters
    }
    pub fn group_by_labels(&self) -> Vec<String> {
        let parsed = self.parsed();
        let mut labels = parsed.group_by_labels;
        for label in &self.deployment.retained_labels {
            if !labels.contains(label) {
                labels.push(label.clone());
            }
        }
        let mut filter_labels: Vec<_> = parsed.label_filters.into_keys().collect();
        filter_labels.sort();
        for label in filter_labels {
            if !labels.contains(&label) {
                labels.push(label);
            }
        }
        labels
    }
    pub fn aggregations(&self) -> Vec<AggType> {
        self.parsed().aggregations
    }
    pub fn time_window(&self) -> Duration {
        self.parsed().time_window
    }
    pub fn quantiles(&self) -> Vec<f64> {
        self.parsed().quantiles
    }
    pub fn exact_required(&self) -> bool {
        self.parsed().exact_required
    }
    pub fn accuracy(&self) -> AccuracyTarget {
        self.entry().requirements.accuracy.target()
    }
    pub fn error_bound(&self) -> f64 {
        match self.accuracy() {
            AccuracyTarget::Exact => 0.0,
            AccuracyTarget::Epsilon(e) | AccuracyTarget::EpsilonDelta { epsilon: e, .. } => e,
        }
    }
    pub fn repeat_every(&self) -> Option<Duration> {
        match self.entry().recurrence {
            QueryRecurrence::Repeated(
                RepeatedDemand::FixedInterval(i)
                | RepeatedDemand::FixedIntervalAt { interval: i, .. },
            ) => Some(Duration::from_millis(u64::from(i.0))),
            _ => None,
        }
    }
    pub fn latency_sla(&self) -> Option<Duration> {
        match self.entry().requirements.response_latency {
            LatencyRequirement::ExplicitMaxMs(ms) => Some(Duration::from_secs_f64(ms / 1000.0)),
            LatencyRequirement::Unspecified => None,
        }
    }
    /// Cost formulas require fresh evidence. Missing facts remain unavailable.
    pub fn characteristics_at(&self, now_ms: u64) -> Option<WorkloadCharacteristics> {
        if self.deployment.bytes_per_raw_sample == 0 {
            return None;
        }
        let data = self.workload.data_workload.as_ref()?;
        let series = *data.input_cardinality.value_at(now_ms)?;
        let rate = data.ingestion_rate.value_at(now_ms)?.0;
        if series == 0 && rate > 0.0 {
            return None;
        }
        let distribution = data.distribution.value_at(now_ms)?;
        Some(WorkloadCharacteristics {
            series_count: series,
            samples_per_sec_per_series: if series == 0 {
                0.0
            } else {
                rate / series as f64
            },
            bytes_per_raw_sample: self.deployment.bytes_per_raw_sample,
            distinct_keys_per_window: self.deployment.distinct_keys_per_window,
            memory_budget_bytes: self.deployment.memory_budget_bytes,
            data_distribution: match distribution {
                DataDistribution::Zipf => crate::types::DataDistribution::Zipf,
                DataDistribution::Uniform => crate::types::DataDistribution::Uniform,
                DataDistribution::Bursty => crate::types::DataDistribution::Bursty,
            },
        })
    }
}

pub(crate) fn metric_query_range(query: &str) -> anyhow::Result<Option<Duration>> {
    use promql_parser::{
        label::MatchOp,
        parser::{Expr, LabelModifier},
        util::{walk_expr, ExprVisitor},
    };
    struct Validator {
        selectors: usize,
        range: Option<Duration>,
    }
    impl ExprVisitor for Validator {
        type Error = anyhow::Error;
        fn pre_visit(&mut self, expr: &Expr) -> anyhow::Result<bool> {
            let selector = match expr {
                Expr::VectorSelector(v) => Some(v),
                Expr::MatrixSelector(m) => {
                    self.range = Some(m.range);
                    Some(&m.vs)
                }
                Expr::Subquery(_) => {
                    anyhow::bail!("metric registration does not support subqueries")
                }
                Expr::Aggregate(a) if matches!(a.modifier, Some(LabelModifier::Exclude(_))) => {
                    anyhow::bail!("metric registration does not support without grouping")
                }
                _ => None,
            };
            if let Some(v) = selector {
                self.selectors += 1;
                ensure!(
                    v.offset.is_none() && v.at.is_none(),
                    "metric registration does not support offset or @ modifiers"
                );
                ensure!(
                    v.matchers.or_matchers.is_empty()
                        && v.matchers
                            .matchers
                            .iter()
                            .all(|m| matches!(m.op, MatchOp::Equal)),
                    "metric registration supports equality label filters only"
                );
            }
            Ok(true)
        }
    }
    let expr = promql_parser::parser::parse(query).map_err(|e| anyhow!(e))?;
    let mut validator = Validator {
        selectors: 0,
        range: None,
    };
    walk_expr(&mut validator, &expr)?;
    ensure!(validator.selectors == 1, "metric registration requires exactly one selector; use the full query compilation API for multi-source expressions");
    Ok(validator.range)
}

pub(crate) fn declared<T>(value: T) -> Evidence<T> {
    Evidence {
        value: Some(value),
        source: EvidenceSource::Declared,
        observed_at_ms: None,
        valid_for_ms: None,
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    /// Test inputs are rendered into canonical expressions before entering a planner.
    pub struct WorkloadFixture {
        pub metric_name: String,
        pub label_filters: HashMap<String, String>,
        pub group_by_labels: Vec<String>,
        pub aggregations: Vec<AggType>,
        pub time_window: Duration,
        pub repeat_every: Option<Duration>,
        pub accuracy: AccuracyTarget,
        pub latency_sla: Option<Duration>,
        pub sketch_type_override: Option<SketchType>,
        pub exact_required: bool,
        pub quantiles: Vec<f64>,
    }
    impl WorkloadFixture {
        pub fn build(self) -> RegisteredWorkload {
            let filters = self
                .label_filters
                .iter()
                .map(|(k, v)| format!("{k}={}", serde_json::to_string(v).unwrap()))
                .collect::<Vec<_>>()
                .join(",");
            let selector = format!(
                "{}{{{filters}}}[{}s]",
                self.metric_name,
                self.time_window.as_secs()
            );
            let query = if self.exact_required {
                format!("sum_over_time({selector})")
            } else {
                self.aggregations
                    .iter()
                    .map(|agg| match agg {
                        AggType::Quantile => format!(
                            "quantile_over_time({}, {selector})",
                            self.quantiles.first().copied().unwrap_or(0.99)
                        ),
                        AggType::Cardinality => format!("distinct_over_time({selector})"),
                        AggType::Frequency => format!("count_over_time({selector})"),
                    })
                    .collect::<Vec<_>>()
                    .join(" + ")
            };
            let requirements = QueryRequirements {
                accuracy: AccuracyRequirement::Explicit(self.accuracy),
                response_latency: self
                    .latency_sla
                    .map(|d| LatencyRequirement::ExplicitMaxMs(d.as_secs_f64() * 1000.0))
                    .unwrap_or(LatencyRequirement::Unspecified),
            };
            let time_selection = TimeSelection {
                scope: QueryTimeScope::RealTime,
                lookback: Some(DurationMs(self.time_window.as_millis() as u64)),
                as_of: None,
            };
            let (query_batch, repeating_queries) = match self.repeat_every {
                Some(d) => (
                    None,
                    Some(vec![RepeatingEntry {
                        query: Query(query),
                        requirements,
                        predictability: Predictability::Unknown,
                        time_selection,
                        demand: RepeatedDemand::FixedInterval(RepetitionInterval(
                            d.as_millis().try_into().unwrap(),
                        )),
                    }]),
                ),
                None => (
                    Some(vec![BatchEntry {
                        query: Query(query),
                        requirements,
                        predictability: Predictability::Unknown,
                        time_selection,
                        invocations: 1,
                        execute_at: None,
                    }]),
                    None,
                ),
            };
            RegisteredWorkload::new(
                QueryWorkload {
                    language: QueryLanguage::PromQL,
                    query_batch,
                    repeating_queries,
                    data_workload: None,
                },
                DeploymentOptions {
                    sketch_type_override: self.sketch_type_override,
                    retained_labels: self.group_by_labels,
                    ..Default::default()
                },
            )
            .unwrap()
        }
    }
    impl RegisteredWorkload {
        pub fn set_repeat_every(&mut self, cadence: Option<Duration>) {
            let entry = self.entry();
            self.workload.query_batch = None;
            self.workload.repeating_queries = None;
            match cadence {
                Some(d) => {
                    self.workload.repeating_queries = Some(vec![RepeatingEntry {
                        query: entry.query,
                        requirements: entry.requirements,
                        predictability: entry.predictability,
                        time_selection: entry.time_selection,
                        demand: RepeatedDemand::FixedInterval(RepetitionInterval(
                            d.as_millis().try_into().unwrap(),
                        )),
                    }])
                }
                None => {
                    self.workload.query_batch = Some(vec![BatchEntry {
                        query: entry.query,
                        requirements: entry.requirements,
                        predictability: entry.predictability,
                        time_selection: entry.time_selection,
                        invocations: 1,
                        execute_at: None,
                    }])
                }
            }
        }
        pub fn set_label_filters(&mut self, filters: HashMap<String, String>) {
            let rendered = filters
                .iter()
                .map(|(k, v)| format!("{k}={}", serde_json::to_string(v).unwrap()))
                .collect::<Vec<_>>()
                .join(",");
            if let Some(batch) = &mut self.workload.query_batch {
                batch[0].query.0 = batch[0].query.0.replace("{}", &format!("{{{rendered}}}"));
            }
            if let Some(repeating) = &mut self.workload.repeating_queries {
                repeating[0].query.0 = repeating[0]
                    .query
                    .0
                    .replace("{}", &format!("{{{rendered}}}"));
            }
        }
        pub fn set_accuracy(&mut self, accuracy: AccuracyTarget) {
            if let Some(batch) = &mut self.workload.query_batch {
                batch[0].requirements.accuracy = AccuracyRequirement::Explicit(accuracy.clone());
            }
            if let Some(repeating) = &mut self.workload.repeating_queries {
                repeating[0].requirements.accuracy = AccuracyRequirement::Explicit(accuracy);
            }
        }
        pub fn set_latency_sla(&mut self, latency: Option<Duration>) {
            let latency = latency
                .map(|d| LatencyRequirement::ExplicitMaxMs(d.as_secs_f64() * 1000.0))
                .unwrap_or(LatencyRequirement::Unspecified);
            if let Some(batch) = &mut self.workload.query_batch {
                batch[0].requirements.response_latency = latency;
            }
            if let Some(repeating) = &mut self.workload.repeating_queries {
                repeating[0].requirements.response_latency = latency;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pipeline::{Analyzer, QuerySpec},
        store::workload::WorkloadStore,
        workload::AggRole,
    };

    fn spec() -> QuerySpec {
        serde_json::from_value(serde_json::json!({
            "query_string": "quantile_over_time(0.9, latency[5m])",
            "accuracy_sla": 0.5,
            "accuracy": {"EpsilonDelta": {"epsilon": 0.02, "delta": 0.001}},
            "repeat_every": "10s", "latency_sla": "2s"
        }))
        .unwrap()
    }

    /// Registration and retrieval preserve the canonical query, requirements and data evidence.
    #[test]
    fn canonical_workload_survives_registry_roundtrip() {
        let mut input = spec();
        input.workload.series_count = 10;
        input.workload.samples_per_sec_per_series = 5.0;
        input.workload.distinct_keys_per_window = Some(7);
        input.sketch_type = Some(SketchType::KLL);
        let registered = Analyzer::new().analyze(input).unwrap();
        let canonical = registered.workload().clone();
        let data = canonical.data_workload.as_ref().unwrap();
        assert_eq!(data.ingestion_rate.value, Some(Rate(50.0)));
        assert_eq!(data.input_cardinality.value, Some(10));
        assert_eq!(
            canonical.entries().next().unwrap().time_selection.lookback,
            Some(DurationMs(300_000))
        );
        let store = WorkloadStore::new();
        store.set("latency", AggRole::Quantile, registered);
        let retrieved = store.get("latency", AggRole::Quantile).unwrap();
        assert_eq!(retrieved.workload(), &canonical);
        assert_eq!(
            retrieved.accuracy(),
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.02,
                delta: 0.001
            }
        );
        assert_eq!(retrieved.repeat_every(), Some(Duration::from_secs(10)));
        assert_eq!(retrieved.latency_sla(), Some(Duration::from_secs(2)));
        assert_eq!(
            retrieved.deployment.sketch_type_override,
            Some(SketchType::KLL)
        );
        let cost = retrieved.characteristics_at(0).unwrap();
        assert_eq!(cost.samples_per_sec_per_series, 5.0);
        assert_eq!(cost.distinct_keys_per_window, Some(7));
    }

    /// Missing, stale or inconsistent evidence cannot become a fabricated rate estimate.
    #[test]
    fn unavailable_data_evidence_stays_unavailable() {
        let original = Analyzer::new().analyze(spec()).unwrap();
        let mut canonical = original.workload().clone();
        let data = canonical.data_workload.as_mut().unwrap();
        data.ingestion_rate = Evidence {
            value: Some(Rate(50.0)),
            source: EvidenceSource::Observed,
            observed_at_ms: Some(10),
            valid_for_ms: Some(5),
        };
        let registered =
            RegisteredWorkload::new(canonical.clone(), original.deployment.clone()).unwrap();
        assert!(registered.characteristics_at(15).is_some());
        assert!(registered.characteristics_at(16).is_none());
        canonical.data_workload.as_mut().unwrap().ingestion_rate = Evidence::default();
        let unknown =
            RegisteredWorkload::new(canonical.clone(), original.deployment.clone()).unwrap();
        assert!(unknown.characteristics_at(0).is_none());
        let data = canonical.data_workload.as_mut().unwrap();
        data.ingestion_rate = declared(Rate(50.0));
        data.input_cardinality = declared(0);
        let inconsistent = RegisteredWorkload::new(canonical, original.deployment).unwrap();
        assert!(inconsistent.characteristics_at(0).is_none());
    }

    /// Unsupported stage projections fail explicitly, before any deployment is emitted.
    #[test]
    fn rejects_lossy_metric_projections() {
        for query in [
            "quantile_over_time(0.9, latency{job!=\"api\"}[5m])",
            "quantile_over_time(0.9, latency{job=~\"api.*\"}[5m])",
            "sum(a) / sum(b)",
            "sum_over_time(latency[5m] offset 1h)",
            "sum without(instance)(latency)",
            "avg_over_time(latency[10m:1m])",
        ] {
            let mut input = spec();
            input.query_string = Some(query.into());
            assert!(
                Analyzer::new().analyze(input).is_err(),
                "must reject {query}"
            );
        }
    }

    /// Cadence and data arrival are independent, and millisecond cadence conversion is checked.
    #[test]
    fn recurrence_is_separate_from_arrival() {
        let mut input = spec();
        input.repeat_every = None;
        input.data = crate::types_v2::DataShape::Batch;
        let once = Analyzer::new().analyze(input.clone()).unwrap();
        assert!(matches!(
            once.entry().recurrence,
            QueryRecurrence::OneTime { .. }
        ));
        assert_eq!(
            once.workload().data_workload.as_ref().unwrap().arrival,
            DataArrival::AtRest
        );
        input.repeat_every = Some("10s".into());
        let repeating = Analyzer::new().analyze(input.clone()).unwrap();
        assert_eq!(repeating.repeat_every(), Some(Duration::from_secs(10)));
        assert_eq!(
            repeating.workload().data_workload.as_ref().unwrap().arrival,
            DataArrival::AtRest
        );
        for cadence in ["0s", "4294968s"] {
            input.repeat_every = Some(cadence.into());
            assert!(Analyzer::new().analyze(input.clone()).is_err());
        }
    }

    /// Public canonical inputs cannot silently discard known time-selection facts.
    #[test]
    fn rejects_conflicting_canonical_time_selection() {
        let original = Analyzer::new().analyze(spec()).unwrap();
        let mut canonical = original.workload().clone();
        canonical.repeating_queries.as_mut().unwrap()[0]
            .time_selection
            .lookback = Some(DurationMs(600_000));
        assert!(RegisteredWorkload::new(canonical, original.deployment).is_err());
    }
}
