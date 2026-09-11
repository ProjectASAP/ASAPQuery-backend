//! Deployment adapter for ASAPPlanner Error–Resource Profiles.

use asap_aware_mapping::erp::{
    AccuracyMode, ErpArtifact, ErpMultiFitSelectionRequest, ErpSelectionRequest, ErpShapeFit,
    ErpShapeObservation,
};
use planner_types::post_asap::{SketchAlgorithm, SketchParams};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ErpObservedShape {
    pub observation: ErpShapeObservation,
    /// Largest interval rate divided by the median non-zero interval rate.
    pub burst_ratio: f64,
}

/// Bounded online observer fed by the same keys that enter a summary. It keeps
/// exact sampled frequencies up to a configured cap and fails closed beyond it
/// instead of silently under-reporting cardinality.
#[derive(Debug)]
pub struct ErpShapeObserver {
    frequencies: HashMap<String, u64>,
    interval_updates: HashMap<usize, u64>,
    max_observed_keys: usize,
    max_observed_intervals: usize,
    updates: u64,
    invalid: bool,
}

impl ErpShapeObserver {
    pub fn new(max_observed_keys: usize) -> Result<Self, &'static str> {
        Self::with_limits(max_observed_keys, 256)
    }

    pub fn with_limits(
        max_observed_keys: usize,
        max_observed_intervals: usize,
    ) -> Result<Self, &'static str> {
        if max_observed_keys == 0 || max_observed_intervals == 0 {
            return Err("observation limits must be positive");
        }
        Ok(Self {
            frequencies: HashMap::new(),
            interval_updates: HashMap::new(),
            max_observed_keys,
            max_observed_intervals,
            updates: 0,
            invalid: false,
        })
    }

    pub fn observe(&mut self, key: &str, interval: usize) -> Result<(), &'static str> {
        if self.invalid {
            return Err("ERP observation was invalidated; start a fresh observation window");
        }
        if key.len() > 4096 {
            self.invalid = true;
            return Err("ERP shape observer key size exceeded");
        }
        if !self.frequencies.contains_key(key) && self.frequencies.len() == self.max_observed_keys {
            self.invalid = true;
            return Err("ERP shape observer cardinality cap exceeded");
        }
        if !self.interval_updates.contains_key(&interval)
            && self.interval_updates.len() == self.max_observed_intervals
        {
            self.invalid = true;
            return Err("ERP shape observer interval cap exceeded");
        }
        let Some(updates) = self.updates.checked_add(1) else {
            self.invalid = true;
            return Err("ERP shape observer count overflow");
        };
        *self.frequencies.entry(key.to_owned()).or_default() += 1;
        *self.interval_updates.entry(interval).or_default() += 1;
        self.updates = updates;
        Ok(())
    }

    pub fn snapshot(&self) -> Option<ErpObservedShape> {
        if self.frequencies.is_empty() || self.invalid {
            return None;
        }
        let mut counts: Vec<_> = self.frequencies.values().copied().collect();
        counts.sort_unstable_by(|left, right| right.cmp(left));
        let exponent = fit_zipf_exponent(&counts);
        let fits = [("uniform", 0.0), ("zipf", exponent)]
            .into_iter()
            // Zipf(0) is exactly uniform; duplicate models are not ambiguity.
            .filter(|(family, _)| *family != "zipf" || counts.first() != counts.last())
            .map(|(family, slope)| {
                let expected: Vec<_> = (1..=counts.len())
                    .map(|rank| (rank as f64).powf(-slope))
                    .collect();
                let total: f64 = expected.iter().sum();
                let distance = counts
                    .iter()
                    .zip(expected)
                    .map(|(count, expected)| {
                        (*count as f64 / self.updates as f64 - expected / total).abs()
                    })
                    .sum::<f64>()
                    / 2.0;
                ErpShapeFit {
                    family: family.into(),
                    parameters: if family == "zipf" {
                        BTreeMap::from([("exponent".into(), slope)])
                    } else {
                        BTreeMap::new()
                    },
                    goodness_of_fit: distance,
                    // A fit-quality score, not an estimator tail-probability claim.
                    confidence: (1.0 - distance) * (1.0 - 1.0 / (self.updates as f64).sqrt()),
                }
            })
            .collect();
        let mut nonzero: Vec<_> = self
            .interval_updates
            .values()
            .copied()
            .filter(|count| *count > 0)
            .collect();
        nonzero.sort_unstable();
        let median = nonzero.get(nonzero.len() / 2).copied().unwrap_or(1);
        let peak = nonzero.last().copied().unwrap_or(median);
        Some(ErpObservedShape {
            observation: ErpShapeObservation {
                cardinality: self.frequencies.len() as u64,
                observed_events: self.updates,
                fits,
                empirical_fingerprint: None,
            },
            burst_ratio: peak as f64 / median as f64,
        })
    }
}

fn fit_zipf_exponent(descending_counts: &[u64]) -> f64 {
    if descending_counts.len() < 2 {
        return 0.0;
    }
    let points: Vec<_> = descending_counts
        .iter()
        .enumerate()
        .filter(|(_, count)| **count > 0)
        .map(|(index, count)| (((index + 1) as f64).ln(), (*count as f64).ln()))
        .collect();
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / points.len() as f64;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / points.len() as f64;
    let covariance = points
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let variance = points
        .iter()
        .map(|(x, _)| (x - mean_x).powi(2))
        .sum::<f64>();
    if variance == 0.0 {
        0.0
    } else {
        (-covariance / variance).max(0.0)
    }
}

/// The measurement units are part of the readout contract, not a property of
/// the shared sketch state. These keys must be supplied by the benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadoutEvidence {
    QuantileRank,
    DistinctRelative,
    FrequencyL2Relative,
    EntropyAbsoluteBits,
}

impl ReadoutEvidence {
    pub(crate) fn for_intent(
        algorithm: &SketchAlgorithm,
        intent: &planner_types::pre_asap::AggIntent,
    ) -> Option<Self> {
        use planner_types::pre_asap::AggIntent;
        match (algorithm, intent) {
            (SketchAlgorithm::Kll, AggIntent::Quantile { .. }) => Some(Self::QuantileRank),
            (SketchAlgorithm::Hll | SketchAlgorithm::UnivMon, AggIntent::Cardinality { .. }) => {
                Some(Self::DistinctRelative)
            }
            (SketchAlgorithm::UnivMon, AggIntent::FrequencyL2 { .. }) => {
                Some(Self::FrequencyL2Relative)
            }
            (SketchAlgorithm::UnivMon, AggIntent::FrequencyEntropy { .. }) => {
                Some(Self::EntropyAbsoluteBits)
            }
            _ => None,
        }
    }

    pub(crate) fn for_query(
        algorithm: &SketchAlgorithm,
        query: &planner_types::post_asap::SketchQuery,
    ) -> Option<Self> {
        use planner_types::post_asap::SketchQuery;
        match (algorithm, query) {
            (SketchAlgorithm::Kll, SketchQuery::Quantile { .. }) => Some(Self::QuantileRank),
            (SketchAlgorithm::Hll | SketchAlgorithm::UnivMon, SketchQuery::Cardinality) => {
                Some(Self::DistinctRelative)
            }
            (SketchAlgorithm::UnivMon, SketchQuery::FrequencyL2) => Some(Self::FrequencyL2Relative),
            (SketchAlgorithm::UnivMon, SketchQuery::FrequencyEntropy) => {
                Some(Self::EntropyAbsoluteBits)
            }
            _ => None,
        }
    }

    fn metric_key(self) -> &'static str {
        match self {
            Self::QuantileRank => "max_rank_err",
            Self::DistinctRelative => "max_cardinality_relative_error",
            Self::FrequencyL2Relative => "max_frequency_l2_relative_error",
            Self::EntropyAbsoluteBits => "max_frequency_entropy_absolute_bits_error",
        }
    }

    fn metric(self) -> planner_types::post_asap::ErrorMetric {
        use planner_types::post_asap::ErrorMetric;
        match self {
            Self::QuantileRank => ErrorMetric::Rank,
            Self::DistinctRelative => ErrorMetric::Cardinality,
            Self::FrequencyL2Relative => ErrorMetric::RelativeValue,
            Self::EntropyAbsoluteBits => ErrorMetric::AbsoluteValue,
        }
    }
}

/// ERP v1 measures error magnitudes, not tail probabilities. Only an explicit
/// epsilon-only request may use these observations as its accuracy contract.
pub(crate) struct ErpAccuracyModel<'a> {
    pub policy: Option<&'a ErpPlanningInput>,
    pub max_error: f64,
}

impl asap_aware_mapping::AccuracyModel for ErpAccuracyModel<'_> {
    fn exact_operation_rule(
        &self,
        operation: &planner_types::post_asap::ExactOperation,
    ) -> Option<planner_types::post_asap::CompositionOperator> {
        asap_aware_mapping::DefaultAccuracyModel.exact_operation_rule(operation)
    }
    fn local_guarantee(
        &self,
        family: &planner_types::post_asap::SummaryFamilyType,
        query: &planner_types::post_asap::SketchQuery,
    ) -> Option<planner_types::post_asap::ResultGuarantee> {
        use planner_types::post_asap::*;
        let theoretical = asap_aware_mapping::DefaultAccuracyModel.local_guarantee(family, query);
        let (Some(policy), SummaryFamilyType::Sketch(kind, _)) = (self.policy, family) else {
            return theoretical;
        };
        let Some(readout) = ReadoutEvidence::for_query(kind.algorithm(), query) else {
            // In particular, UnivMon's total unit count is exact without
            // empirical error evidence. No unsupported readout gets a bound.
            if theoretical.as_ref().is_some_and(ResultGuarantee::is_exact) {
                return theoretical;
            }
            return match policy.select(
                kind.algorithm().clone(),
                self.max_error,
                kind.params().clone(),
            ) {
                ErpParameterDecision::ExactFallback { .. } => None,
                _ => theoretical,
            };
        };
        match policy.evidence_for_readout(
            kind.algorithm().clone(),
            readout,
            self.max_error,
            kind.params(),
        ) {
            ErpParameterDecision::ExactFallback { .. } => None,
            ErpParameterDecision::TheoreticalFallback { .. } => theoretical,
            ErpParameterDecision::Empirical {
                params,
                record_id,
                observed_error,
                ..
            } => {
                if &params != kind.params() {
                    return None;
                }
                Some(ResultGuarantee {
                    metric: readout.metric(),
                    bound: BoundExpr::Constant {
                        value: observed_error,
                    },
                    failure_probability: ProbabilityExpr::Unknown {
                        statistic: "erp_v1_has_no_failure_probability_evidence".into(),
                    },
                    provenance: vec![GuaranteeSource::SketchReadout {
                        algorithm: format!("{:?}", kind.algorithm()),
                        contract: format!(
                            "erp_v1_empirical:{}:{}:{}",
                            policy.artifact.producer_version,
                            record_id,
                            readout.metric_key()
                        ),
                        params: serde_json::to_value(&params).ok()?,
                        query: format!("{query:?}"),
                    }],
                })
            }
        }
    }

    fn propagate(
        &self,
        op: &planner_types::post_asap::CompositionOperator,
        inputs: &[planner_types::post_asap::ResultGuarantee],
        local: Option<&planner_types::post_asap::ResultGuarantee>,
        stats: &asap_aware_mapping::PropagationStats,
    ) -> Result<planner_types::post_asap::ResultGuarantee, planner_types::post_asap::AccuracyError>
    {
        asap_aware_mapping::DefaultAccuracyModel.propagate(op, inputs, local, stats)
    }

    fn satisfies(
        &self,
        guarantee: &planner_types::post_asap::ResultGuarantee,
        target: &crate::types_v2::AccuracyTarget,
    ) -> bool {
        asap_aware_mapping::DefaultAccuracyModel.satisfies(guarantee, target)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErpAccuracyMode {
    Empirical,
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ErpPlanningInput {
    pub artifact: ErpArtifact,
    /// Current deployment distribution descriptor. ERP v1 requires exact
    /// equality with the benchmark descriptor, so drift fails closed.
    pub distribution: Value,
    pub implementation: Option<String>,
    pub error_metric: String,
    pub min_trials: u32,
    pub expected_updates: f64,
    pub expected_queries: f64,
    pub expected_merges: f64,
    pub retention_seconds: f64,
    pub cpu_weight: f64,
    pub byte_second_weight: f64,
    pub mode: ErpAccuracyMode,
    /// Runtime-observed shape. If present, exact descriptor equality is
    /// replaced by bounded nearest-profile matching.
    #[serde(default)]
    pub observed_shape: Option<ErpShapeObservation>,
    #[serde(default)]
    pub observed_populations:
        Option<asap_types::erp_observation::ErpPopulationObservations<ErpObservedShape>>,
    /// Runtime-samples ring key from which the backend resolves the freshest
    /// `erp_observed_shape` payload before compiling a plan.
    #[serde(default)]
    pub observed_shape_source: Option<ErpObservedShapeSource>,
    #[serde(default)]
    pub shape_match: Option<ErpShapeMatchPolicy>,
    #[serde(default)]
    pub runtime: ErpRuntimeCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ErpObservedShapeSource {
    pub source: String,
    pub sketch: String,
    pub implementation: String,
}

impl ErpPlanningInput {
    pub fn hydrate_observed_shape(
        &mut self,
        samples: &crate::runtime_samples::RuntimeSamplesStore,
    ) -> Result<(), String> {
        if self.observed_shape.is_some() {
            return Ok(());
        }
        let Some(source) = &self.observed_shape_source else {
            return Ok(());
        };
        let key = crate::runtime_samples::SampleKey {
            source: source.source.clone(),
            sketch: source.sketch.clone(),
            impl_name: source.implementation.clone(),
        };
        let record = samples.latest(&key).ok_or_else(|| {
            format!(
                "no runtime shape sample for {}/{}/{}",
                source.source, source.sketch, source.implementation
            )
        })?;
        let value = record.payload.get("erp_observed_shape").ok_or_else(|| {
            format!(
                "latest runtime sample for {}/{}/{} has no erp_observed_shape",
                source.source, source.sketch, source.implementation
            )
        })?;
        let observed: ErpObservedShape = serde_json::from_value(value.clone())
            .map_err(|error| format!("invalid erp_observed_shape: {error}"))?;
        self.observed_shape = Some(observed.observation);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ErpShapeMatchPolicy {
    pub minimum_benchmark_events: u64,
    pub max_log2_cardinality_distance: f64,
    pub max_parameter_distance: f64,
    pub max_goodness_of_fit: f64,
    pub minimum_confidence: f64,
    pub minimum_confidence_margin: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ErpRuntimeCapabilities {
    /// Empty means the normal backend catalog is authoritative. A non-empty
    /// list restricts ERP/theoretical materialization to these algorithms.
    #[serde(default)]
    pub allowed_algorithms: Vec<SketchAlgorithm>,
    pub max_memory_bytes: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErpParameterDecision {
    Empirical {
        params: SketchParams,
        record_id: String,
        observed_error: f64,
        estimated_cost: f64,
    },
    TheoreticalFallback {
        params: SketchParams,
        reason: String,
    },
    ExactFallback {
        reason: String,
    },
}

impl ErpParameterDecision {
    pub fn params(&self) -> Option<&SketchParams> {
        match self {
            Self::Empirical { params, .. } | Self::TheoreticalFallback { params, .. } => {
                Some(params)
            }
            Self::ExactFallback { .. } => None,
        }
    }
}

impl ErpPlanningInput {
    pub fn select(
        &self,
        algorithm: SketchAlgorithm,
        max_error: f64,
        theoretical: SketchParams,
    ) -> ErpParameterDecision {
        self.select_metric(algorithm, &self.error_metric, max_error, theoretical, None)
    }

    pub(crate) fn select_readout(
        &self,
        algorithm: SketchAlgorithm,
        readout: ReadoutEvidence,
        max_error: f64,
        theoretical: SketchParams,
    ) -> ErpParameterDecision {
        self.select_metric(
            algorithm,
            readout.metric_key(),
            max_error,
            theoretical,
            None,
        )
    }

    /// Validate an existing state's readout independently of which new state
    /// would be cheapest to allocate for this one consumer.
    fn evidence_for_readout(
        &self,
        algorithm: SketchAlgorithm,
        readout: ReadoutEvidence,
        max_error: f64,
        actual: &SketchParams,
    ) -> ErpParameterDecision {
        self.select_metric(
            algorithm,
            readout.metric_key(),
            max_error,
            actual.clone(),
            Some(actual),
        )
    }

    fn select_metric(
        &self,
        algorithm: SketchAlgorithm,
        error_metric: &str,
        max_error: f64,
        theoretical: SketchParams,
        required_params: Option<&SketchParams>,
    ) -> ErpParameterDecision {
        if let Some(populations) = &self.observed_populations {
            // Every installed partition must support the same configuration.
            // Never aggregate their frequency maps into a fictitious population.
            let mut best: Option<ErpParameterDecision> = None;
            if populations.invalid_reason.is_none() && !populations.populations.is_empty() {
                for row in &self.artifact.records {
                    if !sketch_name_matches(&row.sketch, &algorithm) {
                        continue;
                    }
                    let Some(params) = parse_params(&algorithm, &row.parameters) else {
                        continue;
                    };
                    if required_params.is_some_and(|required| required != &params) {
                        continue;
                    }
                    let mut policy = self.clone();
                    policy.observed_populations = None;
                    policy.mode = ErpAccuracyMode::Empirical;
                    let mut errors: f64 = 0.0;
                    let mut cost = 0.0;
                    let mut evidence = Vec::new();
                    let valid = populations.populations.iter().all(|population| {
                        policy.observed_shape = Some(population.shape.observation.clone());
                        match policy.select_metric(
                            algorithm.clone(),
                            error_metric,
                            max_error,
                            theoretical.clone(),
                            Some(&params),
                        ) {
                            ErpParameterDecision::Empirical {
                                record_id,
                                observed_error,
                                estimated_cost,
                                ..
                            } => {
                                errors = errors.max(observed_error);
                                cost += estimated_cost;
                                evidence.push(record_id);
                                true
                            }
                            _ => false,
                        }
                    });
                    if valid && cost.is_finite() && best.as_ref().is_none_or(|previous| {
                        matches!(previous, ErpParameterDecision::Empirical { estimated_cost, .. } if cost < *estimated_cost)
                    }) {
                        best = Some(ErpParameterDecision::Empirical {
                            params, record_id: serde_json::to_string(&evidence).expect("string IDs serialize"),
                            observed_error: errors, estimated_cost: cost,
                        });
                    }
                }
            }
            return best.unwrap_or_else(|| {
                let reason =
                    "ERP has no configuration valid for every observed population".to_owned();
                if self.mode == ErpAccuracyMode::Hybrid
                    && self.runtime.supports(&algorithm, &theoretical, None)
                {
                    ErpParameterDecision::TheoreticalFallback {
                        params: theoretical,
                        reason,
                    }
                } else {
                    ErpParameterDecision::ExactFallback { reason }
                }
            });
        }
        // Runtime admissibility belongs before ranking: an unusable cheap
        // profile must not hide a more expensive executable alternative.
        let mut artifact = self.artifact.clone();
        artifact.records.retain(|row| {
            sketch_name_matches(&row.sketch, &algorithm)
                && parse_params(&algorithm, &row.parameters).is_some_and(|params| {
                    required_params.is_none_or(|required| required == &params)
                        && self.runtime.supports(
                            &algorithm,
                            &params,
                            Some(row.resources.memory_bytes),
                        )
                })
        });
        let allowed_sketches = artifact
            .records
            .iter()
            .map(|row| row.sketch.clone())
            .collect();
        let request = ErpSelectionRequest {
            distribution: self.distribution.clone(),
            implementation: self.implementation.clone(),
            allowed_sketches,
            error_metric: error_metric.to_owned(),
            max_error,
            min_trials: self.min_trials,
            expected_updates: self.expected_updates,
            expected_queries: self.expected_queries,
            expected_merges: self.expected_merges,
            retention_seconds: self.retention_seconds,
            cpu_weight: self.cpu_weight,
            byte_second_weight: self.byte_second_weight,
            mode: match self.mode {
                ErpAccuracyMode::Empirical => AccuracyMode::Empirical,
                ErpAccuracyMode::Hybrid => AccuracyMode::Hybrid,
            },
        };
        let empirical = if request.allowed_sketches.is_empty() {
            Err(asap_aware_mapping::erp::ErpError::NoApplicableConfiguration)
        } else {
            let selected = match (&self.observed_shape, self.shape_match) {
                (Some(observed), Some(policy)) => {
                    artifact.select_multi_fit(&ErpMultiFitSelectionRequest {
                        selection: request.clone(),
                        observed: observed.clone(),
                        minimum_benchmark_events: policy.minimum_benchmark_events,
                        max_log2_cardinality_distance: policy.max_log2_cardinality_distance,
                        max_parameter_distance: policy.max_parameter_distance,
                        max_goodness_of_fit: policy.max_goodness_of_fit,
                        minimum_confidence: policy.minimum_confidence,
                        minimum_confidence_margin: policy.minimum_confidence_margin,
                    })
                }
                (None, None) => artifact.select(&request),
                _ => Err(asap_aware_mapping::erp::ErpError::Invalid(
                    "observed_shape and shape_match must be supplied together",
                )),
            };
            selected.and_then(|selected| {
                parse_params(&algorithm, &selected.record.parameters)
                    .filter(|params| {
                        self.runtime.supports(
                            &algorithm,
                            params,
                            Some(selected.record.resources.memory_bytes),
                        )
                    })
                    .map(|params| (selected, params))
                    .ok_or(asap_aware_mapping::erp::ErpError::NoApplicableConfiguration)
            })
        };
        match empirical {
            Ok((selected, params)) => ErpParameterDecision::Empirical {
                params,
                record_id: selected.record.id.clone(),
                observed_error: selected.observed_error,
                estimated_cost: selected.estimated_cost,
            },
            Err(error) if self.mode == ErpAccuracyMode::Hybrid => {
                if self.runtime.supports(&algorithm, &theoretical, None) {
                    ErpParameterDecision::TheoreticalFallback {
                        params: theoretical,
                        reason: error.to_string(),
                    }
                } else {
                    ErpParameterDecision::ExactFallback {
                        reason: format!(
                            "ERP unavailable ({error}); theoretical {algorithm:?} is unsupported"
                        ),
                    }
                }
            }
            Err(error) => ErpParameterDecision::ExactFallback {
                reason: format!("empirical ERP selection failed: {error}"),
            },
        }
    }
}

impl ErpRuntimeCapabilities {
    fn supports(
        &self,
        algorithm: &SketchAlgorithm,
        params: &SketchParams,
        measured_memory: Option<f64>,
    ) -> bool {
        if !self.allowed_algorithms.is_empty() && !self.allowed_algorithms.contains(algorithm) {
            return false;
        }
        if let Some(limit) = self.max_memory_bytes {
            let Some(measured) = measured_memory else {
                // A theoretical configuration has no measured byte size in
                // ERP v1. Do not claim it satisfies a deployment byte cap.
                return false;
            };
            if !limit.is_finite() || limit < 0.0 || measured > limit {
                return false;
            }
        }
        valid_runtime_params(algorithm, params)
    }
}

fn normalized(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn sketch_name_matches(name: &str, algorithm: &SketchAlgorithm) -> bool {
    let name = normalized(name);
    match algorithm {
        SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap => {
            name.starts_with("cms") || name.starts_with("countmin")
        }
        SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap => {
            name.starts_with("countsketch")
        }
        SketchAlgorithm::Hll => name.starts_with("hll") || name.starts_with("hyperloglog"),
        SketchAlgorithm::Kll => name.starts_with("kll"),
        SketchAlgorithm::DDSketch => name.starts_with("ddsketch"),
        SketchAlgorithm::UnivMon => name == "univmon",
        _ => false,
    }
}

fn number(parameters: &Value, names: &[&str]) -> Option<f64> {
    names
        .iter()
        .find_map(|name| parameters.get(name).and_then(Value::as_f64))
        .or_else(|| {
            parameters.get("params").and_then(|params| {
                names
                    .iter()
                    .find_map(|name| params.get(name).and_then(Value::as_f64))
            })
        })
}

fn u32_param(parameters: &Value, names: &[&str]) -> Option<u32> {
    let value = number(parameters, names)?;
    (value.is_finite() && value.fract() == 0.0 && value >= 0.0 && value <= u32::MAX as f64)
        .then_some(value as u32)
}

fn parse_params(algorithm: &SketchAlgorithm, parameters: &Value) -> Option<SketchParams> {
    let width = || u32_param(parameters, &["width", "cols"]);
    let depth = || u32_param(parameters, &["depth", "rows"]);
    Some(match algorithm {
        SketchAlgorithm::Cms => SketchParams::Cms {
            width: width()?,
            depth: depth()?,
        },
        SketchAlgorithm::CmsWithHeap => SketchParams::CmsWithHeap {
            width: width()?,
            depth: depth()?,
            heap_size: u32_param(parameters, &["heap_size", "k"])?,
        },
        SketchAlgorithm::CountSketch => SketchParams::CountSketch {
            width: width()?,
            depth: depth()?,
        },
        SketchAlgorithm::CountSketchWithHeap => SketchParams::CountSketchWithHeap {
            width: width()?,
            depth: depth()?,
            heap_size: u32_param(parameters, &["heap_size", "k"])?,
        },
        SketchAlgorithm::Hll => {
            let precision = u32_param(parameters, &["precision", "lg_k", "p"])?;
            SketchParams::Hll {
                precision: u8::try_from(precision).ok()?,
            }
        }
        SketchAlgorithm::Kll => SketchParams::Kll {
            k: u32_param(parameters, &["k"])?,
        },
        SketchAlgorithm::UnivMon => SketchParams::UnivMon {
            heap_size: u32_param(parameters, &["heap_size"])?,
            sketch_rows: u32_param(parameters, &["sketch_rows"])?,
            sketch_cols: u32_param(parameters, &["sketch_cols"])?,
            layers: u8::try_from(u32_param(parameters, &["layers"])?).ok()?,
        },
        SketchAlgorithm::DDSketch => SketchParams::DDSketch {
            alpha: number(parameters, &["alpha", "relative_accuracy"])?,
        },
        _ => return None,
    })
}

fn valid_runtime_params(algorithm: &SketchAlgorithm, params: &SketchParams) -> bool {
    match (algorithm, params) {
        (SketchAlgorithm::Cms, SketchParams::Cms { width, depth })
        | (SketchAlgorithm::CountSketch, SketchParams::CountSketch { width, depth }) => {
            *width >= 2 && width.is_power_of_two() && *depth >= 1
        }
        (
            SketchAlgorithm::CmsWithHeap,
            SketchParams::CmsWithHeap {
                width,
                depth,
                heap_size,
            },
        )
        | (
            SketchAlgorithm::CountSketchWithHeap,
            SketchParams::CountSketchWithHeap {
                width,
                depth,
                heap_size,
            },
        ) => *width >= 2 && width.is_power_of_two() && *depth >= 1 && *heap_size >= 1,
        (
            SketchAlgorithm::UnivMon,
            SketchParams::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            },
        ) => {
            *heap_size > 0
                && *sketch_cols > 0
                && (1..=20).contains(sketch_rows)
                && (1..=64).contains(layers)
                && sketch_rows
                    .checked_mul(*sketch_cols)
                    .and_then(|n| n.checked_mul(u32::from(*layers)))
                    .is_some()
        }
        (SketchAlgorithm::Hll, SketchParams::Hll { precision }) => (4..=18).contains(precision),
        (SketchAlgorithm::Kll, SketchParams::Kll { k }) => (8..=65_535).contains(k),
        (SketchAlgorithm::DDSketch, SketchParams::DDSketch { alpha }) => {
            alpha.is_finite() && (0.0..1.0).contains(alpha)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use asap_aware_mapping::erp::{ErpRecord, ErpResourceProfile, ERP_SCHEMA_VERSION};

    use super::*;

    /// Match the collector and portable-state decoder's supported k range.
    #[test]
    fn kll_runtime_limits_reject_unusable_profiles() {
        for k in [0, 2, 7, 65_536] {
            assert!(!valid_runtime_params(
                &SketchAlgorithm::Kll,
                &SketchParams::Kll { k }
            ));
        }
        for k in [8, 32, 65_535] {
            assert!(valid_runtime_params(
                &SketchAlgorithm::Kll,
                &SketchParams::Kll { k }
            ));
        }
    }

    fn input(mode: ErpAccuracyMode) -> ErpPlanningInput {
        ErpPlanningInput {
            artifact: ErpArtifact {
                schema_version: ERP_SCHEMA_VERSION,
                producer_version: "bench-rev".into(),
                records: vec![ErpRecord {
                    id: "cms-512".into(),
                    sketch: "cms-fastpath-vector2d".into(),
                    implementation: "oxide".into(),
                    parameters: serde_json::json!({"rows": 3, "cols": 512}),
                    distribution: serde_json::json!({"synthetic":{"kind":"zipf","s":1.1}}),
                    trials: 20,
                    error_metrics: BTreeMap::from([("relative_error".into(), 0.009)]),
                    resources: ErpResourceProfile {
                        memory_bytes: 12_288.0,
                        update_cpu_seconds: 1e-7,
                        merge_cpu_seconds: 1e-5,
                        query_cpu_seconds: 1e-6,
                    },
                }],
            },
            distribution: serde_json::json!({"synthetic":{"kind":"zipf","s":1.1}}),
            implementation: Some("oxide".into()),
            error_metric: "relative_error".into(),
            min_trials: 10,
            expected_updates: 1_000.0,
            expected_queries: 100.0,
            expected_merges: 0.0,
            retention_seconds: 60.0,
            cpu_weight: 1.0,
            byte_second_weight: 1e-9,
            mode,
            observed_shape: None,
            observed_populations: None,
            observed_shape_source: None,
            shape_match: None,
            runtime: ErpRuntimeCapabilities::default(),
        }
    }

    /// Structural evidence fixture, not measured calibration data.
    #[test]
    fn existing_univmon_readout_uses_its_own_evidence_despite_cheaper_sibling() {
        use asap_aware_mapping::AccuracyModel;
        use planner_types::post_asap::*;
        let mut policy = input(ErpAccuracyMode::Empirical);
        let small = &mut policy.artifact.records[0];
        small.sketch = "univmon".into();
        small.parameters =
            serde_json::json!({"heap_size":32,"sketch_rows":5,"sketch_cols":128,"layers":4});
        small.error_metrics =
            BTreeMap::from([("max_frequency_entropy_absolute_bits_error".into(), 0.1)]);
        let mut large = small.clone();
        large.id = "larger-shared-state".into();
        large.parameters["heap_size"] = 128.into();
        large.resources.memory_bytes *= 2.0;
        large
            .error_metrics
            .insert("max_frequency_entropy_absolute_bits_error".into(), 0.02);
        policy.artifact.records.push(large);
        let small_params = SketchParams::UnivMon {
            heap_size: 32,
            sketch_rows: 5,
            sketch_cols: 128,
            layers: 4,
        };
        let actual = SketchParams::UnivMon {
            heap_size: 128,
            sketch_rows: 5,
            sketch_cols: 128,
            layers: 4,
        };
        assert_eq!(
            policy
                .select_readout(
                    SketchAlgorithm::UnivMon,
                    ReadoutEvidence::EntropyAbsoluteBits,
                    0.2,
                    actual.clone()
                )
                .params(),
            Some(&small_params)
        );
        let family = SummaryFamilyType::Sketch(
            SketchKind::new(SketchAlgorithm::UnivMon, actual),
            GroupingStrategy::PerSubpopulationInstance,
        );
        let model = ErpAccuracyModel {
            policy: Some(&policy),
            max_error: 0.2,
        };
        let guarantee = model
            .local_guarantee(&family, &SketchQuery::FrequencyEntropy)
            .unwrap();
        assert_eq!(guarantee.bound, BoundExpr::Constant { value: 0.02 });
        assert!(matches!(
            guarantee.failure_probability,
            ProbabilityExpr::Unknown { .. }
        ));
        policy.artifact.records[1].error_metrics.clear();
        assert!(ErpAccuracyModel {
            policy: Some(&policy),
            max_error: 0.2
        }
        .local_guarantee(&family, &SketchQuery::FrequencyEntropy)
        .is_none());
    }

    #[test]
    fn hll_cardinality_uses_measured_relative_error_not_rse() {
        use asap_aware_mapping::AccuracyModel;
        use planner_types::post_asap::*;
        let mut policy = input(ErpAccuracyMode::Hybrid);
        let row = &mut policy.artifact.records[0];
        row.sketch = "hll".into();
        row.parameters = serde_json::json!({"precision":12});
        row.error_metrics = BTreeMap::from([("max_cardinality_relative_error".into(), 0.04)]);
        let family = SummaryFamilyType::Sketch(
            SketchKind::new(SketchAlgorithm::Hll, SketchParams::Hll { precision: 12 }),
            GroupingStrategy::PerSubpopulationInstance,
        );
        let model = ErpAccuracyModel {
            policy: Some(&policy),
            max_error: 0.05,
        };
        let guarantee = model
            .local_guarantee(&family, &SketchQuery::Cardinality)
            .unwrap();
        assert_eq!(guarantee.metric, ErrorMetric::Cardinality);
        assert_eq!(guarantee.bound, BoundExpr::Constant { value: 0.04 });
        assert!(matches!(
            guarantee.failure_probability,
            ProbabilityExpr::Unknown { .. }
        ));
        assert_eq!(
            ReadoutEvidence::for_query(&SketchAlgorithm::Hll, &SketchQuery::FrequencyEntropy),
            None
        );
    }

    /// Contract fixture only; the process test measures real sketch errors.
    #[test]
    fn readout_evidence_keeps_units_and_missing_metrics_fail_closed() {
        use asap_aware_mapping::AccuracyModel;
        use planner_types::post_asap::*;
        let mut policy = input(ErpAccuracyMode::Hybrid);
        let row = &mut policy.artifact.records[0];
        row.sketch = "univmon".into();
        row.parameters =
            serde_json::json!({"heap_size": 32, "sketch_rows": 5, "sketch_cols": 128, "layers": 4});
        row.error_metrics = BTreeMap::from([
            ("max_cardinality_relative_error".into(), 0.03),
            ("max_frequency_l2_relative_error".into(), 0.02),
            ("max_frequency_entropy_absolute_bits_error".into(), 0.1),
        ]);
        let family = SummaryFamilyType::Sketch(
            SketchKind::new(
                SketchAlgorithm::UnivMon,
                SketchParams::UnivMon {
                    heap_size: 32,
                    sketch_rows: 5,
                    sketch_cols: 128,
                    layers: 4,
                },
            ),
            GroupingStrategy::PerSubpopulationInstance,
        );
        let model = ErpAccuracyModel {
            policy: Some(&policy),
            max_error: 0.2,
        };
        for (query, metric, bound) in [
            (SketchQuery::Cardinality, ErrorMetric::Cardinality, 0.03),
            (SketchQuery::FrequencyL2, ErrorMetric::RelativeValue, 0.02),
            (
                SketchQuery::FrequencyEntropy,
                ErrorMetric::AbsoluteValue,
                0.1,
            ),
        ] {
            let guarantee = model.local_guarantee(&family, &query).unwrap();
            assert_eq!(guarantee.metric, metric);
            assert_eq!(guarantee.bound, BoundExpr::Constant { value: bound });
            assert!(matches!(
                guarantee.failure_probability,
                ProbabilityExpr::Unknown { .. }
            ));
            assert!(!model.satisfies(
                &guarantee,
                &crate::types_v2::AccuracyTarget::EpsilonDelta {
                    epsilon: 0.2,
                    delta: 0.01
                }
            ));
        }
        policy.artifact.records[0]
            .error_metrics
            .remove("max_frequency_entropy_absolute_bits_error");
        let model = ErpAccuracyModel {
            policy: Some(&policy),
            max_error: 0.2,
        };
        assert!(model
            .local_guarantee(&family, &SketchQuery::FrequencyEntropy)
            .is_none());
        assert!(model
            .local_guarantee(&family, &SketchQuery::FrequencyL2)
            .is_some());
    }

    /// A matching benchmark context may reduce CMS state below theory.
    #[test]
    fn matching_profile_selects_empirical_parameters() {
        let decision = input(ErpAccuracyMode::Hybrid).select(
            SketchAlgorithm::Cms,
            0.01,
            SketchParams::Cms {
                width: 4096,
                depth: 5,
            },
        );
        assert!(matches!(
            decision,
            ErpParameterDecision::Empirical {
                params: SketchParams::Cms {
                    width: 512,
                    depth: 3
                },
                ..
            }
        ));
    }

    #[test]
    fn unusable_cheapest_profile_does_not_hide_executable_alternative() {
        let mut policy = input(ErpAccuracyMode::Hybrid);
        let mut unusable = policy.artifact.records[0].clone();
        unusable.id = "invalid-cheapest".into();
        unusable.parameters = serde_json::json!({"rows": 0, "cols": 0});
        unusable.resources.memory_bytes = 1.0;
        policy.artifact.records.insert(0, unusable);
        assert!(matches!(policy.select(SketchAlgorithm::Cms, 0.01,
            SketchParams::Cms { width: 4096, depth: 5 }),
            ErpParameterDecision::Empirical { record_id, .. } if record_id == "cms-512"));
    }

    /// Distribution drift in Hybrid mode preserves the analytical fallback.
    #[test]
    fn hybrid_drift_falls_back_to_theoretical_parameters() {
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.distribution = serde_json::json!({"synthetic":{"kind":"uniform"}});
        let theory = SketchParams::Cms {
            width: 4096,
            depth: 5,
        };
        assert!(matches!(
            policy.select(SketchAlgorithm::Cms, 0.01, theory.clone()),
            ErpParameterDecision::TheoreticalFallback { params, .. } if params == theory
        ));
    }

    /// When neither empirical nor theoretical state is deployable, Hybrid
    /// explicitly requests exact execution.
    #[test]
    fn hybrid_capability_miss_falls_back_to_exact() {
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.distribution = serde_json::json!({"synthetic":{"kind":"uniform"}});
        policy.runtime.allowed_algorithms = vec![SketchAlgorithm::Hll];
        assert!(matches!(
            policy.select(
                SketchAlgorithm::Cms,
                0.01,
                SketchParams::Cms {
                    width: 4096,
                    depth: 5,
                }
            ),
            ErpParameterDecision::ExactFallback { .. }
        ));
    }

    #[test]
    fn observer_detects_skew_cardinality_and_burst() {
        let mut observer = ErpShapeObserver::new(10).unwrap();
        for _ in 0..100 {
            observer.observe("hot", 5).unwrap();
        }
        for key in ["a", "b", "c"] {
            observer.observe(key, 0).unwrap();
        }
        for interval in 1..5 {
            for _ in 0..3 {
                observer.observe("a", interval).unwrap();
            }
        }
        let observed = observer.snapshot().unwrap();
        assert_eq!(observed.observation.cardinality, 4);
        assert_eq!(observed.observation.fits.len(), 2);
        assert!(
            observed
                .observation
                .fits
                .iter()
                .find(|fit| fit.family == "zipf")
                .unwrap()
                .parameters["exponent"]
                > 1.0
        );
        assert!(observed.burst_ratio > 30.0);
    }

    #[test]
    fn planning_input_hydrates_shape_from_runtime_feedback() {
        let samples = crate::runtime_samples::RuntimeSamplesStore::new(4);
        samples.append_for_test(crate::runtime_samples::RuntimeRecord {
            source: "edge-a".into(),
            sketch: "cms".into(),
            impl_name: "oxide".into(),
            schema_version: 1,
            payload: serde_json::json!({
                "erp_observed_shape": {
                    "observation": {
                        "cardinality": 1000,
                        "observed_events": 500000,
                        "fits": [{"family": "zipf", "parameters": {"exponent": 1.1}, "goodness_of_fit": 0.01, "confidence": 0.99}]
                    },
                    "burst_ratio": 2.5
                }
            }),
        });
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.observed_shape_source = Some(ErpObservedShapeSource {
            source: "edge-a".into(),
            sketch: "cms".into(),
            implementation: "oxide".into(),
        });
        policy.hydrate_observed_shape(&samples).unwrap();
        assert_eq!(policy.observed_shape.unwrap().cardinality, 1000);
    }

    #[test]
    fn observer_fails_loud_at_cardinality_cap() {
        let mut observer = ErpShapeObserver::new(1).unwrap();
        observer.observe("a", 0).unwrap();
        assert_eq!(
            observer.observe("b", 0),
            Err("ERP shape observer cardinality cap exceeded")
        );
        assert!(observer.snapshot().is_none());
        assert!(observer.observe("a", 0).is_err());
    }

    #[test]
    fn sparse_intervals_and_overflow_cannot_publish_partial_observations() {
        let mut observer = ErpShapeObserver::with_limits(2, 2).unwrap();
        observer.observe("a", usize::MAX).unwrap();
        observer.observe("a", 0).unwrap();
        assert_eq!(observer.interval_updates.len(), 2);
        assert!(observer.observe("a", 1).is_err());
        assert!(observer.snapshot().is_none());
        let mut observer = ErpShapeObserver::new(2).unwrap();
        observer.observe("a", 0).unwrap();
        observer.updates = u64::MAX;
        assert!(observer.observe("a", 0).is_err());
        assert!(observer.snapshot().is_none());
    }

    #[test]
    fn uniform_observation_matches_without_degenerate_zipf_ambiguity() {
        let mut observer = ErpShapeObserver::new(4).unwrap();
        for _ in 0..1000 {
            for key in ["a", "b", "c", "d"] {
                observer.observe(key, 0).unwrap();
            }
        }
        let observed = observer.snapshot().unwrap().observation;
        assert_eq!(observed.fits.len(), 1);
        assert_eq!(observed.fits[0].family, "uniform");
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.artifact.records[0].distribution = serde_json::json!({"erp_shape": {
            "cardinality": 4, "family": "uniform", "parameters": {},
            "benchmark_events": 4000
        }});
        policy.observed_shape = Some(observed);
        policy.shape_match = Some(ErpShapeMatchPolicy {
            minimum_benchmark_events: 1000,
            max_log2_cardinality_distance: 1.0,
            max_parameter_distance: 0.1,
            max_goodness_of_fit: 0.1,
            minimum_confidence: 0.9,
            minimum_confidence_margin: 0.05,
        });
        assert!(matches!(
            policy.select(
                SketchAlgorithm::Cms,
                0.01,
                SketchParams::Cms {
                    width: 4096,
                    depth: 5
                }
            ),
            ErpParameterDecision::Empirical { .. }
        ));
    }

    #[test]
    fn nearest_profile_miss_keeps_hybrid_fallback() {
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.observed_shape = Some(ErpShapeObservation {
            cardinality: 1_000_000,
            observed_events: 10_000,
            fits: vec![ErpShapeFit {
                family: "zipf".into(),
                parameters: BTreeMap::from([("exponent".into(), 2.0)]),
                goodness_of_fit: 0.01,
                confidence: 0.99,
            }],
            empirical_fingerprint: None,
        });
        policy.shape_match = Some(ErpShapeMatchPolicy {
            minimum_benchmark_events: 1_000,
            max_log2_cardinality_distance: 1.0,
            max_parameter_distance: 0.2,
            max_goodness_of_fit: 0.2,
            minimum_confidence: 0.5,
            minimum_confidence_margin: 0.05,
        });
        let theory = SketchParams::Cms {
            width: 4096,
            depth: 5,
        };
        assert!(matches!(
            policy.select(SketchAlgorithm::Cms, 0.01, theory.clone()),
            ErpParameterDecision::TheoreticalFallback { params, .. } if params == theory
        ));
    }

    #[test]
    fn custom_dataset_can_match_an_evidenced_shape_without_same_identity() {
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.distribution = serde_json::json!({
            "workload": {"external": {"dataset": "customer-a"}}
        });
        policy.artifact.records[0].distribution = serde_json::json!({
            "workload": {"external": {"dataset": "customer-b"}},
            "erp_shape": {
                "cardinality": 1000,
                "family": "zipf",
                "parameters": {"exponent": 1.1},
                "benchmark_events": 100000
            }
        });
        policy.observed_shape = Some(ErpShapeObservation {
            cardinality: 1000,
            observed_events: 100000,
            fits: vec![ErpShapeFit {
                family: "zipf".into(),
                parameters: BTreeMap::from([("exponent".into(), 1.1)]),
                goodness_of_fit: 0.01,
                confidence: 0.99,
            }],
            empirical_fingerprint: None,
        });
        policy.shape_match = Some(ErpShapeMatchPolicy {
            minimum_benchmark_events: 1000,
            max_log2_cardinality_distance: 1.0,
            max_parameter_distance: 0.2,
            max_goodness_of_fit: 0.2,
            minimum_confidence: 0.5,
            minimum_confidence_margin: 0.05,
        });
        assert!(matches!(
            policy.select(
                SketchAlgorithm::Cms,
                0.01,
                SketchParams::Cms {
                    width: 4096,
                    depth: 5,
                }
            ),
            ErpParameterDecision::Empirical { .. }
        ));
    }
}
