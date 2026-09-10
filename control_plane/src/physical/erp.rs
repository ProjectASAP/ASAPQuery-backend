//! Deployment adapter for ASAPPlanner Error–Resource Profiles.

use asap_aware_mapping::erp::{
    AccuracyMode, ErpArtifact, ErpDataShape, ErpNearestSelectionRequest, ErpSelectionRequest,
};
use planner_types::post_asap::{SketchAlgorithm, SketchParams};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ErpObservedShape {
    pub shape: ErpDataShape,
    /// Largest interval rate divided by the median non-zero interval rate.
    pub burst_ratio: f64,
}

/// Bounded online observer fed by the same keys that enter a summary. It keeps
/// exact sampled frequencies up to a configured cap and fails closed beyond it
/// instead of silently under-reporting cardinality.
#[derive(Debug)]
pub struct ErpShapeObserver {
    frequencies: HashMap<String, u64>,
    interval_updates: Vec<u64>,
    max_observed_keys: usize,
    updates: u64,
}

impl ErpShapeObserver {
    pub fn new(max_observed_keys: usize) -> Result<Self, &'static str> {
        if max_observed_keys == 0 {
            return Err("max_observed_keys must be positive");
        }
        Ok(Self {
            frequencies: HashMap::new(),
            interval_updates: Vec::new(),
            max_observed_keys,
            updates: 0,
        })
    }

    pub fn observe(&mut self, key: &str, interval: usize) -> Result<(), &'static str> {
        if !self.frequencies.contains_key(key) && self.frequencies.len() == self.max_observed_keys {
            return Err("ERP shape observer cardinality cap exceeded");
        }
        *self.frequencies.entry(key.to_owned()).or_default() += 1;
        if self.interval_updates.len() <= interval {
            self.interval_updates.resize(interval + 1, 0);
        }
        self.interval_updates[interval] += 1;
        self.updates += 1;
        Ok(())
    }

    pub fn snapshot(&self, uniform_exponent_threshold: f64) -> Option<ErpObservedShape> {
        if self.frequencies.is_empty()
            || !uniform_exponent_threshold.is_finite()
            || uniform_exponent_threshold < 0.0
        {
            return None;
        }
        let mut counts: Vec<_> = self.frequencies.values().copied().collect();
        counts.sort_unstable_by(|left, right| right.cmp(left));
        let exponent = fit_zipf_exponent(&counts);
        let (family, parameters) = if exponent > uniform_exponent_threshold {
            (
                "zipf".to_owned(),
                BTreeMap::from([("exponent".to_owned(), exponent)]),
            )
        } else {
            ("uniform".to_owned(), BTreeMap::new())
        };
        let mut nonzero: Vec<_> = self
            .interval_updates
            .iter()
            .copied()
            .filter(|count| *count > 0)
            .collect();
        nonzero.sort_unstable();
        let median = nonzero.get(nonzero.len() / 2).copied().unwrap_or(1);
        let peak = nonzero.last().copied().unwrap_or(median);
        Some(ErpObservedShape {
            shape: ErpDataShape {
                cardinality: self.frequencies.len() as u64,
                family,
                parameters,
                benchmark_events: self.updates,
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
        let mut guarantee =
            asap_aware_mapping::DefaultAccuracyModel.local_guarantee(family, query)?;
        if let (Some(policy), SummaryFamilyType::Sketch(kind, _)) = (self.policy, family) {
            let decision = policy.select(
                kind.algorithm().clone(),
                self.max_error,
                kind.params().clone(),
            );
            if matches!(decision, ErpParameterDecision::ExactFallback { .. }) {
                return None;
            }
            if let ErpParameterDecision::Empirical {
                params,
                record_id,
                observed_error,
                ..
            } = decision
            {
                if &params == kind.params() {
                    // This is the only benchmark-to-query metric mapping currently
                    // validated end to end. Means and value errors are not rank bounds.
                    if guarantee.metric != ErrorMetric::Rank
                        || policy.error_metric != "max_rank_err"
                    {
                        return None;
                    }
                    guarantee.bound = BoundExpr::Constant {
                        value: observed_error,
                    };
                    guarantee.failure_probability = ProbabilityExpr::Unknown {
                        statistic: "erp_v1_has_no_failure_probability_evidence".into(),
                    };
                    guarantee.provenance = vec![GuaranteeSource::SketchReadout {
                        algorithm: format!("{:?}", kind.algorithm()),
                        contract: format!(
                            "erp_v1_empirical:{}:{}",
                            policy.artifact.producer_version, record_id
                        ),
                        params: serde_json::to_value(&params).ok()?,
                        query: format!("{query:?}"),
                    }];
                }
            }
        }
        Some(guarantee)
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
    pub observed_shape: Option<ErpDataShape>,
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
        self.observed_shape = Some(observed.shape);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ErpShapeMatchPolicy {
    pub minimum_benchmark_events: u64,
    pub max_log2_cardinality_distance: f64,
    pub max_parameter_distance: f64,
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
        let allowed_sketches = self
            .artifact
            .records
            .iter()
            .filter(|row| sketch_name_matches(&row.sketch, &algorithm))
            .map(|row| row.sketch.clone())
            .collect();
        let request = ErpSelectionRequest {
            distribution: self.distribution.clone(),
            implementation: self.implementation.clone(),
            allowed_sketches,
            error_metric: self.error_metric.clone(),
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
                    self.artifact.select_nearest(&ErpNearestSelectionRequest {
                        selection: request.clone(),
                        observed: observed.clone(),
                        minimum_benchmark_events: policy.minimum_benchmark_events,
                        max_log2_cardinality_distance: policy.max_log2_cardinality_distance,
                        max_parameter_distance: policy.max_parameter_distance,
                    })
                }
                (None, None) => self.artifact.select(&request),
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
            observed_shape_source: None,
            shape_match: None,
            runtime: ErpRuntimeCapabilities::default(),
        }
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
        let observed = observer.snapshot(0.05).unwrap();
        assert_eq!(observed.shape.cardinality, 4);
        assert_eq!(observed.shape.family, "zipf");
        assert!(observed.shape.parameters["exponent"] > 1.0);
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
                    "shape": {
                        "cardinality": 1000,
                        "family": "zipf",
                        "parameters": {"exponent": 1.1},
                        "benchmark_events": 500000
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
    }

    #[test]
    fn nearest_profile_miss_keeps_hybrid_fallback() {
        let mut policy = input(ErpAccuracyMode::Hybrid);
        policy.observed_shape = Some(ErpDataShape {
            cardinality: 1_000_000,
            family: "zipf".into(),
            parameters: BTreeMap::from([("exponent".into(), 2.0)]),
            benchmark_events: 10_000,
        });
        policy.shape_match = Some(ErpShapeMatchPolicy {
            minimum_benchmark_events: 1_000,
            max_log2_cardinality_distance: 1.0,
            max_parameter_distance: 0.2,
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
}
