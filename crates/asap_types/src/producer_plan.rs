//! Installed collector and transmission contracts shared across producers and consumers.
//! Deployment choices and accuracy-budget allocation remain in the control plane.
use crate::precompute_plan::{PlanEnvelope, PrecomputePlan, StateEncoding, StateFamilyContract};
use planner_types::post_asap::{SketchAlgorithm, SummaryWindowFramework};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorMaterialization {
    pub query_id: String,
    pub materialization: crate::sds::SummaryDefinitionId,
    pub metric: String,
    pub algorithm: String,
    pub parameters: Value,
    pub group_by: Vec<String>,
    pub window_secs: u64,
    pub abstract_window_framework: SummaryWindowFramework,
    pub window_implementation_id: String,
    pub slide_secs: u64,
    #[serde(
        default,
        alias = "paneOriginMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub pane_origin_ms: Option<i64>,
    pub window_layout: crate::WindowMaterializationLayout,
    pub evidence_source: Option<String>,
    pub lifecycle: CollectorLifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollectorLifecycle {
    pub kind: String,
    pub maintenance_mode: String,
    pub evaluation_schedule: String,
    pub output_representation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorPlan {
    /// Absent only in legacy artifacts; catalog-aware validation requires it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<crate::sds::CatalogGeneration>,
    pub collector_id: String,
    pub envelope: PlanEnvelope,
    pub materializations: Vec<CollectorMaterialization>,
    pub transmission_rules: Vec<TransmissionRule>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TransmissionMode {
    Full,
    Delta,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SequenceScope {
    MaterializationSeriesProducerEpoch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrameIdentityContract {
    pub identity_version: u32,
    pub sequence_scope: SequenceScope,
    pub require_checkpoint_for_full: bool,
    pub require_base_checkpoint_for_delta: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransmissionRule {
    pub materialization: crate::sds::SummaryDefinitionId,
    pub producer_id: String,
    pub schema_id: String,
    pub mode: TransmissionMode,
    pub encoding: StateEncoding,
    pub emit_every_ms: u64,
    pub full_checkpoint_every_ms: Option<u64>,
    pub destination_ref: String,
    /// Plan-owned runtime knobs. These values are part of the immutable plan
    /// generation; live feedback may only change them by publishing a
    /// successor generation accepted by [`TransmissionPlan::authorize_successor`].
    #[serde(default)]
    pub runtime_policy: RuntimeRulePolicy,
}

/// How the collector admits updates before sketch maintenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SamplingPolicy {
    Disabled,
    Fixed {
        /// Probability in `(0, 1]`; `1` is valid but should normally be
        /// represented by `Disabled`.
        probability: f64,
        estimator: SamplingEstimator,
    },
}

impl Default for SamplingPolicy {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SamplingEstimator {
    /// Hash-threshold element sampling, used by cardinality summaries.
    HashThreshold,
    /// Geometric admission/Nitro-style update sampling, used by frequency
    /// summaries. The sketch readout carries the corresponding correction.
    GeometricAdmission,
}

/// Norm-adaptive Group-of-Sketches delta gating. GOS is meaningful only for
/// CountSketch families and only when the transmission rule is in delta mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GosPolicy {
    pub epsilon_staleness: f64,
    pub sites: u32,
    pub threshold_mode: GosThresholdMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GosThresholdMode {
    Isotropic,
    Anisotropic,
}

/// Sparse-delta semantics within a delta transmission rule. An absolute
/// threshold of zero sends every changed cell. When `gos` is present it
/// replaces the fixed threshold with the GOS norm-adaptive threshold.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeltaPolicy {
    pub absolute_threshold: f64,
    pub gos: Option<GosPolicy>,
}

/// Inclusive bounds for one floating-point adaptation knob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveF64Bounds {
    pub min: f64,
    pub max: f64,
    pub max_step: f64,
}

/// Inclusive bounds for one integer adaptation knob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveU64Bounds {
    pub min: u64,
    pub max: u64,
    pub max_step: u64,
}

/// Guardrails for telemetry-driven runtime adaptation. This is an
/// authorization contract, not an instruction to mutate the active plan.
/// Every accepted change becomes a staged successor PhysicalPlan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdaptationPolicy {
    pub enabled: bool,
    pub not_before_unix_ms: u64,
    pub max_evidence_age_ms: u64,
    pub min_evidence_samples: u64,
    pub sample_probability: Option<AdaptiveF64Bounds>,
    pub emit_every_ms: Option<AdaptiveU64Bounds>,
    pub delta_threshold: Option<AdaptiveF64Bounds>,
    pub gos_epsilon_staleness: Option<AdaptiveF64Bounds>,
}

impl Default for RuntimeAdaptationPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            not_before_unix_ms: 0,
            max_evidence_age_ms: 0,
            min_evidence_samples: 0,
            sample_probability: None,
            emit_every_ms: None,
            delta_threshold: None,
            gos_epsilon_staleness: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRulePolicy {
    #[serde(default)]
    pub sampling: SamplingPolicy,
    pub delta: Option<DeltaPolicy>,
    #[serde(default)]
    pub adaptation: RuntimeAdaptationPolicy,
}

/// Identity and sufficiency information for evidence authorizing one rule's
/// successor knobs. Raw measurements remain in the runtime-samples store; the
/// authorization boundary needs only their exact provenance and sample count.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdaptationEvidence {
    pub plan_id: u64,
    pub plan_version: u64,
    pub materialization: crate::sds::SummaryDefinitionId,
    pub producer_id: String,
    pub schema_id: String,
    pub producer_version: String,
    pub observed_at_unix_ms: u64,
    pub sample_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransmissionPlan {
    /// Absent only in legacy artifacts; catalog-aware validation requires it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<crate::sds::CatalogGeneration>,
    pub envelope: PlanEnvelope,
    pub frame_identity: FrameIdentityContract,
    pub rules: Vec<TransmissionRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SummaryFrameKind {
    Full,
    Delta,
}

/// Identity attached to every summary record. Window bounds come from the
/// data point; the remaining fields are carried as reserved `asap.frame.*`
/// attributes until the modified-OTLP schema gains a dedicated message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SummaryFrameIdentity {
    pub identity_version: u32,
    pub plan_id: u64,
    pub plan_version: u64,
    pub backend_compat: String,
    pub materialization: crate::sds::SummaryDefinitionId,
    /// Canonical producer-side identity for one concrete retained-label group.
    pub series_identity: String,
    pub schema_id: String,
    pub producer_id: String,
    pub producer_epoch: String,
    pub window_start_unix_nano: u64,
    pub window_end_unix_nano: u64,
    pub sequence: u64,
    pub kind: SummaryFrameKind,
    pub encoding: StateEncoding,
    pub checkpoint_id: Option<String>,
    pub base_checkpoint_id: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransmissionPlanError {
    #[error("summary catalog mismatch: {0}")]
    Catalog(String),
    #[error("TransmissionPlan envelope differs from PrecomputePlan")]
    EnvelopeMismatch,
    #[error("transmission rules do not exactly match precompute producer bindings")]
    ProducerSetMismatch,
    #[error("invalid transmission rule for producer {0}")]
    InvalidRule(String),
    #[error("frame identity is invalid: {0}")]
    InvalidFrame(String),
    #[error("frame has no matching transmission rule")]
    UnknownFrame,
    #[error("runtime policy for producer {producer_id} is invalid: {reason}")]
    InvalidRuntimePolicy { producer_id: String, reason: String },
    #[error("runtime adaptation successor is invalid: {0}")]
    InvalidSuccessor(String),
    #[error("runtime adaptation evidence for producer {0} is missing or invalid")]
    InvalidAdaptationEvidence(String),
    #[error("runtime adaptation for producer {producer_id} exceeds guardrails: {knob}")]
    AdaptationOutOfBounds {
        producer_id: String,
        knob: &'static str,
    },
}

fn validate_catalog_projection(
    reference: Option<&crate::sds::CatalogGeneration>,
    envelope: &PlanEnvelope,
    materializations: impl IntoIterator<Item = crate::sds::SummaryDefinitionId>,
    catalog: &crate::summary_catalog::SummaryCatalog,
) -> Result<(), TransmissionPlanError> {
    let expected = catalog
        .reference()
        .map_err(|error| TransmissionPlanError::Catalog(error.to_string()))?;
    if reference != Some(&expected)
        || envelope.plan_id != catalog.plan_id
        || envelope.plan_version != catalog.plan_version
    {
        return Err(TransmissionPlanError::Catalog(
            "missing or different snapshot reference".into(),
        ));
    }
    for id in materializations {
        if !catalog.materializations.contains_key(&id) {
            return Err(TransmissionPlanError::Catalog(format!(
                "unknown materialization {}",
                id.as_u64()
            )));
        }
    }
    Ok(())
}

impl CollectorPlan {
    pub fn validate_against_catalog(
        &self,
        catalog: &crate::summary_catalog::SummaryCatalog,
    ) -> Result<(), TransmissionPlanError> {
        validate_catalog_projection(
            self.summary_catalog.as_ref(),
            &self.envelope,
            self.materializations
                .iter()
                .map(|m| m.materialization)
                .chain(self.transmission_rules.iter().map(|r| r.materialization)),
            catalog,
        )
    }
}

impl TransmissionPlan {
    pub fn validate_against_catalog(
        &self,
        catalog: &crate::summary_catalog::SummaryCatalog,
    ) -> Result<(), TransmissionPlanError> {
        validate_catalog_projection(
            self.summary_catalog.as_ref(),
            &self.envelope,
            self.rules.iter().map(|r| r.materialization),
            catalog,
        )
    }

    pub fn validate(&self, precompute: &PrecomputePlan) -> Result<(), TransmissionPlanError> {
        if self.summary_catalog != precompute.summary_catalog {
            return Err(TransmissionPlanError::Catalog(
                "transmission and precompute plans reference different catalog snapshots".into(),
            ));
        }
        if self.envelope != precompute.envelope {
            return Err(TransmissionPlanError::EnvelopeMismatch);
        }
        let expected: BTreeSet<_> = precompute
            .producers
            .iter()
            .map(|producer| {
                (
                    producer.materialization,
                    producer.producer_id.as_str(),
                    producer.schema_id.as_str(),
                )
            })
            .collect();
        let actual: BTreeSet<_> = self
            .rules
            .iter()
            .map(|rule| {
                (
                    rule.materialization,
                    rule.producer_id.as_str(),
                    rule.schema_id.as_str(),
                )
            })
            .collect();
        if expected != actual || actual.len() != self.rules.len() {
            return Err(TransmissionPlanError::ProducerSetMismatch);
        }
        for rule in &self.rules {
            let valid_checkpoint_cadence = match (rule.mode, rule.full_checkpoint_every_ms) {
                (TransmissionMode::Full, None) => true,
                (TransmissionMode::Delta, Some(full_every)) => {
                    rule.emit_every_ms > 0
                        && full_every >= rule.emit_every_ms
                        && full_every % rule.emit_every_ms == 0
                }
                _ => false,
            };
            if rule.emit_every_ms == 0
                || rule.destination_ref.is_empty()
                || !valid_checkpoint_cadence
            {
                return Err(TransmissionPlanError::InvalidRule(rule.producer_id.clone()));
            }
            let schema = precompute
                .schemas
                .iter()
                .find(|schema| schema.materialization == rule.materialization)
                .expect("producer set validation guarantees a matching schema");
            if !schema.encodings.contains(&rule.encoding) {
                return Err(TransmissionPlanError::InvalidRule(rule.producer_id.clone()));
            }
            validate_runtime_rule_policy(rule, &schema.family)?;
        }
        Ok(())
    }

    /// Authorize a telemetry-driven successor without mutating this active
    /// plan. Semantic identity, codecs, destination, transmission mode and
    /// checkpoint cadence remain fixed. Only explicitly bounded runtime knobs
    /// may move, and each changed rule needs fresh evidence attributed to the
    /// exact active generation.
    pub fn authorize_successor(
        &self,
        successor: &TransmissionPlan,
        evidence: &[RuntimeAdaptationEvidence],
        now_unix_ms: u64,
    ) -> Result<(), TransmissionPlanError> {
        if successor.envelope.plan_id != self.envelope.plan_id
            || successor.envelope.plan_version != self.envelope.plan_version.saturating_add(1)
            || successor.envelope.backend_compat != self.envelope.backend_compat
            || successor.envelope.planner_revision != self.envelope.planner_revision
            || successor.envelope.capability_snapshot_id != self.envelope.capability_snapshot_id
            || successor.envelope.generated_at_unix_ms < self.envelope.generated_at_unix_ms
            || successor.envelope.activation_unix_ms < successor.envelope.generated_at_unix_ms
            || successor.rules.len() != self.rules.len()
            || successor.frame_identity != self.frame_identity
        {
            return Err(TransmissionPlanError::InvalidSuccessor(
                "successor must be the next version of the same semantic/capability generation"
                    .into(),
            ));
        }

        for current in &self.rules {
            let Some(next) = successor.rules.iter().find(|candidate| {
                candidate.materialization == current.materialization
                    && candidate.producer_id == current.producer_id
                    && candidate.schema_id == current.schema_id
            }) else {
                return Err(TransmissionPlanError::InvalidSuccessor(format!(
                    "missing rule for producer {}",
                    current.producer_id
                )));
            };
            if current.mode != next.mode
                || current.encoding != next.encoding
                || current.full_checkpoint_every_ms != next.full_checkpoint_every_ms
                || current.destination_ref != next.destination_ref
                || current.runtime_policy.adaptation != next.runtime_policy.adaptation
                || sampling_estimator(&current.runtime_policy.sampling)
                    != sampling_estimator(&next.runtime_policy.sampling)
                || delta_shape(&current.runtime_policy.delta)
                    != delta_shape(&next.runtime_policy.delta)
            {
                return Err(TransmissionPlanError::InvalidSuccessor(format!(
                    "rule identity/codec/mode/guardrails drifted for producer {}",
                    current.producer_id
                )));
            }
            if current.emit_every_ms == next.emit_every_ms
                && current.runtime_policy.sampling == next.runtime_policy.sampling
                && current.runtime_policy.delta == next.runtime_policy.delta
            {
                continue;
            }
            let policy = &current.runtime_policy.adaptation;
            if !policy.enabled || now_unix_ms < policy.not_before_unix_ms {
                return Err(TransmissionPlanError::AdaptationOutOfBounds {
                    producer_id: current.producer_id.clone(),
                    knob: "adaptation_disabled_or_in_cooldown",
                });
            }
            let has_evidence = evidence.iter().any(|item| {
                item.plan_id == self.envelope.plan_id
                    && item.plan_version == self.envelope.plan_version
                    && item.materialization == current.materialization
                    && item.producer_id == current.producer_id
                    && item.schema_id == current.schema_id
                    && !item.producer_version.trim().is_empty()
                    && item.sample_count >= policy.min_evidence_samples
                    && item.observed_at_unix_ms <= now_unix_ms
                    && now_unix_ms.saturating_sub(item.observed_at_unix_ms)
                        <= policy.max_evidence_age_ms
            });
            if !has_evidence {
                return Err(TransmissionPlanError::InvalidAdaptationEvidence(
                    current.producer_id.clone(),
                ));
            }
            authorize_f64_change(
                sampling_probability(&current.runtime_policy.sampling),
                sampling_probability(&next.runtime_policy.sampling),
                policy.sample_probability.as_ref(),
                &current.producer_id,
                "sample_probability",
            )?;
            authorize_u64_change(
                current.emit_every_ms,
                next.emit_every_ms,
                policy.emit_every_ms.as_ref(),
                &current.producer_id,
                "emit_every_ms",
            )?;
            authorize_f64_change(
                delta_threshold(&current.runtime_policy.delta),
                delta_threshold(&next.runtime_policy.delta),
                policy.delta_threshold.as_ref(),
                &current.producer_id,
                "delta_threshold",
            )?;
            authorize_f64_change(
                gos_epsilon(&current.runtime_policy.delta),
                gos_epsilon(&next.runtime_policy.delta),
                policy.gos_epsilon_staleness.as_ref(),
                &current.producer_id,
                "gos_epsilon_staleness",
            )?;
        }
        Ok(())
    }

    pub fn validate_frame(
        &self,
        frame: &SummaryFrameIdentity,
    ) -> Result<(), TransmissionPlanError> {
        if frame.identity_version != self.frame_identity.identity_version
            || frame.plan_id != self.envelope.plan_id
            || frame.plan_version != self.envelope.plan_version
            || frame.backend_compat != self.envelope.backend_compat
            || frame.series_identity.is_empty()
            || frame.producer_epoch.is_empty()
            || frame.sequence == 0
            || frame.window_start_unix_nano >= frame.window_end_unix_nano
            || (frame.kind == SummaryFrameKind::Full
                && self.frame_identity.require_checkpoint_for_full
                && frame.checkpoint_id.is_none())
            || (frame.kind == SummaryFrameKind::Delta
                && self.frame_identity.require_base_checkpoint_for_delta
                && frame.base_checkpoint_id.is_none())
        {
            return Err(TransmissionPlanError::InvalidFrame(
                "identity/lifecycle/window/checkpoint fields do not satisfy the active contract"
                    .into(),
            ));
        }
        if self.rules.iter().any(|rule| {
            rule.materialization == frame.materialization
                && rule.producer_id == frame.producer_id
                && rule.schema_id == frame.schema_id
                // A delta rule necessarily emits periodic full checkpoints;
                // a full-only rule must never emit deltas.
                && (frame.kind == SummaryFrameKind::Full
                    || rule.mode == TransmissionMode::Delta)
                && rule.encoding == frame.encoding
        }) {
            Ok(())
        } else {
            Err(TransmissionPlanError::UnknownFrame)
        }
    }
}

fn validate_runtime_rule_policy(
    rule: &TransmissionRule,
    family: &StateFamilyContract,
) -> Result<(), TransmissionPlanError> {
    let invalid = |reason: &str| TransmissionPlanError::InvalidRuntimePolicy {
        producer_id: rule.producer_id.clone(),
        reason: reason.into(),
    };
    if let SamplingPolicy::Fixed {
        probability,
        estimator,
    } = &rule.runtime_policy.sampling
    {
        if !probability.is_finite() || !(0.0..=1.0).contains(probability) || *probability == 0.0 {
            return Err(invalid("sample probability must be finite and in (0, 1]"));
        }
        let supported = matches!(
            (family, *estimator),
            (
                StateFamilyContract::Sketch {
                    algorithm: SketchAlgorithm::Hll,
                    ..
                },
                SamplingEstimator::HashThreshold,
            ) | (
                StateFamilyContract::Sketch {
                    algorithm: SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap,
                    ..
                },
                SamplingEstimator::GeometricAdmission,
            )
        );
        if !supported {
            return Err(invalid(
                "sampling estimator is not implemented for the materialization family",
            ));
        }
    }

    if (rule.mode == TransmissionMode::Delta) != rule.runtime_policy.delta.is_some() {
        return Err(invalid(
            "delta policy must be present exactly when transmission mode is delta",
        ));
    }
    if let Some(delta) = &rule.runtime_policy.delta {
        if !delta.absolute_threshold.is_finite() || delta.absolute_threshold < 0.0 {
            return Err(invalid("delta threshold must be finite and non-negative"));
        }
        if !matches!(
            family,
            StateFamilyContract::Sketch {
                algorithm: SketchAlgorithm::DDSketch
                    | SketchAlgorithm::Hll
                    | SketchAlgorithm::Cms
                    | SketchAlgorithm::CmsWithHeap
                    | SketchAlgorithm::CountSketch
                    | SketchAlgorithm::CountSketchWithHeap,
                ..
            }
        ) {
            return Err(invalid(
                "delta transmission is not implemented for the materialization family",
            ));
        }
        if let Some(gos) = &delta.gos {
            if !matches!(
                family,
                StateFamilyContract::Sketch {
                    algorithm: SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap,
                    ..
                }
            ) || !gos.epsilon_staleness.is_finite()
                || !(0.0..=1.0).contains(&gos.epsilon_staleness)
                || gos.epsilon_staleness == 0.0
                || gos.sites == 0
            {
                return Err(invalid(
                    "GOS requires a CountSketch family, epsilon in (0, 1], and at least one site",
                ));
            }
        }
    }

    let adaptation = &rule.runtime_policy.adaptation;
    if adaptation.enabled
        && (adaptation.max_evidence_age_ms == 0 || adaptation.min_evidence_samples == 0)
    {
        return Err(invalid(
            "enabled adaptation requires non-zero evidence age and sample-count requirements",
        ));
    }
    validate_f64_bounds(adaptation.sample_probability.as_ref(), 0.0, 1.0)
        .map_err(|reason| invalid(reason))?;
    validate_f64_bounds(adaptation.delta_threshold.as_ref(), 0.0, f64::MAX)
        .map_err(|reason| invalid(reason))?;
    validate_f64_bounds(adaptation.gos_epsilon_staleness.as_ref(), 0.0, 1.0)
        .map_err(|reason| invalid(reason))?;
    if let Some(bounds) = &adaptation.emit_every_ms {
        if bounds.min == 0
            || bounds.min > bounds.max
            || bounds.max_step == 0
            || !(bounds.min..=bounds.max).contains(&rule.emit_every_ms)
        {
            return Err(invalid("emit interval guardrails are invalid"));
        }
    }
    if let Some(bounds) = &adaptation.sample_probability {
        let current = sampling_probability(&rule.runtime_policy.sampling);
        if current < bounds.min || current > bounds.max {
            return Err(invalid(
                "current sampling probability is outside guardrails",
            ));
        }
    }
    if let Some(bounds) = &adaptation.delta_threshold {
        let Some(current) = rule
            .runtime_policy
            .delta
            .as_ref()
            .map(|policy| policy.absolute_threshold)
        else {
            return Err(invalid("delta guardrails require an active delta policy"));
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current delta threshold is outside guardrails"));
        }
    }
    if let Some(bounds) = &adaptation.gos_epsilon_staleness {
        let Some(current) = rule
            .runtime_policy
            .delta
            .as_ref()
            .and_then(|policy| policy.gos.as_ref())
            .map(|gos| gos.epsilon_staleness)
        else {
            return Err(invalid("GOS guardrails require an active GOS policy"));
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current GOS epsilon is outside guardrails"));
        }
    }
    Ok(())
}

fn validate_f64_bounds(
    bounds: Option<&AdaptiveF64Bounds>,
    domain_min: f64,
    domain_max: f64,
) -> Result<(), &'static str> {
    let Some(bounds) = bounds else {
        return Ok(());
    };
    if !bounds.min.is_finite()
        || !bounds.max.is_finite()
        || !bounds.max_step.is_finite()
        || bounds.min < domain_min
        || bounds.max > domain_max
        || bounds.min > bounds.max
        || bounds.max_step <= 0.0
    {
        Err("floating-point adaptation guardrails are invalid")
    } else {
        Ok(())
    }
}

fn sampling_probability(policy: &SamplingPolicy) -> f64 {
    match policy {
        SamplingPolicy::Disabled => 1.0,
        SamplingPolicy::Fixed { probability, .. } => *probability,
    }
}

fn sampling_estimator(policy: &SamplingPolicy) -> Option<SamplingEstimator> {
    match policy {
        SamplingPolicy::Disabled => None,
        SamplingPolicy::Fixed { estimator, .. } => Some(*estimator),
    }
}

fn delta_threshold(policy: &Option<DeltaPolicy>) -> f64 {
    policy
        .as_ref()
        .map(|policy| policy.absolute_threshold)
        .unwrap_or(0.0)
}

fn gos_epsilon(policy: &Option<DeltaPolicy>) -> f64 {
    policy
        .as_ref()
        .and_then(|policy| policy.gos.as_ref())
        .map(|gos| gos.epsilon_staleness)
        .unwrap_or(0.0)
}

fn delta_shape(policy: &Option<DeltaPolicy>) -> Option<(Option<(u32, GosThresholdMode)>,)> {
    policy.as_ref().map(|policy| {
        (policy
            .gos
            .as_ref()
            .map(|gos| (gos.sites, gos.threshold_mode)),)
    })
}

fn authorize_f64_change(
    current: f64,
    next: f64,
    bounds: Option<&AdaptiveF64Bounds>,
    producer_id: &str,
    knob: &'static str,
) -> Result<(), TransmissionPlanError> {
    if current == next {
        return Ok(());
    }
    let allowed = bounds.is_some_and(|bounds| {
        next.is_finite()
            && (bounds.min..=bounds.max).contains(&next)
            && (next - current).abs() <= bounds.max_step
    });
    if allowed {
        Ok(())
    } else {
        Err(TransmissionPlanError::AdaptationOutOfBounds {
            producer_id: producer_id.into(),
            knob,
        })
    }
}

fn authorize_u64_change(
    current: u64,
    next: u64,
    bounds: Option<&AdaptiveU64Bounds>,
    producer_id: &str,
    knob: &'static str,
) -> Result<(), TransmissionPlanError> {
    if current == next {
        return Ok(());
    }
    let allowed = bounds.is_some_and(|bounds| {
        (bounds.min..=bounds.max).contains(&next) && current.abs_diff(next) <= bounds.max_step
    });
    if allowed {
        Ok(())
    } else {
        Err(TransmissionPlanError::AdaptationOutOfBounds {
            producer_id: producer_id.into(),
            knob,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::post_asap::SketchParams;

    #[test]
    fn runtime_policy_is_family_and_mode_checked() {
        let mut rule = TransmissionRule {
            materialization: crate::PolicyFingerprint(1).into(),
            producer_id: "test".into(),
            schema_id: "test".into(),
            mode: TransmissionMode::Full,
            encoding: StateEncoding::SketchlibProtobufV1,
            emit_every_ms: 60_000,
            full_checkpoint_every_ms: None,
            destination_ref: "backend".into(),
            runtime_policy: Default::default(),
        };
        rule.runtime_policy.sampling = SamplingPolicy::Fixed {
            probability: 0.5,
            estimator: SamplingEstimator::HashThreshold,
        };
        let hll = StateFamilyContract::Sketch {
            algorithm: SketchAlgorithm::Hll,
            parameters: SketchParams::Hll { precision: 14 },
        };
        let count_sketch = StateFamilyContract::Sketch {
            algorithm: SketchAlgorithm::CountSketch,
            parameters: SketchParams::CountSketch {
                width: 128,
                depth: 4,
            },
        };
        validate_runtime_rule_policy(&rule, &hll).expect("HLL supports hash-threshold sampling");
        assert!(matches!(
            validate_runtime_rule_policy(&rule, &count_sketch),
            Err(TransmissionPlanError::InvalidRuntimePolicy { .. })
        ));

        rule.runtime_policy.sampling = SamplingPolicy::Disabled;
        rule.mode = TransmissionMode::Delta;
        rule.full_checkpoint_every_ms = Some(300_000);
        rule.runtime_policy.delta = Some(DeltaPolicy {
            absolute_threshold: 0.0,
            gos: Some(GosPolicy {
                epsilon_staleness: 0.02,
                sites: 2,
                threshold_mode: GosThresholdMode::Isotropic,
            }),
        });
        validate_runtime_rule_policy(&rule, &count_sketch).expect("CountSketch supports delta GOS");
        assert!(matches!(
            validate_runtime_rule_policy(&rule, &hll),
            Err(TransmissionPlanError::InvalidRuntimePolicy { .. })
        ));
    }
}
