//! Typed schema fragments converging toward `control_plane/docs/design.md`.
//!
//! The design defines a typed `QuerySpec` with `id`, `language`,
//! `accuracy: AccuracyTarget`, `shape: QueryShape`, `data: DataShape`, etc.
//! The current `analyzer::QuerySpec` is a JSON-friendly impl that pre-dates
//! that design.
//!
//! Strategy: **additive extension**. The new types live here and are folded
//! into `analyzer::QuerySpec` as `#[serde(default)] Option<…>` fields so
//! existing JSON callers (the `/plan` HTTP endpoint, planner pre-population
//! from `workloads.yaml`, every test that constructs a `QuerySpec`) keep
//! working byte-for-byte. Code that wants to *consume* the new fields can
//! pattern-match on them; code that doesn't care can ignore them.
//!
//! Accuracy is shared with ASAPPlanner and resolved once at the compatibility
//! boundary. The remaining shape/deployment fields retain their existing
//! partial support; they are not silently promoted to executable capabilities.

// Several types in this module (`BindingName`, the `new` / `as_str` helpers on `QueryId`
// and `BindingName`) are intentionally part of the public surface but
// have no in-tree consumers yet — they're targets for the downstream
// PR that wires the planner to consume the typed schema. Suppress the
// dead_code warnings until then so this module doesn't visually
// regress the lint baseline.
#![allow(dead_code)]

use std::time::Duration;

use serde::{Deserialize, Serialize};

pub use asap_types::QueryLanguage;

// ── AccuracyTarget ────────────────────────────────────────────────────────────

/// Per-target accuracy SLA, in the typed form `design.md` §6 calls for.
///
/// Re-exported from `planner_types::types` (formerly `asap_ir::types` -- ASAPPlanner
/// consolidated `asap-ir` into `asap-types`, see
/// control_plane/docs/design-asapplanner-pin-migration.md) rather than
/// defined locally -- `AggIntent`'s `accuracy` fields are typed against
/// ASAPPlanner's `AccuracyTarget`, so keeping a separate local type here
/// would force a conversion at every one of the ~400 `AggIntent` call
/// sites. Two real differences from the pre-merge local type, both
/// confirmed safe to fold on (no external YAML/JSON persists the old wire
/// shape -- only one in-Rust test fixture, `pipeline.rs`, needed updating):
/// - Wire shape: was `#[serde(tag = "kind", content = "value")]`
///   (`{"kind": "epsilon", "value": 0.02}`); now serde's default
///   externally-tagged representation (`{"Epsilon": 0.02}`).
/// - `EpsilonDelta`'s second field is `epsilon`, not `eps`.
pub use planner_types::types::AccuracyTarget;

/// Translate the legacy `accuracy_sla: f64` field -- a fractional
/// "1.0 = exact, 0.0 = anything goes" SLA -- into the typed form.
/// `accuracy_sla == 1.0` round-trips to `Exact`; everything else becomes
/// `Epsilon(1.0 - accuracy_sla)` (the implied error bound). Free function,
/// not `impl AccuracyTarget` -- Rust's orphan rules don't allow inherent
/// impls on a foreign type.
pub fn accuracy_target_from_legacy_accuracy_sla(accuracy_sla: f64) -> AccuracyTarget {
    if accuracy_sla >= 1.0 {
        AccuracyTarget::Exact
    } else {
        AccuracyTarget::Epsilon((1.0 - accuracy_sla).max(0.0))
    }
}

/// Resolve the public compatibility input once, preserving typed delta/exact semantics.
pub fn resolve_accuracy_target(
    typed: Option<&AccuracyTarget>,
    legacy_confidence: f64,
) -> Result<AccuracyTarget, String> {
    let target = if let Some(target) = typed {
        target.clone()
    } else {
        if !legacy_confidence.is_finite() || !(0.0..=1.0).contains(&legacy_confidence) {
            return Err("accuracy_sla must be finite and in [0,1]".into());
        }
        accuracy_target_from_legacy_accuracy_sla(legacy_confidence)
    };
    let valid = match target {
        AccuracyTarget::Exact => true,
        AccuracyTarget::Epsilon(epsilon) => epsilon.is_finite() && (0.0..=1.0).contains(&epsilon),
        AccuracyTarget::EpsilonDelta { epsilon, delta } => {
            epsilon.is_finite()
                && (0.0..=1.0).contains(&epsilon)
                && delta.is_finite()
                && delta > 0.0
                && delta < 1.0
        }
    };
    if valid {
        Ok(target)
    } else {
        Err("invalid typed accuracy requirement".into())
    }
}

// ── QueryShape ────────────────────────────────────────────────────────────────

/// How the query is *evaluated*: one-shot, continuous, or scheduled.
///
/// Drives L4 binding (mergeable vs one-shot sketch family) and the L5
/// wire format (`OneShot` emits config + result; `Streaming` and
/// `Periodic` emit a config that keeps running). Distinct from
/// [`DataShape`] — see `design.md` §6.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryShape {
    /// Evaluate once. Plan, execute, return result, discard state.
    /// One-off PromQL via `POST /plan`.
    #[default]
    OneShot,
    /// Continuous query — output stream that the executor keeps emitting
    /// as new data arrives. No fixed cadence; the runtime emits whenever
    /// the underlying state changes. Streaming dashboards, alerting
    /// expressions evaluated by the agent rather than by a poller.
    Streaming,
    /// Re-evaluated at a fixed cadence — Prometheus recording rules,
    /// scheduled dashboard panels, alerting evaluation cycles. The
    /// planner amortises sketch / aggregate build cost across
    /// evaluations within the cadence and reuses state between
    /// adjacent windows.
    Periodic {
        /// Re-evaluation cadence.
        every: Duration,
    },
}

// ── DataShape ─────────────────────────────────────────────────────────────────

/// Shape of the *data* feeding the query. Workload-level summary; per-leaf
/// detail rides on `Source::data_shape` per `design.md` §6 L3.
///
/// Drives L4 binding choices: an `AppendOnlyStream` unlocks incremental,
/// mergeable sketches and retraction-free aggregation; `Batch` lets the
/// planner pick a non-mergeable estimator (e.g. exact percentile over a
/// sort) that wouldn't survive a distributed streaming setting; `Mutable`
/// requires retraction-aware operators (out of scope today — the planner
/// refuses sketch binding and falls back to re-scan).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DataShape {
    /// Bounded relation, fully materialised at plan time. Parquet / CSV
    /// files, in-process columnar tables.
    Batch,
    /// Append-only stream — events arrive over time, never updated or
    /// deleted. Metrics, logs, event streams. The common case for the
    /// asap-collector / asap-query deployments and so the control plane
    /// default when the field is omitted.
    #[default]
    AppendOnlyStream,
    /// Mutable relation — inserts + updates + deletes. Operational
    /// databases, CRUD-style tables. Sketch binding is currently
    /// refused for this shape (no retraction-aware sketches in the
    /// catalog yet).
    Mutable,
    /// Join across sources of differing shape. The planner consults
    /// `Source::data_shape` per leaf during L4; this variant exists so
    /// callers don't have to flatten a workload-level summary.
    Mixed,
}

// ── QueryId / BindingName ─────────────────────────────────────────────────────

/// Stable identifier preserved across `replan` cycles so the runtime can
/// correlate plan outputs with the originating spec, and L4 reuse rules
/// can name shared producers across consumers in the same workload.
///
/// Idiomatically a string here — the control plane already round-trips
/// metric names + agent IDs as strings (see `monitor::Endpoint`,
/// `opamp::AgentRole`), so adding a `Uuid` dependency for one field
/// with no DB-side semantics would be churn for no benefit. The HTTP
/// API is JSON; callers can supply any string they want, including a
/// stringified UUID. When omitted, `Analyzer::analyze` derives a
/// deterministic id from the parsed metric name + accuracy target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct QueryId(pub String);

impl QueryId {
    pub fn new(id: impl Into<String>) -> Self {
        QueryId(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Name of a hoisted shared sub-expression in a [`WorkloadPlan`]. Each
/// binding is referenced by ≥2 roots via `QueryExpr::Ref` (`design.md`
/// §6 batched-queries example).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct BindingName(pub String);

impl BindingName {
    pub fn new(name: impl Into<String>) -> Self {
        BindingName(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BindingName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── WorkloadPlan ──────────────────────────────────────────────────────────────

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_language_serde_roundtrip() {
        let variant = QueryLanguage::PromQl;
        let json = serde_json::to_string(&variant).unwrap();
        let back: QueryLanguage = serde_json::from_str(&json).unwrap();
        assert_eq!(variant, back, "round-trip failed for {variant:?}");
    }

    #[test]
    fn accuracy_target_serde_roundtrip() {
        let cases = [
            AccuracyTarget::Exact,
            AccuracyTarget::Epsilon(0.05),
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.001,
            },
        ];
        for variant in cases {
            let json = serde_json::to_string(&variant).unwrap();
            let back: AccuracyTarget = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn accuracy_target_from_legacy() {
        // 1.0 means exact in the legacy schema.
        assert_eq!(
            accuracy_target_from_legacy_accuracy_sla(1.0),
            AccuracyTarget::Exact
        );
        // 0.99 SLA → ε = 0.01.
        match accuracy_target_from_legacy_accuracy_sla(0.99) {
            AccuracyTarget::Epsilon(eps) => {
                assert!((eps - 0.01).abs() < 1e-9, "got eps={eps}");
            }
            other => panic!("expected Epsilon, got {other:?}"),
        }
        // Out-of-range guard — analyzer rejects these upstream, but the
        // helper itself must not panic on a 0.0 SLA.
        match accuracy_target_from_legacy_accuracy_sla(0.0) {
            AccuracyTarget::Epsilon(eps) => assert!((eps - 1.0).abs() < 1e-9),
            other => panic!("expected Epsilon, got {other:?}"),
        }
    }

    #[test]
    fn query_shape_serde_roundtrip_and_default() {
        assert_eq!(QueryShape::default(), QueryShape::OneShot);
        let cases = [
            QueryShape::OneShot,
            QueryShape::Streaming,
            QueryShape::Periodic {
                every: Duration::from_secs(60),
            },
        ];
        for variant in cases {
            let json = serde_json::to_string(&variant).unwrap();
            let back: QueryShape = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn data_shape_serde_roundtrip_and_default() {
        assert_eq!(DataShape::default(), DataShape::AppendOnlyStream);
        for variant in [
            DataShape::Batch,
            DataShape::AppendOnlyStream,
            DataShape::Mutable,
            DataShape::Mixed,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: DataShape = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn query_id_transparent_string_serde() {
        let id = QueryId::new("metric-x@0.99");
        let json = serde_json::to_string(&id).unwrap();
        // `#[serde(transparent)]` should serialize as a bare string.
        assert_eq!(json, "\"metric-x@0.99\"");
        let back: QueryId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
        assert_eq!(id.as_str(), "metric-x@0.99");
    }
}
