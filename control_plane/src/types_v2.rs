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
//! Nothing in this module is yet load-bearing for cost / binding decisions
//! in `planner/`. That's a separate downstream change once the planner has
//! an L4 rule engine to pivot on `AccuracyTarget` and a stage allocator
//! that respects `QueryShape::Streaming`.

// Several types in this module (`BindingName`, `WorkloadPlan`,
// `QueryExprPlaceholder`, the `new` / `as_str` helpers on `QueryId`
// and `BindingName`) are intentionally part of the public surface but
// have no in-tree consumers yet — they're targets for the downstream
// PR that wires the planner to consume the typed schema. Suppress the
// dead_code warnings until then so this module doesn't visually
// regress the lint baseline.
#![allow(dead_code)]

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── QueryLanguage ─────────────────────────────────────────────────────────────

/// Source language the raw query string is written in. Drives which L1
/// parser the control plane dispatches to.
///
/// The control plane consumes `PromQL` only (see `query_parser/promql.rs`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    /// Prometheus query language. Parsed via `promql-parser`.
    #[serde(rename = "prom_ql", alias = "prom_q_l")]
    PromQL,
}

// ── AccuracyTarget ────────────────────────────────────────────────────────────

/// Per-target accuracy SLA, in the typed form `design.md` §6 calls for.
///
/// Drives L4 sketch binding (`Exact` disables every `Bind*` rule, so the
/// optimiser falls back to an exact `HashAgg` / `SortAgg`; `Epsilon` and
/// `EpsilonDelta` set the ε / δ budget the cost model has to satisfy when
/// it picks a sketch family + parameters).
///
/// The legacy `analyzer::QuerySpec.accuracy_sla: f64` field is preserved
/// for back-compat — when a caller supplies a typed `accuracy: Some(…)`
/// it takes precedence; otherwise the analyzer translates the legacy
/// fraction to `Epsilon(1.0 - accuracy_sla)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AccuracyTarget {
    /// No approximation allowed. L4 must pick an exact path; sketch
    /// binding rules are skipped.
    Exact,
    /// Bound on relative error. The L4 cost model must pick sketch
    /// parameters that satisfy `error ≤ eps` with whatever default
    /// confidence the sketch family provides.
    Epsilon(f64),
    /// Bound on relative error and the probability of exceeding it
    /// (Pr[error > eps] ≤ delta). Required for sketches whose
    /// guarantees are inherently probabilistic (CMS, HLL).
    EpsilonDelta {
        /// Relative-error bound.
        eps: f64,
        /// Probability of exceeding the bound.
        delta: f64,
    },
}

impl AccuracyTarget {
    /// Translate the legacy `accuracy_sla: f64` field — a fractional
    /// "1.0 = exact, 0.0 = anything goes" SLA — into the typed form.
    /// `accuracy_sla == 1.0` round-trips to `Exact`; everything else
    /// becomes `Epsilon(1.0 - accuracy_sla)` (the implied error bound).
    pub fn from_legacy_accuracy_sla(accuracy_sla: f64) -> Self {
        if accuracy_sla >= 1.0 {
            AccuracyTarget::Exact
        } else {
            AccuracyTarget::Epsilon((1.0 - accuracy_sla).max(0.0))
        }
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

/// Multi-root DAG container, one level above `QueryExpr` (`design.md` §6
/// "Multi-root DAGs live one level above `QueryExpr`"). `QueryExpr`
/// stays single-root; this struct holds N roots plus the hoisted
/// bindings the CSE pass shares between them.
///
/// **Container only — no CSE pass yet.** This type is defined so the
/// `analyzer::QuerySpec.id` field has a target to dock against and so
/// the downstream planner can grow into a `WorkloadPlan` consumer
/// without another schema rev. The `bindings` and `roots` payloads use
/// `String` placeholders for the `QueryExpr` slot; the real `QueryExpr`
/// from `algebra/expr.rs` lacks `Serialize` today, and the design's L3
/// rewrites — `LetBinding` / `Ref` / sketch-binding split — haven't
/// landed in `algebra/`. When they do, the placeholder becomes a
/// `QueryExpr` and the surrounding plumbing stays put.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkloadPlan {
    /// Named shared producers, hoisted out of individual queries by
    /// the CSE pass. Each binding is referenced by ≥2 roots via
    /// `QueryExpr::Ref` once that lowering exists.
    pub bindings: Vec<(BindingName, QueryExprPlaceholder)>,
    /// One root per `QuerySpec` in the workload, in input order.
    pub roots: Vec<(QueryId, QueryExprPlaceholder)>,
}

/// Placeholder for `QueryExpr` until `algebra::expr::QueryExpr` gets a
/// `Serialize` impl + the L3 rewrites that `WorkloadPlan` consumers
/// expect (CTE-style `LetBinding` / `Ref`, sketch-binding split). Today
/// it's a string carrying the source-level query text or a debug
/// `format!("{qe:?}")` of the algebra tree — enough for the control plane
/// to round-trip a `WorkloadPlan` through JSON without losing identity,
/// but not yet enough for L4 to consume.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(transparent)]
pub struct QueryExprPlaceholder(pub String);

impl QueryExprPlaceholder {
    pub fn new(text: impl Into<String>) -> Self {
        QueryExprPlaceholder(text.into())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_language_serde_roundtrip() {
        let variant = QueryLanguage::PromQL;
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
                eps: 0.01,
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
            AccuracyTarget::from_legacy_accuracy_sla(1.0),
            AccuracyTarget::Exact
        );
        // 0.99 SLA → ε = 0.01.
        match AccuracyTarget::from_legacy_accuracy_sla(0.99) {
            AccuracyTarget::Epsilon(eps) => {
                assert!((eps - 0.01).abs() < 1e-9, "got eps={eps}");
            }
            other => panic!("expected Epsilon, got {other:?}"),
        }
        // Out-of-range guard — analyzer rejects these upstream, but the
        // helper itself must not panic on a 0.0 SLA.
        match AccuracyTarget::from_legacy_accuracy_sla(0.0) {
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

    #[test]
    fn workload_plan_default_is_empty() {
        let wp = WorkloadPlan::default();
        assert!(wp.bindings.is_empty());
        assert!(wp.roots.is_empty());
        // Round-trip an empty WorkloadPlan as JSON.
        let json = serde_json::to_string(&wp).unwrap();
        let back: WorkloadPlan = serde_json::from_str(&json).unwrap();
        assert!(back.bindings.is_empty());
        assert!(back.roots.is_empty());
    }
}
