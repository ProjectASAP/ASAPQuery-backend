//! `BackendPlan` — the typed control-plane → data-plane wire contract.
//!
//! See `control_plane/docs/design-backend-plan-wire-format.md` for the
//! full design. This module is the wire-types half only (§3 of that
//! doc): the `BackendPlan`/`Materialization`/`RoutingEntry` domain types,
//! their proto encoding (`proto` submodule, generated from
//! `proto/backend_plan.proto`), and the conversions between them.
//!
//! Deliberately reuses this deployment's existing canonical vocabulary
//! rather than re-encoding it: `planner_types::post_asap::{SummaryKind, SummaryParams}`
//! for the materialization payload (no separate `ExactAggregate` variant —
//! see the design doc §3 for why), `asap_ir`/`crate::intent_algebra`'s
//! `Source`/`ColumnRef`/`WindowKind` for the L3 IR fragments,
//! `crate::sketch_algebra::capability::Capability` for routing, and
//! `asap_types::{PolicyFingerprint, MonitorSpec}` for the two types
//! already shared with `data_plane` for exactly this cross-crate reason.
//!
//! Not yet wired into `emit/`, `main.rs`'s planning path, or any
//! `data_plane` consumer — see `RoutingIndex` (design doc §4), which is
//! what actually reads this at query time, landing separately.

pub mod proto {
    #![allow(clippy::all)]
    include!(concat!(
        env!("OUT_DIR"),
        "/control_plane.backend_plan.v1.rs"
    ));
}

mod from_stage_config;
pub use from_stage_config::from_stage_config;

use std::collections::HashMap;

use asap_types::{AggregationType, MonitorSpec, PolicyFingerprint};
use asap_types::{SummaryKind, SummaryParams};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::intent_algebra::{ColumnRef, Source, WindowKind};
use crate::sketch_algebra::capability::{Capability, SketchKindHandle};

/// Errors decoding a `BackendPlan` (or one of its parts) from its proto
/// wire form. Encoding (`From<&T> for proto::T`) is always infallible —
/// every domain type is a strict subset of what the wire schema can
/// represent — but decoding a proto message built from *bytes* can
/// always fail (a missing `oneof`, an out-of-range enum value from a
/// future wire version, ...).
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("prost decode failed: {0}")]
    Prost(#[from] prost::DecodeError),
    #[error("{0} missing its oneof field")]
    MissingOneof(&'static str),
    #[error("unspecified/unknown enum value {value} for {field}")]
    UnknownEnumValue { field: &'static str, value: i32 },
}

// ── WindowSpec ───────────────────────────────────────────────────────────────

/// `kind`/`size`/`slide` triple — mirrors `QueryExpr::Window`'s fields
/// without carrying the rest of that node (a `Materialization` names a
/// window shape, not an L3 subtree).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub size_ms: u64,
    pub slide_ms: Option<u64>,
}

impl From<&WindowSpec> for proto::WindowSpec {
    fn from(w: &WindowSpec) -> Self {
        proto::WindowSpec {
            kind: proto::WindowKind::from(w.kind) as i32,
            size_ms: w.size_ms,
            slide_ms: w.slide_ms,
        }
    }
}

impl TryFrom<proto::WindowSpec> for WindowSpec {
    type Error = DecodeError;
    fn try_from(w: proto::WindowSpec) -> Result<Self, DecodeError> {
        let kind =
            proto::WindowKind::try_from(w.kind).map_err(|_| DecodeError::UnknownEnumValue {
                field: "WindowSpec.kind",
                value: w.kind,
            })?;
        Ok(WindowSpec {
            kind: kind.try_into()?,
            size_ms: w.size_ms,
            slide_ms: w.slide_ms,
        })
    }
}

impl From<WindowKind> for proto::WindowKind {
    fn from(k: WindowKind) -> Self {
        match k {
            WindowKind::Tumbling => proto::WindowKind::Tumbling,
            WindowKind::Sliding => proto::WindowKind::Sliding,
            WindowKind::Session => proto::WindowKind::Session,
        }
    }
}

impl TryFrom<proto::WindowKind> for WindowKind {
    type Error = DecodeError;
    fn try_from(k: proto::WindowKind) -> Result<Self, DecodeError> {
        match k {
            proto::WindowKind::Tumbling => Ok(WindowKind::Tumbling),
            proto::WindowKind::Sliding => Ok(WindowKind::Sliding),
            proto::WindowKind::Session => Ok(WindowKind::Session),
            proto::WindowKind::Unspecified => Err(DecodeError::UnknownEnumValue {
                field: "WindowKind",
                value: proto::WindowKind::Unspecified as i32,
            }),
        }
    }
}

// ── Source / ColumnRef ──────────────────────────────────────────────────────

impl From<&Source> for proto::Source {
    fn from(s: &Source) -> Self {
        use proto::source::Source as Wire;
        let source = match s {
            Source::TimeSeries { metric } => Wire::TimeSeriesMetric(metric.clone()),
            Source::Table { table_ref } => Wire::TableRef(table_ref.clone()),
        };
        proto::Source {
            source: Some(source),
        }
    }
}

impl TryFrom<proto::Source> for Source {
    type Error = DecodeError;
    fn try_from(s: proto::Source) -> Result<Self, DecodeError> {
        use proto::source::Source as Wire;
        match s.source.ok_or(DecodeError::MissingOneof("Source"))? {
            Wire::TimeSeriesMetric(metric) => Ok(Source::TimeSeries { metric }),
            Wire::TableRef(table_ref) => Ok(Source::Table { table_ref }),
        }
    }
}

impl From<&ColumnRef> for proto::ColumnRef {
    fn from(c: &ColumnRef) -> Self {
        use proto::column_ref::ColumnRef as Wire;
        let column_ref = match c {
            ColumnRef::Named(name) => Wire::Named(name.clone()),
            ColumnRef::Qualified { table, name } => Wire::Qualified(proto::QualifiedColumn {
                table: table.clone(),
                name: name.clone(),
            }),
            ColumnRef::SampleValue => Wire::SampleValue(true),
            ColumnRef::Wildcard => Wire::Wildcard(true),
        };
        proto::ColumnRef {
            column_ref: Some(column_ref),
        }
    }
}

impl TryFrom<proto::ColumnRef> for ColumnRef {
    type Error = DecodeError;
    fn try_from(c: proto::ColumnRef) -> Result<Self, DecodeError> {
        use proto::column_ref::ColumnRef as Wire;
        match c.column_ref.ok_or(DecodeError::MissingOneof("ColumnRef"))? {
            Wire::Named(name) => Ok(ColumnRef::Named(name)),
            Wire::Qualified(q) => Ok(ColumnRef::Qualified {
                table: q.table,
                name: q.name,
            }),
            Wire::SampleValue(_) => Ok(ColumnRef::SampleValue),
            Wire::Wildcard(_) => Ok(ColumnRef::Wildcard),
        }
    }
}

// ── SummaryKind / SummaryParams ─────────────────────────────────────────────
//
// `SummaryParams`'s wire form is a single self-describing `oneof` --
// `SummaryKind` is always recoverable from which arm is set, so encoding
// only ever needs `&SummaryParams`. Decoding produces the `(SummaryKind,
// SummaryParams)` pair `Materialization` needs.

impl From<&SummaryParams> for proto::SummaryParams {
    fn from(p: &SummaryParams) -> Self {
        use proto::summary_params::Params as Wire;
        let params = match p {
            SummaryParams::Sum => Wire::Sum(true),
            SummaryParams::Count => Wire::Count(true),
            SummaryParams::MinMax => Wire::MinMax(true),
            SummaryParams::Increase => Wire::Increase(true),
            SummaryParams::Rate => Wire::Rate(true),
            SummaryParams::Kll { k } => Wire::Kll(proto::KllParams { k: *k }),
            SummaryParams::Cms { width, depth } => Wire::Cms(proto::CmsParams {
                width: *width,
                depth: *depth,
            }),
            SummaryParams::Hll { precision } => Wire::Hll(proto::HllParams {
                precision: *precision as u32,
            }),
            SummaryParams::DDSketch { alpha } => {
                Wire::Ddsketch(proto::DdSketchParams { alpha: *alpha })
            }
            SummaryParams::CmsWithHeap {
                width,
                depth,
                heap_size,
            } => Wire::CmsWithHeap(proto::CmsWithHeapParams {
                width: *width,
                depth: *depth,
                heap_size: *heap_size,
            }),
            SummaryParams::Kmv { k } => Wire::Kmv(proto::KmvParams { k: *k }),
            SummaryParams::Theta { k } => Wire::Theta(proto::ThetaParams { k: *k }),
            SummaryParams::CountSketch { width, depth } => {
                Wire::CountSketch(proto::CountSketchParams {
                    width: *width,
                    depth: *depth,
                })
            }
            SummaryParams::CountSketchWithHeap {
                width,
                depth,
                heap_size,
            } => Wire::CountSketchWithHeap(proto::CountSketchWithHeapParams {
                width: *width,
                depth: *depth,
                heap_size: *heap_size,
            }),
        };
        proto::SummaryParams {
            params: Some(params),
        }
    }
}

/// Decode a wire `SummaryParams` back to the `(SummaryKind, SummaryParams)`
/// pair. Not a `TryFrom` impl because the single wire message decodes to
/// TWO domain values (`SummaryKind` is implied, not carried separately on
/// the wire) — see this module's doc.
pub fn decode_summary_params(
    p: proto::SummaryParams,
) -> Result<(SummaryKind, SummaryParams), DecodeError> {
    use proto::summary_params::Params as Wire;
    let params = p.params.ok_or(DecodeError::MissingOneof("SummaryParams"))?;
    Ok(match params {
        Wire::Sum(_) => (SummaryKind::Sum, SummaryParams::Sum),
        Wire::Count(_) => (SummaryKind::Count, SummaryParams::Count),
        Wire::MinMax(_) => (SummaryKind::MinMax, SummaryParams::MinMax),
        Wire::Increase(_) => (SummaryKind::Increase, SummaryParams::Increase),
        Wire::Rate(_) => (SummaryKind::Rate, SummaryParams::Rate),
        Wire::Kll(k) => (SummaryKind::Kll, SummaryParams::Kll { k: k.k }),
        Wire::Cms(c) => (
            SummaryKind::Cms,
            SummaryParams::Cms {
                width: c.width,
                depth: c.depth,
            },
        ),
        Wire::Hll(h) => (
            SummaryKind::Hll,
            SummaryParams::Hll {
                precision: h.precision as u8,
            },
        ),
        Wire::Ddsketch(d) => (
            SummaryKind::DDSketch,
            SummaryParams::DDSketch { alpha: d.alpha },
        ),
        Wire::CmsWithHeap(c) => (
            SummaryKind::CmsWithHeap,
            SummaryParams::CmsWithHeap {
                width: c.width,
                depth: c.depth,
                heap_size: c.heap_size,
            },
        ),
        Wire::Kmv(k) => (SummaryKind::Kmv, SummaryParams::Kmv { k: k.k }),
        Wire::Theta(t) => (SummaryKind::Theta, SummaryParams::Theta { k: t.k }),
        Wire::CountSketch(c) => (
            SummaryKind::CountSketch,
            SummaryParams::CountSketch {
                width: c.width,
                depth: c.depth,
            },
        ),
        Wire::CountSketchWithHeap(c) => (
            SummaryKind::CountSketchWithHeap,
            SummaryParams::CountSketchWithHeap {
                width: c.width,
                depth: c.depth,
                heap_size: c.heap_size,
            },
        ),
    })
}

// ── SketchKindHandle / AggregationType / Capability ─────────────────────────

impl From<SketchKindHandle> for proto::SketchKindHandle {
    fn from(h: SketchKindHandle) -> Self {
        match h {
            SketchKindHandle::DDSketch => proto::SketchKindHandle::Ddsketch,
            SketchKindHandle::Kll => proto::SketchKindHandle::Kll,
            SketchKindHandle::Hll => proto::SketchKindHandle::Hll,
            SketchKindHandle::CountSketch => proto::SketchKindHandle::CountSketch,
            SketchKindHandle::CountMin => proto::SketchKindHandle::CountMin,
            SketchKindHandle::CmsWithHeap => proto::SketchKindHandle::CmsWithHeap,
            SketchKindHandle::CountSketchWithHeap => proto::SketchKindHandle::CountSketchWithHeap,
            SketchKindHandle::Any => proto::SketchKindHandle::Any,
        }
    }
}

impl TryFrom<proto::SketchKindHandle> for SketchKindHandle {
    type Error = DecodeError;
    fn try_from(h: proto::SketchKindHandle) -> Result<Self, DecodeError> {
        match h {
            proto::SketchKindHandle::Ddsketch => Ok(SketchKindHandle::DDSketch),
            proto::SketchKindHandle::Kll => Ok(SketchKindHandle::Kll),
            proto::SketchKindHandle::Hll => Ok(SketchKindHandle::Hll),
            proto::SketchKindHandle::CountSketch => Ok(SketchKindHandle::CountSketch),
            proto::SketchKindHandle::CountMin => Ok(SketchKindHandle::CountMin),
            proto::SketchKindHandle::CmsWithHeap => Ok(SketchKindHandle::CmsWithHeap),
            proto::SketchKindHandle::CountSketchWithHeap => {
                Ok(SketchKindHandle::CountSketchWithHeap)
            }
            proto::SketchKindHandle::Any => Ok(SketchKindHandle::Any),
            proto::SketchKindHandle::Unspecified => Err(DecodeError::UnknownEnumValue {
                field: "SketchKindHandle",
                value: proto::SketchKindHandle::Unspecified as i32,
            }),
        }
    }
}

impl From<AggregationType> for proto::AggregationType {
    fn from(a: AggregationType) -> Self {
        match a {
            AggregationType::Sum => proto::AggregationType::Sum,
            AggregationType::Increase => proto::AggregationType::Increase,
            AggregationType::MinMax => proto::AggregationType::MinMax,
            AggregationType::DatasketchesKLL => proto::AggregationType::DatasketchesKll,
            AggregationType::MultipleSum => proto::AggregationType::MultipleSum,
            AggregationType::MultipleIncrease => proto::AggregationType::MultipleIncrease,
            AggregationType::MultipleMinMax => proto::AggregationType::MultipleMinMax,
            AggregationType::HydraKLL => proto::AggregationType::HydraKll,
            AggregationType::CountMinSketch => proto::AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap => {
                proto::AggregationType::CountMinSketchWithHeap
            }
            AggregationType::CountSketch => proto::AggregationType::CountSketch,
            AggregationType::CountSketchWithHeap => proto::AggregationType::CountSketchWithHeap,
            AggregationType::HLL => proto::AggregationType::Hll,
            AggregationType::DDSketch => proto::AggregationType::Ddsketch,
            AggregationType::SingleSubpopulation => proto::AggregationType::SingleSubpopulation,
            AggregationType::MultipleSubpopulation => proto::AggregationType::MultipleSubpopulation,
        }
    }
}

impl TryFrom<proto::AggregationType> for AggregationType {
    type Error = DecodeError;
    fn try_from(a: proto::AggregationType) -> Result<Self, DecodeError> {
        match a {
            proto::AggregationType::Sum => Ok(AggregationType::Sum),
            proto::AggregationType::Increase => Ok(AggregationType::Increase),
            proto::AggregationType::MinMax => Ok(AggregationType::MinMax),
            proto::AggregationType::DatasketchesKll => Ok(AggregationType::DatasketchesKLL),
            proto::AggregationType::MultipleSum => Ok(AggregationType::MultipleSum),
            proto::AggregationType::MultipleIncrease => Ok(AggregationType::MultipleIncrease),
            proto::AggregationType::MultipleMinMax => Ok(AggregationType::MultipleMinMax),
            proto::AggregationType::HydraKll => Ok(AggregationType::HydraKLL),
            proto::AggregationType::CountMinSketch => Ok(AggregationType::CountMinSketch),
            proto::AggregationType::CountMinSketchWithHeap => {
                Ok(AggregationType::CountMinSketchWithHeap)
            }
            proto::AggregationType::CountSketch => Ok(AggregationType::CountSketch),
            proto::AggregationType::CountSketchWithHeap => Ok(AggregationType::CountSketchWithHeap),
            proto::AggregationType::Hll => Ok(AggregationType::HLL),
            proto::AggregationType::Ddsketch => Ok(AggregationType::DDSketch),
            proto::AggregationType::SingleSubpopulation => Ok(AggregationType::SingleSubpopulation),
            proto::AggregationType::MultipleSubpopulation => {
                Ok(AggregationType::MultipleSubpopulation)
            }
            proto::AggregationType::Unspecified => Err(DecodeError::UnknownEnumValue {
                field: "AggregationType",
                value: proto::AggregationType::Unspecified as i32,
            }),
        }
    }
}

impl From<&Capability> for proto::Capability {
    fn from(c: &Capability) -> Self {
        use proto::capability::Capability as Wire;
        let capability = match c {
            Capability::QuantileApprox(h) => {
                Wire::QuantileApprox(proto::SketchKindHandle::from(*h) as i32)
            }
            Capability::CardinalityApprox => Wire::CardinalityApprox(true),
            Capability::FrequencyEstimate(h) => {
                Wire::FrequencyEstimate(proto::SketchKindHandle::from(*h) as i32)
            }
            Capability::FrequencyTopk(h) => {
                Wire::FrequencyTopk(proto::SketchKindHandle::from(*h) as i32)
            }
            Capability::ExactAgg(a) => Wire::ExactAgg(proto::AggregationType::from(*a) as i32),
        };
        proto::Capability {
            capability: Some(capability),
        }
    }
}

impl TryFrom<proto::Capability> for Capability {
    type Error = DecodeError;
    fn try_from(c: proto::Capability) -> Result<Self, DecodeError> {
        use proto::capability::Capability as Wire;
        let decode_handle =
            |v: i32, field: &'static str| -> Result<SketchKindHandle, DecodeError> {
                proto::SketchKindHandle::try_from(v)
                    .map_err(|_| DecodeError::UnknownEnumValue { field, value: v })?
                    .try_into()
            };
        match c
            .capability
            .ok_or(DecodeError::MissingOneof("Capability"))?
        {
            Wire::QuantileApprox(v) => Ok(Capability::QuantileApprox(decode_handle(
                v,
                "Capability.quantile_approx",
            )?)),
            Wire::CardinalityApprox(_) => Ok(Capability::CardinalityApprox),
            Wire::FrequencyEstimate(v) => Ok(Capability::FrequencyEstimate(decode_handle(
                v,
                "Capability.frequency_estimate",
            )?)),
            Wire::FrequencyTopk(v) => Ok(Capability::FrequencyTopk(decode_handle(
                v,
                "Capability.frequency_topk",
            )?)),
            Wire::ExactAgg(v) => {
                let agg = proto::AggregationType::try_from(v).map_err(|_| {
                    DecodeError::UnknownEnumValue {
                        field: "Capability.exact_agg",
                        value: v,
                    }
                })?;
                Ok(Capability::ExactAgg(agg.try_into()?))
            }
        }
    }
}

// ── StorageBackend (deployment-local; control_plane cannot depend on
//    data_plane::storage_engines::types::StorageBackend, which this
//    mirrors -- see that type's own doc and asap_types::MonitorSpec's for
//    the same constraint) ───────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    #[default]
    SketchStore,
    GorillaObjectStore,
    DoubleWrite,
    PrometheusRemote,
}

impl From<StorageBackend> for proto::StorageBackend {
    fn from(b: StorageBackend) -> Self {
        match b {
            StorageBackend::SketchStore => proto::StorageBackend::SketchStore,
            StorageBackend::GorillaObjectStore => proto::StorageBackend::GorillaObjectStore,
            StorageBackend::DoubleWrite => proto::StorageBackend::DoubleWrite,
            StorageBackend::PrometheusRemote => proto::StorageBackend::PrometheusRemote,
        }
    }
}

impl TryFrom<proto::StorageBackend> for StorageBackend {
    type Error = DecodeError;
    fn try_from(b: proto::StorageBackend) -> Result<Self, DecodeError> {
        match b {
            proto::StorageBackend::SketchStore => Ok(StorageBackend::SketchStore),
            proto::StorageBackend::GorillaObjectStore => Ok(StorageBackend::GorillaObjectStore),
            proto::StorageBackend::DoubleWrite => Ok(StorageBackend::DoubleWrite),
            proto::StorageBackend::PrometheusRemote => Ok(StorageBackend::PrometheusRemote),
            proto::StorageBackend::Unspecified => Err(DecodeError::UnknownEnumValue {
                field: "StorageBackend",
                value: proto::StorageBackend::Unspecified as i32,
            }),
        }
    }
}

// ── RetentionPolicy ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetentionPolicy {
    pub num_aggregates_to_retain: Option<u32>,
}

impl From<&RetentionPolicy> for proto::RetentionPolicy {
    fn from(r: &RetentionPolicy) -> Self {
        proto::RetentionPolicy {
            num_aggregates_to_retain: r.num_aggregates_to_retain,
        }
    }
}

impl From<proto::RetentionPolicy> for RetentionPolicy {
    fn from(r: proto::RetentionPolicy) -> Self {
        RetentionPolicy {
            num_aggregates_to_retain: r.num_aggregates_to_retain,
        }
    }
}

// ── MonitorSpec ──────────────────────────────────────────────────────────────

impl From<&MonitorSpec> for proto::MonitorSpec {
    fn from(m: &MonitorSpec) -> Self {
        proto::MonitorSpec {
            agg_id: m.agg_id,
            functional: m.functional.clone(),
            key: m.key.clone(),
            tau: m.tau,
            epsilon: m.epsilon,
            window_ms: m.window_ms,
            d: m.d as u64,
            w: m.w as u64,
            mode: m.mode.clone(),
        }
    }
}

impl From<proto::MonitorSpec> for MonitorSpec {
    fn from(m: proto::MonitorSpec) -> Self {
        MonitorSpec {
            agg_id: m.agg_id,
            functional: m.functional,
            key: m.key,
            tau: m.tau,
            epsilon: m.epsilon,
            window_ms: m.window_ms,
            d: m.d as usize,
            w: m.w as usize,
            mode: m.mode,
        }
    }
}

// ── Materialization ──────────────────────────────────────────────────────────

/// One materialization `control_plane` has decided `data_plane` should
/// build/maintain. `kind`/`params` together are the exact pair
/// `SummaryExecutor::find_candidates` matches on — see this crate's
/// design doc §3 for why there's no separate exact-vs-approximate arm.
#[derive(Debug, Clone, PartialEq)]
pub struct Materialization {
    pub fingerprint: PolicyFingerprint,
    pub source: Source,
    pub window: WindowSpec,
    pub group_by: Vec<String>,
    pub rollup: Vec<String>,
    pub kind: SummaryKind,
    pub params: SummaryParams,
    pub col: ColumnRef,
    pub retention: Option<RetentionPolicy>,
}

impl From<&Materialization> for proto::Materialization {
    fn from(m: &Materialization) -> Self {
        proto::Materialization {
            fingerprint: m.fingerprint.0,
            source: Some((&m.source).into()),
            window: Some((&m.window).into()),
            group_by: m.group_by.clone(),
            rollup: m.rollup.clone(),
            params: Some((&m.params).into()),
            col: Some((&m.col).into()),
            retention: m.retention.as_ref().map(Into::into),
        }
    }
}

impl TryFrom<proto::Materialization> for Materialization {
    type Error = DecodeError;
    fn try_from(m: proto::Materialization) -> Result<Self, DecodeError> {
        let (kind, params) = decode_summary_params(
            m.params
                .ok_or(DecodeError::MissingOneof("Materialization.params"))?,
        )?;
        Ok(Materialization {
            fingerprint: PolicyFingerprint(m.fingerprint),
            source: m
                .source
                .ok_or(DecodeError::MissingOneof("Materialization.source"))?
                .try_into()?,
            window: m
                .window
                .ok_or(DecodeError::MissingOneof("Materialization.window"))?
                .try_into()?,
            group_by: m.group_by,
            rollup: m.rollup,
            kind,
            params,
            col: m
                .col
                .ok_or(DecodeError::MissingOneof("Materialization.col"))?
                .try_into()?,
            retention: m.retention.map(Into::into),
        })
    }
}

// ── RoutingEntry ─────────────────────────────────────────────────────────────

/// One "this capability, for this metric, is answered by that
/// materialization" fact. Kept as its own table rather than folded into
/// `Materialization` 1:1 — see design doc §3.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutingEntry {
    pub satisfies: Capability,
    pub materialization: PolicyFingerprint,
    pub storage_backend: StorageBackend,
}

impl From<&RoutingEntry> for proto::RoutingEntry {
    fn from(r: &RoutingEntry) -> Self {
        proto::RoutingEntry {
            satisfies: Some((&r.satisfies).into()),
            materialization: r.materialization.0,
            storage_backend: proto::StorageBackend::from(r.storage_backend) as i32,
        }
    }
}

impl TryFrom<proto::RoutingEntry> for RoutingEntry {
    type Error = DecodeError;
    fn try_from(r: proto::RoutingEntry) -> Result<Self, DecodeError> {
        let storage_backend = proto::StorageBackend::try_from(r.storage_backend).map_err(|_| {
            DecodeError::UnknownEnumValue {
                field: "RoutingEntry.storage_backend",
                value: r.storage_backend,
            }
        })?;
        Ok(RoutingEntry {
            satisfies: r
                .satisfies
                .ok_or(DecodeError::MissingOneof("RoutingEntry.satisfies"))?
                .try_into()?,
            materialization: PolicyFingerprint(r.materialization),
            storage_backend: storage_backend.try_into()?,
        })
    }
}

// ── BackendPlan ──────────────────────────────────────────────────────────────

/// The message `control_plane` pushes to `data_plane`'s backend process.
/// See this module's doc + the design doc for the full rationale.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BackendPlan {
    /// Observability only, not identity (mirrors `plan_streaming_config`'s
    /// existing convention for the pre-`BackendPlan` wire format).
    pub plan_id: u64,
    pub generated_at_unix_ms: u64,
    pub materializations: HashMap<PolicyFingerprint, Materialization>,
    pub routing: Vec<RoutingEntry>,
    pub monitors: Vec<MonitorSpec>,
}

impl From<&BackendPlan> for proto::BackendPlan {
    fn from(p: &BackendPlan) -> Self {
        proto::BackendPlan {
            plan_id: p.plan_id,
            generated_at_unix_ms: p.generated_at_unix_ms,
            materializations: p
                .materializations
                .iter()
                .map(|(fp, m)| (fp.0, m.into()))
                .collect(),
            routing: p.routing.iter().map(Into::into).collect(),
            monitors: p.monitors.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::BackendPlan> for BackendPlan {
    type Error = DecodeError;
    fn try_from(p: proto::BackendPlan) -> Result<Self, DecodeError> {
        let materializations = p
            .materializations
            .into_iter()
            .map(|(fp, m)| Ok((PolicyFingerprint(fp), Materialization::try_from(m)?)))
            .collect::<Result<HashMap<_, _>, DecodeError>>()?;
        let routing = p
            .routing
            .into_iter()
            .map(RoutingEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BackendPlan {
            plan_id: p.plan_id,
            generated_at_unix_ms: p.generated_at_unix_ms,
            materializations,
            routing,
            monitors: p.monitors.into_iter().map(Into::into).collect(),
        })
    }
}

impl BackendPlan {
    /// Encode to the wire-format bytes `data_plane` consumes.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        proto::BackendPlan::from(self).encode_to_vec()
    }

    /// Decode from wire bytes. `Err` covers both a malformed proto
    /// stream and a structurally-incomplete message (a missing `oneof`,
    /// an enum value this build doesn't recognize).
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        BackendPlan::try_from(proto::BackendPlan::decode(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_materialization(
        fingerprint: u64,
        kind: SummaryKind,
        params: SummaryParams,
    ) -> Materialization {
        Materialization {
            fingerprint: PolicyFingerprint(fingerprint),
            source: Source::TimeSeries {
                metric: "http_requests_total".to_string(),
            },
            window: WindowSpec {
                kind: WindowKind::Sliding,
                size_ms: Duration::from_secs(300).as_millis() as u64,
                slide_ms: None,
            },
            group_by: vec!["zone".to_string()],
            rollup: vec![],
            kind,
            params,
            col: ColumnRef::SampleValue,
            retention: Some(RetentionPolicy {
                num_aggregates_to_retain: Some(1000),
            }),
        }
    }

    fn sample_plan() -> BackendPlan {
        let mut materializations = HashMap::new();
        // Approximate sketch.
        materializations.insert(
            PolicyFingerprint(1),
            sample_materialization(1, SummaryKind::Kll, SummaryParams::Kll { k: 200 }),
        );
        // Exact accumulator -- same `SummaryKind`/`SummaryParams` vocabulary,
        // no separate wire representation (design doc §3).
        materializations.insert(
            PolicyFingerprint(2),
            sample_materialization(2, SummaryKind::Sum, SummaryParams::Sum),
        );

        BackendPlan {
            plan_id: 42,
            generated_at_unix_ms: 1_735_000_000_000,
            materializations,
            routing: vec![
                RoutingEntry {
                    satisfies: Capability::QuantileApprox(SketchKindHandle::Any),
                    materialization: PolicyFingerprint(1),
                    storage_backend: StorageBackend::SketchStore,
                },
                RoutingEntry {
                    satisfies: Capability::ExactAgg(AggregationType::Sum),
                    materialization: PolicyFingerprint(2),
                    storage_backend: StorageBackend::SketchStore,
                },
            ],
            monitors: vec![MonitorSpec {
                agg_id: 2,
                functional: "sum".to_string(),
                key: String::new(),
                tau: 100.0,
                epsilon: 0.05,
                window_ms: 60_000,
                d: 0,
                w: 0,
                mode: String::new(),
            }],
        }
    }

    #[test]
    fn round_trips_a_plan_with_both_exact_and_approximate_materializations() {
        let plan = sample_plan();
        let bytes = plan.encode_to_vec();
        let decoded = BackendPlan::decode(&bytes).expect("decode must succeed");
        assert_eq!(decoded, plan);
    }

    #[test]
    fn kll_survives_round_trip_with_kind_and_params_agreeing() {
        let m = sample_materialization(1, SummaryKind::Kll, SummaryParams::Kll { k: 200 });
        let wire = proto::Materialization::from(&m);
        let back = Materialization::try_from(wire).expect("decode must succeed");
        assert_eq!(back.kind, SummaryKind::Kll);
        assert_eq!(back.params, SummaryParams::Kll { k: 200 });
        assert_eq!(back, m);
    }

    #[test]
    fn sum_is_exact_and_carries_no_tuning_parameters() {
        let m = sample_materialization(2, SummaryKind::Sum, SummaryParams::Sum);
        let wire = proto::Materialization::from(&m);
        let back = Materialization::try_from(wire).expect("decode must succeed");
        assert!(back.kind.is_exact());
        assert_eq!(back, m);
    }

    #[test]
    fn every_summary_kind_round_trips() {
        let cases = [
            (SummaryKind::Sum, SummaryParams::Sum),
            (SummaryKind::Count, SummaryParams::Count),
            (SummaryKind::MinMax, SummaryParams::MinMax),
            (SummaryKind::Increase, SummaryParams::Increase),
            (SummaryKind::Rate, SummaryParams::Rate),
            (SummaryKind::Kll, SummaryParams::Kll { k: 200 }),
            (
                SummaryKind::Cms,
                SummaryParams::Cms {
                    width: 64,
                    depth: 4,
                },
            ),
            (SummaryKind::Hll, SummaryParams::Hll { precision: 14 }),
            (
                SummaryKind::DDSketch,
                SummaryParams::DDSketch { alpha: 0.01 },
            ),
            (
                SummaryKind::CmsWithHeap,
                SummaryParams::CmsWithHeap {
                    width: 64,
                    depth: 4,
                    heap_size: 10,
                },
            ),
            (SummaryKind::Kmv, SummaryParams::Kmv { k: 1024 }),
            (SummaryKind::Theta, SummaryParams::Theta { k: 1024 }),
            (
                SummaryKind::CountSketch,
                SummaryParams::CountSketch {
                    width: 64,
                    depth: 4,
                },
            ),
            (
                SummaryKind::CountSketchWithHeap,
                SummaryParams::CountSketchWithHeap {
                    width: 64,
                    depth: 4,
                    heap_size: 10,
                },
            ),
        ];
        for (kind, params) in cases {
            let wire = proto::SummaryParams::from(&params);
            let (decoded_kind, decoded_params) =
                decode_summary_params(wire).expect("decode must succeed");
            assert_eq!(decoded_kind, kind, "kind mismatch for {params:?}");
            assert_eq!(decoded_params, params);
        }
    }

    #[test]
    fn every_capability_variant_round_trips() {
        let cases = [
            Capability::QuantileApprox(SketchKindHandle::DDSketch),
            Capability::QuantileApprox(SketchKindHandle::Any),
            Capability::CardinalityApprox,
            Capability::FrequencyEstimate(SketchKindHandle::CountMin),
            Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap),
            Capability::ExactAgg(AggregationType::Sum),
            Capability::ExactAgg(AggregationType::MultipleSubpopulation),
        ];
        for cap in cases {
            let wire = proto::Capability::from(&cap);
            let back = Capability::try_from(wire).expect("decode must succeed");
            assert_eq!(back, cap, "round-trip mismatch for {cap:?}");
        }
    }

    #[test]
    fn every_storage_backend_round_trips() {
        for backend in [
            StorageBackend::SketchStore,
            StorageBackend::GorillaObjectStore,
            StorageBackend::DoubleWrite,
            StorageBackend::PrometheusRemote,
        ] {
            let wire = proto::StorageBackend::from(backend);
            let back = StorageBackend::try_from(wire).expect("decode must succeed");
            assert_eq!(back, backend);
        }
    }

    #[test]
    fn source_and_column_ref_round_trip() {
        let sources = [
            Source::TimeSeries {
                metric: "m".to_string(),
            },
            Source::Table {
                table_ref: "t".to_string(),
            },
        ];
        for s in sources {
            let wire = proto::Source::from(&s);
            let back = Source::try_from(wire).expect("decode must succeed");
            assert_eq!(back, s);
        }

        let columns = [
            ColumnRef::Named("service".to_string()),
            ColumnRef::Qualified {
                table: "t".to_string(),
                name: "c".to_string(),
            },
            ColumnRef::SampleValue,
            ColumnRef::Wildcard,
        ];
        for c in columns {
            let wire = proto::ColumnRef::from(&c);
            let back = ColumnRef::try_from(wire).expect("decode must succeed");
            assert_eq!(back, c);
        }
    }

    #[test]
    fn missing_oneof_decodes_to_an_error_not_a_panic() {
        let empty = proto::SummaryParams { params: None };
        assert!(decode_summary_params(empty).is_err());

        let empty_cap = proto::Capability { capability: None };
        assert!(Capability::try_from(empty_cap).is_err());
    }
}
