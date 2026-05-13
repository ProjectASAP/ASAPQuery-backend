//! Sketch-DB data taxonomy — payload types, the sketch-vs-precompute
//! kind discriminator, the canonical sid hash, and the accuracy-bound
//! derivation.
//!
//! This module is intentionally **stateless**: every type here is a
//! plain data carrier or a pure function. The `index/` module (sid
//! registry + per-sid columnar substrate) and `lifecycle/` module
//! (Active/Retired/Expired) layer state on top of these.
//!
//! ## What's here
//!
//! - [`AggKind`] — `Sketch { kind, config } | Precompute { agg_type,
//!   parameters_canonical }`. The discriminator that decides what
//!   shape a sid's payload takes.
//! - [`AggPayload`] — `Sketch(SketchSampleState) | Precompute(Box<dyn
//!   AggregateCore>)`. The actual byte/accumulator stored at each
//!   (sid, window, label_values) cell.
//! - [`SketchConfig`] — variant-specific tuning parameters
//!   (DDSketch's α, KLL's k, HLL's precision, etc.). Part of
//!   `AggKind::Sketch`.
//! - [`SketchSampleState`] — `bytes + encoding` carried by sketch
//!   payloads.
//! - [`SketchEncoding`] — wire-format hint (PROTO / PROTO_DELTA /
//!   MSGPACK / MSGPACK_DELTA).
//! - [`SketchTimeSeries`] — one materialized series row returned by
//!   the read path (sid + label values + per-window samples).
//! - [`AccuracyBound`] — `(epsilon, confidence)` derived from a
//!   `SketchConfig`. Surfaces in HTTP response headers.
//! - [`compute_sid`] / [`compute_sketch_sid`] — the canonical sid
//!   hash. Deterministic across hosts; defines the sid identity.
//! - [`canonical_parameters`] — helper that renders a parameters
//!   `HashMap` into the canonical string form
//!   `AggKind::Precompute::parameters_canonical` expects.
//!
//! ## Re-exports for callers
//!
//! - [`Capability`] / [`SketchKindHandle`] — the canonical
//!   controller-side capability vocabulary.
//! - [`AggregationType`] — the agg-type enum that
//!   `AggKind::Precompute` carries.

use xxhash_rust::xxh64::xxh64;

// ── Capability re-exports ────────────────────────────────────────────────────
//
// Step 2a consolidated all capability state into
// `controller::sketch_algebra::capability`. The backend re-exports the
// canonical types so there's exactly one definition in the codebase.
// `is_satisfied_by` (used by the engine warm-tier hook) lives on the
// controller-side `Capability` impl.

pub use controller::sketch_algebra::{Capability, SketchKindHandle};

/// Re-export so callers don't need to depend on promql_utilities
/// directly for the agg_type tag.
pub use promql_utilities::query_logics::enums::AggregationType;

// ── Payload taxonomy ────────────────────────────────────────────────────────

/// Sketch-instance configuration carried per-Metric on the OTLP wire
/// (Phase 2 lifted these from per-DP up to the parent sketch container).
/// Backend reads the relevant variant at ingest time and stores it in
/// `SketchInstanceMetadata.agg_kind`.
#[derive(Debug, Clone)]
pub enum SketchConfig {
    DDSketch { relative_accuracy: f64 },
    Kll { k: u32 },
    Hll { precision: u32 },
    CountSketch { rows: i32, cols: i32 },
    CountMin { rows: i32, cols: i32 },
}

/// What kind of aggregation a `sid` identifies. M2.3 generalization
/// — sids cover BOTH opaque-sketch state and partial-accumulator
/// (Sum / Count / Avg / Rate / MinMax) state, so a single store can
/// host both.
///
/// The sid hash distinguishes the two branches structurally: a
/// `Sketch` sid is content-addressed over `(metric, attrs,
/// sketch_kind, sketch_config)`; a `Precompute` sid is content-
/// addressed over `(metric, attrs, agg_type, parameters)`.
#[derive(Debug, Clone)]
pub enum AggKind {
    /// Opaque sketch state — DDSketch, KLL, HLL, CountMin, CountSketch.
    /// Payload at storage layer is an encoded byte string
    /// (`SketchSampleState`).
    Sketch {
        kind: SketchKindHandle,
        config: SketchConfig,
    },
    /// Partial-accumulator state — Sum, Count, Avg, Rate, MinMax,
    /// SetAggregator, etc. Payload at storage layer is whatever the
    /// per-accumulator serializer produces.
    Precompute {
        agg_type: AggregationType,
        /// Stable canonical encoding of the agg's
        /// `parameters: HashMap<String, Value>` — sorted keys, each
        /// value rendered via serde_json. Held as a single owned
        /// string so equality / hashing stay cheap and independent of
        /// the original HashMap's iteration order.
        parameters_canonical: String,
    },
}

/// Render a `HashMap<String, Value>` of parameters into the canonical
/// string form `AggKind::Precompute::parameters_canonical` expects.
/// Keys sorted lexicographically; each value via `serde_json`.
pub fn canonical_parameters(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    let sorted: std::collections::BTreeMap<&String, &serde_json::Value> =
        parameters.iter().collect();
    let mut buf = String::new();
    for (k, v) in sorted {
        buf.push_str(k);
        buf.push('=');
        buf.push_str(&serde_json::to_string(v).unwrap_or_default());
        buf.push(';');
    }
    buf
}

// ── sid hash ────────────────────────────────────────────────────────────────

/// Compute a deterministic `series_id` (sid) for one sketch instance.
///
/// Folds the four DataPoint inputs the backend has at ingest time —
/// `metric_name`, `attrs` (keys + values, canonicalized), `sketch_kind`,
/// and `sketch_config` — into a 64-bit xxhash. Same inputs always
/// produce the same sid across restarts and across hosts.
///
/// `attrs_fingerprint` MUST be the canonical fingerprint string
/// (`canonical_attrs_fingerprint` — keys sorted, joined `k=v;`); the
/// hash is sensitive to whitespace, ordering, and trailing separator.
///
/// sid=0 is reserved on the wire (means "unresolved"); if a real input
/// hashes to 0 (vanishingly unlikely with 64-bit xxhash), we perturb
/// to 1.
pub fn compute_sketch_sid(
    metric_name: &str,
    attrs_fingerprint: &str,
    sketch_kind: SketchKindHandle,
    sketch_config: &SketchConfig,
) -> u64 {
    compute_sid(
        metric_name,
        attrs_fingerprint,
        &AggKind::Sketch {
            kind: sketch_kind,
            config: sketch_config.clone(),
        },
    )
}

/// Generalized version of [`compute_sketch_sid`] covering both sketch
/// and precompute aggregations. Same `(metric, attrs, agg_kind)` tuple
/// always yields the same sid.
///
/// The two branches encode disjointly: a `Sketch` payload starts with
/// `sketch_kind_tag` (1..=7), while a `Precompute` payload starts with
/// the byte `b'P'` (ASCII 80), which no sketch tag will ever produce.
/// So an attacker (or a colliding hash input) can't force a sketch sid
/// to overlap a precompute sid at the encoding level.
///
/// Critically, the `Sketch` branch is bit-identical to the historical
/// `compute_sketch_sid` byte layout — existing sketch sids the M2
/// wire format introduced stay stable across this generalization.
pub fn compute_sid(
    metric_name: &str,
    attrs_fingerprint: &str,
    agg_kind: &AggKind,
) -> u64 {
    let mut buf: Vec<u8> =
        Vec::with_capacity(metric_name.len() + attrs_fingerprint.len() + 32);
    buf.extend_from_slice(metric_name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(attrs_fingerprint.as_bytes());
    buf.push(0);
    match agg_kind {
        AggKind::Sketch { kind, config } => {
            buf.push(sketch_kind_tag(*kind));
            buf.push(0);
            encode_sketch_config(config, &mut buf);
        }
        AggKind::Precompute {
            agg_type,
            parameters_canonical,
        } => {
            buf.push(b'P');
            buf.push(0);
            buf.extend_from_slice(agg_type.as_str().as_bytes());
            buf.push(0);
            buf.extend_from_slice(parameters_canonical.as_bytes());
        }
    }
    let h = xxh64(&buf, 0);
    if h == 0 {
        1
    } else {
        h
    }
}

fn sketch_kind_tag(k: SketchKindHandle) -> u8 {
    match k {
        SketchKindHandle::DDSketch => 1,
        SketchKindHandle::Kll => 2,
        SketchKindHandle::Hll => 3,
        SketchKindHandle::CountSketch => 4,
        SketchKindHandle::CountMin => 5,
        SketchKindHandle::CmsWithHeap => 6,
        SketchKindHandle::CountSketchWithHeap => 7,
        SketchKindHandle::Any => 0,
    }
}

fn encode_sketch_config(cfg: &SketchConfig, buf: &mut Vec<u8>) {
    match cfg {
        SketchConfig::DDSketch { relative_accuracy } => {
            buf.push(b'D');
            buf.extend_from_slice(&relative_accuracy.to_le_bytes());
        }
        SketchConfig::Kll { k } => {
            buf.push(b'K');
            buf.extend_from_slice(&k.to_le_bytes());
        }
        SketchConfig::Hll { precision } => {
            buf.push(b'H');
            buf.extend_from_slice(&precision.to_le_bytes());
        }
        SketchConfig::CountSketch { rows, cols } => {
            buf.push(b'S');
            buf.extend_from_slice(&rows.to_le_bytes());
            buf.extend_from_slice(&cols.to_le_bytes());
        }
        SketchConfig::CountMin { rows, cols } => {
            buf.push(b'M');
            buf.extend_from_slice(&rows.to_le_bytes());
            buf.extend_from_slice(&cols.to_le_bytes());
        }
    }
}

// ── Accuracy ────────────────────────────────────────────────────────────────

/// Accuracy bound derived from `SketchConfig`. Surfaced to the user
/// via query response metadata so they know the precision / confidence
/// of each result.
#[derive(Debug, Clone, Copy)]
pub struct AccuracyBound {
    /// Approximate error bound (e.g. DDSketch's α, HLL's std-error).
    pub epsilon: f64,
    /// Probability of staying within `epsilon` (1.0 - δ).
    pub confidence: f64,
}

impl AccuracyBound {
    /// Compute accuracy bound from sketch config. Variant-specific
    /// formulas; the HTTP layer can present this in
    /// `X-ASAP-Accuracy: 0.01` so callers know the warm-tier answer's
    /// error envelope.
    pub fn from_config(cfg: &SketchConfig) -> Self {
        match cfg {
            SketchConfig::DDSketch { relative_accuracy } => Self {
                epsilon: *relative_accuracy,
                confidence: 1.0,
            },
            SketchConfig::Kll { k } => Self {
                epsilon: if *k > 0 { 1.0 / (*k as f64) } else { 1.0 },
                confidence: 0.99,
            },
            SketchConfig::Hll { precision } => Self {
                epsilon: 1.04 / ((1u64 << *precision) as f64).sqrt(),
                confidence: 0.68,
            },
            SketchConfig::CountSketch { rows, cols } => {
                let e = std::f64::consts::E;
                let eps = if *cols > 0 {
                    (e / (*cols as f64)).sqrt()
                } else {
                    1.0
                };
                let half_rows = (*rows as f64) / 2.0;
                let delta = 2f64.powf(-half_rows);
                Self {
                    epsilon: eps,
                    confidence: 1.0 - delta,
                }
            }
            SketchConfig::CountMin { rows, cols } => {
                let e = std::f64::consts::E;
                let eps = if *cols > 0 { e / (*cols as f64) } else { 1.0 };
                let delta = (-(*rows as f64)).exp();
                Self {
                    epsilon: eps,
                    confidence: 1.0 - delta,
                }
            }
        }
    }
}

// ── Per-sample payload ──────────────────────────────────────────────────────

/// Per-sample sketch state. Stored as the payload column inside the
/// per-sid `SidStoreData` columnar storage.
#[derive(Debug, Clone)]
pub struct SketchSampleState {
    pub bytes: Vec<u8>,
    /// Wire-encoding hint from the OTLP DataPoint's `encoding` field.
    pub encoding: SketchEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SketchEncoding {
    ProtoFull,
    ProtoDelta,
    MsgpackFull,
    MsgpackDelta,
}

/// One materialized series row returned by the query path. Resolved
/// from the per-sid intern table at read time.
#[derive(Debug, Clone)]
pub struct SketchTimeSeries {
    pub sid: u64,
    pub series_label_values: std::collections::BTreeMap<String, String>,
    /// `window_end_unix_ms → sketch payload`. BTreeMap so the query
    /// path can iterate in time order without an extra sort.
    pub samples: std::collections::BTreeMap<i64, SketchSampleState>,
}

/// Unified payload variant — what one window of one (sid, label-values
/// vector) physically holds. Phase 5 M2.3 generalizes storage so the
/// same store can host both sketch state and partial-accumulator state
/// under content-addressed sids.
///
/// Each sid stores exactly one variant for its entire lifetime — the
/// variant is fixed by the sid's `agg_kind` at registration. Mixing
/// variants under one sid is a logic error the storage layer doesn't
/// guard against (a mismatch crashes the reducer at runtime). The
/// ingest path is responsible for not doing that.
#[derive(Clone)]
pub enum AggPayload {
    /// Opaque sketch state — see [`SketchSampleState`].
    Sketch(SketchSampleState),
    /// Partial-accumulator state — Sum / Count / Avg / Rate / MinMax.
    /// Cloned via the `Clone` impl on `Box<dyn AggregateCore>` (which
    /// dispatches through `clone_boxed_core`).
    Precompute(Box<dyn crate::storage_engines::types::AggregateCore>),
}

impl std::fmt::Debug for AggPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggPayload::Sketch(s) => f.debug_tuple("Sketch").field(s).finish(),
            AggPayload::Precompute(p) => f
                .debug_struct("Precompute")
                .field("type_name", &p.type_name())
                .finish(),
        }
    }
}

impl AggPayload {
    /// Return the sketch payload if this is a sketch variant; `None`
    /// otherwise. The warm-tier sketch reducer uses this to filter
    /// non-sketch payloads out of its `query_range` results.
    pub fn as_sketch(&self) -> Option<&SketchSampleState> {
        match self {
            AggPayload::Sketch(s) => Some(s),
            AggPayload::Precompute(_) => None,
        }
    }

    /// Return the precompute payload if this is a precompute variant;
    /// `None` otherwise. The precompute query path uses this.
    pub fn as_precompute(&self) -> Option<&dyn crate::storage_engines::types::AggregateCore> {
        match self {
            AggPayload::Precompute(p) => Some(p.as_ref()),
            AggPayload::Sketch(_) => None,
        }
    }

    /// Approximate in-memory byte footprint of the payload — used by
    /// the persistence layer's memory-pressure trigger. Sketch payloads
    /// report their byte buffer length (the dominant cost); precompute
    /// payloads forward to the accumulator's own `approx_memory_bytes()`.
    pub fn approx_bytes(&self) -> usize {
        match self {
            AggPayload::Sketch(s) => {
                s.bytes.len() + std::mem::size_of::<SketchSampleState>()
            }
            AggPayload::Precompute(p) => p.approx_memory_bytes(),
        }
    }
}
