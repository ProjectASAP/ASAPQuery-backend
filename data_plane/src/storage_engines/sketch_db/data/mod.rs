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
//! - [`AggKind`] — `Sketch { kind, config, spatial_filter_canonical }
//!   | ExactAgg { agg_type, parameters_canonical,
//!   spatial_filter_canonical }`. The discriminator that decides
//!   what shape a sid's payload takes. The `spatial_filter_canonical`
//!   field on each variant participates in sid identity so
//!   filter-distinct policies don't collide.
//! - [`AggPayload`] — `Sketch(SketchSampleState) | ExactAgg(Box<dyn
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
//! - sid minting is no longer in this module — `SeriesIdResolver`
//!   (in `drivers::ingest::series_resolver`) is the single mint
//!   authority for every sid in the system. `AggKind::canonical_string()`
//!   here produces the third element of the resolver's cache key.
//! - [`canonical_parameters`] — helper that renders a parameters
//!   `HashMap` into the canonical string form
//!   `AggKind::ExactAgg::parameters_canonical` expects.
//!
//! ## Re-exports for callers
//!
//! - [`Capability`] / [`SketchKindHandle`] — the canonical
//!   control-plane-side capability vocabulary.
//! - [`AggregationType`] — the agg-type enum that
//!   `AggKind::ExactAgg` carries.

// `xxhash_rust::xxh64` import retired alongside `compute_sid` (PR-4).
// Sid minting is now registry-allocated via `SeriesIdResolver` — no
// content-addressed hash is computed at this layer.

// ── Capability re-exports ────────────────────────────────────────────────────
//
// Step 2a consolidated all capability state into
// `control_plane::sketch_algebra::capability`. The backend re-exports the
// canonical types so there's exactly one definition in the codebase.
// `is_satisfied_by` (used by the engine ASAP-tier hook) lives on the
// control-plane-side `Capability` impl.

pub use control_plane::sketch_algebra::{Capability, SketchKindHandle};

/// Re-export so callers don't need to depend on promql_utilities
/// directly for the agg_type tag.
pub use asap_types::AggregationType;

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
/// — sids cover BOTH opaque-sketch state and exact-aggregation state
/// (Sum / Count / Avg / Rate / MinMax / SetAggregator / …), so a
/// single store can host both.
///
/// The sid resolver distinguishes the two branches structurally:
/// a `Sketch` sid keys on `(metric, attrs, sketch_kind, sketch_config,
/// spatial_filter)`; an `ExactAgg` sid keys on `(metric, attrs,
/// agg_type, parameters, spatial_filter)`. The spatial_filter
/// participates in identity so two policies differing only in
/// their ingest-side label predicate (e.g. `status="200"` vs
/// `status="500"`) mint distinct sids even after the filter
/// dimension is projected out by group-by.
#[derive(Debug, Clone)]
pub enum AggKind {
    /// Opaque sketch state — DDSketch, KLL, HLL, CountMin, CountSketch.
    /// Payload at storage layer is an encoded byte string
    /// (`SketchSampleState`).
    Sketch {
        kind: SketchKindHandle,
        config: SketchConfig,
        /// Canonical form of the policy's spatial-filter predicate,
        /// produced by [`asap_types::utils::normalize_spatial_filter`]
        /// (e.g. `{status="200",zone="us-east"}`). Empty string when
        /// the policy has no spatial filter.
        spatial_filter_canonical: String,
    },
    /// Exact aggregation state — Sum, Count, Avg, Rate, MinMax,
    /// SetAggregator, etc. Payload at storage layer is whatever the
    /// per-accumulator serializer produces.
    ///
    /// Variant name history: previously `AggKind::Precompute` — the
    /// rename to `ExactAgg` (PR `refactor/sid-identity-spatial-filter-and-rename`)
    /// reflects that `precompute_engine/` is the *engine* that
    /// produces both sketch and exact-aggregation outputs, while this
    /// variant names what the *payload* is. The two had been
    /// confusingly conflated.
    ExactAgg {
        agg_type: AggregationType,
        /// Stable canonical encoding of the agg's
        /// `parameters: HashMap<String, Value>` — sorted keys, each
        /// value rendered via serde_json. Held as a single owned
        /// string so equality / hashing stay cheap and independent of
        /// the original HashMap's iteration order.
        parameters_canonical: String,
        /// Canonical form of the policy's spatial-filter predicate;
        /// see the `Sketch` variant's field doc.
        spatial_filter_canonical: String,
    },
}

/// Render a `HashMap<String, Value>` of parameters into the canonical
/// string form `AggKind::ExactAgg::parameters_canonical` expects.
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

impl AggKind {
    /// Stable string form of this `AggKind`, used as the third element
    /// of the `SeriesIdResolver` cache key and as the `agg_kind_canonical`
    /// field in the resolver's WAL.
    ///
    /// Sid identity is `(metric, attrs_fingerprint, agg_kind_canonical)`.
    /// Two `AggKind`s that compare equal MUST produce the same
    /// canonical string; two that differ in any observable parameter
    /// MUST produce different strings.
    ///
    /// Format — always a `:filter=...` suffix (empty after `=` means
    /// no spatial filter), so consumers can parse without knowing
    /// whether a filter exists:
    /// - `Sketch { DDSketch, α=0.01, no filter }`
    ///   → `"sketch:DDSketch:D:0.01:filter="`
    /// - `Sketch { Kll, k=200, status=200 }`
    ///   → `"sketch:Kll:K:200:filter={status=\"200\"}"`
    /// - `ExactAgg { Sum, no params, no filter }`
    ///   → `"exact_agg:Sum::filter="`
    /// - `ExactAgg { DatasketchesKLL, k=200, zone=us-east }`
    ///   → `"exact_agg:DatasketchesKLL:k=200;:filter={zone=\"us-east\"}"`
    pub fn canonical_string(&self) -> String {
        match self {
            AggKind::Sketch {
                kind,
                config,
                spatial_filter_canonical,
            } => {
                format!(
                    "sketch:{}:{}:filter={}",
                    sketch_kind_canonical(*kind),
                    sketch_config_canonical(config),
                    spatial_filter_canonical,
                )
            }
            AggKind::ExactAgg {
                agg_type,
                parameters_canonical,
                spatial_filter_canonical,
            } => {
                // `AggregationType`'s `Display` impl is stable
                // (matches the snake-case form on the wire) and
                // `parameters_canonical` is already canonicalized
                // upstream (see [`canonical_parameters`]).
                format!(
                    "exact_agg:{}:{}:filter={}",
                    agg_type, parameters_canonical, spatial_filter_canonical,
                )
            }
        }
    }
}

fn sketch_kind_canonical(k: SketchKindHandle) -> &'static str {
    match k {
        SketchKindHandle::DDSketch => "DDSketch",
        SketchKindHandle::Kll => "Kll",
        SketchKindHandle::Hll => "Hll",
        SketchKindHandle::CountSketch => "CountSketch",
        SketchKindHandle::CountMin => "CountMin",
        SketchKindHandle::CmsWithHeap => "CmsWithHeap",
        SketchKindHandle::CountSketchWithHeap => "CountSketchWithHeap",
        // `Any` is the analysis-time wildcard; never reaches the
        // ingest path which detects a concrete kind from the OTLP
        // wire variant. Mapping it to a unique tag anyway keeps the
        // canonical form total.
        SketchKindHandle::Any => "Any",
    }
}

fn sketch_config_canonical(cfg: &SketchConfig) -> String {
    match cfg {
        SketchConfig::DDSketch { relative_accuracy } => {
            format!("D:{relative_accuracy}")
        }
        SketchConfig::Kll { k } => format!("K:{k}"),
        SketchConfig::Hll { precision } => format!("H:{precision}"),
        SketchConfig::CountSketch { rows, cols } => format!("S:{rows}:{cols}"),
        SketchConfig::CountMin { rows, cols } => format!("M:{rows}:{cols}"),
    }
}

// ── sid hash ────────────────────────────────────────────────────────────────
//
// `compute_sid` was retired alongside `compute_sketch_sid` (PR-3) and
// the precompute ingest migration (PR-4). All sid minting now flows
// through `SeriesIdResolver` — one registry-allocated u64 per
// `(metric, attrs_fingerprint, agg_kind_canonical)` triple, shared
// across the OTel sketch ingest path and the precompute output path.
// See `AggKind::canonical_string` for the agg-kind canonicalization
// that replaces the byte-layout this hash used to produce.

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
    /// `X-ASAP-Accuracy: 0.01` so callers know the ASAP-tier answer's
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
    /// `window_end_unix_ms → sketch payload(s)`. BTreeMap so the query
    /// path can iterate in time order without an extra sort.
    ///
    /// The value is a `Vec` because the edge can emit MULTIPLE frames that
    /// all stamp the SAME `(window_start, window_end)` range — the
    /// sub-window delta_transmission case, where each sub-window emit
    /// carries the FULL window range rather than the sub-window slice. All
    /// such frames must survive read-back (the leading `Full`/seed
    /// establishes the rolling base; the trailing `Delta`s are increments
    /// onto it), in INSERTION ORDER. Keying by `window_end` alone and
    /// storing one payload silently dropped all but the last frame, which
    /// erased the base and produced empty / wildly-wrong warm-tier query
    /// answers under delta_transmission (the `fix/pwr-delta-query` bug).
    /// The reducer's `per_window_evaluate` / `cumulative_evaluate` already
    /// fold repeated-window-end frames correctly once they arrive.
    pub samples: std::collections::BTreeMap<i64, Vec<SketchSampleState>>,
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
    /// Exact-aggregation state — Sum / Count / Avg / Rate / MinMax.
    /// Cloned via the `Clone` impl on `Box<dyn AggregateCore>` (which
    /// dispatches through `clone_boxed_core`).
    ExactAgg(Box<dyn crate::storage_engines::types::AggregateCore>),
}

impl std::fmt::Debug for AggPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggPayload::Sketch(s) => f.debug_tuple("Sketch").field(s).finish(),
            AggPayload::ExactAgg(p) => f
                .debug_struct("ExactAgg")
                .field("type_name", &p.type_name())
                .finish(),
        }
    }
}

impl AggPayload {
    /// Return the sketch payload if this is a sketch variant; `None`
    /// otherwise. The ASAP-tier sketch reducer uses this to filter
    /// non-sketch payloads out of its `query_range` results.
    pub fn as_sketch(&self) -> Option<&SketchSampleState> {
        match self {
            AggPayload::Sketch(s) => Some(s),
            AggPayload::ExactAgg(_) => None,
        }
    }

    /// Return the exact-aggregation payload if this is an exact-agg
    /// variant; `None` otherwise. The exact-aggregation query path
    /// uses this.
    pub fn as_exact_agg(&self) -> Option<&dyn crate::storage_engines::types::AggregateCore> {
        match self {
            AggPayload::ExactAgg(p) => Some(p.as_ref()),
            AggPayload::Sketch(_) => None,
        }
    }

    /// Approximate in-memory byte footprint of the payload — used by
    /// the persistence layer's memory-pressure trigger. Sketch payloads
    /// report their byte buffer length (the dominant cost);
    /// exact-aggregation payloads forward to the accumulator's own
    /// `approx_memory_bytes()`.
    pub fn approx_bytes(&self) -> usize {
        match self {
            AggPayload::Sketch(s) => s.bytes.len() + std::mem::size_of::<SketchSampleState>(),
            AggPayload::ExactAgg(p) => p.approx_memory_bytes(),
        }
    }
}
