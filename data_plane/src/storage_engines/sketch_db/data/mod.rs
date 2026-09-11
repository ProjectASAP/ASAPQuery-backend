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
//! - [`Capability`] / [`SketchAlgorithm`] — the canonical
//!   control-plane-side capability vocabulary.
//! - [`AggregationType`] — the agg-type enum that
//!   `AggKind::ExactAgg` carries.

// `xxhash_rust::xxh64` import retired alongside `compute_sid` (PR-4).
// Sid minting is now registry-allocated via `SeriesIdResolver` — no
// content-addressed hash is computed at this layer.

// ── Capability re-exports ────────────────────────────────────────────────────
//
// Step 2a consolidated all capability state into
// `control_plane::physical::runtime_capability`. The backend re-exports the
// canonical types so there's exactly one definition in the codebase.
// `is_satisfied_by` (used by the engine ASAP-tier hook) lives on the
// control-plane-side `Capability` impl.

pub use control_plane::physical::runtime_capability::{Capability, SketchAlgorithm};

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
    UnivMon {
        heap_size: u32,
        sketch_rows: u32,
        sketch_cols: u32,
        layers: u8,
    },
    DDSketch {
        relative_accuracy: f64,
    },
    Kll {
        k: u32,
    },
    Hll {
        precision: u32,
    },
    CountSketch {
        rows: i32,
        cols: i32,
    },
    CountMin {
        rows: i32,
        cols: i32,
    },
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
        algorithm: SketchAlgorithm,
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

/// Complete resolver identity for a configured materialization. All live and
/// replay paths must include policy semantics, not just the sketch family.
pub(crate) fn materialization_kind_for_config(
    config: &asap_types::aggregation_config::AggregationConfig,
) -> String {
    format!(
        "{}|{}",
        agg_kind_for_config(config).canonical_string(),
        config.policy_fingerprint()
    )
}

/// Resolve the physical state family produced by a precompute policy. This is
/// shared by SID minting and store registration so a sketch policy can never
/// be minted as `ExactAgg` and later registered as `Sketch` (or vice versa).
pub fn agg_kind_for_config(config: &asap_types::aggregation_config::AggregationConfig) -> AggKind {
    use planner_types::post_asap::{SketchAlgorithm as Algorithm, SketchParams, SummaryFamilyType};

    // HLL is intentionally absent from raw-value accumulator dispatch because
    // it arrives through SketchEnvelope ingest. It is still a sketch for SID
    // identity and store registration, so classify it before consulting the
    // accumulator factory contract.
    let sketch = (config.aggregation_type == AggregationType::HLL)
        .then(|| {
            let precision = config
                .parameters
                .get("precision")
                .or_else(|| config.parameters.get("p"))
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(14);
            AggKind::Sketch {
                algorithm: SketchAlgorithm::Hll,
                config: SketchConfig::Hll { precision },
                spatial_filter_canonical: config.spatial_filter_normalized.clone(),
            }
        })
        .or_else(|| {
            config.accumulator_spec().ok().and_then(|spec| {
                let SummaryFamilyType::Sketch(kind, _) = spec.family else {
                    return None;
                };
                let algorithm = kind.algorithm().clone();
                let physical = match (kind.algorithm(), kind.params()) {
                    (
                        Algorithm::UnivMon,
                        SketchParams::UnivMon {
                            heap_size,
                            sketch_rows,
                            sketch_cols,
                            layers,
                        },
                    ) => SketchConfig::UnivMon {
                        heap_size: *heap_size,
                        sketch_rows: *sketch_rows,
                        sketch_cols: *sketch_cols,
                        layers: *layers,
                    },
                    (Algorithm::DDSketch, SketchParams::DDSketch { alpha }) => {
                        SketchConfig::DDSketch {
                            relative_accuracy: *alpha,
                        }
                    }
                    (Algorithm::Kll, SketchParams::Kll { k }) => SketchConfig::Kll { k: *k },
                    (Algorithm::Hll, SketchParams::Hll { precision }) => SketchConfig::Hll {
                        precision: (*precision).into(),
                    },
                    (Algorithm::Cms, SketchParams::Cms { width, depth })
                    | (Algorithm::CmsWithHeap, SketchParams::CmsWithHeap { width, depth, .. }) => {
                        SketchConfig::CountMin {
                            rows: *depth as i32,
                            cols: *width as i32,
                        }
                    }
                    (Algorithm::CountSketch, SketchParams::CountSketch { width, depth })
                    | (
                        Algorithm::CountSketchWithHeap,
                        SketchParams::CountSketchWithHeap { width, depth, .. },
                    ) => SketchConfig::CountSketch {
                        rows: *depth as i32,
                        cols: *width as i32,
                    },
                    _ => return None,
                };
                Some(AggKind::Sketch {
                    algorithm,
                    config: physical,
                    spatial_filter_canonical: config.spatial_filter_normalized.clone(),
                })
            })
        });

    sketch.unwrap_or_else(|| AggKind::ExactAgg {
        agg_type: config.aggregation_type,
        parameters_canonical: canonical_parameters(&config.parameters),
        spatial_filter_canonical: config.spatial_filter_normalized.clone(),
    })
}

impl AggKind {
    /// Canonical summary-operator identity. Population filters belong to the
    /// SDS Data Descriptor and are intentionally excluded here.
    pub fn operator_canonical_string(&self) -> String {
        match self {
            Self::Sketch {
                algorithm, config, ..
            } => format!(
                "summary:v1:sketch:{}:{}",
                sketch_algorithm_canonical(algorithm.clone()),
                sketch_config_canonical(config)
            ),
            Self::ExactAgg {
                agg_type,
                parameters_canonical,
                ..
            } => format!("summary:v1:exact:{agg_type}:{parameters_canonical}"),
        }
    }

    pub fn spatial_filter_canonical(&self) -> &str {
        match self {
            Self::Sketch {
                spatial_filter_canonical,
                ..
            }
            | Self::ExactAgg {
                spatial_filter_canonical,
                ..
            } => spatial_filter_canonical,
        }
    }

    /// Runtime query capability and accuracy metadata implied by this state.
    pub fn capability_and_accuracy(&self) -> (Capability, Option<AccuracyBound>) {
        match self {
            Self::ExactAgg { agg_type, .. } => (Capability::ExactAgg(*agg_type), None),
            Self::Sketch {
                algorithm, config, ..
            } => {
                let capability = match algorithm {
                    SketchAlgorithm::DDSketch | SketchAlgorithm::Kll => {
                        Capability::QuantileApprox(Some(algorithm.clone()))
                    }
                    SketchAlgorithm::Hll | SketchAlgorithm::UnivMon => {
                        Capability::CardinalityApprox
                    }
                    SketchAlgorithm::Cms | SketchAlgorithm::CountSketch => {
                        Capability::FrequencyEstimate(Some(algorithm.clone()))
                    }
                    SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap => {
                        Capability::FrequencyTopk(Some(algorithm.clone()))
                    }
                    SketchAlgorithm::Kmv | SketchAlgorithm::Theta => Capability::CardinalityApprox,
                };
                (capability, Some(AccuracyBound::from_config(config)))
            }
        }
    }
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
                algorithm: kind,
                config,
                spatial_filter_canonical,
            } => {
                format!(
                    "sketch:{}:{}:filter={}",
                    sketch_algorithm_canonical(kind.clone()),
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

fn sketch_algorithm_canonical(k: SketchAlgorithm) -> &'static str {
    match k {
        SketchAlgorithm::DDSketch => "DDSketch",
        SketchAlgorithm::Kll => "Kll",
        SketchAlgorithm::Hll => "Hll",
        SketchAlgorithm::UnivMon => "UnivMon",
        SketchAlgorithm::CountSketch => "CountSketch",
        SketchAlgorithm::Cms => "CountMin",
        SketchAlgorithm::CmsWithHeap => "CmsWithHeap",
        SketchAlgorithm::CountSketchWithHeap => "CountSketchWithHeap",
        // `Any` is the analysis-time wildcard; never reaches the
        // ingest path which detects a concrete kind from the OTLP
        // wire variant. Mapping it to a unique tag anyway keeps the
        // canonical form total.
        SketchAlgorithm::Kmv => "Kmv",
        SketchAlgorithm::Theta => "Theta",
    }
}

fn sketch_config_canonical(cfg: &SketchConfig) -> String {
    match cfg {
        SketchConfig::UnivMon {
            heap_size,
            sketch_rows,
            sketch_cols,
            layers,
        } => format!("U:{heap_size}:{sketch_rows}:{sketch_cols}:{layers}"),
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
            // Family dimensions alone do not establish a readout error bound.
            SketchConfig::UnivMon { .. } => Self {
                epsilon: f64::MAX,
                confidence: 0.0,
            },
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
    /// Shared by range readers so querying thousands of panes does not clone
    /// every accumulator while holding the store lock.
    ExactAgg(std::sync::Arc<dyn crate::storage_engines::types::AggregateCore>),
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

    pub fn as_exact_agg_arc(
        &self,
    ) -> Option<&std::sync::Arc<dyn crate::storage_engines::types::AggregateCore>> {
        match self {
            AggPayload::ExactAgg(payload) => Some(payload),
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

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{enums::WindowKind, KeyByLabelNames};
    use std::collections::HashMap;

    #[test]
    fn hll_envelope_config_is_registered_as_a_sketch() {
        let config = asap_types::aggregation_config::AggregationConfig::new(
            AggregationType::HLL,
            String::new(),
            HashMap::from([("precision".to_string(), serde_json::json!(12))]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "unique_users".to_string(),
            None,
            None,
            None,
        );

        assert!(matches!(
            agg_kind_for_config(&config),
            AggKind::Sketch {
                algorithm: SketchAlgorithm::Hll,
                config: SketchConfig::Hll { precision: 12 },
                ..
            }
        ));
    }
}
