//! Sketch index — Phase 5 of the controller-into-backend refactor (2026-05).
//!
//! Two-level index that the SimpleStore migrates to. Replaces the
//! aggregation_id-keyed lookup with a content-addressable design where
//! the index key is the `(raw_metric_name, group_by_keys, capability)`
//! tuple — represented compactly by the centrally-assigned `series_id`
//! (Phase 4) when one is available.
//!
//! Two levels:
//! - `instances`: sid → SketchInstanceMetadata (one entry per logical
//!   sketch instance — its metric name, group-by KEY set, capability,
//!   sketch_type, sketch_config, accuracy bound).
//! - `series`: sid → Vec<SketchTimeSeries> (per-series time-windowed
//!   sketch state; one entry per distinct group-by VALUES vector).
//!
//! Ghost sids (registered but never carrying state) are valid — they
//! exist when an agent registers a pre-merge identity that the gateway
//! folds into a different (post-merge) identity before backend ever sees
//! the sketch payload. Query path treats ghost sids as warm-tier MISS
//! and falls through to Thanos archive (Phase 6).
//!
//! See design doc §4.6 ("OTLP metadata model + backend store layout") at
//! `docs/design-controller-into-backend.md`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Capability the controller's plan made for this sketch instance.
/// Mirrors the design-doc Capability enum (§4.5). One Capability variant
/// per logical query family the warm tier can answer. The inner
/// `SketchKind` is the implementation choice (e.g. DDSketch vs KLL for
/// QuantileApprox); query routing keys on the variant, not the
/// implementation, so two CMS instances and one CountSketch instance
/// for the same metric-and-group-by all map to FrequencyTopk and the
/// query path picks any of them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    QuantileApprox(SketchKindHandle),
    CardinalityApprox,
    FrequencyTopk(SketchKindHandle),
    // Sum / Rate / LastOverTime are answered from raw counter via
    // Thanos forward; not represented as warm-tier capabilities.
}

/// Compact, hashable handle for sketch implementation choice.
/// Mirrors `controller::sketch_algebra::params::SketchKind` — duplicated
/// here as a thin enum so this module can be used independently of the
/// controller's full sketch algebra. The wire-format pdata variant tag
/// (`Metric.data_case`) maps 1:1 onto these handles at ingest time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SketchKindHandle {
    DDSketch,
    Kll,
    Hll,
    CountSketch,
    CountMin,
}

/// Sketch-instance configuration carried per-Metric on the OTLP wire
/// (Phase 2 lifted these from per-DP up to the parent sketch container).
/// Backend reads the relevant variant at ingest time and stores it in
/// `SketchInstanceMetadata.sketch_config`.
#[derive(Debug, Clone)]
pub enum SketchConfig {
    DDSketch { relative_accuracy: f64 },
    Kll { k: u32 },
    Hll { precision: u32 },
    CountSketch { rows: i32, cols: i32 },
    CountMin { rows: i32, cols: i32 },
}

/// Accuracy bound derived from `SketchConfig`. Surfaced to the user via
/// query response metadata so they know the precision / confidence of
/// each result.
#[derive(Debug, Clone, Copy)]
pub struct AccuracyBound {
    /// Approximate error bound (e.g. DDSketch's α, HLL's std-error).
    pub epsilon: f64,
    /// Probability of staying within `epsilon` (1.0 - δ).
    pub confidence: f64,
}

impl AccuracyBound {
    /// Compute accuracy bound from sketch config. Variant-specific
    /// formulas; backends can present this to PromQL response headers
    /// (e.g. `X-ASAP-Accuracy: 0.01`) so callers know the warm-tier
    /// answer's error envelope.
    pub fn from_config(cfg: &SketchConfig) -> Self {
        match cfg {
            // DDSketch's relative-accuracy α IS the epsilon; confidence
            // is 1.0 (deterministic bucket placement).
            SketchConfig::DDSketch { relative_accuracy } => Self {
                epsilon: *relative_accuracy,
                confidence: 1.0,
            },
            // KLL's rank error: ε ≈ 1 / k, confidence 99% by default.
            SketchConfig::Kll { k } => Self {
                epsilon: if *k > 0 { 1.0 / (*k as f64) } else { 1.0 },
                confidence: 0.99,
            },
            // HLL std error: σ ≈ 1.04 / sqrt(2^precision); 1σ ~ 68%
            // confidence. Surface the std-error as epsilon.
            SketchConfig::Hll { precision } => Self {
                epsilon: 1.04 / ((1u64 << *precision) as f64).sqrt(),
                confidence: 0.68,
            },
            // Count-Sketch: ε ≈ √(e/cols) (L2 estimation), δ ≈
            // 1/(2^(rows/2)).
            SketchConfig::CountSketch { rows, cols } => {
                let e = std::f64::consts::E;
                let eps = if *cols > 0 { (e / (*cols as f64)).sqrt() } else { 1.0 };
                let half_rows = (*rows as f64) / 2.0;
                let delta = 2f64.powf(-half_rows);
                Self { epsilon: eps, confidence: 1.0 - delta }
            }
            // CMS: ε ≈ e/cols, δ ≈ exp(-rows).
            SketchConfig::CountMin { rows, cols } => {
                let e = std::f64::consts::E;
                let eps = if *cols > 0 { e / (*cols as f64) } else { 1.0 };
                let delta = (-(*rows as f64)).exp();
                Self { epsilon: eps, confidence: 1.0 - delta }
            }
        }
    }
}

/// Metadata for one logical sketch instance, keyed by `series_id`.
/// Populated at ingest time when a sketch DataPoint with a fresh sid
/// arrives (or `(metric, attrs)` produces a fresh sid via the
/// SeriesIdResolver). Subsequent emits of the same sid append to the
/// associated `SketchTimeSeries` without re-touching this metadata.
#[derive(Debug, Clone)]
pub struct SketchInstanceMetadata {
    pub sid: u64,
    pub metric_name: String,
    /// The group-by KEY set — `dp.attributes.keys()` after the agent's
    /// `AggregateBy` rollup folded other labels into the sketch state.
    pub group_by_keys: BTreeSet<String>,
    pub capability: Capability,
    pub sketch_kind: SketchKindHandle,
    pub sketch_config: SketchConfig,
    pub accuracy: AccuracyBound,
    pub first_seen_unix_ms: i64,
}

/// Per-series time-windowed sketch state. One `SketchTimeSeries` per
/// distinct group-by VALUES vector under a single sid.
#[derive(Debug, Default)]
pub struct SketchTimeSeries {
    pub sid: u64,
    /// The group-by VALUES (one value per key in
    /// `SketchInstanceMetadata.group_by_keys`).
    pub series_label_values: BTreeMap<String, String>,
    /// `window_end_unix_ms → sketch payload bytes + encoding tag`.
    pub samples: BTreeMap<i64, SketchSampleState>,
}

#[derive(Debug, Clone)]
pub struct SketchSampleState {
    pub bytes: Vec<u8>,
    /// Wire-encoding hint from the OTLP DataPoint's `encoding` field
    /// (PROTO / PROTO_DELTA / MSGPACK / MSGPACK_DELTA).
    pub encoding: SketchEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SketchEncoding {
    ProtoFull,
    ProtoDelta,
    MsgpackFull,
    MsgpackDelta,
}

/// Two-level sketch index. Replaces the legacy `aggregation_id`-keyed
/// SimpleStore lookup once Phase 5 wiring lands at the streaming engine
/// ingest path and the query path.
#[derive(Debug, Default)]
pub struct SketchIndex {
    /// sid → metadata. May contain ghost sids (registered identities
    /// whose state was merged away by an upstream gateway before
    /// reaching this backend).
    pub instances: HashMap<u64, SketchInstanceMetadata>,
    /// sid → per-series time-windowed state. Empty `Vec` (or absent
    /// key) for ghost sids — query path detects this and falls through
    /// to Thanos archive.
    pub series: HashMap<u64, Vec<SketchTimeSeries>>,
}

/// Three possible outcomes of looking up a sid in the SketchIndex.
/// Query path uses this enum to drive routing decisions:
/// - `Hit`: warm-tier sketch has data — evaluate.
/// - `Ghost`: backend knows the identity (metadata is present) but no
///   sketch state ever arrived under this sid — fall through to Thanos
///   for raw archive. See design doc §5.4 ("Ghost sids").
/// - `Unknown`: sid not registered. Sender's cache is stale; respond
///   with `unknown_series_ids` so sender re-emits with attributes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SidLookup {
    Hit,
    Ghost,
    Unknown,
}

impl SketchIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn classify(&self, sid: u64) -> SidLookup {
        match (self.instances.get(&sid), self.series.get(&sid)) {
            (Some(_), Some(series)) if !series.is_empty() => SidLookup::Hit,
            (Some(_), _) => SidLookup::Ghost,
            (None, _) => SidLookup::Unknown,
        }
    }

    /// Insert metadata for a freshly-resolved sid.
    pub fn register(&mut self, meta: SketchInstanceMetadata) {
        self.instances.insert(meta.sid, meta);
    }

    /// Append a window's sketch state under `sid`. Caller is responsible
    /// for ensuring the corresponding `SketchInstanceMetadata` was
    /// registered (or the sketch arrives orphan and the caller chooses
    /// to drop / reject / register-on-the-fly).
    pub fn append_sample(
        &mut self,
        sid: u64,
        series_label_values: BTreeMap<String, String>,
        window_end_unix_ms: i64,
        sample: SketchSampleState,
    ) {
        let series_vec = self.series.entry(sid).or_default();
        // Find or create the SketchTimeSeries for this label-values vector.
        let ts = match series_vec
            .iter_mut()
            .find(|s| s.series_label_values == series_label_values)
        {
            Some(s) => s,
            None => {
                series_vec.push(SketchTimeSeries {
                    sid,
                    series_label_values,
                    samples: BTreeMap::new(),
                });
                series_vec.last_mut().unwrap()
            }
        };
        ts.samples.insert(window_end_unix_ms, sample);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ghost_classification() {
        let mut idx = SketchIndex::new();
        let meta = SketchInstanceMetadata {
            sid: 42,
            metric_name: "m".into(),
            group_by_keys: BTreeSet::new(),
            capability: Capability::QuantileApprox(SketchKindHandle::DDSketch),
            sketch_kind: SketchKindHandle::DDSketch,
            sketch_config: SketchConfig::DDSketch { relative_accuracy: 0.01 },
            accuracy: AccuracyBound::from_config(&SketchConfig::DDSketch {
                relative_accuracy: 0.01,
            }),
            first_seen_unix_ms: 0,
        };
        idx.register(meta);
        // Metadata exists but no series state — ghost.
        assert_eq!(idx.classify(42), SidLookup::Ghost);
        // Unregistered sid — unknown.
        assert_eq!(idx.classify(999), SidLookup::Unknown);
    }

    #[test]
    fn hit_after_append() {
        let mut idx = SketchIndex::new();
        let cfg = SketchConfig::Hll { precision: 14 };
        let meta = SketchInstanceMetadata {
            sid: 7,
            metric_name: "m".into(),
            group_by_keys: BTreeSet::new(),
            capability: Capability::CardinalityApprox,
            sketch_kind: SketchKindHandle::Hll,
            sketch_config: cfg.clone(),
            accuracy: AccuracyBound::from_config(&cfg),
            first_seen_unix_ms: 0,
        };
        idx.register(meta);
        idx.append_sample(
            7,
            BTreeMap::new(),
            1000,
            SketchSampleState { bytes: vec![1, 2, 3], encoding: SketchEncoding::ProtoFull },
        );
        assert_eq!(idx.classify(7), SidLookup::Hit);
    }

    #[test]
    fn ddsketch_accuracy_bound() {
        let bound = AccuracyBound::from_config(&SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        });
        assert!((bound.epsilon - 0.01).abs() < 1e-9);
        assert!((bound.confidence - 1.0).abs() < 1e-9);
    }
}
