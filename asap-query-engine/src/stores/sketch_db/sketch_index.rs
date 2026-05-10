//! Sketch index — Phase 5 of the controller-into-backend refactor (2026-05).
//!
//! Two-level index:
//! - `instances`: sid → SketchInstanceMetadata (one entry per logical
//!   sketch instance — its metric name, group-by KEY set, capability,
//!   sketch_type, sketch_config, accuracy bound).
//! - `series`: sid → per-sid storage (`SidStoreData`) carrying the
//!   per-window sketch state. Intern table per sid maps the group-by
//!   VALUES vector to a compact `LabelValuesId = u32`; columnar
//!   `MutableEpoch` + sealed-epoch ring delivers the legacy
//!   SimpleMapStore's six storage optimizations end-to-end.
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
use std::sync::{Arc, RwLock};

use dashmap::DashMap;

use super::epoch_columnar::{LabelValuesId, SidStoreData, TimestampRange};

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
    /// CMS-with-heap. Detected at the application level via the
    /// precompute_operators `count_min_sketch_with_heap_accumulator`
    /// flow — the OTLP `CountMinSketch` wire struct doesn't carry the
    /// heap natively, so the gateway/precompute layer marks the
    /// sid with this variant when the parent container's heap field
    /// is non-empty. The warm-tier reducer reads the heap directly
    /// when answering `topk` / `topk_over_time`.
    CmsWithHeap,
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
/// associated `SidStoreData` without re-touching this metadata.
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

/// Per-sample sketch state. Stored as the payload column inside the
/// per-sid `SidStoreData` columnar storage.
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

/// One materialized series row returned by the query path. Resolved
/// from the per-sid intern table at read time.
#[derive(Debug, Clone)]
pub struct SketchTimeSeries {
    pub sid: u64,
    pub series_label_values: BTreeMap<String, String>,
    /// `window_end_unix_ms → sketch payload`. BTreeMap so the query
    /// path can iterate in time order without an extra sort.
    pub samples: BTreeMap<i64, SketchSampleState>,
}

/// Per-sid storage value — wraps `SidStoreData` in an `RwLock` so the
/// outer DashMap stays read-mostly and per-sid writes don't block one
/// another.
type SidStore = Arc<RwLock<SidStoreData<BTreeMap<String, String>, SketchSampleState>>>;

/// Two-level sketch index. Replaces the legacy `aggregation_id`-keyed
/// SimpleStore lookup once Phase 5 wiring lands at the streaming engine
/// ingest path and the query path.
///
/// `instances` is keyed under a `RwLock<HashMap>` because the registration
/// rate is low (one write per first-seen sid) and reads dominate;
/// `series` is a `DashMap` because per-sid writes happen on every DP.
#[derive(Default)]
pub struct SketchIndex {
    /// sid → metadata. May contain ghost sids (registered identities
    /// whose state was merged away by an upstream gateway before
    /// reaching this backend).
    instances: RwLock<HashMap<u64, SketchInstanceMetadata>>,
    /// sid → per-sid columnar storage. Empty `SidStoreData` (or absent
    /// key) for ghost sids — query path detects this and falls through
    /// to Thanos archive.
    series: DashMap<u64, SidStore>,
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

    /// Classify a sid for query routing. See `SidLookup` for semantics.
    pub fn classify(&self, sid: u64) -> SidLookup {
        let known = self.instances.read().unwrap().contains_key(&sid);
        if !known {
            return SidLookup::Unknown;
        }
        match self.series.get(&sid) {
            Some(store) => {
                let g = store.read().unwrap();
                if !g.current_epoch.is_empty() || !g.sealed_epochs.is_empty() {
                    SidLookup::Hit
                } else {
                    SidLookup::Ghost
                }
            }
            None => SidLookup::Ghost,
        }
    }

    /// Insert metadata for a freshly-resolved sid.
    pub fn register(&self, meta: SketchInstanceMetadata) {
        self.instances.write().unwrap().insert(meta.sid, meta);
    }

    /// Look up the metadata for a sid (cloned because callers usually
    /// release the index lock before working with it).
    pub fn instance(&self, sid: u64) -> Option<SketchInstanceMetadata> {
        self.instances.read().unwrap().get(&sid).cloned()
    }

    /// Append a window's sketch state under `sid`. Caller is responsible
    /// for ensuring the corresponding `SketchInstanceMetadata` was
    /// registered (or the sketch arrives orphan and the caller chooses
    /// to drop / reject / register-on-the-fly).
    ///
    /// `window` is the OTLP DataPoint's `(start_time_unix_ms, time_unix_ms)`.
    pub fn append_sample(
        &self,
        sid: u64,
        series_label_values: BTreeMap<String, String>,
        window: TimestampRange,
        sample: SketchSampleState,
    ) {
        let store = self
            .series
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(SidStoreData::new())))
            .clone();
        let mut guard = store.write().unwrap();
        guard.insert(window, series_label_values, sample);
    }

    /// Range-query the warm-tier state for one sid. Window-end-keyed
    /// time series result, one entry per distinct group-by VALUES
    /// vector. `(start, end)` is the inclusive query window; entries
    /// whose `(window_start, window_end)` lies fully within the query
    /// range are returned.
    pub fn query_range(
        &self,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Vec<SketchTimeSeries> {
        let store = match self.series.get(&sid) {
            Some(s) => s.clone(),
            None => return Vec::new(),
        };
        let guard = store.write().unwrap(); // exact_query may build the lazy index
        let mut by_label_id: HashMap<LabelValuesId, BTreeMap<i64, SketchSampleState>> =
            HashMap::new();

        let mut buf: Vec<(TimestampRange, LabelValuesId, &SketchSampleState)> = Vec::new();
        guard
            .current_epoch
            .range_query_into(start_unix_ms, end_unix_ms, &mut buf);
        for (win, label_id, payload) in &buf {
            by_label_id
                .entry(*label_id)
                .or_default()
                .insert(win.1 as i64, (*payload).clone());
        }
        buf.clear();

        for sealed in guard.sealed_epochs.values() {
            sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
            for (win, label_id, payload) in &buf {
                by_label_id
                    .entry(*label_id)
                    .or_default()
                    .insert(win.1 as i64, (*payload).clone());
            }
            buf.clear();
        }

        by_label_id
            .into_iter()
            .map(|(label_id, samples)| {
                let label_values = guard
                    .intern
                    .resolve(label_id)
                    .cloned()
                    .unwrap_or_default();
                SketchTimeSeries {
                    sid,
                    series_label_values: label_values,
                    samples,
                }
            })
            .collect()
    }

    /// Find every registered sid whose instance matches `metric_name` and
    /// whose `group_by_keys` is a superset of (or equal to) the user's
    /// requested label-key set. Phase 5 query path uses this to pick
    /// candidate sids for warm-tier dispatch — a sid whose group-by KEYS
    /// don't cover the user's PromQL label matchers can't answer the
    /// query and must fall through to archive.
    ///
    /// Returns `Vec<u64>` rather than an iterator so callers can release
    /// the read lock immediately. The `instances` map is read-mostly
    /// (one write per first-seen sid), so taking the lock per query is
    /// inexpensive.
    pub fn instances_matching(
        &self,
        metric_name: &str,
        required_keys: &BTreeSet<String>,
    ) -> Vec<u64> {
        let g = self.instances.read().unwrap();
        g.iter()
            .filter(|(_, m)| {
                m.metric_name == metric_name && required_keys.is_subset(&m.group_by_keys)
            })
            .map(|(sid, _)| *sid)
            .collect()
    }

    /// Number of distinct sids carrying state (excludes ghosts).
    pub fn series_len(&self) -> usize {
        self.series.len()
    }

    /// Number of registered instances (includes ghosts).
    pub fn instance_count(&self) -> usize {
        self.instances.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(sid: u64) -> SketchInstanceMetadata {
        let cfg = SketchConfig::DDSketch { relative_accuracy: 0.01 };
        SketchInstanceMetadata {
            sid,
            metric_name: "m".into(),
            group_by_keys: BTreeSet::new(),
            capability: Capability::QuantileApprox(SketchKindHandle::DDSketch),
            sketch_kind: SketchKindHandle::DDSketch,
            sketch_config: cfg.clone(),
            accuracy: AccuracyBound::from_config(&cfg),
            first_seen_unix_ms: 0,
        }
    }

    fn sample(b: u8) -> SketchSampleState {
        SketchSampleState { bytes: vec![b], encoding: SketchEncoding::ProtoFull }
    }

    #[test]
    fn ghost_classification() {
        let idx = SketchIndex::new();
        idx.register(meta(42));
        assert_eq!(idx.classify(42), SidLookup::Ghost);
        assert_eq!(idx.classify(999), SidLookup::Unknown);
    }

    #[test]
    fn hit_after_append() {
        let idx = SketchIndex::new();
        idx.register(meta(7));
        idx.append_sample(7, BTreeMap::new(), (1000, 1010), sample(1));
        assert_eq!(idx.classify(7), SidLookup::Hit);
    }

    #[test]
    fn range_query_returns_distinct_series() {
        let idx = SketchIndex::new();
        idx.register(meta(11));
        let mut lv_a = BTreeMap::new();
        lv_a.insert("host".to_string(), "a".to_string());
        let mut lv_b = BTreeMap::new();
        lv_b.insert("host".to_string(), "b".to_string());

        idx.append_sample(11, lv_a.clone(), (0, 10), sample(1));
        idx.append_sample(11, lv_a.clone(), (10, 20), sample(2));
        idx.append_sample(11, lv_b.clone(), (10, 20), sample(3));
        idx.append_sample(11, lv_b.clone(), (20, 30), sample(4));

        let mut series = idx.query_range(11, 0, 30);
        series.sort_by(|x, y| x.series_label_values.cmp(&y.series_label_values));
        assert_eq!(series.len(), 2);

        let s_a = &series[0];
        assert_eq!(s_a.series_label_values, lv_a);
        assert_eq!(s_a.samples.len(), 2);
        assert_eq!(s_a.samples[&10].bytes, vec![1]);
        assert_eq!(s_a.samples[&20].bytes, vec![2]);

        let s_b = &series[1];
        assert_eq!(s_b.series_label_values, lv_b);
        assert_eq!(s_b.samples.len(), 2);
    }

    #[test]
    fn range_query_clips_to_window_bounds() {
        let idx = SketchIndex::new();
        idx.register(meta(13));
        let lv = BTreeMap::new();
        idx.append_sample(13, lv.clone(), (0, 10), sample(1));
        idx.append_sample(13, lv.clone(), (10, 20), sample(2));
        idx.append_sample(13, lv.clone(), (20, 30), sample(3));

        // Only the middle window is fully within [5, 25].
        let series = idx.query_range(13, 5, 25);
        assert_eq!(series.len(), 1);
        let s = &series[0];
        assert_eq!(s.samples.len(), 1);
        assert!(s.samples.contains_key(&20));
    }

    #[test]
    fn ddsketch_accuracy_bound() {
        let bound = AccuracyBound::from_config(&SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        });
        assert!((bound.epsilon - 0.01).abs() < 1e-9);
        assert!((bound.confidence - 1.0).abs() < 1e-9);
    }

    #[test]
    fn epoch_rotation_is_visible_to_query() {
        let idx = SketchIndex::new();
        idx.register(meta(17));

        // Force aggressive rotation by touching the SidStoreData
        // capacity *after* the entry is created. We do this by
        // first appending one sample to materialize the entry, then
        // mutating its config, then appending more.
        idx.append_sample(17, BTreeMap::new(), (0, 10), sample(1));
        if let Some(s) = idx.series.get(&17) {
            let mut g = s.write().unwrap();
            g.epoch_capacity = Some(2);
            g.max_epochs = 4;
        }
        idx.append_sample(17, BTreeMap::new(), (10, 20), sample(2));
        idx.append_sample(17, BTreeMap::new(), (20, 30), sample(3));
        idx.append_sample(17, BTreeMap::new(), (30, 40), sample(4));

        // All four windows should still be query-visible across the
        // mutable + sealed boundary.
        let series = idx.query_range(17, 0, 40);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples.len(), 4);
    }
}
