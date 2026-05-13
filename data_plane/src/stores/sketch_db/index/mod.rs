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
//!   SketchStore's six storage optimizations end-to-end.
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use xxhash_rust::xxh64::xxh64;

use self::epoch_columnar::{LabelValuesId, SidStoreData, TimestampRange};
use crate::stores::sketch_db::schema::AggStatus;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── Capability re-exports ────────────────────────────────────────────────────
//
// Step 2a consolidated all capability state into
// `controller::sketch_algebra::capability`. The backend no longer
// defines its own `Capability` / `SketchKindHandle`; it re-exports the
// canonical types so there's exactly one definition in the codebase.
// `is_satisfied_by` (used by the engine warm-tier hook) now lives on
// the controller-side `Capability` impl.

pub use controller::sketch_algebra::{Capability, SketchKindHandle};

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

/// What kind of aggregation a `sid` identifies. M2.3 generalization
/// — sids now cover BOTH opaque-sketch state and partial-accumulator
/// (Sum / Count / Avg / Rate / MinMax) state, so the future
/// unified store can host both.
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
        /// Stable canonical encoding of the agg's `parameters: HashMap<String, Value>`
        /// — sorted keys, each value rendered via serde_json. Held as
        /// a single owned string so equality / hashing stay cheap and
        /// independent of the original HashMap's iteration order.
        parameters_canonical: String,
    },
}

/// Re-export so callers don't need to depend on promql_utilities
/// for the agg_type tag.
pub use promql_utilities::query_logics::enums::AggregationType;

/// Render a `HashMap<String, Value>` of parameters into the canonical
/// string form `AggKind::Precompute::parameters_canonical` expects.
/// Keys sorted lexicographically; each value via `serde_json`.
pub fn canonical_parameters(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    let sorted: std::collections::BTreeMap<&String, &serde_json::Value> = parameters.iter().collect();
    let mut buf = String::new();
    for (k, v) in sorted {
        buf.push_str(k);
        buf.push('=');
        buf.push_str(&serde_json::to_string(v).unwrap_or_default());
        buf.push(';');
    }
    buf
}

/// Compute a deterministic `series_id` (sid) for one sketch instance.
///
/// Folds the four DataPoint inputs the backend has at ingest time —
/// `metric_name`, `attrs` (keys + values, canonicalized), `sketch_kind`,
/// and `sketch_config` — into a 64-bit xxhash. Same inputs always
/// produce the same sid across restarts and across hosts, so the
/// controller no longer needs to mint and emit an `aggregation_id` for
/// each (metric, agg-type, params) tuple. Phase 5 M2 directive.
///
/// `attrs_fingerprint` MUST be the canonical fingerprint string
/// (`canonical_attrs_fingerprint` — keys sorted, joined `k=v;`); the
/// hash is sensitive to whitespace, ordering, and trailing separator,
/// so callers must round-trip through that one function for the
/// pre-flight ResolveSeriesIDs RPC and the ingest path to agree.
///
/// sid=0 is reserved on the wire (means "unresolved"); if a real input
/// hashes to 0 (vanishingly unlikely with 64-bit xxhash), we perturb to
/// 1.
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
/// and precompute aggregations. Same `(metric, attrs, agg_kind)`
/// tuple always yields the same sid.
///
/// The two branches encode disjointly: a `Sketch` payload starts with
/// `sketch_kind_tag` (1..=7, see [`sketch_kind_tag`]), while a
/// `Precompute` payload starts with the byte `b'P'` (ASCII 80), which
/// no sketch tag will ever produce. So an attacker (or a colliding
/// hash input) can't force a sketch sid to overlap a precompute sid
/// at the encoding level.
///
/// Critically, the `Sketch` branch is bit-identical to what the
/// retired `compute_sketch_sid` function produced — same encoding,
/// same hash. Existing sketch sids the M2 wire format introduced
/// stay stable across this generalization.
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
            // Sketch branch — match `compute_sketch_sid`'s historical
            // byte layout exactly so M2-issued sketch sids survive.
            buf.push(sketch_kind_tag(*kind));
            buf.push(0);
            encode_sketch_config(config, &mut buf);
        }
        AggKind::Precompute {
            agg_type,
            parameters_canonical,
        } => {
            // Precompute branch starts with 'P' which can never be a
            // `sketch_kind_tag` (those live in 1..=7) — no collision.
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
            // CMS: ε ≈ e/cols, δ ≈ exp(-rows).
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

/// Metadata for one logical sketch instance, keyed by `series_id`.
/// Populated at ingest time when a sketch DataPoint with a fresh sid
/// arrives (or `(metric, attrs)` produces a fresh sid via the
/// SeriesIdResolver). Subsequent emits of the same sid append to the
/// associated `SidStoreData` without re-touching this metadata.
///
/// **Lifecycle fields** (Phase 5 M1): mirror `AggSchema`'s
/// `Active → Retired → Expired` state machine so the sid-keyed path
/// has the same write-side barrier semantics as the agg_id-keyed path.
/// `status()` is derived from `retired_at_ms` + `expires_at_ms` and
/// the wall clock — never stored directly. M2 cuts the ingest barrier
/// over from `SchemaRegistry::is_writable(agg_id)` to
/// `SketchStore::is_writable(sid)`; until then both registries run
/// side by side.
#[derive(Debug, Clone)]
pub struct SketchInstanceMetadata {
    pub sid: u64,
    pub metric_name: String,
    /// The group-by KEY set — `dp.attributes.keys()` after the agent's
    /// `AggregateBy` rollup folded other labels into the sketch state.
    pub group_by_keys: BTreeSet<String>,
    /// Warm-tier capability surfaced to the analyzer. For sketch-backed
    /// instances this is one of the `*Approx` variants; for precompute-
    /// backed instances (M2.3+) it's `None` because precomputes answer
    /// exact statistics — the analyzer routes them via `agg_kind` /
    /// `agg_type` instead.
    pub capability: Option<Capability>,
    /// M2.3 — the canonical "what kind of aggregation lives at this
    /// sid" descriptor. Replaces the M2-era `sketch_kind` +
    /// `sketch_config` field pair so a single registry can host both
    /// sketches and partial-accumulator (Sum/Count/Avg/Rate/MinMax)
    /// state.
    pub agg_kind: AggKind,
    /// Approximate accuracy bound — `Some` for sketch-backed sids,
    /// `None` for exact precomputes.
    pub accuracy: Option<AccuracyBound>,
    pub first_seen_unix_ms: i64,

    /// Wall-clock millis when the sid was retired (removed from the
    /// active config). `None` while `Active`. Mirrors
    /// `AggSchema::retired_at_ms`.
    pub retired_at_ms: Option<u64>,
    /// Wall-clock millis after which the sid's data may be deleted.
    /// `None` while `Active`. Set on retirement to
    /// `retired_at_ms + retention_ms`. Mirrors
    /// `AggSchema::expires_at_ms`.
    pub expires_at_ms: Option<u64>,
}

impl SketchInstanceMetadata {
    /// Compute the current `AggStatus` against the wall clock.
    /// Mirrors `AggSchema::status` — purely a function of timestamps.
    pub fn status(&self) -> AggStatus {
        let now = now_ms();
        match (self.retired_at_ms, self.expires_at_ms) {
            (None, _) => AggStatus::Active,
            (Some(_), Some(exp)) if now >= exp => AggStatus::Expired,
            (Some(_), _) => AggStatus::Retired,
        }
    }

    /// Whether this sid accepts writes. Equivalent to
    /// `status() == AggStatus::Active`. Phase 5 ingest barrier
    /// (M2 cutover) will call this in place of
    /// `SchemaRegistry::is_writable(agg_id)`.
    pub fn is_writable(&self) -> bool {
        matches!(self.status(), AggStatus::Active)
    }

    /// Mark the sid retired, scheduling expiry `retention` from now.
    /// Idempotent — re-retiring a Retired sid is a no-op.
    pub fn retire(&mut self, retention: Duration) {
        if self.retired_at_ms.is_some() {
            return;
        }
        let now = now_ms();
        self.retired_at_ms = Some(now);
        self.expires_at_ms = Some(now + retention.as_millis() as u64);
    }

    /// Sketch-handle accessor for the legacy sketch path. Returns
    /// `Some(handle)` iff this sid is sketch-backed; `None` for
    /// precompute-backed sids. Consumers that only meaningfully run on
    /// sketches (e.g. the warm-tier reducer) `.expect` it.
    pub fn sketch_kind(&self) -> Option<SketchKindHandle> {
        match &self.agg_kind {
            AggKind::Sketch { kind, .. } => Some(*kind),
            AggKind::Precompute { .. } => None,
        }
    }

    /// Sketch-config accessor mirroring [`Self::sketch_kind`].
    pub fn sketch_config(&self) -> Option<&SketchConfig> {
        match &self.agg_kind {
            AggKind::Sketch { config, .. } => Some(config),
            AggKind::Precompute { .. } => None,
        }
    }
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

/// Unified payload variant — what one window of one (sid, label-values
/// vector) physically holds. Phase 5 M2.3 generalizes the storage so
/// the same `SketchStore` can host both sketch state and partial-
/// accumulator (Sum / Count / Avg / Rate / MinMax) state under
/// content-addressed sids.
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
    Precompute(Box<dyn crate::stores::types::AggregateCore>),
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
    /// `None` otherwise. The precompute query path (M2.3.5) uses this.
    pub fn as_precompute(&self) -> Option<&dyn crate::stores::types::AggregateCore> {
        match self {
            AggPayload::Precompute(p) => Some(p.as_ref()),
            AggPayload::Sketch(_) => None,
        }
    }

    /// Approximate in-memory byte footprint of the payload — used by
    /// the persistence layer's memory-pressure trigger. Sketch
    /// payloads report their byte buffer length (the dominant cost);
    /// precompute payloads forward to the accumulator's own
    /// `approx_memory_bytes()`.
    pub fn approx_bytes(&self) -> usize {
        match self {
            AggPayload::Sketch(s) => {
                s.bytes.len() + std::mem::size_of::<SketchSampleState>()
            }
            AggPayload::Precompute(p) => p.approx_memory_bytes(),
        }
    }
}

/// Per-sid storage value — wraps `SidStoreData` in an `RwLock` so the
/// outer DashMap stays read-mostly and per-sid writes don't block one
/// another. Payload type is the unified [`AggPayload`] enum so one
/// `SketchStore` can host both sketches and precomputes.
type SidStore = Arc<RwLock<SidStoreData<BTreeMap<String, String>, AggPayload>>>;

/// Two-level sketch index. Replaces the legacy `aggregation_id`-keyed
/// SimpleStore lookup once Phase 5 wiring lands at the streaming engine
/// ingest path and the query path.
///
/// `instances` is keyed under a `RwLock<HashMap>` because the registration
/// rate is low (one write per first-seen sid) and reads dominate;
/// `series` is a `DashMap` because per-sid writes happen on every DP.
#[derive(Default)]
pub struct SketchStore {
    /// sid → metadata. May contain ghost sids (registered identities
    /// whose state was merged away by an upstream gateway before
    /// reaching this backend).
    instances: RwLock<HashMap<u64, SketchInstanceMetadata>>,
    /// sid → per-sid columnar storage. Empty `SidStoreData` (or absent
    /// key) for ghost sids — query path detects this and falls through
    /// to Thanos archive.
    series: DashMap<u64, SidStore>,
}

/// Three possible outcomes of looking up a sid in the SketchStore.
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

impl SketchStore {
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
        guard.insert(window, series_label_values, AggPayload::Sketch(sample));
    }

    /// Append a window's precompute (Sum/Count/Avg/Rate/MinMax) state
    /// under `sid`. Mirror of [`Self::append_sample`] for the
    /// precompute branch — Phase 5 M2.3.3.
    ///
    /// Caller invariant: `sid` was registered with
    /// `AggKind::Precompute { .. }`. Mixing sketch + precompute
    /// payloads under one sid is a logic error this layer doesn't
    /// guard against (it'll crash the reducer at runtime, not silently
    /// corrupt).
    pub fn append_precompute(
        &self,
        sid: u64,
        series_label_values: BTreeMap<String, String>,
        window: TimestampRange,
        payload: Box<dyn crate::stores::types::AggregateCore>,
    ) {
        let store = self
            .series
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(SidStoreData::new())))
            .clone();
        let mut guard = store.write().unwrap();
        guard.insert(window, series_label_values, AggPayload::Precompute(payload));
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

        let mut buf: Vec<(TimestampRange, LabelValuesId, &AggPayload)> = Vec::new();
        guard
            .current_epoch
            .range_query_into(start_unix_ms, end_unix_ms, &mut buf);
        for (win, label_id, payload) in &buf {
            // Filter to sketch-variant payloads only; precompute sids
            // (M2.3) are served via the precompute query path
            // (M2.3.5).
            if let Some(s) = payload.as_sketch() {
                by_label_id
                    .entry(*label_id)
                    .or_default()
                    .insert(win.1 as i64, s.clone());
            }
        }
        buf.clear();

        for sealed in guard.sealed_epochs.values() {
            sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
            for (win, label_id, payload) in &buf {
                if let Some(s) = payload.as_sketch() {
                    by_label_id
                        .entry(*label_id)
                        .or_default()
                        .insert(win.1 as i64, s.clone());
                }
            }
            buf.clear();
        }

        by_label_id
            .into_iter()
            .map(|(label_id, samples)| {
                let label_values = guard.intern.resolve(label_id).cloned().unwrap_or_default();
                SketchTimeSeries {
                    sid,
                    series_label_values: label_values,
                    samples,
                }
            })
            .collect()
    }

    /// Phase 5 M2.3.5 — query the precompute payloads across every sid
    /// belonging to one `AggregationConfig` (identified by `metric` +
    /// `agg_cfg.aggregation_type`), shaped as the legacy `Store`
    /// trait's `TimestampedBucketsMap`. Lets the query engine swap
    /// `Store::query_precomputed_output` for `SketchStore` without
    /// reshaping its consumer code in the same PR.
    ///
    /// `start_unix_ms`, `end_unix_ms` are inclusive window bounds —
    /// rows whose `(start, end)` falls within the range are returned.
    ///
    /// Iterates the `instances` map once. Cheap for the registry sizes
    /// the production deployment runs at; if instance counts grow into
    /// the millions, replace with an `agg_id → Vec<sid>` secondary
    /// index.
    pub fn query_precomputes_by_agg(
        &self,
        metric: &str,
        agg_type: AggregationType,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> std::collections::HashMap<
        Option<crate::stores::types::KeyByLabelValues>,
        Vec<((u64, u64), Arc<dyn crate::stores::types::AggregateCore>)>,
    > {
        let mut out: std::collections::HashMap<
            Option<crate::stores::types::KeyByLabelValues>,
            Vec<((u64, u64), Arc<dyn crate::stores::types::AggregateCore>)>,
        > = std::collections::HashMap::new();

        // Pick the sids whose metadata describes this (metric,
        // agg_type) tuple. We don't gate on `group_by_keys` here —
        // the engine's query-side filtering (label matchers) handles
        // that. Returning the superset is correct; over-returning is
        // just a perf cost the engine already absorbs.
        let candidate_sids: Vec<u64> = {
            let g = self.instances.read().unwrap();
            g.iter()
                .filter(|(_, m)| {
                    if m.metric_name != metric {
                        return false;
                    }
                    matches!(
                        &m.agg_kind,
                        AggKind::Precompute { agg_type: t, .. } if *t == agg_type
                    )
                })
                .map(|(sid, _)| *sid)
                .collect()
        };

        for sid in candidate_sids {
            let store = match self.series.get(&sid) {
                Some(s) => s.clone(),
                None => continue,
            };
            let guard = store.write().unwrap();
            let mut buf: Vec<(TimestampRange, LabelValuesId, &AggPayload)> = Vec::new();
            guard
                .current_epoch
                .range_query_into(start_unix_ms, end_unix_ms, &mut buf);
            for (win, label_id, payload) in &buf {
                if let Some(p) = payload.as_precompute() {
                    let label_values_map = guard
                        .intern
                        .resolve(*label_id)
                        .cloned()
                        .unwrap_or_default();
                    let key = if label_values_map.is_empty() {
                        None
                    } else {
                        Some(crate::stores::types::KeyByLabelValues {
                            labels: label_values_map.values().cloned().collect(),
                        })
                    };
                    out.entry(key).or_default().push((
                        *win,
                        Arc::from(p.clone_boxed_core()),
                    ));
                }
            }
            buf.clear();

            for sealed in guard.sealed_epochs.values() {
                sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
                for (win, label_id, payload) in &buf {
                    if let Some(p) = payload.as_precompute() {
                        let label_values_map = guard
                            .intern
                            .resolve(*label_id)
                            .cloned()
                            .unwrap_or_default();
                        let key = if label_values_map.is_empty() {
                            None
                        } else {
                            Some(crate::stores::types::KeyByLabelValues {
                                labels: label_values_map.values().cloned().collect(),
                            })
                        };
                        out.entry(key).or_default().push((
                            *win,
                            Arc::from(p.clone_boxed_core()),
                        ));
                    }
                }
                buf.clear();
            }
        }

        out
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

    // ── Phase 5 M1: lifecycle-status surface ─────────────────────────
    //
    // Mirror the `SchemaRegistry` lifecycle methods so the ingest /
    // eviction paths can cut over from `agg_id` to `sid` in M2. Until
    // M2 lands, both registries run side by side.

    /// Whether `sid` accepts writes. Equivalent to
    /// `status(sid) == AggStatus::Active`. Returns `false` for
    /// unknown sids (caller falls through to the `Unknown` path).
    /// O(1) on a `RwLock::read` of the instances map.
    pub fn is_writable(&self, sid: u64) -> bool {
        self.instances
            .read()
            .ok()
            .and_then(|m| m.get(&sid).map(|s| s.is_writable()))
            .unwrap_or(false)
    }

    /// Iterate (clones) all instance metadata matching `status`.
    /// Used by the eviction service to enumerate `Expired` sids
    /// without holding a long read lock.
    pub fn list_by_status(&self, status: AggStatus) -> Vec<SketchInstanceMetadata> {
        let map = match self.instances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        map.values().filter(|s| s.status() == status).cloned().collect()
    }

    /// Force `sid` into `Retired` status, scheduling expiry
    /// `retention` from now. Idempotent — re-retiring a Retired or
    /// Expired sid is a no-op and returns the unchanged metadata.
    /// Returns `None` if the sid is unknown.
    pub fn force_retire(&self, sid: u64, retention: Duration) -> Option<SketchInstanceMetadata> {
        let mut map = self.instances.write().ok()?;
        let meta = map.get_mut(&sid)?;
        if matches!(meta.status(), AggStatus::Active) {
            meta.retire(retention);
        }
        Some(meta.clone())
    }

    /// Force `sid` into `Expired` status immediately by setting both
    /// `retired_at_ms` and `expires_at_ms` to now. Returns the new
    /// state, or `None` if the sid is unknown. Intended for
    /// operator / debug-endpoint use so eviction can be observed in
    /// e2e tests without waiting out retirement retention.
    pub fn force_expire(&self, sid: u64) -> Option<SketchInstanceMetadata> {
        let mut map = self.instances.write().ok()?;
        let meta = map.get_mut(&sid)?;
        let now = now_ms();
        meta.retired_at_ms = Some(now);
        meta.expires_at_ms = Some(now);
        Some(meta.clone())
    }

    /// Drop a sid's metadata + its series state. Mirrors
    /// `SchemaRegistry::remove_schema` for the eviction path's
    /// post-data-drop cleanup. Returns the removed metadata, or
    /// `None` if the sid was absent.
    pub fn remove_instance(&self, sid: u64) -> Option<SketchInstanceMetadata> {
        let removed = self.instances.write().ok()?.remove(&sid);
        if removed.is_some() {
            self.series.remove(&sid);
        }
        removed
    }
}

impl SketchStore {
    /// Phase 5 M2.3.6g — runtime-info / diagnostic helper. Returns the
    /// per-sid `first_seen_unix_ms` for every registered sid. The
    /// legacy `Store::get_earliest_timestamp_per_aggregation_id` returned
    /// an analogous `agg_id → ts` map; this is the SketchStore
    /// equivalent. HTTP server's `/api/v1/status/runtimeinfo` adapter
    /// surfaces it under the JSON field `earliest_timestamp_per_sid`.
    pub fn earliest_timestamps_per_sid(&self) -> std::collections::HashMap<u64, u64> {
        let g = self.instances.read().unwrap();
        g.iter()
            .map(|(sid, m)| (*sid, m.first_seen_unix_ms.max(0) as u64))
            .collect()
    }

    /// Phase 5 M2.3.6e — write-side helper. Given an
    /// `AggregationConfig` and one `(PrecomputedOutput, AggregateCore)`
    /// pair (the shape both the live worker AND the backfill processor
    /// emit), compute the precompute sid, register a metadata entry on
    /// first sight, and append the payload window. Used by
    /// `SketchIndexSink` (live ingest) and `BackfillWindowProcessor`
    /// (archive replay) so they share one canonical sid-derivation
    /// path.
    ///
    /// Returns the sid the entry landed under (or `None` when the
    /// agg_config / output combination doesn't fit the precompute
    /// model — caller logs and skips).
    pub fn ingest_precompute_for_agg_config(
        &self,
        agg_cfg: &asap_types::aggregation_config::AggregationConfig,
        output: &crate::stores::types::PrecomputedOutput,
        accumulator: &dyn crate::stores::types::AggregateCore,
    ) -> Option<u64> {
        let label_values_vec = output
            .key
            .as_ref()
            .map(|k| k.labels.clone())
            .unwrap_or_default();
        let key_names = &agg_cfg.grouping_labels.labels;
        let mut attrs_fp = String::new();
        let mut label_values_map: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in key_names.iter().zip(label_values_vec.iter()) {
            attrs_fp.push_str(k);
            attrs_fp.push('=');
            attrs_fp.push_str(v);
            attrs_fp.push(';');
            label_values_map.insert(k.clone(), v.clone());
        }

        let agg_kind = AggKind::Precompute {
            agg_type: agg_cfg.aggregation_type,
            parameters_canonical: canonical_parameters(&agg_cfg.parameters),
        };
        let sid = compute_sid(&agg_cfg.metric, &attrs_fp, &agg_kind);

        if self.instance(sid).is_none() {
            let group_by_keys: BTreeSet<String> = key_names.iter().cloned().collect();
            self.register(SketchInstanceMetadata {
                sid,
                metric_name: agg_cfg.metric.clone(),
                group_by_keys,
                capability: None,
                agg_kind: agg_kind.clone(),
                accuracy: None,
                first_seen_unix_ms: output.start_timestamp as i64,
                retired_at_ms: None,
                expires_at_ms: None,
            });
        }

        let window = (output.start_timestamp, output.end_timestamp);
        self.append_precompute(sid, label_values_map, window, accumulator.clone_boxed_core());
        Some(sid)
    }

    /// Phase 5 M2.3.6d — eviction-side helper. Removes every sid in the
    /// index whose metadata was registered against `agg_cfg`, i.e.
    /// shares the same metric, agg_type, parameters canonicalization,
    /// and grouping-keys set the `SketchIndexSink` used at write time.
    /// Returns how many sids were removed. Used by
    /// `SchemaEvictionService` to drop a retired schema's residual sid
    /// state.
    pub fn remove_instances_for_agg_config(
        &self,
        agg_cfg: &asap_types::aggregation_config::AggregationConfig,
    ) -> usize {
        let target_metric = agg_cfg.metric.as_str();
        let target_agg_type = agg_cfg.aggregation_type;
        let target_params = canonical_parameters(&agg_cfg.parameters);
        let target_group_keys: BTreeSet<String> =
            agg_cfg.grouping_labels.labels.iter().cloned().collect();

        // Collect the matching sids under a short read lock; then call
        // `remove_instance` per sid (which takes its own write lock).
        let to_remove: Vec<u64> = {
            let g = self.instances.read().unwrap();
            g.iter()
                .filter(|(_, m)| {
                    if m.metric_name != target_metric {
                        return false;
                    }
                    if m.group_by_keys != target_group_keys {
                        return false;
                    }
                    matches!(
                        &m.agg_kind,
                        AggKind::Precompute { agg_type, parameters_canonical }
                            if *agg_type == target_agg_type
                                && parameters_canonical == &target_params
                    )
                })
                .map(|(sid, _)| *sid)
                .collect()
        };
        let count = to_remove.len();
        for sid in to_remove {
            self.remove_instance(sid);
        }
        count
    }
}

/// Persistence harness for `SketchStore` — Phase 5 M2.3.6c.
///
/// Owns the manifest + flusher thread + part cache that back the
/// sid-keyed warm tier. Constructed via [`SketchStore::start_persistence`];
/// the flusher reads sealed epochs through the
/// [`EpochSource`](crate::stores::sketch_db::store::persistence::EpochSource)
/// impl on `SketchStore` and writes parts under `disk_path/parts/`.
///
/// Drop or call [`Self::shutdown`] to stop the flusher cleanly. The
/// `part_cache` field is exposed so the query path can be wired up to
/// read-back from disk in a subsequent sub-PR; today it sits idle
/// because the in-memory `query_range` doesn't yet consult it.
pub struct SketchIndexPersistence {
    pub manifest: Arc<crate::stores::sketch_db::store::persistence::Manifest>,
    pub part_cache: crate::stores::sketch_db::store::persistence::cache::PartCache,
    pub flusher: crate::stores::sketch_db::store::persistence::flusher::FlusherHandle,
    pub parts_root: std::path::PathBuf,
}

impl SketchIndexPersistence {
    pub fn shutdown(&mut self) {
        self.flusher.shutdown();
    }
}

impl SketchStore {
    /// Spin up the persistence layer behind this `SketchStore`. Runs
    /// startup recovery (sweeps corrupt + orphan parts), opens the
    /// manifest, and starts the background flusher thread with
    /// `Arc::clone(self)` as its `EpochSource`. The returned
    /// `SketchIndexPersistence` MUST stay alive for the lifetime of
    /// the index — dropping it shuts the flusher down and stops
    /// flushing to disk.
    pub fn start_persistence(
        self: &Arc<Self>,
        cfg: crate::stores::sketch_db::store::persistence::SketchStorePersistenceConfig,
    ) -> crate::stores::sketch_db::store::persistence::PersistResult<SketchIndexPersistence>
    {
        use crate::stores::sketch_db::store::persistence::{
            cache::PartCache, flusher::FlusherHandle, recovery, Manifest,
        };

        let (_loaded_manifest, report) = recovery::recover(&cfg.disk_path)?;
        tracing::info!(
            live = report.live_parts,
            corrupt_removed = report.corrupt_parts_removed,
            orphans_removed = report.orphan_parts_removed,
            "SketchStore persistence recovery complete"
        );

        let manifest = Arc::new(Manifest::open_or_init(&cfg.disk_path)?);
        let parts_root =
            crate::stores::sketch_db::store::persistence::flusher::parts_root(&cfg.disk_path);
        let part_cache = PartCache::new(parts_root.clone(), cfg.part_cache_bytes);

        let flusher = FlusherHandle::start(cfg, Arc::clone(&manifest), Arc::clone(self))?;

        Ok(SketchIndexPersistence {
            manifest,
            part_cache,
            flusher,
            parts_root,
        })
    }
}

// ── Phase 5 M2.3.6b — EpochSource impl ──────────────────────────────────────
//
// Lets the existing persistence flusher (`store/persistence/flusher.rs`)
// drive `SketchStore` instead of `SketchStorePerKey`. The `agg_id: u64`
// field on `SealedEpochRef` / `EpochSnapshot` carries a `sid` here —
// the trait keeps the historical name so the flusher / manifest /
// part-writer stay untouched.
impl crate::stores::sketch_db::store::persistence::EpochSource for SketchStore {
    fn list_sealed_epochs(
        &self,
    ) -> Vec<crate::stores::sketch_db::store::persistence::SealedEpochRef> {
        use crate::stores::sketch_db::store::persistence::SealedEpochRef;
        let mut out = Vec::new();
        for entry in self.series.iter() {
            let sid = *entry.key();
            let Ok(data) = entry.value().read() else {
                continue;
            };
            for (epoch_id, epoch) in data.sealed_epochs.iter() {
                if let Some((_, max_end)) = epoch.time_bounds() {
                    let approx_bytes: usize =
                        epoch.entries.iter().map(|(_, _, p)| p.approx_bytes()).sum();
                    out.push(SealedEpochRef {
                        agg_id: sid,
                        epoch_id: *epoch_id,
                        end_ts: max_end,
                        approx_bytes,
                    });
                }
            }
        }
        out
    }

    fn snapshot_sealed_epoch(
        &self,
        sid: u64,
        epoch_id: u64,
    ) -> crate::stores::sketch_db::store::persistence::PersistResult<
        Option<crate::stores::sketch_db::store::persistence::source::EpochSnapshot>,
    > {
        use crate::stores::sketch_db::store::persistence::source::{
            EpochSnapshot, EpochSnapshotEntry,
        };
        use crate::stores::sketch_db::store::persistence::PersistError;

        let Some(store_ref) = self.series.get(&sid) else {
            return Ok(None);
        };
        let data = store_ref
            .read()
            .map_err(|_| PersistError::Internal(format!("sid {sid}: read lock poisoned")))?;
        let Some(epoch) = data.sealed_epochs.get(&epoch_id) else {
            return Ok(None);
        };
        let Some((min_ts, max_ts)) = epoch.time_bounds() else {
            return Ok(None);
        };

        // Resolve sketch_kind once from instance metadata so sketch
        // payloads can label their bytes for read-back dispatch.
        // Precompute payloads pull their type_name directly from the
        // accumulator trait.
        let sketch_kind_label: Option<String> = {
            self.instances
                .read()
                .ok()
                .and_then(|g| g.get(&sid).cloned())
                .and_then(|m| match &m.agg_kind {
                    AggKind::Sketch { kind, .. } => Some(format!("{:?}", kind)),
                    AggKind::Precompute { .. } => None,
                })
        };

        let mut entries = Vec::with_capacity(epoch.entries.len());
        let mut approx_bytes: usize = 0;
        for (window, label_id, payload) in &epoch.entries {
            let label_map = data.intern.resolve(*label_id).cloned();
            let label_kv = label_map.and_then(|m| {
                if m.is_empty() {
                    None
                } else {
                    Some(crate::stores::types::KeyByLabelValues {
                        labels: m.values().cloned().collect(),
                    })
                }
            });
            let (type_name, bytes) = match payload {
                AggPayload::Sketch(s) => (
                    sketch_kind_label
                        .clone()
                        .unwrap_or_else(|| "UnknownSketch".to_string()),
                    s.bytes.clone(),
                ),
                AggPayload::Precompute(p) => (p.type_name().to_string(), {
                    use asap_types::traits::SerializableToSink;
                    p.serialize_to_bytes()
                }),
            };
            approx_bytes += payload.approx_bytes();
            entries.push(EpochSnapshotEntry {
                start_ts: window.0,
                end_ts: window.1,
                label: label_kv,
                sketch_type_name: type_name,
                sketch_bytes: bytes,
            });
        }

        Ok(Some(EpochSnapshot {
            agg_id: sid,
            epoch_id,
            min_ts,
            max_ts,
            entries,
            approx_bytes,
        }))
    }

    fn evict_sealed_epoch(&self, sid: u64, epoch_id: u64) {
        let Some(store_ref) = self.series.get(&sid) else {
            return;
        };
        let Ok(mut data) = store_ref.write() else {
            return;
        };
        data.sealed_epochs.remove(&epoch_id);
    }

    fn approx_memory_bytes(&self) -> usize {
        let mut total = 0usize;
        for entry in self.series.iter() {
            let Ok(data) = entry.value().read() else {
                continue;
            };
            for epoch in data.sealed_epochs.values() {
                for (_, _, payload) in &epoch.entries {
                    total += payload.approx_bytes();
                }
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(sid: u64) -> SketchInstanceMetadata {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        SketchInstanceMetadata {
            sid,
            metric_name: "m".into(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::DDSketch)),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::DDSketch,
                config: cfg.clone(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
        }
    }

    fn sample(b: u8) -> SketchSampleState {
        SketchSampleState {
            bytes: vec![b],
            encoding: SketchEncoding::ProtoFull,
        }
    }

    #[test]
    fn ghost_classification() {
        let idx = SketchStore::new();
        idx.register(meta(42));
        assert_eq!(idx.classify(42), SidLookup::Ghost);
        assert_eq!(idx.classify(999), SidLookup::Unknown);
    }

    #[test]
    fn hit_after_append() {
        let idx = SketchStore::new();
        idx.register(meta(7));
        idx.append_sample(7, BTreeMap::new(), (1000, 1010), sample(1));
        assert_eq!(idx.classify(7), SidLookup::Hit);
    }

    #[test]
    fn range_query_returns_distinct_series() {
        let idx = SketchStore::new();
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
        let idx = SketchStore::new();
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

    // ── Phase 5 M1 lifecycle tests ────────────────────────────────────

    #[test]
    fn fresh_instance_is_active_and_writable() {
        let idx = SketchStore::new();
        idx.register(meta(1));
        let m = idx.instance(1).unwrap();
        assert_eq!(m.status(), AggStatus::Active);
        assert!(m.is_writable());
        assert!(idx.is_writable(1));
    }

    #[test]
    fn unknown_sid_is_not_writable() {
        let idx = SketchStore::new();
        assert!(!idx.is_writable(999));
    }

    #[test]
    fn force_retire_transitions_active_to_retired() {
        let idx = SketchStore::new();
        idx.register(meta(1));
        let after = idx
            .force_retire(1, Duration::from_secs(3600))
            .expect("sid known");
        assert_eq!(after.status(), AggStatus::Retired);
        assert!(after.retired_at_ms.is_some());
        assert!(after.expires_at_ms.is_some());
        // is_writable now returns false through the index too.
        assert!(!idx.is_writable(1));
    }

    #[test]
    fn force_retire_is_idempotent() {
        let idx = SketchStore::new();
        idx.register(meta(1));
        let first = idx.force_retire(1, Duration::from_secs(3600)).unwrap();
        let first_retired_at = first.retired_at_ms.unwrap();
        let first_expires_at = first.expires_at_ms.unwrap();
        // Re-retire after a tick — same timestamps.
        std::thread::sleep(Duration::from_millis(2));
        let second = idx.force_retire(1, Duration::from_secs(7200)).unwrap();
        assert_eq!(second.retired_at_ms, Some(first_retired_at));
        assert_eq!(second.expires_at_ms, Some(first_expires_at));
    }

    #[test]
    fn force_expire_makes_status_expired_immediately() {
        let idx = SketchStore::new();
        idx.register(meta(1));
        let after = idx.force_expire(1).expect("sid known");
        assert_eq!(after.status(), AggStatus::Expired);
        assert!(!idx.is_writable(1));
    }

    #[test]
    fn list_by_status_partitions_correctly() {
        let idx = SketchStore::new();
        idx.register(meta(1));
        idx.register(meta(2));
        idx.register(meta(3));
        idx.force_retire(2, Duration::from_secs(3600));
        idx.force_expire(3);

        let active = idx.list_by_status(AggStatus::Active);
        let retired = idx.list_by_status(AggStatus::Retired);
        let expired = idx.list_by_status(AggStatus::Expired);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].sid, 1);
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].sid, 2);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].sid, 3);
    }

    #[test]
    fn remove_instance_drops_metadata_and_series() {
        let idx = SketchStore::new();
        idx.register(meta(1));
        idx.append_sample(1, BTreeMap::new(), (0, 10), sample(1));
        assert_eq!(idx.classify(1), SidLookup::Hit);
        let removed = idx.remove_instance(1).expect("sid known");
        assert_eq!(removed.sid, 1);
        assert_eq!(idx.classify(1), SidLookup::Unknown);
    }

    #[test]
    fn unknown_sid_returns_none_from_lifecycle_methods() {
        let idx = SketchStore::new();
        assert!(idx.force_retire(999, Duration::from_secs(1)).is_none());
        assert!(idx.force_expire(999).is_none());
        assert!(idx.remove_instance(999).is_none());
    }

    #[test]
    fn epoch_rotation_is_visible_to_query() {
        let idx = SketchStore::new();
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

    #[test]
    fn compute_sketch_sid_is_deterministic() {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let a = compute_sketch_sid("http_requests_total", "zone=z0;", SketchKindHandle::DDSketch, &cfg);
        let b = compute_sketch_sid("http_requests_total", "zone=z0;", SketchKindHandle::DDSketch, &cfg);
        assert_eq!(a, b);
        assert_ne!(a, 0);
    }

    #[test]
    fn compute_sketch_sid_distinguishes_metric() {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let a = compute_sketch_sid("metric_a", "zone=z0;", SketchKindHandle::DDSketch, &cfg);
        let b = compute_sketch_sid("metric_b", "zone=z0;", SketchKindHandle::DDSketch, &cfg);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_sketch_sid_distinguishes_attrs_values() {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let a = compute_sketch_sid("m", "zone=z0;", SketchKindHandle::DDSketch, &cfg);
        let b = compute_sketch_sid("m", "zone=z1;", SketchKindHandle::DDSketch, &cfg);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_sketch_sid_distinguishes_sketch_kind() {
        let cfg_dd = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let cfg_kll = SketchConfig::Kll { k: 200 };
        let a = compute_sketch_sid("m", "zone=z0;", SketchKindHandle::DDSketch, &cfg_dd);
        let b = compute_sketch_sid("m", "zone=z0;", SketchKindHandle::Kll, &cfg_kll);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_sketch_sid_distinguishes_container_config() {
        let cfg_a = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let cfg_b = SketchConfig::DDSketch {
            relative_accuracy: 0.005,
        };
        let a = compute_sketch_sid("m", "zone=z0;", SketchKindHandle::DDSketch, &cfg_a);
        let b = compute_sketch_sid("m", "zone=z0;", SketchKindHandle::DDSketch, &cfg_b);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_sid_precompute_is_deterministic() {
        let kind = AggKind::Precompute {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
        };
        let a = compute_sid("cpu_seconds", "zone=z0;", &kind);
        let b = compute_sid("cpu_seconds", "zone=z0;", &kind);
        assert_eq!(a, b);
        assert_ne!(a, 0);
    }

    #[test]
    fn compute_sid_sketch_vs_precompute_never_collide() {
        // Same metric + attrs; one is a sketch, one is a precompute.
        // The 'S'/'P' discriminator byte must make the hashes differ.
        let sketch = AggKind::Sketch {
            kind: SketchKindHandle::DDSketch,
            config: SketchConfig::DDSketch {
                relative_accuracy: 0.01,
            },
        };
        let precompute = AggKind::Precompute {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
        };
        let a = compute_sid("m", "zone=z0;", &sketch);
        let b = compute_sid("m", "zone=z0;", &precompute);
        assert_ne!(a, b, "sketch and precompute sids must not collide");
    }

    #[test]
    fn compute_sid_precompute_distinguishes_agg_type() {
        let sum = AggKind::Precompute {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
        };
        let count = AggKind::Precompute {
            agg_type: AggregationType::Increase,
            parameters_canonical: String::new(),
        };
        let a = compute_sid("m", "zone=z0;", &sum);
        let b = compute_sid("m", "zone=z0;", &count);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_sid_precompute_distinguishes_parameters() {
        let p1 = AggKind::Precompute {
            agg_type: AggregationType::DatasketchesKLL,
            parameters_canonical: "k=200;".to_string(),
        };
        let p2 = AggKind::Precompute {
            agg_type: AggregationType::DatasketchesKLL,
            parameters_canonical: "k=400;".to_string(),
        };
        let a = compute_sid("m", "zone=z0;", &p1);
        let b = compute_sid("m", "zone=z0;", &p2);
        assert_ne!(a, b);
    }

    #[test]
    fn canonical_parameters_is_insertion_order_independent() {
        let mut p_ab = std::collections::HashMap::new();
        p_ab.insert("alpha".to_string(), serde_json::json!(1));
        p_ab.insert("beta".to_string(), serde_json::json!(2));
        let mut p_ba = std::collections::HashMap::new();
        p_ba.insert("beta".to_string(), serde_json::json!(2));
        p_ba.insert("alpha".to_string(), serde_json::json!(1));
        assert_eq!(canonical_parameters(&p_ab), canonical_parameters(&p_ba));
    }

    #[test]
    fn precompute_payload_round_trips_through_storage() {
        use crate::precompute_engine::operators::SumAccumulator;

        let idx = SketchStore::new();
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let mut precompute_meta = meta(42);
        precompute_meta.capability = None;
        precompute_meta.accuracy = None;
        precompute_meta.agg_kind = AggKind::Precompute {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
        };
        let _ = cfg; // silence unused-binding lint
        idx.register(precompute_meta);

        idx.append_precompute(
            42,
            BTreeMap::new(),
            (1000, 1010),
            Box::new(SumAccumulator::with_sum(5.0)),
        );

        // Sketch-side query_range filters out precompute payloads, so
        // a precompute sid produces no SketchTimeSeries entries even
        // though the storage has data.
        let series = idx.query_range(42, 0, 5000);
        assert!(
            series.iter().all(|s| s.samples.is_empty()),
            "precompute payloads must not surface as sketch results"
        );
        assert_eq!(idx.classify(42), SidLookup::Hit, "storage has data — Hit");
    }

    #[test]
    fn query_precomputes_by_agg_returns_data_grouped_by_label_values() {
        use crate::precompute_engine::operators::SumAccumulator;

        let idx = SketchStore::new();
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let _ = cfg;
        let mut precompute_meta = meta(99);
        precompute_meta.metric_name = "cpu_seconds".into();
        precompute_meta.capability = None;
        precompute_meta.accuracy = None;
        precompute_meta.agg_kind = AggKind::Precompute {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
        };
        idx.register(precompute_meta);

        // Two writes under sid=99 with the same label_values + different
        // windows — they should collapse into the same group_key on
        // the way out.
        let mut lv = BTreeMap::new();
        lv.insert("zone".to_string(), "z0".to_string());
        idx.append_precompute(
            99,
            lv.clone(),
            (1000, 2000),
            Box::new(SumAccumulator::with_sum(1.0)),
        );
        idx.append_precompute(
            99,
            lv,
            (2000, 3000),
            Box::new(SumAccumulator::with_sum(2.0)),
        );

        let result = idx.query_precomputes_by_agg(
            "cpu_seconds",
            AggregationType::Sum,
            0,
            10_000,
        );
        assert_eq!(result.len(), 1, "one label-values key");
        let buckets = result.values().next().expect("populated");
        assert_eq!(buckets.len(), 2, "two windows for that key");
    }

    #[test]
    fn query_precomputes_by_agg_skips_sketch_payloads() {
        let idx = SketchStore::new();
        // A SKETCH sid for the same metric — must not show up in the
        // precompute query path.
        idx.register(meta(7));
        idx.append_sample(7, BTreeMap::new(), (1000, 2000), sample(1));

        let result = idx.query_precomputes_by_agg(
            "m",
            AggregationType::Sum,
            0,
            10_000,
        );
        assert!(result.is_empty(), "sketch sids must not surface");
    }

    /// Helper: append one sample to materialize the SidStoreData, then
    /// set its epoch_capacity so subsequent appends rotate aggressively.
    fn with_tight_rotation(idx: &SketchStore, sid: u64) {
        idx.append_sample(sid, BTreeMap::new(), (0, 10), sample(0));
        if let Some(s) = idx.series.get(&sid) {
            let mut g = s.write().unwrap();
            g.epoch_capacity = Some(1);
            g.max_epochs = 8;
        }
    }

    #[test]
    fn epoch_source_lists_only_sealed_epochs() {
        use crate::stores::sketch_db::store::persistence::EpochSource;
        let idx = SketchStore::new();
        idx.register(meta(13));
        with_tight_rotation(&idx, 13);
        idx.append_sample(13, BTreeMap::new(), (10, 20), sample(2));
        idx.append_sample(13, BTreeMap::new(), (20, 30), sample(3));

        let refs = idx.list_sealed_epochs();
        assert!(
            !refs.is_empty(),
            "rotation should have produced at least one sealed epoch"
        );
        assert!(
            refs.iter().all(|r| r.agg_id == 13),
            "all sealed-epoch refs come from sid=13"
        );
    }

    #[test]
    fn epoch_source_snapshot_round_trips_sketch_payload() {
        use crate::stores::sketch_db::store::persistence::EpochSource;
        let idx = SketchStore::new();
        idx.register(meta(21));
        with_tight_rotation(&idx, 21);
        idx.append_sample(21, BTreeMap::new(), (1000, 2000), sample(0xAB));
        idx.append_sample(21, BTreeMap::new(), (2000, 3000), sample(0xCD));

        let refs = idx.list_sealed_epochs();
        let first = refs.first().expect("a sealed epoch exists");
        let snap = idx
            .snapshot_sealed_epoch(first.agg_id, first.epoch_id)
            .expect("snapshot ok")
            .expect("populated");
        assert_eq!(snap.agg_id, 21);
        assert!(!snap.entries.is_empty());
        let entry = &snap.entries[0];
        assert!(
            entry.sketch_type_name.starts_with("DDSketch"),
            "sketch_type_name should reflect sid metadata's sketch_kind: {}",
            entry.sketch_type_name
        );
        assert_eq!(entry.sketch_bytes.len(), 1, "single-byte sample bytes carry");
    }

    #[test]
    fn epoch_source_evict_drops_the_epoch() {
        use crate::stores::sketch_db::store::persistence::EpochSource;
        let idx = SketchStore::new();
        idx.register(meta(31));
        with_tight_rotation(&idx, 31);
        idx.append_sample(31, BTreeMap::new(), (10, 20), sample(2));

        let refs = idx.list_sealed_epochs();
        let one = refs.first().cloned().expect("populated");
        idx.evict_sealed_epoch(one.agg_id, one.epoch_id);
        let after = idx.list_sealed_epochs();
        assert!(
            !after.iter().any(|r| r.epoch_id == one.epoch_id),
            "the evicted epoch must no longer appear"
        );
    }

    #[test]
    fn epoch_source_approx_memory_bytes_grows_with_sealed_state() {
        use crate::stores::sketch_db::store::persistence::EpochSource;
        let idx = SketchStore::new();
        let before = idx.approx_memory_bytes();
        idx.register(meta(41));
        with_tight_rotation(&idx, 41);
        idx.append_sample(41, BTreeMap::new(), (10, 20), sample(2));
        let after = idx.approx_memory_bytes();
        assert!(after > before, "sealed state contributes to memory total");
    }

    #[test]
    fn agg_payload_accessors_are_disjoint() {
        let sketch = AggPayload::Sketch(SketchSampleState {
            bytes: vec![0xAA],
            encoding: SketchEncoding::ProtoFull,
        });
        assert!(sketch.as_sketch().is_some());
        assert!(sketch.as_precompute().is_none());

        use crate::precompute_engine::operators::SumAccumulator;
        let precompute = AggPayload::Precompute(Box::new(SumAccumulator::with_sum(1.0)));
        assert!(precompute.as_sketch().is_none());
        assert!(precompute.as_precompute().is_some());
    }

    #[test]
    fn compute_sketch_sid_matches_new_compute_sid_for_sketch_branch() {
        // The legacy `compute_sketch_sid` wrapper must produce
        // the exact same value as `compute_sid` with an
        // `AggKind::Sketch` — otherwise existing ingest sids
        // would skew across the migration.
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        let legacy = compute_sketch_sid("m", "zone=z0;", SketchKindHandle::DDSketch, &cfg);
        let new = compute_sid(
            "m",
            "zone=z0;",
            &AggKind::Sketch {
                kind: SketchKindHandle::DDSketch,
                config: cfg,
            },
        );
        assert_eq!(legacy, new);
    }
}

// 2026-05 reorg: generic epoch-partitioned columnar storage lives
// alongside the index that uses it.
pub mod epoch_columnar;
