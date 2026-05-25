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
//! the sketch payload. Query path treats ghost sids as ASAP-tier MISS
//! and falls through to Thanos archive (Phase 6).
//!
//! See design doc §4.6 ("OTLP metadata model + backend store layout") at
//! `docs/design-controller-into-backend.md`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asap_types::PolicyFingerprint;
use dashmap::DashMap;

use self::epoch_columnar::{LabelValuesId, SidStoreData, TimestampRange};
use crate::storage_engines::sketch_db::lifecycle::AggStatus;

// Phase-5 reorg: payload taxonomy + sid hashing + accuracy moved to
// `sketch_db::data`. Re-exported here so existing call sites
// (`crate::storage_engines::sketch_db::index::*`) keep compiling
// during the reorg.
pub use crate::storage_engines::sketch_db::data::{
    canonical_parameters, AccuracyBound, AggKind, AggPayload, AggregationType, Capability,
    SketchConfig, SketchEncoding, SketchKindHandle, SketchSampleState, SketchTimeSeries,
};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Map a [`SketchEncoding`] to the on-disk encoding tag stored per part
/// entry, so the disk read-back path can reconstruct the Full-vs-Delta
/// distinction the delta-stitching carry-in relies on.
fn encoding_to_tag(enc: SketchEncoding) -> u8 {
    use crate::storage_engines::sketch_db::persistence::part::encoding_tag as t;
    match enc {
        SketchEncoding::ProtoFull => t::PROTO_FULL,
        SketchEncoding::ProtoDelta => t::PROTO_DELTA,
        SketchEncoding::MsgpackFull => t::MSGPACK_FULL,
        SketchEncoding::MsgpackDelta => t::MSGPACK_DELTA,
    }
}

/// Inverse of [`encoding_to_tag`]. The unknown tag (`0`, written by the
/// original v1 part writer) decodes to `ProtoFull` — the safe default
/// for a carry-in base, since a Full snapshot establishes its own
/// rolling state with no predecessor.
fn tag_to_encoding(tag: u8) -> SketchEncoding {
    use crate::storage_engines::sketch_db::persistence::part::encoding_tag as t;
    match tag {
        t::PROTO_DELTA => SketchEncoding::ProtoDelta,
        t::MSGPACK_FULL => SketchEncoding::MsgpackFull,
        t::MSGPACK_DELTA => SketchEncoding::MsgpackDelta,
        // t::PROTO_FULL and t::UNKNOWN (legacy) both → Full.
        _ => SketchEncoding::ProtoFull,
    }
}

/// Reconstruct an exact-aggregation accumulator from its on-disk
/// `(type_name, bytes)` pair so the durable tier can serve the
/// exact-agg query path (`query_exact_agg_range` / `sum by (...)`) after
/// flush+evict. Covers the deterministic scalar accumulators the live
/// marquee `sum by (zone)` path uses; the sketch-backed accumulator forms
/// (DDSketch/KLL/HLL/CountSketch — registered as `AggKind::Sketch`) are
/// served as opaque bytes via [`SketchStore::query_range`] and are NOT
/// reconstructed here. Returns `None` for an unrecognized `type_name`
/// (the caller skips the disk entry rather than fabricating a wrong
/// payload) — see the remaining-follow-up note in the PR.
fn reconstruct_exact_agg(
    type_name: &str,
    bytes: &[u8],
) -> Option<Box<dyn crate::storage_engines::types::AggregateCore>> {
    use crate::precompute_engine::operators::{
        IncreaseAccumulator, MinMaxAccumulator, MultipleIncreaseAccumulator,
        MultipleSumAccumulator, SumAccumulator,
    };
    use crate::storage_engines::types::AggregateCore;
    match type_name {
        "SumAccumulator" => SumAccumulator::deserialize_from_bytes(bytes)
            .ok()
            .map(|a| Box::new(a) as Box<dyn AggregateCore>),
        "IncreaseAccumulator" => IncreaseAccumulator::deserialize_from_bytes(bytes)
            .ok()
            .map(|a| Box::new(a) as Box<dyn AggregateCore>),
        "MinMaxAccumulator" => MinMaxAccumulator::deserialize_from_bytes(bytes)
            .ok()
            .map(|a| Box::new(a) as Box<dyn AggregateCore>),
        "MultipleSumAccumulator" => MultipleSumAccumulator::deserialize_from_bytes(bytes)
            .ok()
            .map(|a| Box::new(a) as Box<dyn AggregateCore>),
        "MultipleIncreaseAccumulator" => MultipleIncreaseAccumulator::deserialize_from_bytes(bytes)
            .ok()
            .map(|a| Box::new(a) as Box<dyn AggregateCore>),
        // `MultipleMinMaxAccumulator` needs an external `sub_type`
        // (min/max) not recorded in the part, and the sketch-backed
        // accumulator forms have no generic byte factory — both are left
        // to the deferred exact-agg/sketch precompute read-back work (see
        // PR follow-up note). They are still served from memory; only the
        // evicted-to-disk portion is skipped for these types.
        _ => None,
    }
}

/// Joint helper shared by [`SketchStore::ingest_precompute_for_agg_config`]
/// and [`SketchStore::ingest_precompute_with_sid`] — folds the
/// grouping-label values on `output` against the
/// `agg_cfg.grouping_labels` ordering into:
///
/// 1. `attrs_fp`: the `key=value;` canonical attrs-fingerprint string
///    `SeriesIdResolver` uses as one third of the sid identity tuple.
/// 2. `label_values_map`: the `BTreeMap<String, String>` shape
///    `append_precompute` records on the per-window precompute row.
///
/// Both are pure functions of `(agg_cfg, output.key)` — kept together
/// so the mint-driven path (B7.6) and the sid-direct path (B7.7) stay
/// byte-identical on the values they hand to the index.
fn build_attrs_fp_and_label_map(
    agg_cfg: &asap_types::aggregation_config::AggregationConfig,
    output: &crate::storage_engines::types::PrecomputedOutput,
) -> (String, BTreeMap<String, String>) {
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
    (attrs_fp, label_values_map)
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
    /// Content-addressed back-reference to the policy that minted this
    /// sid. Together with [`SketchStore::policy_to_sids`] this gives
    /// the query path a direct `policy_fp → [sid]` index without
    /// walking the metadata map. `PolicyFingerprint::UNSET` is reserved
    /// for the legacy registration path that doesn't carry a source
    /// `AggregationConfig` (test fixtures + the early-Phase-5 sketch
    /// ingest path that didn't thread the config through); the index
    /// skips those entries — they're reachable through the legacy
    /// `instances_matching(metric, gbk)` walk if a query needs them.
    pub policy_fp: PolicyFingerprint,
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
    /// exact-aggregation-backed sids. Consumers that only meaningfully
    /// run on sketches (e.g. the ASAP-tier reducer) `.expect` it.
    pub fn sketch_kind(&self) -> Option<SketchKindHandle> {
        match &self.agg_kind {
            AggKind::Sketch { kind, .. } => Some(*kind),
            AggKind::ExactAgg { .. } => None,
        }
    }

    /// Sketch-config accessor mirroring [`Self::sketch_kind`].
    pub fn sketch_config(&self) -> Option<&SketchConfig> {
        match &self.agg_kind {
            AggKind::Sketch { config, .. } => Some(config),
            AggKind::ExactAgg { .. } => None,
        }
    }
}

// `SketchSampleState`, `SketchEncoding`, `SketchTimeSeries`, and
// `AggPayload` moved to `sketch_db::data` in the Phase-5 reorg; they're
// re-exported at the top of this file for backwards compatibility.

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
    /// Reverse index: `policy_fp → {sids}`. Lets the query path resolve
    /// "which sids belong to this policy?" in O(1) without walking
    /// `instances`. Maintained by [`Self::register`] /
    /// [`Self::remove_instance`] / [`Self::remove_instances_for_agg_config`].
    /// Entries with `PolicyFingerprint::UNSET` are NOT recorded (the
    /// sentinel means "no source config"); legacy callers that mint
    /// sids without a fingerprint reach those sids through
    /// `instances_matching(metric, gbk)`.
    policy_to_sids: RwLock<HashMap<PolicyFingerprint, BTreeSet<u64>>>,
    /// Pointer (as `usize`) of the `Arc<StreamingConfig>` this store
    /// last reconciled against. `reconcile_from_streaming_config` runs
    /// on every ingest batch, but the config is a lock-free
    /// `Arc<ArcSwap<StreamingConfig>>` that only changes its `Arc`
    /// identity on a control-plane swap (rare). Gating the full
    /// catalog scan on a cheap pointer compare against this field lets
    /// the steady-state ingest path skip reconcile entirely.
    /// `0` (the `Default`) means "never reconciled" so the first batch
    /// always runs. A real `Arc` data pointer is never null.
    last_reconciled_config_ptr: std::sync::atomic::AtomicUsize,
    /// Durable-tier read handle, installed by [`Self::start_persistence`]
    /// when `--persistence-enabled`. `None` (the default) means the
    /// in-memory-only deployment: `query_range` reads HOT + SEALED
    /// in-memory state and #327 retention bounds memory. When `Some`,
    /// `query_range` ALSO unions in flushed-then-evicted DISK parts for
    /// the portion of the range that has left memory, and the per-sid
    /// `SidStoreData` is configured to seal on a cadence (so the flusher
    /// has sealed epochs to persist) with retention-drop disabled (the
    /// flush-then-evict loop is the memory bound).
    persistence_read: RwLock<Option<Arc<PersistenceReadHandle>>>,
    /// Seal cadence in distinct windows, applied to every per-sid
    /// `SidStoreData` once persistence is enabled. `0` (the default)
    /// disables cadence sealing. Set by [`Self::enable_persistence_mode`].
    seal_window_count: std::sync::atomic::AtomicUsize,
}

/// Read-side handle to the durable tier — the manifest of live disk
/// parts plus the byte-bounded `PartCache` that mmaps them. Cloned (as
/// an `Arc`) into `SketchStore::persistence_read` so the query path can
/// consult disk parts without holding a reference to the flusher.
///
/// Also recovered on restart: `start_persistence` installs a fresh
/// handle pointing at the recovered manifest, so a reopened store sees
/// every part that was durable before the crash.
pub struct PersistenceReadHandle {
    pub manifest: Arc<crate::storage_engines::sketch_db::index::persistence::Manifest>,
    pub part_cache: crate::storage_engines::sketch_db::index::persistence::cache::PartCache,
}

/// Three possible outcomes of looking up a sid in the SketchStore.
/// Query path uses this enum to drive routing decisions:
/// - `Hit`: ASAP-tier sketch has data — evaluate.
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

    /// Insert metadata for a freshly-resolved sid. Also records the
    /// sid in the `policy_fp → {sids}` reverse index when the metadata
    /// carries a non-UNSET fingerprint.
    pub fn register(&self, meta: SketchInstanceMetadata) {
        let sid = meta.sid;
        let policy_fp = meta.policy_fp;
        self.instances.write().unwrap().insert(sid, meta);
        if !policy_fp.is_unset() {
            self.policy_to_sids
                .write()
                .unwrap()
                .entry(policy_fp)
                .or_default()
                .insert(sid);
        }
    }

    /// Resolve a policy fingerprint to the set of sids it has minted.
    /// Returns an empty vector when no sid is bound to the fingerprint
    /// (e.g. fresh policy with no ingest activity yet) or when the
    /// caller passes [`PolicyFingerprint::UNSET`]. The order of the
    /// returned slice is sorted (the underlying index is a `BTreeSet`)
    /// so callers can hash / compare it deterministically.
    pub fn sids_for_policy(&self, policy_fp: PolicyFingerprint) -> Vec<u64> {
        if policy_fp.is_unset() {
            return Vec::new();
        }
        self.policy_to_sids
            .read()
            .unwrap()
            .get(&policy_fp)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Live policy count — number of distinct fingerprints with at
    /// least one sid. Useful for telemetry / `/runtime` introspection
    /// (mirrors the legacy "active aggregation count" metric).
    pub fn policy_count(&self) -> usize {
        self.policy_to_sids.read().unwrap().len()
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
            .or_insert_with(|| Arc::new(RwLock::new(self.fresh_sid_store())))
            .clone();
        let mut guard = store.write().unwrap();
        guard.insert(window, series_label_values, AggPayload::Sketch(sample));
    }

    /// Build a `SidStoreData` pre-configured for the store's current
    /// persistence mode. When persistence is enabled it seals on the
    /// configured window cadence and disables retention-drop (the
    /// flush-then-evict loop bounds memory). When disabled it's the
    /// plain in-memory store with #327 retention.
    fn fresh_sid_store(&self) -> SidStoreData<BTreeMap<String, String>, AggPayload> {
        use std::sync::atomic::Ordering;
        let mut data = SidStoreData::new();
        let cadence = self.seal_window_count.load(Ordering::Relaxed);
        if cadence > 0 {
            data.seal_window_count = Some(cadence);
            data.persistence_enabled = true;
        }
        data
    }

    /// Append a window's exact-aggregation (Sum/Count/Avg/Rate/MinMax)
    /// state under `sid`. Mirror of [`Self::append_sample`] for the
    /// exact-agg branch — Phase 5 M2.3.3.
    ///
    /// Caller invariant: `sid` was registered with
    /// `AggKind::ExactAgg { .. }`. Mixing sketch + exact-agg
    /// payloads under one sid is a logic error this layer doesn't
    /// guard against (it'll crash the reducer at runtime, not silently
    /// corrupt).
    pub fn append_precompute(
        &self,
        sid: u64,
        series_label_values: BTreeMap<String, String>,
        window: TimestampRange,
        payload: Box<dyn crate::storage_engines::types::AggregateCore>,
    ) {
        let store = self
            .series
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(self.fresh_sid_store())))
            .clone();
        let mut guard = store.write().unwrap();
        guard.insert(window, series_label_values, AggPayload::ExactAgg(payload));
    }

    /// Range-query the ASAP-tier state for one sid. Window-end-keyed
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
        // Result is keyed by the resolved label MAP so the in-memory tier
        // (its own intern space) and the durable disk tier (independent
        // intern space) union by label identity, not `LabelValuesId`.
        let mut by_label_map: HashMap<BTreeMap<String, String>, BTreeMap<i64, SketchSampleState>> =
            HashMap::new();

        // ── In-memory tier ──────────────────────────────────────────────
        // Absent series is NOT an early return: under persistence the
        // sid's hot+sealed state may have been fully flushed-then-evicted
        // (or recovered from disk after a restart with no fresh ingest
        // yet), so the answer can live entirely on disk. We still run the
        // disk union below.
        if let Some(store) = self.series.get(&sid).map(|s| s.clone()) {
        let guard = store.write().unwrap(); // exact_query may build the lazy index
        let mut by_label_id: HashMap<LabelValuesId, BTreeMap<i64, SketchSampleState>> =
            HashMap::new();

        let mut buf: Vec<(TimestampRange, LabelValuesId, &AggPayload)> = Vec::new();
        // Sketch read path uses HALF-OPEN OVERLAP, not containment: the
        // agent emits ~30s tumbling panes, so a short query window (a
        // `[30s]` range, or an instant query whose freshest pane straddles
        // `now`) can't FULLY CONTAIN any pane. Containment then returns
        // zero in-window samples → the carry-in never fires and the
        // reducer yields an empty series (the live "No result" bug for
        // `[30s]` + bare-instant selectors). The reducer's own
        // `w_end >= t0` / `latest_end` filters keep out-of-range values
        // from leaking into the answer. See
        // `MutableEpoch::range_query_overlap_into`.
        guard
            .current_epoch
            .range_query_overlap_into(start_unix_ms, end_unix_ms, &mut buf);
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
            sealed.range_query_overlap_into(start_unix_ms, end_unix_ms, &mut buf);
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

        // Delta-stitching carry-in (issue: quantile/HLL "No result"
        // bug). The agent emits a periodic Full snapshot followed by
        // many cheap Delta frames. A short query window (e.g. `[30s]`)
        // routinely contains ONLY deltas — the Full landed earlier,
        // outside `[start, end]`. The downstream delta-apply reducer
        // can't establish a rolling base from a leading delta, so it
        // silently produces an empty result that the engine returns as
        // `Ok(empty)` (NOT a capability-miss), so the router never
        // fails over and the caller sees "No result". To fix, for each
        // label series whose earliest in-window sample is a Delta,
        // splice in the most-recent Full snapshot ending at or before
        // `start` as a carry-in base. Its window-end is `< start`, so
        // it sorts first in the per-label `BTreeMap` and the reducer's
        // cumulative/per-window walk uses it as the base; the reducer
        // drops out-of-range output windows so the carry-in never leaks
        // into the answer's time domain.
        if start_unix_ms > 0 {
            // Which labels need a base? Those present in-window whose
            // earliest sample is a Delta (a leading Full needs nothing).
            let need_base: Vec<LabelValuesId> = by_label_id
                .iter()
                .filter(|(_, samples)| {
                    samples
                        .values()
                        .next()
                        .map(|s| {
                            matches!(
                                s.encoding,
                                SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta
                            )
                        })
                        .unwrap_or(false)
                })
                .map(|(label_id, _)| *label_id)
                .collect();

            if !need_base.is_empty() {
                let before = start_unix_ms.saturating_sub(1);
                // Track the latest Full per label (by window-end).
                let mut latest_full: HashMap<LabelValuesId, (i64, SketchSampleState)> =
                    HashMap::new();
                let mut consider = |buf: &Vec<(TimestampRange, LabelValuesId, &AggPayload)>| {
                    for (win, label_id, payload) in buf {
                        if !need_base.contains(label_id) {
                            continue;
                        }
                        let Some(s) = payload.as_sketch() else {
                            continue;
                        };
                        if !matches!(
                            s.encoding,
                            SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
                        ) {
                            continue;
                        }
                        let w_end = win.1 as i64;
                        match latest_full.get(label_id) {
                            Some((prev_end, _)) if *prev_end >= w_end => {}
                            _ => {
                                latest_full.insert(*label_id, (w_end, s.clone()));
                            }
                        }
                    }
                };
                guard
                    .current_epoch
                    .collect_ending_at_or_before(before, &mut buf);
                consider(&buf);
                buf.clear();
                for sealed in guard.sealed_epochs.values() {
                    sealed.collect_ending_at_or_before(before, &mut buf);
                    consider(&buf);
                    buf.clear();
                }
                for (label_id, (w_end, state)) in latest_full {
                    by_label_id
                        .entry(label_id)
                        .or_default()
                        .entry(w_end)
                        .or_insert(state);
                }
            }
        }

        // Materialize the in-memory result keyed by the resolved label
        // MAP so the disk tier (which has its own intern space) can be
        // unioned by label identity rather than `LabelValuesId`.
        for (label_id, samples) in by_label_id {
            let label_values = guard.intern.resolve(label_id).cloned().unwrap_or_default();
            by_label_map.entry(label_values).or_default().extend(samples);
        }
        // Release the per-sid lock before touching disk — disk reads can
        // mmap/decode and must not hold the hot ingest lock.
        drop(guard);
        } // end in-memory tier

        // Union the DURABLE DISK TIER for the part of `[start, end)` that
        // has been flushed-then-evicted from memory. Preserves the
        // #323–#326 read contract across the in-mem/on-disk boundary:
        // the same half-open overlap admits straddling panes, and a
        // delta-stitching carry-in Full base is fetched from disk when
        // it has aged out of memory. In-memory samples win on a
        // window-end collision (disk is a strict older suffix in steady
        // state; the guard is belt-and-suspenders).
        self.union_disk_parts_into(sid, start_unix_ms, end_unix_ms, &mut by_label_map);

        by_label_map
            .into_iter()
            .map(|(label_values, samples)| {
                SketchTimeSeries {
                    sid,
                    series_label_values: label_values,
                    samples,
                }
            })
            .collect()
    }

    /// Resolve the sorted group-by KEYS for a sid from its instance
    /// metadata. Disk parts store only label VALUES (a `KeyByLabelValues`
    /// vector); the per-sid intern table records `BTreeMap<String,String>`
    /// (key-sorted), so `values()` yields values in key-sorted order.
    /// Zipping the sorted `group_by_keys` against a disk values vector
    /// rebuilds the exact `BTreeMap<String,String>` that the in-memory
    /// path produced — no part-format change needed to round-trip keys.
    fn sid_group_by_keys(&self, sid: u64) -> Option<Vec<String>> {
        self.instances
            .read()
            .ok()?
            .get(&sid)
            .map(|m| m.group_by_keys.iter().cloned().collect())
    }

    /// Reconstruct the full label MAP for one disk entry by zipping the
    /// sid's sorted group-by keys against the stored values vector.
    fn rebuild_label_map(
        keys: &[String],
        label: &Option<crate::storage_engines::types::KeyByLabelValues>,
    ) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        if let Some(kv) = label {
            for (k, v) in keys.iter().zip(kv.labels.iter()) {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }

    /// Union the durable disk tier into `by_label_map` for the requested
    /// `[start, end)`. No-op when persistence is disabled. Mirrors the
    /// in-memory read contract: half-open overlap admits straddling
    /// panes, and the delta-stitching carry-in fetches a Full base from
    /// disk when it has aged out of memory. In-memory samples already in
    /// `by_label_map` win on a window-end collision.
    fn union_disk_parts_into(
        &self,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
        by_label_map: &mut HashMap<BTreeMap<String, String>, BTreeMap<i64, SketchSampleState>>,
    ) {
        let handle = {
            let g = self.persistence_read.read().unwrap();
            match g.as_ref() {
                Some(h) => Arc::clone(h),
                None => return,
            }
        };
        let Some(keys) = self.sid_group_by_keys(sid) else {
            return;
        };

        // ---- Overlap scan over disk parts in [start, end) ----
        let parts = handle
            .manifest
            .live_parts_overlapping(start_unix_ms, end_unix_ms);
        for pe in &parts {
            let reader = match handle.part_cache.get_or_load(pe.part_id) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(part_id = pe.part_id, error = %e, "sketch disk read: open part failed");
                    continue;
                }
            };
            for rec in reader.index_records() {
                if rec.agg_id != sid {
                    continue;
                }
                // Half-open overlap, matching the in-memory scan:
                // `end_ts > start && start_ts < end`.
                if !(rec.end_ts > start_unix_ms && rec.start_ts < end_unix_ms) {
                    continue;
                }
                let Ok(entry) = reader.load_entry(&rec) else {
                    continue;
                };
                let label_map = Self::rebuild_label_map(&keys, &entry.label);
                let sample = SketchSampleState {
                    bytes: entry.sketch_bytes,
                    encoding: tag_to_encoding(entry.encoding_tag),
                };
                by_label_map
                    .entry(label_map)
                    .or_default()
                    .entry(rec.end_ts as i64)
                    // In-memory wins — only fill window-ends disk uniquely
                    // owns.
                    .or_insert(sample);
            }
        }

        // ---- Delta-stitching carry-in from disk ----
        // For each label whose earliest in-window sample is a Delta and
        // which lacks a Full base ending before `start`, fetch the
        // most-recent Full snapshot ending at/before `start-1` from disk.
        if start_unix_ms == 0 {
            return;
        }
        let before = start_unix_ms.saturating_sub(1);
        let need_base: std::collections::HashSet<BTreeMap<String, String>> = by_label_map
            .iter()
            .filter(|(_, samples)| {
                // Earliest sample is a Delta and there is no Full base
                // already present at/before `start`.
                let earliest_is_delta = samples
                    .values()
                    .next()
                    .map(|s| {
                        matches!(
                            s.encoding,
                            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta
                        )
                    })
                    .unwrap_or(false);
                let has_base_before = samples
                    .iter()
                    .any(|(w_end, s)| {
                        *w_end < start_unix_ms as i64
                            && matches!(
                                s.encoding,
                                SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
                            )
                    });
                earliest_is_delta && !has_base_before
            })
            .map(|(label_map, _)| label_map.clone())
            .collect();
        if need_base.is_empty() {
            return;
        }

        let carry_parts = handle.manifest.live_parts_overlapping(0, before);
        // latest Full per label-map (by window-end).
        let mut latest_full: HashMap<BTreeMap<String, String>, (i64, SketchSampleState)> =
            HashMap::new();
        for pe in &carry_parts {
            let reader = match handle.part_cache.get_or_load(pe.part_id) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for rec in reader.index_records() {
                if rec.agg_id != sid || rec.end_ts > before {
                    continue;
                }
                let Ok(entry) = reader.load_entry(&rec) else {
                    continue;
                };
                let encoding = tag_to_encoding(entry.encoding_tag);
                if !matches!(
                    encoding,
                    SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
                ) {
                    continue;
                }
                let label_map = Self::rebuild_label_map(&keys, &entry.label);
                if !need_base.contains(&label_map) {
                    continue;
                }
                let w_end = rec.end_ts as i64;
                match latest_full.get(&label_map) {
                    Some((prev_end, _)) if *prev_end >= w_end => {}
                    _ => {
                        latest_full.insert(
                            label_map,
                            (
                                w_end,
                                SketchSampleState {
                                    bytes: entry.sketch_bytes,
                                    encoding,
                                },
                            ),
                        );
                    }
                }
            }
        }
        for (label_map, (w_end, state)) in latest_full {
            by_label_map
                .entry(label_map)
                .or_default()
                .entry(w_end)
                .or_insert(state);
        }
    }

    /// Range-query the ExactAgg state for ONE sid. Sister of
    /// [`Self::query_range`] for the exact-aggregation branch — same
    /// `[start, end]` semantics, but yields `Box<dyn AggregateCore>`
    /// payloads keyed by their FULL `BTreeMap<String, String>` label
    /// map (label KEYS preserved, not just values).
    ///
    /// Used by the ASAP-tier `sum by (...)` dispatch path
    /// (`SketchReducer::evaluate_exact_agg`) so the engine can
    /// project label maps onto a query-time `group_by_keys` subset
    /// (`{zone: z0, rack: r0}` → grouped by `zone` only). The
    /// existing [`Self::query_precomputes_by_agg`] flattens labels
    /// to a `KeyByLabelValues` (values only, no keys), which loses
    /// the projection information the engine needs.
    ///
    /// Returns an empty Vec when the sid carries no ExactAgg state
    /// in `[start, end]` (or is sketch-backed). Defensive — caller
    /// is responsible for confirming the sid's `agg_kind` is
    /// `AggKind::ExactAgg { .. }` before calling.
    pub fn query_exact_agg_range(
        &self,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Vec<(
        BTreeMap<String, String>,
        BTreeMap<i64, Arc<dyn crate::storage_engines::types::AggregateCore>>,
    )> {
        // Key by the resolved label MAP (not `LabelValuesId`) so the
        // in-memory tier and the durable disk tier — which carry
        // independent intern spaces — union by label identity. Mirrors
        // `query_range`. Absent in-memory store is NOT an early return:
        // under persistence the windows may have been flushed-then-evicted
        // (or recovered from disk on restart), so the disk union below
        // still runs.
        let mut by_label_map: HashMap<
            BTreeMap<String, String>,
            BTreeMap<i64, Arc<dyn crate::storage_engines::types::AggregateCore>>,
        > = HashMap::new();

        if let Some(store) = self.series.get(&sid).map(|s| s.clone()) {
            let guard = store.write().unwrap();
            let mut by_label_id: HashMap<
                LabelValuesId,
                BTreeMap<i64, Arc<dyn crate::storage_engines::types::AggregateCore>>,
            > = HashMap::new();

            let mut buf: Vec<(TimestampRange, LabelValuesId, &AggPayload)> = Vec::new();
            guard
                .current_epoch
                .range_query_into(start_unix_ms, end_unix_ms, &mut buf);
            for (win, label_id, payload) in &buf {
                if let Some(p) = payload.as_exact_agg() {
                    by_label_id
                        .entry(*label_id)
                        .or_default()
                        .insert(win.1 as i64, Arc::from(p.clone_boxed_core()));
                }
            }
            buf.clear();

            for sealed in guard.sealed_epochs.values() {
                sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
                for (win, label_id, payload) in &buf {
                    if let Some(p) = payload.as_exact_agg() {
                        by_label_id
                            .entry(*label_id)
                            .or_default()
                            .insert(win.1 as i64, Arc::from(p.clone_boxed_core()));
                    }
                }
                buf.clear();
            }

            for (label_id, samples) in by_label_id {
                let label_values_map =
                    guard.intern.resolve(label_id).cloned().unwrap_or_default();
                by_label_map.entry(label_values_map).or_default().extend(samples);
            }
            drop(guard);
        }

        // Union the durable disk tier for the flushed-then-evicted portion
        // of the range. In-memory wins on a window-end collision.
        self.union_disk_exact_agg_into(sid, start_unix_ms, end_unix_ms, &mut by_label_map);

        by_label_map.into_iter().collect()
    }

    /// Union the durable disk tier's exact-aggregation entries into
    /// `by_label_map` for `[start, end)`. No-op when persistence is off.
    /// Disk entries are reconstructed via [`reconstruct_exact_agg`]; an
    /// unrecognized accumulator type is skipped (sketch-backed forms are
    /// served by `query_range`, not here — see PR follow-up note).
    /// Containment scan (`start_ts >= start && end_ts <= end`) matches the
    /// in-memory exact-agg `range_query_into`.
    fn union_disk_exact_agg_into(
        &self,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
        by_label_map: &mut HashMap<
            BTreeMap<String, String>,
            BTreeMap<i64, Arc<dyn crate::storage_engines::types::AggregateCore>>,
        >,
    ) {
        let handle = {
            let g = self.persistence_read.read().unwrap();
            match g.as_ref() {
                Some(h) => Arc::clone(h),
                None => return,
            }
        };
        let Some(keys) = self.sid_group_by_keys(sid) else {
            return;
        };
        let parts = handle
            .manifest
            .live_parts_overlapping(start_unix_ms, end_unix_ms);
        for pe in &parts {
            let reader = match handle.part_cache.get_or_load(pe.part_id) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(part_id = pe.part_id, error = %e, "exact-agg disk read: open part failed");
                    continue;
                }
            };
            for rec in reader.index_records() {
                if rec.agg_id != sid {
                    continue;
                }
                // Containment, matching the in-memory exact-agg scan.
                if !(rec.start_ts >= start_unix_ms && rec.end_ts <= end_unix_ms) {
                    continue;
                }
                let Ok(entry) = reader.load_entry(&rec) else {
                    continue;
                };
                let Some(acc) =
                    reconstruct_exact_agg(&entry.sketch_type_name, &entry.sketch_bytes)
                else {
                    continue;
                };
                let label_map = Self::rebuild_label_map(&keys, &entry.label);
                by_label_map
                    .entry(label_map)
                    .or_default()
                    // In-memory wins — only fill window-ends disk uniquely owns.
                    .entry(rec.end_ts as i64)
                    .or_insert_with(|| Arc::from(acc));
            }
        }
    }

    /// Actual coverage bounds `(min_window_start_ms, max_window_end_ms)`
    /// of the exact-agg windows this sid holds within `[start_unix_ms,
    /// end_unix_ms]`. `None` when no in-range exact-agg window exists.
    ///
    /// Issue #301 (Layer 4): `evaluate_exact_agg_rate` must divide by the
    /// ACTUAL data span — not the nominal `[r]` — when the producer has
    /// run for less than the requested range. `query_exact_agg_range`
    /// keys samples by `window_end` only, dropping `window_start`; this
    /// companion preserves the full `(start, end)` so the rate reducer
    /// can compute a coverage-aware divisor. Cheap (one epoch scan); the
    /// rate reducer already walks the same windows.
    pub fn exact_agg_coverage_bounds(
        &self,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Option<(u64, u64)> {
        let mut min_start: u64 = u64::MAX;
        let mut max_end: u64 = 0;
        let mut any = false;

        // In-memory tier (absent store is not an early return — disk may
        // still cover the range after flush+evict / restart).
        if let Some(store) = self.series.get(&sid).map(|s| s.clone()) {
            let guard = store.read().unwrap();
            let mut buf: Vec<(TimestampRange, LabelValuesId, &AggPayload)> = Vec::new();
            guard
                .current_epoch
                .range_query_into(start_unix_ms, end_unix_ms, &mut buf);
            for (win, _label_id, payload) in &buf {
                if payload.as_exact_agg().is_some() {
                    any = true;
                    min_start = min_start.min(win.0);
                    max_end = max_end.max(win.1);
                }
            }
            buf.clear();

            for sealed in guard.sealed_epochs.values() {
                sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
                for (win, _label_id, payload) in &buf {
                    if payload.as_exact_agg().is_some() {
                        any = true;
                        min_start = min_start.min(win.0);
                        max_end = max_end.max(win.1);
                    }
                }
                buf.clear();
            }
        }

        // Durable disk tier — same coverage-aware divisor must see the
        // flushed-then-evicted windows, else `rate` over-divides by the
        // nominal `[r]` once the recent data ages onto disk.
        if let Some(handle) = {
            let g = self.persistence_read.read().unwrap();
            g.as_ref().map(Arc::clone)
        } {
            let parts = handle
                .manifest
                .live_parts_overlapping(start_unix_ms, end_unix_ms);
            for pe in &parts {
                let Ok(reader) = handle.part_cache.get_or_load(pe.part_id) else {
                    continue;
                };
                for rec in reader.index_records() {
                    if rec.agg_id != sid {
                        continue;
                    }
                    if !(rec.start_ts >= start_unix_ms && rec.end_ts <= end_unix_ms) {
                        continue;
                    }
                    // Only count entries that reconstruct as exact-agg
                    // (skip sketch-backed disk entries under this sid).
                    let Ok(entry) = reader.load_entry(&rec) else {
                        continue;
                    };
                    if reconstruct_exact_agg(&entry.sketch_type_name, &entry.sketch_bytes)
                        .is_none()
                    {
                        continue;
                    }
                    any = true;
                    min_start = min_start.min(rec.start_ts);
                    max_end = max_end.max(rec.end_ts);
                }
            }
        }

        if any {
            Some((min_start, max_end))
        } else {
            None
        }
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
        Option<crate::storage_engines::types::KeyByLabelValues>,
        Vec<(
            (u64, u64),
            Arc<dyn crate::storage_engines::types::AggregateCore>,
        )>,
    > {
        let mut out: std::collections::HashMap<
            Option<crate::storage_engines::types::KeyByLabelValues>,
            Vec<(
                (u64, u64),
                Arc<dyn crate::storage_engines::types::AggregateCore>,
            )>,
        > = std::collections::HashMap::new();

        // Pick the sids whose metadata describes this (metric,
        // agg_type) tuple. We don't gate on `group_by_keys` here —
        // the engine's query-side filtering (label matchers) handles
        // that. Returning the superset is correct; over-returning is
        // just a perf cost the engine already absorbs.
        //
        // ── KNOWN GAP — sketch-backed aggs return empty here ────────
        // The `matches!` predicate below ONLY matches
        // `AggKind::ExactAgg`. Sketch-backed sids
        // (`AggKind::Sketch { kind, config, .. }`, registered by
        // `route_modified_otlp_sketches_to_precompute` for every
        // OTLP DDSketch/KLL/HLL/CountSketch/CountMinSketch DP) are
        // NEVER picked up — and the agg-keyed precompute query
        // unconditionally returns an empty map for them. The
        // legacy `ASAPQueryEngine::handle_query` path
        // (`query_engines/asap_query_engine/engine.rs::execute_store_query`)
        // falls through to "No precomputed outputs found" → the
        // HTTP layer renders `errorType: bad_data` / `error: "No
        // result for query"`. Sketches ARE in `SketchStore` and
        // are readable via the sid-keyed `query_range(sid, ...)`
        // path — they just aren't reachable via this agg-keyed
        // precompute lookup. Closing the gap means either teaching
        // this function to also collect sketch payloads (assemble
        // `Box<dyn AggregateCore>` from `payload.as_sketch()`),
        // OR routing `handle_query` through the newer
        // `ASAPQueryEngine::execute(&str)` trait path which uses
        // `idx.sids_for_policy(fp)` + reducer dispatch and
        // already handles sketches natively.
        //
        // Diagnosed in the e2e test arc (#247 → #248 → #249 →
        // #250 → engine-path debug session 2026-05).
        let candidate_sids: Vec<u64> = {
            let g = self.instances.read().unwrap();
            g.iter()
                .filter(|(_, m)| {
                    if m.metric_name != metric {
                        return false;
                    }
                    matches!(
                        &m.agg_kind,
                        AggKind::ExactAgg { agg_type: t, .. } if *t == agg_type
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
                if let Some(p) = payload.as_exact_agg() {
                    let label_values_map =
                        guard.intern.resolve(*label_id).cloned().unwrap_or_default();
                    let key = if label_values_map.is_empty() {
                        None
                    } else {
                        Some(crate::storage_engines::types::KeyByLabelValues {
                            labels: label_values_map.values().cloned().collect(),
                        })
                    };
                    out.entry(key)
                        .or_default()
                        .push((*win, Arc::from(p.clone_boxed_core())));
                }
            }
            buf.clear();

            for sealed in guard.sealed_epochs.values() {
                sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
                for (win, label_id, payload) in &buf {
                    if let Some(p) = payload.as_exact_agg() {
                        let label_values_map =
                            guard.intern.resolve(*label_id).cloned().unwrap_or_default();
                        let key = if label_values_map.is_empty() {
                            None
                        } else {
                            Some(crate::storage_engines::types::KeyByLabelValues {
                                labels: label_values_map.values().cloned().collect(),
                            })
                        };
                        out.entry(key)
                            .or_default()
                            .push((*win, Arc::from(p.clone_boxed_core())));
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
    /// candidate sids for ASAP-tier dispatch — a sid whose group-by KEYS
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

    /// Total count of in-memory SEALED epochs across all sids — i.e.
    /// epochs sealed (pending flush) but not yet evicted to disk. `0`
    /// once the flusher has drained everything. Used by tests and
    /// `/runtime` diagnostics to observe the flush-then-evict loop.
    pub fn list_sealed_epochs_len(&self) -> usize {
        let mut n = 0usize;
        for entry in self.series.iter() {
            if let Ok(data) = entry.value().read() {
                n += data.sealed_epochs.len();
            }
        }
        n
    }

    /// Number of registered instances (includes ghosts).
    pub fn instance_count(&self) -> usize {
        self.instances.read().unwrap().len()
    }

    /// Clone every registered `SketchInstanceMetadata` into a snapshot
    /// vec. Used by read-side primitives that need to scan the whole
    /// catalog without holding the registry lock across user code
    /// (e.g. `query::timeline::timeline_for_metric`). O(N) clone +
    /// O(N) memory; cheap at production catalog sizes.
    pub fn snapshot_instances(&self) -> Vec<SketchInstanceMetadata> {
        match self.instances.read() {
            Ok(map) => map.values().cloned().collect(),
            Err(_) => Vec::new(),
        }
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

    /// Record that the store has reconciled against the
    /// `Arc<StreamingConfig>` identified by `config_ptr` (the value of
    /// `Arc::as_ptr(..) as usize`), returning `true` if this is a *new*
    /// config pointer (i.e. the caller should run a full reconcile) or
    /// `false` if the store already reconciled against this exact
    /// config and the scan can be skipped.
    ///
    /// Used by [`crate::storage_engines::sketch_db::lifecycle::reconcile_if_config_changed`]
    /// to make the per-ingest-batch reconcile a single relaxed atomic
    /// load in the common (config-unchanged) case.
    pub fn mark_reconciled_config(&self, config_ptr: usize) -> bool {
        use std::sync::atomic::Ordering;
        if self.last_reconciled_config_ptr.load(Ordering::Relaxed) == config_ptr {
            return false;
        }
        self.last_reconciled_config_ptr
            .store(config_ptr, Ordering::Relaxed);
        true
    }

    /// Visit every registered instance under a single read lock,
    /// invoking `f(sid, &meta)` for each. Lets read-side scans that
    /// only need to *inspect* metadata (signature derivation,
    /// status filtering) avoid the O(N) deep clone that
    /// [`Self::snapshot_instances`] performs — each
    /// `SketchInstanceMetadata` carries a `String` + `BTreeSet<String>`
    /// + `AggKind` (more strings), so the clone is allocation-heavy at
    /// production catalog sizes.
    ///
    /// The closure runs while the read lock is held, so it must not
    /// call back into the store (which would deadlock) and should stay
    /// allocation-light. Callers that need to mutate or call user code
    /// should collect the cheap data they need (e.g. `Vec<u64>` of
    /// sids) here, then act after this returns.
    pub fn for_each_instance<F: FnMut(u64, &SketchInstanceMetadata)>(&self, mut f: F) {
        if let Ok(map) = self.instances.read() {
            for (sid, meta) in map.iter() {
                f(*sid, meta);
            }
        }
    }

    /// Iterate (clones) all instance metadata matching `status`.
    /// Used by the eviction service to enumerate `Expired` sids
    /// without holding a long read lock.
    pub fn list_by_status(&self, status: AggStatus) -> Vec<SketchInstanceMetadata> {
        let map = match self.instances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        map.values()
            .filter(|s| s.status() == status)
            .cloned()
            .collect()
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

    /// Drop a sid's metadata + its series state + the reverse-index
    /// entry. Mirrors `SchemaRegistry::remove_schema` for the eviction
    /// path's post-data-drop cleanup. Returns the removed metadata, or
    /// `None` if the sid was absent.
    pub fn remove_instance(&self, sid: u64) -> Option<SketchInstanceMetadata> {
        let removed = self.instances.write().ok()?.remove(&sid);
        if let Some(meta) = &removed {
            self.series.remove(&sid);
            if !meta.policy_fp.is_unset() {
                let mut idx = self.policy_to_sids.write().unwrap();
                if let Some(set) = idx.get_mut(&meta.policy_fp) {
                    set.remove(&sid);
                    if set.is_empty() {
                        idx.remove(&meta.policy_fp);
                    }
                }
            }
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
    /// `SketchStoreSink` (live ingest) and `BackfillWindowProcessor`
    /// (archive replay) so they share one canonical sid-derivation
    /// path.
    ///
    /// Returns the sid the entry landed under, or `None` when the
    /// write is dropped: either because the agg_config / output
    /// combination doesn't fit the precompute model (caller logs and
    /// skips), or because the sid already exists in `Retired` /
    /// `Expired` status. The latter is the sid-level mirror of the
    /// `SchemaRegistry::is_writable(agg_id)` §6.3 ingest barrier:
    /// once a sid is retired by [`crate::storage_engines::sketch_db::lifecycle::reconcile_from_streaming_config`]
    /// further writes are rejected here so the eviction sweep can
    /// drop residual state cleanly.
    pub fn ingest_precompute_for_agg_config(
        &self,
        mint_sid: impl FnOnce(&str, &str, &str) -> u64,
        agg_cfg: &asap_types::aggregation_config::AggregationConfig,
        output: &crate::storage_engines::types::PrecomputedOutput,
        accumulator: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        // B7.7 — this wrapper now derives the sid via `mint_sid` and
        // delegates to `ingest_precompute_with_sid`. Callers that
        // already hold the bucket sid (B7.6's worker passes it on the
        // `WorkerMessage`; B7.7's backfill processor groups raw
        // samples by sid up-front) skip the resolver round-trip by
        // invoking the sid-direct sibling.
        let (attrs_fp, _label_values_map) = build_attrs_fp_and_label_map(agg_cfg, output);
        let agg_kind = AggKind::ExactAgg {
            agg_type: agg_cfg.aggregation_type,
            parameters_canonical: canonical_parameters(&agg_cfg.parameters),
            // The canonical spatial-filter participates in sid identity
            // so filter-distinct policies don't collide on the same
            // (metric, attrs, agg_kind) tuple. `spatial_filter_normalized`
            // is the canonicalized form produced by
            // `asap_types::utils::normalize_spatial_filter`.
            spatial_filter_canonical: agg_cfg.spatial_filter_normalized.clone(),
        };
        // Sid mint delegated to the caller's closure — typically
        // `|m, fp, ak| series_resolver.resolve(m, fp, ak)`. Keeps the
        // SketchStore free of any layer-inverted dependency on the
        // resolver type (which lives in `drivers::ingest`). Tests
        // pass either a real local resolver or a counter-mock
        // closure.
        let agg_kind_canonical = agg_kind.canonical_string();
        let sid = mint_sid(&agg_cfg.metric, &attrs_fp, &agg_kind_canonical);
        self.ingest_precompute_with_sid(sid, agg_cfg, output, accumulator)
    }

    /// B7.7 sid-direct sibling of [`Self::ingest_precompute_for_agg_config`].
    ///
    /// Callers that already hold the bucket sid (the live worker after
    /// B7.6 reshaped its `WorkerMessage`, and the backfill processor
    /// after B7.7 rekeyed its per-window grouping from group_key to
    /// sid) skip the mint round-trip by handing the sid in directly.
    /// The mint-driven [`Self::ingest_precompute_for_agg_config`] is
    /// content-addressed and idempotent with this method — passing the
    /// resolver-minted sid here yields the same state under the same
    /// sid — so both methods can coexist while migration finishes.
    ///
    /// The §6.3 ingest barrier (`Retired` / `Expired` sids reject
    /// writes) and first-sight metadata registration are identical to
    /// the mint-driven path.
    pub fn ingest_precompute_with_sid(
        &self,
        sid: u64,
        agg_cfg: &asap_types::aggregation_config::AggregationConfig,
        output: &crate::storage_engines::types::PrecomputedOutput,
        accumulator: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        let (_attrs_fp, label_values_map) = build_attrs_fp_and_label_map(agg_cfg, output);
        let key_names = &agg_cfg.grouping_labels.labels;
        let agg_kind = AggKind::ExactAgg {
            agg_type: agg_cfg.aggregation_type,
            parameters_canonical: canonical_parameters(&agg_cfg.parameters),
            spatial_filter_canonical: agg_cfg.spatial_filter_normalized.clone(),
        };

        match self.instance(sid) {
            None => {
                let group_by_keys: BTreeSet<String> = key_names.iter().cloned().collect();
                // PR 6 follow-up: ExactAgg-backed sids carry an
                // `ExactAgg(agg_type)` capability so the analyzer can
                // route ASAP-tier-answerable exact intents (Sum / Rate /
                // Increase / Count{Exact}) to this sid instead of falling
                // through to the archive engine. Pre-PR-6 this field was
                // unconditionally `None`, which meant ASAP-tier ExactAgg
                // state was reachable only through the legacy precompute
                // query path; capability-matching couldn't see it.
                self.register(SketchInstanceMetadata {
                    sid,
                    metric_name: agg_cfg.metric.clone(),
                    group_by_keys,
                    capability: Some(Capability::ExactAgg(agg_cfg.aggregation_type)),
                    agg_kind,
                    accuracy: None,
                    first_seen_unix_ms: output.start_timestamp as i64,
                    retired_at_ms: None,
                    expires_at_ms: None,
                    // Trust the caller's output — it carries the
                    // policy fingerprint computed at emit time
                    // (precompute worker / backfill processor).
                    // Falling back to `from_config(&agg_cfg)` here
                    // would also be correct but redundant.
                    policy_fp: output.policy_fp,
                });
            }
            Some(existing) if !existing.is_writable() => {
                return None;
            }
            Some(_) => {}
        }

        let window = (output.start_timestamp, output.end_timestamp);
        self.append_precompute(
            sid,
            label_values_map,
            window,
            accumulator.clone_boxed_core(),
        );
        Some(sid)
    }

    /// Phase 5 M2.3.6d — eviction-side helper. Removes every sid in the
    /// index whose metadata was registered against `agg_cfg`, i.e.
    /// shares the same metric, agg_type, parameters canonicalization,
    /// and grouping-keys set the `SketchStoreSink` used at write time.
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
                        AggKind::ExactAgg { agg_type, parameters_canonical, .. }
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
/// sid-keyed ASAP tier. Constructed via [`SketchStore::start_persistence`];
/// the flusher reads sealed epochs through the
/// [`EpochSource`](crate::storage_engines::sketch_db::index::persistence::EpochSource)
/// impl on `SketchStore` and writes parts under `disk_path/parts/`.
///
/// Drop or call [`Self::shutdown`] to stop the flusher cleanly. The
/// `part_cache` field is exposed so the query path can be wired up to
/// read-back from disk in a subsequent sub-PR; today it sits idle
/// because the in-memory `query_range` doesn't yet consult it.
pub struct SketchIndexPersistence {
    pub manifest: Arc<crate::storage_engines::sketch_db::index::persistence::Manifest>,
    pub part_cache: crate::storage_engines::sketch_db::index::persistence::cache::PartCache,
    pub flusher: crate::storage_engines::sketch_db::index::persistence::flusher::FlusherHandle,
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
        cfg: crate::storage_engines::sketch_db::index::persistence::SketchStorePersistenceConfig,
    ) -> crate::storage_engines::sketch_db::index::persistence::PersistResult<SketchIndexPersistence>
    {
        use crate::storage_engines::sketch_db::index::persistence::{
            cache::PartCache, flusher::FlusherHandle, recovery, Manifest,
        };

        let (_loaded_manifest, report) = recovery::recover(&cfg.disk_path)?;
        tracing::info!(
            live = report.live_parts,
            corrupt_removed = report.corrupt_parts_removed,
            orphans_removed = report.orphan_parts_removed,
            "SketchStore persistence recovery complete"
        );

        // Re-register every disk-resident sid from the metadata sidecar so
        // the query path can find and serve recovered series. Without this
        // a freshly-reopened store recovers the parts manifest/cache but
        // has an EMPTY `instances` registry (registration only happens on
        // the live ingest path), so `instances_matching` enumerates nothing
        // for the recovered metrics and `query_range`'s disk-union
        // early-returns on the missing `sid_group_by_keys` → "No result"
        // cluster-wide even though the data is durable on disk. Idempotent:
        // sids already registered (e.g. by an in-flight DataPoint) are kept.
        let recovered = self.register_recovered_disk_series(&cfg.disk_path);
        if recovered > 0 {
            tracing::info!(
                recovered_sids = recovered,
                "SketchStore: re-registered disk-resident sids from metadata sidecar"
            );
        }

        let manifest = Arc::new(Manifest::open_or_init(&cfg.disk_path)?);
        let parts_root = crate::storage_engines::sketch_db::index::persistence::flusher::parts_root(
            &cfg.disk_path,
        );
        let part_cache = PartCache::new(parts_root.clone(), cfg.part_cache_bytes);

        // Install the durable-tier read handle + seal cadence so the
        // query path unions disk parts and the per-sid stores seal on
        // cadence with retention-drop disabled. Done BEFORE the flusher
        // starts so any series created between here and the first flush
        // tick are already in persistence mode.
        self.enable_persistence_mode(
            cfg.seal_window_count,
            Arc::new(PersistenceReadHandle {
                manifest: Arc::clone(&manifest),
                part_cache: part_cache.clone(),
            }),
        );

        let flusher = FlusherHandle::start(cfg, Arc::clone(&manifest), Arc::clone(self))?;

        Ok(SketchIndexPersistence {
            manifest,
            part_cache,
            flusher,
            parts_root,
        })
    }

    /// Replay the per-sid metadata sidecar at `disk_path` and register
    /// each disk-resident sid as a queryable instance, UNLESS the sid is
    /// already registered (a live DataPoint won the race — its in-memory
    /// metadata is authoritative, so we don't clobber it). Returns the
    /// number of sids freshly registered from disk.
    ///
    /// `capability` / `accuracy` are re-derived from the persisted
    /// `agg_kind` exactly as the ingest path derives them. The sidecar is
    /// missing only for parts written before this feature landed (or a
    /// fresh dir) — those sids stay invisible until a live DataPoint
    /// re-registers them, the same as pre-fix behavior.
    pub fn register_recovered_disk_series(&self, disk_path: &std::path::Path) -> usize {
        use crate::storage_engines::sketch_db::index::persistence::metadata::SidMetadataStore;

        let store = SidMetadataStore::new(disk_path);
        let records = match store.load() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load sid metadata sidecar on recovery");
                return 0;
            }
        };

        let mut registered = 0usize;
        for rec in records {
            // Don't clobber a live-registered instance.
            if self.instance(rec.sid).is_some() {
                continue;
            }
            let Some(agg_kind) = rec.agg_kind() else {
                tracing::warn!(
                    sid = rec.sid,
                    "skipping recovered sid: unrecognized agg_kind in sidecar"
                );
                continue;
            };
            let capability = rec.capability();
            let accuracy = rec.accuracy();
            self.register(SketchInstanceMetadata {
                sid: rec.sid,
                metric_name: rec.metric_name,
                group_by_keys: rec.group_by_keys.into_iter().collect(),
                capability,
                agg_kind,
                accuracy,
                first_seen_unix_ms: rec.first_seen_unix_ms,
                retired_at_ms: None,
                expires_at_ms: None,
                // The sidecar doesn't carry the policy fingerprint; the
                // recovered sid is reachable through the
                // `instances_matching(metric, gbk)` walk regardless (the
                // policy_fp reverse index is an optimization, not a
                // correctness requirement for the query path).
                policy_fp: PolicyFingerprint::UNSET,
            });
            registered += 1;
        }
        registered
    }

    /// Switch the store into durable-tier mode: install the read handle
    /// the query path uses to consult disk parts, set the per-sid seal
    /// cadence, and retro-fit any already-created `SidStoreData` so they
    /// seal on cadence and stop dropping aged windows (the flush-then-
    /// evict loop becomes the memory bound). Idempotent.
    pub fn enable_persistence_mode(
        &self,
        seal_window_count: usize,
        read_handle: Arc<PersistenceReadHandle>,
    ) {
        use std::sync::atomic::Ordering;
        *self.persistence_read.write().unwrap() = Some(read_handle);
        self.seal_window_count
            .store(seal_window_count, Ordering::Relaxed);
        if seal_window_count > 0 {
            // Retro-fit existing per-sid stores (e.g. series that
            // ingested before persistence finished starting).
            for entry in self.series.iter() {
                if let Ok(mut data) = entry.value().write() {
                    data.seal_window_count = Some(seal_window_count);
                    data.persistence_enabled = true;
                }
            }
        }
    }
}

// ── Phase 5 M2.3.6b — EpochSource impl ──────────────────────────────────────
//
// Lets the existing persistence flusher (`store/persistence/flusher.rs`)
// drive `SketchStore` instead of `SketchStorePerKey`. The `agg_id: u64`
// field on `SealedEpochRef` / `EpochSnapshot` carries a `sid` here —
// the trait keeps the historical name so the flusher / manifest /
// part-writer stay untouched.
impl crate::storage_engines::sketch_db::index::persistence::EpochSource for SketchStore {
    fn list_sealed_epochs(
        &self,
    ) -> Vec<crate::storage_engines::sketch_db::index::persistence::SealedEpochRef> {
        use crate::storage_engines::sketch_db::index::persistence::SealedEpochRef;
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

    fn instance_metadata_for_persist(
        &self,
        sid: u64,
    ) -> Option<crate::storage_engines::sketch_db::index::persistence::metadata::SidMetaRecord> {
        let g = self.instances.read().ok()?;
        let m = g.get(&sid)?;
        Some(
            crate::storage_engines::sketch_db::index::persistence::metadata::SidMetaRecord::new(
                m.sid,
                m.metric_name.clone(),
                m.group_by_keys.iter().cloned().collect(),
                &m.agg_kind,
                m.first_seen_unix_ms,
            ),
        )
    }

    fn snapshot_sealed_epoch(
        &self,
        sid: u64,
        epoch_id: u64,
    ) -> crate::storage_engines::sketch_db::index::persistence::PersistResult<
        Option<crate::storage_engines::sketch_db::index::persistence::source::EpochSnapshot>,
    > {
        use crate::storage_engines::sketch_db::index::persistence::source::{
            EpochSnapshot, EpochSnapshotEntry,
        };
        use crate::storage_engines::sketch_db::index::persistence::PersistError;

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
                    AggKind::ExactAgg { .. } => None,
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
                    Some(crate::storage_engines::types::KeyByLabelValues {
                        labels: m.values().cloned().collect(),
                    })
                }
            });
            let (type_name, encoding_tag, bytes) = match payload {
                AggPayload::Sketch(s) => (
                    sketch_kind_label
                        .clone()
                        .unwrap_or_else(|| "UnknownSketch".to_string()),
                    encoding_to_tag(s.encoding),
                    s.bytes.clone(),
                ),
                AggPayload::ExactAgg(p) => (p.type_name().to_string(), 0u8, {
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
                encoding_tag,
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

    fn seal_aged_epochs(&self, cutoff_end: u64) -> usize {
        let mut sealed = 0usize;
        for entry in self.series.iter() {
            let Ok(mut data) = entry.value().write() else {
                continue;
            };
            sealed += data.seal_aged_windows(cutoff_end);
        }
        sealed
    }

    fn approx_memory_bytes(&self) -> usize {
        // Account for BOTH `current_epoch` (hot, un-sealed) AND sealed
        // epochs. Counting sealed-only under-reports the true footprint
        // (the live "0.00–0.12 KB approx sealed bytes" diagnostic) and,
        // worse, blinds the flusher's memory-pressure trigger to the bulk
        // of memory — which under persistence (retention-drop disabled)
        // lives in `current_epoch` until the time-driven seal rolls it
        // over. The hot-window seal (`seal_aged_epochs`) handles the
        // common case; this keeps the memory-pressure backstop honest for
        // a burst that outruns the hot window.
        let mut total = 0usize;
        for entry in self.series.iter() {
            let Ok(data) = entry.value().read() else {
                continue;
            };
            for (_, _, payload) in data.current_epoch.iter_entries() {
                total += payload.approx_bytes();
            }
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
        meta_with_policy(sid, asap_types::PolicyFingerprint::UNSET)
    }

    fn meta_with_policy(
        sid: u64,
        policy_fp: asap_types::PolicyFingerprint,
    ) -> SketchInstanceMetadata {
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
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp,
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
    fn range_query_uses_overlap_not_containment() {
        // The sketch read path uses HALF-OPEN OVERLAP, not containment:
        // any pane intersecting `[start, end)` is returned so the reducer
        // can establish a rolling base even when no pane is fully
        // contained (the ~30s-pane case behind the live `[30s]` "No
        // result" bug). Of `(0,10)`, `(10,20)`, `(20,30)` against `[5,25)`:
        //  - `(0,10)`  overlaps  (10 > 5)            → included (straddles left edge)
        //  - `(10,20)` overlaps                       → included
        //  - `(20,30)` overlaps  (20 < 25)            → included (straddles right edge)
        let idx = SketchStore::new();
        idx.register(meta(13));
        let lv = BTreeMap::new();
        idx.append_sample(13, lv.clone(), (0, 10), sample(1));
        idx.append_sample(13, lv.clone(), (10, 20), sample(2));
        idx.append_sample(13, lv.clone(), (20, 30), sample(3));

        let series = idx.query_range(13, 5, 25);
        assert_eq!(series.len(), 1);
        let s = &series[0];
        assert_eq!(s.samples.len(), 3, "all three panes overlap [5,25)");
        assert!(s.samples.contains_key(&10));
        assert!(s.samples.contains_key(&20));
        assert!(s.samples.contains_key(&30));
    }

    #[test]
    fn range_query_excludes_non_overlapping_panes() {
        // Overlap must still EXCLUDE panes that don't intersect the
        // window — a pane ending exactly at `start` (half-open: `w.1 >
        // start` is false) and one starting at/after `end`.
        let idx = SketchStore::new();
        idx.register(meta(14));
        let lv = BTreeMap::new();
        idx.append_sample(14, lv.clone(), (0, 10), sample(1)); // ends at start=10 → excluded
        idx.append_sample(14, lv.clone(), (10, 20), sample(2)); // overlaps → included
        idx.append_sample(14, lv.clone(), (30, 40), sample(3)); // starts at end=30 → excluded

        let series = idx.query_range(14, 10, 30);
        assert_eq!(series.len(), 1);
        let s = &series[0];
        assert_eq!(s.samples.len(), 1, "only the (10,20) pane overlaps [10,30)");
        assert!(s.samples.contains_key(&20));
    }

    #[test]
    fn range_query_short_window_straddling_pane_with_delta_carry_in() {
        // Live gap 1: a `[30s]`-style window narrower than the agent's
        // ~30s pane cadence. The freshest pane STRADDLES the window's left
        // edge (starts before `start`, ends inside), so strict containment
        // returned nothing → "No result". Overlap admits the straddling
        // delta pane, and the carry-in splices the prior Full as its base.
        let idx = SketchStore::new();
        idx.register(meta(15));
        let lv = BTreeMap::new();
        // Full pane fully before the window.
        idx.append_sample(15, lv.clone(), (260, 290), sample(1));
        // Delta pane STRADDLING the window's left edge [300,330): starts
        // at 295 (< 300), ends at 325 (inside). Containment excludes it
        // (295 < 300); overlap includes it.
        idx.append_sample(15, lv.clone(), (295, 325), delta_sample(2));

        let series = idx.query_range(15, 300, 330);
        assert_eq!(series.len(), 1, "straddling delta pane is now visible");
        let s = &series[0];
        assert!(
            s.samples.contains_key(&325),
            "straddling in-window delta (end=325) admitted by overlap"
        );
        assert!(
            s.samples.contains_key(&290),
            "prior Full (end=290) carried in as the delta's base"
        );
    }

    fn delta_sample(b: u8) -> SketchSampleState {
        SketchSampleState {
            bytes: vec![b],
            encoding: SketchEncoding::ProtoDelta,
        }
    }

    #[test]
    fn range_query_carries_in_latest_full_before_window() {
        // The delta-stitching carry-in: a Full lands BEFORE the query
        // window and only deltas land inside it. `query_range` must
        // splice in the most-recent pre-window Full so the downstream
        // delta-apply reducer can establish a rolling base. Without it,
        // a short window that contains only deltas yields an
        // unanswerable series (the live quantile/HLL "No result" bug).
        let idx = SketchStore::new();
        idx.register(meta(21));
        let lv = BTreeMap::new();
        // Two Fulls before the window; the LATER one (end=200) is the
        // base that must be carried in.
        idx.append_sample(21, lv.clone(), (90, 100), sample(1));
        idx.append_sample(21, lv.clone(), (190, 200), sample(2));
        // Delta-only inside the window [300, 400].
        idx.append_sample(21, lv.clone(), (310, 320), delta_sample(3));

        let series = idx.query_range(21, 300, 400);
        assert_eq!(series.len(), 1);
        let s = &series[0];
        // In-window delta (end=320) + carried-in latest Full (end=200).
        assert!(s.samples.contains_key(&320), "in-window delta present");
        assert!(
            s.samples.contains_key(&200),
            "latest pre-window Full (end=200) carried in as base"
        );
        assert!(
            !s.samples.contains_key(&100),
            "only the LATEST pre-window Full is carried in, not older ones"
        );
        // The carried-in entry must be a Full (the reducer needs a base).
        assert_eq!(s.samples.get(&200).unwrap().encoding, SketchEncoding::ProtoFull);
    }

    #[test]
    fn range_query_no_carry_in_when_window_leads_with_full() {
        // If the in-window samples already lead with a Full, no carry-in
        // is needed (and none should be spliced — it would be redundant
        // and could skew coverage).
        let idx = SketchStore::new();
        idx.register(meta(22));
        let lv = BTreeMap::new();
        idx.append_sample(22, lv.clone(), (90, 100), sample(1));
        idx.append_sample(22, lv.clone(), (310, 320), sample(2)); // Full in-window
        idx.append_sample(22, lv.clone(), (330, 340), delta_sample(3));

        let series = idx.query_range(22, 300, 400);
        assert_eq!(series.len(), 1);
        let s = &series[0];
        assert!(s.samples.contains_key(&320));
        assert!(s.samples.contains_key(&340));
        assert!(
            !s.samples.contains_key(&100),
            "no carry-in when the window already leads with a Full"
        );
    }

    /// Test-only: override a single sid's WARM retention horizon so a
    /// test can drive eviction without mutating the process-global
    /// `ASAP_SKETCH_RETENTION_MS` env (which would race other tests).
    /// The sid must already have state (call after the first
    /// `append_sample`).
    fn set_retention_horizon_for_test(store: &SketchStore, sid: u64, horizon_ms: Option<u64>) {
        if let Some(s) = store.series.get(&sid) {
            s.write().unwrap().retention_horizon_ms = horizon_ms;
        }
    }

    #[test]
    fn retention_bounds_memory_yet_keeps_recent_windows_queryable() {
        // Regression for the production leak: under steady ~30s-pane
        // ingest the per-sid window count grew unbounded because nothing
        // sealed and `current_epoch` was never trimmed. With a bounded
        // horizon (a) old windows are evicted (memory stays O(horizon)),
        // and (b) recent windows within the horizon remain queryable via
        // the overlap-scan + delta carry-in read path (#323–#326).
        let idx = SketchStore::new();
        idx.register(meta(77));
        let lv = BTreeMap::new();
        let window_ms = 30_000u64; // 30s panes
        let horizon_ms = 60 * 60 * 1000u64; // 1h

        // Seed one window, then set the horizon, then stream the rest.
        idx.append_sample(77, lv.clone(), (0, window_ms), sample(0));
        set_retention_horizon_for_test(&idx, 77, Some(horizon_ms));

        // 4h of ingest = 480 panes. Emit a Full at the start of each
        // 30-pane (~15m) block, deltas otherwise — mirrors the agent's
        // periodic-Full + cheap-delta cadence so the carry-in has a base.
        let mut start = window_ms;
        let mut last_end = window_ms;
        for i in 1..480u64 {
            last_end = start + window_ms;
            let s = if i % 30 == 0 {
                sample((i % 250) as u8)
            } else {
                delta_sample((i % 250) as u8)
            };
            idx.append_sample(77, lv.clone(), (start, last_end), s);
            start += window_ms;
        }

        // (a) Memory bound: distinct windows ≈ horizon/window, NOT 480.
        let retained = {
            let g = idx.series.get(&77).unwrap();
            let r = g.read().unwrap().current_epoch.distinct_windows();
            r
        };
        let expected = (horizon_ms / window_ms) as usize;
        assert!(
            retained <= expected + 2,
            "retained {retained} windows; horizon should bound to ~{expected}"
        );
        assert!(retained < 480, "old windows were not evicted (leak persists)");

        // (b) A 30m range query ending at the freshest window still
        // resolves (well within the 1h horizon) AND the carry-in finds a
        // Full base for any leading delta — no regression to #323–#326.
        let q_start = last_end - 30 * 60 * 1000;
        let series = idx.query_range(77, q_start, last_end);
        assert_eq!(series.len(), 1, "recent 30m window must stay queryable");
        let s = &series[0];
        assert!(!s.samples.is_empty(), "30m range query returned no samples");
        let first = s.samples.values().next().unwrap();
        assert!(
            matches!(
                first.encoding,
                SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
            ),
            "earliest sample in the answer must be a Full base (carry-in intact)"
        );
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

    // sid-hash unit tests removed alongside `compute_sid` (PR-4) and
    // `compute_sketch_sid` (PR-3). The identity properties they
    // exercised — `(metric, attrs, agg_kind)` discriminates sids,
    // sketch and precompute never collide, agg_type and parameters
    // each contribute to identity — are now covered by:
    //
    //  - `series_resolver::tests::distinct_agg_kinds_same_series_distinct_sids`
    //    (the resolver's identity contract under Interpretation B)
    //  - `AggKind::canonical_string` is exhaustive over the AggKind
    //    enum, so two variants whose fields differ produce different
    //    canonical strings → different resolver cache keys → different
    //    sids by construction.

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
        precompute_meta.agg_kind = AggKind::ExactAgg {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
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
        precompute_meta.agg_kind = AggKind::ExactAgg {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
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

        let result = idx.query_precomputes_by_agg("cpu_seconds", AggregationType::Sum, 0, 10_000);
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

        let result = idx.query_precomputes_by_agg("m", AggregationType::Sum, 0, 10_000);
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
        use crate::storage_engines::sketch_db::index::persistence::EpochSource;
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
        use crate::storage_engines::sketch_db::index::persistence::EpochSource;
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
        assert_eq!(
            entry.sketch_bytes.len(),
            1,
            "single-byte sample bytes carry"
        );
    }

    #[test]
    fn epoch_source_evict_drops_the_epoch() {
        use crate::storage_engines::sketch_db::index::persistence::EpochSource;
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
        use crate::storage_engines::sketch_db::index::persistence::EpochSource;
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
        assert!(sketch.as_exact_agg().is_none());

        use crate::precompute_engine::operators::SumAccumulator;
        let exact_agg = AggPayload::ExactAgg(Box::new(SumAccumulator::with_sum(1.0)));
        assert!(exact_agg.as_sketch().is_none());
        assert!(exact_agg.as_exact_agg().is_some());
    }

    // `compute_sketch_sid_matches_new_compute_sid_for_sketch_branch`
    // removed — it tested parity between two hash wrappers, and the
    // outer wrapper is now gone. The remaining `compute_sid_*` tests
    // exercise the encoding properties that PR-4 will lean on when it
    // migrates the precompute path to the resolver.

    // ── policy_fp reverse-index tests ────────────────────────────────

    #[test]
    fn sids_for_policy_returns_empty_for_unset_or_missing() {
        let idx = SketchStore::new();
        // Empty store → nothing for any fp.
        assert!(idx
            .sids_for_policy(asap_types::PolicyFingerprint(42))
            .is_empty());
        // The UNSET sentinel always returns empty regardless of state.
        idx.register(meta_with_policy(1, asap_types::PolicyFingerprint::UNSET));
        assert!(idx
            .sids_for_policy(asap_types::PolicyFingerprint::UNSET)
            .is_empty());
    }

    #[test]
    fn register_indexes_one_sid_under_its_policy() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        assert_eq!(idx.sids_for_policy(fp), vec![1]);
        assert_eq!(idx.policy_count(), 1);
    }

    #[test]
    fn register_groups_multiple_sids_under_one_policy() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        idx.register(meta_with_policy(2, fp));
        idx.register(meta_with_policy(3, fp));
        let mut sids = idx.sids_for_policy(fp);
        sids.sort();
        assert_eq!(sids, vec![1, 2, 3]);
        assert_eq!(idx.policy_count(), 1);
    }

    #[test]
    fn register_separates_distinct_policies() {
        let idx = SketchStore::new();
        let fp_a = asap_types::PolicyFingerprint(7);
        let fp_b = asap_types::PolicyFingerprint(8);
        idx.register(meta_with_policy(1, fp_a));
        idx.register(meta_with_policy(2, fp_b));
        idx.register(meta_with_policy(3, fp_a));
        assert_eq!(idx.sids_for_policy(fp_a), vec![1, 3]);
        assert_eq!(idx.sids_for_policy(fp_b), vec![2]);
        assert_eq!(idx.policy_count(), 2);
    }

    #[test]
    fn unset_policy_sids_are_not_in_reverse_index() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        // sid 2 has UNSET — should NOT show up under any fp.
        idx.register(meta_with_policy(2, asap_types::PolicyFingerprint::UNSET));
        assert_eq!(idx.sids_for_policy(fp), vec![1]);
        assert_eq!(idx.policy_count(), 1);
        // But it IS still in the main `instances` map.
        assert!(idx.instance(2).is_some());
    }

    #[test]
    fn remove_instance_drops_reverse_index_entry() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        idx.register(meta_with_policy(2, fp));
        idx.remove_instance(1);
        assert_eq!(idx.sids_for_policy(fp), vec![2]);
        assert_eq!(idx.policy_count(), 1);
        idx.remove_instance(2);
        assert!(idx.sids_for_policy(fp).is_empty());
        // Empty entry collapses — policy_count drops to 0.
        assert_eq!(idx.policy_count(), 0);
    }

    // ── Durable disk-backed tier (feat/sketch-durable-tier) ─────────────

    use crate::storage_engines::sketch_db::index::persistence::EpochSource;
    use crate::storage_engines::sketch_db::index::persistence::SketchStorePersistenceConfig;

    /// Metadata with a single group-by key `host`, so the disk read-back
    /// path can rebuild the `{host: <v>}` label map from the stored
    /// values vector.
    fn meta_with_host_key(sid: u64) -> SketchInstanceMetadata {
        let mut m = meta(sid);
        m.group_by_keys = ["host".to_string()].into_iter().collect();
        m
    }

    fn lv_host(v: &str) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert("host".to_string(), v.to_string());
        m
    }

    /// Aggressive persistence config: seal every window, force-flush
    /// everything (hot_window=0), tiny flush interval, no disk TTL, small
    /// part cache. Memory limit high so the seal/flush is driven by the
    /// hot-window watermark, not memory pressure — keeps the test
    /// deterministic.
    fn durable_cfg(disk_path: std::path::PathBuf) -> SketchStorePersistenceConfig {
        SketchStorePersistenceConfig {
            memory_limit_bytes: 1 << 30,
            memory_low_watermark_bytes: 1 << 29,
            hard_cap_bytes: 1 << 31,
            hot_window_ms: Some(0), // every sealed epoch is "old" → flush now
            delete_older_than_ms: None,
            flush_interval: std::time::Duration::from_millis(5),
            disk_path,
            part_cache_bytes: 1 << 20,
            seal_window_count: 1, // seal on every distinct window
        }
    }

    fn wait_until<F: Fn() -> bool>(f: F, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if f() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        f()
    }

    #[test]
    fn sealing_fires_under_persistence() {
        let tmp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(SketchStore::new());
        idx.register(meta_with_host_key(101));
        let _p = idx.start_persistence(durable_cfg(tmp.path().to_path_buf())).unwrap();

        // seal_window_count = 1: each *new distinct window* seals the
        // prior one. Append several distinct windows for one series.
        for i in 0..5u64 {
            let s = i * 30_000;
            idx.append_sample(101, lv_host("a"), (s, s + 30_000), sample(i as u8));
        }
        // At least some sealed epochs must exist (each new window seals
        // the prior current_epoch). The flusher may evict some before we
        // look, so we assert that sealing happened OR a part landed.
        let sealed_now = !idx.list_sealed_epochs().is_empty();
        let flushed = wait_until(|| !_p.manifest.live_parts().is_empty(), std::time::Duration::from_secs(3));
        assert!(
            sealed_now || flushed,
            "no epochs sealed and nothing flushed — sealing did not fire under persistence"
        );
    }

    #[test]
    fn sealed_epochs_flush_to_disk_and_memory_drops() {
        let tmp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(SketchStore::new());
        idx.register(meta_with_host_key(202));
        let p = idx.start_persistence(durable_cfg(tmp.path().to_path_buf())).unwrap();

        for i in 0..10u64 {
            let s = i * 30_000;
            idx.append_sample(202, lv_host("a"), (s, s + 30_000), sample(i as u8));
        }

        // The flusher should drain sealed epochs to disk; memory
        // (sealed-epoch bytes) drops to ~0 and parts appear.
        let drained = wait_until(
            || idx.approx_memory_bytes() == 0 && !p.manifest.live_parts().is_empty(),
            std::time::Duration::from_secs(5),
        );
        assert!(
            drained,
            "flush+evict did not bound memory: sealed_bytes={}, parts={}",
            idx.approx_memory_bytes(),
            p.manifest.live_parts().len()
        );
    }

    #[test]
    fn query_resolves_from_disk_after_flush_evict() {
        let tmp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(SketchStore::new());
        idx.register(meta_with_host_key(303));
        let p = idx.start_persistence(durable_cfg(tmp.path().to_path_buf())).unwrap();

        // Append windows for series "a" across [0, 300_000).
        for i in 0..10u64 {
            let s = i * 30_000;
            idx.append_sample(303, lv_host("a"), (s, s + 30_000), sample((i + 1) as u8));
        }
        // Wait for everything to flush+evict from memory.
        assert!(
            wait_until(
                || idx.approx_memory_bytes() == 0 && idx.list_sealed_epochs_len() == 0,
                std::time::Duration::from_secs(5)
            ),
            "data never fully evicted from memory"
        );
        // current_epoch may still hold the most-recent un-sealed window;
        // query a range covering the EVICTED portion [0, 150_000).
        let series = idx.query_range(303, 0, 150_000);
        assert_eq!(series.len(), 1, "expected one series resolved from disk");
        let s = &series[0];
        assert_eq!(s.series_label_values, lv_host("a"), "label map rebuilt from disk");
        assert!(
            !s.samples.is_empty(),
            "query over evicted range returned no samples from disk"
        );
        // Window-end 30_000 (window (0,30_000)) must be present from disk.
        assert!(
            s.samples.contains_key(&30_000),
            "disk window (0,30000) missing from query result: {:?}",
            s.samples.keys().collect::<Vec<_>>()
        );
        drop(p);
    }

    #[test]
    fn query_carry_in_full_base_lives_on_disk() {
        let tmp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(SketchStore::new());
        idx.register(meta_with_host_key(404));
        let p = idx.start_persistence(durable_cfg(tmp.path().to_path_buf())).unwrap();

        // A Full snapshot early (end=100_000), then delta windows later.
        idx.append_sample(404, lv_host("a"), (70_000, 100_000), sample(1)); // Full base
        for i in 0..6u64 {
            let s = 100_000 + i * 30_000;
            idx.append_sample(404, lv_host("a"), (s, s + 30_000), delta_sample((i + 2) as u8));
        }
        // Flush+evict everything to disk.
        assert!(
            wait_until(
                || idx.approx_memory_bytes() == 0 && idx.list_sealed_epochs_len() == 0,
                std::time::Duration::from_secs(5)
            ),
            "data never fully evicted"
        );

        // Query a window that contains ONLY deltas; the Full base lives on
        // disk before the window. The carry-in must splice it in.
        let series = idx.query_range(404, 200_000, 280_000);
        assert_eq!(series.len(), 1);
        let s = &series[0];
        // A Full-encoded carry-in base (end < 200_000) must be present.
        let has_full_base = s.samples.iter().any(|(w_end, smp)| {
            *w_end < 200_000
                && matches!(
                    smp.encoding,
                    SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
                )
        });
        assert!(
            has_full_base,
            "delta-only window did not get a disk-resident Full carry-in base: {:?}",
            s.samples
                .iter()
                .map(|(k, v)| (*k, v.encoding))
                .collect::<Vec<_>>()
        );
        drop(p);
    }

    #[test]
    fn restart_recovery_makes_flushed_data_queryable() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = tmp.path().to_path_buf();
        {
            let idx = Arc::new(SketchStore::new());
            idx.register(meta_with_host_key(505));
            let p = idx.start_persistence(durable_cfg(disk.clone())).unwrap();
            for i in 0..8u64 {
                let s = i * 30_000;
                idx.append_sample(505, lv_host("a"), (s, s + 30_000), sample((i + 1) as u8));
            }
            assert!(
                wait_until(
                    || !p.manifest.live_parts().is_empty()
                        && idx.list_sealed_epochs_len() == 0
                        && idx.approx_memory_bytes() == 0,
                    std::time::Duration::from_secs(5)
                ),
                "data never flushed before restart"
            );
            // Shutdown the flusher cleanly so the manifest is durable.
            let mut p = p;
            p.shutdown();
        }

        // "Restart": brand-new store + resolver on the SAME disk dir. The
        // metadata is re-registered (the SeriesIdResolver WAL recovers
        // sids in prod; here we re-register to model that), then
        // persistence recovers the manifest+parts.
        let idx2 = Arc::new(SketchStore::new());
        idx2.register(meta_with_host_key(505));
        let p2 = idx2.start_persistence(durable_cfg(disk.clone())).unwrap();
        assert!(
            !p2.manifest.live_parts().is_empty(),
            "recovery did not reload any parts"
        );

        let series = idx2.query_range(505, 0, 120_000);
        assert_eq!(series.len(), 1, "recovered data not queryable");
        let s = &series[0];
        assert_eq!(s.series_label_values, lv_host("a"));
        assert!(
            s.samples.contains_key(&30_000),
            "recovered disk window missing after restart"
        );
        drop(p2);
    }

    // ── query-from-recovered-disk (fix/query-from-recovered-disk) ───────
    //
    // The #330 tests `restart_recovery_makes_flushed_data_queryable` and
    // `live_aged_unsealed_panes_flush_and_survive_restart` both call
    // `idx2.register(...)` on the FRESH store BEFORE querying (they even
    // comment "here we re-register to model that"). That masks the real
    // restart bug: in production NOBODY calls `SketchStore::register` on
    // restart — registration only happens on the LIVE INGEST path when a
    // fresh DataPoint arrives. The SeriesIdResolver WAL recovers
    // `(metric, attrs_fp, agg_kind) → sid` but does NOT push identities
    // into the SketchStore's `instances` registry. So after a true restart
    // the `instances` map is EMPTY for the recovered metrics:
    //   * `instances_matching(metric, gbk)` enumerates nothing → the engine
    //     returns "No result" before reading any window, and
    //   * even if a sid were enumerated, `query_range`'s `union_disk_parts_into`
    //     early-returns on the missing `sid_group_by_keys(sid)`.
    // → the cluster-wide "No result" + collapsed-SketchStore symptom.
    //
    // These two tests do a GENUINE fresh reopen (no `register`) for BOTH
    // the sketch (KLL quantile) and exact-agg (Sum) shapes. On origin/main
    // they FAIL ("No result"); with the metadata-sidecar fix they pass
    // because recovery re-registers the disk-resident sids.

    fn meta_kll_host(sid: u64) -> SketchInstanceMetadata {
        let cfg = SketchConfig::Kll { k: 200 };
        SketchInstanceMetadata {
            sid,
            metric_name: "http_latency".into(),
            group_by_keys: ["host".to_string()].into_iter().collect(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::Kll)),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::Kll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    #[test]
    fn recovered_sketch_series_queryable_after_fresh_reopen_without_register() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = tmp.path().to_path_buf();
        // ---- session 1: ingest → seal → flush → EVICT (disk-only) ----
        {
            let idx = Arc::new(SketchStore::new());
            idx.register(meta_kll_host(7100));
            let p = idx.start_persistence(durable_cfg(disk.clone())).unwrap();
            for i in 0..10u64 {
                let s = i * 30_000;
                idx.append_sample(7100, lv_host("a"), (s, s + 30_000), sample((i + 1) as u8));
            }
            assert!(
                wait_until(
                    || !p.manifest.live_parts().is_empty()
                        && idx.approx_memory_bytes() == 0
                        && idx.list_sealed_epochs_len() == 0,
                    std::time::Duration::from_secs(5),
                ),
                "data never flushed+evicted before restart"
            );
            let mut p = p;
            p.shutdown();
        }

        // ---- session 2: TRUE fresh reopen — NO register() ----
        let idx2 = Arc::new(SketchStore::new());
        // Sanity: before recovery the registry is empty (mirrors prod).
        assert_eq!(idx2.instance_count(), 0, "precondition: empty registry");
        let p2 = idx2.start_persistence(durable_cfg(disk.clone())).unwrap();
        assert!(
            !p2.manifest.live_parts().is_empty(),
            "recovery did not reload any parts"
        );

        // (a) instances_matching must find the recovered series WITHOUT a
        //     re-register. On origin/main this is empty → "No result".
        let gbk = ["host".to_string()].into_iter().collect();
        let sids = idx2.instances_matching("http_latency", &gbk);
        assert_eq!(
            sids,
            vec![7100],
            "instances_matching blind to disk-only series after fresh reopen \
             (registry={})",
            idx2.instance_count(),
        );
        // The recovered sid's metadata must be query-routable.
        let meta = idx2.instance(7100).expect("recovered sid metadata present");
        assert_eq!(meta.metric_name, "http_latency");
        assert!(matches!(
            meta.capability,
            Some(Capability::QuantileApprox(SketchKindHandle::Kll))
        ));

        // (b) a range query over the EVICTED window returns the data.
        let series = idx2.query_range(7100, 0, 150_000);
        assert_eq!(series.len(), 1, "recovered KLL series not queryable");
        let s = &series[0];
        assert_eq!(
            s.series_label_values,
            lv_host("a"),
            "label map rebuilt from recovered group_by_keys + disk values"
        );
        assert!(
            s.samples.contains_key(&30_000),
            "recovered disk window (0,30000) missing: {:?}",
            s.samples.keys().collect::<Vec<_>>()
        );
        drop(p2);
    }

    #[test]
    fn recovered_exact_agg_series_queryable_after_fresh_reopen_without_register() {
        use crate::storage_engines::types::AggregationType;
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = tmp.path().to_path_buf();

        let lv_zone = |v: &str| {
            let mut x = BTreeMap::new();
            x.insert("zone".to_string(), v.to_string());
            x
        };

        // ---- session 1: ExactAgg(Sum) by (zone), flush+evict ----
        {
            let idx = Arc::new(SketchStore::new());
            let mut m = meta(8100);
            m.metric_name = "http_requests_total".into();
            m.group_by_keys = ["zone".to_string()].into_iter().collect();
            m.capability = Some(Capability::ExactAgg(AggregationType::Sum));
            m.agg_kind = AggKind::ExactAgg {
                agg_type: AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            };
            m.accuracy = None;
            idx.register(m);
            let p = idx.start_persistence(durable_cfg(disk.clone())).unwrap();
            for i in 0..10u64 {
                let s = i * 30_000;
                idx.append_precompute(
                    8100,
                    lv_zone("z0"),
                    (s, s + 30_000),
                    Box::new(
                        crate::precompute_engine::operators::SumAccumulator::with_sum(
                            (i + 1) as f64,
                        ),
                    ),
                );
            }
            assert!(
                wait_until(
                    || !p.manifest.live_parts().is_empty()
                        && idx.approx_memory_bytes() == 0
                        && idx.list_sealed_epochs_len() == 0,
                    std::time::Duration::from_secs(5),
                ),
                "exact-agg windows never flushed+evicted"
            );
            let mut p = p;
            p.shutdown();
        }

        // ---- session 2: TRUE fresh reopen — NO register() ----
        let idx2 = Arc::new(SketchStore::new());
        assert_eq!(idx2.instance_count(), 0, "precondition: empty registry");
        let p2 = idx2.start_persistence(durable_cfg(disk.clone())).unwrap();

        // instances_matching must surface the exact-agg sid.
        let gbk = ["zone".to_string()].into_iter().collect();
        assert_eq!(
            idx2.instances_matching("http_requests_total", &gbk),
            vec![8100],
            "exact-agg sid blind to instances_matching after fresh reopen"
        );
        let meta = idx2.instance(8100).expect("recovered exact-agg metadata");
        assert!(matches!(
            meta.agg_kind,
            AggKind::ExactAgg { agg_type: AggregationType::Sum, .. }
        ));

        // The Sum exact-agg range query must resolve from disk.
        let series = idx2.query_exact_agg_range(8100, 0, 150_000);
        assert!(
            !series.is_empty(),
            "recovered exact-agg query returned No result after fresh reopen"
        );
        let (label, samples) = &series[0];
        assert_eq!(label.get("zone").map(String::as_str), Some("z0"));
        assert!(
            samples.contains_key(&30_000),
            "recovered exact-agg window (0,30000) missing from disk read-back"
        );
        // Rate divisor helper must also see the recovered disk windows.
        assert!(
            idx2.exact_agg_coverage_bounds(8100, 0, 150_000).is_some(),
            "exact_agg_coverage_bounds blind to recovered disk after fresh reopen"
        );
        drop(p2);
    }

    #[test]
    fn persistence_disabled_keeps_327_retention_behavior() {
        // Non-regression: with persistence OFF, query_range reads
        // in-memory only and #327 retention still bounds memory. The
        // #323–#326 overlap + carry-in shapes still pass (covered by the
        // dedicated tests above); here we confirm the disk union is a
        // no-op when no read handle is installed.
        let idx = SketchStore::new();
        idx.register(meta_with_host_key(606));
        idx.append_sample(606, lv_host("a"), (0, 10), sample(1));
        idx.append_sample(606, lv_host("a"), (10, 20), sample(2));
        let series = idx.query_range(606, 0, 20);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples.len(), 2);
        // No persistence handle → seal cadence disabled → no sealing.
        assert!(idx.persistence_read.read().unwrap().is_none());
    }

    // ── LIVE-scenario regression tests (fix/sketch-durable-live) ────────
    //
    // The unit tests above use `hot_window_ms: Some(0)` + epoch-1970
    // timestamps, which force-flush everything immediately. The LIVE run
    // (`--persistence-seal-window-count=4 --persistence-hot-window-secs=120`)
    // ingests panes stamped at WALL-CLOCK ms and uses a 120s hot window,
    // and exposed three bugs these helpers must reproduce.

    fn now_ms_wall() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Mirror of the live config: seal every 4 windows, 120s hot window,
    /// memory limit high (so the flush is driven by the hot-window
    /// watermark, exactly as in the live run that produced empty parts/).
    fn live_cfg(disk_path: std::path::PathBuf) -> SketchStorePersistenceConfig {
        SketchStorePersistenceConfig {
            memory_limit_bytes: 2048 * 1024 * 1024,
            memory_low_watermark_bytes: 2048 * 1024 * 1024 * 8 / 10,
            hard_cap_bytes: 2048 * 1024 * 1024 * 125 / 100,
            hot_window_ms: Some(120_000), // live: --persistence-hot-window-secs=120
            delete_older_than_ms: None,
            flush_interval: std::time::Duration::from_millis(20),
            disk_path,
            part_cache_bytes: 1 << 20,
            seal_window_count: 4, // live: --persistence-seal-window-count=4
        }
    }

    /// BUG #1 + #3 (most severe): in the LIVE run the freshest windows of
    /// every series sit UN-SEALED in `current_epoch` — sealing only fires
    /// once `current_epoch` reaches the cadence (4 distinct windows). The
    /// flusher's hot-window phase ONLY ever considers SEALED epochs, so
    /// any window that ages past the 120s hot window while still in
    /// `current_epoch` (because the series stopped/slowed before hitting
    /// cadence) is NEVER flushed. That is exactly what produced the empty
    /// `parts/` + 0-byte manifest log on node2 after 13 min of ingest:
    /// data flowed (the resolver WAL grew) but nothing was ever made
    /// durable, so a `docker restart` recovered `live=0` and the post-
    /// restart query returned "No result".
    ///
    /// This test ingests aged panes that DON'T reach cadence-4, so they
    /// stay un-sealed, then asserts the flusher still makes them durable
    /// and they survive a restart. On origin/main nothing flushes.
    #[test]
    fn live_aged_unsealed_panes_flush_and_survive_restart() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = tmp.path().to_path_buf();
        // Panes ending 10 minutes ago → comfortably behind the 120s hot
        // window the moment they're ingested.
        let base = now_ms_wall().saturating_sub(10 * 60 * 1000);
        {
            let idx = Arc::new(SketchStore::new());
            idx.register(meta_with_host_key(7001));
            let p = idx.start_persistence(live_cfg(disk.clone())).unwrap();
            // Only 3 distinct 30s panes — BELOW the 4-window seal cadence,
            // so they never rotate into sealed_epochs and (on origin/main)
            // the flusher's sealed-only hot-window scan never sees them.
            for i in 0..3u64 {
                let s = base + i * 30_000;
                idx.append_sample(7001, lv_host("a"), (s, s + 30_000), sample((i + 1) as u8));
            }
            // These aged windows MUST become durable parts even though the
            // cadence was never reached. On origin/main this never happens
            // → empty parts/, matching the live failure.
            let flushed = wait_until(
                || !p.manifest.live_parts().is_empty(),
                std::time::Duration::from_secs(5),
            );
            assert!(
                flushed,
                "LIVE BUG #1: aged un-sealed panes never flushed to disk \
                 (parts={}, sealed={}, sealed_bytes={})",
                p.manifest.live_parts().len(),
                idx.list_sealed_epochs_len(),
                idx.approx_memory_bytes(),
            );
            let mut p = p;
            p.shutdown();
        }

        // "Restart" on the SAME dir — recovery must reload the parts.
        let idx2 = Arc::new(SketchStore::new());
        idx2.register(meta_with_host_key(7001));
        let p2 = idx2.start_persistence(live_cfg(disk.clone())).unwrap();
        assert!(
            !p2.manifest.live_parts().is_empty(),
            "LIVE BUG #1: recovery found 0 live parts after restart"
        );
        let series = idx2.query_range(7001, base, base + 90_000);
        assert_eq!(series.len(), 1, "recovered data not queryable after restart");
        assert!(
            !series[0].samples.is_empty(),
            "restart query returned No result — flushed data lost"
        );
        drop(p2);
    }

    /// BUG #2: after flush+evict, an exact-agg (`sum by (zone)` shape)
    /// range query must still resolve from disk. On origin/main
    /// `query_exact_agg_range` reads ONLY the in-memory current+sealed
    /// epochs — it never unions disk parts — so once the windows are
    /// flushed-then-evicted the query returns empty ("No result").
    #[test]
    fn live_exact_agg_resolves_from_disk_after_evict() {
        use crate::storage_engines::types::AggregationType;
        let tmp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(SketchStore::new());
        // Register an ExactAgg(Sum) sid keyed by `zone`.
        let mut m = meta(8001);
        m.metric_name = "http_requests_total".into();
        m.group_by_keys = ["zone".to_string()].into_iter().collect();
        m.agg_kind = AggKind::ExactAgg {
            agg_type: AggregationType::Sum,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
        };
        idx.register(m);
        let p = idx.start_persistence(durable_cfg(tmp.path().to_path_buf())).unwrap();

        let lv_zone = |v: &str| {
            let mut x = BTreeMap::new();
            x.insert("zone".to_string(), v.to_string());
            x
        };
        for i in 0..10u64 {
            let s = i * 30_000;
            idx.append_precompute(
                8001,
                lv_zone("z0"),
                (s, s + 30_000),
                Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(
                    (i + 1) as f64,
                )),
            );
        }
        assert!(
            wait_until(
                || idx.approx_memory_bytes() == 0 && idx.list_sealed_epochs_len() == 0,
                std::time::Duration::from_secs(5),
            ),
            "exact-agg windows never fully evicted"
        );
        // Query the EVICTED portion [0, 150_000) — must come back from disk.
        let series = idx.query_exact_agg_range(8001, 0, 150_000);
        assert!(
            !series.is_empty(),
            "LIVE BUG #2: exact-agg query returned No result after flush+evict \
             (disk read-back missing)"
        );
        let (_label, samples) = &series[0];
        assert!(
            samples.contains_key(&30_000),
            "LIVE BUG #2: evicted exact-agg window (0,30000) missing from disk read-back"
        );
        // The coverage-bounds helper (rate divisor) must also see disk.
        let cov = idx.exact_agg_coverage_bounds(8001, 0, 150_000);
        assert!(
            cov.is_some(),
            "LIVE BUG #2: exact_agg_coverage_bounds blind to disk after evict"
        );
        drop(p);
    }

    /// BUG #3: the memory diagnostic + the flusher's memory-pressure
    /// trigger must account for `current_epoch`, not just sealed epochs.
    /// On origin/main `approx_memory_bytes()` sums ONLY sealed epochs, so
    /// a store holding megabytes of un-sealed `current_epoch` data reports
    /// ~0 bytes (the live "0.00–0.12 KB approx sealed bytes" under-report)
    /// and the flusher's `mem > memory_limit` trigger never fires.
    #[test]
    fn live_total_memory_accounts_for_current_epoch() {
        let idx = SketchStore::new();
        idx.register(meta_with_host_key(9001));
        // No persistence → no sealing → all data sits in current_epoch.
        for i in 0..20u64 {
            let s = i * 30_000;
            idx.append_sample(9001, lv_host("a"), (s, s + 30_000), sample((i + 1) as u8));
        }
        assert_eq!(
            idx.list_sealed_epochs_len(),
            0,
            "precondition: nothing sealed (no persistence)"
        );
        assert!(
            idx.approx_memory_bytes() > 0,
            "LIVE BUG #3: approx_memory_bytes() reports 0 while current_epoch holds 20 \
             windows — the diagnostic under-reports and the flusher's memory trigger is blind"
        );
    }
}

// 2026-05 reorg: generic epoch-partitioned columnar storage lives
// alongside the store that uses it.
pub mod epoch_columnar;

// `persistence` moved up to `sketch_db::persistence`. Re-exported here
// so legacy `crate::storage_engines::sketch_db::index::persistence::*`
// paths continue working without consumer changes.
pub use crate::storage_engines::sketch_db::persistence;
