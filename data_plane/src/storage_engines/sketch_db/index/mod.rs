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
//! `docs/design_docs/series-identity.md`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asap_types::sds::{
    CatalogGeneration, HalfOpenTimeRange, InstanceCompleteness, InstanceLifecycle,
    ObservedSummaryInventory, SummaryDefinitionId, SummaryInstance, SummaryInstanceId,
    SummaryInstanceStatus, SummaryPlacement, SummaryStateReference,
};
use asap_types::PolicyFingerprint;
use dashmap::DashMap;

use self::epoch_columnar::{LabelValuesId, SidStoreData, TimestampRange};
use crate::storage_engines::sketch_db::lifecycle::AggStatus;
use crate::storage_engines::sketch_db::sds::{
    DataDescriptor, SdsBinding, SummaryDescriptor, SummaryDescriptorRegistry,
};

// Phase-5 reorg: payload taxonomy + sid hashing + accuracy moved to
// `sketch_db::data`. Re-exported here so existing call sites
// (`crate::storage_engines::sketch_db::index::*`) keep compiling
// during the reorg.
pub use crate::storage_engines::sketch_db::data::{
    canonical_parameters, AccuracyBound, AggKind, AggPayload, AggregationType, Capability,
    SketchAlgorithm, SketchConfig, SketchEncoding, SketchSampleState, SketchTimeSeries,
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
/// and [`SketchStore::ingest_precompute_with_series_id`] — folds the
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
    if let Some(labels) = &output.population_labels {
        let attrs_fp = labels
            .iter()
            .map(|(name, value)| format!("{name}={value};"))
            .collect();
        return (attrs_fp, labels.clone());
    }
    let label_values_vec = output
        .key
        .as_ref()
        .map(|k| k.labels.clone())
        .unwrap_or_default();
    let key_names = &agg_cfg.grouping_labels.names();
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
    /// sid. Together with [`SketchStore::policy_to_series_ids`] this gives
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
    pub fn sketch_algorithm(&self) -> Option<SketchAlgorithm> {
        match &self.agg_kind {
            AggKind::Sketch {
                algorithm: kind, ..
            } => Some(kind.clone()),
            AggKind::ExactAgg { .. } => None,
        }
    }

    /// Sketch-config accessor mirroring [`Self::sketch_algorithm`].
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

#[derive(Debug, Clone, Copy)]
struct ReductionRollupNode {
    end_ms: u64,
    value: f64,
}

#[derive(Debug)]
struct ReductionRollupSeries {
    reduction: RollupReduction,
    anchor_start_ms: Option<u64>,
    base_width_ms: Option<u64>,
    next_start_ms: Option<u64>,
    valid: bool,
    levels: Vec<BTreeMap<u64, ReductionRollupNode>>,
}

impl ReductionRollupSeries {
    fn new(reduction: RollupReduction) -> Self {
        Self {
            reduction,
            anchor_start_ms: None,
            base_width_ms: None,
            next_start_ms: None,
            valid: false,
            levels: Vec::new(),
        }
    }
}

impl ReductionRollupSeries {
    fn append(&mut self, window: TimestampRange, value: f64, retention_horizon_ms: Option<u64>) {
        let width = window.1.saturating_sub(window.0);
        if width == 0 || self.next_start_ms.is_some_and(|next| next != window.0) {
            self.valid = false;
            return;
        }
        let anchor = *self.anchor_start_ms.get_or_insert(window.0);
        let base_width = *self.base_width_ms.get_or_insert(width);
        if width != base_width || window.0 < anchor || (window.0 - anchor) % base_width != 0 {
            self.valid = false;
            return;
        }
        self.valid = true;
        self.next_start_ms = Some(window.1);
        if self.levels.is_empty() {
            self.levels.push(BTreeMap::new());
        }
        self.levels[0].insert(
            window.0,
            ReductionRollupNode {
                end_ms: window.1,
                value,
            },
        );

        let mut level = 0usize;
        let mut node_start = window.0;
        let mut node = ReductionRollupNode {
            end_ms: window.1,
            value,
        };
        loop {
            let level_width = match base_width.checked_shl(level as u32) {
                Some(width) => width,
                None => break,
            };
            let ordinal = (node_start - anchor) / level_width;
            if ordinal % 2 == 0 || node_start < level_width {
                break;
            }
            let left_start = node_start - level_width;
            let Some(left) = self.levels[level].get(&left_start).copied() else {
                break;
            };
            if left.end_ms != node_start {
                break;
            }
            node_start = left_start;
            node = ReductionRollupNode {
                end_ms: node.end_ms,
                value: self.reduction.combine(left.value, node.value),
            };
            level += 1;
            if self.levels.len() <= level {
                self.levels.push(BTreeMap::new());
            }
            self.levels[level].insert(node_start, node);
        }

        if let Some(horizon) = retention_horizon_ms {
            let cutoff = window.1.saturating_sub(horizon);
            for nodes in &mut self.levels {
                while nodes
                    .first_key_value()
                    .is_some_and(|(_, node)| node.end_ms < cutoff)
                {
                    nodes.pop_first();
                }
            }
        }
    }

    fn query(&self, start_ms: u64, end_ms: u64) -> Option<f64> {
        if !self.valid {
            return None;
        }
        let base = self.levels.first()?;
        let mut cursor = start_ms;
        let mut result: Option<f64> = None;
        loop {
            let Some((&base_start, base_node)) =
                base.range(cursor..).find(|(_, node)| node.end_ms <= end_ms)
            else {
                return result;
            };
            let mut chosen = *base_node;
            for nodes in self.levels.iter().skip(1) {
                match nodes.get(&base_start) {
                    Some(node) if node.end_ms <= end_ms => chosen = *node,
                    _ => break,
                }
            }
            result = Some(result.map_or(chosen.value, |current| {
                self.reduction.combine(current, chosen.value)
            }));
            cursor = chosen.end_ms;
            if cursor >= end_ms || base.range(cursor..).next().is_none() {
                return result;
            }
        }
    }

    fn approx_bytes(&self) -> usize {
        self.levels
            .iter()
            .map(|nodes| nodes.len() * std::mem::size_of::<(u64, ReductionRollupNode)>())
            .sum()
    }
}

/// Readout categories supported by the derived rollup tier. New categories
/// belong here rather than as additional top-level `SketchStore` fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RollupReduction {
    Max,
}

impl RollupReduction {
    fn combine(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Max => left.max(right),
        }
    }
}

type ReductionRollupMap = DashMap<
    (RollupReduction, u64),
    Arc<RwLock<HashMap<BTreeMap<String, String>, ReductionRollupSeries>>>,
>;

/// Rebuildable indexes over canonical SummaryStore panes. This owns derived
/// query accelerators only; base summary instances remain the source of truth.
#[derive(Default)]
struct Rollups {
    reductions: ReductionRollupMap,
}

impl Rollups {
    fn append(
        &self,
        reduction: RollupReduction,
        sid: u64,
        labels: BTreeMap<String, String>,
        window: TimestampRange,
        value: f64,
        retention_horizon_ms: Option<u64>,
    ) {
        self.reductions
            .entry((reduction, sid))
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .write()
            .unwrap()
            .entry(labels)
            .or_insert_with(|| ReductionRollupSeries::new(reduction))
            .append(window, value, retention_horizon_ms);
    }

    fn query(
        &self,
        reduction: RollupReduction,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Option<Vec<(BTreeMap<String, String>, f64)>> {
        let rollups = self.reductions.get(&(reduction, sid))?.clone();
        let guard = rollups.read().unwrap();
        let values = guard
            .iter()
            .map(|(labels, series)| {
                series
                    .query(start_unix_ms, end_unix_ms)
                    .map(|value| (labels.clone(), value))
            })
            .collect::<Option<Vec<_>>>()?;
        (!values.is_empty()).then_some(values)
    }

    fn remove_sid(&self, sid: u64) {
        self.reductions
            .retain(|(_, series_id), _| *series_id != sid);
    }

    fn clear(&self) {
        self.reductions.clear();
    }

    fn approx_bytes(&self) -> usize {
        self.reductions
            .iter()
            .filter_map(|entry| {
                entry.value().read().ok().map(|series| {
                    series
                        .iter()
                        .map(|(labels, rollup)| {
                            labels
                                .iter()
                                .map(|(key, value)| key.len() + value.len())
                                .sum::<usize>()
                                + rollup.approx_bytes()
                        })
                        .sum::<usize>()
                })
            })
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IncompleteSummaryLineage {
    plan_id: u64,
    plan_version: u64,
    producer_id: String,
    producer_epoch: String,
    window_start_unix_ms: u64,
    window_end_unix_ms: u64,
}

impl From<&asap_types::producer_plan::SummaryFrameIdentity> for IncompleteSummaryLineage {
    fn from(frame: &asap_types::producer_plan::SummaryFrameIdentity) -> Self {
        Self {
            plan_id: frame.plan_id,
            plan_version: frame.plan_version,
            producer_id: frame.producer_id.clone(),
            producer_epoch: frame.producer_epoch.clone(),
            window_start_unix_ms: frame.window_start_unix_nano / 1_000_000,
            window_end_unix_ms: frame.window_end_unix_nano / 1_000_000,
        }
    }
}

/// Two-level sketch index. Replaces the legacy `aggregation_id`-keyed
/// SimpleStore lookup once Phase 5 wiring lands at the streaming engine
/// ingest path and the query path.
///
/// `instances` is keyed under a `RwLock<HashMap>` because the registration
/// rate is low (one write per first-seen sid) and reads dominate;
/// `series` is a `DashMap` because per-sid writes happen on every DP.
#[derive(Clone, Copy)]
pub(crate) struct SummaryReadRevision {
    admission: u64,
    mutation: u64,
    in_flight: usize,
}
impl SummaryReadRevision {
    fn capture(
        admission: u64,
        mutation: &std::sync::atomic::AtomicU64,
        active: &std::sync::atomic::AtomicUsize,
        between_reads: impl FnOnce(),
    ) -> Self {
        use std::sync::atomic::Ordering::SeqCst;
        let before = mutation.load(SeqCst);
        between_reads();
        let in_flight = active.load(SeqCst);
        let after = mutation.load(SeqCst);
        Self {
            admission,
            mutation: after,
            in_flight: if before == after {
                in_flight
            } else {
                in_flight.max(1)
            },
        }
    }

    pub(crate) fn matches(self, other: Self) -> bool {
        self.in_flight == 0
            && other.in_flight == 0
            && self.admission == other.admission
            && self.mutation == other.mutation
    }
}

struct StateMutation<'a>(&'a SketchStore);
impl Drop for StateMutation<'_> {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::SeqCst;
        self.0.mutation_revision.fetch_add(1, SeqCst);
        self.0.active_mutations.fetch_sub(1, SeqCst);
    }
}

#[derive(Default)]
pub struct SketchStore {
    /// Held through each state append; completion takes the exclusive guard.
    completed_windows: RwLock<HashMap<u64, u64>>,
    completion_flush_before: std::sync::atomic::AtomicU64,
    admission: RwLock<admission::AdmissionInventory>,
    mutation_revision: std::sync::atomic::AtomicU64,
    active_mutations: std::sync::atomic::AtomicUsize,
    admitted_mutations: std::sync::atomic::AtomicU64,
    finite_mutation_revision: std::sync::atomic::AtomicU64,
    /// sid → metadata. May contain ghost sids (registered identities
    /// whose state was merged away by an upstream gateway before
    /// reaching this backend).
    instances: RwLock<HashMap<u64, SdsBinding>>,
    /// Interns immutable SDS descriptors across all Series IDs and panes.
    descriptors: SummaryDescriptorRegistry,
    /// sid → item_label (the data-point attribute NAME, e.g. "service"
    /// or "endpoint") for CountMin/CountSketch sids registered in
    /// per-item mode. Its presence is what makes a CMS sid answerable by
    /// the per-item `estimate(key)` path (the query engine extracts the
    /// matching selector value and gates the safe-miss on it). Absent =>
    /// per-attribute-set CMS (only the bucket total is meaningful).
    /// Kept as a decoupled side-table so recording item_label does not
    /// change sid identity (`AggKind` canonical string) or churn the many
    /// `SketchInstanceMetadata` / `AggKind::Sketch` literals.
    item_labels: RwLock<HashMap<u64, String>>,
    /// sid → per-sid columnar storage. Empty `SidStoreData` (or absent
    /// key) for ghost sids — query path detects this and falls through
    /// to Thanos archive.
    series: DashMap<u64, SidStore>,
    /// Derived rollup categories over canonical SummaryStore panes.
    rollups: Rollups,
    /// Receiver-observed delta gaps. Query reads overlapping an incomplete
    /// lineage fail closed to the exact tier until a recovery full checkpoint
    /// for that exact producer/window lineage is accepted.
    incomplete_summary_lineages: DashMap<u64, HashSet<IncompleteSummaryLineage>>,
    /// Reverse index: `policy_fp → {sids}`. Lets the query path resolve
    /// "which sids belong to this policy?" in O(1) without walking
    /// `instances`. Maintained by [`Self::register`] /
    /// [`Self::remove_instance`] / [`Self::remove_instances_for_agg_config`].
    /// Entries with `PolicyFingerprint::UNSET` are NOT recorded (the
    /// sentinel means "no source config"); legacy callers that mint
    /// sids without a fingerprint reach those sids through
    /// `instances_matching(metric, gbk)`.
    ///
    /// ## Cross-index atomicity invariant (P2-1)
    ///
    /// `instances`, `policy_to_series_ids`, and `metric_to_series_ids` form ONE
    /// logical index whose three maps must agree: every sid present in
    /// `instances` must also be present in `metric_to_series_ids` (keyed by
    /// its metric_name) and — when its `policy_fp` is non-UNSET — in
    /// `policy_to_series_ids`. A concurrent reader must never observe a sid in
    /// `instances` that is missing from the secondary indexes (or vice
    /// versa). To preserve this, every writer ([`Self::register`],
    /// [`Self::remove_instance`]) acquires ALL THREE write guards
    /// together in the fixed order `instances → policy_to_series_ids →
    /// metric_to_series_ids` BEFORE mutating any of them, so the update is
    /// atomic with respect to any reader that takes the `instances` lock.
    /// The fixed acquisition order is also the deadlock-avoidance order:
    /// no code path takes these locks in a different order.
    policy_to_series_ids: RwLock<HashMap<PolicyFingerprint, BTreeSet<u64>>>,
    /// Secondary index: `metric_name → {sids}` (P2-2). Lets
    /// [`Self::instances_matching`] do a keyed lookup of the sids for a
    /// metric instead of an O(N) full scan of `instances`. Maintained in
    /// lock-step with `instances` under the same write-lock domain (see
    /// the atomicity invariant on `policy_to_series_ids`). Holds every
    /// registered sid (UNSET-policy sids included), since the query path
    /// keys candidate selection on metric name, not policy.
    metric_to_series_ids: RwLock<HashMap<String, BTreeSet<u64>>>,
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
    persistence_metadata: RwLock<Option<Arc<persistence::metadata::SidMetadataStore>>>,
    immutable_publisher: RwLock<std::sync::Weak<persistence::flusher::FlusherShared>>,
    removed_sids: RwLock<
        BTreeMap<
            u64,
            (
                Option<Arc<asap_types::sds::CatalogGeneration>>,
                Option<SummaryDefinitionId>,
            ),
        >,
    >,
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
pub enum SeriesLookup {
    Hit,
    Ghost,
    Unknown,
}

/// Borrowed write access issued only while the store holds its admission guard.
/// Receipt fields are data, not evidence that this synchronization is held.
pub(crate) struct SummaryPublicationWriter<'a>(&'a SketchStore);

impl SummaryPublicationWriter<'_> {
    pub(crate) fn ingest_precompute_with_series_id(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        state: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        self.0
            .ingest_precompute_with_admission(sid, config, output, state)
    }

    pub(crate) fn ingest_precompute_for_agg_config<R: Into<Option<u64>>>(
        &self,
        mint: impl FnOnce(&str, &str, &str) -> R,
        config: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        state: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        self.0
            .ingest_precompute_config_with_admission(mint, config, output, state)
    }

    #[cfg(test)]
    fn append_sample(
        &self,
        sid: u64,
        labels: BTreeMap<String, String>,
        window: TimestampRange,
        sample: SketchSampleState,
    ) -> bool {
        let _instances = self.0.instances.read().unwrap();
        self.0
            .append_sample_with_binding(sid, labels, window, sample)
    }
}

impl SketchStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Classify a sid for query routing. See `SeriesLookup` for semantics.
    pub fn classify(&self, sid: u64) -> SeriesLookup {
        let known = self.instances.read().unwrap().contains_key(&sid);
        if !known {
            return SeriesLookup::Unknown;
        }
        match self.series.get(&sid) {
            Some(store) => {
                let g = store.read().unwrap();
                if !g.current_epoch.is_empty() || !g.sealed_epochs.is_empty() {
                    SeriesLookup::Hit
                } else {
                    SeriesLookup::Ghost
                }
            }
            None => SeriesLookup::Ghost,
        }
    }

    /// Insert metadata for a freshly-resolved sid. Also records the sid
    /// in the `policy_fp → {sids}` reverse index (when the metadata
    /// carries a non-UNSET fingerprint) and in the `metric_name →
    /// {sids}` secondary index.
    ///
    /// ## Atomicity (P2-1)
    ///
    /// All three index write guards are acquired together, in the fixed
    /// order `instances → policy_to_series_ids → metric_to_series_ids`, BEFORE any
    /// map is mutated. This makes the three updates atomic with respect
    /// to a concurrent reader that takes the `instances` lock: such a
    /// reader can never see the sid in `instances` while it is still
    /// absent from `policy_to_series_ids` / `metric_to_series_ids` (the pre-fix race
    /// where the two indexes were written under separate sequential
    /// locks). See the index-field doc comments for the full invariant.
    pub fn register(&self, meta: SketchInstanceMetadata) {
        let sid = meta.sid;
        let policy_fp = meta.policy_fp;
        // A non-legacy materialization must resolve through the installed
        // authoritative catalog. Unknown identities fail closed and never
        // become queryable SummaryStore entries.
        let instance = match self.descriptors.bind(meta) {
            Ok(instance) => instance,
            Err(error) => {
                tracing::warn!(sid, %error, "rejecting SummaryStore registration outside the active SummaryCatalog");
                return;
            }
        };
        let metric_name = instance
            .data_descriptor
            .time_series_metric()
            .map(str::to_owned);
        // Fixed lock order: instances → policy_to_series_ids → metric_to_series_ids.
        let mut instances = self.instances.write().unwrap();
        if self.removed_sids.read().unwrap().contains_key(&sid) {
            tracing::warn!(sid, "rejecting reuse of a removed summary instance ID");
            return;
        }
        let mut policy_idx = self.policy_to_series_ids.write().unwrap();
        let mut metric_idx = self.metric_to_series_ids.write().unwrap();
        instances.insert(sid, instance);
        if !policy_fp.is_unset() {
            policy_idx.entry(policy_fp).or_default().insert(sid);
        }
        if let Some(metric_name) = metric_name {
            metric_idx.entry(metric_name).or_default().insert(sid);
        }
    }

    /// Install one authoritative catalog snapshot for future registrations.
    /// Existing Series IDs retain their generation's descriptor Arcs while draining.
    pub fn install_summary_catalog(
        &self,
        catalog: Arc<asap_types::summary_catalog::SummaryCatalog>,
    ) -> Result<(), String> {
        let reference = catalog.reference().map_err(|error| error.to_string())?;
        let mut inventory = self.admission.write().unwrap();
        let generation = CatalogGeneration {
            schema_version: reference.schema_version,
            plan_id: reference.plan_id,
            plan_version: reference.plan_version,
            snapshot_sha256: reference.snapshot_sha256,
        };
        let closed = self
            .persistence_metadata
            .read()
            .unwrap()
            .as_ref()
            .map(|writer| writer.load_finite_closure())
            .transpose()
            .map_err(|error| error.to_string())?
            .flatten();
        self.descriptors
            .install_catalog(Arc::clone(&catalog))
            .map_err(|error| error.to_string())?;
        inventory.install(generation.clone());
        if closed.as_ref() == Some(&generation) {
            inventory.seal_finite(&generation)?;
        }
        Ok(())
    }

    pub(crate) fn admit_summary_updates(
        &self,
        generation: &CatalogGeneration,
        coordinates: BTreeSet<asap_types::sds::SummaryInstanceCoordinates>,
    ) -> Result<u64, String> {
        let catalog = self
            .descriptors
            .authoritative_catalog()
            .ok_or("summary admission requires an installed catalog")?;
        if coordinates.iter().any(|coordinate| {
            !catalog
                .materializations
                .contains_key(&coordinate.summary_definition_id)
        }) {
            return Err("summary admission references an uninstalled definition".into());
        }
        self.admission
            .write()
            .unwrap()
            .admit(generation, coordinates)
    }

    pub(crate) fn publish_unadmitted_summary_update(
        &self,
        persist: impl FnOnce(&SummaryPublicationWriter<'_>) -> Option<u64>,
    ) -> Option<u64> {
        let admission = self.admission.read().ok()?;
        if admission.is_finite_closed() {
            return None;
        }
        persist(&SummaryPublicationWriter(self))
    }

    pub(crate) fn publish_admitted_summary_update(
        &self,
        generation: &CatalogGeneration,
        coordinate: &asap_types::sds::SummaryInstanceCoordinates,
        first_revision: u64,
        revision: u64,
        replay_horizon_ms: u64,
        persist: impl FnOnce(&SummaryPublicationWriter<'_>) -> Option<u64>,
    ) -> Result<(), String> {
        // Fence installation and read validation across the state write: an old
        // producer cannot mutate a new generation before its receipt is rejected.
        let mut inventory = self.admission.write().unwrap();
        if inventory.validate_publication(generation, coordinate, first_revision, revision)? {
            return Ok(());
        }
        let series_id =
            persist(&SummaryPublicationWriter(self)).ok_or("summary state publication failed")?;
        inventory.record_series(generation, coordinate, series_id)?;
        inventory.acknowledge(generation, coordinate, revision)?;
        self.admitted_mutations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let floor = coordinate
            .time_range
            .end_ms
            .saturating_sub(i64::try_from(replay_horizon_ms).unwrap_or(i64::MAX));
        inventory.retire_completed_before(coordinate.summary_definition_id, floor);
        Ok(())
    }

    pub(crate) fn seal_finite_summary_input(
        &self,
        generation: &CatalogGeneration,
    ) -> Result<bool, String> {
        use std::sync::atomic::Ordering::SeqCst;
        // Same order as admitted publication: admission -> metadata -> append fence.
        // Closing a receiver alone is insufficient: the fence also rejects writes
        // from every other producer once these physical windows are complete.
        let mut inventory = self.admission.write().unwrap();
        inventory.validate_finite(generation)?;
        if inventory.is_finite_complete() {
            return Ok(true);
        }
        let frontiers = inventory.published_frontiers().clone();
        let instances = self.instances.read().unwrap();
        let mut records = Vec::new();
        for (sid, end) in &frontiers {
            let instance = instances
                .get(sid)
                .ok_or("completed series has no identity")?;
            let mut record = self
                .metadata_record(instance)
                .ok_or("completed series has no catalog provenance")?;
            record.completed_through_ms = record.completed_through_ms.max(Some(*end));
            records.push(record);
        }
        let mut completed = self.completed_windows.write().unwrap();
        let mutation = self.mutation_revision.load(SeqCst);
        if self.active_mutations.load(SeqCst) != 0
            || mutation != self.admitted_mutations.load(SeqCst)
        {
            return Err("finite summary completion cannot certify untracked state writes".into());
        }
        inventory.validate_finite(generation)?;
        if self.persistence_read.read().unwrap().is_some() {
            if let Some(end) = frontiers.values().max() {
                self.completion_flush_before
                    .fetch_max(end.saturating_add(1), SeqCst);
            }
            // The flusher evicts an epoch only after its payload and manifest
            // are durable. Until then a restart must remain able to replay it.
            let pending = frontiers.iter().any(|(sid, end)| {
                self.series.get(sid).is_some_and(|data| {
                    data.read()
                        .unwrap()
                        .contains_window_ending_at_or_before(*end)
                })
            });
            if pending {
                return Ok(false);
            }
        }
        // A failed durable close remains closed to raw writers but is not a
        // usable completion proof. Retrying the barrier finishes persistence.
        inventory.begin_finite_close();
        if let Some(writer) = self.persistence_metadata.read().unwrap().as_ref() {
            writer
                .upsert_all(&records)
                .map_err(|error| error.to_string())?;
            writer
                .persist_finite_closure(generation)
                .map_err(|error| error.to_string())?;
        }
        inventory.seal_finite(generation)?;
        for (sid, end) in frontiers {
            completed
                .entry(sid)
                .and_modify(|value| *value = (*value).max(end))
                .or_insert(end);
        }
        self.finite_mutation_revision.store(mutation, SeqCst);
        Ok(true)
    }

    pub(crate) fn summary_window_known_empty(
        &self,
        definition: SummaryDefinitionId,
        series_id: u64,
        range: HalfOpenTimeRange,
    ) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        self.active_mutations.load(SeqCst) == 0
            && self.finite_mutation_revision.load(SeqCst) == self.mutation_revision.load(SeqCst)
            && self
                .admission
                .read()
                .unwrap()
                .known_empty(definition, series_id, range)
    }

    pub(crate) fn summary_update_revision(&self) -> SummaryReadRevision {
        SummaryReadRevision::capture(
            self.admission.read().unwrap().revision(),
            &self.mutation_revision,
            &self.active_mutations,
            || {},
        )
    }

    fn begin_state_mutation(&self) -> StateMutation<'_> {
        self.active_mutations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        StateMutation(self)
    }

    pub(crate) fn has_pending_summary_updates(
        &self,
        definition: SummaryDefinitionId,
        range: HalfOpenTimeRange,
    ) -> bool {
        self.admission
            .read()
            .unwrap()
            .has_pending(definition, range)
    }

    /// Share the installed metadata snapshot without copying descriptors or state.
    pub(crate) fn summary_catalog_snapshot(
        &self,
    ) -> Option<Arc<asap_types::summary_catalog::SummaryCatalog>> {
        self.descriptors.authoritative_catalog()
    }

    /// Record that `sid` is a per-item (item_label-mode) frequency sketch
    /// keyed by the data-point attribute `label` (e.g. "service"). The
    /// query engine consults this to decide whether a keyed selector like
    /// `cms_metric{service="X"}` can be answered by the per-item
    /// `estimate(key)` path. A no-op `label` (empty) clears it.
    pub fn set_item_label(&self, sid: u64, label: &str) {
        let mut m = self.item_labels.write().unwrap();
        if label.is_empty() {
            m.remove(&sid);
        } else {
            m.insert(sid, label.to_string());
        }
    }

    /// The per-item attribute name recorded for `sid`, if any. `Some`
    /// means the sketch hashes that label's VALUE (so `estimate(value)`
    /// is meaningful); `None` means per-attribute-set keying (only the
    /// bucket total is meaningful — keyed selectors must safe-miss).
    pub fn item_label_for(&self, sid: u64) -> Option<String> {
        self.item_labels.read().unwrap().get(&sid).cloned()
    }

    /// Resolve a policy fingerprint to the set of sids it has minted.
    /// Returns an empty vector when no sid is bound to the fingerprint
    /// (e.g. fresh policy with no ingest activity yet) or when the
    /// caller passes [`PolicyFingerprint::UNSET`]. The order of the
    /// returned slice is sorted (the underlying index is a `BTreeSet`)
    /// so callers can hash / compare it deterministically.
    pub fn series_ids_for_policy(&self, policy_fp: PolicyFingerprint) -> Vec<u64> {
        if policy_fp.is_unset() {
            return Vec::new();
        }
        let candidates: Vec<_> = self
            .policy_to_series_ids
            .read()
            .unwrap()
            .get(&policy_fp)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        let generation = self.active_catalog_generation();
        let instances = self.instances.read().unwrap();
        candidates
            .into_iter()
            .filter(|sid| {
                instances.get(sid).is_some_and(|binding| {
                    Self::instance_visible_in_generation(binding, generation.as_deref())
                })
            })
            .collect()
    }

    fn instance_visible_in_generation(
        binding: &SdsBinding,
        generation: Option<&CatalogGeneration>,
    ) -> bool {
        !matches!(
            binding.data_descriptor.source,
            asap_types::sds::DataSourceIdentity::Derived { .. }
        ) || generation
            .is_some_and(|generation| binding.catalog_generation.as_deref() == Some(generation))
    }

    /// Live policy count — number of distinct fingerprints with at
    /// least one sid. Useful for telemetry / `/runtime` introspection
    /// (mirrors the legacy "active aggregation count" metric).
    pub fn policy_count(&self) -> usize {
        self.policy_to_series_ids.read().unwrap().len()
    }

    /// Look up the metadata for a sid (cloned because callers usually
    /// release the index lock before working with it).
    pub fn instance(&self, sid: u64) -> Option<Arc<SketchInstanceMetadata>> {
        self.instances
            .read()
            .unwrap()
            .get(&sid)
            .map(|instance| Arc::clone(&instance.metadata))
    }

    /// Shared SDS descriptors bound to a materialized SeriesId.
    pub fn descriptors_for_series_id(
        &self,
        sid: u64,
    ) -> Option<(Arc<SummaryDescriptor>, Arc<DataDescriptor>)> {
        let instances = self.instances.read().ok()?;
        let instance = instances.get(&sid)?;
        Some((
            Arc::clone(&instance.summary_descriptor),
            Arc::clone(&instance.data_descriptor),
        ))
    }

    pub fn descriptor_counts(&self) -> (usize, usize) {
        (
            self.descriptors.summary_count(),
            self.descriptors.data_count(),
        )
    }

    /// Build observed SDS inventory from the actual registered SummaryStore
    /// entries across the memory and durable tiers. Payload bytes remain in the
    /// store and are referenced by their fully-spelled-out series identity.
    pub fn observed_summary_inventory(
        &self,
        reporter_id: &str,
        storage_node_id: &str,
        producers: &BTreeMap<SummaryDefinitionId, String>,
        inventory_version: u64,
        observed_at_ms: i64,
    ) -> Result<ObservedSummaryInventory, String> {
        let catalog = self
            .descriptors
            .authoritative_catalog()
            .ok_or_else(|| "no authoritative SummaryCatalog is installed".to_string())?;
        let reference = catalog.reference().map_err(|error| error.to_string())?;
        let generation = CatalogGeneration {
            schema_version: reference.schema_version,
            plan_id: reference.plan_id,
            plan_version: reference.plan_version,
            snapshot_sha256: reference.snapshot_sha256,
        };
        let instances = self.instances.read().unwrap();
        let durable = self.persistence_read.read().unwrap().clone();
        let mut reported = BTreeMap::new();
        for (series_id, binding) in instances.iter() {
            if !Self::instance_visible_in_generation(binding, Some(&generation)) {
                continue;
            }
            let summary_definition_id = SummaryDefinitionId::from(binding.metadata.policy_fp);
            if binding.metadata.policy_fp.is_unset()
                || !catalog
                    .materializations
                    .contains_key(&summary_definition_id)
            {
                continue;
            }
            let producer_id = producers
                .get(&summary_definition_id)
                .map(String::as_str)
                .ok_or_else(|| {
                    format!(
                        "materialization {} has no producer in the active PrecomputePlan",
                        summary_definition_id.as_u64()
                    )
                })?;
            let store = self
                .series
                .get(series_id)
                .map(|entry| Arc::clone(entry.value()));
            let completed_through = self
                .completed_windows
                .read()
                .unwrap()
                .get(series_id)
                .copied();
            let status = match binding.metadata.status() {
                AggStatus::Active => SummaryInstanceStatus::Ready,
                AggStatus::Retired | AggStatus::Expired => SummaryInstanceStatus::Retiring,
            };
            let mut record = |window: TimestampRange,
                              group_values: BTreeMap<String, String>|
             -> Result<(), String> {
                let start_ms = i64::try_from(window.0)
                    .map_err(|_| "summary instance start exceeds signed timestamp range")?;
                let end_ms = i64::try_from(window.1)
                    .map_err(|_| "summary instance end exceeds signed timestamp range")?;
                let group_bytes =
                    serde_json::to_vec(&group_values).map_err(|error| error.to_string())?;
                let group_fingerprint = xxhash_rust::xxh64::xxh64(&group_bytes, 0);
                let instance_id = SummaryInstanceId::new(format!(
                    "summary-instance:v1:{}:{}:{}:{}",
                    summary_definition_id.as_u64(),
                    window.0,
                    window.1,
                    group_fingerprint
                ))
                .map_err(|error| error.to_string())?;
                let instance = SummaryInstance {
                    instance_id: instance_id.clone(),
                    summary_definition_id,
                    summary_descriptor_id: binding.summary_descriptor.id().clone(),
                    data_descriptor_id: binding.data_descriptor.id().clone(),
                    time_range: HalfOpenTimeRange { start_ms, end_ms },
                    group_values,
                    catalog_generation: generation.clone(),
                    placement: SummaryPlacement {
                        producer_id: producer_id.into(),
                        storage_node_id: storage_node_id.into(),
                    },
                    state_reference: SummaryStateReference {
                        store: "summary-store".into(),
                        key: format!(
                            "series:{series_id}:pane:{}-{}:group:{group_fingerprint:016x}",
                            window.0, window.1
                        ),
                        state_schema_version: binding.summary_descriptor.state_schema_version,
                        generation: generation.plan_version,
                        sequence: window.1,
                        checksum: None,
                    },
                    status: status.clone(),
                    completeness: if completed_through.is_some_and(|end| window.1 <= end) {
                        InstanceCompleteness::Complete
                    } else {
                        InstanceCompleteness::Unknown
                    },
                    lifecycle: InstanceLifecycle::Persistent,
                    observed_at_ms,
                };
                instance.validate().map_err(|error| error.to_string())?;
                reported.insert(instance_id, instance);
                Ok(())
            };
            if let Some(store) = store {
                let store = store.read().map_err(|_| "summary series lock poisoned")?;
                for (window, label_id, _) in store.current_epoch.iter_entries() {
                    let group = store.intern.resolve(label_id).cloned().ok_or_else(|| {
                        "summary instance has an unresolved group identity".to_string()
                    })?;
                    record(window, group)?;
                }
                for epoch in store.sealed_epochs.values() {
                    for (window, label_id, _) in &epoch.entries {
                        let group = store.intern.resolve(*label_id).cloned().ok_or_else(|| {
                            "summary instance has an unresolved group identity".to_string()
                        })?;
                        record(*window, group)?;
                    }
                }
            }
            if let Some(handle) = &durable {
                let keys: Vec<_> = binding.metadata.group_by_keys.iter().cloned().collect();
                for part in handle.manifest.live_parts() {
                    let reader = handle
                        .part_cache
                        .get_or_load(part.part_id)
                        .map_err(|error| format!("load summary part {}: {error}", part.part_id))?;
                    for index in reader.index_records() {
                        if index.agg_id != *series_id {
                            continue;
                        }
                        let entry = reader.load_entry(&index).map_err(|error| {
                            format!("load summary part {} entry: {error}", part.part_id)
                        })?;
                        let values = entry.label.map(|label| label.labels).unwrap_or_default();
                        if values.len() != keys.len() {
                            return Err(format!(
                                "summary series {series_id} has {} group keys but durable state has {} values",
                                keys.len(), values.len()
                            ));
                        }
                        record(
                            (entry.start_ts, entry.end_ts),
                            keys.iter().cloned().zip(values).collect(),
                        )?;
                    }
                }
            }
        }
        let inventory = ObservedSummaryInventory {
            schema_version: 1,
            reporter_id: reporter_id.into(),
            inventory_version,
            observed_at_ms,
            instances: reported,
        };
        inventory.validate().map_err(|error| error.to_string())?;
        Ok(inventory)
    }

    /// Borrow-style metadata accessor (P2-2). Runs `f(&meta)` while
    /// holding the `instances` read lock and returns its result, WITHOUT
    /// cloning the (allocation-heavy: `String` + `BTreeSet<String>` +
    /// `AggKind`) metadata. Returns `None` (without invoking `f`) when
    /// the sid is unknown.
    ///
    /// Prefer this over [`Self::instance`] on hot per-candidate paths
    /// (the engine's capability check + the topk-over-rate fallback)
    /// that only need to *read* a field or two from the metadata. `f`
    /// runs under the read lock, so it must not call back into the store
    /// (which would deadlock) and should stay allocation-light — extract
    /// the small data you need (a `Capability` clone, a `bool`) and act
    /// after this returns.
    pub fn with_instance<R, F: FnOnce(&SketchInstanceMetadata) -> R>(
        &self,
        sid: u64,
        f: F,
    ) -> Option<R> {
        let g = self.instances.read().ok()?;
        g.get(&sid).map(|instance| f(&instance.metadata))
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
    ) -> bool {
        // Hold the existing admission guard through append so the global
        // finite barrier cannot race an unadmitted producer's last write.
        let admission = self.admission.read().unwrap();
        if admission.is_finite_closed() {
            return false;
        }
        let instances = self.instances.read().unwrap();
        if instances.get(&sid).is_some_and(|binding| {
            matches!(
                binding.data_descriptor.source,
                asap_types::sds::DataSourceIdentity::Derived { .. }
            )
        }) {
            return false;
        }
        self.append_sample_with_binding(sid, series_label_values, window, sample)
    }

    // Caller retains the existing metadata guard and has rejected derived
    // definitions. Avoid recursively acquiring it when a writer is waiting.
    fn append_sample_with_binding(
        &self,
        sid: u64,
        series_label_values: BTreeMap<String, String>,
        window: TimestampRange,
        sample: SketchSampleState,
    ) -> bool {
        let completed = self.completed_windows.read().unwrap();
        if completed.get(&sid).is_some_and(|end| window.1 <= *end) {
            return false;
        }
        let _mutation = self.begin_state_mutation();
        let store = self
            .series
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(self.fresh_sid_store())))
            .clone();
        let mut guard = store.write().unwrap();
        guard.insert(window, series_label_values, AggPayload::Sketch(sample));
        guard.last_write_unix_ms = now_ms();
        true
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
    ) -> bool {
        // Hold the existing admission guard through append so the global
        // finite barrier cannot race an unadmitted producer's last write.
        let admission = self.admission.read().unwrap();
        if admission.is_finite_closed() {
            return false;
        }
        let instances = self.instances.read().unwrap();
        if instances.get(&sid).is_some_and(|binding| {
            matches!(
                binding.data_descriptor.source,
                asap_types::sds::DataSourceIdentity::Derived { .. }
            )
        }) {
            return false;
        }
        self.append_precompute_with_binding(sid, series_label_values, window, payload)
    }

    // Caller retains the existing metadata guard and has rejected derived
    // definitions. Avoid recursively acquiring it when a writer is waiting.
    fn append_precompute_with_binding(
        &self,
        sid: u64,
        series_label_values: BTreeMap<String, String>,
        window: TimestampRange,
        payload: Box<dyn crate::storage_engines::types::AggregateCore>,
    ) -> bool {
        let completed = self.completed_windows.read().unwrap();
        if completed.get(&sid).is_some_and(|end| window.1 <= *end) {
            return false;
        }
        let _mutation = self.begin_state_mutation();
        let max_value = payload
            .as_any()
            .downcast_ref::<crate::precompute_engine::operators::MinMaxAccumulator>()
            .filter(|acc| acc.sub_type == "max")
            .map(|acc| acc.value);
        let store = self
            .series
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(self.fresh_sid_store())))
            .clone();
        let mut guard = store.write().unwrap();
        guard.insert(
            window,
            series_label_values.clone(),
            AggPayload::ExactAgg(Arc::from(payload)),
        );
        guard.last_write_unix_ms = now_ms();
        let retention_horizon_ms = guard.retention_horizon_ms;
        drop(guard);
        if let Some(value) = max_value.filter(|_| self.persistence_read.read().unwrap().is_none()) {
            self.rollups.append(
                RollupReduction::Max,
                sid,
                series_label_values,
                window,
                value,
                retention_horizon_ms,
            );
        }
        true
    }

    /// Read a category through the derived in-memory rollup. Returns `None`
    /// when persistence/recovery is active or no complete rollup exists, so
    /// callers can fall back to the canonical exact-agg range path.
    pub fn query_rollup_range(
        &self,
        category: RollupReduction,
        sid: u64,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Option<Vec<(BTreeMap<String, String>, f64)>> {
        if self.persistence_read.read().unwrap().is_some() {
            return None;
        }
        self.rollups
            .query(category, sid, start_unix_ms, end_unix_ms)
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
        if self
            .incomplete_summary_lineages
            .get(&sid)
            .is_some_and(|lineages| {
                lineages.iter().any(|lineage| {
                    lineage.window_start_unix_ms < end_unix_ms
                        && lineage.window_end_unix_ms > start_unix_ms
                })
            })
        {
            return Vec::new();
        }
        // Result is keyed by the resolved label MAP so the in-memory tier
        // (its own intern space) and the durable disk tier (independent
        // intern space) union by label identity, not `LabelValuesId`.
        let mut by_label_map: HashMap<
            BTreeMap<String, String>,
            BTreeMap<i64, Vec<SketchSampleState>>,
        > = HashMap::new();

        // ── In-memory tier ──────────────────────────────────────────────
        // Absent series is NOT an early return: under persistence the
        // sid's hot+sealed state may have been fully flushed-then-evicted
        // (or recovered from disk after a restart with no fresh ingest
        // yet), so the answer can live entirely on disk. We still run the
        // disk union below.
        if let Some(store) = self.series.get(&sid).map(|s| s.clone()) {
            let guard = store.write().unwrap(); // exact_query may build the lazy index
            let mut by_label_id: HashMap<LabelValuesId, BTreeMap<i64, Vec<SketchSampleState>>> =
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
                    // PUSH, not insert: a window_end can carry MULTIPLE
                    // sub-window frames (delta_transmission), all of which
                    // must survive in insertion (= column) order. The
                    // overlap scan iterates `windows_col` by index, so push
                    // preserves the producer's emit order within a window.
                    by_label_id
                        .entry(*label_id)
                        .or_default()
                        .entry(win.1 as i64)
                        .or_default()
                        .push(s.clone());
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
                            .entry(win.1 as i64)
                            .or_default()
                            .push(s.clone());
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
                        // Earliest window's FIRST frame: a leading Full (or
                        // sub-window seed Full) needs no carry-in.
                        samples
                            .values()
                            .next()
                            .and_then(|frames| frames.first())
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
                        // Carry-in base sorts before `start` (its w_end <
                        // start), so it heads the per-label BTreeMap and the
                        // reducer uses it as the rolling base. Only splice it
                        // when that window-end has no frames yet (don't
                        // duplicate a base the in-window scan already saw).
                        let frames = by_label_id
                            .entry(label_id)
                            .or_default()
                            .entry(w_end)
                            .or_default();
                        if frames.is_empty() {
                            frames.push(state);
                        }
                    }
                }
            }

            // Materialize the in-memory result keyed by the resolved label
            // MAP so the disk tier (which has its own intern space) can be
            // unioned by label identity rather than `LabelValuesId`. Merge
            // per-window-end frame lists rather than overwriting, so two
            // label_ids that resolve to the same label map (distinct intern
            // ids, same values) union their frames instead of clobbering.
            for (label_id, samples) in by_label_id {
                let label_values = guard.intern.resolve(label_id).cloned().unwrap_or_default();
                let dst = by_label_map.entry(label_values).or_default();
                for (w_end, frames) in samples {
                    dst.entry(w_end).or_default().extend(frames);
                }
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
            .map(|(label_values, samples)| SketchTimeSeries {
                sid,
                series_label_values: label_values,
                samples,
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
        by_label_map: &mut HashMap<BTreeMap<String, String>, BTreeMap<i64, Vec<SketchSampleState>>>,
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

        // Snapshot which `(label_map, window_end)` keys the in-memory tier
        // already populated, so the disk scan can apply the "in-memory wins"
        // rule per window-end without colliding frame lists across tiers.
        let in_mem_owned_ends: std::collections::HashSet<(BTreeMap<String, String>, i64)> =
            by_label_map
                .iter()
                .flat_map(|(lm, by_end)| by_end.keys().map(move |w| (lm.clone(), *w)))
                .collect();

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
                let w_end = rec.end_ts as i64;
                // In-memory wins on a window-end collision: only contribute
                // disk frames at window-ends the in-memory tier left empty.
                // When disk uniquely owns a window-end it may hold MULTIPLE
                // sub-window frames there (a flushed sub-window window kept
                // every frame); push them all in disk-record order. We never
                // mix disk + in-memory frames at one window-end.
                let by_end = by_label_map.entry(label_map.clone()).or_default();
                if in_mem_owned_ends.contains(&(label_map, w_end)) {
                    continue;
                }
                by_end.entry(w_end).or_default().push(sample);
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
                    .and_then(|frames| frames.first())
                    .map(|s| {
                        matches!(
                            s.encoding,
                            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta
                        )
                    })
                    .unwrap_or(false);
                let has_base_before = samples.iter().any(|(w_end, frames)| {
                    *w_end < start_unix_ms as i64
                        && frames.iter().any(|s| {
                            matches!(
                                s.encoding,
                                SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
                            )
                        })
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
            let frames = by_label_map
                .entry(label_map)
                .or_default()
                .entry(w_end)
                .or_default();
            if frames.is_empty() {
                frames.push(state);
            }
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
                if let Some(p) = payload.as_exact_agg_arc() {
                    by_label_id
                        .entry(*label_id)
                        .or_default()
                        .insert(win.1 as i64, Arc::clone(p));
                }
            }
            buf.clear();

            for sealed in guard.sealed_epochs.values() {
                sealed.range_query_into(start_unix_ms, end_unix_ms, &mut buf);
                for (win, label_id, payload) in &buf {
                    if let Some(p) = payload.as_exact_agg_arc() {
                        by_label_id
                            .entry(*label_id)
                            .or_default()
                            .insert(win.1 as i64, Arc::clone(p));
                    }
                }
                buf.clear();
            }

            for (label_id, samples) in by_label_id {
                let label_values_map = guard.intern.resolve(label_id).cloned().unwrap_or_default();
                by_label_map
                    .entry(label_values_map)
                    .or_default()
                    .extend(samples);
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
                let Some(acc) = reconstruct_exact_agg(&entry.sketch_type_name, &entry.sketch_bytes)
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
                    if reconstruct_exact_agg(&entry.sketch_type_name, &entry.sketch_bytes).is_none()
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
        // (`AggKind::Sketch { algorithm: kind, config, .. }`, registered by
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
        // `idx.series_ids_for_policy(fp)` + reducer dispatch and
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
    ///
    /// P2-2: uses the `metric_to_series_ids` secondary index for a keyed
    /// lookup of the candidate sids for `metric_name` instead of an O(N)
    /// full scan of `instances`; only that metric's (typically small)
    /// sid set is then filtered on the `group_by_keys` superset test.
    /// The two locks are taken in a read-only, non-overlapping fashion
    /// (metric index first, then a brief `instances` read per matched
    /// sid through the held guard) so the keyed path observes a
    /// consistent snapshot of the same write domain `register` /
    /// `remove_instance` maintain atomically.
    pub fn instances_matching(
        &self,
        metric_name: &str,
        required_keys: &BTreeSet<String>,
    ) -> Vec<u64> {
        let candidate_sids: Vec<_> = self
            .metric_to_series_ids
            .read()
            .unwrap()
            .get(metric_name)
            .map(|sids| sids.iter().copied().collect())
            .unwrap_or_default();
        let generation = self.active_catalog_generation();
        let instances = self.instances.read().unwrap();
        candidate_sids
            .iter()
            .filter(|sid| {
                instances
                    .get(sid)
                    .map(|m| {
                        required_keys.is_subset(&m.group_by_keys)
                            && Self::instance_visible_in_generation(m, generation.as_deref())
                    })
                    .unwrap_or(false)
            })
            .copied()
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

    /// True when a sid's in-memory `SidStoreData` may be dropped to reclaim
    /// resident memory: persistence owns its durability, NOTHING is pending
    /// in memory (so dropping it loses no un-flushed data), and it has been
    /// write-idle for at least `idle_threshold_ms`. `last_write_unix_ms == 0`
    /// (never written / freshly rehydrated) is never evictable.
    fn is_idle_evictable(
        data: &SidStoreData<BTreeMap<String, String>, AggPayload>,
        now: u64,
        idle_threshold_ms: u64,
    ) -> bool {
        data.persistence_enabled
            && data.sealed_epochs.is_empty()
            && data.current_epoch.is_empty()
            && data.last_write_unix_ms != 0
            && now.saturating_sub(data.last_write_unix_ms) >= idle_threshold_ms
    }

    /// Idle-sid eviction (memory reclaim). Drops the in-memory
    /// `SidStoreData` (epoch columns + intern-table label cache + the
    /// `series` slot) for every sketch sid that has gone write-idle past
    /// `idle_threshold_ms` AND whose state is fully durable on disk, while
    /// KEEPING its [`SketchInstanceMetadata`] in `instances`.
    ///
    /// Why keep the metadata: the query path's disk union
    /// ([`Self::query_range`] → `union_disk_parts_into`) needs
    /// `sid_group_by_keys(sid)` and `instances_matching` needs the
    /// metric/keys entry — drop those and the series silently stops
    /// resolving warm and falls through to the archive. So only the heavy,
    /// reconstructable part is evicted; the series stays queryable from the
    /// durable tier and the append path
    /// ([`Self::append_sample`]/[`Self::append_precompute`], both
    /// `entry(..).or_insert_with(..)`) transparently rehydrates a fresh
    /// store on the next write.
    ///
    /// Returns the number of sids evicted. `idle_threshold_ms == 0` is a
    /// no-op (feature disabled). O(N sids); meant for a periodic sweep, not
    /// the hot path. NOTE: because eviction requires `current_epoch` to be
    /// empty (all windows sealed+flushed), the effective idle horizon is
    /// `max(idle_threshold_ms, persistence_hot_window)`.
    pub fn evict_idle_series(&self, idle_threshold_ms: u64) -> usize {
        if idle_threshold_ms == 0 {
            return 0;
        }
        let now = now_ms();

        // Pass 1: collect candidates under read-only iteration. Removing
        // during `iter()` can deadlock against our own shard guards, so we
        // only gather here and remove afterwards.
        let mut candidates = Vec::new();
        for entry in self.series.iter() {
            if let Ok(data) = entry.value().read() {
                if Self::is_idle_evictable(&data, now, idle_threshold_ms) {
                    candidates.push(*entry.key());
                }
            }
        }
        if candidates.is_empty() {
            return 0;
        }

        // Pass 2: remove each, RE-CHECKING under the per-sid write lock so a
        // concurrent write that rehydrated/appended between the two passes
        // is not dropped. `remove_if` only deletes when the closure returns
        // true; taking the write lock there serializes with the append
        // path's `store.write()`. (No lock-order inversion: no path holds a
        // per-sid lock while acquiring a `series` shard lock.)
        let mut evicted = 0usize;
        for sid in candidates {
            let removed = self.series.remove_if(&sid, |_, store| {
                store
                    .write()
                    .map(|d| Self::is_idle_evictable(&d, now, idle_threshold_ms))
                    .unwrap_or(false)
            });
            if removed.is_some() {
                self.rollups.remove_sid(sid);
                evicted += 1;
            }
        }
        evicted
    }

    /// Approximate TOTAL resident bytes held by the store — the honest
    /// counterpart to the [`persistence::EpochSource::approx_memory_bytes`]
    /// payload gauge.
    ///
    /// `approx_memory_bytes` (used by the flusher's pressure trigger)
    /// counts ONLY live sketch payloads in `current_epoch` + `sealed_epochs`
    /// — which is correct for deciding what to FLUSH, because flushing only
    /// relieves payload. But once payloads are sealed to disk it reads ~0,
    /// even while the per-sid registry (`instances` metadata, the secondary
    /// indexes, and the per-series `InternTable` label caches) keeps
    /// hundreds of MB resident. That residue is NOT evictable by flushing —
    /// it is only released by retiring/evicting the sid itself — so it must
    /// not feed the flush trigger, but the memory DIAGNOSTIC must surface it
    /// or operators are blind to the real footprint. This method is that
    /// surface; it is O(N sids) and meant for the 30 s diagnostic tick, not
    /// the hot path.
    pub fn approx_resident_bytes(&self) -> usize {
        let mut total = 0usize;

        // 1. SeriesId bindings, compatibility metadata, and shared SDS descriptors.
        if let Ok(insts) = self.instances.read() {
            for m in insts.values() {
                total += std::mem::size_of::<SketchInstanceMetadata>();
                total += m.metric_name.len();
                for k in &m.group_by_keys {
                    total += k.len() + std::mem::size_of::<String>();
                }
            }
            total +=
                insts.capacity() * (std::mem::size_of::<u64>() + std::mem::size_of::<SdsBinding>());
        }
        total += self.descriptors.approx_resident_bytes();

        // 2. Per-sid series storage: live payloads + interned label maps +
        //    the `Arc<RwLock<SidStoreData>>` container slot.
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
            total += data.intern.approx_heap_bytes();
            total += std::mem::size_of::<SidStore>();
        }

        // 3. Derived exact-max rollups. They are deliberately reported in
        // resident memory even though they are not part of the durable
        // payload/flush accounting.
        total += self.rollups.approx_bytes();

        total
    }

    /// Snapshot shared metadata handles without holding the registry lock
    /// across user code. This is O(N) pointer cloning and does not copy
    /// descriptor strings, label sets, or aggregation configuration.
    pub fn snapshot_instances(&self) -> Vec<Arc<SketchInstanceMetadata>> {
        match self.instances.read() {
            Ok(map) => map
                .values()
                .map(|instance| Arc::clone(&instance.metadata))
                .collect(),
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

    /// Visit every registered binding under a single read lock,
    /// invoking `f(sid, &meta)` for each. Lets read-side scans that
    /// only need to *inspect* metadata (signature derivation,
    /// status filtering) avoid even the O(N) `Arc` snapshot allocation.
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
    pub fn list_by_status(&self, status: AggStatus) -> Vec<Arc<SketchInstanceMetadata>> {
        let map = match self.instances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        map.values()
            .filter(|s| s.status() == status)
            .map(|instance| Arc::clone(&instance.metadata))
            .collect()
    }

    fn metadata_record(&self, m: &SdsBinding) -> Option<persistence::metadata::SidMetaRecord> {
        let mut record = self.metadata_record_without_completion(m)?;
        record.completed_through_ms = self.completed_windows.read().unwrap().get(&m.sid).copied();
        Some(record)
    }

    fn metadata_record_without_completion(
        &self,
        m: &SdsBinding,
    ) -> Option<persistence::metadata::SidMetaRecord> {
        let mut record =
            crate::storage_engines::sketch_db::index::persistence::metadata::SidMetaRecord::new(
                m.sid,
                m.metric_name.clone(),
                m.group_by_keys.iter().cloned().collect(),
                &m.agg_kind,
                m.first_seen_unix_ms,
            );
        if !m.policy_fp.is_unset() {
            record.summary_definition_id = Some(SummaryDefinitionId::from(m.policy_fp));
            record.catalog_generation = Some(Arc::clone(m.catalog_generation.as_ref()?));
        }
        record.retired_at_ms = m.retired_at_ms;
        record.expires_at_ms = m.expires_at_ms;
        Some(record)
    }

    fn persist_lifecycle(&self, instance: &SdsBinding, removed: bool) -> Result<(), String> {
        let Some(writer) = self.persistence_metadata.read().unwrap().clone() else {
            return Ok(());
        };
        // A retired definition can disappear from the desired catalog before
        // its stored instances are collected. Preserve its persisted provenance
        // rather than rebinding it to the new catalog generation.
        let mut record = match self.metadata_record(instance) {
            Some(record) => record,
            None => writer
                .load()
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|record| record.sid == instance.sid)
                .ok_or("cannot persist lifecycle without catalog identity")?,
        };
        record.retired_at_ms = instance.retired_at_ms;
        record.expires_at_ms = instance.expires_at_ms;
        record.removed = removed;
        writer.upsert_all(&[record]).map_err(|error| {
            tracing::error!(sid = instance.sid, %error, "durable summary lifecycle publication failed");
            error.to_string()
        })
    }

    /// Force `sid` into `Retired` status, scheduling expiry
    /// `retention` from now. Idempotent — re-retiring a Retired or
    /// Expired sid is a no-op and returns the unchanged metadata.
    /// Returns `None` if the sid is unknown or durable lifecycle publication fails.
    pub fn force_retire(
        &self,
        sid: u64,
        retention: Duration,
    ) -> Option<Arc<SketchInstanceMetadata>> {
        let _mutation = self.begin_state_mutation();
        let mut map = self.instances.write().ok()?;
        let instance = map.get_mut(&sid)?;
        let mut next = instance.clone();
        let meta = Arc::make_mut(&mut next.metadata);
        if matches!(meta.status(), AggStatus::Active) {
            meta.retire(retention);
        }
        self.persist_lifecycle(&next, false).ok()?;
        *instance = next;
        Some(Arc::clone(&instance.metadata))
    }

    /// Force `sid` into `Expired` status immediately by setting both
    /// `retired_at_ms` and `expires_at_ms` to now. Returns the new
    /// state, or `None` if the sid is unknown. Intended for
    /// operator / debug-endpoint use so eviction can be observed in
    /// e2e tests without waiting out retirement retention.
    pub fn force_expire(&self, sid: u64) -> Option<Arc<SketchInstanceMetadata>> {
        let _mutation = self.begin_state_mutation();
        let mut map = self.instances.write().ok()?;
        let instance = map.get_mut(&sid)?;
        let mut next = instance.clone();
        let meta = Arc::make_mut(&mut next.metadata);
        let now = now_ms();
        meta.retired_at_ms = Some(meta.retired_at_ms.unwrap_or(now).min(now));
        meta.expires_at_ms = Some(meta.expires_at_ms.unwrap_or(now).min(now));
        self.persist_lifecycle(&next, false).ok()?;
        *instance = next;
        Some(Arc::clone(&instance.metadata))
    }

    pub(crate) fn active_catalog_generation(
        &self,
    ) -> Option<Arc<asap_types::sds::CatalogGeneration>> {
        self.descriptors
            .authoritative_snapshot()
            .map(|(_, generation)| generation)
    }

    /// Resolving a logical key may already return a replacement physical SID.
    /// Validate producer provenance before either a cache hit or a rotation.
    pub(crate) fn validate_routed_catalog_generation(
        &self,
        captured: Option<&asap_types::sds::CatalogGeneration>,
    ) -> Result<(), String> {
        if self.active_catalog_generation().as_deref() != captured {
            return Err(
                "unbound or stale producer cannot resolve the active catalog's physical series"
                    .into(),
            );
        }
        Ok(())
    }

    /// Return the current generation only when it explicitly reintroduces a
    /// previously removed logical materialization. Ordinary same-generation
    /// writes and missing provenance cannot start a new physical lifetime.
    pub(crate) fn authorize_series_reactivation(
        &self,
        sid: u64,
        definition: SummaryDefinitionId,
    ) -> Result<Option<Arc<asap_types::sds::CatalogGeneration>>, String> {
        if let Some(binding) = self
            .instances
            .read()
            .map_err(|_| "instance registry poisoned")?
            .get(&sid)
        {
            if matches!(
                binding.data_descriptor.source,
                asap_types::sds::DataSourceIdentity::Derived { .. }
            ) {
                let (catalog, generation) = self
                    .descriptors
                    .authoritative_snapshot()
                    .ok_or("derived reactivation requires an authoritative catalog")?;
                if binding.metadata.policy_fp != definition.fingerprint()
                    || !catalog.materializations.contains_key(&definition)
                {
                    return Err("derived reactivation differs from its installed definition".into());
                }
                if binding.catalog_generation.as_deref() != Some(generation.as_ref()) {
                    return Ok(Some(generation));
                }
            }
        }
        let removed = self
            .removed_sids
            .read()
            .map_err(|_| "series tombstone lock poisoned")?;
        let Some((old_generation, old_definition)) = removed.get(&sid) else {
            return Ok(None);
        };
        let (catalog, generation) = self
            .descriptors
            .authoritative_snapshot()
            .ok_or("series reactivation requires an authoritative catalog")?;
        if *old_definition != Some(definition)
            || !catalog.materializations.contains_key(&definition)
        {
            return Err("series reactivation does not match the installed materialization".into());
        }
        let old_generation = old_generation
            .as_ref()
            .ok_or("removed series has no catalog provenance")?;
        if old_generation.as_ref() == generation.as_ref() {
            return Err("same-generation append cannot reactivate a removed series".into());
        }
        Ok(Some(generation))
    }

    /// Drop a sid's metadata + its series state + both secondary-index
    /// entries (`policy_to_series_ids` and `metric_to_series_ids`). Mirrors
    /// `SchemaRegistry::remove_schema` for the eviction path's
    /// post-data-drop cleanup. Returns the removed metadata, or `None`
    /// if the sid was absent.
    ///
    /// ## Atomicity (P2-1)
    ///
    /// Takes all three index write guards together in the fixed order
    /// `instances → policy_to_series_ids → metric_to_series_ids` so the removal is
    /// atomic with respect to a concurrent reader — the sid never
    /// lingers in a secondary index after it has left `instances`. The
    /// `series` DashMap is touched after the index guards are released
    /// (it is independently keyed and not part of the metadata-index
    /// invariant).
    pub fn remove_instance(&self, sid: u64) -> Option<Arc<SketchInstanceMetadata>> {
        let _mutation = self.begin_state_mutation();
        let removed = {
            // Fixed lock order: instances → policy_to_series_ids → metric_to_series_ids.
            let mut instances = self.instances.write().ok()?;
            if let Some(instance) = instances.get(&sid) {
                self.persist_lifecycle(instance, true).ok()?;
                let record = self.metadata_record(instance);
                self.removed_sids.write().ok()?.insert(
                    sid,
                    (
                        record
                            .as_ref()
                            .and_then(|value| value.catalog_generation.clone()),
                        record.and_then(|value| value.summary_definition_id),
                    ),
                );
            }
            let mut policy_idx = self.policy_to_series_ids.write().unwrap();
            let mut metric_idx = self.metric_to_series_ids.write().unwrap();
            let removed = instances.remove(&sid);
            if let Some(meta) = &removed {
                if !meta.policy_fp.is_unset() {
                    if let Some(set) = policy_idx.get_mut(&meta.policy_fp) {
                        set.remove(&sid);
                        if set.is_empty() {
                            policy_idx.remove(&meta.policy_fp);
                        }
                    }
                }
                if let Some(set) = metric_idx.get_mut(&meta.metric_name) {
                    set.remove(&sid);
                    if set.is_empty() {
                        metric_idx.remove(&meta.metric_name);
                    }
                }
            }
            removed.map(|instance| instance.metadata)
        };
        if removed.is_some() {
            self.series.remove(&sid);
            self.rollups.remove_sid(sid);
            self.incomplete_summary_lineages.remove(&sid);
            self.descriptors.prune();
        }
        removed
    }

    /// Mark a producer/window delta lineage unsafe for warm reads.
    pub fn mark_summary_lineage_incomplete(
        &self,
        sid: u64,
        frame: &asap_types::producer_plan::SummaryFrameIdentity,
    ) {
        self.incomplete_summary_lineages
            .entry(sid)
            .or_default()
            .insert(frame.into());
    }

    /// A full checkpoint repairs only its exact producer/window lineage.
    pub fn clear_summary_lineage_incomplete(
        &self,
        sid: u64,
        frame: &asap_types::producer_plan::SummaryFrameIdentity,
    ) {
        let key = IncompleteSummaryLineage::from(frame);
        if let Some(mut lineages) = self.incomplete_summary_lineages.get_mut(&sid) {
            lineages.remove(&key);
        }
    }
}

impl SketchStore {
    /// Phase 5 M2.3.6g — runtime-info / diagnostic helper. Returns the
    /// per-sid `first_seen_unix_ms` for every registered sid. The
    /// legacy `Store::get_earliest_timestamp_per_aggregation_id` returned
    /// an analogous `agg_id → ts` map; this is the SketchStore
    /// equivalent. HTTP server's `/api/v1/status/runtimeinfo` adapter
    /// surfaces it under the JSON field `earliest_timestamp_per_sid`.
    pub fn earliest_timestamps_per_series_id(&self) -> std::collections::HashMap<u64, u64> {
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
    pub fn ingest_precompute_for_agg_config<R: Into<Option<u64>>>(
        &self,
        mint_sid: impl FnOnce(&str, &str, &str) -> R,
        agg_cfg: &asap_types::aggregation_config::AggregationConfig,
        output: &crate::storage_engines::types::PrecomputedOutput,
        accumulator: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        let admission = self.admission.read().ok()?;
        if admission.is_finite_closed() {
            return None;
        }
        self.ingest_precompute_config_with_admission(mint_sid, agg_cfg, output, accumulator)
    }

    fn ingest_precompute_config_with_admission<R: Into<Option<u64>>>(
        &self,
        mint_sid: impl FnOnce(&str, &str, &str) -> R,
        agg_cfg: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        accumulator: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        // B7.7 — this wrapper now derives the sid via `mint_sid` and
        // delegates to `ingest_precompute_with_series_id`. Callers that
        // already hold the bucket sid (B7.6's worker passes it on the
        // `WorkerMessage`; B7.7's backfill processor groups raw
        // samples by sid up-front) skip the resolver round-trip by
        // invoking the sid-direct sibling.
        let (attrs_fp, _label_values_map) = build_attrs_fp_and_label_map(agg_cfg, output);
        let agg_kind_canonical =
            crate::storage_engines::sketch_db::data::materialization_kind_for_config(agg_cfg);
        let sid = mint_sid(&agg_cfg.metric, &attrs_fp, &agg_kind_canonical).into()?;
        self.ingest_precompute_with_admission(sid, agg_cfg, output, accumulator)
    }

    fn register_precompute_output(
        &self,
        sid: u64,
        agg_cfg: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
    ) -> Option<BTreeMap<String, String>> {
        let (_attrs_fp, label_values_map) = build_attrs_fp_and_label_map(agg_cfg, output);
        let key_names = &agg_cfg.grouping_labels.names();
        let agg_kind = crate::storage_engines::sketch_db::data::agg_kind_for_config(agg_cfg);
        let (capability, accuracy) = agg_kind.capability_and_accuracy();

        match self.instance(sid) {
            None => {
                if self.active_catalog_generation().as_deref()
                    != output.catalog_generation.as_deref()
                {
                    return None;
                }

                let group_by_keys: BTreeSet<String> = if agg_cfg.partitioning
                    == Some(asap_types::sds::PopulationPartitioning::PerEntity)
                {
                    // The catalog describes per-entity partitioning; physical
                    // SID metadata must retain the observed label names used
                    // to decode its value-only durable population key.
                    output
                        .population_labels
                        .as_ref()
                        .map(|labels| labels.keys().cloned().collect())
                        .unwrap_or_else(|| key_names.iter().cloned().collect())
                } else {
                    key_names.iter().cloned().collect()
                };
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
                    capability: Some(capability),
                    agg_kind,
                    accuracy,
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
            Some(existing) if !existing.is_writable() || existing.policy_fp != output.policy_fp => {
                return None;
            }
            Some(_) => {}
        }

        Some(label_values_map)
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
    pub fn ingest_precompute_with_series_id(
        &self,
        sid: u64,
        agg_cfg: &asap_types::aggregation_config::AggregationConfig,
        output: &crate::storage_engines::types::PrecomputedOutput,
        accumulator: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        let admission = self.admission.read().ok()?;
        if admission.is_finite_closed() {
            return None;
        }
        self.ingest_precompute_with_admission(sid, agg_cfg, output, accumulator)
    }

    fn ingest_precompute_with_admission(
        &self,
        sid: u64,
        agg_cfg: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        accumulator: &dyn crate::storage_engines::types::AggregateCore,
    ) -> Option<u64> {
        let label_values_map = self.register_precompute_output(sid, agg_cfg, output)?;

        // Keep the physical lifetime alive through publication. Removal takes
        // this same lock exclusively, so it cannot race metadata validation and
        // recreate orphan payload after the tombstone commits.
        let instances = self.instances.read().ok()?;
        let binding = instances.get(&sid)?;
        if !binding.metadata.is_writable()
            || binding.metadata.policy_fp != output.policy_fp
            || matches!(
                binding.data_descriptor.source,
                asap_types::sds::DataSourceIdentity::Derived { .. }
            )
        {
            return None;
        }
        if binding.catalog_generation.is_some() && output.catalog_generation.is_none() {
            return None;
        }
        if let Some(captured) = output.catalog_generation.as_deref() {
            // Existing unchanged series may drain their birth generation or
            // accept the currently installed generation. A replacement born in
            // a newer generation cannot accept an older unbound cache hit.
            if binding.catalog_generation.as_deref() != Some(captured)
                && self.active_catalog_generation().as_deref() != Some(captured)
            {
                return None;
            }
        }

        if let Some(retained_windows) = agg_cfg.num_aggregates_to_retain {
            let required_horizon_ms = retained_windows
                .saturating_mul(agg_cfg.slide_interval)
                .saturating_mul(1_000);
            let store = self
                .series
                .entry(sid)
                .or_insert_with(|| Arc::new(RwLock::new(self.fresh_sid_store())))
                .clone();
            let mut store = store.write().unwrap();
            if !store.persistence_enabled {
                store.retention_horizon_ms = Some(
                    store
                        .retention_horizon_ms
                        .unwrap_or(0)
                        .max(required_horizon_ms),
                );
            }
        }

        let window = (output.start_timestamp, output.end_timestamp);
        let accepted = match crate::storage_engines::sketch_db::data::agg_kind_for_config(agg_cfg) {
            AggKind::Sketch { .. } => self.append_sample_with_binding(
                sid,
                label_values_map,
                window,
                SketchSampleState {
                    bytes: accumulator.serialize_to_bytes(),
                    encoding: SketchEncoding::MsgpackFull,
                },
            ),
            AggKind::ExactAgg { .. } => self.append_precompute_with_binding(
                sid,
                label_values_map,
                window,
                accumulator.clone_boxed_core(),
            ),
        };
        accepted.then_some(sid)
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
        let target_group_keys: BTreeSet<String> = agg_cfg.grouping_labels.iter().cloned().collect();

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
/// `part_cache` backs disk reads; `query_range` combines durable parts with
/// live in-memory state through the installed persistence read handle.
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

        // Publish the writer before recovery or any background work so a
        // concurrent lifecycle operation cannot succeed without persistence.
        let metadata_writer =
            Arc::new(persistence::metadata::SidMetadataStore::new(&cfg.disk_path));
        // Restore the generation-wide raw admission barrier before exposing
        // recovered state or accepting another producer after restart.
        if let Some(closed) = metadata_writer.load_finite_closure()? {
            if self.active_catalog_generation().as_deref() == Some(&closed) {
                self.admission
                    .write()
                    .unwrap()
                    .seal_finite(&closed)
                    .map_err(persistence::PersistError::Internal)?;
            }
        }
        {
            // Registration and lifecycle changes take this lock first too.
            let _instances = self.instances.write().unwrap();
            *self.persistence_metadata.write().unwrap() = Some(Arc::clone(&metadata_writer));
            self.removed_sids.write().unwrap().extend(
                metadata_writer
                    .load()?
                    .into_iter()
                    .filter(|record| record.removed)
                    .map(|record| {
                        (
                            record.sid,
                            (record.catalog_generation, record.summary_definition_id),
                        )
                    }),
            );
        }
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

        let flusher = FlusherHandle::start_with_metadata(
            cfg,
            Arc::clone(&manifest),
            Arc::clone(self),
            metadata_writer,
        )?;

        *self.immutable_publisher.write().unwrap() = Arc::downgrade(&flusher.publication_handle());
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
            if let Some(end) = rec.completed_through_ms {
                self.completed_windows
                    .write()
                    .unwrap()
                    .entry(rec.sid)
                    .and_modify(|current| *current = (*current).max(end))
                    .or_insert(end);
            }
            if rec.removed || rec.expires_at_ms.is_some_and(|expiry| expiry <= now_ms()) {
                continue;
            }
            // Don't clobber a live-registered instance.
            if self.instance(rec.sid).is_some() {
                continue;
            }
            let catalog = self.descriptors.authoritative_snapshot();
            let policy_fp = match (
                &rec.summary_definition_id,
                &rec.catalog_generation,
                &catalog,
            ) {
                (Some(definition), Some(generation), Some((catalog, installed_generation))) => {
                    if generation != installed_generation
                        || !catalog.materializations.contains_key(definition)
                    {
                        tracing::warn!(
                            sid = rec.sid,
                            "persisted summary catalog provenance differs; leaving state unbound"
                        );
                        continue;
                    }
                    definition.fingerprint()
                }
                // Legacy deployments without an authoritative plan retain their
                // legacy path. Never promote such state into a catalog binding.
                (None, None, None) => PolicyFingerprint::UNSET,
                _ => {
                    tracing::warn!(sid = rec.sid, "persisted summary has no matching authoritative identity; leaving state unbound");
                    continue;
                }
            };
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
                retired_at_ms: rec.retired_at_ms,
                expires_at_ms: rec.expires_at_ms,
                policy_fp,
            });
            if self.instance(rec.sid).is_some() {
                registered += 1;
            }
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
        self.rollups.clear();
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
    fn flush_before_ms(&self) -> Option<u64> {
        let cutoff = self
            .completion_flush_before
            .load(std::sync::atomic::Ordering::SeqCst);
        (cutoff != 0).then_some(cutoff)
    }

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
    ) -> Option<crate::storage_engines::sketch_db::index::persistence::metadata::SidMetaRecord>
    {
        let g = self.instances.read().ok()?;
        let m = g.get(&sid)?;
        self.metadata_record(m)
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
                    AggKind::Sketch {
                        algorithm: kind, ..
                    } => Some(format!("{:?}", kind)),
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
                AggPayload::ExactAgg(p) => (p.type_name().to_string(), 0u8, p.serialize_to_bytes()),
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

    #[test]
    fn max_rollup_answers_aligned_and_partial_ranges_and_prunes_history() {
        let mut rollup = ReductionRollupSeries::new(RollupReduction::Max);
        for index in 0..16u64 {
            let start = 5_000 + index * 30_000;
            rollup.append((start, start + 30_000), index as f64, Some(12 * 30_000));
        }
        assert_eq!(
            rollup.query(5_000 + 4 * 30_000, 5_000 + 16 * 30_000),
            Some(15.0)
        );
        assert_eq!(
            rollup.query(5_000 + 7 * 30_000, 5_000 + 11 * 30_000),
            Some(10.0)
        );
        assert_eq!(rollup.query(5_000, 35_000), None);

        let mut gapped = ReductionRollupSeries::new(RollupReduction::Max);
        gapped.append((0, 30_000), 1.0, None);
        gapped.append((60_000, 90_000), 2.0, None);
        assert_eq!(gapped.query(0, 90_000), None);
    }

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
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::DDSketch,
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

    #[test]
    fn completing_writer_between_revision_loads_cannot_certify_a_snapshot() {
        let store = SketchStore::new();
        let before = store.summary_update_revision();
        let writer = store.begin_state_mutation();
        let after = SummaryReadRevision::capture(
            0,
            &store.mutation_revision,
            &store.active_mutations,
            || drop(writer),
        );
        assert!(!before.matches(after));
        assert!(!after.matches(after), "capture crossed a writer completion");
    }

    #[test]
    fn direct_store_writes_invalidate_query_snapshots_even_without_admission() {
        let store = SketchStore::new();
        let before = store.summary_update_revision();
        let mutation = store.begin_state_mutation();
        let during = store.summary_update_revision();
        assert!(
            !during.matches(during),
            "an in-flight write cannot certify a snapshot"
        );
        drop(mutation);
        assert!(!before.matches(store.summary_update_revision()));
        let before = store.summary_update_revision();
        store.append_sample(1, BTreeMap::new(), (0, 1000), sample(1));
        assert!(!before.matches(store.summary_update_revision()));
    }

    #[test]
    fn store_lookups_and_equivalent_sids_share_sds_allocations() {
        let store = SketchStore::new();
        store.register(meta(1));
        store.register(meta(2));

        let first_lookup = store.instance(1).unwrap();
        let second_lookup = store.instance(1).unwrap();
        assert!(Arc::ptr_eq(&first_lookup, &second_lookup));

        let (summary_a, data_a) = store.descriptors_for_series_id(1).unwrap();
        let (summary_b, data_b) = store.descriptors_for_series_id(2).unwrap();
        assert!(Arc::ptr_eq(&summary_a, &summary_b));
        assert!(Arc::ptr_eq(&data_a, &data_b));
        assert_eq!(store.descriptor_counts(), (1, 1));
    }

    #[test]
    fn observed_inventory_uses_installed_catalog_and_real_store_entries() {
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let store = SketchStore::new();
        store
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        store.register(meta_with_policy(41, fingerprint));
        store.append_sample(
            41,
            BTreeMap::from([("job".to_string(), "api".to_string())]),
            (0, 10_000),
            sample(1),
        );
        store.append_sample(
            41,
            BTreeMap::from([("job".to_string(), "worker".to_string())]),
            (10_000, 20_000),
            sample(2),
        );

        let producers = BTreeMap::from([(
            SummaryDefinitionId::from(fingerprint),
            "producer-a".to_string(),
        )]);
        let inventory = store
            .observed_summary_inventory("backend-a", "store-a", &producers, 1, 100)
            .unwrap();
        inventory.validate().unwrap();
        assert_eq!(inventory.instances.len(), 2);
        let instance = inventory.instances.values().next().unwrap();
        assert_eq!(instance.summary_definition_id.fingerprint(), fingerprint);
        assert_eq!(instance.status, SummaryInstanceStatus::Ready);
        assert_eq!(instance.completeness, InstanceCompleteness::Unknown);
        assert!(!instance.group_values.is_empty());
        assert!(inventory
            .instances
            .values()
            .all(|instance| instance.time_range.start_ms < instance.time_range.end_ms));
        assert_eq!(
            inventory
                .instances
                .values()
                .map(|instance| instance.state_reference.key.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        let next_inventory = store
            .observed_summary_inventory("backend-a", "store-a", &producers, 2, 200)
            .unwrap();
        assert_eq!(
            inventory.instances.keys().collect::<Vec<_>>(),
            next_inventory.instances.keys().collect::<Vec<_>>()
        );
        let catalog_identity =
            &plan.summary_catalog.materializations[&instance.summary_definition_id];
        assert_eq!(
            instance.summary_descriptor_id,
            catalog_identity.summary_descriptor_id
        );
        assert_eq!(
            instance.data_descriptor_id,
            catalog_identity.data_descriptor_id
        );
    }

    #[test]
    fn registered_series_without_payload_is_not_a_summary_instance() {
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let store = SketchStore::new();
        store
            .install_summary_catalog(Arc::new(plan.summary_catalog))
            .unwrap();
        store.register(meta_with_policy(42, fingerprint));
        let producers = BTreeMap::from([(
            SummaryDefinitionId::from(fingerprint),
            "producer-a".to_string(),
        )]);
        let inventory = store
            .observed_summary_inventory("backend-a", "store-a", &producers, 1, 100)
            .unwrap();
        assert!(inventory.instances.is_empty());
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
        assert_eq!(idx.classify(42), SeriesLookup::Ghost);
        assert_eq!(idx.classify(999), SeriesLookup::Unknown);
    }

    #[test]
    fn hit_after_append() {
        let idx = SketchStore::new();
        idx.register(meta(7));
        idx.append_sample(7, BTreeMap::new(), (1000, 1010), sample(1));
        assert_eq!(idx.classify(7), SeriesLookup::Hit);
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
        assert_eq!(s_a.samples[&10][0].bytes, vec![1]);
        assert_eq!(s_a.samples[&20][0].bytes, vec![2]);

        let s_b = &series[1];
        assert_eq!(s_b.series_label_values, lv_b);
        assert_eq!(s_b.samples.len(), 2);
    }

    #[test]
    fn incomplete_delta_window_fails_closed_until_matching_full_checkpoint() {
        use asap_types::producer_plan::{SummaryFrameIdentity, SummaryFrameKind};
        use control_plane::physical::compiler::StateEncoding;

        let idx = SketchStore::new();
        idx.register(meta(12));
        idx.append_sample(12, BTreeMap::new(), (0, 10), sample(0));
        idx.append_sample(12, BTreeMap::new(), (1000, 1010), sample(1));
        let frame = SummaryFrameIdentity {
            identity_version: 1,
            plan_id: 7,
            plan_version: 2,
            backend_compat: "asap-query-backend.v1".into(),
            materialization: PolicyFingerprint(41).into(),
            series_identity: "service=checkout,zone=a".into(),
            schema_id: "schema-41".into(),
            producer_id: "edge-a".into(),
            producer_epoch: "boot-1".into(),
            window_start_unix_nano: 1_000_000_000,
            window_end_unix_nano: 1_010_000_000,
            sequence: 3,
            kind: SummaryFrameKind::Delta,
            encoding: StateEncoding::SketchlibProtobufV1,
            checkpoint_id: None,
            base_checkpoint_id: Some("cp-1".into()),
        };
        idx.mark_summary_lineage_incomplete(12, &frame);
        assert!(idx.query_range(12, 1000, 1010).is_empty());
        assert_eq!(idx.query_range(12, 0, 999).len(), 1);

        let recovery = SummaryFrameIdentity {
            kind: SummaryFrameKind::Full,
            checkpoint_id: Some("cp-4".into()),
            base_checkpoint_id: None,
            sequence: 4,
            ..frame
        };
        idx.clear_summary_lineage_incomplete(12, &recovery);
        assert_eq!(idx.query_range(12, 1000, 1010).len(), 1);
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
        assert_eq!(
            s.samples.get(&200).unwrap()[0].encoding,
            SketchEncoding::ProtoFull
        );
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
        assert!(
            retained < 480,
            "old windows were not evicted (leak persists)"
        );

        // (b) A 30m range query ending at the freshest window still
        // resolves (well within the 1h horizon) AND the carry-in finds a
        // Full base for any leading delta — no regression to #323–#326.
        let q_start = last_end - 30 * 60 * 1000;
        let series = idx.query_range(77, q_start, last_end);
        assert_eq!(series.len(), 1, "recent 30m window must stay queryable");
        let s = &series[0];
        assert!(!s.samples.is_empty(), "30m range query returned no samples");
        let first = &s.samples.values().next().unwrap()[0];
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
        assert_eq!(idx.classify(1), SeriesLookup::Hit);
        let removed = idx.remove_instance(1).expect("sid known");
        assert_eq!(removed.sid, 1);
        assert_eq!(idx.classify(1), SeriesLookup::Unknown);
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
        assert_eq!(
            idx.classify(42),
            SeriesLookup::Hit,
            "storage has data — Hit"
        );
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
        let exact_agg = AggPayload::ExactAgg(Arc::new(SumAccumulator::with_sum(1.0)));
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
    fn series_ids_for_policy_returns_empty_for_unset_or_missing() {
        let idx = SketchStore::new();
        // Empty store → nothing for any fp.
        assert!(idx
            .series_ids_for_policy(asap_types::PolicyFingerprint(42))
            .is_empty());
        // The UNSET sentinel always returns empty regardless of state.
        idx.register(meta_with_policy(1, asap_types::PolicyFingerprint::UNSET));
        assert!(idx
            .series_ids_for_policy(asap_types::PolicyFingerprint::UNSET)
            .is_empty());
    }

    #[test]
    fn register_indexes_one_sid_under_its_policy() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        assert_eq!(idx.series_ids_for_policy(fp), vec![1]);
        assert_eq!(idx.policy_count(), 1);
    }

    #[test]
    fn register_groups_multiple_sids_under_one_policy() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        idx.register(meta_with_policy(2, fp));
        idx.register(meta_with_policy(3, fp));
        let mut sids = idx.series_ids_for_policy(fp);
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
        assert_eq!(idx.series_ids_for_policy(fp_a), vec![1, 3]);
        assert_eq!(idx.series_ids_for_policy(fp_b), vec![2]);
        assert_eq!(idx.policy_count(), 2);
    }

    #[test]
    fn unset_policy_sids_are_not_in_reverse_index() {
        let idx = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(7);
        idx.register(meta_with_policy(1, fp));
        // sid 2 has UNSET — should NOT show up under any fp.
        idx.register(meta_with_policy(2, asap_types::PolicyFingerprint::UNSET));
        assert_eq!(idx.series_ids_for_policy(fp), vec![1]);
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
        assert_eq!(idx.series_ids_for_policy(fp), vec![2]);
        assert_eq!(idx.policy_count(), 1);
        idx.remove_instance(2);
        assert!(idx.series_ids_for_policy(fp).is_empty());
        // Empty entry collapses — policy_count drops to 0.
        assert_eq!(idx.policy_count(), 0);
    }

    // ── metric_to_series_ids secondary-index tests (P2-2) ──────────────────

    /// Metadata with a chosen metric name + group-by key set, so the
    /// secondary-index tests can register several metrics/keys.
    fn meta_metric_keys(sid: u64, metric: &str, keys: &[&str]) -> SketchInstanceMetadata {
        let mut m = meta(sid);
        m.metric_name = metric.to_string();
        m.group_by_keys = keys.iter().map(|k| k.to_string()).collect();
        m
    }

    #[test]
    fn instances_matching_keyed_lookup_filters_by_metric_and_keys() {
        let idx = SketchStore::new();
        // Two metrics; sid 1/2 on metric_a (different key coverage),
        // sid 3 on metric_b.
        idx.register(meta_metric_keys(1, "metric_a", &["zone", "rack"]));
        idx.register(meta_metric_keys(2, "metric_a", &["zone"]));
        idx.register(meta_metric_keys(3, "metric_b", &["zone"]));

        // Keyed lookup returns ONLY the requested metric's sids, and only
        // those whose group_by_keys ⊇ required_keys.
        let req_zone: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
        let mut a = idx.instances_matching("metric_a", &req_zone);
        a.sort_unstable();
        assert_eq!(a, vec![1, 2], "both metric_a sids cover {{zone}}");

        let req_zone_rack: BTreeSet<String> = ["zone".to_string(), "rack".to_string()]
            .into_iter()
            .collect();
        assert_eq!(
            idx.instances_matching("metric_a", &req_zone_rack),
            vec![1],
            "only sid 1 covers {{zone,rack}}"
        );

        assert_eq!(idx.instances_matching("metric_b", &req_zone), vec![3]);
        // A metric with no registered sids → empty (no full scan, no panic).
        assert!(idx
            .instances_matching("metric_absent", &BTreeSet::new())
            .is_empty());
    }

    #[test]
    fn instances_matching_secondary_index_updated_on_retire_and_remove() {
        let idx = SketchStore::new();
        idx.register(meta_metric_keys(1, "metric_a", &["zone"]));
        idx.register(meta_metric_keys(2, "metric_a", &["zone"]));
        let req: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
        let mut got = idx.instances_matching("metric_a", &req);
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);

        // Retire (force_retire) does NOT remove the sid from the index —
        // it stays queryable so an in-flight query can still read its
        // pre-expiry state; the eviction sweep calls remove_instance
        // later. Confirm the index still surfaces both.
        idx.force_retire(1, Duration::from_secs(3600));
        let mut still = idx.instances_matching("metric_a", &req);
        still.sort_unstable();
        assert_eq!(
            still,
            vec![1, 2],
            "retire keeps the sid in the metric index"
        );

        // remove_instance (the post-eviction cleanup) drops it from the
        // secondary index too.
        idx.remove_instance(1);
        assert_eq!(
            idx.instances_matching("metric_a", &req),
            vec![2],
            "removed sid leaves the metric index"
        );
        idx.remove_instance(2);
        assert!(
            idx.instances_matching("metric_a", &req).is_empty(),
            "last sid gone → metric key collapses, keyed lookup empty"
        );
    }

    #[test]
    fn metric_index_agrees_with_instances_after_mixed_churn() {
        // Cross-check the P2-1 atomicity invariant statically: after a
        // sequence of register/remove the metric index's union must equal
        // the set of sids in `instances`.
        let idx = SketchStore::new();
        idx.register(meta_metric_keys(10, "m1", &["a"]));
        idx.register(meta_metric_keys(11, "m1", &["a", "b"]));
        idx.register(meta_metric_keys(12, "m2", &["a"]));
        idx.remove_instance(10);
        idx.register(meta_metric_keys(13, "m2", &["a"]));

        let from_instances: BTreeSet<u64> = idx.instances.read().unwrap().keys().copied().collect();
        let from_metric_idx: BTreeSet<u64> = idx
            .metric_to_series_ids
            .read()
            .unwrap()
            .values()
            .flat_map(|s| s.iter().copied())
            .collect();
        assert_eq!(
            from_instances, from_metric_idx,
            "metric_to_series_ids union must equal the instances key set (P2-1 invariant)"
        );
        // And the cleared metric key must be gone, not lingering empty.
        assert_eq!(
            idx.instances_matching(
                "m1",
                &["a".to_string()].into_iter().collect::<BTreeSet<_>>()
            ),
            vec![11]
        );
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
        let _p = idx
            .start_persistence(durable_cfg(tmp.path().to_path_buf()))
            .unwrap();

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
        let flushed = wait_until(
            || !_p.manifest.live_parts().is_empty(),
            std::time::Duration::from_secs(3),
        );
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
        let p = idx
            .start_persistence(durable_cfg(tmp.path().to_path_buf()))
            .unwrap();

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
        let p = idx
            .start_persistence(durable_cfg(tmp.path().to_path_buf()))
            .unwrap();

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
        assert_eq!(
            s.series_label_values,
            lv_host("a"),
            "label map rebuilt from disk"
        );
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
        let p = idx
            .start_persistence(durable_cfg(tmp.path().to_path_buf()))
            .unwrap();

        // A Full snapshot early (end=100_000), then delta windows later.
        idx.append_sample(404, lv_host("a"), (70_000, 100_000), sample(1)); // Full base
        for i in 0..6u64 {
            let s = 100_000 + i * 30_000;
            idx.append_sample(
                404,
                lv_host("a"),
                (s, s + 30_000),
                delta_sample((i + 2) as u8),
            );
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
        let has_full_base = s.samples.iter().any(|(w_end, frames)| {
            *w_end < 200_000
                && frames.iter().any(|smp| {
                    matches!(
                        smp.encoding,
                        SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull
                    )
                })
        });
        assert!(
            has_full_base,
            "delta-only window did not get a disk-resident Full carry-in base: {:?}",
            s.samples
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|s| s.encoding).collect::<Vec<_>>()))
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

    #[test]
    fn catalog_recovery_keeps_legacy_and_foreign_generation_state_unbound() {
        use crate::storage_engines::sketch_db::index::persistence::metadata::{
            SidMetaRecord, SidMetadataStore,
        };
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let metadata = meta_with_policy(507, fingerprint);
        let record = SidMetaRecord::new(
            metadata.sid,
            metadata.metric_name.clone(),
            metadata.group_by_keys.iter().cloned().collect(),
            &metadata.agg_kind,
            0,
        );
        let tmp = tempfile::tempdir().unwrap();
        let sidecar = SidMetadataStore::new(tmp.path());
        sidecar.upsert_all(&[record.clone()]).unwrap();
        let store = SketchStore::new();
        store
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        assert_eq!(store.register_recovered_disk_series(tmp.path()), 0);
        assert!(store.instance(507).is_none());
        let mut foreign = record;
        foreign.summary_definition_id = Some(fingerprint.into());
        let reference = plan.summary_catalog.reference().unwrap();
        foreign.catalog_generation = Some(Arc::new(CatalogGeneration {
            schema_version: reference.schema_version,
            plan_id: reference.plan_id,
            plan_version: reference.plan_version + 1,
            snapshot_sha256: reference.snapshot_sha256,
        }));
        sidecar.upsert_all(&[foreign]).unwrap();
        assert_eq!(store.register_recovered_disk_series(tmp.path()), 0);
        assert!(store.series_ids_for_policy(fingerprint).is_empty());
    }

    #[test]
    fn catalog_reactivation_uses_new_physical_series_without_old_disk_payload() {
        use crate::drivers::ingest::series_resolver::SeriesIdResolver;
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let definition = fingerprint.into();
        let mut next_catalog = plan.summary_catalog.clone();
        next_catalog.plan_version += 1;
        let directory = tempfile::tempdir().unwrap();
        let disk = directory.path().join("state");
        let wal = directory.path().join("resolver.wal");
        let old_sid;
        let new_sid;
        {
            let resolver = SeriesIdResolver::open(wal.clone()).unwrap();
            let store = Arc::new(SketchStore::new());
            store
                .install_summary_catalog(Arc::new(plan.summary_catalog))
                .unwrap();
            let mut persistence = store.start_persistence(durable_cfg(disk.clone())).unwrap();
            old_sid = resolver.resolve("metric", "group", "family");
            store.register(meta_with_policy(old_sid, fingerprint));
            for pane in 0..4 {
                store.append_sample(
                    old_sid,
                    BTreeMap::new(),
                    (pane * 30_000, (pane + 1) * 30_000),
                    sample(1),
                );
            }
            assert!(wait_until(
                || !persistence.manifest.live_parts().is_empty(),
                Duration::from_secs(5)
            ));
            let old_parts = persistence.manifest.live_parts().len();
            store.remove_instance(old_sid).unwrap();
            assert!(resolver
                .resolve_with_reactivation("metric", "group", "family", |sid| store
                    .authorize_series_reactivation(sid, definition))
                .is_err());
            store
                .install_summary_catalog(Arc::new(next_catalog.clone()))
                .unwrap();
            new_sid = resolver
                .resolve_with_reactivation("metric", "group", "family", |sid| {
                    store.authorize_series_reactivation(sid, definition)
                })
                .unwrap();
            assert_ne!(new_sid, old_sid);
            store.register(meta_with_policy(new_sid, fingerprint));
            for pane in 0..4 {
                store.append_sample(
                    new_sid,
                    BTreeMap::new(),
                    (pane * 30_000, (pane + 1) * 30_000),
                    sample(2),
                );
            }
            assert!(wait_until(
                || persistence.manifest.live_parts().len() > old_parts,
                Duration::from_secs(5)
            ));
            persistence.shutdown();
        }
        let resolver = SeriesIdResolver::open(wal).unwrap();
        assert_eq!(resolver.resolve("metric", "group", "family"), new_sid);
        let recovered = Arc::new(SketchStore::new());
        recovered
            .install_summary_catalog(Arc::new(next_catalog))
            .unwrap();
        let _persistence = recovered.start_persistence(durable_cfg(disk)).unwrap();
        assert!(recovered.query_range(old_sid, 0, 90_000).is_empty());
        let rows = recovered.query_range(new_sid, 0, 90_000);
        assert!(!rows.is_empty());
        assert!(rows
            .iter()
            .flat_map(|row| row.samples.values())
            .flatten()
            .all(|sample| sample.bytes == vec![2]));
    }

    #[test]
    fn force_expire_never_extends_existing_lifecycle_deadlines() {
        let store = SketchStore::new();
        let mut metadata = meta(799);
        metadata.retired_at_ms = Some(1);
        metadata.expires_at_ms = Some(2);
        store.register(metadata);
        let expired = store.force_expire(799).unwrap();
        assert_eq!(expired.retired_at_ms, Some(1));
        assert_eq!(expired.expires_at_ms, Some(2));
    }

    #[test]
    fn completed_windows_reject_late_updates_after_restart() {
        // Completion is a storage admission rule, including legacy producers,
        // and survives restart without allowing a correction into consumed state.
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let directory = tempfile::tempdir().unwrap();
        let store = SketchStore::new();
        store
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        store.register(meta_with_policy(850, fingerprint));
        let generation = store.active_catalog_generation().unwrap();
        let writer = Arc::new(persistence::metadata::SidMetadataStore::new(
            directory.path(),
        ));
        *store.persistence_metadata.write().unwrap() = Some(writer.clone());
        let coordinate = asap_types::sds::SummaryInstanceCoordinates {
            summary_definition_id: fingerprint.into(),
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 30_000,
            },
            group_values: BTreeMap::new(),
        };
        let revision = store
            .admit_summary_updates(&generation, [coordinate.clone()].into())
            .unwrap();
        assert!(store.seal_finite_summary_input(&generation).is_err());
        store
            .publish_admitted_summary_update(
                &generation,
                &coordinate,
                revision,
                revision,
                120_000,
                |writer| {
                    writer
                        .append_sample(850, BTreeMap::new(), (0, 30_000), sample(1))
                        .then_some(850)
                },
            )
            .unwrap();
        let stale_record = store
            .metadata_record(&store.instances.read().unwrap()[&850])
            .unwrap();
        let before_failed_seal = store.summary_update_revision();
        std::fs::create_dir(writer.path()).unwrap();
        assert!(store.seal_finite_summary_input(&generation).is_err());
        assert!(store.summary_update_revision().matches(before_failed_seal));
        assert!(!store.completed_windows.read().unwrap().contains_key(&850));
        std::fs::remove_dir(writer.path()).unwrap();
        store.seal_finite_summary_input(&generation).unwrap();
        let producers = BTreeMap::from([(fingerprint.into(), "producer".to_string())]);
        let inventory = store
            .observed_summary_inventory("backend", "store", &producers, 1, 30_000)
            .unwrap();
        assert_eq!(inventory.instances.len(), 1);
        assert_eq!(
            inventory.instances.values().next().unwrap().completeness,
            InstanceCompleteness::Complete
        );
        assert!(!store.append_sample(850, BTreeMap::new(), (0, 30_000), sample(2)));
        assert!(!store.append_precompute(
            850,
            BTreeMap::new(),
            (0, 30_000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::new())
        ));
        // A flusher that captured metadata before completion cannot reopen it.
        writer.upsert_all(&[stale_record]).unwrap();
        assert_eq!(writer.load().unwrap()[0].completed_through_ms, Some(30_000));
        let restored = SketchStore::new();
        restored
            .install_summary_catalog(Arc::new(plan.summary_catalog))
            .unwrap();
        restored.register_recovered_disk_series(directory.path());
        assert!(!restored.append_sample(850, BTreeMap::new(), (0, 30_000), sample(3)));
        assert!(restored.append_sample(850, BTreeMap::new(), (30_000, 60_000), sample(4)));
    }

    #[test]
    fn finite_completion_flushes_payload_before_persisting_immutability() {
        // With neither memory pressure nor a hot-tier deadline, completion must
        // explicitly flush its payload before persisting a non-replayable window.
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let directory = tempfile::tempdir().unwrap();
        {
            let store = Arc::new(SketchStore::new());
            store
                .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
                .unwrap();
            store.register(meta_with_policy(851, fingerprint));
            let mut config = durable_cfg(directory.path().to_path_buf());
            config.hot_window_ms = None;
            config.seal_window_count = 100;
            let mut persistence = store.start_persistence(config).unwrap();
            let generation = store.active_catalog_generation().unwrap();
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                summary_definition_id: fingerprint.into(),
                time_range: HalfOpenTimeRange {
                    start_ms: 0,
                    end_ms: 30_000,
                },
                group_values: BTreeMap::new(),
            };
            let revision = store
                .admit_summary_updates(&generation, [coordinate.clone()].into())
                .unwrap();
            store
                .publish_admitted_summary_update(
                    &generation,
                    &coordinate,
                    revision,
                    revision,
                    120_000,
                    |writer| {
                        writer
                            .append_sample(851, BTreeMap::new(), (0, 30_000), sample(1))
                            .then_some(851)
                    },
                )
                .unwrap();
            assert!(!store.seal_finite_summary_input(&generation).unwrap());
            assert!(!store.completed_windows.read().unwrap().contains_key(&851));
            let checkpoint = directory.path().join("finite_input_generation.json");
            std::fs::create_dir(&checkpoint).unwrap();
            assert!(wait_until(
                || store.seal_finite_summary_input(&generation).is_err(),
                Duration::from_secs(5)
            ));
            assert!(store.admission.read().unwrap().is_finite_closed());
            assert!(!store.admission.read().unwrap().is_finite_complete());
            assert!(!store.append_sample(851, BTreeMap::new(), (30_000, 60_000), sample(2)));
            std::fs::remove_dir(&checkpoint).unwrap();
            assert!(store.seal_finite_summary_input(&generation).unwrap());
            assert!(!persistence.manifest.live_parts().is_empty());
            persistence.shutdown();
        }
        let restored = Arc::new(SketchStore::new());
        restored
            .install_summary_catalog(Arc::new(plan.summary_catalog))
            .unwrap();
        let _persistence = restored
            .start_persistence(durable_cfg(directory.path().to_path_buf()))
            .unwrap();
        assert!(!restored.append_sample(851, BTreeMap::new(), (0, 30_000), sample(2)));
        // Finite closure also rejects a new future window and a newly arriving
        // physical series after restart; per-window frontiers alone cannot.
        assert!(!restored.append_sample(851, BTreeMap::new(), (30_000, 60_000), sample(2)));
        restored.register(meta(852));
        assert!(!restored.append_sample(852, BTreeMap::new(), (30_000, 60_000), sample(2)));
        assert!(restored
            .publish_unadmitted_summary_update(|_| panic!("closed callback ran"))
            .is_none());
        let rows = restored.query_range(851, 0, 30_000);
        assert_eq!(rows.len(), 1);
        let payloads: Vec<_> = rows
            .iter()
            .flat_map(|row| row.samples.values())
            .flatten()
            .collect();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].bytes, vec![1]);
    }

    #[test]
    fn failed_durable_lifecycle_write_preserves_live_instance() {
        let store = SketchStore::new();
        store.register(meta(800));
        let directory = tempfile::tempdir().unwrap();
        let writer = Arc::new(persistence::metadata::SidMetadataStore::new(
            directory.path(),
        ));
        // A directory in place of the sidecar causes the real writer to fail.
        std::fs::create_dir(writer.path()).unwrap();
        *store.persistence_metadata.write().unwrap() = Some(writer);
        assert!(store.force_retire(800, Duration::from_secs(60)).is_none());
        assert!(store.force_expire(800).is_none());
        assert!(store.remove_instance(800).is_none());
        let instance = store.instance(800).unwrap();
        assert!(instance.retired_at_ms.is_none());
        assert!(instance.expires_at_ms.is_none());
    }

    #[test]
    fn durable_lifecycle_is_not_resurrected_by_restart_or_a_stale_flush() {
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let directory = tempfile::tempdir().unwrap();
        let disk = directory.path().to_path_buf();
        let expected_retirement;
        {
            let store = Arc::new(SketchStore::new());
            store
                .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
                .unwrap();
            for sid in [801, 802, 803] {
                store.register(meta_with_policy(sid, fingerprint));
            }
            let mut persistence = store.start_persistence(durable_cfg(disk.clone())).unwrap();
            for sid in [801, 802, 803] {
                for pane in 0..4 {
                    store.append_sample(
                        sid,
                        BTreeMap::new(),
                        (pane * 30_000, (pane + 1) * 30_000),
                        sample(1),
                    );
                }
            }
            assert!(wait_until(
                || !persistence.manifest.live_parts().is_empty(),
                Duration::from_secs(5)
            ));
            let stale: Vec<_> = {
                let instances = store.instances.read().unwrap();
                [801, 802, 803]
                    .iter()
                    .map(|sid| store.metadata_record(&instances[sid]).unwrap())
                    .collect()
            };
            expected_retirement = store.force_retire(801, Duration::from_secs(3600)).unwrap();
            assert!(store.force_expire(802).is_some());
            assert!(store.remove_instance(803).is_some());
            store.register(meta_with_policy(803, fingerprint));
            assert!(
                store.instance(803).is_none(),
                "removed SID reused before restart"
            );
            // This models a flush that captured metadata before the lifecycle
            // operation and reaches the shared writer afterward.
            persistence
                .flusher
                .metadata_store()
                .upsert_all(&stale)
                .unwrap();
            persistence.shutdown();
        }
        let recovered = Arc::new(SketchStore::new());
        recovered
            .install_summary_catalog(Arc::new(plan.summary_catalog))
            .unwrap();
        let _persistence = recovered.start_persistence(durable_cfg(disk)).unwrap();
        let retired = recovered.instance(801).unwrap();
        assert_eq!(retired.retired_at_ms, expected_retirement.retired_at_ms);
        assert_eq!(retired.expires_at_ms, expected_retirement.expires_at_ms);
        assert!(
            recovered.instance(802).is_none(),
            "expired state resurrected"
        );
        assert!(
            recovered.instance(803).is_none(),
            "removed state resurrected"
        );
        recovered.register(meta_with_policy(803, fingerprint));
        assert!(
            recovered.instance(803).is_none(),
            "removed SID reused after restart"
        );
    }

    #[test]
    fn observed_inventory_includes_durable_instances_after_restart() {
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
        let fingerprint = plan.precompute_plan.materializations[0].policy_fingerprint();
        let definition_id = SummaryDefinitionId::from(fingerprint);
        let producers = BTreeMap::from([(definition_id, "producer-a".to_string())]);
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = tmp.path().to_path_buf();
        let mut metadata = meta_with_policy(506, fingerprint);
        metadata.group_by_keys = ["job".to_string()].into_iter().collect();

        {
            let store = Arc::new(SketchStore::new());
            store
                .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
                .unwrap();
            store.register(metadata.clone());
            let mut persistence = store.start_persistence(durable_cfg(disk.clone())).unwrap();
            for index in 0..4u64 {
                let start = index * 30_000;
                store.append_sample(
                    506,
                    BTreeMap::from([("job".to_string(), "api".to_string())]),
                    (start, start + 30_000),
                    sample((index + 1) as u8),
                );
            }
            assert!(wait_until(
                || !persistence.manifest.live_parts().is_empty(),
                std::time::Duration::from_secs(5)
            ));
            persistence.shutdown();
        }

        let recovered = Arc::new(SketchStore::new());
        recovered
            .install_summary_catalog(Arc::new(plan.summary_catalog))
            .unwrap();
        let persistence = recovered.start_persistence(durable_cfg(disk)).unwrap();
        assert!(!persistence.manifest.live_parts().is_empty());
        assert_eq!(recovered.series_ids_for_policy(fingerprint), vec![506]);
        assert!(!recovered.query_range(506, 0, 120_000).is_empty());
        let inventory = recovered
            .observed_summary_inventory("backend-a", "store-a", &producers, 1, 100)
            .unwrap();
        assert!(!inventory.instances.is_empty());
        assert!(inventory.instances.values().all(|instance| {
            instance.group_values.get("job").map(String::as_str) == Some("api")
                && instance.state_reference.key.starts_with("series:506:pane:")
        }));
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
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::Kll))),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::Kll,
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
            Some(Capability::QuantileApprox(Some(SketchAlgorithm::Kll)))
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
            AggKind::ExactAgg {
                agg_type: AggregationType::Sum,
                ..
            }
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
        assert_eq!(
            series.len(),
            1,
            "recovered data not queryable after restart"
        );
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
        let p = idx
            .start_persistence(durable_cfg(tmp.path().to_path_buf()))
            .unwrap();

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
                Box::new(
                    crate::precompute_engine::operators::SumAccumulator::with_sum((i + 1) as f64),
                ),
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

    // Raw sample counts must survive production durable storage.
    #[test]
    fn raw_count_survives_disk_eviction() {
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
        let p = idx
            .start_persistence(durable_cfg(tmp.path().to_path_buf()))
            .unwrap();

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
                Box::new({
                    let mut acc = crate::precompute_engine::operators::SumAccumulator::new();
                    acc.update((i + 1) as f64);
                    acc.update(10.0);
                    acc
                }),
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
            "exact-agg query returned no result after flush and eviction"
        );
        let (_label, samples) = &series[0];
        assert!(
            samples.contains_key(&30_000),
            "evicted exact-agg window missing from disk"
        );
        let stats = samples[&30_000].aux_stats();
        assert_eq!(stats.count, Some(2));
        assert_eq!(stats.sum, Some(11.0));
        assert_eq!(stats.sum.unwrap() / stats.count.unwrap() as f64, 5.5);
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

    /// The idle under-report this fix targets: once a sid's payload has
    /// been flushed/evicted to disk, `approx_memory_bytes` (the flusher's
    /// evictable gauge) reads 0 — but the registered sid still costs
    /// resident registry/metadata memory. `approx_resident_bytes` must
    /// surface that so the memory diagnostic isn't blind (the live
    /// "0.00 KB while 600 MB RSS" symptom).
    #[test]
    fn approx_resident_bytes_counts_registry_when_payload_is_zero() {
        let idx = SketchStore::new();
        idx.register(meta_with_host_key(9001));
        // No samples appended → no live payload (models the idle sid whose
        // epochs were sealed+flushed to disk).
        assert_eq!(
            idx.approx_memory_bytes(),
            0,
            "precondition: no resident payload"
        );
        assert!(
            idx.approx_resident_bytes() > 0,
            "approx_resident_bytes must account for the registered sid's \
             metadata even when no payload is resident"
        );
    }

    /// Resident accounting must include the per-series intern-table label
    /// cache, which grows with the number of distinct label-value maps a
    /// sid has seen — the dominant per-sid resident cost at scale.
    #[test]
    fn approx_resident_bytes_grows_with_interned_label_cardinality() {
        let idx = SketchStore::new();
        idx.register(meta_with_host_key(9100));
        for i in 0..50u64 {
            let s = i * 30_000;
            idx.append_sample(
                9100,
                lv_host(&format!("host-{i}")),
                (s, s + 30_000),
                sample((i + 1) as u8),
            );
        }
        let many = idx.approx_resident_bytes();

        let idx2 = SketchStore::new();
        idx2.register(meta_with_host_key(9101));
        idx2.append_sample(9101, lv_host("host-0"), (0, 30_000), sample(1));
        let few = idx2.approx_resident_bytes();

        assert!(
            many > few,
            "resident bytes should grow with interned label cardinality: \
             many={many} few={few}"
        );
    }

    #[test]
    fn is_idle_evictable_predicate() {
        let now = now_ms();
        let mut d = SidStoreData::<BTreeMap<String, String>, AggPayload>::new();
        d.persistence_enabled = true;
        d.last_write_unix_ms = now.saturating_sub(120_000);
        assert!(
            SketchStore::is_idle_evictable(&d, now, 60_000),
            "idle 120s past a 60s threshold, durable + empty → evictable"
        );
        assert!(
            !SketchStore::is_idle_evictable(&d, now, 300_000),
            "idle 120s under a 300s threshold → spared"
        );
        d.last_write_unix_ms = 0;
        assert!(
            !SketchStore::is_idle_evictable(&d, now, 1),
            "never-written (0) is never evictable"
        );
        // In-memory-only (no persistence) sids are never idle-evicted — there
        // is no durable copy to serve them from.
        let mut d2 = SidStoreData::<BTreeMap<String, String>, AggPayload>::new();
        d2.persistence_enabled = false;
        d2.last_write_unix_ms = now.saturating_sub(120_000);
        assert!(!SketchStore::is_idle_evictable(&d2, now, 1));
    }

    #[test]
    fn evict_idle_series_drops_state_but_keeps_metadata() {
        let idx = SketchStore::new();
        idx.register(meta_with_host_key(7001));
        // Durable, write-idle, empty-in-memory sid (models a series whose
        // windows have all sealed+flushed to disk and then gone quiet).
        let mut d = SidStoreData::<BTreeMap<String, String>, AggPayload>::new();
        d.persistence_enabled = true;
        d.last_write_unix_ms = now_ms().saturating_sub(120_000);
        idx.series.insert(7001, Arc::new(RwLock::new(d)));
        assert_eq!(idx.series.len(), 1);

        let evicted = idx.evict_idle_series(60_000);
        assert_eq!(evicted, 1, "the idle sid is evicted");
        assert_eq!(idx.series.len(), 0, "in-memory state dropped");
        assert!(
            idx.instances.read().unwrap().contains_key(&7001),
            "metadata retained → series stays queryable from disk + rehydrates"
        );

        // A subsequent append rehydrates the series entry transparently.
        idx.append_sample(7001, lv_host("h"), (0, 30_000), sample(1));
        assert_eq!(idx.series.len(), 1, "append rehydrated the evicted sid");
    }

    #[test]
    fn evict_idle_series_spares_recent_and_pending_sids() {
        let idx = SketchStore::new();
        // Recently written → not idle.
        idx.register(meta_with_host_key(7101));
        let mut recent = SidStoreData::<BTreeMap<String, String>, AggPayload>::new();
        recent.persistence_enabled = true;
        recent.last_write_unix_ms = now_ms();
        idx.series.insert(7101, Arc::new(RwLock::new(recent)));
        // Idle, but still holds un-flushed data in current_epoch → dropping it
        // would lose data, so it MUST be spared.
        idx.register(meta_with_host_key(7102));
        let mut pending = SidStoreData::<BTreeMap<String, String>, AggPayload>::new();
        pending.persistence_enabled = true;
        pending.last_write_unix_ms = now_ms().saturating_sub(120_000);
        pending.insert((0, 30_000), lv_host("h"), AggPayload::Sketch(sample(1)));
        idx.series.insert(7102, Arc::new(RwLock::new(pending)));

        assert_eq!(
            idx.evict_idle_series(60_000),
            0,
            "recent + pending-data sids are spared"
        );
        assert_eq!(idx.series.len(), 2);
    }
}

// 2026-05 reorg: generic epoch-partitioned columnar storage lives
// alongside the store that uses it.
mod admission;
mod maintenance;
pub(crate) use maintenance::FrozenExactWindows;
pub mod epoch_columnar;

// `persistence` moved up to `sketch_db::persistence`. Re-exported here
// so legacy `crate::storage_engines::sketch_db::index::persistence::*`
// paths continue working without consumer changes.
pub use crate::storage_engines::sketch_db::persistence;
