//! Sid-level `timeline_for_metric` — schema retirement #1.
//!
//! Produces the same `Vec<TimelineSegment>` shape as
//! [`SchemaRegistry::timeline_for_metric`](crate::storage_engines::sketch_db::schema::SchemaRegistry::timeline_for_metric)
//! but reads exclusively from the sid catalog
//! ([`SketchStore::instances`](crate::storage_engines::sketch_db::index::SketchStore)).
//! Lets the schema retirement remove the `SchemaRegistry`-keyed
//! historical timeline without losing the cross-reconfigure query
//! routing primitive.
//!
//! ## Algorithm
//!
//! 1. Snapshot all `SketchInstanceMetadata` for `metric` from the
//!    sid catalog.
//! 2. Group by content signature: `(agg_kind, group_by_keys)`. Each
//!    group corresponds to one logical agg-config (multiple sids of
//!    the same config share this signature; they differ only in
//!    attrs values).
//! 3. For each group, fold the per-sid lifecycle fields into one
//!    per-group representative:
//!    - `start_ms` = `min(first_seen_unix_ms)` across the group
//!    - `retired_at_ms` = `Some(min(retired_at_ms))` iff every sid
//!      in the group is retired, else `None`
//!    - `status` = same fold (group is Active if any sid is Active;
//!      Retired if all retired and none expired; Expired otherwise)
//! 4. Apply the same segmenting logic as `SchemaRegistry`:
//!    sort by start_ms, compute `own_end` = `min(next.start_ms,
//!    self.retired_at_ms, u64::MAX)`, clip to `[t1_ms, t2_ms]`.
//!
//! ## `agg_id` field — what we put in it
//!
//! `TimelineSegment.agg_id: u64` made sense when timelines were
//! produced from a `SchemaRegistry` of agg-configs. In the sid model
//! we don't have a single agg_id per config — we have an
//! agg-signature (the content tuple `(metric, agg_kind, group_by_keys)`).
//! For schema-retirement back-compat with HTTP consumers, we surface
//! a deterministic 64-bit hash of the signature in that field — same
//! kind of stable-across-restarts content-derived id that PR #151
//! used for `compute_agg_config_id`.

use std::collections::{BTreeMap, BTreeSet};

use xxhash_rust::xxh64::xxh64;

use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig, SketchKindHandle};
use crate::storage_engines::sketch_db::index::{SketchInstanceMetadata, SketchStore};
use crate::storage_engines::sketch_db::schema::{AggStatus, TimelineCoverage, TimelineSegment};

/// Produce the per-metric timeline of `TimelineSegment`s entirely from
/// the sid catalog, with no `SchemaRegistry` lookup. See module-level
/// doc for the algorithm.
///
/// O(N log N + N) where N = instances matching `metric`. Production
/// scales: dozens of metrics × tens of sids each → microseconds. The
/// HTTP `/api/v1/db/timeline` endpoint calls this once per request.
pub fn timeline_for_metric(
    store: &SketchStore,
    metric: &str,
    t1_ms: u64,
    t2_ms: u64,
) -> Vec<TimelineSegment> {
    if t1_ms > t2_ms {
        return Vec::new();
    }

    // 1. Snapshot all instances matching `metric` from the sid
    //    catalog. `list_by_status(Active)` would miss retired+
    //    expired sids; we want them all. Iterate the full catalog
    //    once.
    let all = store.snapshot_instances();

    // 2. Group by content signature.
    let mut groups: BTreeMap<SignatureKey, AggSignatureGroup> = BTreeMap::new();
    for meta in all.into_iter().filter(|m| m.metric_name == metric) {
        let key = signature_key(&meta);
        groups.entry(key).or_default().fold_in(&meta);
    }

    // 3 + 4. Sort groups by start, compute per-group `own_end`, clip
    //        to query range, build segments.
    let mut entries: Vec<AggSignatureGroup> = groups.into_values().collect();
    entries.sort_by_key(|g| (g.start_ms, g.signature_id));

    let mut segments = Vec::with_capacity(entries.len());
    for (i, group) in entries.iter().enumerate() {
        let successor_start = entries.get(i + 1).map(|n| n.start_ms);
        let own_end = match (successor_start, group.retired_at_ms) {
            (Some(s), Some(r)) => s.min(r),
            (Some(s), None) => s,
            (None, Some(r)) => r,
            (None, None) => u64::MAX,
        };
        let clipped_start = group.start_ms.max(t1_ms);
        let clipped_end = own_end.min(t2_ms.saturating_add(1));
        if clipped_start >= clipped_end {
            continue;
        }
        segments.push(TimelineSegment {
            agg_id: group.signature_id,
            start_ms: clipped_start,
            end_ms: clipped_end,
            status: group.status,
            coverage: match group.status {
                AggStatus::Expired => TimelineCoverage::Purged,
                _ => TimelineCoverage::Sketch,
            },
        });
    }
    segments
}

/// Per-(agg_kind, group_by_keys) content signature. Two sids with the
/// same signature represent the same agg-config; they differ only in
/// their attrs values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SignatureKey {
    /// Stable encoded form of the (agg_kind, group_by_keys) tuple.
    encoded: Vec<u8>,
}

fn signature_key(meta: &SketchInstanceMetadata) -> SignatureKey {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(meta.metric_name.as_bytes());
    buf.push(0);
    encode_agg_kind(&meta.agg_kind, &mut buf);
    buf.push(0);
    for k in &meta.group_by_keys {
        buf.extend_from_slice(k.as_bytes());
        buf.push(b',');
    }
    SignatureKey { encoded: buf }
}

/// Per-group fold of all sids sharing one signature.
#[derive(Debug)]
struct AggSignatureGroup {
    signature_id: u64,
    /// Earliest `first_seen_unix_ms` across all sids in the group.
    start_ms: u64,
    /// `Some(min)` iff every sid is retired; `None` if any sid is
    /// still Active.
    retired_at_ms: Option<u64>,
    /// Folded lifecycle status — Active if any sid is Active; else
    /// Retired if all retired and no sid is expired; else Expired.
    status: AggStatus,
}

impl Default for AggSignatureGroup {
    fn default() -> Self {
        Self {
            signature_id: 0,
            start_ms: u64::MAX,
            retired_at_ms: Some(u64::MAX),
            status: AggStatus::Expired, // upgraded by `fold_in`
        }
    }
}

impl AggSignatureGroup {
    fn fold_in(&mut self, meta: &SketchInstanceMetadata) {
        // Stable signature id: xxh64 over the same canonical encoding
        // `signature_key` uses. Computed lazily on first fold-in;
        // every sid in the group produces the same hash.
        if self.signature_id == 0 {
            let key = signature_key(meta);
            self.signature_id = xxh64(&key.encoded, 0).max(1);
        }
        let seen = meta.first_seen_unix_ms.max(0) as u64;
        if seen < self.start_ms {
            self.start_ms = seen;
        }
        // Retire fold: keep Some(min) only if EVERY sid is retired.
        match (self.retired_at_ms, meta.retired_at_ms) {
            (Some(prev), Some(r)) => self.retired_at_ms = Some(prev.min(r)),
            _ => self.retired_at_ms = None,
        }
        // Status fold: Active wins over Retired wins over Expired.
        let s = meta.status();
        self.status = match (self.status, s) {
            (_, AggStatus::Active) | (AggStatus::Active, _) => AggStatus::Active,
            (_, AggStatus::Retired) | (AggStatus::Retired, _) => AggStatus::Retired,
            _ => AggStatus::Expired,
        };
    }
}

fn encode_agg_kind(agg_kind: &AggKind, buf: &mut Vec<u8>) {
    match agg_kind {
        AggKind::Sketch { kind, config } => {
            buf.push(b'S');
            buf.push(sketch_kind_byte(*kind));
            encode_sketch_config(config, buf);
        }
        AggKind::Precompute {
            agg_type,
            parameters_canonical,
        } => {
            buf.push(b'P');
            buf.extend_from_slice(agg_type.as_str().as_bytes());
            buf.push(b';');
            buf.extend_from_slice(parameters_canonical.as_bytes());
        }
    }
}

fn sketch_kind_byte(k: SketchKindHandle) -> u8 {
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

// Silence unused-import warning on `BTreeSet` if the fold paths above
// stop using it directly (kept for forward-compat with richer signature
// encoding).
const _: fn() = || {
    let _set: BTreeSet<String> = BTreeSet::new();
    let _ = _set;
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::data::{AggregationType, Capability};

    fn meta(
        sid: u64,
        metric: &str,
        agg_kind: AggKind,
        first_seen: i64,
        retired: Option<u64>,
        expires: Option<u64>,
    ) -> SketchInstanceMetadata {
        SketchInstanceMetadata {
            sid,
            metric_name: metric.into(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::DDSketch)),
            agg_kind,
            accuracy: None,
            first_seen_unix_ms: first_seen,
            retired_at_ms: retired,
            expires_at_ms: expires,
        }
    }

    fn precompute(agg_type: AggregationType) -> AggKind {
        AggKind::Precompute {
            agg_type,
            parameters_canonical: String::new(),
        }
    }

    #[test]
    fn empty_store_yields_empty_timeline() {
        let store = SketchStore::new();
        let segs = timeline_for_metric(&store, "m", 0, 1000);
        assert!(segs.is_empty());
    }

    #[test]
    fn t1_greater_than_t2_yields_empty() {
        let store = SketchStore::new();
        store.register(meta(1, "m", precompute(AggregationType::Sum), 100, None, None));
        let segs = timeline_for_metric(&store, "m", 1000, 500);
        assert!(segs.is_empty());
    }

    #[test]
    fn single_active_signature_produces_one_segment_clipped_to_range() {
        let store = SketchStore::new();
        store.register(meta(
            1,
            "m",
            precompute(AggregationType::Sum),
            100,
            None,
            None,
        ));
        let segs = timeline_for_metric(&store, "m", 50, 500);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].start_ms, 100, "clipped to instance's first_seen");
        assert_eq!(segs[0].end_ms, 501, "open-ended → t2 inclusive");
        assert_eq!(segs[0].status, AggStatus::Active);
        assert_eq!(segs[0].coverage, TimelineCoverage::Sketch);
    }

    #[test]
    fn two_signatures_in_sequence_produce_two_segments() {
        let store = SketchStore::new();
        // sig_a (Sum) active from 100; sig_b (Increase) active from 500.
        store.register(meta(
            1,
            "m",
            precompute(AggregationType::Sum),
            100,
            Some(500),
            None,
        ));
        store.register(meta(
            2,
            "m",
            precompute(AggregationType::Increase),
            500,
            None,
            None,
        ));
        let segs = timeline_for_metric(&store, "m", 0, 1000);
        assert_eq!(segs.len(), 2);
        // First segment: sig_a, [100, 500)
        assert_eq!(segs[0].start_ms, 100);
        assert_eq!(segs[0].end_ms, 500);
        assert_eq!(segs[0].status, AggStatus::Retired);
        // Second segment: sig_b, [500, 1001)
        assert_eq!(segs[1].start_ms, 500);
        assert_eq!(segs[1].end_ms, 1001);
        assert_eq!(segs[1].status, AggStatus::Active);
    }

    #[test]
    fn multiple_sids_same_signature_fold_into_one_segment() {
        let store = SketchStore::new();
        // Three sids of the same config, with different first_seen
        // (because their data points arrived at different times).
        store.register(meta(
            1,
            "m",
            precompute(AggregationType::Sum),
            200,
            None,
            None,
        ));
        store.register(meta(
            2,
            "m",
            precompute(AggregationType::Sum),
            300,
            None,
            None,
        ));
        store.register(meta(
            3,
            "m",
            precompute(AggregationType::Sum),
            150,
            None,
            None,
        ));
        let segs = timeline_for_metric(&store, "m", 0, 1000);
        assert_eq!(segs.len(), 1, "all 3 sids fold into one segment");
        assert_eq!(segs[0].start_ms, 150, "earliest first_seen wins");
        assert_eq!(segs[0].status, AggStatus::Active);
    }

    #[test]
    fn distinct_metrics_are_isolated() {
        let store = SketchStore::new();
        store.register(meta(
            1,
            "cpu",
            precompute(AggregationType::Sum),
            100,
            None,
            None,
        ));
        store.register(meta(
            2,
            "mem",
            precompute(AggregationType::Sum),
            200,
            None,
            None,
        ));
        let cpu = timeline_for_metric(&store, "cpu", 0, 1000);
        let mem = timeline_for_metric(&store, "mem", 0, 1000);
        assert_eq!(cpu.len(), 1);
        assert_eq!(mem.len(), 1);
        assert_ne!(cpu[0].agg_id, mem[0].agg_id);
    }

    #[test]
    fn signature_id_is_deterministic() {
        let store_a = SketchStore::new();
        let store_b = SketchStore::new();
        let m_a = meta(1, "m", precompute(AggregationType::Sum), 100, None, None);
        let m_b = meta(42, "m", precompute(AggregationType::Sum), 100, None, None);
        store_a.register(m_a);
        store_b.register(m_b);
        let a = timeline_for_metric(&store_a, "m", 0, 1000);
        let b = timeline_for_metric(&store_b, "m", 0, 1000);
        assert_eq!(
            a[0].agg_id, b[0].agg_id,
            "signature id depends on content, not on the specific sid"
        );
    }
}
