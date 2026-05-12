//! Epoch-partitioned columnar storage — generic payload type.
//!
//! Lifted from `simple_map_store::common` (legacy SimpleMapStore index)
//! with the payload column type made generic so the new SketchIndex
//! (Phase 5) can reuse the legacy's six storage optimizations
//! (`INDEX_DESIGN.md`) without dragging in `Arc<dyn AggregateCore>`
//! dynamic dispatch.
//!
//! # Optimizations carried over from legacy
//!
//! | Opt | What |
//! |-----|------|
//! | 1 | Lazy `window_to_ids` index — built on first exact query, invalidated cheaply on insert |
//! | 2 | Offset-based index — stores `u32` column offsets, not payload clones |
//! | 3 | Monotonic ingest fast path — skip `HashSet` probe for consecutive same-window inserts |
//! | 4 | Batch metadata hoisting — caller responsibility (the OTLP receive path groups DPs by sid) |
//! | 5 | Columnar storage — three parallel arrays; range scan touches only `windows_col` |
//! | 6 | Pre-allocated epoch buffers on rotation |
//!
//! # Differences from legacy
//!
//! - **Generic payload type**: `MutableEpoch<P>` instead of
//!   `Vec<Arc<dyn AggregateCore>>`. The new SketchIndex stores
//!   `SketchSampleState` directly (typed bytes + encoding tag) — no
//!   dyn dispatch, no Arc cloning, payload moves into the column.
//! - **Series-values keyed via `LabelValuesId = u32`** (renamed from
//!   legacy `MetricID = u32`). The intern table maps the per-series
//!   group-by VALUES vector to a compact ID, since the SketchIndex's
//!   sid already captures the metric identity at the level above.
//!
//! See INDEX_DESIGN.md in `simple_map_store/` for the full complexity
//! analysis (Insert O(1), range query O(M) mutable / O(log N + k)
//! sealed, etc.) — those bounds carry over verbatim because the
//! algorithmic structure is unchanged.

use std::collections::{BTreeMap, HashMap, HashSet};

/// Compact identifier for an interned label-values vector. 4 bytes —
/// hashing/comparing this in the hot loop is one CPU instruction
/// instead of a `BTreeMap<String, String>` walk.
pub type LabelValuesId = u32;

/// Monotonically increasing epoch counter.
pub type EpochId = u64;

/// `(start_unix_ms, end_unix_ms)`.
pub type TimestampRange = (u64, u64);

/// Intern table for per-series group-by VALUES vectors.
///
/// Each `SidStoreData` owns one InternTable. The first time a series's
/// label-values vector is seen, it gets assigned a `LabelValuesId`
/// (`u32`); all subsequent appearances re-use the existing id. Hot-path
/// columnar arrays carry only the `u32` ids — the full label-values
/// vectors live once in the intern table and are resolved on query.
///
/// Bumped from legacy's `Option<KeyByLabelValues>` key to
/// `BTreeMap<String,String>` because the new SketchIndex receives
/// canonicalized group-by attributes from the OTLP DataPoint (sorted
/// by key already at the receive layer).
pub struct InternTable<K: Eq + std::hash::Hash + Clone> {
    label_to_id: HashMap<K, LabelValuesId>,
    id_to_label: Vec<K>,
}

impl<K: Eq + std::hash::Hash + Clone> InternTable<K> {
    pub fn new() -> Self {
        Self {
            label_to_id: HashMap::new(),
            id_to_label: Vec::new(),
        }
    }

    /// Intern a key, assigning a new `LabelValuesId` if first seen.
    /// Uses `HashMap::entry` to avoid double-hashing.
    pub fn intern(&mut self, key: K) -> LabelValuesId {
        let next_id = self.id_to_label.len() as LabelValuesId;
        match self.label_to_id.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => *e.get(),
            std::collections::hash_map::Entry::Vacant(e) => {
                self.id_to_label.push(e.key().clone());
                *e.insert(next_id)
            }
        }
    }

    /// O(1) resolution by id.
    pub fn resolve(&self, id: LabelValuesId) -> Option<&K> {
        self.id_to_label.get(id as usize)
    }

    pub fn len(&self) -> usize {
        self.id_to_label.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_label.is_empty()
    }
}

impl<K: Eq + std::hash::Hash + Clone> Default for InternTable<K> {
    fn default() -> Self {
        Self::new()
    }
}

/// Active (mutable) epoch: append-only insert, O(1) amortized.
///
/// Three parallel columns (Opt 5) — windows / label-id / payload —
/// keep the range-scan hot loop hitting only `windows_col`. The
/// payload column is owned (no `Arc` indirection); legacy used
/// `Arc<dyn AggregateCore>` because aggregation type was variable,
/// but the SketchIndex specializes to `SketchSampleState` (typed
/// bytes + encoding tag).
pub struct MutableEpoch<P> {
    // Columnar storage: three parallel arrays (Opt 5)
    windows_col: Vec<TimestampRange>,
    label_ids_col: Vec<LabelValuesId>,
    payloads_col: Vec<P>,

    // Distinct-window count for epoch rotation threshold.
    windows_set: HashSet<TimestampRange>,

    // Monotonic ingest fast path: skip windows_set probe for consecutive
    // same-window inserts — the common case in ordered ingestion (Opt 3).
    last_window: Option<TimestampRange>,

    // Lazy offset index — Some(_) after the first exact_query;
    // invalidated to None on any insert (one pointer-width write).
    // Stores `Vec<u32>` column offsets, not payload clones (Opt 1 + 2).
    window_to_ids: Option<HashMap<TimestampRange, Vec<u32>>>,

    // Epoch time bounds for O(1) skip-check, updated incrementally on insert.
    min_start: Option<u64>,
    max_end: Option<u64>,
}

impl<P> MutableEpoch<P> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Pre-allocate column buffers with a capacity hint (Opt 6).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            windows_col: Vec::with_capacity(cap),
            label_ids_col: Vec::with_capacity(cap),
            payloads_col: Vec::with_capacity(cap),
            windows_set: HashSet::new(),
            last_window: None,
            window_to_ids: None,
            min_start: None,
            max_end: None,
        }
    }

    pub fn window_count(&self) -> usize {
        self.windows_set.len()
    }

    pub fn len(&self) -> usize {
        self.windows_col.len()
    }

    pub fn is_empty(&self) -> bool {
        self.windows_col.is_empty()
    }

    pub fn min_start(&self) -> Option<u64> {
        self.min_start
    }

    pub fn max_end(&self) -> Option<u64> {
        self.max_end
    }

    /// Append-only insert. O(1) amortized.
    ///
    /// Hot path: three `Vec::push` calls + conditional `windows_set` probe
    /// (skipped by Opt 3 on consecutive same-window inserts) + invalidation
    /// of `window_to_ids` to `None` (single pointer-width write).
    pub fn insert(&mut self, window: TimestampRange, label_id: LabelValuesId, payload: P) {
        // Opt 3: monotonic same-window fast path.
        if self.last_window != Some(window) {
            self.windows_set.insert(window);
            self.last_window = Some(window);
        }

        self.windows_col.push(window);
        self.label_ids_col.push(label_id);
        self.payloads_col.push(payload);

        // Opt 1: invalidate the lazy index — single pointer-width write.
        self.window_to_ids = None;

        // Update epoch bounds incrementally for O(1) range-skip check.
        match self.min_start {
            Some(s) if s <= window.0 => {}
            _ => self.min_start = Some(window.0),
        }
        match self.max_end {
            Some(e) if e >= window.1 => {}
            _ => self.max_end = Some(window.1),
        }
    }

    /// Build (or rebuild) the lazy `window_to_ids` index from `windows_col` —
    /// O(M) one-pass scan, called on the first exact query after any insert.
    fn ensure_window_index(&mut self) -> &HashMap<TimestampRange, Vec<u32>> {
        if self.window_to_ids.is_none() {
            let mut idx = HashMap::with_capacity(self.windows_set.len());
            for (i, w) in self.windows_col.iter().enumerate() {
                idx.entry(*w).or_insert_with(Vec::new).push(i as u32);
            }
            self.window_to_ids = Some(idx);
        }
        self.window_to_ids.as_ref().unwrap()
    }

    /// Return all entries whose window matches `target` exactly.
    /// O(M) on first call after a write (builds the index); O(m) cached.
    pub fn exact_query(&mut self, target: TimestampRange) -> Vec<(LabelValuesId, &P)> {
        let idx = self.ensure_window_index();
        let offsets = match idx.get(&target) {
            Some(v) => v.clone(),
            None => return Vec::new(),
        };
        offsets
            .into_iter()
            .map(|off| {
                let i = off as usize;
                (self.label_ids_col[i], &self.payloads_col[i])
            })
            .collect()
    }

    /// Range query into a caller-provided buffer. Hot loop touches only
    /// `windows_col` (Opt 5) — chase the payload pointer only on a hit.
    /// O(M) — linear scan; bounded by the epoch size.
    pub fn range_query_into<'a>(
        &'a self,
        start: u64,
        end: u64,
        out: &mut Vec<(TimestampRange, LabelValuesId, &'a P)>,
    ) {
        // O(1) skip if the epoch's bounds don't overlap the query range.
        if let (Some(min_s), Some(max_e)) = (self.min_start, self.max_end) {
            if min_s > end || max_e < start {
                return;
            }
        }
        for (i, w) in self.windows_col.iter().enumerate() {
            if w.0 >= start && w.1 <= end {
                out.push((*w, self.label_ids_col[i], &self.payloads_col[i]));
            }
        }
    }

    /// Total accumulated entries — caller compares against
    /// `epoch_capacity` to decide whether to seal + rotate.
    pub fn distinct_windows(&self) -> usize {
        self.windows_set.len()
    }
}

impl<P> Default for MutableEpoch<P> {
    fn default() -> Self {
        Self::new()
    }
}

/// Sealed (immutable) epoch: flat sorted `Vec` for cache-friendly
/// binary-search range scans. Built once at rotation time from the
/// then-active `MutableEpoch`.
pub struct SealedEpoch<P> {
    /// Sorted by `(TimestampRange, LabelValuesId)`. Binary search on
    /// `start_unix_ms` to seek; linear scan within the matched range.
    entries: Vec<(TimestampRange, LabelValuesId, P)>,
    min_start: Option<u64>,
    max_end: Option<u64>,
}

impl<P> SealedEpoch<P> {
    /// Consume a `MutableEpoch` and produce its sorted immutable form.
    /// O(M log M) — paid once at rotation, off the insert hot path.
    pub fn from_mutable(mut m: MutableEpoch<P>) -> Self {
        let min_start = m.min_start;
        let max_end = m.max_end;
        let len = m.windows_col.len();
        let mut entries: Vec<(TimestampRange, LabelValuesId, P)> = Vec::with_capacity(len);
        // Drain via swap_remove from the back to move payloads without
        // cloning. Equivalent to consuming the parallel arrays in order.
        for i in 0..len {
            entries.push((
                m.windows_col[i],
                m.label_ids_col[i],
                std::mem::replace(&mut m.payloads_col[i], unsafe {
                    std::mem::MaybeUninit::zeroed().assume_init()
                }),
            ));
        }
        // Forget the columns to avoid double-drop (the moved-out payloads
        // were replaced with zeroed memory; their drop should not run).
        std::mem::forget(m.payloads_col);
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        Self {
            entries,
            min_start,
            max_end,
        }
    }

    pub fn min_start(&self) -> Option<u64> {
        self.min_start
    }

    pub fn max_end(&self) -> Option<u64> {
        self.max_end
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Range query — O(log N + k) via binary search to the first
    /// matching entry, then linear scan.
    pub fn range_query_into<'a>(
        &'a self,
        start: u64,
        end: u64,
        out: &mut Vec<(TimestampRange, LabelValuesId, &'a P)>,
    ) {
        if let (Some(min_s), Some(max_e)) = (self.min_start, self.max_end) {
            if min_s > end || max_e < start {
                return;
            }
        }
        // Binary search for the first entry whose start >= start.
        let from = self.entries.partition_point(|e| e.0 .0 < start);
        for entry in &self.entries[from..] {
            if entry.0 .0 > end {
                break;
            }
            if entry.0 .1 <= end {
                out.push((entry.0, entry.1, &entry.2));
            }
        }
    }

    /// Exact-window query — O(log N + m). Binary search for the
    /// matching range; linear scan while the range matches.
    pub fn exact_query(&self, target: TimestampRange) -> Vec<(LabelValuesId, &P)> {
        let from = self.entries.partition_point(|e| e.0 < target);
        let mut out = Vec::new();
        for entry in &self.entries[from..] {
            if entry.0 != target {
                break;
            }
            out.push((entry.1, &entry.2));
        }
        out
    }
}

/// Per-sid storage — drop-in replacement for the new SketchIndex's
/// `series` map's value type. Pairs an active `MutableEpoch` with a
/// rotation-ordered `BTreeMap<EpochId, SealedEpoch>`. Concurrency is
/// owned by the outer `RwLock<SidStoreData>` (mirrors legacy's
/// per-key concurrency model).
pub struct SidStoreData<K: Eq + std::hash::Hash + Clone, P> {
    pub intern: InternTable<K>,
    pub current_epoch: MutableEpoch<P>,
    pub sealed_epochs: BTreeMap<EpochId, SealedEpoch<P>>,
    pub current_epoch_id: EpochId,
    pub epoch_capacity: Option<usize>,
    pub max_epochs: usize,
}

impl<K: Eq + std::hash::Hash + Clone, P> SidStoreData<K, P> {
    pub fn new() -> Self {
        Self {
            intern: InternTable::new(),
            current_epoch: MutableEpoch::new(),
            sealed_epochs: BTreeMap::new(),
            current_epoch_id: 0,
            epoch_capacity: None,
            max_epochs: 4,
        }
    }

    /// Insert a labeled payload for a specific time window. Caller has
    /// already canonicalized the label-values key (e.g. sorted). Hot
    /// path: amortized O(1) per the optimizations above.
    pub fn insert(&mut self, window: TimestampRange, label_key: K, payload: P) {
        let label_id = self.intern.intern(label_key);
        self.current_epoch.insert(window, label_id, payload);
        self.maybe_rotate_epoch();
    }

    fn maybe_rotate_epoch(&mut self) {
        let cap = match self.epoch_capacity {
            Some(c) if c > 0 => c,
            _ => return,
        };
        if self.current_epoch.distinct_windows() < cap {
            return;
        }
        // Seal the current epoch and rotate.
        let prev_len = self.current_epoch.len();
        let sealed = SealedEpoch::from_mutable(std::mem::replace(
            &mut self.current_epoch,
            MutableEpoch::with_capacity(prev_len),
        ));
        self.sealed_epochs.insert(self.current_epoch_id, sealed);
        self.current_epoch_id += 1;

        // Drop oldest sealed if we exceed `max_epochs`.
        while self.sealed_epochs.len() + 1 > self.max_epochs {
            // BTreeMap::pop_first is stable in 1.66+
            if let Some((id, _)) = self.sealed_epochs.iter().next().map(|(k, _)| (*k, ())) {
                self.sealed_epochs.remove(&id);
            } else {
                break;
            }
        }
    }
}

impl<K: Eq + std::hash::Hash + Clone, P> Default for SidStoreData<K, P> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_idempotent() {
        let mut t = InternTable::<String>::new();
        let a = t.intern("foo".into());
        let b = t.intern("foo".into());
        let c = t.intern("bar".into());
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(t.resolve(a), Some(&"foo".to_string()));
        assert_eq!(t.resolve(c), Some(&"bar".to_string()));
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn mutable_epoch_insert_and_query() {
        let mut e = MutableEpoch::<String>::new();
        e.insert((0, 100), 1, "a".into());
        e.insert((100, 200), 2, "b".into());
        e.insert((100, 200), 3, "c".into()); // Opt 3 fast path: same window
        e.insert((200, 300), 4, "d".into());
        assert_eq!(e.distinct_windows(), 3);
        assert_eq!(e.len(), 4);

        // Exact query (builds lazy index)
        let r = e.exact_query((100, 200));
        let mut ids: Vec<_> = r.iter().map(|(id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![2, 3]);

        // Range query
        let mut buf = Vec::new();
        e.range_query_into(50, 250, &mut buf);
        let mut ids2: Vec<_> = buf.iter().map(|(_, id, _)| *id).collect();
        ids2.sort();
        assert_eq!(ids2, vec![2, 3]); // (0,100) is below; (200,300) is above
    }

    #[test]
    fn sealed_epoch_binary_search_range() {
        let mut m = MutableEpoch::<u32>::new();
        m.insert((0, 10), 1, 100);
        m.insert((10, 20), 2, 200);
        m.insert((20, 30), 3, 300);
        m.insert((30, 40), 4, 400);
        let s = SealedEpoch::from_mutable(m);
        let mut buf = Vec::new();
        s.range_query_into(10, 30, &mut buf);
        let payloads: Vec<u32> = buf.iter().map(|(_, _, p)| **p).collect();
        // (10,20)=200 and (20,30)=300 are fully within [10,30]
        assert!(payloads.contains(&200));
        assert!(payloads.contains(&300));
    }

    #[test]
    fn sid_store_rotation() {
        let mut s = SidStoreData::<String, String>::new();
        s.epoch_capacity = Some(2);
        s.max_epochs = 2;
        s.insert((0, 10), "a".into(), "p1".into());
        s.insert((10, 20), "a".into(), "p2".into()); // capacity hit → seal
        s.insert((20, 30), "a".into(), "p3".into()); // new epoch
        s.insert((30, 40), "a".into(), "p4".into()); // capacity hit → seal again
        s.insert((40, 50), "a".into(), "p5".into()); // third epoch; oldest sealed evicted
        assert!(s.sealed_epochs.len() <= 2);
    }
}
