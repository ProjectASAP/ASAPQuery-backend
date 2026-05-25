//! Epoch-partitioned columnar storage — generic payload type.
//!
//! Lifted from `sketch_store::common` (legacy SketchStore index)
//! with the payload column type made generic so the new SketchStore
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
//!   `Vec<Arc<dyn AggregateCore>>`. The new SketchStore stores
//!   `SketchSampleState` directly (typed bytes + encoding tag) — no
//!   dyn dispatch, no Arc cloning, payload moves into the column.
//! - **Series-values keyed via `LabelValuesId = u32`** (renamed from
//!   legacy `MetricID = u32`). The intern table maps the per-series
//!   group-by VALUES vector to a compact ID, since the SketchStore's
//!   sid already captures the metric identity at the level above.
//!
//! See INDEX_DESIGN.md in `sketch_store/` for the full complexity
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
/// `BTreeMap<String,String>` because the new SketchStore receives
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
/// but the SketchStore specializes to `SketchSampleState` (typed
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

    /// Range query with HALF-OPEN OVERLAP semantics: include any window
    /// whose `[w.0, w.1)` intersects `[start, end)` (i.e. `w.1 > start &&
    /// w.0 < end`). Sister of [`Self::range_query_into`], which uses
    /// strict CONTAINMENT (`w.0 >= start && w.1 <= end`).
    ///
    /// The sketch read path ([`SketchStore::query_range`]) needs overlap,
    /// not containment: the agent emits ~30s tumbling panes, and a short
    /// query window (e.g. `quantile_over_time(...[30s])`, or an instant
    /// query whose freshest pane straddles `now`) routinely fails to FULLY
    /// CONTAIN any single pane — the pane that covers the window's edge
    /// starts before `start` or ends after `end`. Containment then returns
    /// ZERO in-window samples, so the delta-stitching carry-in never fires
    /// (it keys off the earliest in-window sample) and the reducer yields
    /// an empty series → the live "No result for query" bug for `[30s]`
    /// and bare-instant selectors. Overlap admits the straddling pane; the
    /// reducer's per-window `w_end >= t0` filter and cumulative `latest_end`
    /// projection already drop windows that fall outside the requested
    /// `[t0, t1]` time domain, so no out-of-range value leaks into the
    /// answer. This matches the overlap filter [`Self::range_query_into_grouped`]
    /// already uses (and the legacy `SketchStore`'s 30s-pane fix).
    pub fn range_query_overlap_into<'a>(
        &'a self,
        start: u64,
        end: u64,
        out: &mut Vec<(TimestampRange, LabelValuesId, &'a P)>,
    ) {
        // O(1) skip if the epoch's bounds don't overlap the query range.
        if let (Some(min_s), Some(max_e)) = (self.min_start, self.max_end) {
            if min_s >= end || max_e <= start {
                return;
            }
        }
        for (i, w) in self.windows_col.iter().enumerate() {
            if w.1 > start && w.0 < end {
                out.push((*w, self.label_ids_col[i], &self.payloads_col[i]));
            }
        }
    }

    /// Push every entry whose window-END is at or before `before` into
    /// `out`. Used by the sketch read path to fetch a delta-stitching
    /// "carry-in base" — the most-recent Full snapshot that landed
    /// before the query window — so a short query window that contains
    /// only delta frames can still establish its rolling state. The
    /// caller is responsible for picking the latest Full per label (the
    /// columnar layer is payload-agnostic). O(M) linear scan.
    pub fn collect_ending_at_or_before<'a>(
        &'a self,
        before: u64,
        out: &mut Vec<(TimestampRange, LabelValuesId, &'a P)>,
    ) {
        if let Some(min_s) = self.min_start {
            if min_s > before {
                return;
            }
        }
        for (i, w) in self.windows_col.iter().enumerate() {
            if w.1 <= before {
                out.push((*w, self.label_ids_col[i], &self.payloads_col[i]));
            }
        }
    }

    /// Total accumulated entries — caller compares against
    /// `epoch_capacity` to decide whether to seal + rotate.
    pub fn distinct_windows(&self) -> usize {
        self.windows_set.len()
    }

    /// `(min_start, max_end)` across all windows, or `None` if empty.
    /// Convenience for the epoch-skip check
    /// `min_start > end || max_end < start`.
    pub fn time_bounds(&self) -> Option<(u64, u64)> {
        match (self.min_start, self.max_end) {
            (Some(s), Some(e)) => Some((s, e)),
            _ => None,
        }
    }

    /// Consume self and produce a `SealedEpoch<P>` — convenience for
    /// epoch rotation. Equivalent to `SealedEpoch::from_mutable(self)`.
    pub fn seal(self) -> SealedEpoch<P> {
        SealedEpoch::from_mutable(self)
    }

    /// Drop every entry whose window-END is at or before `cutoff_end`,
    /// returning the number of distinct windows evicted.
    ///
    /// This is the in-memory retention primitive for the WARM sketch
    /// store: it bounds `current_epoch` to a recent time horizon so its
    /// memory is `O(active_series × horizon)` rather than
    /// `O(active_series × total_elapsed_time)`. It does NOT require
    /// sealing — unsealed recent windows stay queryable (the read path
    /// scans `current_epoch` directly), which is the explicit design
    /// intent. Older data lives in the cold/raw tier.
    ///
    /// Uses window-END (`w.1 <= cutoff_end`) rather than window-START so
    /// a half-open pane that straddles the cutoff is RETAINED until it is
    /// fully behind the horizon — the read path's overlap scan and the
    /// delta-stitching carry-in (`collect_ending_at_or_before`) can still
    /// see it. Callers pick `cutoff_end = newest_end - horizon`, so
    /// anything kept is within `horizon` of the freshest window.
    ///
    /// O(N) — rebuilds the three columns in one pass, same shape as
    /// [`Self::remove_windows`].
    pub fn evict_window_ends_before(&mut self, cutoff_end: u64) -> usize {
        // O(1) skip: nothing is old enough to evict.
        match self.min_start {
            // Cheapest guard: if the earliest window-START is already
            // past the cutoff, no window can END at/before it either.
            Some(min_s) if min_s > cutoff_end => return 0,
            None => return 0,
            _ => {}
        }
        let old_windows = std::mem::take(&mut self.windows_col);
        let old_ids = std::mem::take(&mut self.label_ids_col);
        let old_payloads = std::mem::take(&mut self.payloads_col);
        let prev_distinct = self.windows_set.len();
        self.windows_set.clear();
        for ((w, id), p) in old_windows.into_iter().zip(old_ids).zip(old_payloads) {
            if w.1 <= cutoff_end {
                continue;
            }
            self.windows_set.insert(w);
            self.windows_col.push(w);
            self.label_ids_col.push(id);
            self.payloads_col.push(p);
        }
        let dropped = prev_distinct.saturating_sub(self.windows_set.len());
        if dropped > 0 {
            self.window_to_ids = None;
            self.last_window = None;
            self.min_start = self.windows_col.iter().map(|w| w.0).min();
            self.max_end = self.windows_col.iter().map(|w| w.1).max();
        }
        dropped
    }

    /// Remove all entries whose window is in `windows`.
    /// Mirrors the legacy `SketchStore` CircularBuffer
    /// cleanup contract. O(N) — rebuilds columns in one pass.
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        use std::collections::HashSet as StdHashSet;
        let drop_set: StdHashSet<TimestampRange> = windows.iter().copied().collect();
        let old_windows = std::mem::take(&mut self.windows_col);
        let old_ids = std::mem::take(&mut self.label_ids_col);
        let old_payloads = std::mem::take(&mut self.payloads_col);
        for ((w, id), p) in old_windows.into_iter().zip(old_ids).zip(old_payloads) {
            if !drop_set.contains(&w) {
                self.windows_col.push(w);
                self.label_ids_col.push(id);
                self.payloads_col.push(p);
            }
        }
        for w in windows {
            self.windows_set.remove(w);
        }
        self.window_to_ids = None;
        self.last_window = None;
        self.min_start = self.windows_col.iter().map(|w| w.0).min();
        self.max_end = self.windows_col.iter().map(|w| w.1).max();
    }
}

impl<P: Clone> MutableEpoch<P> {
    /// Range query into a caller-provided `HashMap<LabelValuesId, Vec<(TimestampRange, P)>>`,
    /// matching the legacy `SketchStore`'s `MetricBucketMap` shape.
    /// Also pushes each matched window into `matched_windows` for the
    /// downstream accounting (e.g. metrics).
    ///
    /// Uses the same overlap-filter semantics as the flat
    /// `range_query_into`: include any window whose `[w.0, w.1)`
    /// intersects `[start, end)`. Tumbling panes that straddle the
    /// query boundaries match; same fix that the legacy implementation
    /// carried (see legacy module comment about
    /// `quantile_over_time(...[1m])` against 30s panes).
    pub fn range_query_into_grouped(
        &self,
        start: u64,
        end: u64,
        out: &mut HashMap<LabelValuesId, Vec<(TimestampRange, P)>>,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        for (i, &w) in self.windows_col.iter().enumerate() {
            if w.1 <= start || w.0 >= end {
                continue;
            }
            out.entry(self.label_ids_col[i])
                .or_default()
                .push((w, self.payloads_col[i].clone()));
            matched_windows.push(w);
        }
    }

    /// Exact-window query returning OWNED payload clones — for callers
    /// that need to hand the payload out across a lock boundary.
    /// `None` when the window has no entries.
    pub fn exact_query_owned(
        &mut self,
        target: TimestampRange,
    ) -> Option<Vec<(LabelValuesId, P)>> {
        let r = self.exact_query(target);
        if r.is_empty() {
            None
        } else {
            Some(r.into_iter().map(|(id, p)| (id, p.clone())).collect())
        }
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
    pub entries: Vec<(TimestampRange, LabelValuesId, P)>,
    min_start: Option<u64>,
    max_end: Option<u64>,
}

impl<P> SealedEpoch<P> {
    /// Consume a `MutableEpoch` and produce its sorted immutable form.
    /// O(M log M) — paid once at rotation, off the insert hot path.
    ///
    /// Safe payload move: zip-consumes the three parallel columns
    /// into owned tuples. Previously used `MaybeUninit::zeroed()` +
    /// `mem::forget` which is UB for any `P` with non-trivial Drop
    /// (e.g. `Arc<_>`); the new form has the same algorithmic cost
    /// and works for arbitrary `P`.
    pub fn from_mutable(m: MutableEpoch<P>) -> Self {
        let min_start = m.min_start;
        let max_end = m.max_end;
        let mut entries: Vec<(TimestampRange, LabelValuesId, P)> = m
            .windows_col
            .into_iter()
            .zip(m.label_ids_col)
            .zip(m.payloads_col)
            .map(|((w, lid), p)| (w, lid, p))
            .collect();
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

    /// Range query with HALF-OPEN OVERLAP semantics: include any window
    /// whose `[w.0, w.1)` intersects `[start, end)` (`w.1 > start && w.0 <
    /// end`). Sealed sister of [`MutableEpoch::range_query_overlap_into`] —
    /// see its doc for why the sketch read path needs overlap rather than
    /// the containment that [`Self::range_query_into`] applies.
    ///
    /// Entries are sorted by window-START, but an overlapping window may
    /// START before `start` (it straddles the left edge), so the
    /// `partition_point(|e| e.0.0 < start)` seek the containment variant
    /// uses would WRONGLY skip it. We instead scan from the front while
    /// `w.0 < end`, testing `w.1 > start` per entry. O(k) over the prefix
    /// of windows that start before `end` — bounded by the epoch size and
    /// fine for the warm read path (epochs are small; nothing is sealed in
    /// the live single-epoch deployment anyway).
    pub fn range_query_overlap_into<'a>(
        &'a self,
        start: u64,
        end: u64,
        out: &mut Vec<(TimestampRange, LabelValuesId, &'a P)>,
    ) {
        if let (Some(min_s), Some(max_e)) = (self.min_start, self.max_end) {
            if min_s >= end || max_e <= start {
                return;
            }
        }
        for entry in &self.entries {
            // Sorted by start; once a window starts at/after `end` it (and
            // every later one) cannot overlap `[start, end)`.
            if entry.0 .0 >= end {
                break;
            }
            if entry.0 .1 > start {
                out.push((entry.0, entry.1, &entry.2));
            }
        }
    }

    /// Push every entry whose window-END is at or before `before` into
    /// `out`. Sealed sister of
    /// [`MutableEpoch::collect_ending_at_or_before`]. Entries sort by
    /// window-START; since `w.1 >= w.0`, any entry with `w.1 <= before`
    /// also has `w.0 <= before`, so we can stop the scan once
    /// `w.0 > before`. O(log N + k).
    pub fn collect_ending_at_or_before<'a>(
        &'a self,
        before: u64,
        out: &mut Vec<(TimestampRange, LabelValuesId, &'a P)>,
    ) {
        if let Some(min_s) = self.min_start {
            if min_s > before {
                return;
            }
        }
        for entry in &self.entries {
            if entry.0 .0 > before {
                break;
            }
            if entry.0 .1 <= before {
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

    /// `(min_start, max_end)` or `None` if empty.
    pub fn time_bounds(&self) -> Option<(u64, u64)> {
        match (self.min_start, self.max_end) {
            (Some(s), Some(e)) => Some((s, e)),
            _ => None,
        }
    }

    /// Count of distinct time windows in this sealed epoch — O(N)
    /// scan (entries are sorted, so consecutive dupes are adjacent).
    pub fn distinct_window_count(&self) -> usize {
        let mut count = 0usize;
        let mut last: Option<TimestampRange> = None;
        for (w, _, _) in &self.entries {
            if last != Some(*w) {
                count += 1;
                last = Some(*w);
            }
        }
        count
    }

    /// Sorted-deduplicated windows. Used by the legacy SketchStore to
    /// surface the windows that were dropped on epoch eviction.
    pub fn unique_windows(&self) -> Vec<TimestampRange> {
        let mut windows: Vec<TimestampRange> =
            self.entries.iter().map(|(w, _, _)| *w).collect();
        windows.dedup();
        windows
    }

    /// Remove all entries whose window is in `windows`. O(N) scan;
    /// preserves sortedness since `retain` keeps relative order.
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        use std::collections::HashSet as StdHashSet;
        let drop_set: StdHashSet<TimestampRange> = windows.iter().copied().collect();
        self.entries.retain(|(w, _, _)| !drop_set.contains(w));
        self.min_start = self.entries.iter().map(|(w, _, _)| w.0).min();
        self.max_end = self.entries.iter().map(|(w, _, _)| w.1).max();
    }
}

impl<P: Clone> SealedEpoch<P> {
    /// Range query into a caller-provided
    /// `HashMap<LabelValuesId, Vec<(TimestampRange, P)>>` — the
    /// legacy `MetricBucketMap` shape used by `SketchStore`.
    /// Binary-search start + linear scan. Same overlap semantics
    /// as the mutable variant.
    pub fn range_query_into_grouped(
        &self,
        start: u64,
        end: u64,
        out: &mut HashMap<LabelValuesId, Vec<(TimestampRange, P)>>,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        // Entries are sorted by `(w.0, label_id)`. Bound the upper
        // end with `w.0 < end`; entries past that point can't overlap.
        let end_pos = self.entries.partition_point(|(w, _, _)| w.0 < end);
        for (w, id, p) in &self.entries[..end_pos] {
            if w.1 <= start {
                continue;
            }
            out.entry(*id).or_default().push((*w, p.clone()));
            matched_windows.push(*w);
        }
    }

    /// Exact-window query returning OWNED clones — used by callers
    /// that need to release the lock before reading the payloads.
    /// `None` when no entry matches.
    pub fn exact_query_owned(
        &self,
        target: TimestampRange,
    ) -> Option<Vec<(LabelValuesId, P)>> {
        let r = self.exact_query(target);
        if r.is_empty() {
            None
        } else {
            Some(r.into_iter().map(|(id, p)| (id, p.clone())).collect())
        }
    }
}

/// Per-sid storage — drop-in replacement for the new SketchStore's
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
    /// In-memory WARM retention horizon, in milliseconds. On each
    /// insert, windows whose END is older than `newest_end - horizon`
    /// are evicted from `current_epoch` (and from any `sealed_epochs`),
    /// bounding per-sid memory to `O(horizon)` instead of growing with
    /// total elapsed time. `None` disables retention (legacy/unbounded
    /// behavior — used by tests that want full history).
    ///
    /// Defaults from [`default_retention_horizon_ms`] which reads the
    /// `ASAP_SKETCH_RETENTION_MS` env once. The horizon is deliberately
    /// larger than the max query window (~30m) plus the delta-stitching
    /// carry-in reach, so recent range/instant queries never lose a
    /// window or its Full base. See `evict_window_ends_before`.
    pub retention_horizon_ms: Option<u64>,
    /// Seal cadence (in DISTINCT WINDOWS) for the durable tier. When
    /// `Some(n)`, `current_epoch` is sealed into `sealed_epochs` once it
    /// holds `n` distinct windows, so the persistence flusher has sealed
    /// epochs to flush to disk. `None` (the default) means "never seal
    /// on cadence" — the in-memory-only deployment where #327 retention
    /// bounds memory by dropping aged windows from `current_epoch`.
    ///
    /// Distinct from [`Self::epoch_capacity`], which is the test-only
    /// rotation threshold; both feed [`Self::maybe_rotate_epoch`], and
    /// the smaller of the two (when set) wins. The store sets THIS field
    /// (not `epoch_capacity`) when persistence is enabled so the
    /// production seal cadence is decoupled from the test knob.
    pub seal_window_count: Option<usize>,
    /// When `true`, this sid's memory is bounded by the persistence
    /// flush-then-evict loop, NOT by [`Self::enforce_retention`]. The
    /// flusher owns the lifecycle of sealed epochs (seal → flush to a
    /// durable disk part → evict from memory), and the disk-tier TTL
    /// (`delete_older_than_ms`) bounds the durable copy. Retention must
    /// not drop a sealed epoch out from under a pending flush, nor evict
    /// un-sealed `current_epoch` windows that were never made durable.
    /// So when this is `true`, `enforce_retention` is a no-op. When
    /// `false` (the default), #327 retention is the memory bound.
    pub persistence_enabled: bool,
}

/// Default in-memory retention horizon (ms) for the WARM sketch store.
/// 2 hours — comfortably exceeds the ~30m max range-query window plus
/// the delta-stitching carry-in's Full-base reach, so bounding memory
/// to this horizon cannot regress the recent-window read path. Older
/// data is served from the cold/raw tier.
pub const DEFAULT_SKETCH_RETENTION_MS: u64 = 2 * 60 * 60 * 1000;

/// Resolve the WARM retention horizon once from the
/// `ASAP_SKETCH_RETENTION_MS` env var, caching the result for the life
/// of the process (read off the per-insert hot path). Falls back to
/// [`DEFAULT_SKETCH_RETENTION_MS`] when unset or unparseable. A value of
/// `0` explicitly DISABLES retention (returns `None`) for operators who
/// need full in-memory history (and accept the unbounded growth).
pub fn default_retention_horizon_ms() -> Option<u64> {
    use std::sync::OnceLock;
    static HORIZON: OnceLock<Option<u64>> = OnceLock::new();
    *HORIZON.get_or_init(|| {
        match std::env::var("ASAP_SKETCH_RETENTION_MS") {
            Ok(v) => match v.trim().parse::<u64>() {
                Ok(0) => None,
                Ok(ms) => Some(ms),
                Err(_) => Some(DEFAULT_SKETCH_RETENTION_MS),
            },
            Err(_) => Some(DEFAULT_SKETCH_RETENTION_MS),
        }
    })
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
            retention_horizon_ms: default_retention_horizon_ms(),
            seal_window_count: None,
            persistence_enabled: false,
        }
    }

    /// Insert a labeled payload for a specific time window. Caller has
    /// already canonicalized the label-values key (e.g. sorted). Hot
    /// path: amortized O(1) per the optimizations above, plus a bounded
    /// retention sweep when the freshest window advances the horizon.
    pub fn insert(&mut self, window: TimestampRange, label_key: K, payload: P) {
        let label_id = self.intern.intern(label_key);
        self.current_epoch.insert(window, label_id, payload);
        self.maybe_rotate_epoch();
        self.enforce_retention();
    }

    /// Bound in-memory footprint to the configured horizon. Drops every
    /// window (in `current_epoch` AND any `sealed_epochs`) whose END is
    /// older than `newest_end - horizon`. This is what makes SketchStore
    /// memory `O(active_series × horizon)`: without it `current_epoch`
    /// accumulates one window per tumbling pane forever (the leak), since
    /// nothing seals (`epoch_capacity == None`) and the persistence
    /// flusher only ever evicts SEALED epochs.
    ///
    /// Reads stay correct: anything within `horizon` of the freshest
    /// window is retained, so a `[30m]` range query and the carry-in
    /// Full base both still resolve. Eviction keys on window-END so a
    /// straddling pane survives until fully behind the horizon.
    fn enforce_retention(&mut self) {
        // Persistence-enabled sids are bounded by the flush-then-evict
        // loop, not by dropping. Retention must NOT race the flusher by
        // dropping a sealed epoch before it has been made durable, nor
        // evict un-sealed `current_epoch` windows that were never
        // flushed. The flusher's disk-tier TTL bounds the durable copy.
        if self.persistence_enabled {
            return;
        }
        let Some(horizon) = self.retention_horizon_ms else {
            return;
        };
        // Newest window-end across mutable + sealed state defines "now"
        // for retention; using max_end (not wall-clock) keeps the bound
        // robust to clock skew and backfill.
        let newest_end = self
            .current_epoch
            .max_end()
            .into_iter()
            .chain(self.sealed_epochs.values().filter_map(|s| s.max_end()))
            .max();
        let Some(newest_end) = newest_end else {
            return;
        };
        let cutoff_end = newest_end.saturating_sub(horizon);
        if cutoff_end == 0 {
            return;
        }
        self.current_epoch.evict_window_ends_before(cutoff_end);
        if !self.sealed_epochs.is_empty() {
            let drop_ids: Vec<EpochId> = self
                .sealed_epochs
                .iter()
                .filter(|(_, ep)| ep.max_end().map(|e| e <= cutoff_end).unwrap_or(true))
                .map(|(id, _)| *id)
                .collect();
            for id in drop_ids {
                self.sealed_epochs.remove(&id);
            }
        }
    }

    fn maybe_rotate_epoch(&mut self) {
        // The effective rotation threshold is the smaller of the
        // test-only `epoch_capacity` and the durable-tier
        // `seal_window_count` (whichever is set); when both are unset,
        // we never seal on cadence.
        let cap = match (self.epoch_capacity, self.seal_window_count) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => return,
        };
        if cap == 0 || self.current_epoch.distinct_windows() < cap {
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

        // Drop oldest sealed if we exceed `max_epochs` — but ONLY when
        // persistence is OFF. Under persistence the flusher owns sealed-
        // epoch lifecycle (seal → durable part → evict); dropping a
        // sealed epoch here would discard data that was never flushed,
        // defeating the durable tier. So persistence-enabled sids keep
        // every sealed epoch in memory until the flusher evicts it.
        if !self.persistence_enabled {
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
    fn mutable_epoch_overlap_vs_containment() {
        let mut e = MutableEpoch::<u32>::new();
        e.insert((0, 100), 1, 100);
        e.insert((100, 200), 2, 200);
        e.insert((200, 300), 3, 300);

        // Containment `[50, 250]`: only (100,200) is fully inside.
        let mut buf = Vec::new();
        e.range_query_into(50, 250, &mut buf);
        let mut ids: Vec<_> = buf.iter().map(|(_, id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![2]);

        // Overlap `[50, 250)`: (0,100) straddles left, (100,200) inside,
        // (200,300) straddles right → all three intersect.
        buf.clear();
        e.range_query_overlap_into(50, 250, &mut buf);
        let mut ids: Vec<_> = buf.iter().map(|(_, id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        // Half-open boundary: a window ending exactly at `start` does NOT
        // overlap; one starting exactly at `end` does NOT either.
        buf.clear();
        e.range_query_overlap_into(100, 200, &mut buf); // start=100, end=200
        let mut ids: Vec<_> = buf.iter().map(|(_, id, _)| *id).collect();
        ids.sort();
        // (0,100) ends at start → out; (100,200) overlaps; (200,300) starts at end → out.
        assert_eq!(ids, vec![2]);
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
    fn sealed_epoch_overlap_admits_straddling_windows() {
        let mut m = MutableEpoch::<u32>::new();
        m.insert((0, 10), 1, 100);
        m.insert((10, 20), 2, 200);
        m.insert((20, 30), 3, 300);
        m.insert((30, 40), 4, 400);
        let s = SealedEpoch::from_mutable(m);

        // Overlap `[5, 25)`: (0,10) straddles left edge — a window the
        // start-keyed binary search of the containment scan would skip.
        let mut buf = Vec::new();
        s.range_query_overlap_into(5, 25, &mut buf);
        let mut payloads: Vec<u32> = buf.iter().map(|(_, _, p)| **p).collect();
        payloads.sort();
        // (0,10) overlaps (10>5), (10,20) inside, (20,30) overlaps (20<25);
        // (30,40) is entirely after.
        assert_eq!(payloads, vec![100, 200, 300]);

        // Half-open: window ending at `start` excluded; starting at `end`
        // excluded.
        buf.clear();
        s.range_query_overlap_into(10, 30, &mut buf); // [10,30)
        let mut payloads: Vec<u32> = buf.iter().map(|(_, _, p)| **p).collect();
        payloads.sort();
        // (0,10) ends at 10=start → out; (10,20) & (20,30) overlap;
        // (30,40) starts at 30=end → out.
        assert_eq!(payloads, vec![200, 300]);
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

    #[test]
    fn evict_window_ends_before_drops_old_keeps_recent() {
        let mut e = MutableEpoch::<u32>::new();
        e.insert((0, 100), 1, 1);
        e.insert((100, 200), 2, 2);
        e.insert((150, 250), 3, 3); // straddles a cutoff of 200 (ends after)
        e.insert((200, 300), 4, 4);
        assert_eq!(e.distinct_windows(), 4);

        // Cutoff 200: drop windows ending <= 200, i.e. (0,100) & (100,200).
        // (150,250) straddles (ends at 250 > 200) → retained.
        let dropped = e.evict_window_ends_before(200);
        assert_eq!(dropped, 2);
        assert_eq!(e.distinct_windows(), 2);
        let mut buf = Vec::new();
        e.range_query_overlap_into(0, 400, &mut buf);
        let mut ids: Vec<_> = buf.iter().map(|(_, id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![3, 4]);
        // bounds recomputed
        assert_eq!(e.min_start(), Some(150));
        assert_eq!(e.max_end(), Some(300));
    }

    #[test]
    fn evict_window_ends_before_noop_when_all_recent() {
        let mut e = MutableEpoch::<u32>::new();
        e.insert((1000, 1100), 1, 1);
        e.insert((1100, 1200), 2, 2);
        // Cutoff below everything → nothing dropped, no rebuild.
        assert_eq!(e.evict_window_ends_before(500), 0);
        assert_eq!(e.distinct_windows(), 2);
    }

    #[test]
    fn retention_bounds_window_count_over_long_elapsed_time() {
        // Ingest a long stream of 30s tumbling windows. With a bounded
        // horizon the per-sid distinct-window count stays bounded even as
        // elapsed time grows without limit — the production leak fix.
        let horizon_ms = 60 * 60 * 1000; // 1h
        let window_ms = 30_000u64; // 30s panes
        let mut s = SidStoreData::<String, u32>::new();
        s.retention_horizon_ms = Some(horizon_ms);

        let mut start = 0u64;
        // 4 hours of ingest = 480 windows; unbounded would retain all 480.
        for i in 0..480u32 {
            let w = (start, start + window_ms);
            s.insert(w, "series".into(), i);
            start += window_ms;
        }

        // Bounded: at most ~horizon/window windows retained (plus the
        // straddling boundary pane). 1h / 30s = 120 windows.
        let retained = s.current_epoch.distinct_windows();
        assert!(
            retained <= (horizon_ms / window_ms) as usize + 2,
            "retained {retained} windows; expected ~{} (bounded by horizon)",
            horizon_ms / window_ms
        );
        // And it actually dropped the bulk of them.
        assert!(retained < 480, "retention did not evict old windows");
    }

    #[test]
    fn retention_disabled_keeps_full_history() {
        let mut s = SidStoreData::<String, u32>::new();
        s.retention_horizon_ms = None; // disabled
        let window_ms = 30_000u64;
        let mut start = 0u64;
        for i in 0..200u32 {
            s.insert((start, start + window_ms), "series".into(), i);
            start += window_ms;
        }
        assert_eq!(s.current_epoch.distinct_windows(), 200);
    }

    #[test]
    fn retention_keeps_windows_within_horizon_queryable() {
        // The freshest `horizon` worth of windows must survive eviction
        // so a recent range query still resolves (no regression to the
        // overlap-scan / carry-in read path).
        let horizon_ms = 60 * 60 * 1000; // 1h
        let window_ms = 30_000u64;
        let mut s = SidStoreData::<String, u32>::new();
        s.retention_horizon_ms = Some(horizon_ms);

        let mut start = 0u64;
        let mut last_end = 0u64;
        for i in 0..480u32 {
            last_end = start + window_ms;
            s.insert((start, last_end), "series".into(), i);
            start += window_ms;
        }

        // A 30m range query ending at the newest window must still find
        // windows (well inside the 1h horizon).
        let q_start = last_end - 30 * 60 * 1000;
        let mut buf = Vec::new();
        s.current_epoch
            .range_query_overlap_into(q_start, last_end, &mut buf);
        assert!(
            !buf.is_empty(),
            "30m range query within horizon returned no windows — read path regressed"
        );
    }

    #[test]
    fn seal_window_count_seals_current_epoch_on_cadence() {
        // Persistence mode: seal every 3 distinct windows. After 7
        // windows we expect 2 sealed epochs (windows 0..3, 3..6) plus a
        // partial current epoch (window 6). Nothing is dropped — the
        // flusher owns sealed-epoch lifecycle, so max_epochs does NOT
        // bite under persistence.
        let mut s = SidStoreData::<String, u32>::new();
        s.seal_window_count = Some(3);
        s.persistence_enabled = true;
        s.max_epochs = 2; // would normally cap sealed at 1; ignored here
        let window_ms = 30_000u64;
        let mut start = 0u64;
        for i in 0..7u32 {
            s.insert((start, start + window_ms), "series".into(), i);
            start += window_ms;
        }
        assert_eq!(
            s.sealed_epochs.len(),
            2,
            "expected 2 sealed epochs at cadence 3 over 7 windows; max_epochs must not drop under persistence"
        );
        assert!(s.current_epoch.distinct_windows() >= 1);
    }

    #[test]
    fn persistence_enabled_disables_retention_drop() {
        // With persistence on, enforce_retention must be a no-op even
        // when a retention horizon is set — the flush-then-evict loop
        // (not dropping) is the memory bound. A long stream keeps every
        // window in memory until the flusher evicts the sealed epochs.
        let mut s = SidStoreData::<String, u32>::new();
        s.persistence_enabled = true;
        s.retention_horizon_ms = Some(60 * 60 * 1000); // 1h — would normally drop
        // No seal cadence: everything stays in current_epoch.
        let window_ms = 30_000u64;
        let mut start = 0u64;
        for i in 0..480u32 {
            // 4h of ingest
            s.insert((start, start + window_ms), "series".into(), i);
            start += window_ms;
        }
        assert_eq!(
            s.current_epoch.distinct_windows(),
            480,
            "persistence mode must not retention-drop; the flusher bounds memory instead"
        );
    }

    #[test]
    fn sealed_epochs_survive_for_flush_under_persistence() {
        // Sealed epochs accumulate (pending flush) and are NOT dropped by
        // either max_epochs rotation or retention while persistence is on.
        let mut s = SidStoreData::<String, u32>::new();
        s.seal_window_count = Some(2);
        s.persistence_enabled = true;
        s.retention_horizon_ms = Some(1); // aggressive; must be ignored
        s.max_epochs = 2;
        let window_ms = 30_000u64;
        let mut start = 0u64;
        for i in 0..10u32 {
            s.insert((start, start + window_ms), "series".into(), i);
            start += window_ms;
        }
        // 10 windows / cadence 2 = up to 5 sealed epochs; none dropped.
        assert!(
            s.sealed_epochs.len() >= 4,
            "sealed epochs were dropped under persistence (got {})",
            s.sealed_epochs.len()
        );
    }
}
