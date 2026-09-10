use crate::precompute_engine::operators::{
    CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, CountSketchAccumulator,
    CountSketchWithHeapAccumulator, DDSketchAccumulator, DatasketchesKLLAccumulator,
    HydraKllSketchAccumulator, IncreaseAccumulator, MinMaxAccumulator, MultipleIncreaseAccumulator,
    MultipleMinMaxAccumulator, MultipleSumAccumulator, SumAccumulator,
};
use crate::storage_engines::types::{
    AggregateCore, AggregationType, KeyByLabelValues, Measurement,
};
use asap_types::aggregation_config::AggregationConfig;
// Step 5 (sketch-identity unification, see
// scratchpad/artifacts/enum-unification-plan.md): dispatch below is
// driven by `AccumulatorSpec` (SummaryFamilyType + typed family parameters +
// keyed-axis grouping) instead of raw `AggregationType` +
// `aggregation_sub_type` string matching. Numeric params come straight
// off the committed family's typed params (no HashMap lookups) except
// `cms_params`, kept as a raw-`parameters` read for the one case Planner's
// family parameters have no field for: HydraKLL's `(row, col)` tiling grid (see
// `asap_types::accumulator_spec`'s module doc for why). `cms_params`
// now lives there — the only place that still needs the other three
// former local helpers (`kll_k_param`, `heap_size_param`,
// `ddsketch_alpha_param`) is that module's own `AccumulatorSpec`
// construction, so they aren't re-imported here.
use asap_types::accumulator_spec::{cms_params, AccumulatorSpecError};
use planner_types::post_asap::{ExactKind, SketchAlgorithm, SketchParams, SummaryFamilyType};

/// Generate the two boilerplate clone-based `AccumulatorUpdater` methods
/// for updaters whose inner `acc` field implements `Clone + AggregateCore`.
/// Not applicable to `IncreaseAccumulatorUpdater` (its `acc` is `Option<_>`
/// with non-trivial `None` handling).
macro_rules! impl_clone_accumulator_methods {
    ($acc_field:ident) => {
        fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
            let result = Box::new(self.$acc_field.clone());
            self.reset();
            result
        }

        fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
            Box::new(self.$acc_field.clone())
        }

        fn into_accumulator(self: Box<Self>) -> Box<dyn AggregateCore> {
            // Consume the updater and MOVE the accumulator out — no clone.
            // Avoids the expensive `Clone` (a full msgpack serialize/deserialize
            // round-trip for sketch accumulators) when a pane is evicted at
            // window close.
            let this = *self;
            Box::new(this.$acc_field)
        }
    };
}

/// Trait for feeding samples into accumulators in the precompute engine.
///
/// This provides a uniform interface over all accumulator types so that the
/// worker loop doesn't need to know which concrete type it's dealing with.
pub trait AccumulatorUpdater: Send {
    /// Feed a single (value, timestamp_ms) pair — for SingleSubpopulation types.
    fn update_single(&mut self, value: f64, timestamp_ms: i64);

    /// Feed a keyed (key, value, timestamp_ms) triple — for MultipleSubpopulation types.
    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64);

    /// Extract the final accumulator as a boxed `AggregateCore`.
    fn take_accumulator(&mut self) -> Box<dyn AggregateCore>;

    /// Non-destructive read of the current accumulator state (clone without reset).
    /// Used by pane-based sliding windows to read shared panes.
    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore>;

    /// Consume the updater and return its accumulator BY MOVE, avoiding the
    /// `Clone` that `take_accumulator`/`snapshot_accumulator` pay (for sketch
    /// accumulators that clone is a full msgpack serialize/deserialize
    /// round-trip). Used by `merge_panes_for_window` when a pane is evicted at
    /// window close. Default falls back to a clone for updaters that can't
    /// cheaply move their inner accumulator out.
    fn into_accumulator(self: Box<Self>) -> Box<dyn AggregateCore> {
        self.snapshot_accumulator()
    }

    /// Reset internal state for reuse (avoids re-allocation).
    fn reset(&mut self);

    /// Whether this updater is keyed (MultipleSubpopulation).
    fn is_keyed(&self) -> bool;

    /// Estimated memory usage in bytes.
    fn memory_usage_bytes(&self) -> usize;
}

// ---------------------------------------------------------------------------
// SumAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct SumAccumulatorUpdater {
    acc: SumAccumulator,
}

impl SumAccumulatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: SumAccumulator::new(),
        }
    }
}

impl Default for SumAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for SumAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = SumAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<SumAccumulator>()
    }
}

// ---------------------------------------------------------------------------
// MinMaxAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MinMaxAccumulatorUpdater {
    acc: MinMaxAccumulator,
    is_max: bool,
}

impl MinMaxAccumulatorUpdater {
    pub fn new(is_max: bool) -> Self {
        Self {
            acc: if is_max {
                MinMaxAccumulator::new_max()
            } else {
                MinMaxAccumulator::new_min()
            },
            is_max,
        }
    }
}

impl AccumulatorUpdater for MinMaxAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = if self.is_max {
            MinMaxAccumulator::new_max()
        } else {
            MinMaxAccumulator::new_min()
        };
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MinMaxAccumulator>()
    }
}

// ---------------------------------------------------------------------------
// IncreaseAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct IncreaseAccumulatorUpdater {
    acc: Option<IncreaseAccumulator>,
}

impl IncreaseAccumulatorUpdater {
    pub fn new() -> Self {
        Self { acc: None }
    }
}

impl Default for IncreaseAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for IncreaseAccumulatorUpdater {
    fn update_single(&mut self, value: f64, timestamp_ms: i64) {
        let measurement = Measurement::new(value);
        match &mut self.acc {
            Some(acc) => acc.update(measurement, timestamp_ms),
            None => {
                self.acc = Some(IncreaseAccumulator::new(
                    measurement.clone(),
                    timestamp_ms,
                    measurement,
                    timestamp_ms,
                ));
            }
        }
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    // Hand-written: acc is Option<_> with non-trivial None handling.
    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let acc = self.acc.take().unwrap_or_else(|| {
            IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(0.0), 0)
        });
        let result = Box::new(acc);
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        match &self.acc {
            Some(acc) => Box::new(acc.clone()),
            None => Box::new(IncreaseAccumulator::new(
                Measurement::new(0.0),
                0,
                Measurement::new(0.0),
                0,
            )),
        }
    }

    fn reset(&mut self) {
        self.acc = None;
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Option<IncreaseAccumulator>>()
    }
}

// ---------------------------------------------------------------------------
// KllAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct KllAccumulatorUpdater {
    acc: DatasketchesKLLAccumulator,
    k: u16,
}

impl KllAccumulatorUpdater {
    pub fn new(k: u16) -> Self {
        Self {
            acc: DatasketchesKLLAccumulator::new(k),
            k,
        }
    }
}

impl AccumulatorUpdater for KllAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = DatasketchesKLLAccumulator::new(self.k);
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        // KLL sketch size is hard to estimate precisely; use a rough estimate
        std::mem::size_of::<DatasketchesKLLAccumulator>() + 4096
    }
}

// ---------------------------------------------------------------------------
// DDSketchAccumulatorUpdater — pendant to KllAccumulatorUpdater
// ---------------------------------------------------------------------------
//
// Drives the agent-aggregated DDSketch path: the worker either
// (a) merges an inbound `DDSketchAccumulator` from the
// modified-OTLP `Data::Ddsketch` ingest (via the worker's
// `merge_with`), or (b) consumes raw values via `update_single`
// when an OTLP scalar datapoint matches an aggregation typed as
// DDSketch. (b) is the less common path but it lets the same
// aggregation slot serve both pre-aggregated agent sketches and
// raw OTLP gauges.
pub struct DDSketchAccumulatorUpdater {
    acc: DDSketchAccumulator,
    alpha: f64,
}

impl DDSketchAccumulatorUpdater {
    pub fn new(alpha: f64) -> Self {
        Self {
            acc: DDSketchAccumulator::new(alpha),
            alpha,
        }
    }
}

impl AccumulatorUpdater for DDSketchAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        // sketch-core's DdSketch (the inner of DDSketchAccumulator)
        // exposes `update(f64)` for single-value ingestion. The
        // worker calls this when a raw OTLP datapoint matches an
        // aggregation typed as DDSketch — the sketch-merge path
        // uses `merge_with` directly.
        self.acc.inner.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = DDSketchAccumulator::new(self.alpha);
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        // Bucket store is variable; rough estimate matches KLL.
        std::mem::size_of::<DDSketchAccumulator>() + 4096
    }
}

// ---------------------------------------------------------------------------
// MultipleSumAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MultipleSumAccumulatorUpdater {
    acc: MultipleSumAccumulator,
}

impl MultipleSumAccumulatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: MultipleSumAccumulator::new(),
        }
    }
}

impl Default for MultipleSumAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for MultipleSumAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = MultipleSumAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MultipleSumAccumulator>()
            + self.acc.sums.len() * (std::mem::size_of::<KeyByLabelValues>() + 8)
    }
}

// ---------------------------------------------------------------------------
// MultipleMinMaxAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MultipleMinMaxAccumulatorUpdater {
    acc: MultipleMinMaxAccumulator,
    is_max: bool,
}

impl MultipleMinMaxAccumulatorUpdater {
    pub fn new(is_max: bool) -> Self {
        Self {
            acc: if is_max {
                MultipleMinMaxAccumulator::new_max()
            } else {
                MultipleMinMaxAccumulator::new_min()
            },
            is_max,
        }
    }
}

impl AccumulatorUpdater for MultipleMinMaxAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = if self.is_max {
            MultipleMinMaxAccumulator::new_max()
        } else {
            MultipleMinMaxAccumulator::new_min()
        };
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MultipleMinMaxAccumulator>()
            + self.acc.values.len() * (std::mem::size_of::<KeyByLabelValues>() + 8)
    }
}

// ---------------------------------------------------------------------------
// MultipleIncreaseAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MultipleIncreaseAccumulatorUpdater {
    acc: MultipleIncreaseAccumulator,
}

impl MultipleIncreaseAccumulatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: MultipleIncreaseAccumulator::new(),
        }
    }
}

impl Default for MultipleIncreaseAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for MultipleIncreaseAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        let measurement = Measurement::new(value);
        match self.acc.increases.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().update(measurement, timestamp_ms);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(IncreaseAccumulator::new(
                    measurement.clone(),
                    timestamp_ms,
                    measurement,
                    timestamp_ms,
                ));
            }
        }
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = MultipleIncreaseAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MultipleIncreaseAccumulator>()
            + self.acc.increases.len()
                * (std::mem::size_of::<KeyByLabelValues>()
                    + std::mem::size_of::<IncreaseAccumulator>())
    }
}

// ---------------------------------------------------------------------------
// CmsAccumulatorUpdater (CountMinSketch)
// ---------------------------------------------------------------------------

/// Keyed weighted-frequency updater.
///
/// A raw Prometheus sample represents the observed metric value, so a bare CMS
/// adds `value` for its key. Counting each received sample as one is a distinct
/// event-count operation and requires an explicit typed plan contract; it must
/// not be inferred from the sketch algorithm alone.
pub struct CmsAccumulatorUpdater {
    acc: CountMinSketchAccumulator,
    row_num: usize,
    col_num: usize,
}

impl CmsAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            acc: CountMinSketchAccumulator::new(row_num, col_num),
            row_num,
            col_num,
        }
    }
}

impl AccumulatorUpdater for CmsAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.inner.update(&key.to_semicolon_str(), value);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = CountMinSketchAccumulator::new(self.row_num, self.col_num);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<CountMinSketchAccumulator>()
            + self.row_num * self.col_num * std::mem::size_of::<f64>()
    }
}

// ---------------------------------------------------------------------------
// CmsHeapAccumulatorUpdater — value-weighted / count-weighted top-k
// ---------------------------------------------------------------------------

/// What quantity the top-k heap ranks keys by.
///
/// These are DIFFERENT query semantics and must be chosen explicitly:
///
/// * [`TopkWeight::Value`] — accumulate **Σ of the datapoint value** per key.
///   This answers "top-k <group-by> by total <metric>" (e.g. "top-k hosts by
///   total CPU"). The heap value is the summed metric value, so the read-side
///   reducer's "sort heap descending by value" yields the correct ranking.
///
/// * [`TopkWeight::Count`] — accumulate **+1 per event** per key (occurrence
///   frequency), the textbook heavy-hitter / frequency-top-k semantics
///   ("which keys appear most often").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopkWeight {
    /// Σ datapoint value per key (value-weighted top-k).
    Value,
    /// +1 per event per key (count-weighted / frequency top-k).
    Count,
}

/// Keyed top-k updater backed by a real `CountMinSketchWithHeap` (a CMS
/// matrix PLUS a size-`heap_size` top-k heap). Unlike the heap-LESS
/// `CmsAccumulatorUpdater`, this enumerates top-k keys at read time
/// (`get_topk_keys` / `topk_heap_items`), which is what `topk(...)` queries
/// need.
///
/// The key is the configured group-by (`aggregated_labels`) value vector —
/// e.g. `host` — formed by `extract_aggregated_key_from_series` in the worker,
/// NOT the hardcoded metric label `item`. The accumulated quantity is selected
/// by [`TopkWeight`]:
///   * `Value` → `inner.update(key, value)` adds the datapoint value (Σ value).
///   * `Count` → `inner.update(key, 1.0)` adds one per event (Σ count).
///
/// Both `CountMinSketchWithHeap` and `CountSketchWithHeap` raw-input policies
/// route here; the heap is the shared distinguishing payload.
pub struct CmsHeapAccumulatorUpdater {
    acc: CountMinSketchWithHeapAccumulator,
    row_num: usize,
    col_num: usize,
    heap_size: usize,
    weight: TopkWeight,
    weight_scale: f64,
}

impl CmsHeapAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize, weight: TopkWeight) -> Self {
        Self::with_weight_scale(row_num, col_num, heap_size, weight, 1.0)
    }

    pub fn with_weight_scale(
        row_num: usize,
        col_num: usize,
        heap_size: usize,
        weight: TopkWeight,
        weight_scale: f64,
    ) -> Self {
        Self {
            acc: CountMinSketchWithHeapAccumulator::new(row_num, col_num, heap_size),
            row_num,
            col_num,
            heap_size,
            weight,
            weight_scale,
        }
    }
}

impl AccumulatorUpdater for CmsHeapAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        // Heap key = the group-by label-value vector (e.g. `host`), joined the
        // same way the read-side `get_topk_keys` splits it back apart (`;`).
        let weighted = match self.weight {
            // Σ value: feed the datapoint value. sketchlib's CMS-heap
            // `update(key, w)` adds `w.round()` occurrences of `key`, so the
            // heap value accumulates the (rounded) summed metric value.
            TopkWeight::Value => value * self.weight_scale,
            // Σ count: one occurrence per event, regardless of value.
            TopkWeight::Count => 1.0,
        };
        self.acc.inner.update(&key.to_semicolon_str(), weighted);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc =
            CountMinSketchWithHeapAccumulator::new(self.row_num, self.col_num, self.heap_size);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<CountMinSketchWithHeapAccumulator>()
            + self.row_num * self.col_num * std::mem::size_of::<f64>()
            + self.heap_size * (std::mem::size_of::<asap_sketchlib::CmsHeapItem>() + 32)
    }
}

// ---------------------------------------------------------------------------
// CountSketchAccumulatorUpdater (real median-of-signed-rows CountSketch)
// ---------------------------------------------------------------------------

/// Keyed point-frequency updater backed by a real `asap_sketchlib::CountSketch`
/// (signed rows, median-of-rows estimator) — distinct math from
/// `CmsAccumulatorUpdater`'s CMS (min-of-rows). Closes, on the raw-metric
/// ingest path, the conflation bug where `SketchAlgorithm::CountSketch` silently
/// shared `CmsAccumulatorUpdater` with bare CMS.
///
/// As with bare CMS, each raw Prometheus sample contributes its `value`.
/// Unit event counting must be selected explicitly by a future typed plan
/// contract rather than being implied by `SketchAlgorithm::CountSketch`.
pub struct CountSketchAccumulatorUpdater {
    acc: CountSketchAccumulator,
    row_num: usize,
    col_num: usize,
}

impl CountSketchAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            acc: CountSketchAccumulator::new(row_num, col_num),
            row_num,
            col_num,
        }
    }
}

impl AccumulatorUpdater for CountSketchAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.inner.update(&key.to_semicolon_str(), value);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = CountSketchAccumulator::new(self.row_num, self.col_num);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<CountSketchAccumulator>()
            + self.row_num * self.col_num * std::mem::size_of::<f64>()
    }
}

// ---------------------------------------------------------------------------
// CountSketchWithHeapAccumulatorUpdater (real CountSketch + top-k heap)
// ---------------------------------------------------------------------------

/// Keyed top-k updater backed by a real `CountSketchWithHeap` (signed-row
/// CountSketch matrix PLUS a size-`heap_size` top-k heap). Distinct math from
/// `CmsHeapAccumulatorUpdater`'s CMS-with-heap (min-of-rows); shares the same
/// [`TopkWeight`] semantics and heap payload shape.
pub struct CountSketchWithHeapAccumulatorUpdater {
    acc: CountSketchWithHeapAccumulator,
    row_num: usize,
    col_num: usize,
    heap_size: usize,
    weight: TopkWeight,
    weight_scale: f64,
}

impl CountSketchWithHeapAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize, weight: TopkWeight) -> Self {
        Self::with_weight_scale(row_num, col_num, heap_size, weight, 1.0)
    }

    pub fn with_weight_scale(
        row_num: usize,
        col_num: usize,
        heap_size: usize,
        weight: TopkWeight,
        weight_scale: f64,
    ) -> Self {
        Self {
            acc: CountSketchWithHeapAccumulator::new(row_num, col_num, heap_size),
            row_num,
            col_num,
            heap_size,
            weight,
            weight_scale,
        }
    }
}

impl AccumulatorUpdater for CountSketchWithHeapAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        let weighted = match self.weight {
            TopkWeight::Value => value * self.weight_scale,
            TopkWeight::Count => 1.0,
        };
        self.acc.inner.update(&key.to_semicolon_str(), weighted);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = CountSketchWithHeapAccumulator::new(self.row_num, self.col_num, self.heap_size);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<CountSketchWithHeapAccumulator>()
            + self.row_num * self.col_num * std::mem::size_of::<f64>()
            + self.heap_size * (std::mem::size_of::<asap_sketchlib::CsHeapItem>() + 32)
    }
}

// ---------------------------------------------------------------------------
// HydraKllAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct HydraKllAccumulatorUpdater {
    acc: HydraKllSketchAccumulator,
    row_num: usize,
    col_num: usize,
    k: u16,
}

impl HydraKllAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, k: u16) -> Self {
        Self {
            acc: HydraKllSketchAccumulator::new(row_num, col_num, k),
            row_num,
            col_num,
            k,
        }
    }
}

impl AccumulatorUpdater for HydraKllAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key, value);
    }

    impl_clone_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = HydraKllSketchAccumulator::new(self.row_num, self.col_num, self.k);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        // Rough estimate: each cell is a KLL sketch
        std::mem::size_of::<HydraKllSketchAccumulator>() + self.row_num * self.col_num * 4096
    }
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

/// Return `true` if `config` produces a keyed (MultipleSubpopulation) updater,
/// without allocating an updater object.
///
/// **Contract:** this must agree with every concrete `AccumulatorUpdater::is_keyed()`
/// implementation. When a new accumulator type is added, update both here and
/// in the corresponding struct.
pub fn config_is_keyed(config: &AggregationConfig) -> bool {
    matches!(
        config.aggregation_type,
        AggregationType::MultipleSubpopulation
            | AggregationType::MultipleSum
            | AggregationType::MultipleIncrease
            | AggregationType::MultipleMinMax
            | AggregationType::CountMinSketch
            | AggregationType::CountMinSketchWithHeap
            | AggregationType::CountSketch
            | AggregationType::CountSketchWithHeap
            | AggregationType::HydraKLL
    )
}

/// Top-k ranking quantity for the `*WithHeap` configs.
///
/// Selected by `parameters["weight_mode"]` (or alias `topk_weight`):
///   * `"value"` / `"sum"` → [`TopkWeight::Value`] (Σ value per key).
///   * `"count"` / `"frequency"` / `"freq"` → [`TopkWeight::Count`].
///
/// DEFAULT is `Value` (value-weighted). The heap variants previously
/// routed to the heap-LESS `CmsAccumulatorUpdater` (top-k unanswerable —
/// recall 0), so there is no count-weighted heap caller to regress;
/// value-weighting is the semantics `topk(sum_by_key(value))` needs, and
/// genuine frequency-top-k callers opt in with `weight_mode: count`.
fn topk_weight_param(config: &AggregationConfig) -> TopkWeight {
    match config
        .parameters
        .get("weight_mode")
        .or_else(|| config.parameters.get("topk_weight"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("count") | Some("frequency") | Some("freq") => TopkWeight::Count,
        // "value" / "sum" / unset / anything else → value-weighted default.
        _ => TopkWeight::Value,
    }
}

fn topk_weight_scale_param(config: &AggregationConfig) -> f64 {
    config
        .parameters
        .get("weight_scale")
        .and_then(|value| value.as_f64())
        .filter(|scale| scale.is_finite() && *scale > 0.0)
        .unwrap_or(1.0)
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Read the KLL `k` out of `SketchParams::Kll`. `accumulator_spec()`
/// always builds a `SketchKind` whose `SketchAlgorithm::Kll` is paired with
/// `SketchParams::Kll`, so the
/// other arm is unreachable from a `spec` this module builds itself.
fn kll_k(params: &SketchParams) -> u16 {
    match params {
        // Lossless: `accumulator_spec()` only ever stores a value that
        // already fit in `u16` (via `kll_k_param`'s own `u16::try_from`
        // fallback) widened to `u32`.
        SketchParams::Kll { k } => *k as u16,
        other => unreachable!(
            "accumulator_spec() paired SketchAlgorithm::Kll with non-Kll params: {other:?}"
        ),
    }
}

/// Read `(width, depth)` out of `SketchParams::Cms` or `::CountSketch`
/// — same shape, different variant per bare-sketch identity.
fn cms_dims(params: &SketchParams) -> (usize, usize) {
    match params {
        SketchParams::Cms { width, depth } | SketchParams::CountSketch { width, depth } => {
            (*width as usize, *depth as usize)
        }
        other => unreachable!(
            "accumulator_spec() paired SketchAlgorithm::Cms/CountSketch with unexpected params: {other:?}"
        ),
    }
}

/// Read `(width, depth, heap_size)` out of `SketchParams::CmsWithHeap`
/// or `::CountSketchWithHeap`.
fn cms_heap_dims(params: &SketchParams) -> (usize, usize, usize) {
    match params {
        SketchParams::CmsWithHeap {
            width,
            depth,
            heap_size,
        }
        | SketchParams::CountSketchWithHeap {
            width,
            depth,
            heap_size,
        } => (*width as usize, *depth as usize, *heap_size as usize),
        other => unreachable!(
            "accumulator_spec() paired a WithHeap SketchAlgorithm with unexpected params: {other:?}"
        ),
    }
}

/// Read the DDSketch relative-accuracy `alpha` out of `SketchParams::DDSketch`.
fn ddsketch_alpha(params: &SketchParams) -> f64 {
    match params {
        SketchParams::DDSketch { alpha } => *alpha,
        other => unreachable!(
            "accumulator_spec() paired SketchAlgorithm::DDSketch with non-DDSketch params: {other:?}"
        ),
    }
}

/// Create an appropriate `AccumulatorUpdater` from an `AggregationConfig`.
///
/// Dispatches on [`asap_types::AccumulatorSpec`] — `SummaryFamilyType` identity
/// plus the keyed/unkeyed `grouping` axis — instead of the pre-Step-5
/// `AggregationType` + `aggregation_sub_type` string combo. See
/// `asap_types::accumulator_spec`'s module doc for why min/max direction,
/// HydraKLL's `(row, col)` tiling, and top-k `weight_mode` still read
/// `config` directly rather than going through Planner family parameters —
/// none of those three have a field in the Planner-owned types.
pub fn create_accumulator_updater(config: &AggregationConfig) -> Box<dyn AccumulatorUpdater> {
    let spec = match config.accumulator_spec() {
        Ok(spec) => spec,
        // Three fallback paths, preserved verbatim from the pre-Step-5
        // dispatch: same warning text, same default updater per case
        // (Single- and MultipleSubpopulation default to *different*
        // updaters — see `AccumulatorSpecError`'s doc).
        Err(AccumulatorSpecError::UnknownSingleSubpopulationSubType(sub_type)) => {
            tracing::warn!(
                "Unknown SingleSubpopulation sub_type '{}', defaulting to Sum",
                sub_type
            );
            return Box::new(SumAccumulatorUpdater::new());
        }
        Err(AccumulatorSpecError::UnknownMultipleSubpopulationSubType(sub_type)) => {
            tracing::warn!(
                "Unknown MultipleSubpopulation sub_type '{}', defaulting to Sum",
                sub_type
            );
            return Box::new(MultipleSumAccumulatorUpdater::new());
        }
        Err(AccumulatorSpecError::UnmappedAggregationType(other)) => {
            tracing::warn!(
                "Unknown aggregation_type '{:?}', defaulting to SingleSubpopulation Sum",
                other
            );
            return Box::new(SumAccumulatorUpdater::new());
        }
    };

    let keyed = spec.grouping.is_some();

    match (&spec.family, keyed) {
        (SummaryFamilyType::ExactAggregate(ExactKind::Sum, _), false) => {
            Box::new(SumAccumulatorUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Sum, _), true) => {
            Box::new(MultipleSumAccumulatorUpdater::new())
        }

        // Min/max direction isn't part of `ExactParams::MinMax`
        // (upstream models no direction axis) — read straight off
        // `aggregation_sub_type`, exactly as the pre-Step-5 dispatch did
        // for the direct `AggregationType::MinMax`/`MultipleMinMax`
        // arms. `accumulator_spec()` only resolves a wrapper's sub_type
        // to `ExactKind::MinMax` for an exact "Min"/"min"/"Max"/"max"
        // match, so re-deriving via `eq_ignore_ascii_case("max")` here
        // reproduces the same true/false split for that path too.
        (SummaryFamilyType::ExactAggregate(ExactKind::MinMax, _), false) => Box::new(
            MinMaxAccumulatorUpdater::new(config.aggregation_sub_type.eq_ignore_ascii_case("max")),
        ),
        (SummaryFamilyType::ExactAggregate(ExactKind::MinMax, _), true) => {
            Box::new(MultipleMinMaxAccumulatorUpdater::new(
                config.aggregation_sub_type.eq_ignore_ascii_case("max"),
            ))
        }

        (SummaryFamilyType::ExactAggregate(ExactKind::Increase, _), false) => {
            Box::new(IncreaseAccumulatorUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Increase, _), true) => {
            Box::new(MultipleIncreaseAccumulatorUpdater::new())
        }

        (SummaryFamilyType::Sketch(kind, _), false)
            if kind.algorithm() == &SketchAlgorithm::Kll =>
        {
            Box::new(KllAccumulatorUpdater::new(kll_k(kind.params())))
        }
        // HydraKLL: `k` comes off the typed params like the unkeyed case,
        // but the `(row, col)` tiling grid has no `SketchParams::Kll`
        // field to live in (see `asap_types::accumulator_spec`'s module
        // doc) — read it the same way bare CMS does, via `cms_params`.
        (SummaryFamilyType::Sketch(kind, _), true) if kind.algorithm() == &SketchAlgorithm::Kll => {
            let (row_num, col_num) = cms_params(config);
            Box::new(HydraKllAccumulatorUpdater::new(
                row_num,
                col_num,
                kll_k(kind.params()),
            ))
        }

        // Bare CMS: point-frequency only, min-of-rows estimator. `keyed=false`
        // can't actually arise here today (no `AggregationType` resolves to
        // bare Cms unkeyed — see accumulator_spec.rs), matched anyway as a
        // safe default.
        (SummaryFamilyType::Sketch(kind, _), _) if kind.algorithm() == &SketchAlgorithm::Cms => {
            let (row_num, col_num) = cms_dims(kind.params());
            Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
        }

        // Bare CountSketch: real median-of-signed-rows estimator, via the
        // dedicated `CountSketchAccumulatorUpdater` (previously conflated
        // with `CmsAccumulatorUpdater`'s CMS min-math — see that struct's
        // doc).
        (SummaryFamilyType::Sketch(kind, _), _)
            if kind.algorithm() == &SketchAlgorithm::CountSketch =>
        {
            let (row_num, col_num) = cms_dims(kind.params());
            Box::new(CountSketchAccumulatorUpdater::new(row_num, col_num))
        }

        // Heap-bearing top-k variant (raw-input ingest path): route to the
        // real `CmsHeapAccumulatorUpdater` so the per-policy top-k heap is
        // BUILT (heap-less CMS could not answer `topk(...)` — recall 0).
        // Keyed by the configured group-by `aggregated_labels` (e.g. `host`),
        // ranked by Σ value per key by default (`weight_mode: value`), or Σ
        // count for genuine frequency-top-k (`weight_mode: count`). The OTLP
        // modified-sketch path builds the heap agent-side and uses
        // `SketchEnvelope` ingest, not this raw arm.
        (SummaryFamilyType::Sketch(kind, _), _)
            if kind.algorithm() == &SketchAlgorithm::CmsWithHeap =>
        {
            let (row_num, col_num, heap_size) = cms_heap_dims(kind.params());
            Box::new(CmsHeapAccumulatorUpdater::with_weight_scale(
                row_num,
                col_num,
                heap_size,
                topk_weight_param(config),
                topk_weight_scale_param(config),
            ))
        }

        // Heap-bearing top-k variant, real CountSketch math (previously
        // conflated with `CmsHeapAccumulatorUpdater`'s CMS-with-heap — see
        // `CountSketchWithHeapAccumulatorUpdater`'s doc).
        (SummaryFamilyType::Sketch(kind, _), _)
            if kind.algorithm() == &SketchAlgorithm::CountSketchWithHeap =>
        {
            let (row_num, col_num, heap_size) = cms_heap_dims(kind.params());
            Box::new(CountSketchWithHeapAccumulatorUpdater::with_weight_scale(
                row_num,
                col_num,
                heap_size,
                topk_weight_param(config),
                topk_weight_scale_param(config),
            ))
        }

        (SummaryFamilyType::Sketch(kind, _), _)
            if kind.algorithm() == &SketchAlgorithm::DDSketch =>
        {
            Box::new(DDSketchAccumulatorUpdater::new(ddsketch_alpha(
                kind.params(),
            )))
        }

        // unsupported HLL, Count, Rate, Kmv, and Theta families: no
        // `AggregationType` resolves to one of these via
        // `accumulator_spec()`'s `Ok` path today — HLL is caught by
        // `AccumulatorSpecError::UnmappedAggregationType` above (see its
        // doc for why: a pre-existing gap, not introduced here), and the
        // other four have no `AggregationType` counterpart at all. Kept
        // as an explicit warning fallback rather than `unreachable!()`
        // so a future `SummaryFamilyType` this dispatch doesn't yet know how
        // to build fails safe instead of panicking.
        (other_family, keyed) => {
            tracing::warn!(
                "SummaryFamilyType {:?} (keyed={}) has no accumulator_factory mapping, defaulting to Sum",
                other_family,
                keyed
            );
            Box::new(SumAccumulatorUpdater::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;

    #[test]
    fn test_sum_updater() {
        let mut updater = SumAccumulatorUpdater::new();
        assert!(!updater.is_keyed());

        updater.update_single(1.0, 1000);
        updater.update_single(2.0, 2000);
        updater.update_single(3.0, 3000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "SumAccumulator");
    }

    #[test]
    fn test_minmax_updater() {
        let mut updater = MinMaxAccumulatorUpdater::new(true);
        updater.update_single(5.0, 1000);
        updater.update_single(3.0, 2000);
        updater.update_single(7.0, 3000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "MinMaxAccumulator");
    }

    #[test]
    fn test_increase_updater() {
        let mut updater = IncreaseAccumulatorUpdater::new();
        updater.update_single(10.0, 1000);
        updater.update_single(15.0, 2000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "IncreaseAccumulator");
    }

    #[test]
    fn test_kll_updater() {
        let mut updater = KllAccumulatorUpdater::new(200);
        for i in 1..=10 {
            updater.update_single(i as f64, i * 1000);
        }

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "DatasketchesKLLAccumulator");
    }

    #[test]
    fn test_multiple_sum_updater() {
        let mut updater = MultipleSumAccumulatorUpdater::new();
        assert!(updater.is_keyed());

        let key_a = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        let key_b = KeyByLabelValues::new_with_labels(vec!["b".to_string()]);

        updater.update_keyed(&key_a, 1.0, 1000);
        updater.update_keyed(&key_b, 2.0, 2000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "MultipleSumAccumulator");
    }

    #[test]
    fn bare_cms_adds_sample_values() {
        let mut updater = CmsAccumulatorUpdater::new(4, 256);
        let key = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        updater.update_keyed(&key, 2.0, 1000);
        updater.update_keyed(&key, 3.0, 2000);
        updater.update_keyed(&key, 5.0, 3000);

        let acc = updater.snapshot_accumulator();
        let cms = acc
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .expect("should be a CountMinSketchAccumulator");
        assert_eq!(cms.query_key(&key), 10.0);
    }

    #[test]
    fn bare_count_sketch_adds_sample_values() {
        let mut updater = CountSketchAccumulatorUpdater::new(5, 256);
        let key = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        updater.update_keyed(&key, 2.0, 1000);
        updater.update_keyed(&key, 3.0, 2000);
        updater.update_keyed(&key, 5.0, 3000);

        let acc = updater.snapshot_accumulator();
        let count_sketch = acc
            .as_any()
            .downcast_ref::<CountSketchAccumulator>()
            .expect("should be a CountSketchAccumulator");
        assert_eq!(count_sketch.query_key(&key), 10.0);
    }

    #[test]
    fn test_reset_clears_state() {
        let mut updater = SumAccumulatorUpdater::new();
        updater.update_single(100.0, 1000);
        updater.reset();
        // After reset, should produce a fresh accumulator
        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "SumAccumulator");
    }

    #[test]
    fn test_config_is_keyed() {
        use std::collections::HashMap;

        let make_config = |agg_type: AggregationType, sub_type: &str| {
            AggregationConfig::new(
                agg_type,
                sub_type.to_string(),
                HashMap::new(),
                asap_types::KeyByLabelNames::new(vec![]),
                asap_types::KeyByLabelNames::new(vec![]),
                asap_types::KeyByLabelNames::new(vec![]),
                String::new(),
                60,
                0,
                WindowKind::Tumbling,
                "m".to_string(),
                "m".to_string(),
                None,
                None,
                None,
            )
        };

        // Non-keyed types
        assert!(!config_is_keyed(&make_config(
            AggregationType::SingleSubpopulation,
            "Sum"
        )));
        assert!(!config_is_keyed(&make_config(AggregationType::Sum, "")));
        assert!(!config_is_keyed(&make_config(
            AggregationType::DatasketchesKLL,
            ""
        )));
        assert!(!config_is_keyed(&make_config(
            AggregationType::Increase,
            ""
        )));

        // Keyed types
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleSubpopulation,
            "Sum"
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleSum,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleIncrease,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleMinMax,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::CountMinSketch,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::CountMinSketchWithHeap,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::CountSketch,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::CountSketchWithHeap,
            ""
        )));
        assert!(config_is_keyed(&make_config(AggregationType::HydraKLL, "")));

        // Verify agreement with updater.is_keyed()
        for (agg_type, sub_type) in &[
            (AggregationType::SingleSubpopulation, "Sum"),
            (AggregationType::MultipleSubpopulation, "Sum"),
            (AggregationType::MultipleSum, ""),
            (AggregationType::DatasketchesKLL, ""),
            (AggregationType::CountMinSketch, ""),
        ] {
            let config = make_config(*agg_type, sub_type);
            let updater = create_accumulator_updater(&config);
            assert_eq!(
                config_is_keyed(&config),
                updater.is_keyed(),
                "config_is_keyed disagrees with updater.is_keyed() for type={:?}",
                agg_type
            );
        }
    }

    #[test]
    fn test_kll_k_param_capital_k() {
        // SingleSubpopulation/KLL with capital "K" param should use it (not default to 200)
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("K".to_string(), serde_json::Value::from(50_u64));
        let config = AggregationConfig::new(
            AggregationType::SingleSubpopulation,
            "DatasketchesKLL".to_string(),
            params,
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            0,
            WindowKind::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
        );
        let updater = create_accumulator_updater(&config);
        let acc = updater.snapshot_accumulator();
        let kll = acc
            .as_any()
            .downcast_ref::<crate::precompute_engine::operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator>()
            .expect("should be KLL");
        assert_eq!(kll.inner.k, 50, "k should be 50 from capital-K param");
    }

    #[test]
    fn cms_params_reads_canonical_w_d_keys() {
        use std::collections::HashMap;
        // Canonical `w`/`d` form — what the control plane's
        // `sketch_params_to_json` emits and what asapcollector
        // streaming-config YAMLs ship (asapcollector PR
        // `sync-config-canonical-w-d` migrated them in lock-step
        // with the legacy-fallback removal).
        let mut params = HashMap::new();
        params.insert("d".to_string(), serde_json::Value::from(7_u64));
        params.insert("w".to_string(), serde_json::Value::from(2048_u64));
        let config = AggregationConfig::new(
            AggregationType::CountMinSketch,
            String::new(),
            params,
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            0,
            WindowKind::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
        );
        assert_eq!(super::cms_params(&config), (7, 2048));

        // Empty params — defaults `(4, 1000)`.
        let empty_config = AggregationConfig::new(
            AggregationType::CountMinSketch,
            String::new(),
            HashMap::new(),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            0,
            WindowKind::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
        );
        assert_eq!(super::cms_params(&empty_config), (4, 1000));
    }

    // -----------------------------------------------------------------
    // value-weighted vs count-weighted top-k (fix/value-weighted-topk)
    // -----------------------------------------------------------------

    /// Build a `*WithHeap` config keyed by group-by label `host`, with the
    /// given `weight_mode` param (None → default = value-weighted).
    fn topk_config(agg_type: AggregationType, weight_mode: Option<&str>) -> AggregationConfig {
        use std::collections::HashMap;
        let mut params = HashMap::new();
        // Small, deterministic geometry; heap big enough to hold all hosts.
        params.insert("d".to_string(), serde_json::Value::from(4_u64));
        params.insert("w".to_string(), serde_json::Value::from(256_u64));
        params.insert("heap_size".to_string(), serde_json::Value::from(8_u64));
        if let Some(m) = weight_mode {
            params.insert("weight_mode".to_string(), serde_json::Value::from(m));
        }
        AggregationConfig::new(
            agg_type,
            String::new(),
            params,
            asap_types::KeyByLabelNames::new(vec![]),
            // group-by = `host` (NOT the metric label `item`).
            asap_types::KeyByLabelNames::new(vec!["host".to_string()]),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            0,
            WindowKind::Tumbling,
            "cpu".to_string(),
            "cpu".to_string(),
            None,
            None,
            None,
        )
    }

    /// Read the heap as a sorted-descending `(host, value)` list from a
    /// finished accumulator — mirrors the read-side reducer's
    /// `topk_heap_items()` + sort-by-value-desc.
    fn ranked_topk(acc: &dyn AggregateCore) -> Vec<(String, f64)> {
        let heap = acc
            .as_any()
            .downcast_ref::<CountMinSketchWithHeapAccumulator>()
            .expect("WithHeap config must build a heap accumulator");
        let mut items = heap.inner.topk_heap_items();
        items.sort_by(|a, b| {
            b.value
                .partial_cmp(&a.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        items.into_iter().map(|i| (i.key, i.value)).collect()
    }

    /// Same as `ranked_topk`, but for the real `CountSketchWithHeapAccumulator`
    /// (median-of-signed-rows) built by `SketchAlgorithm::CountSketchWithHeap` —
    /// no longer conflated with the CMS-family accumulator above.
    fn ranked_topk_cs(acc: &dyn AggregateCore) -> Vec<(String, f64)> {
        let heap = acc
            .as_any()
            .downcast_ref::<CountSketchWithHeapAccumulator>()
            .expect("CountSketchWithHeap config must build a CountSketchWithHeapAccumulator");
        let mut items = heap.inner.topk_heap_items();
        items.sort_by(|a, b| {
            b.value
                .partial_cmp(&a.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        items.into_iter().map(|i| (i.key, i.value)).collect()
    }

    fn host_key(h: &str) -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec![h.to_string()])
    }

    /// A multi-host CPU stream where value-rank and count-rank DISAGREE,
    /// so the test distinguishes a correct value-weighted answer from the
    /// (buggy) count-weighted one.
    ///
    ///   host-a: ONE big sample  -> value 100, count 1
    ///   host-b: TWO mid samples -> value  60, count 2
    ///   host-c: FOUR tiny ones  -> value  20, count 4
    ///
    /// By Σ VALUE: a(100) > b(60) > c(20)  → top-2 = [a, b]
    /// By Σ COUNT: c(4)   > b(2)  > a(1)   → top-2 = [c, b]
    const STREAM: &[(&str, f64)] = &[
        ("host-a", 100.0),
        ("host-b", 30.0),
        ("host-b", 30.0),
        ("host-c", 5.0),
        ("host-c", 5.0),
        ("host-c", 5.0),
        ("host-c", 5.0),
    ];

    fn feed_stream(updater: &mut dyn AccumulatorUpdater) {
        for (i, (host, val)) in STREAM.iter().enumerate() {
            updater.update_keyed(&host_key(host), *val, 1_000 + i as i64);
        }
    }

    #[test]
    fn value_weighted_topk_ranks_hosts_by_sum_of_value() {
        // DEFAULT mode (no weight_mode param) must be value-weighted.
        let config = topk_config(AggregationType::CountMinSketchWithHeap, None);
        let mut updater = create_accumulator_updater(&config);
        assert!(updater.is_keyed());

        feed_stream(&mut *updater);
        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "CountMinSketchWithHeapAccumulator");

        let ranked = ranked_topk(&*acc);
        // Σ value: host-a=100, host-b=60, host-c=20.
        assert_eq!(ranked[0].0, "host-a", "top host by Σ value");
        assert_eq!(ranked[0].1, 100.0);
        assert_eq!(ranked[1].0, "host-b");
        assert_eq!(ranked[1].1, 60.0);
        assert_eq!(ranked[2].0, "host-c");
        assert_eq!(ranked[2].1, 20.0);

        // Recall of value-weighted top-2 against ground truth {host-a, host-b}.
        let truth: std::collections::HashSet<&str> = ["host-a", "host-b"].into_iter().collect();
        let got: std::collections::HashSet<&str> =
            ranked.iter().take(2).map(|(h, _)| h.as_str()).collect();
        let recall = got.intersection(&truth).count() as f64 / truth.len() as f64;
        assert_eq!(recall, 1.0, "value-weighted top-2 recall must be 1.0");
    }

    #[test]
    fn counter_delta_scale_preserves_sub_unit_membership_weights() {
        let mut config = topk_config(
            AggregationType::CountMinSketchWithHeap,
            Some("counter_delta"),
        );
        config
            .parameters
            .insert("weight_scale".into(), serde_json::json!(1_000_000));
        let mut updater = create_accumulator_updater(&config);
        updater.update_keyed(&host_key("payment"), 0.004, 1_000);
        updater.update_keyed(&host_key("order"), 0.002, 1_000);
        let ranked = ranked_topk(&*updater.take_accumulator());
        assert_eq!(ranked[0], ("payment".into(), 4_000.0));
        assert_eq!(ranked[1], ("order".into(), 2_000.0));
    }

    #[test]
    fn count_weighted_topk_still_ranks_by_occurrence_frequency() {
        // Opt-in frequency-top-k: weight_mode=count must rank by event count.
        let config = topk_config(AggregationType::CountMinSketchWithHeap, Some("count"));
        let mut updater = create_accumulator_updater(&config);
        feed_stream(&mut *updater);
        let acc = updater.take_accumulator();

        let ranked = ranked_topk(&*acc);
        // Σ count: host-c=4, host-b=2, host-a=1.
        assert_eq!(ranked[0].0, "host-c", "top host by Σ count");
        assert_eq!(ranked[0].1, 4.0);
        assert_eq!(ranked[1].0, "host-b");
        assert_eq!(ranked[1].1, 2.0);
        assert_eq!(ranked[2].0, "host-a");
        assert_eq!(ranked[2].1, 1.0);
    }

    #[test]
    fn countsketch_with_heap_also_routes_to_value_weighted_heap() {
        // CountSketchWithHeap gets its OWN dedicated updater/accumulator
        // (real median-of-signed-rows math) — same value-weighted default
        // as the CMS-family heap path, but no longer conflated with it.
        let config = topk_config(AggregationType::CountSketchWithHeap, None);
        let mut updater = create_accumulator_updater(&config);
        feed_stream(&mut *updater);
        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "CountSketchWithHeapAccumulator");
        let ranked = ranked_topk_cs(&*acc);
        assert_eq!(ranked[0].0, "host-a");
        assert_eq!(ranked[0].1, 100.0);
    }

    #[test]
    fn topk_weight_param_parses_modes() {
        assert_eq!(
            super::topk_weight_param(&topk_config(AggregationType::CountMinSketchWithHeap, None)),
            TopkWeight::Value,
            "unset defaults to value-weighted"
        );
        for m in ["value", "sum", "VALUE"] {
            assert_eq!(
                super::topk_weight_param(&topk_config(
                    AggregationType::CountMinSketchWithHeap,
                    Some(m)
                )),
                TopkWeight::Value,
            );
        }
        for m in ["count", "frequency", "freq", "COUNT"] {
            assert_eq!(
                super::topk_weight_param(&topk_config(
                    AggregationType::CountMinSketchWithHeap,
                    Some(m)
                )),
                TopkWeight::Count,
            );
        }
    }
}
