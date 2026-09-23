use crate::accumulators::{
    CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, CountSketchAccumulator,
    CountSketchWithHeapAccumulator, DDSketchAccumulator, DatasketchesKLLAccumulator,
    HydraKllSketchAccumulator, IncreaseAccumulator, KeyedCounterState, KeyedMaxState,
    KeyedMinState, KeyedSumCountAccumulator, MaxAccumulator, MinAccumulator, SumAccumulator,
};
#[cfg(test)]
use crate::AggregationType;
use crate::{AggregateCore, KeyByLabelValues, Measurement};
#[cfg(test)]
use asap_types::aggregation_config::PrecomputeMaterialization;
// Production dispatch consumes Planner SummaryAgg payloads directly. The
// config adapter below is compiled only for isolated historical kernel tests.
use crate::accumulators::hll_sketch_accumulator::HllSketchAccumulator;
use crate::accumulators::univmon_accumulator::UnivMonAccumulator;
#[cfg(test)]
use asap_types::accumulator_spec::cms_params;
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

/// Shared update interface for query-time and maintenance-time accumulation.
///
/// This provides a uniform interface over all accumulator types so that the
/// worker loop doesn't need to know which concrete type it's dealing with.
pub trait AccumulatorUpdater: Send {
    /// Validate an immutable maintenance input before an updater can silently
    /// discard a value outside its representable domain.
    fn validate_single_input(&self, value: f64) -> Result<(), String> {
        if value.is_finite() {
            Ok(())
        } else {
            Err("accumulator input must be finite".into())
        }
    }

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
// MinAccumulatorUpdater / MaxAccumulatorUpdater
// ---------------------------------------------------------------------------

macro_rules! extremum_updater {
    ($updater:ident, $acc:ty) => {
        #[derive(Default)]
        pub struct $updater {
            acc: $acc,
        }

        impl $updater {
            pub fn new() -> Self {
                Self::default()
            }
        }

        impl AccumulatorUpdater for $updater {
            fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
                self.acc.update(value);
            }

            fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
                self.update_single(value, timestamp_ms);
            }

            impl_clone_accumulator_methods!(acc);

            fn reset(&mut self) {
                self.acc = <$acc>::new();
            }

            fn is_keyed(&self) -> bool {
                false
            }

            fn memory_usage_bytes(&self) -> usize {
                std::mem::size_of::<$acc>()
            }
        }
    };
}

extremum_updater!(MinAccumulatorUpdater, MinAccumulator);
extremum_updater!(MaxAccumulatorUpdater, MaxAccumulator);

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
    fn validate_single_input(&self, value: f64) -> Result<(), String> {
        let (minimum, maximum) =
            asap_sketchlib::sketches::ddsketch::ddsketch_indexable_bounds(self.alpha);
        if value.is_finite() && value > 0.0 && value >= minimum && value <= maximum {
            Ok(())
        } else {
            Err("DDS maintenance input is outside its positive representable domain".into())
        }
    }

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
// KeyedSumCountAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct KeyedSumCountAccumulatorUpdater {
    acc: KeyedSumCountAccumulator,
}

impl KeyedSumCountAccumulatorUpdater {
    pub fn new() -> Self {
        Self::for_family(ExactKind::Sum)
    }

    pub fn for_family(family: ExactKind) -> Self {
        Self {
            acc: KeyedSumCountAccumulator::for_family(family),
        }
    }
}

impl Default for KeyedSumCountAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for KeyedSumCountAccumulatorUpdater {
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
        self.acc = KeyedSumCountAccumulator::for_family(self.acc.family.clone());
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<KeyedSumCountAccumulator>()
            + self.acc.sums.len() * (std::mem::size_of::<KeyByLabelValues>() + 16)
    }
}

// ---------------------------------------------------------------------------
// KeyedMinStateUpdater / KeyedMaxStateUpdater
// ---------------------------------------------------------------------------

macro_rules! multiple_extremum_updater {
    ($updater:ident, $acc:ty) => {
        #[derive(Default)]
        pub struct $updater {
            acc: $acc,
        }

        impl $updater {
            pub fn new() -> Self {
                Self::default()
            }
        }

        impl AccumulatorUpdater for $updater {
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
                self.acc = <$acc>::new();
            }

            fn is_keyed(&self) -> bool {
                true
            }

            fn memory_usage_bytes(&self) -> usize {
                std::mem::size_of::<$acc>()
                    + self.acc.values.len() * (std::mem::size_of::<KeyByLabelValues>() + 8)
            }
        }
    };
}

multiple_extremum_updater!(KeyedMinStateUpdater, KeyedMinState);
multiple_extremum_updater!(KeyedMaxStateUpdater, KeyedMaxState);

// ---------------------------------------------------------------------------
// KeyedCounterStateUpdater
// ---------------------------------------------------------------------------

pub struct KeyedCounterStateUpdater {
    acc: KeyedCounterState,
}

impl KeyedCounterStateUpdater {
    pub fn new() -> Self {
        Self {
            acc: KeyedCounterState::new(),
        }
    }
}

impl Default for KeyedCounterStateUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for KeyedCounterStateUpdater {
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
        self.acc = KeyedCounterState::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<KeyedCounterState>()
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

#[cfg(test)]
/// Return `true` if `config` produces a keyed (MultipleSubpopulation) updater,
/// without allocating an updater object.
///
/// **Contract:** this must agree with every concrete `AccumulatorUpdater::is_keyed()`
/// implementation. When a new accumulator type is added, update both here and
/// in the corresponding struct.
pub fn config_is_keyed(config: &PrecomputeMaterialization) -> bool {
    config
        .accumulator_spec()
        .expect("valid fixture")
        .grouping
        .is_some()
}

/// Top-k ranking quantity, selected by `weight_mode` or its alias `topk_weight`.
///
/// * `value` / `sum`: sum values per key (default).
/// * `count` / `frequency` / `freq`: count occurrences per key.
#[cfg(test)]
fn topk_weight_param(config: &PrecomputeMaterialization) -> TopkWeight {
    match config.sample_update_rule() {
        asap_types::SampleUpdateRule::Count => TopkWeight::Count,
        asap_types::SampleUpdateRule::Value { .. }
        | asap_types::SampleUpdateRule::CounterDelta { .. } => TopkWeight::Value,
    }
}

#[cfg(test)]
fn topk_weight_scale_param(config: &PrecomputeMaterialization) -> f64 {
    match config.sample_update_rule() {
        asap_types::SampleUpdateRule::Value { scale } => scale,
        asap_types::SampleUpdateRule::CounterDelta { scale } => scale,
        asap_types::SampleUpdateRule::Count => 1.0,
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Read the KLL `k` out of `SketchParams::Kll`. `accumulator_spec()`
/// always builds a `SketchKind` whose `SketchAlgorithm::Kll` is paired with
/// `SketchParams::Kll`, so the
/// other arm is unreachable from a `spec` this module builds itself.
#[cfg(test)]
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

/// Read `(rows = depth, columns = width)` out of `SketchParams::Cms` or `::CountSketch`
/// — same shape, different variant per bare-sketch identity.
fn cms_dims(params: &SketchParams) -> (usize, usize) {
    match params {
        SketchParams::Cms { width, depth } | SketchParams::CountSketch { width, depth } => {
            (*depth as usize, *width as usize)
        }
        other => unreachable!(
            "accumulator_spec() paired SketchAlgorithm::Cms/CountSketch with unexpected params: {other:?}"
        ),
    }
}

/// Read `(rows = depth, columns = width, heap_size)` out of `SketchParams::CmsWithHeap`
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
        } => (*depth as usize, *width as usize, *heap_size as usize),
        other => unreachable!(
            "accumulator_spec() paired a WithHeap SketchAlgorithm with unexpected params: {other:?}"
        ),
    }
}

/// Read the DDSketch relative-accuracy `alpha` out of `SketchParams::DDSketch`.
#[cfg(test)]
fn ddsketch_alpha(params: &SketchParams) -> f64 {
    match params {
        SketchParams::DDSketch { alpha } => *alpha,
        other => unreachable!(
            "accumulator_spec() paired SketchAlgorithm::DDSketch with non-DDSketch params: {other:?}"
        ),
    }
}

/// Construct isolated payload fixtures for kernel/storage unit tests.
/// Production execution requires a validated Planner DAG program.
#[cfg(test)]
pub fn create_fixture_accumulator(
    config: &PrecomputeMaterialization,
) -> Box<dyn AccumulatorUpdater> {
    let spec = config
        .accumulator_spec()
        .expect("invalid isolated kernel fixture");

    let keyed = spec.grouping.is_some();

    match (&spec.family, keyed) {
        (SummaryFamilyType::ExactAggregate(ExactKind::Sum | ExactKind::Count, _), false) => {
            Box::new(SumAccumulatorUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Sum, _), true) => {
            Box::new(KeyedSumCountAccumulatorUpdater::for_family(ExactKind::Sum))
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Count, _), true) => Box::new(
            KeyedSumCountAccumulatorUpdater::for_family(ExactKind::Count),
        ),

        // Direction comes off the family itself now. It used to be read
        // back out of `aggregation_sub_type` because Planner had one
        // `MinMax` accumulator for both directions, which meant a config
        // whose sub_type was lost or misspelled silently built the wrong
        // extremum.
        (SummaryFamilyType::ExactAggregate(ExactKind::Min, _), false) => {
            Box::new(MinAccumulatorUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Min, _), true) => {
            Box::new(KeyedMinStateUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Max, _), false) => {
            Box::new(MaxAccumulatorUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Max, _), true) => {
            Box::new(KeyedMaxStateUpdater::new())
        }

        (SummaryFamilyType::ExactAggregate(ExactKind::Increase | ExactKind::Rate, _), false) => {
            Box::new(IncreaseAccumulatorUpdater::new())
        }
        (SummaryFamilyType::ExactAggregate(ExactKind::Increase | ExactKind::Rate, _), true) => {
            Box::new(KeyedCounterStateUpdater::new())
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

        // CountSketch uses the median-of-signed-rows estimator.
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

        // Heap-bearing CountSketch retains CountSketch estimation semantics.
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

        (SummaryFamilyType::Sketch(kind, _), false)
            if kind.algorithm() == &SketchAlgorithm::UnivMon =>
        {
            let SketchParams::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } = kind.params()
            else {
                unreachable!("validated UnivMon family parameters")
            };
            Box::new(UnivMonUpdater {
                acc: UnivMonAccumulator::new(
                    *heap_size as usize,
                    *sketch_rows as usize,
                    *sketch_cols as usize,
                    *layers as usize,
                )
                .expect("validated UnivMon dimensions"),
            })
        }

        (SummaryFamilyType::Sketch(kind, _), false)
            if kind.algorithm() == &SketchAlgorithm::Hll =>
        {
            let SketchParams::Hll { precision } = kind.params() else {
                unreachable!("validated HLL family parameters")
            };
            Box::new(HllUpdater {
                acc: HllSketchAccumulator::new(
                    asap_sketchlib::HllVariant::Regular,
                    u32::from(*precision),
                ),
            })
        }

        (other_family, keyed) => {
            panic!("unsupported isolated kernel fixture {other_family:?}, keyed={keyed}")
        }
    }
}

struct UnivMonUpdater {
    acc: UnivMonAccumulator,
}

struct HllUpdater {
    acc: HllSketchAccumulator,
}

impl AccumulatorUpdater for HllUpdater {
    fn is_keyed(&self) -> bool {
        false
    }
    fn memory_usage_bytes(&self) -> usize {
        self.acc.approx_memory_bytes()
    }
    fn update_single(&mut self, value: f64, _: i64) {
        if !value.is_nan() {
            let bits = if value == 0.0 { 0 } else { value.to_bits() };
            self.acc.inner.update(&bits.to_le_bytes());
        }
    }
    fn update_keyed(&mut self, _: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }
    impl_clone_accumulator_methods!(acc);
    fn reset(&mut self) {
        self.acc.reset_to_empty();
    }
}

impl AccumulatorUpdater for UnivMonUpdater {
    fn is_keyed(&self) -> bool {
        false
    }
    fn memory_usage_bytes(&self) -> usize {
        self.acc.approx_memory_bytes()
    }
    fn update_single(&mut self, value: f64, _: i64) {
        self.acc
            .insert_sample(value)
            .expect("UnivMon sample counter overflow");
    }
    fn update_keyed(&mut self, _: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }
    impl_clone_accumulator_methods!(acc);
    fn reset(&mut self) {
        self.acc.reset_to_empty();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;

    #[test]
    fn immutable_dds_inputs_reject_nonpositive_and_unrepresentable_values() {
        let updater = DDSketchAccumulatorUpdater::new(0.01);
        for value in [-20.0, -0.0, 0.0, f64::NAN, f64::INFINITY, f64::MAX] {
            assert!(updater.validate_single_input(value).is_err());
        }
        for value in [0.5, 20.0, 40.0] {
            assert!(updater.validate_single_input(value).is_ok());
        }
    }

    /// Both cardinality implementations consume values, with a single signed-zero identity.
    #[test]
    fn hll_and_univmon_raw_updates_share_value_identity() {
        for family in [AggregationType::HLL, AggregationType::UnivMon] {
            let config = PrecomputeMaterialization::new(
                family,
                String::new(),
                Default::default(),
                asap_types::KeyByLabelNames::new(vec![]),
                asap_types::KeyByLabelNames::new(vec![]),
                asap_types::KeyByLabelNames::new(vec![]),
                String::new(),
                60,
                60,
                WindowKind::Tumbling,
                "m".into(),
                "m".into(),
                None,
                None,
                None,
            );
            let mut updater = create_fixture_accumulator(&config);
            for value in [0.0, -0.0, 2.0, 2.0, f64::NAN] {
                updater.update_single(value, 1000);
            }
            let state = updater.take_accumulator();
            assert_eq!(state.get_accumulator_type(), family);
            let estimate = state
                .query_statistic(
                    asap_types::Statistic::Cardinality,
                    &None,
                    &Default::default(),
                )
                .unwrap();
            assert!((estimate - 2.0).abs() < 0.05, "{family:?}: {estimate}");
            assert!(updater.memory_usage_bytes() >= 4096);
            let empty = updater
                .snapshot_accumulator()
                .query_statistic(
                    asap_types::Statistic::Cardinality,
                    &None,
                    &Default::default(),
                )
                .unwrap();
            assert_eq!(empty, 0.0);
        }
    }

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
        let mut updater = MaxAccumulatorUpdater::new();
        updater.update_single(5.0, 1000);
        updater.update_single(3.0, 2000);
        updater.update_single(7.0, 3000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "MaxAccumulator");
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
        let mut updater = KeyedSumCountAccumulatorUpdater::new();
        assert!(updater.is_keyed());

        let key_a = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        let key_b = KeyByLabelValues::new_with_labels(vec!["b".to_string()]);

        updater.update_keyed(&key_a, 1.0, 1000);
        updater.update_keyed(&key_b, 2.0, 2000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "KeyedSumCountAccumulator");
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
            PrecomputeMaterialization::new(
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
        let mut keyed = make_config(AggregationType::Sum, "");
        keyed.aggregated_labels = asap_types::KeyByLabelNames::new(vec!["host".into()]);
        assert!(config_is_keyed(&keyed));
        let mut keyed = make_config(AggregationType::Increase, "");
        keyed.aggregated_labels = asap_types::KeyByLabelNames::new(vec!["host".into()]);
        assert!(config_is_keyed(&keyed));
        let mut keyed = make_config(AggregationType::Max, "");
        keyed.aggregated_labels = asap_types::KeyByLabelNames::new(vec!["host".into()]);
        assert!(config_is_keyed(&keyed));
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
            (AggregationType::Sum, ""),
            (AggregationType::DatasketchesKLL, ""),
            (AggregationType::CountMinSketch, ""),
        ] {
            let config = make_config(*agg_type, sub_type);
            let updater = create_fixture_accumulator(&config);
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
        let config = PrecomputeMaterialization::new(
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
        let updater = create_fixture_accumulator(&config);
        let acc = updater.snapshot_accumulator();
        let kll = acc
            .as_any()
            .downcast_ref::<crate::accumulators::datasketches_kll_accumulator::DatasketchesKLLAccumulator>()
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
        let config = PrecomputeMaterialization::new(
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
        let empty_config = PrecomputeMaterialization::new(
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
    fn topk_config(
        agg_type: AggregationType,
        weight_mode: Option<&str>,
    ) -> PrecomputeMaterialization {
        use std::collections::HashMap;
        let mut params = HashMap::new();
        // Small, deterministic geometry; heap big enough to hold all hosts.
        params.insert("d".to_string(), serde_json::Value::from(4_u64));
        params.insert("w".to_string(), serde_json::Value::from(256_u64));
        params.insert("heap_size".to_string(), serde_json::Value::from(8_u64));
        if let Some(m) = weight_mode {
            params.insert("weight_mode".to_string(), serde_json::Value::from(m));
        }
        PrecomputeMaterialization::new(
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
        let mut updater = create_fixture_accumulator(&config);
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
        use planner_types::post_asap::{
            EntityIdentity, NonNegativeWeightProof, SummaryInputExpr, SummaryUpdate, WeightDomain,
        };
        let config = topk_config(AggregationType::CountMinSketchWithHeap, None);
        let family = config.accumulator_spec().unwrap().family;
        let input = SummaryUpdate {
            item: Some(SummaryInputExpr::Column(
                planner_types::pre_asap::ColumnRef::Named("host".into()),
            )),
            weight: SummaryInputExpr::ResetAwareCounterDelta {
                value: planner_types::pre_asap::ColumnRef::SampleValue,
                series: EntityIdentity::PromqlLabelSet { excluding: vec![] },
            },
            weight_domain: WeightDomain::NonNegative {
                proof: NonNegativeWeightProof::ResetAwareCounterDerivative,
            },
        };
        let mut updater = create_planner_accumulator(&family, &input, &Default::default()).unwrap();
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
        let mut updater = create_fixture_accumulator(&config);
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
        let mut updater = create_fixture_accumulator(&config);
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

#[cfg(test)]
mod planner_family_regression {
    use super::*;
    use asap_types::{enums::WindowKind, KeyByLabelNames};

    // Every installed exact producer must retain its family in runtime state.
    #[test]
    fn exact_state_identity_survives_factory_and_reset() {
        for kind in [
            AggregationType::Sum,
            AggregationType::Count,
            AggregationType::Rate,
            AggregationType::Increase,
            AggregationType::Min,
            AggregationType::Max,
        ] {
            let config = PrecomputeMaterialization::new(
                kind,
                String::new(),
                Default::default(),
                KeyByLabelNames::empty(),
                KeyByLabelNames::empty(),
                KeyByLabelNames::empty(),
                String::new(),
                60,
                60,
                WindowKind::Tumbling,
                String::new(),
                "metric".into(),
                None,
                None,
                None,
            );
            let mut updater = create_planner_accumulator(
                &config.accumulator_spec().unwrap().family,
                &planner_types::post_asap::SummaryUpdate::column(
                    planner_types::pre_asap::ColumnRef::SampleValue,
                ),
                &Default::default(),
            )
            .unwrap();
            updater.update_single(4.0, 1000);
            updater.update_single(7.0, 2000);
            assert_eq!(updater.take_accumulator().get_accumulator_type(), kind);
            assert_eq!(updater.snapshot_accumulator().get_accumulator_type(), kind);
        }
    }
}

/// Construct the kernel declared by a Planner SummaryAgg. No backend config
/// tags participate in this dispatch and unsupported payloads are errors.
pub fn create_planner_accumulator(
    family: &SummaryFamilyType,
    input: &planner_types::post_asap::SummaryUpdate,
    grouping: &planner_types::post_asap::GroupingStrategy,
) -> Result<Box<dyn AccumulatorUpdater>, String> {
    crate::capability::validate_summary_kernel(family, input, grouping)?;
    use planner_types::post_asap::GroupingStrategy;
    if grouping != &GroupingStrategy::PerSubpopulationInstance {
        return Err("shared summary grouping requires a supported Planner Hydra kernel".into());
    }
    if matches!(family, SummaryFamilyType::ExactAggregate(..)) {
        return Ok(Box::new(PlannerExactUpdater {
            acc: crate::accumulators::exact_accumulator::ExactAccumulator::new(
                family.clone(),
                input.item.is_some(),
            )?,
        }));
    }
    let SummaryFamilyType::Sketch(kind, family_grouping) = family else {
        return Err(format!("unsupported Planner summary family {family:?}"));
    };
    if family_grouping != grouping {
        return Err("Planner family and operator grouping disagree".into());
    }
    // Heap counters use fixed-point storage for fractional counter deltas.
    // This encodes the selected update; it does not choose another family.
    let weight_scale = if matches!(
        input.weight,
        planner_types::post_asap::SummaryInputExpr::ResetAwareCounterDelta { .. }
    ) {
        1_000_000.0
    } else {
        1.0
    };
    let updater: Box<dyn AccumulatorUpdater> = match (kind.algorithm(), kind.params()) {
        (SketchAlgorithm::Kll, SketchParams::Kll { k }) => Box::new(KllAccumulatorUpdater::new(
            u16::try_from(*k).map_err(|_| "KLL k exceeds runtime bound")?,
        )),
        (SketchAlgorithm::DDSketch, SketchParams::DDSketch { alpha }) => {
            Box::new(DDSketchAccumulatorUpdater::new(*alpha))
        }
        (SketchAlgorithm::Cms, params @ SketchParams::Cms { .. }) => {
            let (r, c) = cms_dims(params);
            Box::new(CmsAccumulatorUpdater::new(r, c))
        }
        (SketchAlgorithm::CountSketch, params @ SketchParams::CountSketch { .. }) => {
            let (r, c) = cms_dims(params);
            Box::new(CountSketchAccumulatorUpdater::new(r, c))
        }
        (SketchAlgorithm::CmsWithHeap, params @ SketchParams::CmsWithHeap { .. }) => {
            let (r, c, h) = cms_heap_dims(params);
            Box::new(CmsHeapAccumulatorUpdater::with_weight_scale(
                r,
                c,
                h,
                TopkWeight::Value,
                weight_scale,
            ))
        }
        (
            SketchAlgorithm::CountSketchWithHeap,
            params @ SketchParams::CountSketchWithHeap { .. },
        ) => {
            let (r, c, h) = cms_heap_dims(params);
            Box::new(CountSketchWithHeapAccumulatorUpdater::with_weight_scale(
                r,
                c,
                h,
                TopkWeight::Value,
                weight_scale,
            ))
        }
        (SketchAlgorithm::Hll, SketchParams::Hll { precision }) => Box::new(HllUpdater {
            acc: HllSketchAccumulator::new(
                asap_sketchlib::HllVariant::Regular,
                u32::from(*precision),
            ),
        }),
        (
            SketchAlgorithm::UnivMon,
            SketchParams::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            },
        ) => Box::new(UnivMonUpdater {
            acc: UnivMonAccumulator::new(
                *heap_size as usize,
                *sketch_rows as usize,
                *sketch_cols as usize,
                *layers as usize,
            )
            .map_err(|e| e.to_string())?,
        }),
        _ => {
            return Err(format!(
                "unsupported Planner algorithm/parameters: {kind:?}"
            ))
        }
    };
    if updater.is_keyed() != input.item.is_some()
        && !asap_types::accumulator_spec::is_unit_sample_frequency(input)
    {
        return Err("Planner item expression does not match the selected kernel layout".into());
    }
    Ok(updater)
}

struct PlannerExactUpdater {
    acc: crate::accumulators::exact_accumulator::ExactAccumulator,
}
impl AccumulatorUpdater for PlannerExactUpdater {
    fn update_single(&mut self, value: f64, timestamp: i64) {
        self.acc.update(None, value, timestamp);
    }
    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp: i64) {
        self.acc.update(Some(key), value, timestamp);
    }
    impl_clone_accumulator_methods!(acc);
    fn reset(&mut self) {
        self.acc = crate::accumulators::exact_accumulator::ExactAccumulator::new(
            self.acc.family().clone(),
            self.acc.is_keyed(),
        )
        .expect("installed exact family");
    }
    fn is_keyed(&self) -> bool {
        self.acc.is_keyed()
    }
    fn memory_usage_bytes(&self) -> usize {
        self.acc.approx_memory_bytes()
    }
}

#[cfg(test)]
mod planner_parameter_regression {
    use super::*;
    use planner_types::post_asap::{SketchKind, SummaryInputExpr, SummaryUpdate};

    // Planner width is the bucket count; depth is the independent hash-row count.
    #[test]
    fn planner_sketch_dimensions_are_not_transposed() {
        for (algorithm, params) in [
            (
                SketchAlgorithm::Cms,
                SketchParams::Cms {
                    width: 128,
                    depth: 3,
                },
            ),
            (
                SketchAlgorithm::CountSketch,
                SketchParams::CountSketch {
                    width: 128,
                    depth: 3,
                },
            ),
            (
                SketchAlgorithm::CmsWithHeap,
                SketchParams::CmsWithHeap {
                    width: 128,
                    depth: 3,
                    heap_size: 8,
                },
            ),
            (
                SketchAlgorithm::CountSketchWithHeap,
                SketchParams::CountSketchWithHeap {
                    width: 128,
                    depth: 3,
                    heap_size: 8,
                },
            ),
        ] {
            let family = SummaryFamilyType::Sketch(
                SketchKind::new(algorithm.clone(), params),
                Default::default(),
            );
            let update = SummaryUpdate {
                item: Some(SummaryInputExpr::Column(
                    planner_types::pre_asap::ColumnRef::Named("host".into()),
                )),
                weight: SummaryInputExpr::Constant(1.0),
                weight_domain: Default::default(),
            };
            let state = create_planner_accumulator(&family, &update, &Default::default())
                .unwrap()
                .snapshot_accumulator();
            let dims = match algorithm {
                SketchAlgorithm::Cms => {
                    let s = state
                        .as_any()
                        .downcast_ref::<CountMinSketchAccumulator>()
                        .unwrap();
                    (s.inner.rows(), s.inner.cols())
                }
                SketchAlgorithm::CountSketch => {
                    let s = state
                        .as_any()
                        .downcast_ref::<CountSketchAccumulator>()
                        .unwrap();
                    (s.inner.rows, s.inner.cols)
                }
                SketchAlgorithm::CmsWithHeap => {
                    let s = state
                        .as_any()
                        .downcast_ref::<CountMinSketchWithHeapAccumulator>()
                        .unwrap();
                    (s.inner.rows(), s.inner.cols())
                }
                SketchAlgorithm::CountSketchWithHeap => {
                    let s = state
                        .as_any()
                        .downcast_ref::<CountSketchWithHeapAccumulator>()
                        .unwrap();
                    (s.inner.rows(), s.inner.cols())
                }
                _ => unreachable!(),
            };
            assert_eq!(dims, (3, 128), "{algorithm:?}");
        }
    }
}
