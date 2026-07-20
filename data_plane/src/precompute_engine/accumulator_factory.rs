use crate::precompute_engine::operators::{
    CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, DDSketchAccumulator,
    DatasketchesKLLAccumulator, HydraKllSketchAccumulator, IncreaseAccumulator, MinMaxAccumulator,
    MultipleIncreaseAccumulator, MultipleMinMaxAccumulator, MultipleSumAccumulator, SumAccumulator,
};
use crate::storage_engines::types::{
    AggregateCore, AggregationType, KeyByLabelValues, Measurement,
};
use asap_types::aggregation_config::AggregationConfig;

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

/// Pull `relativeAccuracy` (or canonical aliases) out of a
/// streaming-config aggregation entry. Defaults to 0.01 (1% rel-
/// err, the same default the agent's `ddsketchprocessor` uses).
fn ddsketch_alpha_param(config: &AggregationConfig) -> f64 {
    let parsed = config
        .parameters
        .get("relativeAccuracy")
        .or_else(|| config.parameters.get("relative_accuracy"))
        .or_else(|| config.parameters.get("alpha"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.01);
    if parsed > 0.0 && parsed < 1.0 {
        parsed
    } else {
        tracing::warn!(
            "DDSketch relativeAccuracy {} out of (0,1); using default 0.01",
            parsed
        );
        0.01
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
}

impl CmsHeapAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize, weight: TopkWeight) -> Self {
        Self {
            acc: CountMinSketchWithHeapAccumulator::new(row_num, col_num, heap_size),
            row_num,
            col_num,
            heap_size,
            weight,
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
            TopkWeight::Value => value,
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

/// Extract the KLL `k` parameter. Capital `"K"` takes precedence over lowercase
/// `"k"` to match the convention used by the top-level aggregation type arms.
fn kll_k_param(config: &AggregationConfig) -> u16 {
    config
        .parameters
        .get("K")
        .or_else(|| config.parameters.get("k"))
        .and_then(|v| v.as_u64())
        .and_then(|v| u16::try_from(v).ok())
        .unwrap_or(200)
}

/// Extract `(row_num, col_num)` for CMS / HydraKLL configs.
///
/// Reads canonical `d` (depth = rows) / `w` (width = cols) keys —
/// matches what the control plane's `sketch_params_to_json` emits
/// and what `sketch_config_to_params` uses for OTLP policy_fp
/// content matching. The legacy `row_num` / `col_num` form (the
/// only pre-PR-268 reader) was retired in lock-step with the
/// asapcollector migration to canonical keys.
fn cms_params(config: &AggregationConfig) -> (usize, usize) {
    let row_num = config
        .parameters
        .get("d")
        .and_then(|v| v.as_u64())
        .unwrap_or(4) as usize;
    let col_num = config
        .parameters
        .get("w")
        .and_then(|v| v.as_u64())
        .unwrap_or(1000) as usize;
    (row_num, col_num)
}

/// Top-k heap size for the `*WithHeap` configs. Reads `heap_size` /
/// `k` from `parameters`; defaults to 20 (the heap holds the top-k
/// candidates — it must be ≥ the largest `k` a query asks for).
fn heap_size_param(config: &AggregationConfig) -> usize {
    config
        .parameters
        .get("heap_size")
        .or_else(|| config.parameters.get("k"))
        .or_else(|| config.parameters.get("K"))
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .filter(|&v| v > 0)
        .unwrap_or(20)
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

/// Extract `(row_num, col_num, k)` for HydraKLL configs.
fn hydra_kll_params(config: &AggregationConfig) -> (usize, usize, u16) {
    let (row_num, col_num) = cms_params(config);
    (row_num, col_num, kll_k_param(config))
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Create an appropriate `AccumulatorUpdater` from an `AggregationConfig`.
pub fn create_accumulator_updater(config: &AggregationConfig) -> Box<dyn AccumulatorUpdater> {
    let sub_type = config.aggregation_sub_type.as_str();

    match config.aggregation_type {
        AggregationType::SingleSubpopulation => match sub_type {
            "Sum" | "sum" => Box::new(SumAccumulatorUpdater::new()),
            "Min" | "min" => Box::new(MinMaxAccumulatorUpdater::new(false)),
            "Max" | "max" => Box::new(MinMaxAccumulatorUpdater::new(true)),
            "Increase" | "increase" => Box::new(IncreaseAccumulatorUpdater::new()),
            "DatasketchesKLL" | "datasketches_kll" | "KLL" | "kll" => {
                Box::new(KllAccumulatorUpdater::new(kll_k_param(config)))
            }
            other => {
                tracing::warn!(
                    "Unknown SingleSubpopulation sub_type '{}', defaulting to Sum",
                    other
                );
                Box::new(SumAccumulatorUpdater::new())
            }
        },
        AggregationType::MultipleSubpopulation => match sub_type {
            "Sum" | "sum" => Box::new(MultipleSumAccumulatorUpdater::new()),
            "Min" | "min" => Box::new(MultipleMinMaxAccumulatorUpdater::new(false)),
            "Max" | "max" => Box::new(MultipleMinMaxAccumulatorUpdater::new(true)),
            "Increase" | "increase" => Box::new(MultipleIncreaseAccumulatorUpdater::new()),
            "CountMinSketch" | "count_min_sketch" | "CMS" | "cms" => {
                let (row_num, col_num) = cms_params(config);
                Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
            }
            "HydraKLL" | "hydra_kll" => {
                let (row_num, col_num, k) = hydra_kll_params(config);
                Box::new(HydraKllAccumulatorUpdater::new(row_num, col_num, k))
            }
            other => {
                tracing::warn!(
                    "Unknown MultipleSubpopulation sub_type '{}', defaulting to Sum",
                    other
                );
                Box::new(MultipleSumAccumulatorUpdater::new())
            }
        },
        AggregationType::DatasketchesKLL => {
            Box::new(KllAccumulatorUpdater::new(kll_k_param(config)))
        }
        AggregationType::MultipleSum => Box::new(MultipleSumAccumulatorUpdater::new()),
        AggregationType::MultipleIncrease => Box::new(MultipleIncreaseAccumulatorUpdater::new()),
        AggregationType::MultipleMinMax => Box::new(MultipleMinMaxAccumulatorUpdater::new(
            sub_type.eq_ignore_ascii_case("max"),
        )),
        AggregationType::Sum => Box::new(SumAccumulatorUpdater::new()),
        AggregationType::MinMax => Box::new(MinMaxAccumulatorUpdater::new(
            sub_type.eq_ignore_ascii_case("max"),
        )),
        AggregationType::Increase => Box::new(IncreaseAccumulatorUpdater::new()),
        AggregationType::CountMinSketch => {
            let (row_num, col_num) = cms_params(config);
            Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
        }
        // Heap-bearing top-k variants (raw-input ingest path): route to
        // the real `CmsHeapAccumulatorUpdater` so the per-policy top-k
        // heap is BUILT (heap-less CMS could not answer `topk(...)` —
        // recall 0). Keyed by the configured group-by `aggregated_labels`
        // (e.g. `host`), ranked by Σ value per key by default
        // (`weight_mode: value`), or Σ count for genuine frequency-top-k
        // (`weight_mode: count`). `CountSketchWithHeap` shares the wire
        // shape (heap is the distinguishing payload), so it routes here
        // too. The OTLP modified-sketch path builds the heap agent-side
        // and uses `SketchEnvelope` ingest, not this raw arm.
        AggregationType::CountMinSketchWithHeap | AggregationType::CountSketchWithHeap => {
            let (row_num, col_num) = cms_params(config);
            Box::new(CmsHeapAccumulatorUpdater::new(
                row_num,
                col_num,
                heap_size_param(config),
                topk_weight_param(config),
            ))
        }
        // Heap-LESS CountSketch (raw-input ingest path): route to
        // `CmsAccumulatorUpdater` — it handles the same `(rows, cols)`
        // matrix shape and answers point-frequency only. CountSketch
        // proper has no top-k heap, so `topk(...)` against it routes
        // through the heap-bearing variant above.
        AggregationType::CountSketch => {
            let (row_num, col_num) = cms_params(config);
            Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
        }
        AggregationType::HydraKLL => {
            let (row_num, col_num, k) = hydra_kll_params(config);
            Box::new(HydraKllAccumulatorUpdater::new(row_num, col_num, k))
        }
        AggregationType::DDSketch => Box::new(DDSketchAccumulatorUpdater::new(
            ddsketch_alpha_param(config),
        )),
        other => {
            tracing::warn!(
                "Unknown aggregation_type '{:?}', defaulting to SingleSubpopulation Sum",
                other
            );
            Box::new(SumAccumulatorUpdater::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::{AggregationType, WindowType};

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
                WindowType::Tumbling,
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
            WindowType::Tumbling,
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
            WindowType::Tumbling,
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
            WindowType::Tumbling,
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
            WindowType::Tumbling,
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
        // CountSketchWithHeap shares the heap path — same value-weighted default.
        let config = topk_config(AggregationType::CountSketchWithHeap, None);
        let mut updater = create_accumulator_updater(&config);
        feed_stream(&mut *updater);
        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "CountMinSketchWithHeapAccumulator");
        let ranked = ranked_topk(&*acc);
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
