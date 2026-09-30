//! Kernel fixtures for backend unit tests: a stored state family and the
//! Planner update it ingests. Production executes the bound Planner DAG.
use asap_physical_operators::factory::*;
use asap_physical_operators::summary_kernels::exact::ExactAccumulator;
use asap_summary_state::{AggregateCore, AggregationType, KeyByLabelValues};
use planner_types::post_asap::{
    ExactKind, GroupingStrategy, HydraParams, SketchAlgorithm, SketchParams, SummaryFamilyType,
    SummaryInputExpr, SummaryUpdate,
};

/// Whether `family` stores a keyed (multi-population) accumulator.
///
/// **Contract:** this must agree with every concrete `AccumulatorUpdater::is_keyed()`
/// implementation. When a new accumulator type is added, update both here and
/// in the corresponding struct.
#[cfg(test)]
pub fn family_is_keyed(family: &SummaryFamilyType) -> bool {
    match family {
        SummaryFamilyType::Sketch(_, GroupingStrategy::SharedMultiSubpopulation { .. }) => true,
        SummaryFamilyType::Sketch(kind, _) => matches!(
            kind.algorithm(),
            SketchAlgorithm::Cms
                | SketchAlgorithm::CmsWithHeap
                | SketchAlgorithm::CountSketch
                | SketchAlgorithm::CountSketchWithHeap
        ),
        _ => false,
    }
}

/// Top-k ranking quantity: a unit weight counts occurrences per key,
/// otherwise values are summed.
#[cfg(test)]
fn topk_weight(update: &SummaryUpdate) -> TopkWeight {
    match update.weight {
        SummaryInputExpr::Constant(1.0) => TopkWeight::Count,
        _ => TopkWeight::Value,
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
    family: &SummaryFamilyType,
    update: &SummaryUpdate,
) -> Box<dyn AccumulatorUpdater> {
    let keyed = family_is_keyed(family);

    match (family, keyed) {
        (SummaryFamilyType::ExactAggregate(..), keyed) => Box::new(ExactUpdater {
            acc: ExactAccumulator::new(family.clone(), keyed).expect("exact fixture family"),
        }),

        (SummaryFamilyType::Sketch(kind, _), false)
            if kind.algorithm() == &SketchAlgorithm::Kll =>
        {
            Box::new(KllAccumulatorUpdater::new(kll_k(kind.params())))
        }
        // HydraKLL: `k` comes off the typed params like the unkeyed case;
        // the shared bucket count is the tiling width over four rows.
        (
            SummaryFamilyType::Sketch(
                kind,
                GroupingStrategy::SharedMultiSubpopulation {
                    params: HydraParams::HydraKll { shared_buckets, .. },
                    ..
                },
            ),
            true,
        ) if kind.algorithm() == &SketchAlgorithm::Kll => Box::new(
            HydraKllAccumulatorUpdater::new(4, *shared_buckets as usize, kll_k(kind.params())),
        ),

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
            Box::new(CmsHeapAccumulatorUpdater::new(
                row_num,
                col_num,
                heap_size,
                topk_weight(update),
            ))
        }

        // Heap-bearing CountSketch retains CountSketch estimation semantics.
        (SummaryFamilyType::Sketch(kind, _), _)
            if kind.algorithm() == &SketchAlgorithm::CountSketchWithHeap =>
        {
            let (row_num, col_num, heap_size) = cms_heap_dims(kind.params());
            Box::new(CountSketchWithHeapAccumulatorUpdater::new(
                row_num,
                col_num,
                heap_size,
                topk_weight(update),
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
            create_planner_accumulator(
                family,
                &planner_types::post_asap::SummaryUpdate::column(
                    planner_types::pre_asap::ColumnRef::SampleValue,
                ),
                &Default::default(),
            )
            .unwrap()
        }

        (SummaryFamilyType::Sketch(kind, _), false)
            if kind.algorithm() == &SketchAlgorithm::Hll =>
        {
            let SketchParams::Hll { precision } = kind.params() else {
                unreachable!("validated HLL family parameters")
            };
            create_planner_accumulator(
                family,
                &planner_types::post_asap::SummaryUpdate::column(
                    planner_types::pre_asap::ColumnRef::SampleValue,
                ),
                &Default::default(),
            )
            .unwrap()
        }

        (other_family, keyed) => {
            panic!("unsupported isolated kernel fixture {other_family:?}, keyed={keyed}")
        }
    }
}

/// Feeds Planner's exact state directly; Planner keeps its own exact updater private.
#[cfg(test)]
struct ExactUpdater {
    acc: ExactAccumulator,
}

#[cfg(test)]
impl AccumulatorUpdater for ExactUpdater {
    fn update_single(&mut self, value: f64, timestamp_ms: i64) {
        self.acc.update(None, value, timestamp_ms);
    }
    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.acc.update(Some(key), value, timestamp_ms);
    }
    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let taken = Box::new(self.acc.clone());
        self.reset();
        taken
    }
    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }
    fn reset(&mut self) {
        self.acc = ExactAccumulator::new(self.acc.family().clone(), self.acc.is_keyed())
            .expect("exact fixture family");
    }
    fn is_keyed(&self) -> bool {
        self.acc.is_keyed()
    }
    fn memory_usage_bytes(&self) -> usize {
        self.acc.approx_memory_bytes()
    }
}

/// Planner's unkeyed exact Sum holding `sum`.
#[cfg(test)]
pub fn sum_state(sum: f64) -> ExactAccumulator {
    asap_summary_state::stored_state::codec::exact_value(ExactKind::Sum, sum)
}

/// The value of an unkeyed exact Sum state.
#[cfg(test)]
pub fn sum_of(state: &dyn AggregateCore) -> f64 {
    state
        .as_any()
        .downcast_ref::<ExactAccumulator>()
        .expect("exact Sum state")
        .readout(asap_types::Statistic::Sum, None, None)
        .expect("exact Sum readout")
        .expect("present Sum population")
}

/// Test view of an unkeyed exact Sum state's value.
#[cfg(test)]
pub struct SumView {
    pub sum: f64,
}

/// Panics unless `state` is an unkeyed exact Sum.
#[cfg(test)]
pub fn sum_view(state: &ExactAccumulator) -> SumView {
    SumView {
        sum: state
            .readout(asap_types::Statistic::Sum, None, None)
            .expect("exact Sum readout")
            .expect("present exact population"),
    }
}

/// Planner's unkeyed exact counter state (Rate or Increase) over `samples`
/// of `(timestamp_ms, value)`.
#[cfg(test)]
pub fn counter_state(kind: ExactKind, samples: &[(i64, f64)]) -> ExactAccumulator {
    use planner_types::post_asap::ExactParams;
    let params = match kind {
        ExactKind::Rate => ExactParams::Rate,
        ExactKind::Increase => ExactParams::Increase,
        other => panic!("{other:?} is not a counter family"),
    };
    let mut state = ExactAccumulator::new(SummaryFamilyType::ExactAggregate(kind, params), false)
        .expect("counter family");
    for (timestamp, value) in samples {
        state.update(None, *value, *timestamp);
    }
    state
}
