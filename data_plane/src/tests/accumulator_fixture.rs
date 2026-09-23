//! Config fixtures for backend integration tests; production binds Planner payloads.
use asap_physical_operators::factory::*;
use asap_physical_operators::{AggregateCore, AggregationType};
use asap_types::{accumulator_spec::cms_params, PrecomputeMaterialization};
use planner_types::post_asap::{ExactKind, SketchAlgorithm, SketchParams, SummaryFamilyType};
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
            asap_physical_operators::factory::create_planner_accumulator(
                &spec.family,
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
            asap_physical_operators::factory::create_planner_accumulator(
                &spec.family,
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
