//! Exercise the public library without a backend server, store, or scheduler.
use asap_physical_operators::planner::{
    post_asap::{
        GroupingStrategy, SketchAlgorithm, SketchKind, SketchParams, SummaryFamilyType,
        SummaryUpdate,
    },
    pre_asap::ColumnRef,
};
use asap_physical_operators::{factory::create_planner_accumulator, AggregateCore, Statistic};
use std::collections::HashMap;

fn family(k: u32) -> SummaryFamilyType {
    SummaryFamilyType::Sketch(
        SketchKind::new(SketchAlgorithm::Kll, SketchParams::Kll { k }),
        GroupingStrategy::PerSubpopulationInstance,
    )
}
fn build(values: &[f64]) -> Box<dyn AggregateCore> {
    let mut operator = create_planner_accumulator(
        &family(512),
        &SummaryUpdate::column(ColumnRef::SampleValue),
        &Default::default(),
    )
    .unwrap();
    for (at, value) in values.iter().enumerate() {
        operator.validate_single_input(*value).unwrap();
        operator.update_single(*value, at as i64);
    }
    operator.into_accumulator()
}
fn read(state: &dyn AggregateCore) -> f64 {
    state
        .query_statistic(
            Statistic::Quantile,
            &None,
            &HashMap::from([("quantile".into(), "0.5".into())]),
        )
        .unwrap()
}

// The same kernels work when every build is query-time, when only a prefix
// was precomputed, and when all state was precomputed before the readout.
#[test]
fn raw_partial_and_fully_precomputed_use_the_same_kernels() {
    let raw: Vec<f64> = (0..128).map(f64::from).collect();
    let raw_only = build(&raw);
    let stored_prefix = build(&raw[..64]);
    let query_time_suffix = build(&raw[64..]);
    let partial = stored_prefix.merge_with(&*query_time_suffix).unwrap();
    let stored_complete = build(&raw);
    assert_eq!(read(&*raw_only), read(&*partial));
    assert_eq!(read(&*partial), read(&*stored_complete));
    assert!((read(&*raw_only) - 64.0).abs() <= 1.0);
}

// A compiler must reject invalid physical parameters before starting execution.
#[test]
fn invalid_kll_parameters_are_rejected_at_binding() {
    let result = create_planner_accumulator(
        &family(0),
        &SummaryUpdate::column(ColumnRef::SampleValue),
        &Default::default(),
    );
    assert!(result.is_err());
}
