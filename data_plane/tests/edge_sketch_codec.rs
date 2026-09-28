//! Portable edge sketch envelopes reconstruct through the neutral codec and
//! remain readable by the backend's query accumulators.

use asap_sketch_codec::{encode_ddsketch, reconstruct_ddsketch};
use asap_sketchlib::proto::sketchlib::sketch_envelope::SketchState;
use data_plane::storage_engines::types::AggregateCore;

#[test]
fn ddsketch_full_envelope_round_trips_without_collector_runtime() {
    let mut source = asap_sketchlib::DdSketch::new(0.01);
    for value in 1..=200 {
        source.update(value as f64);
    }
    let bytes = asap_sketch_codec::encode_ddsketch(&source);
    assert!(matches!(
        asap_sketch_codec::envelope_state(&bytes).unwrap(),
        Some(SketchState::Ddsketch(_))
    ));
    let (decoded, _) = reconstruct_ddsketch(&bytes).unwrap();
    assert_eq!(decoded.total_count(), 200);
    assert_eq!(encode_ddsketch(&decoded), bytes);
}

#[test]
fn ddsketch_bare_state_is_rejected_and_envelope_supports_query_readout() {
    let mut source = asap_sketchlib::DdSketch::new(0.01);
    for value in 1..=100 {
        source.update(value as f64);
    }
    let envelope = asap_sketch_codec::encode_ddsketch(&source);
    let Some(SketchState::Ddsketch(state)) = asap_sketch_codec::envelope_state(&envelope).unwrap()
    else {
        panic!("DDSketch state required")
    };
    let bare = prost::Message::encode_to_vec(&state);
    assert!(asap_sketch_codec::reconstruct_ddsketch(&bare).is_err());
    let (decoded, _) = asap_sketch_codec::reconstruct_ddsketch(&envelope).unwrap();
    let accumulator = asap_physical_operators::summary_kernels::DDSketchAccumulator {
        inner: decoded,
        sample_p: 1.0,
    };
    let median = accumulator
        .query_statistic(
            asap_types::Statistic::Quantile,
            &None,
            &[("quantile".to_string(), "0.5".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
    assert!((median - 50.0).abs() / 50.0 < 0.05);
}

#[test]
fn kll_envelope_keeps_level_layout_for_backend_readout() {
    let mut source = asap_sketchlib::sketches::kll::KLL::<f64>::init_kll_with_seed(200, 7);
    for value in 1..=50 {
        source.update(&(value as f64));
    }
    let bytes = asap_sketch_codec::encode_kll(&source);
    let state = asap_sketch_codec::kll_state(&bytes).unwrap();
    assert_eq!(state.k, 200);
    assert_eq!(state.items.len(), 50);
    let snapshot_bytes = bytes;
    let accumulator = asap_physical_operators::summary_kernels::DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(&snapshot_bytes).unwrap();
    assert!(accumulator.get_quantile(0.5).is_finite());
}

#[test]
fn a_different_sketch_family_cannot_be_decoded_as_ddsketch() {
    let mut kll = asap_sketchlib::sketches::kll::KLL::<f64>::init_kll_with_seed(200, 7);
    kll.update(&42.0);
    let bytes = asap_sketch_codec::encode_kll(&kll);
    assert!(asap_sketch_codec::reconstruct_ddsketch(&bytes).is_err());
}
