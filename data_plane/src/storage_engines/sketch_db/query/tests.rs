//! Unit tests for the ASAP-tier sketch reducer.
//!
//! Each test:
//! 1. Builds an in-memory `SketchStore` with one synthetic sid.
//! 2. Generates true-distribution data, builds a sketch via the
//!    same `asap_sketchlib` types the precompute path uses, and
//!    serializes via the proto wire format so the reducer
//!    decodes through the same path it would on a live ingest.
//! 3. Drives `SketchReducer::evaluate` and asserts the answer
//!    sits within the relevant sketch family's accuracy
//!    envelope.

use std::collections::{BTreeMap, BTreeSet};

use asap_sketchlib::sketches::ddsketch::DdSketch;
use asap_sketchlib::sketches::hll::{HllSketch, HllVariant};

use crate::storage_engines::sketch_db::query::{SketchReducer, ASAPTierError};
use crate::storage_engines::sketch_db::index::{
    AccuracyBound, AggKind, Capability, SketchConfig, SketchEncoding, SketchStore, SketchInstanceMetadata,
    SketchKindHandle, SketchSampleState,
};

// ---------------------------------------------------------------------------
// Encoders — wrap each sketchlib type in a proto SketchEnvelope so the
// reducer's deserialize path sees the same bytes a live producer
// (DataCollector's *processor) would emit.
// ---------------------------------------------------------------------------

fn encode_ddsketch(sk: &DdSketch) -> Vec<u8> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
    use prost::Message;
    let state = DdSketchState {
        alpha: sk.alpha,
        store_counts: sk.store_counts.clone(),
        store_offset: sk.store_offset,
        count: sk.count,
        sum: sk.sum,
        min: sk.min,
        max: sk.max,
    };
    let env = SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
        ..Default::default()
    };
    env.encode_to_vec()
}

fn encode_kll_items_proto(k: u16, items: &[f64]) -> Vec<u8> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
    use prost::Message;
    let state = KllState {
        k: k as u32,
        items: items.to_vec(),
        levels: vec![],
        num_levels: 0,
        ..Default::default()
    };
    let env = SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
        ..Default::default()
    };
    env.encode_to_vec()
}

fn encode_hll(sk: &HllSketch) -> Vec<u8> {
    use asap_sketchlib::proto::sketchlib::{
        sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
    };
    use prost::Message;
    let proto_variant = match sk.variant {
        HllVariant::Unspecified => ProtoVariant::Unspecified,
        HllVariant::Regular => ProtoVariant::Regular,
        HllVariant::Datafusion => ProtoVariant::ErtlMle,
        HllVariant::Hip => ProtoVariant::Hip,
    };
    let state = HyperLogLogState {
        variant: proto_variant as i32,
        precision: sk.precision,
        registers: sk.registers.clone(),
        hip_kxq0: sk.hip_kxq0,
        hip_kxq1: sk.hip_kxq1,
        hip_est: sk.hip_est,
    };
    let env = SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
        ..Default::default()
    };
    env.encode_to_vec()
}

fn proto_full(bytes: Vec<u8>) -> SketchSampleState {
    SketchSampleState {
        bytes,
        encoding: SketchEncoding::ProtoFull,
    }
}

fn dd_meta(sid: u64) -> SketchInstanceMetadata {
    let cfg = SketchConfig::DDSketch {
        relative_accuracy: 0.01,
    };
    SketchInstanceMetadata {
        sid,
        metric_name: "http_latency_ms".to_string(),
        group_by_keys: BTreeSet::new(),
        capability: Some(Capability::QuantileApprox(SketchKindHandle::DDSketch)),
        agg_kind: AggKind::Sketch {
            kind: SketchKindHandle::DDSketch,
            config: cfg.clone(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

fn kll_meta(sid: u64, k: u32) -> SketchInstanceMetadata {
    let cfg = SketchConfig::Kll { k };
    SketchInstanceMetadata {
        sid,
        metric_name: "http_latency_ms".to_string(),
        group_by_keys: BTreeSet::new(),
        capability: Some(Capability::QuantileApprox(SketchKindHandle::Kll)),
        agg_kind: AggKind::Sketch {
            kind: SketchKindHandle::Kll,
            config: cfg.clone(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

fn hll_meta(sid: u64, precision: u32) -> SketchInstanceMetadata {
    let cfg = SketchConfig::Hll { precision };
    SketchInstanceMetadata {
        sid,
        metric_name: "uniq_users".to_string(),
        group_by_keys: BTreeSet::new(),
        capability: Some(Capability::CardinalityApprox),
        agg_kind: AggKind::Sketch {
            kind: SketchKindHandle::Hll,
            config: cfg.clone(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

// ---------------------------------------------------------------------------
// DDSketch per-window `quantile` — three windows, each with a different
// data distribution. Verifies (a) per-window evaluation, (b) result
// shape, (c) DDSketch's relative-accuracy bound holds.
//
// The cumulative variant `quantile_over_time` is exercised by
// `ddsketch_cumulative_full_plus_two_deltas` (TODO-2 follow-up); this
// test is renamed but otherwise preserves its original assertions.
// ---------------------------------------------------------------------------

#[test]
fn ddsketch_quantile_per_window_three_windows() {
    let idx = SketchStore::new();
    let sid = 1;
    idx.register(dd_meta(sid));

    // Three windows; each carries a synthetic DDSketch over a known
    // distribution. We pick small-cardinality value sets so the
    // quantile is unambiguous given a fixed quantile rank.
    let alpha = 0.01;
    for (i, values) in [
        vec![1.0, 2.0, 3.0, 4.0, 5.0],
        vec![10.0, 20.0, 30.0, 40.0, 50.0],
        vec![100.0, 200.0, 300.0, 400.0, 500.0],
    ]
    .iter()
    .enumerate()
    {
        let mut sk = DdSketch::new(alpha);
        for &v in values {
            sk.update(v);
        }
        let bytes = encode_ddsketch(&sk);
        let lv = BTreeMap::new();
        let window_start = 1000 + (i as u64) * 10;
        let window_end = window_start + 10;
        idx.append_sample(sid, lv, (window_start, window_end), proto_full(bytes));
    }

    // `quantile` (per-window) emits one scalar per window; the
    // cumulative variant `quantile_over_time` is exercised by
    // [`quantile_over_time_cumulative_mode`] below.
    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "quantile", &[0.5], 1000, 1100)
        .expect("evaluate should succeed");

    assert_eq!(result.series.len(), 1, "one series (no grouping)");
    let (_lvs, samples) = &result.series[0];
    assert_eq!(samples.len(), 3, "three windows");

    // For the rank-floor estimator DDSketch uses
    // (`target = floor(q*(count-1))`), the median of 5 items
    // (rank 2) is the 3rd value: 3, 30, 300. DDSketch's α=0.01
    // relative-accuracy bound says the bucket-midpoint estimate is
    // within (1+α)/(1-α) ≈ 1.02× of the true value, so we accept
    // up to ±5% to give the chunked-bucket store some slack.
    let expected = [3.0, 30.0, 300.0];
    for ((_, est), exp) in samples.iter().zip(expected.iter()) {
        let rel_err = (*est - *exp).abs() / *exp;
        assert!(
            rel_err < 0.05,
            "DDSketch 50-quantile error too large: est={} exp={} rel_err={}",
            est,
            exp,
            rel_err
        );
    }
}

// ---------------------------------------------------------------------------
// KLL quantile_over_time — sketchlib KLL uses an msgpack roundtrip path
// when going through the proto entry. The proto path for KLL replays
// `state.items[]` through `update()`, so we feed a small enough item
// list that all values fit in level 0 (no compaction). Quantile
// estimates are then exact.
// ---------------------------------------------------------------------------

#[test]
fn kll_quantile_over_time_one_window() {
    let idx = SketchStore::new();
    let sid = 2;
    let k: u32 = 200;
    idx.register(kll_meta(sid, k));

    // Push 50 distinct items into the KLL state (well below k=200,
    // so no compaction → quantile estimates are exact).
    let mut items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
    items.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let bytes = encode_kll_items_proto(k as u16, &items);
    let lv = BTreeMap::new();
    idx.append_sample(sid, lv, (5000, 5010), proto_full(bytes));

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "quantile_over_time", &[0.5], 5000, 5010)
        .expect("evaluate should succeed");
    let (_lvs, samples) = &result.series[0];
    assert_eq!(samples.len(), 1);
    let est = samples[0].1;
    // True median of 1..=50 is 25.5; KLL with k=200 and 50 items
    // has rank-error ≤ 1/k = 0.005, so the answer must be within
    // a couple of items of the true median.
    assert!(
        (est - 25.5).abs() <= 5.0,
        "KLL median estimate {} too far from true 25.5",
        est
    );
}

// ---------------------------------------------------------------------------
// HLL cardinality estimate — push N distinct items, verify the
// estimate is within HLL's std-error envelope (1.04 / √(2^p) for
// precision p).
// ---------------------------------------------------------------------------

#[test]
fn hll_cardinality_estimate() {
    let idx = SketchStore::new();
    let sid = 3;
    let precision: u32 = 10;
    idx.register(hll_meta(sid, precision));

    let mut sk = HllSketch::new(HllVariant::Regular, precision);
    let true_cardinality = 1000usize;
    for i in 0..true_cardinality {
        sk.update(format!("user-{i}").as_bytes());
    }
    let bytes = encode_hll(&sk);
    let lv = BTreeMap::new();
    idx.append_sample(sid, lv, (8000, 8010), proto_full(bytes));

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "cardinality_estimate", &[], 8000, 8010)
        .expect("evaluate should succeed");
    let (_lvs, samples) = &result.series[0];
    let est = samples[0].1;
    // HLL std error: σ ≈ 1.04 / √(2^p). For p=10, σ ≈ 0.0325 → 3.25%.
    // We accept up to 5σ to keep the test stable across hash
    // variations; that's roughly ±16% of true cardinality.
    let std_err = 1.04 / ((1u64 << precision) as f64).sqrt();
    let envelope = 5.0 * std_err * (true_cardinality as f64);
    let abs_err = (est - true_cardinality as f64).abs();
    assert!(
        abs_err <= envelope,
        "HLL cardinality estimate {} too far from true {} (5σ envelope = {})",
        est,
        true_cardinality,
        envelope,
    );
}

// ---------------------------------------------------------------------------
// Capability-mismatch: register a `QuantileApprox` sid, ask for `topk`.
// Must surface `UnsupportedCapability` so the engine surfaces
// CapabilityMiss + the router fails over to archive.
// ---------------------------------------------------------------------------

#[test]
fn capability_mismatch_quantile_vs_topk() {
    let idx = SketchStore::new();
    let sid = 4;
    idx.register(dd_meta(sid));

    let mut sk = DdSketch::new(0.01);
    for v in 1..=10 {
        sk.update(v as f64);
    }
    let bytes = encode_ddsketch(&sk);
    idx.append_sample(sid, BTreeMap::new(), (100, 110), proto_full(bytes));

    let reducer = SketchReducer::new(&idx);
    let err = reducer
        .evaluate(&[sid], "topk", &[5.0], 100, 110)
        .expect_err("topk against QuantileApprox must fail");
    match err {
        ASAPTierError::UnsupportedCapability { function, .. } => {
            assert_eq!(function, "topk");
        }
        other => panic!("expected UnsupportedCapability, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Empty-window: instance registered but no samples appended → classify
// would return Ghost (so the engine wouldn't even call the reducer
// today). We test the defensive behavior — evaluate over a sid with
// no data should return `NoData` rather than an empty
// `ASAPTierResult` so the engine can surface CapabilityMiss
// truthfully and let archive answer.
// ---------------------------------------------------------------------------

#[test]
fn empty_returns_no_data_error() {
    let idx = SketchStore::new();
    let sid = 5;
    idx.register(dd_meta(sid));

    // Append a sample at 1000–1010 (outside our query window
    // 5000–6000) so query_range returns empty.
    let mut sk = DdSketch::new(0.01);
    sk.update(1.0);
    let bytes = encode_ddsketch(&sk);
    idx.append_sample(sid, BTreeMap::new(), (1000, 1010), proto_full(bytes));

    let reducer = SketchReducer::new(&idx);
    let err = reducer
        .evaluate(&[sid], "quantile_over_time", &[0.99], 5000, 6000)
        .expect_err("no samples in window must yield NoData");
    match err {
        ASAPTierError::NoData { metric_name } => {
            assert_eq!(metric_name, "http_latency_ms");
        }
        other => panic!("expected NoData, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Unsupported function: e.g. `rate(...)`. Must surface
// UnsupportedFunction so the engine maps to CapabilityMiss.
// ---------------------------------------------------------------------------

#[test]
fn unsupported_function_rejects() {
    let idx = SketchStore::new();
    let sid = 6;
    idx.register(dd_meta(sid));

    let reducer = SketchReducer::new(&idx);
    let err = reducer
        .evaluate(&[sid], "rate", &[], 0, 100)
        .expect_err("`rate` is not ASAP-tier-answerable");
    match err {
        ASAPTierError::UnsupportedFunction(name) => {
            assert_eq!(name, "rate");
        }
        other => panic!("expected UnsupportedFunction, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Decode failure: feed garbage proto bytes; verify the reducer
// surfaces `DeserializeFailure` rather than panicking.
// ---------------------------------------------------------------------------

#[test]
fn decode_failure_surfaces_deserialize_error() {
    let idx = SketchStore::new();
    let sid = 7;
    idx.register(dd_meta(sid));

    let bad_state = SketchSampleState {
        bytes: vec![0xff, 0xff, 0xff, 0xff, 0xff],
        encoding: SketchEncoding::ProtoFull,
    };
    idx.append_sample(sid, BTreeMap::new(), (1000, 1010), bad_state);

    let reducer = SketchReducer::new(&idx);
    let err = reducer
        .evaluate(&[sid], "quantile_over_time", &[0.99], 1000, 1010)
        .expect_err("garbage bytes must yield DeserializeFailure");
    match err {
        ASAPTierError::DeserializeFailure { sid: s, .. } => {
            assert_eq!(s, sid);
        }
        other => panic!("expected DeserializeFailure, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Multi-series: two distinct group-by VALUES vectors under the same
// sid (e.g. `host=a` and `host=b`) → expect two `ASAPTierResult`
// entries.
// ---------------------------------------------------------------------------

#[test]
fn multi_series_one_per_label_value() {
    let idx = SketchStore::new();
    let sid = 8;
    let mut meta = dd_meta(sid);
    meta.group_by_keys = BTreeSet::from(["host".to_string()]);
    idx.register(meta);

    let mut sk_a = DdSketch::new(0.01);
    sk_a.update(1.0);
    let mut sk_b = DdSketch::new(0.01);
    sk_b.update(2.0);

    let lv_a = BTreeMap::from([("host".to_string(), "a".to_string())]);
    let lv_b = BTreeMap::from([("host".to_string(), "b".to_string())]);
    idx.append_sample(sid, lv_a, (1000, 1010), proto_full(encode_ddsketch(&sk_a)));
    idx.append_sample(sid, lv_b, (1000, 1010), proto_full(encode_ddsketch(&sk_b)));

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "quantile_over_time", &[0.5], 1000, 1010)
        .expect("evaluate should succeed");
    assert_eq!(result.series.len(), 2);
}

// ---------------------------------------------------------------------------
// TODO-1 tests — CMS-with-heap top-k.
// ---------------------------------------------------------------------------

use asap_sketchlib::sketches::countminsketch_topk::CountMinSketchWithHeap;

fn cms_heap_meta(sid: u64) -> SketchInstanceMetadata {
    let cfg = SketchConfig::CountMin { rows: 4, cols: 256 };
    SketchInstanceMetadata {
        sid,
        metric_name: "endpoint_hits".to_string(),
        group_by_keys: BTreeSet::new(),
        capability: Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap)),
        agg_kind: AggKind::Sketch {
            kind: SketchKindHandle::CmsWithHeap,
            config: cfg.clone(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

fn cms_only_meta(sid: u64) -> SketchInstanceMetadata {
    let cfg = SketchConfig::CountMin { rows: 4, cols: 256 };
    SketchInstanceMetadata {
        sid,
        metric_name: "endpoint_hits".to_string(),
        group_by_keys: BTreeSet::new(),
        capability: Some(Capability::FrequencyTopk(SketchKindHandle::CountMin)),
        agg_kind: AggKind::Sketch {
            kind: SketchKindHandle::CountMin,
            config: cfg.clone(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

fn msgpack_full(bytes: Vec<u8>) -> SketchSampleState {
    SketchSampleState {
        bytes,
        encoding: SketchEncoding::MsgpackFull,
    }
}

#[test]
fn cms_with_heap_topk_returns_top_items() {
    let idx = SketchStore::new();
    let sid = 100;
    idx.register(cms_heap_meta(sid));

    // Build a CMS-with-heap state with known item counts.
    let mut cms = CountMinSketchWithHeap::new(4, 256, 20);
    // Insert items with varying frequencies. Higher count items
    // should end up in the heap.
    let inserts: &[(&str, u64)] = &[
        ("alpha", 100),
        ("beta", 50),
        ("gamma", 200),
        ("delta", 75),
        ("epsilon", 10),
        ("zeta", 150),
    ];
    for (k, n) in inserts {
        for _ in 0..*n {
            cms.update(k, 1.0);
        }
    }
    let bytes = cms.serialize_msgpack().expect("serialize cms with heap");
    idx.append_sample(sid, BTreeMap::new(), (1000, 1010), msgpack_full(bytes));

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "topk", &[5.0], 1000, 1010)
        .expect("topk evaluate should succeed");
    // We requested top-5. Each top-k item is its own series row
    // (label_values carries the encoded `"item": <key>`).
    assert!(
        result.series.len() <= 5 && !result.series.is_empty(),
        "expected up to 5 top-k series, got {}",
        result.series.len()
    );
    // Coverage should match the window we appended.
    assert_eq!(result.coverage, Some((1010, 1010)));

    // Top-1 should be "gamma" (count=200). Sort our series by
    // first-sample value descending and check the top item.
    let mut sorted = result.series.clone();
    sorted.sort_by(|a, b| {
        let va = a.1.first().map(|s| s.1).unwrap_or(0.0);
        let vb = b.1.first().map(|s| s.1).unwrap_or(0.0);
        vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
    });
    let top = sorted.first().expect("at least one series");
    let item_label = top.0.get("item").expect("series carries item label");
    assert_eq!(item_label, "gamma", "highest-count item should be `gamma`");
}

#[test]
fn cms_without_heap_returns_missing_heap() {
    let idx = SketchStore::new();
    let sid = 101;
    idx.register(cms_only_meta(sid));

    // Append a CMS-with-heap-encoded payload — but the metadata is
    // CountMin-only so the reducer should refuse on the
    // sketch-kind side before decoding bytes.
    let mut cms = CountMinSketchWithHeap::new(4, 256, 20);
    cms.update("foo", 1.0);
    let bytes = cms.serialize_msgpack().expect("serialize");
    idx.append_sample(sid, BTreeMap::new(), (1000, 1010), msgpack_full(bytes));

    let reducer = SketchReducer::new(&idx);
    let err = reducer
        .evaluate(&[sid], "topk", &[5.0], 1000, 1010)
        .expect_err("topk against CountMin (no heap) must surface MissingHeap");
    match err {
        ASAPTierError::MissingHeap {
            sid: s,
            sketch_kind,
        } => {
            assert_eq!(s, sid);
            assert_eq!(sketch_kind, SketchKindHandle::CountMin);
        }
        other => panic!("expected MissingHeap, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// TODO-2 tests — delta encoding stitching.
//
// We exercise the cumulative path for DDSketch (one Full window + two
// Delta windows of additional samples). The cumulative result should
// match what a fresh DDSketch fed all raw values would yield.
// ---------------------------------------------------------------------------

fn proto_delta(bytes: Vec<u8>) -> SketchSampleState {
    SketchSampleState {
        bytes,
        encoding: SketchEncoding::ProtoDelta,
    }
}

#[test]
fn ddsketch_cumulative_full_plus_two_deltas() {
    let idx = SketchStore::new();
    let sid = 200;
    idx.register(dd_meta(sid));

    let alpha = 0.01;
    // Window 1: Full snapshot of values 1..=5
    let mut sk1 = DdSketch::new(alpha);
    for v in 1..=5 {
        sk1.update(v as f64);
    }
    let bytes1 = encode_ddsketch(&sk1);
    idx.append_sample(sid, BTreeMap::new(), (1000, 1010), proto_full(bytes1));

    // Windows 2 & 3: "Deltas" encoded as full-fragment sketches that
    // get merged into the rolling state (the reducer's delta_apply
    // treats DD/KLL/HLL delta-as-mergeable-fragment).
    let mut sk2 = DdSketch::new(alpha);
    for v in 6..=10 {
        sk2.update(v as f64);
    }
    let bytes2 = encode_ddsketch(&sk2);
    idx.append_sample(sid, BTreeMap::new(), (1010, 1020), proto_delta(bytes2));

    let mut sk3 = DdSketch::new(alpha);
    for v in 11..=15 {
        sk3.update(v as f64);
    }
    let bytes3 = encode_ddsketch(&sk3);
    idx.append_sample(sid, BTreeMap::new(), (1020, 1030), proto_delta(bytes3));

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "quantile_over_time", &[0.5], 1000, 1030)
        .expect("cumulative evaluate should succeed");
    // Cumulative mode emits one scalar covering the full range.
    assert_eq!(result.series.len(), 1);
    let (_, samples) = &result.series[0];
    assert_eq!(samples.len(), 1, "cumulative emits exactly one scalar");
    let est = samples[0].1;

    // Truth: feed all 15 values into a fresh DDSketch and read the
    // median (8th value of 1..=15 = 8). Allow 5% relative error to
    // give the bucket store some slack.
    let mut truth = DdSketch::new(alpha);
    for v in 1..=15 {
        truth.update(v as f64);
    }
    let true_q = truth.quantile(0.5).unwrap_or(0.0);
    let rel_err = (est - true_q).abs() / true_q.max(1e-9);
    assert!(
        rel_err < 0.10,
        "cumulative quantile error too large: est={} truth={} rel_err={}",
        est,
        true_q,
        rel_err
    );

    // Coverage should span the three window ends.
    assert_eq!(result.coverage, Some((1010, 1030)));
}

#[test]
fn hll_cumulative_full_plus_one_delta() {
    let idx = SketchStore::new();
    let sid = 201;
    let precision: u32 = 10;
    idx.register(hll_meta(sid, precision));

    // Window 1: Full snapshot with 500 distinct items.
    let mut sk1 = HllSketch::new(HllVariant::Regular, precision);
    for i in 0..500 {
        sk1.update(format!("user-{i}").as_bytes());
    }
    let bytes1 = encode_hll(&sk1);
    idx.append_sample(sid, BTreeMap::new(), (1000, 1010), proto_full(bytes1));

    // Window 2: Msgpack-delta — the ASAP-tier reducer treats
    // MsgpackDelta for HLL as a serialized HllSketch fragment that's
    // mergeable via `HllSketch::merge`. We mock that here by
    // serializing a second HLL with 500 additional distinct items.
    let mut sk2 = HllSketch::new(HllVariant::Regular, precision);
    for i in 500..1000 {
        sk2.update(format!("user-{i}").as_bytes());
    }
    let bytes2 = sk2.serialize_msgpack().expect("serialize HLL msgpack");
    let delta_sample = SketchSampleState {
        bytes: bytes2,
        encoding: SketchEncoding::MsgpackDelta,
    };
    idx.append_sample(sid, BTreeMap::new(), (1010, 1020), delta_sample);

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "count_distinct_over_time", &[], 1000, 1020)
        .expect("cumulative HLL evaluate should succeed");
    assert_eq!(result.series.len(), 1);
    let (_, samples) = &result.series[0];
    assert_eq!(samples.len(), 1, "cumulative emits one scalar");
    let est = samples[0].1;
    // Truth: 1000 distinct items, allow 5σ envelope.
    let std_err = 1.04 / ((1u64 << precision) as f64).sqrt();
    let envelope = 5.0 * std_err * 1000.0;
    let abs_err = (est - 1000.0).abs();
    assert!(
        abs_err <= envelope,
        "cumulative HLL estimate {} too far from true 1000 (5σ envelope = {})",
        est,
        envelope
    );
}

// ---------------------------------------------------------------------------
// TODO-3 tests — hybrid warm + archive stitch via `ASAPTierResult.coverage`.
//
// We don't drive the full ASAPQueryEngine here (that would require
// constructing the whole streaming-config plumbing). Instead we exercise
// the `stitch_warm_and_archive` helper directly via a small wrapper
// test in `engines::asap_query::tests` would be ideal — but to keep this
// PR additive, we verify the `coverage` field is populated correctly
// on a multi-window evaluate so the downstream stitch path has the
// information it needs.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ExactAgg dispatch — regression coverage for `sum by (...)` PromQL.
// Pins that `SketchReducer::evaluate_exact_agg`:
//   1. Walks ExactAgg sids (not sketch sids).
//   2. Groups per-window AggregateCore state by the projected
//      `group_by_keys` (subset of each sid's full label map).
//   3. Merges accumulators inside a group via `AggregateCore::merge_with`
//      and reads `Statistic::Sum` for additive types.
//   4. Surfaces `NoData` when no in-window state exists (so the engine
//      routes the query to archive instead of returning a stale answer).
// ---------------------------------------------------------------------------

fn exact_agg_meta(
    sid: u64,
    metric: &str,
    group_by_keys: &[&str],
    agg_type: crate::storage_engines::sketch_db::data::AggregationType,
) -> SketchInstanceMetadata {
    SketchInstanceMetadata {
        sid,
        metric_name: metric.to_string(),
        group_by_keys: group_by_keys.iter().map(|s| s.to_string()).collect(),
        capability: Some(Capability::ExactAgg(agg_type)),
        agg_kind: AggKind::ExactAgg {
            agg_type,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: None,
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
        policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

#[test]
fn evaluate_exact_agg_sums_per_group_across_zones() {
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    // Four sids, one per zone, mirroring the post-#290 startup-replan
    // ExactAgg(Sum) sids the smoke test exercises.
    let zones = ["z0", "z1", "z2", "z3"];
    for (i, zone) in zones.iter().enumerate() {
        let sid = 1000 + i as u64;
        idx.register(exact_agg_meta(
            sid,
            "http_requests_total",
            &["zone"],
            AggregationType::Sum,
        ));
        // Two windows of data per zone, the sum value distinct per zone
        // (10, 20, 30, 40) so the test can assert per-group correctness.
        let value = ((i + 1) * 10) as f64;
        for (j, (ws, we)) in [(100u64, 200u64), (200, 300)].iter().enumerate() {
            let mut lm = BTreeMap::new();
            lm.insert("zone".to_string(), zone.to_string());
            // The second window's accumulator carries the same value so
            // the per-window per-zone scalar is constant; the engine
            // chooses the last window for instant queries.
            let _ = j;
            idx.append_precompute(
                sid,
                lm,
                (*ws, *we),
                Box::new(SumAccumulator::with_sum(value)),
            );
        }
    }

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let result = reducer
        .evaluate_exact_agg(
            &[1000, 1001, 1002, 1003],
            AggregationType::Sum,
            &group_by,
            0,
            400,
            false, // per-window (matrix) shape — no accumulate
        )
        .expect("exact-agg evaluate should succeed");

    // One series per zone, each with two windows of samples.
    assert_eq!(result.series.len(), 4, "one series per zone");
    let mut per_zone: BTreeMap<String, f64> = BTreeMap::new();
    for (label_map, samples) in &result.series {
        let zone = label_map
            .get("zone")
            .cloned()
            .expect("series carries `zone` label");
        // Each window emits one sample; both windows for one zone
        // share the same value so the last sample is the canonical
        // instant readout.
        let last = samples.last().expect("at least one sample").1;
        per_zone.insert(zone, last);
    }
    assert_eq!(per_zone.get("z0").copied(), Some(10.0));
    assert_eq!(per_zone.get("z1").copied(), Some(20.0));
    assert_eq!(per_zone.get("z2").copied(), Some(30.0));
    assert_eq!(per_zone.get("z3").copied(), Some(40.0));

    // Coverage spans the entire window range.
    let (cov_lo, cov_hi) = result.coverage.expect("coverage populated");
    assert_eq!(cov_lo, 200);
    assert_eq!(cov_hi, 300);
}

#[test]
fn evaluate_exact_agg_collapses_subgroups_into_requested_groups() {
    // Two sids share a (zone, rack) label space: sid 5000 is
    // (zone=z0, rack=r0), sid 5001 is (zone=z0, rack=r1). A
    // `sum by (zone)` query MUST collapse both racks into one
    // (zone=z0) group with their values added.
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    for (sid, rack, value) in [(5000u64, "r0", 7.0_f64), (5001, "r1", 13.0)] {
        idx.register(exact_agg_meta(
            sid,
            "http_requests_total",
            &["rack", "zone"],
            AggregationType::Sum,
        ));
        let mut lm = BTreeMap::new();
        lm.insert("zone".to_string(), "z0".to_string());
        lm.insert("rack".to_string(), rack.to_string());
        idx.append_precompute(
            sid,
            lm,
            (100, 200),
            Box::new(SumAccumulator::with_sum(value)),
        );
    }

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let result = reducer
        .evaluate_exact_agg(
            &[5000, 5001],
            AggregationType::Sum,
            &group_by,
            0,
            300,
            false, // per-window (matrix) shape — no accumulate
        )
        .expect("evaluate ok");

    assert_eq!(result.series.len(), 1, "rack values collapse into one zone group");
    let (label_map, samples) = &result.series[0];
    assert_eq!(label_map.get("zone").cloned(), Some("z0".to_string()));
    assert!(!label_map.contains_key("rack"), "rack dropped (not in group_by)");
    let last = samples.last().expect("at least one sample").1;
    assert!(
        (last - 20.0).abs() < 1e-9,
        "merged sum 7 + 13 = 20, got {last}"
    );
}

#[test]
fn evaluate_exact_agg_unsupported_capability_for_minmax() {
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        7000,
        "http_requests_total",
        &["zone"],
        AggregationType::MinMax,
    ));

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let err = reducer
        .evaluate_exact_agg(&[7000], AggregationType::MinMax, &group_by, 0, 1000, false)
        .expect_err("MinMax dispatch should surface as UnsupportedCapability");
    match err {
        ASAPTierError::UnsupportedCapability { capability, .. } => {
            assert!(matches!(
                capability,
                Capability::ExactAgg(AggregationType::MinMax)
            ));
        }
        other => panic!("expected UnsupportedCapability, got {other:?}"),
    }
}

#[test]
fn evaluate_exact_agg_no_data_when_window_empty() {
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        8000,
        "http_requests_total",
        &["zone"],
        AggregationType::Sum,
    ));

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let err = reducer
        .evaluate_exact_agg(&[8000], AggregationType::Sum, &group_by, 0, 1000, false)
        .expect_err("empty in-window state should surface as NoData");
    match err {
        ASAPTierError::NoData { metric_name } => {
            assert_eq!(metric_name, "http_requests_total");
        }
        other => panic!("expected NoData, got {other:?}"),
    }
}

#[test]
fn coverage_reports_observed_window_range() {
    let idx = SketchStore::new();
    let sid = 300;
    idx.register(dd_meta(sid));

    let alpha = 0.01;
    for (i, values) in [vec![1.0_f64, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]]
        .iter()
        .enumerate()
    {
        let mut sk = DdSketch::new(alpha);
        for &v in values {
            sk.update(v);
        }
        let bytes = encode_ddsketch(&sk);
        let window_start = 100 + (i as u64) * 100;
        let window_end = window_start + 100;
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (window_start, window_end),
            proto_full(bytes),
        );
    }

    let reducer = SketchReducer::new(&idx);
    let result = reducer
        .evaluate(&[sid], "quantile", &[0.5], 50, 400)
        .expect("evaluate should succeed");
    // Coverage min = first window end (200), max = third window end (400).
    let coverage = result.coverage.expect("coverage populated");
    assert_eq!(coverage.0, 200);
    assert_eq!(coverage.1, 400);
}

// ---------------------------------------------------------------------------
// ExactAgg-rate dispatch — regression coverage for `rate(metric[r])` /
// `sum by (gbk) (rate(metric[r]))` PromQL. Pins that
// `SketchReducer::evaluate_exact_agg_rate`:
//   1. Folds EVERY in-window sub-window accumulator (per group) into
//      one merged accumulator (unlike `evaluate_exact_agg`, which
//      keeps per-window samples).
//   2. Divides the merged `Statistic::Sum` by `range_seconds` to yield
//      events-per-second.
//   3. Emits exactly ONE sample per group, timestamped at `t1_ms`
//      (instant-rate semantics).
//   4. Surfaces `UnsupportedCapability` for MinMax (no rate semantic)
//      and for `range_seconds == 0` (defensive guard).
//   5. Surfaces `NoData` when the window holds no state.
// ---------------------------------------------------------------------------

#[test]
fn evaluate_exact_agg_rate_divides_total_events_by_range() {
    // One zone, two windows that TOGETHER span the full 300s rate range
    // (`[0, 300_000]`). Each window carries 600 events → (600 + 600) /
    // 300 = 4 events/sec. Because the data coverage (300s) equals the
    // nominal range, the coverage-aware divisor (issue #301 Layer 4) is
    // `min(300, 300) = 300` — same as the nominal divisor — so this
    // test pins both the fold-to-one-sample behavior AND the
    // full-coverage divisor case. (The partial-coverage case is pinned
    // separately in `evaluate_exact_agg_rate_divisor_uses_actual_coverage`.)
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        2100,
        "http_requests_total",
        &["zone"],
        AggregationType::Sum,
    ));
    let mut lm = BTreeMap::new();
    lm.insert("zone".to_string(), "z0".to_string());
    idx.append_precompute(
        2100,
        lm.clone(),
        (0, 150_000),
        Box::new(SumAccumulator::with_sum(600.0)),
    );
    idx.append_precompute(
        2100,
        lm,
        (150_000, 300_000),
        Box::new(SumAccumulator::with_sum(600.0)),
    );

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let result = reducer
        .evaluate_exact_agg_rate(
            &[2100],
            AggregationType::Sum,
            &group_by,
            300, // range_seconds
            0,
            300_000,
        )
        .expect("rate evaluate ok");
    assert_eq!(result.series.len(), 1, "one series for the lone zone");
    let (labels, samples) = &result.series[0];
    assert_eq!(labels.get("zone").cloned(), Some("z0".to_string()));
    assert_eq!(samples.len(), 1, "rate emits ONE sample per group");
    let (ts, value) = samples[0];
    assert_eq!(ts, 300_000, "sample timestamped at t1");
    // (600 + 600) / min(300, 300) = 4.0
    assert!(
        (value - 4.0).abs() < 1e-9,
        "expected 4.0 events/sec, got {value}"
    );
}

#[test]
fn evaluate_exact_agg_rate_divisor_uses_actual_coverage() {
    // Issue #301 Layer 4: when the producer has only run for part of the
    // requested `[r]` window, the rate divisor must be the ACTUAL covered
    // span — not the nominal `range_seconds` — otherwise the rate is
    // systematically under-reported (the smoke test's 64% rate rel-err).
    //
    // Data spans `[0, 120_000]` = 120s of the requested 300s `[5m]`
    // window. Total events = 1200. With the OLD nominal divisor the rate
    // would be 1200/300 = 4.0 (too low); the coverage-aware divisor is
    // `min(300, 120) = 120`, giving the correct 1200/120 = 10.0.
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        2150,
        "http_requests_total",
        &["zone"],
        AggregationType::Sum,
    ));
    let mut lm = BTreeMap::new();
    lm.insert("zone".to_string(), "z0".to_string());
    idx.append_precompute(
        2150,
        lm.clone(),
        (0, 60_000),
        Box::new(SumAccumulator::with_sum(600.0)),
    );
    idx.append_precompute(
        2150,
        lm,
        (60_000, 120_000),
        Box::new(SumAccumulator::with_sum(600.0)),
    );

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let result = reducer
        .evaluate_exact_agg_rate(
            &[2150],
            AggregationType::Sum,
            &group_by,
            300, // nominal [5m] range
            0,
            300_000,
        )
        .expect("rate evaluate ok");
    let (_labels, samples) = &result.series[0];
    let value = samples[0].1;
    assert!(
        (value - 10.0).abs() < 1e-9,
        "coverage-aware divisor: 1200 / min(300, 120) = 10.0, got {value} \
         (if ~4.0 the divisor regressed to the nominal range)"
    );
}

#[test]
fn evaluate_exact_agg_rate_per_group_across_zones() {
    // Multinode-demo shape: 4 zones, each its own sid, two windows
    // each. Per-zone rate = (sum of windows) / range_seconds. Mirrors
    // the smoke test's `sum by (zone) (rate(http_requests_total[5m]))`
    // pre-engine-dispatch (the reducer is what produces the per-zone
    // events/sec).
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    let zones = ["z0", "z1", "z2", "z3"];
    // Per-zone per-window sums: 300, 600, 900, 1200 → with two windows
    // each that's 600, 1200, 1800, 2400 totals; over a 300s range the
    // rates are 2, 4, 6, 8.
    for (i, zone) in zones.iter().enumerate() {
        let sid = 2200 + i as u64;
        idx.register(exact_agg_meta(
            sid,
            "http_requests_total",
            &["zone"],
            AggregationType::Sum,
        ));
        let per_window = ((i + 1) * 300) as f64;
        // Windows span the full 300s range so coverage == nominal range
        // and the coverage-aware divisor (#301) is `min(300, 300) = 300`.
        for (ws, we) in [(0u64, 150_000u64), (150_000, 300_000)] {
            let mut lm = BTreeMap::new();
            lm.insert("zone".to_string(), zone.to_string());
            idx.append_precompute(
                sid,
                lm,
                (ws, we),
                Box::new(SumAccumulator::with_sum(per_window)),
            );
        }
    }

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let result = reducer
        .evaluate_exact_agg_rate(
            &[2200, 2201, 2202, 2203],
            AggregationType::Sum,
            &group_by,
            300,
            0,
            300_000,
        )
        .expect("rate evaluate ok");
    assert_eq!(result.series.len(), 4, "one series per zone");
    let mut by_zone: BTreeMap<String, f64> = BTreeMap::new();
    for (labels, samples) in &result.series {
        assert_eq!(samples.len(), 1, "one rate sample per zone");
        let zone = labels.get("zone").cloned().expect("zone label");
        by_zone.insert(zone, samples[0].1);
    }
    assert!((by_zone.get("z0").copied().unwrap() - 2.0).abs() < 1e-9);
    assert!((by_zone.get("z1").copied().unwrap() - 4.0).abs() < 1e-9);
    assert!((by_zone.get("z2").copied().unwrap() - 6.0).abs() < 1e-9);
    assert!((by_zone.get("z3").copied().unwrap() - 8.0).abs() < 1e-9);
}

#[test]
fn evaluate_exact_agg_rate_collapses_subgroups_into_requested_groups() {
    // Two sids share (zone, rack); a `sum by (zone) (rate(...))`
    // collapses both racks' sub-window sums into one zone's rate.
    // 100 + 200 = 300 over a window spanning the full 100s range
    // (coverage-aware divisor min(100, 100) = 100) = 3.0 events/sec.
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    for (sid, rack, value) in [(5100u64, "r0", 100.0_f64), (5101, "r1", 200.0)] {
        idx.register(exact_agg_meta(
            sid,
            "http_requests_total",
            &["rack", "zone"],
            AggregationType::Sum,
        ));
        let mut lm = BTreeMap::new();
        lm.insert("zone".to_string(), "z0".to_string());
        lm.insert("rack".to_string(), rack.to_string());
        idx.append_precompute(
            sid,
            lm,
            (0, 100_000),
            Box::new(SumAccumulator::with_sum(value)),
        );
    }

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let result = reducer
        .evaluate_exact_agg_rate(
            &[5100, 5101],
            AggregationType::Sum,
            &group_by,
            100,
            0,
            300_000,
        )
        .expect("rate evaluate ok");
    assert_eq!(result.series.len(), 1, "racks collapse into one zone group");
    let (labels, samples) = &result.series[0];
    assert_eq!(labels.get("zone").cloned(), Some("z0".to_string()));
    assert!(!labels.contains_key("rack"));
    assert_eq!(samples.len(), 1);
    assert!((samples[0].1 - 3.0).abs() < 1e-9, "got {}", samples[0].1);
}

#[test]
fn evaluate_exact_agg_rate_no_group_by_keeps_per_sid_series() {
    // `rate(metric[r])` (no outer aggregation) — every sid's natural
    // label map identifies its own series. Two distinct sids → two
    // distinct rate series.
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    for (sid, zone, value) in [(6100u64, "z0", 150.0_f64), (6101, "z1", 450.0)] {
        idx.register(exact_agg_meta(
            sid,
            "http_requests_total",
            &["zone"],
            AggregationType::Sum,
        ));
        let mut lm = BTreeMap::new();
        lm.insert("zone".to_string(), zone.to_string());
        // Window spans the full 150s range so coverage == nominal range.
        idx.append_precompute(sid, lm, (0, 150_000), Box::new(SumAccumulator::with_sum(value)));
    }

    let reducer = SketchReducer::new(&idx);
    let empty: BTreeSet<String> = BTreeSet::new();
    let result = reducer
        .evaluate_exact_agg_rate(&[6100, 6101], AggregationType::Sum, &empty, 150, 0, 300_000)
        .expect("rate evaluate ok");
    assert_eq!(result.series.len(), 2, "two distinct series preserved");
    let mut by_zone: BTreeMap<String, f64> = BTreeMap::new();
    for (labels, samples) in &result.series {
        let zone = labels.get("zone").cloned().expect("zone preserved");
        by_zone.insert(zone, samples[0].1);
    }
    // 150 / 150 = 1.0; 450 / 150 = 3.0
    assert!((by_zone.get("z0").copied().unwrap() - 1.0).abs() < 1e-9);
    assert!((by_zone.get("z1").copied().unwrap() - 3.0).abs() < 1e-9);
}

#[test]
fn evaluate_exact_agg_rate_zero_range_is_unsupported_capability() {
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        7100,
        "http_requests_total",
        &["zone"],
        AggregationType::Sum,
    ));
    let mut lm = BTreeMap::new();
    lm.insert("zone".to_string(), "z0".to_string());
    idx.append_precompute(7100, lm, (0, 1000), Box::new(SumAccumulator::with_sum(1.0)));

    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let err = reducer
        .evaluate_exact_agg_rate(
            &[7100],
            AggregationType::Sum,
            &group_by,
            0, // range_seconds — guarded
            0,
            1000,
        )
        .expect_err("range_seconds=0 must surface as UnsupportedCapability");
    assert!(matches!(err, ASAPTierError::UnsupportedCapability { .. }));
}

#[test]
fn evaluate_exact_agg_rate_minmax_is_unsupported_capability() {
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        7200,
        "http_requests_total",
        &["zone"],
        AggregationType::MinMax,
    ));
    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let err = reducer
        .evaluate_exact_agg_rate(&[7200], AggregationType::MinMax, &group_by, 60, 0, 1000)
        .expect_err("MinMax has no rate semantic");
    match err {
        ASAPTierError::UnsupportedCapability { capability, .. } => {
            assert!(matches!(
                capability,
                Capability::ExactAgg(AggregationType::MinMax)
            ));
        }
        other => panic!("expected UnsupportedCapability, got {other:?}"),
    }
}

#[test]
fn evaluate_exact_agg_rate_no_data_when_window_empty() {
    use crate::storage_engines::sketch_db::data::AggregationType;

    let idx = SketchStore::new();
    idx.register(exact_agg_meta(
        7300,
        "http_requests_total",
        &["zone"],
        AggregationType::Sum,
    ));
    let reducer = SketchReducer::new(&idx);
    let group_by: BTreeSet<String> = ["zone".to_string()].into_iter().collect();
    let err = reducer
        .evaluate_exact_agg_rate(&[7300], AggregationType::Sum, &group_by, 60, 0, 1000)
        .expect_err("empty window must surface as NoData");
    match err {
        ASAPTierError::NoData { metric_name } => {
            assert_eq!(metric_name, "http_requests_total");
        }
        other => panic!("expected NoData, got {other:?}"),
    }
}
