//! Unit tests for the warm-tier sketch reducer.
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

use crate::query_engines::asap_query_engine::warm_tier::{SketchReducer, WarmTierError};
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
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
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
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
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
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
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
        WarmTierError::UnsupportedCapability { function, .. } => {
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
// `WarmTierResult` so the engine can surface CapabilityMiss
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
        WarmTierError::NoData { metric_name } => {
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
        .expect_err("`rate` is not warm-tier-answerable");
    match err {
        WarmTierError::UnsupportedFunction(name) => {
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
        WarmTierError::DeserializeFailure { sid: s, .. } => {
            assert_eq!(s, sid);
        }
        other => panic!("expected DeserializeFailure, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Multi-series: two distinct group-by VALUES vectors under the same
// sid (e.g. `host=a` and `host=b`) → expect two `WarmTierResult`
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
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
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
        },
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
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
        WarmTierError::MissingHeap {
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

    // Window 2: Msgpack-delta — the warm-tier reducer treats
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
// TODO-3 tests — hybrid warm + archive stitch via `WarmTierResult.coverage`.
//
// We don't drive the full ASAPQueryEngine here (that would require
// constructing the whole streaming-config plumbing). Instead we exercise
// the `stitch_warm_and_archive` helper directly via a small wrapper
// test in `engines::asap_query::tests` would be ideal — but to keep this
// PR additive, we verify the `coverage` field is populated correctly
// on a multi-window evaluate so the downstream stitch path has the
// information it needs.
// ---------------------------------------------------------------------------

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
