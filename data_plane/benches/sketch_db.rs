//! Criterion benchmarks for `SketchStore` — the ASAP-tier sketch DB.
//!
//! Run with: `cargo bench -p data_plane --bench sketch_db`
//!
//! Covers:
//!   * `append_sample`            — sketch insert
//!   * `append_precompute`        — precompute insert
//!   * `query_range`              — per-sid range query
//!   * `query_precomputes_by_agg` — cross-sid precompute scan
//!
//! Varied dimensions: sid cardinality, windows-per-sid (depth),
//! query-window width, and sketch kind (DDSketch / KLL / HLL).
//!
//! NOTE: the insert benches use `iter_batched_ref`, not `iter_batched`.
//! `iter_batched` (criterion-0.5.1 `src/bencher.rs:264-272`) consumes its
//! input by value, so the per-iteration `Drop` of the setup `SketchStore`
//! runs inside the timed region. The `SketchStore` Drop scales linearly
//! with `num_sids` (each `SketchInstanceMetadata` owns a `String`,
//! `BTreeSet`, `SketchConfig`, `Option<AccuracyBound>` — roughly ~100 ns
//! to drop apiece). At 10k sids that's ~1 ms of pure drop work per
//! "append" — i.e. ~1000× the actual `append_sample` cost. See
//! `examples/sketch_db_diag.rs` for the breakdown.

use std::collections::{BTreeMap, BTreeSet};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, DdSketchState, HllVariant as ProtoVariant, HyperLogLogState, KllState,
    SketchEnvelope,
};
use asap_sketchlib::DdSketch;
use asap_sketchlib::{HllSketch, HllVariant};
use prost::Message;

use data_plane::precompute_engine::operators::SumAccumulator;
use data_plane::storage_engines::sketch_db::data::{
    AccuracyBound, AggKind, AggregationType, Capability, SketchAlgorithm, SketchConfig,
    SketchEncoding,
};
use data_plane::storage_engines::sketch_db::index::{SketchInstanceMetadata, SketchStore};
use data_plane::storage_engines::SketchSampleState;

// ── Payload builders ────────────────────────────────────────────────────────

fn encode_ddsketch(values: &[f64], alpha: f64) -> Vec<u8> {
    let mut sk = DdSketch::new(alpha);
    for v in values {
        sk.update(*v);
    }
    let state = DdSketchState {
        alpha: sk.alpha,
        store_counts: sk.store_counts.clone(),
        store_offset: sk.store_offset,
    };
    SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
        ..Default::default()
    }
    .encode_to_vec()
}

fn encode_kll(k: u32, items: &[f64]) -> Vec<u8> {
    let state = KllState {
        k,
        items: items.to_vec(),
        levels: vec![],
        num_levels: 0,
        ..Default::default()
    };
    SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
        ..Default::default()
    }
    .encode_to_vec()
}

fn encode_hll(distinct_items: usize, precision: u32) -> Vec<u8> {
    let mut sk = HllSketch::new(HllVariant::Regular, precision);
    for i in 0..distinct_items {
        sk.update(format!("user-{i}").as_bytes());
    }
    let state = HyperLogLogState {
        variant: ProtoVariant::Regular as i32,
        precision: sk.precision,
        registers: sk.registers.clone(),
        hip_kxq0: sk.hip_kxq0,
        hip_kxq1: sk.hip_kxq1,
        hip_est: sk.hip_est,
        registers_sparse: None,
    };
    SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
        ..Default::default()
    }
    .encode_to_vec()
}

fn sample(bytes: Vec<u8>) -> SketchSampleState {
    SketchSampleState {
        bytes,
        encoding: SketchEncoding::ProtoFull,
    }
}

// ── Metadata builders ───────────────────────────────────────────────────────

fn sketch_meta(
    sid: u64,
    algorithm: SketchAlgorithm,
    config: SketchConfig,
) -> SketchInstanceMetadata {
    SketchInstanceMetadata {
        sid,
        metric_name: "bench_metric".into(),
        group_by_keys: BTreeSet::new(),
        capability: Some(match algorithm {
            SketchAlgorithm::Hll => Capability::CardinalityApprox,
            _ => Capability::QuantileApprox(Some(algorithm.clone())),
        }),
        accuracy: Some(AccuracyBound::from_config(&config)),
        agg_kind: AggKind::Sketch {
            algorithm,
            config,
            spatial_filter_canonical: String::new(),
        },
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
        policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

fn precompute_meta(sid: u64, metric: &str, agg_type: AggregationType) -> SketchInstanceMetadata {
    SketchInstanceMetadata {
        sid,
        metric_name: metric.to_string(),
        group_by_keys: BTreeSet::new(),
        capability: None,
        accuracy: None,
        agg_kind: AggKind::ExactAgg {
            agg_type,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
        },
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
        policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

#[derive(Clone)]
struct KindCase {
    name: &'static str,
    algorithm: SketchAlgorithm,
    build: fn() -> (SketchConfig, Vec<u8>),
}

fn dd_small() -> (SketchConfig, Vec<u8>) {
    let cfg = SketchConfig::DDSketch {
        relative_accuracy: 0.01,
    };
    let bytes = encode_ddsketch(&(1..=64).map(|i| i as f64).collect::<Vec<_>>(), 0.01);
    (cfg, bytes)
}

fn kll_small() -> (SketchConfig, Vec<u8>) {
    let cfg = SketchConfig::Kll { k: 200 };
    let items: Vec<f64> = (1..=64).map(|i| i as f64).collect();
    (cfg, encode_kll(200, &items))
}

fn hll_small() -> (SketchConfig, Vec<u8>) {
    let cfg = SketchConfig::Hll { precision: 10 };
    let bytes = encode_hll(256, 10);
    (cfg, bytes)
}

const KINDS: &[KindCase] = &[
    KindCase {
        name: "DDSketch",
        algorithm: SketchAlgorithm::DDSketch,
        build: dd_small,
    },
    KindCase {
        name: "KLL",
        algorithm: SketchAlgorithm::Kll,
        build: kll_small,
    },
    KindCase {
        name: "HLL",
        algorithm: SketchAlgorithm::Hll,
        build: hll_small,
    },
];

// ── append_sample ───────────────────────────────────────────────────────────
//
// Pre-populates the store with `num_sids` registered metadata entries once
// per setup call; the timed routine performs ONE `append_sample`. Using
// `iter_batched_ref` so the setup store is dropped OUTSIDE the timed region
// (the original `iter_batched` formulation timed that drop, which scales
// linearly with `num_sids` and was masking the real per-op cost — see the
// module doc-comment).

fn bench_append_sample(c: &mut Criterion) {
    let mut g = c.benchmark_group("append_sample");
    g.sample_size(20);

    for kind in KINDS {
        let (cfg, payload_bytes) = (kind.build)();
        for num_sids in [1usize, 100, 10_000] {
            g.throughput(Throughput::Elements(1));
            g.bench_function(
                BenchmarkId::new(kind.name, format!("sids={num_sids}")),
                |b| {
                    b.iter_batched_ref(
                        || {
                            let store = SketchStore::new();
                            for sid in 0..num_sids as u64 {
                                store.register(sketch_meta(
                                    sid + 1,
                                    kind.algorithm.clone(),
                                    cfg.clone(),
                                ));
                            }
                            (store, 0u64)
                        },
                        |(store, counter)| {
                            let c = *counter;
                            *counter = c.wrapping_add(1);
                            let sid = (c % num_sids as u64) + 1;
                            let win = (1_000 + c * 10, 1_000 + c * 10 + 10);
                            store.append_sample(
                                sid,
                                BTreeMap::new(),
                                win,
                                sample(payload_bytes.clone()),
                            );
                            black_box(&*store);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    g.finish();
}

// ── append_precompute ───────────────────────────────────────────────────────

fn bench_append_precompute(c: &mut Criterion) {
    let mut g = c.benchmark_group("append_precompute");
    g.sample_size(20);

    for num_sids in [1usize, 100, 10_000] {
        g.throughput(Throughput::Elements(1));
        g.bench_function(BenchmarkId::new("Sum", format!("sids={num_sids}")), |b| {
            b.iter_batched_ref(
                || {
                    let store = SketchStore::new();
                    for sid in 0..num_sids as u64 {
                        store.register(precompute_meta(
                            sid + 1,
                            "bench_metric",
                            AggregationType::Sum,
                        ));
                    }
                    (store, 0u64)
                },
                |(store, counter)| {
                    let c = *counter;
                    *counter = c.wrapping_add(1);
                    let sid = (c % num_sids as u64) + 1;
                    let win = (1_000 + c * 10, 1_000 + c * 10 + 10);
                    store.append_precompute(
                        sid,
                        BTreeMap::new(),
                        win,
                        Box::new(SumAccumulator::with_sum(c as f64)),
                    );
                    black_box(&*store);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

// ── query_range ─────────────────────────────────────────────────────────────

fn build_populated_store(depth: u64, kind: &KindCase, sid: u64) -> SketchStore {
    let store = SketchStore::new();
    let (cfg, payload_bytes) = (kind.build)();
    store.register(sketch_meta(sid, kind.algorithm.clone(), cfg));
    for i in 0..depth {
        let win = (1_000 + i * 10, 1_000 + i * 10 + 10);
        store.append_sample(sid, BTreeMap::new(), win, sample(payload_bytes.clone()));
    }
    store
}

fn bench_query_range(c: &mut Criterion) {
    let mut g = c.benchmark_group("query_range");
    g.sample_size(20);

    let sid = 1u64;
    for kind in KINDS {
        for depth in [10u64, 100, 1_000] {
            let store = build_populated_store(depth, kind, sid);
            let full_end = 1_000 + depth * 10 + 10;
            let cases = [
                ("w=1", (1_000u64, 1_010u64)),
                ("w=half", (1_000u64, 1_000 + (depth / 2) * 10)),
                ("w=full", (1_000u64, full_end)),
            ];
            for (width_label, (start, end)) in cases {
                g.throughput(Throughput::Elements(1));
                g.bench_function(
                    BenchmarkId::new(kind.name, format!("depth={depth}/{width_label}")),
                    |b| {
                        b.iter(|| {
                            let r =
                                store.query_range(black_box(sid), black_box(start), black_box(end));
                            black_box(r);
                        });
                    },
                );
            }
        }
    }
    g.finish();
}

// ── query_precomputes_by_agg ───────────────────────────────────────────────

fn build_precompute_store(num_sids: usize, windows_per_sid: u64, metric: &str) -> SketchStore {
    let store = SketchStore::new();
    for s in 0..num_sids as u64 {
        let sid = s + 1;
        store.register(precompute_meta(sid, metric, AggregationType::Sum));
        for i in 0..windows_per_sid {
            let win = (1_000 + i * 10, 1_000 + i * 10 + 10);
            store.append_precompute(
                sid,
                BTreeMap::new(),
                win,
                Box::new(SumAccumulator::with_sum((sid + i) as f64)),
            );
        }
    }
    store
}

fn bench_query_precomputes_by_agg(c: &mut Criterion) {
    let mut g = c.benchmark_group("query_precomputes_by_agg");
    g.sample_size(15);

    let metric = "bench_metric";
    let cases: &[(usize, u64)] = &[(1, 100), (100, 10), (100, 100), (10_000, 10)];
    for (num_sids, depth) in cases {
        let store = build_precompute_store(*num_sids, *depth, metric);
        let full_end = 1_000 + depth * 10 + 10;
        let widths = [
            ("w=1", (1_000u64, 1_010u64)),
            ("w=full", (1_000u64, full_end)),
        ];
        for (width_label, (start, end)) in widths {
            g.throughput(Throughput::Elements(*num_sids as u64));
            g.bench_function(
                BenchmarkId::new(format!("sids={num_sids}/depth={depth}"), width_label),
                |b| {
                    b.iter(|| {
                        let r = store.query_precomputes_by_agg(
                            black_box(metric),
                            AggregationType::Sum,
                            black_box(start),
                            black_box(end),
                        );
                        black_box(r);
                    });
                },
            );
        }
    }
    g.finish();
}

/// Build a `StreamingConfig` whose single agg-config's content
/// signature matches every sid registered by `build_precompute_store`
/// (metric / `Sum` / no grouping / empty params+filter). With this
/// config the reconciler retires nothing — the steady-state ingest
/// case, where the per-batch reconcile is pure scan overhead.
fn matching_streaming_config(metric: &str) -> data_plane::storage_engines::types::StreamingConfig {
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType as AT;
    use asap_types::KeyByLabelNames;
    use std::collections::HashMap;

    let cfg = AggregationConfig::new(
        AT::Sum,
        String::new(),
        HashMap::new(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        60,
        60,
        WindowKind::Tumbling,
        String::new(),
        metric.to_string(),
        None,
        None,
        None,
    );
    let mut map = HashMap::new();
    map.insert(1u64, cfg);
    data_plane::storage_engines::types::StreamingConfig::new(map)
}

/// `reconcile_from_streaming_config` ran on EVERY ingest batch and, in
/// the pre-optimization code, deep-cloned every `SketchInstanceMetadata`
/// in the catalog (`snapshot_instances()`) — the dominant ingest-path
/// CPU cost in live `perf` profiling (BTreeMap/String clone + malloc
/// churn). This bench measures one un-gated reconcile against a
/// populated store at a few catalog sizes, so the before/after clone
/// elimination is directly visible.
fn bench_reconcile_per_batch(c: &mut Criterion) {
    use std::time::Duration;

    let mut g = c.benchmark_group("reconcile_per_batch");
    g.sample_size(50);

    let metric = "bench_metric";
    for num_sids in [100usize, 1_000, 10_000] {
        let store = build_precompute_store(num_sids, 1, metric);
        let config = matching_streaming_config(metric);
        g.throughput(Throughput::Elements(num_sids as u64));
        g.bench_function(BenchmarkId::new("full_scan", num_sids), |b| {
            b.iter(|| {
                let summary =
                    data_plane::storage_engines::sketch_db::lifecycle::reconcile_from_streaming_config(
                        black_box(&store),
                        black_box(&config),
                        Duration::from_secs(60),
                    );
                black_box(summary);
            });
        });
    }
    g.finish();
}

fn bench_group_key_projection(c: &mut Criterion) {
    use data_plane::precompute_engine::group_key::intern_pairs;

    let cardinality = 100_000usize;
    let labels = (0..cardinality)
        .map(|index| (format!("region;{index}"), format!("service={index}")))
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("group_key_projection");
    group.throughput(Throughput::Elements(cardinality as u64));
    group.bench_function("cold_high_cardinality", |b| {
        b.iter(|| {
            for (region, service) in &labels {
                black_box(intern_pairs([
                    ("region", region.as_str()),
                    ("service", service.as_str()),
                ]));
            }
        });
    });
    // Warm the bounded interner, then measure shared-DAG reuse.
    for (region, service) in &labels {
        black_box(intern_pairs([
            ("region", region.as_str()),
            ("service", service.as_str()),
        ]));
    }
    group.bench_function("warm_shared_projection", |b| {
        b.iter(|| {
            for (region, service) in &labels {
                black_box(intern_pairs([
                    ("region", region.as_str()),
                    ("service", service.as_str()),
                ]));
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_append_sample,
    bench_append_precompute,
    bench_query_range,
    bench_query_precomputes_by_agg,
    bench_reconcile_per_batch,
    bench_group_key_projection,
);
criterion_main!(benches);
