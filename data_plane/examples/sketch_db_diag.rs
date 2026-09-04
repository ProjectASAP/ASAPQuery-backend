//! Diagnostic for the 10k-sids slowdown observed in `benches/sketch_db.rs`.
//!
//! The original bench used `Bencher::iter_batched(setup, routine, SmallInput)`.
//! Criterion's `iter_batched` consumes the input by value and the input's
//! `Drop` runs *inside* the timed region (see criterion-0.5.1
//! `src/bencher.rs:264-272`). Our `setup` builds a `SketchStore` with N
//! `SketchInstanceMetadata` entries registered; dropping that store at N=10_000
//! pays for 10k Drops of `metric_name: String`, `BTreeSet`, `SketchConfig`,
//! `AccuracyBound`, etc. That drop, not `append_sample`, is what makes the
//! 10k case look ~90× slower than the 100-sid case.
//!
//! This diagnostic separates three quantities:
//!   (A) rebuild-per-iter (matches the original bench): build + 1 append + drop
//!   (B) isolated append:                                pre-built store, K appends
//!   (C) isolated drop:                                  build, time drop only
//!
//! Run: `cargo run --release --example sketch_db_diag`

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
use asap_sketchlib::DdSketch;
use prost::Message;

use data_plane::storage_engines::sketch_db::data::{
    AccuracyBound, AggKind, Capability, SketchAlgorithm, SketchConfig, SketchEncoding,
    SketchSampleState,
};
use data_plane::storage_engines::sketch_db::index::{SketchInstanceMetadata, SketchStore};

fn ddsketch_payload() -> Vec<u8> {
    let mut sk = DdSketch::new(0.01);
    for v in 1..=64 {
        sk.update(v as f64);
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

fn dd_meta(sid: u64) -> SketchInstanceMetadata {
    let cfg = SketchConfig::DDSketch {
        relative_accuracy: 0.01,
    };
    SketchInstanceMetadata {
        sid,
        metric_name: "bench_metric".into(),
        group_by_keys: BTreeSet::new(),
        capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))),
        accuracy: Some(AccuracyBound::from_config(&cfg)),
        agg_kind: AggKind::Sketch {
            algorithm: SketchAlgorithm::DDSketch,
            config: cfg,
            spatial_filter_canonical: String::new(),
        },
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
        policy_fp: asap_types::PolicyFingerprint::UNSET,
    }
}

fn build_store(num_sids: u64) -> SketchStore {
    let store = SketchStore::new();
    for sid in 1..=num_sids {
        store.register(dd_meta(sid));
    }
    store
}

fn sample(bytes: Vec<u8>) -> SketchSampleState {
    SketchSampleState {
        bytes,
        encoding: SketchEncoding::ProtoFull,
    }
}

fn bench_a_rebuild_per_iter(num_sids: u64, iters: u64, payload: &[u8]) -> f64 {
    // (A) reproduces what `iter_batched` actually measured: build store, one
    // append, then drop the store — all timed.
    let t0 = Instant::now();
    for i in 0..iters {
        let store = build_store(num_sids);
        let win = (1_000 + i * 10, 1_000 + i * 10 + 10);
        store.append_sample(1, BTreeMap::new(), win, sample(payload.to_vec()));
        // drop happens at end of loop body — inside the timed region.
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

fn bench_b_isolated_append(num_sids: u64, iters: u64, payload: &[u8]) -> f64 {
    // (B) pre-build the store ONCE; loop pure `append_sample` calls.
    // The store accumulates depth over the run, but DashMap entry + intern
    // + columnar push is O(1) amortized.
    let store = build_store(num_sids);
    let t0 = Instant::now();
    for i in 0..iters {
        let sid = (i % num_sids) + 1;
        let win = (1_000 + i * 10, 1_000 + i * 10 + 10);
        store.append_sample(sid, BTreeMap::new(), win, sample(payload.to_vec()));
    }
    let avg = t0.elapsed().as_nanos() as f64 / iters as f64;
    drop(store);
    avg
}

fn bench_c_isolated_drop(num_sids: u64, iters: u64) -> f64 {
    // (C) drop-only cost. Build outside, drop inside, time the drop.
    let mut stores: Vec<SketchStore> = (0..iters).map(|_| build_store(num_sids)).collect();
    let t0 = Instant::now();
    while let Some(s) = stores.pop() {
        drop(s);
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

fn main() {
    let payload = ddsketch_payload();
    println!("payload bytes: {}", payload.len());
    println!();
    println!(
        "{:>10} | {:>14} | {:>14} | {:>14} | {:>14}",
        "num_sids", "(A) rebuild+1", "(B) append", "(C) drop", "A - B - C"
    );
    println!("{}", "-".repeat(80));

    for (num_sids, iters_a, iters_b, iters_c) in [
        (1u64, 50_000u64, 200_000u64, 50_000u64),
        (100, 5_000, 200_000, 5_000),
        (10_000, 500, 200_000, 500),
    ] {
        let a = bench_a_rebuild_per_iter(num_sids, iters_a, &payload);
        let b = bench_b_isolated_append(num_sids, iters_b, &payload);
        let c = bench_c_isolated_drop(num_sids, iters_c);
        let residual = a - b - c;
        println!(
            "{:>10} | {:>12.0} ns | {:>12.0} ns | {:>12.0} ns | {:>12.0} ns",
            num_sids, a, b, c, residual
        );
    }

    println!();
    println!("Legend:");
    println!(
        "  (A) per-iter store rebuild + 1 append + drop  (≈ what the criterion bench reported)"
    );
    println!("  (B) pure append_sample on a pre-built store    (the actual per-op cost)");
    println!("  (C) drop of a freshly-built store              (the accounting term we missed)");
    println!("  A - B - C should be ≈ build_store cost (register() x num_sids).");
}
