//! Phase 3 step 3 acceptance tests: backend ingest **consumes**
//! `asap-precompute-rs` for the shared envelope-parsing,
//! sketch-reconstruction, and merge logic.
//!
//! Each test produces an envelope via `asap-precompute-rs`'s wrappers
//! (the canonical Rust edge runtime) and routes the bytes through the
//! backend's runtime adapter (`precompute_operators::edge_runtime_adapter`).
//! Successful round-trips prove that the asap-precompute-rs `Sketch`
//! trait family is sitting in the backend's ingest path — i.e. the
//! shared logic actually runs in this repo, not just in agents.
//!
//! Sketch coverage:
//! - **DDSketch**: round-trip + structural assertions are live — DDSketch
//!   is a deterministic histogram, so `snapshot → reconstruct → snapshot`
//!   is byte-identical (`asap_sketchlib`#40).
//! - **KLL**: the *structural* round-trip is live, but the **byte-parity**
//!   round-trip is gated `#[ignore]`. KLL compaction is randomized and
//!   lossy; `reconstruct_via_runtime` rebuilds the sketch by replaying the
//!   envelope's retained items through `apply_delta`, which re-compacts
//!   them (different RNG, double compaction) — so the re-snapshot is a
//!   valid KLL summary but not byte-identical to the original. True byte
//!   parity needs an `asap_sketchlib` `KLL::from_wire_state` that consumes
//!   the on-wire `levels`/`items`/`coin` directly — tracked in
//!   `asap_sketchlib`#41.
//! - **HLL + CountSketch + CountMinSketch**: the byte-parity work
//!   for these three sketches is tracked under
//!   `ProjectASAP/ASAPCollector#243`. Tests are present and gated
//!   `#[ignore = "blocked on ASAPCollector#243 HLL/CS/CMS byte parity"]`
//!   so the gap is visible without breaking CI.

use asap_precompute_rs::sketches::{
    CMSWrapper, CountSketchWrapper, DDSketchWrapper, HLLWrapper, KLLWrapper,
};
use asap_precompute_rs::Sketch;

use data_plane::precompute_engine::operators::edge_runtime_adapter::{
    encode_ddsketch_envelope, reconstruct_via_runtime, snapshot_ddsketch_via_runtime,
    unwrap_envelope_state, ReconstructedSketch, SketchType,
};
use data_plane::storage_engines::types::AggregateCore;

// --- DDSketch -----------------------------------------------------

/// Round-trip: an envelope produced by asap-precompute-rs's
/// `DDSketchWrapper` is reconstructed by the backend's runtime adapter
/// to a backend-shaped `DdSketch`, re-encoded through the same
/// envelope shape, and the resulting bytes match the original.
///
/// This proves the **shared envelope wire format** flows through both
/// crates with byte parity — the dedup target.
#[test]
fn ddsketch_envelope_round_trip_through_backend_adapter() {
    let mut w = DDSketchWrapper::new(0.01);
    for i in 1..=200 {
        w.update(i as f64);
    }
    let original = w.snapshot().expect("DDSketchWrapper snapshot");
    assert!(!original.is_empty());

    let reconstructed = reconstruct_via_runtime(SketchType::DDSketch, &original)
        .expect("runtime adapter reconstruction");
    let dd = match reconstructed {
        ReconstructedSketch::DdSketch(d) => d,
        _ => panic!("expected DDSketch reconstruction"),
    };
    // `count` is recovered from the bucket store now that the scalar was
    // dropped (ProjectASAP/sketchlib-go#243 / asap_sketchlib#57).
    assert_eq!(
        dd.total_count(),
        200,
        "count preserved through runtime adapter"
    );

    let re_encoded = encode_ddsketch_envelope(&dd);
    assert_eq!(
        re_encoded, original,
        "envelope round-trip via asap-precompute-rs runtime must be byte-identical"
    );
}

/// Structural: an envelope produced by asap-precompute-rs's
/// `DDSketchWrapper` is unwrapped via the backend's
/// `unwrap_envelope_state` (which itself goes through asap-precompute-rs's
/// `ProtoSketchEnvelope`), and the typed inner state has the expected
/// count + alpha shape.
#[test]
fn ddsketch_envelope_structural_assertions() {
    use asap_sketchlib::proto::sketchlib::sketch_envelope::SketchState;

    let mut w = DDSketchWrapper::new(0.005);
    w.update(1.0);
    w.update(2.0);
    w.update(3.0);
    let bytes = w.snapshot().expect("snapshot");
    let state = unwrap_envelope_state(&bytes)
        .expect("unwrap")
        .expect("state");
    match state {
        SketchState::Ddsketch(s) => {
            // `count` was dropped from `DdSketchState`
            // (ProjectASAP/sketchlib-go#243 / asap_sketchlib#57); it is
            // recovered by summing the bucket store counts.
            assert_eq!(s.store_counts.iter().sum::<u64>(), 3, "structural count");
            assert!(
                s.alpha > 0.0 && s.alpha < 1.0,
                "alpha within (0,1): got {}",
                s.alpha
            );
        }
        other => panic!("expected DDSketch state, got {other:?}"),
    }
}

/// End-to-end: DDSketch envelope → backend `AggregateCore` (via the
/// runtime adapter), then the backend's `query_statistic` API answers a
/// quantile query on the reconstructed accumulator. This proves the
/// **adapter integration is live** — backend ingest goes through
/// asap-precompute-rs and the resulting accumulator works on the
/// query-side surface.
#[test]
fn ddsketch_envelope_ends_up_in_backend_accumulator() {
    use data_plane::precompute_engine::operators::DDSketchAccumulator;

    let mut w = DDSketchWrapper::new(0.01);
    for i in 1..=100 {
        w.update(i as f64);
    }
    let bytes = w.snapshot().expect("snapshot");

    let reconstructed = reconstruct_via_runtime(SketchType::DDSketch, &bytes).expect("reconstruct");
    let dd = match reconstructed {
        ReconstructedSketch::DdSketch(d) => d,
        _ => panic!(),
    };
    let acc = DDSketchAccumulator {
        inner: dd,
        sample_p: 1.0,
    };

    let q = acc
        .query_statistic(
            asap_types::Statistic::Quantile,
            &None,
            &[("quantile".to_string(), "0.5".to_string())]
                .into_iter()
                .collect(),
        )
        .expect("quantile query");
    assert!(
        (q - 50.0).abs() / 50.0 < 0.05,
        "median estimate close to 50: got {q}"
    );
    let count = acc
        .query_statistic(asap_types::Statistic::Count, &None, &Default::default())
        .expect("count query");
    assert_eq!(count as u64, 100);
}

/// Snapshot a backend-side `DdSketch` *back through*
/// asap-precompute-rs's `Sketch::snapshot` and assert byte-equality
/// with the canonical envelope bytes. Closes the round-trip
/// (encode side) — proves backend can EMIT the same wire bytes as
/// asap-precompute-rs.
#[test]
fn ddsketch_backend_sketch_snapshots_to_canonical_envelope_bytes() {
    let mut w = DDSketchWrapper::new(0.01);
    for i in 1..=50 {
        w.update(i as f64);
    }
    let canonical = w.snapshot().expect("snapshot");
    let dd = match reconstruct_via_runtime(SketchType::DDSketch, &canonical).unwrap() {
        ReconstructedSketch::DdSketch(d) => d,
        _ => panic!(),
    };
    let via_runtime = snapshot_ddsketch_via_runtime(&dd).expect("runtime snapshot");
    assert_eq!(
        via_runtime, canonical,
        "snapshot via runtime adapter is byte-identical to source envelope"
    );
}

// --- KLL ----------------------------------------------------------

/// Round-trip: a KLL envelope produced by asap-precompute-rs's
/// `KLLWrapper` is reconstructed by the backend's runtime adapter,
/// which returns a re-snapshot from the runtime.
///
/// **Byte parity is gated `#[ignore]`** — see below. `reconstruct_via_runtime`
/// rebuilds the KLL by replaying the envelope's retained `items` through
/// `apply_delta` → `update()`. For an input large enough to compact
/// (here, 400 items at k=200 → 2 levels), that re-feed re-compacts
/// lossily and with a different RNG seed than the original, so the
/// re-snapshot — though a valid KLL summary — is not byte-identical.
/// True byte parity needs an `asap_sketchlib` `KLL::from_wire_state`
/// that restores the exact compactor state (`levels`/`items`/`coin`)
/// instead of replaying items; tracked in `asap_sketchlib`#41. The
/// *structural* round-trip is covered live by the test below.
#[test]
#[ignore = "KLL byte parity needs asap_sketchlib KLL::from_wire_state (asap_sketchlib#41); \
            reconstruct_via_runtime replays items, which re-compacts lossily"]
fn kll_envelope_round_trip_through_backend_adapter() {
    let mut w = KLLWrapper::new(200, Some(42));
    for i in 1..=400 {
        w.update(i as f64);
    }
    let original = w.snapshot().expect("KLL snapshot");
    assert!(!original.is_empty());

    let reconstructed = reconstruct_via_runtime(SketchType::KLLSketch, &original)
        .expect("runtime adapter reconstruction");
    let snapshot_bytes = match reconstructed {
        ReconstructedSketch::Kll { snapshot_bytes } => snapshot_bytes,
        _ => panic!("expected KLL reconstruction"),
    };
    assert!(!snapshot_bytes.is_empty(), "non-empty re-snapshot");
    assert_eq!(
        snapshot_bytes, original,
        "KLL envelope round-trip via asap-precompute-rs runtime is byte-identical"
    );
}

/// Structural: KLL envelope unwraps to the expected oneof variant via
/// the shared `unwrap_envelope_state` helper.
#[test]
fn kll_envelope_structural_assertions() {
    use asap_sketchlib::proto::sketchlib::sketch_envelope::SketchState;

    let mut w = KLLWrapper::new(200, Some(7));
    for i in 1..=10 {
        w.update(i as f64);
    }
    let bytes = w.snapshot().expect("snapshot");
    let state = unwrap_envelope_state(&bytes)
        .expect("unwrap")
        .expect("state");
    match state {
        SketchState::Kll(s) => {
            assert_eq!(s.k, 200, "structural k");
            assert_eq!(s.items.len(), 10, "all 10 items retained");
        }
        other => panic!("expected KLL state, got {other:?}"),
    }
}

// --- Sketches gated on ASAPCollector#243 --------------------------

#[test]
#[ignore = "blocked on ASAPCollector#243 HLL/CS/CMS byte parity"]
fn hll_envelope_round_trip_through_backend_adapter() {
    use asap_sketchlib::HllVariant;
    let mut w = HLLWrapper::new(HllVariant::Regular, 12);
    for i in 0..1000u32 {
        w.update(&i.to_le_bytes());
    }
    let bytes = w.snapshot().expect("HLL snapshot");
    let _ = reconstruct_via_runtime(SketchType::HLLSketch, &bytes)
        .expect("HLL reconstruction (gated until #243)");
}

#[test]
#[ignore = "blocked on ASAPCollector#243 HLL/CS/CMS byte parity"]
fn countsketch_envelope_round_trip_through_backend_adapter() {
    let mut w = CountSketchWrapper::new(4, 1024);
    for i in 0..100u32 {
        w.update(&format!("k{}", i % 10), 1.0);
    }
    let bytes = w.snapshot().expect("CountSketch snapshot");
    let _ = reconstruct_via_runtime(SketchType::CountSketch, &bytes)
        .expect("CountSketch reconstruction (gated until #243)");
}

#[test]
#[ignore = "blocked on ASAPCollector#243 HLL/CS/CMS byte parity"]
fn cms_envelope_round_trip_through_backend_adapter() {
    let mut w = CMSWrapper::new(4, 1024);
    for i in 0..100u32 {
        w.update(&format!("k{}", i % 10), 1.0);
    }
    let bytes = w.snapshot().expect("CMS snapshot");
    let _ = reconstruct_via_runtime(SketchType::CountMinSketch, &bytes)
        .expect("CMS reconstruction (gated until #243)");
}
