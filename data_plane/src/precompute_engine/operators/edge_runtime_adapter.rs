//! Edge-runtime adapter — Phase 3 step 3 of the ASAP edge-framework
//! migration (`docs/design-asap-edge-framework.md`, ADR-0002).
//!
//! Routes the **shared** ingest work (envelope wire-format parsing,
//! per-sketch state extraction, sketch reconstruction, sketch merge)
//! through the [`asap_precompute_rs`] crate so the same code runs in
//! agents (Rust edge runtime) and the backend (this repo). What stays
//! in this repo: the QUERY-side engine — PromQL aggregation, storage,
//! and query planning. See README §"Ingest path consumes
//! asap-precompute-rs" for the contract.
//!
//! # What's shared
//!
//! - **Envelope wire-format parsing.** The runtime view
//!   [`SketchEnvelope`] (renamed from a backend-internal struct to
//!   asap-precompute-rs's canonical type) decodes the on-the-wire
//!   `asap_sketchlib::proto::sketchlib::SketchEnvelope` proto plus the
//!   surrounding metadata (window bounds, `agg_id`, encoding tag,
//!   sketch type, host-neutral labels, metric name, count,
//!   temporality).
//! - **Per-sketch state extraction.** [`unwrap_envelope_state`] dispatches
//!   the [`asap_sketchlib::proto::sketchlib::sketch_envelope::SketchState`]
//!   oneof into a typed `*State` proto for the requested sketch type,
//!   producing the same diagnostic shape every accumulator's
//!   `from_sketchlib_proto_bytes` used to repeat by hand.
//! - **Sketch reconstruction (DDSketch + KLL today).**
//!   [`reconstruct_via_runtime`] constructs an asap-precompute-rs
//!   `*Wrapper`, runs `Sketch::apply_delta(envelope_bytes)` (which is
//!   the Layer-3 runtime's reconstruct-from-bytes path), then extracts
//!   the underlying `asap_sketchlib::sketches::*` state via
//!   `wrapper.inner().clone()`. DDSketch and KLL byte parity holds in
//!   `asap_sketchlib::main` (PRs #40, #41); HLL / CountSketch /
//!   CountMinSketch are tracked under
//!   ProjectASAP/ASAPCollector#243 — until those land we keep the
//!   backend's per-accumulator decoders for the three sketches and
//!   only delegate envelope-parsing.
//! - **Cross-runtime sketch merge.** [`merge_via_runtime`] uses
//!   asap-precompute-rs's `Sketch::merge` + `Sketch::snapshot` round-
//!   trip so the merge logic lives in one place. Identical results to
//!   `asap_sketchlib::sketches::*::merge_refs` because both call into
//!   the same underlying merge implementation.
//!
//! # What stays put
//!
//! - The backend's **per-accumulator query-side surface** (`AggregateCore`,
//!   `query_statistic`, `MergeableAccumulator`, ...) — query-side and
//!   not what asap-precompute-rs is for.
//! - **Sparse delta application** (`apply_proto_delta_bytes` on the
//!   backend's accumulators) — `asap_sketchlib` doesn't yet expose the
//!   `compute_delta` family (Go's `sketchlib-go` has it; tracked
//!   upstream), so asap-precompute-rs's wrappers fall back to "always
//!   full" delta encoding. Backend's typed-delta apply is independent
//!   and stays.

use asap_precompute_rs::{
    envelope::ProtoSketchEnvelope, sketches::DDSketchWrapper, sketches::KLLWrapper, Sketch,
};
use asap_sketchlib::proto::sketchlib::sketch_envelope::SketchState;
use prost::Message;

/// Re-export of asap-precompute-rs's runtime view of the
/// `SketchEnvelope` proto (the prost-generated wire-format type plus
/// surrounding host-neutral metadata: window bounds, `agg_id`,
/// encoding tag, sketch-type tag, labels, metric name, count,
/// temporality).
///
/// Kept as a re-export so all backend code that touches the runtime
/// envelope reaches for the canonical type from the edge-framework
/// runtime — preventing a "backend-flavored" runtime view from
/// drifting alongside the agent-flavored one.
pub use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};

/// Re-export the [`asap_precompute_rs::Sketch`] trait family so backend
/// code that wants to operate on envelope bytes via the host-neutral
/// runtime trait (`snapshot`, `apply_delta`, `merge`, `reset`) imports
/// from one place.
pub use asap_precompute_rs::{CardinalitySketch, FrequencySketch, QuantileSketch};

/// Decode the wire-format `asap_sketchlib` `SketchEnvelope` proto from
/// `bytes` and return the inner [`SketchState`] oneof variant.
///
/// Mirrors the per-accumulator `match SketchEnvelope::decode(buffer)
/// → Some(SketchState::X(st)) → st` ladder that every
/// `from_sketchlib_proto_bytes` used to inline. The fall-back to
/// decoding `bytes` as the bare typed-state proto is preserved at the
/// caller so the existing PR #14 contract continues to work for unit
/// tests that encode the state directly (without an envelope wrapper).
///
/// This is the **single shared envelope-unwrap path** between
/// asap-precompute-rs and backend ingest. asap-precompute-rs's
/// per-wrapper `decode_envelope` does the same prost-decode +
/// oneof-extraction work; the difference is that asap-precompute-rs
/// then constructs a typed `asap_sketchlib::sketches::*` from the
/// state, which the backend may or may not want depending on the
/// downstream caller.
pub fn unwrap_envelope_state(
    bytes: &[u8],
) -> Result<Option<SketchState>, Box<dyn std::error::Error>> {
    let env =
        ProtoSketchEnvelope::decode(bytes).map_err(|e| format!("decode SketchEnvelope: {e}"))?;
    Ok(env.sketch_state)
}

/// Result of [`reconstruct_via_runtime`] — backend uses the inner
/// `asap_sketchlib::sketches::*` to construct its own accumulator.
pub enum ReconstructedSketch {
    /// Reconstructed [`asap_sketchlib::DdSketch`] state.
    DdSketch(asap_sketchlib::DdSketch),
    /// Reconstructed KLL — asap-precompute-rs's [`KLLWrapper`] owns
    /// the high-throughput `asap_sketchlib::sketches::kll::KLL<f64>`
    /// internally; backend's KLL accumulator wraps the wire-format-
    /// aligned `KllSketch` instead. We surface the wrapper's
    /// re-snapshot bytes so the caller can route them through
    /// backend's existing `KllSketch::deserialize_msgpack` /
    /// proto-state path or replay items via `KllSketch::update()`.
    Kll {
        /// Bytes of the reconstructed sketch's snapshot — same shape
        /// as the input envelope, validated round-trip.
        snapshot_bytes: Vec<u8>,
    },
}

/// Reconstruct an `asap_sketchlib` sketch from envelope bytes by
/// delegating envelope parsing + sketch construction to
/// asap-precompute-rs's `Sketch` trait family.
///
/// The function is sketch-type-aware because the wrappers' constructor
/// parameters (alpha for DDSketch, k+seed for KLL, ...) live partly in
/// the envelope state proto. We peek the state, construct a wrapper
/// with matching parameters, then call [`Sketch::apply_delta`] which
/// runs asap-precompute-rs's canonical decode + reconstruct pathway.
///
/// Today wires DDSketch (byte parity per asap_sketchlib#40) and KLL
/// (byte parity per asap_sketchlib#41). HLL / CountSketch /
/// CountMinSketch are tracked under ProjectASAP/ASAPCollector#243 —
/// callers fall back to backend's per-accumulator decoder for those.
pub fn reconstruct_via_runtime(
    sketch_type: SketchType,
    envelope_bytes: &[u8],
) -> Result<ReconstructedSketch, Box<dyn std::error::Error>> {
    match sketch_type {
        SketchType::DDSketch => {
            // Peek the state to learn alpha, then construct the
            // wrapper with matching alpha so `apply_delta`'s merge
            // step doesn't trip on `DdSketch::merge`'s alpha-equality
            // guard.
            let state = unwrap_envelope_state(envelope_bytes)?;
            let alpha = match &state {
                Some(SketchState::Ddsketch(s)) => s.alpha,
                Some(_) => {
                    return Err("envelope is not a DDSketch".into());
                }
                None => return Err("envelope has no sketch_state".into()),
            };
            if !(alpha > 0.0 && alpha < 1.0) {
                return Err(format!("DDSketch alpha {alpha} out of (0,1)").into());
            }
            let mut wrapper = DDSketchWrapper::new(alpha);
            wrapper
                .apply_delta(envelope_bytes)
                .map_err(|e| format!("DDSketchWrapper apply_delta: {e}"))?;
            Ok(ReconstructedSketch::DdSketch(wrapper.inner().clone()))
        }
        SketchType::KLLSketch => {
            // KLL: peek `k`, construct an empty wrapper, apply.
            let state = unwrap_envelope_state(envelope_bytes)?;
            let k = match state {
                Some(SketchState::Kll(s)) => {
                    if s.k > i32::MAX as u32 {
                        return Err(format!("KllState.k too large: {}", s.k).into());
                    }
                    s.k as i32
                }
                Some(_) => return Err("envelope is not a KLL".into()),
                None => return Err("envelope has no sketch_state".into()),
            };
            let mut wrapper = KLLWrapper::new(k, None);
            wrapper
                .apply_delta(envelope_bytes)
                .map_err(|e| format!("KLLWrapper apply_delta: {e}"))?;
            // Re-snapshot via the wrapper's `Sketch::snapshot` —
            // canonical asap-precompute-rs encode of the reconstructed
            // state. Backend's KLL accumulator can then re-decode via
            // its existing `from_sketchlib_proto_bytes` path; the
            // edge-runtime adapter has done the envelope + state
            // unwrap, the wire-format reshape, and the reconstruction
            // round-trip.
            let snapshot_bytes = wrapper
                .snapshot()
                .map_err(|e| format!("KLLWrapper snapshot: {e}"))?;
            Ok(ReconstructedSketch::Kll { snapshot_bytes })
        }
        SketchType::HLLSketch | SketchType::CountSketch | SketchType::CountMinSketch => {
            Err(format!(
                "reconstruct_via_runtime({sketch_type:?}): byte parity for \
                 HLL / CountSketch / CountMinSketch not yet in upstream \
                 asap_sketchlib — tracked at ProjectASAP/ASAPCollector#243. \
                 Caller must fall back to backend's per-accumulator decoder."
            )
            .into())
        }
        SketchType::Unspecified => Err("reconstruct_via_runtime: SketchType::Unspecified".into()),
    }
}

/// Snapshot a backend-side `asap_sketchlib::DdSketch` through
/// asap-precompute-rs's `Sketch` trait — the canonical encode path
/// shared with the agent runtime. Used by the round-trip test
/// (`tests/edge_runtime_adapter.rs`).
pub fn snapshot_ddsketch_via_runtime(
    sk: &asap_sketchlib::DdSketch,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut wrapper = DDSketchWrapper::new(sk.alpha);
    // Bridge into the wrapper by merging in the existing sketch.
    // We can't move-construct the wrapper from a non-empty `DdSketch`,
    // but `Sketch::apply_delta` against the existing snapshot bytes
    // is equivalent.
    if sk.count > 0 {
        // Re-encode the source's state into the canonical envelope
        // shape that asap-precompute-rs's wrapper recognizes, then
        // round-trip through `apply_delta`. Mirrors the agent runtime's
        // own merge path.
        let bridge_bytes = encode_ddsketch_envelope(sk);
        wrapper
            .apply_delta(&bridge_bytes)
            .map_err(|e| format!("DDSketchWrapper apply_delta (bridge): {e}"))?;
    }
    wrapper
        .snapshot()
        .map_err(|e| format!("DDSketchWrapper snapshot: {e}").into())
}

/// Encode a backend-side `DdSketch` as the same `SketchEnvelope` proto
/// shape that asap-precompute-rs's `DDSketchWrapper::snapshot` emits.
///
/// Matches asap-precompute-rs's `DDSketchWrapper::build_state` +
/// `encode_envelope` byte-for-byte — they call into the same
/// `asap_sketchlib::proto::sketchlib::*` types. Lives here so the
/// backend's existing accumulators don't need to import the wrapper
/// internals.
pub fn encode_ddsketch_envelope(sk: &asap_sketchlib::DdSketch) -> Vec<u8> {
    use asap_sketchlib::proto::sketchlib::{
        sketch_envelope, DdSketchState, SketchEnvelope as ProtoEnvelope,
    };
    let state = DdSketchState {
        // Use `wire_alpha` so the bytes match Go's
        // `sketchlib-go::DDSketch.SerializePortable` (PR
        // asap_sketchlib#40 closes this).
        alpha: sk.wire_alpha(),
        store_counts: sk.store_counts.clone(),
        store_offset: sk.store_offset,
        count: sk.count,
        sum: sk.sum,
        min: if sk.count == 0 { f64::INFINITY } else { sk.min },
        max: if sk.count == 0 {
            f64::NEG_INFINITY
        } else {
            sk.max
        },
    };
    let env = ProtoEnvelope {
        format_version: 1,
        producer: None,
        hash_spec: None,
        sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
    };
    let mut buf = Vec::with_capacity(env.encoded_len());
    env.encode(&mut buf).expect("prost encode");
    buf
}

/// Merge two `DdSketch` instances by routing through asap-precompute-rs's
/// runtime `Sketch::merge`. The result is byte-identical to
/// `asap_sketchlib::DdSketch::merge_refs(&[a, b])` because
/// both paths call the same underlying merge logic.
///
/// Used by the cross-runtime parity test
/// (`tests/edge_runtime_adapter.rs::ddsketch_merge_via_runtime_matches_native`).
pub fn merge_ddsketches_via_runtime(
    a: &asap_sketchlib::DdSketch,
    b: &asap_sketchlib::DdSketch,
) -> Result<asap_sketchlib::DdSketch, Box<dyn std::error::Error>> {
    if (a.alpha - b.alpha).abs() > f64::EPSILON {
        return Err(format!(
            "merge_ddsketches_via_runtime: alpha mismatch ({} vs {})",
            a.alpha, b.alpha
        )
        .into());
    }
    let mut wrapper_a = DDSketchWrapper::new(a.alpha);
    if a.count > 0 {
        let bridge = encode_ddsketch_envelope(a);
        wrapper_a
            .apply_delta(&bridge)
            .map_err(|e| format!("merge_ddsketches_via_runtime/a: {e}"))?;
    }
    let mut wrapper_b = DDSketchWrapper::new(b.alpha);
    if b.count > 0 {
        let bridge = encode_ddsketch_envelope(b);
        wrapper_b
            .apply_delta(&bridge)
            .map_err(|e| format!("merge_ddsketches_via_runtime/b: {e}"))?;
    }
    // `Sketch::merge` takes a `&dyn Sketch` (round-trips through
    // snapshot bytes), which is the canonical Layer-3 runtime fold.
    wrapper_a
        .merge(&wrapper_b)
        .map_err(|e| format!("DDSketchWrapper merge: {e}"))?;
    Ok(wrapper_a.inner().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// asap-precompute-rs's wrapper produces an envelope; backend's
    /// shared envelope-unwrap path returns the matching oneof variant.
    /// The dedup target.
    #[test]
    fn unwrap_envelope_state_matches_wrapper_output() {
        let mut w = DDSketchWrapper::new(0.01);
        for i in 1..=10 {
            w.update(i as f64);
        }
        let bytes = w.snapshot().expect("snapshot ok");
        let state = unwrap_envelope_state(&bytes).expect("decode ok");
        match state {
            Some(SketchState::Ddsketch(s)) => {
                assert!(s.count > 0);
                assert!(s.alpha > 0.0 && s.alpha < 1.0);
            }
            other => panic!("expected DDSketch state, got {other:?}"),
        }
    }

    /// asap-precompute-rs's wrapper produces an envelope; the runtime
    /// adapter's reconstruction returns a backend-shaped
    /// `asap_sketchlib::DdSketch` whose serialized bytes
    /// (re-encoded through the same envelope shape) match the
    /// original.
    #[test]
    fn ddsketch_round_trip_through_runtime_adapter() {
        let mut w = DDSketchWrapper::new(0.01);
        for i in 1..=100 {
            w.update(i as f64);
        }
        let original_bytes = w.snapshot().expect("snapshot ok");
        let reconstructed =
            reconstruct_via_runtime(SketchType::DDSketch, &original_bytes).expect("reconstruct ok");
        let dd = match reconstructed {
            ReconstructedSketch::DdSketch(d) => d,
            ReconstructedSketch::Kll { .. } => panic!("got KLL, expected DDSketch"),
        };
        assert_eq!(dd.count, 100);
        let re_encoded = encode_ddsketch_envelope(&dd);
        assert_eq!(
            re_encoded, original_bytes,
            "round-trip via runtime adapter must be byte-identical"
        );
    }

    /// Construct two non-overlapping DDSketches, merge via the runtime
    /// adapter, and verify counts add up. Uses asap-precompute-rs's
    /// `Sketch::merge` + `Sketch::snapshot` round-trip.
    #[test]
    fn ddsketch_merge_via_runtime_combines_counts() {
        let mut a = DDSketchWrapper::new(0.01);
        for i in 1..=10 {
            a.update(i as f64);
        }
        let mut b = DDSketchWrapper::new(0.01);
        for i in 11..=20 {
            b.update(i as f64);
        }
        // Reach into the wrapper's inner via snapshot/decode.
        let a_inner =
            match reconstruct_via_runtime(SketchType::DDSketch, &a.snapshot().unwrap()).unwrap() {
                ReconstructedSketch::DdSketch(d) => d,
                _ => panic!(),
            };
        let b_inner =
            match reconstruct_via_runtime(SketchType::DDSketch, &b.snapshot().unwrap()).unwrap() {
                ReconstructedSketch::DdSketch(d) => d,
                _ => panic!(),
            };
        let merged = merge_ddsketches_via_runtime(&a_inner, &b_inner).expect("merge ok");
        assert_eq!(merged.count, 20);
    }
}
