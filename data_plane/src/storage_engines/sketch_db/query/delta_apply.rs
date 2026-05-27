//! Per-window delta stitching for the ASAP-tier sketch reducer.
//!
//! The reducer walks a sid's per-window samples in time order. When a
//! window's payload is a `Full` encoding (PROTO / MSGPACK), it
//! initializes a rolling "current state" sketch. Subsequent `Delta`
//! encodings merge into that rolling state — either via
//! `asap_sketchlib::HllSketch::apply_delta` for HLL register deltas,
//! or via `Sketch::merge(decoded_delta)` for DD / KLL where the wire
//! format ships a sparse-but-mergeable sketch fragment.
//!
//! Two reducer modes, picked by the PromQL function name in
//! [`crate::storage_engines::sketch_db::query::sketch_reducer`]:
//!
//! * **per-window** (`quantile`, `histogram_quantile`,
//!   `cardinality_estimate`): emit one scalar per window. A `Full`
//!   resets the rolling state; a `Delta` merges then emits from the
//!   merged state. Window-end timestamps come from the index.
//! * **cumulative** (`quantile_over_time`, `count_distinct_over_time`):
//!   walk the full `[t0, t1]` range, accumulating Full + all subsequent
//!   Deltas into a single rolling state. Emit one scalar at the last
//!   window's end_ms (the rolled-up answer covers the whole window).
//!
//! ## Corner case: leading delta
//!
//! If the first sample in a query window is a `Delta`, the base
//! snapshot from the previous (out-of-range) window isn't available
//! to the reducer. The leading delta is dropped (logged via the
//! `DeserializeFailure { reason: "leading delta without base" }` path
//! when no Full follows in the same window) and we wait for the next
//! `Full`. For cumulative mode this means the cumulative answer
//! starts at the first Full in the range, not at `t0`.

use asap_sketchlib::DdSketch;
use asap_sketchlib::HllSketch;
use asap_sketchlib::KllSketch;
use asap_sketchlib::MessagePackCodec;

use crate::storage_engines::sketch_db::index::{SketchEncoding, SketchSampleState};

/// Whether a sketch family supports delta-via-merge (DD/KLL) or
/// delta-via-`apply_delta` (HLL). The reducer reads bytes through
/// the appropriate `decode_*_full` path and folds the result into a
/// rolling state.
#[derive(Debug, Clone, Copy)]
pub enum DeltaSketchKind {
    DDSketch,
    Hll,
    Kll,
}

/// Try to decode a "full" sketch from the bytes (used by both
/// per-window and cumulative modes when the encoding is `*Full`).
fn decode_full(
    kind: &DeltaSketchKind,
    bytes: &[u8],
    encoding: SketchEncoding,
) -> Result<RollingState, String> {
    match (kind, encoding) {
        (DeltaSketchKind::DDSketch, SketchEncoding::ProtoFull) => {
            let sk = dd_from_proto(bytes)?;
            Ok(RollingState::Dd(sk))
        }
        (DeltaSketchKind::DDSketch, SketchEncoding::MsgpackFull) => {
            let sk = DdSketch::from_msgpack(bytes)
                .map_err(|e| format!("deserialize DDSketch msgpack: {e}"))?;
            Ok(RollingState::Dd(sk))
        }
        (DeltaSketchKind::Hll, SketchEncoding::ProtoFull) => {
            let sk = hll_from_proto(bytes)?;
            Ok(RollingState::Hll(sk))
        }
        (DeltaSketchKind::Hll, SketchEncoding::MsgpackFull) => {
            let sk = HllSketch::from_msgpack(bytes)
                .map_err(|e| format!("deserialize HllSketch msgpack: {e}"))?;
            Ok(RollingState::Hll(sk))
        }
        (DeltaSketchKind::Kll, SketchEncoding::ProtoFull) => {
            let sk = kll_from_proto(bytes)?;
            Ok(RollingState::Kll(sk))
        }
        (DeltaSketchKind::Kll, SketchEncoding::MsgpackFull) => {
            let sk = KllSketch::from_msgpack(bytes)
                .map_err(|e| format!("deserialize KllSketch msgpack: {e}"))?;
            Ok(RollingState::Kll(sk))
        }
        (_, e) => Err(format!("decode_full called with non-Full encoding {e:?}")),
    }
}

/// The rolling state the delta-application loop maintains.
pub enum RollingState {
    Dd(DdSketch),
    Hll(HllSketch),
    Kll(KllSketch),
}

impl RollingState {
    /// Apply a delta-encoded payload from a window sample. For DD / KLL,
    /// the delta is interpreted as a "mergeable fragment" decoded
    /// through the same full-state decoder and merged into the
    /// rolling state. For HLL, the wire delta is a sparse register
    /// update applied via the sketch's `apply_delta`.
    ///
    /// On encoding mismatch (e.g. trying to apply an HllDelta to a
    /// DDSketch rolling state) returns Err.
    pub fn apply_delta_bytes(
        &mut self,
        bytes: &[u8],
        encoding: SketchEncoding,
    ) -> Result<(), String> {
        if !matches!(
            encoding,
            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta
        ) {
            return Err(format!(
                "apply_delta_bytes called with non-Delta encoding {encoding:?}"
            ));
        }
        match self {
            RollingState::Dd(sk) => {
                // The wire delta for DD/KLL today is a full-sketch
                // fragment (sparse buckets); decode it via the same
                // full-state path and merge into `sk`. Treat the bytes
                // as a Full payload of the matching encoding family.
                let full_enc = match encoding {
                    SketchEncoding::ProtoDelta => SketchEncoding::ProtoFull,
                    SketchEncoding::MsgpackDelta => SketchEncoding::MsgpackFull,
                    _ => unreachable!(),
                };
                let other = match decode_full(&DeltaSketchKind::DDSketch, bytes, full_enc) {
                    Ok(RollingState::Dd(s)) => s,
                    Ok(_) => {
                        return Err("decode_full(DDSketch) returned non-DDSketch state".to_string())
                    }
                    Err(e) => return Err(e),
                };
                sk.merge(&other)
                    .map_err(|e| format!("merge DDSketch delta: {e}"))?;
                Ok(())
            }
            RollingState::Hll(sk) => {
                // HLL has a true sparse register delta in the proto
                // wire format. Use the same path the precompute
                // accumulator uses (`apply_proto_delta_bytes`-style).
                if encoding == SketchEncoding::ProtoDelta {
                    apply_hll_proto_delta(sk, bytes)
                } else {
                    // MsgpackDelta for HLL isn't a sparse encoding;
                    // it's a serialized HllSketch fragment, mergeable
                    // via `HllSketch::merge`.
                    let other = HllSketch::from_msgpack(bytes)
                        .map_err(|e| format!("deserialize HllSketch (delta-as-msgpack): {e}"))?;
                    sk.merge(&other)
                        .map_err(|e| format!("merge HLL delta: {e}"))?;
                    Ok(())
                }
            }
            RollingState::Kll(sk) => {
                let full_enc = match encoding {
                    SketchEncoding::ProtoDelta => SketchEncoding::ProtoFull,
                    SketchEncoding::MsgpackDelta => SketchEncoding::MsgpackFull,
                    _ => unreachable!(),
                };
                let other = match decode_full(&DeltaSketchKind::Kll, bytes, full_enc) {
                    Ok(RollingState::Kll(s)) => s,
                    Ok(_) => return Err("decode_full(Kll) returned non-Kll state".to_string()),
                    Err(e) => return Err(e),
                };
                sk.merge(&other)
                    .map_err(|e| format!("merge KLL delta: {e}"))?;
                Ok(())
            }
        }
    }

    pub fn quantile(&self, q: f64) -> f64 {
        match self {
            RollingState::Dd(sk) => sk.quantile(q).unwrap_or(0.0),
            RollingState::Kll(sk) => sk.quantile(q),
            RollingState::Hll(_) => 0.0,
        }
    }

    pub fn cardinality(&self) -> f64 {
        match self {
            RollingState::Hll(sk) => sk.estimate(),
            _ => 0.0,
        }
    }
}

/// Walk a sorted-by-window-end slice of samples in time order and
/// produce per-window scalars. On a `Full` payload, replace the
/// rolling state; on a `Delta`, apply it into the rolling state.
/// Each window emits one `(window_end_ms, scalar)`.
///
/// `eval` reads a scalar from the rolling state (`quantile(q)` /
/// `cardinality()`). Leading deltas (before any Full) are skipped
/// with a debug-grade error returned to the caller for reporting.
///
/// Returns `Ok(samples, skipped_leading_deltas)`.
pub fn per_window_evaluate<E>(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
    eval: E,
) -> Result<(Vec<(i64, f64)>, usize), String>
where
    E: Fn(&RollingState) -> f64,
{
    let mut out: Vec<(i64, f64)> = Vec::with_capacity(samples.len());
    let mut rolling: Option<RollingState> = None;
    let mut skipped = 0usize;

    for (window_end, state) in samples {
        match state.encoding {
            SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull => {
                rolling = Some(decode_full(&kind, &state.bytes, state.encoding)?);
                if let Some(rs) = &rolling {
                    out.push((*window_end, eval(rs)));
                }
            }
            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
                if let Some(rs) = rolling.as_mut() {
                    rs.apply_delta_bytes(&state.bytes, state.encoding)?;
                    out.push((*window_end, eval(rs)));
                } else {
                    skipped += 1;
                }
            }
        }
    }
    Ok((out, skipped))
}

/// Cumulative-mode rollup: fold every window in `[t0, t1]` into a
/// single rolling state and emit one scalar at the latest
/// `window_end_ms` seen (or the largest if all were Deltas that got
/// skipped). Used by `quantile_over_time` / `count_distinct_over_time`.
///
/// Returns `Ok((window_end, scalar), skipped_leading_deltas)`. Returns
/// `Ok(None, _)` if every sample was a leading delta (no Full ever
/// landed in the range).
pub fn cumulative_evaluate<E>(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
    eval: E,
) -> Result<(Option<(i64, f64)>, usize), String>
where
    E: Fn(&RollingState) -> f64,
{
    let mut rolling: Option<RollingState> = None;
    let mut latest_end = i64::MIN;
    let mut skipped = 0usize;

    for (window_end, state) in samples {
        if *window_end > latest_end {
            latest_end = *window_end;
        }
        match state.encoding {
            SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull => {
                let new_state = decode_full(&kind, &state.bytes, state.encoding)?;
                // Merge new_state into any existing rolling state — a
                // mid-range Full effectively "restarts" the window in
                // the cumulative roll-up if the agent flushed a new
                // snapshot. Merging keeps the answer monotonic in
                // sample inclusion.
                rolling = Some(match (rolling.take(), new_state) {
                    (None, n) => n,
                    (Some(RollingState::Dd(mut a)), RollingState::Dd(b)) => {
                        a.merge(&b).map_err(|e| format!("cum merge DD: {e}"))?;
                        RollingState::Dd(a)
                    }
                    (Some(RollingState::Hll(mut a)), RollingState::Hll(b)) => {
                        a.merge(&b).map_err(|e| format!("cum merge HLL: {e}"))?;
                        RollingState::Hll(a)
                    }
                    (Some(RollingState::Kll(mut a)), RollingState::Kll(b)) => {
                        a.merge(&b).map_err(|e| format!("cum merge KLL: {e}"))?;
                        RollingState::Kll(a)
                    }
                    (Some(_), _) => {
                        return Err("cumulative merge across sketch family mismatch".to_string())
                    }
                });
            }
            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
                if let Some(rs) = rolling.as_mut() {
                    rs.apply_delta_bytes(&state.bytes, state.encoding)?;
                } else {
                    skipped += 1;
                }
            }
        }
    }
    let out = rolling.map(|rs| (latest_end, eval(&rs)));
    Ok((out, skipped))
}

// ---------------------------------------------------------------------------
// Proto-envelope decoders — P2-4: ONE decoder per family.
//
// These delegate to the precompute-side accumulators'
// `from_sketchlib_proto_bytes`, which are the single source of truth for
// the modified-OTLP proto wire format (envelope unwrapping, alpha/k/
// precision validation, and — critically for HLL — SPARSE
// `registers_sparse` expansion). Folding the warm read path onto the
// same decoder the ingest path uses means the sparse-register fix (and
// any future format change) can never drift between the two copies again
// — the bug class P2-3 / P2-4 closed. We extract the accumulator's
// public `inner` sketch for the rolling-state merge.
// ---------------------------------------------------------------------------

fn dd_from_proto(buffer: &[u8]) -> Result<DdSketch, String> {
    use crate::precompute_engine::operators::dd_sketch_accumulator::DDSketchAccumulator;
    DDSketchAccumulator::from_sketchlib_proto_bytes(buffer)
        .map(|acc| acc.inner)
        .map_err(|e| e.to_string())
}

fn kll_from_proto(buffer: &[u8]) -> Result<KllSketch, String> {
    use crate::precompute_engine::operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
    DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(buffer)
        .map(|acc| acc.inner)
        .map_err(|e| e.to_string())
}

fn hll_from_proto(buffer: &[u8]) -> Result<HllSketch, String> {
    use crate::precompute_engine::operators::hll_sketch_accumulator::HllSketchAccumulator;
    HllSketchAccumulator::from_sketchlib_proto_bytes(buffer)
        .map(|acc| acc.inner)
        .map_err(|e| e.to_string())
}

/// Apply a proto-encoded `HllDelta` frame onto the HLL register vector — the
/// delta is a varint-packed (index_delta, value) blob; decode + apply
/// (register-wise max) via the shared sketch library so the unpacking stays a
/// single source of truth.
fn apply_hll_proto_delta(sk: &mut HllSketch, buffer: &[u8]) -> Result<(), String> {
    sk.apply_delta_bytes(buffer)
        .map_err(|e| format!("apply HLLDelta: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! P2-3 / P2-4 regression tests for the consolidated single-decoder
    //! path. These exercise the family proto decoders that now delegate
    //! to the precompute accumulators (the single source of truth), so a
    //! divergence between the warm read path and the ingest path —
    //! notably the SPARSE-register HLL handling the deleted dead decoder
    //! got wrong — fails the build.
    use super::*;
    use asap_sketchlib::HllVariant;

    fn encode_dd(sk: &DdSketch) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
        use prost::Message;
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

    fn encode_kll(k: u16, items: &[f64]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;
        let state = KllState {
            k: k as u32,
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

    fn encode_hll_dense(sk: &HllSketch) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use prost::Message;
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

    /// Build a SPARSE HLL proto frame: dense `registers` left empty,
    /// `registers_sparse.packed` = varint (index_delta, value) pairs.
    /// This is exactly the wire form a low-cardinality producer emits
    /// (sketchlib-go below its dense/sparse crossover) — the frame the
    /// DELETED `HllSketch_from_sketchlib_proto_bytes` hard-rejected with
    /// "registers has 0 bytes".
    fn encode_hll_sparse(precision: u32, nonzero: &[(u64, u8)]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllSparseRegisters, HllVariant as ProtoVariant, HyperLogLogState,
            SketchEnvelope,
        };
        use prost::Message;
        // Varint-pack (index_delta, value), ascending index order.
        let mut packed: Vec<u8> = Vec::new();
        let mut prev: u64 = 0;
        let mut put_uvarint = |buf: &mut Vec<u8>, mut v: u64| {
            loop {
                let b = (v & 0x7f) as u8;
                v >>= 7;
                if v != 0 {
                    buf.push(b | 0x80);
                } else {
                    buf.push(b);
                    break;
                }
            }
        };
        let mut sorted = nonzero.to_vec();
        sorted.sort_by_key(|(i, _)| *i);
        for (idx, val) in &sorted {
            put_uvarint(&mut packed, idx - prev);
            put_uvarint(&mut packed, *val as u64);
            prev = *idx;
        }
        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision,
            registers: Vec::new(), // dense field empty → sparse path
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
            // `num_registers` is informational — the decoder expands
            // against `expected_len` from precision, not this field.
            registers_sparse: Some(HllSparseRegisters {
                num_registers: 1u32 << precision,
                packed,
            }),
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn hll_from_proto_accepts_sparse_frame() {
        // The consolidated decoder must accept the sparse wire form (the
        // deleted dead decoder rejected it). Build a sparse frame setting
        // a handful of registers, decode it, and confirm those register
        // slots came back set in the dense array.
        let precision = 12u32;
        let nonzero = [(3u64, 5u8), (100, 2), (4000, 7)];
        let bytes = encode_hll_sparse(precision, &nonzero);
        let sk = hll_from_proto(&bytes).expect("sparse HLL frame must decode (P2-3 regression)");
        assert_eq!(sk.registers.len(), 1usize << precision);
        for (idx, val) in nonzero {
            assert_eq!(
                sk.registers[idx as usize], val,
                "sparse register {idx} expanded to wrong value"
            );
        }
    }

    #[test]
    fn hll_from_proto_matches_accumulator_decoder() {
        // P2-4: the warm read path and the ingest accumulator must decode
        // the SAME bytes to the SAME sketch (one source of truth).
        use crate::precompute_engine::operators::hll_sketch_accumulator::HllSketchAccumulator;
        let mut sk = HllSketch::new(HllVariant::Regular, 12);
        for i in 0..500u64 {
            sk.update(format!("item-{i}").as_bytes());
        }
        let bytes = encode_hll_dense(&sk);
        let via_delta = hll_from_proto(&bytes).expect("delta_apply hll decode");
        let via_acc = HllSketchAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("accumulator hll decode")
            .inner;
        assert_eq!(
            via_delta.registers, via_acc.registers,
            "delta_apply and accumulator must produce identical HLL registers"
        );
        assert!((via_delta.estimate() - via_acc.estimate()).abs() < 1e-9);
    }

    #[test]
    fn dd_from_proto_matches_accumulator_decoder() {
        use crate::precompute_engine::operators::dd_sketch_accumulator::DDSketchAccumulator;
        let mut sk = DdSketch::new(0.01);
        for v in [1.0, 2.0, 5.0, 5.0, 9.0, 42.0] {
            sk.update(v);
        }
        let bytes = encode_dd(&sk);
        let via_delta = dd_from_proto(&bytes).expect("delta_apply dd decode");
        let via_acc = DDSketchAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("accumulator dd decode")
            .inner;
        // Same quantile answers from the same bytes through both paths.
        assert_eq!(via_delta.quantile(0.5), via_acc.quantile(0.5));
        assert_eq!(via_delta.quantile(0.99), via_acc.quantile(0.99));
    }

    #[test]
    fn kll_from_proto_matches_accumulator_decoder() {
        use crate::precompute_engine::operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
        let items: Vec<f64> = (0..200).map(|i| i as f64).collect();
        let bytes = encode_kll(256, &items);
        let via_delta = kll_from_proto(&bytes).expect("delta_apply kll decode");
        let via_acc = DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(&bytes)
            .expect("accumulator kll decode")
            .inner;
        assert_eq!(via_delta.quantile(0.5), via_acc.quantile(0.5));
    }
}
