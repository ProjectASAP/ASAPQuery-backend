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
use asap_sketchlib::HllVariant;
use asap_sketchlib::KllSketch;
use asap_sketchlib::MessagePackCodec;

use crate::storage_engines::sketch_db::index::{SketchEncoding, SketchSampleState};

/// Whether a sketch family supports delta-via-merge (DD/KLL) or
/// delta-via-`apply_delta` (HLL). The reducer reads bytes through
/// the appropriate `decode_*_full` path and folds the result into a
/// rolling state.
///
/// Each variant carries the sketch parameters (`alpha` / `k` /
/// `precision`) needed to *bootstrap an empty rolling state* — required
/// by the per-window-reset (PWR) delta model where a window's FIRST
/// frame is a delta-from-empty (no carry-in Full). DD/KLL deltas embed
/// their own params in the wire fragment, but HLL register deltas are
/// applied onto a pre-sized register array, so the precision must be
/// known up front to allocate it.
#[derive(Debug, Clone, Copy)]
pub enum DeltaSketchKind {
    DDSketch { alpha: f64 },
    Hll { precision: u32 },
    Kll { k: u32 },
}

impl DeltaSketchKind {
    /// Construct an EMPTY rolling state for this kind, used to seed a
    /// new window when its first frame is a delta-from-empty (PWR). A
    /// delta applied onto this empty base reconstructs exactly that
    /// window's state (delta-from-empty ⊕ empty = window state).
    fn bootstrap_empty(&self) -> RollingState {
        match self {
            DeltaSketchKind::DDSketch { alpha } => RollingState::Dd(DdSketch::new(*alpha)),
            DeltaSketchKind::Kll { k } => RollingState::Kll(KllSketch::new(*k as u16)),
            DeltaSketchKind::Hll { precision } => {
                RollingState::Hll(HllSketch::new(HllVariant::Regular, *precision))
            }
        }
    }
}

/// Try to decode a "full" sketch from the bytes (used by both
/// per-window and cumulative modes when the encoding is `*Full`).
fn decode_full(
    kind: &DeltaSketchKind,
    bytes: &[u8],
    encoding: SketchEncoding,
) -> Result<RollingState, String> {
    match (kind, encoding) {
        (DeltaSketchKind::DDSketch { .. }, SketchEncoding::ProtoFull) => {
            let sk = dd_from_proto(bytes)?;
            Ok(RollingState::Dd(sk))
        }
        (DeltaSketchKind::DDSketch { .. }, SketchEncoding::MsgpackFull) => {
            let sk = DdSketch::from_msgpack(bytes)
                .map_err(|e| format!("deserialize DDSketch msgpack: {e}"))?;
            Ok(RollingState::Dd(sk))
        }
        (DeltaSketchKind::Hll { .. }, SketchEncoding::ProtoFull) => {
            let sk = hll_from_proto(bytes)?;
            Ok(RollingState::Hll(sk))
        }
        (DeltaSketchKind::Hll { .. }, SketchEncoding::MsgpackFull) => {
            let sk = HllSketch::from_msgpack(bytes)
                .map_err(|e| format!("deserialize HllSketch msgpack: {e}"))?;
            Ok(RollingState::Hll(sk))
        }
        (DeltaSketchKind::Kll { .. }, SketchEncoding::ProtoFull) => {
            let sk = kll_from_proto(bytes)?;
            Ok(RollingState::Kll(sk))
        }
        (DeltaSketchKind::Kll { .. }, SketchEncoding::MsgpackFull) => {
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
                // The params here are unused: `decode_full` reads
                // alpha/k from the wire fragment, not from the kind tag.
                let other = match decode_full(&DeltaSketchKind::DDSketch { alpha: 0.0 }, bytes, full_enc)
                {
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
                let other = match decode_full(&DeltaSketchKind::Kll { k: 0 }, bytes, full_enc) {
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
/// produce ONE per-window scalar `(window_end_ms, scalar)`.
///
/// ## Per-window-reset (PWR) delta model
///
/// The edge emits frames grouped by window (all frames of one window
/// share the same `window_end` key; the key changes across windows).
/// The edge RESETS its snapshot base at each window boundary, so each
/// window's state is built *from empty*:
///
/// * Within a window, frames accumulate to the window total. The first
///   frame may be a `Full` (window 1, or a periodic re-snapshot) or a
///   `Delta`-from-empty (windows 2+ under PWR); subsequent frames are
///   `Delta` INCREMENTS applied onto the window's running base.
/// * Across windows, the base MUST reset — a new `window_end` discards
///   the previous window's rolling state and starts from empty. Never
///   carry one window's state into the next (that would inflate via
///   cross-window accumulation).
///
/// Concretely this fixes two bugs in the old "single rolling Option that
/// only ever resets on a Full" walk:
///   1. A query range whose Full lives only in window 1 (or out of
///      range) left windows 2+ as deltas with `rolling=None`, all
///      skipped → empty result.
///   2. A window 2+ delta applied onto window 1's leftover rolling state
///      → cross-window inflation.
///
/// For a `Delta` that is the window's FIRST frame (the PWR delta-from-
/// empty case), we bootstrap an EMPTY rolling state of `kind` and apply
/// the delta onto it (delta-from-empty ⊕ empty = that window's state).
///
/// The delta-OFF path (exactly one `Full` per window) still produces one
/// correct value per window: the window opens with a Full, has no
/// further frames, and emits that Full's scalar.
///
/// `eval` reads a scalar from the rolling state (`quantile(q)` /
/// `cardinality()`). `skipped` counts frames that could not contribute
/// (a delta we genuinely couldn't bootstrap from — should be rare).
///
/// Returns `Ok(per_window_samples, skipped)`.
pub fn per_window_evaluate<E>(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
    eval: E,
) -> Result<(Vec<(i64, f64)>, usize), String>
where
    E: Fn(&RollingState) -> f64,
{
    let mut out: Vec<(i64, f64)> = Vec::new();
    let mut skipped = 0usize;

    // Rolling state for the CURRENT window only. Reset to None whenever
    // `window_end` changes (a new window establishes its own base from
    // empty). `cur_end` tracks which window `rolling` belongs to.
    let mut rolling: Option<RollingState> = None;
    let mut cur_end: Option<i64> = None;

    for (window_end, state) in samples {
        // Window boundary: flush the previous window's final accumulated
        // value, then reset the base so this window starts from empty.
        if cur_end != Some(*window_end) {
            if let (Some(prev_end), Some(rs)) = (cur_end, rolling.as_ref()) {
                out.push((prev_end, eval(rs)));
            }
            rolling = None;
            cur_end = Some(*window_end);
        }

        match state.encoding {
            SketchEncoding::ProtoFull | SketchEncoding::MsgpackFull => {
                // A Full (re)sets this window's base.
                rolling = Some(decode_full(&kind, &state.bytes, state.encoding)?);
            }
            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
                // Apply onto this window's running base. If this is the
                // window's first frame (PWR delta-from-empty), bootstrap
                // an empty base and apply onto it.
                if rolling.is_none() {
                    rolling = Some(kind.bootstrap_empty());
                }
                match rolling.as_mut() {
                    Some(rs) => rs.apply_delta_bytes(&state.bytes, state.encoding)?,
                    None => skipped += 1,
                }
            }
        }
    }

    // Flush the final window.
    if let (Some(prev_end), Some(rs)) = (cur_end, rolling.as_ref()) {
        out.push((prev_end, eval(rs)));
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
                // PWR: the range may have NO carry-in Full (it starts
                // mid-stream), so the first frame is a delta-from-empty.
                // Bootstrap an empty base and apply onto it. Under PWR
                // each window's frames are increments-since-its-own-base;
                // folding them all (Full-fragment merge for DD/KLL,
                // register-max for HLL) yields the union over the range,
                // which is the cumulative (`*_over_time`) answer.
                if rolling.is_none() {
                    rolling = Some(kind.bootstrap_empty());
                }
                match rolling.as_mut() {
                    Some(rs) => rs.apply_delta_bytes(&state.bytes, state.encoding)?,
                    None => skipped += 1,
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
        let mut put_uvarint = |buf: &mut Vec<u8>, mut v: u64| loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                buf.push(b | 0x80);
            } else {
                buf.push(b);
                break;
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

    // -----------------------------------------------------------------
    // Per-window-reset (PWR) delta-apply regression tests.
    //
    // The edge resets its snapshot base at every window boundary, so a
    // window's first frame is either a Full (window 1 / re-snapshot) or
    // a Delta-from-empty (windows 2+). The query-side walk must:
    //   * reset the rolling base when `window_end` changes,
    //   * bootstrap an empty base for a window's leading Delta,
    //   * emit ONE value per window (the window's final accumulated
    //     state), never per-frame and never cross-window-accumulated.
    // -----------------------------------------------------------------

    fn full(bytes: Vec<u8>) -> SketchSampleState {
        SketchSampleState {
            bytes,
            encoding: SketchEncoding::ProtoFull,
        }
    }
    fn delta(bytes: Vec<u8>) -> SketchSampleState {
        SketchSampleState {
            bytes,
            encoding: SketchEncoding::ProtoDelta,
        }
    }

    fn dd_over(alpha: f64, vals: &[f64]) -> DdSketch {
        let mut sk = DdSketch::new(alpha);
        for &v in vals {
            sk.update(v);
        }
        sk
    }

    /// PWR across 3 windows: window 1 is `[Full]`, windows 2 & 3 are
    /// `[Delta-from-empty]` (NO Full carry-in). Each window must
    /// reconstruct its OWN distribution's median — not empty (the old
    /// "skip delta with no base" bug) and not cross-window-inflated.
    #[test]
    fn pwr_ddsketch_three_windows_delta_from_empty() {
        let alpha = 0.01;
        let w1 = dd_over(alpha, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let w2 = dd_over(alpha, &[10.0, 20.0, 30.0, 40.0, 50.0]);
        let w3 = dd_over(alpha, &[100.0, 200.0, 300.0, 400.0, 500.0]);

        // window 1 ships a Full; windows 2+ ship a delta-from-empty.
        let s1 = full(encode_dd(&w1));
        let s2 = delta(encode_dd(&w2));
        let s3 = delta(encode_dd(&w3));
        let samples = vec![(1000_i64, &s1), (2000, &s2), (3000, &s3)];

        let kind = DeltaSketchKind::DDSketch { alpha };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.quantile(0.5)).expect("pwr eval");
        assert_eq!(skipped, 0, "PWR must not skip delta-from-empty frames");
        assert_eq!(out.len(), 3, "one value per window");

        // Each window's median ≈ that window's own distribution median,
        // independent of the others (no carry-in inflation).
        let truth = [
            w1.quantile(0.5).unwrap(),
            w2.quantile(0.5).unwrap(),
            w3.quantile(0.5).unwrap(),
        ];
        for (i, (w_end, est)) in out.iter().enumerate() {
            assert_eq!(*w_end, (i as i64 + 1) * 1000);
            let rel = (est - truth[i]).abs() / truth[i].max(1e-9);
            assert!(
                rel < 0.05,
                "window {i}: est={est} truth={} rel={rel}",
                truth[i]
            );
        }
        // Cross-window-inflation guard: window 2's median must NOT have
        // absorbed window 1 (would pull it well below 30).
        assert!(
            out[1].1 > 20.0,
            "window 2 median {} suggests cross-window accumulation",
            out[1].1
        );
    }

    /// Sub-window producer: a SINGLE window carries multiple frames
    /// `[Full, Delta, Delta]`, where each later delta is an increment
    /// since the previous emit in that window. The walk must COLLAPSE
    /// them to ONE value = the window's running total, not emit three.
    #[test]
    fn pwr_ddsketch_subwindow_frames_collapse_to_window_total() {
        let alpha = 0.01;
        // Three sub-window increments that together cover 1..=15.
        let a = dd_over(alpha, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let b = dd_over(alpha, &[6.0, 7.0, 8.0, 9.0, 10.0]);
        let c = dd_over(alpha, &[11.0, 12.0, 13.0, 14.0, 15.0]);
        let s_a = full(encode_dd(&a));
        let s_b = delta(encode_dd(&b));
        let s_c = delta(encode_dd(&c));
        // All three share the same window_end (one window, sub-window frames).
        let samples = vec![(5000_i64, &s_a), (5000, &s_b), (5000, &s_c)];

        let kind = DeltaSketchKind::DDSketch { alpha };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.quantile(0.5)).expect("subwindow eval");
        assert_eq!(skipped, 0);
        assert_eq!(out.len(), 1, "sub-window frames collapse to ONE value");
        assert_eq!(out[0].0, 5000);

        let truth = dd_over(alpha, &(1..=15).map(|v| v as f64).collect::<Vec<_>>())
            .quantile(0.5)
            .unwrap();
        let rel = (out[0].1 - truth).abs() / truth.max(1e-9);
        assert!(rel < 0.05, "window total est={} truth={truth}", out[0].1);
    }

    /// Same sub-window collapse, but the window's FIRST frame is a
    /// Delta-from-empty (PWR window 2+ with sub-window frames):
    /// `[Delta-from-empty, Delta, Delta]`.
    #[test]
    fn pwr_ddsketch_subwindow_first_frame_delta_from_empty() {
        let alpha = 0.01;
        let a = dd_over(alpha, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let b = dd_over(alpha, &[6.0, 7.0, 8.0, 9.0, 10.0]);
        let c = dd_over(alpha, &[11.0, 12.0, 13.0, 14.0, 15.0]);
        let s_a = delta(encode_dd(&a)); // first frame is delta-from-empty
        let s_b = delta(encode_dd(&b));
        let s_c = delta(encode_dd(&c));
        let samples = vec![(9000_i64, &s_a), (9000, &s_b), (9000, &s_c)];

        let kind = DeltaSketchKind::DDSketch { alpha };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.quantile(0.5)).expect("eval");
        assert_eq!(skipped, 0);
        assert_eq!(out.len(), 1);
        let truth = dd_over(alpha, &(1..=15).map(|v| v as f64).collect::<Vec<_>>())
            .quantile(0.5)
            .unwrap();
        let rel = (out[0].1 - truth).abs() / truth.max(1e-9);
        assert!(rel < 0.05, "est={} truth={truth}", out[0].1);
    }

    /// PWR for HLL across 3 windows, each a Delta-from-empty (sparse
    /// register delta). Bootstrapping an EMPTY HLL of the right precision
    /// is required (register deltas index into a pre-sized array). Each
    /// window's cardinality must reflect its OWN item set.
    #[test]
    fn pwr_hll_three_windows_delta_from_empty() {
        let precision = 12u32;
        // Build per-window HLLs, then encode each as a register-delta
        // against an EMPTY sketch (= that window's full register state,
        // the PWR delta-from-empty wire form).
        let empty = HllSketch::new(HllVariant::Regular, precision);
        let mut frames = Vec::new();
        let truths = [200usize, 800, 1500];
        for (w, &n) in truths.iter().enumerate() {
            let mut sk = HllSketch::new(HllVariant::Regular, precision);
            let base = (w as u64) * 100_000; // disjoint item sets per window
            for i in 0..n as u64 {
                sk.update(format!("u-{}", base + i).as_bytes());
            }
            let bytes = sk.compute_delta(&empty, 0);
            frames.push((((w as u64) + 1) * 1000, delta(bytes)));
        }
        let samples: Vec<(i64, &SketchSampleState)> = frames
            .iter()
            .map(|(t, s)| (*t as i64, s))
            .collect();

        let kind = DeltaSketchKind::Hll { precision };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.cardinality()).expect("hll pwr eval");
        assert_eq!(skipped, 0, "HLL delta-from-empty must bootstrap, not skip");
        assert_eq!(out.len(), 3);
        for (i, (_w_end, est)) in out.iter().enumerate() {
            let n = truths[i] as f64;
            let rel = (est - n).abs() / n;
            assert!(
                rel < 0.15,
                "window {i}: HLL est={est} truth={n} rel={rel} (each window independent)"
            );
        }
    }
}
