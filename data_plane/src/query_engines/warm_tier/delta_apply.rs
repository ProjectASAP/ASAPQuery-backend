//! Per-window delta stitching for the warm-tier sketch reducer.
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
//! [`crate::query_engines::warm_tier::sketch_reducer`]:
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

use asap_sketchlib::sketches::ddsketch::DdSketch;
use asap_sketchlib::sketches::hll::HllSketch;
use asap_sketchlib::sketches::kll::KllSketch;

use crate::stores::sketch_db::sketch_index::{SketchEncoding, SketchSampleState};

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
            let sk = DdSketch::deserialize_msgpack(bytes)
                .map_err(|e| format!("deserialize DDSketch msgpack: {e}"))?;
            Ok(RollingState::Dd(sk))
        }
        (DeltaSketchKind::Hll, SketchEncoding::ProtoFull) => {
            let sk = hll_from_proto(bytes)?;
            Ok(RollingState::Hll(sk))
        }
        (DeltaSketchKind::Hll, SketchEncoding::MsgpackFull) => {
            let sk = HllSketch::deserialize_msgpack(bytes)
                .map_err(|e| format!("deserialize HllSketch msgpack: {e}"))?;
            Ok(RollingState::Hll(sk))
        }
        (DeltaSketchKind::Kll, SketchEncoding::ProtoFull) => {
            let sk = kll_from_proto(bytes)?;
            Ok(RollingState::Kll(sk))
        }
        (DeltaSketchKind::Kll, SketchEncoding::MsgpackFull) => {
            let sk = KllSketch::deserialize_msgpack(bytes)
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
                    let other = HllSketch::deserialize_msgpack(bytes)
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
// Proto-envelope decoders — duplicated minimally from the inline forms
// in `sketch_reducer.rs` so this module can decode "delta as full
// fragment" without re-entering the reducer's private functions.
// ---------------------------------------------------------------------------

fn dd_from_proto(buffer: &[u8]) -> Result<DdSketch, String> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
    use prost::Message;
    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Ddsketch(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-DDSketch sketch".to_string()),
            None => {
                DdSketchState::decode(buffer).map_err(|e| format!("decode DDSketchState: {e}"))?
            }
        },
        Err(_) => {
            DdSketchState::decode(buffer).map_err(|e| format!("decode DDSketchState: {e}"))?
        }
    };
    if !(state.alpha > 0.0 && state.alpha < 1.0) {
        return Err(format!(
            "DDSketchState alpha {} out of range (expected 0 < alpha < 1)",
            state.alpha
        ));
    }
    Ok(DdSketch::from_raw(
        state.alpha,
        state.store_counts.clone(),
        state.store_offset,
        state.count,
        state.sum,
        state.min,
        state.max,
    ))
}

fn kll_from_proto(buffer: &[u8]) -> Result<KllSketch, String> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
    use prost::Message;
    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Kll(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-KLL sketch".to_string()),
            None => KllState::decode(buffer).map_err(|e| format!("decode KllState: {e}"))?,
        },
        Err(_) => KllState::decode(buffer).map_err(|e| format!("decode KllState: {e}"))?,
    };
    if state.k < 8 {
        return Err(format!("KllState.k must be >= 8 (got {})", state.k));
    }
    if state.k > u16::MAX as u32 {
        return Err(format!(
            "KllState.k does not fit in u16 (got {}, max {})",
            state.k,
            u16::MAX
        ));
    }
    let k = state.k as u16;
    let mut sk = KllSketch::new(k);
    for item in &state.items {
        sk.update(*item);
    }
    Ok(sk)
}

fn hll_from_proto(buffer: &[u8]) -> Result<HllSketch, String> {
    use asap_sketchlib::proto::sketchlib::{
        sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
    };
    use asap_sketchlib::sketches::hll::HllVariant;
    use prost::Message;
    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Hll(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-HLL sketch".to_string()),
            None => HyperLogLogState::decode(buffer)
                .map_err(|e| format!("decode HyperLogLogState: {e}"))?,
        },
        Err(_) => {
            HyperLogLogState::decode(buffer).map_err(|e| format!("decode HyperLogLogState: {e}"))?
        }
    };
    if state.precision == 0 || state.precision > 20 {
        return Err(format!(
            "HyperLogLogState precision {} out of range (expected 1..=20)",
            state.precision
        ));
    }
    let expected_len = 1usize << state.precision;
    if state.registers.len() != expected_len {
        return Err(format!(
            "HyperLogLogState registers has {} bytes, expected 2^precision = {}",
            state.registers.len(),
            expected_len
        ));
    }
    let proto_variant = ProtoVariant::try_from(state.variant)
        .map_err(|_| format!("HyperLogLogState has unknown variant tag {}", state.variant))?;
    let variant = match proto_variant {
        ProtoVariant::Unspecified => HllVariant::Unspecified,
        ProtoVariant::Regular => HllVariant::Regular,
        ProtoVariant::ErtlMle => HllVariant::Datafusion,
        ProtoVariant::Hip => HllVariant::Hip,
    };
    Ok(HllSketch::from_raw(
        variant,
        state.precision,
        state.registers.clone(),
        state.hip_kxq0,
        state.hip_kxq1,
        state.hip_est,
    ))
}

/// Apply a proto-encoded `HllDelta` frame onto the HLL register vector
/// — mirrors `HllSketchAccumulator::apply_proto_delta_bytes` (sparse
/// `(index, value)` updates, `register = max(register, value)`).
fn apply_hll_proto_delta(sk: &mut HllSketch, buffer: &[u8]) -> Result<(), String> {
    use asap_otel_proto::sketchlib::v1::HllDelta as PbDelta;
    use asap_sketchlib::sketches::hll::HllSketchDelta;
    use prost::Message;

    let pb = PbDelta::decode(buffer).map_err(|e| format!("decode HLLDelta: {e}"))?;
    let updates = pb
        .updates
        .into_iter()
        .map(|u| (u.index, u.value as u8))
        .collect();
    let delta = HllSketchDelta { updates };
    sk.apply_delta(&delta)
        .map_err(|e| format!("apply HLLDelta: {e}"))?;
    Ok(())
}
