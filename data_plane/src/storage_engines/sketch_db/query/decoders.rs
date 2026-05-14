//! Per-sketch-kind decoder helpers — out-of-line wrappers around
//! `asap_sketchlib` deserialize / proto-decode paths.
//!
//! Lifted from the inline closures in [`crate::storage_engines::sketch_db::query::sketch_reducer`]
//! once the reducer started decoding CMS / CountSketch / CMS-with-heap
//! payloads in addition to DDSketch / KLL / HLL. The CMS / CountSketch
//! / CMS-with-heap decoders mirror
//! `precompute_operators::{count_min_sketch, count_sketch,
//! count_min_sketch_with_heap}_accumulator.rs` bit-for-bit so the
//! ASAP-tier reducer's output matches what the precompute (ingest-side)
//! accumulator would have produced from the same bytes.
//!
//! Each entry point returns the typed sketchlib struct on success or a
//! plain `String` error; the reducer wraps the error into a
//! `ASAPTierError::DeserializeFailure` so the engine router falls over
//! to archive cleanly.

use asap_sketchlib::sketches::countminsketch::CountMinSketch;
use asap_sketchlib::sketches::countminsketch_topk::CountMinSketchWithHeap;
use asap_sketchlib::sketches::countsketch::CountSketch;

/// Decode a `CountMinSketch` from the modified-OTLP wire bytes.
/// MSGPACK path round-trips `CountMinSketch::deserialize_msgpack`;
/// PROTO path decodes a `SketchEnvelope{count_min: CountMinState}`
/// (or bare `CountMinState`) and re-projects to a flat matrix. Mirrors
/// `precompute_operators::count_min_sketch_accumulator::from_sketchlib_proto_bytes`.
pub fn decode_cms_from_proto(buffer: &[u8]) -> Result<CountMinSketch, String> {
    use asap_sketchlib::proto::sketchlib::{
        sketch_envelope, CountMinState, CounterType, SketchEnvelope,
    };
    use prost::Message;

    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::CountMin(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-CountMin sketch".to_string()),
            None => {
                CountMinState::decode(buffer).map_err(|e| format!("decode CountMinState: {e}"))?
            }
        },
        Err(_) => {
            CountMinState::decode(buffer).map_err(|e| format!("decode CountMinState: {e}"))?
        }
    };
    let rows = state.rows as usize;
    let cols = state.cols as usize;
    if rows == 0 || cols == 0 {
        return Err(format!(
            "CountMinState has zero dims (rows={rows}, cols={cols})"
        ));
    }
    let expected_len = rows * cols;
    let counter_type = CounterType::try_from(state.counter_type)
        .map_err(|_| format!("CountMinState unknown counter_type {}", state.counter_type))?;
    let flat: Vec<f64> = match counter_type {
        CounterType::Int32 | CounterType::Int64 => {
            if state.counts_int.len() != expected_len {
                return Err(format!(
                    "CountMinState counts_int has {} entries, expected {}",
                    state.counts_int.len(),
                    expected_len
                ));
            }
            state.counts_int.iter().map(|&v| v as f64).collect()
        }
        CounterType::Float64 => {
            if state.counts_float.len() != expected_len {
                return Err(format!(
                    "CountMinState counts_float has {} entries, expected {}",
                    state.counts_float.len(),
                    expected_len
                ));
            }
            state.counts_float.clone()
        }
        other => {
            return Err(format!(
                "CountMinState counter_type {other:?} not yet supported in reducer"
            ));
        }
    };
    let mut matrix = Vec::with_capacity(rows);
    for r in 0..rows {
        let start = r * cols;
        matrix.push(flat[start..start + cols].to_vec());
    }
    Ok(CountMinSketch::from_legacy_matrix(matrix, rows, cols))
}

/// Decode a `CountMinSketch` from msgpack bytes (sketch-core wire
/// format). Mirrors
/// `CountMinSketchAccumulator::from_msgpack_bytes`.
pub fn decode_cms_from_msgpack(buffer: &[u8]) -> Result<CountMinSketch, String> {
    CountMinSketch::deserialize_msgpack(buffer)
        .map_err(|e| format!("deserialize CountMinSketch msgpack: {e}"))
}

/// Decode a `CountSketch` from the modified-OTLP proto wire bytes.
/// Mirrors
/// `precompute_operators::count_sketch_accumulator::from_sketchlib_proto_bytes`.
pub fn decode_cs_from_proto(buffer: &[u8]) -> Result<CountSketch, String> {
    use asap_sketchlib::proto::sketchlib::{
        sketch_envelope, CountSketchState, CounterType, SketchEnvelope,
    };
    use prost::Message;

    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::CountSketch(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-CountSketch sketch".to_string()),
            None => CountSketchState::decode(buffer)
                .map_err(|e| format!("decode CountSketchState: {e}"))?,
        },
        Err(_) => {
            CountSketchState::decode(buffer).map_err(|e| format!("decode CountSketchState: {e}"))?
        }
    };
    let rows = state.rows as usize;
    let cols = state.cols as usize;
    if rows == 0 || cols == 0 {
        return Err(format!(
            "CountSketchState has zero dims (rows={rows}, cols={cols})"
        ));
    }
    let expected_len = rows * cols;
    let counter_type = CounterType::try_from(state.counter_type).map_err(|_| {
        format!(
            "CountSketchState unknown counter_type {}",
            state.counter_type
        )
    })?;
    let flat: Vec<f64> = match counter_type {
        CounterType::Int32 | CounterType::Int64 => {
            if state.counts_int.len() != expected_len {
                return Err(format!(
                    "CountSketchState counts_int has {} entries, expected {}",
                    state.counts_int.len(),
                    expected_len
                ));
            }
            state.counts_int.iter().map(|&v| v as f64).collect()
        }
        CounterType::Float64 => {
            if state.counts_float.len() != expected_len {
                return Err(format!(
                    "CountSketchState counts_float has {} entries, expected {}",
                    state.counts_float.len(),
                    expected_len
                ));
            }
            state.counts_float.clone()
        }
        other => {
            return Err(format!(
                "CountSketchState counter_type {other:?} not yet supported in reducer"
            ));
        }
    };
    let mut matrix = Vec::with_capacity(rows);
    for r in 0..rows {
        let start = r * cols;
        matrix.push(flat[start..start + cols].to_vec());
    }
    Ok(CountSketch::from_legacy_matrix(matrix, rows, cols))
}

/// Decode a `CountSketch` from msgpack bytes (sketch-core wire format).
pub fn decode_cs_from_msgpack(buffer: &[u8]) -> Result<CountSketch, String> {
    CountSketch::deserialize_msgpack(buffer)
        .map_err(|e| format!("deserialize CountSketch msgpack: {e}"))
}

/// Decode a `CountMinSketchWithHeap` from msgpack bytes — the OTLP
/// `CountMinSketch` wire bytes when the gateway/precompute layer
/// marked the sid as CmsWithHeap (heap embedded in the
/// `CountMinSketchWithHeapSerialized` outer wrapper). Delegates to
/// `asap_sketchlib::sketches::CountMinSketchWithHeap::deserialize_msgpack`.
pub fn decode_cms_with_heap_from_msgpack(buffer: &[u8]) -> Result<CountMinSketchWithHeap, String> {
    CountMinSketchWithHeap::deserialize_msgpack(buffer)
        .map_err(|e| format!("deserialize CountMinSketchWithHeap msgpack: {e}"))
}
