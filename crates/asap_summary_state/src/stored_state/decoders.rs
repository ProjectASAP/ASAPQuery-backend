//! Edge wire formats: sketchlib protobuf envelopes, msgpack frames and delta
//! frames, decoded into the sketchlib states that Planner kernels wrap.
//!
//! These are formats, not algorithms: every update, merge and estimate is done
//! by the sketchlib types (through Planner kernels) after decoding.
use asap_sketchlib::proto::sketchlib::{sketch_envelope, CounterType, SketchEnvelope};
use asap_sketchlib::{
    CmsHeapItem, CountMinSketch, CountMinSketchDelta, CountMinSketchWithHeap, CountSketch,
    CountSketchDelta, CountSketchWithHeap, CsHeapItem, DdSketch, DdSketchDelta, HllSketch,
    HllVariant, KllSketch, MessagePackCodec,
};
use prost::Message;

/// Planner kernels carry no edge sampling probability, so a sampled frame
/// (`0 < sample_p < 1`) would silently read as unscaled counts. `0` (proto3
/// default) and `1` both mean unsampled.
fn unsampled(what: &str, sample_p: f64) -> Result<(), String> {
    if sample_p.is_finite() && sample_p > 0.0 && sample_p < 1.0 {
        return Err(format!(
            "{what} frame is edge-sampled (sample_p={sample_p}); sampled sketch \
             state is not supported by Planner kernels"
        ));
    }
    Ok(())
}

/// Envelope-wrapped state, or the bare state for producers that omit the
/// envelope. Returns the envelope's `sample_p` (0 when absent).
fn enveloped<T: Message + Default>(
    buffer: &[u8],
    what: &str,
    pick: impl FnOnce(sketch_envelope::SketchState) -> Option<T>,
) -> Result<(T, f64), String> {
    let bare = || {
        T::decode(buffer)
            .map(|state| (state, 0.0))
            .map_err(|e| format!("decode {what}: {e}"))
    };
    match SketchEnvelope::decode(buffer) {
        Ok(SketchEnvelope {
            sketch_state: Some(state),
            sample_p,
            ..
        }) => pick(state)
            .map(|state| (state, sample_p))
            .ok_or_else(|| format!("SketchEnvelope does not contain a {what}")),
        _ => bare(),
    }
}

/// Whether `buffer` is a `SketchEnvelope` carrying a sketch state, as opposed
/// to a bare delta message.
pub fn carries_sketch_state(buffer: &[u8]) -> bool {
    SketchEnvelope::decode(buffer).is_ok_and(|envelope| envelope.sketch_state.is_some())
}

// ---------------------------------------------------------------------------
// DDSketch
// ---------------------------------------------------------------------------

/// A `SketchEnvelope{DdSketchState}` frame. Bare states are rejected.
pub fn ddsketch_from_proto(buffer: &[u8]) -> Result<DdSketch, String> {
    let (state, sample_p) = asap_sketch_codec::ddsketch_state(buffer)?;
    unsampled("DDSketch", sample_p)?;
    if !(state.alpha > 0.0 && state.alpha < 1.0) {
        return Err(format!(
            "DDSketchState alpha {} out of range (expected 0 < alpha < 1)",
            state.alpha
        ));
    }
    Ok(DdSketch::from_proto(state))
}

pub fn ddsketch_from_msgpack(buffer: &[u8]) -> Result<DdSketch, String> {
    DdSketch::from_msgpack(buffer).map_err(|e| format!("deserialize DdSketch msgpack: {e}"))
}

/// Add a `DDSketchDelta` bucket-count frame onto `sketch`.
pub fn apply_ddsketch_proto_delta(sketch: &mut DdSketch, buffer: &[u8]) -> Result<(), String> {
    use asap_sketchlib::proto::sketchlib::DdSketchDelta as PbDelta;
    let pb = PbDelta::decode(buffer).map_err(|e| format!("decode DDSketchDelta: {e}"))?;
    let delta = DdSketchDelta {
        buckets: pb
            .buckets
            .into_iter()
            .map(|b| (b.index, b.d_count))
            .collect(),
        negative_buckets: pb
            .negative_buckets
            .into_iter()
            .map(|b| (b.index, b.d_count))
            .collect(),
        zero_count: pb.zero_count,
        ..Default::default()
    };
    sketch
        .apply_delta(&delta)
        .map_err(|error| format!("apply DDSketchDelta: {error}"))
}

// ---------------------------------------------------------------------------
// KLL
// ---------------------------------------------------------------------------

/// A `SketchEnvelope{KllState}` frame, reconstructed bit-exactly from its
/// level layout (or by replay when the producer sent no levels).
pub fn kll_from_proto(buffer: &[u8]) -> Result<KllSketch, String> {
    let state = asap_sketch_codec::kll_state(buffer)?;
    if state.k < 8 {
        return Err(format!("KllState.k must be >= 8 (got {})", state.k));
    }
    let k = u16::try_from(state.k).map_err(|_| {
        format!(
            "KllState.k does not fit in u16 (got {}, max {})",
            state.k,
            u16::MAX
        )
    })?;
    if state.levels.is_empty() {
        let mut sketch = KllSketch::new(k);
        for item in &state.items {
            sketch.update(*item);
        }
        return Ok(sketch);
    }
    if state.levels.len() as u32 != state.num_levels + 1 {
        return Err(format!(
            "KllState levels length = {}, expected num_levels+1 = {}",
            state.levels.len(),
            state.num_levels + 1
        ));
    }
    if state.levels[0] != 0 {
        return Err(format!(
            "KllState.levels[0] = {}, expected 0",
            state.levels[0]
        ));
    }
    if *state.levels.last().unwrap() as usize != state.items.len() {
        return Err(format!(
            "KllState.levels[{}] = {}, expected items.len() = {}",
            state.num_levels,
            state.levels.last().unwrap(),
            state.items.len()
        ));
    }
    if state
        .levels
        .windows(2)
        .any(|bounds| bounds[0] > bounds[1] || bounds[1] as usize > state.items.len())
    {
        return Err("KllState levels must be monotonic and within items".into());
    }
    // KllState is highest level first; the in-memory constructor expects L0
    // first. Copying the wire order changes retained-item weights.
    let mut items = Vec::with_capacity(state.items.len());
    let mut levels = vec![0];
    for bounds in state.levels.windows(2).rev() {
        items.extend_from_slice(&state.items[bounds[0] as usize..bounds[1] as usize]);
        levels.push(items.len());
    }
    KllSketch::from_portable_state(k, &items, &levels, state.num_levels as usize)
        .map_err(|e| e.to_string())
}

pub fn kll_from_msgpack(buffer: &[u8]) -> Result<KllSketch, String> {
    KllSketch::from_msgpack(buffer).map_err(|e| format!("deserialize KllSketch msgpack: {e}"))
}

// ---------------------------------------------------------------------------
// HLL
// ---------------------------------------------------------------------------

/// Decode one protobuf base-128 varint from the front of `buf`, returning
/// `(value, bytes_consumed)`.
fn read_uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    for (i, &b) in buf.iter().enumerate() {
        let shift = 7 * i as u32;
        if shift >= 64 {
            return None;
        }
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
    }
    None
}

/// Expand sketchlib-go's sparse register form (varint `(index_delta, value)`
/// pairs in ascending index order) into the dense register array.
fn expand_sparse_hll_registers(packed: &[u8], num_registers: usize) -> Result<Vec<u8>, String> {
    let mut regs = vec![0u8; num_registers];
    let (mut prev, mut pos) = (0u64, 0usize);
    while pos < packed.len() {
        let (delta, n1) = read_uvarint(&packed[pos..])
            .ok_or("HLLSparseRegisters.packed: truncated index_delta varint")?;
        pos += n1;
        let (value, n2) = read_uvarint(&packed[pos..])
            .ok_or("HLLSparseRegisters.packed: truncated value varint")?;
        pos += n2;
        let idx = prev + delta;
        let i = usize::try_from(idx)
            .map_err(|_| format!("HLLSparseRegisters: index {idx} overflows usize"))?;
        if i >= num_registers {
            return Err(format!(
                "HLLSparseRegisters: register index {i} >= num_registers {num_registers}"
            ));
        }
        regs[i] = u8::try_from(value)
            .map_err(|_| format!("HLLSparseRegisters: register value {value} > 255"))?;
        prev = idx;
    }
    Ok(regs)
}

/// A `SketchEnvelope{HyperLogLogState}` (or bare state) frame with dense,
/// sparse or absent (all-zero) registers.
pub fn hll_from_proto(buffer: &[u8]) -> Result<HllSketch, String> {
    use asap_sketchlib::proto::sketchlib::{HllVariant as ProtoVariant, HyperLogLogState};
    let (state, sample_p) =
        enveloped::<HyperLogLogState>(buffer, "HyperLogLogState", |s| match s {
            sketch_envelope::SketchState::Hll(state) => Some(state),
            _ => None,
        })?;
    unsampled("HLL", sample_p)?;
    if state.precision == 0 || state.precision > 20 {
        return Err(format!(
            "HyperLogLogState precision {} out of range (expected 1..=20)",
            state.precision
        ));
    }
    let expected_len = 1usize << state.precision;
    let registers = if state.registers.len() == expected_len {
        state.registers.clone()
    } else if !state.registers.is_empty() {
        return Err(format!(
            "HyperLogLogState registers has {} bytes, expected 2^precision = {}",
            state.registers.len(),
            expected_len
        ));
    } else if let Some(sparse) = state.registers_sparse.as_ref() {
        expand_sparse_hll_registers(&sparse.packed, expected_len)?
    } else {
        vec![0u8; expected_len]
    };
    let variant = match ProtoVariant::try_from(state.variant)
        .map_err(|_| format!("HyperLogLogState has unknown variant tag {}", state.variant))?
    {
        ProtoVariant::Unspecified => HllVariant::Unspecified,
        ProtoVariant::Regular => HllVariant::Regular,
        ProtoVariant::ErtlMle => HllVariant::Datafusion,
        ProtoVariant::Hip => HllVariant::Hip,
    };
    Ok(HllSketch::from_raw(
        variant,
        state.precision,
        registers,
        state.hip_kxq0,
        state.hip_kxq1,
        state.hip_est,
    ))
}

pub fn hll_from_msgpack(buffer: &[u8]) -> Result<HllSketch, String> {
    HllSketch::from_msgpack(buffer).map_err(|e| format!("deserialize HllSketch msgpack: {e}"))
}

/// Apply an `HLLDelta` register frame (register-wise max) onto `sketch`.
pub fn apply_hll_proto_delta(sketch: &mut HllSketch, buffer: &[u8]) -> Result<(), String> {
    sketch
        .apply_delta_bytes(buffer)
        .map_err(|e| format!("apply HLLDelta: {e}"))
}

// ---------------------------------------------------------------------------
// Count-Min and Count Sketch matrices
// ---------------------------------------------------------------------------

/// Upper bound on wire-declared matrix cells, so a malformed frame cannot
/// force a huge allocation before its counts are checked.
const MAX_SKETCH_CELLS: usize = 8 * 1024 * 1024;

/// Reject degenerate, oversized, or packed-hash-incompatible dimensions: the
/// wire hasher reads `ceil(log2(cols))` bits per row from one 64-bit word.
fn validate_sketch_dims(what: &str, rows: usize, cols: usize) -> Result<(), String> {
    if rows < 1 || cols < 1 {
        return Err(format!(
            "{what} has degenerate dims (rows={rows}, cols={cols}); rejecting"
        ));
    }
    let mask_bits = cols.ilog2() as usize + usize::from(!cols.is_power_of_two());
    if rows.saturating_mul(mask_bits) > 64 {
        return Err(format!(
            "{what} dims (rows={rows}, cols={cols}) exceed the 64-bit packed-hash \
             column budget (rows * ceil(log2(cols)) = {} > 64)",
            rows.saturating_mul(mask_bits)
        ));
    }
    if rows.saturating_mul(cols) > MAX_SKETCH_CELLS {
        return Err(format!(
            "{what} dims (rows={rows}, cols={cols}) exceed the {MAX_SKETCH_CELLS}-cell cap"
        ));
    }
    Ok(())
}

/// Reshape a flat row-major counter array into the legacy matrix layout.
fn matrix(
    what: &str,
    rows: u32,
    cols: u32,
    counter_type: i32,
    counts_int: &[i64],
    counts_float: &[f64],
) -> Result<(Vec<Vec<f64>>, usize, usize), String> {
    let (rows, cols) = (rows as usize, cols as usize);
    validate_sketch_dims(what, rows, cols)?;
    let expected = rows * cols;
    let flat: Vec<f64> = match CounterType::try_from(counter_type)
        .map_err(|_| format!("{what} has unknown counter_type tag {counter_type}"))?
    {
        CounterType::Int32 | CounterType::Int64 if counts_int.len() == expected => {
            counts_int.iter().map(|&v| v as f64).collect()
        }
        CounterType::Float64 if counts_float.len() == expected => counts_float.to_vec(),
        CounterType::Int32 | CounterType::Int64 | CounterType::Float64 => {
            return Err(format!("{what} counts do not match rows*cols = {expected}"))
        }
        other => return Err(format!("{what} counter_type {other:?} is not supported")),
    };
    Ok((flat.chunks(cols).map(<[f64]>::to_vec).collect(), rows, cols))
}

/// A `SketchEnvelope{CountMinState}` (or bare state) frame.
pub fn cms_from_proto(buffer: &[u8]) -> Result<CountMinSketch, String> {
    use asap_sketchlib::proto::sketchlib::CountMinState;
    let (state, sample_p) = enveloped::<CountMinState>(buffer, "CountMinState", |s| match s {
        sketch_envelope::SketchState::CountMin(state) => Some(state),
        _ => None,
    })?;
    unsampled("CountMin", sample_p)?;
    let (m, rows, cols) = matrix(
        "CountMinState",
        state.rows,
        state.cols,
        state.counter_type,
        &state.counts_int,
        &state.counts_float,
    )?;
    Ok(CountMinSketch::from_legacy_matrix(m, rows, cols))
}

pub fn cms_from_msgpack(buffer: &[u8]) -> Result<CountMinSketch, String> {
    CountMinSketch::from_msgpack(buffer)
        .map_err(|e| format!("deserialize CountMinSketch msgpack: {e}"))
}

/// Add a `CountMinDelta` cell frame onto `sketch`.
pub fn apply_cms_proto_delta(sketch: &mut CountMinSketch, buffer: &[u8]) -> Result<(), String> {
    use asap_sketchlib::proto::sketchlib::CountMinDelta as PbDelta;
    let pb = PbDelta::decode(buffer).map_err(|e| format!("decode CountMinDelta: {e}"))?;
    let cells = cells("CountMinDelta", &pb.cell_rows, &pb.cell_cols, &pb.d_counts)?;
    // The vendored proto bindings do not decode `hh_keys`, and CountMin has
    // no heap to rebuild from them.
    let delta = CountMinSketchDelta {
        rows: pb.rows,
        cols: pb.cols,
        cells,
        l1: pb.l1,
        l2: pb.l2,
        hh_keys: Vec::new(),
    };
    sketch
        .apply_delta(&delta)
        .map_err(|e| format!("apply CountMinDelta: {e}"))
}

/// A `CountMinDelta` frame applied onto an empty base of its declared shape
/// (the per-window-reset contract: a window's delta is its whole state).
pub fn cms_from_proto_delta(buffer: &[u8]) -> Result<CountMinSketch, String> {
    use asap_sketchlib::proto::sketchlib::CountMinDelta as PbDelta;
    let pb = PbDelta::decode(buffer).map_err(|e| format!("decode CountMinDelta: {e}"))?;
    let (rows, cols) = (pb.rows as usize, pb.cols as usize);
    validate_sketch_dims("CountMinDelta", rows, cols)?;
    let mut sketch = CountMinSketch::from_legacy_matrix(vec![vec![0.0; cols]; rows], rows, cols);
    apply_cms_proto_delta(&mut sketch, buffer)?;
    Ok(sketch)
}

/// A `SketchEnvelope{CountSketchState}` (or bare state) frame.
pub fn cs_from_proto(buffer: &[u8]) -> Result<CountSketch, String> {
    use asap_sketchlib::proto::sketchlib::CountSketchState;
    let (state, sample_p) =
        enveloped::<CountSketchState>(buffer, "CountSketchState", |s| match s {
            sketch_envelope::SketchState::CountSketch(state) => Some(state),
            _ => None,
        })?;
    unsampled("CountSketch", sample_p)?;
    let (m, rows, cols) = matrix(
        "CountSketchState",
        state.rows,
        state.cols,
        state.counter_type,
        &state.counts_int,
        &state.counts_float,
    )?;
    Ok(CountSketch::from_legacy_matrix(m, rows, cols))
}

pub fn cs_from_msgpack(buffer: &[u8]) -> Result<CountSketch, String> {
    CountSketch::from_msgpack(buffer).map_err(|e| format!("deserialize CountSketch msgpack: {e}"))
}

/// Add a `CountSketchDelta` cell frame onto `sketch`.
pub fn apply_cs_proto_delta(sketch: &mut CountSketch, buffer: &[u8]) -> Result<(), String> {
    use asap_sketchlib::proto::sketchlib::CountSketchDelta as PbDelta;
    let pb = PbDelta::decode(buffer).map_err(|e| format!("decode CountSketchDelta: {e}"))?;
    let cells = cells(
        "CountSketchDelta",
        &pb.cell_rows,
        &pb.cell_cols,
        &pb.d_counts,
    )?;
    let delta = CountSketchDelta {
        rows: pb.rows,
        cols: pb.cols,
        cells,
        l2: pb.l2,
        hh_keys: Vec::new(),
    };
    sketch
        .apply_delta(&delta)
        .map_err(|e| format!("apply CountSketchDelta: {e}"))
}

/// A `CountSketchDelta` frame applied onto an empty base of its declared shape.
pub fn cs_from_proto_delta(buffer: &[u8]) -> Result<CountSketch, String> {
    use asap_sketchlib::proto::sketchlib::CountSketchDelta as PbDelta;
    let pb = PbDelta::decode(buffer).map_err(|e| format!("decode CountSketchDelta: {e}"))?;
    let (rows, cols) = (pb.rows as usize, pb.cols as usize);
    validate_sketch_dims("CountSketchDelta", rows, cols)?;
    let mut sketch = CountSketch::from_legacy_matrix(vec![vec![0.0; cols]; rows], rows, cols);
    apply_cs_proto_delta(&mut sketch, buffer)?;
    Ok(sketch)
}

fn cells(
    what: &str,
    rows: &[u32],
    cols: &[u32],
    d_counts: &[i64],
) -> Result<Vec<(u32, u32, i64)>, String> {
    if rows.len() != cols.len() || rows.len() != d_counts.len() {
        return Err(format!(
            "{what} packed-array length mismatch: cell_rows={}, cell_cols={}, d_counts={}",
            rows.len(),
            cols.len(),
            d_counts.len()
        ));
    }
    Ok(rows
        .iter()
        .zip(cols)
        .zip(d_counts)
        .map(|((r, c), d)| (*r, *c, *d))
        .collect())
}

// ---------------------------------------------------------------------------
// Heap-bearing frequency sketches (legacy integer heaps)
// ---------------------------------------------------------------------------

pub fn cms_with_heap_from_msgpack(buffer: &[u8]) -> Result<CountMinSketchWithHeap, String> {
    CountMinSketchWithHeap::from_msgpack(buffer)
        .map_err(|e| format!("deserialize CountMinSketchWithHeap msgpack: {e}"))
}

pub fn cs_with_heap_from_msgpack(buffer: &[u8]) -> Result<CountSketchWithHeap, String> {
    CountSketchWithHeap::from_msgpack(buffer)
        .map_err(|e| format!("deserialize CountSketchWithHeap msgpack: {e}"))
}

/// sketchlib-go's DELTA-HEAP frame (`MSGPACK_DELTA`): a positional array of
/// `[is_delta, [rows, cols, cells], topk_heap, heap_size]`. The matrix part is
/// a sparse signed delta; the heap is the window's full heap.
#[derive(serde::Deserialize)]
struct HeapDeltaWire {
    is_delta: bool,
    matrix_delta: MatrixDeltaWire,
    topk_heap: Vec<(String, f64)>,
    heap_size: u64,
}

#[derive(serde::Deserialize)]
struct MatrixDeltaWire {
    rows: u32,
    cols: u32,
    cells: Vec<(u32, u32, i64)>,
}

fn heap_delta(buffer: &[u8]) -> Result<HeapDeltaWire, String> {
    let wire: HeapDeltaWire =
        rmp_serde::from_slice(buffer).map_err(|e| format!("decode heap delta msgpack: {e}"))?;
    if !wire.is_delta {
        return Err("heap delta frame has is_delta=false".into());
    }
    Ok(wire)
}

/// Add a DELTA-HEAP frame's cells to `matrix`; cells outside it are ignored.
fn fold_heap_delta(matrix: &mut [Vec<f64>], wire: &HeapDeltaWire) {
    for &(r, c, d) in &wire.matrix_delta.cells {
        if let Some(cell) = matrix
            .get_mut(r as usize)
            .and_then(|row| row.get_mut(c as usize))
        {
            *cell += d as f64;
        }
    }
}

/// Apply a DELTA-HEAP frame onto a Count-Min heap state, replacing its heap.
pub fn apply_cms_heap_delta(
    sketch: &mut CountMinSketchWithHeap,
    buffer: &[u8],
) -> Result<(), String> {
    let wire = heap_delta(buffer)?;
    let (rows, cols, heap_size) = (sketch.rows(), sketch.cols(), sketch.heap_size);
    let mut m = sketch.sketch_matrix();
    fold_heap_delta(&mut m, &wire);
    let heap = wire
        .topk_heap
        .into_iter()
        .map(|(key, value)| CmsHeapItem { key, value })
        .collect();
    *sketch = CountMinSketchWithHeap::from_legacy_matrix(m, heap, rows, cols, heap_size);
    Ok(())
}

/// Apply a DELTA-HEAP frame onto a Count Sketch heap state, replacing its heap.
pub fn apply_cs_heap_delta(sketch: &mut CountSketchWithHeap, buffer: &[u8]) -> Result<(), String> {
    let wire = heap_delta(buffer)?;
    let (rows, cols, heap_size) = (sketch.rows(), sketch.cols(), sketch.heap_size);
    let mut m = sketch.sketch_matrix();
    fold_heap_delta(&mut m, &wire);
    let heap = wire
        .topk_heap
        .into_iter()
        .map(|(key, value)| CsHeapItem { key, value })
        .collect();
    *sketch = CountSketchWithHeap::from_legacy_matrix(m, heap, rows, cols, heap_size);
    Ok(())
}

fn heap_delta_shape(buffer: &[u8]) -> Result<(usize, usize, usize), String> {
    let wire = heap_delta(buffer)?;
    let (rows, cols) = (
        wire.matrix_delta.rows as usize,
        wire.matrix_delta.cols as usize,
    );
    if rows == 0 || cols == 0 {
        return Err(format!(
            "heap delta frame has zero dims (rows={rows}, cols={cols})"
        ));
    }
    Ok((rows, cols, wire.heap_size as usize))
}

/// A DELTA-HEAP frame applied onto an empty Count-Min heap of its shape.
pub fn cms_with_heap_from_msgpack_delta(buffer: &[u8]) -> Result<CountMinSketchWithHeap, String> {
    let (rows, cols, heap_size) = heap_delta_shape(buffer)?;
    let mut sketch = CountMinSketchWithHeap::new(rows, cols, heap_size);
    apply_cms_heap_delta(&mut sketch, buffer)?;
    Ok(sketch)
}

/// A DELTA-HEAP frame applied onto an empty Count Sketch heap of its shape.
pub fn cs_with_heap_from_msgpack_delta(buffer: &[u8]) -> Result<CountSketchWithHeap, String> {
    let (rows, cols, heap_size) = heap_delta_shape(buffer)?;
    let mut sketch = CountSketchWithHeap::new(rows, cols, heap_size);
    apply_cs_heap_delta(&mut sketch, buffer)?;
    Ok(sketch)
}

// ---------------------------------------------------------------------------
// Exact Sum payload
// ---------------------------------------------------------------------------

/// The edge Sum payload: little-endian `f64` sum followed by `u64` count.
pub fn sum_payload(buffer: &[u8]) -> Result<f64, String> {
    let sum: [u8; 8] = buffer
        .get(..8)
        .filter(|_| buffer.len() >= 16)
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| format!("Sum payload too short: {} bytes (want 16)", buffer.len()))?;
    Ok(f64::from_le_bytes(sum))
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_sketchlib::proto::sketchlib as pb;

    fn envelope(state: sketch_envelope::SketchState, sample_p: f64) -> Vec<u8> {
        SketchEnvelope {
            sketch_state: Some(state),
            sample_p,
            ..Default::default()
        }
        .encode_to_vec()
    }
    fn cms_state(rows: u32, cols: u32, counts: Vec<i64>) -> pb::CountMinState {
        pb::CountMinState {
            rows,
            cols,
            counter_type: CounterType::Int64 as i32,
            counts_int: counts,
            ..Default::default()
        }
    }

    // Envelope-wrapped and bare matrix frames decode to the same sketch.
    #[test]
    fn matrix_frames_decode_with_or_without_envelope() {
        let state = cms_state(2, 4, vec![1, 0, 0, 0, 0, 2, 0, 0]);
        let bare = cms_from_proto(&state.encode_to_vec()).unwrap();
        let wrapped = cms_from_proto(&envelope(
            sketch_envelope::SketchState::CountMin(state),
            0.0,
        ))
        .unwrap();
        assert_eq!(bare.sketch(), wrapped.sketch());
        assert_eq!(bare.sketch()[1][1], 2.0);
        let cs = pb::CountSketchState {
            rows: 1,
            cols: 2,
            counter_type: CounterType::Float64 as i32,
            counts_float: vec![-1.5, 2.0],
            ..Default::default()
        };
        assert_eq!(
            cs_from_proto(&cs.encode_to_vec()).unwrap().sketch()[0],
            vec![-1.5, 2.0]
        );
    }

    // Malformed shapes, wrong families and sampled frames are rejected.
    #[test]
    fn invalid_or_sampled_frames_are_rejected() {
        assert!(cms_from_proto(&cms_state(2, 4, vec![1]).encode_to_vec()).is_err());
        assert!(cms_from_proto(&cms_state(0, 4, vec![]).encode_to_vec()).is_err());
        assert!(cms_from_proto(&cms_state(64, 1 << 20, vec![]).encode_to_vec()).is_err());
        let kll = envelope(
            sketch_envelope::SketchState::Kll(pb::KllState::default()),
            0.0,
        );
        assert!(cms_from_proto(&kll).is_err());
        let sampled = envelope(
            sketch_envelope::SketchState::CountMin(cms_state(1, 2, vec![1, 1])),
            0.5,
        );
        assert!(cms_from_proto(&sampled)
            .unwrap_err()
            .contains("edge-sampled"));
        let mut dd = DdSketch::new(0.01);
        dd.update(1.0);
        let mut dd_state =
            asap_sketch_codec::ddsketch_state(&asap_sketch_codec::encode_ddsketch(&dd))
                .unwrap()
                .0;
        assert!(ddsketch_from_proto(&envelope(
            sketch_envelope::SketchState::Ddsketch(dd_state.clone()),
            0.25
        ))
        .unwrap_err()
        .contains("edge-sampled"));
        dd_state.alpha = 0.0;
        assert!(ddsketch_from_proto(&envelope(
            sketch_envelope::SketchState::Ddsketch(dd_state),
            0.0
        ))
        .is_err());
    }

    // Cell deltas add onto the base; a standalone delta starts from empty.
    #[test]
    fn matrix_deltas_add_cells() {
        let delta = pb::CountMinDelta {
            rows: 2,
            cols: 4,
            cell_rows: vec![0, 1],
            cell_cols: vec![1, 3],
            d_counts: vec![5, 7],
            ..Default::default()
        }
        .encode_to_vec();
        let mut base =
            cms_from_proto(&cms_state(2, 4, vec![0, 1, 0, 0, 0, 0, 0, 0]).encode_to_vec()).unwrap();
        apply_cms_proto_delta(&mut base, &delta).unwrap();
        assert_eq!(base.sketch()[0][1], 6.0);
        assert_eq!(base.sketch()[1][3], 7.0);
        assert_eq!(cms_from_proto_delta(&delta).unwrap().sketch()[0][1], 5.0);
        let mismatched = pb::CountSketchDelta {
            rows: 1,
            cols: 2,
            cell_rows: vec![0],
            ..Default::default()
        }
        .encode_to_vec();
        assert!(cs_from_proto_delta(&mismatched).is_err());
    }

    // Dense, sparse and absent HLL registers reconstruct the same register array.
    #[test]
    fn hll_register_forms_decode_and_delta_takes_register_max() {
        let dense = pb::HyperLogLogState {
            precision: 4,
            variant: pb::HllVariant::Regular as i32,
            registers: vec![0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ..Default::default()
        };
        let sparse = pb::HyperLogLogState {
            registers: Vec::new(),
            registers_sparse: Some(pb::HllSparseRegisters {
                packed: vec![1, 3, 14, 1],
                num_registers: 16,
            }),
            ..dense.clone()
        };
        let empty = pb::HyperLogLogState {
            registers: Vec::new(),
            ..dense.clone()
        };
        let mut sketch = hll_from_proto(&dense.encode_to_vec()).unwrap();
        assert_eq!(
            sketch.registers,
            hll_from_proto(&sparse.encode_to_vec()).unwrap().registers
        );
        assert_eq!(
            hll_from_proto(&empty.encode_to_vec()).unwrap().registers,
            vec![0; 16]
        );
        let delta = pb::HllDelta {
            packed_updates: vec![0, 2, 1, 1],
        };
        apply_hll_proto_delta(&mut sketch, &delta.encode_to_vec()).unwrap();
        assert_eq!(&sketch.registers[..2], &[2, 3]);
    }

    // KLL frames reconstruct from their level layout and reject bad layouts.
    #[test]
    fn kll_frames_follow_their_level_layout() {
        let frame = |k, levels: Vec<u32>, items: Vec<f64>| {
            envelope(
                sketch_envelope::SketchState::Kll(pb::KllState {
                    k,
                    num_levels: levels.len().saturating_sub(1) as u32,
                    levels,
                    items,
                    ..Default::default()
                }),
                0.0,
            )
        };
        let sketch = kll_from_proto(&frame(200, vec![0, 3], vec![1.0, 2.0, 3.0])).unwrap();
        assert_eq!(sketch.count(), 3);
        assert_eq!(
            kll_from_proto(&frame(200, vec![], vec![4.0]))
                .unwrap()
                .count(),
            1
        );
        assert!(kll_from_proto(&frame(4, vec![0, 1], vec![1.0])).is_err());
        assert!(kll_from_proto(&frame(200, vec![0, 2], vec![1.0])).is_err());
    }

    // A heap delta adds its cells and replaces the heap with the frame's heap.
    #[test]
    fn heap_delta_adds_cells_and_replaces_heap() {
        #[derive(serde::Serialize)]
        struct Frame(
            bool,
            (u32, u32, Vec<(u32, u32, i64)>),
            Vec<(String, f64)>,
            u64,
        );
        let frame = rmp_serde::to_vec(&Frame(
            true,
            (2, 8, vec![(0, 1, 4), (1, 2, 4)]),
            vec![("b".into(), 4.0)],
            2,
        ))
        .unwrap();
        let mut base = CountMinSketchWithHeap::new(2, 8, 2);
        base.update("a", 1.0);
        let before = base.sketch_matrix();
        apply_cms_heap_delta(&mut base, &frame).unwrap();
        assert_eq!(base.sketch_matrix()[0][1], before[0][1] + 4.0);
        let heap = base.topk_heap_items();
        assert_eq!((heap.len(), heap[0].key.as_str()), (1, "b"));
        let standalone = cs_with_heap_from_msgpack_delta(&frame).unwrap();
        assert_eq!(standalone.sketch_matrix()[1][2], 4.0);
        let not_delta = rmp_serde::to_vec(&Frame(false, (2, 8, vec![]), vec![], 2)).unwrap();
        assert!(cms_with_heap_from_msgpack_delta(&not_delta).is_err());
    }

    // The edge Sum payload is a little-endian f64 sum followed by a count.
    #[test]
    fn sum_payload_reads_the_sum_and_requires_the_count() {
        let mut bytes = 2.5f64.to_le_bytes().to_vec();
        assert!(sum_payload(&bytes).is_err());
        bytes.extend(3u64.to_le_bytes());
        assert_eq!(sum_payload(&bytes).unwrap(), 2.5);
    }
}
