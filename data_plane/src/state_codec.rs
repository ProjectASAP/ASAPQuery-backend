//! Compact encodings of a fragment's state and text columns
//! (docs/design/tiered-view-storage.md, "On-disk format").
//!
//! A state column is written in the executor's mapped layout
//! ([`asap_executor::values::state_view_bytes`]): fixed-width fields and
//! 8-byte-aligned arrays, read in place. That layout is large for the small
//! states of narrow panes: a DDSketch of ten samples is an 88-byte header
//! plus a dense run of 1-byte bucket counts, most of them empty. This module
//! stores the same states in a compact form and rebuilds the mapped layout,
//! byte for byte, when a window is read cold:
//!
//! - DDSketch: only non-empty buckets, as a varint gap from the previous
//!   bucket and a varint count; the first bucket of each row as a signed
//!   delta from the previous row's; the index mapping (`alpha`) and the
//!   admission rate once per column; the sample count left out (it is the
//!   sum of the bucket and zero counts).
//! - Exact records: a tag byte of which fields are present, the floats
//!   that are, and timestamps as varint deltas.
//! - Text keys: each row's dictionary key as a signed varint delta from
//!   the previous row's.
//!
//! Everything here is lossless: decoding gives the bytes the mapped layout
//! held, so every answer read from a compact column is the one read from
//! the mapped column. A state the encoder does not recognize as canonical
//! is kept whole (a raw-record tag), never approximated.

/// The state families with a compact form, as the mapped layout names
/// them: which fixed-size exact record, or which sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactKind {
    DDSketch,
    /// Exact sum: `sum`, `compensation` (f64), `seen` (0 or 1).
    Sum,
    /// Exact count: one `u64`.
    Count,
    /// Exact minimum or maximum: `present` (0 or 1), `value` (f64).
    MinMax,
    /// Exact counter: `present`, `starting_value`, `starting_timestamp`,
    /// `last_seen_value`, `last_seen_timestamp`, `total_increase`,
    /// `sample_count`.
    Counter,
    /// Exact last two samples: two of (`present`, `timestamp`, `value`).
    LastTwoSamples,
}

impl CompactKind {
    /// Its code in a fragment's column descriptor.
    pub fn code(self) -> u32 {
        match self {
            Self::DDSketch => 1,
            Self::Sum => 2,
            Self::Count => 3,
            Self::MinMax => 4,
            Self::Counter => 5,
            Self::LastTwoSamples => 6,
        }
    }

    pub fn from_code(code: u32) -> Option<Self> {
        Some(match code {
            1 => Self::DDSketch,
            2 => Self::Sum,
            3 => Self::Count,
            4 => Self::MinMax,
            5 => Self::Counter,
            6 => Self::LastTwoSamples,
            _ => return None,
        })
    }

    /// The bytes of one record in the mapped layout, for an exact kind.
    fn record_bytes(self) -> Option<usize> {
        Some(match self {
            Self::DDSketch => return None,
            Self::Sum => 24,
            Self::Count => 8,
            Self::MinMax => 16,
            Self::Counter => 56,
            Self::LastTwoSamples => 48,
        })
    }
}

// ---------------------------------------------------------------- varints

fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn uvarint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at)?;
        *at += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn put_ivarint(out: &mut Vec<u8>, value: i64) {
    put_uvarint(out, ((value << 1) ^ (value >> 63)) as u64);
}

fn ivarint(bytes: &[u8], at: &mut usize) -> Option<i64> {
    let zigzag = uvarint(bytes, at)?;
    Some((zigzag >> 1) as i64 ^ -((zigzag & 1) as i64))
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn u64_at(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let raw = bytes.get(*at..*at + 8)?;
    *at += 8;
    Some(u64::from_le_bytes(raw.try_into().ok()?))
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn padded(n: usize) -> usize {
    n.div_ceil(8) * 8
}

// --------------------------------------------------------------- DDSketch

const DD_MAGIC: [u8; 4] = *b"DDV1";
const VIEW_LAYOUT_VERSION: u32 = 1;
/// Bytes of a DDSketch view before its arrays: magic and version, seven
/// 8-byte fields, six 4-byte fields.
const DD_HEAD_BYTES: usize = 88;
/// The widest store a DDSketch view may span, in buckets (the view
/// reader's limit): bucket index arithmetic stays far inside `i32`.
const MAX_SPAN: i64 = 1 << 20;

/// One DDSketch store's non-empty buckets: absolute bucket index, count.
type Buckets = Vec<(i32, u64)>;

/// A DDSketch view's contents, as its compact form keeps them.
#[derive(Debug, Default, Clone, PartialEq)]
struct DdContents {
    alpha: u64,
    sum: u64,
    min: u64,
    max: u64,
    zero_count: u64,
    admission_rate: u64,
    positive: Buckets,
    negative: Buckets,
}

/// Count bytes of a store's counts at `width` (padded to 8).
fn counts_bytes(entries: usize, width: usize) -> usize {
    padded(entries * width)
}

/// The width the view writer picks for counts up to `largest`.
fn count_width(largest: u64) -> usize {
    match largest {
        0..=0xff => 1,
        0x100..=0xffff => 2,
        0x1_0000..=0xffff_ffff => 4,
        _ => 8,
    }
}

/// The dense span of a store: last index - first index + 1.
fn span(buckets: &Buckets) -> usize {
    match (buckets.first(), buckets.last()) {
        (Some(first), Some(last)) => (i64::from(last.0) - i64::from(first.0) + 1) as usize,
        _ => 0,
    }
}

/// Reads a DDSketch view's contents into `dd` (its bucket lists reused);
/// `None` when the bytes are not a DDSketch view of layout version 1 in the
/// form its writer writes.
fn read_dd(view: &[u8], dd: &mut DdContents) -> Option<()> {
    if view.get(..4)? != DD_MAGIC || u32_at(view, 4)? != VIEW_LAYOUT_VERSION {
        return None;
    }
    let mut at = 8;
    let alpha = u64_at(view, &mut at)?;
    let count = u64_at(view, &mut at)?;
    let sum = u64_at(view, &mut at)?;
    let min = u64_at(view, &mut at)?;
    let max = u64_at(view, &mut at)?;
    let zero_count = u64_at(view, &mut at)?;
    let admission_rate = u64_at(view, &mut at)?;
    let width = u32_at(view, 64)? as usize;
    let sparse = match u32_at(view, 68)? {
        0 => false,
        1 => true,
        _ => return None,
    };
    if ![1, 2, 4, 8].contains(&width) {
        return None;
    }
    let mut at = DD_HEAD_BYTES;
    // An array, and the zero padding after it.
    let mut array = |bytes: usize| -> Option<&[u8]> {
        let array = view.get(at..at + bytes)?;
        let end = padded(at + bytes);
        if view.get(at + bytes..end)?.iter().any(|&b| b != 0) {
            return None;
        }
        at = end;
        Some(array)
    };
    let mut store = |offset: i32, len: usize, buckets: &mut Buckets| -> Option<()> {
        buckets.clear();
        let keys = if sparse { Some(array(len * 4)?) } else { None };
        let counts = array(len * width)?;
        for i in 0..len {
            let raw = &counts[i * width..(i + 1) * width];
            let mut word = [0u8; 8];
            word[..width].copy_from_slice(raw);
            let count = u64::from_le_bytes(word);
            let relative = match keys {
                Some(keys) => u32::from_le_bytes(keys[i * 4..i * 4 + 4].try_into().ok()?),
                None => i as u32,
            };
            let index = offset.checked_add(i32::try_from(relative).ok()?)?;
            if buckets.last().is_some_and(|&(last, _)| last >= index) {
                return None;
            }
            if count != 0 {
                buckets.push((index, count));
            } else if sparse {
                // The writer keeps only non-empty buckets in a sparse store.
                return None;
            }
        }
        Some(())
    };
    let (positive_offset, negative_offset) = (u32_at(view, 72)? as i32, u32_at(view, 80)? as i32);
    store(
        positive_offset,
        u32_at(view, 76)? as usize,
        &mut dd.positive,
    )?;
    store(
        negative_offset,
        u32_at(view, 84)? as usize,
        &mut dd.negative,
    )?;
    if at != view.len() {
        return None;
    }
    (dd.alpha, dd.sum, dd.min, dd.max) = (alpha, sum, min, max);
    (dd.zero_count, dd.admission_rate) = (zero_count, admission_rate);
    let contents = &*dd;
    // Only the form the view writer writes, so the compact form rebuilds
    // these bytes exactly: the narrowest count width, the smaller of the
    // dense and sparse forms, a dense store trimmed to non-empty ends, an
    // empty store at offset 0. The count is left out of the compact form:
    // it must be the buckets'.
    let (form_width, form_sparse) = contents.form();
    let trimmed = |buckets: &Buckets, offset: i32, len: usize| match buckets.first() {
        None => offset == 0 && len == 0,
        Some((first, _)) => *first == offset && (sparse || span(buckets) == len),
    };
    let canonical = width == form_width
        && span(&contents.positive) as i64 <= MAX_SPAN
        && span(&contents.negative) as i64 <= MAX_SPAN
        && sparse == form_sparse
        && trimmed(
            &contents.positive,
            positive_offset,
            u32_at(view, 76)? as usize,
        )
        && trimmed(
            &contents.negative,
            negative_offset,
            u32_at(view, 84)? as usize,
        );
    (canonical && contents.count()? == count).then_some(())
}

impl DdContents {
    /// Its sample count: the bucket counts and the zero count.
    fn count(&self) -> Option<u64> {
        self.positive
            .iter()
            .chain(&self.negative)
            .try_fold(self.zero_count, |total, (_, count)| {
                total.checked_add(*count)
            })
    }

    /// The count width and store form (sparse or not) the view writer
    /// picks for its buckets.
    fn form(&self) -> (usize, bool) {
        let largest = self
            .positive
            .iter()
            .chain(&self.negative)
            .map(|(_, count)| *count)
            .max()
            .unwrap_or(0);
        let width = count_width(largest);
        let (positive, negative) = (&self.positive, &self.negative);
        let dense = counts_bytes(span(positive), width) + counts_bytes(span(negative), width);
        let sparse = padded(positive.len() * 4)
            + counts_bytes(positive.len(), width)
            + padded(negative.len() * 4)
            + counts_bytes(negative.len(), width);
        (width, sparse < dense)
    }

    /// Its view bytes, as `DdSketch::to_view_bytes` writes them.
    fn view_bytes(&self, out: &mut Vec<u8>) -> Option<()> {
        let (width, sparse) = self.form();
        let (positive, negative) = (&self.positive, &self.negative);
        let start = out.len();
        out.extend_from_slice(&DD_MAGIC);
        out.extend_from_slice(&VIEW_LAYOUT_VERSION.to_le_bytes());
        for word in [
            self.alpha,
            self.count()?,
            self.sum,
            self.min,
            self.max,
            self.zero_count,
            self.admission_rate,
        ] {
            put_u64(out, word);
        }
        let entries = |store: &Buckets| if sparse { store.len() } else { span(store) };
        let offset = |store: &Buckets| store.first().map_or(0, |(index, _)| *index);
        for word in [
            width as u32,
            u32::from(sparse),
            offset(positive) as u32,
            entries(positive) as u32,
            offset(negative) as u32,
            entries(negative) as u32,
        ] {
            out.extend_from_slice(&word.to_le_bytes());
        }
        let pad = |out: &mut Vec<u8>| out.resize(start + padded(out.len() - start), 0);
        for store in [positive, negative] {
            let first = offset(store);
            if sparse {
                for (index, _) in store {
                    out.extend_from_slice(&((index - first) as u32).to_le_bytes());
                }
                pad(out);
                for (_, count) in store {
                    out.extend_from_slice(&count.to_le_bytes()[..width]);
                }
            } else {
                let mut next = first;
                for (index, count) in store {
                    let empty = (index - next) as usize * width;
                    out.resize(out.len() + empty, 0);
                    out.extend_from_slice(&count.to_le_bytes()[..width]);
                    next = index + 1;
                }
            }
            pad(out);
        }
        Some(())
    }
}

// ---------------------------------------------------------------- streams

/// Float fields a kind keeps per row, at most.
const FLOAT_FIELDS: usize = 3;

/// A compact column as it is written: the varints and tags of every row in
/// one stream, and each float field of every row in a stream of its own,
/// stored as byte planes (all rows' first bytes, then all second bytes,
/// ...): the sign and exponent bytes of one field across series repeat,
/// which a general-purpose codec over the fragment then compresses.
#[derive(Default)]
struct Streams {
    ints: Vec<u8>,
    floats: [Vec<u64>; FLOAT_FIELDS],
}

impl Streams {
    /// `uvarint` lengths of the int stream and each float stream, then the
    /// int stream, then each float stream's byte planes.
    fn finish(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            8 + self.ints.len() + self.floats.iter().map(|f| f.len() * 8).sum::<usize>(),
        );
        put_uvarint(&mut out, self.ints.len() as u64);
        for floats in &self.floats {
            put_uvarint(&mut out, floats.len() as u64);
        }
        out.extend_from_slice(&self.ints);
        for floats in &self.floats {
            for plane in 0..8 {
                out.extend(floats.iter().map(|word| (word >> (8 * plane)) as u8));
            }
        }
        out
    }
}

/// Reads a column [`Streams::finish`] wrote.
struct StreamReader<'a> {
    ints: &'a [u8],
    at: usize,
    floats: [Vec<u64>; FLOAT_FIELDS],
    next: [usize; FLOAT_FIELDS],
}

impl<'a> StreamReader<'a> {
    fn new(bytes: &'a [u8]) -> Option<Self> {
        let mut at = 0;
        let ints_len = usize::try_from(uvarint(bytes, &mut at)?).ok()?;
        let mut lens = [0usize; FLOAT_FIELDS];
        for len in &mut lens {
            *len = usize::try_from(uvarint(bytes, &mut at)?).ok()?;
        }
        let ints = bytes.get(at..at.checked_add(ints_len)?)?;
        at += ints_len;
        let mut floats: [Vec<u64>; FLOAT_FIELDS] = Default::default();
        for (field, &len) in lens.iter().enumerate() {
            let planes = bytes.get(at..at.checked_add(len.checked_mul(8)?)?)?;
            at += len * 8;
            let mut words = vec![0u64; len];
            for (plane, bytes) in planes.chunks_exact(len.max(1)).take(8).enumerate() {
                for (word, &byte) in words.iter_mut().zip(bytes) {
                    *word |= u64::from(byte) << (8 * plane);
                }
            }
            floats[field] = words;
        }
        (at == bytes.len()).then_some(Self {
            ints,
            at: 0,
            floats,
            next: [0; FLOAT_FIELDS],
        })
    }

    fn uvarint(&mut self) -> Option<u64> {
        uvarint(self.ints, &mut self.at)
    }

    fn ivarint(&mut self) -> Option<i64> {
        ivarint(self.ints, &mut self.at)
    }

    fn byte(&mut self) -> Option<u8> {
        let byte = *self.ints.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    fn raw(&mut self, len: usize) -> Option<&'a [u8]> {
        let raw = self.ints.get(self.at..self.at.checked_add(len)?)?;
        self.at += len;
        Some(raw)
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.raw(8)?.try_into().ok()?))
    }

    fn float(&mut self, field: usize) -> Option<u64> {
        let word = *self.floats[field].get(self.next[field])?;
        self.next[field] += 1;
        Some(word)
    }

    /// Whether every stream was read to its end.
    fn finished(&self) -> bool {
        self.at == self.ints.len()
            && self
                .next
                .iter()
                .zip(&self.floats)
                .all(|(&next, floats)| next == floats.len())
    }
}

// --------------------------------------------------------------- buckets

fn uvarint_len(value: u64) -> usize {
    (64 - value.max(1).leading_zeros() as usize).div_ceil(7)
}

/// Appends one store's buckets: a header (the entry count, and whether the
/// store is dense), the first index (a delta from `previous_first`, the
/// previous row's), then either the gaps between non-empty buckets and
/// their counts (sparse) or the count of every bucket from the first to the
/// last, empty ones as 0 (dense), whichever is shorter: a narrow pane's few
/// samples are spread out, a wide pane's buckets are mostly filled.
fn put_buckets(out: &mut Vec<u8>, buckets: &Buckets, previous_first: &mut i32) {
    let Some(&(first, _)) = buckets.first() else {
        put_uvarint(out, 0);
        return;
    };
    let gaps: usize = buckets
        .windows(2)
        .map(|pair| uvarint_len((pair[1].0 - pair[0].0 - 1) as u64))
        .sum();
    // Both forms write the non-empty counts; dense adds a byte per empty
    // bucket, sparse the gaps.
    let empty = span(buckets) - buckets.len();
    let dense = empty < gaps;
    let entries = if dense { span(buckets) } else { buckets.len() };
    put_uvarint(out, (entries as u64) << 1 | u64::from(dense));
    put_ivarint(out, i64::from(first) - i64::from(*previous_first));
    *previous_first = first;
    if dense {
        let mut next = first;
        for (index, count) in buckets {
            out.resize(out.len() + (index - next) as usize, 0);
            put_uvarint(out, *count);
            next = index + 1;
        }
    } else {
        for pair in buckets.windows(2) {
            put_uvarint(out, (pair[1].0 - pair[0].0 - 1) as u64);
        }
        for (_, count) in buckets {
            put_uvarint(out, *count);
        }
    }
}

/// Reads one store's buckets into `out` (cleared first).
fn buckets(reader: &mut StreamReader, previous_first: &mut i32, out: &mut Buckets) -> Option<()> {
    out.clear();
    let header = reader.uvarint()?;
    let (entries, dense) = (usize::try_from(header >> 1).ok()?, header & 1 == 1);
    if entries == 0 {
        return (!dense).then_some(());
    }
    // Every entry takes at least a byte; a bad length must not size an
    // allocation.
    if entries > reader.ints.len() - reader.at {
        return None;
    }
    let first = i64::from(*previous_first).checked_add(reader.ivarint()?)?;
    let first = i32::try_from(first).ok()?;
    *previous_first = first;
    out.reserve(entries);
    if dense {
        for slot in 0..entries {
            let count = reader.uvarint()?;
            if count != 0 {
                out.push((first.checked_add(i32::try_from(slot).ok()?)?, count));
            } else if slot == 0 || slot == entries - 1 {
                // A dense store spans exactly its non-empty buckets.
                return None;
            }
        }
    } else {
        let mut index = first;
        out.push((index, 0));
        for _ in 1..entries {
            let gap = i32::try_from(reader.uvarint()?).ok()?;
            index = index.checked_add(gap)?.checked_add(1)?;
            out.push((index, 0));
        }
        for bucket in out.iter_mut() {
            bucket.1 = reader.uvarint()?;
            if bucket.1 == 0 {
                return None;
            }
        }
    }
    (span(out) as i64 <= MAX_SPAN).then_some(())
}

// ------------------------------------------------------------ the columns

/// The tag of an exact record kept whole, in its mapped layout.
const RAW: u8 = 0xff;

/// A column of `states` (each the mapped-layout bytes of one state of
/// `kind`) in compact form; `None` when the column has no compact form
/// (its sketches differ in index mapping or admission rate, or a state is
/// not of `kind`'s layout).
pub fn encode_states(
    kind: CompactKind,
    states: impl Iterator<Item = impl AsRef<[u8]>>,
) -> Option<Vec<u8>> {
    let mut out = Streams::default();
    match kind {
        CompactKind::DDSketch => {
            let mut shared: Option<(u64, u64)> = None;
            let mut previous = (0i32, 0i32);
            let mut dd = DdContents::default();
            for state in states {
                read_dd(state.as_ref(), &mut dd)?;
                match shared {
                    None => {
                        shared = Some((dd.alpha, dd.admission_rate));
                        put_u64(&mut out.ints, dd.alpha);
                        put_u64(&mut out.ints, dd.admission_rate);
                    }
                    Some(shared) if shared != (dd.alpha, dd.admission_rate) => return None,
                    Some(_) => {}
                }
                put_uvarint(&mut out.ints, dd.zero_count);
                out.floats[0].push(dd.sum);
                out.floats[1].push(dd.min);
                out.floats[2].push(dd.max);
                put_buckets(&mut out.ints, &dd.positive, &mut previous.0);
                put_buckets(&mut out.ints, &dd.negative, &mut previous.1);
            }
        }
        _ => {
            let size = kind.record_bytes().expect("an exact kind");
            let mut previous_times = [0i64; 2];
            let mut words = [0u64; 7];
            for state in states {
                let state = state.as_ref();
                if state.len() != size {
                    return None;
                }
                for (word, bytes) in words.iter_mut().zip(state.as_chunks::<8>().0) {
                    *word = u64::from_le_bytes(*bytes);
                }
                if !put_record(&mut out, kind, &words[..size / 8], &mut previous_times) {
                    out.ints.push(RAW);
                    out.ints.extend_from_slice(state);
                }
            }
        }
    }
    Some(out.finish())
}

/// The absent counter's record after its presence word, as
/// `IncreaseAccumulator::new(0.0, 0, 0.0, 0)` writes it.
const ABSENT_COUNTER: [u64; 6] = [0, 0, 0, 0, 0, 1];

/// Appends exact record `words` of `kind` in compact form; `false` when
/// the record is not in the form the executor writes (it is then kept
/// whole). Each timestamp is a delta from the same field of the previous
/// record that had one (`previous_times`).
fn put_record(
    out: &mut Streams,
    kind: CompactKind,
    words: &[u64],
    previous_times: &mut [i64; 2],
) -> bool {
    let mut time = |ints: &mut Vec<u8>, field: usize, word: u64| {
        put_ivarint(ints, (word as i64).wrapping_sub(previous_times[field]));
        previous_times[field] = word as i64;
    };
    match (kind, words) {
        (CompactKind::Count, [count]) => {
            out.ints.push(0);
            put_uvarint(&mut out.ints, *count);
        }
        (CompactKind::Sum, [sum, compensation, seen @ (0 | 1)]) => {
            let tag = *seen as u8 | u8::from(*compensation != 0) << 1;
            out.ints.push(tag);
            out.floats[0].push(*sum);
            if *compensation != 0 {
                out.floats[1].push(*compensation);
            }
        }
        (CompactKind::MinMax, [0, 0]) => out.ints.push(0),
        (CompactKind::MinMax, [1, value]) => {
            out.ints.push(1);
            out.floats[0].push(*value);
        }
        (CompactKind::Counter, [0, rest @ ..]) if *rest == ABSENT_COUNTER => out.ints.push(0),
        (CompactKind::Counter, [1, start, start_time, last, last_time, total, samples]) => {
            // The increase, when it is the plain difference (no reset).
            // (Never for a NaN difference: its payload need not repeat.)
            let difference = f64::from_bits(*last) - f64::from_bits(*start);
            let plain = !difference.is_nan() && difference.to_bits() == *total;
            out.ints.push(1 | u8::from(plain) << 1);
            time(&mut out.ints, 0, *start_time);
            time(&mut out.ints, 1, *last_time);
            put_uvarint(&mut out.ints, *samples);
            out.floats[0].push(*start);
            out.floats[1].push(*last);
            if !plain {
                out.floats[2].push(*total);
            }
        }
        (CompactKind::LastTwoSamples, [p0 @ (0 | 1), t0, v0, p1 @ (0 | 1), t1, v1]) => {
            let samples = [(*p0, *t0, *v0), (*p1, *t1, *v1)];
            if samples
                .iter()
                .any(|&(present, t, v)| present == 0 && (t, v) != (0, 0))
            {
                return false;
            }
            out.ints.push(*p0 as u8 | (*p1 as u8) << 1);
            for (field, (present, t, v)) in samples.into_iter().enumerate() {
                if present == 1 {
                    time(&mut out.ints, field, t);
                    out.floats[field].push(v);
                }
            }
        }
        _ => return false,
    }
    true
}

/// Decodes `rows` states of `kind` from their compact form into the mapped
/// layout: the states back to back in 8-byte words (each state starting on
/// a word), and their byte offsets (`rows + 1`). `None` when the bytes are
/// damaged.
pub fn decode_states(kind: CompactKind, bytes: &[u8], rows: usize) -> Option<(Vec<u64>, Vec<u32>)> {
    let mut reader = StreamReader::new(bytes)?;
    // Every row takes at least a byte: `rows` past that is damage, and must
    // not size an allocation.
    if rows > bytes.len() {
        return None;
    }
    let mut words: Vec<u64> = Vec::new();
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0u32);
    let offset = |words: &Vec<u64>| u32::try_from(words.len() * 8).ok();
    match kind {
        CompactKind::DDSketch => {
            if rows > 0 {
                let mut dd = DdContents {
                    alpha: reader.u64()?,
                    admission_rate: reader.u64()?,
                    ..DdContents::default()
                };
                let mut previous = (0i32, 0i32);
                let mut view = Vec::new();
                for _ in 0..rows {
                    dd.zero_count = reader.uvarint()?;
                    buckets(&mut reader, &mut previous.0, &mut dd.positive)?;
                    buckets(&mut reader, &mut previous.1, &mut dd.negative)?;
                    (dd.sum, dd.min, dd.max) =
                        (reader.float(0)?, reader.float(1)?, reader.float(2)?);
                    view.clear();
                    dd.view_bytes(&mut view)?;
                    let (view_words, _) = view.as_chunks::<8>();
                    words.extend(view_words.iter().map(|w| u64::from_le_bytes(*w)));
                    offsets.push(offset(&words)?);
                }
            }
        }
        _ => {
            let size = kind.record_bytes().expect("an exact kind");
            words.reserve(rows * size / 8);
            let mut previous_times = [0i64; 2];
            for _ in 0..rows {
                record(kind, &mut reader, &mut previous_times, &mut words)?;
                offsets.push(offset(&words)?);
            }
        }
    }
    reader.finished().then_some((words, offsets))
}

/// One exact record of `kind`, appended to `view` in its mapped layout.
fn record(
    kind: CompactKind,
    reader: &mut StreamReader,
    previous_times: &mut [i64; 2],
    view: &mut Vec<u64>,
) -> Option<()> {
    let size = kind.record_bytes().expect("an exact kind");
    let tag = reader.byte()?;
    if tag == RAW {
        let (raw, _) = reader.raw(size)?.as_chunks::<8>();
        view.extend(raw.iter().map(|w| u64::from_le_bytes(*w)));
        return Some(());
    }
    let mut time = |reader: &mut StreamReader, field: usize| -> Option<u64> {
        let t = previous_times[field].wrapping_add(reader.ivarint()?);
        previous_times[field] = t;
        Some(t as u64)
    };
    let mut words = [0u64; 7];
    let record: &[u64] = match (kind, tag) {
        (CompactKind::Count, 0) => {
            words[0] = reader.uvarint()?;
            &words[..1]
        }
        (CompactKind::Sum, 0..=3) => {
            words[0] = reader.float(0)?;
            words[1] = if tag & 2 != 0 { reader.float(1)? } else { 0 };
            words[2] = u64::from(tag & 1);
            &words[..3]
        }
        (CompactKind::MinMax, 0) => &words[..2],
        (CompactKind::MinMax, 1) => {
            words[..2].copy_from_slice(&[1, reader.float(0)?]);
            &words[..2]
        }
        (CompactKind::Counter, 0) => {
            words[1..].copy_from_slice(&ABSENT_COUNTER);
            &words
        }
        (CompactKind::Counter, 1 | 3) => {
            let start_time = time(reader, 0)?;
            let last_time = time(reader, 1)?;
            let samples = reader.uvarint()?;
            let (start, last) = (reader.float(0)?, reader.float(1)?);
            let total = if tag & 2 != 0 {
                (f64::from_bits(last) - f64::from_bits(start)).to_bits()
            } else {
                reader.float(2)?
            };
            words = [1, start, start_time, last, last_time, total, samples];
            &words
        }
        (CompactKind::LastTwoSamples, 0..=3) => {
            for sample in 0..2 {
                if tag >> sample & 1 == 1 {
                    let t = time(reader, sample)?;
                    words[sample * 3..sample * 3 + 3].copy_from_slice(&[
                        1,
                        t,
                        reader.float(sample)?,
                    ]);
                }
            }
            &words[..6]
        }
        _ => return None,
    };
    view.extend_from_slice(record);
    Some(())
}

/// A column of text keys in compact form: each a signed varint delta from
/// the previous row's.
pub fn encode_text_keys(keys: impl Iterator<Item = u32>) -> Vec<u8> {
    let mut out = Vec::new();
    let mut previous = 0i64;
    for key in keys {
        put_ivarint(&mut out, i64::from(key) - previous);
        previous = i64::from(key);
    }
    out
}

/// `rows` text keys from their compact form.
pub fn decode_text_keys(bytes: &[u8], rows: usize) -> Option<Vec<u32>> {
    if rows > bytes.len() {
        return None;
    }
    let mut keys = Vec::with_capacity(rows);
    let (mut at, mut previous) = (0, 0i64);
    for _ in 0..rows {
        previous = previous.checked_add(ivarint(bytes, &mut at)?)?;
        keys.push(u32::try_from(previous).ok()?);
    }
    (at == bytes.len()).then_some(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_sketchlib::{DdSketch, DdSketchView};

    fn dd_contents(view: &[u8]) -> Option<DdContents> {
        let mut dd = DdContents::default();
        read_dd(view, &mut dd).map(|()| dd)
    }

    fn dd(alpha: f64, samples: &[f64]) -> Vec<u8> {
        let mut sketch = DdSketch::new(alpha);
        for sample in samples {
            sketch.add(sample);
        }
        sketch.to_view_bytes()
    }

    /// `bytes` copied to an 8-byte-aligned buffer, as a view needs.
    fn aligned(bytes: &[u8]) -> Vec<u64> {
        let mut words = vec![0u64; bytes.len().div_ceil(8)];
        for (word, chunk) in words.iter_mut().zip(bytes.chunks(8)) {
            let mut raw = [0u8; 8];
            raw[..chunk.len()].copy_from_slice(chunk);
            *word = u64::from_le_bytes(raw);
        }
        words
    }

    fn view_of(words: &[u64], len: usize) -> DdSketchView<'_> {
        // SAFETY: `words` holds at least `len` initialized bytes.
        let bytes = unsafe { std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), len) };
        DdSketchView::from_bytes(bytes).unwrap()
    }

    /// The states `decode_states` rebuilds, one byte vector each.
    fn round_trip(kind: CompactKind, states: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let compact = encode_states(kind, states.iter().map(Vec::as_slice)).expect("compact");
        let (words, offsets) = decode_states(kind, &compact, states.len()).expect("decodes");
        let bytes: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
        offsets
            .windows(2)
            .map(|pair| bytes[pair[0] as usize..pair[1] as usize].to_vec())
            .collect()
    }

    /// Ten samples per series in a 1 s pane at alpha 0.02 (the demo's
    /// finest DDSketch view): the mapped layout takes 120 bytes a state (an
    /// 88-byte head and a dense run of 1-byte counts, half of them empty),
    /// the compact column 47 before compression.
    #[test]
    fn a_ten_sample_ddsketch_state_takes_47_compact_bytes() {
        let samples: Vec<f64> = (0..10).map(|i| 40.0 + 7.3 * f64::from(i)).collect();
        let view = dd(0.02, &samples);
        assert_eq!(view.len(), 120);
        let rows = 1000;
        let states = vec![view; rows];
        let compact =
            encode_states(CompactKind::DDSketch, states.iter().map(Vec::as_slice)).unwrap();
        // Per row, 23 varint bytes (the zero count, two store headers, the
        // first bucket's delta from the previous row's, 9 gaps, 10 counts)
        // and 24 float bytes (sum, min, max); per column, alpha and the
        // admission rate (16), the stream lengths (9), and one more byte
        // for the first row's first bucket (no previous row).
        assert_eq!(compact.len(), rows * (23 + 24) + 16 + 9 + 1);
    }

    /// Decoding gives the mapped layout's bytes exactly, so every answer
    /// read from a compact column is the answer read from the mapped one:
    /// empty sketches, zeros, negative samples, counts past one byte,
    /// dense and sparse stores.
    #[test]
    fn compact_ddsketch_columns_rebuild_the_view_bytes() {
        let wide: Vec<f64> = (1..=2000).map(|i| 1.0 + f64::from(i % 7)).collect();
        let spread: Vec<f64> = (0..50).map(|i| 1.5f64.powi(i)).collect();
        let dense: Vec<f64> = (0..400).map(|i| 100.0 + f64::from(i) * 0.25).collect();
        let states = vec![
            dd(0.02, &[]),
            dd(0.02, &[0.0, 0.0, 3.0]),
            dd(0.02, &[-5.0, -1.0, 0.0, 2.0, 9.5]),
            dd(0.02, &wide),
            dd(0.02, &spread),
            dd(0.02, &dense),
            dd(0.02, &[-1e300, 1e-300, 7.0]),
        ];
        assert_ne!(states[4][68], states[5][68], "both store forms are covered");
        let back = round_trip(CompactKind::DDSketch, &states);
        assert_eq!(back, states);
        for (a, b) in states.iter().zip(&back) {
            let (wa, wb) = (aligned(a), aligned(b));
            let (va, vb) = (view_of(&wa, a.len()), view_of(&wb, b.len()));
            for q in [0.0, 0.25, 0.5, 0.9, 0.99, 1.0] {
                assert_eq!(va.quantile_interpolated(q), vb.quantile_interpolated(q));
            }
        }
    }

    /// Sketches of different index mappings have no shared column head:
    /// the column keeps its mapped layout.
    #[test]
    fn ddsketches_of_two_alphas_have_no_compact_column() {
        let states = [dd(0.02, &[1.0]), dd(0.05, &[1.0])];
        assert!(encode_states(CompactKind::DDSketch, states.iter().map(Vec::as_slice)).is_none());
    }

    /// Views the writer never writes (keys out of order, non-zero padding)
    /// have no compact form: their column keeps the mapped layout rather
    /// than being rebuilt differently.
    #[test]
    fn non_writer_ddsketch_views_have_no_compact_form() {
        let spread: Vec<f64> = (0..5).map(|i| 1.5f64.powi(i * 3)).collect();
        let view = dd(0.02, &spread);
        assert_eq!(view[68], 1, "a sparse store");
        assert!(dd_contents(&view).is_some());
        let mut swapped = view.clone();
        // Swap the second and third sparse keys.
        let (a, b) = (DD_HEAD_BYTES + 4, DD_HEAD_BYTES + 8);
        let key = swapped[a..a + 4].to_vec();
        swapped.copy_within(b..b + 4, a);
        swapped[b..b + 4].copy_from_slice(&key);
        assert!(dd_contents(&swapped).is_none());
        let mut padding = view.clone();
        *padding.last_mut().unwrap() = 1;
        assert!(dd_contents(&padding).is_none());
    }

    fn words(words: &[u64]) -> Vec<u8> {
        words.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    /// Every exact record kind decodes to the bytes it was encoded from,
    /// including records the executor never writes (kept whole).
    #[test]
    fn compact_exact_columns_rebuild_the_records() {
        let f = f64::to_bits;
        let cases: Vec<(CompactKind, Vec<Vec<u64>>)> = vec![
            (
                CompactKind::Sum,
                vec![
                    vec![f(1.5), 0, 1],
                    vec![f(-2.0), f(1e-17), 1],
                    vec![0, 0, 0],
                    vec![1, 2, 7],
                ],
            ),
            (CompactKind::Count, vec![vec![0], vec![12], vec![u64::MAX]]),
            (
                CompactKind::MinMax,
                vec![vec![0, 0], vec![1, f(-3.25)], vec![0, 5]],
            ),
            (
                CompactKind::Counter,
                vec![
                    vec![1, f(10.0), 1_000, f(25.0), 1_900, f(15.0), 10],
                    // a reset: the increase is not the difference
                    vec![1, f(10.0), 1_000, f(3.0), 1_900, f(13.0), 10],
                    [0].into_iter().chain(ABSENT_COUNTER).collect(),
                    vec![0, 1, 2, 3, 4, 5, 6],
                ],
            ),
            (
                CompactKind::LastTwoSamples,
                vec![
                    vec![1, 900, f(1.0), 1, 1_000, f(2.0)],
                    vec![0, 0, 0, 1, 5, f(1.0)],
                    vec![0, 0, 0, 0, 0, 0],
                    vec![0, 3, 0, 0, 0, 0],
                ],
            ),
        ];
        for (kind, records) in cases {
            let states: Vec<Vec<u8>> = records.iter().map(|r| words(r)).collect();
            assert_eq!(round_trip(kind, &states), states, "{kind:?}");
        }
    }

    /// A counter takes 56 bytes in the mapped layout and 20 in a compact
    /// column: a tag, two timestamp deltas from the previous row's, the
    /// sample count, and the start and last value; the increase is their
    /// difference (no reset), so it is left out.
    #[test]
    fn a_counter_record_takes_20_compact_bytes() {
        let f = f64::to_bits;
        let rows = 1000;
        let states: Vec<Vec<u8>> = (0..rows)
            .map(|row| {
                let start = 10.0 + row as f64;
                words(&[1, f(start), 1_000, f(start + 15.0), 1_900, f(15.0), 10])
            })
            .collect();
        let compact =
            encode_states(CompactKind::Counter, states.iter().map(Vec::as_slice)).unwrap();
        // The first row's timestamps are 2-byte deltas from 0; the stream
        // lengths take 7 bytes.
        assert_eq!(compact.len(), rows * (4 + 16) + 2 + 7);
    }

    /// Damaged bytes are an error, never a panic or a huge allocation.
    #[test]
    fn damaged_compact_columns_are_rejected() {
        let states = [dd(0.02, &[1.0, 2.0, 3.0]), dd(0.02, &[4.0])];
        let compact =
            encode_states(CompactKind::DDSketch, states.iter().map(Vec::as_slice)).unwrap();
        for cut in 0..compact.len() {
            assert!(decode_states(CompactKind::DDSketch, &compact[..cut], 2).is_none());
        }
        assert!(decode_states(CompactKind::DDSketch, &compact, 3).is_none());
        let mut huge = Vec::new();
        put_uvarint(&mut huge, u64::MAX / 2);
        assert!(decode_states(CompactKind::Counter, &huge, 1).is_none());
        assert!(decode_text_keys(&[0xff; 3], 2).is_none());
    }

    /// Text keys round-trip; sorted series keys take one byte each.
    #[test]
    fn text_keys_round_trip() {
        let keys: Vec<u32> = vec![0, 1, 2, 7, 3, u32::MAX, 0];
        let compact = encode_text_keys(keys.iter().copied());
        assert_eq!(decode_text_keys(&compact, keys.len()).unwrap(), keys);
        assert_eq!(encode_text_keys(0..1000).len(), 1000);
    }
}
