//! Prometheus Remote Write v1 decoding and validation.
//!
//! [`Decoder`] walks a request body into samples without building a
//! [`WriteRequest`]. It knows no engine type: the caller interns each new
//! series into its own series handle (any `Copy` value, such as an
//! interned `&'static` series) and builds its own sample type from the
//! handle, timestamp and value, so samples are decoded straight into the
//! engine's type with no copy.

use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// Request size limits; a request over any limit is rejected whole.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_samples: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_compressed_bytes: 32 * 1024 * 1024,
            max_decompressed_bytes: 128 * 1024 * 1024,
            max_samples: 1_000_000,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RemoteWriteError {
    #[error("compressed request exceeds {0} bytes")]
    CompressedTooLarge(usize),
    #[error("decompressed request exceeds {0} bytes")]
    DecompressedTooLarge(usize),
    #[error("snappy decompression failed: {0}")]
    Snappy(String),
    #[error("protobuf decoding failed: {0}")]
    Protobuf(String),
    #[error("request has more than {0} samples")]
    TooManySamples(usize),
    #[error("series without a __name__ label")]
    MissingMetricName,
    #[error("series has duplicate label {0:?}")]
    DuplicateLabel(String),
}

/// A decoder's state, kept between requests (one per thread): its
/// decompression buffer and its series cache, which maps the encoded
/// labels of each series seen to the caller's series handle `S`.
pub struct Decoder<S> {
    /// A request's decompressed bytes are written over the last one's,
    /// with no allocation, zeroing or page faults once it has grown.
    decompressed: Vec<u8>,
    cache: SeriesCache<S>,
}

impl<S: Copy> Default for Decoder<S> {
    fn default() -> Self {
        Self {
            decompressed: Vec::new(),
            cache: SeriesCache::default(),
        }
    }
}

impl<S: Copy> Decoder<S> {
    /// The samples of a snappy-compressed Remote Write v1 body, in request
    /// order, each built by `make_sample` from its series' handle, timestamp
    /// (ms) and value. Stale markers are kept: the executor's sample
    /// selector drops them from range windows and ends instant selectors.
    ///
    /// The body is decompressed into this decoder's buffer, and the
    /// protobuf is walked in place, without building a `WriteRequest`. A
    /// series is looked up by the bytes of its encoded labels in this
    /// decoder's cache, so a known series costs a compare (and a hash when
    /// the sender changed its order); only a new one is parsed, validated
    /// (UTF-8, `__name__`, no duplicate label) and passed to `intern` as
    /// (name, value) pairs in request order, which returns its handle.
    /// Errors: the body is too large, not snappy or not protobuf, has too
    /// many samples, or a series has no `__name__` or a label twice.
    pub fn decode<T>(
        &mut self,
        body: &[u8],
        limits: &Limits,
        mut intern: impl FnMut(&[(&str, &str)]) -> S,
        make_sample: impl Fn(S, i64, f64) -> T,
    ) -> Result<Vec<T>, RemoteWriteError> {
        let Self {
            decompressed: buffer,
            cache,
        } = self;
        let decoded = (|| {
            let decoded = decompress_into(body, limits, buffer)?;
            // Room for one sample per 40 encoded bytes: a sender's one-sample
            // series take 60 to 150, so this grows only for tiny label sets.
            let mut samples = Vec::with_capacity(decoded.len() / 40);
            let mut request = Reader::new(decoded);
            while let Some((field, value)) = request.field()? {
                if let (1, Value::Bytes(series)) = (field, value) {
                    decode_series(
                        series,
                        cache,
                        &mut intern,
                        &mut samples,
                        &make_sample,
                        limits,
                    )?;
                }
            }
            Ok(samples)
        })();
        // An unusually large request's buffer is not kept, whether or not the
        // request decoded: a short body claiming a large size and then failing
        // to decompress must not leave the decoder holding that buffer.
        if buffer.len() > KEPT_DECOMPRESSED_BYTES {
            *buffer = Vec::new();
        }
        decoded
    }
}

/// The largest decompression buffer a decoder keeps between requests.
const KEPT_DECOMPRESSED_BYTES: usize = 32 * 1024 * 1024;

/// The snappy-decompressed bytes of `body`, within the size limits,
/// written into `buffer` (grown as needed).
fn decompress_into<'a>(
    body: &[u8],
    limits: &Limits,
    buffer: &'a mut Vec<u8>,
) -> Result<&'a [u8], RemoteWriteError> {
    if body.len() > limits.max_compressed_bytes {
        return Err(RemoteWriteError::CompressedTooLarge(
            limits.max_compressed_bytes,
        ));
    }
    let len =
        snap::raw::decompress_len(body).map_err(|e| RemoteWriteError::Snappy(e.to_string()))?;
    if len > limits.max_decompressed_bytes {
        return Err(RemoteWriteError::DecompressedTooLarge(
            limits.max_decompressed_bytes,
        ));
    }
    if len > KEPT_DECOMPRESSED_BYTES {
        // Not kept after this request (see `Decoder::decode`): a fresh zeroed
        // allocation, whose pages are only touched as snappy writes them,
        // rather than growing and zeroing the kept buffer.
        *buffer = vec![0; len];
    } else if buffer.len() < len {
        buffer.resize(len, 0);
    }
    let written = snap::raw::Decoder::new()
        .decompress(body, buffer)
        .map_err(|e| RemoteWriteError::Snappy(e.to_string()))?;
    Ok(&buffer[..written])
}

/// Appends the samples of one encoded `TimeSeries` to `samples`.
fn decode_series<S: Copy, T>(
    series: &[u8],
    cache: &mut SeriesCache<S>,
    intern: &mut impl FnMut(&[(&str, &str)]) -> S,
    samples: &mut Vec<T>,
    make_sample: &impl Fn(S, i64, f64) -> T,
    limits: &Limits,
) -> Result<(), RemoteWriteError> {
    // First pass, over every field: where the labels' encoded bytes are,
    // the key of the series cache; one slice of `series` when the labels
    // are contiguous, as senders write them.
    let mut labels_start = None;
    let mut labels_end = 0;
    let mut contiguous = true;
    let mut reader = Reader::new(series);
    // Senders write the labels first: skip over them by their one-byte
    // tag and short length, leaving anything else to the general walk.
    while let Some(end) = short_label_end(series, reader.position) {
        labels_start = Some(0);
        reader.position = end;
        labels_end = end;
    }
    while let Some((field, value)) = reader.field()? {
        if let (1, Value::Bytes(_)) = (field, value) {
            match labels_start {
                None => labels_start = Some(reader.field_start),
                Some(_) if reader.field_start != labels_end => contiguous = false,
                Some(_) => {}
            }
            labels_end = reader.position;
        }
    }
    let labels = match (labels_start, contiguous) {
        (Some(start), true) => {
            let labels = cache.get_or_intern(&series[start..labels_end], intern)?;
            // Second pass: the samples, in the fields before and after the
            // labels (both whole fields, as the first pass found).
            push_samples(&series[..start], labels, samples, make_sample, limits)?;
            return push_samples(&series[labels_end..], labels, samples, make_sample, limits);
        }
        (None, _) => return Err(RemoteWriteError::MissingMetricName),
        (Some(_), false) => {
            // Rare: labels interleaved with other fields; key by their
            // concatenation.
            let mut key = Vec::new();
            let mut reader = Reader::new(series);
            while let Some((field, value)) = reader.field()? {
                if let (1, Value::Bytes(_)) = (field, value) {
                    key.extend_from_slice(&series[reader.field_start..reader.position]);
                }
            }
            cache.get_or_intern(&key, intern)?
        }
    };
    push_samples(series, labels, samples, make_sample, limits)
}

/// The end of the `Label` field (tag `0x0a`) at `start` of `series`, when
/// its length takes one or two bytes and the field ends within `series`;
/// `None` otherwise, for the general walk.
#[inline(always)]
fn short_label_end(series: &[u8], start: usize) -> Option<usize> {
    let (&tag, rest) = series.get(start..)?.split_first()?;
    if tag != 0x0a {
        return None;
    }
    let (len, len_bytes) = match *rest {
        [low, ..] if low < 0x80 => (usize::from(low), 1),
        [low, high, ..] if high < 0x80 => (usize::from(low & 0x7f) | usize::from(high) << 7, 2),
        _ => return None,
    };
    let end = start + 1 + len_bytes + len;
    (end <= series.len()).then_some(end)
}

/// Appends the `Sample`s (field 2) of the encoded `TimeSeries` fields
/// `fields` to `samples`, each with series `labels`.
#[inline]
fn push_samples<S: Copy, T>(
    fields: &[u8],
    labels: S,
    samples: &mut Vec<T>,
    make_sample: &impl Fn(S, i64, f64) -> T,
    limits: &Limits,
) -> Result<(), RemoteWriteError> {
    let mut reader = Reader::new(fields);
    while let Some((field, value)) = reader.field()? {
        let (2, Value::Bytes(sample)) = (field, value) else {
            continue;
        };
        if samples.len() >= limits.max_samples {
            return Err(RemoteWriteError::TooManySamples(limits.max_samples));
        }
        let (timestamp_ms, value) = match sample_fast(sample) {
            Some(decoded) => decoded,
            None => sample_generic(sample)?,
        };
        samples.push(make_sample(labels, timestamp_ms, value));
    }
    Ok(())
}

/// A `Sample` in the layout senders write: `value` (tag 0x09, 8 bytes) then
/// `timestamp` (tag 0x10, varint) and nothing else; `None` for any other
/// layout, which [`sample_generic`] reads.
fn sample_fast(sample: &[u8]) -> Option<(i64, f64)> {
    if sample.len() < 11 || sample[0] != 0x09 || sample[9] != 0x10 {
        return None;
    }
    // The timestamp's varint, ending at the end of the sample.
    let tail = &sample[10..];
    if tail.len() > 10 {
        return None;
    }
    let mut raw = 0u64;
    for (i, &byte) in tail.iter().enumerate() {
        raw |= u64::from(byte & 0x7f) << (7 * i);
        if (byte < 0x80) != (i + 1 == tail.len()) {
            return None;
        }
    }
    let value = f64::from_bits(u64::from_le_bytes(sample[1..9].try_into().ok()?));
    Some((raw as i64, value))
}

/// A `Sample` with its fields in any order, unknown fields skipped.
fn sample_generic(sample: &[u8]) -> Result<(i64, f64), RemoteWriteError> {
    let (mut timestamp_ms, mut value) = (0i64, 0f64);
    let mut fields = Reader::new(sample);
    while let Some((field, field_value)) = fields.field()? {
        match (field, field_value) {
            (1, Value::Fixed64(bits)) => value = f64::from_bits(bits),
            (2, Value::Varint(raw)) => timestamp_ms = raw as i64,
            _ => {}
        }
    }
    Ok((timestamp_ms, value))
}

/// A decoder's series by the bytes of their encoded labels. A sender
/// writes its series in the same order request after request, so each
/// entry remembers the entry looked up after it last time, and a lookup
/// first compares the key with that prediction: a hit costs one compare,
/// no hash. Every hit is a byte-for-byte match of the key.
struct SeriesCache<S> {
    entries: Vec<CacheEntry<S>>,
    /// Entries by the hash of their key.
    by_key: hashbrown::HashTable<u32>,
    /// The entry looked up last.
    last: Option<u32>,
}

impl<S> Default for SeriesCache<S> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            by_key: hashbrown::HashTable::new(),
            last: None,
        }
    }
}

struct CacheEntry<S> {
    key: Box<[u8]>,
    series: S,
    /// The entry looked up right after this one, last time.
    next: Option<u32>,
}

impl<S: Copy> SeriesCache<S> {
    /// The series of the encoded labels `key` (a run of `TimeSeries`
    /// field-1 entries): from the cache, or parsed, validated and interned
    /// with `intern`.
    fn get_or_intern(
        &mut self,
        key: &[u8],
        intern: impl FnOnce(&[(&str, &str)]) -> S,
    ) -> Result<S, RemoteWriteError> {
        if let Some(series) = self.get(key) {
            return Ok(series);
        }
        let series = parse_labels(key, intern)?;
        self.insert(key, series);
        Ok(series)
    }

    fn hash(key: &[u8]) -> u64 {
        static STATE: std::sync::OnceLock<ahash::RandomState> = std::sync::OnceLock::new();
        STATE.get_or_init(ahash::RandomState::new).hash_one(key)
    }

    #[inline]
    fn get(&mut self, key: &[u8]) -> Option<S> {
        let predicted = self.last.and_then(|last| self.entries[last as usize].next);
        if let Some(index) = predicted {
            let entry = &self.entries[index as usize];
            if *entry.key == *key {
                self.last = Some(index);
                return Some(entry.series);
            }
        }
        let entries = &self.entries;
        let index = *self
            .by_key
            .find(Self::hash(key), |&i| *entries[i as usize].key == *key)?;
        self.follow(index);
        Some(self.entries[index as usize].series)
    }

    fn insert(&mut self, key: &[u8], series: S) {
        let index = u32::try_from(self.entries.len()).expect("fewer than 2^32 series");
        self.entries.push(CacheEntry {
            key: key.into(),
            series,
            next: None,
        });
        let entries = &self.entries;
        self.by_key.insert_unique(Self::hash(key), index, |&i| {
            Self::hash(&entries[i as usize].key)
        });
        self.follow(index);
    }

    /// Records that `index` was looked up after the last entry.
    fn follow(&mut self, index: u32) {
        if let Some(last) = self.last {
            self.entries[last as usize].next = Some(index);
        }
        self.last = Some(index);
    }
}

/// Parses and validates the labels encoded in `key`, and interns them
/// with `intern`.
fn parse_labels<S>(
    key: &[u8],
    intern: impl FnOnce(&[(&str, &str)]) -> S,
) -> Result<S, RemoteWriteError> {
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    let mut reader = Reader::new(key);
    while let Some((_, value)) = reader.field()? {
        let Value::Bytes(label) = value else { continue };
        let (mut name, mut text) = ("", "");
        let mut fields = Reader::new(label);
        let utf8 = |bytes| {
            std::str::from_utf8(bytes)
                .map_err(|e| RemoteWriteError::Protobuf(format!("label is not UTF-8: {e}")))
        };
        while let Some((field, field_value)) = fields.field()? {
            match (field, field_value) {
                (1, Value::Bytes(bytes)) => name = utf8(bytes)?,
                (2, Value::Bytes(bytes)) => text = utf8(bytes)?,
                _ => {}
            }
        }
        pairs.push((name, text));
    }
    let mut names: Vec<&str> = pairs.iter().map(|(name, _)| *name).collect();
    names.sort_unstable();
    if let Some(pair) = names.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(RemoteWriteError::DuplicateLabel(pair[0].to_string()));
    }
    if !names.contains(&"__name__") {
        return Err(RemoteWriteError::MissingMetricName);
    }
    Ok(intern(&pairs))
}

/// One protobuf field value.
enum Value<'a> {
    Varint(u64),
    Fixed64(u64),
    Bytes(&'a [u8]),
    Fixed32,
}

/// A protobuf reader over borrowed bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
    /// Where the field read last starts (its tag).
    field_start: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            field_start: 0,
        }
    }

    #[cold]
    fn error(message: &str) -> RemoteWriteError {
        RemoteWriteError::Protobuf(message.into())
    }

    #[inline(always)]
    fn varint(&mut self) -> Result<u64, RemoteWriteError> {
        // One byte: most tags and lengths.
        if let Some(&byte) = self.bytes.get(self.position) {
            if byte < 0x80 {
                self.position += 1;
                return Ok(u64::from(byte));
            }
        }
        self.varint_slow()
    }

    #[inline(never)]
    fn varint_slow(&mut self) -> Result<u64, RemoteWriteError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self
                .bytes
                .get(self.position)
                .ok_or_else(|| Self::error("truncated varint"))?;
            self.position += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return Ok(value);
            }
        }
        Err(Self::error("varint too long"))
    }

    #[inline(always)]
    fn take(&mut self, len: usize) -> Result<&'a [u8], RemoteWriteError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| Self::error("field runs past the end of its message"))?;
        let taken = &self.bytes[self.position..end];
        self.position = end;
        Ok(taken)
    }

    /// The next field number and value; `None` at the end.
    #[inline(always)]
    fn field(&mut self) -> Result<Option<(u64, Value<'a>)>, RemoteWriteError> {
        if self.position == self.bytes.len() {
            return Ok(None);
        }
        self.field_start = self.position;
        let tag = self.varint()?;
        let field = tag >> 3;
        if field == 0 {
            return Err(Self::error("field number 0"));
        }
        let value = match tag & 7 {
            0 => Value::Varint(self.varint()?),
            1 => Value::Fixed64(u64::from_le_bytes(
                self.take(8)?.try_into().expect("8 bytes"),
            )),
            2 => {
                let len =
                    usize::try_from(self.varint()?).map_err(|_| Self::error("length too large"))?;
                Value::Bytes(self.take(len)?)
            }
            5 => {
                self.take(4)?;
                Value::Fixed32
            }
            wire => return Err(Self::error(&format!("unsupported wire type {wire}"))),
        };
        Ok(Some((field, value)))
    }
}

/// Snappy-compressed encoding of `request`, as a Remote Write sender sends it.
pub fn encode(request: &WriteRequest) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy compresses any input")
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_large_body_that_fails_to_decompress_leaves_no_buffer_behind() {
        // A 6-byte body: a snappy header claiming 120 MiB, then a literal cut short.
        let claimed = 120usize << 20;
        let mut body = Vec::new();
        let mut n = claimed;
        while n >= 0x80 {
            body.push((n as u8) | 0x80);
            n >>= 7;
        }
        body.push(n as u8);
        body.extend_from_slice(&[0xf0, 0x00]);
        let limits = super::Limits::default();
        assert!(claimed <= limits.max_decompressed_bytes);
        let mut decoder = Labeled::default();
        assert!(matches!(
            decoder.decode(&body, &limits),
            Err(super::RemoteWriteError::Snappy(_))
        ));
        assert!(decoder.decoder.decompressed.capacity() <= super::KEPT_DECOMPRESSED_BYTES);
    }

    use super::*;
    use std::collections::BTreeMap;

    type Labels = BTreeMap<String, String>;

    /// A decoder whose series handle is an index into `series`, the labels
    /// of each series it interned, in order.
    #[derive(Default)]
    struct Labeled {
        decoder: Decoder<usize>,
        series: Vec<Labels>,
    }

    impl Labeled {
        /// `body`'s samples: each one's labels, timestamp and value bits.
        fn decode(
            &mut self,
            body: &[u8],
            limits: &Limits,
        ) -> Result<Vec<(Labels, i64, u64)>, RemoteWriteError> {
            let series = &mut self.series;
            let intern = |pairs: &[(&str, &str)]| {
                series.push(
                    pairs
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect(),
                );
                series.len() - 1
            };
            let samples =
                self.decoder
                    .decode(body, limits, intern, |series, timestamp_ms, value| {
                        (series, timestamp_ms, value)
                    })?;
            Ok(samples
                .into_iter()
                .map(|(series, timestamp_ms, value)| {
                    (self.series[series].clone(), timestamp_ms, value.to_bits())
                })
                .collect())
        }
    }

    /// Decodes a snappy-compressed Remote Write v1 body into a
    /// `WriteRequest`, as [`Decoder::decode`] validates it: every series
    /// must name its metric and carry each label once.
    fn decode(body: &[u8], limits: &Limits) -> Result<WriteRequest, RemoteWriteError> {
        let mut buffer = Vec::new();
        let decoded = decompress_into(body, limits, &mut buffer)?;
        let request =
            WriteRequest::decode(decoded).map_err(|e| RemoteWriteError::Protobuf(e.to_string()))?;
        let samples: usize = request.timeseries.iter().map(|s| s.samples.len()).sum();
        if samples > limits.max_samples {
            return Err(RemoteWriteError::TooManySamples(limits.max_samples));
        }
        for series in &request.timeseries {
            let mut names: Vec<&str> = series.labels.iter().map(|l| l.name.as_str()).collect();
            names.sort_unstable();
            if let Some(pair) = names.windows(2).find(|pair| pair[0] == pair[1]) {
                return Err(RemoteWriteError::DuplicateLabel(pair[0].to_string()));
            }
            if !names.contains(&"__name__") {
                return Err(RemoteWriteError::MissingMetricName);
            }
        }
        Ok(request)
    }

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
        TimeSeries {
            labels: labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            samples: samples
                .iter()
                .map(|&(timestamp, value)| Sample { value, timestamp })
                .collect(),
        }
    }

    /// A well-formed request decodes to the series and samples it was built from.
    #[test]
    fn decodes_what_a_sender_encodes() {
        let request = WriteRequest {
            timeseries: vec![series(
                &[("__name__", "http_requests_total"), ("job", "api")],
                &[(1_000, 1.0), (16_000, 3.0)],
            )],
        };
        assert_eq!(decode(&encode(&request), &Limits::default()), Ok(request));
    }

    /// Bodies that are not snappy or not Remote Write protobuf are rejected.
    #[test]
    fn rejects_malformed_bodies() {
        let limits = Limits::default();
        assert!(matches!(
            decode(b"not snappy", &limits),
            Err(RemoteWriteError::Snappy(_))
        ));
        let garbage = snap::raw::Encoder::new().compress_vec(&[0xff; 8]).unwrap();
        assert!(matches!(
            decode(&garbage, &limits),
            Err(RemoteWriteError::Protobuf(_))
        ));
    }

    /// Series must name their metric and may not repeat a label.
    #[test]
    fn rejects_invalid_series() {
        let limits = Limits::default();
        let unnamed = WriteRequest {
            timeseries: vec![series(&[("job", "api")], &[(1, 1.0)])],
        };
        assert_eq!(
            decode(&encode(&unnamed), &limits),
            Err(RemoteWriteError::MissingMetricName)
        );
        let duplicate = WriteRequest {
            timeseries: vec![series(
                &[("__name__", "x"), ("job", "a"), ("job", "b")],
                &[(1, 1.0)],
            )],
        };
        assert_eq!(
            decode(&encode(&duplicate), &limits),
            Err(RemoteWriteError::DuplicateLabel("job".into()))
        );
    }

    /// Walking the protobuf in place gives the samples and series of the
    /// full decode, for both request shapes and labels in any order; and
    /// the same errors for invalid series and malformed bodies.
    #[test]
    fn decoded_samples_match_the_full_decode() {
        let limits = Limits::default();
        let request = WriteRequest {
            timeseries: vec![
                series(
                    &[("__name__", "m_rw_test"), ("job", "a")],
                    &[(1, 1.0), (2, 2.5)],
                ),
                series(&[("job", "b"), ("__name__", "m_rw_test")], &[(3, -1.0)]),
                series(&[("__name__", "m_rw_test"), ("job", "a")], &[(4, f64::NAN)]),
                series(&[("__name__", "m_rw_test"), ("job", "c")], &[]),
            ],
        };
        let body = encode(&request);
        let mut decoder = Labeled::default();
        for _ in 0..2 {
            let got = decoder.decode(&body, &limits).unwrap();
            let expected: Vec<(BTreeMap<String, String>, i64, u64)> = request
                .timeseries
                .iter()
                .flat_map(|s| {
                    let labels: BTreeMap<String, String> = s
                        .labels
                        .iter()
                        .map(|l| (l.name.clone(), l.value.clone()))
                        .collect();
                    s.samples
                        .iter()
                        .map(move |x| (labels.clone(), x.timestamp, x.value.to_bits()))
                })
                .collect();
            assert_eq!(got, expected);
        }
        // Each series interned once: the second request, and the repeated
        // series in the first, are cache hits.
        assert_eq!(decoder.series.len(), 3);
        let unnamed = WriteRequest {
            timeseries: vec![series(&[("job", "api")], &[(1, 1.0)])],
        };
        assert_eq!(
            decoder.decode(&encode(&unnamed), &limits).err(),
            Some(RemoteWriteError::MissingMetricName)
        );
        let duplicate = WriteRequest {
            timeseries: vec![series(
                &[("__name__", "y"), ("job", "a"), ("job", "b")],
                &[(1, 1.0)],
            )],
        };
        assert_eq!(
            decoder.decode(&encode(&duplicate), &limits).err(),
            Some(RemoteWriteError::DuplicateLabel("job".into()))
        );
        let garbage = snap::raw::Encoder::new().compress_vec(&[0xff; 8]).unwrap();
        assert!(matches!(
            decoder.decode(&garbage, &limits),
            Err(RemoteWriteError::Protobuf(_))
        ));
        let one = Limits {
            max_samples: 1,
            ..Limits::default()
        };
        assert_eq!(
            decoder.decode(&body, &one).err(),
            Some(RemoteWriteError::TooManySamples(1))
        );
    }

    /// The fast sample path reads what the generic one reads, and leaves
    /// any other layout (fields swapped, an unknown field, a value-less or
    /// negative-timestamp sample) to it.
    #[test]
    fn fast_and_generic_sample_paths_agree() {
        let encoded = |sample: &Sample| sample.encode_to_vec();
        for sample in [
            Sample {
                value: 1.5,
                timestamp: 1_700_000_000_000,
            },
            Sample {
                value: f64::NAN,
                timestamp: 1,
            },
            Sample {
                value: -2.0,
                timestamp: -5,
            },
        ] {
            let bytes = encoded(&sample);
            let generic = sample_generic(&bytes).unwrap();
            let fast = sample_fast(&bytes).expect("the sender's layout");
            assert_eq!(fast.0, generic.0);
            assert_eq!(fast.1.to_bits(), generic.1.to_bits());
        }
        // Timestamp before value, an unknown field after, a zero value
        // (omitted by protobuf): read by the generic path only.
        let swapped = [0x10, 0x07, 0x09, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f];
        assert_eq!(sample_fast(&swapped), None);
        assert_eq!(sample_generic(&swapped).unwrap(), (7, 1.5));
        let mut extra = encoded(&Sample {
            value: 1.5,
            timestamp: 7,
        });
        extra.extend_from_slice(&[0x18, 0x01]);
        assert_eq!(sample_fast(&extra), None);
        assert_eq!(sample_generic(&extra).unwrap(), (7, 1.5));
        let zero = encoded(&Sample {
            value: 0.0,
            timestamp: 7,
        });
        assert_eq!(sample_fast(&zero), None);
        assert_eq!(sample_generic(&zero).unwrap(), (7, 0.0));
    }

    /// Requests over a size limit are rejected before they are processed.
    #[test]
    fn enforces_limits() {
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "x")], &[(1, 1.0), (2, 2.0)])],
        };
        let limits = Limits {
            max_samples: 1,
            ..Limits::default()
        };
        assert_eq!(
            decode(&encode(&request), &limits),
            Err(RemoteWriteError::TooManySamples(1))
        );
        let limits = Limits {
            max_compressed_bytes: 1,
            ..Limits::default()
        };
        assert_eq!(
            decode(&encode(&request), &limits),
            Err(RemoteWriteError::CompressedTooLarge(1))
        );
    }
}
