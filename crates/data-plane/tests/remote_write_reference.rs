//! The Remote Write decoder gives, for every request, exactly what the
//! decoder before its speed-ups gave (kept here, as it was, as the
//! reference): the same samples in the same order, each with the same
//! series, values bit for bit (stale markers included), and the same error
//! for a malformed body. Checked on generated requests (label orders,
//! labels interleaved with samples, series appearing mid-stream, several
//! samples per series, unknown fields), on every truncation and on
//! byte flips of them, and, with `RW_CORPUS_DIR` set, on a directory of
//! captured request bodies.

use data_plane::remote_write::decode_samples;
use prometheus_remote_write::{Limits, RemoteWriteError};

/// The decoder before its speed-ups, verbatim but for returning the
/// interned `&'static Series`.
mod reference {
    use prometheus_remote_write::{Limits, RemoteWriteError};
    use storage::series_ids::Series;
    use storage::store::RawSample;

    fn decompress(body: &[u8], limits: &Limits) -> Result<Vec<u8>, RemoteWriteError> {
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
        snap::raw::Decoder::new()
            .decompress_vec(body)
            .map_err(|e| RemoteWriteError::Snappy(e.to_string()))
    }

    pub fn decode_samples(
        body: &[u8],
        limits: &Limits,
    ) -> Result<Vec<RawSample>, RemoteWriteError> {
        let decoded = decompress(body, limits)?;
        // A sample takes at least 11 encoded bytes (tag, length, value,
        // timestamp), so this rarely grows.
        let mut samples = Vec::with_capacity(decoded.len() / 12);
        let mut request = Reader::new(&decoded);
        while let Some((field, value)) = request.field()? {
            if let (1, Value::Bytes(series)) = (field, value) {
                decode_series(series, &mut samples, limits)?;
            }
        }
        Ok(samples)
    }

    /// Appends the samples of one encoded `TimeSeries` to `samples`.
    fn decode_series(
        series: &[u8],
        samples: &mut Vec<RawSample>,
        limits: &Limits,
    ) -> Result<(), RemoteWriteError> {
        // First pass, skipping over the samples: the labels' encoded bytes, the
        // key of the series cache; one slice of `series` when the labels are
        // contiguous, as senders write them.
        let mut labels_start = None;
        let mut labels_end = 0;
        let mut contiguous = true;
        let mut reader = Reader::new(series);
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
            (Some(start), true) => cached_series(&series[start..labels_end])?,
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
                cached_series(&key)?
            }
        };
        // Second pass: the samples.
        let mut reader = Reader::new(series);
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
            samples.push(RawSample {
                labels,
                timestamp_ms,
                value,
            });
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
        let mut tail = Reader::new(&sample[10..]);
        let raw = tail.varint().ok()?;
        if tail.position != tail.bytes.len() {
            return None;
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

    /// The series of the encoded labels `key` (a run of `TimeSeries` field-1
    /// entries): from this thread's cache, or parsed, validated and interned.
    fn cached_series(key: &[u8]) -> Result<&'static Series, RemoteWriteError> {
        thread_local! {
            static CACHE: std::cell::RefCell<ahash::HashMap<Box<[u8]>, &'static Series>> =
                std::cell::RefCell::default();
        }
        if let Some(series) = CACHE.with(|cache| cache.borrow().get(key).copied()) {
            return Ok(series);
        }
        let series = parse_labels(key)?;
        CACHE.with(|cache| cache.borrow_mut().insert(key.into(), series));
        Ok(series)
    }

    /// Parses, validates and interns the labels encoded in `key`.
    fn parse_labels(key: &[u8]) -> Result<&'static Series, RemoteWriteError> {
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
        Ok(storage::series_ids::intern_pairs(&pairs))
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

        fn error(message: &str) -> RemoteWriteError {
            RemoteWriteError::Protobuf(message.into())
        }

        fn varint(&mut self) -> Result<u64, RemoteWriteError> {
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
                    let len = usize::try_from(self.varint()?)
                        .map_err(|_| Self::error("length too large"))?;
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
}

/// Each decoded sample as (series id, time, value bits), or the error.
type Decoded = Result<Vec<(u32, i64, u64)>, RemoteWriteError>;

fn decoded(samples: Result<Vec<storage::store::RawSample>, RemoteWriteError>) -> Decoded {
    samples.map(|samples| {
        samples
            .iter()
            .map(|s| (s.labels.id, s.timestamp_ms, s.value.to_bits()))
            .collect()
    })
}

/// Decodes `body` with both decoders; panics where they differ.
fn assert_same(body: &[u8], limits: &Limits) -> Decoded {
    let new = decoded(decode_samples(body, limits));
    let old = decoded(reference::decode_samples(body, limits));
    assert_eq!(new, old, "body {body:02x?}");
    new
}

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn bytes_field(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    varint(out, field << 3 | 2);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn label(name: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    bytes_field(&mut out, 1, name);
    bytes_field(&mut out, 2, value);
    out
}

/// A `Sample` as protobuf encodes it (a zero value or time left out), or,
/// with `swapped`, with its time first.
fn sample(value: f64, timestamp: i64, swapped: bool) -> Vec<u8> {
    let mut value_field = Vec::new();
    if value.to_bits() != 0 {
        value_field.push(0x09);
        value_field.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    let mut time_field = Vec::new();
    if timestamp != 0 {
        time_field.push(0x10);
        varint(&mut time_field, timestamp as u64);
    }
    if swapped {
        [time_field, value_field].concat()
    } else {
        [value_field, time_field].concat()
    }
}

/// The label sets the generated requests draw from: grows as requests
/// go, so series appear mid-stream.
fn label_set(rng: &mut Rng, index: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut labels = vec![(
        b"__name__".to_vec(),
        format!("m_rw_reference_{}", index % 7).into_bytes(),
    )];
    for (i, name) in ["instance", "job", "le", "region", "service", "z"]
        .iter()
        .enumerate()
    {
        if (index >> i) & 1 == 1 || i == 0 {
            let value = match rng.below(4) {
                0 => format!("{index}.{i}"),
                1 => format!("värde-{index}"),
                // Long values take a two- or three-byte length.
                2 => "x".repeat(100 + index % 300),
                _ => "y".repeat(16_380 + index % 10),
            };
            labels.push((name.as_bytes().to_vec(), value.into_bytes()));
        }
    }
    labels
}

/// One `TimeSeries`: its labels in a random order, 0 to 3 samples, at
/// times an unknown field, labels and samples sometimes interleaved.
fn time_series(rng: &mut Rng, labels: &[(Vec<u8>, Vec<u8>)], time: i64) -> Vec<u8> {
    let mut labels: Vec<Vec<u8>> = labels.iter().map(|(n, v)| label(n, v)).collect();
    match rng.below(3) {
        0 => {}
        1 => labels.reverse(),
        _ => {
            for i in (1..labels.len()).rev() {
                labels.swap(i, rng.below(i + 1));
            }
        }
    }
    let values = [
        1.5,
        0.0,
        -0.0,
        f64::NAN,
        // The Prometheus stale marker.
        f64::from_bits(0x7ff0_0000_0000_0002),
        1e300,
        -7.25,
    ];
    let samples: Vec<Vec<u8>> = (0..rng.below(4))
        .map(|i| {
            let value = values[rng.below(values.len())];
            let timestamp = match rng.below(5) {
                0 => 0,
                1 => -5 - i as i64,
                _ => time + i as i64,
            };
            sample(value, timestamp, rng.chance(10))
        })
        .collect();
    let mut fields: Vec<(u64, Vec<u8>)> = labels.into_iter().map(|l| (1, l)).collect();
    if rng.chance(15) {
        // Interleaved: samples among the labels.
        for s in samples {
            let at = rng.below(fields.len() + 1);
            fields.insert(at, (2, s));
        }
    } else if rng.chance(10) {
        // Samples first.
        for s in samples.into_iter().rev() {
            fields.insert(0, (2, s));
        }
    } else {
        fields.extend(samples.into_iter().map(|s| (2, s)));
    }
    let mut out = Vec::new();
    for (field, bytes) in fields {
        bytes_field(&mut out, field, &bytes);
    }
    if rng.chance(5) {
        // Unknown fields: a varint and a fixed32.
        out.extend_from_slice(&[0x18, 0x05, 0x25, 1, 2, 3, 4]);
    }
    out
}

/// The protobuf of a request of `series` time series drawn from the first
/// `known` label sets.
fn request(rng: &mut Rng, known: usize, series: usize, time: i64) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..series {
        let index = rng.below(known);
        let labels = label_set(&mut Rng(index as u64 * 7919 + 1), index);
        bytes_field(&mut out, 1, &time_series(rng, &labels, time));
        if rng.chance(2) {
            // Unknown top-level field (metadata).
            bytes_field(&mut out, 3, b"metadata");
        }
    }
    out
}

fn compress(protobuf: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(protobuf).unwrap()
}

/// Generated requests, with series appearing mid-stream, decode the same.
#[test]
fn generated_requests_decode_as_before() {
    let mut rng = Rng(0x5eed);
    let limits = Limits::default();
    let mut samples = 0;
    for r in 0..300 {
        let known = 4 + r / 2;
        let series = 1 + rng.below(40);
        let body = compress(&request(&mut rng, known, series, 1_000 * r as i64));
        samples += assert_same(&body, &limits).expect("a valid request").len();
    }
    assert!(samples > 5_000);
}

/// Invalid series (no `__name__`, a repeated label, a label that is not
/// UTF-8) give the same error, as do requests over the sample limit.
#[test]
fn invalid_requests_fail_as_before() {
    let limits = Limits::default();
    let one = |labels: &[(&[u8], &[u8])]| {
        let mut series = Vec::new();
        for (name, value) in labels {
            bytes_field(&mut series, 1, &label(name, value));
        }
        bytes_field(&mut series, 2, &sample(1.0, 5, false));
        let mut request = Vec::new();
        bytes_field(&mut request, 1, &series);
        compress(&request)
    };
    for labels in [
        &[(&b"job"[..], &b"a"[..])][..],
        &[(b"__name__", b"m"), (b"job", b"a"), (b"job", b"b")],
        &[(b"__name__", b"m"), (b"job", b"\xff\xfe")],
        &[(b"__name__", b"m"), (b"\xc3", b"a")],
        &[],
    ] {
        assert!(assert_same(&one(labels), &limits).is_err());
    }
    let mut rng = Rng(7);
    let body = compress(&request(&mut rng, 20, 30, 1_000));
    for max_samples in 0..6 {
        let limits = Limits {
            max_samples,
            ..Limits::default()
        };
        assert_same(&body, &limits).ok();
    }
}

/// Every truncation of a request, and random byte flips in it, decode
/// (or fail) the same; so do truncated and corrupted snappy bodies.
#[test]
fn malformed_bodies_fail_as_before() {
    let limits = Limits::default();
    let mut rng = Rng(42);
    let mut failures = 0;
    for _ in 0..6 {
        let series = 1 + rng.below(4);
        let protobuf = request(&mut rng, 12, series, 9_000);
        for end in 0..protobuf.len() {
            failures += usize::from(assert_same(&compress(&protobuf[..end]), &limits).is_err());
        }
        for _ in 0..2_000 {
            let mut flipped = protobuf.clone();
            let at = rng.below(flipped.len());
            flipped[at] ^= 1 << rng.below(8);
            if rng.chance(30) {
                let at = rng.below(flipped.len());
                flipped[at] = rng.next() as u8;
            }
            failures += usize::from(assert_same(&compress(&flipped), &limits).is_err());
        }
        let body = compress(&protobuf);
        for end in 0..body.len() {
            assert_same(&body[..end], &limits).ok();
        }
        for _ in 0..200 {
            let mut corrupted = body.clone();
            let at = rng.below(corrupted.len());
            corrupted[at] ^= 1 << rng.below(8);
            assert_same(&corrupted, &limits).ok();
        }
    }
    assert!(failures > 1_000);
}

/// With `RW_CORPUS_DIR` set: every file in it, a captured Remote Write
/// body, decodes the same.
#[test]
fn captured_bodies_decode_as_before() {
    let Some(dir) = std::env::var_os("RW_CORPUS_DIR") else {
        return;
    };
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    paths.sort();
    let limits = Limits::default();
    let mut samples = 0;
    for path in &paths {
        samples += assert_same(&std::fs::read(path).unwrap(), &limits)
            .expect("a valid request")
            .len();
    }
    println!("{} bodies, {samples} samples decode the same", paths.len());
}
