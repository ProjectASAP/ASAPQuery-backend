//! OTLP (OpenTelemetry protocol) metrics over HTTP: an
//! `ExportMetricsServiceRequest` body, as `POST /v1/metrics` receives it,
//! decoded into the caller's samples ([`Decoder`]), and the replies the
//! OTLP/HTTP specification asks for ([`export_response_body`], [`status_body`]).
//!
//! # From OTLP to series
//!
//! The decoder maps data points to Prometheus-style series as Prometheus'
//! own OTLP receiver does with its default configuration (Prometheus 3.x,
//! `storage/remote/otlptranslator/prometheusremotewrite` and the `otlp:`
//! defaults in `config/config.go`):
//!
//! - A gauge or sum point is one sample of the series named by its metric.
//!   A histogram, exponential histogram or summary point gives
//!   `<name>_sum` (when set) and `<name>_count`; buckets and quantiles are
//!   not read. A point without a value reads 0, as in Prometheus.
//! - The point's attributes are the series' labels, their names escaped as
//!   Prometheus' default `UnderscoreEscapingWithSuffixes` strategy escapes
//!   them (`http.method` is `http_method`). Attributes whose
//!   names escape to the same label are joined with `;`, in the order of
//!   their names; an empty value is no label.
//! - Resource attributes are not labels. `service.name` gives `job`
//!   (`<service.namespace>/<service.name>` when the namespace is set) and
//!   `service.instance.id` gives `instance`, over any point attribute of
//!   that name. [`LabelOptions::promote_resource_attributes`] names resource
//!   attributes that are also labels, as Prometheus'
//!   `promote_resource_attributes` does; a point attribute of the same
//!   label wins. No `target_info` series is written.
//! - The scope's name, version and attributes are not labels (Prometheus'
//!   `promote_scope_metadata: false`).
//! - A sum with DELTA temporality is rejected, each point counted in
//!   [`Decoded::rejected_points`]: its points are increments, not the
//!   running total a cumulative sum and `rate` read (Prometheus drops them
//!   with a warning).
//! - A point flagged "no recorded value" gives Prometheus' stale marker
//!   (`StaleNaN`) for each sample it gives, as Prometheus writes it; the
//!   Remote Write path keeps stale markers the same way.
//!
//! Where this differs from Prometheus' defaults:
//!
//! - Metric names are not translated (no escaping, no unit or `_total`
//!   suffix).
//! - Histograms and exponential histograms give `_sum` and `_count`
//!   whatever their temporality, and a sum of unspecified temporality is
//!   read as cumulative (Prometheus drops both).
//! - An attribute whose name Prometheus refuses (empty, or only
//!   underscores once escaped) is no label; Prometheus drops the point.
//! - Values print as Rust prints them: bytes as their debug text (`[1, 2]`,
//!   Prometheus: base64), arrays and key-value lists as empty text, so no
//!   label (Prometheus: JSON), doubles without an exponent (`1e21` is
//!   `1000000000000000000000`, Prometheus `1e+21`).
//! - A body is read as protobuf whatever its `Content-Type` other than
//!   `application/json` (Prometheus refuses other types), and OTLP/JSON is
//!   not decoded.
//! - Points with a sketch payload attribute (`kll.`, `cms.` or
//!   `countsketch.sketch_payload`) are skipped, as ASAPQuery skips them.
//!
//! # Decoding
//!
//! As the Remote Write decoder, [`Decoder`] walks the protobuf in place,
//! without building the request's messages, into the caller's
//! [`SampleBuilder`] series, and keeps a series cache between requests: a
//! point's series is looked up by the bytes of its resource's labels, its
//! metric name and its encoded attributes, so a known series costs a
//! compare (or a hash); only a new one is translated and interned.

use std::borrow::Cow;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io::Read;

use prometheus_remote_write::series_cache::SeriesCache;
use prometheus_remote_write::wire::{Reader, Value};
use prometheus_remote_write::RemoteWriteError;
use prost::Message;

/// The caller's series and sample types, as the Remote Write decoder
/// takes them: the labels passed to [`SampleBuilder::intern`] are sorted
/// by name, each name once, `__name__` among them.
pub use prometheus_remote_write::SampleBuilder;

/// The OTLP protobuf messages, for senders and tests.
pub use opentelemetry_proto::tonic as proto;

/// Request size limits; a request over any limit is rejected whole.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_decompressed_bytes: usize,
    pub max_points: usize,
}

/// How resource attributes become labels.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LabelOptions {
    /// Resource attributes that are labels of every series of their
    /// resource, by their OTLP names (`deployment.environment`), as
    /// Prometheus' `otlp.promote_resource_attributes`. Empty: none.
    pub promote_resource_attributes: Vec<String>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum OtlpError {
    #[error("Gzip decode error: {0}")]
    Gzip(String),
    #[error("decompressed request exceeds {0} bytes")]
    DecompressedTooLarge(usize),
    #[error("Protobuf decode error: {0}")]
    Protobuf(String),
    #[error("request has more than {0} points")]
    TooManyPoints(usize),
}

/// The protobuf reader's errors ([`RemoteWriteError::Protobuf`]).
impl From<RemoteWriteError> for OtlpError {
    #[cold]
    fn from(error: RemoteWriteError) -> Self {
        match error {
            RemoteWriteError::Protobuf(message) => OtlpError::Protobuf(message),
            other => OtlpError::Protobuf(other.to_string()),
        }
    }
}

/// A decoded request.
#[derive(Debug, PartialEq)]
pub struct Decoded<S> {
    /// The samples, in request order.
    pub samples: Vec<S>,
    /// Data points refused, which the reply reports as rejected: the points
    /// of sums with DELTA temporality.
    pub rejected_points: u64,
}

impl<S> Decoded<S> {
    /// Why points were rejected, for the reply; `None` when none were.
    pub fn rejection_message(&self) -> Option<String> {
        (self.rejected_points > 0).then(|| {
            format!(
                "{} data points of sums with DELTA aggregation temporality were rejected: \
                 only cumulative sums are ingested",
                self.rejected_points
            )
        })
    }
}

/// A decoder's state, kept between requests: its decompression buffer, its
/// series cache and the buffer it builds cache keys in.
pub struct Decoder<B: SampleBuilder> {
    decompressed: Vec<u8>,
    series: SeriesCache<B::Series>,
    builder: B,
    /// Where a point's cache key is built, kept between points.
    key_buffer: Vec<u8>,
}

impl<B: SampleBuilder> Decoder<B> {
    pub fn new(builder: B) -> Self {
        Self {
            decompressed: Vec::new(),
            series: SeriesCache::new(),
            builder,
            key_buffer: Vec::new(),
        }
    }

    /// The samples of an OTLP metrics request body, in request order.
    /// `content_encoding` is the request's `Content-Encoding`: `gzip` bodies
    /// are decompressed; any other encoding is read as is, as ASAPQuery
    /// does. Errors: the body is not gzip (when it says so) or not an
    /// `ExportMetricsServiceRequest`, or is over a limit.
    pub fn decode(
        &mut self,
        body: &[u8],
        content_encoding: Option<&str>,
        options: &LabelOptions,
        limits: &Limits,
    ) -> Result<Decoded<B::Sample>, OtlpError> {
        let gzip = content_encoding.is_some_and(|e| e.trim().eq_ignore_ascii_case("gzip"));
        let Self {
            decompressed,
            series,
            builder,
            key_buffer,
        } = self;
        let decoded = if gzip {
            gunzip(body, limits.max_decompressed_bytes, decompressed)
                .and_then(|request| walk(request, series, builder, key_buffer, options, limits))
        } else if body.len() > limits.max_decompressed_bytes {
            Err(OtlpError::DecompressedTooLarge(
                limits.max_decompressed_bytes,
            ))
        } else {
            walk(body, series, builder, key_buffer, options, limits)
        };
        // An unusually large request's buffer is not kept, whether or not it
        // decoded, as the Remote Write decoder does.
        if decompressed.capacity() > prometheus_remote_write::KEPT_DECOMPRESSED_BYTES {
            *decompressed = Vec::new();
        }
        decoded
    }
}

/// The samples of the uncompressed request `request` ([`Decoder::decode`]).
fn walk<B: SampleBuilder>(
    request: &[u8],
    series: &mut SeriesCache<B::Series>,
    builder: &mut B,
    key_buffer: &mut Vec<u8>,
    options: &LabelOptions,
    limits: &Limits,
) -> Result<Decoded<B::Sample>, OtlpError> {
    let mut walk = Walk {
        series,
        builder,
        key_buffer,
        options,
        max_points: limits.max_points,
        decoded: Decoded {
            // Room for one sample per 50 encoded bytes, about one gauge
            // point with a few short attributes.
            samples: Vec::with_capacity(request.len() / 50),
            rejected_points: 0,
        },
    };
    walk.request(request)?;
    Ok(walk.decoded)
}

/// `body` gunzipped into `out`, reading at most one byte past `max_bytes`.
fn gunzip<'a>(body: &[u8], max_bytes: usize, out: &'a mut Vec<u8>) -> Result<&'a [u8], OtlpError> {
    out.clear();
    flate2::read::MultiGzDecoder::new(body)
        .take(max_bytes as u64 + 1)
        .read_to_end(out)
        .map_err(|e| OtlpError::Gzip(e.to_string()))?;
    if out.len() > max_bytes {
        return Err(OtlpError::DecompressedTooLarge(max_bytes));
    }
    Ok(out)
}

/// The labels a resource gives each of its series.
struct ResourceLabels {
    job: Option<String>,
    instance: Option<String>,
    /// The promoted attributes, by label name.
    promoted: Vec<(String, String)>,
    /// These labels' bytes, the start of each of the resource's cache keys.
    key_prefix: Vec<u8>,
}

impl ResourceLabels {
    /// The labels of the encoded `Resource` `resource`.
    fn new(resource: &[u8], options: &LabelOptions) -> Result<Self, OtlpError> {
        let mut name = None;
        let mut namespace = None;
        let mut instance = None;
        let mut promoted = BTreeMap::new();
        let mut reader = Reader::new(resource);
        while let Some((field, value)) = reader.field()? {
            let (1, Value::Bytes(attribute)) = (field, value) else {
                continue;
            };
            let (key, value) = key_value(attribute)?;
            match key {
                "service.name" => name = Some(value.text().into_owned()),
                "service.namespace" => namespace = Some(value.text().into_owned()),
                "service.instance.id" => instance = Some(value.text().into_owned()),
                _ => {}
            }
            if options.promote_resource_attributes.iter().any(|p| p == key)
                && !value.text().is_empty()
            {
                if let Some(label) = label_name(key) {
                    promoted.insert(label.into_owned(), value.text().into_owned());
                }
            }
        }
        let job = name.map(|name| match namespace {
            Some(namespace) => format!("{namespace}/{name}"),
            None => name,
        });
        let job = job.filter(|job| !job.is_empty());
        let instance = instance.filter(|instance| !instance.is_empty());
        let promoted: Vec<(String, String)> = promoted.into_iter().collect();
        let mut key_prefix = Vec::new();
        for text in [job.as_deref(), instance.as_deref()] {
            push_length_prefixed(&mut key_prefix, text.unwrap_or(""));
        }
        for (label, value) in &promoted {
            push_length_prefixed(&mut key_prefix, label);
            push_length_prefixed(&mut key_prefix, value);
        }
        // Ends the resource's part of a key: no label name is empty.
        push_length_prefixed(&mut key_prefix, "");
        Ok(Self {
            job,
            instance,
            promoted,
            key_prefix,
        })
    }
}

/// Appends `text` to `key`, length first.
fn push_length_prefixed(key: &mut Vec<u8>, text: &str) {
    key.extend_from_slice(&(text.len() as u32).to_le_bytes());
    key.extend_from_slice(text.as_bytes());
}

/// What a data point message gives: a gauge or sum point one sample; a
/// histogram, exponential histogram or summary point `_sum` and `_count`
/// (a summary's `_sum` always set, the histograms' only when present).
#[derive(Clone, Copy, PartialEq)]
enum PointKind {
    Number,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl PointKind {
    /// The field numbers of the point's attributes and of its flags.
    fn attributes_and_flags_fields(self) -> (u64, u64) {
        match self {
            PointKind::Number => (7, 8),
            PointKind::Histogram => (9, 10),
            PointKind::ExponentialHistogram => (1, 10),
            PointKind::Summary => (7, 8),
        }
    }
}

/// A metric's last point: its encoded attributes and the series of each of
/// its samples found so far.
struct PreviousPoint<'p, S> {
    attributes: &'p [u8],
    series: [Option<S>; 2],
}

/// `DataPointFlags.FLAG_NO_RECORDED_VALUE_MASK`.
const FLAG_NO_RECORDED_VALUE: u64 = 1;
/// Prometheus' stale marker (`value.StaleNaN`), the value of each sample
/// of a point flagged "no recorded value".
const STALE_NAN: f64 = f64::from_bits(0x7ff0_0000_0000_0002);
/// `AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA`.
const TEMPORALITY_DELTA: u64 = 1;

/// One request's walk.
struct Walk<'a, B: SampleBuilder> {
    series: &'a mut SeriesCache<B::Series>,
    builder: &'a mut B,
    key_buffer: &'a mut Vec<u8>,
    options: &'a LabelOptions,
    max_points: usize,
    decoded: Decoded<B::Sample>,
}

impl<B: SampleBuilder> Walk<'_, B> {
    /// An encoded `ExportMetricsServiceRequest`.
    fn request(&mut self, request: &[u8]) -> Result<(), OtlpError> {
        let mut reader = Reader::new(request);
        while let Some((field, value)) = reader.field()? {
            if let (1, Value::Bytes(resource_metrics)) = (field, value) {
                self.resource_metrics(resource_metrics)?;
            }
        }
        Ok(())
    }

    /// An encoded `ResourceMetrics`: its resource (field 1), then the
    /// metrics of each of its scopes (field 2), in any order.
    fn resource_metrics(&mut self, bytes: &[u8]) -> Result<(), OtlpError> {
        let mut resource: &[u8] = &[];
        let mut reader = Reader::new(bytes);
        while let Some((field, value)) = reader.field()? {
            if let (1, Value::Bytes(found)) = (field, value) {
                resource = found;
            }
        }
        let resource = ResourceLabels::new(resource, self.options)?;
        let mut reader = Reader::new(bytes);
        while let Some((field, value)) = reader.field()? {
            let (2, Value::Bytes(scope_metrics)) = (field, value) else {
                continue;
            };
            let mut metrics = Reader::new(scope_metrics);
            while let Some((field, value)) = metrics.field()? {
                if let (2, Value::Bytes(metric)) = (field, value) {
                    self.metric(&resource, metric)?;
                }
            }
        }
        Ok(())
    }

    /// An encoded `Metric`: its name (field 1) and its data, one of gauge
    /// (5), sum (7), histogram (9), exponential histogram (10) or summary
    /// (11), whose points (field 1) each give samples.
    fn metric(&mut self, resource: &ResourceLabels, bytes: &[u8]) -> Result<(), OtlpError> {
        let mut name = "";
        let mut data = None;
        let mut reader = Reader::new(bytes);
        while let Some((field, value)) = reader.field()? {
            match (field, value) {
                (1, Value::Bytes(text)) => name = utf8(text)?,
                (5 | 7 | 9 | 10 | 11, Value::Bytes(found)) => data = Some((field, found)),
                _ => {}
            }
        }
        let Some((field, data)) = data else {
            return Ok(());
        };
        if name.is_empty() {
            return Ok(());
        }
        let kind = match field {
            5 | 7 => PointKind::Number,
            9 => PointKind::Histogram,
            10 => PointKind::ExponentialHistogram,
            _ => PointKind::Summary,
        };
        let delta_sum = field == 7 && {
            let mut temporality = 0;
            let mut reader = Reader::new(data);
            while let Some((field, value)) = reader.field()? {
                if let (2, Value::Varint(found)) = (field, value) {
                    temporality = found;
                }
            }
            temporality == TEMPORALITY_DELTA
        };
        let mut previous = None;
        let mut reader = Reader::new(data);
        while let Some((field, value)) = reader.field()? {
            let (1, Value::Bytes(point)) = (field, value) else {
                continue;
            };
            if delta_sum {
                self.decoded.rejected_points += 1;
            } else {
                self.point(resource, name, kind, point, &mut previous)?;
            }
        }
        Ok(())
    }

    /// An encoded data point of `kind` of metric `name`. `previous` is the
    /// metric's point before it, whose series it takes when it has the same
    /// encoded attributes, as a series' consecutive points do.
    fn point<'p>(
        &mut self,
        resource: &ResourceLabels,
        name: &str,
        kind: PointKind,
        point: &'p [u8],
        previous: &mut Option<PreviousPoint<'p, B::Series>>,
    ) -> Result<(), OtlpError> {
        let (attributes_field, flags_field) = kind.attributes_and_flags_fields();
        let mut time_nanos = 0u64;
        let mut flags = 0u64;
        let mut number = 0f64;
        let mut count = 0u64;
        let mut sum = (kind == PointKind::Summary).then_some(0f64);
        // Where the encoded attributes are.
        let mut attributes_start = None;
        let mut attributes_end = 0;
        let mut contiguous = true;
        let mut reader = Reader::new(point);
        while let Some((field, value)) = reader.field()? {
            match (field, value) {
                (field, Value::Bytes(_)) if field == attributes_field => {
                    match attributes_start {
                        None => attributes_start = Some(reader.field_start),
                        Some(_) if reader.field_start != attributes_end => contiguous = false,
                        Some(_) => {}
                    }
                    attributes_end = reader.position;
                }
                (3, Value::Fixed64(found)) => time_nanos = found,
                (field, Value::Varint(found)) if field == flags_field => flags = found,
                (4, Value::Fixed64(bits)) if kind == PointKind::Number => {
                    number = f64::from_bits(bits)
                }
                (6, Value::Fixed64(bits)) if kind == PointKind::Number => {
                    number = bits as i64 as f64
                }
                (4, Value::Fixed64(found)) => count = found,
                (5, Value::Fixed64(bits)) => sum = Some(f64::from_bits(bits)),
                _ => {}
            }
        }
        // The encoded attributes: one slice of `point` when contiguous, as
        // senders write them; the metric's previous point's series serve
        // the same slice.
        let concatenated;
        let attributes = match (attributes_start, contiguous) {
            (None, _) | (Some(_), true) => {
                let attributes = &point[attributes_start.unwrap_or(0)..attributes_end];
                if previous
                    .as_ref()
                    .is_none_or(|last| last.attributes != attributes)
                {
                    *previous = Some(PreviousPoint {
                        attributes,
                        series: [None; 2],
                    });
                }
                attributes
            }
            (Some(_), false) => {
                // Rare: attributes interleaved with other fields; key by
                // their concatenation.
                *previous = None;
                let mut joined = Vec::new();
                let mut reader = Reader::new(point);
                while let Some((field, value)) = reader.field()? {
                    if let (field, Value::Bytes(_)) = (field, value) {
                        if field == attributes_field {
                            joined.extend_from_slice(&point[reader.field_start..reader.position]);
                        }
                    }
                }
                concatenated = joined;
                &concatenated[..]
            }
        };
        let timestamp_ms = (time_nanos / 1_000_000) as i64;
        let mut samples: [(&str, Option<f64>); 2] = match kind {
            PointKind::Number => [("", Some(number)), ("", None)],
            _ => [("_sum", sum), ("_count", Some(count as f64))],
        };
        if flags & FLAG_NO_RECORDED_VALUE != 0 {
            for (_, value) in &mut samples {
                if value.is_some() {
                    *value = Some(STALE_NAN);
                }
            }
        }
        for (index, (suffix, value)) in samples.into_iter().enumerate() {
            let Some(value) = value else { continue };
            let known = previous.as_ref().and_then(|last| last.series[index]);
            let series = match known {
                Some(series) => series,
                None => {
                    let Some(series) = self.series(resource, name, suffix, attributes)? else {
                        // A sketch payload point.
                        return Ok(());
                    };
                    if let Some(last) = previous.as_mut() {
                        last.series[index] = Some(series);
                    }
                    series
                }
            };
            if self.decoded.samples.len() >= self.max_points {
                return Err(OtlpError::TooManyPoints(self.max_points));
            }
            self.decoded
                .samples
                .push(B::sample(series, timestamp_ms, value));
        }
        Ok(())
    }

    /// The series of a point of metric `name` (with `suffix`) and the
    /// encoded `attributes` (`KeyValue` fields), from the cache or
    /// translated and interned; `None` for a point that carries a sketch
    /// payload.
    #[inline]
    fn series(
        &mut self,
        resource: &ResourceLabels,
        name: &str,
        suffix: &str,
        attributes: &[u8],
    ) -> Result<Option<B::Series>, OtlpError> {
        let key = &mut *self.key_buffer;
        key.clear();
        key.extend_from_slice(&resource.key_prefix);
        key.extend_from_slice(&((name.len() + suffix.len()) as u32).to_le_bytes());
        key.extend_from_slice(name.as_bytes());
        key.extend_from_slice(suffix.as_bytes());
        key.extend_from_slice(attributes);
        if let Some(series) = self.series.get(key) {
            return Ok(Some(series));
        }
        let mut pairs = Vec::new();
        let mut reader = Reader::new(attributes);
        while let Some((_, value)) = reader.field()? {
            if let Value::Bytes(attribute) = value {
                pairs.push(key_value(attribute)?);
            }
        }
        if pairs
            .iter()
            .any(|(key, value)| is_sketch_payload(key, value))
        {
            return Ok(None);
        }
        let labels = series_labels(resource, &format!("{name}{suffix}"), &mut pairs);
        let labels: Vec<(&str, &str)> = labels
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_str()))
            .collect();
        let series = self.builder.intern(&labels);
        self.series.insert(key, series);
        Ok(Some(series))
    }
}

/// Whether an attribute is a sketch payload: bytes under one of the
/// sketch payload keys.
fn is_sketch_payload(key: &str, value: &AttributeValue) -> bool {
    matches!(
        key,
        "kll.sketch_payload" | "cms.sketch_payload" | "countsketch.sketch_payload"
    ) && matches!(value, AttributeValue::Bytes(_))
}

/// An attribute's value: its label text, or bytes, whose label text
/// ([`AttributeValue::text`]) is only built for a label.
enum AttributeValue<'a> {
    Text(Cow<'a, str>),
    Bytes(&'a [u8]),
}

impl AttributeValue<'_> {
    /// The value as label text: bytes as their debug text (`[1, 2]`).
    fn text(&self) -> Cow<'_, str> {
        match self {
            AttributeValue::Text(text) => Cow::Borrowed(text),
            AttributeValue::Bytes(bytes) => Cow::Owned(format!("{bytes:?}")),
        }
    }
}

/// The key and value of an encoded `KeyValue`: its key (field 1) and its
/// `AnyValue` (field 2): a string as is, an integer, double or boolean
/// printed, bytes as bytes; an array, a key-value list or no value as empty
/// text.
fn key_value(bytes: &[u8]) -> Result<(&str, AttributeValue<'_>), OtlpError> {
    let mut key = "";
    let mut value = AttributeValue::Text(Cow::Borrowed(""));
    let mut reader = Reader::new(bytes);
    while let Some((field, found)) = reader.field()? {
        match (field, found) {
            (1, Value::Bytes(text)) => key = utf8(text)?,
            (2, Value::Bytes(any_value)) => value = attribute_value(any_value)?,
            _ => {}
        }
    }
    Ok((key, value))
}

/// An encoded `AnyValue` ([`key_value`]).
fn attribute_value(bytes: &[u8]) -> Result<AttributeValue<'_>, OtlpError> {
    let mut value = AttributeValue::Text(Cow::Borrowed(""));
    let mut reader = Reader::new(bytes);
    while let Some((field, found)) = reader.field()? {
        value = match (field, found) {
            (1, Value::Bytes(text)) => AttributeValue::Text(Cow::Borrowed(utf8(text)?)),
            (2, Value::Varint(boolean)) => {
                AttributeValue::Text(Cow::Borrowed(if boolean != 0 { "true" } else { "false" }))
            }
            (3, Value::Varint(integer)) => {
                AttributeValue::Text(Cow::Owned((integer as i64).to_string()))
            }
            (4, Value::Fixed64(bits)) => {
                AttributeValue::Text(Cow::Owned(f64::from_bits(bits).to_string()))
            }
            (7, Value::Bytes(bytes)) => AttributeValue::Bytes(bytes),
            _ => AttributeValue::Text(Cow::Borrowed("")),
        };
    }
    Ok(value)
}

fn utf8(bytes: &[u8]) -> Result<&str, OtlpError> {
    std::str::from_utf8(bytes).map_err(|e| OtlpError::Protobuf(format!("string is not UTF-8: {e}")))
}

/// A series' labels, sorted by name: the point's `attributes` (sorted
/// here), then the resource's promoted attributes where no attribute gave
/// the label, then `job` and `instance` from the resource, then
/// `__name__`, as Prometheus' `createAttributes` merges them.
fn series_labels(
    resource: &ResourceLabels,
    name: &str,
    attributes: &mut [(&str, AttributeValue)],
) -> Vec<(Cow<'static, str>, String)> {
    attributes.sort_by(|a, b| a.0.cmp(b.0));
    let mut labels: BTreeMap<Cow<'static, str>, String> = BTreeMap::new();
    for (key, value) in attributes.iter() {
        let Some(label) = label_name(key) else {
            continue;
        };
        match labels.entry(Cow::Owned(label.into_owned())) {
            Entry::Occupied(mut joined) => {
                joined.get_mut().push(';');
                joined.get_mut().push_str(&value.text());
            }
            Entry::Vacant(_) if value.text().is_empty() => {}
            Entry::Vacant(entry) => {
                entry.insert(value.text().into_owned());
            }
        }
    }
    for (label, value) in &resource.promoted {
        labels
            .entry(Cow::Owned(label.clone()))
            .or_insert_with(|| value.clone());
    }
    if let Some(job) = &resource.job {
        labels.insert(Cow::Borrowed("job"), job.clone());
    }
    if let Some(instance) = &resource.instance {
        labels.insert(Cow::Borrowed("instance"), instance.clone());
    }
    labels.insert(Cow::Borrowed("__name__"), name.to_string());
    labels.into_iter().collect()
}

/// An OTLP attribute name as a Prometheus label name, as Prometheus' OTLP
/// receiver escapes it by default (`otlptranslator.LabelNamer` with UTF-8
/// names off, `label_name_underscore_sanitization: true` and
/// `label_name_preserve_multiple_underscores: true`): each character other
/// than an ASCII letter or digit becomes `_`; then a name that starts with
/// a digit gets `key_` before it, and one that starts with a single `_`
/// gets `key`. `None` for a name Prometheus refuses: empty, or only
/// underscores once escaped.
pub(crate) fn label_name(name: &str) -> Option<Cow<'_, str>> {
    let first = *name.as_bytes().first()?;
    let valid = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let label = if name.chars().all(valid) && !first.is_ascii_digit() && !single_underscore(name) {
        Cow::Borrowed(name)
    } else {
        let escaped: String = name
            .chars()
            .map(|c| if valid(c) { c } else { '_' })
            .collect();
        if escaped.as_bytes()[0].is_ascii_digit() {
            Cow::Owned(format!("key_{escaped}"))
        } else if single_underscore(&escaped) {
            Cow::Owned(format!("key{escaped}"))
        } else {
            Cow::Owned(escaped)
        }
    };
    (!label.bytes().all(|b| b == b'_')).then_some(label)
}

fn single_underscore(name: &str) -> bool {
    name.starts_with('_') && !name.starts_with("__")
}

/// How an OTLP/HTTP request's body is encoded, by its `Content-Type`; the
/// reply is encoded the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// `application/x-protobuf`, and any other or no content type.
    Protobuf,
    /// `application/json`.
    Json,
}

impl Encoding {
    pub fn of_content_type(content_type: Option<&str>) -> Self {
        let media_type = content_type
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if media_type.is_some_and(|t| t.eq_ignore_ascii_case("application/json")) {
            Encoding::Json
        } else {
            Encoding::Protobuf
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Encoding::Protobuf => "application/x-protobuf",
            Encoding::Json => "application/json",
        }
    }
}

/// The body of a 200 reply to a protobuf request: an encoded
/// `ExportMetricsServiceResponse`, with `partial_success` set to
/// `rejected_points` and `message` when points were rejected, as OTLP/HTTP
/// asks. The empty message (no `partial_success`) is no bytes.
pub fn export_response_body(rejected_points: u64, message: Option<&str>) -> Vec<u8> {
    use proto::collector::metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceResponse,
    };
    let partial_success = (rejected_points > 0).then(|| ExportMetricsPartialSuccess {
        rejected_data_points: rejected_points as i64,
        error_message: message.unwrap_or_default().to_string(),
    });
    ExportMetricsServiceResponse { partial_success }.encode_to_vec()
}

/// `google.rpc.Status`, the body of every 4xx and 5xx OTLP/HTTP reply,
/// with its `message` only: OTLP/HTTP does not use `code` (field 1), and
/// no `details` (field 3) are sent.
#[derive(Clone, PartialEq, Message)]
pub struct Status {
    #[prost(string, tag = "2")]
    pub message: String,
}

/// The body of a 4xx or 5xx reply: a `Status` with `message`, encoded as
/// the request was.
pub fn status_body(encoding: Encoding, message: &str) -> Vec<u8> {
    match encoding {
        Encoding::Protobuf => Status {
            message: message.to_string(),
        }
        .encode_to_vec(),
        Encoding::Json => serde_json::to_vec(&serde_json::json!({ "message": message }))
            .expect("a JSON object serializes"),
    }
}

#[cfg(test)]
mod tests;
