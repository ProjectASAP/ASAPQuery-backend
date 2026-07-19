use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::query_parser;
use crate::types::{AggType, QueryWorkload, SketchType, WorkloadCharacteristics};
use crate::types_v2::{AccuracyTarget, DataShape, QueryId, QueryLanguage, QueryShape};

// ── Public API ────────────────────────────────────────────────────────────────

/// JSON-friendly representation of a query workload submitted by callers.
///
/// There are two ways to populate a `QuerySpec`:
///
/// 1. **Explicit fields** — supply `metric_name`, `aggregations`,
///    `time_window`, etc. directly. This is the original API.
///
/// 2. **Query string** — supply a raw PromQL string in
///    `query_string`.  The analyzer parses it and fills in `metric_name`,
///    `aggregations`, `group_by_labels`, `label_filters`, and `time_window`
///    automatically.  Any explicit fields that are non-empty / non-default
///    **override** the parsed values, so the two approaches compose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    /// Raw PromQL query string to parse (SP-1 automatic extraction).
    /// When provided, metric_name / aggregations / time_window may be omitted
    /// and will be derived from the query.
    #[serde(default)]
    pub query_string: Option<String>,

    /// Metric name override.  Required when `query_string` is absent.
    #[serde(default)]
    pub metric_name: String,
    #[serde(default)]
    pub label_filters: HashMap<String, String>,
    #[serde(default)]
    pub group_by_labels: Vec<String>,
    /// Aggregation type overrides ("quantile", "cardinality", "frequency").
    /// Required when `query_string` is absent.
    #[serde(default)]
    pub aggregations: Vec<String>,
    /// Time window override (e.g. "5m").  Required when `query_string` is absent.
    #[serde(default)]
    pub time_window: String,
    #[serde(default)]
    pub repeat_every: Option<String>,
    pub accuracy_sla: f64,
    pub latency_sla: Option<String>,
    /// Optional: pin a specific sketch type, bypassing the cost-model planner.
    pub sketch_type: Option<SketchType>,
    /// Observable data-stream characteristics used for delta / raw-vs-sketch
    /// bandwidth comparison. Omit to use conservative defaults.
    #[serde(default)]
    pub workload: WorkloadCharacteristics,

    // ── design.md alignment: new fields, defaulted for back-compat ────────
    //
    // These fields converge `QuerySpec` toward the typed schema in
    // `control_plane/docs/design.md` §6 `core::workload`. Each is defaulted
    // so the existing JSON API surface (POST /api/v1/plan handlers,
    // pre-population from `workloads.yaml`, the test fixtures elsewhere
    // in the control plane) keeps working without supplying them. The
    // planner does not yet consume these — see `Analyzer::analyze` for
    // the L1 cross-product validation that does fire today.
    /// Stable identifier preserved across replan cycles. Optional;
    /// auto-derived from `metric_name + accuracy_sla` if omitted
    /// (existing API callers don't supply this).
    #[serde(default)]
    pub id: Option<QueryId>,

    /// Source language. Inferred from `query_string` syntax / parser
    /// dispatch when omitted (existing API callers default to PromQL
    /// behavior, which matches today's `query_parser::parse_query`).
    #[serde(default)]
    pub language: Option<QueryLanguage>,

    /// Typed accuracy target. When present, takes precedence over the
    /// legacy `accuracy_sla: f64` field. When absent, the legacy field
    /// is converted to `Epsilon(1.0 - accuracy_sla)` (or `Exact` when
    /// `accuracy_sla == 1.0`).
    #[serde(default)]
    pub accuracy: Option<AccuracyTarget>,

    /// Per-evaluation $ budget. Optional; the cost model picks freely
    /// when unset.
    #[serde(default)]
    pub dollars: Option<f64>,

    /// Deployment-model routing hint. Optional; defaults to the model
    /// bound to the inbound HTTP route.
    #[serde(default)]
    pub deployment_model: Option<String>,

    /// Evaluation cadence shape. Defaults to `OneShot`.
    #[serde(default = "default_query_shape")]
    pub shape: QueryShape,

    /// Source data shape. Defaults to `AppendOnlyStream` (the
    /// asap-collector / asap-query default).
    #[serde(default = "default_data_shape")]
    pub data: DataShape,
}

fn default_query_shape() -> QueryShape {
    QueryShape::default()
}
fn default_data_shape() -> DataShape {
    DataShape::default()
}

pub struct Analyzer;

impl Analyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn analyze(&self, spec: QuerySpec) -> anyhow::Result<QueryWorkload> {
        if !(0.0..=1.0).contains(&spec.accuracy_sla) {
            return Err(anyhow!(
                "accuracy_sla must be in [0,1], got {}",
                spec.accuracy_sla
            ));
        }

        // ── design.md L1: shape × data cross-product check ─────────────────
        // The cross-product table in design.md §6 enumerates which
        // (shape, data) combinations the planner accepts. The two
        // hard rejections are at L1 because they have no semantically
        // valid plan: a streaming query over a static dataset, and a
        // streaming query over a mutable relation (no retraction-aware
        // sketches in the catalog yet). Everything else is accepted
        // here — downstream rule firing can still narrow further.
        match (&spec.shape, &spec.data) {
            (QueryShape::Streaming, DataShape::Batch) => {
                return Err(anyhow!(
                    "QueryShape::Streaming over DataShape::Batch is rejected at L1: \
                     no semantically valid plan (no stream over a static dataset). \
                     See control_plane/docs/design.md §6 cross-product table."
                ));
            }
            (QueryShape::Streaming, DataShape::Mutable) => {
                return Err(anyhow!(
                    "QueryShape::Streaming over DataShape::Mutable is rejected at L1: \
                     no retraction-aware sketches in the catalog yet. \
                     See control_plane/docs/design.md §6 cross-product table."
                ));
            }
            _ => {}
        }

        // ── design.md accuracy precedence: typed `accuracy` > legacy ───────
        // When the caller supplies `accuracy: Some(AccuracyTarget)` it
        // takes precedence. Otherwise the legacy `accuracy_sla: f64`
        // field is translated into the typed form. The downstream
        // planner currently consumes the legacy `f64` field; we keep
        // it populated either way so cost-model behaviour does not
        // regress for callers that supply the new field. Once the
        // planner switches to consuming `AccuracyTarget` directly
        // (separate downstream PR), this back-translation stops being
        // needed.
        let accuracy_sla = match &spec.accuracy {
            Some(AccuracyTarget::Exact) => 1.0,
            Some(AccuracyTarget::Epsilon(eps)) => (1.0 - eps).clamp(0.0, 1.0),
            Some(AccuracyTarget::EpsilonDelta { epsilon, .. }) => (1.0 - epsilon).clamp(0.0, 1.0),
            None => spec.accuracy_sla,
        };

        // ── Step 1: parse query_string if provided ─────────────────────────
        let parsed = spec
            .query_string
            .as_deref()
            .map(|q| query_parser::parse_query(q))
            .transpose()
            .with_context(|| "failed to parse query_string")?;

        // ── Step 2: resolve metric_name ────────────────────────────────────
        let metric_name = if !spec.metric_name.trim().is_empty() {
            spec.metric_name.clone()
        } else if let Some(ref p) = parsed {
            p.metric_name.clone()
        } else {
            return Err(anyhow!("metric_name is required (or provide query_string)"));
        };

        // ── Step 3: resolve aggregations ───────────────────────────────────
        let aggregations = if !spec.aggregations.is_empty() {
            parse_agg_types(&spec.aggregations)?
        } else if let Some(ref p) = parsed {
            if p.aggregations.is_empty() && !p.exact_required {
                return Err(anyhow!(
                    "could not infer aggregation type from query_string; \
                     provide explicit aggregations"
                ));
            }
            p.aggregations.clone()
        } else {
            return Err(anyhow!("at least one aggregation is required"));
        };

        // ── Step 4: resolve time_window ────────────────────────────────────
        let time_window = if !spec.time_window.trim().is_empty() {
            let d = parse_duration(&spec.time_window)
                .with_context(|| format!("invalid time_window {:?}", spec.time_window))?;
            if d.is_zero() {
                return Err(anyhow!("time_window must be positive"));
            }
            d
        } else if let Some(ref p) = parsed {
            p.time_window
        } else {
            return Err(anyhow!("time_window is required (or provide query_string)"));
        };

        // ── Step 5: resolve dimensions (group_by + label_filter keys) ──────
        // Parsed values are the base; explicit spec fields override / extend.
        let parsed_group_by = parsed
            .as_ref()
            .map(|p| p.group_by_labels.as_slice())
            .unwrap_or(&[]);
        let parsed_filters: HashMap<String, String> = parsed
            .as_ref()
            .map(|p| p.label_filters.clone())
            .unwrap_or_default();

        let merged_filters: HashMap<String, String> = {
            let mut m = parsed_filters;
            m.extend(spec.label_filters.clone()); // explicit overrides parsed
            m
        };

        let filter_keys: Vec<String> = merged_filters.keys().cloned().collect();
        let all_group_by: Vec<String> = dedup_dims(
            &dedup_dims(parsed_group_by, &spec.group_by_labels),
            &filter_keys,
        );

        // ── Step 6: scalar fields ──────────────────────────────────────────
        let repeat_every = spec
            .repeat_every
            .as_deref()
            .map(parse_duration)
            .transpose()
            .with_context(|| "invalid repeat_every")?;

        let latency_sla = spec
            .latency_sla
            .as_deref()
            .map(parse_duration)
            .transpose()
            .with_context(|| "invalid latency_sla")?;

        let exact_required = parsed.as_ref().map(|p| p.exact_required).unwrap_or(false);
        let quantiles = parsed
            .as_ref()
            .map(|p| p.quantiles.clone())
            .unwrap_or_default();

        // Note: `(planner not yet using this)` — these are populated for
        // downstream consumers but the planner / cost model still keys
        // off `accuracy_sla`, `time_window`, `aggregations`, etc. The
        // L4-aware downstream PR will switch the cost model to read
        // `spec.accuracy`, the L5 stage allocator to gate on
        // `spec.shape`, and the leaf planner to gate on `spec.data`.
        let _ = (
            &spec.accuracy,
            &spec.shape,
            &spec.data,
            &spec.id,
            &spec.language,
            &spec.dollars,
            &spec.deployment_model,
        );

        Ok(QueryWorkload {
            metric_name,
            label_filters: merged_filters,
            group_by_labels: all_group_by,
            aggregations,
            time_window,
            repeat_every,
            accuracy_sla,
            latency_sla,
            sketch_type_override: spec.sketch_type,
            exact_required,
            quantiles,
        })
    }
}

// ── Duration helpers (used by other modules) ──────────────────────────────────

/// Parses duration strings like "5m", "1h", "30s", "1h30m", "1h5m30s".
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty duration string"));
    }
    let mut total_secs: u64 = 0;
    let mut current_num = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else {
            let n: u64 = current_num
                .parse()
                .map_err(|_| anyhow!("invalid number in duration {:?}", s))?;
            current_num.clear();
            match ch {
                'h' => total_secs += n * 3600,
                'm' => total_secs += n * 60,
                's' => total_secs += n,
                _ => return Err(anyhow!("unknown unit {:?} in duration {:?}", ch, s)),
            }
        }
    }
    if !current_num.is_empty() {
        return Err(anyhow!("trailing digits without unit in {:?}", s));
    }
    Ok(Duration::from_secs(total_secs))
}

/// Formats a Duration as a compact string: "5m", "1h30m", "30s".
pub fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let sec = s % 60;
    let mut out = String::new();
    if h > 0 {
        out.push_str(&format!("{}h", h));
    }
    if m > 0 {
        out.push_str(&format!("{}m", m));
    }
    if sec > 0 || out.is_empty() {
        out.push_str(&format!("{}s", sec));
    }
    out
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn parse_agg_types(raw: &[String]) -> anyhow::Result<Vec<AggType>> {
    raw.iter()
        .map(|s| match s.to_lowercase().trim() {
            "quantile" => Ok(AggType::Quantile),
            "cardinality" => Ok(AggType::Cardinality),
            "frequency" => Ok(AggType::Frequency),
            other => Err(anyhow!(
                "unknown aggregation type {:?} (want: quantile, cardinality, frequency)",
                other
            )),
        })
        .collect()
}

fn dedup_dims(a: &[String], b: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for v in a.iter().chain(b.iter()) {
        if seen.insert(v.clone()) {
            out.push(v.clone());
        }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn basic_spec() -> QuerySpec {
        QuerySpec {
            query_string: None,
            metric_name: "request_latency".into(),
            label_filters: [("service".into(), "web".into())].into(),
            group_by_labels: vec!["host.name".into()],
            aggregations: vec!["quantile".into()],
            time_window: "5m".into(),
            repeat_every: Some("1m".into()),
            accuracy_sla: 0.01,
            latency_sla: Some("10m".into()),
            sketch_type: None,
            workload: Default::default(),
            // design.md alignment: defaults preserve legacy behaviour.
            id: None,
            language: None,
            accuracy: None,
            dollars: None,
            deployment_model: None,
            shape: QueryShape::default(),
            data: DataShape::default(),
        }
    }

    #[test]
    fn valid_spec() {
        let w = Analyzer::new().analyze(basic_spec()).unwrap();
        assert_eq!(w.metric_name, "request_latency");
        assert_eq!(w.accuracy_sla, 0.01);
        assert_eq!(w.time_window, Duration::from_secs(300));
        assert_eq!(w.repeat_every, Some(Duration::from_secs(60)));
        assert_eq!(w.latency_sla, Some(Duration::from_secs(600)));
        assert_eq!(w.aggregations, vec![AggType::Quantile]);
    }

    #[test]
    fn dimension_merge_dedup() {
        let mut spec = basic_spec();
        spec.label_filters = [
            ("service".into(), "api".into()),
            ("host.name".into(), "h1".into()),
        ]
        .into();
        spec.group_by_labels = vec!["host.name".into(), "region".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        for dim in &["host.name", "region", "service"] {
            assert!(
                w.group_by_labels.contains(&dim.to_string()),
                "missing {dim}"
            );
        }
        // host.name must appear exactly once after dedup
        assert_eq!(
            w.group_by_labels
                .iter()
                .filter(|d| d.as_str() == "host.name")
                .count(),
            1
        );
    }

    #[test]
    fn multiple_aggregations() {
        let mut spec = basic_spec();
        spec.aggregations = vec!["cardinality".into(), "frequency".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(
            w.aggregations,
            vec![AggType::Cardinality, AggType::Frequency]
        );
    }

    #[test]
    fn missing_metric_name() {
        let mut spec = basic_spec();
        spec.metric_name = "".into();
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn missing_aggregations() {
        let mut spec = basic_spec();
        spec.aggregations = vec![];
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn invalid_aggregation_type() {
        let mut spec = basic_spec();
        spec.aggregations = vec!["histogram".into()];
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn invalid_duration() {
        let mut spec = basic_spec();
        spec.time_window = "not-a-duration".into();
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn invalid_accuracy_sla() {
        for bad in &[-0.1f64, 1.5] {
            let mut spec = basic_spec();
            spec.accuracy_sla = *bad;
            assert!(
                Analyzer::new().analyze(spec).is_err(),
                "expected error for accuracy_sla={bad}"
            );
        }
    }

    #[test]
    fn parse_duration_formats() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(
            parse_duration("1h5m30s").unwrap(),
            Duration::from_secs(3930)
        );
    }

    #[test]
    fn format_duration_roundtrip() {
        for secs in [30u64, 300, 3600, 5400, 3930] {
            let d = Duration::from_secs(secs);
            let s = format_duration(d);
            let parsed = parse_duration(&s).unwrap();
            assert_eq!(parsed, d, "roundtrip failed for {secs}s → {s:?}");
        }
    }

    #[test]
    fn trailing_digits_error() {
        assert!(parse_duration("5").is_err());
    }

    // ── query_string path ─────────────────────────────────────────────────────

    /// Build a minimal QuerySpec driven entirely by a query_string.
    fn qs_only(query: &str) -> QuerySpec {
        QuerySpec {
            query_string: Some(query.into()),
            metric_name: "".into(),
            label_filters: Default::default(),
            group_by_labels: vec![],
            aggregations: vec![],
            time_window: "".into(),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type: None,
            workload: Default::default(),
            // design.md alignment: defaults preserve legacy behaviour.
            id: None,
            language: None,
            accuracy: None,
            dollars: None,
            deployment_model: None,
            shape: QueryShape::default(),
            data: DataShape::default(),
        }
    }

    /// PromQL query_string auto-populates metric_name, aggregations,
    /// time_window, and quantiles — no explicit fields required.
    #[test]
    fn query_string_promql_populates_workload() {
        let w = Analyzer::new()
            .analyze(qs_only(
                "sum by (host) (quantile_over_time(0.99, latency[5m]))",
            ))
            .unwrap();
        assert_eq!(w.metric_name, "latency");
        assert_eq!(w.aggregations, vec![AggType::Quantile]);
        assert_eq!(w.time_window, Duration::from_secs(300));
        assert_eq!(w.quantiles, vec![0.99]);
        assert!(!w.exact_required);
    }

    /// Explicit metric_name overrides the name derived from query_string.
    #[test]
    fn explicit_metric_name_overrides_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))");
        spec.metric_name = "my_custom_metric".into();
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.metric_name, "my_custom_metric");
        // aggregations still come from parse — `avg_over_time` is exact
        // (no ASAP-tier sketch substitute for `AggIntent::Avg`), so
        // `aggregations` stays empty and `exact_required` flips instead.
        assert_eq!(w.aggregations, Vec::<AggType>::new());
        assert!(w.exact_required);
    }

    /// Explicit time_window overrides the window derived from query_string.
    #[test]
    fn explicit_time_window_overrides_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))");
        spec.time_window = "1h".into();
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.time_window, Duration::from_secs(3600));
    }

    /// Explicit aggregations override those derived from query_string.
    #[test]
    fn explicit_aggregations_override_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))"); // → Quantile
        spec.aggregations = vec!["cardinality".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.aggregations, vec![AggType::Cardinality]);
    }

    /// sum_over_time is a stateful exact aggregation; exact_required is set.
    #[test]
    fn query_string_exact_required_propagated() {
        let w = Analyzer::new()
            .analyze(qs_only(
                "sum by (service) (sum_over_time(request_bytes[1h]))",
            ))
            .unwrap();
        assert!(w.exact_required, "sum_over_time must set exact_required");
        assert_eq!(w.aggregations, vec![]);
    }

    /// DDSketch quantile φ values are surfaced through the workload.
    #[test]
    fn query_string_quantiles_populated() {
        let w = Analyzer::new()
            .analyze(qs_only(
                "sum by (host) (quantile_over_time(0.5, latency[5m]))",
            ))
            .unwrap();
        assert_eq!(w.quantiles, vec![0.5]);
    }

    /// Existing callers that supply all fields explicitly and omit
    /// query_string continue to work unchanged (backward compatibility).
    #[test]
    fn backward_compat_no_query_string() {
        let w = Analyzer::new().analyze(basic_spec()).unwrap();
        assert_eq!(w.metric_name, "request_latency");
        assert_eq!(w.aggregations, vec![AggType::Quantile]);
        assert_eq!(w.time_window, Duration::from_secs(300));
        assert!(!w.exact_required);
        assert!(w.quantiles.is_empty());
    }

    // ── design.md alignment tests ─────────────────────────────────────────────

    /// Typed `accuracy: Some(Epsilon(0.05))` overrides the legacy
    /// `accuracy_sla: 0.99` (which would translate to `Epsilon(0.01)`),
    /// and the resolved value flows through to `QueryWorkload.accuracy_sla`.
    #[test]
    fn typed_accuracy_overrides_legacy_accuracy_sla() {
        let mut spec = basic_spec();
        spec.accuracy_sla = 0.99; // legacy: ε = 0.01
        spec.accuracy = Some(AccuracyTarget::Epsilon(0.05));
        let w = Analyzer::new().analyze(spec).unwrap();
        // The resolved 1.0 - 0.05 = 0.95 must reach the QueryWorkload, not
        // the legacy 0.99.
        assert!(
            (w.accuracy_sla - 0.95).abs() < 1e-9,
            "got {}",
            w.accuracy_sla
        );
    }

    /// Typed `accuracy: Some(Exact)` clamps the SLA to 1.0 regardless of
    /// the legacy field's value.
    #[test]
    fn typed_accuracy_exact_clamps_to_one() {
        let mut spec = basic_spec();
        spec.accuracy_sla = 0.5;
        spec.accuracy = Some(AccuracyTarget::Exact);
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.accuracy_sla, 1.0);
    }

    /// L1 rejects `(QueryShape::Streaming, DataShape::Batch)` per the
    /// `design.md` §6 cross-product table.
    #[test]
    fn l1_rejects_streaming_over_batch() {
        let mut spec = basic_spec();
        spec.shape = QueryShape::Streaming;
        spec.data = DataShape::Batch;
        let err = Analyzer::new().analyze(spec).unwrap_err().to_string();
        assert!(
            err.contains("Streaming") && err.contains("Batch"),
            "expected the error to name the rejected combination: {err}"
        );
    }

    /// L1 rejects `(QueryShape::Streaming, DataShape::Mutable)` — no
    /// retraction-aware sketches in the catalog yet.
    #[test]
    fn l1_rejects_streaming_over_mutable() {
        let mut spec = basic_spec();
        spec.shape = QueryShape::Streaming;
        spec.data = DataShape::Mutable;
        let err = Analyzer::new().analyze(spec).unwrap_err().to_string();
        assert!(
            err.contains("Streaming") && err.contains("Mutable"),
            "expected the error to name the rejected combination: {err}"
        );
    }

    /// `(QueryShape::Streaming, DataShape::AppendOnlyStream)` — the
    /// canonical streaming case — is accepted.
    #[test]
    fn l1_accepts_streaming_over_append_only_stream() {
        let mut spec = basic_spec();
        spec.shape = QueryShape::Streaming;
        spec.data = DataShape::AppendOnlyStream;
        assert!(Analyzer::new().analyze(spec).is_ok());
    }

    /// JSON without any of the new fields parses correctly via serde —
    /// the existing `/api/v1/plan` HTTP API surface keeps working
    /// byte-for-byte. Fields default to `None` / `OneShot` /
    /// `AppendOnlyStream` per the `#[serde(default)]` annotations.
    #[test]
    fn json_back_compat_omitting_new_fields() {
        let json = r#"{
            "metric_name":   "request_latency",
            "aggregations":  ["quantile"],
            "time_window":   "5m",
            "accuracy_sla":  0.99
        }"#;
        let spec: QuerySpec = serde_json::from_str(json).unwrap();
        assert!(spec.id.is_none());
        assert!(spec.language.is_none());
        assert!(spec.accuracy.is_none());
        assert!(spec.dollars.is_none());
        assert!(spec.deployment_model.is_none());
        assert_eq!(spec.shape, QueryShape::OneShot);
        assert_eq!(spec.data, DataShape::AppendOnlyStream);
        // And the analyzer accepts it.
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.metric_name, "request_latency");
        // Legacy accuracy_sla=0.99 round-trips through resolution
        // (no typed `accuracy` supplied → translate from legacy →
        // Epsilon(0.01) → back to 1 - 0.01 = 0.99).
        assert!(
            (w.accuracy_sla - 0.99).abs() < 1e-9,
            "got {}",
            w.accuracy_sla
        );
    }

    /// JSON *with* the new fields parses correctly — the wire schema
    /// is forward-compatible with callers that supply them. Exercises the
    /// adjacently-tagged `AccuracyTarget` form (`kind` + `value`) and the
    /// internally-tagged `QueryShape::Periodic` form.
    #[test]
    fn json_forward_compat_supplying_new_fields() {
        let json = r#"{
            "metric_name":      "request_latency",
            "aggregations":     ["quantile"],
            "time_window":      "5m",
            "accuracy_sla":     0.5,
            "id":               "q-001",
            "language":         "prom_ql",
            "accuracy":         { "Epsilon": 0.02 },
            "dollars":          0.001,
            "deployment_model": "asaplifecycle",
            "shape":            { "kind": "periodic", "every": { "secs": 60, "nanos": 0 } },
            "data":             "batch"
        }"#;
        let spec: QuerySpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.id.as_ref().unwrap().as_str(), "q-001");
        assert_eq!(spec.language, Some(QueryLanguage::PromQL));
        assert_eq!(spec.accuracy, Some(AccuracyTarget::Epsilon(0.02)));
        assert_eq!(spec.dollars, Some(0.001));
        assert_eq!(spec.deployment_model.as_deref(), Some("asaplifecycle"));
        assert!(matches!(spec.shape, QueryShape::Periodic { .. }));
        assert_eq!(spec.data, DataShape::Batch);
        // Periodic + Batch is accepted at L1 (scheduled batch report row
        // in the design.md cross-product table).
        let w = Analyzer::new().analyze(spec).unwrap();
        // typed `accuracy: Epsilon(0.02)` overrode the legacy 0.5 →
        // resolved accuracy_sla in the workload is 1.0 - 0.02 = 0.98.
        assert!(
            (w.accuracy_sla - 0.98).abs() < 1e-9,
            "got {}",
            w.accuracy_sla
        );
    }
}
