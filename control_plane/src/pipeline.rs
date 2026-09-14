use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::query_parser;
use crate::types::{AccuracyTarget, DataShape, QueryId, QueryLanguage, QueryShape};
use crate::types::{AggType, RegisteredWorkload, SketchType, WorkloadCharacteristics};

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
///    automatically. Explicit semantic fields must agree with the expression;
///    conflicting overrides are rejected before registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    /// Raw PromQL query string to parse (SP-1 automatic extraction).
    /// When provided, metric_name / aggregations / time_window may be omitted
    /// and will be derived from the query.
    #[serde(default)]
    pub query_string: Option<String>,

    /// Metric identity. Required without a query; must match a supplied query.
    #[serde(default)]
    pub metric_name: String,
    #[serde(default)]
    pub label_filters: HashMap<String, String>,
    /// Additional labels the collector must retain; does not rewrite query GROUP BY.
    #[serde(default)]
    pub group_by_labels: Vec<String>,
    /// Field-only aggregation ("quantile", "cardinality", "frequency").
    /// Required when `query_string` is absent.
    #[serde(default)]
    pub aggregations: Vec<String>,
    /// Time window (e.g. "5m"). Required without a query; otherwise must agree.
    #[serde(default)]
    pub time_window: String,
    #[serde(default)]
    pub repeat_every: Option<String>,
    pub accuracy_sla: f64,
    pub latency_sla: Option<String>,
    /// Optional implementation constraint, still subject to Planner legality.
    pub sketch_type: Option<SketchType>,
    /// Observable data-stream characteristics used for delta / raw-vs-sketch
    /// bandwidth comparison. Omit to use conservative defaults.
    #[serde(default)]
    pub workload: WorkloadCharacteristics,

    /// Optional stable registration identifier, preserved as metadata.
    #[serde(default)]
    pub id: Option<QueryId>,

    /// This metric-registration endpoint accepts PromQL; other languages use
    /// their dedicated compilation paths.
    #[serde(default)]
    pub language: Option<QueryLanguage>,

    /// Typed accuracy target. When present, takes precedence over the
    /// legacy `accuracy_sla: f64` field. When absent, the legacy field
    /// is converted to `Epsilon(1.0 - accuracy_sla)` (or `Exact` when
    /// `accuracy_sla == 1.0`).
    #[serde(default)]
    pub accuracy: Option<AccuracyTarget>,

    /// Reserved compatibility field. Explicit dollar constraints are rejected
    /// because metric registration does not implement them.
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

impl Default for Analyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn analyze(&self, spec: QuerySpec) -> anyhow::Result<RegisteredWorkload> {
        let accuracy =
            crate::types::resolve_accuracy_target(spec.accuracy.as_ref(), spec.accuracy_sla)
                .map_err(|error| anyhow!(error))?;

        // ── design.md L1: shape × data cross-product check ─────────────────
        // The cross-product table in design.md §6 enumerates which
        // (shape, data) combinations the planner accepts. The two
        // hard rejections are at L1 because they have no semantically
        // valid plan: a streaming query over a static dataset, and a
        // streaming query over a mutable relation (no retraction-aware
        // sketches in the catalog yet). Canonical conversion below also checks
        // which recurrence and data shapes this deployment path can represent.
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

        // ── Step 1: parse query_string if provided ─────────────────────────
        // Parsing and downstream binding receive this same resolved target.
        let parsed = spec
            .query_string
            .as_deref()
            .map(|q| query_parser::parse_query(q, accuracy.clone()))
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

        // ── Step 5: resolve filters; conflicts are checked against the query ─
        // Collector retention labels remain independent deployment options.
        let parsed_filters: HashMap<String, String> = parsed
            .as_ref()
            .map(|p| p.label_filters.clone())
            .unwrap_or_default();

        let merged_filters: HashMap<String, String> = {
            let mut m = parsed_filters;
            m.extend(spec.label_filters.clone()); // explicit overrides parsed
            m
        };

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

        use crate::registered_workload::{declared, DeploymentOptions};
        use planner_types::workload::*;
        let query = if let Some(query) = &spec.query_string {
            let p = parsed.as_ref().expect("parsed above");
            anyhow::ensure!(metric_name == p.metric_name && time_window == p.time_window
                && aggregations == p.aggregations && merged_filters == p.label_filters,
                "explicit overrides conflict with query_string; update the query expression instead");
            query.clone()
        } else {
            anyhow::ensure!(
                aggregations.len() == 1,
                "field-only input requires one aggregation"
            );
            let mut filters: Vec<_> = merged_filters
                .iter()
                .map(|(k, v)| format!("{k}={}", serde_json::to_string(v).expect("string")))
                .collect();
            filters.sort();
            let selector = if filters.is_empty() {
                metric_name.clone()
            } else {
                format!("{}{{{}}}", metric_name, filters.join(","))
            };
            match aggregations[0] {
                AggType::Quantile => format!(
                    "quantile_over_time(0.99, {selector}[{}s])",
                    time_window.as_secs()
                ),
                AggType::Cardinality => {
                    format!("distinct_over_time({selector}[{}s])", time_window.as_secs())
                }
                AggType::Frequency => {
                    format!("count_over_time({selector}[{}s])", time_window.as_secs())
                }
            }
        };
        anyhow::ensure!(
            spec.language.is_none()
                || matches!(spec.language, Some(crate::types::QueryLanguage::PromQl)),
            "metric registration requires PromQL"
        );
        anyhow::ensure!(
            spec.dollars.is_none(),
            "dollars constraints are not supported by metric registration"
        );
        let cadence = match spec.shape {
            QueryShape::Periodic { every } => {
                anyhow::ensure!(repeat_every.is_none_or(|r| r == every), "conflicting repetition intervals");
                Some(every)
            },
            QueryShape::Streaming => return Err(anyhow!("streaming demand without a fixed cadence is not supported; specify periodic demand")),
            QueryShape::OneShot => repeat_every,
        };
        let requirements = QueryRequirements {
            accuracy: AccuracyRequirement::Explicit(accuracy),
            response_latency: latency_sla
                .map(|d| LatencyRequirement::ExplicitMaxMs(d.as_secs_f64() * 1000.0))
                .unwrap_or(LatencyRequirement::Unspecified),
        };
        let time_selection = TimeSelection {
            scope: QueryTimeScope::RealTime,
            lookback: crate::registered_workload::metric_query_range(&query)?
                .map(|range| u64::try_from(range.as_millis()).map(DurationMs))
                .transpose()?,
            as_of: None,
        };
        let (query_batch, repeating_queries) = if let Some(cadence) = cadence {
            anyhow::ensure!(
                cadence.subsec_nanos().is_multiple_of(1_000_000),
                "repetition interval requires whole milliseconds"
            );
            let interval = u32::try_from(cadence.as_millis())
                .context("repetition interval exceeds u32 milliseconds")?;
            anyhow::ensure!(interval > 0, "repetition interval must be positive");
            (
                None,
                Some(vec![RepeatingEntry {
                    query: Query(query),
                    demand: RepeatedDemand::FixedInterval(RepetitionInterval(interval)),
                    requirements,
                    predictability: Predictability::Predictable { known_at: None },
                    time_selection,
                }]),
            )
        } else {
            (
                Some(vec![BatchEntry {
                    query: Query(query),
                    requirements,
                    predictability: Predictability::AdHoc,
                    invocations: 1,
                    execute_at: None,
                    time_selection,
                }]),
                None,
            )
        };
        let wc = &spec.workload;
        let rate = wc.series_count as f64 * wc.samples_per_sec_per_series;
        anyhow::ensure!(
            wc.samples_per_sec_per_series.is_finite()
                && wc.samples_per_sec_per_series >= 0.0
                && rate.is_finite(),
            "sample rate must be finite and nonnegative"
        );
        let arrival = match spec.data {
            DataShape::Batch => DataArrival::AtRest,
            DataShape::AppendOnlyStream => DataArrival::ContinuouslyIngesting,
            DataShape::Mixed => DataArrival::Mixed,
            DataShape::Mutable => {
                return Err(anyhow!(
                    "mutable data is not supported by metric registration"
                ))
            }
        };
        let data_workload = Some(DataWorkload {
            arrival,
            ingestion_rate: declared(Rate(if matches!(spec.data, DataShape::Batch) {
                0.0
            } else {
                rate
            })),
            input_cardinality: declared(wc.series_count),
            distribution: declared(match wc.data_distribution {
                crate::types::DataDistribution::Zipf => DataDistribution::Zipf,
                crate::types::DataDistribution::Uniform => DataDistribution::Uniform,
                crate::types::DataDistribution::Bursty => DataDistribution::Bursty,
            }),
            ingestion_volume: Evidence::default(),
        });
        RegisteredWorkload::new(
            QueryWorkload {
                language: QueryLanguage::PromQL,
                query_batch,
                repeating_queries,
                data_workload,
            },
            DeploymentOptions {
                sketch_type_override: spec.sketch_type,
                query_id: spec.id,
                deployment_model: spec.deployment_model,
                retained_labels: dedup_dims(&spec.group_by_labels, &[]),
                bytes_per_raw_sample: wc.bytes_per_raw_sample,
                distinct_keys_per_window: wc.distinct_keys_per_window,
                memory_budget_bytes: wc.memory_budget_bytes,
            },
        )
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
            let multiplier = match ch {
                'h' => 3600,
                'm' => 60,
                's' => 1,
                _ => return Err(anyhow!("unknown unit {:?} in duration {:?}", ch, s)),
            };
            total_secs = n
                .checked_mul(multiplier)
                .and_then(|part| total_secs.checked_add(part))
                .ok_or_else(|| anyhow!("duration overflow in {:?}", s))?;
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
        assert_eq!(w.metric_name(), "request_latency");
        assert!(((1.0 - w.error_bound()) - 0.01).abs() < 1e-12);
        assert_eq!(w.time_window(), Duration::from_secs(300));
        assert_eq!(w.repeat_every(), Some(Duration::from_secs(60)));
        assert_eq!(w.latency_sla(), Some(Duration::from_secs(600)));
        assert_eq!(w.aggregations(), vec![AggType::Quantile]);
    }

    #[test]
    fn dimension_merge_dedup() {
        let mut spec = basic_spec();
        spec.label_filters = [
            ("service".into(), "api".into()),
            ("host_name".into(), "h1".into()),
        ]
        .into();
        spec.group_by_labels = vec!["host_name".into(), "region".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        for dim in &["host_name", "region", "service"] {
            assert!(
                w.group_by_labels().contains(&dim.to_string()),
                "missing {dim}"
            );
        }
        // host.name must appear exactly once after dedup
        assert_eq!(
            w.group_by_labels()
                .iter()
                .filter(|d| d.as_str() == "host_name")
                .count(),
            1
        );
    }

    #[test]
    fn multiple_aggregations() {
        let mut spec = basic_spec();
        spec.aggregations = vec!["cardinality".into(), "frequency".into()];
        assert!(Analyzer::new().analyze(spec).is_err());
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
        // L1 adoption (design-target-architecture.md Part B), accepted
        // behavior change: `sum by (host) (quantile_over_time(...))` no
        // longer fuses into one shape -- it's genuinely two operations
        // (sum the per-series quantiles, grouped by host), so the outer
        // `Sum` now also contributes to this flat summary and flips
        // `exact_required` (an outer exact fold over sketch-derived
        // quantile values is real complexity the old fused behavior
        // papered over, not something a sketch alone answers).
        let w = Analyzer::new()
            .analyze(qs_only(
                "sum by (host) (quantile_over_time(0.99, latency[5m]))",
            ))
            .unwrap();
        assert_eq!(w.metric_name(), "latency");
        assert_eq!(w.aggregations(), vec![AggType::Quantile]);
        assert_eq!(w.time_window(), Duration::from_secs(300));
        assert_eq!(w.quantiles(), vec![0.99]);
        assert!(w.exact_required());
    }

    /// A conflicting metric field cannot change only the stored projection.
    #[test]
    fn explicit_metric_name_overrides_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))");
        spec.metric_name = "my_custom_metric".into();
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    /// A conflicting window cannot disagree with the canonical expression.
    #[test]
    fn explicit_time_window_overrides_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))");
        spec.time_window = "1h".into();
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    /// A conflicting aggregation cannot replace canonical query semantics.
    #[test]
    fn explicit_aggregations_override_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))"); // → Quantile
        spec.aggregations = vec!["cardinality".into()];
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    /// sum_over_time is a stateful exact aggregation; exact_required is set.
    #[test]
    fn query_string_exact_required_propagated() {
        let w = Analyzer::new()
            .analyze(qs_only(
                "sum by (service) (sum_over_time(request_bytes[1h]))",
            ))
            .unwrap();
        assert!(w.exact_required(), "sum_over_time must set exact_required");
        assert_eq!(w.aggregations(), vec![]);
    }

    /// DDSketch quantile φ values are surfaced through the workload.
    #[test]
    fn query_string_quantiles_populated() {
        let w = Analyzer::new()
            .analyze(qs_only(
                "sum by (host) (quantile_over_time(0.5, latency[5m]))",
            ))
            .unwrap();
        assert_eq!(w.quantiles(), vec![0.5]);
    }

    /// Existing callers that supply all fields explicitly and omit
    /// query_string continue to work unchanged (backward compatibility).
    #[test]
    fn backward_compat_no_query_string() {
        let w = Analyzer::new().analyze(basic_spec()).unwrap();
        assert_eq!(w.metric_name(), "request_latency");
        assert_eq!(w.aggregations(), vec![AggType::Quantile]);
        assert_eq!(w.time_window(), Duration::from_secs(300));
        assert!(!w.exact_required());
        assert_eq!(w.quantiles(), vec![0.99]);
    }

    // ── design.md alignment tests ─────────────────────────────────────────────

    /// Typed `accuracy: Some(Epsilon(0.05))` overrides the legacy
    /// `accuracy_sla: 0.99` (which would translate to `Epsilon(0.01)`),
    /// and the resolved value flows through to `RegisteredWorkload.accuracy_sla`.
    #[test]
    fn typed_accuracy_overrides_legacy_accuracy_sla() {
        let mut spec = basic_spec();
        spec.accuracy_sla = 0.99; // legacy: ε = 0.01
        spec.accuracy = Some(AccuracyTarget::Epsilon(0.05));
        let w = Analyzer::new().analyze(spec).unwrap();
        // The resolved 1.0 - 0.05 = 0.95 must reach the RegisteredWorkload, not
        // the legacy 0.99.
        assert!(
            ((1.0 - w.error_bound()) - 0.95).abs() < 1e-9,
            "got {}",
            (1.0 - w.error_bound())
        );
    }

    /// Typed `accuracy: Some(Exact)` clamps the SLA to 1.0 regardless of
    /// the legacy field's value.
    /// A typed confidence requirement is validated without clamping or dropping delta.
    #[test]
    fn invalid_typed_accuracy_is_rejected() {
        for target in [
            AccuracyTarget::Epsilon(f64::NAN),
            AccuracyTarget::Epsilon(-0.1),
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.05,
                delta: 0.0,
            },
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.05,
                delta: 1.0,
            },
        ] {
            let mut spec = basic_spec();
            spec.accuracy = Some(target);
            assert!(Analyzer::new().analyze(spec).is_err());
        }
    }

    /// Typed requirements survive public analysis and determine the actual bound DDS size.
    #[test]
    fn typed_accuracy_survives_analysis_and_binding() {
        use crate::physical::post_asap::deployment_expr::{PhysicalExpr, PostAsapPlan};
        use planner_types::post_asap::{SketchParams, SummaryExpr, SummaryFamilyType};
        for target in [
            AccuracyTarget::Epsilon(0.05),
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.05,
                delta: 0.001,
            },
            AccuracyTarget::Exact,
        ] {
            let mut spec = basic_spec();
            spec.accuracy_sla = 0.2;
            spec.accuracy = Some(target.clone());
            let workload = Analyzer::new().analyze(spec).unwrap();
            assert_eq!(workload.accuracy(), target);
            // Canonical requirements have no independently mutable scalar mirror.
            let bound = crate::physical::workload_planner::bind_workload_typed(&workload);
            if target == AccuracyTarget::Exact {
                assert!(
                    bound.is_none(),
                    "exact quantile must not become an approximate sketch"
                );
                assert_eq!(
                    crate::physical::workload_planner::DeploymentPlanCompiler::new()
                        .plan(&workload)
                        .agent_config
                        .output_mode,
                    crate::types::OutputMode::Raw
                );
            } else {
                let Some(PhysicalExpr::Committed(PostAsapPlan::Summary(root))) = bound else {
                    panic!("expected bound quantile")
                };
                let SummaryExpr::SummaryEstimate { summary_input, .. } = &root.expr else {
                    panic!("expected quantile readout")
                };
                let SummaryExpr::SummaryAgg {
                    family: SummaryFamilyType::Sketch(kind, _),
                    ..
                } = &summary_input.expr
                else {
                    panic!("expected sketch state")
                };
                let SketchParams::DDSketch { alpha } = kind.params() else {
                    panic!("expected DDS")
                };
                assert!((alpha - 0.05).abs() < 1e-12, "actual alpha={alpha}");
            }
        }
    }

    #[test]
    fn typed_accuracy_exact_clamps_to_one() {
        let mut spec = basic_spec();
        spec.accuracy_sla = 0.5;
        spec.accuracy = Some(AccuracyTarget::Exact);
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!((1.0 - w.error_bound()), 1.0);
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

    /// Continuous demand needs a supported cadence before metric registration.
    #[test]
    fn rejects_streaming_demand_without_fixed_cadence() {
        let mut spec = basic_spec();
        spec.shape = QueryShape::Streaming;
        spec.data = DataShape::AppendOnlyStream;
        assert!(Analyzer::new().analyze(spec).is_err());
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
        assert_eq!(w.metric_name(), "request_latency");
        // Legacy accuracy_sla=0.99 round-trips through resolution
        // (no typed `accuracy` supplied → translate from legacy →
        // Epsilon(0.01) → back to 1 - 0.01 = 0.99).
        assert!(
            ((1.0 - w.error_bound()) - 0.99).abs() < 1e-9,
            "got {}",
            (1.0 - w.error_bound())
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
            "dollars":          null,
            "deployment_model": "asaplifecycle",
            "shape":            { "kind": "periodic", "every": { "secs": 60, "nanos": 0 } },
            "data":             "batch"
        }"#;
        let spec: QuerySpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.id.as_ref().unwrap().0.as_str(), "q-001");
        assert_eq!(spec.language, Some(QueryLanguage::PromQl));
        assert_eq!(spec.accuracy, Some(AccuracyTarget::Epsilon(0.02)));
        assert_eq!(spec.dollars, None);
        assert_eq!(spec.deployment_model.as_deref(), Some("asaplifecycle"));
        assert!(matches!(spec.shape, QueryShape::Periodic { .. }));
        assert_eq!(spec.data, DataShape::Batch);
        // Periodic + Batch is accepted at L1 (scheduled batch report row
        // in the design.md cross-product table).
        let w = Analyzer::new().analyze(spec).unwrap();
        // typed `accuracy: Epsilon(0.02)` overrode the legacy 0.5 →
        // resolved accuracy_sla in the workload is 1.0 - 0.02 = 0.98.
        assert!(
            ((1.0 - w.error_bound()) - 0.98).abs() < 1e-9,
            "got {}",
            (1.0 - w.error_bound())
        );
    }
}
