//! Per-metric storage-backend routing table consulted by the HTTP query
//! handler at request time.
//!
//! ## Why this exists
//!
//! Phase-5 (PR #87) wired the `EngineRouter` into the HTTP query handler,
//! but the per-metric `StorageBackend` axis was sourced from
//! `StreamingConfig::storage_backend()` — a single field that applies to
//! the entire streaming config. In production deploys (the
//! `precompute_engine` binary loading `backend-streaming.yaml`) the
//! field decodes via `Self::new(...)` which always defaults to
//! `SketchWarmTier`, so the handler always took the
//! `SimpleEngine`-direct-dispatch branch and the `EngineRouter` was
//! effectively bypassed for every query — the `data_source:
//! gorilla_archive` info-line never landed on cold-archive responses
//! even when the chunks were on disk in MinIO.
//!
//! The fix lives **outside** the streaming pipeline: the streaming
//! engine on the OTLP-ingest path never sees Gorilla data (the
//! `gorillas3processor` writes chunks directly to S3), so there is
//! nothing for `StreamingConfig::from_yaml_data` to learn. What we
//! actually need is a tiny standalone routing table — one entry per
//! metric whose storage backend differs from the default — that the
//! HTTP handler consults to pick the right engine for each query.
//!
//! ## v7: dual-routing per metric
//!
//! v6.1 surfaced an architectural gap: routing one metric to one engine
//! forces an exclusive trade-off between criterion ④ (warm-tier
//! accuracy) and criterion ⑤ (cold-fallback). Every quantile/sum-by
//! query on `http_requests_total` had to go to either the warm tier
//! (so the accuracy reducer could compute relative error) or the
//! archive (so the `data_source: gorilla_archive` info-line landed on
//! the cold-fallback probe). v7 closes this by letting one metric have
//! multiple targets, each with an optional query-shape filter; the
//! HTTP handler inspects the parsed PromQL and picks the matching
//! target. Predictable / planned queries (quantile, sum_over_time)
//! land on the warm tier; ad-hoc / post-hoc queries
//! (count, topk, rate-post-hoc) route to the cold archive.
//!
//! ## Schema
//!
//! Two compatible shapes are accepted. The single-target form is
//! preserved verbatim from v6.1 so existing deploys keep working
//! unchanged:
//!
//! ```yaml
//! # v6.1 form (single-target):
//! default: sketch_warm_tier
//! metrics:
//!   audit_events: gorilla_s3_archive
//! ```
//!
//! ```yaml
//! # v7 form (multi-target with query-shape selection):
//! default: sketch_warm_tier
//! routes:
//!   - metric: http_requests_total
//!     targets:
//!       - backend: sketch_warm_tier
//!         # default — predictable / planned queries land here
//!       - backend: gorilla_s3_archive
//!         applies_to_query_shape: [count, topk, rate_post_hoc]
//!   - metric: http_freshness_probe_warm
//!     targets:
//!       - backend: sketch_warm_tier
//!   - metric: http_freshness_probe_archive
//!     targets:
//!       - backend: gorilla_s3_archive
//! ```
//!
//! The two shapes can be mixed in the same YAML — metrics under
//! `metrics:` keep the old single-target semantics; metrics under
//! `routes:` use the new list-of-targets semantics. A metric listed
//! in BOTH wins from `routes:` (multi-target overrides single-target).
//!
//! Valid `StorageBackend` values mirror the snake-cased serde tags on
//! `asap_types::StorageBackend`: `sketch_warm_tier`,
//! `gorilla_s3_archive`, `cold_jsonl_fallback`, `double_write`.
//!
//! Loaded once at backend startup (CLI flag `--backend-storage-routing`
//! on `precompute_engine`) and stored in `AppState`. Lookup is
//! O(metric-name-hash); a query that doesn't match any entry falls back
//! to `default` (which itself falls back to `SketchWarmTier`).
//!
//! ## Out of scope
//!
//! * Hot reload — the controller's plan-push is the long-term answer
//!   for per-metric routing; this YAML layer is the bridge that
//!   unblocks issue #46 criteria ④/⑤/⑥ until the plan-push lands.
//! * Per-`(metric, statistic, accuracy)` granularity — `StorageBackend`
//!   already encodes the `DoubleWrite` axis the cost-aware dispatcher
//!   uses to pick warm-vs-archive per query.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use asap_types::StorageBackend;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Query-shape taxonomy
// ---------------------------------------------------------------------------

/// A coarse-grained classification of an incoming PromQL query. The
/// HTTP handler extracts this from the parsed AST and consults the
/// routing table's `applies_to_query_shape` filters to pick a target.
///
/// The shapes intentionally mirror the v7 spec's
/// `[count, topk, rate_post_hoc]` enumeration — each is a PromQL
/// shape the cold-archive engine answers natively, and which the
/// warm-tier sketch path either can't serve at all (count over an
/// approximate sketch is misleading) or serves with worse precision
/// than the archive (rate post-hoc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryShape {
    /// `count(<metric>{...})` — series count after label predicates.
    /// Cold archive serves exactly via postings index; warm tier has
    /// no compatible aggregation (a CMS doesn't track per-series
    /// existence).
    Count,
    /// `topk(k, ...)` — top-k by value. Cold archive answers by
    /// streaming through chunk samples and tracking k-largest; warm
    /// tier needs a `CountMinSketchWithHeap` to answer at all.
    Topk,
    /// `rate(<metric>[<range>])` — per-second rate over a range.
    /// Marked "post-hoc" because the warm tier's pre-computed
    /// `Increase` aggregation answers `rate` natively for known
    /// queries, so this shape only kicks in for ad-hoc rate queries
    /// the controller didn't pre-plan for.
    RatePostHoc,
    /// `quantile_over_time(φ, <metric>[<range>])` and
    /// `quantile(...)` aggregations. Warm tier serves natively via
    /// DDSketch / KLL accumulators; cold archive can also serve but
    /// at higher latency.
    Quantile,
    /// `sum_over_time(...)` and `sum(...) by (...)`. Warm tier
    /// serves via a sum-typed accumulator.
    Sum,
    /// `last_over_time(<metric>[<range>])`. Used by the v6
    /// freshness probes — warm tier serves via a counter-typed
    /// accumulator (Change B), archive serves by selecting the
    /// most-recent sample in each chunk.
    LastOverTime,
    /// Anything else — `min/max_over_time`, `count_over_time`,
    /// `avg_over_time`, etc. Lets the routing table register
    /// targets that catch the long tail without enumerating every
    /// PromQL function.
    Other,
}

impl QueryShape {
    /// Stable string tag used in YAML.
    pub fn as_str(self) -> &'static str {
        match self {
            QueryShape::Count => "count",
            QueryShape::Topk => "topk",
            QueryShape::RatePostHoc => "rate_post_hoc",
            QueryShape::Quantile => "quantile",
            QueryShape::Sum => "sum",
            QueryShape::LastOverTime => "last_over_time",
            QueryShape::Other => "other",
        }
    }
}

/// Classify a parsed PromQL expression into a [`QueryShape`]. Walks
/// the AST and returns the first shape that matches any node — so
/// `topk(5, sum by (zone) (rate(http_requests_total[5m])))` is
/// classified as `Topk` (the outermost shape wins).
///
/// `RatePostHoc` is conservative: any `rate(...)` call surfaces as
/// `RatePostHoc`. The routing-table consumer can decide whether to
/// honour that or fall through to the default target.
pub fn classify_query_shape(expr: &promql_parser::parser::Expr) -> QueryShape {
    use promql_parser::parser::Expr;
    match expr {
        // Aggregations are the outermost shape — `topk(...)` wins
        // over any inner call. We use the operator's Display impl
        // (the canonical PromQL keyword: "sum", "count", "topk",
        // "quantile", ...) — Debug-formatting the underlying
        // `TokenType` returns the numeric token id, not the
        // keyword.
        Expr::Aggregate(agg) => {
            let op = agg.op.to_string().to_lowercase();
            if op == "topk" || op == "bottomk" {
                QueryShape::Topk
            } else if op == "count" || op == "count_values" {
                QueryShape::Count
            } else if op == "quantile" {
                QueryShape::Quantile
            } else if op == "sum" {
                // `sum by (...) (...)` — recurse on the inner
                // expression; if the inner is a `rate(...)` the
                // post-hoc rate path wins.
                let inner = classify_query_shape(&agg.expr);
                if matches!(inner, QueryShape::RatePostHoc) {
                    QueryShape::RatePostHoc
                } else {
                    QueryShape::Sum
                }
            } else {
                // min/max/avg/group/stddev/stdvar/...
                classify_query_shape(&agg.expr)
            }
        }
        Expr::Call(call) => {
            let name = call.func.name.to_lowercase();
            if name == "rate" || name == "irate" {
                QueryShape::RatePostHoc
            } else if name == "quantile_over_time" {
                QueryShape::Quantile
            } else if name == "sum_over_time" {
                QueryShape::Sum
            } else if name == "count_over_time" {
                QueryShape::Count
            } else if name == "last_over_time" {
                QueryShape::LastOverTime
            } else {
                QueryShape::Other
            }
        }
        Expr::Paren(p) => classify_query_shape(&p.expr),
        Expr::Unary(u) => classify_query_shape(&u.expr),
        Expr::Binary(bin) => {
            // Pick whichever side has the more-specific shape; a
            // `rate(...)` on either side is enough to mark
            // RatePostHoc.
            let l = classify_query_shape(&bin.lhs);
            if !matches!(l, QueryShape::Other) {
                l
            } else {
                classify_query_shape(&bin.rhs)
            }
        }
        Expr::Subquery(sq) => classify_query_shape(&sq.expr),
        // Bare vector / matrix selectors — no function applied.
        _ => QueryShape::Other,
    }
}

// ---------------------------------------------------------------------------
// On-disk YAML schema
// ---------------------------------------------------------------------------

/// One target in the v7 multi-target form.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoutingTargetYaml {
    backend: StorageBackend,
    /// Optional list of query shapes this target applies to. When
    /// `None`, the target is the default — it catches any shape the
    /// other targets didn't claim. When `Some(list)`, the target
    /// only fires when the incoming query's shape is in `list`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applies_to_query_shape: Option<Vec<QueryShape>>,
}

/// One row in the v7 multi-target form.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RouteYaml {
    metric: String,
    targets: Vec<RoutingTargetYaml>,
}

/// On-disk YAML schema. Public only so the loader / tests can build it
/// from literals; runtime callers should go through
/// [`BackendStorageRouting`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BackendStorageRoutingYaml {
    /// Fallback storage backend for any metric not explicitly listed.
    /// Optional; defaults to `SketchWarmTier`.
    #[serde(default)]
    default: StorageBackend,
    /// v6.1 form — per-metric overrides keyed by the bare metric name
    /// (no labels). Each value is a single `StorageBackend`. A metric
    /// listed here keeps the v6.1 semantics: every query for the
    /// metric routes to the named backend regardless of shape.
    #[serde(default)]
    metrics: HashMap<String, StorageBackend>,
    /// v7 form — per-metric overrides as a list of `(backend,
    /// applies_to_query_shape)` targets. The HTTP handler inspects
    /// the parsed PromQL, classifies it via [`classify_query_shape`],
    /// and picks the first target whose `applies_to_query_shape`
    /// either is `None` (default) or contains the query's shape.
    /// Falls back to the first target on no match.
    #[serde(default)]
    routes: Vec<RouteYaml>,
}

// ---------------------------------------------------------------------------
// In-memory routing table
// ---------------------------------------------------------------------------

/// One target in the in-memory routing table — the runtime form of
/// [`RoutingTargetYaml`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingTarget {
    /// Storage backend the HTTP handler should dispatch to.
    pub backend: StorageBackend,
    /// Optional list of query shapes this target claims. `None` =
    /// default (always fires); `Some(list)` = this target only fires
    /// when the query's classified shape is in `list`.
    pub applies_to_query_shape: Option<Vec<QueryShape>>,
}

impl RoutingTarget {
    /// Build a target that applies to every query shape (the v6.1
    /// single-target form).
    pub fn always(backend: StorageBackend) -> Self {
        Self {
            backend,
            applies_to_query_shape: None,
        }
    }

    /// Build a target that only applies to the listed shapes.
    pub fn for_shapes(backend: StorageBackend, shapes: Vec<QueryShape>) -> Self {
        Self {
            backend,
            applies_to_query_shape: Some(shapes),
        }
    }
}

/// In-memory routing table consulted by the HTTP handler at request
/// time. Build via [`Self::from_yaml_file`] / [`Self::from_yaml_str`]
/// or [`Self::empty`] (everything routes to `SketchWarmTier`).
#[derive(Debug, Clone)]
pub struct BackendStorageRouting {
    default: StorageBackend,
    /// For each metric, the ordered list of targets the HTTP handler
    /// walks to pick a backend. v6.1 single-target rows come in as a
    /// single-element vec with `applies_to_query_shape: None`.
    metrics: HashMap<String, Vec<RoutingTarget>>,
}

impl BackendStorageRouting {
    /// Build an empty router — every metric resolves to
    /// `SketchWarmTier`. Equivalent to "no routing config at all" and
    /// preserves pre-Phase-5 dispatch (`SimpleEngine` direct path).
    pub fn empty() -> Self {
        Self {
            default: StorageBackend::default(),
            metrics: HashMap::new(),
        }
    }

    /// Construct directly from an explicit map. Used by tests; production
    /// callers go through [`Self::from_yaml_file`].
    pub fn new(default: StorageBackend, metrics: HashMap<String, Vec<RoutingTarget>>) -> Self {
        Self { default, metrics }
    }

    /// Construct from the v6.1 single-target map shape. Each entry
    /// maps to a single-target list with no query-shape filter.
    /// Preserved for tests and existing call sites; new code should
    /// use [`Self::new`] with explicit `RoutingTarget`s.
    pub fn new_from_single_targets(
        default: StorageBackend,
        metrics: HashMap<String, StorageBackend>,
    ) -> Self {
        let metrics = metrics
            .into_iter()
            .map(|(k, v)| (k, vec![RoutingTarget::always(v)]))
            .collect();
        Self { default, metrics }
    }

    /// Parse YAML text. See module docs for the schema.
    pub fn from_yaml_str(text: &str) -> Result<Self> {
        let parsed: BackendStorageRoutingYaml =
            serde_yaml::from_str(text).context("failed to parse backend-storage-routing YAML")?;

        // Start from the v6.1 single-target map.
        let mut metrics: HashMap<String, Vec<RoutingTarget>> = parsed
            .metrics
            .into_iter()
            .map(|(k, v)| (k, vec![RoutingTarget::always(v)]))
            .collect();

        // Overlay v7 multi-target rows. A metric in BOTH wins from
        // `routes:` (multi-target overrides single-target).
        for row in parsed.routes {
            if row.targets.is_empty() {
                anyhow::bail!(
                    "backend-storage-routing: metric '{}' has empty targets list",
                    row.metric,
                );
            }
            let targets = row
                .targets
                .into_iter()
                .map(|t| RoutingTarget {
                    backend: t.backend,
                    applies_to_query_shape: t.applies_to_query_shape,
                })
                .collect();
            metrics.insert(row.metric, targets);
        }

        Ok(Self {
            default: parsed.default,
            metrics,
        })
    }

    /// Read + parse a YAML file. Returns the populated router on
    /// success. The caller is expected to log the entry count at
    /// startup so operators can spot-check the deployment.
    pub fn from_yaml_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read backend-storage-routing YAML: {:?}", path))?;
        let routing = Self::from_yaml_str(&text)?;
        info!(
            path = %path.display(),
            default = ?routing.default,
            entries = routing.metrics.len(),
            multi_target_entries = routing.metrics.values().filter(|t| t.len() > 1).count(),
            "Loaded backend-storage-routing YAML",
        );
        Ok(routing)
    }

    /// Look up the storage backend for `metric_name`, ignoring query
    /// shape. Walks the metric's target list and returns the first
    /// target's backend (the v6.1 default-target slot). Falls back
    /// to the table's `default` when the metric is not listed.
    ///
    /// Exists for v6.1 callers that haven't been threaded with a
    /// parsed PromQL query. New code on the dual-routing path
    /// should call [`Self::lookup_with_shape`].
    pub fn lookup(&self, metric_name: &str) -> StorageBackend {
        match self.metrics.get(metric_name) {
            Some(targets) => {
                let backend = targets
                    .first()
                    .map(|t| t.backend)
                    .unwrap_or(self.default);
                debug!(
                    metric = metric_name,
                    backend = ?backend,
                    target_count = targets.len(),
                    "backend-storage-routing: lookup (no shape) — picked first target",
                );
                backend
            }
            None => {
                debug!(
                    metric = metric_name,
                    default = ?self.default,
                    "backend-storage-routing: no override, using default",
                );
                self.default
            }
        }
    }

    /// Look up the storage backend for `(metric_name, query_shape)`.
    ///
    /// Walks the metric's target list and returns the backend of the
    /// first target whose `applies_to_query_shape` either is `None`
    /// (default) or contains `shape`. If no target matches (empty
    /// or all filters miss), returns the first target's backend
    /// (matches v6.1's single-target semantics for the default
    /// slot). Falls back to `self.default` when the metric is not
    /// listed.
    ///
    /// Selection ordering: v7 puts the *default* target (no filter)
    /// first in the YAML, then the shape-specific overrides; this
    /// matches v6.1 behaviour for callers that don't supply a
    /// shape, but lets shape-specific lookups skip past the default
    /// slot if any later target's filter matches `shape`. The
    /// implementation walks the list in two passes:
    ///   1. First, prefer a target whose filter explicitly includes
    ///      `shape` — this lets a `[count, topk, rate_post_hoc]`
    ///      target win over the default warm-tier slot for those
    ///      shapes.
    ///   2. If no shape-specific target matches, fall back to the
    ///      first target with `applies_to_query_shape: None`
    ///      (the default slot).
    ///   3. If even that's missing, use the first target.
    pub fn lookup_with_shape(&self, metric_name: &str, shape: QueryShape) -> StorageBackend {
        match self.metrics.get(metric_name) {
            Some(targets) => {
                // Pass 1: explicit shape match.
                for t in targets {
                    if let Some(list) = &t.applies_to_query_shape {
                        if list.contains(&shape) {
                            debug!(
                                metric = metric_name,
                                shape = ?shape,
                                backend = ?t.backend,
                                "backend-storage-routing: shape-specific match",
                            );
                            return t.backend;
                        }
                    }
                }
                // Pass 2: default slot.
                for t in targets {
                    if t.applies_to_query_shape.is_none() {
                        debug!(
                            metric = metric_name,
                            shape = ?shape,
                            backend = ?t.backend,
                            "backend-storage-routing: default-slot fallback",
                        );
                        return t.backend;
                    }
                }
                // Pass 3: the first target, regardless of filter
                // (only reachable when the metric only has
                // shape-specific targets and none matched).
                let backend = targets
                    .first()
                    .map(|t| t.backend)
                    .unwrap_or(self.default);
                debug!(
                    metric = metric_name,
                    shape = ?shape,
                    backend = ?backend,
                    "backend-storage-routing: no match; first-target fallback",
                );
                backend
            }
            None => {
                debug!(
                    metric = metric_name,
                    shape = ?shape,
                    default = ?self.default,
                    "backend-storage-routing: metric unlisted, using default",
                );
                self.default
            }
        }
    }

    /// Read-only view of the configured default. Tests use this; the
    /// HTTP handler goes through `lookup` / `lookup_with_shape`.
    pub fn default_backend(&self) -> StorageBackend {
        self.default
    }

    /// Number of explicit per-metric overrides. Tests / operator
    /// tooling.
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// `true` iff no per-metric overrides are configured. Equivalent to
    /// `Self::empty()` but preserves a custom `default`.
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }

    /// Number of targets registered for `metric_name`. Returns 0 for
    /// unlisted metrics. Tests use this to assert dual-routing is
    /// wired correctly.
    pub fn target_count(&self, metric_name: &str) -> usize {
        self.metrics.get(metric_name).map(|t| t.len()).unwrap_or(0)
    }
}

impl Default for BackendStorageRouting {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_router_routes_everything_to_warm_tier() {
        let r = BackendStorageRouting::empty();
        assert_eq!(r.lookup("anything"), StorageBackend::SketchWarmTier);
        assert_eq!(r.lookup("http_requests_total"), StorageBackend::SketchWarmTier);
        assert_eq!(
            r.lookup_with_shape("anything", QueryShape::Count),
            StorageBackend::SketchWarmTier
        );
    }

    #[test]
    fn yaml_with_per_metric_override_routes_correctly_v6_1_form() {
        // v6.1 form: `metrics:` map. Each value is a single backend.
        let yaml = r#"
default: sketch_warm_tier
metrics:
  http_requests_total: gorilla_s3_archive
  audit_events: cold_jsonl_fallback
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(
            r.lookup("http_requests_total"),
            StorageBackend::GorillaS3Archive
        );
        assert_eq!(
            r.lookup("audit_events"),
            StorageBackend::ColdJsonlFallback
        );
        assert_eq!(r.lookup("unlisted"), StorageBackend::SketchWarmTier);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn yaml_default_only_routes_all_metrics_to_default() {
        let yaml = "default: gorilla_s3_archive\n";
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(r.lookup("anything"), StorageBackend::GorillaS3Archive);
        assert!(r.is_empty());
        assert_eq!(r.default_backend(), StorageBackend::GorillaS3Archive);
    }

    #[test]
    fn yaml_omitted_default_falls_back_to_sketch_warm() {
        let yaml = "metrics:\n  foo: gorilla_s3_archive\n";
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(r.lookup("foo"), StorageBackend::GorillaS3Archive);
        assert_eq!(r.lookup("bar"), StorageBackend::SketchWarmTier);
    }

    #[test]
    fn empty_yaml_is_valid_and_empty() {
        let r = BackendStorageRouting::from_yaml_str("").expect("empty parse");
        assert_eq!(r.lookup("foo"), StorageBackend::SketchWarmTier);
        assert!(r.is_empty());
    }

    #[test]
    fn invalid_yaml_returns_error() {
        let yaml = "metrics: not-a-map\n";
        assert!(BackendStorageRouting::from_yaml_str(yaml).is_err());
    }

    // ── v7 dual-routing tests ──────────────────────────────────────────

    #[test]
    fn yaml_v7_multi_target_routes_count_to_archive_quantile_to_warm() {
        // v7 form: `routes:` list. http_requests_total has TWO
        // targets — the default warm-tier slot and a cold-archive
        // slot scoped to count/topk/rate_post_hoc.
        let yaml = r#"
default: sketch_warm_tier
routes:
  - metric: http_requests_total
    targets:
      - backend: sketch_warm_tier
      - backend: gorilla_s3_archive
        applies_to_query_shape: [count, topk, rate_post_hoc]
  - metric: http_freshness_probe_warm
    targets:
      - backend: sketch_warm_tier
  - metric: http_freshness_probe_archive
    targets:
      - backend: gorilla_s3_archive
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");

        // Count + topk + rate_post_hoc → archive.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Count),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Topk),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::RatePostHoc),
            StorageBackend::GorillaS3Archive,
        );

        // Quantile + sum_over_time + everything else → warm.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Quantile),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Sum),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Other),
            StorageBackend::SketchWarmTier,
        );

        // Single-target metrics keep v6.1 semantics regardless of shape.
        assert_eq!(
            r.lookup_with_shape("http_freshness_probe_warm", QueryShape::LastOverTime),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_freshness_probe_archive", QueryShape::LastOverTime),
            StorageBackend::GorillaS3Archive,
        );
    }

    #[test]
    fn yaml_v6_1_single_target_keeps_old_semantics_under_lookup_with_shape() {
        // A v6.1 entry with no targets list — every shape resolves
        // to the same backend (no dual-routing).
        let yaml = r#"
metrics:
  audit_events: gorilla_s3_archive
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        for shape in [
            QueryShape::Count,
            QueryShape::Topk,
            QueryShape::Quantile,
            QueryShape::Sum,
            QueryShape::Other,
        ] {
            assert_eq!(
                r.lookup_with_shape("audit_events", shape),
                StorageBackend::GorillaS3Archive,
                "shape={shape:?} must resolve to gorilla_s3_archive (single-target)",
            );
        }
    }

    #[test]
    fn yaml_mixed_metrics_and_routes_routes_wins() {
        // Both `metrics:` and `routes:` populated; a metric in BOTH
        // wins from `routes:` (multi-target overrides single-target).
        let yaml = r#"
default: sketch_warm_tier
metrics:
  http_requests_total: cold_jsonl_fallback
routes:
  - metric: http_requests_total
    targets:
      - backend: sketch_warm_tier
      - backend: gorilla_s3_archive
        applies_to_query_shape: [count]
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        // Default slot wins for non-count shapes.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Quantile),
            StorageBackend::SketchWarmTier,
        );
        // Count → archive.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Count),
            StorageBackend::GorillaS3Archive,
        );
        // The `metrics:` entry was overridden — no trace of
        // ColdJsonlFallback.
        assert_eq!(r.target_count("http_requests_total"), 2);
    }

    #[test]
    fn empty_targets_list_is_a_parse_error() {
        let yaml = r#"
routes:
  - metric: http_requests_total
    targets: []
"#;
        let err = BackendStorageRouting::from_yaml_str(yaml).expect_err("must reject empty list");
        assert!(err.to_string().contains("empty targets"));
    }

    #[test]
    fn lookup_falls_back_to_first_target_when_no_shape_matches() {
        // Manually-built entry where every target has a filter and
        // none matches `Quantile`. The lookup must return *some*
        // backend rather than panic.
        let mut metrics = HashMap::new();
        metrics.insert(
            "x".to_string(),
            vec![
                RoutingTarget::for_shapes(StorageBackend::GorillaS3Archive, vec![QueryShape::Count]),
                RoutingTarget::for_shapes(
                    StorageBackend::ColdJsonlFallback,
                    vec![QueryShape::Topk],
                ),
            ],
        );
        let r = BackendStorageRouting::new(StorageBackend::SketchWarmTier, metrics);
        // No filter matches `Quantile`; must return the first target's
        // backend.
        assert_eq!(
            r.lookup_with_shape("x", QueryShape::Quantile),
            StorageBackend::GorillaS3Archive,
        );
    }

    // ── classify_query_shape unit tests ────────────────────────────────

    fn parse(query: &str) -> promql_parser::parser::Expr {
        promql_parser::parser::parse(query).expect("parse")
    }

    #[test]
    fn classifies_count_aggregate_as_count() {
        let e = parse("count(http_requests_total{service=\"payments\"})");
        assert_eq!(classify_query_shape(&e), QueryShape::Count);
    }

    #[test]
    fn classifies_topk_as_topk() {
        let e = parse("topk(5, sum by (zone) (rate(http_requests_total[5m])))");
        assert_eq!(classify_query_shape(&e), QueryShape::Topk);
    }

    #[test]
    fn classifies_rate_call_as_rate_post_hoc() {
        let e = parse("rate(http_requests_total[5m])");
        assert_eq!(classify_query_shape(&e), QueryShape::RatePostHoc);
    }

    #[test]
    fn classifies_sum_by_rate_as_rate_post_hoc() {
        // `sum by (zone) (rate(...))` — the inner rate makes this
        // post-hoc, the outer sum doesn't change that.
        let e = parse("sum by (zone) (rate(http_requests_total[5m]))");
        assert_eq!(classify_query_shape(&e), QueryShape::RatePostHoc);
    }

    #[test]
    fn classifies_quantile_over_time_as_quantile() {
        let e = parse("quantile_over_time(0.99, http_requests_total_latency_ms[1m])");
        assert_eq!(classify_query_shape(&e), QueryShape::Quantile);
    }

    #[test]
    fn classifies_sum_by_as_sum() {
        let e = parse("sum by (zone) (http_requests_total)");
        assert_eq!(classify_query_shape(&e), QueryShape::Sum);
    }

    #[test]
    fn classifies_sum_over_time_as_sum() {
        let e = parse("sum_over_time(http_requests_total[1m])");
        assert_eq!(classify_query_shape(&e), QueryShape::Sum);
    }

    #[test]
    fn classifies_last_over_time_as_last_over_time() {
        let e = parse("last_over_time(http_freshness_probe_warm[10s])");
        assert_eq!(classify_query_shape(&e), QueryShape::LastOverTime);
    }

    #[test]
    fn classifies_bare_selector_as_other() {
        let e = parse("http_requests_total");
        assert_eq!(classify_query_shape(&e), QueryShape::Other);
    }
}
